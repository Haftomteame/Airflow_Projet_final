"""Récupération ProxyScrape, validation des proxies actifs et cache local."""

from __future__ import annotations

import logging
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

PROXYSCRAPE_BASE = "https://api.proxyscrape.com/v2/"
DEFAULT_PROTOCOLS = ("http", "socks5", "socks4")
PROTOCOL_PREFIX = {
    "http": "http",
    "https": "http",
    "socks5": "socks5",
    "socks4": "socks4",
    "socks": "socks5",
}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
# HTTP d'abord : la plupart des proxies gratuits ne supportent pas HTTPS
DEFAULT_TEST_URLS = (
    "http://httpbin.org/ip",
    "http://www.google.com/generate_204",
    "https://httpbin.org/get",
)
# Ports courants → protocole probable (comme les vérificateurs en ligne)
PORT_HINTS: dict[int, str] = {
    80: "http",
    443: "http",
    8080: "http",
    3128: "http",
    8127: "http",
    8888: "http",
    999: "http",
    1080: "socks5",
    1081: "socks5",
    4145: "socks4",
    9050: "socks5",
}


def _proxyscrape_api_url(protocol: str, timeout_ms: int) -> str:
    return (
        f"{PROXYSCRAPE_BASE}?request=displayproxies"
        f"&protocol={protocol}&timeout={timeout_ms}"
        "&country=all&ssl=all&anonymity=all"
    )


def normalize_proxy_line(line: str, default_protocol: str = "http") -> str | None:
    """Convertit ip:port ou URL en proxy utilisable par requests."""
    raw = line.strip()
    if not raw or ":" not in raw:
        return None
    if raw.startswith(("http://", "https://", "socks4://", "socks5://", "socks5h://")):
        parsed = urlparse(raw if "://" in raw else f"http://{raw}")
        if not parsed.hostname or not parsed.port:
            return None
        scheme = parsed.scheme.lower()
        if scheme in ("http", "https"):
            return f"http://{parsed.hostname}:{parsed.port}"
        if scheme == "socks5h":
            return f"socks5h://{parsed.hostname}:{parsed.port}"
        if scheme in ("socks4", "socks5"):
            return f"{scheme}://{parsed.hostname}:{parsed.port}"
        return None
    host, _, port_str = raw.partition(":")
    if not host or not port_str.isdigit():
        return None
    port = int(port_str)
    prefix = PORT_HINTS.get(port, PROTOCOL_PREFIX.get(default_protocol.lower(), "http"))
    return f"{prefix}://{host}:{port}"


def _proxy_variants(proxy_url: str) -> list[str]:
    """Variantes SOCKS (socks5h = résolution DNS distante, souvent plus fiable)."""
    if proxy_url.startswith("socks5://"):
        return [proxy_url, proxy_url.replace("socks5://", "socks5h://", 1)]
    return [proxy_url]


def _tcp_reachable(host: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _parse_test_urls() -> tuple[str, ...]:
    raw = os.getenv("PROXY_VALIDATE_URLS", "")
    if raw.strip():
        return tuple(u.strip() for u in raw.split(",") if u.strip())
    single = os.getenv("PROXY_VALIDATE_URL", "").strip()
    if single:
        return (single,)
    return DEFAULT_TEST_URLS


def _http_probe(proxy_url: str, test_urls: tuple[str, ...], timeout: float) -> bool:
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    for variant in _proxy_variants(proxy_url):
        proxies = {"http": variant, "https": variant}
        for url in test_urls:
            try:
                resp = requests.head(
                    url,
                    proxies=proxies,
                    timeout=timeout,
                    headers=headers,
                    allow_redirects=True,
                )
                if resp.status_code < 500:
                    return True
            except requests.RequestException:
                pass
            try:
                resp = requests.get(
                    url,
                    proxies=proxies,
                    timeout=timeout,
                    headers=headers,
                    allow_redirects=True,
                    stream=True,
                )
                resp.close()
                # Comme les vérificateurs : toute réponse HTTP valide = proxy actif
                if 100 <= resp.status_code < 500:
                    return True
            except requests.RequestException:
                pass
    return False


def _probe_proxy(proxy_url: str, test_urls: tuple[str, ...], timeout: float) -> bool:
    parsed = urlparse(proxy_url)
    if not parsed.hostname or not parsed.port:
        return False
    tcp_timeout = min(3.0, timeout)
    if not _tcp_reachable(parsed.hostname, parsed.port, tcp_timeout):
        return False
    return _http_probe(proxy_url, test_urls, timeout)


def fetch_proxyscrape_all(
    protocols: tuple[str, ...] | None = None,
    timeout_ms: int = 10000,
) -> list[str]:
    """Télécharge toutes les listes ProxyScrape (HTTP, SOCKS4, SOCKS5)."""
    protocols = protocols or DEFAULT_PROTOCOLS
    seen: set[str] = set()
    proxies: list[str] = []

    for proto in protocols:
        url = _proxyscrape_api_url(proto, timeout_ms)
        try:
            resp = requests.get(url, timeout=45)
            resp.raise_for_status()
            count = 0
            for line in resp.text.splitlines():
                normalized = normalize_proxy_line(line, default_protocol=proto)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    proxies.append(normalized)
                    count += 1
            logger.info("ProxyScrape [%s]: %d proxies", proto, count)
        except requests.RequestException as exc:
            logger.warning("ProxyScrape [%s] indisponible: %s", proto, exc)

    logger.info("ProxyScrape total: %d proxies uniques", len(proxies))
    return proxies


def load_proxy_file(path: Path) -> list[str]:
    if not path.is_file():
        return []
    proxies: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        normalized = normalize_proxy_line(line.strip())
        if normalized:
            proxies.append(normalized)
    logger.info("Fichier proxies: %d entrées (%s)", len(proxies), path)
    return proxies


def _sort_candidates(proxies: list[str]) -> list[str]:
    """HTTP en premier, puis SOCKS5, SOCKS4 — sans mélange aléatoire."""
    return sorted(
        proxies,
        key=lambda p: (
            0 if p.startswith("http://") else 1 if p.startswith("socks5") else 2,
            p,
        ),
    )


def filter_active_proxies(
    proxies: list[str],
    *,
    test_urls: tuple[str, ...] | None = None,
    timeout: float | None = None,
    max_to_test: int | None = None,
    workers: int | None = None,
) -> list[str]:
    """Teste les proxies en parallèle ; garde tous ceux qui répondent (pas d'arrêt anticipé)."""
    if not proxies:
        return []

    test_urls = test_urls or _parse_test_urls()
    timeout = timeout if timeout is not None else float(os.getenv("PROXY_VALIDATE_TIMEOUT", "10"))
    max_raw = int(os.getenv("PROXY_VALIDATE_MAX", "0"))
    max_to_test = len(proxies) if max_raw <= 0 else min(max_raw, len(proxies))
    workers = workers if workers is not None else int(os.getenv("PROXY_VALIDATE_WORKERS", "60"))

    candidates = _sort_candidates(proxies)[:max_to_test]
    active: list[str] = []
    seen_active: set[str] = set()

    logger.info(
        "Validation de %d/%d proxies (workers=%d, timeout=%.1fs, urls=%d)…",
        len(candidates),
        len(proxies),
        workers,
        timeout,
        len(test_urls),
    )
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_probe_proxy, p, test_urls, timeout): p for p in candidates}
        for future in as_completed(futures):
            proxy = futures[future]
            try:
                if future.result() and proxy not in seen_active:
                    seen_active.add(proxy)
                    active.append(proxy)
                    if len(active) % 10 == 0:
                        logger.info("Proxies actifs trouvés: %d…", len(active))
            except Exception:
                pass

    elapsed = time.monotonic() - started
    logger.info("%d proxies actifs sur %d testés (%.1fs)", len(active), len(candidates), elapsed)
    return active


def _normalize_proxy_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        normalized = normalize_proxy_line(line.strip())
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def load_tor_proxies() -> list[str]:
    """Proxies Tor dédiés (socks5h) — priorité pour KBO HTTPS."""
    raw = os.getenv("TOR_PROXIES", "").strip()
    if not raw:
        return []
    out: list[str] = []
    for item in raw.split(","):
        normalized = normalize_proxy_line(item.strip())
        if normalized:
            if normalized.startswith("socks5://"):
                normalized = normalized.replace("socks5://", "socks5h://", 1)
            out.append(normalized)
    return out


def to_requests_proxy_url(proxy_url: str, target_url: str = "") -> str:
    """socks5h pour HTTPS (résolution DNS via le proxy, requis pour Tor/KBO)."""
    if target_url.startswith("https://") and proxy_url.startswith("socks5://"):
        return proxy_url.replace("socks5://", "socks5h://", 1)
    return proxy_url


def sort_proxies_http_first(proxies: list[str]) -> list[str]:
    """Tor/socks5h → HTTP → SOCKS (meilleur taux de succès KBO HTTPS)."""
    tor_hosts = {p for p in load_tor_proxies()}

    def _rank(p: str) -> tuple[int, str]:
        if p in tor_hosts or p.startswith("socks5h://"):
            return (0, p)
        if p.startswith("http://"):
            return (1, p)
        if p.startswith("socks5://"):
            return (2, p)
        return (3, p)

    return sorted(proxies, key=_rank)


def load_proxy_cache(cache_path: Path, ttl_hours: float) -> list[str] | None:
    if not cache_path.is_file():
        return None
    age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
    if ttl_hours > 0 and age_hours > ttl_hours:
        logger.info("Cache proxies expiré (%.1fh > %.1fh)", age_hours, ttl_hours)
        return None
    raw = [ln.strip() for ln in cache_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    proxies = sort_proxies_http_first(_normalize_proxy_lines(raw))
    if proxies:
        logger.info("Cache proxies: %d actifs (%s)", len(proxies), cache_path)
    return proxies or None


def load_proxy_cache_file(path: Path | None = None, *, ignore_ttl: bool = False) -> list[str]:
    """Charge tous les proxies du fichier cache (sans re-télécharger ProxyScrape)."""
    cache_path = path or Path(
        os.getenv("PROXY_CACHE_FILE", "/opt/airflow/data/proxies_active.txt")
    )
    if not cache_path.is_file():
        return []
    ttl_hours = 0.0 if ignore_ttl else float(os.getenv("PROXY_CACHE_TTL_HOURS", "6"))
    if not ignore_ttl and ttl_hours > 0:
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours > ttl_hours:
            logger.info("Cache proxies expiré (%.1fh) — re-fetch possible", age_hours)
            return []
    raw = [ln.strip() for ln in cache_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    proxies = sort_proxies_http_first(_normalize_proxy_lines(raw))
    if proxies:
        logger.info("Fichier proxies actifs: %d entrées (%s)", len(proxies), cache_path)
    return proxies


def save_proxy_cache(cache_path: Path, proxies: list[str]) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("\n".join(proxies) + "\n", encoding="utf-8")
        logger.info("Cache proxies enregistré: %d → %s", len(proxies), cache_path)
    except OSError as exc:
        logger.warning("Impossible d'écrire le cache proxies: %s", exc)


def resolve_active_proxies(
    *,
    remote_url: str | None = None,
    extra: list[str] | None = None,
) -> list[str]:
    """
    Charge les proxies actifs : cache → fichier → ProxyScrape → validation.
    Ne lève jamais d'exception (retourne [] en cas d'échec).
    """
    try:
        validate = os.getenv("PROXY_VALIDATE", "true").lower() in ("1", "true", "yes")
        auto_fetch = os.getenv("PROXY_AUTO_FETCH", "true").lower() in ("1", "true", "yes")
        ttl_hours = float(os.getenv("PROXY_CACHE_TTL_HOURS", "6"))
        cache_file = Path(
            os.getenv("PROXY_CACHE_FILE", "/opt/airflow/data/proxies_active.txt")
        )

        use_cache_file = os.getenv("PROXY_USE_CACHE_FILE", "true").lower() in (
            "1",
            "true",
            "yes",
        )
        if use_cache_file:
            ignore_ttl = os.getenv("PROXY_CACHE_IGNORE_TTL", "false").lower() in (
                "1",
                "true",
                "yes",
            )
            from_file = load_proxy_cache_file(cache_file, ignore_ttl=ignore_ttl)
            if from_file:
                return from_file

        if validate:
            cached = load_proxy_cache(cache_file, ttl_hours)
            if cached:
                return cached

        collected: list[str] = []
        if extra:
            for item in extra:
                normalized = normalize_proxy_line(item.strip())
                if normalized:
                    collected.append(normalized)

        list_file = os.getenv("PROXY_LIST_FILE", "").strip()
        if list_file:
            collected.extend(load_proxy_file(Path(list_file)))

        url = remote_url or os.getenv("PROXY_LIST_URL", "")
        use_proxyscrape = auto_fetch or "proxyscrape.com" in url.lower()

        if use_proxyscrape:
            protocols_raw = os.getenv("PROXY_FETCH_PROTOCOLS", "http,socks5,socks4")
            protocols = tuple(p.strip() for p in protocols_raw.split(",") if p.strip())
            timeout_ms = int(os.getenv("PROXY_FETCH_TIMEOUT_MS", "10000"))
            collected.extend(fetch_proxyscrape_all(protocols=protocols, timeout_ms=timeout_ms))
        elif url:
            try:
                resp = requests.get(url, timeout=45)
                resp.raise_for_status()
                for line in resp.text.splitlines():
                    normalized = normalize_proxy_line(line.strip())
                    if normalized:
                        collected.append(normalized)
            except requests.RequestException as exc:
                logger.warning("Chargement PROXY_LIST_URL: %s", exc)

        seen: set[str] = set()
        unique: list[str] = []
        for p in collected:
            if p not in seen:
                seen.add(p)
                unique.append(p)

        if not unique:
            logger.warning("Aucun proxy récupéré")
            return []

        if not validate:
            return unique

        active = sort_proxies_http_first(filter_active_proxies(unique))
        if active:
            save_proxy_cache(cache_file, active)
        else:
            logger.warning(
                "Aucun proxy actif après validation — scraping en connexion directe"
            )
        return active
    except Exception as exc:
        logger.error("resolve_active_proxies: %s", exc, exc_info=True)
        return []
