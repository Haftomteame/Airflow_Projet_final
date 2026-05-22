"""Gestion de la rotation des proxies avec blacklist et chargement depuis le cache."""

import logging
import os
import threading

import requests

from scraper.proxy_fetcher import (
    load_proxy_cache_file,
    load_tor_proxies,
    normalize_proxy_line,
    resolve_active_proxies,
    sort_proxies_http_first,
    to_requests_proxy_url,
)

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes")


class ProxyManager:
    def __init__(self, proxy_list: list[str] | None = None, remote_url: str | None = None):
        self._lock = threading.Lock()
        self._index = 0
        self._blacklist: set[str] = set()
        self._proxies: list[str] = []
        self._tor_proxies: list[str] = []
        self._tor_set: set[str] = set()

        use_tor = _env_bool("PROXY_USE_TOR", "true")
        if use_tor:
            self._tor_proxies = load_tor_proxies()
            self._tor_set = set(self._tor_proxies)
            self._proxies.extend(self._tor_proxies)

        if proxy_list:
            for item in proxy_list:
                normalized = normalize_proxy_line(item) if "://" not in item else item
                if normalized:
                    self._proxies.append(normalized)

        env_list = os.getenv("PROXY_LIST", "")
        if env_list:
            for item in env_list.split(","):
                normalized = normalize_proxy_line(item.strip())
                if normalized:
                    self._proxies.append(normalized)

        use_cache = _env_bool("PROXY_USE_CACHE_FILE", "true")
        if use_cache:
            ignore_ttl = _env_bool("PROXY_CACHE_IGNORE_TTL", "true")
            cached = load_proxy_cache_file(ignore_ttl=ignore_ttl)
            if cached:
                self._proxies.extend(cached)

        non_tor = [p for p in self._proxies if p not in self._tor_set]
        if not non_tor:
            env_url = remote_url or os.getenv("PROXY_LIST_URL", "")
            auto_fetch = _env_bool("PROXY_AUTO_FETCH", "false")
            validate = _env_bool("PROXY_VALIDATE", "true")

            if env_url or auto_fetch:
                fetched = resolve_active_proxies(remote_url=env_url, extra=self._proxies.copy())
                if fetched:
                    self._proxies.extend(fetched)
                elif env_url and not validate:
                    self._proxies.extend(self._load_remote(env_url))
            elif env_url:
                self._proxies.extend(self._load_remote(env_url))

        self._proxies = sort_proxies_http_first(self._dedupe(self._proxies))

        if self._proxies:
            http_count = sum(1 for p in self._proxies if p.startswith("http://"))
            tor_count = len(self._tor_proxies)
            logger.info(
                "ProxyManager: %d proxies (%d Tor, %d HTTP, rotation active)",
                len(self._proxies),
                tor_count,
                http_count,
            )
        else:
            logger.warning(
                "ProxyManager: aucun proxy — le scraping utilisera la connexion directe"
            )

    @staticmethod
    def _dedupe(proxies: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for p in proxies:
            if p not in seen:
                seen.add(p)
                deduped.append(p)
        return deduped

    @staticmethod
    def _load_remote(url: str) -> list[str]:
        try:
            resp = requests.get(url, timeout=45)
            resp.raise_for_status()
            proxies = []
            for line in resp.text.splitlines():
                normalized = normalize_proxy_line(line.strip())
                if normalized:
                    proxies.append(normalized)
            logger.info("Chargé %d proxies depuis %s", len(proxies), url)
            return proxies
        except Exception as exc:
            logger.warning("Échec chargement proxies distants: %s", exc)
            return []

    def _pool_for_url(self, url: str) -> list[str]:
        pool = [p for p in self._proxies if p not in self._blacklist]
        if not pool:
            return []

        tor = [p for p in pool if p in self._tor_set]
        http = [p for p in pool if p.startswith("http://")]
        socks5h = [
            p for p in pool
            if p.startswith("socks5h://") and p not in self._tor_set
        ]
        socks5 = [p for p in pool if p.startswith("socks5://")]

        if url.startswith("https://"):
            if _env_bool("PROXY_HTTPS_HTTP_ONLY", "false"):
                ordered = tor + http + socks5h
                for p in socks5:
                    upgraded = p.replace("socks5://", "socks5h://", 1)
                    if upgraded not in ordered:
                        ordered.append(upgraded)
                return ordered or pool

            # Mode legacy : HTTP d'abord, puis Tor/socks5h si HTTP épuisés
            if http:
                return tor + http
            if tor or socks5h:
                return tor + socks5h
            upgraded = [p.replace("socks5://", "socks5h://", 1) for p in socks5]
            if upgraded:
                logger.debug(
                    "Pool HTTPS: %d proxies SOCKS5h (HTTP épuisés)",
                    len(upgraded),
                )
                return upgraded

        return pool

    def get_next_proxy(self, url: str = "") -> dict[str, str] | None:
        with self._lock:
            pool = self._pool_for_url(url) if url else [p for p in self._proxies if p not in self._blacklist]
            if not pool:
                return None
            for _ in range(len(pool)):
                proxy = pool[self._index % len(pool)]
                self._index += 1
                if proxy not in self._blacklist:
                    effective = to_requests_proxy_url(proxy, url)
                    return {"http": effective, "https": effective}
            logger.warning("Tous les proxies sont blacklistés — connexion directe")
            return None

    def blacklist_proxy(self, proxy_dict: dict[str, str] | None) -> None:
        if not proxy_dict:
            return
        proxy = proxy_dict.get("http") or proxy_dict.get("https")
        if not proxy:
            return
        # Normaliser socks5h → socks5 pour la clé blacklist (même proxy physique)
        canonical = proxy.replace("socks5h://", "socks5://")
        with self._lock:
            self._blacklist.add(canonical)
            if proxy != canonical:
                self._blacklist.add(proxy)
            logger.info(
                "Proxy blacklisté (%d restants): %s",
                self.available_count,
                proxy,
            )

    def report_success(self, proxy_dict: dict[str, str] | None) -> None:
        if proxy_dict:
            logger.debug("Proxy OK: %s", proxy_dict.get("http"))

    @property
    def available_count(self) -> int:
        return len([p for p in self._proxies if p not in self._blacklist])

    @property
    def total_count(self) -> int:
        return len(self._proxies)
