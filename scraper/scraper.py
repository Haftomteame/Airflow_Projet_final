"""Scraping KBO, Moniteur belge et BNB."""

import logging
import os
import random
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlencode

import requests

from scraper.pagination import (
    compute_moniteur_last_page,
    extract_max_page_from_html,
    moniteur_page_has_results,
    normalize_bce,
)
from scraper.proxy_manager import ProxyManager
from scraper.validator import is_valid_page

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36",
]

DEFAULT_TIMEOUT = 30
MONITEUR_MAX_PAGES = int(os.getenv("MONITEUR_MAX_PAGES", "500"))
SCRAPE_PAGE_DELAY = float(os.getenv("SCRAPE_PAGE_DELAY", "0.3"))
PROXY_MAX_ROTATIONS = int(os.getenv("PROXY_MAX_ROTATIONS", "50"))
PROXY_RETRY_DELAY = float(os.getenv("PROXY_RETRY_DELAY", "0.5"))
PROXY_BLACKLIST_ON_RATE_LIMIT = os.getenv("PROXY_BLACKLIST_ON_RATE_LIMIT", "false").lower() in (
    "1",
    "true",
    "yes",
)


def _should_blacklist_proxy(status_code: int) -> bool:
    if status_code in (403, 429):
        return PROXY_BLACKLIST_ON_RATE_LIMIT
    return status_code not in (0, 200)


def scrape_page_with_rotation(
    url: str,
    proxy_manager: ProxyManager,
    *,
    allow_direct: bool = True,
) -> tuple[str | None, int, int, str]:
    """
    Télécharge une page en changeant de proxy à chaque échec (pas 3× le même).
    Retourne (html, status_code, attempts, proxy_label).
    """
    available = proxy_manager.available_count
    max_rotations = max(5, min(PROXY_MAX_ROTATIONS, available or 5))
    last_status = 0
    total_attempts = 0
    proxy_used = ""
    tried_direct = False

    for rot in range(max_rotations):
        proxy = proxy_manager.get_next_proxy(url)
        if proxy is None:
            if allow_direct and not tried_direct:
                tried_direct = True
                proxy_label = "direct"
                proxy = None
            else:
                break
        else:
            proxy_label = proxy.get("http", "direct")

        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "fr-BE,fr;q=0.9,nl;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        total_attempts += 1
        try:
            resp = requests.get(
                url,
                headers=headers,
                proxies=proxy,
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=True,
            )
            last_status = resp.status_code
            if resp.status_code == 200:
                proxy_manager.report_success(proxy)
                logger.info(
                    "Scrape OK %s via %s (rotation %d/%d)",
                    url,
                    proxy_label,
                    rot + 1,
                    max_rotations,
                )
                return resp.text, resp.status_code, total_attempts, proxy_label

            if resp.status_code == 500 and resp.text and len(resp.text.strip()) > 500:
                logger.warning(
                    "HTTP 500 avec corps HTML pour %s via %s — poursuite",
                    url,
                    proxy_label,
                )
                proxy_manager.report_success(proxy)
                return resp.text, resp.status_code, total_attempts, proxy_label

            logger.warning(
                "HTTP %s pour %s via %s (rotation %d)",
                resp.status_code,
                url,
                proxy_label,
                rot + 1,
            )
            if proxy and _should_blacklist_proxy(resp.status_code):
                proxy_manager.blacklist_proxy(proxy)
            elif resp.status_code in (403, 429):
                if PROXY_RETRY_DELAY > 0:
                    time.sleep(PROXY_RETRY_DELAY * 2)
        except requests.RequestException as exc:
            logger.error(
                "Erreur %s via %s (rotation %d/%d): %s",
                url,
                proxy_label,
                rot + 1,
                max_rotations,
                exc,
            )
            last_status = 0
            if proxy:
                proxy_manager.blacklist_proxy(proxy)

        if PROXY_RETRY_DELAY > 0:
            time.sleep(PROXY_RETRY_DELAY)

    return None, last_status, total_attempts, proxy_used


# Alias rétrocompatibilité (tests / imports existants)
def scrape_page(
    url: str,
    proxy: dict[str, str] | None = None,
    retries: int = 3,
) -> tuple[str | None, int, int]:
    """Ancienne API : un seul proxy fixe. Préférer scrape_page_with_rotation + ProxyManager."""
    last_status = 0
    for attempt in range(1, retries + 1):
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "fr-BE,fr;q=0.9,nl;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        try:
            resp = requests.get(
                url,
                headers=headers,
                proxies=proxy,
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=True,
            )
            last_status = resp.status_code
            if resp.status_code == 200:
                return resp.text, resp.status_code, attempt
            if resp.status_code == 500 and resp.text and len(resp.text.strip()) > 500:
                return resp.text, resp.status_code, attempt
        except requests.RequestException:
            last_status = 0
        time.sleep(min(attempt * 2, 10))
    return None, last_status, retries


class BelgianScraper:
    def __init__(self, proxy_manager: ProxyManager | None = None):
        self.proxy_manager = proxy_manager or ProxyManager()
        self.kbo_base = os.getenv(
            "KBO_BASE_URL",
            "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html",
        )
        self.moniteur_base = os.getenv(
            "MONITEUR_BASE_URL",
            "https://www.ejustice.just.fgov.be/cgi_tsv/list.pl",
        )
        self.bnb_base = os.getenv("BNB_BASE_URL", "https://consult.cbso.nbb.be/")

    def _scrape_with_proxy(self, url: str, source: str) -> dict[str, Any]:
        html, status_code, attempts, proxy_label = scrape_page_with_rotation(
            url,
            self.proxy_manager,
        )

        if not html:
            return {
                "html": None,
                "status_code": status_code,
                "attempts": attempts,
                "proxy_used": proxy_label,
                "valid": False,
                "url": url,
            }

        acceptable_status = status_code == 200 or (
            source == "moniteur" and status_code == 500 and is_valid_page(html, source)
        )
        if not acceptable_status:
            return {
                "html": None,
                "status_code": status_code,
                "attempts": attempts,
                "proxy_used": proxy_label,
                "valid": False,
                "url": url,
            }

        if not is_valid_page(html, source):
            return {
                "html": None,
                "status_code": status_code,
                "attempts": attempts,
                "proxy_used": proxy_label,
                "valid": False,
                "url": url,
            }

        return {
            "html": html,
            "status_code": status_code,
            "attempts": attempts,
            "proxy_used": proxy_label,
            "valid": True,
            "url": url,
        }

    def _finalize_result(
        self,
        result: dict[str, Any],
        *,
        source: str,
        bce_number: str,
        page: int,
        total_pages: int | None = None,
    ) -> dict[str, Any]:
        result["source"] = source
        result["bce_number"] = bce_number
        result["page"] = page
        if total_pages is not None:
            result["total_pages"] = total_pages
        return result

    def _scrape_single_page(
        self,
        bce_number: str,
        source: str,
        url: str,
        page: int = 1,
    ) -> dict[str, Any]:
        result = self._scrape_with_proxy(url, source)
        return self._finalize_result(result, source=source, bce_number=bce_number, page=page)

    def _scrape_paginated(
        self,
        bce_number: str,
        source: str,
        build_url: Callable[[str, int], str],
        *,
        resolve_last_page: Callable[[str, int], int] | None = None,
        page_has_results: Callable[[str], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Scrape toutes les pages d'une entreprise avant de passer à la suivante.
        """
        page = 1
        last_page = 1
        results: list[dict[str, Any]] = []

        while page <= last_page and page <= MONITEUR_MAX_PAGES:
            url = build_url(bce_number, page)
            logger.info("Scrape %s %s page %d/%d", source, bce_number, page, last_page)
            result = self._scrape_with_proxy(url, source)
            result = self._finalize_result(
                result,
                source=source,
                bce_number=bce_number,
                page=page,
                total_pages=last_page,
            )
            results.append(result)

            if not result.get("valid") or not result.get("html"):
                logger.warning(
                    "Arrêt pagination %s %s : page %d invalide",
                    source,
                    bce_number,
                    page,
                )
                break

            html = result["html"]
            if page_has_results and not page_has_results(html):
                logger.info(
                    "Arrêt pagination %s %s : page %d sans résultats",
                    source,
                    bce_number,
                    page,
                )
                break

            if resolve_last_page:
                last_page = max(last_page, resolve_last_page(html, page))
            else:
                last_page = max(last_page, extract_max_page_from_html(html))

            if page >= last_page:
                break

            page += 1
            if SCRAPE_PAGE_DELAY > 0:
                time.sleep(SCRAPE_PAGE_DELAY)

        final_total = len(results)
        for item in results:
            item["total_pages"] = final_total

        logger.info(
            "Scrape %s %s terminé : %d page(s) collectée(s)",
            source,
            bce_number,
            len(results),
        )
        return results

    def build_kbo_url(self, bce_number: str, page: int = 1) -> str:
        params = {"lang": "fr", "ondernemingsnummer": bce_number}
        if page > 1:
            params["page"] = str(page)
        return f"{self.kbo_base}?{urlencode(params)}"

    def build_moniteur_url(self, bce_number: str, page: int = 1) -> str:
        view_numac, btw = normalize_bce(bce_number)
        params = {
            "language": "fr",
            "sum_date": "",
            "page": str(page),
            "view_numac": view_numac,
            "btw": btw,
        }
        return f"{self.moniteur_base}?{urlencode(params)}"

    def build_bnb_url(self, bce_number: str, page: int = 1) -> str:
        base = self.bnb_base.rstrip("/")
        digits = "".join(c for c in bce_number if c.isdigit())
        formatted = (
            f"{digits[:4]}.{digits[4:7]}.{digits[7:10]}"
            if len(digits) >= 10
            else bce_number
        )
        url = f"{base}/enterprise/{formatted}?lang=FR"
        if page > 1:
            url += f"&page={page}"
        return url

    def scrape_kbo(self, bce_number: str) -> list[dict[str, Any]]:
        """KBO : une fiche par entreprise (pas de pagination multi-pages)."""
        url = self.build_kbo_url(bce_number)
        logger.info("Scrape KBO %s", bce_number)
        return [self._scrape_single_page(bce_number, "kbo", url)]

    def scrape_moniteur(self, bce_number: str) -> list[dict[str, Any]]:
        """Moniteur : toutes les pages de la liste avant l'entreprise suivante."""
        logger.info("Scrape Moniteur %s (pagination)", bce_number)
        return self._scrape_paginated(
            bce_number,
            "moniteur",
            self.build_moniteur_url,
            resolve_last_page=lambda html, current: max(
                compute_moniteur_last_page(html),
                extract_max_page_from_html(html),
                current,
            ),
            page_has_results=moniteur_page_has_results,
        )

    def scrape_bnb(self, bce_number: str) -> list[dict[str, Any]]:
        """BNB : une fiche par entreprise (SPA, pas de pagination HTML)."""
        url = self.build_bnb_url(bce_number)
        logger.info("Scrape BNB %s", bce_number)
        return [self._scrape_single_page(bce_number, "bnb", url)]

    def scrape_batch(
        self,
        bce_numbers: list[str],
        source: str,
        *,
        on_progress: Callable[[int, int, str], None] | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        total = len(bce_numbers)
        for index, bce in enumerate(bce_numbers):
            if on_progress:
                on_progress(index, total, bce)
            if source == "kbo":
                results.extend(self.scrape_kbo(bce))
            elif source == "moniteur":
                results.extend(self.scrape_moniteur(bce))
            elif source == "bnb":
                results.extend(self.scrape_bnb(bce))
            else:
                raise ValueError(f"Source inconnue: {source}")
        return results
