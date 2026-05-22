"""Extraction des publications Moniteur belge."""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def parse_moniteur_html(html: str, bce_number: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    publications: list[dict[str, Any]] = []

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        title = " ".join(cells[0].get_text().split())
        date = " ".join(cells[1].get_text().split()) if len(cells) > 1 else None
        link = cells[0].find("a")
        url = link["href"] if link and link.get("href") else None
        if title and len(title) > 5:
            publications.append({
                "title": title[:512],
                "publication_date": date,
                "url": url,
                "raw_excerpt": title[:1000],
            })

    # Fallback: liens numac
    if not publications:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "numac" in href.lower() or "moniteur" in href.lower():
                title = " ".join(a.get_text().split())
                if title:
                    publications.append({
                        "title": title[:512],
                        "publication_date": None,
                        "url": href,
                        "raw_excerpt": title[:500],
                    })

    logger.info("Moniteur: %d publications pour %s", len(publications), bce_number)
    return {"bce_number": bce_number, "publications": publications}
