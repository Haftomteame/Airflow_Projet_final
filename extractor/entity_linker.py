"""Découverte dynamique d'entreprises liées via les liens HTML."""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from db.repository import Repository

logger = logging.getLogger(__name__)

BCE_REGEX = re.compile(
    r"(?:ondernemingsnummer|btw|enterprise|bce)[^\d]{0,20}(\d{10})|(\d{4}\.\d{3}\.\d{3})",
    re.IGNORECASE,
)
PLAIN_BCE = re.compile(r"\b(\d{10})\b")


def normalize_bce(raw: str) -> str:
    return raw.replace(".", "").replace(" ", "").strip()


def find_linked_companies(html: str) -> list[str]:
    """Extrait les numéros BCE présents dans les liens et le texte."""
    found: set[str] = set()
    soup = BeautifulSoup(html, "lxml")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        for m in BCE_REGEX.finditer(href):
            bce = normalize_bce(m.group(1) or m.group(2) or "")
            if len(bce) == 10 and bce.isdigit():
                found.add(bce)
        text = a.get_text()
        for m in PLAIN_BCE.finditer(text):
            found.add(m.group(1))

    for m in BCE_REGEX.finditer(html):
        bce = normalize_bce(m.group(1) or m.group(2) or "")
        if len(bce) == 10 and bce.isdigit():
            found.add(bce)

    return sorted(found)


def process_discoveries(
    html: str,
    source_company_id: int,
    source_bce: str,
    repo: Repository | None = None,
) -> list[str]:
    """
    Détecte les BCE liés et les ajoute à discovery_queue si absents en BDD.
    """
    repo = repo or Repository()
    linked = find_linked_companies(html)
    added: list[str] = []

    for bce in linked:
        if bce == source_bce:
            continue
        reason = f"link_from_{source_bce}"
        if repo.add_discovery(source_company_id, bce, reason):
            added.append(bce)
            logger.info("Découverte %s depuis %s", bce, source_bce)

    return added
