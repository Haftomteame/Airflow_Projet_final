"""Détection de pagination pour les sources HTML paginées."""

import logging
import re
from typing import Final

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

PAGE_PARAM_RE: Final[re.Pattern[str]] = re.compile(r"page=(\d+)", re.IGNORECASE)
LISTE_TOTAL_RE: Final[re.Pattern[str]] = re.compile(r"Liste\s*\((\d+)\)", re.IGNORECASE)


def normalize_bce(bce_number: str) -> tuple[str, str]:
    """
    Retourne (view_numac 10 chiffres, btw sans zéros initiaux).
    Ex. 0203430576 -> ('0203430576', '203430576')
    """
    digits = re.sub(r"\D", "", bce_number or "")
    if not digits:
        raise ValueError(f"Numéro BCE invalide: {bce_number!r}")
    view_numac = digits.zfill(10)[-10:]
    btw = view_numac.lstrip("0") or view_numac
    return view_numac, btw


def extract_max_page_from_html(html: str) -> int:
    """Numéro de page le plus élevé visible dans les liens de pagination."""
    pages = [int(p) for p in PAGE_PARAM_RE.findall(html)]
    return max(pages) if pages else 1


def extract_list_total(html: str) -> int | None:
    """Total annoncé dans l'en-tête « Liste (N) » du Moniteur."""
    match = LISTE_TOTAL_RE.search(html)
    return int(match.group(1)) if match else None


def moniteur_page_has_results(html: str) -> bool:
    """True si la page contient des publications (boutons DETAIL ou contenu substantiel)."""
    if not html or len(html.strip()) < 2000:
        return False
    soup = BeautifulSoup(html, "html.parser")
    details = soup.find_all(string=re.compile(r"DETAIL", re.IGNORECASE))
    if details:
        return True
    # Pages vides du Moniteur (~10 ko) vs pages remplies (~50–100 ko)
    return len(html) > 20000


def compute_moniteur_last_page(html: str, results_per_page: int = 100) -> int:
    """
    Estime la dernière page à partir du total « Liste (N) » et des liens visibles.
    """
    link_max = extract_max_page_from_html(html)
    total = extract_list_total(html)
    if total and total > 0:
        estimated = max(1, (total + results_per_page - 1) // results_per_page)
        return max(link_max, estimated)
    return link_max
