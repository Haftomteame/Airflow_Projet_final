"""Validation des pages HTML scrapées avant stockage HDFS."""

import logging
import re
from typing import Final

logger = logging.getLogger(__name__)

GLOBAL_ERROR_PATTERNS: Final[list[str]] = [
    r"page\s+not\s+found",
    r"404\s+not\s+found",
    r"internal\s+server\s+error",
    r"erreur\s+technique",
    r"technical\s+error",
    r"application\s+error",
    # Éviter \berror\b / \bfout\b : faux positifs sur KBO (JS, attributs HTML, etc.)
    r"aucun\s+résultat",
    r"geen\s+resultaat",
    r"entreprise\s+introuvable",
    r"onderneming\s+niet\s+gevonden",
]

SOURCE_PATTERNS: Final[dict[str, list[str]]] = {
    "kbo": [
        r"numéro\s+d'entreprise\s+inconnu",
        r"ondernemingsnummer\s+onbekend",
        r"la\s+recherche\s+n'a\s+donné\s+aucun\s+résultat",
    ],
    "moniteur": [
        r"aucune\s+publication",
        r"no\s+records\s+found",
        r"geen\s+gegevens",
    ],
    "bnb": [
        r"no\s+data\s+available",
        r"aucune\s+donnée",
        r"entreprise\s+non\s+répertoriée",
        r"company\s+not\s+found",
    ],
}


def _matches_patterns(html: str, patterns: list[str]) -> bool:
    text = html.lower()
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def is_valid_page(html: str | None, source: str) -> bool:
    """
    Retourne True si la page semble valide pour la source donnée.
    """
    if not html or len(html.strip()) < 200:
        logger.warning("Page trop courte ou vide pour source=%s", source)
        return False

    if _matches_patterns(html, GLOBAL_ERROR_PATTERNS):
        logger.warning("Signaux d'erreur globaux détectés source=%s", source)
        return False

    source_patterns = SOURCE_PATTERNS.get(source, [])
    if source_patterns and _matches_patterns(html, source_patterns):
        logger.warning("Signaux d'erreur spécifiques source=%s", source)
        return False

    # Heuristiques positives minimales par source (tolère les SPA modernes)
    lower = html.lower()
    if source == "kbo" and "onderneming" not in lower and "entreprise" not in lower:
        return False
    if source == "moniteur" and not any(
        k in lower for k in ("moniteur", "ejustice", "justitie", "publication", "numac")
    ):
        return False
    if source == "bnb" and not any(
        k in lower for k in ("nbb", "bilan", "cbso", "consult", "banque nationale")
    ):
        return False

    return True
