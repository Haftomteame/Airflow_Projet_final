"""Extraction structurée des pages KBO publiques."""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BCE_PATTERN = re.compile(r"\b(\d{4}\.?\d{3}\.?\d{3})\b|\b(\d{10})\b")


def _clean(text: str | None) -> str | None:
    if not text:
        return None
    return " ".join(text.split()).strip() or None


def _map_status(raw: str | None) -> str:
    if not raw:
        return "active"
    low = raw.lower()
    if any(x in low for x in ("radié", "radiation", "opgeheven", "cessation")):
        return "radiated"
    if any(x in low for x in ("inactif", "inactive", "gesloten", "fermé", "closed")):
        return "inactive" if "inactif" in low or "inactive" in low else "closed"
    return "active"


def parse_kbo_html(html: str, bce_number: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    data: dict[str, Any] = {
        "bce_number": bce_number,
        "name": None,
        "address": None,
        "postal_code": None,
        "legal_form": None,
        "status": "active",
        "creation_date": None,
        "nace_codes": [],
        "directors": [],
        "establishments": [],
        "vat_activities": [],
    }

    # Nom — titre ou premier h1/h2
    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = _clean(tag.get_text())
        if text and len(text) > 3:
            data["name"] = text
            break

    # Tableaux KBO typiques
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [_clean(c.get_text()) for c in row.find_all(["th", "td"])]
            if len(cells) < 2:
                continue
            label, value = cells[0], cells[1]
            if not label or not value:
                continue
            low = label.lower()
            if "dénomination" in low or "naam" in low:
                data["name"] = data["name"] or value
            elif "adresse" in low or "adres" in low:
                data["address"] = value
            elif "code postal" in low or "postcode" in low:
                m = re.search(r"(\d{4})", value)
                data["postal_code"] = m.group(1) if m else value[:16]
            elif "forme juridique" in low or "rechtsvorm" in low:
                data["legal_form"] = value
            elif "statut" in low or "status" in low or "situation" in low:
                data["status"] = _map_status(value)
            elif "date" in low and ("création" in low or "oprichting" in low or "start" in low):
                data["creation_date"] = value
            elif "nace" in low or "activité" in low:
                codes = re.findall(r"\d{4,5}", value)
                data["nace_codes"].extend(codes)
            elif "tva" in low:
                data["vat_activities"].append(value)

    # Dirigeants — lignes de tableaux mandats (exclut les champs entité type "Dénomination:")
    director_roles = (
        "administrateur",
        "gérant",
        "directeur",
        "mandataire",
        "bestuurder",
        "délégué",
        "président",
        "commissaire",
    )
    for section in soup.find_all("table"):
        text = section.get_text(" ", strip=True).lower()
        if not any(k in text for k in director_roles):
            continue
        for row in section.find_all("tr"):
            cells = [_clean(c.get_text()) for c in row.find_all("td")]
            if len(cells) < 2:
                continue
            name, role = cells[0], cells[1]
            if not name or not role or name.endswith(":"):
                continue
            if len(name) > 80 and ":" in name:
                continue
            data["directors"].append({
                "name": name[:256],
                "role": role[:128],
                "start_date": (cells[2][:32] if len(cells) > 2 and cells[2] else None),
            })

    # Établissements
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "vestiging" in href or "etablissement" in href:
            name = _clean(link.get_text())
            if name:
                data["establishments"].append({"name": name, "url": href})

    if data["nace_codes"]:
        data["nace_code"] = data["nace_codes"][0]
    else:
        data["nace_code"] = None

    logger.info("KBO parsé pour %s: %s", bce_number, data.get("name"))
    return data
