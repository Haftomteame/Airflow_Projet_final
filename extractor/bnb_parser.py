"""Extraction des bilans BNB (Centrale des bilans)."""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def _parse_amount(text: str) -> str | None:
    m = re.search(r"[\d\s.,]+", text or "")
    return m.group(0).strip() if m else None


def parse_bnb_html(html: str, bce_number: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    financials: list[dict[str, Any]] = []

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            label = cells[0].lower()
            record = {
                "fiscal_year": None,
                "total_assets": None,
                "equity": None,
                "turnover": None,
                "employees": None,
            }
            if "exercice" in label or "jaar" in label or re.match(r"^\d{4}$", cells[0]):
                record["fiscal_year"] = cells[0]
            for i, cell in enumerate(cells):
                cl = cell.lower() if i == 0 else ""
            low = label
            val = cells[-1]
            if any(k in low for k in ("actif", "total assets", "totaal activa")):
                record["total_assets"] = _parse_amount(val)
            elif any(k in low for k in ("capitaux", "equity", "eigen vermogen")):
                record["equity"] = _parse_amount(val)
            elif any(k in low for k in ("chiffre", "turnover", "omzet")):
                record["turnover"] = _parse_amount(val)
            elif any(k in low for k in ("employ", "personeel", "travailleurs")):
                record["employees"] = _parse_amount(val)
            if any(record.values()):
                financials.append(record)

    # Extraction par regex si tables absentes
    if not financials:
        year_matches = re.findall(r"(20\d{2})", html)
        asset_match = re.search(r"(?:actif\s+total|total\s+assets)[:\s]*([\d\s.,]+)", html, re.I)
        if year_matches or asset_match:
            financials.append({
                "fiscal_year": year_matches[0] if year_matches else None,
                "total_assets": asset_match.group(1).strip() if asset_match else None,
                "equity": None,
                "turnover": None,
                "employees": None,
            })

    logger.info("BNB: %d enregistrements financiers pour %s", len(financials), bce_number)
    return {"bce_number": bce_number, "financials": financials}
