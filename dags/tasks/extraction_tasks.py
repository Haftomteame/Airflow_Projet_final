"""Tâches d'extraction / parsing HDFS → PostgreSQL."""

import logging

from common import get_hdfs, get_repo
from db.models import Company
from extractor.bnb_parser import parse_bnb_html
from extractor.entity_linker import process_discoveries
from extractor.kbo_parser import parse_kbo_html
from extractor.moniteur_parser import parse_moniteur_html

logger = logging.getLogger(__name__)

PARSERS = {
    "kbo": parse_kbo_html,
    "moniteur": parse_moniteur_html,
    "bnb": parse_bnb_html,
}


def _persist_parsed_record(repo, data: dict) -> None:
    company_id = data.get("_company_id")
    bce = data.get("bce_number")

    if "name" in data or "address" in data:
        repo.upsert_company({
            "bce_number": bce,
            "name": data.get("name"),
            "address": data.get("address"),
            "postal_code": data.get("postal_code"),
            "status": data.get("status", "active"),
            "legal_form": data.get("legal_form"),
            "nace_code": data.get("nace_code"),
        })

    if data.get("directors") and company_id:
        repo.replace_directors(company_id, data["directors"])

    if data.get("publications") and company_id:
        repo.replace_publications(company_id, data["publications"])

    if data.get("financials") and company_id:
        repo.replace_financials(company_id, data["financials"])


def extract_list_unparsed_files(**_context):
    repo = get_repo()
    unparsed = repo.list_unparsed_hdfs()
    count = len(unparsed)
    logger.info("Fichiers HDFS non parsés: %d", count)
    if count == 0:
        logger.warning("Aucun fichier à parser — vérifiez qu'un scraping a réussi")
    return count


def _parse_source(source: str, **_context):
    repo = get_repo()
    hdfs = get_hdfs()
    files = repo.list_unparsed_hdfs(source=source)
    if not files:
        logger.info("Aucun fichier %s à parser", source)
        return {"source": source, "parsed": 0, "discoveries": 0}

    parser = PARSERS[source]
    parsed_count = 0
    discovery_count = 0

    for meta in files:
        try:
            html = hdfs.read_document(meta.hdfs_path)
            with repo.session() as s:
                company = s.get(Company, meta.company_id)
            bce = company.bce_number if company else ""

            data = parser(html, bce)
            data["_meta_id"] = meta.id
            data["_company_id"] = meta.company_id
            data["_html"] = html

            _persist_parsed_record(repo, data)
            repo.mark_parsed(meta.id)
            parsed_count += 1

            if meta.company_id and html:
                added = process_discoveries(html, meta.company_id, bce, repo)
                discovery_count += len(added)
        except Exception as exc:
            repo.log_error(
                "parsing",
                str(exc),
                company_id=meta.company_id,
                source=source,
            )
            logger.exception("Erreur parsing %s meta_id=%s", source, meta.id)
            # Continue sur les autres fichiers ; échec global si aucun succès
            continue

    if files and parsed_count == 0:
        raise RuntimeError(
            f"Extraction {source}: {len(files)} fichier(s) en file, aucun parsé avec succès"
        )

    logger.info("Parsing %s: %d fichiers, %d découvertes", source, parsed_count, discovery_count)
    return {"source": source, "parsed": parsed_count, "discoveries": discovery_count}


def extract_parse_kbo(**_context):
    return _parse_source("kbo")


def extract_parse_moniteur(**_context):
    return _parse_source("moniteur")


def extract_parse_bnb(**_context):
    return _parse_source("bnb")
