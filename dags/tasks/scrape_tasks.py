"""Tâches d'acquisition (scraping)."""

import json
import logging
from datetime import datetime, timezone

from airflow.models import Variable

from common import (
    get_hdfs,
    get_mongo,
    get_repo,
    get_scraper,
    load_bce_from_csv,
    seed_companies_from_csv,
)

logger = logging.getLogger(__name__)

SCRAPE_STALE_VAR = "pipeline_scrape_stale_bces"
# Sources dont l'échec total bloque le pipeline (Moniteur/BNB : SPA externes instables)
SOURCES_REQUIRE_STORAGE = frozenset({"kbo"})


def _store_scrape_results(results: list[dict]) -> int:
    from datetime import datetime as dt

    hdfs = get_hdfs()
    mongo = get_mongo()
    repo = get_repo()
    stored = 0

    for item in results:
        if not item.get("valid") or item.get("status_code") != 200 or not item.get("html"):
            repo.log_error(
                "validation" if item.get("status_code") == 200 else "scraping",
                f"Page non stockée: {item.get('url')}",
                bce_number=item.get("bce_number"),
                source=item.get("source"),
            )
            proxy_label = item.get("proxy_used") or ""
            if item.get("status_code") == 0 and proxy_label not in ("", "direct"):
                repo.log_error(
                    "proxy",
                    "Échec proxy",
                    bce_number=item.get("bce_number"),
                    source=item.get("source"),
                )
            continue

        bce = item["bce_number"]
        source = item["source"]
        company = repo.upsert_company({"bce_number": bce})
        page = item.get("page", 1)
        total_pages = item.get("total_pages", 1)
        meta = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "status": "success",
            "http_code": item["status_code"],
            "proxy_used": item.get("proxy_used", ""),
            "attempts": item.get("attempts", 1),
            "page": page,
            "total_pages": total_pages,
        }
        html_path = hdfs.save_html(bce, source, item["html"], page=page)
        pg_meta = repo.add_scrape_metadata({
            "company_id": company.id,
            "source": source,
            "hdfs_path": html_path,
            "scraped_at": dt.utcnow(),
            "http_code": item["status_code"],
            "proxy_used": item.get("proxy_used"),
            "attempts": item.get("attempts"),
            "status": "success",
            "parsed": False,
        })
        mongo_id = mongo.insert_scrape_metadata(
            bce_number=bce,
            company_id=company.id,
            source=source,
            hdfs_html_path=html_path,
            scraped_at=meta["scraped_at"],
            http_code=item["status_code"],
            proxy_used=item.get("proxy_used"),
            attempts=item.get("attempts", 1),
            status="success",
            page=page,
            total_pages=total_pages,
            postgres_meta_id=pg_meta.id,
        )
        repo.set_scrape_mongo_id(pg_meta.id, mongo_id)
        repo.upsert_company({"bce_number": bce, "last_scraped": dt.utcnow()})
        stored += 1

    logger.info("Documents stockés (HTML HDFS + métadonnées MongoDB): %d", stored)
    return stored


def scrape_prepare_batch(**_context):
    """Prépare la liste BCE à scraper (partagée via Variable Airflow)."""
    seed_companies_from_csv([])
    bce_list = load_bce_from_csv()
    repo = get_repo()
    queued = repo.dequeue_scrape_batch(limit=0)
    db_stale = [c.bce_number for c in repo.find_stale_companies(days=14)]
    combined = list(dict.fromkeys(bce_list + queued + db_stale))
    stale = repo.filter_fresh_companies(combined, days=14)
    Variable.set(SCRAPE_STALE_VAR, json.dumps(stale))
    logger.info("BCE à traiter (stale): %d", len(stale))
    if not stale:
        logger.warning("Aucun BCE à scraper — les DAGs sources peuvent échouer volontairement")
    return len(stale)


def _run_scrape_source(source: str, **_context):
    stale = json.loads(Variable.get(SCRAPE_STALE_VAR, "[]"))
    if not stale:
        raise ValueError(
            "Liste BCE vide. Exécutez d'abord dag_t_scrape_prepare (pipeline scraping)."
        )
    scraper = get_scraper()
    results = scraper.scrape_batch(stale, source=source)
    stored = _store_scrape_results(results)
    invalid = sum(1 for r in results if r.get("valid") is False)
    if stored == 0 and results:
        msg = (
            f"Scraping {source}: {len(results)} pages, {invalid} invalides, "
            f"aucune stockée (vérifier validateur, proxies ou portail source)"
        )
        if source in SOURCES_REQUIRE_STORAGE:
            raise RuntimeError(msg)
        logger.warning("%s — le pipeline scraping continue", msg)
    companies = len({r.get("bce_number") for r in results if r.get("bce_number")})
    if invalid:
        logger.warning(
            "Scraping %s: %d/%d pages rejetées par validation (%d entreprises)",
            source,
            invalid,
            len(results),
            companies,
        )
    logger.info(
        "Scraping %s: %d entreprises, %d pages HTML, %d stockées",
        source,
        companies,
        len(results),
        stored,
    )
    return {
        "source": source,
        "scraped": len(results),
        "stored": stored,
        "invalid": invalid,
        "companies": companies,
    }


def scrape_kbo(**_context):
    return _run_scrape_source("kbo")


def scrape_moniteur(**_context):
    return _run_scrape_source("moniteur")


def scrape_bnb(**_context):
    return _run_scrape_source("bnb")
