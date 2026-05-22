"""Tâches KBO Open Data."""

import logging
import os

from common import get_repo

logger = logging.getLogger(__name__)


def kbo_import_schema_and_data(**context):
    from db.kbo_loader import (
        get_kbo_schema_dir,
        load_all_csv,
        migrate_kbo_activity_primary_key,
        run_sql_file,
    )

    schema_dir = get_kbo_schema_dir()
    db_url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(schema_dir / "01_schema.sql", db_url)
    migrate_kbo_activity_primary_key(db_url)
    counts = load_all_csv(db_url=db_url, context=context)
    total_rows = sum(counts.values())
    logger.info("Import CSV KBO terminé: %d lignes sur %d fichiers", total_rows, len(counts))
    # Retour léger (évite blocage XCom / state mismatch Celery)
    return {"status": "ok", "files": len(counts), "rows": total_rows}


def kbo_seed_companies(**_context):
    from db.kbo_loader import (
        get_kbo_schema_dir,
        load_kbo_activities_for_loaded_enterprises,
        load_kbo_addresses_for_loaded_enterprises,
        run_sql_file,
        sync_nace_codes_to_companies,
        sync_postal_codes_to_companies,
    )

    schema_dir = get_kbo_schema_dir()
    db_url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(schema_dir / "02_seed_companies.sql", db_url)
    addresses = load_kbo_addresses_for_loaded_enterprises(db_url=db_url)
    activities = load_kbo_activities_for_loaded_enterprises(db_url=db_url)
    with_postal = sync_postal_codes_to_companies(db_url)
    with_nace = sync_nace_codes_to_companies(db_url)
    logger.info(
        "Seed companies KBO — adresses: %d, activités: %d, %d CP, %d NACE",
        addresses,
        activities,
        with_postal,
        with_nace,
    )
    return {
        "seed": True,
        "addresses_loaded": addresses,
        "activities_loaded": activities,
        "companies_with_postal": with_postal,
        "companies_with_nace": with_nace,
    }


def kbo_create_views(**_context):
    from db.kbo_loader import get_kbo_schema_dir, run_sql_file

    schema_dir = get_kbo_schema_dir()
    db_url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(schema_dir / "03_analytics_views.sql", db_url)
    logger.info("Vues analytics KBO créées")
    return {"views": True}


def kbo_sync_scrape_queue(**_context):
    from batch_utils import get_kbo_batch_limit, get_kbo_batch_offset
    from common import load_bce_from_csv

    repo = get_repo()
    batch_limit = get_kbo_batch_limit()
    batch_offset = get_kbo_batch_offset()
    bces = load_bce_from_csv()
    if not bces:
        logger.warning(
            "File scraping : lot vide (offset=%d, limit=%d)",
            batch_offset,
            batch_limit,
        )
        return 0
    enqueued = 0
    for bce in bces:
        repo.enqueue_scrape(bce, reason="kbo_opendata_sql", priority=0)
        enqueued += 1
    logger.info(
        "File scraping alimentée: %d BCE (offset=%d, limit=%d)",
        enqueued,
        batch_offset,
        batch_limit,
    )
    return enqueued
