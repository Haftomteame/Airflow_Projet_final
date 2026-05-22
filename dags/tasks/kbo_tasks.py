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
        load_kbo_addresses_for_loaded_enterprises,
        run_sql_file,
        sync_postal_codes_to_companies,
    )

    schema_dir = get_kbo_schema_dir()
    db_url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(schema_dir / "02_seed_companies.sql", db_url)
    addresses = load_kbo_addresses_for_loaded_enterprises(db_url=db_url)
    with_postal = sync_postal_codes_to_companies(db_url)
    logger.info(
        "Seed companies KBO — adresses: %d lignes, %d entreprises avec code postal",
        addresses,
        with_postal,
    )
    return {"seed": True, "addresses_loaded": addresses, "companies_with_postal": with_postal}


def kbo_create_views(**_context):
    from db.kbo_loader import get_kbo_schema_dir, run_sql_file

    schema_dir = get_kbo_schema_dir()
    db_url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(schema_dir / "03_analytics_views.sql", db_url)
    logger.info("Vues analytics KBO créées")
    return {"views": True}


def kbo_sync_scrape_queue(**_context):
    from sqlalchemy import text

    repo = get_repo()
    limit = int(os.getenv("KBO_SCRAPE_QUEUE_LIMIT", "500"))
    with repo.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT bce_number FROM v_kbo_bce_queue WHERE bce_number IS NOT NULL")
        ).fetchall()
    enqueued = 0
    for (bce,) in rows[:limit]:
        repo.enqueue_scrape(bce, reason="kbo_opendata_sql", priority=0)
        enqueued += 1
    logger.info("File scraping alimentée: %d BCE", enqueued)
    return enqueued
