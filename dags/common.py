"""Utilitaires partagés entre les DAGs."""

import csv
import logging
import os
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import false

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from db.repository import Repository
from scraper.hdfs_client import HDFSClient
from scraper.proxy_manager import ProxyManager
from scraper.scraper import BelgianScraper

logger = logging.getLogger(__name__)

DATA_PATH = Path("/opt/airflow/data/companies.csv")

# Tâches unitaires : retries limités, une seule exécution à la fois
TASK_DAG_DEFAULT_ARGS = {
    "owner": "belgian-companies",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

# Orchestrateurs : pas de retry sur le trigger (l'échec doit bloquer la suite)
PIPELINE_DEFAULT_ARGS = {
    "owner": "belgian-companies",
    "retries": 0,
    "depends_on_past": False,
}

# Attente bloquante sur le worker Celery (deferrable=False, demandé explicitement).
TRIGGER_WAIT_KWARGS = {
    "wait_for_completion": True,
    "poke_interval": 30,
    "allowed_states": ["success"],
    "failed_states": ["failed"],
    "deferrable": False,
}

# Timeout max d'une attente (scraping / extraction peuvent durer des heures)
TRIGGER_EXECUTION_TIMEOUT = timedelta(
    hours=int(os.getenv("TRIGGER_EXECUTION_TIMEOUT_HOURS", "24"))
)


def get_repo() -> Repository:
    return Repository()


def get_scraper() -> BelgianScraper:
    return BelgianScraper(ProxyManager())


def get_hdfs() -> HDFSClient:
    return HDFSClient()


def get_mongo():
    """Import paresseux : évite l'échec au parse DAG si pymongo n'est pas encore installé."""
    from db.mongo_client import MongoMetadataStore

    return MongoMetadataStore()


def trigger_task_dag(
    task_id: str,
    trigger_dag_id: str,
    *,
    execution_timeout: timedelta | None = None,
) -> TriggerDagRunOperator:
    """Déclenche un DAG tâche et attend le succès ; échec = blocage des tâches suivantes."""
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        reset_dag_run=True,
        execution_timeout=execution_timeout or TRIGGER_EXECUTION_TIMEOUT,
        **TRIGGER_WAIT_KWARGS,
    )


def build_task_dag(
    dag_id: str,
    python_callable: Callable[..., Any],
    description: str,
    tags: list[str],
    *,
    execution_timeout: timedelta | None = None,
    do_xcom_push: bool = True,
) -> DAG:
    """Construit un DAG à une seule tâche (monitoring granulaire dans l'UI)."""
    op_kwargs: dict[str, Any] = {"do_xcom_push": do_xcom_push}
    if execution_timeout is not None:
        op_kwargs["execution_timeout"] = execution_timeout

    with DAG(
        dag_id=dag_id,
        default_args=TASK_DAG_DEFAULT_ARGS,
        description=description,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["task"] + tags,
    ) as dag:
        PythonOperator(
            task_id="run",
            python_callable=python_callable,
            **op_kwargs,
        )
    return dag


def load_bce_from_kbo_sql(
    *,
    offset: int | None = None,
    limit: int | None = None,
) -> list[str]:
    """Lit les numéros BCE depuis SQL KBO (lot paginé ORDER BY + OFFSET)."""
    from batch_utils import get_kbo_batch_limit, get_kbo_batch_offset

    repo = get_repo()
    batch_limit = get_kbo_batch_limit() if limit is None else max(0, int(limit))
    batch_offset = get_kbo_batch_offset() if offset is None else max(0, int(offset))

    if batch_limit <= 0:
        sql = """
            SELECT bce_number FROM (
                SELECT DISTINCT REPLACE(enterprise_number, '.', '') AS bce_number
                FROM kbo_enterprise
                WHERE status = 'AC'
                UNION
                SELECT bce_number FROM companies WHERE source = 'kbo_opendata'
            ) q
            WHERE bce_number IS NOT NULL AND bce_number <> ''
            ORDER BY bce_number
        """
        params: dict[str, int] = {}
    else:
        sql = """
            SELECT bce_number FROM (
                SELECT DISTINCT REPLACE(enterprise_number, '.', '') AS bce_number
                FROM kbo_enterprise
                WHERE status = 'AC'
                UNION
                SELECT bce_number FROM companies WHERE source = 'kbo_opendata'
            ) q
            WHERE bce_number IS NOT NULL AND bce_number <> ''
            ORDER BY bce_number
            OFFSET :offset LIMIT :limit
        """
        params = {"offset": batch_offset, "limit": batch_limit}

    try:
        with repo.engine.connect() as conn:
            from sqlalchemy import text

            rows = conn.execute(text(sql), params).fetchall()
        bces = [r[0] for r in rows]
        if bces:
            logger.info(
                "Chargé %d numéros BCE depuis SQL KBO (offset=%d, limit=%s)",
                len(bces),
                batch_offset,
                batch_limit if batch_limit > 0 else "∞",
            )
            return bces
        if batch_limit > 0:
            logger.warning(
                "Aucun BCE au offset %d (limit=%d) — fin du jeu ou curseur trop avancé",
                batch_offset,
                batch_limit,
            )
    except Exception as exc:
        logger.warning("SQL KBO indisponible (%s), repli sur companies.csv", exc)
    return []


def load_bce_from_csv() -> list[str]:
    from batch_utils import get_kbo_batch_limit, get_kbo_batch_offset

    kbo_bces = load_bce_from_kbo_sql()
    if kbo_bces:
        return kbo_bces

    path = os.getenv("COMPANIES_CSV", str(DATA_PATH))
    if not Path(path).exists():
        path = "/opt/airflow/data/companies.csv"
    bces = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bce = row.get("bce_number", "").strip().replace(".", "")
            if bce:
                bces.append(bce)
    bces.sort()
    batch_limit = get_kbo_batch_limit()
    batch_offset = get_kbo_batch_offset()
    if batch_limit > 0:
        bces = bces[batch_offset : batch_offset + batch_limit]
    logger.info(
        "Chargé %d numéros BCE depuis CSV (offset=%d, total fichier trié)",
        len(bces),
        batch_offset,
    )
    return bces


def seed_companies_from_csv(bce_list: list[dict]) -> None:
    """Seed initial uniquement (le seed massif KBO est réservé au pipeline KBO)."""
    repo = get_repo()
    try:
        with repo.engine.connect() as conn:
            from sqlalchemy import text

            existing = conn.execute(text("SELECT COUNT(*) FROM companies")).scalar() or 0
        if existing > 0:
            logger.info("Table companies déjà remplie (%d lignes), seed ignoré", existing)
            return
    except Exception as exc:
        logger.warning("Vérification companies ignorée: %s", exc)

    path = os.getenv("COMPANIES_CSV", str(DATA_PATH))
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bce = row.get("bce_number", "").strip().replace(".", "")
            if bce:
                repo.upsert_company({
                    "bce_number": bce,
                    "name": row.get("name"),
                    "status": "active" if row.get("status") == "AC" else row.get("status", "active"),
                    "source": "csv",
                })
