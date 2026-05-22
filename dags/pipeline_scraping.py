"""
Pipeline scraping — séquentiel (plus de parallèle) : une erreur bloque la suite.
"""

from datetime import datetime

from airflow import DAG

from common import PIPELINE_DEFAULT_ARGS, trigger_task_dag

with DAG(
    dag_id="dag_pipeline_scraping",
    default_args=PIPELINE_DEFAULT_ARGS,
    description="Pipeline scraping: prepare → KBO → Moniteur → BNB → extraction",
    # Déclenché par dag_pipeline_kbo (hebdo) — pas de schedule pour éviter
    # max_active_runs=1 bloqué par des runs planifiés en parallèle.
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "scraping"],
) as dag:
    t_prepare = trigger_task_dag("trigger_prepare", "dag_t_scrape_prepare")
    t_kbo = trigger_task_dag("trigger_kbo", "dag_t_scrape_kbo")
    t_mon = trigger_task_dag("trigger_moniteur", "dag_t_scrape_moniteur")
    t_bnb = trigger_task_dag("trigger_bnb", "dag_t_scrape_bnb")
    t_extract = trigger_task_dag("trigger_extraction_pipeline", "dag_pipeline_extraction")

    t_prepare >> t_kbo >> t_mon >> t_bnb >> t_extract
