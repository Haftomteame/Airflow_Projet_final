"""
Pipeline KBO — enchaîne les DAGs tâches ; s'arrête dès la première erreur.
"""

from datetime import datetime

from airflow import DAG

from common import PIPELINE_DEFAULT_ARGS, trigger_task_dag

with DAG(
    dag_id="dag_pipeline_kbo",
    default_args=PIPELINE_DEFAULT_ARGS,
    description="Pipeline KBO: import → seed → vues → file scraping → pipeline scraping",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "kbo"],
) as dag:
    t_import = trigger_task_dag("trigger_import_data", "dag_t_kbo_import_data")
    t_seed = trigger_task_dag("trigger_seed", "dag_t_kbo_seed_companies")
    t_views = trigger_task_dag("trigger_views", "dag_t_kbo_create_views")
    t_queue = trigger_task_dag("trigger_sync_queue", "dag_t_kbo_sync_queue")
    t_scrape = trigger_task_dag("trigger_scrape_pipeline", "dag_pipeline_scraping")

    t_import >> t_seed >> t_views >> t_queue >> t_scrape
