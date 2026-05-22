"""
Pipeline lifecycle — détection puis actions (séquentiel, fail-fast).
"""

from datetime import datetime

from airflow import DAG

from common import PIPELINE_DEFAULT_ARGS, trigger_task_dag

with DAG(
    dag_id="dag_pipeline_lifecycle",
    default_args=PIPELINE_DEFAULT_ARGS,
    description="Pipeline lifecycle: sync KBO → stale → inactive → rescrape → archive → découvertes",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "lifecycle"],
) as dag:
    t_sync = trigger_task_dag("trigger_sync_kbo", "dag_t_lifecycle_sync_kbo")
    t_stale = trigger_task_dag("trigger_find_stale", "dag_t_lifecycle_find_stale")
    t_inactive = trigger_task_dag("trigger_find_inactive", "dag_t_lifecycle_find_inactive")
    t_rescrape = trigger_task_dag("trigger_rescrape", "dag_t_lifecycle_rescrape")
    t_archive = trigger_task_dag("trigger_archive", "dag_t_lifecycle_archive")
    t_discoveries = trigger_task_dag("trigger_discoveries", "dag_t_lifecycle_process_discoveries")

    t_sync >> t_stale >> t_inactive >> t_rescrape >> t_archive >> t_discoveries
