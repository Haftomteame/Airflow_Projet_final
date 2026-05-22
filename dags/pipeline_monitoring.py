"""
Pipeline monitoring — snapshot unique (planifiable seul).
"""

from datetime import datetime

from airflow import DAG

from common import PIPELINE_DEFAULT_ARGS, trigger_task_dag

with DAG(
    dag_id="dag_pipeline_monitoring",
    default_args=PIPELINE_DEFAULT_ARGS,
    description="Pipeline monitoring: collecte snapshot",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "monitoring"],
) as dag:
    trigger_task_dag("trigger_snapshot", "dag_t_monitoring_snapshot")
