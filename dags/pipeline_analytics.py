"""
Pipeline analytics — rapports générés un par un.
"""

from datetime import datetime

from airflow import DAG

from common import PIPELINE_DEFAULT_ARGS, trigger_task_dag

with DAG(
    dag_id="dag_pipeline_analytics",
    default_args=PIPELINE_DEFAULT_ARGS,
    description="Pipeline analytics: clear → postal → nace → financial → ratio → temporal",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "analytics"],
) as dag:
    t_clear = trigger_task_dag("trigger_clear", "dag_t_analytics_clear")
    t_postal = trigger_task_dag("trigger_postal", "dag_t_analytics_postal")
    t_nace = trigger_task_dag("trigger_nace", "dag_t_analytics_nace")
    t_fin = trigger_task_dag("trigger_financial", "dag_t_analytics_financial")
    t_ratio = trigger_task_dag("trigger_ratio", "dag_t_analytics_ratio")
    t_temporal = trigger_task_dag("trigger_temporal", "dag_t_analytics_temporal")

    t_clear >> t_postal >> t_nace >> t_fin >> t_ratio >> t_temporal
