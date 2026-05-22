"""
Pipeline extraction — parsing séquentiel par source.
"""

from datetime import datetime

from airflow import DAG

from common import PIPELINE_DEFAULT_ARGS, trigger_task_dag

with DAG(
    dag_id="dag_pipeline_extraction",
    default_args=PIPELINE_DEFAULT_ARGS,
    description="Pipeline extraction: lister → parser KBO → Moniteur → BNB",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "extraction"],
) as dag:
    t_list = trigger_task_dag("trigger_list_files", "dag_t_extract_list_files")
    t_kbo = trigger_task_dag("trigger_parse_kbo", "dag_t_extract_parse_kbo")
    t_mon = trigger_task_dag("trigger_parse_moniteur", "dag_t_extract_parse_moniteur")
    t_bnb = trigger_task_dag("trigger_parse_bnb", "dag_t_extract_parse_bnb")

    t_list >> t_kbo >> t_mon >> t_bnb
