"""
DAGs unitaires — une tâche métier = un DAG (monitoring granulaire dans l'UI Airflow).
Déclenchés par les DAGs orchestrateurs (pipelines) ou manuellement.
"""

import os
from datetime import timedelta

from common import build_task_dag
from tasks.analytics_tasks import (
    analytics_by_nace,
    analytics_by_postal_code,
    analytics_clear_tables,
    analytics_financial_ranking,
    analytics_open_closed_ratio,
    analytics_temporal,
)
from tasks.extraction_tasks import (
    extract_list_unparsed_files,
    extract_parse_bnb,
    extract_parse_kbo,
    extract_parse_moniteur,
)
from tasks.kbo_tasks import (
    kbo_create_views,
    kbo_import_schema_and_data,
    kbo_seed_companies,
    kbo_sync_scrape_queue,
)
from tasks.lifecycle_tasks import (
    lifecycle_archive_closed,
    lifecycle_find_inactive,
    lifecycle_find_stale,
    lifecycle_process_discoveries,
    lifecycle_sync_kbo_status,
    lifecycle_trigger_rescrape,
)
from tasks.monitoring_tasks import collect_monitoring_snapshot
from tasks.scrape_tasks import (
    scrape_advance_batch_offset,
    scrape_bnb,
    scrape_kbo,
    scrape_moniteur,
    scrape_prepare_batch,
)

_TASK_SPECS = [
    # KBO
    ("dag_t_kbo_import_data", kbo_import_schema_and_data, "KBO: schéma SQL + import CSV", ["kbo", "import"]),
    ("dag_t_kbo_seed_companies", kbo_seed_companies, "KBO: seed table companies", ["kbo", "sql"]),
    ("dag_t_kbo_create_views", kbo_create_views, "KBO: vues analytics SQL", ["kbo", "sql"]),
    ("dag_t_kbo_sync_queue", kbo_sync_scrape_queue, "KBO: alimentation file scraping", ["kbo", "queue"]),
    # Scraping
    ("dag_t_scrape_prepare", scrape_prepare_batch, "Scraping: préparer lot BCE", ["scraping", "prepare"]),
    ("dag_t_scrape_kbo", scrape_kbo, "Scraping: source KBO", ["scraping", "kbo"]),
    ("dag_t_scrape_moniteur", scrape_moniteur, "Scraping: source Moniteur", ["scraping", "moniteur"]),
    ("dag_t_scrape_bnb", scrape_bnb, "Scraping: source BNB", ["scraping", "bnb"]),
    (
        "dag_t_scrape_advance_batch",
        scrape_advance_batch_offset,
        "Scraping: avancer curseur lot KBO",
        ["scraping", "batch"],
    ),
    # Extraction
    ("dag_t_extract_list_files", extract_list_unparsed_files, "Extraction: lister fichiers HDFS non parsés", ["extraction"]),
    ("dag_t_extract_parse_kbo", extract_parse_kbo, "Extraction: parser HTML KBO", ["extraction", "kbo"]),
    ("dag_t_extract_parse_moniteur", extract_parse_moniteur, "Extraction: parser HTML Moniteur", ["extraction", "moniteur"]),
    ("dag_t_extract_parse_bnb", extract_parse_bnb, "Extraction: parser HTML BNB", ["extraction", "bnb"]),
    # Lifecycle
    ("dag_t_lifecycle_sync_kbo", lifecycle_sync_kbo_status, "Lifecycle: sync statuts KBO Open Data", ["lifecycle", "kbo"]),
    ("dag_t_lifecycle_find_stale", lifecycle_find_stale, "Lifecycle: entreprises obsolètes", ["lifecycle"]),
    ("dag_t_lifecycle_find_inactive", lifecycle_find_inactive, "Lifecycle: entreprises inactives", ["lifecycle"]),
    ("dag_t_lifecycle_rescrape", lifecycle_trigger_rescrape, "Lifecycle: remise en file scraping", ["lifecycle"]),
    ("dag_t_lifecycle_archive", lifecycle_archive_closed, "Lifecycle: archivage entreprises fermées", ["lifecycle"]),
    ("dag_t_lifecycle_process_discoveries", lifecycle_process_discoveries, "Lifecycle: traiter file découvertes", ["lifecycle", "discovery"]),
    # Analytics
    ("dag_t_analytics_clear", analytics_clear_tables, "Analytics: vider tables rapports", ["analytics"]),
    ("dag_t_analytics_postal", analytics_by_postal_code, "Analytics: rapport par code postal", ["analytics"]),
    ("dag_t_analytics_nace", analytics_by_nace, "Analytics: rapport par NACE", ["analytics"]),
    ("dag_t_analytics_financial", analytics_financial_ranking, "Analytics: classement financier", ["analytics"]),
    ("dag_t_analytics_ratio", analytics_open_closed_ratio, "Analytics: ratio ouvert/fermé", ["analytics"]),
    ("dag_t_analytics_temporal", analytics_temporal, "Analytics: évolution temporelle", ["analytics"]),
    # Monitoring
    ("dag_t_monitoring_snapshot", collect_monitoring_snapshot, "Monitoring: snapshot métriques", ["monitoring"]),
]

_SCRAPE_TIMEOUT_HOURS = int(os.getenv("SCRAPE_TASK_TIMEOUT_HOURS", "2"))
_SCRAPE_MONITEUR_TIMEOUT_HOURS = int(
    os.getenv("SCRAPE_MONITEUR_TIMEOUT_HOURS", str(max(_SCRAPE_TIMEOUT_HOURS, 12)))
)

_TASK_EXTRA_KWARGS: dict[str, dict] = {
    "dag_t_kbo_import_data": {
        "execution_timeout": timedelta(hours=3),
        "do_xcom_push": False,
    },
    "dag_t_scrape_prepare": {
        "execution_timeout": timedelta(hours=1),
    },
    "dag_t_scrape_kbo": {
        "execution_timeout": timedelta(hours=_SCRAPE_TIMEOUT_HOURS),
    },
    "dag_t_scrape_moniteur": {
        "execution_timeout": timedelta(hours=_SCRAPE_MONITEUR_TIMEOUT_HOURS),
    },
    "dag_t_scrape_bnb": {
        "execution_timeout": timedelta(hours=_SCRAPE_TIMEOUT_HOURS),
    },
}

for _dag_id, _callable, _desc, _tags in _TASK_SPECS:
    globals()[_dag_id] = build_task_dag(
        _dag_id,
        _callable,
        _desc,
        _tags,
        **_TASK_EXTRA_KWARGS.get(_dag_id, {}),
    )
