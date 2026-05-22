"""Tâches cycle de vie des entreprises."""

import logging

from common import get_repo

logger = logging.getLogger(__name__)


def lifecycle_find_stale(**_context):
    repo = get_repo()
    stale = repo.find_stale_companies(days=14)
    ids = [{"id": c.id, "bce_number": c.bce_number} for c in stale]
    logger.info("Entreprises obsolètes: %d", len(ids))
    return ids


def lifecycle_find_inactive(**_context):
    repo = get_repo()
    inactive = repo.find_inactive_status_changes()
    records = [
        {"id": c.id, "bce_number": c.bce_number, "status": c.status}
        for c in inactive
    ]
    logger.info("Entreprises inactives/fermées: %d", len(records))
    return records


def lifecycle_trigger_rescrape(**_context):
    repo = get_repo()
    stale = repo.find_stale_companies(days=14)
    for company in stale:
        repo.enqueue_scrape(company.bce_number, reason="stale_14_days", priority=1)
    logger.info("Remises en file de scraping: %d", len(stale))
    return len(stale)


def lifecycle_archive_closed(**_context):
    repo = get_repo()
    inactive = repo.find_inactive_status_changes()
    archived = 0
    for item in inactive:
        status = item.status if hasattr(item, "status") else item.get("status")
        company_id = item.id if hasattr(item, "id") else item.get("id")
        if status in ("closed", "radiated", "inactive") and company_id:
            repo.archive_company(company_id)
            archived += 1
    logger.info("Entreprises archivées: %d", archived)
    return archived


def lifecycle_sync_kbo_status(**_context):
    """Synchronise le statut des entreprises depuis KBO Open Data."""
    repo = get_repo()
    updated = repo.sync_company_status_from_kbo()
    logger.info("Statuts synchronisés depuis KBO: %d", updated)
    return updated


def lifecycle_process_discoveries(**_context):
    """Traite les découvertes en attente et les remet en file de scraping."""
    repo = get_repo()
    processed = repo.process_pending_discoveries()
    logger.info("Découvertes traitées: %d", processed)
    return processed
