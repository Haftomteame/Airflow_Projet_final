"""Tâche de collecte monitoring."""

import logging
from datetime import datetime, timedelta

from common import get_repo
from db.models import Company, ScrapeError, ScrapeMetadata, ScrapeQueue

logger = logging.getLogger(__name__)


def collect_monitoring_snapshot(**_context):
    repo = get_repo()
    with repo.session() as s:
        nb_attente = (
            s.query(Company).filter(Company.last_scraped.is_(None)).count()
            + s.query(ScrapeQueue).filter_by(processed=False).count()
        )
        nb_traites = s.query(Company).filter(Company.last_scraped.isnot(None)).count()
        nb_en_cours = s.query(ScrapeMetadata).filter(
            ScrapeMetadata.scraped_at >= datetime.utcnow().replace(hour=0, minute=0, second=0)
        ).count()
        nb_decouvertes = repo.count_discoveries()
        cutoff_24h = datetime.utcnow() - timedelta(hours=24)
        nb_erreurs_scraping = (
            s.query(ScrapeError)
            .filter(ScrapeError.error_type == "scraping", ScrapeError.created_at >= cutoff_24h)
            .count()
        )
        nb_erreurs_parsing = (
            s.query(ScrapeError)
            .filter(ScrapeError.error_type == "parsing", ScrapeError.created_at >= cutoff_24h)
            .count()
        )
        nb_erreurs_validation = (
            s.query(ScrapeError)
            .filter(ScrapeError.error_type == "validation", ScrapeError.created_at >= cutoff_24h)
            .count()
        )
        nb_echecs_proxy = (
            s.query(ScrapeError)
            .filter(ScrapeError.error_type == "proxy", ScrapeError.created_at >= cutoff_24h)
            .count()
        )

    snap = repo.save_monitoring_snapshot({
        "nb_en_cours": nb_en_cours,
        "nb_traites": nb_traites,
        "nb_attente": nb_attente,
        "nb_decouvertes": nb_decouvertes,
        "nb_erreurs_scraping": nb_erreurs_scraping,
        "nb_erreurs_parsing": nb_erreurs_parsing,
        "nb_erreurs_validation": nb_erreurs_validation,
        "nb_echecs_proxy": nb_echecs_proxy,
    })
    logger.info("Snapshot monitoring #%s enregistré", snap.id)
    return snap.id
