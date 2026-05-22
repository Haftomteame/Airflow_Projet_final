"""Tâches analytics (un rapport par DAG)."""

import logging

from analytics.reports import AnalyticsEngine

logger = logging.getLogger(__name__)


def analytics_clear_tables(**_context):
    engine = AnalyticsEngine()
    engine.repo.clear_analytics_tables()
    logger.info("Tables analytics vidées")
    return {"cleared": True}


def analytics_by_postal_code(**_context):
    engine = AnalyticsEngine()
    count = engine.generate_by_postal_code()
    logger.info("Rapport code postal: %d lignes", count)
    return count


def analytics_by_nace(**_context):
    engine = AnalyticsEngine()
    count = engine.generate_by_nace()
    logger.info("Rapport NACE: %d lignes", count)
    return count


def analytics_financial_ranking(**_context):
    engine = AnalyticsEngine()
    count = engine.generate_financial_ranking()
    logger.info("Rapport financier: %d lignes", count)
    return count


def analytics_open_closed_ratio(**_context):
    engine = AnalyticsEngine()
    count = engine.generate_open_closed_ratio()
    logger.info("Rapport ratio ouvert/fermé: %d lignes", count)
    return count


def analytics_temporal(**_context):
    engine = AnalyticsEngine()
    count = engine.generate_temporal()
    logger.info("Rapport temporel: %d lignes", count)
    return count
