"""Pagination lots BCE (partagé import KBO / scraping, sans dépendance Airflow obligatoire)."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

KBO_BATCH_OFFSET_VAR = "kbo_bce_batch_offset"


def get_kbo_batch_limit() -> int:
    return max(0, int(os.getenv("KBO_SCRAPE_QUEUE_LIMIT", "500") or "500"))


def get_kbo_batch_offset() -> int:
    try:
        from airflow.models import Variable

        return max(0, int(Variable.get(KBO_BATCH_OFFSET_VAR, "0") or "0"))
    except Exception:
        env_raw = os.getenv("KBO_BCE_BATCH_OFFSET")
        if env_raw is not None and str(env_raw).strip() != "":
            return max(0, int(env_raw))
        return 0


def set_kbo_batch_offset(offset: int) -> None:
    offset = max(0, int(offset))
    os.environ["KBO_BCE_BATCH_OFFSET"] = str(offset)
    try:
        from airflow.models import Variable

        Variable.set(KBO_BATCH_OFFSET_VAR, str(offset))
    except Exception as exc:
        logger.debug("Variable Airflow indisponible (%s)", exc)


def advance_kbo_batch_offset(step: int | None = None) -> int:
    if step is None:
        step = get_kbo_batch_limit()
    step = max(0, int(step))
    new_offset = get_kbo_batch_offset() + step
    set_kbo_batch_offset(new_offset)
    logger.info("Curseur lot KBO : +%d → offset %d", step, new_offset)
    return new_offset
