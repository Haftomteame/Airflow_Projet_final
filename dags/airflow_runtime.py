"""Utilitaires runtime Airflow (heartbeat, tâches longues Celery)."""

from typing import Any


def airflow_heartbeat(context: dict[str, Any] | None) -> None:
    """Évite le zombie kill pendant les opérations longues (Celery / state mismatch)."""
    if not context:
        return
    ti = context.get("ti")
    if ti is None:
        return
    for method in ("heartbeat", "emit_heartbeat"):
        fn = getattr(ti, method, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception:
                pass
