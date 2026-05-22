"""Métriques de performance pour le scraping (durée, débit, taux de succès)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ScrapeBenchmark:
    """Chronomètre et agrégats pour un lot de scraping (une source)."""

    source: str
    companies_target: int = 0
    started_at_mono: float = field(default_factory=time.monotonic)
    finished_at_mono: float | None = None
    started_at_utc: str = field(default_factory=_utc_now_iso)

    companies_processed: int = 0
    pages_total: int = 0
    pages_valid: int = 0
    pages_invalid: int = 0
    fetch_duration_sec: float = 0.0
    storage_duration_sec: float = 0.0
    total_attempts: int = 0

    def record_company(self, results: list[dict[str, Any]], fetch_sec: float) -> None:
        """Enregistre les stats d'une entreprise après son scrape."""
        self.companies_processed += 1
        self.fetch_duration_sec += max(0.0, fetch_sec)
        for item in results:
            self.pages_total += 1
            if item.get("valid"):
                self.pages_valid += 1
            else:
                self.pages_invalid += 1
            self.total_attempts += int(item.get("attempts") or 0)

    def finish(self) -> None:
        self.finished_at_mono = time.monotonic()

    @property
    def duration_sec(self) -> float:
        end = self.finished_at_mono if self.finished_at_mono is not None else time.monotonic()
        return max(0.0, end - self.started_at_mono)

    @property
    def wall_duration_sec(self) -> float:
        """Durée totale incluant stockage (fetch + storage)."""
        return self.duration_sec

    def pages_per_sec(self) -> float:
        d = self.fetch_duration_sec or self.duration_sec
        if d <= 0 or self.pages_total <= 0:
            return 0.0
        return self.pages_total / d

    def companies_per_sec(self) -> float:
        d = self.fetch_duration_sec or self.duration_sec
        if d <= 0 or self.companies_processed <= 0:
            return 0.0
        return self.companies_processed / d

    def avg_sec_per_page(self) -> float:
        if self.pages_total <= 0:
            return 0.0
        return self.fetch_duration_sec / self.pages_total

    def avg_sec_per_company(self) -> float:
        if self.companies_processed <= 0:
            return 0.0
        return self.fetch_duration_sec / self.companies_processed

    def success_rate_pct(self) -> float:
        if self.pages_total <= 0:
            return 0.0
        return 100.0 * self.pages_valid / self.pages_total

    def to_dict(self, *, stored: int | None = None) -> dict[str, Any]:
        """Résumé sérialisable (logs, XCom, Variable Airflow)."""
        out: dict[str, Any] = {
            "source": self.source,
            "started_at": self.started_at_utc,
            "finished_at": _utc_now_iso(),
            "companies_target": self.companies_target,
            "companies_processed": self.companies_processed,
            "pages_total": self.pages_total,
            "pages_valid": self.pages_valid,
            "pages_invalid": self.pages_invalid,
            "total_attempts": self.total_attempts,
            "fetch_duration_sec": round(self.fetch_duration_sec, 3),
            "storage_duration_sec": round(self.storage_duration_sec, 3),
            "duration_sec": round(self.duration_sec, 3),
            "wall_duration_sec": round(self.wall_duration_sec, 3),
            "pages_per_sec": round(self.pages_per_sec(), 4),
            "companies_per_sec": round(self.companies_per_sec(), 4),
            "avg_sec_per_page": round(self.avg_sec_per_page(), 3),
            "avg_sec_per_company": round(self.avg_sec_per_company(), 3),
            "success_rate_pct": round(self.success_rate_pct(), 2),
        }
        if stored is not None:
            out["stored"] = stored
        return out

    def format_summary(self, *, stored: int | None = None) -> str:
        """Ligne de log lisible (français)."""
        parts = [
            f"source={self.source}",
            f"durée={self.duration_sec:.1f}s",
            f"fetch={self.fetch_duration_sec:.1f}s",
            f"entreprises={self.companies_processed}/{self.companies_target}",
            f"pages={self.pages_valid}/{self.pages_total} valides",
            f"vitesse={self.pages_per_sec():.3f} pages/s",
            f"{self.companies_per_sec():.3f} entreprises/s",
            f"moy={self.avg_sec_per_page():.2f}s/page",
        ]
        if self.storage_duration_sec > 0:
            parts.append(f"stockage={self.storage_duration_sec:.1f}s")
        if stored is not None:
            parts.append(f"stockées={stored}")
        return "Benchmark scraping — " + ", ".join(parts)


def merge_pipeline_benchmarks(sources: list[dict[str, Any]]) -> dict[str, Any]:
    """Agrège les benchmarks de chaque source (KBO + Moniteur + BNB)."""
    if not sources:
        return {
            "sources": [],
            "sources_count": 0,
            "total_duration_sec": 0.0,
            "total_fetch_sec": 0.0,
            "total_storage_sec": 0.0,
            "companies_processed": 0,
            "pages_total": 0,
            "pages_valid": 0,
            "stored": 0,
            "pages_per_sec": 0.0,
            "companies_per_sec": 0.0,
        }

    total_fetch = sum(float(s.get("fetch_duration_sec") or 0) for s in sources)
    total_storage = sum(float(s.get("storage_duration_sec") or 0) for s in sources)
    total_wall = sum(float(s.get("duration_sec") or 0) for s in sources)
    companies = sum(int(s.get("companies_processed") or 0) for s in sources)
    pages = sum(int(s.get("pages_total") or 0) for s in sources)
    pages_valid = sum(int(s.get("pages_valid") or 0) for s in sources)
    stored = sum(int(s.get("stored") or 0) for s in sources)

    return {
        "sources": [s.get("source") for s in sources],
        "sources_count": len(sources),
        "pipeline_finished_at": _utc_now_iso(),
        "total_duration_sec": round(total_wall, 3),
        "total_fetch_sec": round(total_fetch, 3),
        "total_storage_sec": round(total_storage, 3),
        "companies_processed": companies,
        "pages_total": pages,
        "pages_valid": pages_valid,
        "stored": stored,
        "pages_per_sec": round(pages / total_fetch, 4) if total_fetch > 0 and pages > 0 else 0.0,
        "companies_per_sec": round(companies / total_fetch, 4)
        if total_fetch > 0 and companies > 0
        else 0.0,
        "success_rate_pct": round(100.0 * pages_valid / pages, 2) if pages > 0 else 0.0,
        "per_source": sources,
    }


def format_pipeline_summary(merged: dict[str, Any]) -> str:
    return (
        "Benchmark pipeline scraping — "
        f"sources={merged.get('sources_count', 0)} "
        f"({', '.join(merged.get('sources') or [])}), "
        f"durée_totale={merged.get('total_duration_sec', 0):.1f}s "
        f"(fetch={merged.get('total_fetch_sec', 0):.1f}s, "
        f"stockage={merged.get('total_storage_sec', 0):.1f}s), "
        f"entreprises={merged.get('companies_processed', 0)}, "
        f"pages={merged.get('pages_valid', 0)}/{merged.get('pages_total', 0)}, "
        f"vitesse={merged.get('pages_per_sec', 0):.3f} pages/s, "
        f"{merged.get('companies_per_sec', 0):.3f} entreprises/s, "
        f"stockées={merged.get('stored', 0)}"
    )
