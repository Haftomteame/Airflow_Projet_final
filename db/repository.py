"""Fonctions CRUD PostgreSQL pour la plateforme."""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, func, or_, text
from sqlalchemy.orm import sessionmaker

from db.models import (
    AnalyticsByNace,
    AnalyticsByPostalCode,
    AnalyticsFinancialRanking,
    AnalyticsOpenClosedRatio,
    AnalyticsTemporal,
    Base,
    Company,
    CompanyDirector,
    CompanyFinancial,
    CompanyHistory,
    DiscoveryQueue,
    MoniteurPublication,
    MonitoringSnapshot,
    ScrapeError,
    ScrapeMetadata,
    ScrapeQueue,
)

logger = logging.getLogger(__name__)


def get_engine():
    url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    return create_engine(url, pool_pre_ping=True)


class Repository:
    def __init__(self, engine=None):
        self.engine = engine or get_engine()
        Base.metadata.create_all(self.engine)
        self._ensure_mongo_id_column()
        self.Session = sessionmaker(bind=self.engine)

    def _ensure_mongo_id_column(self) -> None:
        """Migration légère pour bases PostgreSQL déjà créées."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE scrape_metadata "
                        "ADD COLUMN IF NOT EXISTS mongo_id VARCHAR(32)"
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_scrape_metadata_mongo_id "
                        "ON scrape_metadata (mongo_id)"
                    )
                )
        except Exception as exc:
            logger.debug("Colonne mongo_id déjà présente ou indisponible: %s", exc)

    def session(self):
        return self.Session()

    # --- Companies ---

    def upsert_company(self, data: dict[str, Any]) -> Company:
        with self.session() as s:
            company = s.query(Company).filter_by(bce_number=data["bce_number"]).first()
            if not company:
                company = Company(
                    bce_number=data["bce_number"],
                    source=data.get("source", "csv"),
                )
                s.add(company)
            old_snapshot = self._company_snapshot(company)
            for key in (
                "name", "address", "postal_code", "status", "legal_form",
                "nace_code", "last_scraped", "source",
            ):
                if key in data and data[key] is not None:
                    setattr(company, key, data[key])
            new_snapshot = self._company_snapshot(company)
            if old_snapshot != new_snapshot and company.id:
                s.flush()
                self._add_history(s, company.id, new_snapshot)
            s.commit()
            s.refresh(company)
            return company

    def get_company_by_bce(self, bce_number: str) -> Company | None:
        with self.session() as s:
            return s.query(Company).filter_by(bce_number=bce_number).first()

    def get_all_bce_numbers(self) -> list[str]:
        with self.session() as s:
            return [r[0] for r in s.query(Company.bce_number).all()]

    def filter_fresh_companies(self, bce_list: list[str], days: int = 14) -> list[str]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.session() as s:
            stale = {
                r[0]
                for r in s.query(Company.bce_number).filter(
                    or_(Company.last_scraped.is_(None), Company.last_scraped < cutoff),
                    Company.bce_number.in_(bce_list),
                    Company.is_deleted.is_(False),
                ).all()
            }
        return [b for b in bce_list if b in stale]

    def find_stale_companies(self, days: int = 14) -> list[Company]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.session() as s:
            return (
                s.query(Company)
                .filter(
                    or_(Company.last_scraped.is_(None), Company.last_scraped < cutoff),
                    Company.is_deleted.is_(False),
                    Company.is_archived.is_(False),
                )
                .all()
            )

    def find_inactive_status_changes(self) -> list[Company]:
        with self.session() as s:
            return (
                s.query(Company)
                .filter(Company.status.in_(["inactive", "closed", "radiated"]))
                .filter(Company.is_archived.is_(False))
                .all()
            )

    def archive_company(self, company_id: int) -> None:
        with self.session() as s:
            company = s.get(Company, company_id)
            if company:
                company.is_archived = True
                company.status = "closed"
                self._add_history(s, company_id, self._company_snapshot(company))
                s.commit()

    def enqueue_scrape(self, bce_number: str, reason: str = "rescrape", priority: int = 1) -> None:
        with self.session() as s:
            exists = (
                s.query(ScrapeQueue)
                .filter_by(bce_number=bce_number, processed=False)
                .first()
            )
            if not exists:
                s.add(ScrapeQueue(bce_number=bce_number, reason=reason, priority=priority))
                s.commit()

    def dequeue_scrape_batch(self, limit: int = 100) -> list[str]:
        with self.session() as s:
            q = (
                s.query(ScrapeQueue)
                .filter_by(processed=False)
                .order_by(ScrapeQueue.priority.desc(), ScrapeQueue.queued_at)
            )
            if limit > 0:
                q = q.limit(limit)
            rows = q.all()
            bces = [r.bce_number for r in rows]
            for r in rows:
                r.processed = True
            s.commit()
            return bces

    # --- Scrape metadata ---

    def add_scrape_metadata(self, data: dict[str, Any]) -> ScrapeMetadata:
        with self.session() as s:
            meta = ScrapeMetadata(**data)
            s.add(meta)
            s.commit()
            s.refresh(meta)
            return meta

    def list_unparsed_hdfs(self, source: str | None = None) -> list[ScrapeMetadata]:
        with self.session() as s:
            q = s.query(ScrapeMetadata).filter_by(parsed=False, status="success")
            if source:
                q = q.filter_by(source=source)
            return q.all()

    def mark_parsed(self, metadata_id: int) -> None:
        with self.session() as s:
            meta = s.get(ScrapeMetadata, metadata_id)
            if meta:
                meta.parsed = True
                s.commit()

    def set_scrape_mongo_id(self, metadata_id: int, mongo_id: str) -> None:
        with self.session() as s:
            meta = s.get(ScrapeMetadata, metadata_id)
            if meta:
                meta.mongo_id = mongo_id
                s.commit()

    # --- Discovery ---

    def add_discovery(self, source_company_id: int, discovered_bce: str, reason: str) -> bool:
        with self.session() as s:
            existing = s.query(Company).filter_by(bce_number=discovered_bce).first()
            if existing:
                return False
            try:
                s.add(
                    DiscoveryQueue(
                        source_company_id=source_company_id,
                        discovered_bce=discovered_bce,
                        reason=reason,
                        processed=True,
                    )
                )
                s.add(
                    Company(bce_number=discovered_bce, source="discovered", status="pending")
                )
                s.add(
                    ScrapeQueue(bce_number=discovered_bce, reason=f"discovery:{reason}", priority=2)
                )
                s.commit()
                return True
            except Exception:
                s.rollback()
                return False

    def process_pending_discoveries(self, limit: int = 200) -> int:
        """Remet en file les découvertes non traitées (repli si échec précédent)."""
        with self.session() as s:
            rows = (
                s.query(DiscoveryQueue)
                .filter_by(processed=False)
                .order_by(DiscoveryQueue.discovered_at)
                .limit(limit)
                .all()
            )
            count = 0
            for row in rows:
                if not s.query(Company).filter_by(bce_number=row.discovered_bce).first():
                    s.add(
                        Company(
                            bce_number=row.discovered_bce,
                            source="discovered",
                            status="pending",
                        )
                    )
                exists = (
                    s.query(ScrapeQueue)
                    .filter_by(bce_number=row.discovered_bce, processed=False)
                    .first()
                )
                if not exists:
                    s.add(
                        ScrapeQueue(
                            bce_number=row.discovered_bce,
                            reason=f"discovery:{row.reason}",
                            priority=2,
                        )
                    )
                row.processed = True
                count += 1
            s.commit()
            return count

    def sync_company_status_from_kbo(self) -> int:
        """Aligne companies.status sur kbo_enterprise (AC/ST/AF)."""
        from sqlalchemy import text

        sql = text("""
            UPDATE companies c
            SET status = CASE k.status
                WHEN 'AC' THEN 'active'
                WHEN 'ST' THEN 'inactive'
                WHEN 'AF' THEN 'closed'
                ELSE c.status
            END
            FROM kbo_enterprise k
            WHERE REPLACE(k.enterprise_number, '.', '') = c.bce_number
              AND c.is_deleted = FALSE
              AND c.is_archived = FALSE
              AND (
                c.status IS DISTINCT FROM CASE k.status
                    WHEN 'AC' THEN 'active'
                    WHEN 'ST' THEN 'inactive'
                    WHEN 'AF' THEN 'closed'
                    ELSE c.status
                END
              )
        """)
        try:
            with self.engine.begin() as conn:
                result = conn.execute(sql)
                return result.rowcount or 0
        except Exception as exc:
            logger.warning("Sync statuts KBO ignorée: %s", exc)
            return 0

    def count_discoveries(self) -> int:
        with self.session() as s:
            return s.query(DiscoveryQueue).count()

    def get_discovery_stats(self) -> dict[str, int]:
        with self.session() as s:
            total = s.query(DiscoveryQueue).count()
            pending = s.query(DiscoveryQueue).filter_by(processed=False).count()
            return {"total": total, "pending": pending, "processed": total - pending}

    def get_recent_history(self, limit: int = 50) -> list[dict]:
        with self.session() as s:
            rows = (
                s.query(CompanyHistory, Company)
                .join(Company, CompanyHistory.company_id == Company.id)
                .order_by(CompanyHistory.changed_at.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "bce_number": company.bce_number,
                    "name": company.name,
                    "changed_at": hist.changed_at,
                    "snapshot": hist.snapshot,
                }
                for hist, company in rows
            ]

    # --- Directors / publications / financials ---

    def replace_directors(self, company_id: int, directors: list[dict]) -> None:
        with self.session() as s:
            s.query(CompanyDirector).filter_by(company_id=company_id).delete()
            for d in directors:
                name = (d.get("name") or "").strip()
                role = (d.get("role") or "").strip()
                if not name or not role or name.endswith(":"):
                    continue
                s.add(
                    CompanyDirector(
                        company_id=company_id,
                        name=name[:256],
                        role=role[:128],
                        start_date=(d.get("start_date") or "")[:32] or None,
                    )
                )
            s.commit()

    def replace_publications(self, company_id: int, pubs: list[dict]) -> None:
        with self.session() as s:
            s.query(MoniteurPublication).filter_by(company_id=company_id).delete()
            for p in pubs:
                s.add(MoniteurPublication(company_id=company_id, **p))
            s.commit()

    def replace_financials(self, company_id: int, financials: list[dict]) -> None:
        with self.session() as s:
            s.query(CompanyFinancial).filter_by(company_id=company_id).delete()
            for f in financials:
                s.add(CompanyFinancial(company_id=company_id, **f))
            s.commit()

    # --- Errors & monitoring ---

    def log_error(
        self,
        error_type: str,
        message: str,
        company_id: int | None = None,
        bce_number: str | None = None,
        source: str | None = None,
    ) -> None:
        with self.session() as s:
            s.add(
                ScrapeError(
                    company_id=company_id,
                    bce_number=bce_number,
                    source=source,
                    error_type=error_type,
                    message=message,
                )
            )
            s.commit()

    def save_monitoring_snapshot(self, metrics: dict[str, int]) -> MonitoringSnapshot:
        with self.session() as s:
            snap = MonitoringSnapshot(**metrics)
            s.add(snap)
            s.commit()
            s.refresh(snap)
            return snap

    def get_monitoring_stats(self) -> dict[str, Any]:
        with self.session() as s:
            total = s.query(Company).count()
            pending = s.query(Company).filter(Company.last_scraped.is_(None)).count()
            errors = s.query(ScrapeError).filter(
                ScrapeError.created_at >= datetime.utcnow() - timedelta(hours=24)
            ).count()
            in_queue = s.query(ScrapeQueue).filter_by(processed=False).count()
            return {
                "total_companies": total,
                "pending": pending + in_queue,
                "errors_24h": errors,
            }

    def get_latest_snapshot(self) -> MonitoringSnapshot | None:
        with self.session() as s:
            return (
                s.query(MonitoringSnapshot)
                .order_by(MonitoringSnapshot.timestamp.desc())
                .first()
            )

    def get_recent_errors(self, limit: int = 50) -> list[ScrapeError]:
        with self.session() as s:
            return (
                s.query(ScrapeError)
                .order_by(ScrapeError.created_at.desc())
                .limit(limit)
                .all()
            )

    def get_status_distribution(self) -> dict[str, int]:
        with self.session() as s:
            rows = s.query(Company.status, func.count(Company.id)).group_by(Company.status).all()
            return {status or "unknown": count for status, count in rows}

    def get_scrape_timeline(self, days: int = 7) -> list[tuple]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.session() as s:
            return (
                s.query(
                    func.date(ScrapeMetadata.scraped_at),
                    func.count(ScrapeMetadata.id),
                )
                .filter(ScrapeMetadata.scraped_at >= cutoff)
                .group_by(func.date(ScrapeMetadata.scraped_at))
                .order_by(func.date(ScrapeMetadata.scraped_at))
                .all()
            )

    def get_source_health(self) -> dict[str, dict]:
        with self.session() as s:
            result = {}
            for source in ("kbo", "moniteur", "bnb"):
                total = s.query(ScrapeMetadata).filter_by(source=source).count()
                ok = (
                    s.query(ScrapeMetadata)
                    .filter_by(source=source, status="success", http_code=200)
                    .count()
                )
                result[source] = {"total": total, "success": ok, "healthy": ok >= total * 0.8 if total else True}
            return result

    # --- Analytics persistence ---

    def clear_analytics_tables(self) -> None:
        with self.session() as s:
            for model in (
                AnalyticsByPostalCode,
                AnalyticsByNace,
                AnalyticsFinancialRanking,
                AnalyticsOpenClosedRatio,
                AnalyticsTemporal,
            ):
                s.query(model).delete()
            s.commit()

    def bulk_insert_analytics(self, model, rows: list[dict]) -> None:
        with self.session() as s:
            s.bulk_insert_mappings(model, rows)
            s.commit()

    # --- Helpers ---

    @staticmethod
    def _company_snapshot(company: Company | None) -> dict:
        if not company:
            return {}
        return {
            "bce_number": company.bce_number,
            "name": company.name,
            "address": company.address,
            "postal_code": company.postal_code,
            "status": company.status,
            "legal_form": company.legal_form,
            "nace_code": company.nace_code,
            "is_archived": company.is_archived,
        }

    def _add_history(self, session, company_id: int, snapshot: dict) -> None:
        session.add(CompanyHistory(company_id=company_id, snapshot=snapshot))
