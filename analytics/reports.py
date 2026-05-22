"""Génération des rapports analytiques périodiques (SQL KBO si disponible)."""

import logging
from datetime import datetime

from sqlalchemy import text

from db.models import (
    AnalyticsByNace,
    AnalyticsByPostalCode,
    AnalyticsFinancialRanking,
    AnalyticsOpenClosedRatio,
    AnalyticsTemporal,
    Company,
    CompanyFinancial,
)
from db.repository import Repository

logger = logging.getLogger(__name__)


class AnalyticsEngine:
    KBO_VIEWS = {
        "postal": "v_analytics_by_postal_code",
        "nace": "v_analytics_by_nace",
        "ratio": "v_analytics_open_closed_ratio",
        "temporal": "v_analytics_temporal",
    }

    def __init__(self, repo: Repository | None = None):
        self.repo = repo or Repository()

    def _view_exists(self, view_name: str) -> bool:
        try:
            with self.repo.engine.connect() as conn:
                r = conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.views "
                        "WHERE table_schema = 'public' AND table_name = :v"
                    ),
                    {"v": view_name},
                ).fetchone()
            return r is not None
        except Exception:
            return False

    def _load_from_kbo_view(self, view: str, model, column_map: dict) -> int | None:
        if not self._view_exists(view):
            return None
        with self.repo.engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {view}"))
            records = [dict(row._mapping) for row in result]
        if not records:
            return 0
        rows = []
        for row in records:
            item = {db_col: row[src] for db_col, src in column_map.items() if src in row}
            rows.append(item)
        self.repo.bulk_insert_analytics(model, rows)
        return len(rows)

    def run_all(self) -> dict[str, int]:
        self.repo.clear_analytics_tables()
        counts = {
            "postal": self.generate_by_postal_code(),
            "nace": self.generate_by_nace(),
            "financial": self.generate_financial_ranking(),
            "ratio": self.generate_open_closed_ratio(),
            "temporal": self.generate_temporal(),
        }
        logger.info("Analytics terminées: %s", counts)
        return counts

    def generate_by_postal_code(self) -> int:
        kbo = self._load_from_kbo_view(
            self.KBO_VIEWS["postal"],
            AnalyticsByPostalCode,
            {
                "code_postal": "code_postal",
                "total": "total",
                "actives": "actives",
                "fermees": "fermees",
            },
        )
        if kbo is not None:
            return kbo

        with self.repo.session() as s:
            companies = s.query(Company).all()
        agg: dict[str, dict] = {}
        for c in companies:
            if getattr(c, "is_deleted", False):
                continue
            pc = (c.postal_code or "").strip() or "non renseigné"
            if pc not in agg:
                agg[pc] = {"code_postal": pc, "total": 0, "actives": 0, "fermees": 0}
            agg[pc]["total"] += 1
            if c.status == "active":
                agg[pc]["actives"] += 1
            elif c.status in ("closed", "radiated", "inactive"):
                agg[pc]["fermees"] += 1
        data = list(agg.values())
        self.repo.bulk_insert_analytics(AnalyticsByPostalCode, data)
        return len(data)

    def generate_by_nace(self) -> int:
        kbo = self._load_from_kbo_view(
            self.KBO_VIEWS["nace"],
            AnalyticsByNace,
            {
                "code_nace": "code_nace",
                "libelle": "libelle",
                "total_entreprises": "total_entreprises",
            },
        )
        if kbo is not None:
            return kbo

        with self.repo.session() as s:
            companies = s.query(Company).filter(Company.nace_code.isnot(None)).all()
        agg: dict[str, dict] = {}
        for c in companies:
            code = c.nace_code or "unknown"
            if code not in agg:
                agg[code] = {"code_nace": code, "libelle": code, "total_entreprises": 0}
            agg[code]["total_entreprises"] += 1
        data = list(agg.values())
        self.repo.bulk_insert_analytics(AnalyticsByNace, data)
        return len(data)

    def generate_financial_ranking(self) -> int:
        with self.repo.session() as s:
            financials = (
                s.query(CompanyFinancial, Company)
                .join(Company, CompanyFinancial.company_id == Company.id)
                .all()
            )
        records = []
        for fin, company in financials:
            records.append({
                "company_id": company.id,
                "bce_number": company.bce_number,
                "total_actif": fin.total_assets,
                "rang": 0,
            })
        def _amount(val):
            try:
                return float(str(val or "0").replace(" ", "").replace(",", "."))
            except ValueError:
                return 0.0

        records.sort(key=lambda x: _amount(x.get("total_actif")), reverse=True)
        for i, rec in enumerate(records, start=1):
            rec["rang"] = i
        self.repo.bulk_insert_analytics(AnalyticsFinancialRanking, records)
        return len(records)

    def generate_open_closed_ratio(self) -> int:
        kbo = self._load_from_kbo_view(
            self.KBO_VIEWS["ratio"],
            AnalyticsOpenClosedRatio,
            {"date": "date", "taux_ouvertes": "taux_ouvertes", "taux_fermees": "taux_fermees"},
        )
        if kbo is not None:
            return kbo

        with self.repo.session() as s:
            total = s.query(Company).count()
            open_c = s.query(Company).filter(Company.status == "active").count()
            closed_c = s.query(Company).filter(Company.status.in_(["closed", "radiated", "inactive"])).count()
        if total == 0:
            return 0
        row = {
            "date": datetime.utcnow(),
            "taux_ouvertes": f"{round(100 * open_c / total, 2)}%",
            "taux_fermees": f"{round(100 * closed_c / total, 2)}%",
        }
        self.repo.bulk_insert_analytics(AnalyticsOpenClosedRatio, [row])
        return 1

    def generate_temporal(self) -> int:
        kbo = self._load_from_kbo_view(
            self.KBO_VIEWS["temporal"],
            AnalyticsTemporal,
            {
                "mois": "mois",
                "nouvelles_entreprises": "nouvelles_entreprises",
                "fermees_mois": "fermees_mois",
            },
        )
        if kbo is not None:
            return kbo

        with self.repo.session() as s:
            companies = s.query(Company).all()
        agg: dict[str, dict] = {}
        for c in companies:
            month = (c.created_at or datetime.utcnow()).strftime("%Y-%m")
            if month not in agg:
                agg[month] = {"mois": month, "nouvelles_entreprises": 0, "fermees_mois": 0}
            agg[month]["nouvelles_entreprises"] += 1
            if c.status in ("closed", "radiated"):
                agg[month]["fermees_mois"] += 1
        data = list(agg.values())
        self.repo.bulk_insert_analytics(AnalyticsTemporal, data)
        return len(data)
