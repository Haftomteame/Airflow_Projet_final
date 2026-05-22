"""Modèles SQLAlchemy pour la plateforme entreprises belges."""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bce_number = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(512))
    address = Column(Text)
    postal_code = Column(String(16))
    status = Column(String(32), default="active")  # active, inactive, closed, radiated
    legal_form = Column(String(128))
    nace_code = Column(String(32))
    created_at = Column(DateTime, default=datetime.utcnow)
    last_scraped = Column(DateTime)
    is_deleted = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)
    source = Column(String(32), default="csv")  # csv | discovered

    history = relationship("CompanyHistory", back_populates="company")
    scrape_metadata = relationship("ScrapeMetadata", back_populates="company")
    financials = relationship("CompanyFinancial", back_populates="company")
    publications = relationship("MoniteurPublication", back_populates="company")
    directors = relationship("CompanyDirector", back_populates="company")


class CompanyHistory(Base):
    __tablename__ = "company_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    snapshot = Column(JSONB, nullable=False)
    changed_at = Column(DateTime, default=datetime.utcnow, index=True)

    company = relationship("Company", back_populates="history")


class ScrapeMetadata(Base):
    __tablename__ = "scrape_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    source = Column(String(32), nullable=False)  # kbo | moniteur | bnb
    hdfs_path = Column(String(512))
    mongo_id = Column(String(32), index=True)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    http_code = Column(Integer)
    proxy_used = Column(String(128))
    attempts = Column(Integer, default=1)
    status = Column(String(32), default="success")
    parsed = Column(Boolean, default=False)

    company = relationship("Company", back_populates="scrape_metadata")

    __table_args__ = (
        Index("ix_scrape_metadata_unparsed", "parsed", "source"),
    )


class DiscoveryQueue(Base):
    __tablename__ = "discovery_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    discovered_bce = Column(String(20), nullable=False)
    reason = Column(String(256))
    discovered_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint("source_company_id", "discovered_bce", name="uq_discovery"),
    )


class MonitoringSnapshot(Base):
    __tablename__ = "monitoring_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    nb_en_cours = Column(Integer, default=0)
    nb_traites = Column(Integer, default=0)
    nb_attente = Column(Integer, default=0)
    nb_decouvertes = Column(Integer, default=0)
    nb_erreurs_scraping = Column(Integer, default=0)
    nb_erreurs_parsing = Column(Integer, default=0)
    nb_erreurs_validation = Column(Integer, default=0)
    nb_echecs_proxy = Column(Integer, default=0)


class ScrapeError(Base):
    __tablename__ = "scrape_errors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"))
    bce_number = Column(String(20))
    source = Column(String(32))
    error_type = Column(String(64))  # scraping | parsing | validation | proxy
    message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class CompanyDirector(Base):
    __tablename__ = "company_directors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    name = Column(String(256))
    role = Column(String(128))
    start_date = Column(String(32))

    company = relationship("Company", back_populates="directors")


class MoniteurPublication(Base):
    __tablename__ = "moniteur_publications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    title = Column(String(512))
    publication_date = Column(String(32))
    url = Column(String(1024))
    raw_excerpt = Column(Text)

    company = relationship("Company", back_populates="publications")


class CompanyFinancial(Base):
    __tablename__ = "company_financials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    fiscal_year = Column(String(16))
    total_assets = Column(String(64))
    equity = Column(String(64))
    turnover = Column(String(64))
    employees = Column(String(32))

    company = relationship("Company", back_populates="financials")


class AnalyticsByPostalCode(Base):
    __tablename__ = "analytics_by_postal_code"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code_postal = Column(String(16), index=True)
    total = Column(Integer, default=0)
    actives = Column(Integer, default=0)
    fermees = Column(Integer, default=0)
    computed_at = Column(DateTime, default=datetime.utcnow)


class AnalyticsByNace(Base):
    __tablename__ = "analytics_by_nace"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code_nace = Column(String(32), index=True)
    libelle = Column(String(256))
    total_entreprises = Column(Integer, default=0)
    computed_at = Column(DateTime, default=datetime.utcnow)


class AnalyticsFinancialRanking(Base):
    __tablename__ = "analytics_financial_ranking"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"))
    bce_number = Column(String(20))
    total_actif = Column(String(64))
    rang = Column(Integer)
    computed_at = Column(DateTime, default=datetime.utcnow)


class AnalyticsOpenClosedRatio(Base):
    __tablename__ = "analytics_open_closed_ratio"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, default=datetime.utcnow)
    taux_ouvertes = Column(String(16))
    taux_fermees = Column(String(16))
    computed_at = Column(DateTime, default=datetime.utcnow)


class AnalyticsTemporal(Base):
    __tablename__ = "analytics_temporal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    mois = Column(String(16), index=True)
    nouvelles_entreprises = Column(Integer, default=0)
    fermees_mois = Column(Integer, default=0)
    computed_at = Column(DateTime, default=datetime.utcnow)


class ScrapeQueue(Base):
    """File interne pour le rescraping et les nouvelles découvertes."""

    __tablename__ = "scrape_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bce_number = Column(String(20), nullable=False, index=True)
    priority = Column(Integer, default=0)
    reason = Column(String(128))
    queued_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)
