-- Base applicative belgian_companies (Airflow utilise la DB airflow séparément)
CREATE DATABASE belgian_companies;

\c belgian_companies;

CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    bce_number VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(512),
    address TEXT,
    postal_code VARCHAR(16),
    status VARCHAR(32) DEFAULT 'active',
    legal_form VARCHAR(128),
    nace_code VARCHAR(32),
    created_at TIMESTAMP DEFAULT NOW(),
    last_scraped TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,
    is_archived BOOLEAN DEFAULT FALSE,
    source VARCHAR(32) DEFAULT 'csv'
);

CREATE INDEX IF NOT EXISTS ix_companies_bce ON companies(bce_number);
CREATE INDEX IF NOT EXISTS ix_companies_last_scraped ON companies(last_scraped);
CREATE INDEX IF NOT EXISTS ix_companies_status ON companies(status);

CREATE TABLE IF NOT EXISTS company_history (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    snapshot JSONB NOT NULL,
    changed_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_company_history_company ON company_history(company_id);

CREATE TABLE IF NOT EXISTS scrape_metadata (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    source VARCHAR(32) NOT NULL,
    hdfs_path VARCHAR(512),
    mongo_id VARCHAR(32),
    scraped_at TIMESTAMP DEFAULT NOW(),
    http_code INTEGER,
    proxy_used VARCHAR(128),
    attempts INTEGER DEFAULT 1,
    status VARCHAR(32) DEFAULT 'success',
    parsed BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ix_scrape_metadata_parsed ON scrape_metadata(parsed, source);
CREATE INDEX IF NOT EXISTS ix_scrape_metadata_mongo_id ON scrape_metadata(mongo_id);

CREATE TABLE IF NOT EXISTS discovery_queue (
    id SERIAL PRIMARY KEY,
    source_company_id INTEGER NOT NULL REFERENCES companies(id),
    discovered_bce VARCHAR(20) NOT NULL,
    reason VARCHAR(256),
    discovered_at TIMESTAMP DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    UNIQUE(source_company_id, discovered_bce)
);

CREATE TABLE IF NOT EXISTS monitoring_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    nb_en_cours INTEGER DEFAULT 0,
    nb_traites INTEGER DEFAULT 0,
    nb_attente INTEGER DEFAULT 0,
    nb_decouvertes INTEGER DEFAULT 0,
    nb_erreurs_scraping INTEGER DEFAULT 0,
    nb_erreurs_parsing INTEGER DEFAULT 0,
    nb_erreurs_validation INTEGER DEFAULT 0,
    nb_echecs_proxy INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scrape_errors (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    bce_number VARCHAR(20),
    source VARCHAR(32),
    error_type VARCHAR(64),
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS company_directors (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    name VARCHAR(256),
    role VARCHAR(128),
    start_date VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS moniteur_publications (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    title VARCHAR(512),
    publication_date VARCHAR(32),
    url VARCHAR(1024),
    raw_excerpt TEXT
);

CREATE TABLE IF NOT EXISTS company_financials (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    fiscal_year VARCHAR(16),
    total_assets VARCHAR(64),
    equity VARCHAR(64),
    turnover VARCHAR(64),
    employees VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS scrape_queue (
    id SERIAL PRIMARY KEY,
    bce_number VARCHAR(20) NOT NULL,
    priority INTEGER DEFAULT 0,
    reason VARCHAR(128),
    queued_at TIMESTAMP DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS analytics_by_postal_code (
    id SERIAL PRIMARY KEY,
    code_postal VARCHAR(16),
    total INTEGER DEFAULT 0,
    actives INTEGER DEFAULT 0,
    fermees INTEGER DEFAULT 0,
    computed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_by_nace (
    id SERIAL PRIMARY KEY,
    code_nace VARCHAR(32),
    libelle VARCHAR(256),
    total_entreprises INTEGER DEFAULT 0,
    computed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_financial_ranking (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    bce_number VARCHAR(20),
    total_actif VARCHAR(64),
    rang INTEGER,
    computed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_open_closed_ratio (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP DEFAULT NOW(),
    taux_ouvertes VARCHAR(16),
    taux_fermees VARCHAR(16),
    computed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_temporal (
    id SERIAL PRIMARY KEY,
    mois VARCHAR(16),
    nouvelles_entreprises INTEGER DEFAULT 0,
    fermees_mois INTEGER DEFAULT 0,
    computed_at TIMESTAMP DEFAULT NOW()
);
