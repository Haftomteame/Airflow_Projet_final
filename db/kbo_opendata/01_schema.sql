-- Schéma PostgreSQL aligné sur KboOpenData_0335_2026_04_19_Full (fichiers CSV officiels)
\c belgian_companies;

CREATE TABLE IF NOT EXISTS kbo_meta (
    variable VARCHAR(64) PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS kbo_enterprise (
    enterprise_number VARCHAR(20) PRIMARY KEY,
    status VARCHAR(8),
    juridical_situation VARCHAR(8),
    type_of_enterprise VARCHAR(8),
    juridical_form VARCHAR(16),
    juridical_form_cac VARCHAR(16),
    start_date VARCHAR(16)
);

CREATE TABLE IF NOT EXISTS kbo_denomination (
    entity_number VARCHAR(20),
    language VARCHAR(4),
    type_of_denomination VARCHAR(8),
    denomination TEXT,
    PRIMARY KEY (entity_number, language, type_of_denomination)
);

CREATE TABLE IF NOT EXISTS kbo_address (
    entity_number VARCHAR(20),
    type_of_address VARCHAR(16),
    country_nl VARCHAR(64),
    country_fr VARCHAR(64),
    zipcode VARCHAR(16),
    municipality_nl VARCHAR(128),
    municipality_fr VARCHAR(128),
    street_nl VARCHAR(256),
    street_fr VARCHAR(256),
    house_number VARCHAR(32),
    box VARCHAR(32),
    extra_address_info TEXT,
    date_striking_off VARCHAR(16)
);

CREATE INDEX IF NOT EXISTS ix_kbo_address_entity ON kbo_address(entity_number);
CREATE INDEX IF NOT EXISTS ix_kbo_address_zip ON kbo_address(zipcode);

CREATE TABLE IF NOT EXISTS kbo_activity (
    entity_number VARCHAR(20),
    activity_group VARCHAR(8),
    nace_version VARCHAR(8),
    nace_code VARCHAR(16),
    classification VARCHAR(16),
    PRIMARY KEY (entity_number, activity_group, nace_version, nace_code, classification)
);

CREATE INDEX IF NOT EXISTS ix_kbo_activity_nace ON kbo_activity(nace_code);

CREATE TABLE IF NOT EXISTS kbo_contact (
    entity_number VARCHAR(20),
    entity_contact VARCHAR(16),
    contact_type VARCHAR(16),
    value TEXT
);

CREATE TABLE IF NOT EXISTS kbo_establishment (
    establishment_number VARCHAR(20) PRIMARY KEY,
    start_date VARCHAR(16),
    enterprise_number VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS ix_kbo_establishment_ent ON kbo_establishment(enterprise_number);

CREATE TABLE IF NOT EXISTS kbo_branch (
    branch_id VARCHAR(20) PRIMARY KEY,
    start_date VARCHAR(16),
    enterprise_number VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS kbo_code (
    category VARCHAR(64),
    code VARCHAR(32),
    language VARCHAR(4),
    description TEXT,
    PRIMARY KEY (category, code, language)
);
