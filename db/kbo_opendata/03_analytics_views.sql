-- Vues analytiques basées sur KBO Open Data + table companies
\c belgian_companies;

CREATE OR REPLACE VIEW v_kbo_bce_queue AS
SELECT DISTINCT REPLACE(e.enterprise_number, '.', '') AS bce_number
FROM kbo_enterprise e
WHERE e.status = 'AC';

CREATE OR REPLACE VIEW v_analytics_by_postal_code AS
SELECT
    COALESCE(NULLIF(TRIM(c.postal_code), ''), 'non renseigné') AS code_postal,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE c.status = 'active') AS actives,
    COUNT(*) FILTER (WHERE c.status IN ('closed', 'radiated', 'inactive')) AS fermees
FROM companies c
WHERE c.is_deleted = FALSE
GROUP BY COALESCE(NULLIF(TRIM(c.postal_code), ''), 'non renseigné');

CREATE OR REPLACE VIEW v_analytics_by_nace AS
SELECT
    COALESCE(c.nace_code, 'unknown') AS code_nace,
    COALESCE(k.description, c.nace_code) AS libelle,
    COUNT(*) AS total_entreprises
FROM companies c
LEFT JOIN kbo_code k
    ON k.code = c.nace_code
   AND k.language = 'FR'
WHERE c.source = 'kbo_opendata'
GROUP BY COALESCE(c.nace_code, 'unknown'), COALESCE(k.description, c.nace_code);

CREATE OR REPLACE VIEW v_analytics_open_closed_ratio AS
SELECT
    CURRENT_DATE AS date,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'active') / NULLIF(COUNT(*), 0), 2)::TEXT || '%' AS taux_ouvertes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status IN ('closed', 'radiated', 'inactive')) / NULLIF(COUNT(*), 0), 2)::TEXT || '%' AS taux_fermees
FROM companies
WHERE source = 'kbo_opendata';

CREATE OR REPLACE VIEW v_analytics_temporal AS
SELECT
    TO_CHAR(
        TO_DATE(NULLIF(e.start_date, ''), 'DD-MM-YYYY'),
        'YYYY-MM'
    ) AS mois,
    COUNT(*) AS nouvelles_entreprises,
    COUNT(*) FILTER (WHERE e.status IN ('AF', 'ST')) AS fermees_mois
FROM kbo_enterprise e
WHERE NULLIF(e.start_date, '') IS NOT NULL
GROUP BY 1
HAVING TO_CHAR(TO_DATE(NULLIF(e.start_date, ''), 'DD-MM-YYYY'), 'YYYY-MM') IS NOT NULL;
