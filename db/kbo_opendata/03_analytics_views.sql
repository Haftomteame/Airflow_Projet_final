-- Vues analytiques basées sur KBO Open Data + table companies
\c belgian_companies;

DROP VIEW IF EXISTS v_analytics_temporal CASCADE;
DROP VIEW IF EXISTS v_analytics_open_closed_ratio CASCADE;
DROP VIEW IF EXISTS v_analytics_by_nace CASCADE;
DROP VIEW IF EXISTS v_analytics_by_postal_code CASCADE;
DROP VIEW IF EXISTS v_kbo_bce_queue CASCADE;

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

-- NACE : activité principale KBO (groupe 006 prioritaire) + libellé FR
CREATE OR REPLACE VIEW v_analytics_by_nace AS
WITH company_nace AS (
    SELECT DISTINCT ON (c.id)
        c.id,
        COALESCE(NULLIF(TRIM(act.nace_code), ''), NULLIF(TRIM(c.nace_code), '')) AS code_nace
    FROM companies c
    LEFT JOIN kbo_activity act
        ON REPLACE(act.entity_number, '.', '') = c.bce_number
       AND UPPER(TRIM(act.classification)) = 'MAIN'
    WHERE c.is_deleted = FALSE
    ORDER BY
        c.id,
        CASE act.activity_group WHEN '006' THEN 0 WHEN '001' THEN 1 ELSE 2 END,
        act.nace_version DESC NULLS LAST,
        act.nace_code
)
SELECT
    COALESCE(cn.code_nace, 'Non renseigné') AS code_nace,
    COALESCE(
        (
            SELECT k.description
            FROM kbo_code k
            WHERE k.code = cn.code_nace
              AND k.language = 'FR'
              AND k.category LIKE 'Nace%'
            ORDER BY k.category DESC
            LIMIT 1
        ),
        cn.code_nace,
        'Non renseigné'
    ) AS libelle,
    COUNT(*) AS total_entreprises
FROM company_nace cn
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_analytics_open_closed_ratio AS
SELECT
    CURRENT_DATE AS date,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'active') / NULLIF(COUNT(*), 0), 2)::TEXT || '%' AS taux_ouvertes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status IN ('closed', 'radiated', 'inactive')) / NULLIF(COUNT(*), 0), 2)::TEXT || '%' AS taux_fermees
FROM companies
WHERE is_deleted = FALSE;

-- Créations KBO : uniquement entreprises du jeu courant, dates >= 2000
CREATE OR REPLACE VIEW v_analytics_temporal AS
WITH parsed AS (
    SELECT
        e.status,
        CASE
            WHEN NULLIF(TRIM(e.start_date), '') ~ '^\d{2}-\d{2}-\d{4}$'
                THEN TO_DATE(NULLIF(TRIM(e.start_date), ''), 'DD-MM-YYYY')
            WHEN NULLIF(TRIM(e.start_date), '') ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TO_DATE(NULLIF(TRIM(e.start_date), ''), 'YYYY-MM-DD')
            ELSE NULL
        END AS kbo_start
    FROM kbo_enterprise e
    INNER JOIN companies c
        ON c.bce_number = REPLACE(e.enterprise_number, '.', '')
       AND c.is_deleted = FALSE
)
SELECT
    TO_CHAR(kbo_start, 'YYYY-MM') AS mois,
    COUNT(*) AS nouvelles_entreprises,
    COUNT(*) FILTER (WHERE status IN ('AF', 'ST')) AS fermees_mois
FROM parsed
WHERE kbo_start IS NOT NULL
  AND kbo_start >= DATE '1900-01-01'
  AND kbo_start <= CURRENT_DATE + INTERVAL '1 year'
GROUP BY TO_CHAR(kbo_start, 'YYYY-MM')
ORDER BY 1;
