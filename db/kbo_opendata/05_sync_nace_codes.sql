-- Copie le code NACE principal KBO vers companies.nace_code
\c belgian_companies;

UPDATE companies c
SET nace_code = sub.nace_code
FROM (
    SELECT DISTINCT ON (REPLACE(act.entity_number, '.', ''))
        REPLACE(act.entity_number, '.', '') AS bce_number,
        NULLIF(TRIM(act.nace_code), '') AS nace_code
    FROM kbo_activity act
    WHERE UPPER(TRIM(act.classification)) = 'MAIN'
      AND NULLIF(TRIM(act.nace_code), '') IS NOT NULL
    ORDER BY
        REPLACE(act.entity_number, '.', ''),
        CASE act.activity_group WHEN '006' THEN 0 WHEN '001' THEN 1 ELSE 2 END,
        act.nace_version DESC NULLS LAST,
        act.nace_code
) sub
WHERE c.bce_number = sub.bce_number
  AND (c.nace_code IS NULL OR TRIM(c.nace_code) = '');
