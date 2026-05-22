-- Copie les codes postaux KBO Open Data vers companies.postal_code
\c belgian_companies;

UPDATE companies c
SET postal_code = sub.zipcode
FROM (
    SELECT DISTINCT ON (REPLACE(a.entity_number, '.', ''))
        REPLACE(a.entity_number, '.', '') AS bce_number,
        NULLIF(TRIM(a.zipcode), '') AS zipcode
    FROM kbo_address a
    WHERE a.type_of_address = 'REGO'
      AND NULLIF(TRIM(a.zipcode), '') IS NOT NULL
    ORDER BY REPLACE(a.entity_number, '.', ''), a.date_striking_off NULLS FIRST
) sub
WHERE c.bce_number = sub.bce_number
  AND (c.postal_code IS NULL OR TRIM(c.postal_code) = '');
