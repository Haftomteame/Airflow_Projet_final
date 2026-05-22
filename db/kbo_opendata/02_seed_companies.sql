-- Alimente la table applicative companies depuis les tables KBO Open Data
\c belgian_companies;

INSERT INTO companies (bce_number, name, address, postal_code, status, legal_form, nace_code, source)
SELECT
    REPLACE(e.enterprise_number, '.', '') AS bce_number,
    COALESCE(d.denomination, 'Entreprise ' || REPLACE(e.enterprise_number, '.', '')) AS name,
    TRIM(BOTH ' ' FROM CONCAT_WS(' ',
        NULLIF(a.street_fr, ''),
        NULLIF(a.house_number, ''),
        NULLIF(a.box, ''),
        NULLIF(a.municipality_fr, '')
    )) AS address,
    NULLIF(a.zipcode, '') AS postal_code,
    CASE e.status
        WHEN 'AC' THEN 'active'
        WHEN 'ST' THEN 'inactive'
        WHEN 'AF' THEN 'closed'
        ELSE 'unknown'
    END AS status,
    NULLIF(e.juridical_form, '') AS legal_form,
    NULLIF(act.nace_code, '') AS nace_code,
    'kbo_opendata' AS source
FROM kbo_enterprise e
LEFT JOIN LATERAL (
    SELECT denomination
    FROM kbo_denomination d
    WHERE d.entity_number = e.enterprise_number
      AND d.type_of_denomination = '001'
    ORDER BY CASE d.language WHEN '2' THEN 1 WHEN '1' THEN 2 ELSE 3 END
    LIMIT 1
) d ON TRUE
LEFT JOIN LATERAL (
    SELECT zipcode, street_fr, house_number, box, municipality_fr
    FROM kbo_address a
    WHERE a.entity_number = e.enterprise_number
      AND a.type_of_address = 'REGO'
    LIMIT 1
) a ON TRUE
LEFT JOIN LATERAL (
    SELECT nace_code
    FROM kbo_activity act
    WHERE act.entity_number = e.enterprise_number
      AND act.classification = 'MAIN'
    LIMIT 1
) act ON TRUE
ON CONFLICT (bce_number) DO UPDATE SET
    name = EXCLUDED.name,
    address = EXCLUDED.address,
    postal_code = EXCLUDED.postal_code,
    status = EXCLUDED.status,
    legal_form = EXCLUDED.legal_form,
    nace_code = EXCLUDED.nace_code,
    source = EXCLUDED.source;
