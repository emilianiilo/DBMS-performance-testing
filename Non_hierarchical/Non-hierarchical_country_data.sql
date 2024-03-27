INSERT INTO non_hierarchical.country (country_code, data)
SELECT 
    country_code,
    jsonb_build_object(
        'name', name
    )
FROM traditional.country;
