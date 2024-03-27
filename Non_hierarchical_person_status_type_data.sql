INSERT INTO non_hierarchical.person_status_type (person_status_type_code, data)
SELECT 
    person_status_type_code,
    jsonb_build_object(
        'name', name,
        'description', description
    ) AS data
FROM traditional.person_status_type;
