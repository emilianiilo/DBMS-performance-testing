INSERT INTO non_hierarchical.occupation (occupation_code, data)
SELECT 
    occupation_code,
    jsonb_build_object(
        'name', name,
        'description', description
    )
FROM traditional.occupation;
