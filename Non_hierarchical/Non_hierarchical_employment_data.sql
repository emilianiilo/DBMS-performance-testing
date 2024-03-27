INSERT INTO non_hierarchical.employment (person_id, occupation_code, data)
SELECT 
    person_id,
    occupation_code,
    jsonb_build_object(
        'start_time', start_time,
        'end_time', end_time
    ) AS data
FROM traditional.employment;