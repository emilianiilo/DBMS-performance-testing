INSERT INTO traditional.employment (person_id, occupation_code, start_time, end_time)
SELECT 
    subquery.person_id,
    subquery.occupation_code,
    LEAST(subquery.start_time, subquery.end_time) AS start_time,
    GREATEST(subquery.start_time, subquery.end_time) AS end_time
FROM 
    (SELECT 
        e.person_id,
        o.occupation_code,
        CASE 
            WHEN NOW() - INTERVAL '1 year' * FLOOR(RANDOM() * 5) > NOW() THEN NOW()
            ELSE NOW() - INTERVAL '1 year' * FLOOR(RANDOM() * 5)
        END AS start_time,
        CASE 
            WHEN RANDOM() < 0.9 THEN NOW()
            ELSE NOW() - INTERVAL '1 month'
        END AS end_time
    FROM 
        traditional.employee e
    CROSS JOIN LATERAL (
        SELECT occupation_code
        FROM traditional.occupation
        ORDER BY RANDOM()
        LIMIT 5
    ) AS o) AS subquery;
