INSERT INTO traditional.employee (person_id, employee_status_type_code)
SELECT 
    p._id as person_id,
    FLOOR(RANDOM() * (SELECT COUNT(*) FROM traditional.employee_status_type)) + 1 as employee_status_type_code
FROM
    traditional.person p
LIMIT 80000;


UPDATE traditional.employee AS e
SET mentor_id = subquery.sub_mentor_id
FROM (
    SELECT e.person_id AS emp_id,
           e2.person_id AS sub_mentor_id
    FROM traditional.employee AS e
    CROSS JOIN LATERAL (
        SELECT e2.person_id
        FROM traditional.employee AS e2
        WHERE e2.person_id <> e.person_id
        ORDER BY RANDOM()
        LIMIT 1
    ) AS e2
    WHERE e.mentor_id IS NULL
    LIMIT 100
) AS subquery
WHERE e.person_id = subquery.emp_id;


UPDATE traditional.employee AS e
SET mentor_id = subquery.sub_mentor_id
FROM (
    SELECT e.person_id AS emp_id,
           e2.person_id AS sub_mentor_id
    FROM traditional.employee AS e
    CROSS JOIN LATERAL (
        SELECT e2.person_id
        FROM traditional.employee AS e2
        WHERE e2.person_id <> e.person_id
        ORDER BY RANDOM()
        LIMIT 1
    ) AS e2
    WHERE e.mentor_id IS NULL
    LIMIT 40000
) AS subquery
WHERE e.person_id = subquery.emp_id;

