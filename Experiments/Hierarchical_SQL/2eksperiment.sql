--S1_1
SELECT _id, (data ->> 'e_mail') AS e_mail, (data ->> 'birth_date') AS birth_date
FROM hierarchical.person
WHERE (data ->> 'birth_date') BETWEEN '1940-01-01' AND '1980-12-31'
ORDER BY EXTRACT(YEAR FROM (data ->> 'birth_date')::date), (data ->> 'e_mail');

--S1_2
SELECT 
    _id,
    (o.data ->> 'name') AS occupation_name,
    EXTRACT(YEAR FROM (employee.value ->> 'start_time')::timestamp) AS start_year,
    EXTRACT(YEAR FROM (employee.value ->> 'end_time')::timestamp) AS end_year
FROM 
    hierarchical.person p, jsonb_array_elements(p.employee -> 'employment') employee
JOIN 
    hierarchical.occupation o ON (employee.value ->> 'occupation_code')::int = o.occupation_code
WHERE 
    (employee.value ->> 'start_time')::date BETWEEN '2020-01-01' AND '2022-12-31'
ORDER BY 
    EXTRACT(YEAR FROM (employee.value ->> 'start_time')::timestamp), _id;

--S2_1
SELECT _id, (data ->> 'surname') AS surname
FROM hierarchical.person
WHERE (data ->> 'e_mail') = 'acscu_ed69216fed359e36bd52421c86d40902@example.com';

--S2_2
SELECT (employee.value ->> 'end_time') AS end_time
FROM hierarchical.person p, jsonb_array_elements(p.employee -> 'employment') employee
WHERE (p.data ->> 'e_mail') = 'jnlpu_b07559ff04737ce21a10bb8a5438ac62@example.com'
AND (employee.value ->> 'occupation_code')::int = 42
AND (employee.value ->> 'start_time') = '2023-03-11T20:32:51.062824';

--S3_1
SELECT c.country_code AS country_code,
 		(c.data ->> 'name') AS country_name,
       COUNT(p._id) AS person_count,
       AVG(EXTRACT('year' FROM AGE((p.data ->> 'reg_time')::date, (p.data ->> 'birth_date')::date))) * 365 AS avg_age_days
FROM hierarchical.person p
RIGHT JOIN hierarchical.country c ON c.country_code = p.country_code
GROUP BY c.country_code;

--S3_2
SELECT 
    o.occupation_code, 
    (o.data ->> 'name') AS occupation_name, 
    COUNT(p._id) AS employment_count,
    AVG((employee.value ->> 'end_time')::date - (employee.value ->> 'start_time')::date) AS avg_duration_days
FROM 
    hierarchical.person p, 
    jsonb_array_elements(p.employee -> 'employment') employee 
LEFT JOIN 
    hierarchical.occupation o ON o.occupation_code = (employee ->> 'occupation_code')::int
WHERE 
    (employee.value ->> 'start_time') IS NOT NULL AND (employee.value ->> 'end_time') IS NOT NULL
GROUP BY 
    o.occupation_code, (o.data ->> 'name');

--S3_3
SELECT p._id, (p.data ->> 'e_mail') AS e_mail, COUNT(employee.value ->> 'start_time') AS employment_count
FROM hierarchical.person p, jsonb_array_elements(p.employee -> 'employment') employee
GROUP BY p._id, (p.data ->> 'e_mail');

--S4_1
SELECT (p.data ->> 'e_mail') AS e_mail, (p.data ->> 'surname') AS surname, 
(est.data ->> 'name') AS employee_status_type_name, p.employee -> 'employment' AS employment
FROM hierarchical.person p
JOIN hierarchical.employee_status_type est ON (p.employee ->> 'employee_status_type_code')::int = est.employee_status_type_code;

--S4_2
SELECT (employee.value ->> 'start_time') AS start_time, (employee.value ->> 'end_time') AS end_time, 
(o.data ->> 'name') AS occupation_name, (p.data ->> 'e_mail') AS e_mail, (p.data ->> 'surname') AS surname
FROM hierarchical.person p, jsonb_array_elements(p.employee -> 'employment') employee
JOIN hierarchical.occupation o ON (employee ->> 'occupation_code')::int = o.occupation_code;

--S5_1
SELECT (p.data ->> 'e_mail') AS employee_email, (est.data ->> 'name') AS employee_status, (p_ment.data ->> 'e_mail')
AS mentor_email, (mest.data ->> 'name') AS mentor_status
FROM hierarchical.person p
JOIN hierarchical.employee_status_type est ON (p.employee ->> 'employee_status_type_code')::int = est.employee_status_type_code
LEFT JOIN hierarchical.person p_ment ON (p.employee ->> 'mentor_id')::int = p_ment._id
LEFT JOIN hierarchical.employee_status_type mest ON (p_ment.employee ->> 'employee_status_type_code')::int = mest.employee_status_type_code
WHERE (p.employee ->> 'employee_status_type_code')::int <> (p_ment.employee ->> 'employee_status_type_code')::int;

--S6_1
INSERT INTO hierarchical.person (country_code, person_status_type_code, data, employee)
VALUES ('USA', 1,'{"nat_id_code": "1234567", "e_mail": "example@example.com", "birth_date": "1931-06-03", 
		"given_name": "eesnimi", "surname": "perekonnanimi", "address": "Random 123", "tel_nr": "+1 553344", 
		"reg_time":"2024-04-09"}', '{"mentor_id": 4673567, "employee_status_type_code": 2}');

--6_2
UPDATE hierarchical.person p
SET employee = jsonb_set(employee::jsonb, '{employment}', '[{"start_time": "2024-01-01", "occupation_code": "1"}]'::jsonb)
WHERE p.data ->> 'given_name' = 'eesnimi';


--7_1
UPDATE hierarchical.person
SET data = jsonb_set(data, '{tel_nr}', '"+1 566666"')
WHERE (data ->> 'e_mail') = 'example@example.com';

--7_2
UPDATE hierarchical.person p
SET employee = jsonb_set(
    p.employee, 
    '{employment}', 
    (
        SELECT jsonb_agg(
            CASE 
                WHEN (employee.value ->> 'occupation_code')::int = 1 AND (employee.value ->> 'start_time') = '2024-01-01' THEN
                    jsonb_set(employee.value, '{end_time}', '"2024-04-20"')
                ELSE
                    employee.value
            END
        )
        FROM jsonb_array_elements(p.employee -> 'employment') employee
    )
)
WHERE (p.data ->> 'e_mail') = 'example@example.com';

--8_1
UPDATE hierarchical.person p
SET data = jsonb_set(data, '{address}', 'null')
WHERE EXISTS (
    SELECT 1
    FROM jsonb_array_elements(p.employee -> 'employment') employee
    WHERE (employee.value ->> 'occupation_code')::int BETWEEN 10 AND 30);

--8_2
UPDATE hierarchical.person p
SET employee = jsonb_set(
    p.employee, 
    '{employment}', 
    (
        SELECT jsonb_agg(
            jsonb_set(employee.value, '{end_time}', 'null')
        )
        FROM jsonb_array_elements(p.employee -> 'employment') employee
    )
)
WHERE person_status_type_code IN (1, 2) AND country_code = 'EST';

--9_1
DELETE FROM hierarchical.person
WHERE (data ->> 'e_mail') = 'example@example.com';

--9_2
WITH to_delete AS (
    SELECT p._id
    FROM hierarchical.person p, jsonb_array_elements(p.employee -> 'employment') employee 
    WHERE (employee.value ->> 'occupation_code')::int = 27 
    AND (employee.value ->> 'start_time') = '2022-03-11T20:32:51.062824'
    AND (p.data ->> 'e_mail') = 'bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com'
)
DELETE FROM hierarchical.person 
WHERE _id IN (SELECT _id FROM to_delete);

--10_1
DELETE FROM hierarchical.person p
WHERE EXISTS (
    SELECT 1
    FROM jsonb_array_elements(p.employee -> 'employment') employee
    WHERE (employee.value ->> 'occupation_code')::int BETWEEN 10 AND 30);


--10_2 (var1)
UPDATE hierarchical.person p
SET employee = jsonb_set(
    p.employee, 
    '{employment}', 
    (
        SELECT jsonb_agg(
            CASE 
                WHEN employee.value::text <> 'null' THEN 'null'
                ELSE employee.value
            END
        )
        FROM jsonb_array_elements(p.employee -> 'employment') employee
    )
)
WHERE p.person_status_type_code IN (1, 2) AND p.country_code = 'EST';

--10_2 (var2)
DELETE FROM hierarchical.person p
WHERE person_status_type_code IN (1, 2) 
AND country_code = 'EST';
