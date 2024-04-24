--S1_1
SELECT _id, (data ->> 'e_mail') AS e_mail, (data ->> 'birth_date') AS birth_date
FROM non_hierarchical.person
WHERE (data ->> 'birth_date') BETWEEN '1940-01-01' AND '1980-12-31'
ORDER BY EXTRACT(YEAR FROM (data ->> 'birth_date')::date), (data ->> 'e_mail');

--S1_2
SELECT e.person_id, (o.data ->> 'name') AS occupation_name,
EXTRACT(YEAR FROM (e.data ->> 'start_time')::date) AS start_year,
EXTRACT(YEAR FROM (e.data ->> 'end_time')::date) AS end_year
FROM non_hierarchical.employment e
JOIN non_hierarchical.occupation o ON e.occupation_code = o.occupation_code
WHERE (e.data ->> 'start_time') BETWEEN '2020-01-01' AND '2022-12-31'
AND (e.data ->> 'end_time') IS NOT NULL
ORDER BY EXTRACT(YEAR FROM (e.data ->> 'start_time')::date), e.person_id;

--S2_1
SELECT _id, (data ->> 'surname') AS surname
FROM non_hierarchical.person
WHERE (data ->> 'e_mail') = 'acscu_ed69216fed359e36bd52421c86d40902@example.com';

--S2_2
SELECT (e.data ->> 'end_time') AS end_time
FROM non_hierarchical.employment e
JOIN non_hierarchical.person p ON e.person_id = p._id
WHERE (p.data ->> 'e_mail') = 'jnlpu_b07559ff04737ce21a10bb8a5438ac62@example.com' AND e.occupation_code = 42
AND (e.data ->> 'start_time') = '2023-03-11T20:32:51.062824';

--S3_1
SELECT c.country_code AS country_code,
 		(c.data ->> 'name') AS country_name,
       COUNT(_id) AS person_count,
       AVG(EXTRACT('year' FROM AGE((p.data ->> 'reg_time')::date, (p.data ->> 'birth_date')::date))) * 365 AS avg_age_days
FROM non_hierarchical.person p
RIGHT JOIN non_hierarchical.country c ON c.country_code = p.country_code
GROUP BY c.country_code;

--S3_2
SELECT o.occupation_code, (o.data ->> 'name') AS occupation_name, COUNT(e.person_id) AS employment_count,
AVG((e.data ->> 'end_time')::date - (e.data ->> 'start_time')::date) AS avg_duration_days
FROM non_hierarchical.occupation o
LEFT JOIN non_hierarchical.employment e ON o.occupation_code = e.occupation_code
AND (e.data ->> 'start_time') IS NOT NULL AND (e.data ->> 'end_time') IS NOT NULL
GROUP BY o.occupation_code, (o.data ->> 'name');

--S3_3
SELECT p._id, (p.data ->> 'e_mail') AS e_mail, COUNT(e.person_id) AS employment_count
FROM non_hierarchical.person p
LEFT JOIN non_hierarchical.employment e ON p._id = e.person_id
GROUP BY p._id, (p.data ->> 'e_mail');

--S4_1
SELECT (p.data ->> 'e_mail') AS e_mail, (p.data ->> 'surname') AS surname, (est.data ->> 'name') AS employee_status_type_name, e.data
FROM non_hierarchical.person p
JOIN non_hierarchical.employee emp ON p._id = emp.person_id
JOIN non_hierarchical.employee_status_type est ON emp.employee_status_type_code = est.employee_status_type_code
LEFT JOIN non_hierarchical.employment e ON emp.person_id = e.person_id;

--S4_2
SELECT (e.data ->> 'start_time') AS start_time, (e.data ->> 'end_time') AS end_time, (o.data ->> 'name') AS occupation_name, 
(p.data ->> 'e_mail') AS e_mail, (p.data ->> 'surname') AS surname
FROM non_hierarchical.employment e
JOIN non_hierarchical.occupation o ON e.occupation_code = o.occupation_code
JOIN non_hierarchical.person p ON e.person_id = p._id;


--S5_1
SELECT (p.data ->> 'e_mail') AS employee_email, (est.data ->> 'name') AS employee_status, 
(p_ment.data ->> 'e_mail') AS mentor_email, (mest.data ->> 'name') AS mentor_status
FROM non_hierarchical.employee emp
JOIN non_hierarchical.person p ON emp.person_id = p._id
JOIN non_hierarchical.employee_status_type est ON emp.employee_status_type_code = est.employee_status_type_code
LEFT JOIN non_hierarchical.employee ment ON emp.mentor_id = ment.person_id
LEFT JOIN non_hierarchical.person p_ment ON ment.person_id = p_ment._id
LEFT JOIN non_hierarchical.employee_status_type mest ON ment.employee_status_type_code = mest.employee_status_type_code
WHERE emp.employee_status_type_code <> ment.employee_status_type_code;

--S6_1
INSERT INTO non_hierarchical.person (_id, country_code, person_status_type_code, data)
VALUES (5, 'USA', 1,'{"nat_id_code": "1234567", "e_mail": "example@example.com", "birth_date": "1931-06-03", 
		"given_name": "eesnimi", "surname": "perekonnanimi", "address": "Random 123", "tel_nr": "+1 553344", 
		"reg_time":"2024-04-09"}');
INSERT INTO non_hierarchical.employee (person_id, mentor_id, employee_status_type_code)
VALUES (5, 4673567, 2);


--S6_2
INSERT INTO non_hierarchical.employment (person_id, occupation_code, data)
VALUES (5, 1, '{"start_time": "2024-01-01"}');

--S7_1
UPDATE non_hierarchical.person
SET data = jsonb_set(data, '{tel_nr}', '"+1 566666"')
WHERE data ->> 'e_mail' = 'example@example.com';

--S7_2
UPDATE non_hierarchical.employment
SET data = jsonb_set(data, '{end_time}', '"2024-04-20"')
WHERE person_id = (SELECT _id FROM non_hierarchical.person
WHERE data ->> 'e_mail' = 'example@example.com') AND occupation_code = 1
AND data ->> 'start_time' = '2024-01-01';

--S8_1
UPDATE non_hierarchical.person
SET data = jsonb_set(data, '{address}', 'null')
WHERE _id IN (SELECT person_id FROM non_hierarchical.employment
WHERE occupation_code BETWEEN 10 AND 30);


--S8_2
EXPLAIN ANALYZE
UPDATE non_hierarchical.employment
SET data = jsonb_set(data, '{end_time}', 'null')
WHERE person_id
IN (SELECT _id FROM non_hierarchical.person
	WHERE person_status_type_code IN (1, 2)
AND country_code = 'EST');

--S9_1
DELETE FROM non_hierarchical.person
WHERE data ->> 'e_mail' = 'example@example.com';

--S9_2
DELETE FROM non_hierarchical.employment
WHERE person_id = (SELECT _id FROM non_hierarchical.person
WHERE data ->> 'e_mail' = 'bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com') AND occupation_code = 27
AND data ->> 'start_time' = '2022-03-11T20:32:51.062824';


--S10_1
DELETE FROM non_hierarchical.person
WHERE _id IN (SELECT person_id FROM non_hierarchical.employment
WHERE occupation_code BETWEEN 10 AND 30);

--S10_2
DELETE FROM non_hierarchical.employment
WHERE person_id
IN (SELECT _id FROM non_hierarchical.person
	WHERE person_status_type_code IN (1, 2)
AND country_code = 'EST');
