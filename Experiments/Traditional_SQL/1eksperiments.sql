--S1_1
SELECT _id, e_mail, birth_date
FROM traditional.person
WHERE birth_date BETWEEN '1940-01-01' AND '1980-12-31'
ORDER BY EXTRACT(YEAR FROM birth_date), e_mail;

--S1_2
SELECT e.person_id, o.name AS occupation_name, 
EXTRACT(YEAR FROM e.start_time) AS start_year, 
EXTRACT(YEAR FROM e.end_time) AS end_year
FROM traditional.employment e
JOIN traditional.occupation o ON e.occupation_code = o.occupation_code
WHERE e.start_time BETWEEN '2020-01-01' AND '2022-12-31' 
ORDER BY EXTRACT(YEAR FROM e.start_time), e.person_id;

--S2_1
SELECT _id, surname
FROM traditional.person
WHERE e_mail = 'acscu_ed69216fed359e36bd52421c86d40902@example.com'; -- lower juurde kas siis indeksit kasutatakse

--S2_2
SELECT e.end_time
FROM traditional.employment e
JOIN traditional.person p ON e.person_id = p._id
WHERE p.e_mail = 'bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com' AND e.occupation_code = 4
AND e.start_time = '2023-05-06 14:33:56.363994';

--S3_1
SELECT c.country_code AS country_code,
 		c.name AS country_name,
       COUNT(p._id) AS person_count,
       AVG(EXTRACT('year' FROM AGE(p.reg_time, p.birth_date))) * 365 AS avg_age_days
FROM traditional.person p
RIGHT JOIN traditional.country c ON c.country_code = p.country_code
GROUP BY c.country_code;

--S3_2
SELECT o.occupation_code, o.name, COUNT(e.person_id) AS employment_count, 
AVG(EXTRACT(day FROM (e.end_time - e.start_time))) AS avg_duration_days
FROM traditional.occupation o
LEFT JOIN traditional.employment e ON o.occupation_code = e.occupation_code
AND e.start_time IS NOT NULL AND e.end_time IS NOT NULL
GROUP BY o.occupation_code, o.name;

--S3_3
SELECT p._id, p.e_mail, COUNT(e.person_id) AS employment_count
FROM traditional.person p
LEFT JOIN traditional.employment e ON p._id = e.person_id
GROUP BY p._id, p.e_mail;

--S4_1
SELECT p.e_mail, p.surname, est.name AS employee_status_type_name, e.*
FROM traditional.person p
JOIN traditional.employee emp ON p._id = emp.person_id
JOIN traditional.employee_status_type est ON emp.employee_status_type_code = est.employee_status_type_code
LEFT JOIN traditional.employment e ON emp.person_id = e.person_id;

--S4_2
SELECT e.start_time, e.end_time, o.name AS occupation_name, p.e_mail, p.surname
FROM traditional.employment e
JOIN traditional.occupation o ON e.occupation_code = o.occupation_code
JOIN traditional.person p ON e.person_id = p._id;

--S5_1
SELECT p.e_mail AS employee_email, est.name AS employee_status, p_ment.e_mail 
AS mentor_email, mest.name AS mentor_status
FROM traditional.employee emp
JOIN traditional.person p ON emp.person_id = p._id
JOIN traditional.employee_status_type est ON emp.employee_status_type_code = est.employee_status_type_code
LEFT JOIN traditional.employee ment ON emp.mentor_id = ment.person_id
LEFT JOIN traditional.person p_ment ON ment.person_id = p_ment._id
LEFT JOIN traditional.employee_status_type mest ON ment.employee_status_type_code = mest.employee_status_type_code
WHERE emp.employee_status_type_code <> ment.employee_status_type_code;

--S6_1 
INSERT INTO traditional.person (_id,nat_id_code, country_code, person_status_type_code, 
e_mail, birth_date, given_name, surname, address, tel_nr)
VALUES (5,1234567, 'USA', 3, 'example@example.com', 
'1931-06-03', 'eesnimi', 'perekonnanimi', 'Random 123', '+1 553344');
INSERT INTO traditional.employee (person_id, mentor_id, employee_status_type_code)
VALUES (5, 4673567, 2);

--S6_2 
INSERT INTO traditional.employment (person_id, occupation_code, start_time)
VALUES (5, 1, '2024-01-01');

--S7_1
UPDATE traditional.person
SET tel_nr = '+1 566666'
WHERE e_mail = 'example@example.com';

--S7_2
UPDATE traditional.employment
SET end_time = '2024-04-20'
WHERE person_id = (SELECT _id FROM traditional.person 
WHERE e_mail = 'example@example.com') AND occupation_code = 1 
AND start_time = '2024-01-01 00:00:00';

--S8_1
UPDATE traditional.person
SET address = NULL
WHERE _id IN (SELECT person_id FROM traditional.employment 
WHERE occupation_code BETWEEN 10 AND 30);

--S8_2
UPDATE traditional.employment
SET end_time = NULL
WHERE person_id 
IN (SELECT _id FROM traditional.person 
	WHERE person_status_type_code IN (1, 2) 
AND country_code = 'EST');

--S9_1
DELETE FROM traditional.person
WHERE e_mail = 'example@example.com';

--S9_2
DELETE FROM traditional.employment
WHERE person_id = (SELECT _id FROM traditional.person 
WHERE e_mail = 'bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com') AND occupation_code = 4 
AND start_time = '2023-05-06 14:33:56.363994';

--S10_1
DELETE FROM traditional.person
WHERE _id IN (SELECT person_id FROM traditional.employment 
WHERE occupation_code BETWEEN 10 AND 30);

--S10_2
DELETE FROM traditional.employment
WHERE person_id IN (SELECT _id FROM traditional.person 
WHERE person_status_type_code IN (1, 2) AND country_code = 'EST');
