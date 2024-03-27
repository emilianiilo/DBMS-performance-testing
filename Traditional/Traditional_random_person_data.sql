CREATE OR REPLACE FUNCTION traditional.generate_random_email(name VARCHAR, domain VARCHAR)
RETURNS VARCHAR AS $$
DECLARE
    random_suffix VARCHAR := md5(random()::text);
BEGIN
    RETURN lower(name || '_' || random_suffix || '@' || domain);
END;
$$ LANGUAGE plpgsql;


INSERT INTO traditional.person (nat_id_code, country_code, person_status_type_code, e_mail, birth_date, given_name, surname, address, tel_nr)
SELECT
    md5(random()::text),
    c.country_code,
    floor(random() * (SELECT MAX(person_status_type_code) FROM traditional.person_status_type) + 1),
    traditional.generate_random_email(traditional.generate_random_name(5), 'example.com'),
    TO_DATE('19' || LPAD(TRUNC(random() * 100)::TEXT, 2, '0') || '-' || LPAD(TRUNC(random() * 12 + 1)::TEXT, 2, '0') || '-' || LPAD(TRUNC(random() * 28 + 1)::TEXT, 2, '0'), 'YY-MM-DD'), 
    traditional.generate_random_name(6),
    traditional.generate_random_name(8),
    'Random Address ' || (id + 10000)::TEXT,
    '+1 555 555 ' || LPAD(TRUNC(random() * 10000)::TEXT, 4, '0')
FROM
    generate_series(95001, 100000) AS id
CROSS JOIN
    traditional.country AS c;
