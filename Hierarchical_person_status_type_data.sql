INSERT INTO Person_status_type (person_status_type_code, data)
SELECT person_status_type_code,
       jsonb_build_object('name', name, 'description', description)
FROM traditional.person_status_type;
