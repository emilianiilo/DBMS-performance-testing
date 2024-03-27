INSERT INTO non_hierarchical.person (_id, country_code, person_status_type_code, data)
SELECT 
    _id,
    country_code,
    person_status_type_code,
    jsonb_build_object(
        'nat_id_code', nat_id_code,
        'given_name', given_name,
        'surname', surname,
        'address', address,
        'tel_nr', tel_nr,
        'e_mail', e_mail,
        'birth_date', birth_date,
        'reg_time', reg_time
    )
FROM traditional.person;
