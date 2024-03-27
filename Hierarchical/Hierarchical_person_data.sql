INSERT INTO hierarchical.person (_id, country_code, person_status_type_code, data, employee)
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
    ) AS data,
    jsonb_build_object(
        'employee_status_type_code', e.employee_status_type_code,
        'mentor_id', e.mentor_id,
        'employment', jsonb_agg(
            jsonb_build_object(
                'occupation_code', em.occupation_code,
                'start_time', em.start_time,
                'end_time', em.end_time
            )
        )
    ) AS employee
FROM traditional.person p
LEFT JOIN traditional.employee e ON p._id = e.person_id
LEFT JOIN traditional.employment em ON e.person_id = em.person_id
GROUP BY p._id, country_code, person_status_type_code, nat_id_code, given_name, surname, address, tel_nr, e_mail, birth_date, reg_time, e.employee_status_type_code, e.mentor_id;
