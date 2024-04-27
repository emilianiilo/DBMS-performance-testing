SELECT
   _id,
	nat_id_code,
    country_code,
    person_status_type_code,
    e_mail,
    jsonb_build_object(
        '$date', TO_CHAR(birth_date, 'YYYY-MM-DD"T"HH24:MI:SS.MSZ')
    ) AS birth_date,
    jsonb_build_object(
        '$date', TO_CHAR(reg_time, 'YYYY-MM-DD"T"HH24:MI:SS.MSZ')
    ) AS reg_time,
    given_name,
    surname,
    address,
    tel_nr,
    CASE
        WHEN e.employee_status_type_code IS NOT NULL THEN
            jsonb_build_object(
                'mentor_id', e.mentor_id,
                'employee_status_type_code', e.employee_status_type_code,
                'employment', jsonb_agg(
                    jsonb_build_object(
                        'end_time', jsonb_build_object(
                            '$date', TO_CHAR(em.end_time, 'YYYY-MM-DD"T"HH24:MI:SS.MSZ')
                        ),
                        'start_time', jsonb_build_object(
                            '$date', TO_CHAR(em.start_time, 'YYYY-MM-DD"T"HH24:MI:SS.MSZ')
                        ),
                        'occupation_code', em.occupation_code
                    )
                )
            )
        ELSE
            NULL
    END AS employee
FROM
    traditional.person p
LEFT JOIN
    traditional.employee e ON p._id = e.person_id
LEFT JOIN
    traditional.employment em ON e.person_id = em.person_id
GROUP BY
    _id, nat_id_code, country_code, person_status_type_code, e_mail, birth_date, reg_time, given_name, surname, address, tel_nr, e.employee_status_type_code, e.mentor_id;
