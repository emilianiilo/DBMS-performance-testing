INSERT INTO hierarchical.employee_status_type (employee_status_type_code, data)
SELECT 
    employee_status_type_code,
    jsonb_build_object(
        'name', name,
        'description', description
    )
FROM traditional.employee_status_type;
