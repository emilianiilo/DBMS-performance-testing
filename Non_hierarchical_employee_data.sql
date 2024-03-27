INSERT INTO non_hierarchical.employee (person_id, mentor_id, employee_status_type_code)
SELECT 
    person_id,
    mentor_id,
    employee_status_type_code
FROM traditional.employee;
