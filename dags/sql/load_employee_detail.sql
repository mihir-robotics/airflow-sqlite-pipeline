-- Upsert from stg to core table
INSERT INTO employee_detail (employee_id, name, role, dept_id)
SELECT e.employee_id, e.name, e.role, e.dept_id 
FROM employee_detail_stg e
JOIN employee_detail_stg f on e.employee_id = f.employee_id 
ON CONFLICT(employee_id) 
DO UPDATE SET 
    name = excluded.name,
    role = excluded.role,
    dept_id = excluded.dept_id;

