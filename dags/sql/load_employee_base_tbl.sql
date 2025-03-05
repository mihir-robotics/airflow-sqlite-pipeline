-- Upsert
INSERT INTO Employee_Base (employee_id, name, role, department_name, base_pay, last_updt_ts)
SELECT 
    e.employee_id, 
    e.name, 
    e.role, 
    d.department_name, 
    d.base_pay, 
    CURRENT_TIMESTAMP
FROM employee_detail e
JOIN department_detail d ON e.dept_id = d.dept_id
ON CONFLICT(employee_id) DO UPDATE SET 
    name = excluded.name,
    role = excluded.role,
    department_name = excluded.department_name,
    base_pay = excluded.base_pay,
    last_updt_ts = CURRENT_TIMESTAMP;