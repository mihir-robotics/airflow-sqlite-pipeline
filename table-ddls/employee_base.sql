CREATE TABLE employee_base (
    employee_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    department_name TEXT NOT NULL,
    base_pay REAL NOT NULL,
    last_updt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);