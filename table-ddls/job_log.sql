CREATE TABLE job_log (
    job_id INTEGER PRIMARY KEY,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
	job_runtime TIMESTAMP 
);