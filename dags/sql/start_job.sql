INSERT INTO job_log (job_id, start_ts, end_ts)
SELECT
MAX(job_id) + 1,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP
FROM job_log;