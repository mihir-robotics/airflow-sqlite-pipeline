UPDATE job_log 
SET end_ts = CURRENT_TIMESTAMP,
job_runtime = (CAST(strftime('%s', CURRENT_TIMESTAMP) as integer) - CAST(strftime('%s', start_ts) as integer))
WHERE job_id = (SELECT MAX(job_id) FROM job_log)