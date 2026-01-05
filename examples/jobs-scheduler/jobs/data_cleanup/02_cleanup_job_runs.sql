-- Keep job run history for 30 days
DELETE FROM _mallardb_job_runs
WHERE finished_at < current_timestamp - INTERVAL '30 days';
