-- Delete metrics older than retention period
DELETE FROM ${METRICS_TABLE}
WHERE collected_at < current_timestamp - INTERVAL '${RETENTION_DAYS} days';
