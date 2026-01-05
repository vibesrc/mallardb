-- Collect a sample metric (simulated)
-- In a real scenario, this could query external sources via DuckDB extensions
INSERT INTO ${METRICS_TABLE} (id, metric_name, metric_value)
VALUES (
    nextval('metrics_seq'),
    'sample_metric',
    random() * 100
);
