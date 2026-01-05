-- Create metrics table if it doesn't exist
CREATE TABLE IF NOT EXISTS ${METRICS_TABLE} (
    id INTEGER PRIMARY KEY,
    collected_at TIMESTAMP DEFAULT current_timestamp,
    metric_name VARCHAR,
    metric_value DOUBLE
);

-- Create sequence for IDs if not exists
CREATE SEQUENCE IF NOT EXISTS metrics_seq START 1;
