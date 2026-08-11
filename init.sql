CREATE TABLE IF NOT EXISTS hard_drive_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    serial_number VARCHAR(255) NOT NULL,
    model VARCHAR(255) NOT NULL,
    capacity_bytes BIGINT NOT NULL,
    failure INT NOT NULL,
    smart_5_raw BIGINT,
    smart_5_normalized INT,
    smart_187_raw BIGINT,
    smart_187_normalized INT,
    smart_188_raw BIGINT,
    smart_188_normalized INT,
    smart_197_raw BIGINT,
    smart_197_normalized INT,
    smart_198_raw BIGINT,
    smart_198_normalized INT
);

CREATE INDEX IF NOT EXISTS idx_metrics_model_failures ON hard_drive_metrics (model, failure);

DROP MATERIALIZED VIEW IF EXISTS mv_model_failure_rates;

CREATE MATERIALIZED VIEW mv_model_failure_rates AS
SELECT
    model,
    ROUND(AVG(failure)::numeric, 6) AS average_failure_rate,
    COUNT(*) AS total_snapshots,
    SUM(failure) AS total_failures
FROM hard_drive_metrics
GROUP BY model
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_model_failure_rates_model ON mv_model_failure_rates (model);