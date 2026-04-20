-- Creates the airflow metadata database
SELECT 'CREATE DATABASE airflow_db OWNER admin'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- Application tables (in prediction_db, the default POSTGRES_DB)
CREATE TABLE IF NOT EXISTS predictions (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    input_features  JSON NOT NULL,
    prediction_result INTEGER NOT NULL,
    probability     NUMERIC(6, 4) NOT NULL DEFAULT 0,
    source          VARCHAR(20) NOT NULL DEFAULT 'webapp',
    model_version   VARCHAR(20) NOT NULL DEFAULT 'v1.0',
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_created_at ON predictions (created_at);
CREATE INDEX IF NOT EXISTS idx_predictions_source     ON predictions (source);

CREATE TABLE IF NOT EXISTS ingestion_stats (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(100),
    file_name       TEXT,
    rows_total      INTEGER DEFAULT 0,
    rows_valid      INTEGER DEFAULT 0,
    rows_invalid    INTEGER DEFAULT 0,
    error_summary   JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_stats_ingested_at ON ingestion_stats (ingested_at);
