-- init.sql — runs once when the PostgreSQL container is first created.
-- Creates the application database, tables for predictions and ingestion stats,
-- and a separate database for Airflow metadata.

-- ── Application database ────────────────────────────────────────
\c dsp;

CREATE TABLE IF NOT EXISTS predictions (
    id                      SERIAL PRIMARY KEY,
    predicted_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    order_dow               SMALLINT NOT NULL,
    order_hour_of_day       SMALLINT NOT NULL,
    days_since_prior_order  NUMERIC(5, 2) NOT NULL,
    add_to_cart_order       SMALLINT NOT NULL,
    department_id           SMALLINT NOT NULL,
    aisle_id                SMALLINT NOT NULL,
    reordered               BOOLEAN NOT NULL,
    probability             NUMERIC(6, 4) NOT NULL,
    model_version           VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    source                  VARCHAR(20) NOT NULL DEFAULT 'webapp'
);

CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at ON predictions (predicted_at);
CREATE INDEX IF NOT EXISTS idx_predictions_source       ON predictions (source);

CREATE TABLE IF NOT EXISTS ingestion_stats (
    id                  SERIAL PRIMARY KEY,
    ingested_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    file_name           VARCHAR(255) NOT NULL,
    total_rows          INTEGER NOT NULL DEFAULT 0,
    valid_rows          INTEGER NOT NULL DEFAULT 0,
    invalid_rows        INTEGER NOT NULL DEFAULT 0,
    passed              BOOLEAN NOT NULL DEFAULT TRUE,
    failed_expectations TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_stats_ingested_at ON ingestion_stats (ingested_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_stats_file_name   ON ingestion_stats (file_name);

-- ── Airflow metadata database ───────────────────────────────────
-- The airflow database itself is created via POSTGRES_MULTIPLE_DATABASES
-- (handled by the Docker entrypoint script).  Airflow's own migrations
-- create its tables on first start, so no DDL is needed here.
