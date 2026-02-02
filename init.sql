-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create Raw Telemetry Table
CREATE TABLE IF NOT EXISTS raw_telemetry (
    time        TIMESTAMPTZ       NOT NULL,
    user_id     TEXT              NOT NULL, -- NEW: Explicit User ID
    device_id   TEXT              NOT NULL,
    sequence_id BIGINT            NOT NULL,
    temperature DOUBLE PRECISION  NULL,
    battery     INTEGER           NULL,
    extra_data  JSONB             NULL, -- Stores any extra fields NOT defined above
    -- TimescaleDB requires the partitioning column (time) to be part of the Primary Key
    PRIMARY KEY (time, user_id, device_id, sequence_id)
);

-- Convert to Hypertable
-- chunk_time_interval set to 1 day for this POC example
SELECT create_hypertable('raw_telemetry', 'time', if_not_exists => TRUE);

-- Add indexes for common query patterns
CREATE INDEX IF NOT EXISTS ix_telemetry_user_device_time ON raw_telemetry (user_id, device_id, time DESC);

-- Create Alerts History Table (Standard Postgres Table)
CREATE TABLE IF NOT EXISTS alerts_history (
    id          SERIAL            PRIMARY KEY,
    created_at  TIMESTAMPTZ       DEFAULT NOW(),
    device_id   TEXT              NOT NULL,
    alert_type  TEXT              NOT NULL,
    message     TEXT              NOT NULL,
    value       DOUBLE PRECISION  NULL
);

-- Create index on alerts
CREATE INDEX IF NOT EXISTS ix_alerts_device ON alerts_history (device_id);

-- Create User Configs Table (New for Phase 5)
CREATE TABLE IF NOT EXISTS user_configs (
    user_id         TEXT              PRIMARY KEY,
    temperature_max DOUBLE PRECISION  NOT NULL
);
