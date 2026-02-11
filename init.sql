-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- Enable pgcrypto for password hashing (optional but good practice)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

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
    rules           JSONB             NOT NULL DEFAULT '[]'
);

-- =============================================
-- SECURITY SCHEMA (EMQX Authentication & ACL)
-- =============================================

-- 1. MQTT Users (Authentication)
CREATE TABLE IF NOT EXISTS mqtt_users (
    username        VARCHAR(100)      PRIMARY KEY,
    password_hash   VARCHAR(100),     -- Argon2 or Bcrypt hash
    salt            VARCHAR(40),      -- Optional: Salt for hashing
    is_superuser    BOOLEAN           DEFAULT FALSE,
    created_at      TIMESTAMPTZ       DEFAULT NOW()
);

-- 2. MQTT ACL (Authorization)
CREATE TABLE IF NOT EXISTS mqtt_acl (
    id              SERIAL            PRIMARY KEY,
    username        VARCHAR(100)      NOT NULL,
    permission      VARCHAR(10)       NOT NULL, -- 'publish', 'subscribe', 'all'
    topic           VARCHAR(255)      NOT NULL,
    action          VARCHAR(10)       NOT NULL DEFAULT 'allow', -- 'allow', 'deny'
    created_at      TIMESTAMPTZ       DEFAULT NOW()
);

-- Index for fast ACL lookups
CREATE INDEX IF NOT EXISTS ix_mqtt_acl_username ON mqtt_acl (username);

-- Seed Default System Users (Placeholders - Change Passwords in Production!)
-- Note: Passwords below are NOT hashed. In production, use hashed values.
INSERT INTO mqtt_users (username, password_hash, is_superuser) VALUES ('backend_service', 'secure_password', FALSE);
-- INSERT INTO mqtt_users (username, password_hash, is_superuser) VALUES ('admin', 'admin_hash', TRUE);

-- Seed Default ACLs
-- Allow backend_service to subscribe to all telemetry
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('backend_service', 'subscribe', 'users/+/devices/+/telemetry', 'allow');

