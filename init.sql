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
    value       DOUBLE PRECISION  NULL,
    user_id     TEXT              NOT NULL DEFAULT 'unknown'
);

-- Create index on alerts
CREATE INDEX IF NOT EXISTS ix_alerts_device ON alerts_history (device_id);


-- Create Active Alerts Table (Stateful)
CREATE TABLE IF NOT EXISTS active_alerts (
    user_id         TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    rule_id         TEXT NOT NULL, -- e.g., 'temperature_threshold'
    start_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL, -- 'Active', 'Snoozed', 'Disabled'
    snooze_until    TIMESTAMPTZ NULL,
    current_value   DOUBLE PRECISION NULL,
    PRIMARY KEY (user_id, device_id, rule_id)
);

-- Update Alerts History to track state transitions
ALTER TABLE alerts_history ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'Triggered';
ALTER TABLE alerts_history ADD COLUMN IF NOT EXISTS rule_id TEXT DEFAULT 'unknown';

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
-- 1. Allow backend_service to subscribe to all telemetry and publish/subscribe to management topics
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('backend_service', 'subscribe', 'users/+/devices/+/telemetry', 'allow');
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('backend_service', 'all', 'users/+/config', 'allow');
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('backend_service', 'all', 'users/+/alerts/manage', 'allow');

-- 2. Allow load_tester (superuser) to publish everything
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('load_tester', 'all', 'users/#', 'allow');

-- 3. Allow generic users to publish their own telemetry and subscribe to their own config
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('user_1', 'publish', 'users/user_1/devices/+/telemetry', 'allow');
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('user_1', 'subscribe', 'users/user_1/config', 'allow');
INSERT INTO mqtt_acl (username, permission, topic, action) VALUES ('user_1', 'subscribe', 'users/user_1/alerts/manage', 'allow');

