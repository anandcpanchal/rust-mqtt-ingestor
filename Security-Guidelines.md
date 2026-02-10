# 🔒 Production Security Architecture

This document outlines the security measures required to harden the IoT MQTT Backend for production deployment.

## 1. Network Segmentation (Zero Trust)

In production, components should never be exposed to the public internet directly.

-   **Public Zone (DMZ):**
    -   **Load Balancer (Nginx/HAProxy)**: Validates TLS, terminates SSL, rate limits.
    -   **EMQX Broker (Port 8883 - MQTTS)**: Only accepts encrypted traffic.

-   **Private Zone (Backend Services):**
    -   **Redpanda (Kafka)**: Accessible ONLY by EMQX (Bridge) and Rust Backend.
    -   **Rust Backend**: Runs in a private container network.

-   **Data Zone (Persistence):**
    -   **TimescaleDB**: Accessible ONLY by Rust Backend (Port 5432). NO public access.

---

## 2. EMQX (MQTT Broker) Security

### A. Enable TLS/SSL (MQTTS)
Data in transit must be encrypted.

1.  **Generate Certificates:** Use Let's Encrypt (Public) or OpenSSL (Private CA).
2.  **Configure `emqx.conf`**:
    ```conf
    listeners.ssl.default {
      bind = "0.0.0.0:8883"
      ssl_options {
        keyfile = "/etc/emqx/certs/server.key"
        certfile = "/etc/emqx/certs/server.crt"
        cacertfile = "/etc/emqx/certs/ca.crt"
        verify = verify_peer # Force mTLS (optional but recommended for devices)
      }
    }
    ```

### B. Authentication
Disable anonymous access. Use a Database or JWT for auth.

1.  **Disable Anonymous:**
    ```conf
    allow_anonymous = false
    ```
2.  **Postgres Authentication Backend (Recommended):**
    Create an `mqtt_users` table in Postgres and configure EMQX to query it on connection.
    ```sql
    CREATE TABLE mqtt_users (
        username VARCHAR(100) PRIMARY KEY,
        password_hash VARCHAR(100), -- Argon2 or Bcrypt
        is_superuser BOOLEAN DEFAULT FALSE
    );
    ```

### C. Authorization (ACLs)
Restrict devices to their own topics.

-   **Rule:** Device `device_123` can ONLY publish to `.../devices/device_123/telemetry`.

### D. Detailed ACL Configuration
To achieve the restriction above, you can use **Placeholders** in your ACL rules.

#### Option 1: File-Based (`acl.conf`)
Use `%u` (username) and `%c` (clientid) to create dynamic rules.

```erlang
%% Allow any user to subscribe to their own config topic
{allow, all, subscribe, ["users/%u/config"]}.

%% Allow devices to publish ONLY to their specific telemetry topic
%% Assuming ClientID is the DeviceID
{allow, all, publish, ["users/%u/devices/%c/telemetry"]}.

%% Deny everything else by default
{deny, all, subscribe, ["#"]}.
{deny, all, publish, ["#"]}.
```

#### Option 2: Database-Based (Postgres)
If using the `mqtt_users` table, add an `mqtt_acl` table.

```sql
CREATE TABLE mqtt_acl (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100),
    permission VARCHAR(10), -- 'publish', 'subscribe', 'all'
    topic VARCHAR(255),
    action VARCHAR(10) DEFAULT 'allow'
);

-- Allow all users to subscribe to their config
INSERT INTO mqtt_acl (username, permission, topic) VALUES ('$all', 'subscribe', 'users/%u/config');

-- Allow devices to publish to their own telemetry
-- Note: You might map username=user_id and clientid=device_id structure
INSERT INTO mqtt_acl (username, permission, topic) VALUES ('$all', 'publish', 'users/%u/devices/%c/telemetry');

-- Allow Rust Backend (username='backend_service') to publish Alers
INSERT INTO mqtt_acl (username, permission, topic) VALUES ('backend_service', 'publish', 'users/+/alerts');

-- Allow Vector Bridge (username='vector_bridge') to subscribe to all telemetry
INSERT INTO mqtt_acl (username, permission, topic) VALUES ('vector_bridge', 'subscribe', 'users/+/devices/+/telemetry');

-- Allow Vector Bridge to subscribe to all Configs (for cache updates if needed)
INSERT INTO mqtt_acl (username, permission, topic) VALUES ('vector_bridge', 'subscribe', 'users/+/config');
```

---

## 3. Redpanda (Kafka) Security

By default, Redpanda is strictly internal, but if distributed across nodes:

### A. SASL/SCRAM Authentication
Require username/password for producers (EMQX) and consumers (Rust).

1.  **Enable SASL in `redpanda.yaml`:**
    ```yaml
    redpanda:
      kafka_api:
        - name: private
          address: 0.0.0.0
          port: 9092
          authentication_method: sasl
    ```
2.  **Create Users:**
    ```bash
    rpk acl user create rust-backend -p "strong_password"
    rpk acl user create emqx-bridge -p "bridge_password"
    ```

### B. Encryption (TLS)
Enable TLS for the Kafka API to prevent packet sniffing within the internal network.

---

## 4. Rust Backend Security

### A. Database Least Privilege
Do NOT use the `postgres` superuser.

1.  **Create Scoped User:**
    ```sql
    CREATE USER rust_app WITH PASSWORD 'secure_db_pass';
    GRANT CONNECT ON DATABASE iot_db TO rust_app;
    GRANT USAGE ON SCHEMA public TO rust_app;
    GRANT INSERT, SELECT ON raw_telemetry TO rust_app; -- No DELETE/DROP
    GRANT SELECT, UPDATE ON user_configs TO rust_app;
    ```

### B. Secrets Management
Never hardcode credentials. Use Environment Variables.

-   **Docker Secrets (Swarm/K8s):** Mount secrets as files in `/run/secrets/`.
-   **Bad Practice:** `DATABASE_URL=postgres://root:root...` in `docker-compose.yml`.
-   **Good Practice:** `DATABASE_URL_FILE=/run/secrets/db_url`.

### C. Input Validation
-   Trust no input from MQTT.
-   The Rust `serde` deserialization provides a strong first line of defense (type checking).
-   Add custom validation (e.g., `temperature` must be between -50 and 150) before processing.

---

## 5. TimescaleDB (Postgres) Security

### A. Network Isolation
Bind only to the private network interface.

```conf
# postgresql.conf
listen_addresses = 'private_ip_only'
```

### B. Encryption at Rest (Optional)
If storing sensitive data, ensure the underlying disk volume (EBS/Persistent Disk) is encrypted via the cloud provider (AWS KMS / Google Cloud KMS).

---

## 📘 Implementation Checklist

- [ ] **Step 1**: Generate TLS Certificates for EMQX.
- [ ] **Step 2**: Configure EMQX to enforce MQTTS.
- [ ] **Step 3**: Create `mqtt_users` table in Postgres and link EMQX Auth.
- [ ] **Step 4**: Create `rust_app` DB user with limited permissions.
- [ ] **Step 5**: Move all secrets to `.env` file (not committed to git).
- [ ] **Step 6**: Update `docker-compose.prod.yml` to expose ONLY ports 443/8883.
