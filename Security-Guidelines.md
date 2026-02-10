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

-   **Pattern:** `users/{user_id}/devices/{device_id}/...`
-   **Rule:** Device `device_123` can ONLY publish to `.../devices/device_123/telemetry`.

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
