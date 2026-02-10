# IoT MQTT Backend POC 🚀

A high-performance, scalable, and reliable MQTT backend written in **Rust**.
Designed to ingest high-frequency telemetry data from thousands of IoT devices and process it via a durable **Kafka (Redpanda)** stream.

## 🏗️ Architecture

The system decouples **Ingestion** (MQTT) from **Processing** (Rust) using Redpanda as a durable buffer.

```mermaid
graph TD
    Devices["IoT Devices"] -->|MQTT Publish| EMQX["EMQX Broker"]
    EMQX -->|"Bridge (Vector)"| Redpanda[("Redpanda (Kafka)")]
    
    subgraph "Rust Backend Service"
        Redpanda -->|Sub: iot-stream| Consumer["Kafka Consumer"]
        Consumer -->|"Channel"| Executor["Batch Executor"]
        Consumer -->|Alerts| AlertLogic["Alert Logic"]
        AlertLogic -->|"Pub (MQTT)"| EMQX
    end
    
    Executor -->|"Batch Insert"| PgBouncer
    PgBouncer -->|"Connection Pool"| TimescaleDB[("TimescaleDB")]
```

### Key Components
1.  **Redpanda (Kafka)**:
    *   Acts as the persistent buffer/WAL (Write Ahead Log).
    *   **Topic**: `iot-stream`.
    *   Ensures that even if the backend is down, messages are safely stored on disk.
    *   Decouples the high-concurrency "Fan-In" of MQTT from the linear processing of the backend.

2.  **Vector (Bridge)**:
    *   Sidecar container running `timberio/vector`.
    *   Automatically subscribes to MQTT `users/+/devices/+/telemetry`.
    *   Forwards messages to Redpanda `iot-stream`.
    *   Handles Format Transformation (JSON Parser).

3.  **Rust Backend**:
    *   **Consumer**: Reads from Redpanda using `rdkafka`.
    *   **Batch Executor**: Buffers up to 1,000 messages or 100ms before Bulk Insert.
    *   **Alerting**: Checks thresholds and publishes alerts back to EMQX.

4.  **Infrastructure**:
    *   **EMQX 5.0**: Enterprise-grade MQTT Broker.
    *   **TimescaleDB**: Time-series optimized PostgreSQL.
    *   **Redpanda Console**: UI for monitoring topics (Port `8080`).

---

## 📊 Schema & Data Model

### `raw_telemetry` Table (TimescaleDB Hypertable)

This table stores all incoming high-frequency data. It is partitioned by `time` for efficient time-series queries.

```sql
CREATE TABLE raw_telemetry (
    time        TIMESTAMPTZ       NOT NULL, -- Partition Key
    user_id     TEXT              NOT NULL, 
    device_id   TEXT              NOT NULL,
    sequence_id BIGINT            NOT NULL,
    temperature DOUBLE PRECISION  NULL,
    battery     INTEGER           NULL,
    extra_data  JSONB             NULL,     -- Stores any extra fields
    
    PRIMARY KEY (time, user_id, device_id, sequence_id)
);
```

---

## 🛡️ Reliability & Zero Data Loss
1.  **Durable Buffer**: Redpanda stores all incoming telemetry on disk before processing.
2.  **Backpressure**: The Rust backend consumes at its own pace (Pull Model) rather than being overwhelmed by Pushes.
3.  **Graceful Shutdown**: Finish processing current batch before exit.

---

## 🚀 Scalability Findings
*   **Decoupling**: By moving ingestion to Redpanda, the MQTT broker can scale independently of the Backend.
*   **Replay**: We can re-process historical data by resetting the Kafka consumer group offset.

---

## 🛠️ Setup & Running

### Prerequisites
*   Docker & Docker Compose
*   Rust Toolchain (`cargo`)

### 1. Start Infrastructure
```bash
docker-compose up -d
```
*   **Wait** for `redpanda-init` to finish creating the topic (check `docker-compose logs redpanda-init`).
*   **Check UI**: http://localhost:8080 (Redpanda Console).

### 2. Run Backend
```bash
cargo run --bin poc-mqtt-backend
```

### 3. Run Load Tester
```bash
cargo run --bin load-tester -- --users 30 --devices-per-user 3 --rate 200
```


---

## 🔧 How to Adapt for Your Own Data Schema

To use this project as a base for a different data model (e.g., Logistics, Finance, Healthcare), follow these 3 steps:

### 1. Modify Database Schema (`init.sql`)
Update the SQL table definition to match your new data fields.
```sql
-- Example: Changing from "Telemetry" to "LogisticsPackage"
CREATE TABLE raw_packages (
    ...
    package_id TEXT NOT NULL,
    location   GEOMETRY(POINT, 4326),
    status     TEXT,
    ...
);
```

### 2. Update Domain Model (`src/domain/mod.rs`)
Modify the Rust struct to align with your new data.
```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PackageData {
    // ... matching fields ...
    pub package_id: String,
    pub location: String, 
    // ...
}
```

### 3. Update Persistence Layer (`src/adapters/timescale.rs`)
Rewrite the SQL `INSERT` statement in the `store_telemetry` and `store_telemetry_batch` functions to target your new table and columns.
```rust

let query = r#"
    INSERT INTO raw_packages (time, package_id, location, status)
    VALUES ($1, $2, $3, $4)
"#;
```

---

### Example 2: Financial Market Data

If you are building a **Stock/Crypto Ticker** processing engine:

1.  **SQL Schema** (`init.sql`):
    ```sql
    CREATE TABLE raw_ticks (
        time      TIMESTAMPTZ        NOT NULL,
        symbol    TEXT               NOT NULL, -- e.g. "BTC-USD"
        price     DOUBLE PRECISION   NOT NULL,
        volume    DOUBLE PRECISION   NOT NULL,
        PRIMARY KEY (time, symbol)
    );
    ```

2.  **Rust Struct** (`src/domain/mod.rs`):
    ```rust
    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct MarketTick {
        #[serde(with = "time::serde::iso8601")]
        pub time: OffsetDateTime,
        pub symbol: String,
        pub price: f64,
        pub volume: f64,
    }
    ```

3.  **Persistence** (`src/adapters/timescale.rs`):
    ```rust
    let query = r#"
        INSERT INTO raw_ticks (time, symbol, price, volume)
        VALUES ($1, $2, $3, $4)
    "#;
    ```
