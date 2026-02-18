use serde::Deserialize;
use std::env;
use anyhow::Context;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_client_id_prefix: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
    pub database_url: String,
    pub instance_id: String,
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group: String,
    pub otel_endpoint: String,
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok(); // Load .env file if it exists, ignore if not

        let config = AppConfig {
            mqtt_host: env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string()),
            mqtt_port: env::var("MQTT_PORT")
                .unwrap_or_else(|_| "1883".to_string())
                .parse()
                .context("MQTT_PORT must be a valid u16")?,
            mqtt_client_id_prefix: env::var("MQTT_CLIENT_ID_PREFIX")
                .unwrap_or_else(|_| "backend_processor".to_string()),
            mqtt_username: env::var("MQTT_USERNAME").unwrap_or_else(|_| "backend_service".to_string()),
            mqtt_password: env::var("MQTT_PASSWORD").unwrap_or_else(|_| "secure_password".to_string()),
            database_url: env::var("DATABASE_URL")
                .context("DATABASE_URL must be set")?,
// telemetry_topic removed as we use Kafka for ingest
            instance_id: env::var("INSTANCE_ID").unwrap_or_else(|_| "1".to_string()),
            kafka_brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string()),
            kafka_topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "iot-stream".to_string()),
            kafka_group: env::var("KAFKA_GROUP").unwrap_or_else(|_| "rust-backend-group".to_string()),
            otel_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| "http://tempo:4318".to_string()),
        };

        Ok(config)
    }
}
