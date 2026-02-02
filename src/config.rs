use serde::Deserialize;
use std::env;
use anyhow::Context;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_client_id_prefix: String,
    pub database_url: String,
    pub telemetry_topic: String,
    pub instance_id: String,
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
            database_url: env::var("DATABASE_URL")
                .context("DATABASE_URL must be set")?,
            telemetry_topic: env::var("TELEMETRY_TOPIC")
                .unwrap_or_else(|_| "$share/backend_group/users/+/devices/+/telemetry".to_string()),
            instance_id: env::var("INSTANCE_ID").unwrap_or_else(|_| "1".to_string()),
        };

        Ok(config)
    }
}
