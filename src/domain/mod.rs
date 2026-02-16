use serde::{Deserialize, Serialize}; // Added Serialize for derived data publishing
use sqlx::types::time::OffsetDateTime;


#[derive(Debug, Clone, Deserialize, Serialize)] // Serialize needed for publishing derived types if any
pub struct Telemetry { // Domain model, decoupled from DB or Wire format if needed (though here assumed identical for simplicity)
    #[serde(with = "time::serde::iso8601")] // Standard ISO format for JSON
    pub time: OffsetDateTime,
    #[serde(skip_deserializing)] // Populated key-side, not from JSON
    pub user_id: String,
    pub device_id: String,
    pub sequence_id: i64,
    pub temperature: Option<f64>,
    pub battery: Option<i32>,
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl Telemetry {
    pub fn get_value(&self, key: &str) -> Option<f64> {
        match key {
            "temperature" => self.temperature,
            "battery" => self.battery.map(|v| v as f64),
            _ => self.extra.get(key).and_then(|v| v.as_f64()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub key: String,
    pub operator: String, // ">", "<", ">=", "<=", "=="
    pub threshold: f64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: Option<i32>, // Database ID
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    pub device_id: String,
    pub user_id: String, // Added user_id
    pub alert_type: String, // e.g., "CriticalTemp", "LowBattery"
    pub message: String,
    pub value: Option<f64>,
    pub rule_id: String, // New: Link to config rule
    pub status: String,  // New: Triggered, Resolved, Snoozed, Disabled
}

impl Alert {
    pub fn new(device_id: String, user_id: String, rule_id: String, alert_type: String, message: String, value: Option<f64>, status: String) -> Self {
        Self {
            id: None,
            created_at: OffsetDateTime::now_utc(),
            device_id,
            user_id,
            rule_id,
            alert_type,
            message,
            value,
            status,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlert {
    pub user_id: String,
    pub device_id: String,
    pub rule_id: String,
    #[serde(with = "time::serde::iso8601")]
    pub start_time: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub last_seen: OffsetDateTime,
    pub status: String, // Active, Snoozed, Disabled
    #[serde(with = "time::serde::iso8601::option")]
    pub snooze_until: Option<OffsetDateTime>,
    pub current_value: Option<f64>,
}
