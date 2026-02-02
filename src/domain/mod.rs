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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: Option<i32>, // Database ID
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    pub device_id: String,
    pub alert_type: String, // e.g., "CriticalTemp", "LowBattery"
    pub message: String,
    pub value: Option<f64>,
}

impl Alert {
    pub fn new(device_id: String, alert_type: String, message: String, value: Option<f64>) -> Self {
        Self {
            id: None,
            created_at: OffsetDateTime::now_utc(),
            device_id,
            alert_type,
            message,
            value,
        }
    }
}
