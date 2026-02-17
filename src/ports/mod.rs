use async_trait::async_trait;
use crate::domain::{Telemetry, Alert};

#[async_trait]
pub trait StorageRepository: Send + Sync {
    /// Persist raw telemetry to the database.
    /// Returns successfully if stored OR if it's a guaranteed duplicate (deduplication).
    /// Errors only on persistent DB failures.
    async fn store_telemetry(&self, data: &Telemetry) -> anyhow::Result<()>;

    /// Store an alert in the history table.
    async fn store_alert(&self, alert: &Alert) -> anyhow::Result<()>;

    /// Persist a batch of telemetry data.
    async fn store_telemetry_batch(&self, batch: &[Telemetry]) -> anyhow::Result<()>;

    /// Upsert an active alert. Returns an Alert event if state changed to Active (for publishing).
    async fn upsert_active_alert(&self, alert: &crate::domain::ActiveAlert, message: &str) -> anyhow::Result<Option<Alert>>;

    /// Resolve an active alert. Returns an Alert event if it was unresolved (for publishing).
    async fn resolve_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str) -> anyhow::Result<Option<Alert>>;

    /// Snooze an active alert.
    async fn snooze_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str, duration: i64) -> anyhow::Result<()>;

    /// Disable an active alert.
    async fn disable_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str) -> anyhow::Result<()>;
}

#[async_trait]
pub trait MessageBroker: Send + Sync {
    /// Publish data to a specific topic.
    /// QoS 1 implied.
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> anyhow::Result<()>;
    
// ...
    // Note: Subscribe is typically handled by the event loop stream, 
    // but we could abstract it here if we wanted a push-based model. 
    // For this POC, the loop controls the subscriber stream directly via rumqttc, 
    // so we mainly abstract the "Output" side here to decouple processing from the specific client.
}

#[async_trait]
pub trait DlqRepository: Send + Sync {
    /// Send a failed message to the Dead Letter Queue.
    /// Includes metadata about why it failed.
    async fn send_to_dlq(&self, original_topic: String, payload: Vec<u8>, error_msg: String) -> anyhow::Result<()>;
}
