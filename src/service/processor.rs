use crate::domain::{Telemetry, Alert};
use crate::ports::{StorageRepository, MessageBroker};
use crate::state::config_manager::{ConfigManager, UserConfig};
use std::sync::Arc;
use tracing::{info, error, instrument};
use rumqttc::{Event, Packet, AsyncClient};
use std::time::Duration;

pub struct ServiceProcessor {
    storage: Arc<dyn StorageRepository>,
    broker: Arc<dyn MessageBroker>,
    config_manager: Arc<ConfigManager>,
}

impl ServiceProcessor {
    pub fn new(
        storage: Arc<dyn StorageRepository>, 
        broker: Arc<dyn MessageBroker>,
        config_manager: Arc<ConfigManager>
    ) -> Self {
        Self { storage, broker, config_manager }
    }

    /// Process Ingest: Parse, Validate, Check Alerts, Return Telemetry for Batching
    #[instrument(skip(self, payload), fields(topic = %topic, payload_len = payload.len()))]
    pub async fn process_ingest_logic(&self, topic: &str, payload: &[u8]) -> anyhow::Result<Option<Telemetry>> {
        // 1. Parse
        let telemetry: Telemetry = serde_json::from_slice(payload)
            .map_err(|e| anyhow::anyhow!("Invalid JSON: {:?}", e))?;

        // 2. Validate
        if telemetry.device_id.is_empty() {
             return Err(anyhow::anyhow!("Empty device_id"));
        }

        // 3. Alert Logic (Happens immediately)
        // 3. Alert & Key Logic (Happens immediately)
        // Extract User ID
        let parts: Vec<&str> = topic.split('/').collect();
        let uid = if parts.len() >= 4 && parts[2] == "users" {
            Some(parts[3])
        } else if parts.len() >= 2 && parts[0] == "users" {
             Some(parts[1])
        } else {
             None
        };
        
        let user_id = uid.unwrap_or("unknown").to_string();
        
        // Inject User ID into Telemetry
        let mut telemetry = telemetry;
        telemetry.user_id = user_id.clone();

        if let Some(user_id) = uid {
            let config = self.config_manager.get_config(user_id);
            
            // 1. Legacy Static Check (Backwards Compatibility)
            let max_temp = config.temperature_max;
            if let Some(temp) = telemetry.temperature {
                if temp > max_temp {
                    let alert = Alert::new(
                        telemetry.device_id.clone(),
                        "HighTemperature".to_string(),
                        format!("Temperature {:.2} exceeds user limit {:.2}", temp, max_temp),
                        Some(temp),
                    );
                    self.storage.store_alert(&alert).await?;
                    let alert_topic = format!("users/{}/alerts", user_id);
                    self.broker.publish(&alert_topic, serde_json::to_vec(&alert)?).await?;
                    info!("Legacy Alert published to {}", alert_topic);
                }
            }

            // 2. Dynamic Rules Engine
            for rule in &config.rules {
                if let Some(val) = telemetry.get_value(&rule.key) {
                    let triggered = match rule.operator.as_str() {
                        ">" => val > rule.threshold,
                        "<" => val < rule.threshold,
                        ">=" => val >= rule.threshold,
                        "<=" => val <= rule.threshold,
                        "==" => (val - rule.threshold).abs() < f64::EPSILON,
                        _ => false,
                    };

                    if triggered {
                        let alert = Alert::new(
                            telemetry.device_id.clone(),
                            format!("Rule:{}", rule.key), // Alert Type
                            rule.message.clone(),
                            Some(val),
                        );
                        
                        self.storage.store_alert(&alert).await?;
                        
                        let alert_topic = format!("users/{}/alerts", user_id);
                        self.broker.publish(&alert_topic, serde_json::to_vec(&alert)?).await?;
                        info!("Dynamic Alert ({}) published to {}", rule.key, alert_topic);
                    }
                }
            }
        }

        // Return telemetry for batching
        Ok(Some(telemetry))
    }

    pub async fn process_config_update(&self, topic: &str, payload: &[u8]) -> anyhow::Result<()> {
        let parts: Vec<&str> = topic.split('/').collect();
        let uid = if parts.len() >= 3 && parts[0] == "users" && parts[2] == "config" {
            Some(parts[1])
        } else {
            None
        };

        if let Some(user_id) = uid {
            let new_config: UserConfig = serde_json::from_slice(payload)?;
            self.config_manager.update_user_config(user_id, new_config).await?;
        }
        Ok(())
    }
}

// Message type for Channel
pub struct IngestMessage {
    pub telemetry: Telemetry,
    pub packet: Option<rumqttc::Publish>, // To Ack later
}

/// 1. MQTT MAINTENANCE LOOP (Config & Alerts Support)
/// Replaces the old Ingest Loop. No longer subscribes to Telemetry.
pub async fn run_mqtt_maintenance_loop(
    mut eventloop: rumqttc::EventLoop,
    client: AsyncClient, 
    processor: Arc<ServiceProcessor>,
) -> anyhow::Result<()> {
    
    // Ensure subscriptions for CONFIG only
    client.subscribe("users/+/config", rumqttc::QoS::AtLeastOnce).await?;
    info!("MQTT Maintenance Loop Started. Subscribed to Configs. Telemetry handled via Kafka.");

    loop {
        match eventloop.poll().await {
            Ok(event) => {
                match event {
                    Event::Incoming(Packet::Publish(publish)) => {
                        let topic = publish.topic.clone();
                        let payload = publish.payload.clone();

                        if topic.ends_with("/config") {
                                if let Err(e) = processor.process_config_update(&topic, &payload).await {
                                    error!("Config processing failed: {:?}", e);
                                }
                        }
                        // Ignore other topics (telemetry shouldn't be here)
                    }
                    Event::Incoming(Packet::ConnAck(_)) => {
                        info!("MQTT Connected! Resubscribing to Configs...");
                        client.subscribe("users/+/config", rumqttc::QoS::AtLeastOnce).await?;
                    }
                    _ => {}
                }
            }
            Err(e) => {
                error!("MQTT Error: {:?}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// 2. BATCH EXECUTOR (Buffer & Flush)
pub async fn run_batch_executor(
    mut receiver: tokio::sync::mpsc::Receiver<IngestMessage>,
    storage: Arc<dyn StorageRepository>,
    client: AsyncClient,
) {
    info!("Batch Executor Started.");

    loop {
        let mut batch_telemetry = Vec::with_capacity(1000);
        let mut batch_packets = Vec::with_capacity(1000);

        // Block for 1st item
        let first = match receiver.recv().await {
            Some(m) => m,
            None => {
                info!("Ingest Channel Closed. Stopping Executor.");
                break;
            }
        };
        batch_telemetry.push(first.telemetry);
        if let Some(p) = first.packet { batch_packets.push(p); }

        // Gather more items with timeout
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        
        while batch_telemetry.len() < 1000 {
            let now = tokio::time::Instant::now();
            if now >= deadline { break; }
            
            match tokio::time::timeout(deadline - now, receiver.recv()).await {
                Ok(Some(msg)) => {
                    batch_telemetry.push(msg.telemetry);
                    if let Some(p) = msg.packet { batch_packets.push(p); }
                }
                _ => break, // Timeout or Channel Closed or Error
            }
        }

        // Flush Batch
        if !batch_telemetry.is_empty() {
            // Attempt Fast Batch Insert
            match storage.store_telemetry_batch(&batch_telemetry).await {
                Ok(_) => {
                    // Happy Path: Ack all
                    for pkt in batch_packets {
                         // Fire and forget ack
                        if let Err(e) = client.ack(&pkt).await {
                            error!("Ack Failed: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Batch Insert Failed ({:?}). Switching to Serial Fallback mode to preserve data.", e);
                    
                    // Fallback: Insert One-by-One
                    for (i, telemetry) in batch_telemetry.iter().enumerate() {
                        let pkt = &batch_packets[i];
                        
                        match storage.store_telemetry(telemetry).await {
                            Ok(_) => {
                                // Individual Success -> Ack
                                if let Err(e) = client.ack(pkt).await {
                                    error!("Ack Failed during fallback: {:?}", e);
                                }
                            }
                            Err(e2) => {
                                error!("Serial Fallback also failed for device {}: {:?}", telemetry.device_id, e2);
                                // DO NOT ACK. Broker will redeliver this specific message.
                                // This isolates the "Poison Pill" from the rest of the batch.
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::domain::{Rule, Telemetry};
    use crate::state::config_manager::UserConfig;
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Mutex;
    use std::collections::HashMap;
    use sqlx::types::time::OffsetDateTime;

    struct MockStorage {
        alerts: Mutex<Vec<Alert>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self { alerts: Mutex::new(Vec::new()) }
        }
    }

    #[async_trait]
    impl StorageRepository for MockStorage {
        async fn store_telemetry(&self, _data: &Telemetry) -> anyhow::Result<()> { Ok(()) }
        async fn store_alert(&self, alert: &Alert) -> anyhow::Result<()> {
            self.alerts.lock().unwrap().push(alert.clone());
            Ok(())
        }
        async fn store_telemetry_batch(&self, _batch: &[Telemetry]) -> anyhow::Result<()> { Ok(()) }
    }

    struct MockBroker {
        published: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl MockBroker {
        fn new() -> Self {
            Self { published: Mutex::new(Vec::new()) }
        }
    }

    #[async_trait]
    impl MessageBroker for MockBroker {
        async fn publish(&self, topic: &str, payload: Vec<u8>) -> anyhow::Result<()> {
            self.published.lock().unwrap().push((topic.to_string(), payload));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_dynamic_rules() {
        // 1. Setup Processor with Mocks
        let storage = Arc::new(MockStorage::new());
        let broker = Arc::new(MockBroker::new());
        
        // Mock DB Pool (Lazy connection, won't actually connect during test)
        let pool = PgPoolOptions::new().connect_lazy("postgres://user:pass@localhost:5432/db").unwrap();
        let config_manager = Arc::new(ConfigManager::new(pool));

        // 2. Inject Config with Rules
        let rules = vec![
            Rule { key: "temperature".into(), operator: ">".into(), threshold: 50.0, message: "Too Hot!".into() },
            Rule { key: "battery".into(), operator: "<".into(), threshold: 20.0, message: "Low Battery".into() },
        ];
        config_manager.inject_config("test_user".to_string(), UserConfig { temperature_max: 100.0, rules });

        let processor = ServiceProcessor::new(storage.clone(), broker.clone(), config_manager);

        // 3. Create Telemetry that triggers both rules
        let telemetry = Telemetry {
            time: OffsetDateTime::now_utc(),
            user_id: "test_user".into(),
            device_id: "device_1".into(),
            sequence_id: 1,
            temperature: Some(60.0), // Triggers > 50
            battery: Some(10),       // Triggers < 20
            extra: HashMap::new(),
        };
        let payload = serde_json::to_vec(&telemetry).unwrap();

        // 4. Run Process Logic
        let topic = "users/test_user/telemetry"; 
        processor.process_ingest_logic(topic, &payload).await.unwrap();

        // 5. Assertions
        let alerts = storage.alerts.lock().unwrap();
        
        if alerts.len() != 2 {
            println!("Got alerts: {:?}", *alerts);
        }
        assert_eq!(alerts.len(), 2, "Expected 2 alerts triggered");
        
        let temp_alert = alerts.iter().find(|a| a.alert_type == "Rule:temperature").expect("Missing Temp Alert");
        assert_eq!(temp_alert.message, "Too Hot!");

        let batt_alert = alerts.iter().find(|a| a.alert_type == "Rule:battery").expect("Missing Battery Alert");
        assert_eq!(batt_alert.message, "Low Battery");
    }
}
