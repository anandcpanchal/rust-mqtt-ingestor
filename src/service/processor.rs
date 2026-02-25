use crate::domain::{Telemetry, ActiveAlert};
use crate::ports::{StorageRepository, MessageBroker};
use crate::state::config_manager::{ConfigManager, UserConfig};
use std::sync::Arc;
use tracing::{info, warn, error, instrument};
use rumqttc::{Event, Packet, AsyncClient};
use std::time::Duration;
use opentelemetry::trace::TraceContextExt;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
    #[instrument(
        skip(self, payload), 
        fields(
            topic = %topic, 
            payload_len = payload.len(),
            device_id = tracing::field::Empty,
            sequence_id = tracing::field::Empty,
            payload = tracing::field::Empty
        )
    )]
    pub async fn process_ingest_logic(&self, topic: &str, payload: &[u8]) -> anyhow::Result<Option<Telemetry>> {
        // 1. Parse
        let telemetry: Telemetry = serde_json::from_slice(payload)
            .map_err(|e| {
                let raw_payload = String::from_utf8_lossy(payload);
                tracing::Span::current().record("payload", &raw_payload.as_ref());
                anyhow::anyhow!("Invalid JSON (payload recorded in trace): {:?}", e)
            })?;

        // Record business IDs in span for searchable pinpointing
        let span = tracing::Span::current();
        span.record("device_id", &telemetry.device_id);
        span.record("sequence_id", &telemetry.sequence_id);

        // 2. Validate
        if telemetry.device_id.is_empty() {
             return Err(anyhow::anyhow!("Empty device_id"));
        }

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

        if let Some(uid_str) = uid {
            let config = self.config_manager.get_config(uid_str);

            // 2. Dynamic Rules Engine (Stateful)
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
                        let active_alert = ActiveAlert {
                            user_id: uid_str.to_string(),
                            device_id: telemetry.device_id.clone(),
                            rule_id: rule.key.clone(),
                            start_time: telemetry.time,
                            last_seen: telemetry.time,
                            status: "Active".to_string(),
                            snooze_until: None,
                            current_value: Some(val),
                        };

                        // call upsert (Deduplication)
                        if let Some(alert) = self.storage.upsert_active_alert(&active_alert, &rule.message).await? {
                            // If returned generic Alert -> Publish & Store History
                            self.storage.store_alert(&alert).await?;
                            
                            let alert_topic = format!("users/{}/alerts", uid_str);
                            self.broker.publish(&alert_topic, serde_json::to_vec(&alert)?).await?;
                            info!("Alert TRIGGERED ({}) -> {}", rule.key, alert_topic);
                            
                            metrics::counter!("alerts_triggered_total", 1, "rule" => rule.key.clone(), "user_id" => uid_str.to_string());
                        } else {
                            // Logic handled, but no new publication needed (e.g. heartbeat update or snoozed)
                        }
                    } else {
                        // Not triggered -> Check if we need to RESOLVE
                        // Auto-Recovery Logic
                        if let Some(resolved_alert) = self.storage.resolve_active_alert(uid_str, &telemetry.device_id, &rule.key).await? {
                            self.storage.store_alert(&resolved_alert).await?;
                            let alert_topic = format!("users/{}/alerts", uid_str);
                            self.broker.publish(&alert_topic, serde_json::to_vec(&resolved_alert)?).await?;
                            info!("Alert RESOLVED ({}) -> {}", rule.key, alert_topic);
                        }
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

    pub async fn process_alert_management(&self, topic: &str, payload: &[u8]) -> anyhow::Result<()> {
        let parts: Vec<&str> = topic.split('/').collect();
        // Topic: users/{user_id}/alerts/manage
        let uid = if parts.len() >= 4 && parts[0] == "users" && parts[2] == "alerts" && parts[3] == "manage" {
            Some(parts[1])
        } else {
            None
        };

        if let Some(user_id) = uid {
            let req: AlertManagementRequest = serde_json::from_slice(payload)?;
            match req.action.as_str() {
                "SNOOZE" => {
                    let duration = req.duration_minutes.unwrap_or(30);
                    self.storage.snooze_active_alert(user_id, &req.device_id, &req.rule_id, duration).await?;
                    info!("Alert SNOOZED for user {} device {} rule {}", user_id, req.device_id, req.rule_id);
                },
                "DISABLE" => {
                    self.storage.disable_active_alert(user_id, &req.device_id, &req.rule_id).await?;
                    info!("Alert DISABLED for user {} device {} rule {}", user_id, req.device_id, req.rule_id);
                },
                _ => warn!("Unknown alert action: {}", req.action),
            }
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
struct AlertManagementRequest {
    device_id: String,
    rule_id: String,
    action: String, // SNOOZE, DISABLE
    duration_minutes: Option<i64>,
}

// Message type for Channel
pub struct IngestMessage {
    pub telemetry: Telemetry,
    pub packet: Option<rumqttc::Publish>, // To Ack later
    pub otel_context: std::collections::HashMap<String, String>,
}

/// 1. MQTT MAINTENANCE LOOP (Config & Alerts Support)
/// Replaces the old Ingest Loop. No longer subscribes to Telemetry.
pub async fn run_mqtt_maintenance_loop(
    mut eventloop: rumqttc::EventLoop,
    client: AsyncClient, 
    processor: Arc<ServiceProcessor>,
) -> anyhow::Result<()> {
    
    // Ensure subscriptions for CONFIG and ALERTS management
    client.subscribe("users/+/config", rumqttc::QoS::AtLeastOnce).await?;
    client.subscribe("users/+/alerts/manage", rumqttc::QoS::AtLeastOnce).await?;
    info!("MQTT Maintenance Loop Started. Subscribed to Configs & Alert Mgmt. Telemetry handled via Kafka.");

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
                        } else if topic.ends_with("/alerts/manage") {
                                if let Err(e) = processor.process_alert_management(&topic, &payload).await {
                                    error!("Alert management processing failed: {:?}", e);
                                }
                        }
                        // Ignore other topics
                    }
                    Event::Incoming(Packet::ConnAck(_)) => {
                        info!("MQTT Connected! Resubscribing to Configs & Alerts...");
                        client.subscribe("users/+/config", rumqttc::QoS::AtLeastOnce).await?;
                        client.subscribe("users/+/alerts/manage", rumqttc::QoS::AtLeastOnce).await?;
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
        let mut batch_contexts = Vec::with_capacity(1000);

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
        
        let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&first.otel_context)
        });
        batch_contexts.push(parent_cx);

        // Gather more items with timeout
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        
        while batch_telemetry.len() < 1000 {
            let now = tokio::time::Instant::now();
            if now >= deadline { break; }
            
            match tokio::time::timeout(deadline - now, receiver.recv()).await {
                Ok(Some(msg)) => {
                    batch_telemetry.push(msg.telemetry);
                    if let Some(p) = msg.packet { batch_packets.push(p); }
                    
                    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
                        propagator.extract(&msg.otel_context)
                    });
                    batch_contexts.push(parent_cx);
                }
                _ => break, // Timeout or Channel Closed or Error
            }
        }

        // Flush Batch
        if !batch_telemetry.is_empty() {
             let span = tracing::info_span!("batch_flush", batch_size = batch_telemetry.len());
             
             // Add links to all parent contexts in the batch
             for parent_cx in &batch_contexts {
                 let parent_span = parent_cx.span();
                 let span_context = parent_span.span_context();
                 if span_context.is_valid() {
                    //  info!("Adding Link to Batch Trace: trace_id={}", span_context.trace_id());
                     span.add_link(span_context.clone());
                 }
             }

             let _enter = span.enter();

             let batch_len = batch_telemetry.len() as f64;
             metrics::histogram!("batch_size", batch_len);
             let start = std::time::Instant::now();

            // Attempt Fast Batch Insert
            match storage.store_telemetry_batch(&batch_telemetry).await {
                Ok(_) => {
                    // Metrics: DB duration & rows
                    let duration = start.elapsed().as_secs_f64();
                    let rows = batch_telemetry.len() as u64;
                    metrics::histogram!("db_write_duration_seconds", duration);
                    metrics::counter!("total_rows_inserted", rows);
                    
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
        
        async fn upsert_active_alert(&self, alert: &ActiveAlert, message: &str) -> anyhow::Result<Option<Alert>> {
            // Mock deduplication: Always return new Alert for test simplicity
            // or implement basic logic? 
            // For now, let's behave like "always new" to pass existing test logic
            Ok(Some(Alert::new(
                alert.device_id.clone(),
                alert.user_id.clone(),
                alert.rule_id.clone(),
                format!("Rule:{}", alert.rule_id),
                message.to_string(),
                alert.current_value,
                "Triggered".to_string()
            )))
        }
        async fn resolve_active_alert(&self, _user_id: &str, _device_id: &str, _rule_id: &str) -> anyhow::Result<Option<Alert>> { Ok(None) }
        async fn snooze_active_alert(&self, _user_id: &str, _device_id: &str, _rule_id: &str, _duration: i64) -> anyhow::Result<()> { Ok(()) }
        async fn disable_active_alert(&self, _user_id: &str, _device_id: &str, _rule_id: &str) -> anyhow::Result<()> { Ok(()) }
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
        config_manager.inject_config("test_user".to_string(), UserConfig { rules });

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
