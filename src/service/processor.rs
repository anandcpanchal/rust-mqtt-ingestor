use crate::domain::{Telemetry, Alert};
use crate::ports::{StorageRepository, MessageBroker};
use crate::state::config_manager::{ConfigManager, UserConfig};
use std::sync::Arc;
use tracing::{info, error, instrument};
use rumqttc::{Event, Packet, AsyncClient};
use tokio::sync::mpsc::Sender;
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
            let max_temp = config.temperature_max;

            if let Some(temp) = telemetry.temperature {
                if temp > max_temp {
                    // ALERT!
                    let alert = Alert::new(
                        telemetry.device_id.clone(),
                        "HighTemperature".to_string(),
                        format!("Temperature {:.2} exceeds user limit {:.2}", temp, max_temp),
                        Some(temp),
                    );
                    
                    // We store/publish alert synchronously here (rare)
                    self.storage.store_alert(&alert).await?;
                    
                    let alert_topic = format!("users/{}/alerts", user_id);
                    let alert_json = serde_json::to_vec(&alert)?;
                    self.broker.publish(&alert_topic, alert_json).await?;
                    info!("Alert published to {}", alert_topic);
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

/// 1. INGEST LOOP (High-speed Producer)
pub async fn run_ingest_loop(
    mut eventloop: rumqttc::EventLoop,
    client: AsyncClient, 
    processor: Arc<ServiceProcessor>,
    ingest_topic: &str,
    sender: Sender<IngestMessage>,
    mut shutdown_signal: tokio::sync::watch::Receiver<bool>, // NEW
) -> anyhow::Result<()> {
    
    // Ensure subscriptions
    client.subscribe("users/+/config", rumqttc::QoS::AtLeastOnce).await?;
    client.subscribe(ingest_topic, rumqttc::QoS::AtLeastOnce).await?;
    info!("Ingest Loop Started. Subscribed to {}", ingest_topic);

    // Wrap sender in Option so we can drop it to signal EOF
    let mut sender_opt = Some(sender);
    
    // State for Smart Logging
    let mut is_throttled = false;

    loop {
        tokio::select! {
            // 1. Check for Shutdown Signal
            change = shutdown_signal.changed() => {
                if change.is_ok() && *shutdown_signal.borrow() {
                    if sender_opt.is_some() {
                        info!("Shutdown Signal Received in Loop. Dropping Ingest Channel to flush Executor.");
                        sender_opt = None; // This drops the sender, causing Receiver to returning None once empty.
                        // We CONTINUE looping to process Acks from the Executor!
                    }
                }
            }

            // 2. Poll MQTT Event Loop
            event = eventloop.poll() => {
                match event {
                    Ok(event) => {
                        match event {
                            Event::Incoming(Packet::Publish(publish)) => {
                                // If sender is dropped (shutting down), we MUST NOT process new messages.
                                // We can either Nack them or just ignore (Broker will resend later).
                                if sender_opt.is_none() {
                                    // Ignoring message during shutdown phase
                                    continue;
                                }

                                let topic = publish.topic.clone();
                                let payload = publish.payload.clone();

                                if topic.ends_with("/config") {
                                     if let Err(e) = processor.process_config_update(&topic, &payload).await {
                                         error!("Config processing failed: {:?}", e);
                                     }
                                } else {
                                     // Telemetry
                                     match processor.process_ingest_logic(&topic, &payload).await {
                                        Ok(Some(telemetry)) => {
                                            // 1. Lag Monitoring (Queue Pressure Detect)
                                            let now = time::OffsetDateTime::now_utc();
                                            let msg_time = telemetry.time;
                                            let lag = now - msg_time;
                                            
                                            if lag.as_seconds_f64() > 5.0 {
                                                // Log heavily delayed messages (indicates deep queue draining)
                                                tracing::warn!("🐢 System Lagging! Processing data from {:.1}s ago (Device: {}).", lag.as_seconds_f64(), telemetry.device_id);
                                            }

                                            // Send to Batcher
                                            let msg = IngestMessage { telemetry, packet: Some(publish) };
                                            if let Some(tx) = &sender_opt {
                                                // 2. Smart Backpressure Logging (Burst Handling)
                                                let capacity = tx.capacity();
                                                if capacity < 1000 {
                                                    if !is_throttled {
                                                        tracing::warn!("⚠️ Backpressure Active: Ingest Channel full ({}/10000). Pausing ingest to let DB catch up.", capacity);
                                                        is_throttled = true;
                                                    }
                                                } else if is_throttled && capacity > 5000 {
                                                    tracing::info!("✅ Backpressure Resolved: Channel recovered ({}/10000). Resuming high-speed ingest.", capacity);
                                                    is_throttled = false;
                                                }

                                                if let Err(e) = tx.send(msg).await {
                                                    error!("Channel closed, stopping ingest: {:?}", e);
                                                    break;
                                                }
                                            }
                                        }
                                        Ok(None) => {} // filtered
                                        Err(e) => {
                                            error!("Ingest Error: {:?}", e);
                                        }
                                     }
                                }
                            }
                            Event::Incoming(Packet::ConnAck(_)) => {
                                info!("Connected! Resubscribing...");
                                client.subscribe(ingest_topic, rumqttc::QoS::AtLeastOnce).await?;
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
    }
    Ok(())
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
