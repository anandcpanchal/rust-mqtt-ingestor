use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::message::Message;
use tracing::{info, error, warn, instrument};
use std::sync::Arc;
use crate::service::processor::{ServiceProcessor, IngestMessage};
use tokio::sync::mpsc::Sender;

pub struct KafkaAdapter {
    consumer: StreamConsumer,
    topic: String,
}

#[derive(serde::Deserialize, Debug)]
struct EmqxBridgeMessage {
    topic: String,
    payload: serde_json::Value, // Payload can be string or object
    // timestamp: u64,
    // clientid: String,
}

impl KafkaAdapter {
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true") 
            .set("auto.offset.reset", "latest")
            .create()
            .expect("Consumer creation failed");

        Self {
            consumer,
            topic: topic.to_string(),
        }
    }

    #[instrument(skip(self, processor, sender, shutdown_signal), fields(topic = %self.topic))]
    pub async fn run_loop(
        &self,
        processor: Arc<ServiceProcessor>,
        sender: Sender<IngestMessage>,
        mut shutdown_signal: tokio::sync::watch::Receiver<bool>, // Add Shutdown Signal logic!
    ) -> anyhow::Result<()> {
        
        self.consumer.subscribe(&[&self.topic]).expect("Can't subscribe to specified topic");
        info!("Kafka Consumer started. Subscribed to {}", self.topic);

        loop {
             tokio::select! {
                change = shutdown_signal.changed() => {
                    if change.is_ok() && *shutdown_signal.borrow() {
                        info!("Shutdown Signal Received in Kafka Loop.");
                        break;
                    }
                }
                
                res = self.consumer.recv() => {
                    match res {
                        Err(e) => error!("Kafka error: {}", e),
                        Ok(m) => {
                            if let Some(payload_bytes) = m.payload() {
                                // 1. Determine Input Format
                                // EMQX Bridge usually sends a JSON wrapper.
                                // We try to parse as EmqxBridgeMessage first.
                                let (original_topic, original_payload) = match serde_json::from_slice::<EmqxBridgeMessage>(payload_bytes) {
                                    Ok(bridge_msg) => {
                                        // Payload inside might be a String (JSON stringified) or Object
                                        let bytes = match bridge_msg.payload {
                                            serde_json::Value::String(s) => s.into_bytes(),
                                            v => serde_json::to_vec(&v).unwrap_or_default(),
                                        };
                                        (bridge_msg.topic, bytes)
                                    },
                                    Err(_) => {
                                        // Fallback: Assume raw payload if bridge is configured to "Raw"
                                        // But wait, we need the TOPIC to extract UserID!
                                        // If we can't get the topic, we might fail to process alerts correctly.
                                        // For now, let's treat it as a raw payload and maybe topic is in key?
                                        // Or just log warn and skip.
                                        warn!("Received non-bridge format message. Skipping (need topic for logic).");
                                        continue;
                                    }
                                };
                                
                                // 2. Process Logic (Same as before)
                                match processor.process_ingest_logic(&original_topic, &original_payload).await {
                                    Ok(Some(telemetry)) => {
                                        let msg = IngestMessage { telemetry, packet: None }; // No MQTT Packet to Ack
                                        if let Err(e) = sender.send(msg).await {
                                             error!("Channel closed: {:?}", e);
                                             break; 
                                        }
                                    },
                                    Ok(None) => {}, // Filtered
                                    Err(e) => error!("Processing error: {:?}", e),
                                }
                            }
                        }
                    }
                }
             }
        }
        
        info!("Kafka Loop Exited.");
        Ok(())
    }
}
