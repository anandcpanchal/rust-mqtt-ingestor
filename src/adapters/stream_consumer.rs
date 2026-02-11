use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::message::Message;
use tracing::{info, error, warn, instrument};
use crate::service::worker_pool::RawIngestMessage;
use tokio::sync::mpsc::Sender;

pub struct KafkaAdapter {
    consumer: StreamConsumer,
    topic: String,
}

#[derive(serde::Deserialize, Debug)]
struct EmqxBridgeMessage {
    topic: String,
    payload: serde_json::Value, 
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

    #[instrument(skip(self, sender, shutdown_signal), fields(topic = %self.topic))]
    pub async fn run_loop(
        &self,
        sender: Sender<RawIngestMessage>,
        mut shutdown_signal: tokio::sync::watch::Receiver<bool>,
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
                                let (original_topic, original_payload) = match serde_json::from_slice::<EmqxBridgeMessage>(payload_bytes) {
                                    Ok(bridge_msg) => {
                                        let bytes = match bridge_msg.payload {
                                            serde_json::Value::String(s) => s.into_bytes(),
                                            v => serde_json::to_vec(&v).unwrap_or_default(),
                                        };
                                        (bridge_msg.topic, bytes)
                                    },
                                    Err(_) => {
                                        warn!("Received non-bridge format message. Skipping (need topic for logic).");
                                        continue;
                                    }
                                };
                                
                                // 2. Send to Worker Pool (Decoupled)
                                let msg = RawIngestMessage {
                                    topic: original_topic,
                                    payload: original_payload,
                                };
                                
                                if let Err(e) = sender.send(msg).await {
                                     error!("Worker Pool Channel closed: {:?}", e);
                                     break; 
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
