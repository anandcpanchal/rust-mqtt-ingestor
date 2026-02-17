use crate::ports::DlqRepository;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use std::time::Duration;
use tracing::{info, instrument};
use base64::{Engine as _, engine::general_purpose};

pub struct KafkaDlqProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaDlqProducer {
    pub fn new(brokers: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("DLQ Producer creation failed");

        info!("DLQ Producer initialized for topic: {}", topic);
        Self {
            producer,
            topic: topic.to_string(),
        }
    }
}

#[async_trait]
impl DlqRepository for KafkaDlqProducer {
    #[instrument(skip(self, payload), fields(topic = %original_topic))]
    async fn send_to_dlq(&self, original_topic: String, payload: Vec<u8>, error_msg: String) -> anyhow::Result<()> {
        
        // Base64 encode the payload (safely handling owned Vec<u8>)
        let payload_base64 = general_purpose::STANDARD.encode(&payload);
        
        // Wrap with meta dat
        let dlq_message = json!({
            "original_topic": original_topic,
            "error": error_msg,
            "timestamp": time::OffsetDateTime::now_utc().to_string(),
            "payload_base64": payload_base64,
        });

        let json_bytes = serde_json::to_vec(&dlq_message)?;

        let record = FutureRecord::to(&self.topic)
            .payload(&json_bytes)
            .key(&original_topic); // Use original topic as key for partitioning

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(anyhow::anyhow!("Failed to produce to DLQ: {:?}", e)),
        }
    }
}
