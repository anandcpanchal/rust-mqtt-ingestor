use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::message::Message;
use tracing::{info, error, warn, instrument, Span};
use crate::service::worker_pool::RawIngestMessage;
use tokio::sync::mpsc::Sender;
use crate::telemetry::KafkaHeaderExtractor;
use std::collections::HashMap;
use opentelemetry::propagation::Extractor;

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
            .set("security.protocol", "plaintext")
            .create()
            .expect("Consumer creation failed");

        Self {
            consumer,
            topic: topic.to_string(),
        }
    }

    #[instrument(skip(self, sender, shutdown_signal), fields(topic = %self.topic, trace_id, payload = tracing::field::Empty))]
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
                            // Extract tracing context from headers
                            let mut otel_context_map = HashMap::new();
                            if let Some(headers) = m.headers() {
                                let extractor = KafkaHeaderExtractor(headers);
                                if let Some(val) = extractor.get("traceparent") {
                                    info!("Received traceparent header: {}", val);
                                } else {
                                    warn!("No traceparent header found in message!");
                                    // Log all keys
                                    let keys = opentelemetry::propagation::Extractor::keys(&extractor);
                                    warn!("Available headers: {:?}", keys);
                                }

                                let context = opentelemetry::global::get_text_map_propagator(|propagator| {
                                    propagator.extract(&extractor)
                                });
                                
                                // Set the parent context for the current span
                                // Span::current().set_parent(context.clone()); // REMOVED: This links EVERY message to the long-running loop trace!

                                
                                // Inject into our internal map for propagation
                                opentelemetry::global::get_text_map_propagator(|propagator| {
                                    propagator.inject_context(&context, &mut otel_context_map);
                                });
                            }

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
                                    Err(e) => {
                                        let raw_payload = String::from_utf8_lossy(payload_bytes);
                                        warn!(error = %e, payload = %raw_payload, "Received non-bridge format message. Skipping (need topic for logic).");
                                        
                                        // Record in span for OTel
                                        let span = Span::current();
                                        span.record("payload", &raw_payload.as_ref());
                                        continue;
                                    }
                                };
                                
                                // 2. Send to Worker Pool (Decoupled)
                                let msg = RawIngestMessage {
                                    topic: original_topic,
                                    payload: original_payload,
                                    otel_context: otel_context_map,
                                };
                                
                                // Metrics
                                let topic_label = msg.topic.clone();
                                let len = msg.payload.len() as f64;
                                metrics::counter!("mqtt_messages_received_total", 1, "topic" => topic_label.clone());
                                metrics::histogram!("mqtt_message_size_bytes", len, "topic" => topic_label);
                                
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
