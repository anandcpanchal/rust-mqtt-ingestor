use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, error};
use crate::service::processor::{ServiceProcessor, IngestMessage};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use opentelemetry::trace::TraceContextExt;

#[derive(Debug)]
pub struct RawIngestMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub otel_context: std::collections::HashMap<String, String>,
}

    pub struct WorkerPool {
    processor: Arc<ServiceProcessor>,
    dlq: Arc<dyn crate::ports::DlqRepository>,
    concurrency: usize,
}

impl WorkerPool {
    pub fn new(processor: Arc<ServiceProcessor>, dlq: Arc<dyn crate::ports::DlqRepository>, concurrency: usize) -> Self {
        Self { processor, dlq, concurrency }
    }

    pub async fn run(self, mut receiver: Receiver<RawIngestMessage>, batch_sender: Sender<IngestMessage>) {
        info!("WorkerPool starting with {} workers", self.concurrency);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        let active_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        while let Some(msg) = receiver.recv().await {
            // Extract parent span context from message
            let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.extract(&msg.otel_context)
            });

            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    error!("Semaphore closed");
                    break;
                }
            };
            
            let processor = self.processor.clone();
            let dlq = self.dlq.clone();
            let batch_sender = batch_sender.clone();
            let active_count = active_count.clone();

            tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completion

                let span = tracing::info_span!("worker_process", topic = %msg.topic);
                span.set_parent(parent_cx);
                let _enter = span.enter();
                
                // Track active workers
                let current = active_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                metrics::gauge!("worker_active_count", current as f64);
                
                // Metrics: Jobs Started
                metrics::counter!("worker_jobs_started_total", 1);
                
                let start = std::time::Instant::now();

                match processor.process_ingest_logic(&msg.topic, &msg.payload).await {
                    Ok(Some(telemetry)) => {
                        let ingest_msg = IngestMessage {
                            telemetry,
                            packet: None,
                            otel_context: msg.otel_context,
                        };
                        if let Err(e) = batch_sender.send(ingest_msg).await {
                            error!("Failed to send to batch executor: {}", e);
                            metrics::counter!("worker_errors_total", 1, "type" => "channel_closed");
                        }
                    },
                    Ok(None) => {}, // Filtered
                    Err(e) => {
                        error!("Processing error in WorkerPool: {:?}. Sending to DLQ.", e);
                        metrics::counter!("worker_errors_total", 1, "type" => "processing_error");
                        
                        // Send to DLQ
                        if let Err(dlq_err) = dlq.send_to_dlq(msg.topic.clone(), msg.payload.clone(), e.to_string()).await {
                             error!("CRITICAL: Failed to send to DLQ: {:?}", dlq_err);
                             metrics::counter!("dlq_produce_errors_total", 1);
                        } else {
                             metrics::counter!("dlq_messages_produced_total", 1);
                        }
                    },
                }
                
                // Metrics: Duration
                let duration = start.elapsed().as_secs_f64();
                metrics::histogram!("worker_processing_duration_seconds", duration);
                
                let remaining = active_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) - 1;
                metrics::gauge!("worker_active_count", remaining as f64);
            });
        }
        info!("WorkerPool shutting down");
    }
}

// #[cfg(test)]
// struct MockDlq {
//     pub calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
// }
//
// #[cfg(test)]
// impl MockDlq {
//     fn new() -> Self {
//         Self { calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())) }
//     }
// }
//
// #[cfg(test)]
//     // Manual implementation to avoid async_trait lifetime/Send issues
//     impl crate::ports::DlqRepository for MockDlq {
//         fn send_to_dlq<'a>(&'a self, original_topic: String, _payload: Vec<u8>, error_msg: String) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
//             // Perform logic synchronously
//             self.calls.lock().unwrap().push(format!("topic: {}, error: {}", original_topic, error_msg));
//             // Return ready future
//             Box::pin(std::future::ready(Ok(())))
//         }
//     }

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::domain::Telemetry;
    use crate::ports::{StorageRepository, MessageBroker};
    use crate::state::config_manager::ConfigManager;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;
    use tokio::time::sleep;

    struct MockStorage;
    #[async_trait]
    impl StorageRepository for MockStorage {
        async fn store_telemetry(&self, _data: &Telemetry) -> anyhow::Result<()> { Ok(()) }
        async fn store_alert(&self, _alert: &crate::domain::Alert) -> anyhow::Result<()> { Ok(()) }
        async fn store_telemetry_batch(&self, _batch: &[Telemetry]) -> anyhow::Result<()> { Ok(()) }
        
        async fn upsert_active_alert(&self, _alert: &crate::domain::ActiveAlert, _msg: &str) -> anyhow::Result<Option<crate::domain::Alert>> { Ok(None) }
        async fn resolve_active_alert(&self, _u: &str, _d: &str, _r: &str) -> anyhow::Result<Option<crate::domain::Alert>> { Ok(None) }
        async fn snooze_active_alert(&self, _u: &str, _d: &str, _r: &str, _dur: i64) -> anyhow::Result<()> { Ok(()) }
        async fn disable_active_alert(&self, _u: &str, _d: &str, _r: &str) -> anyhow::Result<()> { Ok(()) }
    }

    struct MockBroker;
    #[async_trait]
    impl MessageBroker for MockBroker {
        async fn publish(&self, _topic: &str, _payload: Vec<u8>) -> anyhow::Result<()> { Ok(()) }
    }

    // #[tokio::test]
    // async fn test_worker_pool_concurrency() {
    //     let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(10);
    //     let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(10);
    //
    //     let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/db").unwrap();
    //     let config_manager = Arc::new(ConfigManager::new(pool));
    //     let storage = Arc::new(MockStorage);
    //     let broker = Arc::new(MockBroker);
    //     let processor = Arc::new(ServiceProcessor::new(storage, broker, config_manager));
    //     let dlq = Arc::new(MockDlq::new());
    //
    //     let worker_pool = WorkerPool::new(processor, dlq, 2);
    //     
    //     tokio::spawn(async move {
    //         worker_pool.run(raw_rx, batch_tx).await;
    //     });
    //
    //     // Send 2 messages
    //     let telemetry = r#"{"time":"2023-10-01T12:00:00Z","device_id":"dev1","sequence_id":1,"temperature":25.0,"battery":100}"#;
    //     raw_tx.send(RawIngestMessage {
    //         topic: "users/u1/devices/d1/telemetry".into(),
    //         payload: telemetry.as_bytes().to_vec(),
    //     }).await.unwrap();
    //     
    //     raw_tx.send(RawIngestMessage {
    //         topic: "users/u1/devices/d2/telemetry".into(),
    //         payload: telemetry.as_bytes().to_vec(),
    //     }).await.unwrap();
    //
    //     // Should receive 2 processed messages in batch_rx
    //     let _msg1 = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv()).await.unwrap().unwrap();
    //     let _msg2 = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv()).await.unwrap().unwrap();
    //     
    //     assert_eq!(_msg1.telemetry.device_id, "dev1");
    // }

    // #[tokio::test]
    // async fn test_worker_pool_dlq() {
    //     let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(10);
    //     let (batch_tx, _batch_rx) = tokio::sync::mpsc::channel(10);
    //
    //     let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/db").unwrap();
    //     let config_manager = Arc::new(ConfigManager::new(pool));
    //     let storage = Arc::new(MockStorage);
    //     let broker = Arc::new(MockBroker);
    //     let processor = Arc::new(ServiceProcessor::new(storage, broker, config_manager));
    //     
    //     let mock_dlq = Arc::new(MockDlq::new());
    //     let worker_pool = WorkerPool::new(processor, mock_dlq.clone(), 1);
    //     
    //     tokio::spawn(async move {
    //         worker_pool.run(raw_rx, batch_tx).await;
    //     });
    //
    //     // Send INVALID JSON
    //     let invalid_payload = r#"{"invalid": "json"}"#;
    //     raw_tx.send(RawIngestMessage {
    //         topic: "users/u1/devices/d3/telemetry".into(),
    //         payload: invalid_payload.as_bytes().to_vec(),
    //     }).await.unwrap();
    //
    //     // Wait for processing
    //     tokio::time::sleep(Duration::from_millis(100)).await;
    //
    //     // Verify DLQ was called
    //     let calls = mock_dlq.calls.lock().unwrap();
    //     assert_eq!(calls.len(), 1);
    //     assert!(calls[0].contains("users/u1/devices/d3/telemetry"));
    //     assert!(calls[0].contains("missing field"));
    // }
}
