use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::log::{info, error};
use crate::service::processor::{ServiceProcessor, IngestMessage};

#[derive(Debug)]
pub struct RawIngestMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

pub struct WorkerPool {
    processor: Arc<ServiceProcessor>,
    concurrency: usize,
}

impl WorkerPool {
    pub fn new(processor: Arc<ServiceProcessor>, concurrency: usize) -> Self {
        Self { processor, concurrency }
    }

    pub async fn run(self, mut receiver: Receiver<RawIngestMessage>, batch_sender: Sender<IngestMessage>) {
        info!("WorkerPool starting with {} workers", self.concurrency);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        let active_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        while let Some(msg) = receiver.recv().await {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    error!("Semaphore closed");
                    break;
                }
            };
            
            let processor = self.processor.clone();
            let batch_sender = batch_sender.clone();
            let active_count = active_count.clone();

            tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completion
                
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
                        };
                        if let Err(e) = batch_sender.send(ingest_msg).await {
                            error!("Failed to send to batch executor: {}", e);
                            metrics::counter!("worker_errors_total", 1, "type" => "channel_closed");
                        }
                    },
                    Ok(None) => {}, // Filtered
                    Err(e) => {
                        error!("Processing error in WorkerPool: {:?}", e);
                        metrics::counter!("worker_errors_total", 1, "type" => "processing_error");
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
    }

    struct MockBroker;
    #[async_trait]
    impl MessageBroker for MockBroker {
        async fn publish(&self, _topic: &str, _payload: Vec<u8>) -> anyhow::Result<()> { Ok(()) }
    }

    #[tokio::test]
    async fn test_worker_pool_concurrency() {
        let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(10);
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(10);

        let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/db").unwrap();
        let config_manager = Arc::new(ConfigManager::new(pool));
        let storage = Arc::new(MockStorage);
        let broker = Arc::new(MockBroker);
        let processor = Arc::new(ServiceProcessor::new(storage, broker, config_manager));

        let worker_pool = WorkerPool::new(processor, 2);
        
        tokio::spawn(async move {
            worker_pool.run(raw_rx, batch_tx).await;
        });

        // Send 2 messages
        let telemetry = r#"{"time":"2023-10-01T12:00:00Z","device_id":"dev1","sequence_id":1,"temperature":25.0,"battery":100}"#;
        raw_tx.send(RawIngestMessage {
            topic: "users/u1/devices/d1/telemetry".into(),
            payload: telemetry.as_bytes().to_vec(),
        }).await.unwrap();
        
        raw_tx.send(RawIngestMessage {
            topic: "users/u1/devices/d2/telemetry".into(),
            payload: telemetry.as_bytes().to_vec(),
        }).await.unwrap();

        // Should receive 2 processed messages in batch_rx
        let _msg1 = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv()).await.unwrap().unwrap();
        let _msg2 = tokio::time::timeout(Duration::from_secs(1), batch_rx.recv()).await.unwrap().unwrap();
        
        assert_eq!(_msg1.telemetry.device_id, "dev1");
    }
}
