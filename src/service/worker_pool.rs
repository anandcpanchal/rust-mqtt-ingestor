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

            tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completion
                
                match processor.process_ingest_logic(&msg.topic, &msg.payload).await {
                    Ok(Some(telemetry)) => {
                        let ingest_msg = IngestMessage {
                            telemetry,
                            packet: None,
                        };
                        if let Err(e) = batch_sender.send(ingest_msg).await {
                            error!("Failed to send to batch executor: {}", e);
                        }
                    },
                    Ok(None) => {}, // Filtered
                    Err(e) => error!("Processing error in WorkerPool: {:?}", e),
                }
            });
        }
        info!("WorkerPool shutting down");
    }
}
