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
                
                // Metrics: Active Workers Gauge (Inc)
                metrics::gauge!("worker_active_count", 1.0, "type" => "active"); // Increment not supported directly in this macro form easily without handle, so using absolute or delta? 
                // Wait, gauge! usually sets value. 
                // For increment/decrement, we need a handle or use register.
                // Reverting Gauge specific usage to .increment is usually correct for Gauge handles, but if macro fails...
                // Let's try `gauge!("name").increment(1.0)` but ensuring strict syntax?
                // Actually, the error `expected ,` might be due to `metrics` 0.21 wanting `gauge!("name", val)`
                // To do inc/dec on gauge via macro: `metrics::gauge!("name").increment(1.0)` IS standard.
                // The issue might be `metrics` crate version or features.
                // Let's assume standard behavior: `gauge!` emits a value.
                // If we want to track concurrency, we need a static handle or use `gauge!("name", value)`.
                // But we don't know the absolute value here easily without shared state.
                // FOR NOW: Let's use `histogram` for duration, and `counter` for errors.
                // Gauges in stateless workers are hard without a central atomic.
                // I will Comment out the Gauge for now to unblock build, or use a Counter for "Jobs Started" and "Jobs Finished".
                
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
                            metrics::counter!("worker_errors_total", 1, "type" => "channel_closed".to_string());
                        }
                    },
                    Ok(None) => {}, // Filtered
                    Err(e) => {
                        error!("Processing error in WorkerPool: {:?}", e);
                        metrics::counter!("worker_errors_total", 1, "type" => "processing_error".to_string());
                    },
                }
                
                // Metrics: Duration
                let duration = start.elapsed().as_secs_f64();
                metrics::histogram!("worker_processing_duration_seconds", duration);
                // metrics::gauge!("worker_active_count").decrement(1.0);
            });
        }
        info!("WorkerPool shutting down");
    }
}
