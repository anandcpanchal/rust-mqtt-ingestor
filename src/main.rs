mod config;
mod domain;
mod ports;
mod adapters;
mod service;
mod state; 

use crate::config::AppConfig;
use crate::service::processor::{ServiceProcessor, run_mqtt_maintenance_loop, run_batch_executor};
use crate::adapters::{TimescaleRepository, MqttAdapter, KafkaAdapter};
use crate::state::config_manager::ConfigManager;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::{info, error, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 0. Load Env Vars First
    dotenvy::dotenv().ok();

    // 1. Initialize Structured Logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting IoT Rust Backend (Kafka Consumer Mode)...");

    // 1b. Initialize Metrics
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], 9000))
        .idle_timeout(
            metrics_util::MetricKindMask::ALL,
            Some(std::time::Duration::from_secs(60)),
        )
        .install()
        .expect("failed to install Prometheus recorder");
    info!("Prometheus Metrics listening on 0.0.0.0:9000");

    // 2. Load Configuration
    let config = match AppConfig::load() {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load configuration: {:?}", e);
            std::process::exit(1);
        }
    };
    info!("Configuration loaded.");

    // 3. Initialize Database Pool
    info!("Connecting to Database...");
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect(&config.database_url)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to DB: {:?}", e))?;
    info!("Database connection established.");

    // 4. Initialize Config Manager
    let config_manager = Arc::new(ConfigManager::new(pool.clone()));
    if let Err(e) = config_manager.load_all().await {
        error!("Failed to load user configs: {:?}", e);
        std::process::exit(1);
    }

    // 5. Initialize Adapters
    let storage_repo = Arc::new(TimescaleRepository::new(pool));
    
    // 5a. MQTT Setup (For Alerts & Configs ONLY)
    let (_mqtt_options, mqtt_client, eventloop) = MqttAdapter::build(&config);
    let mqtt_adapter = Arc::new(MqttAdapter::new(mqtt_client.clone()));

    // 5b. Kafka Setup (For Telemetry Ingest)
    info!("Initializing Kafka Consumer (Brokers: {}, Topic: {})...", config.kafka_brokers, config.kafka_topic);
    let kafka_adapter = Arc::new(KafkaAdapter::new(
        &config.kafka_brokers,
        &config.kafka_group,
        &config.kafka_topic
    ));

    // 6. Initialize Service Processor
    let processor = Arc::new(ServiceProcessor::new(storage_repo.clone(), mqtt_adapter, config_manager));

    // 7. Setup Pipeline Channels
    // Channel 1: Worker -> Batch Executor (IngestMessage)
    let (tx, rx) = tokio::sync::mpsc::channel(10000);
    
    // Channel 2: Kafka -> Worker (RawIngestMessage)
    let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(10000);

    // 8. Start Background Tasks

    // 4b. Initialize Batch Executor (Consumer of Internal Channel -> DB)
    // Re-using storage_repo and mqtt_client clones
    let storage_for_executor = storage_repo.clone();
    let _client_for_executor = mqtt_client.clone();
    
    let executor_handle = tokio::spawn(async move {
        // Instrument Batch Executor task
        run_batch_executor(rx, storage_for_executor, _client_for_executor).await;
    });

    // 8b. Worker Pool
    let worker_pool = crate::service::worker_pool::WorkerPool::new(processor.clone(), 4);
    let worker_handle = tokio::spawn(async move {
        worker_pool.run(raw_rx, tx).await;
    });

    // 8c. Kafka Ingest Loop (Producer to Worker Pool)
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    
    let kafka_handle = tokio::spawn(async move {
        if let Err(e) = kafka_adapter.run_loop(raw_tx, shutdown_rx).await {
            error!("Kafka Ingest Loop Error: {:?}", e);
        }
    });

    // 8c. MQTT Maintenance Loop (Config Updates & Alert Publishing)
    let processor_for_mqtt = processor.clone();
    let mqtt_handle = tokio::spawn(async move {
        if let Err(e) = run_mqtt_maintenance_loop(eventloop, mqtt_client, processor_for_mqtt).await {
            error!("MQTT Loop Error: {:?}", e);
        }
    });

    info!("System Running. Reading from Kafka -> DB. Press Ctrl+C to stop.");

    // 9. Shutdown Signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("Shutdown Signal Received..."),
        Err(err) => error!("Unable to listen for shutdown signal: {}", err),
    }

    // 10. Graceful Shutdown
    // Signal Kafka Loop to stop
    let _ = shutdown_tx.send(true);
    
    info!("Waiting for Batch Executor to flush...");
    // Wait for Kafka loop to exit (it waits for shutdown signal)
    let _ = tokio::join!(kafka_handle); 
    
    info!("Waiting for Worker Pool to stop...");
    let _ = tokio::join!(worker_handle);
    // Logic: Kafka loop exits -> drops `raw_tx` -> Worker exits -> drops `tx` -> Executor drains `rx` -> Executor exits.

    let timeout = std::time::Duration::from_secs(20);
    match tokio::time::timeout(timeout, executor_handle).await {
        Ok(_) => info!("Batch Executor flushed."),
        Err(_) => warn!("Timeout waiting for Executor."),
    }

    mqtt_handle.abort();
    info!("Shutdown Complete.");

    Ok(())
}
