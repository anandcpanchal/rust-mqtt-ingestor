mod config;
mod domain;
mod ports;
mod adapters;
mod service;
mod state; // Added state module

use crate::config::AppConfig;
use crate::service::processor::{ServiceProcessor, run_ingest_loop, run_batch_executor};
use crate::adapters::{TimescaleRepository, MqttAdapter};
use crate::state::config_manager::ConfigManager;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 0. Load Env Vars First
    dotenvy::dotenv().ok();

    // 1. Initialize Structured Logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting IoT MQTT Backend POC (Scalable Architecture)...");

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

    // ADDED: Initialize Config Manager
    let config_manager = Arc::new(ConfigManager::new(pool.clone()));
    if let Err(e) = config_manager.load_all().await {
        error!("Failed to load user configs: {:?}", e);
        std::process::exit(1);
    }

    // 4. Initialize Adapters
    // Use Arc<TimescaleRepository> directly so we can clone it for both Processor and Executor
    let storage_repo = Arc::new(TimescaleRepository::new(pool));
    
    // MQTT Setup
    let (_mqtt_options, mqtt_client, eventloop) = MqttAdapter::build(&config);
    let mqtt_adapter = Arc::new(MqttAdapter::new(mqtt_client.clone()));

    // 5. Initialize Service Processor (Ingest Logic)
    let processor = Arc::new(ServiceProcessor::new(storage_repo.clone(), mqtt_adapter, config_manager));

    // 6. Setup Scalable Pipeline
    let (tx, rx) = tokio::sync::mpsc::channel(10000);

    // 6a. Start Batch Executor (Consumer) in Background Task
    let storage_for_executor = storage_repo.clone();
    let client_for_executor = mqtt_client.clone();
    
    let executor_handle = tokio::spawn(async move {
        run_batch_executor(rx, storage_for_executor, client_for_executor).await;
    });

    // 6b. Start Ingest Loop (Producer)
    // Create Shutdown Signal
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    
    let ingest_handle = tokio::spawn(async move {
        if let Err(e) = run_ingest_loop(eventloop, mqtt_client, processor, &config.telemetry_topic, tx, shutdown_rx).await {
             // If manual break
             error!("Ingest Loop Error: {:?}", e);
        }
    });

    info!("System Running. Press Ctrl+C to stop gracefully.");

    // 7. Wait for Signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Received Shutdown Signal. signaling Ingest Loop...");
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }

    // 8. Shutdown Sequence
    // Signal Ingest to stop accepting new messages but keep EventLoop running
    let _ = shutdown_tx.send(true);
    
    info!("Signal sent. Waiting for Batch Executor to drain queue...");
    
    // Wait for Executor to finish (which happens when Ingest drops the sender)
    let timeout = std::time::Duration::from_secs(20);
    match tokio::time::timeout(timeout, executor_handle).await {
        Ok(_) => info!("Batch Executor flushed all data."),
        Err(_) => error!("Timeout waiting for Executor to flush. Forced Exit."),
    }
    
    // Now that Executor is done (all Acks sent to Client), we can safe kill the EventLoop
    ingest_handle.abort();
    info!("EventLoop stopped. Shutdown Complete.");

    Ok(())
}
