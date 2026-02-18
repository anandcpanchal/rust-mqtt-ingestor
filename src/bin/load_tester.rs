use clap::Parser;
use rumqttc::v5::{AsyncClient, MqttOptions};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde::Serialize;
use std::time::Duration;
use tokio::time;
use rand::Rng;
use sqlx::postgres::PgPoolOptions;
use tracing::{info, info_span, instrument::Instrument};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// MQTT Broker Host
    #[arg(long, default_value = "localhost")]
    host: String,

    /// MQTT Broker Port
    #[arg(long, default_value_t = 1883)]
    port: u16,

    /// Number of simulated users
    #[arg(long, default_value_t = 1)]
    users: u32,

    /// Number of simulated devices per user
    #[arg(long, default_value_t = 1)]
    devices_per_user: u32,

    /// Messages per second (Total throughput target)
    #[arg(long, default_value_t = 10)]
    rate: u64,

    /// Duration of test in seconds (0 for infinite)
    #[arg(long, default_value_t = 60)]
    duration: u64,

    /// Database URL (defaults to adding user/pass if not present)
    #[arg(long, default_value = "postgres://postgres:password@localhost:5432/iot_db")]
    database_url: String,

    /// Superuser for load testing
    #[arg(long, default_value = "load_tester")]
    superuser: String,

    /// Superuser password
    #[arg(long, default_value = "load_tester_password")]
    superuser_password: String,

    /// OTLP Endpoint for Tracing
    #[arg(long, default_value = "http://localhost:4317")]
    otel_endpoint: String,
}

#[derive(Serialize, Clone)]
struct TelemetryPayload {
    time: String, // ISO 8601
    device_id: String,
    sequence_id: u64,
    temperature: f64,
    battery: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("Starting Load Tester with config: {:?}", args);


    // 0b. Connect to Database & Register Users
    println!("Connecting to Database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.database_url)
        .await?;

    // Register Superuser
    println!("Registering Superuser '{}'...", args.superuser);
    sqlx::query(
        "INSERT INTO mqtt_users (username, password_hash, is_superuser) VALUES ($1, $2, TRUE) ON CONFLICT (username) DO UPDATE SET password_hash = $2"
    )
    .bind(&args.superuser)
    .bind(&args.superuser_password)
    .execute(&pool)
    .await?;

    // Register Simulated Users
    println!("Registering {} simulated users...", args.users);
    for i in 1..=args.users {
        let username = format!("user_{}", i);
        sqlx::query(
            "INSERT INTO mqtt_users (username, password_hash, is_superuser) VALUES ($1, 'password', FALSE) ON CONFLICT (username) DO NOTHING"
        )
        .bind(&username)
        .execute(&pool)
        .await?;
    }
    println!("User registration complete.");

    // 1. Setup MQTT v5 Client
    let client_id = format!("load_tester_{}", uuid::Uuid::new_v4());
    let mut mqttoptions = MqttOptions::new(client_id, &args.host, args.port);
    mqttoptions.set_credentials(&args.superuser, &args.superuser_password);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_start(true);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100);

    // Spawn Event Loop
    tokio::spawn(async move {
        while let Ok(_) = eventloop.poll().await { }
    });

    // 2. Load Generation Loop
    let start_time = std::time::Instant::now();
    let interval_duration = Duration::from_micros(1_000_000 / args.rate);
    let mut interval = time::interval(interval_duration);
    
    let mut total_sent = 0;

    loop {
        interval.tick().await;

        if args.duration > 0 && start_time.elapsed().as_secs() >= args.duration {
            println!("Configuration duration {:?} elapsed. Stopping.", args.duration);
            break;
        }

        let total_devices = args.users * args.devices_per_user;
        let index = total_sent % total_devices as u64;
        
        let user_idx = (index / args.devices_per_user as u64) + 1;
        let device_idx = (index % args.devices_per_user as u64) + 1;

        let user_id = format!("user_{}", user_idx);
        let device_id = format!("device_{:03}", device_idx);

        // Generate Payload
        let mut rng = rand::thread_rng();
        let payload = TelemetryPayload {
            time: ::time::OffsetDateTime::now_utc().format(&::time::format_description::well_known::Rfc3339).unwrap(),
            device_id: device_id.clone(),
            sequence_id: (total_sent / total_devices as u64) + 1,
            temperature: rng.gen_range(20.0..45.0),
            battery: rng.gen_range(10..100),
        };

        let topic = format!("users/{}/devices/{}/telemetry", user_id, device_id);
        let payload_json = serde_json::to_vec(&payload)?;

        let payload_json = serde_json::to_vec(&payload)?;
        if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, false, payload_json).await {
            eprintln!("Failed to publish: {:?}", e);
        }

        total_sent += 1;
        if total_sent % args.rate == 0 {
            println!("Sent {} messages...", total_sent);
        }
    }

    println!("Load Test Complete. Total messages sent: {}", total_sent);
    Ok(())
}

