use clap::Parser;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::Serialize;
use std::time::Duration;
use tokio::time;
use rand::Rng;

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
}

#[derive(Serialize)]
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

    // 1. Setup MQTT Client
    let client_id = format!("load_tester_{}", uuid::Uuid::new_v4());
    let mut mqttoptions = MqttOptions::new(client_id, &args.host, args.port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100);

    // Spawn Event Loop in background to handle network traffic
    tokio::spawn(async move {
        while let Ok(_) = eventloop.poll().await {
            // Just drain the event loop
        }
    });

    // 2. Load Generation Loop
    let start_time = std::time::Instant::now();
    let interval_duration = Duration::from_micros(1_000_000 / args.rate);
    let mut interval = time::interval(interval_duration);
    
    // Counter for distribution
    let mut total_sent = 0;
    
    // Simulate until duration expires
    loop {
        interval.tick().await;

        if args.duration > 0 && start_time.elapsed().as_secs() >= args.duration {
            println!("Configuration duration {:?} elapsed. Stopping.", args.duration);
            break;
        }

        // Round-robin selection of user and device
        let total_devices = args.users * args.devices_per_user;
        let index = total_sent % total_devices as u64;
        
        let user_idx = (index / args.devices_per_user as u64) + 1; // 1-based
        let device_idx = (index % args.devices_per_user as u64) + 1; // 1-based

        let user_id = format!("user_{}", user_idx);
        let device_id = format!("device_{:03}", device_idx);

        // Generate Payload
        let mut rng = rand::thread_rng();
        let payload = TelemetryPayload {
            time: ::time::OffsetDateTime::now_utc().format(&::time::format_description::well_known::Rfc3339).unwrap(),
            device_id: device_id.clone(),
            sequence_id: (total_sent / total_devices as u64) + 1, // Sequence per device
            temperature: rng.gen_range(20.0..45.0), // Random temp 20-45 C
            battery: rng.gen_range(10..100),
        };

        let topic = format!("users/{}/devices/{}/telemetry", user_id, device_id);
        let payload_json = serde_json::to_vec(&payload)?;

        // Publish
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
