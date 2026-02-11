use async_trait::async_trait;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use crate::ports::MessageBroker;
use tracing::instrument;
use std::time::Duration;
use crate::config::AppConfig;

pub struct MqttAdapter {
    client: AsyncClient,
}

impl MqttAdapter {
    pub fn new(client: AsyncClient) -> Self {
        Self { client }
    }

    /// Helper to build options and spawn the event loop (which we pass to the main processor later)
    pub fn build(config: &AppConfig) -> (MqttOptions, AsyncClient, rumqttc::EventLoop) {
        // Stable Client ID for Persistent Sessions (Avoids Zombies)
        let client_id = format!("{}_{}", config.mqtt_client_id_prefix, config.instance_id);
        
        let mut mqttoptions = MqttOptions::new(
            client_id,
            &config.mqtt_host,
            config.mqtt_port,
        );
        mqttoptions.set_credentials(&config.mqtt_username, &config.mqtt_password);
        mqttoptions.set_keep_alive(Duration::from_secs(30));
        
        // Reliability settings for QoS 1
        mqttoptions.set_clean_session(false); // Persistent Session: Broker queues msgs while we restart
        mqttoptions.set_manual_acks(true); // Disable Auto-Ack as requested

        // Increase Inflight window to match Batch Size (1000)
        // This is CRITICAL for high throughput, otherwise client blocks after 10 messages
        let (client, eventloop) = AsyncClient::new(mqttoptions, 1000);
        (MqttOptions::new("placeholder", "host", 1883), client, eventloop) // returning dummy options just for signature match if needed, but really we just need client+loop
    }
}

#[async_trait]
impl MessageBroker for MqttAdapter {
    #[instrument(skip(self, payload), fields(topic = %topic, payload_size = payload.len()))]
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| anyhow::anyhow!("MQTT Publish Failed: {:?}", e))?;
        
        // info!("Published message to {}", topic); // Optional: verbose logging
        Ok(())
    }
}
