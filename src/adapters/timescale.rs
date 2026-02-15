use async_trait::async_trait;
use sqlx::{PgPool, QueryBuilder, Postgres};
use crate::domain::{Telemetry, Alert};
use crate::ports::StorageRepository;
use tracing::{info, warn, instrument};

pub struct TimescaleRepository {
    pool: PgPool,
}

impl TimescaleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StorageRepository for TimescaleRepository {
    #[instrument(skip(self, data), fields(device_id = %data.device_id, sequence_id = %data.sequence_id))]
    async fn store_telemetry(&self, data: &Telemetry) -> anyhow::Result<()> {
        // Insert with ON CONFLICT DO NOTHING for deduplication.
        // We rely on the composite PK (time, user_id, device_id, sequence_id) in the DB.
        
        let query = r#"
            INSERT INTO raw_telemetry (time, user_id, device_id, sequence_id, temperature, battery, extra_data)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (time, user_id, device_id, sequence_id) DO NOTHING
        "#;

        let result = sqlx::query(query)
            .bind(data.time)
            .bind(&data.user_id)
            .bind(&data.device_id)
            .bind(data.sequence_id)
            .bind(data.temperature)
            .bind(data.battery)
            .bind(serde_json::to_value(&data.extra).unwrap_or(serde_json::Value::Null))
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            warn!("Telemetry duplicate detected (deduplicated by DB)");
        } else {
            info!("Telemetry stored successfully");
        }

        Ok(())
    }

    #[instrument(skip(self, alert), fields(device_id = %alert.device_id, alert_type = %alert.alert_type))]
    async fn store_alert(&self, alert: &Alert) -> anyhow::Result<()> {
        let query = r#"
            INSERT INTO alerts_history (created_at, device_id, user_id, alert_type, message, value)
            VALUES ($1, $2, $3, $4, $5, $6)
        "#;

        sqlx::query(query)
            .bind(alert.created_at)
            .bind(&alert.device_id)
            .bind(&alert.user_id)
            .bind(&alert.alert_type)
            .bind(&alert.message)
            .bind(alert.value)
            .execute(&self.pool)
            .await?;

        info!("Alert stored successfully");
        Ok(())
    }

    #[instrument(skip(self, batch))]
    async fn store_telemetry_batch(&self, batch: &[Telemetry]) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO raw_telemetry (time, user_id, device_id, sequence_id, temperature, battery, extra_data) "
        );

        query_builder.push_values(batch, |mut b, data| {
            b.push_bind(data.time)
             .push_bind(&data.user_id)
             .push_bind(&data.device_id)
             .push_bind(data.sequence_id)
             .push_bind(data.temperature)
             .push_bind(data.battery)
             .push_bind(serde_json::to_value(&data.extra).unwrap_or(serde_json::Value::Null));
        });

        query_builder.push(" ON CONFLICT (time, user_id, device_id, sequence_id) DO NOTHING");

        let result = query_builder.build().execute(&self.pool).await?;
        
        info!("Batch stored successfully. Rows affected: {}", result.rows_affected());
        Ok(())
    }
}
