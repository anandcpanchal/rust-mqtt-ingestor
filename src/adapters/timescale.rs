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
            INSERT INTO alerts_history (created_at, device_id, user_id, alert_type, message, value, rule_id, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;

        sqlx::query(query)
            .bind(alert.created_at)
            .bind(&alert.device_id)
            .bind(&alert.user_id)
            .bind(&alert.alert_type)
            .bind(&alert.message)
            .bind(alert.value)
            .bind(&alert.rule_id)
            .bind(&alert.status)
            .execute(&self.pool)
            .await?;

        info!("Alert stored successfully");
        Ok(())
    }

    async fn upsert_active_alert(&self, alert: &crate::domain::ActiveAlert, message: &str) -> anyhow::Result<Option<Alert>> {
        // Logic: 
        // 1. Try to fetch existing state
        // 2. If new -> Insert Active -> Return Alert
        // 3. If exists:
        //    - If Snoozed & Expired -> Update Active -> Return Alert
        //    - Else -> Update last_seen/value -> Return None

        let existing = sqlx::query_as::<_, (String, Option<time::OffsetDateTime>)>("SELECT status, snooze_until FROM active_alerts WHERE user_id=$1 AND device_id=$2 AND rule_id=$3")
            .bind(&alert.user_id)
            .bind(&alert.device_id)
            .bind(&alert.rule_id)
            .fetch_optional(&self.pool)
            .await?;

        let should_publish = match existing {
            Some((status, snooze_until)) => {
                if status == "Snoozed" {
                    if let Some(until) = snooze_until {
                        until < time::OffsetDateTime::now_utc()
                    } else {
                        false
                    }
                } else if status == "Active" {
                    // Already active, just updating heartbeat
                    false
                } else {
                    // Disabled
                    false
                }
            },
            None => true // New alert
        };

        if should_publish {
            // Upsert setting status to Active (force reactivation if expired)
            let query = r#"
                INSERT INTO active_alerts (user_id, device_id, rule_id, start_time, last_seen, status, current_value)
                VALUES ($1, $2, $3, $4, $5, 'Active', $6)
                ON CONFLICT (user_id, device_id, rule_id)
                DO UPDATE SET
                    status = 'Active',
                    snooze_until = NULL,
                    last_seen = EXCLUDED.last_seen,
                    current_value = EXCLUDED.current_value
            "#;
            sqlx::query(query)
                .bind(&alert.user_id)
                .bind(&alert.device_id)
                .bind(&alert.rule_id)
                .bind(alert.start_time)
                .bind(alert.last_seen)
                .bind(alert.current_value)
                .execute(&self.pool)
                .await?;

            Ok(Some(Alert::new(
                alert.device_id.clone(),
                alert.user_id.clone(),
                alert.rule_id.clone(),
                format!("Rule:{}", alert.rule_id),
                message.to_string(),
                alert.current_value,
                "Triggered".to_string()
            )))
        } else {
            // Just update last_seen and value, preserving status
            let query = r#"
                UPDATE active_alerts 
                SET last_seen = $1, current_value = $2 
                WHERE user_id = $3 AND device_id = $4 AND rule_id = $5
            "#;
            sqlx::query(query)
                .bind(alert.last_seen)
                .bind(alert.current_value)
                .bind(&alert.user_id)
                .bind(&alert.device_id)
                .bind(&alert.rule_id)
                .execute(&self.pool)
                .await?;
            Ok(None)
        }
    }

    async fn resolve_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str) -> anyhow::Result<Option<Alert>> {
        // Only return Alert (Resolved) if we actually deleted an Active or Snoozed alert.
        // If it didn't exist or was Disabled, do nothing.
        
        let existing = sqlx::query_as::<_, (String,)>("SELECT status FROM active_alerts WHERE user_id=$1 AND device_id=$2 AND rule_id=$3")
            .bind(user_id)
            .bind(device_id)
            .bind(rule_id)
            .fetch_optional(&self.pool)
            .await?;

        if let Some((status,)) = existing {
            if status == "Active" || status == "Snoozed" {
                sqlx::query("DELETE FROM active_alerts WHERE user_id=$1 AND device_id=$2 AND rule_id=$3")
                    .bind(user_id)
                    .bind(device_id)
                    .bind(rule_id)
                    .execute(&self.pool)
                    .await?;
                
                return Ok(Some(Alert::new(
                    device_id.to_string(),
                    user_id.to_string(),
                    rule_id.to_string(),
                    format!("Rule:{}", rule_id),
                    "Alert Resolved".to_string(),
                    None,
                    "Resolved".to_string()
                )));
            }
        }
        Ok(None)
    }

    async fn snooze_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str, duration_minutes: i64) -> anyhow::Result<()> {
        let snooze_until = time::OffsetDateTime::now_utc() + time::Duration::minutes(duration_minutes);
        
        // Only snooze if currently Active. (If already snoozed, extend? Yes. If New/Disabled? Maybe ignore for now or upsert placeholder?)
        // Let's assume we only snooze existing Active/Snoozed alerts.
        sqlx::query("UPDATE active_alerts SET status='Snoozed', snooze_until=$1 WHERE user_id=$2 AND device_id=$3 AND rule_id=$4")
            .bind(snooze_until)
            .bind(user_id)
            .bind(device_id)
            .bind(rule_id)
            .execute(&self.pool)
            .await?;
            
        Ok(())
    }

    async fn disable_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str) -> anyhow::Result<()> {
        sqlx::query("UPDATE active_alerts SET status='Disabled', snooze_until=NULL WHERE user_id=$1 AND device_id=$2 AND rule_id=$3")
            .bind(user_id)
            .bind(device_id)
            .bind(rule_id)
            .execute(&self.pool)
            .await?;
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
