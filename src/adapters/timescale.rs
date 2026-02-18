use async_trait::async_trait;
use sqlx::{PgPool, QueryBuilder, Postgres, postgres::PgQueryResult};
use crate::domain::{Telemetry, Alert};
use crate::ports::StorageRepository;
use tracing::{info, warn, instrument};
use metrics::{counter, gauge};
use failsafe::futures::CircuitBreaker;
use failsafe::{Config, StateMachine, backoff, failure_policy};

struct DbCircuitBreakerInstrument;

impl failsafe::Instrument for DbCircuitBreakerInstrument {
    fn on_call_rejected(&self) {
        metrics::counter!("db_circuit_breaker_rejected_total", 1);
    }

    fn on_open(&self) {
        warn!("Database Circuit Breaker OPENED");
        metrics::gauge!("db_circuit_breaker_state", 1.0);
    }

    fn on_half_open(&self) {
        info!("Database Circuit Breaker HALF-OPEN");
        metrics::gauge!("db_circuit_breaker_state", 0.5);
    }

    fn on_closed(&self) {
        info!("Database Circuit Breaker CLOSED");
        metrics::gauge!("db_circuit_breaker_state", 0.0);
    }
}

pub struct TimescaleRepository {
    pool: PgPool,
    cb: StateMachine<failure_policy::ConsecutiveFailures<backoff::Exponential>, DbCircuitBreakerInstrument>,
}

impl TimescaleRepository {
    pub fn new(pool: PgPool) -> Self {
        let backoff = backoff::exponential(std::time::Duration::from_secs(5), std::time::Duration::from_secs(60));
        let policy = failure_policy::consecutive_failures(5, backoff);
        
        let cb = Config::new()
            .failure_policy(policy)
            .instrument(DbCircuitBreakerInstrument)
            .build();
            
        Self { pool, cb }
    }
}

#[async_trait]
impl StorageRepository for TimescaleRepository {
    #[instrument(
        skip(self, data), 
        fields(
            device_id = %data.device_id, 
            sequence_id = %data.sequence_id,
            db.system = "postgresql",
            db.name = "iot_db",
            otel.kind = "client"
        )
    )]
    async fn store_telemetry(&self, data: &Telemetry) -> anyhow::Result<()> {
        let res = self.cb.call(async move {
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
            
            Ok::<PgQueryResult, anyhow::Error>(result)
        }).await;

        match res {
            Ok(result) => {
                if result.rows_affected() == 0 {
                    warn!("Telemetry duplicate detected (deduplicated by DB)");
                } else {
                    info!("Telemetry stored successfully");
                }
                Ok(())
            },
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Telemetry rejected")),
        }
    }

    #[instrument(
        skip(self, alert), 
        fields(
            device_id = %alert.device_id, 
            alert_type = %alert.alert_type,
            db.system = "postgresql",
            db.name = "iot_db",
            otel.kind = "client"
        )
    )]
    async fn store_alert(&self, alert: &Alert) -> anyhow::Result<()> {
        let res = self.cb.call(async move {
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
            Ok::<(), anyhow::Error>(())
        }).await;

        match res {
            Ok(_) => {
                info!("Alert stored successfully");
                Ok(())
            },
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Alert rejected")),
        }
    }

    async fn upsert_active_alert(&self, alert: &crate::domain::ActiveAlert, message: &str) -> anyhow::Result<Option<Alert>> {
        let res = self.cb.call(async move {
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
                        false
                    } else {
                        false
                    }
                },
                None => true
            };

            if should_publish {
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

                Ok::<Option<Alert>, anyhow::Error>(Some(Alert::new(
                    alert.device_id.clone(),
                    alert.user_id.clone(),
                    alert.rule_id.clone(),
                    format!("Rule:{}", alert.rule_id),
                    message.to_string(),
                    alert.current_value,
                    "Triggered".to_string()
                )))
            } else {
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
                Ok::<Option<Alert>, anyhow::Error>(None)
            }
        }).await;

        match res {
            Ok(opt) => Ok(opt),
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Alert Upsert rejected")),
        }
    }

    async fn resolve_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str) -> anyhow::Result<Option<Alert>> {
        let res = self.cb.call(async move {
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
                    
                    return Ok::<Option<Alert>, anyhow::Error>(Some(Alert::new(
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
            Ok::<Option<Alert>, anyhow::Error>(None)
        }).await;

        match res {
            Ok(opt) => Ok(opt),
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Alert Resolve rejected")),
        }
    }

    async fn snooze_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str, duration_minutes: i64) -> anyhow::Result<()> {
        let snooze_until = time::OffsetDateTime::now_utc() + time::Duration::minutes(duration_minutes);
        
        let res = self.cb.call(async move {
            sqlx::query("UPDATE active_alerts SET status='Snoozed', snooze_until=$1 WHERE user_id=$2 AND device_id=$3 AND rule_id=$4")
                .bind(snooze_until)
                .bind(user_id)
                .bind(device_id)
                .bind(rule_id)
                .execute(&self.pool)
                .await?;
                
            Ok::<(), anyhow::Error>(())
        }).await;

        match res {
            Ok(_) => Ok(()),
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Alert Snooze rejected")),
        }
    }

    async fn disable_active_alert(&self, user_id: &str, device_id: &str, rule_id: &str) -> anyhow::Result<()> {
        let res = self.cb.call(async move {
            sqlx::query("UPDATE active_alerts SET status='Disabled', snooze_until=NULL WHERE user_id=$1 AND device_id=$2 AND rule_id=$3")
                .bind(user_id)
                .bind(device_id)
                .bind(rule_id)
                .execute(&self.pool)
                .await?;
            Ok::<(), anyhow::Error>(())
        }).await;

        match res {
            Ok(_) => Ok(()),
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Alert Disable rejected")),
        }
    }

    #[instrument(
        skip(self, batch),
        fields(
            db.system = "postgresql",
            db.name = "iot_db",
            otel.kind = "client"
        )
    )]
    async fn store_telemetry_batch(&self, batch: &[Telemetry]) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let res = self.cb.call(async move {
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
            Ok::<PgQueryResult, anyhow::Error>(result)
        }).await;

        match res {
            Ok(result) => {
                info!("Batch stored successfully. Rows affected: {}", result.rows_affected());
                Ok(())
            },
            Err(failsafe::Error::Inner(e)) => Err(e),
            Err(failsafe::Error::Rejected) => Err(anyhow::anyhow!("Circuit Breaker Open: Batch rejected")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn test_circuit_breaker_open_after_failures() {
        // Use a lazy pool pointing to a non-existent DB
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://localhost:1234/non_existent")
            .unwrap();
            
        let repo = TimescaleRepository::new(pool);
        
        let telemetry = Telemetry {
            time: time::OffsetDateTime::now_utc(),
            user_id: "test_user".to_string(),
            device_id: "test_device".to_string(),
            sequence_id: 1,
            temperature: Some(25.0),
            battery: Some(85),
            extra: std::collections::HashMap::new(),
        };

        // First 5 calls should fail with "Inner" error (connection failure)
        for i in 1..=5 {
            let res = repo.store_telemetry(&telemetry).await;
            match res {
                Err(e) if e.to_string().contains("Circuit Breaker Open") => {
                    panic!("Circuit breaker opened too early at call {}", i);
                }
                Err(_) => {
                    // This is expected: connection failure
                }
                Ok(_) => {
                    panic!("Call {} unexpectedly succeeded", i);
                }
            }
        }

        // The 6th call should be rejected by the circuit breaker immediately
        let res = repo.store_telemetry(&telemetry).await;
        match res {
            Err(e) if e.to_string().contains("Circuit Breaker Open") => {
                // Success! Circuit breaker is open.
            }
            res => {
                panic!("Expected Circuit Breaker Open error, got: {:?}", res);
            }
        }
    }
}
