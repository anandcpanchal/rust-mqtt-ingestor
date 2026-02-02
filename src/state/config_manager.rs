use dashmap::DashMap;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tracing::info;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserConfig {
    pub temperature_max: f64,
}

impl Default for UserConfig {
    fn default() -> Self {
        Self { temperature_max: 80.0 } // Safe global default
    }
}

pub struct ConfigManager {
    pool: PgPool,
    cache: Arc<DashMap<String, UserConfig>>,
}

impl ConfigManager {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: Arc::new(DashMap::new()),
        }
    }

    /// Load all user configs from DB into Cache on startup
    pub async fn load_all(&self) -> anyhow::Result<()> {
        let rows = sqlx::query("SELECT user_id, temperature_max FROM user_configs")
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
            let user_id: String = row.get("user_id");
            let temp_max: f64 = row.get("temperature_max");
            
            self.cache.insert(user_id.clone(), UserConfig { temperature_max: temp_max });
        }
        
        info!("Loaded {} user configs into cache", self.cache.len());
        Ok(())
    }

    /// Get config for a user (Cache Hit -> DB Lookup -> Default)
    pub fn get_config(&self, user_id: &str) -> UserConfig {
        if let Some(config) = self.cache.get(user_id) {
            return config.clone();
        }
        // Fallback to default if not found (or we could do a just-in-time DB lookup here if not preloaded)
        UserConfig::default() 
    }

    /// Update config for a user (Write-Through: Cache + DB)
    pub async fn update_user_config(&self, user_id: &str, config: UserConfig) -> anyhow::Result<()> {
        // 1. Persist to DB
        sqlx::query(
            r#"
            INSERT INTO user_configs (user_id, temperature_max)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE 
            SET temperature_max = EXCLUDED.temperature_max
            "#
        )
        .bind(user_id)
        .bind(config.temperature_max)
        .execute(&self.pool)
        .await?;

        // 2. Update Cache
        self.cache.insert(user_id.to_string(), config);
        
        info!("Updated config for User {}: {:?}", user_id, self.get_config(user_id));

        Ok(())
    }
}
