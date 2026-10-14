use serde::Serialize;
use std::sync::Arc;

/// Health status for a single backend component.
#[derive(Debug, Clone, Serialize)]
pub struct BackendHealth {
    pub name: String,
    pub status: String, // "up" | "down" | "degraded" | "unknown"
    pub latency_ms: Option<u64>,
    pub last_checked: Option<String>,
    pub error: Option<String>,
}

/// Complete health matrix for all infrastructure backends.
#[derive(Debug, Clone, Serialize)]
pub struct BackendHealthMatrix {
    pub overall: String,
    pub backends: Vec<BackendHealth>,
}

/// Probes infrastructure backend health directly.
#[derive(Clone)]
pub struct BackendHealthProbe {
    /// The shared key-value face this deployment was given, if any. `None` is
    /// "this deployment has none" rather than "the one it has is down": the two
    /// have different answers, and only the second is a fault.
    pub key_value: Option<Arc<dyn cog_core::KeyValueStore>>,
    pub pg_pool: Option<sqlx::PgPool>,
    pub nats_url: Option<String>,
    pub memory_backend: Option<Arc<dyn cog_core::MemoryBackend>>,
}

/// The key whose time-to-live the probe reads. Any key will do — what is being
/// asked is whether the store answers at all, and a read of a key that does not
/// exist (which is the normal answer here) is exactly as much of an answer as a
/// read of one that does, while writing a probe key would put a row in the store
/// that nothing else knows about.
const PROBE_KEY: &str = "cogneva:backend-health:probe";

impl BackendHealthProbe {
    pub async fn probe(&self) -> BackendHealthMatrix {
        let mut backends = Vec::new();
        let mut overall = "up".to_string();

        let redis_start = std::time::Instant::now();
        let redis_status = match self.key_value.as_ref() {
            Some(store) => match store.ttl(PROBE_KEY).await {
                Ok(_) => "up".to_string(),
                Err(e) => {
                    overall = "degraded".to_string();
                    format!("down: {}", e)
                }
            },
            None => "unknown".to_string(),
        };
        backends.push(BackendHealth {
            name: "redis".to_string(),
            status: redis_status.clone(),
            latency_ms: Some(redis_start.elapsed().as_millis() as u64),
            last_checked: Some(chrono::Utc::now().to_rfc3339()),
            // "unknown" is not a fault: it says no store was configured here, and
            // repeating it in the error column would read as a failure that
            // somebody has to investigate. Whether a deployment wants one is a
            // deployment's decision, and a single-process one that made the other
            // choice is not broken.
            error: match redis_status.as_str() {
                "up" | "unknown" => None,
                other => Some(other.to_string()),
            },
        });

        if let Some(ref pool) = self.pg_pool {
            let pg_start = std::time::Instant::now();
            let pg_status = match sqlx::query("SELECT 1").fetch_one(pool).await {
                Ok(_) => "up".to_string(),
                Err(e) => {
                    overall = "degraded".to_string();
                    format!("down: {}", e)
                }
            };
            backends.push(BackendHealth {
                name: "postgres".to_string(),
                status: pg_status.clone(),
                latency_ms: Some(pg_start.elapsed().as_millis() as u64),
                last_checked: Some(chrono::Utc::now().to_rfc3339()),
                error: if pg_status == "up" {
                    None
                } else {
                    Some(pg_status)
                },
            });
        }

        if let Some(ref url) = self.nats_url {
            let nats_start = std::time::Instant::now();
            let nats_status = match tokio::net::TcpStream::connect(url.replace("nats://", "")).await
            {
                Ok(_) => "up".to_string(),
                Err(e) => {
                    overall = if overall == "up" {
                        "degraded".to_string()
                    } else {
                        overall
                    };
                    format!("down: {}", e)
                }
            };
            backends.push(BackendHealth {
                name: "nats".to_string(),
                status: nats_status.clone(),
                latency_ms: Some(nats_start.elapsed().as_millis() as u64),
                last_checked: Some(chrono::Utc::now().to_rfc3339()),
                error: if nats_status == "up" {
                    None
                } else {
                    Some(nats_status)
                },
            });
        }

        if let Some(ref backend) = self.memory_backend {
            let mem_start = std::time::Instant::now();
            let mem_status = match backend.health_check().await {
                Ok(_) => "up".to_string(),
                Err(e) => {
                    overall = "degraded".to_string();
                    format!("down: {}", e)
                }
            };
            backends.push(BackendHealth {
                name: "memory".to_string(),
                status: mem_status.clone(),
                latency_ms: Some(mem_start.elapsed().as_millis() as u64),
                last_checked: Some(chrono::Utc::now().to_rfc3339()),
                error: if mem_status == "up" {
                    None
                } else {
                    Some(mem_status)
                },
            });
        }

        BackendHealthMatrix { overall, backends }
    }
}
