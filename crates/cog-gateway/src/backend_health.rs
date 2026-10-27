use serde::Serialize;
use std::sync::Arc;

/// What a probe concluded about one backend.
///
/// The response carries these words and the probe board colours a row by them,
/// so they are answered from one place. A probe that wrote `down: <error>` into
/// the status field left the board comparing that text to `down`, which matches
/// nothing: a backend that had just refused every call drew the same row as one
/// nobody had probed yet, and the failure text was rendered in a column the
/// board reads as a status word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendState {
    Up,
    Down,
    Degraded,
    Unknown,
}

impl BackendState {
    pub const ALL: [BackendState; 4] = [
        BackendState::Up,
        BackendState::Down,
        BackendState::Degraded,
        BackendState::Unknown,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            BackendState::Up => "up",
            BackendState::Down => "down",
            BackendState::Degraded => "degraded",
            BackendState::Unknown => "unknown",
        }
    }

    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str().eq_ignore_ascii_case(word))
    }
}

/// Serialised through [`BackendState::as_str`] rather than through a
/// `#[serde(rename)]` on every variant: a rename attribute would be a second
/// spelling of the same word, and the two diverge without anything noticing.
impl Serialize for BackendState {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// One probe's outcome as the row carries it: the declared word in `status`,
/// and the reason -- when there is one -- in `error`.
///
/// The reason never rides inside the status word: the field is a closed
/// vocabulary the reader selects on, not prose.
fn outcome<T, E: std::fmt::Display>(result: Result<T, E>) -> (BackendState, Option<String>) {
    match result {
        Ok(_) => (BackendState::Up, None),
        Err(e) => (BackendState::Down, Some(e.to_string())),
    }
}

/// Health status for a single backend component.
#[derive(Debug, Clone, Serialize)]
pub struct BackendHealth {
    pub name: String,
    /// The probe's conclusion. Typed rather than a string so the failure reason
    /// cannot ride here: this field is what the board selects a row's colour by.
    pub status: BackendState,
    pub latency_ms: Option<u64>,
    pub last_checked: Option<String>,
    pub error: Option<String>,
}

/// Complete health matrix for all infrastructure backends.
#[derive(Debug, Clone, Serialize)]
pub struct BackendHealthMatrix {
    pub overall: BackendState,
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
        let mut overall = BackendState::Up;

        let redis_start = std::time::Instant::now();
        // No store configured is "unknown", not a fault: it says this deployment
        // has none, and a row that reads as a failure sends somebody looking for
        // one that was never meant to exist.
        let (redis_state, redis_error) = match self.key_value.as_ref() {
            Some(store) => outcome(store.ttl(PROBE_KEY).await),
            None => (BackendState::Unknown, None),
        };
        if redis_state == BackendState::Down {
            overall = BackendState::Degraded;
        }
        backends.push(BackendHealth {
            name: "redis".to_string(),
            status: redis_state,
            latency_ms: Some(redis_start.elapsed().as_millis() as u64),
            last_checked: Some(chrono::Utc::now().to_rfc3339()),
            error: redis_error,
        });

        if let Some(ref pool) = self.pg_pool {
            let pg_start = std::time::Instant::now();
            let (pg_state, pg_error) = outcome(sqlx::query("SELECT 1").fetch_one(pool).await);
            if pg_state == BackendState::Down {
                overall = BackendState::Degraded;
            }
            backends.push(BackendHealth {
                name: "postgres".to_string(),
                status: pg_state,
                latency_ms: Some(pg_start.elapsed().as_millis() as u64),
                last_checked: Some(chrono::Utc::now().to_rfc3339()),
                error: pg_error,
            });
        }

        if let Some(ref url) = self.nats_url {
            let nats_start = std::time::Instant::now();
            let (nats_state, nats_error) =
                outcome(tokio::net::TcpStream::connect(url.replace("nats://", "")).await);
            if nats_state == BackendState::Down {
                overall = BackendState::Degraded;
            }
            backends.push(BackendHealth {
                name: "nats".to_string(),
                status: nats_state,
                latency_ms: Some(nats_start.elapsed().as_millis() as u64),
                last_checked: Some(chrono::Utc::now().to_rfc3339()),
                error: nats_error,
            });
        }

        if let Some(ref backend) = self.memory_backend {
            let mem_start = std::time::Instant::now();
            let (mem_state, mem_error) = outcome(backend.health_check().await);
            if mem_state == BackendState::Down {
                overall = BackendState::Degraded;
            }
            backends.push(BackendHealth {
                name: "memory".to_string(),
                status: mem_state,
                latency_ms: Some(mem_start.elapsed().as_millis() as u64),
                last_checked: Some(chrono::Utc::now().to_rfc3339()),
                error: mem_error,
            });
        }

        BackendHealthMatrix { overall, backends }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 探针写进 `status` 的必须是声明过的词：面板按这个词选行。失败文本混进
    /// 状态词时那一行落进通用分支，「探了、被拒」与「还没探过」同形，而真正
    /// 该读的原因在 `error` 那一格里也变成了带前缀的同一句话。
    #[test]
    fn a_failed_probe_keeps_the_word_and_the_reason_apart() {
        let (state, error) = outcome::<(), &str>(Err("connection refused"));
        assert_eq!(state, BackendState::Down);
        assert_eq!(state.as_str(), "down");
        assert_eq!(error.as_deref(), Some("connection refused"));
        assert!(
            !state.as_str().contains("connection"),
            "失败文本不能进状态词"
        );

        let (state, error) = outcome::<(), &str>(Ok(()));
        assert_eq!(state, BackendState::Up);
        assert_eq!(error, None);
    }

    /// 每个声明的词都读得回来。漏一个，`parse` 就认不出它，而按状态取行的
    /// 读者会把那一行读成「从没量过」。
    #[test]
    fn every_declared_state_reads_back_as_itself() {
        for state in BackendState::ALL {
            assert_eq!(
                BackendState::parse(state.as_str()),
                Some(state),
                "{} 声明了却读不回来",
                state.as_str()
            );
        }
        assert_eq!(BackendState::parse("ready"), None);
        assert_eq!(BackendState::parse("Down!"), None);
    }
}
