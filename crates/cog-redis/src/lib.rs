//! Redis connection construction, bounded.
//!
//! `redis::aio::ConnectionManager` is the layer that replaces a socket that died,
//! which is why every long-lived redis client in this workspace holds one. Its
//! *default* reconnection strategy, however, is unusable here, and the reason is
//! a unit mismatch inside the driver: `ConnectionManagerConfig::default()` carries
//! `factor = 100` (meant as milliseconds) and feeds it to `with_factor`, which
//! `backoff` reads as a **multiplier**. The backoff builder's own `min_delay` is
//! one second and no `max_delay` is set, so the retry delays become
//! `1s * 100^n` capped at the builder's default ceiling of 60s.
//!
//! What that costs, measured rather than inferred (see
//! `the_default_configuration_stalls_for_minutes`, run with `--ignored`):
//! `ConnectionManager::new` against an address that refuses **instantly** took
//! **473 s (≈8 minutes)** to return the error. Both the first connect and every
//! later reconnect await that same shared retry future, so an in-flight command
//! waits with it.
//!
//! That inverts the meaning of nearly every call site in this workspace: they
//! treat "redis is not reachable" as a reason to *degrade* — skip this backend,
//! skip this plugin — and degrading must not be measured in minutes. A consumer
//! that cannot report progress for five minutes is indistinguishable from one
//! that is wedged.
//!
//! So connections here are always **bounded**: within the budget they either
//! connect or return an error the caller can act on. Callers that want to keep
//! trying have their own retry loop; what they never get from this crate is a
//! call that hangs.
//!
//! Scope: this crate owns *how a connection is built*. What is stored on redis —
//! state, streams, registries, audit — belongs to `cog-storage` and `cog-stream`.

use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::aio::ConnectionManagerConfig;

/// Total connect budget in milliseconds. Bounds the retry delays *and* each
/// attempt, so it is also the order of magnitude of the worst case.
const DEFAULT_BUDGET_MS: u64 = 2_000;
/// Retry attempts within that budget. Kept low on purpose: the callers that can
/// tolerate redis being away for a while already retry the whole operation, and
/// they need the failure to come back to them rather than be absorbed here.
const DEFAULT_RETRIES: usize = 2;
/// A budget below this would turn a momentary refusal into a permanent error.
const MIN_BUDGET_MS: u64 = 100;

/// How long a connection attempt may take, and how many times it is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectBudget {
    /// Total budget in milliseconds.
    pub budget_ms: u64,
    /// Retries after the first attempt.
    pub retries: usize,
}

impl Default for ConnectBudget {
    fn default() -> Self {
        Self {
            budget_ms: DEFAULT_BUDGET_MS,
            retries: DEFAULT_RETRIES,
        }
    }
}

impl ConnectBudget {
    /// Read the budget from the environment.
    ///
    /// Both knobs are defined by injection: an installation that runs redis on a
    /// slow link can widen the budget without a rebuild.
    pub fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// The same, with the value source injected — reading the environment is a
    /// property of the process, not of the decision, and the decision is what
    /// the tests below pin down.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let read = |key: &str| get(key).and_then(|v| v.trim().parse::<u64>().ok());
        let default = Self::default();
        Self {
            budget_ms: read("COGNEVA_REDIS_CONNECT_BUDGET_MS")
                .filter(|v| *v >= MIN_BUDGET_MS)
                .unwrap_or(default.budget_ms),
            retries: read("COGNEVA_REDIS_CONNECT_RETRIES").unwrap_or(default.retries as u64)
                as usize,
        }
    }

    /// Per-attempt timeout. Without it a peer that silently drops packets (rather
    /// than refusing) holds the attempt open for the operating system's TCP
    /// timeout — again minutes, and again not a number this system can wait on.
    pub fn attempt_timeout(&self) -> Duration {
        Duration::from_millis(self.budget_ms)
    }

    /// Ceiling for the delay between attempts. This is the knob that removes the
    /// minutes: the driver's default has no ceiling, so the delays grow with the
    /// multiplier it was handed.
    pub fn max_delay_ms(&self) -> u64 {
        self.budget_ms / (self.retries as u64 + 1)
    }
}

/// Connect with a connection manager that replaces a dead socket, bounded by the
/// budget from the environment.
pub async fn connect(client: &redis::Client) -> redis::RedisResult<ConnectionManager> {
    connect_with(client, ConnectBudget::from_env()).await
}

/// Connect with an explicit budget. For call sites with a reason to differ, and
/// for tests that need a fixed bound.
pub async fn connect_with(
    client: &redis::Client,
    budget: ConnectBudget,
) -> redis::RedisResult<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_number_of_retries(budget.retries)
        .set_max_delay(budget.max_delay_ms())
        .set_connection_timeout(budget.attempt_timeout());
    let started = std::time::Instant::now();
    let connection = ConnectionManager::new_with_config(client.clone(), config).await?;
    // Worth a line: a connect that needed retries means redis was away and came
    // back, which is the difference between "started fine" and "recovered from a
    // blip" when someone reads the log later.
    let elapsed = started.elapsed();
    if elapsed > budget.attempt_timeout() {
        tracing::info!(
            elapsed_ms = elapsed.as_millis(),
            retries = budget.retries,
            "redis connection established after retrying"
        );
    }
    Ok(connection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::AsyncCommands;

    /// An address that refuses immediately: the shape of "redis is not running".
    const REFUSED: &str = "redis://127.0.0.1:1";
    /// TEST-NET-1: packets are dropped, not refused — the shape of "the network
    /// is there but nothing answers", which is the case only a per-attempt
    /// timeout can bound.
    const BLACK_HOLE: &str = "redis://192.0.2.1:6379";

    fn budget() -> ConnectBudget {
        ConnectBudget {
            budget_ms: 500,
            retries: 2,
        }
    }

    /// The delays must stay inside the budget. Without a ceiling the driver
    /// multiplies its one-second floor by the factor it was handed, which is
    /// where the minutes come from.
    #[test]
    fn the_budget_bounds_every_delay() {
        let b = ConnectBudget {
            budget_ms: 900,
            retries: 2,
        };
        assert_eq!(b.max_delay_ms(), 300);
        assert_eq!(b.attempt_timeout(), Duration::from_millis(900));

        let default = ConnectBudget::default();
        assert_eq!(default.budget_ms, DEFAULT_BUDGET_MS);
        assert_eq!(default.retries, DEFAULT_RETRIES);
        assert!(
            default.max_delay_ms() * (default.retries as u64 + 1) <= default.budget_ms,
            "the delays alone must not be able to exceed the budget"
        );
    }

    /// An installation may widen the budget; a nonsense value must not narrow it
    /// into "no retry at all", which would turn a restart into a hard failure.
    #[test]
    fn the_environment_sets_the_budget_and_is_ignored_when_unusable() {
        let lookup = |pairs: &[(&str, &str)]| {
            let owned: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            move |key: &str| owned.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
        };

        let widened = ConnectBudget::from_lookup(lookup(&[
            ("COGNEVA_REDIS_CONNECT_BUDGET_MS", "9000"),
            ("COGNEVA_REDIS_CONNECT_RETRIES", "5"),
        ]));
        assert_eq!(widened.budget_ms, 9000);
        assert_eq!(widened.retries, 5);

        let nonsense = ConnectBudget::from_lookup(lookup(&[
            ("COGNEVA_REDIS_CONNECT_BUDGET_MS", "0"),
            ("COGNEVA_REDIS_CONNECT_RETRIES", "many"),
        ]));
        assert_eq!(
            nonsense,
            ConnectBudget::default(),
            "an unusable value falls back to the default instead of disabling retries"
        );
    }

    /// The regression this crate exists for. Against a refused address the call
    /// must come back as an error within the budget — degrading a backend is
    /// allowed to cost a second, not minutes.
    #[tokio::test]
    async fn a_refused_address_fails_within_the_budget() {
        let client = redis::Client::open(REFUSED).expect("a well-formed url");
        let started = std::time::Instant::now();
        let result = connect_with(&client, budget()).await;
        let elapsed = started.elapsed();
        assert!(
            result.is_err(),
            "nothing is listening on {REFUSED}, so this cannot succeed"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "took {elapsed:?}; the caller reads a slow failure as a wedged process"
        );
    }

    /// The per-attempt timeout: a peer that never answers must be cut off rather
    /// than held for the operating system's TCP timeout.
    #[tokio::test]
    async fn a_silent_peer_is_cut_off_by_the_attempt_timeout() {
        let client = redis::Client::open(BLACK_HOLE).expect("a well-formed url");
        let started = std::time::Instant::now();
        let result = connect_with(&client, budget()).await;
        let elapsed = started.elapsed();
        assert!(result.is_err(), "no redis answers on {BLACK_HOLE}");
        assert!(
            elapsed < Duration::from_secs(10),
            "took {elapsed:?}; a dropped packet must not outlive the budget"
        );
    }

    /// And the ordinary case still works: a reachable redis gives a connection
    /// that answers, so the bound did not buy itself correctness.
    #[tokio::test]
    async fn a_reachable_redis_gives_a_working_connection() {
        let url = std::env::var("COGNEVA_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let client = redis::Client::open(url).expect("a well-formed url");
        let Ok(mut connection) = connect_with(&client, budget()).await else {
            eprintln!("SKIP: no redis reachable for the happy path");
            return;
        };
        let pong: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .expect("a managed connection answers");
        assert_eq!(pong, "PONG");
        let _: () = connection
            .set("cog-redis:test", "1")
            .await
            .expect("and takes commands");
    }

    /// Evidence for the rationale above, kept runnable instead of quoted from a
    /// measurement nobody can repeat: the driver's own default configuration,
    /// on the address that refuses instantly. Ignored by default because it
    /// takes minutes — that is the finding.
    #[tokio::test]
    #[ignore = "records the driver's default stall; run with --ignored"]
    async fn the_default_configuration_stalls_for_minutes() {
        let client = redis::Client::open(REFUSED).expect("a well-formed url");
        let started = std::time::Instant::now();
        let error = match ConnectionManager::new(client).await {
            Ok(_) => panic!("nothing is listening on the address"),
            Err(e) => e,
        };
        let elapsed = started.elapsed();
        println!(
            "default ConnectionManagerConfig on {REFUSED}: {:.1}s before returning {error}",
            elapsed.as_secs_f64()
        );
        assert!(
            elapsed > Duration::from_secs(60),
            "if this ever gets fast, the bound in this crate can be revisited"
        );
    }
}
