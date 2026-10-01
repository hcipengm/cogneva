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

use std::sync::atomic::{AtomicU64, Ordering};
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

/// A connection kept for the life of a process, established when it is first
/// asked for and re-attempted after a failure.
///
/// `connect` above answers "can I reach redis right now", inside a budget, which
/// is what a *call site* needs. A channel -- one connection a process holds and
/// uses for as long as it runs -- needs one thing that a call cannot supply on
/// its own: permission to come back. Made once at startup, a failure is
/// permanent and silent. The process that could not reach redis at that instant
/// stays blind for the rest of its life, and "blind" looks exactly like "nothing
/// to report" on every surface the channel feeds. Observed on 2026-10-01: the
/// security gateway started in the same second as the redis pod it reads, failed
/// to resolve it four seconds later, and from then on published no pool snapshot
/// and read back no verdict -- with no series anywhere saying the channel was
/// down, so the pool looked healthy while nothing was listening to it.
///
/// Redis being away for a few seconds is not a reason to stop listening -- a pod
/// restart, a rollout that recreated both ends, a resolver not up yet -- so the
/// retry lives here, once, instead of in every reader. A caller must read `None`
/// as "this attempt failed", never as a verdict about what is stored: nothing
/// was read.
pub struct Reconnecting {
    client: redis::Client,
    budget: ConnectBudget,
    conn: std::sync::Mutex<Option<ConnectionManager>>,
    failed_attempts: AtomicU64,
}

impl Reconnecting {
    /// A channel bounded by the budget from the environment.
    pub fn new(client: redis::Client) -> Self {
        Self::with_budget(client, ConnectBudget::from_env())
    }

    /// The same, with an explicit budget. For call sites with a reason to
    /// differ, and for tests that need a fixed bound.
    pub fn with_budget(client: redis::Client, budget: ConnectBudget) -> Self {
        Self {
            client,
            budget,
            conn: std::sync::Mutex::new(None),
            failed_attempts: AtomicU64::new(0),
        }
    }

    /// Hand out a live connection, connecting first when there is none.
    ///
    /// `None` means this attempt failed; the next call tries again. That is the
    /// whole point of the type, so a caller that gives up on `None` -- or, worse,
    /// latches the channel off -- reintroduces the failure it exists to remove.
    pub async fn get(&self) -> Option<ConnectionManager> {
        // The lock is not held across the connect: a second caller arriving
        // while the first is still connecting would otherwise wait behind a
        // budget it has no share of, and the two would then share one answer.
        if let Some(conn) = self.held() {
            return Some(conn);
        }
        match connect_with(&self.client, self.budget).await {
            Ok(conn) => {
                *self.conn.lock().unwrap() = Some(conn.clone());
                Some(conn)
            }
            Err(e) => {
                self.failed_attempts.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e, "redis 未接上，本次尝试失败；下次取用时重试");
                None
            }
        }
    }

    /// Connect attempts that did not succeed since this channel was created.
    ///
    /// A reader needs this to tell the two ways of having no connection apart:
    /// "not needed yet" and "tried and failed" produce the same `get`, and only
    /// the second one is a fault.
    pub fn failed_attempts(&self) -> u64 {
        self.failed_attempts.load(Ordering::Relaxed)
    }

    fn held(&self) -> Option<ConnectionManager> {
        self.conn.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::AsyncCommands;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;

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

    /// A failed attempt is not a verdict: the next call tries again. A channel
    /// that stays shut after one bad minute is the failure observed on
    /// 2026-10-01, where the gateway spent the rest of its life with the pool
    /// signal closed because redis was not resolvable at the second it started.
    #[tokio::test]
    async fn a_failed_attempt_does_not_close_the_channel() {
        let client = redis::Client::open(REFUSED).expect("a well-formed url");
        let channel = Reconnecting::with_budget(client, budget());
        for attempt in 1..=2 {
            assert!(
                channel.get().await.is_none(),
                "nothing listens on {REFUSED}, so this cannot succeed"
            );
            assert_eq!(
                channel.failed_attempts(),
                attempt,
                "attempt {attempt} has to be made rather than skipped"
            );
        }
    }

    /// A TCP proxy in front of redis that closes every connection until it is
    /// opened: the shape of "redis is coming back" — a pod restarting, a
    /// resolver that has not answered yet.
    struct GateProxy {
        addr: std::net::SocketAddr,
        open: Arc<AtomicBool>,
        accepts: Arc<AtomicUsize>,
    }

    impl GateProxy {
        async fn start(upstream: String) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind proxy");
            let addr = listener.local_addr().expect("proxy address");
            let open = Arc::new(AtomicBool::new(false));
            let accepts = Arc::new(AtomicUsize::new(0));
            let (open_task, accepts_task) = (Arc::clone(&open), Arc::clone(&accepts));
            tokio::spawn(async move {
                while let Ok((mut downstream, _)) = listener.accept().await {
                    accepts_task.fetch_add(1, Ordering::Relaxed);
                    if !open_task.load(Ordering::Relaxed) {
                        // Dropping it closes it, which is what a client that
                        // reached the port sees while the peer is still starting.
                        drop(downstream);
                        continue;
                    }
                    let upstream = upstream.clone();
                    tokio::spawn(async move {
                        let Ok(mut upstream) =
                            tokio::net::TcpStream::connect(upstream.as_str()).await
                        else {
                            return;
                        };
                        let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
                    });
                }
            });
            Self {
                addr,
                open,
                accepts,
            }
        }

        fn open(&self) {
            self.open.store(true, Ordering::Relaxed);
        }

        fn accepts(&self) -> usize {
            self.accepts.load(Ordering::Relaxed)
        }
    }

    /// The half the type exists for: a channel that could not connect reaches
    /// redis once the peer answers, and a connection it already holds is reused
    /// rather than reopened.
    #[tokio::test]
    async fn a_channel_reaches_redis_once_the_peer_answers() {
        let url = std::env::var("COGNEVA_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let client = redis::Client::open(url).expect("a well-formed url");
        let upstream = match client.get_connection_info().addr {
            redis::ConnectionAddr::Tcp(ref host, port) => format!("{host}:{port}"),
            ref other => {
                eprintln!("SKIP: {other:?} is not a tcp address this test can proxy");
                return;
            }
        };
        if tokio::net::TcpStream::connect(&upstream).await.is_err() {
            eprintln!("SKIP: no redis at {upstream} to proxy");
            return;
        }
        let proxy = GateProxy::start(upstream).await;
        let channel = Reconnecting::with_budget(
            redis::Client::open(format!("redis://{}", proxy.addr)).expect("proxy url"),
            budget(),
        );

        assert!(channel.get().await.is_none(), "the gate is shut");
        assert_eq!(channel.failed_attempts(), 1);

        proxy.open();
        let mut conn = channel.get().await.expect("the peer answers now");
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .expect("a managed connection answers");
        assert_eq!(pong, "PONG");
        assert_eq!(channel.failed_attempts(), 1, "a success is not a failure");

        // Counted from here, not from zero: the shut gate was tried more than
        // once inside that first call (the connect retries within its budget),
        // and those attempts are the failure, not the reuse.
        let after_connect = proxy.accepts();
        for _ in 0..3 {
            assert!(channel.get().await.is_some());
        }
        assert_eq!(
            proxy.accepts(),
            after_connect,
            "an established channel is handed out again, not reopened"
        );
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
