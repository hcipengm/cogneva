use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cog_core::SFResult;
use tokio::sync::{broadcast, Mutex, RwLock};

use super::rate_limit::{is_expired, TokenBucket};
use super::types::{DEFAULT_DEDUP_WINDOW, DEFAULT_HOOK_TIMEOUT};
use cog_core::{
    HookAction, HookDef, HookEvent, HookExecution, HookOutcome, HookScope, HookTrigger, LogLevel,
    RateLimitConfig,
};

/// Pluggable transport for hook actions.
/// Concrete implementations send Webhooks (HTTP), publish to Redis Streams,
/// or deliver in-app notifications.  The engine handles rate-limiting,
/// deduplication, error isolation and timeouts independent of the transport.
#[async_trait]
pub trait HookPublisher: Send + Sync {
    async fn publish_webhook(
        &self,
        url: &str,
        headers: &HashMap<String, String>,
        payload: &serde_json::Value,
    ) -> SFResult<()>;

    async fn publish_redis_stream(
        &self,
        channel: &str,
        payload: &serde_json::Value,
    ) -> SFResult<()>;

    async fn notify_user(&self, user_id: &str, payload: &serde_json::Value) -> SFResult<()>;
}

/// Default publisher that uses an injected [`HttpClient`] for webhooks and an
/// injected [`MessageBackend`](cog_core::MessageBackend) for Redis streams.
pub struct DefaultHookPublisher {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    redis: Option<Arc<dyn cog_core::MessageBackend>>,
}

impl Default for DefaultHookPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultHookPublisher {
    pub fn new() -> Self {
        Self {
            client: None,
            redis: None,
        }
    }

    pub fn with_redis(mut self, redis: Arc<dyn cog_core::MessageBackend>) -> Self {
        self.redis = Some(redis);
        self
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }
}

#[async_trait]
impl HookPublisher for DefaultHookPublisher {
    async fn publish_webhook(
        &self,
        url: &str,
        headers: &HashMap<String, String>,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        let client = self.client.as_ref().ok_or_else(|| {
            cog_core::SFError::Agent("DefaultHookPublisher: no HttpClient configured".into())
        })?;
        let mut req = cog_core::HttpRequest::post(url)
            .json(payload)
            .map_err(|e| {
                cog_core::SFError::Agent(format!("hook webhook serialize failed: {}", e))
            })?;
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let response = client
            .execute(req)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("hook webhook failed: {}", e)))?;
        if !response.is_success() {
            return Err(cog_core::SFError::IO(format!(
                "hook webhook returned {}",
                response.status
            )));
        }
        Ok(())
    }

    async fn publish_redis_stream(
        &self,
        channel: &str,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        let backend = self.redis.as_ref().ok_or_else(|| {
            cog_core::SFError::Agent("DefaultHookPublisher: redis backend not configured".into())
        })?;
        let bytes = serde_json::to_vec(payload).map_err(cog_core::SFError::Serialization)?;
        backend.publish(channel, &bytes).await
    }

    async fn notify_user(&self, user_id: &str, payload: &serde_json::Value) -> SFResult<()> {
        // Default behaviour: route through the configured Redis backend.
        let channel = format!("sf:notify:{}", user_id);
        self.publish_redis_stream(&channel, payload).await
    }
}

/// Configuration knobs for [`HookEngine`].
#[derive(Debug, Clone)]
pub struct HookEngineConfig {
    /// Window inside which identical events are coalesced.  Defaults to 1s.
    pub dedup_window: Duration,
    /// Default rate-limit applied to hooks without an explicit `rate_limit`.
    pub default_rate_limit: RateLimitConfig,
    /// Default per-hook execution timeout.  Defaults to 30s.
    pub hook_timeout: Duration,
}

impl Default for HookEngineConfig {
    fn default() -> Self {
        Self {
            dedup_window: DEFAULT_DEDUP_WINDOW,
            default_rate_limit: RateLimitConfig::default(),
            hook_timeout: DEFAULT_HOOK_TIMEOUT,
        }
    }
}

impl From<cog_core::HookEngineConfig> for HookEngineConfig {
    fn from(c: cog_core::HookEngineConfig) -> Self {
        Self {
            dedup_window: Duration::from_secs(c.dedup_window_secs),
            default_rate_limit: c.default_rate_limit,
            hook_timeout: Duration::from_secs(c.hook_timeout_secs),
        }
    }
}

struct HookEngineInner {
    hooks: RwLock<Vec<HookDef>>,
    rate_limiters: Mutex<HashMap<String, TokenBucket>>,
    dedup_cache: Mutex<HashMap<String, Instant>>,
    publisher: Arc<dyn HookPublisher>,
    config: HookEngineConfig,
    event_tx: broadcast::Sender<HookEvent>,
}

/// Hook execution engine.
/// **Concurrency model**: every matched hook is executed in a fresh
/// `tokio::spawn` task so that one slow webhook never blocks an unrelated
/// publisher.  Failures are captured per-hook and surfaced via the returned
/// [`HookExecution`] vector — a single hook erroring out does not abort the
/// dispatch for other hooks.
#[derive(Clone)]
pub struct HookEngine {
    inner: Arc<HookEngineInner>,
}

impl HookEngine {
    /// Create a new engine with the supplied publisher and default config.
    pub fn new(publisher: Arc<dyn HookPublisher>) -> Self {
        Self::with_config(publisher, HookEngineConfig::default())
    }

    pub fn with_config(publisher: Arc<dyn HookPublisher>, config: HookEngineConfig) -> Self {
        let (event_tx, _event_rx) = broadcast::channel::<HookEvent>(256);
        Self {
            inner: Arc::new(HookEngineInner {
                hooks: RwLock::new(Vec::new()),
                rate_limiters: Mutex::new(HashMap::new()),
                dedup_cache: Mutex::new(HashMap::new()),
                publisher,
                config,
                event_tx,
            }),
        }
    }

    /// Subscribe to hook events broadcast by this engine.
    pub fn subscribe(&self) -> broadcast::Receiver<HookEvent> {
        self.inner.event_tx.subscribe()
    }

    /// Replace the entire hook registry — used after reloading from YAML.
    pub async fn replace_hooks(&self, defs: Vec<HookDef>) {
        let mut hooks = self.inner.hooks.write().await;
        *hooks = defs;
    }

    /// Register a single hook, replacing any existing hook with the same id.
    pub async fn register(&self, def: HookDef) {
        let mut hooks = self.inner.hooks.write().await;
        if let Some(pos) = hooks.iter().position(|h| h.id == def.id) {
            hooks[pos] = def;
        } else {
            hooks.push(def);
        }
    }

    /// Load all `.json` hook definitions from `dir` and register them.
    /// Returns the number of successfully loaded hooks.  Malformed files are
    /// logged and skipped.
    pub async fn load_from_dir(&self, dir: &std::path::Path) -> SFResult<usize> {
        let mut count = 0;
        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(e) => e,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(0);
                }
                return Err(cog_core::SFError::IO(format!(
                    "Failed to read hook dir {}: {}",
                    dir.display(),
                    e
                )));
            }
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            match tokio::fs::read_to_string(&path).await {
                Ok(content) => match serde_json::from_str::<HookDef>(&content) {
                    Ok(def) => {
                        self.register(def).await;
                        count += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse hook file {}: {}", path.display(), e);
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to read hook file {}: {}", path.display(), e);
                }
            }
        }
        Ok(count)
    }

    /// Snapshot the currently registered hooks (cheap clone — `HookDef` is
    /// `Clone`).
    pub async fn list_hooks(&self) -> Vec<HookDef> {
        self.inner.hooks.read().await.clone()
    }

    async fn rate_limit_check(&self, def: &HookDef) -> bool {
        let cfg = def
            .rate_limit
            .clone()
            .unwrap_or_else(|| self.inner.config.default_rate_limit.clone());
        let mut limiters = self.inner.rate_limiters.lock().await;
        let bucket = limiters
            .entry(def.id.clone())
            .or_insert_with(|| TokenBucket::new(cfg.burst, cfg.per_second));
        bucket.try_acquire()
    }

    async fn dedup_check(&self, event: &HookEvent) -> bool {
        let key = event.effective_dedup_key();
        let mut cache = self.inner.dedup_cache.lock().await;
        // Drop expired entries lazily to keep the map bounded.
        cache.retain(|_, ts| !is_expired(*ts, self.inner.config.dedup_window));
        if cache.contains_key(&key) {
            return false;
        }
        cache.insert(key, Instant::now());
        true
    }
}

/// Returns the default broadcast scope for a given trigger.
/// - Crew scope: OnAgentStart, OnAgentEnd, OnTaskComplete, OnTaskFail, OnCrewComplete
/// - Squad scope: OnRalphPass, OnRalphUnrecoverable, OnSquadRetry
/// - Global scope: everything else (default)
pub fn trigger_scope(trigger: HookTrigger) -> HookScope {
    match trigger {
        HookTrigger::OnAgentStart
        | HookTrigger::OnAgentEnd
        | HookTrigger::OnTaskComplete
        | HookTrigger::OnTaskFail
        | HookTrigger::OnCrewComplete => HookScope::Crew,
        HookTrigger::OnRalphPass
        | HookTrigger::OnRalphUnrecoverable
        | HookTrigger::OnSquadRetry => HookScope::Squad,
    }
}

/// Returns true if `def` should receive `event` considering scope and optional filters.
pub fn hook_matches_scope(def: &HookDef, event: &HookEvent) -> bool {
    match def.scope {
        HookScope::Global => true,
        HookScope::Crew => {
            // If the hook has a crew_id_filter, require a match.
            // Otherwise, receive all Crew-scoped events.
            match &def.crew_id_filter {
                Some(filter) => event.crew_id.as_ref() == Some(filter),
                None => true,
            }
        }
        HookScope::Squad => match &def.squad_id_filter {
            Some(filter) => event.squad_id.as_ref() == Some(filter),
            None => true,
        },
    }
}

impl HookEngine {
    /// Emit an event.  Spawns a task for each matched hook, awaits them all,
    /// and returns one [`HookExecution`] per hook attempted.
    /// **Non-blocking variant**: callers that don't care about per-hook
    /// outcomes can use [`HookEngine::emit_detached`].
    pub async fn emit(&self, event: HookEvent) -> Vec<HookExecution> {
        // Broadcast to subscribers first (best-effort).
        let _ = self.inner.event_tx.send(event.clone());

        if !self.dedup_check(&event).await {
            return vec![HookExecution {
                hook_id: "*dedup*".into(),
                trigger: event.trigger,
                outcome: HookOutcome::Deduplicated,
                timestamp: chrono::Utc::now(),
            }];
        }

        let matched: Vec<HookDef> = {
            let hooks = self.inner.hooks.read().await;
            hooks
                .iter()
                .filter(|h| h.trigger == event.trigger)
                .filter(|h| hook_matches_scope(h, &event))
                .cloned()
                .collect()
        };

        if matched.is_empty() {
            return Vec::new();
        }

        let mut handles = Vec::with_capacity(matched.len());
        for def in matched {
            let engine = self.clone();
            let event = event.clone();
            handles.push(tokio::spawn(async move {
                engine.dispatch_one(def, event).await
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            match h.await {
                Ok(rec) => results.push(rec),
                Err(e) => results.push(HookExecution {
                    hook_id: "*panic*".into(),
                    trigger: event.trigger,
                    outcome: HookOutcome::Failed(format!("hook task panicked: {}", e)),
                    timestamp: chrono::Utc::now(),
                }),
            }
        }
        results
    }

    /// Fire-and-forget variant — useful when called from a hot loop where
    /// the caller does not want to await the dispatch.  Returns immediately
    /// after spawning the dispatch task.
    pub fn emit_detached(&self, event: HookEvent) {
        let engine = self.clone();
        tokio::spawn(async move {
            let _ = engine.emit(event).await;
        });
    }

    async fn dispatch_one(&self, def: HookDef, event: HookEvent) -> HookExecution {
        let trigger = def.trigger;
        let hook_id = def.id.clone();
        let timestamp = chrono::Utc::now();

        // Rate limit check (per-hook).  Failure here counts as RateLimited.
        if !self.rate_limit_check(&def).await {
            return HookExecution {
                hook_id,
                trigger,
                outcome: HookOutcome::RateLimited,
                timestamp,
            };
        }

        let payload = build_payload(&event);
        let timeout = def
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(self.inner.config.hook_timeout);
        let publisher = Arc::clone(&self.inner.publisher);
        let action = def.action.clone();

        let exec = async move { execute_action(&publisher, &action, &payload).await };

        let outcome = match tokio::time::timeout(timeout, exec).await {
            Err(_) => HookOutcome::TimedOut,
            Ok(Ok(())) => HookOutcome::Success,
            Ok(Err(e)) => HookOutcome::Failed(e.to_string()),
        };

        HookExecution {
            hook_id,
            trigger,
            outcome,
            timestamp,
        }
    }
}

#[async_trait]
impl cog_core::HookEngine for HookEngine {
    async fn emit(&self, event: cog_core::HookEvent) -> Vec<cog_core::HookExecution> {
        self.emit(event).await
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::HookEvent> {
        self.subscribe()
    }

    async fn list_hooks(&self) -> Vec<cog_core::HookDef> {
        self.list_hooks().await
    }

    async fn register(&self, def: cog_core::HookDef) {
        self.register(def).await
    }

    async fn replace_hooks(&self, defs: Vec<cog_core::HookDef>) {
        self.replace_hooks(defs).await
    }

    fn emit_detached(&self, event: cog_core::HookEvent) {
        self.emit_detached(event)
    }
}

fn build_payload(event: &HookEvent) -> serde_json::Value {
    serde_json::json!({
        "trigger": format!("{:?}", event.trigger),
        "agent_id": event.agent_id,
        "task_id": event.task_id,
        "crew_id": event.crew_id,
        "squad_id": event.squad_id,
        "timestamp": event.timestamp,
        "payload": event.payload,
    })
}

async fn execute_action(
    publisher: &Arc<dyn HookPublisher>,
    action: &HookAction,
    payload: &serde_json::Value,
) -> SFResult<()> {
    match action {
        HookAction::Webhook { url, headers } => {
            publisher.publish_webhook(url, headers, payload).await
        }
        HookAction::RedisStream { channel } => {
            publisher.publish_redis_stream(channel, payload).await
        }
        HookAction::Log { level } => {
            log_payload(*level, payload);
            Ok(())
        }
        HookAction::Notify { user_id } => publisher.notify_user(user_id, payload).await,
    }
}

fn log_payload(level: LogLevel, payload: &serde_json::Value) {
    match level {
        LogLevel::Trace => tracing::trace!(payload = %payload, "hook fired"),
        LogLevel::Debug => tracing::debug!(payload = %payload, "hook fired"),
        LogLevel::Info => tracing::info!(payload = %payload, "hook fired"),
        LogLevel::Warn => tracing::warn!(payload = %payload, "hook fired"),
        LogLevel::Error => tracing::error!(payload = %payload, "hook fired"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::HookTrigger;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    /// Test publisher that records calls and supports configurable behaviour.
    struct TestPublisher {
        webhook_calls: AtomicUsize,
        redis_calls: AtomicUsize,
        notify_calls: AtomicUsize,
        seen: StdMutex<Vec<(String, serde_json::Value)>>,
        fail_webhook: bool,
        webhook_delay: Option<Duration>,
    }

    impl TestPublisher {
        fn new() -> Self {
            Self {
                webhook_calls: AtomicUsize::new(0),
                redis_calls: AtomicUsize::new(0),
                notify_calls: AtomicUsize::new(0),
                seen: StdMutex::new(Vec::new()),
                fail_webhook: false,
                webhook_delay: None,
            }
        }

        fn failing() -> Self {
            let mut p = Self::new();
            p.fail_webhook = true;
            p
        }

        fn slow(delay: Duration) -> Self {
            let mut p = Self::new();
            p.webhook_delay = Some(delay);
            p
        }
    }

    #[async_trait]
    impl HookPublisher for TestPublisher {
        async fn publish_webhook(
            &self,
            url: &str,
            _headers: &HashMap<String, String>,
            payload: &serde_json::Value,
        ) -> SFResult<()> {
            if let Some(d) = self.webhook_delay {
                tokio::time::sleep(d).await;
            }
            self.webhook_calls.fetch_add(1, Ordering::SeqCst);
            self.seen
                .lock()
                .unwrap()
                .push((url.to_string(), payload.clone()));
            if self.fail_webhook {
                Err(cog_core::SFError::IO("simulated webhook failure".into()))
            } else {
                Ok(())
            }
        }

        async fn publish_redis_stream(
            &self,
            channel: &str,
            payload: &serde_json::Value,
        ) -> SFResult<()> {
            self.redis_calls.fetch_add(1, Ordering::SeqCst);
            self.seen
                .lock()
                .unwrap()
                .push((channel.to_string(), payload.clone()));
            Ok(())
        }

        async fn notify_user(&self, user_id: &str, payload: &serde_json::Value) -> SFResult<()> {
            self.notify_calls.fetch_add(1, Ordering::SeqCst);
            self.seen
                .lock()
                .unwrap()
                .push((user_id.to_string(), payload.clone()));
            Ok(())
        }
    }

    fn make_def(id: &str, trigger: HookTrigger, action: HookAction) -> HookDef {
        HookDef {
            id: id.to_string(),
            trigger,
            scope: HookScope::Global,
            crew_id_filter: None,
            squad_id_filter: None,
            action,
            rate_limit: None,
            timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn trigger_matching_dispatches_only_matching_hooks() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        engine
            .replace_hooks(vec![
                make_def(
                    "h1",
                    HookTrigger::OnAgentStart,
                    HookAction::Log {
                        level: LogLevel::Info,
                    },
                ),
                make_def(
                    "h2",
                    HookTrigger::OnTaskComplete,
                    HookAction::Webhook {
                        url: "http://example/wh".into(),
                        headers: HashMap::new(),
                    },
                ),
            ])
            .await;

        let event = HookEvent::new(HookTrigger::OnTaskComplete).with_task_id("t-1");
        let recs = engine.emit(event).await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].hook_id, "h2");
        assert_eq!(recs[0].outcome, HookOutcome::Success);
        assert_eq!(publisher.webhook_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rate_limit_blocks_overflow() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        let mut def = make_def(
            "rl",
            HookTrigger::OnAgentStart,
            HookAction::RedisStream {
                channel: "x".into(),
            },
        );
        def.rate_limit = Some(RateLimitConfig {
            burst: 2,
            per_second: 1,
        });
        engine.register(def).await;

        let mut outcomes = Vec::new();
        for i in 0..5 {
            // Each event needs a unique dedup_key so dedup doesn't shadow rate-limit.
            let evt = HookEvent::new(HookTrigger::OnAgentStart).with_dedup_key(format!("e{}", i));
            outcomes.extend(engine.emit(evt).await);
        }
        let success = outcomes
            .iter()
            .filter(|r| r.outcome == HookOutcome::Success)
            .count();
        let limited = outcomes
            .iter()
            .filter(|r| r.outcome == HookOutcome::RateLimited)
            .count();
        assert_eq!(success, 2, "burst should let exactly 2 through");
        assert_eq!(limited, 3);
    }

    #[tokio::test]
    async fn timeout_protection_kills_long_action() {
        let publisher = Arc::new(TestPublisher::slow(Duration::from_millis(500)));
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        let mut def = make_def(
            "slow",
            HookTrigger::OnAgentEnd,
            HookAction::Webhook {
                url: "http://example/slow".into(),
                headers: HashMap::new(),
            },
        );
        def.timeout_ms = Some(50);
        engine.register(def).await;

        let recs = engine.emit(HookEvent::new(HookTrigger::OnAgentEnd)).await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].outcome, HookOutcome::TimedOut);
    }

    #[tokio::test]
    async fn error_isolation_one_failure_does_not_block_others() {
        let publisher = Arc::new(TestPublisher::failing());
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        engine
            .replace_hooks(vec![
                make_def(
                    "fails",
                    HookTrigger::OnTaskFail,
                    HookAction::Webhook {
                        url: "http://example/wh".into(),
                        headers: HashMap::new(),
                    },
                ),
                make_def(
                    "logs",
                    HookTrigger::OnTaskFail,
                    HookAction::Log {
                        level: LogLevel::Warn,
                    },
                ),
            ])
            .await;

        let recs = engine.emit(HookEvent::new(HookTrigger::OnTaskFail)).await;
        assert_eq!(recs.len(), 2);
        let failed = recs.iter().find(|r| r.hook_id == "fails").unwrap();
        let ok = recs.iter().find(|r| r.hook_id == "logs").unwrap();
        assert!(matches!(failed.outcome, HookOutcome::Failed(_)));
        assert_eq!(ok.outcome, HookOutcome::Success);
    }

    #[tokio::test]
    async fn deduplication_skips_duplicate_within_window() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::with_config(
            publisher.clone(),
            HookEngineConfig {
                dedup_window: Duration::from_millis(200),
                ..Default::default()
            },
        ));
        engine
            .register(make_def(
                "log",
                HookTrigger::OnAgentStart,
                HookAction::Log {
                    level: LogLevel::Info,
                },
            ))
            .await;

        let evt = || {
            HookEvent::new(HookTrigger::OnAgentStart)
                .with_agent_id("a-1")
                .with_dedup_key("same")
        };

        let r1 = engine.emit(evt()).await;
        let r2 = engine.emit(evt()).await;
        // First call dispatches, second is suppressed.
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].outcome, HookOutcome::Deduplicated);

        // Wait past the window and try again.
        tokio::time::sleep(Duration::from_millis(220)).await;
        let r3 = engine.emit(evt()).await;
        assert_eq!(r3.len(), 1);
        assert_eq!(r3[0].outcome, HookOutcome::Success);
    }

    #[tokio::test]
    async fn no_matching_hooks_returns_empty() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher));
        let recs = engine.emit(HookEvent::new(HookTrigger::OnRalphPass)).await;
        assert!(recs.is_empty());
    }

    #[tokio::test]
    async fn notify_action_dispatches_to_user() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        engine
            .register(make_def(
                "notify",
                HookTrigger::OnSquadRetry,
                HookAction::Notify {
                    user_id: "u-42".into(),
                },
            ))
            .await;

        let recs = engine.emit(HookEvent::new(HookTrigger::OnSquadRetry)).await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].outcome, HookOutcome::Success);
        assert_eq!(publisher.notify_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn redis_stream_action_publishes_payload() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        engine
            .register(make_def(
                "rs",
                HookTrigger::OnCrewComplete,
                HookAction::RedisStream {
                    channel: "orchestrator:events".into(),
                },
            ))
            .await;

        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnCrewComplete)
                    .with_payload(serde_json::json!({"crew": "c-1"})),
            )
            .await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].outcome, HookOutcome::Success);
        assert_eq!(publisher.redis_calls.load(Ordering::SeqCst), 1);

        let seen = publisher.seen.lock().unwrap();
        assert_eq!(seen[0].0, "orchestrator:events");
        let payload = &seen[0].1;
        assert_eq!(payload["payload"]["crew"], "c-1");
    }

    #[tokio::test]
    async fn detached_emit_does_not_block_caller() {
        let publisher = Arc::new(TestPublisher::slow(Duration::from_millis(100)));
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        engine
            .register(make_def(
                "wh",
                HookTrigger::OnAgentEnd,
                HookAction::Webhook {
                    url: "http://x".into(),
                    headers: HashMap::new(),
                },
            ))
            .await;

        let start = Instant::now();
        engine.emit_detached(HookEvent::new(HookTrigger::OnAgentEnd));
        // Should return well before the publisher's 100ms sleep finishes.
        assert!(start.elapsed() < Duration::from_millis(50));

        // Wait for the spawned task to run before tearing down the runtime.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(publisher.webhook_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_global_hook_receives_all_events() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        engine
            .register(make_def(
                "global",
                HookTrigger::OnTaskComplete,
                HookAction::Log {
                    level: LogLevel::Info,
                },
            ))
            .await;

        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnTaskComplete)
                    .with_crew_id("crew:A")
                    .with_squad_id("crew:A:squad:0"),
            )
            .await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].hook_id, "global");
        assert_eq!(recs[0].outcome, HookOutcome::Success);
    }

    #[tokio::test]
    async fn test_crew_scoped_hook_filters_by_crew_id() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        let mut def = make_def(
            "crew-filtered",
            HookTrigger::OnTaskComplete,
            HookAction::Log {
                level: LogLevel::Info,
            },
        );
        def.scope = HookScope::Crew;
        def.crew_id_filter = Some("crew:A".into());
        engine.register(def).await;

        // Event with matching crew_id -- should dispatch
        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnTaskComplete)
                    .with_crew_id("crew:A")
                    .with_dedup_key("e1"),
            )
            .await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].hook_id, "crew-filtered");
        assert_eq!(recs[0].outcome, HookOutcome::Success);

        // Event with non-matching crew_id -- should not dispatch
        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnTaskComplete)
                    .with_crew_id("crew:B")
                    .with_dedup_key("e2"),
            )
            .await;
        assert!(recs.is_empty());
    }

    #[tokio::test]
    async fn test_squad_scoped_hook_filters_by_squad_id() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        let mut def = make_def(
            "squad-filtered",
            HookTrigger::OnRalphPass,
            HookAction::Log {
                level: LogLevel::Info,
            },
        );
        def.scope = HookScope::Squad;
        def.squad_id_filter = Some("crew:A:squad:0".into());
        engine.register(def).await;

        // Matching squad_id
        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnRalphPass)
                    .with_squad_id("crew:A:squad:0")
                    .with_dedup_key("e1"),
            )
            .await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].hook_id, "squad-filtered");

        // Non-matching squad_id
        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnRalphPass)
                    .with_squad_id("crew:A:squad:1")
                    .with_dedup_key("e2"),
            )
            .await;
        assert!(recs.is_empty());
    }

    #[tokio::test]
    async fn test_scoped_hook_without_filter_receives_all_in_scope() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        let mut def = make_def(
            "crew-all",
            HookTrigger::OnTaskComplete,
            HookAction::Log {
                level: LogLevel::Info,
            },
        );
        def.scope = HookScope::Crew;
        // No crew_id_filter set -- should receive ALL Crew-scoped events
        engine.register(def).await;

        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnTaskComplete)
                    .with_crew_id("crew:X")
                    .with_dedup_key("e1"),
            )
            .await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].hook_id, "crew-all");

        let recs = engine
            .emit(
                HookEvent::new(HookTrigger::OnTaskComplete)
                    .with_crew_id("crew:Y")
                    .with_dedup_key("e2"),
            )
            .await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].hook_id, "crew-all");
    }

    #[tokio::test]
    async fn register_replaces_existing_hook_with_same_id() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        let def1 = make_def(
            "same-id",
            HookTrigger::OnAgentStart,
            HookAction::Log {
                level: LogLevel::Info,
            },
        );
        engine.register(def1).await;

        let mut def2 = make_def(
            "same-id",
            HookTrigger::OnAgentStart,
            HookAction::Webhook {
                url: "http://updated".into(),
                headers: HashMap::new(),
            },
        );
        def2.rate_limit = Some(RateLimitConfig {
            burst: 5,
            per_second: 1,
        });
        engine.register(def2.clone()).await;

        let hooks = engine.list_hooks().await;
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].id, "same-id");
        assert!(matches!(hooks[0].action, HookAction::Webhook { .. }));
    }

    #[tokio::test]
    async fn load_from_dir_loads_json_hooks() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));

        let tmp = tempfile::tempdir().unwrap();
        let hook_json = serde_json::json!({
            "id": "from-file",
            "trigger": "on_task_complete",
            "action": { "type": "log", "level": "info" }
        });
        tokio::fs::write(tmp.path().join("hook1.json"), hook_json.to_string())
            .await
            .unwrap();

        // Non-JSON file should be ignored.
        tokio::fs::write(tmp.path().join("readme.txt"), "hello")
            .await
            .unwrap();

        let count = engine.load_from_dir(tmp.path()).await.unwrap();
        assert_eq!(count, 1);

        let hooks = engine.list_hooks().await;
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].id, "from-file");
    }

    #[tokio::test]
    async fn load_from_dir_returns_zero_when_dir_missing() {
        let publisher = Arc::new(TestPublisher::new());
        let engine = Arc::new(HookEngine::new(publisher.clone()));
        let count = engine
            .load_from_dir(std::path::Path::new("/nonexistent/hooks"))
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
