use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Prompt Provider ───────────────────────────────────────────────────────

/// Trait for prompt lookup and template rendering.
/// Implementations may load prompts from files, databases, or remote URLs.
/// This trait lives in `cog-core` so that downstream crates can depend on the
/// abstraction rather than the concrete `cog-prompt` crate.
pub trait PromptProvider: Send + Sync {
    /// Get a raw prompt string by key.
    fn get(&self, key: &str) -> Option<String>;

    /// Render a prompt with template variables.
    /// Returns an error if the prompt is not found or rendering fails.
    fn render(&self, key: &str, vars: &HashMap<String, String>) -> crate::SFResult<String>;
}

/// ConfigMap 驱动的配置管理。支持热更新。
/// **定位**：`cog-core` 只保留领域层通用配置。
/// 业务 crate 特有的配置（supervisor、hook_engine、agent_loop、metrics 等）
/// 定义在 `cogneva` 组装层的 `AppConfig` 中，避免污染核心契约。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub app: AppInfo,
    pub providers: ProviderConfigs,
    pub dag_executor: DagExecutorConfig,
    pub gateway: GatewayConfig,
    pub raw_logger: crate::storage::RawLoggerConfig,
    #[serde(default)]
    pub tier_migrator: TierMigratorConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub supervisor: SupervisorConfig,
    #[serde(default)]
    pub hook_engine: HookEngineConfig,
    #[serde(default)]
    pub system: SystemConfig,
    #[serde(default)]
    pub self_evolution: SelfEvolutionConfig,
    #[serde(default)]
    pub multi_backend_consumer: MultiBackendConsumerConfig,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

pub fn redacted(value: &str) -> &str {
    if value.is_empty() {
        ""
    } else {
        "[redacted]"
    }
}

fn redacted_opt(value: &Option<String>) -> &str {
    match value {
        None => "None",
        Some(_) => "Some([redacted])",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub log_level: String,
    pub data_dir: String,   // /var/lib/cogneva-data
    pub config_dir: String, // /etc/cogneva
    pub app_dir: String,    // /opt/cogneva
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct ProviderConfigs {
    pub db: ProviderConfig,
    pub pg: ProviderConfig,
    pub vector: ProviderConfig,
    pub media: ProviderConfig,
    pub storage: ProviderConfig,
    #[serde(default)]
    pub wiki: Option<ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct ProviderConfig {
    pub provider: String, // mysql, tdsql, postgres, lancedb, local-sfu...
    pub enabled: bool,
    /// Backend-specific settings, written in the config file as an `"options"`
    /// object beside `provider` and `enabled`. Flattening this map would lift
    /// those keys one level up, leaving the only key present the literal string
    /// `"options"` and every `options.get(...)` lookup in the consumers empty.
    pub options: HashMap<String, serde_json::Value>,
}

/// NATS connection configuration.
/// Supports single-node, clustered, and TLS-secured deployments.
/// Backward-compatible: a bare `"nats_url"` string is auto-upgraded
/// into `urls: [nats_url]` at load time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NatsConfig {
    /// List of NATS server URLs.  For clusters provide at least 3.
    /// Example: `["nats://n1:4222", "nats://n2:4222", "nats://n3:4222"]`
    pub urls: Vec<String>,
    /// Authentication settings.
    pub auth: NatsAuthConfig,
    /// TLS settings.
    pub tls: NatsTlsConfig,
    /// 消费者 ack 等待时长（秒）：超过未 ack 服务端自动重投。必须大于最坏
    /// 单条处理时延——记忆摄取一条含归档+两次 LLM 调用+重试，分钟级；小于
    /// 它会在处理途中重投造成重复消费。默认 900s 覆盖该最坏情况。
    pub consumer_ack_wait_secs: u64,
    /// 单条消息的最大投递次数。消费方按投递次数判定毒药消息：写 DLQ 后
    /// ack 终止，不让它在流里无限重投。
    pub consumer_max_deliver: i64,
    /// 服务端允许的在途未 ack 消息上限，超出即停止投递。消费端的背压闸门：
    /// 积压在流里（持久），不堆在消费者进程内存里。
    pub consumer_max_ack_pending: usize,
    /// 自动建流的保留策略："workqueue"（消息被任一消费组 ack 后即删，适合
    /// 任务分发）/"limits"（按容量上限保留，适合多消费组各自全量消费的事件
    /// 面）/"interest"。默认 workqueue 保持任务队列既有语义；事件面等多
    /// 订阅方场景必须 limits，否则先 ack 的消费组会把消息从其他组嘴里删掉。
    /// 注意：NATS 不允许在既有流上把 retention 改入/改出 workqueue，策略
    /// 变更只能靠删流重建。
    pub stream_retention: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            auth: NatsAuthConfig::default(),
            tls: NatsTlsConfig::default(),
            consumer_ack_wait_secs: 900,
            consumer_max_deliver: 5,
            consumer_max_ack_pending: 1024,
            stream_retention: "workqueue".into(),
        }
    }
}

/// NATS authentication configuration.
/// `password` / `token` 在 Debug 输出中脱敏。
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct NatsAuthConfig {
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

impl std::fmt::Debug for NatsAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsAuthConfig")
            .field("username", &self.username)
            .field("password", &redacted_opt(&self.password))
            .field("token", &redacted_opt(&self.token))
            .finish()
    }
}

/// NATS TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct NatsTlsConfig {
    pub enabled: bool,
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DagExecutorConfig {
    pub redis_url: String,
    /// Modern NATS configuration (preferred).
    pub nats: NatsConfig,
    /// Deprecated: bare NATS URL string.  When present and `nats.urls`
    /// is the default single-node list, this value is auto-promoted
    /// into `nats.urls` for backward compatibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_url: Option<String>,
    pub workspace_id: String,
    pub consumer_group: String,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    #[serde(default = "default_ready_task_poll_interval_secs")]
    pub ready_task_poll_interval_secs: u64,
    #[serde(default = "default_batch_persistence_enabled")]
    pub batch_persistence_enabled: bool,
    #[serde(default = "default_batch_persistence_max_changes")]
    pub batch_persistence_max_changes: u32,
    #[serde(default = "default_batch_persistence_interval_secs")]
    pub batch_persistence_interval_secs: u64,
    /// Archive terminated tasks from memory after they have been in a
    /// terminal state for longer than `archive_after_secs`.
    #[serde(default = "default_archive_enabled")]
    pub archive_enabled: bool,
    /// How long a task must stay in a terminal state before it is
    /// eligible for archival (seconds).
    #[serde(default = "default_archive_after_secs")]
    pub archive_after_secs: u64,
    /// Interval between archive scans (seconds).
    #[serde(default = "default_archive_poll_interval_secs")]
    pub archive_poll_interval_secs: u64,
    /// Number of decomposition attempts within one goal delivery before an
    /// empty atomic-task result is treated as a hard failure (minimum 1).
    #[serde(default = "default_decomposition_max_attempts")]
    pub decomposition_max_attempts: u32,
    /// Enable the background reconciler that terminates non-executable
    /// parent placeholders with no children stuck in Pending.
    #[serde(default = "default_decomposition_orphan_watch_enabled")]
    pub decomposition_orphan_watch_enabled: bool,
    /// Reconciler scan interval (seconds).
    #[serde(default = "default_decomposition_orphan_poll_interval_secs")]
    pub decomposition_orphan_poll_interval_secs: u64,
    /// How long a childless non-executable placeholder may stay Pending
    /// before the reconciler terminates it and raises an alert (seconds).
    #[serde(default = "default_decomposition_orphan_stall_after_secs")]
    pub decomposition_orphan_stall_after_secs: u64,
    /// Minimum lifetime of an orphan alert before it may resolve (seconds). The
    /// alert consumer (signal watcher) polls on its own cadence; resolving the
    /// moment the placeholder is terminal can make the firing window shorter
    /// than one consumer poll, so the alert stays up for at least this long.
    /// Default is twice the default reconciler / watcher poll interval.
    #[serde(default = "default_decomposition_orphan_alert_dwell_secs")]
    pub decomposition_orphan_alert_dwell_secs: u64,
    /// How long a task-result message may stay unacknowledged before the
    /// consumer's sweep reclaims and replays it (seconds). A result is acked
    /// only after its state transition is applied, so anything still pending
    /// after this long belongs to a consumer that died mid-handling. Must
    /// exceed the longest result-handling time, or an in-flight message is
    /// reclaimed concurrently.
    #[serde(default = "default_result_claim_idle_secs")]
    pub result_claim_idle_secs: u64,
    /// Cadence of the result pending sweep (seconds).
    #[serde(default = "default_result_claim_interval_secs")]
    pub result_claim_interval_secs: u64,
    /// Maximum result messages reclaimed per sweep.
    #[serde(default = "default_result_claim_batch")]
    pub result_claim_batch: usize,
    /// How long a self-evolution task may run before the timeout checker
    /// reclaims it (seconds). These tasks drive multi-agent LLM collaboration,
    /// which outlasts an ordinary atomic task by a wide margin — but every
    /// second here is also how long a task orphaned by a restart stays
    /// invisible, so the value belongs to the deployment rather than to the
    /// code that creates the task.
    #[serde(default = "default_self_evolution_timeout_secs")]
    pub self_evolution_timeout_secs: u64,
    /// How long a running task's lease stays valid without a heartbeat
    /// (seconds). A process holds the lease of every task it started and
    /// pushes the expiry forward while it works; when it is replaced, the
    /// expiry stops moving and whoever observes the lapse takes the task over.
    /// This is what bounds how long work orphaned by a restart stays
    /// invisible — the timeout above is the budget a *live* run gets, not a
    /// restart detector. Keep it above the timeout-checker cadence
    /// (`system.timeout_checker_interval_secs`) so a reclaim lands on the next
    /// sweep rather than on a stale read, and low enough that a task killed by
    /// a rolling replacement is picked up long before the next one.
    #[serde(default = "default_task_lease_secs")]
    pub task_lease_secs: u64,
    /// How long one blocking stream read waits for a message before the
    /// consumer re-issues it (milliseconds). This is the consumer's quiet
    /// period: a stream that is merely empty answers every block with "no
    /// message", so the observed silence of a consumer is bounded by this
    /// value, and a reader that stays silent for several multiples of it is
    /// not idle — it is failing. That is why the value is also published as a
    /// reading: the staleness rule takes its bound from here instead of from
    /// a constant written next to the rule.
    #[serde(default = "default_redis_read_block_ms")]
    pub redis_read_block_ms: u64,
    /// Same, for the read that resumes from an id already seen rather than
    /// from the newest entry. It is shorter because that read is normally
    /// drained immediately: the id is known to exist, so waiting a full block
    /// for it only delays the consumers behind it.
    #[serde(default = "default_redis_read_resume_block_ms")]
    pub redis_read_resume_block_ms: u64,
}

/// Five seconds: long enough that an empty stream costs one round trip per
/// block per consumer, short enough that a message reaches an idle consumer
/// promptly. Every consumer holds its own connection, so this is a per-consumer
/// quiet period, not a shared polling budget.
pub const DEFAULT_REDIS_READ_BLOCK_MS: u64 = 5000;

/// One second: the resume read is a drain, and the entries it is looking for
/// are already in the stream.
pub const DEFAULT_REDIS_READ_RESUME_BLOCK_MS: u64 = 1000;

fn default_redis_read_block_ms() -> u64 {
    DEFAULT_REDIS_READ_BLOCK_MS
}

fn default_redis_read_resume_block_ms() -> u64 {
    DEFAULT_REDIS_READ_RESUME_BLOCK_MS
}

/// Two minutes: the renewal cadence is a third of this, so a process may miss
/// two renewals — a paused container, a loaded node — before another process
/// judges it gone. Shorter values make a live process lose its own tasks to a
/// slow tick; much longer ones put the orphan window back within reach of the
/// replacement cadence this exists to stay ahead of.
pub const DEFAULT_TASK_LEASE_SECS: u64 = 120;

fn default_task_lease_secs() -> u64 {
    DEFAULT_TASK_LEASE_SECS
}

impl Default for DagExecutorConfig {
    fn default() -> Self {
        Self {
            redis_url: String::new(),
            nats: NatsConfig::default(),
            nats_url: None,
            workspace_id: String::new(),
            consumer_group: String::new(),
            max_retries: 3,
            retry_delay_ms: 1000,
            ready_task_poll_interval_secs: default_ready_task_poll_interval_secs(),
            batch_persistence_enabled: default_batch_persistence_enabled(),
            batch_persistence_max_changes: default_batch_persistence_max_changes(),
            batch_persistence_interval_secs: default_batch_persistence_interval_secs(),
            archive_enabled: default_archive_enabled(),
            archive_after_secs: default_archive_after_secs(),
            archive_poll_interval_secs: default_archive_poll_interval_secs(),
            decomposition_max_attempts: default_decomposition_max_attempts(),
            decomposition_orphan_watch_enabled: default_decomposition_orphan_watch_enabled(),
            decomposition_orphan_poll_interval_secs:
                default_decomposition_orphan_poll_interval_secs(),
            decomposition_orphan_stall_after_secs: default_decomposition_orphan_stall_after_secs(),
            decomposition_orphan_alert_dwell_secs: default_decomposition_orphan_alert_dwell_secs(),
            result_claim_idle_secs: default_result_claim_idle_secs(),
            result_claim_interval_secs: default_result_claim_interval_secs(),
            result_claim_batch: default_result_claim_batch(),
            self_evolution_timeout_secs: default_self_evolution_timeout_secs(),
            task_lease_secs: default_task_lease_secs(),
            redis_read_block_ms: default_redis_read_block_ms(),
            redis_read_resume_block_ms: default_redis_read_resume_block_ms(),
        }
    }
}

/// Per-platform webhook configuration (DingTalk / Feishu / WeChat Work).
/// `secret` 在 Debug 输出中脱敏。
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct PlatformWebhookConfig {
    pub webhook_url: String,
    pub secret: Option<String>,
}

impl std::fmt::Debug for PlatformWebhookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlatformWebhookConfig")
            .field("webhook_url", &self.webhook_url)
            .field("secret", &redacted_opt(&self.secret))
            .finish()
    }
}

/// Built-in access-token TTL when the config leaves it at 0. Both the JWT
/// manager (cog-auth) and the HTTP handlers (expires_in responses) derive
/// from this so the two never drift apart.
pub const DEFAULT_ACCESS_TOKEN_TTL_MINUTES: u64 = 15;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct GatewayConfig {
    pub http_port: u16,
    pub ws_port: u16,
    pub metrics_port: u16,
    pub cors_origins: Vec<String>,
    /// Access-token TTL in minutes. 0 = built-in default
    /// ([`DEFAULT_ACCESS_TOKEN_TTL_MINUTES`]).
    #[serde(default)]
    pub access_token_ttl_minutes: u64,
    /// Explicit opt-in for the no-user-store demo login (any credentials get
    /// an admin token). Must stay false outside throwaway demo deployments;
    /// production uses the bootstrap admin password or a real user store.
    #[serde(default)]
    pub demo_login_enabled: bool,
    #[serde(default = "default_websocket_timeout_secs")]
    pub websocket_timeout_secs: u64,
    #[serde(default = "default_websocket_inactivity_timeout_secs")]
    pub websocket_inactivity_timeout_secs: u64,
    #[serde(default = "default_websocket_tick_secs")]
    pub websocket_tick_secs: u64,
    #[serde(default = "default_notification_limit")]
    pub notification_limit: u32,
    #[serde(default = "default_sandbox_task_timeout_secs")]
    pub sandbox_task_timeout_secs: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// How often the contribution OAuth refresher wakes up. 0 = built-in
    /// default ([`DEFAULT_CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS`]).
    #[serde(default)]
    pub contribution_oauth_refresh_interval_secs: u64,
    /// Renew the token once less than this much lifetime remains. 0 = built-in
    /// default ([`DEFAULT_CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS`]).
    #[serde(default)]
    pub contribution_oauth_refresh_threshold_secs: u64,
    /// Optional HTTP webhook URL for outbound notification delivery.
    #[serde(default)]
    pub notification_webhook_url: Option<String>,
    /// DingTalk robot webhook configuration.
    #[serde(default)]
    pub notification_dingtalk: Option<PlatformWebhookConfig>,
    /// Feishu (Lark) robot webhook configuration.
    #[serde(default)]
    pub notification_feishu: Option<PlatformWebhookConfig>,
    /// WeChat Work (企业微信) robot webhook configuration.
    #[serde(default)]
    pub notification_wechat_work: Option<PlatformWebhookConfig>,
}

impl GatewayConfig {
    /// Effective access-token TTL in minutes; never zero.
    pub fn effective_access_token_ttl_minutes(&self) -> u64 {
        if self.access_token_ttl_minutes == 0 {
            DEFAULT_ACCESS_TOKEN_TTL_MINUTES
        } else {
            self.access_token_ttl_minutes
        }
    }

    /// Effective contribution OAuth refresher interval; never zero.
    pub fn effective_contribution_oauth_refresh_interval_secs(&self) -> u64 {
        if self.contribution_oauth_refresh_interval_secs == 0 {
            DEFAULT_CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS
        } else {
            self.contribution_oauth_refresh_interval_secs
        }
    }

    /// Effective contribution OAuth renewal threshold; never zero. A threshold
    /// of zero would renew only after the token had already expired.
    pub fn effective_contribution_oauth_refresh_threshold_secs(&self) -> u64 {
        if self.contribution_oauth_refresh_threshold_secs == 0 {
            DEFAULT_CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS
        } else {
            self.contribution_oauth_refresh_threshold_secs
        }
    }
}

/// Hourly wake-up. The access token lives 24h, so an hourly check leaves room
/// for several failed attempts before the token actually lapses.
pub const DEFAULT_CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS: u64 = 3600;
/// Renew once under 4h of lifetime remain: enough for the token exchange, the
/// Secret patch and the gateway roll to all land before expiry.
pub const DEFAULT_CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS: u64 = 4 * 3600;

fn default_websocket_timeout_secs() -> u64 {
    0
}
fn default_websocket_inactivity_timeout_secs() -> u64 {
    0
}
fn default_websocket_tick_secs() -> u64 {
    0
}
fn default_notification_limit() -> u32 {
    0
}
fn default_sandbox_task_timeout_secs() -> u64 {
    0
}
fn default_request_timeout_secs() -> u64 {
    0
}
fn default_ready_task_poll_interval_secs() -> u64 {
    0
}
fn default_batch_persistence_enabled() -> bool {
    false
}
fn default_batch_persistence_max_changes() -> u32 {
    0
}
fn default_batch_persistence_interval_secs() -> u64 {
    0
}
fn default_archive_enabled() -> bool {
    false
}
fn default_archive_after_secs() -> u64 {
    3600
}
fn default_archive_poll_interval_secs() -> u64 {
    300
}
fn default_decomposition_max_attempts() -> u32 {
    2
}
fn default_decomposition_orphan_watch_enabled() -> bool {
    true
}
fn default_decomposition_orphan_poll_interval_secs() -> u64 {
    300
}
fn default_decomposition_orphan_stall_after_secs() -> u64 {
    1800
}
fn default_decomposition_orphan_alert_dwell_secs() -> u64 {
    600
}
fn default_result_claim_idle_secs() -> u64 {
    600
}
fn default_result_claim_interval_secs() -> u64 {
    60
}
fn default_result_claim_batch() -> usize {
    16
}
fn default_self_evolution_timeout_secs() -> u64 {
    3600
}

/// Configuration for the Hot/Warm/Cold tier migrator.
/// `enabled = false` skips the migrator entirely and leaves files untouched
/// in the hot tier. Durations are seconds so the JSON config stays compact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TierMigratorConfig {
    pub enabled: bool,
    pub hot_duration_secs: u64,
    pub warm_duration_secs: u64,
    pub warm_compression_level: i32,
    pub cold_compression_level: i32,
    pub scan_interval_secs: u64,
    pub cold_key_prefix: String,
    /// How many traces the trace migrator examines per tier per pass.
    /// Entries are taken oldest first, so this bounds one pass' work rather
    /// than what migration can eventually reach; it only has to exceed the
    /// arrivals during one scan interval, or the backlog grows without ever
    /// being examined to the end.
    pub trace_scan_batch: u64,
}

/// The defaults are the shipped configuration, not the zero value of each
/// type. A derived `Default` would give every duration 0, which reads as "every
/// entry is past its tier from the moment it is written" — the raw-log migrator
/// would then upload and remove local files on its first pass, and the trace
/// migrator would demote everything to cold. A section omitted from the config
/// file must behave like the one that ships, so a config typo cannot turn into
/// an unbounded migration.
impl Default for TierMigratorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            hot_duration_secs: 86_400,
            warm_duration_secs: 604_800,
            warm_compression_level: 3,
            cold_compression_level: 9,
            scan_interval_secs: 3_600,
            cold_key_prefix: "raw".into(),
            trace_scan_batch: 5_000,
        }
    }
}

/// Agent registration and heartbeat configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct AgentConfig {
    /// TTL for agent registration in Redis, in seconds.
    pub registration_ttl_secs: u64,
    /// Heartbeat interval in seconds (should be ~1/3 of registration_ttl_secs).
    pub heartbeat_interval_secs: u64,
}

// ---------------------------------------------------------------------------
// Business-crate specific config sections (moved from cogneva assembly layer)
// ---------------------------------------------------------------------------

/// Metrics exporter configuration.
///
/// There is no scrape-interval setting. The endpoint is pulled: observables
/// are collected on demand for each request, nothing in this process samples
/// them on a timer, so there is no cadence here for a number to configure. The
/// cadence belongs to whoever scrapes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
    /// Ceiling on rows held in the metrics sample log. `0` prunes nothing.
    ///
    /// The sample log is append-only: one row per observation. What bounds it
    /// is how much it holds, not how old its rows are — a time window answers
    /// neither question, since traffic can put any number of rows inside any
    /// window. The sweep deletes oldest-first until the log is under this, and
    /// stops at the newest row of each series, which every reader reaches the
    /// log through and which therefore may never be deleted.
    ///
    /// The default covers a full day at the observed observation rate with
    /// room to spare. There is no matching "keep for N seconds" knob on
    /// purpose: how a busy deployment and a quiet one spend the same budget is
    /// exactly what should differ between them, and a duration would force
    /// them to trim at the same rate instead.
    pub sample_max_rows: u64,
    /// An optional narrowing of what the `/metrics` scrape asks for.
    ///
    /// Empty — the default — means no narrowing: the scrape asks every
    /// registered observable for every bounded dimension it declares, which is
    /// the whole set of series that can be read without the scrape growing
    /// with traffic. A non-empty list is an operator saying "only these",
    /// and it can only ever ask for less than the producers declare; naming a
    /// dimension nobody declares, or one that is declared unbounded, yields
    /// nothing rather than an error.
    ///
    /// Whether a dimension's series are bounded is a property of the producer,
    /// not of this list: the agent's D1/D2/D3 branch records per-step series
    /// keyed by `task_id`, which grows without bound, so those stay out no
    /// matter what this says. That keeps one process-wide knob from having to
    /// be right about each of a dozen observables, and it is why a scrape body
    /// is not duplicated when a name appears here that several producers
    /// answer.
    pub scrape_dimensions: Vec<String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".into(),
            sample_max_rows: 200_000,
            scrape_dimensions: Vec::new(),
        }
    }
}

/// Supervisor sub-system intervals and thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SupervisorConfig {
    pub health_interval_secs: u64,
    pub quota_interval_secs: u64,
    pub rebalance_interval_secs: u64,
    pub event_window_secs: u64,
    pub broadcast_capacity: usize,
    pub quota_threshold: u64,
    #[serde(default)]
    pub health_checker: HealthCheckerConfig,
    #[serde(default)]
    pub task_rebalancer: TaskRebalancerConfig,
    pub behavior_history_max: usize,
    pub heartbeat_history_max: usize,
    pub alert_history_max: usize,
    /// Supervisor control plane poll interval (seconds).
    pub control_plane_interval_secs: u64,
    /// Optional control plane URL.
    pub control_plane_url: Option<String>,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            health_interval_secs: 10,
            quota_interval_secs: 60,
            rebalance_interval_secs: 300,
            event_window_secs: 30,
            broadcast_capacity: 256,
            quota_threshold: 1_000,
            health_checker: HealthCheckerConfig::default(),
            task_rebalancer: TaskRebalancerConfig::default(),
            behavior_history_max: 20,
            heartbeat_history_max: 1_000,
            alert_history_max: 10_000,
            control_plane_interval_secs: 30,
            control_plane_url: None,
        }
    }
}

/// Health-checker timing thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthCheckerConfig {
    pub suspect_after_secs: u64,
    pub dead_after_secs: u64,
    pub stuck_after_secs: u64,
}

impl Default for HealthCheckerConfig {
    fn default() -> Self {
        Self {
            suspect_after_secs: 15,
            dead_after_secs: 60,
            stuck_after_secs: 600,
        }
    }
}

/// Task rebalancer limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TaskRebalancerConfig {
    pub max_tasks_per_agent: usize,
    pub max_assignments_per_pass: usize,
}

impl Default for TaskRebalancerConfig {
    fn default() -> Self {
        Self {
            max_tasks_per_agent: 4,
            max_assignments_per_pass: 32,
        }
    }
}

/// Hook engine deduplication and rate-limit settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HookEngineConfig {
    pub dedup_window_secs: u64,
    #[serde(default)]
    pub default_rate_limit: RateLimitConfig,
    pub hook_timeout_secs: u64,
}

impl Default for HookEngineConfig {
    fn default() -> Self {
        Self {
            dedup_window_secs: 1,
            default_rate_limit: RateLimitConfig::default(),
            hook_timeout_secs: 30,
        }
    }
}

/// Per-hook rate-limit configuration (token bucket).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    pub burst: u32,
    pub per_second: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            burst: 100,
            per_second: 100,
        }
    }
}

/// System-wide runtime tunables.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SystemConfig {
    /// Broadcast channel capacity for `AgentEvent`.
    pub event_channel_capacity: usize,
    /// Broadcast channel capacity for `TaskEvent`.
    pub task_event_channel_capacity: usize,
    /// Graceful shutdown timeout in milliseconds.
    pub shutdown_timeout_ms: u64,
    /// Interval between timeout-checker ticks (seconds).
    pub timeout_checker_interval_secs: u64,
    /// Stale-task detector poll interval (seconds).
    pub stale_task_detector_poll_secs: u64,
    /// Interval between monthly-partition maintenance rounds (seconds).
    pub partition_maintenance_interval_secs: u64,
    /// WASM tool execution timeout (seconds).
    pub tool_timeout_secs: u64,
    /// Shell command timeout (seconds). Separate from `tool_timeout_secs`: a
    /// WASM snippet is bounded work, while a command may legitimately run a
    /// compiler — one budget cannot serve both. Must not exceed the executor's
    /// own ceiling, which silently clamps anything larger.
    pub shell_timeout_secs: u64,
    /// URL of the remote sandbox executor (e.g.
    /// `http://cogneva-sandbox-executor.cogneva.svc:9090`). When set, shell
    /// command tools execute in the isolated executor pod; when absent,
    /// commands run in-process (embedded/development mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_executor_url: Option<String>,
    /// When true, shell/exec tools reject calls that carry no task identity:
    /// only invocations reached through `Agent::prompt_for_task` (squad DAG
    /// tasks) may execute commands in the sandbox. Default false keeps legacy
    /// entry points (direct prompt, continue, ReAct loop) working; the
    /// self-evolution deployment profile sets this true as a fail-closed gate.
    #[serde(default)]
    pub require_tool_identity: bool,
    /// gRPC client reconnect interval (seconds).
    pub grpc_reconnect_interval_secs: u64,
    /// Health probe default timeout (seconds).
    pub probe_timeout_secs: u64,
    /// HTTP client default timeout for observability calls (seconds).
    pub http_timeout_secs: u64,
    /// Maximum pattern DB size for ActionPlanOrchestrator.
    pub pattern_db_max_size: usize,
    /// Maximum pattern age in days for ActionPlanOrchestrator.
    pub pattern_max_age_days: i64,
    /// PostgreSQL connection pool max connections.
    pub pg_max_connections: u32,
    /// PostgreSQL connection pool min connections.
    pub pg_min_connections: u32,
    /// PostgreSQL connection acquire timeout (seconds).
    pub pg_acquire_timeout_secs: u64,
    /// PostgreSQL connection idle timeout (seconds).
    pub pg_idle_timeout_secs: u64,
    /// Memory message backend broadcast channel capacity.
    pub memory_message_broadcast_capacity: usize,
    /// Observability gateway event channel capacity.
    pub observability_event_channel_capacity: usize,
    /// Skill directory hot-reload poll interval (seconds).
    pub skill_hot_reload_interval_secs: u64,
    /// WebSocket connection manager event cache capacity.
    pub websocket_event_cache_capacity: usize,
    /// Anthropic provider default max_tokens.
    pub anthropic_default_max_tokens: u32,
    /// When true, fail fast on missing persistence backends (no memory fallback).
    pub strict_persistence: bool,
    /// When true, require a persistent vector backend for summary/embedding layer.
    pub vector_backend_required: bool,
    /// Explicitly enabled system plugins. When `None`, all registered plugins are loaded.
    /// When `Some(list)`, only plugins whose names appear in the list are initialised.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled_plugins: Option<Vec<String>>,
    /// Explicitly disabled system plugins. Applied when `enabled_plugins` is `None`.
    /// Plugins in this list are excluded from initialisation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_plugins: Vec<String>,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            event_channel_capacity: 1024,
            task_event_channel_capacity: 256,
            shutdown_timeout_ms: 30_000,
            timeout_checker_interval_secs: 30,
            stale_task_detector_poll_secs: 15,
            partition_maintenance_interval_secs: 3600,
            tool_timeout_secs: 30,
            shell_timeout_secs: 600,
            sandbox_executor_url: None,
            require_tool_identity: false,
            grpc_reconnect_interval_secs: 5,
            probe_timeout_secs: 5,
            http_timeout_secs: 10,
            pattern_db_max_size: 256,
            pattern_max_age_days: 30,
            pg_max_connections: 50,
            pg_min_connections: 2,
            pg_acquire_timeout_secs: 10,
            pg_idle_timeout_secs: 600,
            memory_message_broadcast_capacity: 1024,
            observability_event_channel_capacity: 256,
            websocket_event_cache_capacity: 1000,
            skill_hot_reload_interval_secs: 30,
            anthropic_default_max_tokens: 4096,
            strict_persistence: true,
            vector_backend_required: true,
            enabled_plugins: None,
            disabled_plugins: Vec::new(),
        }
    }
}

/// Multi-backend event consumer configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MultiBackendConsumerConfig {
    pub enabled: bool,
    pub channel: String,
    pub group: String,
    pub retry_interval_secs: u64,
    /// AgentEnd 事件面走持久总线（true）还是只走进程内 broadcast（false）。
    /// 打开后：agent 在事件产生处把 AgentEnd 写入总线（有界异步缓冲兜底
    /// 总线故障窗），broadcast 由 MultiBackendEventConsumer 回灌一次；
    /// 记忆摄取器改以自己的消费组从总线消费，ack 在归档+抽取完成后。
    /// 必须两侧同开同关——只开一侧会造成重复摄取或事件丢失。
    pub events_on_bus: bool,
    /// 事件面专用 NATS 地址列表。非空时事件面用独立的 NATS JetStream 连接，
    /// 与全局 MessageBackend（任务队列，可能仍在 Redis）解耦；为空时事件面
    /// 复用全局 MessageBackend。
    pub nats_urls: Vec<String>,
    /// 发布端异步缓冲容量。总线故障期间事件在缓冲里排队；排满后新事件
    /// 打 ERROR 并丢弃（事件仍在 WAL/日志链路可查），缓冲绝不无界增长。
    pub publish_buffer_capacity: usize,
    /// 发布失败的重试退避基数（毫秒），指数退避。
    pub publish_retry_base_delay_ms: u64,
    /// 事件面流的保留策略（写入事件面 NatsConfig.stream_retention）。事件面
    /// 有 supervisor 回灌与记忆摄取两个独立消费组、各自要全量事件，必须
    /// "limits"（或 "interest"）；默认即 limits。workqueue 会让先 ack 的
    /// 消费组把事件从其他组删掉，且其消费组只接受 deliver-all。
    pub events_stream_retention: String,
}

impl Default for MultiBackendConsumerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channel: "cogneva-events".into(),
            group: "cogneva-consumer".into(),
            retry_interval_secs: 5,
            events_on_bus: false,
            nats_urls: Vec::new(),
            publish_buffer_capacity: 256,
            publish_retry_base_delay_ms: 1000,
            events_stream_retention: "limits".into(),
        }
    }
}

/// Image-based 滚动更新配置（审计 3.2）：启用后 change 部署从特权 Pod
/// `self_exec` 二进制替换升级为「构建镜像 → change Deployment → 滚动更新」。
/// 默认关闭，保持既有 self_exec/systemd 行为不回退。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ImageRolloutConfig {
    pub enabled: bool,
    /// 镜像仓库地址（不含 tag），如 `localhost/cogneva`。
    pub image_repo: String,
    /// 镜像内基础镜像（COPY 已编译二进制）：必须是正式 cogneva 运行镜像
    /// （WebUI/skills/migrations/动态库齐全，glibc 版本匹配），不能用
    /// debian-slim 这类裸基底——二进制启动即缺库崩。
    pub base_image: String,
    /// 镜像构建器可执行文件：buildah / docker / podman。
    pub builder_bin: String,
    /// 构建后是否执行 `<builder> push`（k3s 节点本地镜像可关）。
    pub registry_push: bool,
    pub kubectl_bin: String,
    pub namespace: String,
    pub deployment: String,
    /// Deployment 内目标容器名。
    pub container: String,
    pub rollout_timeout_secs: u64,
}

impl Default for ImageRolloutConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            image_repo: "localhost/cogneva".into(),
            base_image: "cogneva-registry.cogneva.svc.cluster.local:5000/cogneva:local".into(),
            builder_bin: "buildah".into(),
            registry_push: false,
            kubectl_bin: "kubectl".into(),
            namespace: "cogneva".into(),
            deployment: "cogneva".into(),
            container: "cogneva".into(),
            rollout_timeout_secs: 300,
        }
    }
}

/// Firecracker/KVM 微虚拟机沙盒配置（审计 2.5.4）：启用后自进化执行从
/// K8s Pod 升级为「冷启动 MicroVM → 挂载 PV → 执行进化 → 阅后即焚」。
/// 默认关闭，保持既有 K8s Pod 沙盒行为不回退。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MicroVmConfig {
    pub enabled: bool,
    /// firecracker 可执行文件路径。
    pub firecracker_bin: String,
    /// guest 内核镜像（vmlinux）。
    pub kernel_image: String,
    /// rootfs 镜像（ext4）；冷启动时复制为 COW 副本，原镜像只读复用。
    pub rootfs_image: String,
    /// 持久化卷镜像（ext4，Retain）：进化产物与状态的唯一持久层，
    /// VM 销毁后保留。
    pub pv_image: String,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    /// guest 内核启动参数；init 指向 PV 上的进化入口脚本。
    pub boot_args: String,
    /// API socket 与实例目录的根路径。
    pub instance_root: String,
    /// 单次进化执行超时；超时即销毁 VM。
    pub exec_timeout_secs: u64,
}

impl Default for MicroVmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            firecracker_bin: "firecracker".into(),
            kernel_image: "/opt/cogneva/microvm/vmlinux".into(),
            rootfs_image: "/opt/cogneva/microvm/rootfs.ext4".into(),
            pv_image: "/opt/cogneva/microvm/evolution-pv.ext4".into(),
            vcpu_count: 2,
            mem_size_mib: 2048,
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off init=/evolution/init".into(),
            instance_root: "/tmp/cogneva-microvm".into(),
            exec_timeout_secs: 1800,
        }
    }
}

/// 工作区动态分配配置。取消共用固定树后，部署器与每个进化任务各自从裸仓库
/// 检出 `git worktree`；本段决定这些检出去哪里、编译产物缓存在哪。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfEvolutionWorkspaceConfig {
    /// 工作树根目录（每棵检出是它的一个子目录）。
    pub root: String,
    /// 共享 `CARGO_TARGET_DIR`。故意放在工作树之外，工作树才能被整棵重建而
    /// 不丢增量编译缓存。
    pub target_dir: String,
    /// 临时工作树的存活上限（秒）。属主 pid 是 Pod 进程号，进程内永远"活着"，
    /// 所以存活时长是唯一能揪出"进程还在但任务已崩"的兜底，取值要容得下
    /// 一整轮演进。
    pub ephemeral_ttl_secs: u64,
    /// 身份轮换后遗留的 `evol/<旧id>` 分支的回收判据（秒）：已并入 main 的
    /// 分支立即回收，未并入的要等超过这个时长才回收。取值要明显大于两次移植
    /// 之间的间隔——存活实例的分支每次移植都会被强推刷新，长期不动的分支说明
    /// 其所属实例已经不在了。
    pub orphan_branch_ttl_secs: u64,
    /// How often the shared build cache is re-measured, in seconds. Clamped up
    /// to the reading's own floor: the walk is metadata-only but the cache is
    /// large, and a scan per scrape would spend host IO on a number that moves
    /// in hours.
    pub cache_scan_interval_secs: u64,
}

impl Default for SelfEvolutionWorkspaceConfig {
    fn default() -> Self {
        Self {
            root: "/opt/cogneva/sandbox/workspaces".into(),
            target_dir: "/opt/cogneva/sandbox/src/target".into(),
            ephemeral_ttl_secs: 21600,
            orphan_branch_ttl_secs: 2592000,
            cache_scan_interval_secs: 300,
        }
    }
}

/// 同一宿主上同时进行的构建（编译 / 打镜像）数量上限。
///
/// 构建与集群同宿主：一次 `cargo build --release` 叠上一次
/// `cargo test --workspace` 就足以让整机进入换页，此后所有工作负载一起退化，
/// 包括那些本该把这件事报出来的事件循环。**要卡的是并发构建的条数**，而它是
/// 宿主的属性不是进程的属性——部署器、变更验证、基线搬运、镜像构建跑在不同
/// 进程里，各自"有活就起一个"正是和数涨上去的方式。
///
/// `enabled=false` 或 `max_concurrent=0` 时不设界；`lock_dir` 必须**是所有
/// 构建方看到的同一个目录**，否则每个进程各卡各的，读数里那个
/// `dev:ino` 就是用来分辨这件事的。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BuildGateConfig {
    pub enabled: bool,
    /// 同时允许的构建条数。默认 1：构建是宿主的负载尖峰，串行是唯一不靠
    /// 调参也不会打满的取值。
    pub max_concurrent: usize,
    /// 一次性构建（变更验证这类没有下一轮可回的）愿意排队多久，超过即拒。
    /// 取 0 表示只试一次、不排队。
    pub wait_secs: u64,
    /// 槽位文件所在目录。必须在宿主的共享文件系统上（同一个 hostPath），
    /// 否则闸门退化成每个进程各自为政。
    pub lock_dir: String,
}

impl Default for BuildGateConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent: 1,
            wait_secs: 1800,
            lock_dir: "/opt/cogneva/sandbox/build-gate".into(),
        }
    }
}

/// Configuration for the self-evolution auto-deploy pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfEvolutionConfig {
    pub enabled: bool,
    /// Whether this process runs the change-execution loops (evolution cycle,
    /// baseline porter, mainline deployer, microVM executor). Symmetric with
    /// the GitOps `puller_enabled`: a deployment splits the executor role from
    /// the control-plane role so exactly one process generates and lands
    /// changes. Default true so a single-binary / non-K8s host keeps evolving
    /// with no extra wiring. In a multi-pod cluster the dedicated evolution
    /// worker leaves it true while the main application sets it false — the
    /// main app then still serves the admin API and runs the cluster-side
    /// GitOps puller, but never spawns a second executor that would race the
    /// evolution worker over the shared instance fingerprint, bare repo, and
    /// workspace worktrees.
    pub executor_enabled: bool,
    pub auto_apply: bool,
    pub auto_deploy: bool,
    pub sandbox_mode: bool,
    /// Explicit operator opt-out of the sandbox boundary check: when true,
    /// auto apply/deploy run even if no isolated environment is detected.
    /// Never set this on a host you care about.
    pub force_autonomous: bool,
    /// Optional human-in-the-loop gate: when true, changes that pass tests
    /// are rolled back and held at `AwaitingReview` instead of being
    /// committed/deployed, until an operator approves them via the admin
    /// API (`POST /admin/evolution/changes/:id/approve`). Default false
    /// (fully autonomous).
    pub manual_approve: bool,
    /// Directory holding generated change artifacts. `patch_dir` is the
    /// legacy key, accepted as an alias so older config files still load.
    #[serde(alias = "patch_dir")]
    pub change_dir: String,
    pub binary_dir: String,
    pub backup_dir: String,
    pub switch_mode: String,
    pub health_check_grace_period_secs: u64,
    pub health_check_interval_secs: u64,
    pub health_check_max_retries: u32,
    pub test_timeout_secs: u64,
    pub build_timeout_secs: u64,
    pub poll_interval_secs: u64,
    /// 反思条目的归档与它的派生层（schema）是两次独立写，第二次失败或中途
    /// 重启会留下「归档在、按 schema 检索不到」的孤儿。这个间隔决定多久重扫
    /// 一次并补上缺口，0 表示关闭重扫。补齐只读本地归档、不调 LLM，所以上游
    /// 断供时也该照跑。
    pub schema_repair_interval_secs: u64,
    pub notify_on_success: bool,
    pub notify_on_failure: bool,
    /// Image-based 滚动更新；enabled=false 时忽略整块配置。
    pub image_rollout: ImageRolloutConfig,
    /// Firecracker 微虚拟机沙盒；enabled=false 时忽略整块配置。
    pub microvm: MicroVmConfig,
    /// 工作树动态分配；默认值即生产路径，通常无需显式配置。
    pub workspaces: SelfEvolutionWorkspaceConfig,
    /// 宿主级构建闸门：同时进行的编译 / 打镜像条数上限。
    pub build_gate: BuildGateConfig,
}

/// Where synthesized hooks live inside the change directory. The writer (hook
/// synthesis) and the reader (hook loading at agent startup) run in different
/// processes, so a path assembled independently on either side can point the
/// reader at a directory nothing writes to — a load that yields zero hooks and
/// reports no error. Both sides take the subpath from here instead.
pub fn self_evolution_hook_dir(change_dir: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    change_dir.as_ref().join("hooks")
}

impl Default for SelfEvolutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            executor_enabled: true,
            auto_apply: true,
            auto_deploy: true,
            sandbox_mode: false,
            force_autonomous: false,
            manual_approve: false,
            change_dir: "./evolution-changes".into(),
            binary_dir: "/opt/cogneva/bin".into(),
            backup_dir: "/opt/cogneva/bin/backups".into(),
            switch_mode: "self_exec".into(),
            health_check_grace_period_secs: 10,
            health_check_interval_secs: 5,
            health_check_max_retries: 6,
            test_timeout_secs: 3600,
            build_timeout_secs: 3600,
            poll_interval_secs: 60,
            schema_repair_interval_secs: 600,
            notify_on_success: false,
            notify_on_failure: true,
            image_rollout: ImageRolloutConfig::default(),
            microvm: MicroVmConfig::default(),
            workspaces: SelfEvolutionWorkspaceConfig::default(),
            build_gate: BuildGateConfig::default(),
        }
    }
}

impl Default for Config {
    /// Zero-value default.  All real configuration comes from
    /// files / env vars at runtime.
    fn default() -> Self {
        Self {
            app: AppInfo::default(),
            providers: ProviderConfigs::default(),
            dag_executor: DagExecutorConfig::default(),
            gateway: GatewayConfig::default(),
            raw_logger: crate::storage::RawLoggerConfig::default(),
            tier_migrator: TierMigratorConfig::default(),
            agent: AgentConfig {
                registration_ttl_secs: 30,
                heartbeat_interval_secs: 10,
            },
            metrics: MetricsConfig::default(),
            supervisor: SupervisorConfig::default(),
            hook_engine: HookEngineConfig::default(),
            system: SystemConfig::default(),
            self_evolution: SelfEvolutionConfig::default(),
            multi_backend_consumer: MultiBackendConsumerConfig::default(),
            env: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// 专业配置段 env 覆盖（纯函数，零 IO）
// ---------------------------------------------------------------------------
//
// 专业配置段归实现 crate 所有。各 crate 自读 cogneva.json 取段后，用
// 本函数叠加 env 覆盖再反序列化；cogneva config_loader 对 core 聚合段
// 也走同一函数，语义全系统统一。

/// 把 env 变量按点路径写入 JSON 值。`entries` 为 (env 变量名, 点路径) 表，
/// 未设置的变量跳过。类型推断顺序 bool → i64 → f64 → string。
pub fn apply_env_paths(value: &mut serde_json::Value, entries: &[(&str, &str)]) {
    for (env_name, path) in entries {
        if let Ok(raw) = std::env::var(env_name) {
            set_json_path(value, path, &raw);
        }
    }
}

/// Walk a dot-separated path (`app.name`, `gateway.http_port`) inside a
/// JSON object and overwrite the leaf with `new_val`.
/// Intermediate objects are created automatically if missing.
/// Numeric segments index into arrays (`llm_routing.backends.0.base_url`),
/// but only into existing elements — arrays are never grown implicitly.
pub fn set_json_path(value: &mut serde_json::Value, path: &str, new_val: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return;
    }

    let parsed = if let Ok(b) = new_val.parse::<bool>() {
        serde_json::Value::Bool(b)
    } else if let Ok(n) = new_val.parse::<i64>() {
        serde_json::Value::Number(n.into())
    } else if let Ok(f) = new_val.parse::<f64>() {
        serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into()))
    } else {
        serde_json::Value::String(new_val.into())
    };

    set_json_path_at(value, &parts, parsed);
}

fn set_json_path_at(current: &mut serde_json::Value, parts: &[&str], leaf: serde_json::Value) {
    let Some((head, rest)) = parts.split_first() else {
        return;
    };
    if rest.is_empty() {
        match current {
            serde_json::Value::Object(map) => {
                map.insert(head.to_string(), leaf);
            }
            serde_json::Value::Array(arr) => {
                if let Ok(i) = head.parse::<usize>() {
                    if i < arr.len() {
                        arr[i] = leaf;
                    }
                }
            }
            _ => {}
        }
        return;
    }
    match current {
        serde_json::Value::Object(map) => {
            let next = map
                .entry(head.to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            set_json_path_at(next, rest, leaf);
        }
        serde_json::Value::Array(arr) => {
            if let Ok(i) = head.parse::<usize>() {
                if let Some(next) = arr.get_mut(i) {
                    set_json_path_at(next, rest, leaf);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deployment config that predates `trace_scan_batch` must still scan:
    /// the struct derives `Default`, so a zero would parse cleanly and leave
    /// the migrator examining nothing while looking configured.
    #[test]
    fn trace_scan_batch_falls_back_to_a_usable_default() {
        let cfg: TierMigratorConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.trace_scan_batch, 5000);

        let cfg: TierMigratorConfig = serde_json::from_str(r#"{"trace_scan_batch": 123}"#).unwrap();
        assert_eq!(cfg.trace_scan_batch, 123);
    }

    /// The refresher's two knobs default to zero, which means "use the built-in
    /// default". A zero interval would busy-loop the token exchange and a zero
    /// threshold would renew only once the token had already expired, so the
    /// effective accessors must never return zero.
    #[test]
    fn contribution_oauth_refresh_defaults_are_never_zero() {
        let cfg: GatewayConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            cfg.effective_contribution_oauth_refresh_interval_secs(),
            DEFAULT_CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS
        );
        assert_eq!(
            cfg.effective_contribution_oauth_refresh_threshold_secs(),
            DEFAULT_CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS
        );

        let cfg: GatewayConfig = serde_json::from_str(
            r#"{"contribution_oauth_refresh_interval_secs": 60,
                "contribution_oauth_refresh_threshold_secs": 900}"#,
        )
        .unwrap();
        assert_eq!(cfg.effective_contribution_oauth_refresh_interval_secs(), 60);
        assert_eq!(
            cfg.effective_contribution_oauth_refresh_threshold_secs(),
            900
        );
    }

    /// The synthesizer and the startup loader must agree on where hooks live,
    /// including when `change_dir` carries the trailing separator a config file
    /// may well contain — a doubled separator would make the two sides address
    /// different directories and the load would report zero hooks.
    #[test]
    fn hook_dir_is_the_change_dir_plus_one_segment() {
        assert_eq!(
            self_evolution_hook_dir("/opt/cogneva/sandbox/changes"),
            std::path::Path::new("/opt/cogneva/sandbox/changes/hooks")
        );
        assert_eq!(
            self_evolution_hook_dir("/opt/cogneva/sandbox/changes/"),
            std::path::Path::new("/opt/cogneva/sandbox/changes/hooks")
        );
    }
}
