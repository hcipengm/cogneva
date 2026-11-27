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
    // `bounds` 不在这个类型里，虽然它是一份交付配置的顶层段：这个进程不读它。
    // 它是「系统给自己设的界」的登记簿，读者是 `crates/cogneva/tests/bounds_registry_test.rs`
    // ——在那里按文档原样读（`doc.get("bounds")`），不经过一个没人消费的反序列化。
    // serde 会略过不认识的键，所以文档里那段照样合法。
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
    pub data_dir: String, // see cog_core::paths::DATA_DIR
    pub app_dir: String,  // see cog_core::paths::APP_DIR
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct ProviderConfigs {
    pub db: ProviderConfig,
    pub pg: ProviderConfig,
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

/// 一条消息最多投几次：投完就进 DLQ。
///
/// 它与 [`NatsConfig::consumer_ack_wait_secs`] 一起决定一条消息在总线上的最长
/// 存续时间（次数 × 一次 ack 等待）；重投次数不是独立的档位，是「一条消息的
/// 终局要多久」除以「一次重投等多久」。
// 删掉它，一次确定性失败会被无限重投（消息永远没有终局）；来源是上面这条除法约定。
fn nats_max_deliver() -> crate::Bound<i64> {
    crate::Bound::fixed(
        5,
        "一条消息的终局预算 ÷ 一次 ack 等待：超了就只是没进 DLQ 的慢",
    )
}

/// 一个消费者同时在途、未 ack 的消息上限。
// 删掉它在途无界：消费慢下来时积压只在内存里长，看不见也停不住；来源是上面这条速率约定。
fn nats_max_ack_pending() -> crate::Bound<usize> {
    crate::Bound::fixed(
        1024,
        "一拍处理得完的条数：在途上限小于吃进来的速率，吞吐就卡在这里",
    )
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            auth: NatsAuthConfig::default(),
            tls: NatsTlsConfig::default(),
            consumer_ack_wait_secs: (15 * crate::day::SECONDS_PER_MINUTE) as u64,
            consumer_max_deliver: nats_max_deliver().get(),
            consumer_max_ack_pending: nats_max_ack_pending().get(),
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
    #[serde(default = "default_ready_task_poll_interval_secs")]
    pub ready_task_poll_interval_secs: u64,
    /// How often the process holding a task writes a checkpoint of that task's
    /// progress (seconds), so a task that loses its process can be resumed
    /// instead of re-run from the start.
    ///
    /// The write has to happen while the task runs: what a task is killed by —
    /// a version rollout, an evicted pod, a spent memory limit — never leaves
    /// time for a last write on the way out. This interval is therefore the
    /// bound on how much progress a handover loses, and it is paid for by a
    /// snapshot command per agent per tick.
    #[serde(default = "default_task_checkpoint_interval_secs")]
    pub task_checkpoint_interval_secs: u64,
    /// How long a task may sit in `Scheduled` before the publisher treats its
    /// ready message as lost and puts the task back on the queue (seconds).
    ///
    /// `Scheduled` says one thing: a ready message carrying this task was
    /// handed to the transport. The task's only way out of that state is that
    /// message being consumed, so when the message is acked without the task
    /// reaching `Running` — the consumer died between the two, or the ack
    /// landed and the start did not — the durable row keeps a state that
    /// nothing revisits: the publisher scans `Pending`, and the timeout
    /// checker reclaims only `Running`. The work is then held forever without
    /// ever being reported as lost.
    ///
    /// The value must not be shorter than the transport's own reclaim window
    /// ([`DEFAULT_READY_CLAIM_IDLE_SECS`]). Below it, an unacknowledged
    /// message is still claimable, so this would re-run work that is merely
    /// slow — the cost paid twice to fix a task that was never stuck. At the
    /// default the sweeper has already had its chance, so a task still
    /// `Scheduled` is one whose message is gone.
    #[serde(default = "default_scheduled_task_stall_secs")]
    pub scheduled_task_stall_secs: u64,
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
    ///
    /// A `Failed` row is not only history: it is the record the retry premise
    /// reads. The signal watcher re-drives a self-signal task whose previous
    /// attempt failed, and that branch is only reachable while the row is still
    /// in the store — a reaped row reads as "no such task" and the retry is
    /// silently replaced by a fresh submission. So the retention has to outlive
    /// the wait between two attempts, which is the longest signal cooldown
    /// (24 h), with a margin for a door missed to a restart: the 48 h default is
    /// twice that. A shorter retention does not fail loudly — it makes the
    /// re-drive structurally unreachable, and the reading looks like a system
    /// with nothing to retry. Anything that lengthens the cooldown has to be
    /// weighed against this value; `cog-reflection` holds the pair together
    /// with a gate.
    #[serde(default = "default_archive_after_secs")]
    #[serde(skip_serializing_if = "is_the_default_archive_after_secs")]
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
    /// How long a running task's lease stays valid without a heartbeat
    /// (seconds). A process holds the lease of every task it started and
    /// pushes the expiry forward while it works; when it is replaced, the
    /// expiry stops moving and whoever observes the lapse takes the task over.
    /// This is what bounds how long work orphaned by a restart stays
    /// invisible. It is also the only bound on a live run's duration, and it
    /// bounds *silence*: a run that keeps stepping renews it forever, while one
    /// that stops stepping loses it. There is deliberately no elapsed-time
    /// budget beside it — a run that is producing is never cut, and a run that
    /// stopped is caught by the missing steps, not by the clock. Keep it above the timeout-checker cadence
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
// cap: fixed — 每个消费者各持一条连接：静默期是每个消费者自己的，不是共享轮询预算
// keep: 删了空流会变成忙轮询（每次读立即返回），CPU 随连接数线性上升；由「每消费者一条连接」这条约定兜住
pub const DEFAULT_REDIS_READ_BLOCK_MS: u64 = 5000;

/// One second: the resume read is a drain, and the entries it is looking for
/// are already in the stream.
// cap: fixed — 恢复读是一次排空，它要找的条目已经在流里
// keep: 删了恢复读会变成阻塞未知时长的读；由「恢复读是排空」这条约定兜住
pub const DEFAULT_REDIS_READ_RESUME_BLOCK_MS: u64 = 1000;

fn default_redis_read_block_ms() -> u64 {
    DEFAULT_REDIS_READ_BLOCK_MS
}

fn default_redis_read_resume_block_ms() -> u64 {
    DEFAULT_REDIS_READ_RESUME_BLOCK_MS
}

/// Total connect budget in milliseconds: the ceiling on how long building one
/// redis connection may cost before the caller is told the peer is away.
///
/// This is not a latency target. Every call site in this workspace reads "redis
/// is not reachable" as a reason to degrade, and degrading cannot be measured in
/// minutes: the driver's own default configuration retries an unreachable
/// address for **473 s** (measured, recorded as an ignored test beside the code
/// that overrides it), during which the process reports no progress and looks
/// wedged rather than degraded.
// cap: fixed — 驱动对不可达地址会重试 473s（实测，代码旁有被忽略的测试记着）
// keep: 删了进程在那 473s 里报不出进展，看起来是卡住而不是降级；由那次实测与「降级要秒级」兜住
pub const DEFAULT_REDIS_CONNECT_BUDGET_MS: u64 = 2_000;

/// Retries within that budget. Kept low on purpose: the callers that can
/// tolerate redis being away for a while already retry the whole operation, and
/// they need the failure to come back to them rather than be absorbed here.
// cap: fixed — 容忍 redis 短暂不在的调用方会重试整个操作：失败要回到它们手上
// keep: 删了失败会在连接层被更久地吞掉，调用方拿不到自己的失败；由这条重试约定兜住
pub const DEFAULT_REDIS_CONNECT_RETRIES: usize = 2;

/// How long one command on an established connection may wait for its reply.
///
/// Thirty seconds is not a latency target either: it is the point past which a
/// reply is not late but lost. Every command against redis other than a blocking
/// stream read answers in milliseconds, and the reads that do block widen this
/// explicitly. Left unset, the wait is the kernel's, which is measured in
/// minutes (ten minutes of silence on 2026-10-22, with nothing counted as a
/// refusal because a hung read is not a refused one).
// cap: fixed — 2026-10-22 实测：挂住的读十分钟无声且不会被记成拒绝，因为挂住的读不是被拒的读
// keep: 删了挂住的读无限等且读面上无声；由那次实测兜住
// wall: 等=Redis 对一条命令的应答 / 等不到=这次读以超时失败并被记成一次失败（不是一次拒绝：挂住的读不是被拒的读），调用方拿到错误 / 续跑=能，重来是一次新的命令
pub const DEFAULT_REDIS_RESPONSE_TIMEOUT_MS: u64 = 30_000;

/// A budget below this would turn a momentary refusal into a permanent error.
// cap: fixed — 比它更小的预算把一次瞬时拒绝变成永久错误
// keep: 删了下界，瞬时拒绝会被读成永久错误；由连接预算的下界约定兜住
pub const MIN_REDIS_CONNECT_BUDGET_MS: u64 = 100;

/// A response timeout below this is not a bound, it is a race against the round
/// trip.
// cap: fixed — 比它更小的应答超时不是界，是跟一个往返赛跑
// keep: 删掉后应答超时可以被配得比一次往返还小，每个请求都在跟往返赛跑；由「比这更小的不是界」兜住
// wall: 等=一次 Redis 往返的下限，用来挡住配成更小的值 / 等不到=不适用：这个数本身不会被等，它是别人被等多久的下限 / 续跑=不适用
pub const MIN_REDIS_RESPONSE_TIMEOUT_MS: u64 = 100;

/// How long a redis connection attempt may take, how many times it is retried,
/// and how long one command on the finished connection may wait for its reply.
///
/// The arithmetic lives here, next to the defaults, because more than one crate
/// opens redis connections of its own and the numbers must not drift between
/// them: a caller that read the environment itself would keep the same key names
/// and slowly disagree about what a usable value is. Building the connection
/// stays with the crate that owns that driver call — this type never names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisConnectBudget {
    /// Total budget in milliseconds.
    pub budget_ms: u64,
    /// Retries after the first attempt.
    pub retries: usize,
    /// Upper bound on a single command's round trip, on the connection this
    /// budget builds.
    pub response_timeout_ms: u64,
}

impl Default for RedisConnectBudget {
    fn default() -> Self {
        Self {
            budget_ms: DEFAULT_REDIS_CONNECT_BUDGET_MS,
            retries: DEFAULT_REDIS_CONNECT_RETRIES,
            response_timeout_ms: DEFAULT_REDIS_RESPONSE_TIMEOUT_MS,
        }
    }
}

impl RedisConnectBudget {
    /// Read the budget from the environment.
    ///
    /// Both knobs are defined by injection: an installation that runs redis on a
    /// slow link can widen the budget without a rebuild.
    pub fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// The same, with the value source injected — reading the environment is a
    /// property of the process, not of the decision, and the decision is what
    /// the tests pin down.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let read = |key: &str| get(key).and_then(|v| v.trim().parse::<u64>().ok());
        let default = Self::default();
        Self {
            budget_ms: read("COGNEVA_REDIS_CONNECT_BUDGET_MS")
                .filter(|v| *v >= MIN_REDIS_CONNECT_BUDGET_MS)
                .unwrap_or(default.budget_ms),
            retries: read("COGNEVA_REDIS_CONNECT_RETRIES").unwrap_or(default.retries as u64)
                as usize,
            response_timeout_ms: read("COGNEVA_REDIS_RESPONSE_TIMEOUT_MS")
                .filter(|v| *v >= MIN_REDIS_RESPONSE_TIMEOUT_MS)
                .unwrap_or(default.response_timeout_ms),
        }
    }

    /// The same budget with a different response timeout, for the call sites
    /// whose commands legitimately block past the default.
    pub fn with_response_timeout_ms(mut self, ms: u64) -> Self {
        self.response_timeout_ms = ms.max(MIN_REDIS_RESPONSE_TIMEOUT_MS);
        self
    }

    /// Per-attempt timeout. Without it a peer that silently drops packets
    /// (rather than refusing) holds the attempt open for the operating system's
    /// TCP timeout — again minutes, and again not a number this system can wait
    /// on.
    pub fn attempt_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.budget_ms)
    }

    /// Ceiling for the delay between attempts. This is the knob that removes the
    /// minutes: the driver's default has no ceiling, so the delays grow with the
    /// multiplier it was handed.
    pub fn max_delay_ms(&self) -> u64 {
        self.budget_ms / (self.retries as u64 + 1)
    }

    /// How long one command on the built connection may wait for its reply.
    pub fn response_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.response_timeout_ms)
    }
}

/// Two minutes: the renewal cadence is a third of this, so a process may miss
/// two renewals — a paused container, a loaded node — before another process
/// judges it gone. Shorter values make a live process lose its own tasks to a
/// slow tick; much longer ones put the orphan window back within reach of the
/// replacement cadence this exists to stay ahead of.
// cap: fixed — 一拍慢是秒级；再长就把孤儿窗口推回到这个数要甩开的那个替换节拍里
// keep: 删掉后孤儿窗口落进替换节拍里，租约还没到期就被当孤儿回收；由「一拍慢是秒级」兜住
pub const DEFAULT_TASK_LEASE_SECS: u64 = 120;

fn default_task_lease_secs() -> u64 {
    DEFAULT_TASK_LEASE_SECS
}

/// Ten minutes: how long one ready message may stay unacknowledged before the
/// ready-stream sweeper claims it and re-runs it.
///
/// It lives here rather than next to that sweeper because a second reader
/// needs the same number: a task still `Scheduled` after this long is one
/// whose message is gone rather than one whose message is slow, and the
/// publisher's stall threshold is that same window. Two copies of the value
/// keep compiling when one is edited, and the pair would then disagree by
/// exactly the amount that makes the publisher publish a duplicate while the
/// sweeper is still about to deliver the first copy.
// cap: fixed — 取的就是那个量：再多一点，发布方会在清扫者送出第一份之前先发布一份重复
// keep: 删掉后发布方在清扫者送出第一份之前先发布一份重复；由「取的就是那个量」兜住
pub const DEFAULT_READY_CLAIM_IDLE_SECS: u64 = 600;

/// 一个任务节点失败后重试几次（重试矩阵里 DagNode 那一行的次数）。
///
/// 它是一条退避阶梯的长度：每次等待由矩阵给出，次数由「一条任务的总预算 ÷
/// 一次等待」定，不是可以单拍的档位。
// cap: fixed — 由一条任务的总预算 ÷ 一次退避等待定：次数多到超预算，等于把失败拖长
// keep: 删掉后重试没有次数界，超预算的失败被拉长成无限退避；由「总预算 ÷ 一次退避等待」兜住
const DAG_NODE_MAX_RETRIES: u32 = 3;

impl Default for DagExecutorConfig {
    fn default() -> Self {
        Self {
            redis_url: String::new(),
            nats: NatsConfig::default(),
            nats_url: None,
            workspace_id: String::new(),
            consumer_group: String::new(),
            max_retries: DAG_NODE_MAX_RETRIES,
            ready_task_poll_interval_secs: default_ready_task_poll_interval_secs(),
            task_checkpoint_interval_secs: default_task_checkpoint_interval_secs(),
            scheduled_task_stall_secs: default_scheduled_task_stall_secs(),
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
            task_lease_secs: default_task_lease_secs(),
            redis_read_block_ms: default_redis_read_block_ms(),
            redis_read_resume_block_ms: default_redis_read_resume_block_ms(),
        }
    }
}

impl DagExecutorConfig {
    /// Effective ready-task poll interval; never zero. Zero used to be read as
    /// "poll without waiting" and got clamped to one second by the read site,
    /// which is a busy loop wearing a knob's name.
    pub fn effective_ready_task_poll_interval_secs(&self) -> u64 {
        if self.ready_task_poll_interval_secs == 0 {
            DEFAULT_READY_TASK_POLL_INTERVAL_SECS
        } else {
            self.ready_task_poll_interval_secs
        }
    }
}

/// Per-platform webhook configuration (DingTalk / Feishu / WeChat Work).
///
/// **只有地址。** 曾经这里还有一个 `secret`，用来自算机器人报文的签名——那条路
/// 已废：密钥住在安全网关，业务侧要签名时向网关借一对 `(timestamp, sign)`。
/// 字段留着比删掉更坏：它没有任何消费方，却让「把平台密钥写进配置清单」看起来
/// 是一件被支持的事，而部署面根本没有它的投递键（`config_loader` 的映射里只有
/// 地址三条）。
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
#[derive(Default)]
pub struct PlatformWebhookConfig {
    pub webhook_url: String,
}

/// Built-in access-token TTL when the config leaves it at 0. Both the JWT
/// manager (cog-auth) and the HTTP handlers (expires_in responses) derive
/// from this so the two never drift apart.
// cap: fixed — 认证管理器（cog-auth）与 HTTP 处理器（expires_in）都从这里派生，两处才不会走散
// keep: 删掉后回到「认证管理器（cog-auth）与 HTTP 处理器（expires_in）都从这里派生，两处才不会走散」所描述的情形；由这条约定兜住
pub const DEFAULT_ACCESS_TOKEN_TTL_MINUTES: u64 = 15;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct GatewayConfig {
    pub http_port: u16,
    pub ws_port: u16,
    pub metrics_port: u16,
    /// 零凭证的集群内面端口：只挂 Pod 侧要走的那几条 memory 路由（工具输出归档
    /// 的写、以及 Raw 的读），不挂认证/会话/配额。0＝不开这个监听——没打开过的
    /// 端口不可能被误用，也不用为一个用不上的面配网络策略。
    ///
    /// 与 `http_port` 分开不是为了换路径，是为了换**判据**：主 API 的 memory 路由
    /// 按角色判，而跑 squad 的 Pod 零凭证（红线），按角色判它永远进不来；放在同一
    /// 个端口上匿名，又与运营 API 混成一面，没法只给这几条挂网络策略。端口一分，
    /// 判据就从"按角色"变成"按网络事实"。
    #[serde(default)]
    pub internal_port: u16,
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
    /// WebSocket 心跳间隔。0 ＝ 没写，取
    /// [`DEFAULT_WEBSOCKET_HEARTBEAT_SECS`]（读侧走
    /// [`GatewayConfig::effective_websocket_heartbeat_secs`]）。
    #[serde(default = "default_websocket_timeout_secs")]
    pub websocket_timeout_secs: u64,
    /// 客户端多久不发消息就断开。0 ＝ 没写，取心跳的
    /// [`WEBSOCKET_BEATS_BEFORE_DROPPING`] 倍（读侧走
    /// [`GatewayConfig::effective_websocket_inactivity_timeout_secs`]）。
    #[serde(default = "default_websocket_inactivity_timeout_secs")]
    pub websocket_inactivity_timeout_secs: u64,
    /// 通知列表的默认页大小。0 ＝ 没写，取列表面共用的
    /// [`A2A_LIST_PAGE_SIZE`]。
    #[serde(default = "default_notification_limit")]
    pub notification_limit: u32,
    /// 一次 HTTP 请求的应答期限。
    ///
    /// 0 ＝ 没写，取本次停机的排空时长（`system.shutdown_timeout_ms`，
    /// 由 [`GatewayConfig::effective_request_timeout_secs`] 读进来）：排空愿意为
    /// 一条在飞请求等多久，请求自己就可以占这条连接多久。这个数没有内置常数——
    /// 它要么由部署给，要么跟着排空走。
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
    /// 通知出口要签名时借签名的地方（安全网关的 `/v1/notification/sign`）。
    ///
    /// 与 `COGNEVA_GITHUB_API_BASE` 同形：业务 Pod 零凭证，向网关借一次签名。
    /// 未配置＝这个部署不签名（机器人用关键词安全，是合法形态）。
    #[serde(default)]
    pub notification_sign_base: Option<String>,
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

    /// Effective WebSocket heartbeat; never zero. Zero is not "no heartbeat":
    /// `tokio::time::interval` panics on a zero period, so a connection would
    /// die on connect instead of staying alive.
    pub fn effective_websocket_heartbeat_secs(&self) -> u64 {
        if self.websocket_timeout_secs == 0 {
            DEFAULT_WEBSOCKET_HEARTBEAT_SECS
        } else {
            self.websocket_timeout_secs
        }
    }

    /// Effective client inactivity timeout; never zero. Zero would disconnect
    /// every client the moment it connects.
    ///
    /// 没写时不是取一个常数，而是跟着**这次生效的心跳**走：三拍。心跳被部署改了，
    /// 掉线期限跟着改，两处不会各说各的。
    pub fn effective_websocket_inactivity_timeout_secs(&self) -> u64 {
        if self.websocket_inactivity_timeout_secs == 0 {
            self.effective_websocket_heartbeat_secs() * WEBSOCKET_BEATS_BEFORE_DROPPING
        } else {
            self.websocket_inactivity_timeout_secs
        }
    }

    /// Effective notification page size; never zero. Zero would make every
    /// list response empty, which reads as "you have no notifications".
    pub fn effective_notification_limit(&self) -> u32 {
        if self.notification_limit == 0 {
            A2A_LIST_PAGE_SIZE
        } else {
            self.notification_limit
        }
    }

    /// Effective per-request answer deadline; never zero. Zero would build a
    /// zero-second timeout and answer every request with 408 immediately.
    ///
    /// 没写时取这次停机的排空时长（`system.shutdown_timeout_ms`）：排空愿意为一条
    /// 在飞请求等多久，请求自己就可以占这条连接多久。排空不是常数而是一个配置项，
    /// 所以这里要读它，而不是回落到一个写死的数。
    pub fn effective_request_timeout_secs(&self, shutdown_timeout_ms: u64) -> u64 {
        if self.request_timeout_secs == 0 {
            (shutdown_timeout_ms / 1000).max(1)
        } else {
            self.request_timeout_secs
        }
    }
}

/// Hourly wake-up. The access token lives 24h, so an hourly check leaves room
/// for several failed attempts before the token actually lapses.
// cap: fixed — 访问令牌活 24h，每小时查一次留出几次失败尝试的余量
// keep: 删掉后令牌到期前没有几次失败尝试的余量，一次刷新失败就落到过期；由「令牌活 24h、每小时查一次」兜住
pub const DEFAULT_CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS: u64 = 3600;
/// Renew once under 4h of lifetime remain: enough for the token exchange, the
/// Secret patch and the gateway roll to all land before expiry.
// cap: fixed — 剩不到 4h 就换：够令牌交换、Secret 改写与网关滚动都在到期前落完
// keep: 删掉后等到令牌快过期才换，交换、Secret 改写与网关滚动来不及在到期前落完；由「剩不到 4h 就换」兜住
pub const DEFAULT_CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS: u64 = 4 * 3600;

/// WebSocket 心跳间隔：半开连接靠自己发现不了，一次 ping 让对端在下一次心跳里
/// 暴露自己已经走了。
///
/// 控制台的 `EventStreamClient.HEARTBEAT_INTERVAL_MS`（`web/src/transport/websocket.ts`）
/// 是同一个拍子：同一条 socket 的两端，一边换了拍另一边要跟着换，
/// `the_console_and_the_gateway_beat_at_one_cadence` 判这处一致。
// cap: fixed — 控制台的 `HEARTBEAT_INTERVAL_MS = 30000`：同一条 socket 的两端共用一个拍子
// keep: 删掉后回到「控制台的 `HEARTBEAT_INTERVAL_MS = 30000`：同一条 socket 的两端共用一个拍子」所描述的情形；由这条约定兜住
pub const DEFAULT_WEBSOCKET_HEARTBEAT_SECS: u64 = 30;

/// 一条连接连丢几拍心跳才算死。
///
/// 这是两端约定本身，不是界：不活跃期限 = 心跳 × 它。必须是 3 而不是 1——一次
/// 抖动的丢包不该踢掉一个活着的人；也不能再大，控制台自己 2 拍 ＋ 5 秒就放弃并
/// 重连了，服务端排在它后面才轮得到客户端先发现掉线。
// cap: fixed — 心跳约定：连续丢三拍才断（一次抖动不算离线）
// keep: 删掉后一次心跳抖动就把连接断掉，读者被重连与补发拖一轮；由「连续丢三拍才断」兜住
pub const WEBSOCKET_BEATS_BEFORE_DROPPING: u64 = 3;

/// 列表端点的默认页大小：一屏滚动能读完的量，其余由游标翻页。
///
/// 数出自 A2A 规范对 `page_size` 的说明——不写就按 50（见
/// `crates/cog-gateway/src/a2a.rs` 的 `LIST_PAGE_DEFAULT`，它读的就是这里）。
/// 治理面的 `REFUSED_PAGE` / `TIMELINE_PAGE` 也是这一屏：通知、拒绝、时间线
/// 各写一个 50 只会让「一屏」在几个面上慢慢走散。
// cap: fixed — A2A 规范对 `page_size` 的默认值：不写就按 50，本仓列表面都按这一屏取
// not-a-bound: A2A 规范对 `page_size` 的默认值：不写就按 50，本仓列表面都按这一屏取
pub const A2A_LIST_PAGE_SIZE: u32 = 50;

/// 就绪任务的轮询节拍：任务就绪后最多等一拍被发现。
///
/// 取的是消费者的阻塞读预算：等待就绪任务的 executor 以
/// `redis_read_block_ms` 阻塞读一次，拍长等于它，一条新就绪的任务最多等到
/// 下一次读超时；再长，下一次读超时之后还要再等一拍才有人回来。
// cap: fixed — 一拍＝一个消费者阻塞读预算（`DEFAULT_REDIS_READ_BLOCK_MS`），两处不各走各的
// keep: 删掉后回到「一拍＝一个消费者阻塞读预算（`DEFAULT_REDIS_READ_BLOCK_MS`），两处不各走各的」所描述的情形；由这条约定兜住
pub const DEFAULT_READY_TASK_POLL_INTERVAL_SECS: u64 = DEFAULT_REDIS_READ_BLOCK_MS / 1000;

fn default_websocket_timeout_secs() -> u64 {
    DEFAULT_WEBSOCKET_HEARTBEAT_SECS
}
// cap: fixed — 0 是「没写」的哨兵，不是界；生效值＝心跳×3（`WEBSOCKET_BEATS_BEFORE_DROPPING`）
// not-a-bound: 0 是「没写」的哨兵，不是界；生效值＝心跳×3（`WEBSOCKET_BEATS_BEFORE_DROPPING`）
fn default_websocket_inactivity_timeout_secs() -> u64 {
    0
}
// cap: fixed — 0 是「没写」的哨兵，不是界；生效值＝列表面共用的 `A2A_LIST_PAGE_SIZE`
// not-a-bound: 0 是「没写」的哨兵，不是界；生效值＝列表面共用的 `A2A_LIST_PAGE_SIZE`
fn default_notification_limit() -> u32 {
    0
}
// cap: fixed — 0 是「没写」的哨兵，不是界；生效值＝本次停机的排空时长
// not-a-bound: 0 是「没写」的哨兵，不是界；生效值＝本次停机的排空时长
fn default_request_timeout_secs() -> u64 {
    0
}
fn default_ready_task_poll_interval_secs() -> u64 {
    DEFAULT_READY_TASK_POLL_INTERVAL_SECS
}
/// 检查点节拍：进度最多丢这么久。
///
/// 取值不是拍脑袋的：它是「一次检查点的开销」与「可接受的重跑时长」之间的
/// 取舍，而重跑的上界由任务自己的预算决定——30s 意味着一次换版最多让每个在跑
/// 任务白跑半分钟，代价小于更密的快照对 agent 命令通道的占用。
// cap: fixed — 一拍＝一个消费者阻塞读预算（`DEFAULT_READY_CLAIM_IDLE_SECS`）：一次检查点最多让重跑等一拍
// keep: 删掉后回到「一拍＝一个消费者阻塞读预算（`DEFAULT_READY_CLAIM_IDLE_SECS`）：一次检查点最多让重跑等一拍」所描述的情形；由这条约定兜住
fn default_task_checkpoint_interval_secs() -> u64 {
    (crate::day::SECONDS_PER_MINUTE / 2) as u64
}
fn default_scheduled_task_stall_secs() -> u64 {
    DEFAULT_READY_CLAIM_IDLE_SECS
}
fn default_batch_persistence_enabled() -> bool {
    false
}
// cap: fixed — 0 是「没有分批」的哨兵（分批持久化默认关闭），不是界
// not-a-bound: 0 是「没有分批」的哨兵（分批持久化默认关闭），不是界
fn default_batch_persistence_max_changes() -> u32 {
    0
}
// cap: fixed — 0 是「没有分批」的哨兵（分批持久化默认关闭），不是界
// not-a-bound: 0 是「没有分批」的哨兵（分批持久化默认关闭），不是界
fn default_batch_persistence_interval_secs() -> u64 {
    0
}
fn default_archive_enabled() -> bool {
    false
}
// cap: fixed — 归档默认关闭；这个数只在部署显式开启归档后生效，是那次开启自己的声明
// keep: 删掉后开启归档的部署没有年龄线，归档要么立刻发生要么永不发生；由「开启归档自己的声明」兜住
fn default_archive_after_secs() -> u64 {
    // 两天：按「几天的秒数」写，读的人不必把这串数字除以 86400。
    2 * crate::day::SECONDS_PER_DAY as u64
}
// cap: fixed — 与归档年龄同一档：每天走一遍就够，不是一条要实时响应的界
// keep: 删掉后归档没有走一遍的节拍，部署开启归档后不知道隔多久再走；由「与归档年龄同一档」兜住
fn default_archive_poll_interval_secs() -> u64 {
    (5 * crate::day::SECONDS_PER_MINUTE) as u64
}
// 两遍：一次拆不出原子任务，可能是提供方在那一刻抽出了一个坏结果；两遍都空，
// 说明这个目标本身拆不出可执行的任务，再试只是把同一个坏提示词反复送去烧 token。
// cap: fixed — 两遍：一次是抽样，两遍还空是目标真拆不出来
// keep: 删掉后拆解没有次数界，目标拆不出来时一遍遍重拆；由「一次是抽样、两遍还空是真拆不出来」兜住
const DECOMPOSITION_MAX_ATTEMPTS: u32 = 2;

fn default_decomposition_max_attempts() -> u32 {
    DECOMPOSITION_MAX_ATTEMPTS
}
fn default_decomposition_orphan_watch_enabled() -> bool {
    true
}
// cap: fixed — 五拍：一拍＝`DEFAULT_READY_CLAIM_IDLE_SECS`，孤儿是「连续五拍没人续租」才开始看
// keep: 删掉后回到「五拍：一拍＝`DEFAULT_READY_CLAIM_IDLE_SECS`，孤儿是「连续五拍没人续租」才开始看」所描述的情形；由这条约定兜住
fn default_decomposition_orphan_poll_interval_secs() -> u64 {
    (5 * crate::day::SECONDS_PER_MINUTE) as u64
}
// cap: fixed — 三百拍：五拍一扫、扫到六十次都没人续租才算停住
// keep: 删掉后没有人续租的节点没有停住的时刻，孤儿永远不算停；由「五拍一扫、六十次都没人续租」兜住
fn default_decomposition_orphan_stall_after_secs() -> u64 {
    // 半小时。
    crate::day::SECONDS_PER_HOUR as u64 / 2
}
// cap: fixed — 一百二十拍：停住之后再看两分钟，避免一次抖动就报警
// keep: 删掉后停住之后立刻报警，一次抖动就被读成事故；由「停住之后再看两分钟」兜住
fn default_decomposition_orphan_alert_dwell_secs() -> u64 {
    (10 * crate::day::SECONDS_PER_MINUTE) as u64
}
fn default_result_claim_idle_secs() -> u64 {
    // 与就绪流的清扫窗口同一个数：发布方的停止阈值与清扫者的认领阈值是同一条。
    DEFAULT_READY_CLAIM_IDLE_SECS
}
fn default_result_claim_interval_secs() -> u64 {
    crate::day::SECONDS_PER_MINUTE as u64
}
// 一拍认领十六条：认领与状态迁移在同一条语句里，这个量既能让一次进程死亡留下的
// 积压在一两个扫描周期内清掉，又不至于让一次扫描把事务拖成一把长锁。
// cap: fixed — 十六条：一拍清得掉一次进程死亡留下的积压，又不拖成长事务
// keep: 删掉后一拍要么清不掉一次进程死亡的积压，要么拖成长事务；由「十六条」兜住
const RESULT_CLAIM_BATCH: usize = 16;

fn default_result_claim_batch() -> usize {
    RESULT_CLAIM_BATCH
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
/// trace 层一次冷迁多少行：一拍的搬运量要在一拍的时限内走完。
// cap: fixed — 由一拍搬得动的行数定：批大过一拍搬得动，扫描就积压
// keep: 删掉后批大过一拍搬得动的行数，扫描自己积压；由「一拍搬得动的行数」兜住
const TRACE_SCAN_BATCH: u64 = 5_000;

impl Default for TierMigratorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            // 三层保留是三个时间长度，不是三个数：各写一个秒数只会让「热一天」
            // 这个说法在几处慢慢走散。
            hot_duration_secs: crate::day::SECONDS_PER_DAY as u64,
            warm_duration_secs: crate::day::SECONDS_PER_WEEK as u64,
            warm_compression_level: crate::contract::storage::COMPRESSION_LEVEL_WARM,
            cold_compression_level: crate::contract::storage::COMPRESSION_LEVEL_COLD,
            scan_interval_secs: crate::day::SECONDS_PER_HOUR as u64,
            cold_key_prefix: "raw".into(),
            trace_scan_batch: TRACE_SCAN_BATCH,
        }
    }
}

/// 名册上一条 agent 记录活多久。
///
/// 这不是本仓挑的数：Redis / etcd 的 agent 注册默认 TTL 就是 30 秒，监督者判
/// 「它还在不在」用的也是这同一条——两边各走各的，就会出现「监督者眼里死了、
/// agent 自己还觉得自己活着」。交付文档的 `agent.registration_ttl_secs` 写的就是它。
// cap: fixed — Redis / etcd 的 agent 注册默认 TTL：监督者与 agent 自己必须用同一条
// keep: 删掉后监督者与 agent 各自定 TTL，注册在对方眼里提前消失；由「两侧必须用同一条」兜住
pub const AGENT_REGISTRATION_TTL_SECS: u64 = 30;

/// 一条注册记录要活过几拍心跳。
///
/// 一拍丢包是常态，不该把一个活着的员工判死；连丢三拍才说明它真走了。心跳因此
/// 不是自己挑的数，而是 TTL ÷ 它。
// cap: fixed — 三拍：一次抖动丢一拍不算离线，连丢三拍才算
// keep: 删掉后一次抖动丢一拍就算离线，agent 被反复判死；由「连丢三拍才算」兜住
// not-a-bound: 单位换算（定义性常量：改它只改单位，不改任何阈值行为）
pub const BEATS_PER_REGISTRATION_TTL: u64 = 3;

/// 心跳间隔：worker 向名册续约的拍子。
///
/// 由 TTL 与拍数算出来，不单独声明：改 TTL 或改拍数，它跟着变。
pub const AGENT_HEARTBEAT_INTERVAL_SECS: u64 =
    AGENT_REGISTRATION_TTL_SECS / BEATS_PER_REGISTRATION_TTL;

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
    /// The capacity this deployment **declares** for the metrics sample log,
    /// in rows. `0` declares none.
    ///
    /// It is a statement, not a mechanism: the log is measured every minute and
    /// the measurement is published (`rows`, this capacity, the bytes on disk,
    /// and whether the first exceeds the second), but nothing deletes from the
    /// table on account of it. A reading that deletes what it measures cannot
    /// be read as a reading — by the time anybody sees the number it has
    /// already been acted on, and the rows behind it are gone with nothing but
    /// an inference to say they were there.
    ///
    /// So "over capacity" is a decision point rather than a fault: raise the
    /// declaration if the log is doing its job, change how the series that
    /// grew it is recorded, or stop declaring a capacity at all. Declaring
    /// nothing (`0`) is different from declaring zero and the readings keep
    /// them apart — the first publishes no capacity and no verdict, the second
    /// would claim every row is over a limit nobody asked for.
    ///
    /// The default covers about ten days at the observed observation rate.
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

/// 每次抓取最多返回多少行样本：抓取体的大小上限。
// cap: fixed — 由抓取间隔 × 序列产生速率定：超过它就是一次抓取扛不完的量
// keep: 删掉后一次抓取可以要求搬任意多行，抓取自己扛不完；由「抓取间隔 × 序列产生速率」兜住
const METRICS_SAMPLE_MAX_ROWS: u64 = 500_000;

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_max_rows: METRICS_SAMPLE_MAX_ROWS,
            scrape_dimensions: Vec::new(),
        }
    }
}

/// 监督者巡检心跳活性的节拍。
///
/// 判据（可疑、死亡）都是秒级的，巡检必须比它们密，否则判据被巡检自己拖粗——判据
/// 说「两拍没心跳就算可疑」，而巡检十秒才看一次，实际发现它的时刻就晚了一拍。
// cap: fixed — 与判据的拍子同量级：巡检比判据疏，判据就被巡检拖粗
// keep: 删掉后巡检比判据疏，判据被巡检拖粗；由「与判据的拍子同量级」兜住
pub const SUPERVISOR_HEALTH_INTERVAL_SECS: u64 = 10;

/// 配额巡检的节拍。
///
/// 配额是按分钟结算的额度，一拍一分钟就够；更密只是把同一批计数再读一遍。
// cap: fixed — 配额按分钟结算：一拍一分钟，更密只是重复读同一批计数
// keep: 删掉后配额按更密的拍结算，只是重复读同一批计数；由「配额按分钟结算」兜住
pub const SUPERVISOR_QUOTA_INTERVAL_SECS: u64 = 60;

/// 任务再平衡的节拍。
///
/// 再平衡会挪动在跑的任务，代价远高于一次巡检；五分钟一拍让舰队形状稳定下来，
/// 而不是跟着一次抖动来回搬。
// cap: fixed — 再平衡挪的是在跑的任务：五分钟一拍让形状稳定，不跟着一次抖动来回搬
// keep: 删掉后再平衡跟着一次抖动来回搬在跑的任务；由「五分钟一拍让形状稳定」兜住
pub const SUPERVISOR_REBALANCE_INTERVAL_SECS: u64 = 300;

/// 告警去重与突发判定看的事件窗口。
///
/// 要长到能盖住一次真实的故障脉冲，短到不会把两次独立故障并成一条。
// cap: fixed — 要盖住一次真实故障脉冲，又不把两次独立故障并成一条
// keep: 删掉后窗口把两次独立故障并成一条，或盖不住一次脉冲；由「盖住一次脉冲又不并成一条」兜住
pub const SUPERVISOR_EVENT_WINDOW_SECS: u64 = 30;

/// 控制面巡检的节拍。
///
/// 控制面是可选的外部依赖，读它要一次网络往返；够密才发现它不在了，又不至于让
/// 监控面变成对它的压力源。
// cap: fixed — 外部依赖的一次网络往返：够密才发现它不在了，又不把监控变成压力
// keep: 删掉后外部依赖不在了要更久才发现，或监控变成压力；由「一次网络往返的节奏」兜住
pub const SUPERVISOR_CONTROL_PLANE_INTERVAL_SECS: u64 = 30;

/// 监督者向订阅方广播事件的通道深度。
///
/// 一个巡检周期里可能同时到达的判决条数：通道比它浅，生产方会在两次巡检之间溢出，
/// 而溢出的那几条判决没有第二个读者能补回来。
// cap: fixed — 一个巡检周期内同时到达的判决条数：浅了就会在两次巡检之间溢出
// keep: 删掉后两次巡检之间到达的判决溢出，读数丢条；由「一个巡检周期内同时到达的条数」兜住
pub const SUPERVISOR_BROADCAST_DEPTH: usize = 256;

/// 配额低于它时暂停调度：剩不到这么多 token 就不该再接新任务。
///
/// 一千是一轮 LLM 调用装得下的量级：余量比它还少，下一步派出去的任务只会开个头就
/// 撞上限，而撞限失败比排队更贵——一次调用已经花掉的 token 收不回来。
// cap: fixed — 一轮调用的量级：余量比它少时新任务只会开个头就撞上限
// keep: 删掉后余量比一轮调用还少时照样开新任务，它只开个头就撞上限；由「一轮调用的量级」兜住
pub const SUPERVISOR_QUOTA_PAUSE_REMAINING_TOKENS: u64 = 1_000;

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
            health_interval_secs: SUPERVISOR_HEALTH_INTERVAL_SECS,
            quota_interval_secs: SUPERVISOR_QUOTA_INTERVAL_SECS,
            rebalance_interval_secs: SUPERVISOR_REBALANCE_INTERVAL_SECS,
            event_window_secs: SUPERVISOR_EVENT_WINDOW_SECS,
            broadcast_capacity: SUPERVISOR_BROADCAST_DEPTH,
            quota_threshold: SUPERVISOR_QUOTA_PAUSE_REMAINING_TOKENS,
            health_checker: HealthCheckerConfig::default(),
            task_rebalancer: TaskRebalancerConfig::default(),
            // 三个保留深度没有内置默认：0 ＝ 没写，由持有这些环的那一层换回
            // 它自己的常量（`BehaviorMonitor::DEFAULT_MAX_HISTORY` 等）。
            // cap: fixed — 0 ＝ 没写，由这个环的持有一层换回它自己的常量
            // keep: 删掉后这个 0 表示「没写」的语义消失，持有层把它读成「保留深度为零」；由「0 由持有一层换回自己的常量」兜住
            behavior_history_max: 0,
            // cap: fixed — 同上：0 ＝ 没写，不是保留深度为零
            // keep: 删掉后 0 被读成保留深度为零，心跳历史被关掉；由「0 ＝ 没写」兜住
            heartbeat_history_max: 0,
            // cap: fixed — 同上
            // keep: 删掉后 0 被读成保留深度为零，告警历史被关掉；由「0 ＝ 没写」兜住
            alert_history_max: 0,
            control_plane_interval_secs: SUPERVISOR_CONTROL_PLANE_INTERVAL_SECS,
            control_plane_url: None,
        }
    }
}

/// 多久没心跳算可疑：连丢两拍。
///
/// 一拍抖动是常态；第二拍也没来，才值得看一眼。
// cap: fixed — 心跳的两倍：一拍丢包不算异常，连着两拍没声才值得看一眼
// keep: 删掉后一拍丢包就被读成可疑，正常心跳抖动进入告警；由「连着两拍没声才值得看一眼」兜住
pub const SUSPECT_BEATS: u64 = 2;

/// 多久没心跳算死亡：连丢六拍。
///
/// 可疑之后还接着丢四拍，一条还活着的进程不会这样；两档之间分开的是「网络抖动」
/// 与「进程真没了」。
// cap: fixed — 心跳的六倍：可疑之后还丢四拍，一条活着的进程不会这样
// keep: 删掉后可疑之后丢四拍就判死，一条活着的慢进程被回收；由「心跳的六倍」兜住
pub const DEAD_BEATS: u64 = 6;

/// 多久没有进展算卡住：死亡期限的十倍。
///
/// 一次长任务（构建、一轮评测）可以十分钟没有可见输出；判据比它短会误报，比它
/// 长则卡住的 agent 白占一个槽。
// cap: fixed — 死亡期限的十倍：长任务十分钟不出声是常态，比这更短就是误报
// keep: 删掉后长任务十分钟不出声被读成卡住，正常的沉默被回收；由「长任务十分钟不出声是常态」兜住
pub const STUCK_AFTER_DEATHS: u64 = 10;

/// Health-checker timing thresholds.
///
/// 三个数都是 0 ＝ 没写：它们没有自己的值，读的是**这次生效的心跳**（见
/// [`Self::effective_suspect_after_secs`] 等）。一个部署改心跳而判据不跟着改，
/// 判据就会在一个不存在的拍子上打分。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct HealthCheckerConfig {
    pub suspect_after_secs: u64,
    pub dead_after_secs: u64,
    pub stuck_after_secs: u64,
}

impl HealthCheckerConfig {
    /// 可疑：连丢 [`SUSPECT_BEATS`] 拍心跳。
    pub fn effective_suspect_after_secs(&self, heartbeat_secs: u64) -> u64 {
        if self.suspect_after_secs == 0 {
            heartbeat_secs * SUSPECT_BEATS
        } else {
            self.suspect_after_secs
        }
    }

    /// 死亡：连丢 [`DEAD_BEATS`] 拍心跳。
    pub fn effective_dead_after_secs(&self, heartbeat_secs: u64) -> u64 {
        if self.dead_after_secs == 0 {
            heartbeat_secs * DEAD_BEATS
        } else {
            self.dead_after_secs
        }
    }

    /// 卡住：没有进展达到死亡期限的 [`STUCK_AFTER_DEATHS`] 倍。
    pub fn effective_stuck_after_secs(&self, heartbeat_secs: u64) -> u64 {
        if self.stuck_after_secs == 0 {
            self.effective_dead_after_secs(heartbeat_secs) * STUCK_AFTER_DEATHS
        } else {
            self.stuck_after_secs
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

/// 一个代理同时背多少任务算过载（告警线，不是硬闸）。
// cap: fixed — 由实测的任务时延随并发上升的拐点定：过了拐点再加只是排队
// keep: 删掉后过了时延拐点还继续加，多给的任务只是在排队；由「实测的并发拐点」兜住
// not-a-bound: 单位换算（定义性常量：改它只改单位，不改任何阈值行为）
const REBALANCER_MAX_TASKS_PER_AGENT: usize = 4;

/// 一拍最多给出几条建议：建议由人/上层消费，一拍给太多没有消费者接得住。
// cap: fixed — 由一次调度决策的处理速率定：一拍给得比消费得快，建议就是噪音
// keep: 删掉后一拍给出的建议比消费得快，建议自己成了噪音；由「一次调度决策的处理速率」兜住
// not-a-bound: 单位换算（定义性常量：改它只改单位，不改任何阈值行为）
const REBALANCER_MAX_ASSIGNMENTS_PER_PASS: usize = 32;

impl Default for TaskRebalancerConfig {
    fn default() -> Self {
        Self {
            max_tasks_per_agent: REBALANCER_MAX_TASKS_PER_AGENT,
            max_assignments_per_pass: REBALANCER_MAX_ASSIGNMENTS_PER_PASS,
        }
    }
}

/// Hook engine deduplication and rate-limit settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HookEngineConfig {
    pub dedup_window_secs: u64,
    /// 引擎层的限流缺省。`None` ＝ 没有缺省：没有声明限流的钩子不限流。
    ///
    /// 这里曾经是「每秒 100 次」。那个数没有任何触发密度读数支撑，是拍出来的；
    /// 按「无界是缺省态」的口径，缺省只是一句没有根据的话，删掉它。限流只来自
    /// 钩子自己声明的 `rate_limit`（部署的选择），或将来由实测触发密度导出的闭环。
    #[serde(default)]
    pub default_rate_limit: Option<RateLimitConfig>,
    pub hook_timeout_secs: u64,
}

/// 去重窗口：一秒。
///
/// 窗口本身不是档位——它说的是「同一秒内的重复投递是同一次唤醒」，这是去重的
/// 定义；把窗口调大就变成「相邻两次合法的触发也只跑一次」。
// cap: fixed — 一秒：同一秒内的重复投递按同一次唤醒算，这是去重的定义
// not-a-bound: 一秒：同一秒内的重复投递按同一次唤醒算，这是去重的定义
pub const HOOK_DEDUP_WINDOW_SECS: u64 = 1;

impl Default for HookEngineConfig {
    fn default() -> Self {
        Self {
            dedup_window_secs: HOOK_DEDUP_WINDOW_SECS,
            default_rate_limit: None,
            // 无界是缺省态，不是这条界的一个取值。
            // cap: fixed — 0 ＝「没声明」的哨兵：没写 `timeout_ms` 的钩子跑到完
            // not-a-bound: 0 ＝「没声明」的哨兵：没写 `timeout_ms` 的钩子跑到完
            hook_timeout_secs: 0,
        }
    }
}

/// Per-hook rate-limit configuration (token bucket).
///
/// 两格都必须写：一个只写半边的限流会让另一半落到某个谁也没声明过的数上。
/// 不声明限流就是不限流，这条路径经过 `Option::None`，不经过任何缺省值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub burst: u32,
    pub per_second: u32,
}

/// `AgentEvent` 广播通道的缓冲深度：慢订阅者能落后多少条。
// cap: fixed — 由订阅者的处理时长 × 事件到达率定：落后超过这个深度就是丢事件
// keep: 删掉后通道无界，订阅者落后时事件堆在内存里；由「订阅者处理时长 × 到达率」兜住
const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// `TaskEvent` 广播通道的缓冲深度（同上；任务事件的到达率比代理事件低一档）。
// cap: fixed — 由订阅者的处理时长 × 事件到达率定：落后超过这个深度就是丢事件
// keep: 删掉后任务事件通道无界，订阅者落后时事件堆在内存里；由「落后超过这个深度就是丢事件」兜住
const DEFAULT_TASK_EVENT_CHANNEL_CAPACITY: usize = 256;

/// 动作规划器模式库的容量上限：检索只在一拍扫得完的范围里有意义。
// cap: fixed — 由一拍的检索预算与命中率的拐点定：再深只是把旧模式堆在库里
// keep: 删掉后模式库无界，旧模式一直堆着把检索预算吃完；由「检索预算与命中率的拐点」兜住
const DEFAULT_PATTERN_DB_MAX_SIZE: usize = 256;

/// PG 连接池上限。
///
/// 这不是本仓能单方面定的数：服务端的 `max_connections` 除以副本数才是天花板，
/// `cog-storage` 起池时已经在读那个上限并取两者的小值；这里的默认只是没读到
/// 服务端时的起点。
// cap: fixed — 由服务端 max_connections / 副本数 与一次查询的时延定
// keep: 删掉后回到「由服务端 max_connections / 副本数 与一次查询的时延定」所描述的情形；由这条约定兜住
const DEFAULT_PG_MAX_CONNECTIONS: u32 = 50;

// cap: fixed — 由稳态查询速率 × 一条查询的时延定：保住热连接，又不多占服务端的槽
// keep: 删掉后没有热连接，每个请求都付一次握手；由「稳态速率 × 一条查询的时延」兜住
const DEFAULT_PG_MIN_CONNECTIONS: u32 = 2;

/// 等一条连接的时间上限：超过就是这一请求失败了，等更久只是把失败推后。
// cap: fixed — 由一条请求的预算定：等连接的时间要短于整条请求的超时
// keep: 删掉后等连接可以长过整条请求的超时，请求死在等连接的队列里；由「等连接要短于整条请求的预算」兜住
// wall: wait=pool acquire; on-timeout=the request fails with a database error; resume=yes, the next request asks again
const DEFAULT_PG_ACQUIRE_TIMEOUT_SECS: u64 = 10;

/// 记忆消息后端广播通道的缓冲深度（同 `AgentEvent`）。
// cap: fixed — 由订阅者的处理时长 × 事件到达率定：落后超过这个深度就是丢事件
// keep: 删掉后广播无界，落后订阅者让内存跟着积压走；由「落后超过这个深度就是丢事件」兜住
const DEFAULT_MEMORY_MESSAGE_BROADCAST_CAPACITY: usize = 1024;

/// 观测网关事件通道的缓冲深度（同 `TaskEvent`）。
// cap: fixed — 由订阅者的处理时长 × 事件到达率定：落后超过这个深度就是丢事件
// keep: 删掉后可观测通道无界，观测自己成了内存压力；由「落后超过这个深度就是丢事件」兜住
const DEFAULT_OBSERVABILITY_EVENT_CHANNEL_CAPACITY: usize = 256;

/// WebSocket 连接管理器的事件缓存：一次重连要补回多少条。
// cap: fixed — 由一次重连的补发量与它的时长定：缓存短于补发量，重连就丢事件
// keep: 删掉后重连补发的量没有缓存可依，重连就丢事件；由「一次重连的补发量」兜住
const DEFAULT_WEBSOCKET_EVENT_CACHE_CAPACITY: usize = 1000;

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
    /// How many times in a row a background loop may be run again after
    /// panicking. 0 disables the restart entirely, which makes a panic as
    /// fatal as it was before the mechanism existed. A loop past its budget
    /// is left dead, and the death counter and the stall rule report it.
    pub loop_restart_max_consecutive: u32,
    /// Floor for the wait before running a panicked loop again (seconds). A
    /// loop with a cadence waits by its own period; only event-driven loops
    /// have no period and take this floor. The wait doubles per attempt and
    /// is capped at six times this value.
    pub loop_restart_backoff_floor_secs: u64,
    /// Interval between monthly-partition maintenance rounds (seconds).
    pub partition_maintenance_interval_secs: u64,
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
    /// Explicitly enabled system plugins. When `None`, all registered plugins are loaded.
    /// When `Some(list)`, only plugins whose names appear in the list are initialised.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled_plugins: Option<Vec<String>>,
    /// Explicitly disabled system plugins. Applied when `enabled_plugins` is `None`.
    /// Plugins in this list are excluded from initialisation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_plugins: Vec<String>,
}

/// Anthropic 模型的输出长度：模型卡上的 `max_tokens`。
///
/// 不是本仓的偏好——同一条事实在模型名册里也写着（`cog_llm` 的
/// `default_model_for`），两处各写一个 4096 就是同一件事的两次声明。
// cap: fixed — 模型卡：Claude 一代模型的 `max_tokens` 是 4096
// not-a-bound: 模型卡：Claude 一代模型的 `max_tokens` 是 4096
pub const ANTHROPIC_OUTPUT_TOKEN_DEFAULT: u32 = 4096;

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            event_channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
            task_event_channel_capacity: DEFAULT_TASK_EVENT_CHANNEL_CAPACITY,
            shutdown_timeout_ms: (30 * crate::day::MILLIS_PER_SECOND) as u64,
            timeout_checker_interval_secs: (crate::day::SECONDS_PER_MINUTE / 2) as u64,
            loop_restart_max_consecutive: crate::contract::loop_health::RestartSettings::default()
                .max_consecutive,
            loop_restart_backoff_floor_secs:
                crate::contract::loop_health::RestartSettings::default().backoff_floor_secs,
            partition_maintenance_interval_secs: crate::day::SECONDS_PER_HOUR as u64,
            sandbox_executor_url: None,
            require_tool_identity: false,
            pattern_db_max_size: DEFAULT_PATTERN_DB_MAX_SIZE,
            pattern_max_age_days: crate::day::DAYS_PER_MONTH,
            pg_max_connections: DEFAULT_PG_MAX_CONNECTIONS,
            pg_min_connections: DEFAULT_PG_MIN_CONNECTIONS,
            pg_acquire_timeout_secs: DEFAULT_PG_ACQUIRE_TIMEOUT_SECS,
            pg_idle_timeout_secs: (10 * crate::day::SECONDS_PER_MINUTE) as u64,
            memory_message_broadcast_capacity: DEFAULT_MEMORY_MESSAGE_BROADCAST_CAPACITY,
            observability_event_channel_capacity: DEFAULT_OBSERVABILITY_EVENT_CHANNEL_CAPACITY,
            websocket_event_cache_capacity: DEFAULT_WEBSOCKET_EVENT_CACHE_CAPACITY,
            skill_hot_reload_interval_secs: SKILL_HOT_RELOAD_INTERVAL_SECS,
            anthropic_default_max_tokens: ANTHROPIC_OUTPUT_TOKEN_DEFAULT,
            strict_persistence: true,
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

/// 总线故障窗里重试写回总线的间隔。
// cap: fixed — 由总线恢复时长定：间隔要短于故障窗，长了缓冲先满
// keep: 删掉后总线故障窗内重试要么太密、要么缓冲先满；由「间隔要短于故障窗」兜住
const BUS_RETRY_INTERVAL_SECS: u64 = 5;

/// 持久总线写不进去时，进程内兜底的缓冲深度。
///
/// 它兜的是「总线故障窗」：深度 × 到达速率要盖住那段窗，否则兜底先丢事件。
// cap: fixed — 由事件到达速率 × 总线故障窗定：窗变长或速率变高都要一起抬
// keep: 删掉后故障窗内的发布无界，内存跟着总线故障的时长走；由「到达速率 × 总线故障窗」兜住
const BUS_PUBLISH_BUFFER_CAPACITY: usize = 256;

impl Default for MultiBackendConsumerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channel: "cogneva-events".into(),
            group: "cogneva-consumer".into(),
            retry_interval_secs: BUS_RETRY_INTERVAL_SECS,
            events_on_bus: false,
            nats_urls: Vec::new(),
            publish_buffer_capacity: BUS_PUBLISH_BUFFER_CAPACITY,
            publish_retry_base_delay_ms: crate::day::MILLIS_PER_SECOND as u64,
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
    ///
    /// 同样由部署面声明，代码里不给默认值：集群内的仓库地址是部署事实，写进
    /// 二进制就跟着部署走不动，而空串＝没声明、构建前报错。
    pub image_repo: String,
    /// 镜像内基础镜像（COPY 已编译二进制）：必须是正式 cogneva 运行镜像
    /// （WebUI/skills/migrations/动态库齐全，glibc 版本匹配），不能用
    /// debian-slim 这类裸基底——二进制启动即缺库崩。
    ///
    /// 由部署面声明，代码里不给默认值：默认一个集群内地址等于把一个部署事实
    /// 抄进二进制，`--set` 改不动它；空串＝没声明，构建前报错。
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
            image_repo: String::new(),
            base_image: String::new(),
            builder_bin: "buildah".into(),
            registry_push: false,
            kubectl_bin: "kubectl".into(),
            namespace: crate::topology::namespace(),
            deployment: crate::topology::MAIN_DEPLOYMENT.into(),
            container: crate::topology::MAIN_CONTAINER.into(),
            rollout_timeout_secs: (5 * crate::day::SECONDS_PER_MINUTE) as u64,
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
}

/// 微虚拟机给几个核、多少内存。
///
/// 这是 guest 的规格：由「一次进化任务在 guest 里跑得动」这条要求定，同时受宿主
/// 余量约束——给得比宿主的空闲还大，实例根本起不来。
// cap: fixed — 由一次进化任务的资源需求与宿主余量定：超过宿主的空闲就起不来
// keep: 删掉后微虚机可以超出宿主空闲，进化任务起不来；由「一次任务的资源需求与宿主余量」兜住
const MICROVM_VCPU_COUNT: u32 = 2;

// cap: fixed — 同上：由任务的内存峰值与宿主余量定
// keep: 删掉后内存没有上限，任务峰值把宿主余量吃穿；由「任务的内存峰值与宿主余量」兜住
const MICROVM_MEM_SIZE_MIB: u32 = 2048;

impl Default for MicroVmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            firecracker_bin: "firecracker".into(),
            kernel_image: crate::paths::app_path_str("microvm/vmlinux"),
            rootfs_image: crate::paths::app_path_str("microvm/rootfs.ext4"),
            pv_image: crate::paths::app_path_str("microvm/evolution-pv.ext4"),
            vcpu_count: MICROVM_VCPU_COUNT,
            mem_size_mib: MICROVM_MEM_SIZE_MIB,
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off init=/evolution/init".into(),
            instance_root: "/tmp/cogneva-microvm".into(),
        }
    }
}

/// 工作区动态分配配置。取消共用固定树后，部署器与每个进化任务各自从裸仓库
/// 检出 `git worktree`；本段决定这些检出去哪里、编译产物缓存在哪。
/// How the shared build cache's cap is set.
///
/// Three states because "the number is the deployment's" and "the number is a
/// reading" are different facts that a bare integer cannot tell apart:
///
/// - `0` (or absent) leaves the cache measured and unbounded.
/// - a positive number is a cap the deployment names, and it does not move.
/// - `"auto"` makes the cap a reading: the pre-reclaim working-set peak over the
///   last week, with headroom, held under the free space of the volume the
///   directory sits on. The published cap then carries `source="derived"`, so a
///   cap that stopped tracking its volume can never read like one that is still
///   tracking it.
///
/// The sentinel is a string rather than a reserved number because every number
/// in this field is a legitimate cap; the only way to say "not a number" is to
/// say something that is not a number.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheCapSetting {
    /// Measured, not bounded.
    #[default]
    Unbounded,
    /// A cap the deployment names.
    Fixed(u64),
    /// A cap the walk derives from what it measures.
    Auto,
}

impl CacheCapSetting {
    /// The bytes the deployment named, or `None` when it named none.
    pub fn fixed_bytes(self) -> Option<u64> {
        match self {
            Self::Fixed(bytes) => Some(bytes),
            Self::Unbounded | Self::Auto => None,
        }
    }

    /// Whether the cap is a reading of the cache rather than a number.
    pub fn is_auto(self) -> bool {
        matches!(self, Self::Auto)
    }
}

impl Serialize for CacheCapSetting {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unbounded => serializer.serialize_u64(0),
            Self::Fixed(bytes) => serializer.serialize_u64(*bytes),
            Self::Auto => serializer.serialize_str("auto"),
        }
    }
}

impl<'de> Deserialize<'de> for CacheCapSetting {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SettingVisitor;

        impl serde::de::Visitor<'_> for SettingVisitor {
            type Value = CacheCapSetting;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a byte count, or \"auto\" to derive the cap from the measured working set",
                )
            }

            fn visit_u64<E: serde::de::Error>(self, bytes: u64) -> Result<Self::Value, E> {
                Ok(if bytes == 0 {
                    CacheCapSetting::Unbounded
                } else {
                    CacheCapSetting::Fixed(bytes)
                })
            }

            fn visit_i64<E: serde::de::Error>(self, bytes: i64) -> Result<Self::Value, E> {
                if bytes < 0 {
                    Err(E::custom("a cache cap cannot be negative"))
                } else {
                    self.visit_u64(bytes as u64)
                }
            }

            fn visit_f64<E: serde::de::Error>(self, bytes: f64) -> Result<Self::Value, E> {
                if bytes < 0.0 || bytes.fract() != 0.0 {
                    Err(E::custom("a cache cap is a whole number of bytes"))
                } else {
                    self.visit_u64(bytes as u64)
                }
            }

            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
                let text = text.trim();
                if text.eq_ignore_ascii_case("auto") {
                    return Ok(CacheCapSetting::Auto);
                }
                match text.parse::<u64>() {
                    Ok(bytes) => self.visit_u64(bytes),
                    Err(_) => Err(E::custom(format!(
                        "`{text}` is neither a byte count nor \"auto\""
                    ))),
                }
            }
        }

        deserializer.deserialize_any(SettingVisitor)
    }
}

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
    /// Apparent bytes the shared build cache may hold, or 0 to leave it
    /// unbounded.
    ///
    /// A cap rather than a retention age, because growth is what fills the
    /// volume the cache sits on and a build that fails on a full disk is how
    /// that is found out otherwise. When the cache is over it, the bytes that
    /// cost a build only time are given up first (`tmp`, `incremental`,
    /// `build`) and the compiled results after them — an enforced cap is worth a
    /// recompile. Removal happens only while the build gate holds the slot, so
    /// no build can be reading a file the pass is about to drop.
    ///
    /// 0 by default: the number belongs to the deployment, derived from the
    /// volume behind the directory, and a default that started removing bytes on
    /// upgrade would be a deployment's decision made silently by whoever wrote
    /// the default. The cache is measured either way, and the readings say which
    /// of the two states a cache is in.
    ///
    /// `"auto"` is the state where the deployment has said the number should be
    /// a reading rather than a decision: the walk keeps the pre-reclaim
    /// working-set peak and derives the cap from it, which is the only form in
    /// which a cap can stop being correct the moment the working set outgrows
    /// it while still being reported as a number.
    pub target_max_bytes: CacheCapSetting,
}

impl Default for SelfEvolutionWorkspaceConfig {
    fn default() -> Self {
        Self {
            root: crate::paths::app_path_str("sandbox/workspaces"),
            target_dir: crate::paths::app_path_str("sandbox/src/target"),
            // 两个寿命按「几小时 / 几天」写：一个 21600 读不出它是六小时。
            // cap: fixed — 一次演化运行的最长寿命：跑过六小时的沙箱按「死在半路」收；分支留三十天是仓库的保留约定
            // keep: 删掉后沙箱没有最长寿命，跑过六小时的演化运行永远占着树；由「跑过六小时的按死在半路收」兜住
            ephemeral_ttl_secs: 6 * crate::day::SECONDS_PER_HOUR as u64,
            // cap: fixed — 与上一格同一档：一条被丢下的演化分支留三十天，够人认领，不会长到占满命名空间
            // keep: 删掉后被丢下的演化分支永远留着，长到占满命名空间；由「留三十天够人认领」兜住
            orphan_branch_ttl_secs: 30 * crate::day::SECONDS_PER_DAY as u64,
            // 与体积测量的节拍同一个数：缓存多久重走一遍，两处不该各说各的。
            cache_scan_interval_secs: crate::claim_footprint::DEFAULT_SCAN_INTERVAL_SECS,
            target_max_bytes: CacheCapSetting::Unbounded,
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
/// 条数**默认由实测导出**（宿主自己的 cpu / 内存 / 磁盘，加上构建所在容器
/// 自身的内存上限，取最紧的一项说几就几），随宿主状态在运行期移动，见
/// `build_gate` 模块。导出要读宿主的这些状态，所以 `lock_dir` 必须**是同一宿主
/// 上的所有构建方看到的同一个目录**，否则每个进程各卡各的，读数里那个 `dev:ino`
/// 就是用来分辨这件事的。这里说「同一宿主」而不是「整个部署」：界是按台算的，
/// 多节点上每个节点各有一份锁目录、各自按本节点的容量设界，那是对的，不是两个界
/// 在打架。容器那一项不是重复：宿主的余量再大，构建所在容器
/// 的上限也是硬墙，撞上去的后果是构建被杀。
///
/// `enabled=false` 时不设界。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BuildGateConfig {
    pub enabled: bool,
    /// 同时允许的构建条数**上限**的覆盖位：**0 表示不要覆盖**（默认），
    /// 由宿主实测导出；设成正数则钉死这个数，不再看宿主。
    ///
    /// 覆盖位是给人用的，不是出厂值：它表示"这台机器我知道得比导出更清楚"
    /// （同一台宿主机上还跑着别的吃盘的东西、或者这台机器的盘比一般的好），
    /// 而导出的那几个维度都只看得到本机自己的状态。钉死的数不会随宿主移动，
    /// 读数里的 `dimension` 标签会写 `override`，而不是 cpu / memory / io /
    /// cgroup_memory。
    pub max_concurrent: usize,
    /// 一次性构建（变更验证这类没有下一轮可回的）愿意排队多久，超过即拒。
    /// 取 0 表示只试一次、不排队。
    pub wait_secs: u64,
    /// 槽位文件所在目录。必须是**同一宿主上所有构建方共享**的那一个目录
    /// （同一个 hostPath），否则闸门退化成每个进程各自为政。注意这里的构建方
    /// 不都是同一个用户：目录里每个名字都对所有构建方可写，所以文件操作一律
    /// 不跟随软链（见 `build_gate` 里打开这些文件的几个帮手），而目录本身
    /// 要允许每一个构建方创建文件。
    pub lock_dir: String,
    /// 一次构建在这台宿主上吃掉多少个核，跨整个构建期取中位数。
    ///
    /// 界以它为单位：每个维度回答的都是「我剩下的还装得下几个构建」，所以
    /// 这个数就是「一个构建」。本仓一次 release 构建（`cargo build --release
    /// --bin cogneva`，每 5 秒采样、14 分钟 31 个样本）中位 4.1 核、峰值 4.7；
    /// 取中位不取峰值——界问的是几个构建能**并排**跑，按峰值定会拒掉宿主其实
    /// 跑得动的构建。构建跑在 4 核配额的容器里，`cargo` 的 job 数随那个配额
    /// 走，所以这是**这套部署的属性**：给每个构建更大份额的部署要量出更大的数。
    ///
    /// 只量过 release 这一种构建；dev profile（clippy / test）驱动的是更少更小
    /// 的 `rustc`，它自己的序列还没量过。
    pub build_demand_cores: f64,
    /// 一次构建在这台宿主上占用的内存峰值，MiB。
    ///
    /// 同一个序列的高水位（4481，中位 3616），整棵进程树求和而不是只看
    /// `cargo` 进程——内存都在 `rustc` 子进程和测试二进制里。取峰值，因为
    /// 内存是唯一一个出错不是变慢而是丢命的维度：撞上限的容器会把构建杀掉。
    /// 跟 `MemAvailable` 比，不是跟 `MemFree` 比，构建自己填的页缓存也算可用。
    pub build_peak_rss_mib: u64,
    /// 除构建之外，这台宿主自己必须留住的内存，MiB。
    ///
    /// 集群的常驻工作集（本机：cogneva 17.4 + monitoring 2.1 + system 0.2 GiB）加宿主
    /// 自己的进程约 5 GiB，本机合计 25 GiB，线压在它下面一点。它**不能**从
    /// `MemAvailable` 的七日低点反读：一条不花内存的界正是让宿主停在自家地板
    /// 上的原因，反读量到的是这条界把宿主推到了哪，不是别人占了多少——在低点
    /// 落在线下的宿主上，闸门会从自己的常量推出一条「只许一个」的界，然后再也
    /// 不动。
    pub mem_floor_mib: u64,
    /// 所有构建加起来允许占用的磁盘停顿百分比。
    ///
    /// 构建与容器共用一块盘的宿主上，卡住的是这个维度。一次构建花它多少取决于
    /// 在做什么：已构建树的 release 构建上 `io some avg10` 读到 0.00-0.01%，
    /// 冷编译期同一读数到过 30-58%，七日最差 87%。预算压在「一次构建见过的
    /// 最高」之上、压在一次以上的合力之下，于是构建自己的噪声顶不住这条界。
    /// 用当下的停顿去除它，得到的就是这块盘此刻吸收得下几个构建。
    pub io_stall_budget_pct: f64,
    /// cpu 停顿低于这个百分比，算宿主有余量（七日中位 1.5%、最高 20%）。
    pub cpu_calm_pct: f64,
    /// 磁盘停顿低于这个百分比，算设备安静、界可以上抬（七日中位 0.1%）。
    ///
    /// 上抬要慢慢来、只在本就安静的设备上：判错的代价是不对称的。代价是
    /// 一个个构建运行期间宿主都不算安静，界只在构建之间的空隙里上抬。
    pub io_calm_pct: f64,
    /// 内存停顿低于这个百分比，算宿主有余量（七日中位 0%、最差 16%）。
    pub mem_calm_pct: f64,
    /// 持久化的界两次移动之间至少隔多久。
    ///
    /// 读数一直在采，决定不是：追着每个样本走的界，会被构建自己的编译期推动
    /// 每一次，而 `avg60` 窗口和这一条就是给那场追逐做的阻尼。构建是几十分钟
    /// 量级的事件，一分钟的滞后换稳定性是划算的。
    pub bound_window_secs: u64,
    /// 读 cpu 在忙什么时的两次读间隔。
    ///
    /// 计数器是开机以来的累计值，"cpu 忙不忙"只有差值才回答得了。这里不用
    /// load average：一个卡在磁盘上的宿主机 load 能到二十而核全空，而这个维度
    /// 问的是核。
    pub cpu_sample_gap_ms: u64,
    /// 一个"下一个构建归我"的声明，最后一次刷新之后还能算数多久。
    ///
    /// 比刷新它的那些周期长（部署器的是十分钟），活着的等待者不会在两次询问
    /// 之间丢掉自己那一轮；又短到一个不再询问的等待者在同量级时间内把位置
    /// 还回来。
    pub reserve_fresh_secs: u64,
}

// 构建闸门的九个出厂读数：每一个都是这台宿主的测量值，口径写在各字段上
// （取中位还是峰值、为什么不用 load average、地板为什么不能从 MemAvailable
// 反读）。写成具名常量是为了让「读数」有一个可被引用的落点——读数变了改这里，
// 而不是在 `Default` 里改一个谁也叫不出名字的数字。

// cap: fixed — 一次构建同时吃几个核（实测取中位）；换机器要重量，读侧只压不抬
// keep: 删掉后构建门没有单次构建的核需求，准入按拍脑袋的并发算；由「实测取中位」兜住
const BUILD_GATE_DEMAND_CORES: f64 = 4.1;

// cap: fixed — 一次构建的峰值 RSS（实测取峰值）；换机器要重量，读侧只压不抬
// keep: 删掉后构建门没有峰值内存，准入把两个构建塞进一个装不下它们的槽里；由「实测取峰值」兜住
const BUILD_GATE_PEAK_RSS_MIB: u64 = 4481;

// cap: fixed — 开工前要留的空闲内存（宿主读数）；写成 GiB 换算而不是一个孤零零的 MiB 数
// not-a-bound: 开工前要留的空闲内存（宿主读数）；写成 GiB 换算而不是一个孤零零的 MiB 数
const BUILD_GATE_MEM_FLOOR_MIB: u64 = 24 * 1024;

// cap: fixed — IO 停顿占一拍的预算比例（宿主读数）：超过它就是这台机器的盘跟不上
// keep: 删掉后没有「盘跟不上」的判据线，IO 停顿多少都放行；由「IO 停顿占一拍的预算」兜住
const BUILD_GATE_IO_STALL_BUDGET_PCT: f64 = 70.0;

// cap: fixed — 「安静」的判定线（宿主读数）：CPU 占用低于它算一拍空闲
// keep: 删掉后「安静」没有判定线，忙的机器被读成空；由「CPU 占用低于它算一拍空闲」兜住
const BUILD_GATE_CPU_CALM_PCT: f64 = 5.0;

// cap: fixed — 「安静」的判定线（宿主读数）：IO 停顿低于它算一拍空闲
// keep: 删掉后 IO 停顿多少都算安静，忙的盘被读成空；由「IO 停顿低于它算一拍空闲」兜住
const BUILD_GATE_IO_CALM_PCT: f64 = 1.0;

// cap: fixed — 「安静」的判定线（宿主读数）：内存占用低于它算一拍空闲
// keep: 删掉后内存占用多少都算安静，紧的内存被读成空；由「内存占用低于它算一拍空闲」兜住
const BUILD_GATE_MEM_CALM_PCT: f64 = 1.0;

// cap: fixed — 两次采样之间的最小间隔（宿主读数）：密到采样本身成了负载就没有意义
// keep: 删掉后采样密到采样本身成了负载；由「两次采样之间的最小间隔」兜住
const BUILD_GATE_CPU_SAMPLE_GAP_MS: u64 = 200;

impl Default for BuildGateConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // 不覆盖：装机时不认识这台机器，而"1"是一个跟宿主容量无关的常量。
            // cap: fixed — 0 ＝ 不覆盖，真正的并发由装机时对这台机器的读取决定
            // keep: 删掉后 0 ＝ 不覆盖的语义消失，装机读取被写死成一个数；由「真正的并发由装机时对这台机器的读取决定」兜住
            max_concurrent: 0,
            wait_secs: (30 * crate::day::SECONDS_PER_MINUTE) as u64,
            // 与部署清单挂的那条 hostPath 同名：哪一层（配置文件、环境变量、
            // 默认值）最后说话，落点都是同一个字符串。默认值本身不保证共享——
            // 目录只有在部署真的把它挂出来时才是同一块。
            lock_dir: crate::paths::BUILD_GATE_LOCK_DIR.into(),
            // 下面十个数是出厂值，不是定律：它们全是这套部署的宿主的读数，
            // 换一台机器就该重量一遍、在这里改掉。写在配置文件里是因为它们
            // 是「这台机器是什么样」的声明——宿主容量、构建开销、多安静算安静
            // ——而一条界只有在说得出它按什么定的时候才站得住。写死的常量做
            // 不到这件事：它在别的机器上照样成立，只是错的。
            //
            // 实测口径记在各自字段上。取中位/峰值、为什么不用 load average、
            // 为什么地板不能从 MemAvailable 反读，都在那里。
            //
            // 其中两个数描述的是机器而不是构建——一个构建吃几个核、开工前
            // 要有多少空闲内存——所以读到它们的那一层会把它们压到本机付得起
            // 的范围里（核数、MemTotal 减去一个构建的峰值），只压不抬，并在
            // `source` 标签上写 `fitted`。搬到更小的机器上，这两个出厂值不会
            // 变成两条永远清不掉的墙，也不会被当成有人选过的数。
            build_demand_cores: BUILD_GATE_DEMAND_CORES,
            build_peak_rss_mib: BUILD_GATE_PEAK_RSS_MIB,
            mem_floor_mib: BUILD_GATE_MEM_FLOOR_MIB,
            io_stall_budget_pct: BUILD_GATE_IO_STALL_BUDGET_PCT,
            cpu_calm_pct: BUILD_GATE_CPU_CALM_PCT,
            io_calm_pct: BUILD_GATE_IO_CALM_PCT,
            mem_calm_pct: BUILD_GATE_MEM_CALM_PCT,
            bound_window_secs: crate::day::SECONDS_PER_MINUTE as u64,
            cpu_sample_gap_ms: BUILD_GATE_CPU_SAMPLE_GAP_MS,
            reserve_fresh_secs: (15 * crate::day::SECONDS_PER_MINUTE) as u64,
        }
    }
}

/// 技能目录多久扫一次改动。
///
/// 扫描是读目录、比 mtime，不花模型调用；三十秒是「改完技能到生效」的可接受
/// 延迟与一次空扫的代价之间的那一档。技能注册表与 core 的运行时配置读同一份。
// cap: fixed — 热加载节拍：半分钟
// keep: 删掉后技能热加载没有节拍，要么每拍扫盘、要么改了不生效；由「半分钟」兜住
pub const SKILL_HOT_RELOAD_INTERVAL_SECS: u64 = 30;

/// 换版探活的三个数：启动宽限、探测节拍、连败几次判死。
///
/// 三者是同一件事的三个量：新二进制从 execve 到能答 `/health` 之间必须先给一段
/// 宽限，之后按节拍问，连败到底就换回去。判死的总时长是节拍 × 次数（5s × 6 =
/// 30s），与一次换版的观测窗同量级——宽限比它长、或总时长短到一次慢启动就被判
/// 死，都会让「换版没起来」和「换版还在起」在读数上同形。
// cap: fixed — 探活口径：十秒宽限、五秒一探、六次判死
// not-a-bound: 探活口径：十秒宽限、五秒一探、六次判死
pub const HEALTH_CHECK_GRACE_SECS: u64 = 10;
// cap: fixed — 同上：探测节拍五秒
// keep: 删掉后探测没有节拍，健康状态要么过时要么每拍都探；由「探测节拍五秒」兜住
pub const HEALTH_CHECK_INTERVAL_SECS: u64 = 5;
// cap: fixed — 同上：连败六次判死（合计约三十秒）
// keep: 删掉后一次探测失败就判死，滚动中的 Pod 被误杀；由「连败六次约三十秒」兜住
pub const HEALTH_CHECK_FAILURES_BEFORE_DEAD: u32 = 6;

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
    /// The command that judges a generated change, as argv — a program and its
    /// arguments, not a shell line, so a configured value cannot become one.
    /// The default is this workspace's own suite; a project that is not a Rust
    /// workspace sets its own, and that is what makes the verification gate a
    /// property of the project rather than of cogneva. It must name a program:
    /// an empty list is refused where it is read, because a change accepted
    /// with no command run is not a change that passed anything.
    pub test_command: Vec<String>,
    /// The command that answers whether a change's tree is what this project's
    /// formatter produces, as argv — program and arguments, not a shell line,
    /// so a configured value cannot become one.
    ///
    /// Omitted: this workspace's own formatter, the invocation every deployment
    /// ran while this was hardcoded. That one is a pair of commands, because
    /// cargo fmt can rewrite the tree it is asked about; a change that arrives
    /// unformatted is conformed here rather than sent back a round for it.
    /// Declared: this project's formatter, run as given, and as a check only —
    /// a declared command is never assumed to be able to rewrite anything, so
    /// the tree it is asked about has to already be what it produces.
    /// Empty: this project has no formatter. The stage does not run, and the
    /// change's own evidence says the stage did not run, because a judge that
    /// never spoke and one that spoke and found nothing are different readings.
    pub fmt_check_command: Option<Vec<String>>,
    /// The executable admission conditions for a change that touches the
    /// deployment topology (`deploy/**`), as commands this deployment declares.
    ///
    /// Omitted: the family has no executable condition, and a change touching
    /// it is refused by name. That refusal is deliberate — "wait for a person"
    /// is not a condition, and a ticket no one owns neither lands nor closes.
    /// Declared: `commands` run against the applied tree and all must pass;
    /// `server_dry_run` is the argv prefix of a cluster-side dry run, with the
    /// rendered file the change touched appended as its last argument.
    pub deploy_admission: Option<DeployAdmissionConfig>,
    /// The executable admission conditions for a change that touches the WebUI
    /// source (`web/src/**`), as commands this deployment declares.
    ///
    /// The same shape as [`Self::deploy_admission`] and for the same reason: a
    /// frontend change has an executable answer — the type checker and the
    /// bundler accept it or they do not — so it is judged by running them on
    /// the applied tree rather than by waiting for a person.
    ///
    /// Omitted: the family has no condition here, and a change touching it is
    /// refused by name. That is the state of a deployment whose judging
    /// container has no JavaScript toolchain, and the refusal says so instead
    /// of admitting a change nothing can build.
    pub web_admission: Option<WebAdmissionConfig>,
    /// The command that lints a change's tree, as argv.
    ///
    /// Omitted: this workspace's own linter. It is the only one whose findings
    /// can be compared against the lines the change wrote, because that
    /// comparison reads the compiler's diagnostic interchange.
    /// Declared: this project's linter, whose exit status is the verdict on
    /// this tree. Its findings can be excused as the tree's own only when it
    /// speaks that same interchange; a linter that refuses a tree in any other
    /// form refuses the change, since nothing here can show the findings are
    /// not the change's.
    /// Empty: this project has no linter, recorded as the stage not running.
    pub lint_command: Option<Vec<String>>,
    pub poll_interval_secs: u64,
    /// 反思条目的归档与它的派生层（schema）是两次独立写，第二次失败或中途
    /// 重启会留下「归档在、按 schema 检索不到」的孤儿。这个间隔决定多久重扫
    /// 一次并补上缺口，0 表示关闭重扫。补齐只读本地归档、不调 LLM，所以上游
    /// 断供时也该照跑。
    pub schema_repair_interval_secs: u64,
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

/// What a change that touches `deploy/**` has to clear before it may land.
///
/// Every entry is executable. The family is judged by running these, never by
/// asking someone: a topology change has an answer either way, and an answer
/// that depends on a person being present is not a condition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DeployAdmissionConfig {
    /// Commands run, in order, in the applied tree. Every one must succeed;
    /// the first failure is the refusal, with its output as the reason.
    ///
    /// Argv, not a shell line: a configured value does not become code the
    /// platform has to interpret, and a project declares what it runs rather
    /// than what a shell would do with a string.
    pub commands: Vec<Vec<String>>,
    /// argv prefix for the server-side dry run. Each rendered file the change
    /// touched is appended as the final argument, so the check is "does the
    /// cluster accept this exact object", not "is the file syntactically
    /// YAML". Omitted means the deployment has not enabled this check, and
    /// the family is admitted without it — a deployment that cannot reach its
    /// own API server from the judging process says so by leaving it out,
    /// rather than by the check failing every time.
    pub server_dry_run: Option<Vec<String>>,
}

impl DeployAdmissionConfig {
    /// Whether this declaration names anything that can actually run.
    ///
    /// An empty list, or a list holding an empty argv, is not a condition: it
    /// would admit every change while reading as "conditions declared".
    pub fn is_executable(&self) -> bool {
        let commands_ok = !self.commands.is_empty()
            && self.commands.iter().all(|c| !c.is_empty())
            && self.commands.iter().all(|c| !c[0].is_empty());
        let dry_run_ok = self
            .server_dry_run
            .as_ref()
            .is_none_or(|p| !p.is_empty() && !p[0].is_empty());
        commands_ok && dry_run_ok
    }
}

/// The executable admission conditions for the WebUI source family.
///
/// A separate type from [`DeployAdmissionConfig`] rather than a reused one: the
/// topology family's declaration carries a `server_dry_run` prefix, and a
/// frontend has no server-side object to dry-run. Reusing the type would have
/// left a field every reader of this one has to know is ignored, which is how a
/// later reader comes to believe it is honoured.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebAdmissionConfig {
    /// Commands run, in order, in the applied tree. Every one must succeed; the
    /// first failure is the refusal, with its output as the reason.
    ///
    /// Argv, not a shell line, on the same terms as
    /// [`DeployAdmissionConfig::commands`].
    pub commands: Vec<Vec<String>>,
}

impl WebAdmissionConfig {
    /// Whether this declaration names anything that can actually run, on the
    /// same terms as [`DeployAdmissionConfig::is_executable`].
    pub fn is_executable(&self) -> bool {
        !self.commands.is_empty()
            && self.commands.iter().all(|c| !c.is_empty())
            && self.commands.iter().all(|c| !c[0].is_empty())
    }
}

impl SelfEvolutionConfig {
    /// The command that judges a change when the deployment names none.
    ///
    /// It is a function rather than a literal in `Default` so that the
    /// serialized change-execution world — built in one process, run in
    /// another — can default to the same command instead of carrying a second
    /// copy of it; two copies are two values that can drift, and the drift
    /// would be invisible until a deployment named one and ran the other.
    ///
    /// `--no-fail-fast` because the verdict is read by whoever investigates a
    /// rejection: stopping at the first failing crate hides the rest of the
    /// failure surface and makes an environmental problem look like the only
    /// problem.
    pub fn default_test_command() -> Vec<String> {
        vec![
            "cargo".into(),
            "test".into(),
            "--workspace".into(),
            "--no-fail-fast".into(),
        ]
    }
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
            binary_dir: crate::paths::app_path_str("bin"),
            backup_dir: crate::paths::app_path_str("bin/backups"),
            switch_mode: "self_exec".into(),
            health_check_grace_period_secs: HEALTH_CHECK_GRACE_SECS,
            health_check_interval_secs: HEALTH_CHECK_INTERVAL_SECS,
            health_check_max_retries: HEALTH_CHECK_FAILURES_BEFORE_DEAD,
            test_command: Self::default_test_command(),
            fmt_check_command: None,
            lint_command: None,
            deploy_admission: None,
            web_admission: None,
            poll_interval_secs: crate::day::SECONDS_PER_MINUTE as u64,
            schema_repair_interval_secs: (10 * crate::day::SECONDS_PER_MINUTE) as u64,
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
                registration_ttl_secs: AGENT_REGISTRATION_TTL_SECS,
                heartbeat_interval_secs: AGENT_HEARTBEAT_INTERVAL_SECS,
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
/// Intermediate objects are created automatically if missing or `null`.
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
    // `null` is how a section the type leaves unset appears in the merged
    // document — an `Option` that is `None` serializes to it. "Not configured"
    // and "absent" are the same fact, so a path through it has to be able to
    // build the object; refusing drops the write without a word and leaves the
    // deployment believing it turned a knob. This has to happen here, before
    // the descent decides anything: the write itself arrives as a leaf, and a
    // leaf cannot be put into a `null`. A numeric segment is not that case — a
    // `null` where an array belongs stays a no-op, because arrays are never
    // grown.
    if current.is_null() && head.parse::<usize>().is_err() {
        *current = serde_json::Value::Object(serde_json::Map::new());
    }
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

// ─── Board WIP limits ──────────────────────────────────────────────────────

/// Environment variable holding the board's per-column work-in-progress limits.
///
/// Format: `column=limit[,column=limit]`, columns named by the board's own
/// status vocabulary (`scheduled`, `running` are the admissible ones; see
/// [`BoardWipLimits::from_spec`] for why the other four are refused). Unset
/// means **no limit is configured** — not "zero", and not a platform default:
/// an invented default is a hard admission cap nobody chose.
pub const BOARD_WIP_LIMITS_ENV: &str = "COGNEVA_BOARD_WIP_LIMITS";

/// The board's per-column work-in-progress limits.
///
/// This lives in core because two processes have to answer "what is this
/// column's limit" the same way: the publisher, which refuses to push more work
/// into a full column, and the board reading, which reports the column's count
/// against it. Two parsers would be two rulers for one decision, and the board
/// would sooner or later report a limit the publisher never read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoardWipLimits {
    limits: std::collections::BTreeMap<String, usize>,
}

impl BoardWipLimits {
    /// Parse a `column=limit,…` specification.
    ///
    /// Returns the limits plus one line per entry that could not be used. The
    /// problems are returned rather than logged here so the caller can report
    /// them where its own readers look; dropping a typo'd entry silently would
    /// leave a configuration that reads like a limit and blocks nothing.
    ///
    /// A limit of `0` is refused rather than honoured. "Zero may pass" is a
    /// freeze of the whole column, which is far more often a typo for
    /// "unlimited" than an intention — and one of the two misreadings stops
    /// dispatch across the platform.
    ///
    /// Only `scheduled` and `running` are accepted. The publisher's transition
    /// is `pending` → `scheduled`, so a limit on `pending` would refuse the very
    /// publish that empties it: the column would sit at its cap and never move.
    /// The three terminal columns only ever grow, so their limit would stop
    /// dispatch for good the first time it was reached. Both are reported as
    /// problems rather than honoured, because a board that quietly freezes is
    /// worse than one that refused the setting out loud.
    pub fn from_spec(spec: &str) -> (Self, Vec<String>) {
        let mut limits = std::collections::BTreeMap::new();
        let mut problems = Vec::new();
        for entry in spec.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((column, limit)) = entry.split_once('=') else {
                problems.push(format!("`{entry}` is not `column=limit`"));
                continue;
            };
            let column = column.trim().to_ascii_lowercase();
            let Some(status) = crate::TaskStatus::parse(&column) else {
                problems.push(format!(
                    "`{column}` is not a board column; the limitable ones are scheduled and running"
                ));
                continue;
            };
            let column = status.as_str().to_string();
            if status == crate::TaskStatus::Pending {
                problems.push(format!(
                    "`{column}` is the intake column and publishing is what drains it: a limit \
                     there would stop the queue that would relieve it"
                ));
                continue;
            }
            if status.is_terminal() {
                problems.push(format!(
                    "`{column}` only grows, so a limit there would stop dispatch for good once \
                     reached"
                ));
                continue;
            }
            match limit.trim().parse::<usize>() {
                Ok(0) => problems.push(format!(
                    "`{column}=0` would freeze the column; leave the column out to leave it unlimited"
                )),
                Ok(n) => {
                    limits.insert(column, n);
                }
                Err(_) => problems.push(format!(
                    "`{}` is not a count of tasks",
                    limit.trim()
                )),
            }
        }
        (Self { limits }, problems)
    }

    /// The limits configured for this process's environment.
    pub fn from_env() -> (Self, Vec<String>) {
        match std::env::var(BOARD_WIP_LIMITS_ENV) {
            Ok(raw) => Self::from_spec(&raw),
            Err(_) => (Self::default(), Vec::new()),
        }
    }

    /// This column's limit, or `None` when the column was left out.
    ///
    /// `None` is "no limit", never `0`: a caller that read absence as a zero
    /// cap would stop every column nobody configured.
    pub fn limit_for(&self, column: &str) -> Option<usize> {
        self.limits.get(column).copied()
    }

    /// Every configured `(column, limit)`, in the board's column order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, usize)> + '_ {
        crate::TaskStatus::ALL.iter().filter_map(move |s| {
            self.limits
                .get(s.as_str())
                .map(|limit| (s.as_str(), *limit))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.limits.is_empty()
    }
}

// 等于默认值的归档保留期不出现在序列化里：模板不必抄一遍代码自己给的答案。
fn is_the_default_archive_after_secs(v: &u64) -> bool {
    *v == default_archive_after_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 健康判据的三个阈值不是三个独立旋钮：没写的那几项跟着这次生效的心跳走，
    /// 换一个心跳它们就跟着换（这条测试红，就是有一个数又变回自己挑的了）。
    #[test]
    fn the_health_thresholds_follow_the_heartbeat_in_force() {
        let unset = HealthCheckerConfig::default();
        for beat in [5u64, 10, 20, 60] {
            assert_eq!(
                unset.effective_suspect_after_secs(beat),
                beat * SUSPECT_BEATS,
                "可疑没有跟着 {beat} 秒的心跳走"
            );
            assert_eq!(
                unset.effective_dead_after_secs(beat),
                beat * DEAD_BEATS,
                "死亡没有跟着 {beat} 秒的心跳走"
            );
            assert_eq!(
                unset.effective_stuck_after_secs(beat),
                beat * DEAD_BEATS * STUCK_AFTER_DEATHS,
                "卡住没有跟着 {beat} 秒的心跳走"
            );
        }
        // 部署写了的项不被心跳覆盖：它自己已经回答了这个问题。
        let stated = HealthCheckerConfig {
            suspect_after_secs: 7,
            dead_after_secs: 8,
            stuck_after_secs: 9,
        };
        assert_eq!(stated.effective_suspect_after_secs(60), 7);
        assert_eq!(stated.effective_dead_after_secs(60), 8);
        assert_eq!(stated.effective_stuck_after_secs(60), 9);
    }

    /// 心跳是注册 TTL 分出来的，不是第二个人挑的数：一个部署只该声明 TTL，
    /// 心跳跟着它。
    #[test]
    fn the_heartbeat_is_the_registration_ttl_spread_over_its_beats() {
        assert_eq!(
            AGENT_HEARTBEAT_INTERVAL_SECS,
            AGENT_REGISTRATION_TTL_SECS / BEATS_PER_REGISTRATION_TTL
        );
        // 两者都是常量，所以这是编译期就该成立的事：放在 const 块里，编译不过
        // 比这条测试红更早。`clippy::assertions_on_constants` 要的也正是这个形状。
        const { assert!(AGENT_HEARTBEAT_INTERVAL_SECS > 0) };
    }

    /// 没配就是没配：每一列都读成 `None`，不是 0。0 在调用点是一个具体的
    /// 判据（「一条也不许进」），没配是「没有任何东西在拦」。
    #[test]
    fn an_unset_board_limit_is_absent_and_never_zero() {
        let (limits, problems) = BoardWipLimits::from_spec("");
        assert!(problems.is_empty(), "{problems:?}");
        assert_eq!(limits.limit_for("running"), None);
        assert!(limits.is_empty());
    }

    /// 上限读出来就是人写的那个数，列的拼写大小写不敏感（配置是人手敲的）。
    #[test]
    fn configured_limits_read_back_by_column_name() {
        let (limits, problems) = BoardWipLimits::from_spec("running=4, Scheduled = 32");
        assert!(problems.is_empty(), "{problems:?}");
        assert_eq!(limits.limit_for("running"), Some(4));
        assert_eq!(limits.limit_for("scheduled"), Some(32));
        assert_eq!(limits.limit_for("pending"), None);
        let seen: Vec<(&str, usize)> = limits.iter().collect();
        assert_eq!(
            seen,
            vec![("scheduled", 32), ("running", 4)],
            "按看板的列序输出，同一份配置两次读出的顺序一致"
        );
    }

    /// 敲错的条目必须被点名。静默丢掉一条拼错的配置，留下的是「看起来设了上限、
    /// 实际上一条都没拦」——正是这份门存在要防的那种读数。
    #[test]
    fn a_misspelt_or_zero_entry_is_reported_rather_than_dropped() {
        let (limits, problems) = BoardWipLimits::from_spec("runnning=8, 0, running=0, running=x");
        assert!(limits.is_empty(), "四条都不该生效：{limits:?}");
        assert_eq!(problems.len(), 4, "{problems:?}");
        assert!(problems[0].contains("runnning"), "{problems:?}");
        assert!(problems[2].contains("freeze"), "{problems:?}");
    }

    /// 两头都配不上的列被明确拒绝，而不是收下后把看板冻住：`pending` 的限额会
    /// 拦住唯一能腾空它的那次发布（自锁），终态列的限额一旦达到就再也放不出活
    /// （永冻）。静默接受这两类配置，等于给运维一把「看起来在工作」的锁。
    #[test]
    fn a_limit_on_the_intake_or_a_terminal_column_is_refused_with_the_reason() {
        let (limits, problems) = BoardWipLimits::from_spec("pending=5, completed=100, running=4");
        assert_eq!(limits.limit_for("running"), Some(4));
        assert_eq!(limits.limit_for("pending"), None);
        assert_eq!(limits.limit_for("completed"), None);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].contains("intake"), "{problems:?}");
        assert!(problems[1].contains("only grows"), "{problems:?}");
    }

    /// The cache cap arrives as a string from the environment, through the same
    /// path every other setting takes: a byte count, `0` for "measure only",
    /// or `auto` for "derive it". The three have to survive that path, because
    /// a sentinel that turns into 0 on the way in is a cap that silently stops
    /// bounding anything.
    #[test]
    fn the_cache_cap_reads_a_count_zero_and_auto_from_the_environment() {
        let parse = |raw: &str| -> CacheCapSetting {
            let mut value = serde_json::to_value(Config::default()).unwrap();
            set_json_path(
                &mut value,
                "self_evolution.workspaces.target_max_bytes",
                raw,
            );
            let cfg: Config =
                serde_json::from_value(value).expect("the write keeps the tree loadable");
            cfg.self_evolution.workspaces.target_max_bytes
        };

        assert_eq!(parse("auto"), CacheCapSetting::Auto);
        assert_eq!(parse("AUTO"), CacheCapSetting::Auto);
        assert_eq!(parse("0"), CacheCapSetting::Unbounded);
        assert_eq!(parse("1073741824"), CacheCapSetting::Fixed(1_073_741_824));

        // A typo is refused rather than silently read as "unbounded": the one
        // thing a cap must never do is stop existing without saying so.
        let mut value = serde_json::to_value(Config::default()).unwrap();
        set_json_path(
            &mut value,
            "self_evolution.workspaces.target_max_bytes",
            "autp",
        );
        assert!(serde_json::from_value::<Config>(value).is_err());
    }

    /// A dot path through a section the type leaves unset has to land.
    ///
    /// An `Option` that is `None` serializes to `null`, so a field reached
    /// through it — `gateway.notification_dingtalk.webhook_url` beside the
    /// sibling that is already set — arrives at a `null` node rather than a
    /// missing one. Creating only on *missing* nodes dropped the write in
    /// silence: the variable parsed, applied nothing, and no reading said so.
    #[test]
    fn a_path_through_an_unset_section_lands() {
        let mut value = serde_json::to_value(Config::default()).unwrap();
        // The premise: this section really is null in the tree, not absent.
        assert_eq!(
            value_at_path(&value, "gateway.notification_dingtalk"),
            Some(&serde_json::Value::Null)
        );

        set_json_path(&mut value, "gateway.notification_dingtalk.webhook_url", "u");
        let cfg: Config = serde_json::from_value(value).expect("the write keeps the tree loadable");
        assert_eq!(
            cfg.gateway
                .notification_dingtalk
                .as_ref()
                .map(|c| c.webhook_url.as_str()),
            Some("u")
        );
        // The sibling is untouched: building through the null must not clear
        // the address that was already configured.
        assert!(cfg.gateway.notification_webhook_url.is_none());
    }

    /// A numeric segment is not the same case: arrays are never grown, so a
    /// path that expects an element where there is no array stays a no-op
    /// rather than turning the node into an object with a "0" key.
    #[test]
    fn a_numeric_segment_through_null_stays_a_no_op() {
        let mut value = serde_json::json!({ "backends": null });
        set_json_path(&mut value, "backends.0.base_url", "u");
        assert_eq!(value, serde_json::json!({ "backends": null }));
    }

    /// The value a document holds at a dot-path, if any.
    fn value_at_path<'a>(
        value: &'a serde_json::Value,
        path: &str,
    ) -> Option<&'a serde_json::Value> {
        let segments: Vec<&str> = path.split('.').collect();
        let (leaf, parents) = segments.split_last()?;
        let mut cursor = Some(value);
        for segment in parents {
            cursor = cursor.and_then(|c| c.get(*segment));
        }
        cursor.and_then(|c| c.get(*leaf))
    }

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

    /// 分解重试次数与结果认领批各只有一处声明：默认值读具名常量，常量上写着
    /// 它由什么钉住。
    #[test]
    fn the_dag_executor_retry_and_claim_defaults_read_their_names() {
        let cfg = DagExecutorConfig::default();
        assert_eq!(cfg.decomposition_max_attempts, DECOMPOSITION_MAX_ATTEMPTS);
        assert_eq!(cfg.result_claim_batch, RESULT_CLAIM_BATCH);
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

    /// 「没写」与「写成 0」都不许变成一条坏掉的界。这五个数曾经把 0 当默认值，而 0 在
    /// 读侧不是「不限」：`request_timeout_secs` 0 建一条 0 秒的超时期限（每个请求立刻
    /// 408），`websocket_timeout_secs` 0 让零周期的定时器构造即 panic，
    /// `websocket_inactivity_timeout_secs` 0 一连上就断，`notification_limit` 0 让每页
    /// 都是空的，`ready_task_poll_interval_secs` 0 被读侧夹到 1 秒成了忙轮询。现在
    /// 每个 0 都在读侧换成它的合法来源，没有一个是 0 秒的期限。
    #[test]
    fn an_unwritten_or_zero_gateway_bound_reads_the_built_in_value() {
        let drain_ms = SystemConfig::default().shutdown_timeout_ms;
        let absent: GatewayConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            absent.effective_websocket_heartbeat_secs(),
            DEFAULT_WEBSOCKET_HEARTBEAT_SECS
        );
        assert_eq!(
            absent.effective_websocket_inactivity_timeout_secs(),
            WEBSOCKET_BEATS_BEFORE_DROPPING * DEFAULT_WEBSOCKET_HEARTBEAT_SECS
        );
        assert_eq!(absent.effective_notification_limit(), A2A_LIST_PAGE_SIZE);
        assert_eq!(
            absent.effective_request_timeout_secs(drain_ms),
            drain_ms / 1000
        );

        let zeroed: GatewayConfig = serde_json::from_str(
            r#"{"websocket_timeout_secs": 0, "websocket_inactivity_timeout_secs": 0,
                "notification_limit": 0, "request_timeout_secs": 0}"#,
        )
        .unwrap();
        // 原始字段照文档说的存 0；读到界的地方换回它的来源，不是 0。
        assert_eq!(zeroed.request_timeout_secs, 0);
        assert_eq!(
            zeroed.effective_websocket_heartbeat_secs(),
            DEFAULT_WEBSOCKET_HEARTBEAT_SECS
        );
        assert_eq!(
            zeroed.effective_websocket_inactivity_timeout_secs(),
            WEBSOCKET_BEATS_BEFORE_DROPPING * DEFAULT_WEBSOCKET_HEARTBEAT_SECS
        );
        assert_eq!(zeroed.effective_notification_limit(), A2A_LIST_PAGE_SIZE);
        assert_eq!(
            zeroed.effective_request_timeout_secs(drain_ms),
            drain_ms / 1000
        );

        // 写了一个正数就照它来：生效值不是「总是那个兜底」。
        let set: GatewayConfig = serde_json::from_str(
            r#"{"websocket_timeout_secs": 7, "websocket_inactivity_timeout_secs": 21,
                "notification_limit": 9, "request_timeout_secs": 45}"#,
        )
        .unwrap();
        assert_eq!(set.effective_websocket_heartbeat_secs(), 7);
        assert_eq!(set.effective_websocket_inactivity_timeout_secs(), 21);
        assert_eq!(set.effective_notification_limit(), 9);
        assert_eq!(set.effective_request_timeout_secs(drain_ms), 45);
    }

    /// 不活跃期限跟的是**这次生效的心跳**，不是一条写死的倍数：心跳被部署改成
    /// 10 秒时，掉线期限跟着变 30 秒，而不是还停在按默认心跳算出来的 90 秒。
    #[test]
    fn the_inactivity_timeout_follows_the_effective_heartbeat() {
        let set: GatewayConfig = serde_json::from_str(r#"{"websocket_timeout_secs": 10}"#).unwrap();
        assert_eq!(
            set.effective_websocket_inactivity_timeout_secs(),
            WEBSOCKET_BEATS_BEFORE_DROPPING * 10
        );
        // 两边都写了就各按各的：跟着心跳只发生在「没写」这一半。
        let both: GatewayConfig = serde_json::from_str(
            r#"{"websocket_timeout_secs": 10, "websocket_inactivity_timeout_secs": 90}"#,
        )
        .unwrap();
        assert_eq!(both.effective_websocket_inactivity_timeout_secs(), 90);
    }

    /// 就绪轮询的 0 同样读作「没写」：读侧曾经把 0 夹到 1 秒，那是把「空转」当成
    /// 默认节拍。
    #[test]
    fn an_unwritten_ready_task_poll_reads_the_built_in_value() {
        let absent: DagExecutorConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            absent.ready_task_poll_interval_secs,
            DEFAULT_READY_TASK_POLL_INTERVAL_SECS
        );
        let zeroed: DagExecutorConfig =
            serde_json::from_str(r#"{"ready_task_poll_interval_secs": 0}"#).unwrap();
        assert_eq!(
            zeroed.effective_ready_task_poll_interval_secs(),
            DEFAULT_READY_TASK_POLL_INTERVAL_SECS
        );
        let set: DagExecutorConfig =
            serde_json::from_str(r#"{"ready_task_poll_interval_secs": 12}"#).unwrap();
        assert_eq!(set.effective_ready_task_poll_interval_secs(), 12);
    }

    /// 没写请求期限时，它取的就是**这次停机的排空时长**——部署把排空改成 60 秒，
    /// 没写期限的网关就等 60 秒，而不是回到一个写死的 30。
    #[test]
    fn an_unwritten_request_deadline_is_the_shutdown_drain() {
        let absent: GatewayConfig = serde_json::from_str("{}").unwrap();
        for drain_ms in [15_000u64, 30_000, 60_000] {
            assert_eq!(
                absent.effective_request_timeout_secs(drain_ms),
                drain_ms / 1000,
                "排空 {drain_ms} ms 时，没写的请求期限没有跟着它走"
            );
        }
        // 排空短到不足一秒时仍给一秒：0 秒的期限会让每个请求立刻 408。
        assert_eq!(absent.effective_request_timeout_secs(400), 1);
    }

    /// 控制台与服务端是同一条 socket 的两端：拍子、判死的先后都在这两份源码里。
    ///
    /// 控制台的 `HEARTBEAT_INTERVAL_MS` 与服务端心跳是同一个数；它的 `PONG_TIMEOUT_MS`
    /// （2 拍 ＋ 5 秒）必须**先于**服务端的不活跃期限触发，这样掉线由浏览器自己发现并
    /// 重连；反过来则是服务端先断一条浏览器还以为是活的会话，用户看到的是「连着但没反应」。
    #[test]
    fn the_console_and_the_gateway_beat_at_one_cadence() {
        let console = include_str!("../../../web/src/transport/websocket.ts");

        let marker = "HEARTBEAT_INTERVAL_MS =";
        let at = console
            .find(marker)
            .unwrap_or_else(|| panic!("控制台里找不到 `{marker}`：这条判据的锚点搬走了"));
        let digits: String = console[at + marker.len()..]
            .chars()
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        let console_heartbeat_ms: u64 = digits
            .parse()
            .unwrap_or_else(|_| panic!("`{marker}` 后面不是一个数：{digits:?}"));
        assert_eq!(
            console_heartbeat_ms,
            DEFAULT_WEBSOCKET_HEARTBEAT_SECS * 1000,
            "控制台心跳与服务端心跳不是同一个拍子"
        );

        // 控制台的放弃公式必须是「两拍 ＋ 5 秒」，服务端期限必须排在它后面。
        assert!(
            console.contains("EventStreamClient.HEARTBEAT_INTERVAL_MS * 2 + 5000"),
            "控制台的 `PONG_TIMEOUT_MS` 公式变了：这条判据要跟着重算"
        );
        let console_gives_up_ms = console_heartbeat_ms * 2 + 5000;
        let server_drops_ms =
            WEBSOCKET_BEATS_BEFORE_DROPPING * DEFAULT_WEBSOCKET_HEARTBEAT_SECS * 1000;
        assert!(
            server_drops_ms > console_gives_up_ms,
            "服务端不活跃期限 {server_drops_ms} ms 不晚于控制台放弃的 {console_gives_up_ms} ms：\
             掉线会由服务端先发现，浏览器看到的是「连着但没反应」"
        );
    }

    /// 就绪轮询的拍长就是消费者阻塞读一次的预算：一条新就绪的任务最多等到下一次读超时。
    ///
    /// 拍比它长时，读超时之后还要再等一拍才有人回来，那段空窗没人说得出为什么是
    /// 这么长；拍比它短则每一拍都写一条消费者还没读完的消息。
    #[test]
    fn the_ready_poll_is_one_blocking_read_budget() {
        assert_eq!(
            DEFAULT_READY_TASK_POLL_INTERVAL_SECS * 1000,
            DEFAULT_REDIS_READ_BLOCK_MS,
            "就绪轮询拍长与消费者阻塞读预算不再同值：这两处一个决定「多久看一眼」，\
             一个决定「一眼看多久」"
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

    /// The command that judges a change is configurable, and its default is the
    /// one every deployment ran while it was hardcoded. Both halves are the
    /// point: a project that is not a Rust workspace names its own suite, and a
    /// deployment that names none keeps behaving exactly as it did.
    #[test]
    fn the_test_command_defaults_to_the_workspaces_suite_and_can_be_set() {
        assert_eq!(
            SelfEvolutionConfig::default().test_command,
            ["cargo", "test", "--workspace", "--no-fail-fast"].map(String::from)
        );
        let configured: SelfEvolutionConfig =
            serde_json::from_value(serde_json::json!({ "test_command": ["pytest", "-q"] }))
                .expect("a configured command parses");
        assert_eq!(configured.test_command, ["pytest", "-q"].map(String::from));
        // A document that does not mention it still loads, because the struct
        // default fills it — the key must not be required.
        let unset: SelfEvolutionConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(
            unset.test_command,
            SelfEvolutionConfig::default_test_command()
        );
    }

    /// The formatter and linter are configurable the same way, with the same
    /// default: omitted means this workspace's own, which is what every
    /// deployment ran while the stage was hardcoded. An explicit empty list is
    /// not the same as omitting the key — the type has to keep those two
    /// readings apart so the stage can tell "no formatter" from "the built-in
    /// one".
    #[test]
    fn the_formatter_and_linter_default_to_the_workspaces_own_and_can_be_set() {
        let default = SelfEvolutionConfig::default();
        assert_eq!(default.fmt_check_command, None);
        assert_eq!(default.lint_command, None);

        let configured: SelfEvolutionConfig = serde_json::from_value(serde_json::json!({
            "fmt_check_command": ["ruff", "format", "--check", "."],
            "lint_command": ["ruff", "check", "."],
        }))
        .expect("configured commands parse");
        assert_eq!(
            configured.fmt_check_command,
            Some(
                ["ruff", "format", "--check", "."]
                    .map(String::from)
                    .to_vec()
            )
        );
        assert_eq!(
            configured.lint_command,
            Some(["ruff", "check", "."].map(String::from).to_vec())
        );

        let declared_absent: SelfEvolutionConfig = serde_json::from_value(serde_json::json!({
            "fmt_check_command": [],
            "lint_command": [],
        }))
        .expect("an explicit empty declaration parses");
        assert_eq!(declared_absent.fmt_check_command, Some(Vec::new()));
        assert_eq!(declared_absent.lint_command, Some(Vec::new()));

        // Omitted still loads: the key must not be required.
        let unset: SelfEvolutionConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(unset.fmt_check_command, None);
        assert_eq!(unset.lint_command, None);
    }

    /// The delays must stay inside the budget. Without a ceiling the driver
    /// multiplies its one-second floor by the factor it was handed, which is
    /// where the minutes come from.
    #[test]
    fn the_redis_connect_budget_bounds_every_delay() {
        let b = RedisConnectBudget {
            budget_ms: 900,
            retries: 2,
            ..RedisConnectBudget::default()
        };
        assert_eq!(b.max_delay_ms(), 300);
        assert_eq!(b.attempt_timeout(), std::time::Duration::from_millis(900));

        let default = RedisConnectBudget::default();
        assert_eq!(default.budget_ms, DEFAULT_REDIS_CONNECT_BUDGET_MS);
        assert_eq!(default.retries, DEFAULT_REDIS_CONNECT_RETRIES);
        assert_eq!(
            default.response_timeout(),
            std::time::Duration::from_millis(DEFAULT_REDIS_RESPONSE_TIMEOUT_MS),
            "a connection built from this budget must bound each command's round trip"
        );
        assert!(
            default.max_delay_ms() * (default.retries as u64 + 1) <= default.budget_ms,
            "the delays alone must not be able to exceed the budget"
        );
    }

    /// An installation may widen the budget; a nonsense value must not narrow it
    /// into "no retry at all", which would turn a restart into a hard failure.
    #[test]
    fn the_redis_connect_budget_follows_the_environment_and_ignores_nonsense() {
        let lookup = |pairs: &[(&str, &str)]| {
            let owned: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            move |key: &str| owned.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
        };

        let widened = RedisConnectBudget::from_lookup(lookup(&[
            ("COGNEVA_REDIS_CONNECT_BUDGET_MS", "9000"),
            ("COGNEVA_REDIS_CONNECT_RETRIES", "5"),
        ]));
        assert_eq!(widened.budget_ms, 9000);
        assert_eq!(widened.retries, 5);

        let nonsense = RedisConnectBudget::from_lookup(lookup(&[
            ("COGNEVA_REDIS_CONNECT_BUDGET_MS", "0"),
            ("COGNEVA_REDIS_CONNECT_RETRIES", "many"),
        ]));
        assert_eq!(
            nonsense,
            RedisConnectBudget::default(),
            "an unusable value falls back to the default instead of disabling retries"
        );
    }

    /// The response timeout is a knob for call sites with blocking commands, and
    /// the widening has to survive the reading; a value that would fire inside
    /// the round trip is refused rather than honoured.
    #[test]
    fn the_redis_response_timeout_widens_on_request_and_never_below_the_round_trip() {
        let widened = RedisConnectBudget::default().with_response_timeout_ms(20_000);
        assert_eq!(
            widened.response_timeout(),
            std::time::Duration::from_secs(20)
        );

        let narrowed = RedisConnectBudget::default().with_response_timeout_ms(0);
        assert_eq!(
            narrowed.response_timeout(),
            std::time::Duration::from_millis(MIN_REDIS_RESPONSE_TIMEOUT_MS),
            "a timeout that fires before any answer could arrive would fail every command"
        );

        let from_env = RedisConnectBudget::from_lookup(|key| match key {
            "COGNEVA_REDIS_RESPONSE_TIMEOUT_MS" => Some("45000".into()),
            _ => None,
        });
        assert_eq!(
            from_env.response_timeout(),
            std::time::Duration::from_secs(45)
        );

        let unusable = RedisConnectBudget::from_lookup(|key| match key {
            "COGNEVA_REDIS_RESPONSE_TIMEOUT_MS" => Some("0".into()),
            _ => None,
        });
        assert_eq!(
            unusable.response_timeout(),
            std::time::Duration::from_millis(DEFAULT_REDIS_RESPONSE_TIMEOUT_MS)
        );
    }

    /// 构建闸门的九个出厂读数也读具名常量。
    #[test]
    fn the_build_gate_reads_its_names() {
        let cfg = BuildGateConfig::default();
        assert_eq!(cfg.build_demand_cores, BUILD_GATE_DEMAND_CORES);
        assert_eq!(cfg.build_peak_rss_mib, BUILD_GATE_PEAK_RSS_MIB);
        assert_eq!(cfg.mem_floor_mib, BUILD_GATE_MEM_FLOOR_MIB);
        assert_eq!(cfg.io_stall_budget_pct, BUILD_GATE_IO_STALL_BUDGET_PCT);
        assert_eq!(cfg.cpu_calm_pct, BUILD_GATE_CPU_CALM_PCT);
        assert_eq!(cfg.io_calm_pct, BUILD_GATE_IO_CALM_PCT);
        assert_eq!(cfg.mem_calm_pct, BUILD_GATE_MEM_CALM_PCT);
        assert_eq!(cfg.cpu_sample_gap_ms, BUILD_GATE_CPU_SAMPLE_GAP_MS);

        let source = include_str!("config.rs");
        let body = source
            .split("impl Default for BuildGateConfig")
            .nth(1)
            .and_then(|rest| rest.split("\n}\n").next())
            .expect("BuildGateConfig 的默认值在这份文件里");
        let names = [
            "build_demand_cores",
            "build_peak_rss_mib",
            "mem_floor_mib",
            "io_stall_budget_pct",
            "cpu_calm_pct",
            "io_calm_pct",
            "mem_calm_pct",
            "cpu_sample_gap_ms",
        ];
        let bare: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|line| {
                names.iter().any(|name| {
                    line.strip_prefix(name)
                        .and_then(|rest| rest.strip_prefix(':'))
                        .map(|value| value.trim_start().starts_with(|c: char| c.is_ascii_digit()))
                        .unwrap_or(false)
                })
            })
            .collect();
        assert!(bare.is_empty(), "这些出厂读数要读常量：{bare:?}");
    }
}
#[cfg(test)]
mod system_config_reads_its_names {
    use super::*;

    /// 九条容量／超时默认值都读具名常量：改常量就改默认，改回裸写就红。
    #[test]
    fn every_system_capacity_reads_its_constant() {
        let cfg = SystemConfig::default();
        assert_eq!(cfg.event_channel_capacity, DEFAULT_EVENT_CHANNEL_CAPACITY);
        assert_eq!(
            cfg.task_event_channel_capacity,
            DEFAULT_TASK_EVENT_CHANNEL_CAPACITY
        );
        assert_eq!(cfg.pattern_db_max_size, DEFAULT_PATTERN_DB_MAX_SIZE);
        assert_eq!(cfg.pg_max_connections, DEFAULT_PG_MAX_CONNECTIONS);
        assert_eq!(cfg.pg_min_connections, DEFAULT_PG_MIN_CONNECTIONS);
        assert_eq!(cfg.pg_acquire_timeout_secs, DEFAULT_PG_ACQUIRE_TIMEOUT_SECS);
        assert_eq!(
            cfg.memory_message_broadcast_capacity,
            DEFAULT_MEMORY_MESSAGE_BROADCAST_CAPACITY
        );
        assert_eq!(
            cfg.observability_event_channel_capacity,
            DEFAULT_OBSERVABILITY_EVENT_CHANNEL_CAPACITY
        );
        assert_eq!(
            cfg.websocket_event_cache_capacity,
            DEFAULT_WEBSOCKET_EVENT_CACHE_CAPACITY
        );

        let source = include_str!("config.rs");
        let body = source
            .split("impl Default for SystemConfig")
            .nth(1)
            .and_then(|rest| rest.split("\n#[cfg(test)]").next())
            .expect("SystemConfig 的默认值在这份文件里");
        let names = [
            "event_channel_capacity",
            "task_event_channel_capacity",
            "pattern_db_max_size",
            "pg_max_connections",
            "pg_min_connections",
            "pg_acquire_timeout_secs",
            "memory_message_broadcast_capacity",
            "observability_event_channel_capacity",
            "websocket_event_cache_capacity",
        ];
        let bare: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|line| {
                names.iter().any(|name| {
                    line.strip_prefix(name)
                        .and_then(|rest| rest.strip_prefix(':'))
                        .map(|value| value.trim_start().starts_with(|c: char| c.is_ascii_digit()))
                        .unwrap_or(false)
                })
            })
            .collect();
        assert!(bare.is_empty(), "这些默认值要读常量：{bare:?}");
    }

    /// 总线消费的两个计数与两个兜底参数也读具名常量。
    #[test]
    fn the_bus_consumer_reads_its_names() {
        let nats = NatsConfig::default();
        assert_eq!(nats.consumer_max_deliver, nats_max_deliver().get());
        assert_eq!(nats.consumer_max_ack_pending, nats_max_ack_pending().get());
        let consumer = MultiBackendConsumerConfig::default();
        assert_eq!(consumer.retry_interval_secs, BUS_RETRY_INTERVAL_SECS);
        assert_eq!(
            consumer.publish_buffer_capacity,
            BUS_PUBLISH_BUFFER_CAPACITY
        );

        let source = include_str!("config.rs");
        let bare: Vec<&str> = source
            .lines()
            .map(str::trim)
            .filter(|line| {
                [
                    "consumer_max_deliver",
                    "consumer_max_ack_pending",
                    "retry_interval_secs",
                    "publish_buffer_capacity",
                ]
                .iter()
                .any(|name| {
                    line.strip_prefix(name)
                        .and_then(|rest| rest.strip_prefix(':'))
                        .map(|value| value.trim_start().starts_with(|c: char| c.is_ascii_digit()))
                        .unwrap_or(false)
                })
            })
            .collect();
        assert!(bare.is_empty(), "这些默认值要读常量：{bare:?}");
    }
}
