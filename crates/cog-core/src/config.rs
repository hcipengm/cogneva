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
    pub llm: LLMConfig,
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
    pub lifecycle: LifecycleConfig,
    #[serde(default)]
    pub system: SystemConfig,
    #[serde(default)]
    pub self_evolution: SelfEvolutionConfig,
    #[serde(default)]
    pub multi_backend_consumer: MultiBackendConsumerConfig,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// LLM API configuration.
/// `api_key` 在 Debug 输出中脱敏，防止泄漏到日志。
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LLMConfig {
    pub provider: String,
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_model: Option<String>,
    pub max_tokens: u32,
    pub temperature: f32,
    pub timeout_secs: u64,
}

impl std::fmt::Debug for LLMConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LLMConfig")
            .field("provider", &self.provider)
            .field("api_key", &redacted(&self.api_key))
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("embedding_model", &self.embedding_model)
            .field("max_tokens", &self.max_tokens)
            .field("temperature", &self.temperature)
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
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

impl Default for LLMConfig {
    fn default() -> Self {
        Self {
            provider: String::new(),
            api_key: String::new(),
            base_url: None,
            model: String::new(),
            embedding_model: None,
            max_tokens: 0,
            temperature: 0.0,
            timeout_secs: 0,
        }
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
    #[serde(flatten)]
    pub options: HashMap<String, serde_json::Value>,
}

/// NATS connection configuration.
/// Supports single-node, clustered, and TLS-secured deployments.
/// Backward-compatible: a bare `"nats_url"` string is auto-upgraded
/// into `urls: [nats_url]` at load time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct NatsConfig {
    /// List of NATS server URLs.  For clusters provide at least 3.
    /// Example: `["nats://n1:4222", "nats://n2:4222", "nats://n3:4222"]`
    pub urls: Vec<String>,
    /// Authentication settings.
    pub auth: NatsAuthConfig,
    /// TLS settings.
    pub tls: NatsTlsConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct GatewayConfig {
    pub http_port: u16,
    pub ws_port: u16,
    pub metrics_port: u16,
    pub cors_origins: Vec<String>,
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

/// Configuration for the Hot/Warm/Cold tier migrator.
/// `enabled = false` skips the migrator entirely and leaves files untouched
/// in the hot tier. Durations are seconds so the JSON config stays compact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct TierMigratorConfig {
    pub enabled: bool,
    pub hot_duration_secs: u64,
    pub warm_duration_secs: u64,
    pub warm_compression_level: i32,
    pub cold_compression_level: i32,
    pub scan_interval_secs: u64,
    pub cold_key_prefix: String,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub interval_secs: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".into(),
            interval_secs: 15,
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

/// Lifecycle management configuration (heartbeat, thresholds).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleConfig {
    pub heartbeat_interval_secs: u64,
    pub suspect_threshold: u32,
    pub dead_threshold: u32,
    pub registration_ttl_secs: u64,
    pub cleanup_interval_secs: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_secs: 5,
            suspect_threshold: 2,
            dead_threshold: 5,
            registration_ttl_secs: 60,
            cleanup_interval_secs: 300,
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
    /// Trace tier migrator run interval (seconds).
    pub trace_migrator_interval_secs: u64,
    /// WASM tool execution timeout (seconds).
    pub tool_timeout_secs: u64,
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
            trace_migrator_interval_secs: 3600,
            tool_timeout_secs: 30,
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
}

impl Default for MultiBackendConsumerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channel: "cogneva-events".into(),
            group: "cogneva-consumer".into(),
            retry_interval_secs: 5,
        }
    }
}

/// Image-based 滚动更新配置（审计 3.2）：启用后 patch 部署从特权 Pod
/// `self_exec` 二进制替换升级为「构建镜像 → patch Deployment → 滚动更新」。
/// 默认关闭，保持既有 self_exec/systemd 行为不回退。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ImageRolloutConfig {
    pub enabled: bool,
    /// 镜像仓库地址（不含 tag），如 `localhost/cogneva`。
    pub image_repo: String,
    /// 镜像内基础镜像（COPY 已编译二进制）。
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
            base_image: "debian:bookworm-slim".into(),
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
    /// VM 销毁后保留（docs/2026-06-26_21-23-41_agent沙盒安全.md §4）。
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

/// Configuration for the self-evolution auto-deploy pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfEvolutionConfig {
    pub enabled: bool,
    pub auto_apply: bool,
    pub auto_deploy: bool,
    pub sandbox_mode: bool,
    /// Explicit operator opt-out of the sandbox boundary check: when true,
    /// auto apply/deploy run even if no isolated environment is detected.
    /// Never set this on a host you care about.
    pub force_autonomous: bool,
    /// Optional human-in-the-loop gate: when true, patches that pass tests
    /// are rolled back and held at `AwaitingReview` instead of being
    /// committed/deployed, until an operator approves them via the admin
    /// API (`POST /admin/evolution/patches/:id/approve`). Default false
    /// (fully autonomous).
    pub manual_approve: bool,
    pub patch_dir: String,
    pub binary_dir: String,
    pub backup_dir: String,
    pub switch_mode: String,
    pub health_check_grace_period_secs: u64,
    pub health_check_interval_secs: u64,
    pub health_check_max_retries: u32,
    pub test_timeout_secs: u64,
    pub build_timeout_secs: u64,
    pub poll_interval_secs: u64,
    pub notify_on_success: bool,
    pub notify_on_failure: bool,
    /// Image-based 滚动更新；enabled=false 时忽略整块配置。
    pub image_rollout: ImageRolloutConfig,
    /// Firecracker 微虚拟机沙盒；enabled=false 时忽略整块配置。
    pub microvm: MicroVmConfig,
}

impl Default for SelfEvolutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_apply: true,
            auto_deploy: true,
            sandbox_mode: false,
            force_autonomous: false,
            manual_approve: false,
            patch_dir: "./evolution-patches".into(),
            binary_dir: "/opt/cogneva/bin".into(),
            backup_dir: "/opt/cogneva/bin/backups".into(),
            switch_mode: "self_exec".into(),
            health_check_grace_period_secs: 10,
            health_check_interval_secs: 5,
            health_check_max_retries: 6,
            test_timeout_secs: 600,
            build_timeout_secs: 1800,
            poll_interval_secs: 60,
            notify_on_success: false,
            notify_on_failure: true,
            image_rollout: ImageRolloutConfig::default(),
            microvm: MicroVmConfig::default(),
        }
    }
}

impl Default for Config {
    /// Zero-value default.  All real configuration comes from
    /// files / env vars at runtime.
    fn default() -> Self {
        Self {
            app: AppInfo::default(),
            llm: LLMConfig::default(),
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
            lifecycle: LifecycleConfig::default(),
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
// 专业配置段归实现 crate 所有（docs/cog_core_boundary_and_interface_audit.md
// §7.3）。各 crate 自读 cogneva.json 取段后，用本函数叠加 env 覆盖再反序
// 列化；cogneva config_loader 对 core 聚合段也走同一函数，语义全系统统一。

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
