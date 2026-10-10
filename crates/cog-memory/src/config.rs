//! Memory 配置——cog-memory 自有配置段（core config.rs 不聚合单 crate
//! 配置）。自读 cogneva.json `memory` 段并叠加
//! `COGNEVA_MEMORY_*` env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

/// The env-var names this section answers to. Published so the deploy-config
/// gate can tell "honored, but not by the core schema" from "honored by
/// nobody".
pub const MEMORY_ENV: &[(&str, &str)] = &[
    ("COGNEVA_MEMORY_ENABLED", "enabled"),
    ("COGNEVA_MEMORY_BACKEND_TYPE", "backend_type"),
    ("COGNEVA_MEMORY_EMBEDDING_DIMENSION", "embedding_dimension"),
    ("COGNEVA_MEMORY_AUTO_INGEST", "auto_ingest"),
    (
        "COGNEVA_MEMORY_LOAD_EMBEDDING_MODEL",
        "load_embedding_model",
    ),
    ("COGNEVA_MEMORY_LOAD_RERANKER_MODEL", "load_reranker_model"),
    (
        "COGNEVA_MEMORY_DECAY_INTERVAL_SECS",
        "maintenance.decay_interval_secs",
    ),
    (
        "COGNEVA_MEMORY_DECAY_AGE_THRESHOLD_SECS",
        "maintenance.decay_age_threshold_secs",
    ),
    (
        "COGNEVA_MEMORY_DECAY_IMPORTANCE_THRESHOLD",
        "maintenance.decay_importance_threshold",
    ),
];

/// Memory 子系统配置。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MemoryConfig {
    pub enabled: bool,
    /// Backend type. `composite` builds the three-layer backend on top of the
    /// published ObjectBackend / PostgreSQL / VectorBackend; the in-process
    /// `memory` backend is the non-durable counterpart. Any other value is a
    /// configuration error rather than a silent fallback.
    pub backend_type: String,
    /// Embedding dimension for summary vectors.
    pub embedding_dimension: usize,
    /// Auto-ingest AgentEnd events into memory.
    pub auto_ingest: bool,
    /// 启动期是否加载 ONNX 向量模型（BGE-M3，dense + sparse）。
    ///
    /// 两个 session 读同一份权重文件（实测进程峰值 RSS：dense 与 sparse 两个
    /// session 一起 1.67GiB，单开 dense 不更小——两个 session 走的是同一批文件页，
    /// 加第二个几乎不涨）。模型不在本地缓存时会先从 HuggingFace 拉取——离线集群
    /// 里那次连接既不成功也不失败（客户端无超时），插件 init 会一直挂着，直到存活
    /// 探针把 Pod 杀掉。因此只有权重已就位（由部署侧以只读卷或共享目录提供，
    /// 权重本身不进镜像）且内存吃得下的部署才打开；其余保持关闭，向量能力缺席
    /// 但启动不受影响。重排模型（reranker）另有开关。
    pub load_embedding_model: bool,
    /// 启动期是否加载 ONNX 重排模型（BGE-Reranker-V2-M3，实测进程峰值 RSS 1.67GiB）。
    /// 关闭原因同 [`Self::load_embedding_model`]；其拉取路径写死
    /// `https://huggingface.co`、不吃 `HF_ENDPOINT`，离线集群只能靠本地就位。
    pub load_reranker_model: bool,
    /// 自动摄取（AgentEnd → 记忆三层）的运行参数。
    pub ingest: IngestConfig,
    /// 周期维护（低价值记忆自动衰减）的运行参数。
    pub maintenance: MaintenanceConfig,
}

/// 记忆维护管线的运行参数。代码侧 [`Default`] 只是兜底，集群上调参改
/// cogneva.json 的 `memory.maintenance` 段。
///
/// 这里是「低价值记忆自动衰减」这句承诺的唯一驱动方：没有这一段，衰减只有
/// 一个「谁都能调、却没人调」的方法（`MemoryBackend::decay`），
/// 文档里的自动衰减不成立。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MaintenanceConfig {
    /// 两次衰减扫描之间的间隔（秒）；0 表示关掉整条维护循环。
    ///
    /// 降权是**逐次**的：一次扫描把合格条目的重要性乘 `DECAY_IMPORTANCE_FACTOR`，
    /// 跌到 `DECAY_ARCHIVE_FLOOR` 才归档。所以归档必须跨多次扫描才发生，任何
    /// 一次扫描都不会把一条刚入库的低分记忆直接删掉。
    pub decay_interval_secs: u64,
    /// 只有生成时间早于「现在减去这么多秒」的条目才够格衰减。
    pub decay_age_threshold_secs: u64,
    /// 只有重要性低于这个值的条目才够格衰减。
    pub decay_importance_threshold: f32,
    /// 扫描哪些命名空间。默认只有自动摄取写入的那一个——系统衰减**它自己**的记忆；
    /// 手工摄取写进别的命名空间是调用方的记忆，除非在这里显式列出，否则不动。
    pub decay_namespaces: Vec<String>,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            decay_interval_secs: 3600,
            decay_age_threshold_secs: 7 * 24 * 3600,
            decay_importance_threshold: 0.5,
            decay_namespaces: vec!["default".into()],
        }
    }
}

/// 自动摄取管线的运行参数。代码侧 [`Default`] 只是兜底，集群上调参改
/// cogneva.json 的 `memory.ingest` 段。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    /// 单条消息的最大重试次数（指数退避 1s、2s、4s…）。
    pub max_retries: u32,
    /// 重试退避基数（毫秒）。
    pub retry_base_delay_ms: u64,
    /// 最终失败的消息是否写死信命名空间。
    pub enable_dlq: bool,
    /// 死信命名空间。
    pub dlq_namespace: String,
    /// 抽取并发上限。抽取是 LLM 时延主导的 I/O 任务，这个值决定事件洪峰
    /// 后积压的排空速率。
    pub extraction_concurrency: usize,
    /// 启动时是否对账扫描"已归档未抽取"的 raw 并补驱动（覆盖崩溃窗口）。
    pub startup_reconcile: bool,
    /// 对账只回看最近这么多个小时的 raw。
    pub reconcile_lookback_hours: u64,
    /// 每拍最多补驱动多少条已老过对账回看窗、且没有被终局诊断的 raw。
    ///
    /// 回看窗内的欠账每拍重驱动；窗外的那批转成按固定速率还，速率与事件洪峰
    /// 解耦：一次长断供（长过回看窗）恢复之后，断供早期归档的 raw 全在窗外，
    /// 不设上限就会一次性把全部欠账塞进队列，占满抽取并发。0 = 不补。
    pub aged_out_redrive_batch: usize,
    /// 周期对账间隔（秒）；0 表示只在启动时对账。
    ///
    /// 启动对账只覆盖"进程崩溃到重启"这一小段。一次上游断供比对账回看窗更长
    /// 时，断供早期已归档未抽取的 raw 会掉出窗口、再也不会被补驱动——周期重扫
    /// 让"归档必有抽取"不再依赖重启时机。
    pub reconcile_interval_secs: u64,
    /// 连续多少次环境类抽取失败后暂停拉取。
    ///
    /// 上游断供时重试与继续拉取都只是把同一堵墙撞一遍；暂停让事件留在事件面里
    /// 原样等重放，而不是被逐条终结掉。
    pub pull_pause_after_failures: u32,
    /// 暂停拉取的初始时长（秒），每次再次触发翻倍。
    pub pull_pause_initial_secs: u64,
    /// 暂停拉取的封顶时长（秒）。恢复时刻由上游给出时可能很远，睡死了就错过
    /// 恢复，所以按这个上限醒来重判。
    pub pull_pause_max_secs: u64,
    /// 读取池状态快照的最小间隔（秒）。快照本身由网关按自己的节拍写，读得太密
    /// 只是把同一个答案取回来。
    pub pool_check_secs: u64,
    /// 试跑观察窗（秒）：池快照给的**重试节拍**到点后，还要再等这么久才重开
    /// 拉取闸门。默认取网关嫌疑窗首窗的量级（300 秒）——一次探测从发起到结果
    /// 写进快照大致就是这个尺度。上游自报的复位时刻不加这个窗。
    pub pull_resume_observation_secs: u64,
    /// 积压深度告警起点（达到后每翻倍打一条 WARN）。
    pub backlog_warn_at: usize,
    /// 总线消费组名（durable consumer / consumer group）。
    pub bus_group: String,
    /// pending 认领清扫间隔（秒；Redis Streams 用，JetStream 靠 ack_wait 自动红投）。
    pub bus_claim_interval_secs: u64,
    /// 认领门槛：pending 空闲超过这么多毫秒才被接走（须大于单条最坏处理时长）。
    pub bus_claim_min_idle_ms: u64,
    /// 每轮认领批大小上限。
    pub bus_claim_batch: usize,
    /// 评测数据专用命名空间/桶：列在这里的命名空间，记忆的任何一层都不接受。
    ///
    /// 空表意味着这条规则不生效（不是「不排除任何东西」的另一种写法——两者
    /// 的差别要从读数里看：被拒的次数记在 `memory_operations_total` 的
    /// `benchmark_namespace_refused` 一格上）。桶名是部署事实，代码里不写死：
    /// 写死等于让部署改名只能靠改代码。
    pub benchmark_namespaces: Vec<String>,
    /// 评测数据集自带的标记串（canary）：载荷里出现任一条即拒收，不进任何一层。
    ///
    /// 这一条与上一条互为补集，不是重复：桶名与标签都要求写侧**带着牌子**来，
    /// 而一份经普通任务链路、落在默认命名空间的评测内容两条都不命中，只有随
    /// 数据本身走的标记串还在。空表 = 这条规则不生效；标记串由部署给出（取自
    /// 数据集自身，代码里不写具体串）。
    pub benchmark_canary_markers: Vec<String>,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_base_delay_ms: 1000,
            enable_dlq: true,
            dlq_namespace: "dlq".into(),
            extraction_concurrency: 4,
            startup_reconcile: true,
            reconcile_lookback_hours: 24,
            aged_out_redrive_batch: 32,
            reconcile_interval_secs: 600,
            pull_pause_after_failures: 3,
            pull_pause_initial_secs: 60,
            pull_pause_max_secs: 1800,
            pool_check_secs: 30,
            pull_resume_observation_secs: 300,
            backlog_warn_at: 64,
            bus_group: "memory-ingestor".into(),
            bus_claim_interval_secs: 30,
            bus_claim_min_idle_ms: 900_000,
            bus_claim_batch: 32,
            benchmark_namespaces: Vec::new(),
            benchmark_canary_markers: Vec::new(),
        }
    }
}

impl MemoryConfig {
    /// 自读 cogneva.json `memory` 段 + env 覆盖；文件/段缺失回退默认，
    /// 段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        let mut section = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                root.pointer("/memory")
                    .cloned()
                    .unwrap_or(serde_json::json!({}))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cog_core::config::apply_env_paths(&mut section, MEMORY_ENV);
        serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} memory: {e}", path.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_returns_default() {
        let cfg = MemoryConfig::load_from(std::path::Path::new("/nonexistent/x.json")).unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn reads_section() {
        let dir = std::env::temp_dir().join(format!("cog-mem-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"memory": {"enabled": true, "backend_type": "composite", "embedding_dimension": 1024}}"#,
        )
        .unwrap();
        let cfg = MemoryConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.backend_type, "composite");
        assert_eq!(cfg.embedding_dimension, 1024);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 模型加载默认关：一个 2Gi 上限的 Pod 装不下 BGE-M3 的常驻权重，而本地无缓存
    /// 时那次拉取在离线集群里不会返回，会挂着插件 init。缺省必须是"不加载"。
    #[test]
    fn model_loads_default_off() {
        let cfg = MemoryConfig::load_from(std::path::Path::new("/nonexistent/x.json")).unwrap();
        assert!(!cfg.load_embedding_model);
        assert!(!cfg.load_reranker_model);
    }
}
