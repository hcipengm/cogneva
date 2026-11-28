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
    /// 与嵌入模型一样，插件装载时显式读取 `FASTEMBED_CACHE_DIR` 指名权重目录。
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
    /// 两次「清扫环境拒绝条目」之间的间隔（秒）；0 表示关掉这条清扫循环。
    ///
    /// 它清的是写入端已经不再产出的那一类条目（见 [`crate::refusal_sweep`]），
    /// 所以节拍不需要贴着事件走：一条已经不再增长的行，早半小时删晚半小时删，
    /// 差别只是那半小时里每次读学习库仍然多读它一次。
    pub refusal_sweep_interval_secs: u64,
    /// 清扫哪些命名空间。默认只有反思层写入的那一个——那是唯一会写学习的命名
    /// 空间，别处的内容没有这条判据要处理的东西。
    pub refusal_sweep_namespaces: Vec<String>,
}

/// 重要度低于它的记忆按衰减顺序清掉。
///
/// 一条记忆被回读时重要度会被抬高，所以这条阈值分的是「还会被回读」与「已经
/// 不再被回读」两堆。它由衰减之后回读面里还剩多少被清掉的条目定：清错了就把
/// 它调低，清不干净就调高——不是一个可以拍定的档位。
// cap: fixed — 由衰减后的回读率定：被清掉的条目还出现在回读里，就是它太高
// keep: 删掉后被清掉的条目还出现在回读里，衰减线没有判据；由「由衰减后的回读率定」兜住
const DECAY_IMPORTANCE_THRESHOLD: f32 = 0.5;

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            // 两个时间长度按「几小时 / 几天」写：一个 3600 读不出它是几小时。
            decay_interval_secs: cog_core::day::SECONDS_PER_HOUR as u64,
            decay_age_threshold_secs: cog_core::day::SECONDS_PER_WEEK as u64,
            decay_importance_threshold: DECAY_IMPORTANCE_THRESHOLD,
            decay_namespaces: vec!["default".into()],
            refusal_sweep_interval_secs: cog_core::day::SECONDS_PER_HOUR as u64 / 2,
            refusal_sweep_namespaces: vec!["reflection".into()],
        }
    }
}

/// 一次抽取能发给模型的 transcript 上限（估算 token）。
///
/// 取今天窗口配置的值：raw 层改存完整历史之前，抽取器拿到的最长载荷就是被
/// 裁到一个窗口的那份，默认值让这条请求的量级不变。它是这条链路上第一个显式
/// 的界——窗口配置改了要一起看，两者比的是同一段文本。
// cap: fixed — 与窗口同源：两者量的是同一段文本，窗口配置改了要一起看
// keep: 删掉后抽取输入与窗口各说各的，同一段文本两处预算对不上；由「与窗口同源」兜住
pub const DEFAULT_EXTRACTION_INPUT_BUDGET_TOKENS: usize = 64_000;

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
    /// 一次抽取最多发出去多少估算 token 的 transcript，超出留头留尾、省中间。
    ///
    /// 0 = 不设界（整段发出）。界原先由上游隐式给出：raw 层存的是被窗口裁过
    /// 的那份，长度天然不超过一个窗口。raw 层改存完整历史后这层保护没有了，
    /// 界必须显式写在这里。口径与窗口同一个估算函数——两者拿同一个数比较，
    /// 各算一份就会在中文对话上对不上。
    pub extraction_input_budget_tokens: usize,
    /// 启动时是否对账扫描"已归档未抽取"的 raw 并补驱动（覆盖崩溃窗口）。
    pub startup_reconcile: bool,
    /// 对账只回看最近这么多个小时的 raw。
    pub reconcile_lookback_hours: u64,
    /// 每拍最多补驱动多少条已老过对账回看窗、且没有被终局诊断的 raw。
    ///
    /// 回看窗内的欠账每拍重驱动；窗外的那批转成按固定速率还，速率与事件洪峰
    /// 解耦：一次长断供（长过回看窗）恢复之后，断供早期归档的 raw 全在窗外，
    /// 不设上限就会一次性把全部欠账塞进队列，占满抽取并发。0 = 不补。
    ///
    /// 欠账比这个数深时，批量的四分之一留给窗外最老的那几条（不足四条时不
    /// 留）：只从最新的一头取，每拍补进来的新欠账会把界推着走，最老的那条永远
    /// 排在界外，欠账里有一截再也不会变。
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

/// 一条消息在进 DLQ 之前重试几次。
///
/// 它钉的是「一条消息从首投到终局要多久」与上游断供重放窗口的关系：重试跑完
/// 的时间长过重放窗口，消息在队列里等的那段时间就白等了；短太多则一次抖动就
/// 进 DLQ。
// cap: fixed — 由断供重放窗口定：一条消息从首投到进 DLQ 要短于这个窗
// keep: 删掉后一条消息从首投到进 DLQ 的时间超过断供重放窗，重放再也接不上；由「要短于断供重放窗口」兜住
const INGEST_MAX_RETRIES: u32 = 3;

/// 同时在途的抽取请求条数。
// cap: fixed — 由提供方的速率上限与单条抽取时长定：被 429 顶住就是它太大
// keep: 删掉后并发被提供方的 429 顶住，重试代替了抽取；由「由速率上限与单条时长定」兜住
const EXTRACTION_CONCURRENCY: usize = 4;

/// 一拍搬运多少条：回看窗外的补驱动与 pending 认领问的是同一件事。
///
/// 真正的批量不是这个数：认领那一拍按「一拍时长 ÷ 单次抽取的 p95 耗时」现算
/// （`claim_batch_for_pass`），读数来自 `memory_operation_latency_ms{operation="extract"}`。
/// 这一格只是**量不到时的种子**（还没有一条抽取耗时样本，或者指标面没接上）——
/// 种子是退路，不是结论。
// cap: fixed — 量不到时的种子：真正的一拍批量由实测的抽取耗时现算，这个数只在没有样本时兜底
// keep: 删掉后量不到时没有种子，一拍批量在冷启动时无值可依；由「只在没有样本时兜底」兜住
const INGEST_BATCH: usize = 32;

/// 连续多少次环境类抽取失败后暂停拉取。
// cap: fixed — 由上游断供的判定时长定：暂停要早于断供把事件面塞满
// keep: 删掉后暂停晚于断供把事件面塞满，上游断供不被发现；由「暂停要早于断供塞满」兜住
const PULL_PAUSE_AFTER_FAILURES: u32 = 3;

/// 积压深度告警起点：达到后每翻倍打一条 WARN。
// cap: fixed — 由一拍的抽取吞吐定：它是「几拍才清得完」的积压量
// keep: 删掉后积压到几拍才清得完之前不报警，读数只在事后可见；由「几拍才清得完的积压量」兜住
const BACKLOG_WARN_AT: usize = 64;

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_retries: INGEST_MAX_RETRIES,
            retry_base_delay_ms: cog_core::day::MILLIS_PER_SECOND as u64,
            enable_dlq: true,
            dlq_namespace: "dlq".into(),
            extraction_concurrency: EXTRACTION_CONCURRENCY,
            extraction_input_budget_tokens: DEFAULT_EXTRACTION_INPUT_BUDGET_TOKENS,
            startup_reconcile: true,
            reconcile_lookback_hours: cog_core::day::HOURS_PER_DAY as u64,
            aged_out_redrive_batch: INGEST_BATCH,
            reconcile_interval_secs: (10 * cog_core::day::SECONDS_PER_MINUTE) as u64,
            pull_pause_after_failures: PULL_PAUSE_AFTER_FAILURES,
            pull_pause_initial_secs: cog_core::day::SECONDS_PER_MINUTE as u64,
            pull_pause_max_secs: (30 * cog_core::day::SECONDS_PER_MINUTE) as u64,
            pool_check_secs: (cog_core::day::SECONDS_PER_MINUTE / 2) as u64,
            pull_resume_observation_secs: (5 * cog_core::day::SECONDS_PER_MINUTE) as u64,
            backlog_warn_at: BACKLOG_WARN_AT,
            bus_group: "memory-ingestor".into(),
            bus_claim_interval_secs: (cog_core::day::SECONDS_PER_MINUTE / 2) as u64,
            bus_claim_min_idle_ms: (15
                * cog_core::day::SECONDS_PER_MINUTE
                * cog_core::day::MILLIS_PER_SECOND) as u64,
            bus_claim_batch: INGEST_BATCH,
            benchmark_namespaces: Vec::new(),
            benchmark_canary_markers: Vec::new(),
        }
    }
}

impl MemoryConfig {
    /// 自读 cogneva.json `memory` 段 + env 覆盖；文件/段缺失回退默认，
    /// 段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = cog_core::paths::config_file();
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
        let config: Self = serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} memory: {e}", path.display())))?;
        config.ingest.refuse_zero_pull_gate()?;
        Ok(config)
    }
}

impl IngestConfig {
    /// 拉取闸门的四个输入都是「一个有长度的窗口 / 一段有内容的间隔」：
    /// 0 次失败之后的暂停、0 秒长的暂停、0 秒的封顶、0 秒的检查间隔，都不是
    /// 同一件事的极端写法，而是没有意义的值。此前 `PullGate::new` 把四个都
    /// `.max(1)`，声明的 0 被悄悄换成一个别的数，配置面读到的却不是生效值。
    /// 改成在这一层（声明进入的地方）按字段名拒绝。
    fn refuse_zero_pull_gate(&self) -> SFResult<()> {
        let zero = [
            (
                "memory.ingest.pull_pause_after_failures",
                self.pull_pause_after_failures == 0,
            ),
            (
                "memory.ingest.pull_pause_initial_secs",
                self.pull_pause_initial_secs == 0,
            ),
            (
                "memory.ingest.pull_pause_max_secs",
                self.pull_pause_max_secs == 0,
            ),
            ("memory.ingest.pool_check_secs", self.pool_check_secs == 0),
        ];
        if let Some((name, _)) = zero.iter().find(|(_, is_zero)| *is_zero) {
            return Err(SFError::Config(format!(
                "{name} 声明为 0：拉取闸门要一个长度才能判「暂停多久」或「隔多久看一眼」，0 不是这个长度的极端值。要关掉暂停请改配置语义，不要写 0。"
            )));
        }
        Ok(())
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
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
    }

    /// 模型加载默认关：一个 2Gi 上限的 Pod 装不下 BGE-M3 的常驻权重，而本地无缓存
    /// 时那次拉取在离线集群里不会返回，会挂着插件 init。缺省必须是"不加载"。
    #[test]
    fn model_loads_default_off() {
        let cfg = MemoryConfig::load_from(std::path::Path::new("/nonexistent/x.json")).unwrap();
        assert!(!cfg.load_embedding_model);
        assert!(!cfg.load_reranker_model);
    }

    /// 补驱动与 pending 认领问的是同一件事：一拍搬得动多少条。两个字段读同一个
    /// 常量，改一个数两处一起动；这里从两个面钉住它——值相等，且 `Default` 里
    /// 两个字段后面都不是裸数字。
    #[test]
    fn both_claims_read_one_batch_constant() {
        let ingest = IngestConfig::default();
        assert_eq!(ingest.aged_out_redrive_batch, INGEST_BATCH);
        assert_eq!(ingest.bus_claim_batch, INGEST_BATCH);

        let source = include_str!("config.rs");
        let bare: Vec<&str> = source
            .lines()
            .map(str::trim)
            .filter(|line| {
                (line.starts_with("aged_out_redrive_batch:")
                    || line.starts_with("bus_claim_batch:"))
                    && line
                        .split_once(':')
                        .map(|(_, value)| {
                            value.trim_start().starts_with(|c: char| c.is_ascii_digit())
                        })
                        .unwrap_or(false)
            })
            .collect();
        assert!(
            bare.is_empty(),
            "这两个字段不该各自写一个数，要读 INGEST_BATCH：{bare:?}"
        );
    }

    /// 拉取闸门的四格：声明的值原样生效，0 在加载时按字段名拒绝。
    ///
    /// 没有这条测试，`.max(1)` 回来时没人会红：声明的 0 会被悄悄换成 1，而
    /// 配置面读到的仍然像生效值。
    #[test]
    fn a_declared_pull_gate_survives_and_zero_is_refused() {
        let dir = std::env::temp_dir().join(format!("cog-mem-pull-gate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");

        std::fs::write(
            &path,
            r#"{"memory": {"enabled": true, "ingest": {"pull_pause_initial_secs": 90, "pool_check_secs": 5}}}"#,
        )
        .unwrap();
        let cfg = MemoryConfig::load_from(&path).unwrap();
        assert_eq!(cfg.ingest.pull_pause_initial_secs, 90);
        assert_eq!(cfg.ingest.pool_check_secs, 5);

        for field in [
            "pull_pause_after_failures",
            "pull_pause_initial_secs",
            "pull_pause_max_secs",
            "pool_check_secs",
        ] {
            std::fs::write(
                &path,
                format!(r#"{{"memory": {{"ingest": {{"{field}": 0}}}}}}"#),
            )
            .unwrap();
            let err = MemoryConfig::load_from(&path).unwrap_err().to_string();
            assert!(
                err.contains(field),
                "{field} 声明 0 的错误要点名这个字段，实际是：{err}"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `Default` 里其余几条具名常量也要真的被读到：常量与默认值相等，改常量就
    /// 改默认。
    #[test]
    fn every_named_ingest_default_reads_its_constant() {
        assert_eq!(IngestConfig::default().max_retries, INGEST_MAX_RETRIES);
        assert_eq!(
            IngestConfig::default().extraction_concurrency,
            EXTRACTION_CONCURRENCY
        );
        assert_eq!(
            IngestConfig::default().pull_pause_after_failures,
            PULL_PAUSE_AFTER_FAILURES
        );
        assert_eq!(IngestConfig::default().backlog_warn_at, BACKLOG_WARN_AT);
        assert_eq!(
            MaintenanceConfig::default().decay_importance_threshold,
            DECAY_IMPORTANCE_THRESHOLD
        );
    }
}
