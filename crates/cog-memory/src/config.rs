//! Memory 配置——cog-memory 自有配置段（core config.rs 不聚合单 crate
//! 配置）。自读 cogneva.json `memory` 段并叠加
//! `COGNEVA_MEMORY_*` env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

const MEMORY_ENV: &[(&str, &str)] = &[
    ("COGNEVA_MEMORY_ENABLED", "enabled"),
    ("COGNEVA_MEMORY_BACKEND_TYPE", "backend_type"),
    ("COGNEVA_MEMORY_EMBEDDING_DIMENSION", "embedding_dimension"),
    ("COGNEVA_MEMORY_AUTO_INGEST", "auto_ingest"),
    (
        "COGNEVA_MEMORY_LOAD_EMBEDDING_MODEL",
        "load_embedding_model",
    ),
    ("COGNEVA_MEMORY_LOAD_RERANKER_MODEL", "load_reranker_model"),
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
    /// 加载把约 2.1GiB 权重读进常驻内存，且 dense 与 sparse 各开一个 session，
    /// 同一份权重实际占两遍；模型不在本地缓存时还会先从 HuggingFace 拉取——
    /// 离线集群里那次连接既不成功也不失败（客户端无超时），插件 init 会一直
    /// 挂着，直到存活探针把 Pod 杀掉。因此只有权重已就位（由部署侧以只读卷或
    /// 共享目录提供，权重本身不进镜像）且内存吃得下的部署才打开；其余保持关闭，
    /// 向量能力缺席但启动不受影响。重排模型（reranker）另有开关。
    pub load_embedding_model: bool,
    /// 启动期是否加载 ONNX 重排模型（BGE-Reranker-V2-M3，约 2.1GiB 常驻）。
    /// 关闭原因同 [`Self::load_embedding_model`]；其拉取路径写死
    /// `https://huggingface.co`、不吃 `HF_ENDPOINT`，离线集群只能靠本地就位。
    pub load_reranker_model: bool,
    /// 自动摄取（AgentEnd → 记忆三层）的运行参数。
    pub ingest: IngestConfig,
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
    /// 积压深度告警起点（达到后每翻倍打一条 WARN）。
    pub backlog_warn_at: usize,
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
            backlog_warn_at: 64,
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
