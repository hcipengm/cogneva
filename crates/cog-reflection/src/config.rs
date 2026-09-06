//! 晋级门 / GitOps 分发配置——cog-reflection 自有配置段。
//!
//! schema、解析、env 覆盖全部内聚在本 crate（core config.rs 不聚合单
//! crate 配置）。
//! 配置文件与主程序共用 cogneva.json 的 `self_evolution.promotion` 段，
//! env 覆盖变量保持 `COGNEVA_SELF_EVOLUTION_PROMOTION_*` /
//! `COGNEVA_GITOPS_*` 不变。

use serde::{Deserialize, Serialize};
use std::path::Path;

use cog_core::{SFError, SFResult};

/// 晋级门配置：
/// change 闯过沙盒验证后，按触及文件决定晋级通道（L0 热更新 /
/// L1 金丝雀自动 / L2 人工审批 / 黑名单拒收）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PromotionGateConfig {
    /// 自动晋级总开关；false = 一键暂停，全部变更转人工处理。
    pub enabled: bool,
    /// diff 超过该行数强制转人工审批。
    pub max_diff_lines: usize,
    /// 每 24 小时自动晋级配额，超出排队。
    pub quota_per_day: u32,
    /// 连续晋级后回滚次数达到该值触发熔断（转人工模式）。
    pub rollback_breaker_threshold: u32,
    /// 连续沙盒验证失败次数达到该值触发熔断。
    pub failure_breaker_threshold: u32,
    /// 沙盒部署成功后试跑观察时长（秒），期间健康不劣化才允许晋级。
    pub soak_secs: u64,
    /// L1 白名单路径前缀（低风险代码，可自动晋级）。
    pub whitelist_prefixes: Vec<String>,
    /// L2 核心路径前缀（必须人工审批）。判定优先于白名单。
    pub core_prefixes: Vec<String>,
    /// L0 配置路径前缀（热更新通道，不碰二进制）。
    pub config_prefixes: Vec<String>,
    /// 直接拒收的文件名（依赖清单/密钥文件），连沙盒都不让进。
    pub forbidden_names: Vec<String>,
    /// 直接拒收的扩展名。
    pub forbidden_extensions: Vec<String>,
    /// 晋级周报（eval 长期趋势）开关：周期聚合台账写报告文件 + 趋势向
    /// 下告警。
    pub trend_report_enabled: bool,
    /// 周报生成间隔（秒），默认一周。
    pub trend_report_interval_secs: u64,
    /// GitOps 分发配置。
    pub gitops: GitOpsConfig,
}

impl Default for PromotionGateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_diff_lines: 500,
            quota_per_day: 3,
            rollback_breaker_threshold: 2,
            failure_breaker_threshold: 3,
            soak_secs: 600,
            // 白名单故意收窄（宁严勿宽）：只有纯工具实现、前端、文档。
            // 未列出的路径一律按 L2 转人工。
            // `docs/` 是仓库内的外部/产品文档目录（可入库）；内部文档
            // （设计规格/审计/记录）存于仓库外的 internal-docs/，永不
            // 入库也永不入本白名单——改内部规格的 change 走"模糊从严"
            // 自动落 L2，规格变更审批权归人。
            whitelist_prefixes: vec![
                "crates/cog-agent/src/tools".into(),
                "web/".into(),
                "docs/".into(),
            ],
            core_prefixes: vec![
                "crates/cog-core/".into(),
                "crates/cog-storage/".into(),
                "crates/cog-orchestrator/".into(),
                "crates/cog-security-gateway/".into(),
                "crates/cog-llm/".into(),
                "crates/cog-gateway/src/auth".into(),
                "crates/cog-gateway/src/security_gateway.rs".into(),
                "deploy/".into(),
            ],
            config_prefixes: vec![
                "prompts/".into(),
                "deploy/k3s/cogneva-json-configmap.yaml".into(),
            ],
            forbidden_names: vec![
                "Cargo.toml".into(),
                "Cargo.lock".into(),
                ".env".into(),
                ".envrc".into(),
            ],
            forbidden_extensions: vec!["pem".into(), "key".into(), "crt".into(), "p12".into()],
            trend_report_enabled: true,
            trend_report_interval_secs: 604_800,
            gitops: GitOpsConfig::default(),
        }
    }
}

impl PromotionGateConfig {
    /// 从 cogneva.json 的 `self_evolution.promotion` 段加载，再叠加 env
    /// 覆盖。文件或段缺失时返回 Default（enabled=false，全部变更转人工，
    /// 安全侧）；段存在但解析失败、或 env 值非法时返回 Err——配置写错
    /// 必须响亮失败，不许静默降级成默认。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(Path::new(&path))
    }
    pub fn load_from(path: &Path) -> SFResult<Self> {
        let mut cfg = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                match root.pointer("/self_evolution/promotion") {
                    Some(section) => serde_json::from_value(section.clone()).map_err(|e| {
                        SFError::Config(format!("{} self_evolution.promotion: {e}", path.display()))
                    })?,
                    None => Self::default(),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cfg.apply_env_with(|k| std::env::var(k).ok())?;
        Ok(cfg)
    }

    /// env 覆盖。取值经 `get` 读取（测试可注入假 env）；非法值返回 Err。
    fn apply_env_with(&mut self, get: impl Fn(&str) -> Option<String>) -> SFResult<()> {
        fn parse<T: std::str::FromStr>(key: &str, raw: &str) -> SFResult<T> {
            raw.parse::<T>()
                .map_err(|_| SFError::Config(format!("{key} 值非法: {raw:?}")))
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_ENABLED") {
            self.enabled = parse("COGNEVA_SELF_EVOLUTION_PROMOTION_ENABLED", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_MAX_DIFF_LINES") {
            self.max_diff_lines = parse("COGNEVA_SELF_EVOLUTION_PROMOTION_MAX_DIFF_LINES", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_QUOTA_PER_DAY") {
            self.quota_per_day = parse("COGNEVA_SELF_EVOLUTION_PROMOTION_QUOTA_PER_DAY", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_SOAK_SECS") {
            self.soak_secs = parse("COGNEVA_SELF_EVOLUTION_PROMOTION_SOAK_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_ROLLBACK_BREAKER") {
            self.rollback_breaker_threshold =
                parse("COGNEVA_SELF_EVOLUTION_PROMOTION_ROLLBACK_BREAKER", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_FAILURE_BREAKER") {
            self.failure_breaker_threshold =
                parse("COGNEVA_SELF_EVOLUTION_PROMOTION_FAILURE_BREAKER", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_TREND_REPORT_ENABLED") {
            self.trend_report_enabled =
                parse("COGNEVA_SELF_EVOLUTION_PROMOTION_TREND_REPORT_ENABLED", &v)?;
        }
        if let Some(v) = get("COGNEVA_SELF_EVOLUTION_PROMOTION_TREND_REPORT_INTERVAL_SECS") {
            self.trend_report_interval_secs = parse(
                "COGNEVA_SELF_EVOLUTION_PROMOTION_TREND_REPORT_INTERVAL_SECS",
                &v,
            )?;
        }
        let g = &mut self.gitops;
        if let Some(v) = get("COGNEVA_GITOPS_ENABLED") {
            g.enabled = parse("COGNEVA_GITOPS_ENABLED", &v)?;
        }
        if let Some(v) = get("COGNEVA_GITOPS_REPO_URL") {
            g.repo_url = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_BRANCH") {
            g.branch = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_POLL_INTERVAL_SECS") {
            g.poll_interval_secs = parse("COGNEVA_GITOPS_POLL_INTERVAL_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_GITOPS_REGISTRY") {
            g.registry = if v.is_empty() { None } else { Some(v) };
        }
        if let Some(v) = get("COGNEVA_GITOPS_LOCAL_REGISTRY") {
            g.local_registry = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_WORK_DIR") {
            g.work_dir = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_NAMESPACE") {
            g.namespace = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_DEPLOYMENT") {
            g.deployment = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_CONTAINER") {
            g.container = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_CANARY_WATCH_SECS") {
            g.canary_watch_secs = parse("COGNEVA_GITOPS_CANARY_WATCH_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_GITOPS_KUBECTL_BIN") {
            g.kubectl_bin = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_BUILDER_BIN") {
            g.builder_bin = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_CANARY_ERROR_RATE_MULTIPLIER") {
            g.canary_error_rate_multiplier =
                parse("COGNEVA_GITOPS_CANARY_ERROR_RATE_MULTIPLIER", &v)?;
        }
        if let Some(v) = get("COGNEVA_GITOPS_CANARY_P99_MULTIPLIER") {
            g.canary_p99_multiplier = parse("COGNEVA_GITOPS_CANARY_P99_MULTIPLIER", &v)?;
        }
        if let Some(v) = get("COGNEVA_GITOPS_PULLER_ENABLED") {
            g.puller_enabled = parse("COGNEVA_GITOPS_PULLER_ENABLED", &v)?;
        }
        if let Some(v) = get("COGNEVA_GITOPS_GIT_USER_NAME") {
            g.git_user_name = v;
        }
        if let Some(v) = get("COGNEVA_GITOPS_GIT_USER_EMAIL") {
            g.git_user_email = v;
        }
        Ok(())
    }
}

/// 基线移植触发器配置（规则3：公版出新 release tag 后把历代晋级变更
/// 自治移植到新基线）。porter 本体在沙盒进化 Pod 内运行，轮询上游 tag，
/// 产出 `evol/<id>` 分支与 `gen-n` 代际 tag。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BaselinePortConfig {
    /// 移植触发循环总开关。默认开启：外层已由 self_evolution.enabled 与
    /// 沙盒边界双重把门，进了沙盒的实例应当自治跟上新基线。
    pub enabled: bool,
    /// 上游 release tag 轮询间隔（秒）。
    pub poll_interval_secs: u64,
    /// 同一新基线移植失败后的重试冷却（秒）；成功移植永久不再重跑。
    pub retry_cooldown_secs: u64,
}

impl Default for BaselinePortConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 3600,
            retry_cooldown_secs: 86_400,
        }
    }
}

impl BaselinePortConfig {
    /// 从 cogneva.json 的 `self_evolution.baseline_port` 段加载，再叠加
    /// env 覆盖。文件或段缺失时返回 Default；段存在但解析失败、或 env
    /// 值非法时返回 Err——配置写错必须响亮失败。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(Path::new(&path))
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        let mut cfg = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                match root.pointer("/self_evolution/baseline_port") {
                    Some(section) => serde_json::from_value(section.clone()).map_err(|e| {
                        SFError::Config(format!(
                            "{} self_evolution.baseline_port: {e}",
                            path.display()
                        ))
                    })?,
                    None => Self::default(),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cfg.apply_env_with(|k| std::env::var(k).ok())?;
        Ok(cfg)
    }

    /// env 覆盖。取值经 `get` 读取（测试可注入假 env）；非法值返回 Err。
    fn apply_env_with(&mut self, get: impl Fn(&str) -> Option<String>) -> SFResult<()> {
        fn parse<T: std::str::FromStr>(key: &str, raw: &str) -> SFResult<T> {
            raw.parse::<T>()
                .map_err(|_| SFError::Config(format!("{key} 值非法: {raw:?}")))
        }
        if let Some(v) = get("COGNEVA_BASELINE_PORT_ENABLED") {
            self.enabled = parse("COGNEVA_BASELINE_PORT_ENABLED", &v)?;
        }
        if let Some(v) = get("COGNEVA_BASELINE_PORT_POLL_INTERVAL_SECS") {
            self.poll_interval_secs = parse("COGNEVA_BASELINE_PORT_POLL_INTERVAL_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_BASELINE_PORT_RETRY_COOLDOWN_SECS") {
            self.retry_cooldown_secs = parse("COGNEVA_BASELINE_PORT_RETRY_COOLDOWN_SECS", &v)?;
        }
        Ok(())
    }
}

/// GitOps 分发配置（路线 B：推送端只推中央仓库，拉取端各自自治，
/// 沙盒全程不持有任何集群凭证）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GitOpsConfig {
    pub enabled: bool,
    /// 中央仓库地址（推送端 push / 拉取端 poll，三仓库同步既有通道）。
    pub repo_url: String,
    /// 晋级 release 分支。
    pub branch: String,
    /// 拉取端轮询间隔（秒）。
    pub poll_interval_secs: u64,
    /// 外部镜像仓库（可选，跨集群生产形态）：Some 时推送端 push、拉取端 pull
    /// 该仓库；None 时走集群内 registry（NodePort localhost 引用）。
    pub registry: Option<String>,
    /// 集群内 registry 的节点侧 pull 引用（拉取端 set image 用）：
    /// kubelet/containerd 在节点上经 localhost NodePort pull，http 免 TLS。
    pub local_registry: String,
    /// 拉取端工作目录（checkout / 构建）。
    pub work_dir: String,
    pub kubectl_bin: String,
    /// 推送端镜像构建器可执行文件（buildah / podman）。
    pub builder_bin: String,
    pub namespace: String,
    pub deployment: String,
    pub container: String,
    /// 金丝雀单阶段看护时长（秒）。
    pub canary_watch_secs: u64,
    /// 看护阈值：错误率超过基线该倍数判定回归。
    pub canary_error_rate_multiplier: f64,
    /// 看护阈值：P99 延迟超过基线该倍数判定回归。
    pub canary_p99_multiplier: f64,
    /// 拉取端开关：推送端（沙盒进化 Pod）置 false，只发布晋级产物，
    /// 不在本进程跑 poll/金丝雀（沙盒无 kubectl，也不该操作生产部署）。
    pub puller_enabled: bool,
    /// 拉取端 git 身份。
    pub git_user_name: String,
    pub git_user_email: String,
}

impl Default for GitOpsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            repo_url: String::new(),
            branch: "evolution-release".into(),
            poll_interval_secs: 120,
            registry: None,
            local_registry: "localhost:30500".into(),
            work_dir: "/opt/cogneva/gitops".into(),
            kubectl_bin: "kubectl".into(),
            builder_bin: "buildah".into(),
            namespace: "cogneva".into(),
            deployment: "cogneva".into(),
            container: "cogneva".into(),
            canary_watch_secs: 600,
            canary_error_rate_multiplier: 1.5,
            canary_p99_multiplier: 1.3,
            puller_enabled: true,
            git_user_name: "Cogneva Self-Evolution".into(),
            git_user_email: "self-evolution@cogneva.ai".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn baseline_port_defaults_and_section_load() {
        let dir = std::env::temp_dir().join(format!("cog-reflection-bp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        // 段缺失：默认开启，默认节奏。
        std::fs::write(&path, r#"{"self_evolution": {"enabled": true}}"#).unwrap();
        let cfg = BaselinePortConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.poll_interval_secs, 3600);
        assert_eq!(cfg.retry_cooldown_secs, 86_400);
        // 段存在：按段取值，未写字段保持默认。
        std::fs::write(
            &path,
            r#"{"self_evolution": {"baseline_port": {"enabled": false, "poll_interval_secs": 120}}}"#,
        )
        .unwrap();
        let cfg = BaselinePortConfig::load_from(&path).unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.poll_interval_secs, 120);
        assert_eq!(cfg.retry_cooldown_secs, 86_400);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn baseline_port_env_overrides_and_invalid_is_loud() {
        let env: HashMap<&str, &str> = [
            ("COGNEVA_BASELINE_PORT_ENABLED", "false"),
            ("COGNEVA_BASELINE_PORT_RETRY_COOLDOWN_SECS", "3600"),
        ]
        .into_iter()
        .collect();
        let mut cfg = BaselinePortConfig::default();
        cfg.apply_env_with(|k| env.get(k).map(|s| s.to_string()))
            .unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.retry_cooldown_secs, 3600);

        let bad: HashMap<&str, &str> = [("COGNEVA_BASELINE_PORT_ENABLED", "maybe")]
            .into_iter()
            .collect();
        let mut cfg = BaselinePortConfig::default();
        assert!(cfg
            .apply_env_with(|k| bad.get(k).map(|s| s.to_string()))
            .is_err());
    }

    #[test]
    fn default_is_safe_side() {
        let cfg = PromotionGateConfig::default();
        assert!(!cfg.enabled);
        assert!(!cfg.gitops.enabled);
        assert!(cfg.gitops.puller_enabled);
    }

    #[test]
    fn load_missing_file_returns_default() {
        let cfg = PromotionGateConfig::load_from(Path::new("/nonexistent/cogneva.json")).unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn load_reads_promotion_section() {
        let dir = std::env::temp_dir().join(format!("cog-reflection-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"self_evolution": {"enabled": true, "promotion": {
                "enabled": true, "quota_per_day": 7,
                "gitops": {"enabled": true, "repo_url": "/host-git", "poll_interval_secs": 30}
            }}}"#,
        )
        .unwrap();
        let cfg = PromotionGateConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.quota_per_day, 7);
        assert!(cfg.gitops.enabled);
        assert_eq!(cfg.gitops.repo_url, "/host-git");
        assert_eq!(cfg.gitops.poll_interval_secs, 30);
        // 段内未写的字段保持默认
        assert_eq!(cfg.gitops.branch, "evolution-release");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_section_missing_returns_default() {
        let dir = std::env::temp_dir().join(format!("cog-reflection-cfg2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(&path, r#"{"self_evolution": {"enabled": true}}"#).unwrap();
        let cfg = PromotionGateConfig::load_from(&path).unwrap();
        assert!(!cfg.enabled);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_malformed_section_is_loud_error() {
        let dir = std::env::temp_dir().join(format!("cog-reflection-cfg3-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"self_evolution": {"promotion": {"quota_per_day": "not-a-number"}}}"#,
        )
        .unwrap();
        assert!(PromotionGateConfig::load_from(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn env_overrides_section() {
        let env: HashMap<&str, &str> = [
            ("COGNEVA_GITOPS_ENABLED", "true"),
            ("COGNEVA_GITOPS_REPO_URL", "/host-git"),
            ("COGNEVA_GITOPS_POLL_INTERVAL_SECS", "30"),
            ("COGNEVA_GITOPS_CANARY_WATCH_SECS", "60"),
            ("COGNEVA_GITOPS_REGISTRY", "localhost"),
            ("COGNEVA_GITOPS_PULLER_ENABLED", "false"),
            ("COGNEVA_GITOPS_CANARY_P99_MULTIPLIER", "1.9"),
            ("COGNEVA_SELF_EVOLUTION_PROMOTION_ENABLED", "true"),
            ("COGNEVA_SELF_EVOLUTION_PROMOTION_QUOTA_PER_DAY", "5"),
        ]
        .into_iter()
        .collect();
        let mut cfg = PromotionGateConfig::default();
        cfg.apply_env_with(|k| env.get(k).map(|s| s.to_string()))
            .unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.quota_per_day, 5);
        assert!(cfg.gitops.enabled);
        assert_eq!(cfg.gitops.repo_url, "/host-git");
        assert_eq!(cfg.gitops.poll_interval_secs, 30);
        assert_eq!(cfg.gitops.canary_watch_secs, 60);
        assert_eq!(cfg.gitops.registry.as_deref(), Some("localhost"));
        assert!(!cfg.gitops.puller_enabled);
        assert!((cfg.gitops.canary_p99_multiplier - 1.9).abs() < f64::EPSILON);
    }

    #[test]
    fn env_empty_registry_means_none() {
        let env: HashMap<&str, &str> = [("COGNEVA_GITOPS_REGISTRY", "")].into_iter().collect();
        let mut cfg = PromotionGateConfig::default();
        cfg.gitops.registry = Some("old".into());
        cfg.apply_env_with(|k| env.get(k).map(|s| s.to_string()))
            .unwrap();
        assert_eq!(cfg.gitops.registry, None);
    }

    #[test]
    fn env_invalid_value_is_loud_error() {
        let env: HashMap<&str, &str> = [("COGNEVA_GITOPS_ENABLED", "yes-please")]
            .into_iter()
            .collect();
        let mut cfg = PromotionGateConfig::default();
        assert!(cfg
            .apply_env_with(|k| env.get(k).map(|s| s.to_string()))
            .is_err());
    }
}
