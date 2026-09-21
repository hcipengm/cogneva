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
        if let Some(v) = get("COGNEVA_GITOPS_CANARY_MIN_REQUESTS_FOR_RATE") {
            g.canary_min_requests_for_rate =
                parse("COGNEVA_GITOPS_CANARY_MIN_REQUESTS_FOR_RATE", &v)?;
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
    /// 累积计数器语义下，两次抓取之间至少要新增多少个请求才允许对错误率
    /// 下结论。增量太小时一条 5xx 就能把比值抬到任意高，好版本会被判成回归；
    /// 一版错误率下限 1%，要把 1% 与 0% 分出来需要百量级的样本，故默认 100。
    pub canary_min_requests_for_rate: f64,
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
            canary_min_requests_for_rate: 100.0,
            puller_enabled: true,
            git_user_name: "Cogneva Self-Evolution".into(),
            git_user_email: "self-evolution@cogneva.ai".into(),
        }
    }
}

/// 主线跟踪自动部署器的单个滚动目标。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutTargetConfig {
    /// Deployment 名（`kubectl set image deployment/<name>` 中的 name）。
    pub deployment: String,
    /// Deployment 内容器名。
    pub container: String,
    /// Pod 标签 app.kubernetes.io/component（健康检查选择器用）。
    pub component: String,
    /// Pod 标签 app.kubernetes.io/name（健康检查选择器用）。主应用/网关/
    /// 执行器的 name 都是 `cogneva`，必须 name+component 双标签才能区分。
    pub name: String,
    /// 该 Deployment 在清单目录内的文件名。滚版时 Job 以 apply 这份清单
    /// （镜像改写为目标不可变 tag）交付完整 Pod spec——env/卷/挂载的变更
    /// 因此随镜像一起到达集群；None 时该目标退回 set image 旧路径。
    #[serde(default)]
    pub manifest: Option<String>,
}

/// 上游跟踪的代码平台。取值决定网关 git 透传面的路径段（`/git/{slug}/`）；
/// 凭证由网关在出口注入，本进程不持有任何 token。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodePlatform {
    Github,
    Gitee,
}

impl CodePlatform {
    pub fn slug(self) -> &'static str {
        match self {
            CodePlatform::Github => "github",
            CodePlatform::Gitee => "gitee",
        }
    }
}

/// 一个被跟踪的上游：平台 + `owner/repo`。同一仓库在两端镜像时配两项，
/// 集群内按祖先关系择新；两端真分叉则不动主线并响亮告警——猜一个方向
/// 等于用一次分叉决定集群跑谁的代码。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamTrackConfig {
    pub platform: CodePlatform,
    pub repo: String,
    /// 该平台的 API 基址（安全网关透传端点，凭证由网关出口注入）。空 =
    /// 读不到该平台的 CI 结论，滚动照常推进——把"读不到"当成失败会让一次
    /// 上游抖动停掉整条主线跟踪。留空时与 `repo` 同源推导。
    #[serde(default)]
    pub api_base: Option<String>,
}

/// 主线跟踪自动部署器配置：进化 Pod 内常驻循环，检测集群内 bare 仓库
/// （/host-git）公版 main 前进后，沙盒内增量构建 → buildah 叠层推集群内
/// registry → 派独立 Job 门禁滚动四个 deployment，失败自动回滚。
/// bare 的 main 由本部署器自己从各上游平台拉取推进（upstreams），
/// 不再依赖宿主机上的定时器喂仓。
/// 默认关闭：依赖 RBAC/NetworkPolicy/registry 就位，缺失时不空转报错。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MainlineDeployerConfig {
    pub enabled: bool,
    /// 轮询 bare 仓库 main 的间隔（秒）。
    pub poll_interval_secs: u64,
    /// 集群内 bare 仓库路径（git --git-dir 用）。
    pub bare_repo: String,
    /// 跟踪的上游分支。
    pub branch: String,
    /// 上游跟踪源。空 = 不跟踪上游（只跟随 bare，需外部喂仓）。留空时从
    /// `github_integration` / `gitee_integration` 的 `repo` 推导——仓库身份
    /// 只有一个家，部署器只消费它，不另写一份。
    pub upstreams: Vec<UpstreamTrackConfig>,
    /// 网关 git 透传根：上游 URL = `{base}/{platform}/{owner}/{repo}.git`，
    /// 凭证由网关在出口注入。空 = 不跟踪上游。
    pub git_proxy_base: String,
    /// 单次上游 fetch 的超时（秒）。取的是网络往返的上界：网关到平台
    /// 断了要能在一轮轮询内翻篇，不能把主循环挂在这里。
    pub upstream_fetch_timeout_secs: u64,
    /// buildah 在进化 Pod 内 push/from 的 registry 端点（集群 DNS，http）：
    /// Pod 内可解析 svc 名，配合 --tls-verify=false。
    pub registry: String,
    /// kubelet 在节点上 pull 镜像的引用端点（NodePort localhost，http 免 TLS）：
    /// 节点 containerd 不解析集群 DNS，Job manifest 与 set image 的镜像引用
    /// 必须用这个端点；与 registry 指向同一个 registry 的两条访问路径。
    pub local_registry: String,
    pub namespace: String,
    pub builder_bin: String,
    pub kubectl_bin: String,
    /// 滚动 Job Pod 内 kubectl 的供给：宿主 k3s 多调用二进制路径
    /// （K3s 为 /usr/local/bin/k3s），非空时 Job 以 hostPath File 挂到
    /// /usr/local/bin/kubectl；空串表示镜像自带 kubectl、不挂宿主文件。
    pub kubectl_host_path: String,
    /// 状态/锁文件目录（sandbox PVC 上，跨重启持久）。
    pub state_dir: String,
    /// 单次构建超时（秒），也是构建锁陈旧判定阈值。
    pub build_timeout_secs: u64,
    /// cargo build --jobs 值（4C/7.5G 节点禁并发构建，默认 2）。
    pub cargo_build_jobs: u32,
    /// 四部署全部滚完后的 soak 观察窗（秒）。
    pub soak_secs: u64,
    /// Pod 重启次数超过该值判病。
    pub restart_threshold: u32,
    /// 同一 rev 失败后的冷却（秒），冷却内不重试。
    pub failure_cooldown_secs: u64,
    /// 同一 rev 最多尝试次数（超过则等下一个 rev）。
    pub max_attempts_per_rev: u32,
    /// 单个 deployment 滚动等待超时（秒）。只计"就绪"预算：Pod 的 init
    /// 容器还在跑时不计入（见 startup_timeout_secs）。
    pub rollout_timeout_secs: u64,
    /// 单个 deployment 启动阶段的上界（秒）：Pod 的 init 容器尚未结束时
    /// 滚动处于启动阶段，此阶段不计入 rollout_timeout_secs。种子/镜像拉取
    /// 是必须在主容器之前结束的背景准备工作，其耗时与本次要上线的版本
    /// 无关；把它算进就绪预算，一次慢克隆就能把好版本拖过预算判成失败
    /// 回滚。此值是启动阶段自己的上界：init 真卡死时不能无限等，否则拿不到
    /// 干净回滚。
    pub startup_timeout_secs: u64,
    /// 滚动判定 Job 容器的资源面（requests/limits，K8s 量纲字符串）。
    /// 缺任何一项都会让该容器的 QoS 落回 BestEffort。默认 requests 按容器
    /// 实测常驻量给（轮询等待态 0m / 5Mi），limits 承接 kubectl 子进程尖峰；
    /// 为什么不能让这个容器是 BestEffort 见 Job 清单处的注释。
    pub job_cpu_request: String,
    pub job_memory_request: String,
    pub job_cpu_limit: String,
    pub job_memory_limit: String,
    /// 空闲心跳日志的最小间隔（秒）。SameRev 收敛路径静默返回（无推进即
    /// 无日志），单凭日志无法证明部署器存活；心跳按该间隔打一条 INFO
    /// 状态摘要（bare HEAD / last_good / in_flight / 失败计数）。0 表示
    /// 每轮轮询都打。
    pub heartbeat_log_secs: u64,
    /// 清单目录（仓库内相对路径）。目录里的 `kustomization.yaml` resources
    /// 列表是发布资源集的权威清单：滚动 Job 按它 apply 支撑资源（configmap/
    /// service/RBAC/基础设施负载），集群级资源（Namespace/StorageClass 等）
    /// 属安装期产物、自动跳过，Secret 按设计不入清单、出现即拒绝。
    pub manifest_dir: String,
    /// 是否让滚动 Job 以 apply 仓库清单交付完整 spec。false 时退回纯
    /// set image 旧路径（清单变更不随镜像下发）。
    pub deliver_manifests: bool,
    /// 滚动目标，顺序即滚动顺序。默认：网关代理面先行，进化宿主最后。
    pub targets: Vec<RolloutTargetConfig>,
}

impl Default for MainlineDeployerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_secs: 600,
            bare_repo: "/host-git".into(),
            branch: "main".into(),
            upstreams: Vec::new(),
            git_proxy_base: String::new(),
            upstream_fetch_timeout_secs: 300,
            registry: "cogneva-registry.cogneva.svc.cluster.local:5000".into(),
            local_registry: "localhost:30500".into(),
            namespace: "cogneva".into(),
            builder_bin: "buildah".into(),
            kubectl_bin: "kubectl".into(),
            kubectl_host_path: String::new(),
            state_dir: "/opt/cogneva/sandbox/mainline".into(),
            build_timeout_secs: 3600,
            cargo_build_jobs: 2,
            soak_secs: 120,
            restart_threshold: 1,
            failure_cooldown_secs: 3600,
            max_attempts_per_rev: 2,
            rollout_timeout_secs: 300,
            startup_timeout_secs: 900,
            job_cpu_request: "10m".into(),
            job_memory_request: "32Mi".into(),
            job_cpu_limit: "500m".into(),
            job_memory_limit: "256Mi".into(),
            heartbeat_log_secs: 3600,
            manifest_dir: "deploy/k3s".into(),
            deliver_manifests: true,
            targets: vec![
                RolloutTargetConfig {
                    deployment: "cogneva-security-gateway".into(),
                    container: "security-gateway".into(),
                    component: "security-gateway".into(),
                    name: "cogneva".into(),
                    manifest: Some("gateway-deployment.yaml".into()),
                },
                RolloutTargetConfig {
                    deployment: "cogneva-sandbox-executor".into(),
                    container: "sandbox-executor".into(),
                    component: "sandbox-executor".into(),
                    name: "cogneva".into(),
                    manifest: Some("sandbox-executor-deployment.yaml".into()),
                },
                RolloutTargetConfig {
                    deployment: "cogneva".into(),
                    container: "cogneva".into(),
                    component: "gateway".into(),
                    name: "cogneva".into(),
                    manifest: Some("deployment.yaml".into()),
                },
                RolloutTargetConfig {
                    deployment: "cogneva-evolution".into(),
                    container: "cogneva".into(),
                    component: "evolution".into(),
                    name: "cogneva-evolution".into(),
                    manifest: Some("evolution-deployment.yaml".into()),
                },
            ],
        }
    }
}

/// 跟踪源留空时，从 `github_integration` / `gitee_integration` 的 `repo`
/// 推导：同一仓库在两端镜像，两端都跟。两个平台都关或都没有 repo 时返回
/// 空列表——那表示"这台部署不跟踪上游"，不是错误，但部署器会据此在心跳里
/// 明说（只跟随 bare 的部署必须能从日志看出来，否则它和"跟踪坏了"长得一样）。
fn upstreams_from_integrations(
    root: &serde_json::Value,
    get: impl Fn(&str) -> Option<String>,
) -> Vec<UpstreamTrackConfig> {
    let mut out = Vec::new();
    for (platform, pointer, api_env) in [
        (
            CodePlatform::Github,
            "/github_integration",
            cog_core::contract::ci::GITHUB_API_BASE_ENV,
        ),
        (
            CodePlatform::Gitee,
            "/gitee_integration",
            cog_core::contract::ci::GITEE_API_BASE_ENV,
        ),
    ] {
        let Some(section) = root.pointer(pointer) else {
            continue;
        };
        if section.get("enabled").and_then(|v| v.as_bool()) != Some(true) {
            continue;
        }
        let repo = section
            .get("repo")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        if repo.is_empty() {
            continue;
        }
        // 基址与 repo 的来源顺序一致：先是集成段自己的字段，再是该平台的
        // env 覆盖（部署里真正设的就是 env，段字段多为 null）。
        let api_base = section
            .get("api_base")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| get(api_env).map(|v| v.trim().to_string()))
            .filter(|s| !s.is_empty());
        out.push(UpstreamTrackConfig {
            platform,
            repo: repo.to_string(),
            api_base,
        });
    }
    out
}

impl MainlineDeployerConfig {
    /// 从 cogneva.json 的 `self_evolution.mainline_deployer` 段加载，再叠加
    /// env 覆盖。文件或段缺失时返回 Default（enabled=false，安全侧）；
    /// 段存在但解析失败、或 env 值非法时返回 Err——配置写错必须响亮失败。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(Path::new(&path))
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        let (mut cfg, root) = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                let cfg = match root.pointer("/self_evolution/mainline_deployer") {
                    Some(section) => serde_json::from_value(section.clone()).map_err(|e| {
                        SFError::Config(format!(
                            "{} self_evolution.mainline_deployer: {e}",
                            path.display()
                        ))
                    })?,
                    None => Self::default(),
                };
                (cfg, Some(root))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (Self::default(), None),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cfg.apply_env_with(|k| std::env::var(k).ok())?;
        if cfg.upstreams.is_empty() {
            if let Some(root) = root.as_ref() {
                cfg.upstreams = upstreams_from_integrations(root, |k| std::env::var(k).ok());
            }
        }
        Ok(cfg)
    }

    /// env 覆盖。取值经 `get` 读取（测试可注入假 env）；非法值返回 Err。
    pub fn apply_env_with(&mut self, get: impl Fn(&str) -> Option<String>) -> SFResult<()> {
        fn parse<T: std::str::FromStr>(key: &str, raw: &str) -> SFResult<T> {
            raw.parse::<T>()
                .map_err(|_| SFError::Config(format!("{key} 值非法: {raw:?}")))
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_ENABLED") {
            self.enabled = parse("COGNEVA_MAINLINE_DEPLOYER_ENABLED", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_POLL_INTERVAL_SECS") {
            self.poll_interval_secs = parse("COGNEVA_MAINLINE_DEPLOYER_POLL_INTERVAL_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_BARE_REPO") {
            self.bare_repo = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_BRANCH") {
            self.branch = v;
        }
        if let Some(v) = get("COGNEVA_GIT_PROXY_BASE") {
            self.git_proxy_base = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_UPSTREAM_FETCH_TIMEOUT_SECS") {
            self.upstream_fetch_timeout_secs =
                parse("COGNEVA_MAINLINE_DEPLOYER_UPSTREAM_FETCH_TIMEOUT_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_REGISTRY") {
            self.registry = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_LOCAL_REGISTRY") {
            self.local_registry = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_NAMESPACE") {
            self.namespace = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_STATE_DIR") {
            self.state_dir = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_BUILD_TIMEOUT_SECS") {
            self.build_timeout_secs = parse("COGNEVA_MAINLINE_DEPLOYER_BUILD_TIMEOUT_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_CARGO_BUILD_JOBS") {
            self.cargo_build_jobs = parse("COGNEVA_MAINLINE_DEPLOYER_CARGO_BUILD_JOBS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_SOAK_SECS") {
            self.soak_secs = parse("COGNEVA_MAINLINE_DEPLOYER_SOAK_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_RESTART_THRESHOLD") {
            self.restart_threshold = parse("COGNEVA_MAINLINE_DEPLOYER_RESTART_THRESHOLD", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_FAILURE_COOLDOWN_SECS") {
            self.failure_cooldown_secs =
                parse("COGNEVA_MAINLINE_DEPLOYER_FAILURE_COOLDOWN_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_MAX_ATTEMPTS") {
            self.max_attempts_per_rev = parse("COGNEVA_MAINLINE_DEPLOYER_MAX_ATTEMPTS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_TIMEOUT_SECS") {
            self.rollout_timeout_secs =
                parse("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_TIMEOUT_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_STARTUP_TIMEOUT_SECS") {
            self.startup_timeout_secs =
                parse("COGNEVA_MAINLINE_DEPLOYER_STARTUP_TIMEOUT_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_REQUEST") {
            self.job_cpu_request = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_REQUEST") {
            self.job_memory_request = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT") {
            self.job_cpu_limit = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_LIMIT") {
            self.job_memory_limit = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_HEARTBEAT_LOG_SECS") {
            self.heartbeat_log_secs = parse("COGNEVA_MAINLINE_DEPLOYER_HEARTBEAT_LOG_SECS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_MANIFEST_DIR") {
            self.manifest_dir = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_DELIVER_MANIFESTS") {
            self.deliver_manifests = parse("COGNEVA_MAINLINE_DEPLOYER_DELIVER_MANIFESTS", &v)?;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_BUILDER_BIN") {
            self.builder_bin = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_KUBECTL_BIN") {
            self.kubectl_bin = v;
        }
        if let Some(v) = get("COGNEVA_MAINLINE_DEPLOYER_KUBECTL_HOST_PATH") {
            self.kubectl_host_path = v;
        }
        Ok(())
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

    /// 仓库身份只有一个家：部署器的上游跟踪源留空时从既有的集成段推导，
    /// 两端镜像都跟——只跟一端会让另一个平台上的提交永远进不了集群。
    #[test]
    fn mainline_upstreams_default_to_both_integration_repos() {
        let dir = std::env::temp_dir().join(format!("cog-reflection-ml-up-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{
              "self_evolution": {"enabled": true},
              "github_integration": {"enabled": true, "repo": "o/r"},
              "gitee_integration": {"enabled": true, "repo": "o/r"}
            }"#,
        )
        .unwrap();
        let cfg = MainlineDeployerConfig::load_from(&path).unwrap();
        let platforms: Vec<&str> = cfg.upstreams.iter().map(|u| u.platform.slug()).collect();
        assert_eq!(platforms, vec!["github", "gitee"]);
        assert!(cfg.upstreams.iter().all(|u| u.repo == "o/r"));

        // 显式写了跟踪源就以显式为准，不再推导。
        std::fs::write(
            &path,
            r#"{
              "self_evolution": {"mainline_deployer": {"upstreams": [{"platform": "gitee", "repo": "x/y"}]}},
              "github_integration": {"enabled": true, "repo": "o/r"},
              "gitee_integration": {"enabled": true, "repo": "o/r"}
            }"#,
        )
        .unwrap();
        let cfg = MainlineDeployerConfig::load_from(&path).unwrap();
        assert_eq!(cfg.upstreams.len(), 1);
        assert_eq!(cfg.upstreams[0].repo, "x/y");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 关掉或没配 repo 的平台不进跟踪列表：那表示这台部署不跟这一端，
    /// 不是"跟一个空仓库"。
    #[test]
    fn disabled_platforms_are_not_tracked() {
        let root: serde_json::Value = serde_json::from_str(
            r#"{
              "github_integration": {"enabled": false, "repo": "o/r"},
              "gitee_integration": {"enabled": true, "repo": "  "}
            }"#,
        )
        .unwrap();
        assert!(upstreams_from_integrations(&root, |_| None).is_empty());
    }

    /// 滚动门禁要读该 rev 的 CI 结论，所以跟踪项必须带上读得到的基址；
    /// 段字段优先，其次该平台自己的 env 覆盖（部署里设的正是 env）。
    #[test]
    fn upstreams_carry_the_platform_api_base() {
        let root: serde_json::Value = serde_json::from_str(
            r#"{
              "github_integration": {"enabled": true, "repo": "o/r",
                                     "api_base": "http://gw:8081/github"},
              "gitee_integration": {"enabled": true, "repo": "o/r", "api_base": null}
            }"#,
        )
        .unwrap();
        let ups = upstreams_from_integrations(&root, |k| {
            (k == "COGNEVA_GITEE_API_BASE").then(|| "http://gw:8081/gitee".to_string())
        });
        assert_eq!(ups[0].api_base.as_deref(), Some("http://gw:8081/github"));
        assert_eq!(ups[1].api_base.as_deref(), Some("http://gw:8081/gitee"));

        // 两处都没有基址：跟踪照常，只是读不到 CI 结论（门禁按无证据放行）。
        let bare: serde_json::Value =
            serde_json::from_str(r#"{"github_integration": {"enabled": true, "repo": "o/r"}}"#)
                .unwrap();
        let ups = upstreams_from_integrations(&bare, |_| None);
        assert!(ups[0].api_base.is_none());
        assert_eq!(ups[0].repo, "o/r");
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

    #[test]
    fn mainline_defaults_safe_side_and_target_order() {
        let cfg = MainlineDeployerConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.targets.len(), 4);
        // 滚动顺序：网关代理面先行，进化宿主最后。
        assert_eq!(cfg.targets[0].component, "security-gateway");
        assert_eq!(cfg.targets[1].component, "sandbox-executor");
        assert_eq!(cfg.targets[2].component, "gateway");
        assert_eq!(cfg.targets[3].component, "evolution");
        // 主/网关/执行器 name 都是 cogneva，单标签选择器会跨部署误判。
        assert_eq!(cfg.targets[0].name, "cogneva");
        assert_eq!(cfg.targets[3].name, "cogneva-evolution");
    }

    #[test]
    fn mainline_section_load_and_env_overrides() {
        let dir =
            std::env::temp_dir().join(format!("cog-reflection-ml-env-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"self_evolution": {"mainline_deployer": {
                "enabled": true, "poll_interval_secs": 42,
                "targets": [{"deployment": "d1", "container": "c1", "component": "gateway", "name": "cogneva"}]
            }}}"#,
        )
        .unwrap();
        let cfg = MainlineDeployerConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.poll_interval_secs, 42);
        assert_eq!(cfg.targets.len(), 1);
        assert_eq!(cfg.targets[0].deployment, "d1");
        std::fs::remove_dir_all(&dir).ok();

        let env: HashMap<&str, &str> = [
            ("COGNEVA_MAINLINE_DEPLOYER_ENABLED", "true"),
            ("COGNEVA_MAINLINE_DEPLOYER_POLL_INTERVAL_SECS", "30"),
            ("COGNEVA_MAINLINE_DEPLOYER_REGISTRY", "reg.local:5000"),
            ("COGNEVA_MAINLINE_DEPLOYER_CARGO_BUILD_JOBS", "1"),
            ("COGNEVA_MAINLINE_DEPLOYER_HEARTBEAT_LOG_SECS", "120"),
        ]
        .into_iter()
        .collect();
        let mut cfg = MainlineDeployerConfig::default();
        assert_eq!(cfg.heartbeat_log_secs, 3600);
        cfg.apply_env_with(|k| env.get(k).map(|s| s.to_string()))
            .unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.poll_interval_secs, 30);
        assert_eq!(cfg.registry, "reg.local:5000");
        assert_eq!(cfg.cargo_build_jobs, 1);
        assert_eq!(cfg.heartbeat_log_secs, 120);

        let bad: HashMap<&str, &str> = [("COGNEVA_MAINLINE_DEPLOYER_ENABLED", "nope")]
            .into_iter()
            .collect();
        let mut cfg = MainlineDeployerConfig::default();
        assert!(cfg
            .apply_env_with(|k| bad.get(k).map(|s| s.to_string()))
            .is_err());
    }

    #[test]
    fn rollout_budget_and_job_resources_come_from_the_config_surface() {
        let env: HashMap<&str, &str> = [
            ("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_TIMEOUT_SECS", "420"),
            ("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_REQUEST", "20m"),
            (
                "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_REQUEST",
                "64Mi",
            ),
            ("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT", "1"),
            (
                "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_LIMIT",
                "512Mi",
            ),
        ]
        .into_iter()
        .collect();
        let mut cfg = MainlineDeployerConfig::default();
        // 默认值不与配置面脱钩：就绪预算仍是 300s，Job 有显式 requests/limits
        // （缺任何一项都会退化成 BestEffort）。
        assert_eq!(cfg.rollout_timeout_secs, 300);
        assert!(!cfg.job_cpu_request.is_empty());
        assert!(!cfg.job_memory_request.is_empty());
        assert!(!cfg.job_cpu_limit.is_empty());
        assert!(!cfg.job_memory_limit.is_empty());
        cfg.apply_env_with(|k| env.get(k).map(|s| s.to_string()))
            .unwrap();
        assert_eq!(cfg.rollout_timeout_secs, 420);
        assert_eq!(cfg.job_cpu_request, "20m");
        assert_eq!(cfg.job_memory_request, "64Mi");
        assert_eq!(cfg.job_cpu_limit, "1");
        assert_eq!(cfg.job_memory_limit, "512Mi");
    }
}
