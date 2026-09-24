//! git 传输自适应兜底：HTTPS 快路径 + SSH 兜底镜像。
//!
//! **为什么要兜底**：业务 Pod 的 git 出站只有一条路——网关的
//! `/git/{platform}/...` 透传（沙盒零凭证，见 [`crate::security_gateway`] 文件头）。
//! 透传走 HTTPS 到 github.com，而这条链路实测是**高方差 + 偶发黑洞窗口**：
//! 好时 0.5s，坏时直冲超时。黑洞期一来，`mainline_deployer` 拉不到基线、
//! `landing` 推不上去，整条自进化链路停摆——而 Pod 侧无从自救，它既没有凭证，
//! 也没有第二条通道。
//!
//! **为什么不是"干脆改走 SSH"**：HTTPS 实测比 SSH 快约三倍（各 12 样本，
//! HTTPS 中位 ~1.4s，SSH 中位 ~4.7s）。常态下换 SSH 是纯亏，只在 HTTPS 进
//! 黑洞时才划算。所以两条都要：HTTPS 优先，连续失败即熔断降级，窗口到期
//! 自动回切（熔断形状与网关既有的 `LlmHealthTable` 同源）。
//!
//! **为什么 SSH 兜底必须终结在网关**：SSH 不是 HTTP，代理转发不了。网关自己
//! 当 SSH 客户端，维护一份裸镜像，再用 git smart HTTP 把它讲给 Pod——
//! **Pod 侧代码零改动**：`landing.rs` / `mainline_deployer.rs` 仍然只是在跟
//! `{GIT_PROXY_BASE}/github/{repo}.git` 说 smart HTTP，选路完全发生在网关内部。
//!
//! **推送的等价性**：Pod 认为 push 成功，必须严格等价于 GitHub 真的收到了。
//! 所以 receive-pack 的响应先缓冲、不立即回，随后同步 `git push` 到 SSH 上游；
//! 推失败就把已经缓冲好的成功响应丢掉、改回 502。宁可让 Pod 看到失败并重试，
//! 也不能让它以为推上去了而实际没有——那会静默丢一次演化。
//!
//! **不做什么**（都是有意的取舍，不是遗漏）：
//! - **不发陈旧的基线**：镜像超过保鲜期且刷不动时返回 502，不降级为"发旧的
//!   出去"。基线陈旧会让 `mainline_deployer` 把旧代码当 main 并据此做演化
//!   决策，属于静默错答案，比一次响亮的失败贵得多。
//! - **不传播 ref 删除**：等价性只覆盖 ref 的创建/更新（landing 唯一会做的
//!   操作）。镜像上的删除不推到 GitHub——推错一个不可逆的删除，比下次 refresh
//!   时 ref 自己长回来糟糕得多。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::security_gateway::suspect_backoff_secs;

/// 镜像根目录：`<root>/github/<owner>/<repo>.git`。
pub(crate) const DEFAULT_MIRROR_ROOT: &str = "/var/lib/cogneva-git-mirror";
/// 私钥挂载点（来自 Secret `cogneva-secrets` 的 `git-ssh-private-key`）。
pub(crate) const DEFAULT_SSH_KEY: &str = "/etc/cogneva/git-ssh/id_ed25519";
/// SSH 目标前缀，拼在 `<owner>/<repo>.git` 前。scp 形式，与
/// `pull-upstream-main.sh` 里 `git@github.com:hcipengm/cogneva.git` 同款。
pub(crate) const DEFAULT_SSH_BASE: &str = "git@github.com:";
/// 镜像保鲜期：最后一次成功 fetch 在这个秒数内，才允许把它当基线发出去。
/// 取得短是因为它同时是"兜底期基线能有多旧"的上界。
pub(crate) const MIRROR_FRESHNESS_SECS: u64 = 30;
/// 熔断探测节拍：连续失败按指数退避，封顶见 [`suspect_backoff_secs`]。
const HTTPS_PROBE_INTERVAL_SECS: u64 = 60;
/// HTTPS 快路径的等待上限。GET 是 `info/refs`（应答小、上游立刻回）；POST 是
/// pack 计算 + 传输，给足时间。**这两个值判的是"是不是进了黑洞"，不是给正常
/// 请求设预算**——透传路径原本没有任何总超时，加它就是为了让降级判据存在。
const HTTPS_GET_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const HTTPS_POST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
/// 镜像单次响应体上限，与透传路径的 256MB 缓冲约定一致。
const MAX_MIRROR_BODY: usize = 256 * 1024 * 1024;
/// git 可执行文件。镜像路径只经由它跑 clone/fetch/push/show-ref。
const DEFAULT_GIT_BIN: &str = "git";

/// git 可执行文件从哪来：镜像里 git 不一定在 PATH 上（部署事实），测试也靠它把
/// 路径指到假 git 上。只此一处——镜像刷新、选路探测、身份自证必须指向同一个
/// 二进制，三份各自读环境变量迟早会读到三个不同的东西，而超时判据的实测依赖
/// 它被真正注入。
pub(crate) fn git_bin_from_env() -> PathBuf {
    env_nonempty("COGNEVA_GATEWAY_GIT_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_GIT_BIN))
}

/// 兜底传输的**静默判死**秒数：SSH 侧超过这么久没有任何输出（进度、远端消息）
/// 就判定这条传输已经死了。
///
/// 为什么判"静默"而不是判"总时长"：这条链路的故障形态是**传了几 MB 之后彻底
/// 没声**，而 `git clone`/`fetch` 带 `--progress` 的进度输出是秒级的——静默 N 秒
/// 就等于"没有进展"。用总时长判会把"链路慢"和"链路死"压成同一个结论，而这两种
/// 情况该做的事相反：慢要等，死要立刻换通道。总时长上限另外设一层（见下）。
const DEFAULT_MIRROR_STALL_SECS: u64 = 180;
/// 静默判死的下限。允许部署面调小，但不允许小到把正常传输误杀。
const MIRROR_STALL_FLOOR_SECS: u64 = 30;
/// 单次 clone/fetch/push 的总时长上限（秒）。沉默判死之外再压一层，防的是另一种
/// 形态：远端**一直在吐进度却永远不结束**——那样静默判据永远不触发，只看总时长。
const DEFAULT_MIRROR_OP_TIMEOUT_SECS: u64 = 1800;
const MIRROR_OP_TIMEOUT_FLOOR_SECS: u64 = 60;
/// 看门狗巡检间隔的上限；实际间隔按静默窗的四分之一取（见
/// [`GitTransport::watchdog_interval`]），这样测试里把窗口压到秒级仍然判得准。
const MIRROR_WATCHDOG_MAX_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// 失败信息里保留的 stderr 尾巴长度：传输失败的**原因在最后几行**，而前几千行
/// 是进度刷屏。留尾部才留得住原因，同时也别让一条日志带上几 MB。
const STDERR_TAIL_CHARS: usize = 600;
/// 临时物/锁的清理门槛下限：比这新的东西可能是**活着的**传输手里的
/// （进度秒级更新、锁活不过一次请求），只有明显陈旧才是死进程的遗物。
const RESIDUE_MIN_AGE_SECS: u64 = 60;

/// 兜底路径的部署配置。全部来自环境变量，未配置私钥即视为**未启用**。
#[derive(Clone, Debug)]
pub(crate) struct GitMirrorConfig {
    pub root: PathBuf,
    /// 私钥路径；`None` = 兜底未启用。
    pub ssh_key: Option<PathBuf>,
    /// SSH 目标前缀，拼在 `<owner>/<repo>.git` 前面。默认指向 GitHub；
    /// 独立部署（GitHub Enterprise、自建镜像站）改这一项即可。
    pub ssh_base: String,
    /// git 可执行文件。放进配置面有两个理由：镜像里 git 装在哪是**部署事实**
    /// （不一定在 PATH 上），以及传输超时判据要能在不联网的前提下被实测
    /// （指到一个假 git 上）。
    pub git_bin: PathBuf,
    /// 传输静默多久判死。
    pub stall: std::time::Duration,
    /// 单次传输的总时长上限。
    pub op_timeout: std::time::Duration,
}

impl GitMirrorConfig {
    /// 只给部署三要素，执行预算取生产默认值。选路测量与测试用这个构造——它们
    /// 关心的是"镜像在哪、拿哪把钥匙"，与超时预算无关。
    pub(crate) fn from_parts(root: PathBuf, ssh_key: Option<PathBuf>, ssh_base: String) -> Self {
        Self {
            root,
            ssh_key,
            ssh_base,
            git_bin: PathBuf::from(DEFAULT_GIT_BIN),
            stall: std::time::Duration::from_secs(DEFAULT_MIRROR_STALL_SECS),
            op_timeout: std::time::Duration::from_secs(DEFAULT_MIRROR_OP_TIMEOUT_SECS),
        }
    }

    pub fn from_env() -> Self {
        let root = env_nonempty("COGNEVA_GATEWAY_GIT_MIRROR_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_MIRROR_ROOT));
        let enabled = std::env::var("COGNEVA_GATEWAY_GIT_MIRROR_ENABLED")
            .map(|v| !matches!(v.trim(), "0" | "false" | "no"))
            .unwrap_or(true);
        let key = env_nonempty("COGNEVA_GATEWAY_GIT_SSH_KEY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SSH_KEY));
        let ssh_base =
            env_nonempty("COGNEVA_GATEWAY_GIT_SSH_BASE").unwrap_or_else(|| DEFAULT_SSH_BASE.into());
        // 私钥不存在 = 兜底根本没配好。保持"未启用"而不是每次请求都去试一遍
        // 注定失败的 SSH：缺私钥是**部署事实**，不是瞬时故障，按故障节拍重试
        // 只会把每个请求都拖长。
        let ssh_key = (enabled && key.is_file()).then_some(key.clone());
        if ssh_key.is_none() {
            tracing::warn!(
                key = %key.display(),
                enabled,
                "git SSH 兜底未启用（HTTPS 单通道），缺私钥或已被显式关闭"
            );
        }
        let mut cfg = Self::from_parts(root, ssh_key, ssh_base);
        cfg.git_bin = git_bin_from_env();
        if let Some(secs) = env_secs(
            "COGNEVA_GATEWAY_GIT_MIRROR_STALL_SECS",
            MIRROR_STALL_FLOOR_SECS,
        ) {
            cfg.stall = std::time::Duration::from_secs(secs);
        }
        if let Some(secs) = env_secs(
            "COGNEVA_GATEWAY_GIT_MIRROR_TIMEOUT_SECS",
            MIRROR_OP_TIMEOUT_FLOOR_SECS,
        ) {
            cfg.op_timeout = std::time::Duration::from_secs(secs);
        }
        cfg
    }
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// 读一个"秒"配置，带地板：给得比地板小、或者根本不是数字，都按地板生效并打
/// 一条警告。地板是这里的承重墙——一个被手滑配成 `1` 的静默窗会把每次正常传输
/// 都当成死的杀掉，而症状看起来像"SSH 通道坏了"。
fn env_secs(key: &str, floor: u64) -> Option<u64> {
    let raw = env_nonempty(key)?;
    match raw.trim().parse::<u64>() {
        Ok(v) if v >= floor => Some(v),
        _ => {
            tracing::warn!(key, raw = %raw, floor, "git 传输超时配置低于地板或无法解析，按地板生效");
            Some(floor)
        }
    }
}

/// 镜像（以及一切走部署密钥的 git 操作）对应的 SSH 远端地址。
///
/// 一律**给 URL 而不是远端名**：`clone --mirror` 会在配置里留下
/// `remote.origin.mirror=true`，它与显式 refspec 互斥（git 直接报
/// `--mirror can't be combined with refspecs`）。给 URL 既绕开这条约束，
/// 也让"推到哪"成为显式声明，不再依赖镜像自己的 remote 配置。
pub(crate) fn ssh_url_for(ssh_base: &str, repo: &str) -> String {
    format!("{ssh_base}{repo}.git")
}

/// 某个仓库在镜像根下的落地目录。`owner/name` 里那层目录由 git 自己建，
/// 这里只决定它的位置：镜像的布局是网关自己的缓存策略，上游怎么组织仓库与它无关。
fn mirror_dir_for(cfg: &GitMirrorConfig, repo: &str) -> PathBuf {
    cfg.root.join("github").join(format!("{repo}.git"))
}

/// SSH 传输命令。`accept-new` 与引导脚本同款：首次连接写入 known_hosts，之后
/// 固定。**不设 `no`**——那会让中间人换掉主机指纹也察觉不到。`BatchMode=yes`
/// 保证任何需要交互的提示（口令、yes/no）直接失败而不是挂住。
///
/// 握实测与镜像刷新必须用**逐字节相同的命令**，否则测的是"这条通道大概通不通"，
/// 而不是"我们这条通道通不通"（少一个选项就可能在真实路径上被口令提示挂住）。
pub(crate) fn ssh_command_for(key: &Path) -> String {
    format!(
        "ssh -i {} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o BatchMode=yes",
        key.display()
    )
}

/// 单通道健康态。语义与 `UpstreamHealth` 同形，但不带配额字段——git 通道
/// 没有"上游告诉我们什么时候恢复"这回事，只有我们自己的重试节拍。
#[derive(Default)]
struct ChannelHealth {
    consecutive_failures: u32,
    suspect_until: Option<std::time::Instant>,
}

/// HTTPS git 通道的熔断器。纯进程内状态，重启即清零——代价只是重启后多试
/// 一次 HTTPS，换来无持久化依赖（与 LLM 池健康表同取舍）。
#[derive(Default)]
pub(crate) struct GitTransportHealth {
    https: Mutex<ChannelHealth>,
}

impl GitTransportHealth {
    /// HTTPS 当前是否可用：有未到期的嫌疑窗就是不可用。
    pub fn https_available(&self) -> bool {
        let now = std::time::Instant::now();
        !self
            .https
            .lock()
            .unwrap()
            .suspect_until
            .is_some_and(|t| now < t)
    }

    /// 记一次 HTTPS 失败：开/加窗。返回 `(连续失败数, 窗口秒)`；窗口内的并发
    /// 失败返回 `(n, 0)` 表示没有重开窗——一次事故不该把指数打飞。
    pub fn note_https_failure(&self) -> (u32, u64) {
        let now = std::time::Instant::now();
        let mut h = self.https.lock().unwrap();
        if h.suspect_until.is_some_and(|t| now < t) {
            return (h.consecutive_failures, 0);
        }
        h.consecutive_failures = h.consecutive_failures.saturating_add(1);
        let secs = suspect_backoff_secs(h.consecutive_failures, HTTPS_PROBE_INTERVAL_SECS);
        h.suspect_until = Some(now + std::time::Duration::from_secs(secs));
        (h.consecutive_failures, secs)
    }

    /// 记一次 HTTPS 实证成功：清嫌疑态。返回此前是否处于嫌疑（调用方打恢复日志）。
    /// **只有真实请求成功才清除**——窗口到期只说明"值得再试一次"，不是恢复的证据。
    pub fn note_https_success(&self) -> bool {
        let mut h = self.https.lock().unwrap();
        let was = h.suspect_until.is_some();
        if was {
            h.consecutive_failures = 0;
            h.suspect_until = None;
        }
        was
    }
}

/// 一次 git smart HTTP 请求里网关需要的东西（Pod 侧原样发来，不做改写）。
///
/// `Copy`：选路可能把同一个请求先后交给两条通道，逐字节相同的输入靠拷贝
/// 保证（改了字段就只影响其中一次尝试，"降级后行为等价"也就不成立了）。
#[derive(Clone, Copy)]
pub(crate) struct MirrorRequest<'a> {
    /// `/git/github` 之后的剩余路径，例如 `/hcipengm/cogneva.git/info/refs`。
    pub path: &'a str,
    pub query: Option<&'a str>,
    /// `Git-Protocol` 头（`version=2` 时要在服务端启用 v2）。
    pub git_protocol: Option<&'a str>,
    pub is_get: bool,
    pub body: &'a [u8],
}

/// SSH 兜底镜像：网关自己当 SSH 客户端维护裸镜像，再用 smart HTTP 讲给 Pod。
pub(crate) struct GitTransport {
    config: GitMirrorConfig,
    health: GitTransportHealth,
    /// clone/fetch/receive-pack 都写同一份裸仓，并发跑会互相踩 refs 与
    /// packed-refs。串行化它们；读路径（upload-pack）不加锁，git 自身对
    /// 并发读是安全的。
    ///
    /// 与 `fetched_at` 一样是 `Arc`：刷新的执行体要能**脱离调用者**继续跑
    /// （见 [`MirrorRefresh`]），它必须自己持有这两样，而不是借用 `self`。
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// 逐镜像的"最近一次成功 fetch 时刻"，决定是否还能当基线发出去。
    fetched_at: Arc<Mutex<std::collections::HashMap<PathBuf, std::time::Instant>>>,
    /// 跑 git 子进程的那一半（执行预算 + 进程组收尾）。刷新把它一起搬进
    /// 脱离任务，所以它自带配置、不借用 `self`。
    exec: GitExec,
    /// 选路状态（实测排序 + 网络画像）。与镜像同生命周期：选路的所有输入
    /// 都在这份配置里（密钥、SSH 前缀），分开放只会让两者可能指向不同的远端。
    routing: Arc<crate::git_routing::TransportRouting>,
}

impl GitTransport {
    pub fn new(config: GitMirrorConfig) -> Self {
        Self {
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            fetched_at: Arc::new(Mutex::new(std::collections::HashMap::new())),
            exec: GitExec::new(config.clone()),
            config,
            health: GitTransportHealth::default(),
            routing: Arc::new(crate::git_routing::TransportRouting::from_env()),
        }
    }

    pub fn from_env() -> Self {
        Self::new(GitMirrorConfig::from_env())
    }

    /// 兜底路径是否可用。**这是选路的唯一开关**：私钥没挂上或不启用，
    /// 网关就退回纯 HTTPS 透传，行为与加这个模块之前完全一致。
    pub fn fallback_available(&self) -> bool {
        self.config.ssh_key.is_some()
    }

    pub fn health(&self) -> &GitTransportHealth {
        &self.health
    }

    /// 选路状态：谁该先走（实测优先、策略表兜底）。
    pub fn routing(&self) -> &Arc<crate::git_routing::TransportRouting> {
        &self.routing
    }

    /// 镜像配置的副本（后台实测要拿密钥与 SSH 前缀，且不能借走 `&self`）。
    pub fn config(&self) -> GitMirrorConfig {
        self.config.clone()
    }

    /// HTTPS 快路径的等待上限：GET 短、POST 长（见常量注释）。
    pub fn https_timeout(is_get: bool) -> std::time::Duration {
        if is_get {
            HTTPS_GET_TIMEOUT
        } else {
            HTTPS_POST_TIMEOUT
        }
    }

    /// 应答一个 git smart HTTP 请求（从 SSH 镜像出）。失败一律是
    /// `(StatusCode, String)`，与透传路径同形，调用方无需区分来源。
    pub async fn serve(
        &self,
        req: MirrorRequest<'_>,
    ) -> Result<axum::response::Response, (StatusCode, String)> {
        let (repo, suffix) = split_repo_path(req.path).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("镜像路径无法解析: {}", req.path),
            )
        })?;
        let dir = self.mirror_dir(&repo);

        // 三种请求形态；其余路径对 smart HTTP 没有意义。
        let (service, advertise) = if suffix == "/info/refs" {
            let svc = req
                .query
                .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("service=")))
                .ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "info/refs 缺少 service 参数".to_string(),
                    )
                })?;
            if !matches!(svc, "git-upload-pack" | "git-receive-pack") {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("不支持的 git service: {svc}"),
                ));
            }
            (svc.to_string(), true)
        } else if suffix == "/git-upload-pack" {
            ("git-upload-pack".to_string(), false)
        } else if suffix == "/git-receive-pack" {
            ("git-receive-pack".to_string(), false)
        } else {
            return Err((StatusCode::NOT_FOUND, format!("镜像不提供该路径: {suffix}")));
        };

        // GET 只能取 refs 广告，POST 只能打 RPC 端点——对不上就是客户端行为异常。
        // 早拒比放它进 git 子进程再失败清楚：子进程的错误信息会把"你请求错了"
        // 说成"服务端出问题了"。
        if req.is_get != advertise {
            return Err((
                StatusCode::METHOD_NOT_ALLOWED,
                format!("{suffix} 与请求方法不匹配"),
            ));
        }

        // 三处都要先把镜像刷到最新：
        // - upload-pack 读的是 ref：不刷新就会把旧基线发给 deployer；
        // - receive-pack **尤其**要刷：镜像陈旧会以 non-fast-forward 拒掉一次
        //   本来合法的推送，而 Pod 侧看到的会是"上游拒绝"，归因完全错。
        self.refresh(&repo, false)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e))?;

        // receive-pack 之前先拍一次 ref 快照，推完再拍一次，差集就是要同步到
        // GitHub 的东西。快照必须在 receive-pack 之前——之后的镜像已经被 Pod
        // 改过了，没有基线可比。
        let before = if service == "git-receive-pack" && !advertise {
            Some(
                self.snapshot_refs(&dir)
                    .await
                    .map_err(|e| (StatusCode::BAD_GATEWAY, format!("读取镜像 refs 失败: {e}")))?,
            )
        } else {
            None
        };

        let out = self
            .run_pack(&dir, &service, advertise, req.body, req.git_protocol)
            .await?;

        if let Some(before) = before {
            // 推失败必须是 502：Pod 已经被告知"收下了"的推送若没真的到 GitHub，
            // 就是静默丢一次演化。让它看到失败并重试，是这里唯一正确的选择。
            self.propagate_push(&repo, &dir, &before)
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, e))?;
        }

        let content_type = match (service.as_str(), advertise) {
            ("git-upload-pack", true) => "application/x-git-upload-pack-advertisement",
            ("git-upload-pack", false) => "application/x-git-upload-pack-result",
            ("git-receive-pack", true) => "application/x-git-receive-pack-advertisement",
            _ => "application/x-git-receive-pack-result",
        };
        let mut buf = Vec::with_capacity(out.len() + 64);
        if advertise {
            // smart HTTP 的应答前缀：一条 `# service=...` 的 pkt-line，接一个
            // flush-pkt，然后才是原生 git 协议流。缺了这段，git 客户端会认不出
            // 这是 smart HTTP 而退回哑协议（然后什么都拉不到）。
            buf.extend_from_slice(&pkt_line(&format!("# service={service}\n")));
            buf.extend_from_slice(b"0000");
        }
        buf.extend_from_slice(&out);

        Ok(axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", content_type)
            // git 客户端把 refs 广告当易失数据，上游一律这么回；少了它有些
            // 客户端版本会走缓存路径。
            .header("cache-control", "no-cache")
            .body(axum::body::Body::from(buf))
            .unwrap_or_else(|_| axum::response::Response::new(axum::body::Body::empty())))
    }

    fn mirror_dir(&self, repo: &str) -> PathBuf {
        mirror_dir_for(&self.config, repo)
    }

    /// 镜像对应的 SSH 远端地址。
    ///
    /// 一律**给 URL 而不是远端名**：`clone --mirror` 会在配置里留下
    /// `remote.origin.mirror=true`，它与显式 refspec 互斥（git 直接报
    /// `--mirror can't be combined with refspecs`）。给 URL 既绕开这条约束，
    /// 也让"推到哪"成为显式声明，不再依赖镜像自己的 remote 配置。
    fn ssh_url(&self, repo: &str) -> String {
        ssh_url_for(&self.config.ssh_base, repo)
    }

    fn ssh_command(&self) -> String {
        let key = self
            .config
            .ssh_key
            .as_ref()
            .expect("fallback_available() 已保证私钥存在");
        ssh_command_for(key)
    }

    /// 把镜像刷到最新。保鲜期内（且非 `force`）直接返回，省掉一次 SSH 往返——
    /// 一次 fetch 是秒级，而兜底期每个请求都刷会把延迟乘上去。
    ///
    /// 真正的刷新**脱离调用者**跑（[`MirrorRefresh`]）：这条链路上一次镜像克隆
    /// 是几分钟量级，而触发它的请求随时可能被撤（客户端超时断开会让 axum 丢掉
    /// handler 的 future）。执行体跟着请求死掉实测出两样后果：git 子进程链变成
    /// 没人管的孤儿还在往镜像里写，而写锁被提前释放——下一个请求于是对同一个库
    /// 又起一条 fetch。所以这里只负责"发起 + 等结果"，寿命归任务自己。
    ///
    /// 失败一律是 Err，**不降级为"发陈旧的基线出去"**（见文件头）。
    async fn refresh(&self, repo: &str, force: bool) -> Result<(), String> {
        let dir = self.mirror_dir(repo);
        if !force && self.is_fresh(&dir) {
            return Ok(());
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let work = MirrorRefresh {
            exec: self.exec.clone(),
            lock: self.write_lock.clone(),
            fetched_at: self.fetched_at.clone(),
            repo: repo.to_string(),
            force,
        };
        tokio::spawn(async move {
            let _ = tx.send(work.run().await);
        });
        // 调用者走掉就是走掉：接收端被丢弃，发送失败被忽略，任务照跑。
        rx.await
            .unwrap_or_else(|_| Err("镜像刷新任务在给出结果前消失".to_string()))
    }

    /// 这个镜像是否还在保鲜期内（且已经有库）。
    fn is_fresh(&self, dir: &Path) -> bool {
        mirror_fresh_for(&self.fetched_at, dir)
    }

    /// 在镜像里跑 refs 快照。空仓没有 ref，`git show-ref` 退出码为 1 且无输出
    /// ——那不是错误，是"还没有任何 ref"。
    async fn snapshot_refs(&self, dir: &Path) -> Result<BTreeMap<String, String>, String> {
        let dir_str = dir.to_string_lossy().to_string();
        let out = self
            .exec
            .allow_failure(&["-C", &dir_str, "show-ref"], None)
            .await?;
        let mut map = BTreeMap::new();
        for line in String::from_utf8_lossy(&out).lines() {
            if let Some((sha, name)) = line.split_once(' ') {
                map.insert(name.trim().to_string(), sha.trim().to_string());
            }
        }
        Ok(map)
    }

    /// 把 Pod 推进镜像的 ref 同步到 GitHub（SSH）。**这一步失败必须让整个请求
    /// 失败**：Pod 已经被告知"收下了"的推送，若没真的到 GitHub，就是静默丢失。
    ///
    /// `--atomic`：多个 ref 时全成或全不成，不留"部分推上去了"的中间态。
    /// 不带 `+`（不强制）：与项目"只快进，永不强推/reset"的约束一致；上游若
    /// 因为期间有别人的推送而拒绝，我们如实报 502，让 Pod 重试。
    /// 推给 URL 而非远端名的原因见 [`GitTransport::ssh_url`]。
    async fn propagate_push(
        &self,
        repo: &str,
        dir: &Path,
        before: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let after = self.snapshot_refs(dir).await?;
        let mut refspecs: Vec<String> = Vec::new();
        let mut deleted: Vec<&String> = Vec::new();
        for (name, sha) in &after {
            if before.get(name) != Some(sha) {
                refspecs.push(format!("{name}:{name}"));
            }
        }
        for name in before.keys() {
            if !after.contains_key(name) {
                deleted.push(name);
            }
        }
        if !deleted.is_empty() {
            // 有意不传播删除（见文件头）。这里只留痕：landing 唯一做的是
            // ref 创建/更新，出现删除说明有别的写入者，值得知道。
            tracing::warn!(
                repo,
                refs = ?deleted,
                "镜像上有 ref 被删除，按设计不向 GitHub 传播"
            );
        }
        if refspecs.is_empty() {
            return Ok(());
        }

        let _guard = self.write_lock.lock().await;
        let dir_str = dir.to_string_lossy().to_string();
        let ssh = self.ssh_command();
        let url = self.ssh_url(repo);
        let mut args: Vec<&str> = vec!["-C", &dir_str, "push", "--progress", "--atomic", &url];
        args.extend(refspecs.iter().map(|s| s.as_str()));
        match self.exec.progress(&args, Some(&ssh)).await {
            Ok(_) => {
                tracing::info!(repo, refs = ?refspecs, "推送已同步到 GitHub（SSH 兜底）");
                Ok(())
            }
            Err(e) => Err(format!(
                "镜像已接受推送但同步到 GitHub 失败，本次推送按失败处理: {e}"
            )),
        }
    }

    /// 跑一次 git 协议服务进程，缓冲其 stdout。`advertise` 时走
    /// `--advertise-refs`（refs 广告，无 stdin）。
    async fn run_pack(
        &self,
        dir: &Path,
        service: &str,
        advertise: bool,
        body: &[u8],
        git_protocol: Option<&str>,
    ) -> Result<Vec<u8>, (StatusCode, String)> {
        let mut cmd = tokio::process::Command::new("git");
        // `service` 是 HTTP 协议面上的名字（`git-upload-pack`），而命令行子命令
        // 不带 `git-` 前缀（`git upload-pack`）。http-backend 也是这么转的。
        let subcommand = service.strip_prefix("git-").unwrap_or(service);
        cmd.arg(subcommand).arg("--stateless-rpc");
        if advertise {
            cmd.arg("--advertise-refs");
        }
        cmd.arg(dir);
        cmd.stdin(if advertise {
            Stdio::null()
        } else {
            Stdio::piped()
        });
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        // v2 协议靠环境变量在服务端启用：客户端把版本放在 `Git-Protocol` 头里，
        // smart HTTP 后端的中转方式就是把它变成 GIT_PROTOCOL 传给 upload-pack。
        // 不透传的话，Pod 请求 v2、镜像按 v0 应答，协商看着"成功"但特性集对不上。
        if let Some(p) = git_protocol {
            cmd.env("GIT_PROTOCOL", p);
        }

        let mut child = cmd.spawn().map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("启动 git {subcommand} 失败: {e}"),
            )
        })?;
        // stdin 必须**并发**写：pack 数据可能远大于管道缓冲，边写边等 stdout
        // 会在双方都写满时互锁。
        if let Some(mut stdin) = child.stdin.take() {
            let body = body.to_vec();
            tokio::spawn(async move {
                let _ = stdin.write_all(&body).await;
                let _ = stdin.shutdown().await;
            });
        }
        let out = child.wait_with_output().await.map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("等待 git {subcommand} 失败: {e}"),
            )
        })?;
        if !out.status.success() {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!(
                    "git {subcommand} 退出异常: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ),
            ));
        }
        if out.stdout.len() > MAX_MIRROR_BODY {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("镜像应答超过 {} 字节上限", MAX_MIRROR_BODY),
            ));
        }
        Ok(out.stdout)
    }
}

/// 受限的 git 执行器：把"起子进程 + 两档执行预算 + 进程组收尾"收成一份。
///
/// 镜像刷新与选路探测共用同一条实现：两份实现必然漂移成两套超时语义，而
/// "静默多久算死"这种事两边不一致时，同一台机器上会同时出现"判得太松、卡住"
/// 与"判得太紧、把还活着的传输杀掉"两种症状。
///
/// 自带配置而不是借用外部对象：刷新的执行体要脱离调用者活着（见
/// [`MirrorRefresh`]），它必须能带着执行器一起搬走。
#[derive(Clone)]
struct GitExec {
    cfg: GitMirrorConfig,
}

impl GitExec {
    fn new(cfg: GitMirrorConfig) -> Self {
        Self { cfg }
    }

    /// 跑一条命令并缓冲 stdout；失败信息给 stderr 的**尾部**——进度刷屏在头部，
    /// 真原因在后部。
    async fn progress(&self, args: &[&str], ssh: Option<&str>) -> Result<Vec<u8>, String> {
        let out = self.bounded(args, ssh, true).await?;
        if !out.status.success() {
            return Err(format!(
                "git {} 失败: {}",
                args.first().copied().unwrap_or(""),
                tail_text(&out.stderr, STDERR_TAIL_CHARS)
            ));
        }
        Ok(out.stdout)
    }

    /// 跑一条瞬时命令取 stdout，非零退出即错误。
    async fn capture(&self, args: &[&str], ssh: Option<&str>) -> Result<Vec<u8>, String> {
        let out = self.bounded(args, ssh, false).await?;
        if !out.status.success() {
            return Err(format!(
                "git {} 失败: {}",
                args.first().copied().unwrap_or(""),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(out.stdout)
    }

    /// 退出码本身有意义（`show-ref` 在空仓返回 1），只取 stdout。
    async fn allow_failure(&self, args: &[&str], ssh: Option<&str>) -> Result<Vec<u8>, String> {
        Ok(self.bounded(args, ssh, false).await?.stdout)
    }

    /// 看门狗巡检间隔：静默窗的四分之一（封顶五秒）。由静默窗**派生**而不是
    /// 另立一个旋钮——两个独立旋钮必然会出现"窗口 30s、巡检 60s"这种判据比
    /// 被判断的对象还粗的组合；测试里把窗口压到秒级时它也跟着变细。
    fn watchdog_interval(&self) -> std::time::Duration {
        (self.cfg.stall / 4).clamp(
            std::time::Duration::from_millis(50),
            MIRROR_WATCHDOG_MAX_INTERVAL,
        )
    }

    /// 跑一条 git 命令，并把它的执行**封顶**。
    ///
    /// `watch_stall` 只对会打进度的子命令（clone/fetch/push）为真——那些命令的
    /// stderr 是"还在动"的证据；`show-ref` 这类瞬时命令沉默不代表卡住，只看总上限。
    ///
    /// 超时必须**杀进程组**：SSH 兜底是一条 `git → sh → ssh → 远端` 的链，只杀
    /// 直接子进程会留下还在往镜像里写 pack 的孙进程（上一次事故里 `git index-pack`
    /// 在父进程死后又活了十几分钟，下一次 refresh 与它抢同一个对象库）。所以子
    /// 进程自己成组，超时对整组发 SIGKILL。
    async fn bounded(
        &self,
        args: &[&str],
        ssh: Option<&str>,
        watch_stall: bool,
    ) -> Result<std::process::Output, String> {
        let label = args.first().copied().unwrap_or("").to_string();
        let started = std::time::Instant::now();
        let (mut child, mut group) =
            spawn_git_group(&self.cfg.git_bin, args, ssh, Stdio::piped(), Stdio::piped())?;
        let pid = group.pid;
        let stdout = child.stdout.take().ok_or("git stdout 未接管")?;
        let stderr = child.stderr.take().ok_or("git stderr 未接管")?;

        let last_output = Arc::new(Mutex::new(std::time::Instant::now()));
        let out_task = tokio::spawn(read_all(stdout));
        let err_task = tokio::spawn(read_tail(stderr, last_output.clone()));
        let mut wait_task = tokio::spawn(async move { child.wait().await });

        let verdict: Result<std::process::ExitStatus, String> = loop {
            tokio::select! {
                res = &mut wait_task => {
                    // 两级 Result：外层是"等进程"这个任务自己失败，内层是 wait 失败。
                    let waited = match res {
                        Ok(inner) => inner,
                        Err(e) => break Err(format!("等待 git 进程失败: {e}")),
                    };
                    match waited {
                        Ok(status) => break Ok(status),
                        Err(e) => break Err(format!("等待 git 进程失败: {e}")),
                    }
                }
                _ = tokio::time::sleep(self.watchdog_interval()) => {
                    let total = started.elapsed();
                    if total >= self.cfg.op_timeout {
                        break Err(format!(
                            "git {label} 超过总上限 {}s 仍未结束（判为传输不收敛，已杀进程组）",
                            self.cfg.op_timeout.as_secs()
                        ));
                    }
                    let idle = last_output.lock().unwrap().elapsed();
                    if watch_stall && idle >= self.cfg.stall {
                        break Err(format!(
                            "git {label} 静默 {}s（无任何进度输出，判为传输已死，已杀进程组）",
                            idle.as_secs()
                        ));
                    }
                }
            }
        };

        let status = match verdict {
            Ok(status) => {
                // 进程已经归位，号可以复用了：守卫到此为止。
                group.disarm();
                status
            }
            Err(reason) => {
                kill_group(pid);
                group.disarm();
                let _ = tokio::time::timeout(WATCHDOG_REAP_TIMEOUT, &mut wait_task).await;
                let tail = tokio::time::timeout(WATCHDOG_REAP_TIMEOUT, err_task)
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .unwrap_or_default();
                return Err(format!(
                    "{reason}，耗时 {}s{}",
                    elapsed_secs(started),
                    tail_suffix(&tail, STDERR_TAIL_CHARS)
                ));
            }
        };

        Ok(std::process::Output {
            status,
            stdout: out_task.await.unwrap_or_default(),
            stderr: err_task.await.unwrap_or_default(),
        })
    }
}

/// 超时后等被杀的进程/读取任务收尾的上限。杀的是整个进程组，正常情况下管道
/// 立刻断开；给个上限只为不让"已判定超时"的路径自己再挂住。
const WATCHDOG_REAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// 对进程组发 SIGKILL。`pid` 是组长（调用点用 `process_group(0)` 让子进程自己
/// 成组，组长 pid 即组 id）。
///
/// 不用 `Child::kill()`：它只杀组长，`sh`/`ssh`/`index-pack` 这些孙进程会活下来
/// 继续写镜像——那正是上一次事故的形态。
fn kill_group(pid: u32) {
    // SAFETY: 只传一个 pid 与一个信号常量；无指针、无所有权语义。返回 -1
    // （组已不存在）是"超时判死"与"进程自己刚退出"赛跑时的正常结果。
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

/// 进程组守卫：**被丢弃**时对整组发 SIGKILL。
///
/// 它管的是"取消"，不是超时。超时由执行器自己的看门狗管，而看门狗活在执行体
/// 那个 future 里——future 被丢掉之后就没有人再判静默了，`git → sh → ssh` 这条
/// 链却还在跑。实测到的形态就是这个：触发它的请求早已被撤（客户端断开、外层
/// `timeout` 到期），`git index-pack` 还挂着、镜像目录里留着没写完的 pack，
/// 而下一个请求对同一个库又起一条 fetch。`kill_on_drop` 挡不住它：那只会杀组长。
pub(crate) struct GitGroupGuard {
    pid: u32,
    armed: bool,
}

impl GitGroupGuard {
    /// 正常收尾后解除。拿到退出状态就必须调：pid 会被复用，对已经不存在的组
    /// 发信号最多是 ESRCH，对**被复用的号**发就是误伤别的进程。
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for GitGroupGuard {
    fn drop(&mut self) {
        if self.armed {
            kill_group(self.pid);
        }
    }
}

/// 起一个 git 子进程，让它自成一个进程组，并交回看管它的守卫。
///
/// 两个调用方各有各的时间政策（镜像刷新是"静默窗 + 总上限"两档，选路探测是
/// 一次硬超时），但"起进程、成组、取消即收尾"这件事只有这一份实现——stdout/stderr
/// 由调用方指定，是因为"要不要读输出"是它自己的决定，不该由这里替它假设。
pub(crate) fn spawn_git_group(
    git_bin: &Path,
    args: &[&str],
    ssh: Option<&str>,
    stdout: Stdio,
    stderr: Stdio,
) -> Result<(tokio::process::Child, GitGroupGuard), String> {
    let mut cmd = tokio::process::Command::new(git_bin);
    cmd.args(args);
    // Pod 里没有 tty：任何等待输入的提示（凭证、口令）都会把请求挂死到超时，
    // 所以直接禁掉交互，让它响亮地失败。
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(ssh) = ssh {
        cmd.env("GIT_SSH_COMMAND", ssh);
    }
    cmd.stdin(Stdio::null()).stdout(stdout).stderr(stderr);
    cmd.process_group(0);
    let child = cmd.spawn().map_err(|e| format!("执行 git 失败: {e}"))?;
    let pid = child.id().ok_or_else(|| "git 子进程没有 pid".to_string())?;
    Ok((child, GitGroupGuard { pid, armed: true }))
}

/// 一次镜像刷新的执行体。
///
/// **自带全部输入**（执行器、共享写锁、保鲜表），因为它要脱离触发它的请求运行：
/// 这条链路上一次克隆是几分钟量级，而请求随时可能被撤——跟着请求一起丢掉的话，
/// 白传一遍还留下一串没人管的子进程。发起方只等一个结果，寿命归这个任务。
struct MirrorRefresh {
    exec: GitExec,
    lock: Arc<tokio::sync::Mutex<()>>,
    fetched_at: Arc<Mutex<std::collections::HashMap<PathBuf, std::time::Instant>>>,
    repo: String,
    force: bool,
}

impl MirrorRefresh {
    async fn run(self) -> Result<(), String> {
        let dir = mirror_dir_for(&self.exec.cfg, &self.repo);
        let _guard = self.lock.lock().await;
        // 排队期间别的请求可能已经刷过了，再判一次：抢锁的代价不该白付。
        if !self.force && mirror_fresh_for(&self.fetched_at, &dir) {
            return Ok(());
        }

        let key = self
            .exec
            .cfg
            .ssh_key
            .as_ref()
            .expect("fallback_available() 已保证私钥存在");
        let ssh = ssh_command_for(key);
        let url = ssh_url_for(&self.exec.cfg.ssh_base, &self.repo);
        let dir_str = dir.to_string_lossy().to_string();
        let repo = self.repo.as_str();
        let started = std::time::Instant::now();

        // 目录存在不等于"库是完整的"：`clone` 会**先把仓库骨架写出来**
        // （HEAD/config/objects/refs），再传对象。所以一个被中断的 clone 留下
        // 的是"看着像已经克隆过了"的半个库——按旧判据（HEAD 在即视为已克隆）
        // 它会被就地 fetch，而它连 ref 都还没有，于是"有库但发不出基线"。
        // 判据改成结构性的：能续传就地续传，不能续传就清掉重来。
        let state = match mirror_state(&dir) {
            MirrorState::Unusable => {
                std::fs::remove_dir_all(&dir)
                    .map_err(|e| format!("清理结构不完整的镜像 {} 失败: {e}", dir.display()))?;
                tracing::warn!(repo, dir = %dir.display(), "镜像结构不完整（上次 clone 未写完骨架），已清掉重来");
                MirrorState::Absent
            }
            s => s,
        };

        if state == MirrorState::Absent {
            if let Some(parent) = dir.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("创建镜像目录 {} 失败: {e}", parent.display()))?;
            }
            tracing::info!(repo, url = %url, "git 兜底镜像首次克隆（SSH），期间不向外发基线");
            self.exec
                .progress(
                    &["clone", "--progress", "--mirror", &url, &dir_str],
                    Some(&ssh),
                )
                .await
                .map_err(|e| format!("镜像首次克隆失败（耗时 {}s）: {e}", elapsed_secs(started)))?;
        } else {
            let residue = clean_interrupted_transfer(&dir, residue_max_age_for(&self.exec.cfg));
            if residue.total() > 0 {
                tracing::warn!(
                    repo,
                    tmp_packs = residue.tmp_packs,
                    locks = residue.locks,
                    "清掉上次中断的传输留下的临时物（陈旧才算：新的可能属于活着的传输）"
                );
            }
            // 显式给 URL 与 refspec，不用镜像自己的 origin 配置：被中断的 clone
            // 留下的 config 是可用的，但我们不该把"能不能续传"押在它身上
            // （配置文件本身也可能是半截的）。`+refs/*:refs/*` 与 `--mirror`
            // 同语义（全量 ref，含 tag），`--prune` 保持镜像与上游一致。
            //
            // 续传保下来的是**骨架与已在对象库里的东西**，不是上一次传到一半的
            // pack：git 把未收完的包装在 `tmp_pack_*` 里，中断即作废（对象是内容
            // 寻址的，所以下一轮会重新协商、重新传缺的那部分）。这一点在链路上
            // 很重要——本机到 GitHub 的下行实测只有几十 KB/s 且会长时间静默，
            // 一次传不完就得下一轮接着来。
            self.exec
                .progress(
                    &[
                        "-C",
                        &dir_str,
                        "fetch",
                        "--progress",
                        "--prune",
                        &url,
                        "+refs/*:refs/*",
                    ],
                    Some(&ssh),
                )
                .await
                .map_err(|e| format!("镜像续传失败（耗时 {}s）: {e}", elapsed_secs(started)))?;
            if head_is_dangling(&dir) {
                // fetch 只按 refspec 搬 ref，**不碰本地 HEAD**。被中断的 clone
                // 把 HEAD 留在默认初始分支上（`ref: refs/heads/master`），而
                // 上游的默认分支是 main——留着的后果不是报错，是**静默空克隆**：
                // 从镜像 clone 的客户端以那个不存在的分支为默认分支，git 只打
                // 一句 warn 就交出一个空工作树。
                if let Err(e) = repair_head_from_remote(&self.exec, repo, &dir, &ssh).await {
                    tracing::warn!(repo, error = %e, "镜像 HEAD 悬空且未能按远端修正");
                }
            }
        }
        self.fetched_at
            .lock()
            .unwrap()
            .insert(dir, std::time::Instant::now());
        tracing::info!(
            repo,
            cloned = state == MirrorState::Absent,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "git 镜像已刷新（SSH 兜底）"
        );
        Ok(())
    }
}

/// 按远端默认分支修正悬空的 HEAD。只在 [`head_is_dangling`] 成立时调用：
/// 正常路径上 HEAD 是对的，不必每轮多跑一次 SSH 往返。
async fn repair_head_from_remote(
    exec: &GitExec,
    repo: &str,
    dir: &Path,
    ssh: &str,
) -> Result<(), String> {
    let url = ssh_url_for(&exec.cfg.ssh_base, repo);
    // `ls-remote --symref` 会把 `ref: refs/heads/<x>\tHEAD` 放在首行。
    let out = exec
        .capture(&["ls-remote", "--symref", &url, "HEAD"], Some(ssh))
        .await?;
    let branch = String::from_utf8_lossy(&out)
        .lines()
        .find_map(|l| l.strip_prefix("ref: "))
        .and_then(|l| l.split_whitespace().next())
        .map(str::to_string)
        .ok_or_else(|| "远端没有报告 HEAD 的符号引用".to_string())?;
    exec.capture(
        &[
            "-C",
            &dir.to_string_lossy(),
            "symbolic-ref",
            "HEAD",
            &branch,
        ],
        None,
    )
    .await?;
    tracing::info!(repo, branch, "镜像 HEAD 已按远端默认分支修正");
    Ok(())
}

/// 这个镜像是否还在保鲜期内。必须**有**一次成功刷新记录才算：没有记录时
/// 不能靠"目录看着像库"当新鲜（那正是半个库最容易骗过去的判据）。
fn mirror_fresh_for(
    fetched_at: &Mutex<std::collections::HashMap<PathBuf, std::time::Instant>>,
    dir: &Path,
) -> bool {
    fetched_at
        .lock()
        .unwrap()
        .get(dir)
        .is_some_and(|t| t.elapsed().as_secs() < MIRROR_FRESHNESS_SECS)
}

/// 临时物/锁的清理门槛：静默窗与一分钟取大。比它新的东西**不动**——它们
/// 可能握在一个活着的传输手里，删锁等于让两份 git 同时改同一个 ref。
fn residue_max_age_for(cfg: &GitMirrorConfig) -> std::time::Duration {
    cfg.stall
        .max(std::time::Duration::from_secs(RESIDUE_MIN_AGE_SECS))
}

async fn read_all(mut r: impl tokio::io::AsyncRead + Unpin) -> Vec<u8> {
    let mut out = Vec::new();
    let _ = r.read_to_end(&mut out).await;
    out
}

/// 读 stderr 并**顺带记下最后一次输出的时刻**——这就是"还在动"的证据本身。
///
/// 只留尾部（`STDERR_TAIL_CHARS` 的一小段缓冲）：进度刷屏可能几 MB，而失败原因
/// 在最后几行。
async fn read_tail(
    mut r: impl tokio::io::AsyncRead + Unpin,
    last_output: Arc<Mutex<std::time::Instant>>,
) -> Vec<u8> {
    let cap = STDERR_TAIL_CHARS * 8;
    let mut buf = vec![0u8; 4096];
    let mut out: Vec<u8> = Vec::new();
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                *last_output.lock().unwrap() = std::time::Instant::now();
                out.extend_from_slice(&buf[..n]);
                if out.len() > cap {
                    let drop = out.len() - cap;
                    out.drain(..drop);
                }
            }
        }
    }
    out
}

fn elapsed_secs(since: std::time::Instant) -> u64 {
    since.elapsed().as_secs()
}

fn tail_text(bytes: &[u8], max_chars: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim();
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        return s.to_string();
    }
    chars[chars.len() - max_chars..].iter().collect()
}

fn tail_suffix(bytes: &[u8], max_chars: usize) -> String {
    let tail = tail_text(bytes, max_chars);
    if tail.is_empty() {
        String::new()
    } else {
        format!("；stderr 尾部: {tail}")
    }
}

/// 镜像目录的结构状态。判的是"能不能就地续传"，不是"新鲜不新鲜"。
#[derive(Debug, PartialEq, Eq)]
enum MirrorState {
    /// 目录不存在——从头 clone。
    Absent,
    /// 目录在，但不是一个能 fetch 的 git 库（缺 HEAD/objects/refs：上次 `clone`
    /// 在写仓库骨架时就断了）。留着只会让后续每条 git 命令各自报一个更难懂的
    /// 错，清掉重来。
    Unusable,
    /// 结构完整，可就地续传（可能从未刷完过）。
    Usable,
}

fn mirror_state(dir: &Path) -> MirrorState {
    if !dir.is_dir() {
        return MirrorState::Absent;
    }
    // 骨架三件套。**不查 config**：续传用的是显式 URL + refspec，不依赖镜像
    // 自己的 origin 配置，所以配置文件半截不影响判决。
    let skeleton =
        dir.join("HEAD").is_file() && dir.join("objects").is_dir() && dir.join("refs").is_dir();
    if skeleton {
        MirrorState::Usable
    } else {
        MirrorState::Unusable
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Residue {
    tmp_packs: usize,
    locks: usize,
}

impl Residue {
    fn total(&self) -> usize {
        self.tmp_packs + self.locks
    }
}

/// 清掉上次被中断的传输留下的临时物，只清**明显陈旧**的（超过 `max_age`）。
///
/// 两类：
/// - `objects/pack/tmp_pack_*`：`index-pack` 边收边写的未成包文件。它不是有效
///   对象（收全并校验通过才会改名成 `pack-*.pack`），实测 `git fetch` 自己不会
///   清它——留着白占空间，也让"这个库完不完整"更难判。
/// - `*.lock`：死进程留下的锁。它们在时下一条命令直接报 `Unable to create ...:
///   File exists`，把一个可自愈的状态变成卡死。
///
/// **不动 `*.keep`**：那是"这个 pack 先别合并"的标记，删了会把清理越界成改语义。
fn clean_interrupted_transfer(dir: &Path, max_age: std::time::Duration) -> Residue {
    let mut out = Residue::default();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            let old = meta
                .modified()
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age >= max_age);
            if !old {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("tmp_pack_") {
                if std::fs::remove_file(&path).is_ok() {
                    out.tmp_packs += 1;
                }
            } else if name.ends_with(".lock") && std::fs::remove_file(&path).is_ok() {
                out.locks += 1;
            }
        }
    }
    out
}

/// HEAD 指向的分支在镜像里不存在。refs 可能被打包进 `packed-refs`，两侧都要查。
fn head_is_dangling(dir: &Path) -> bool {
    let Ok(head) = std::fs::read_to_string(dir.join("HEAD")) else {
        return false;
    };
    let Some(target) = head.trim().strip_prefix("ref: ") else {
        return false;
    };
    let target = target.trim();
    if dir.join(target).is_file() {
        return false;
    }
    let packed = std::fs::read_to_string(dir.join("packed-refs")).unwrap_or_default();
    let needle = format!(" {target}");
    !packed.lines().any(|l| l.ends_with(&needle))
}

/// pkt-line 编码：4 位十六进制长度（**含这 4 字节本身**）+ 载荷。
fn pkt_line(payload: &str) -> Vec<u8> {
    let mut out = format!("{:04x}", payload.len() + 4).into_bytes();
    out.extend_from_slice(payload.as_bytes());
    out
}

/// 把 `/owner/repo.git/<suffix>` 拆成 `("owner/repo", "/<suffix>")`。
///
/// 严格限两段路径且逐段校验字符集：这条路径直接参与拼接镜像目录，放任
/// `..`、绝对路径或多余层级过去就等于把请求参数变成文件系统路径。
///
/// 选路的实测也复用它取 `<owner>/<repo>`：探测目标必须与被路由的请求是同一个
/// 仓库，否则测出来的是另一条路径的通断。
pub(crate) fn split_repo_path(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_start_matches('/');
    let idx = trimmed.find(".git/")?;
    let repo = &trimmed[..idx];
    let suffix = &trimmed[idx + 4..];
    let segs: Vec<&str> = repo.split('/').collect();
    if segs.len() != 2 {
        return None;
    }
    let safe = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    if !segs.iter().all(|s| safe(s)) {
        return None;
    }
    Some((repo.to_string(), suffix.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkt_line_length_includes_its_own_four_bytes() {
        // git 的 pkt-line 长度含 4 字节头本身；漏掉这 4 字节的编码会让客户端
        // 把载荷头几个字符读成长度，解析直接错位。
        let line = pkt_line("# service=git-upload-pack\n");
        assert_eq!(&line[..4], b"001e");
        assert_eq!(line.len(), 0x1e);
        assert_eq!(&line[4..], b"# service=git-upload-pack\n");
    }

    #[test]
    fn split_repo_path_accepts_smart_http_shapes() {
        assert_eq!(
            split_repo_path("/hcipengm/cogneva.git/info/refs"),
            Some(("hcipengm/cogneva".into(), "/info/refs".into()))
        );
        assert_eq!(
            split_repo_path("/hcipengm/cogneva.git/git-receive-pack"),
            Some(("hcipengm/cogneva".into(), "/git-receive-pack".into()))
        );
    }

    #[test]
    fn split_repo_path_rejects_traversal_and_depth() {
        // 这些都会让镜像目录跑到 root 之外，或指到别的仓库上。
        for bad in [
            "/../../etc/cogneva.git/info/refs",
            "/hcipengm/cogneva.git",
            "/a/b/c.git/info/refs",
            "/hcipengm/cogneva/info/refs",
            "//cogneva.git/info/refs",
        ] {
            assert!(split_repo_path(bad).is_none(), "不该接受: {bad}");
        }
    }

    #[test]
    fn breaker_opens_on_failure_and_only_real_success_closes_it() {
        let h = GitTransportHealth::default();
        assert!(h.https_available(), "初始无嫌疑，应可用");

        let (n, secs) = h.note_https_failure();
        assert_eq!(n, 1);
        assert!(secs > 0, "首次失败要开窗");
        assert!(!h.https_available(), "开窗后应判不可用");

        // 窗口内的重复失败不重开窗（返回 secs=0），否则一次事故会把指数打飞。
        let (n2, secs2) = h.note_https_failure();
        assert_eq!(n2, 1);
        assert_eq!(secs2, 0);

        // 真实成功才清除；这里是唯一的清除路径。
        assert!(h.note_https_success(), "此前处于嫌疑，应报告恢复");
        assert!(h.https_available());
        assert!(!h.note_https_success(), "已健康时不应重复报告恢复");
    }

    #[test]
    fn fallback_is_off_without_a_key() {
        // 没挂私钥就是未启用——选路的唯一开关，错了会让每个请求都去试
        // 注定失败的 SSH。
        let t = GitTransport::new(GitMirrorConfig::from_parts(
            PathBuf::from("/tmp/cogneva-mirror-test"),
            None,
            DEFAULT_SSH_BASE.into(),
        ));
        assert!(!t.fallback_available());
    }

    // ── 传输看门狗与"半个镜像"的判据 ──────────────────────────

    #[test]
    fn mirror_state_separates_absent_from_half_written() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("github").join("o").join("r.git");
        assert_eq!(
            mirror_state(&dir),
            MirrorState::Absent,
            "不存在就是从头 clone"
        );

        // `clone` 先写骨架再传对象：骨架写一半就被打断的目录**不能**当作
        // "已经克隆过了"——旧判据（HEAD 在即已克隆）会让它被就地 fetch，
        // 而它连 ref 都没有。
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("HEAD"), "ref: refs/heads/master\n").unwrap();
        assert_eq!(
            mirror_state(&dir),
            MirrorState::Unusable,
            "缺 objects/refs 是骨架都不全"
        );

        std::fs::create_dir_all(dir.join("objects")).unwrap();
        std::fs::create_dir_all(dir.join("refs")).unwrap();
        assert_eq!(
            mirror_state(&dir),
            MirrorState::Usable,
            "骨架齐全就该就地续传，而不是清掉重来"
        );
    }

    #[test]
    fn residue_cleanup_spares_what_a_live_transfer_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("r.git");
        std::fs::create_dir_all(dir.join("objects").join("pack")).unwrap();
        std::fs::create_dir_all(dir.join("refs").join("heads")).unwrap();
        let stale_tmp = dir.join("objects").join("pack").join("tmp_pack_OLD");
        let fresh_tmp = dir.join("objects").join("pack").join("tmp_pack_NEW");
        let stale_lock = dir.join("refs").join("heads").join("main.lock");
        let fresh_lock = dir.join("packed-refs.lock");
        let keep = dir.join("objects").join("pack").join("pack-abc.keep");
        for p in [&stale_tmp, &fresh_tmp, &stale_lock, &fresh_lock, &keep] {
            std::fs::write(p, "x").unwrap();
        }
        age_file(&stale_tmp, 600);
        age_file(&stale_lock, 600);

        let removed = clean_interrupted_transfer(&dir, std::time::Duration::from_secs(60));

        assert_eq!(
            removed,
            Residue {
                tmp_packs: 1,
                locks: 1
            },
            "只清明显陈旧的临时物与锁"
        );
        assert!(!stale_tmp.exists() && !stale_lock.exists());
        assert!(
            fresh_tmp.exists() && fresh_lock.exists(),
            "新鲜的东西可能握在一个活着的传输手里，删锁会让两份 git 同时改同一个 ref"
        );
        assert!(keep.exists(), "*.keep 是语义标记，不是垃圾");
    }

    #[test]
    fn head_is_dangling_catches_the_interrupted_clone_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("r.git");
        std::fs::create_dir_all(dir.join("refs").join("heads")).unwrap();
        // 被中断的 clone 留下的正是这个形状：HEAD 指向默认初始分支 master，
        // 而上游的默认分支是 main。
        std::fs::write(dir.join("HEAD"), "ref: refs/heads/master\n").unwrap();
        assert!(head_is_dangling(&dir));

        // 松散 ref 在就算不悬空。
        std::fs::write(dir.join("refs").join("heads").join("master"), "0").unwrap();
        assert!(!head_is_dangling(&dir));
        std::fs::remove_file(dir.join("refs").join("heads").join("master")).unwrap();

        // 打包进 packed-refs 的 ref 同样算在。
        std::fs::write(
            dir.join("packed-refs"),
            "# pack-refs with: peeled fully-peeled sorted \n0 refs/heads/master\n",
        )
        .unwrap();
        assert!(!head_is_dangling(&dir));

        // 非符号 HEAD（detached）没有"悬空"可言。
        std::fs::write(
            dir.join("HEAD"),
            "0000000000000000000000000000000000000000\n",
        )
        .unwrap();
        std::fs::remove_file(dir.join("packed-refs")).unwrap();
        assert!(!head_is_dangling(&dir));
    }

    #[test]
    fn stderr_tail_keeps_the_reason_and_drops_the_progress_spam() {
        // 失败原因在最后几行；前几千行是进度刷屏。留头等于把原因扔掉。
        let mut body = String::new();
        for i in 0..500 {
            body.push_str(&format!("remote: Receiving objects: {i}%\r"));
        }
        body.push_str("fatal: the remote end hung up unexpectedly");
        let tail = tail_text(body.as_bytes(), 80);
        assert!(tail.contains("the remote end hung up unexpectedly"));
        assert!(tail.chars().count() <= 80);
    }

    /// 把文件改成 `secs` 秒前。清理门槛判的是年龄，所以判据测试必须能造出
    /// 陈旧的文件，不能只造"刚写的"。
    fn age_file(path: &Path, secs: i64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let t = libc::timeval {
            tv_sec: now - secs,
            tv_usec: 0,
        };
        let times = [t, t];
        let c = std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap();
        // SAFETY: 两个指针都指向本函数栈上的有效数据，调用期间不会失效。
        let rc = unsafe { libc::utimes(c.as_ptr(), times.as_ptr()) };
        assert_eq!(rc, 0, "utimes 失败: {}", path.display());
    }

    // ── 端到端：真实 git 客户端 ────────────────────────────────

    async fn mirror_handler(
        axum::extract::State(t): axum::extract::State<std::sync::Arc<GitTransport>>,
        req: axum::extract::Request,
    ) -> Result<axum::response::Response, (StatusCode, String)> {
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(str::to_string);
        let git_protocol = req
            .headers()
            .get("git-protocol")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let is_get = req.method() == axum::http::Method::GET;
        let body = axum::body::to_bytes(req.into_body(), MAX_MIRROR_BODY)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        t.serve(MirrorRequest {
            path: &path,
            query: query.as_deref(),
            git_protocol: git_protocol.as_deref(),
            is_get,
            body: &body,
        })
        .await
    }

    /// 在 `dir` 里跑一条 git 命令，成功返回 trim 过的 stdout。
    /// 关掉系统/全局配置：宿主的 gitconfig 不该影响测试结论。
    ///
    /// **必须是异步的**：`#[tokio::test]` 默认单线程，用阻塞式 `Command` 等
    /// git 客户端时，同一线程上的 axum 服务端就没机会被轮询——客户端等应答、
    /// 服务端等被调度，双方互等到超时。
    async fn git(dir: &Path, args: &[&str]) -> String {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@example.com")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@example.com")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .output()
            .await
            .expect("git 应当可执行");
        assert!(
            out.status.success(),
            "git {args:?} 失败: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 用**真实 git 客户端**跑通兜底镜像的 clone 与 push。
    ///
    /// 这是这个模块唯一算数的验证。单元测试只证明 pkt-line 编码和路径解析对，
    /// 证明不了整段东西真的被 git 客户端认成 smart HTTP——而"兜底在黑洞期接不住"
    /// 比"没有兜底"更糟：它会把一次通道故障伪装成一次上游拒绝，让排障方向整个跑偏。
    ///
    /// 全程不碰网络：`ssh_base` 指向本地目录、布局与 GitHub 一致
    /// （`<base>/<owner>/<repo>.git`），"上游"就是那个本地裸仓。于是这条测试
    /// 覆盖了 refresh / upload-pack / receive-pack / propagate_push 全链路，
    /// 唯一被替换掉的只有 SSH 传输本身。
    #[tokio::test]
    async fn real_git_client_can_clone_and_push_through_the_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("mirror-root");
        let upstream_base = tmp.path().join("upstream");
        let upstream = upstream_base.join("local").join("repo.git");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(upstream.parent().unwrap()).unwrap();

        // 造"上游"裸仓（= GitHub 在这一测里的替身）与一个初始提交
        git(
            tmp.path(),
            &["init", "--bare", "-b", "main", upstream.to_str().unwrap()],
        )
        .await;
        git(tmp.path(), &["init", "-b", "main", work.to_str().unwrap()]).await;
        std::fs::write(work.join("a.txt"), "hello\n").unwrap();
        git(&work, &["add", "."]).await;
        git(&work, &["commit", "-m", "first"]).await;
        git(&work, &["push", upstream.to_str().unwrap(), "main"]).await;
        let first = git(&work, &["rev-parse", "HEAD"]).await;

        // 预置镜像（等价于"第一次刷新已经完成"）
        let mirror_dir = root.join("github").join("local").join("repo.git");
        std::fs::create_dir_all(mirror_dir.parent().unwrap()).unwrap();
        git(
            tmp.path(),
            &[
                "clone",
                "--mirror",
                upstream.to_str().unwrap(),
                mirror_dir.to_str().unwrap(),
            ],
        )
        .await;

        // 私钥指向一个存在的文件：serve() 的可达性由 `ssh_key.is_some()` 决定，
        // 而 origin 是本地路径，SSH 命令根本不会被调用。
        let key = tmp.path().join("dummy_key");
        std::fs::write(&key, "not-a-real-key\n").unwrap();
        let transport = std::sync::Arc::new(GitTransport::new(GitMirrorConfig::from_parts(
            root,
            Some(key),
            // 末尾斜杠是必须的：它就是拼在 `<owner>/<repo>.git` 前面的前缀。
            format!("{}/", upstream_base.display()),
        )));
        assert!(transport.fallback_available());

        let app = axum::Router::new()
            .fallback(mirror_handler)
            .with_state(transport);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let url = format!("http://127.0.0.1:{port}/local/repo.git");

        // 1) clone：这一步直接证伪"pkt-line 前缀拼错 / --stateless-rpc 参数顺序
        //    不对 / v2 协议没透传"这类会让兜底完全不可用的问题。
        let clone = tmp.path().join("clone");
        git(tmp.path(), &["clone", &url, clone.to_str().unwrap()]).await;
        assert_eq!(
            git(&clone, &["rev-parse", "HEAD"]).await,
            first,
            "clone 应拿到上游的 main"
        );

        // 2) push：且必须**真的落到上游**，不是只被镜像收下。
        //    这正是"Pod 认为成功 == GitHub 真的收到"那条等价性的实测。
        std::fs::write(clone.join("b.txt"), "second\n").unwrap();
        git(&clone, &["add", "."]).await;
        git(&clone, &["commit", "-m", "second"]).await;
        let second = git(&clone, &["rev-parse", "HEAD"]).await;
        git(&clone, &["push", "origin", "main"]).await;
        assert_eq!(
            git(&upstream, &["rev-parse", "main"]).await,
            second,
            "push 必须已同步到真上游，而不只是被镜像收下"
        );
    }

    // ── 看门狗：真的会杀掉整条链 ─────────────────────────────

    /// 造一个假 git，把它指成镜像路径的 `git_bin`。
    ///
    /// 超时判据只能用假 git 测：真 git 要挂住就得有一条真的会死的链路，而
    /// 那不可复现。这里造的是"输出一行进度之后彻底静默"，正是线上事故的形态。
    fn write_fake_git(dir: &Path, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.join("fake-git");
        std::fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut perm = std::fs::metadata(&p).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&p, perm).unwrap();
        p
    }

    /// 假 git 的 exec 在并行测试里会撞上一个微秒级竞态：刚写完的脚本此刻还被
    /// 别的测试 fork 出来的进程持着一个写 fd（CLOEXEC 要等它自己 exec 才关），
    /// 内核就回 `Text file busy`。这和被测的判据无关，重试一次。
    async fn refresh_retrying_text_busy(t: &GitTransport) -> Result<(), String> {
        match t.refresh("local/repo", true).await {
            Err(e) if e.contains("Text file busy") => t.refresh("local/repo", true).await,
            other => other,
        }
    }

    fn test_transport(
        root: &Path,
        git_bin: PathBuf,
        ssh_base: String,
        stall_secs: u64,
        timeout_secs: u64,
    ) -> GitTransport {
        let mut cfg =
            GitMirrorConfig::from_parts(root.to_path_buf(), Some(root.join("dummy_key")), ssh_base);
        cfg.git_bin = git_bin;
        cfg.stall = std::time::Duration::from_secs(stall_secs);
        cfg.op_timeout = std::time::Duration::from_secs(timeout_secs);
        GitTransport::new(cfg)
    }

    /// 进程是不是还活着（僵尸不算活着：它已经死了，只是没人回收）。
    fn process_alive(pid: i32) -> bool {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        stat.rsplit(')')
            .next()
            .and_then(|s| s.split_whitespace().next())
            != Some("Z")
    }

    #[tokio::test]
    async fn silent_transfer_is_killed_along_with_its_whole_process_group() {
        let tmp = tempfile::tempdir().unwrap();
        let pidfile = tmp.path().join("grandchild.pid");
        // 形态与线上一致：先来一行进度，然后永远不再出声；并且它**有子进程**
        // （`git → sh → ssh → 远端`），子进程也会往镜像里写东西。
        let fake = write_fake_git(
            tmp.path(),
            &format!(
                "echo 'remote: Receiving objects: 1%' >&2\n\
                 sleep 300 &\n\
                 echo $! > {}\n\
                 sleep 300\n",
                pidfile.display()
            ),
        );
        let t = test_transport(tmp.path(), fake, DEFAULT_SSH_BASE.into(), 1, 30);

        let started = std::time::Instant::now();
        let err = refresh_retrying_text_busy(&t)
            .await
            .expect_err("静默的传输必须被判死");
        let elapsed = started.elapsed();
        assert!(err.contains("静默"), "错误应说清是静默判死: {err}");
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "静默窗 1s，判死不该拖这么久: {elapsed:?}"
        );

        // 只杀组长会留下还在写镜像的孙进程——线上那次 index-pack 就活了十几分钟。
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .expect("假 git 应写下孙进程 pid")
            .trim()
            .parse()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while process_alive(pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(!process_alive(pid), "孙进程 {pid} 必须一起被杀掉");
    }

    #[tokio::test]
    async fn progressing_transfer_survives_the_stall_watchdog() {
        let tmp = tempfile::tempdir().unwrap();
        // 每次间隔都短于静默窗：这是"慢"而不是"死"，判死就是误杀。
        let fake = write_fake_git(
            tmp.path(),
            "echo 'remote: Receiving objects: 10%' >&2\n\
             sleep 0.3\n\
             echo 'remote: Receiving objects: 50%' >&2\n\
             sleep 0.3\n\
             echo 'remote: Receiving objects: 90%' >&2\n\
             sleep 0.3\n\
             exit 0",
        );
        let t = test_transport(tmp.path(), fake, DEFAULT_SSH_BASE.into(), 1, 30);

        let started = std::time::Instant::now();
        refresh_retrying_text_busy(&t)
            .await
            .expect("一直在输出的传输不该被杀");
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }

    #[tokio::test]
    async fn chatty_but_endless_transfer_hits_the_total_ceiling() {
        let tmp = tempfile::tempdir().unwrap();
        // 反面形态：远端一直在吐进度却永不结束。静默判据永远不触发，只有总上限
        // 拦得住——少了这层，一个"一直在动但推不完"的传输就能把写锁占死。
        let fake = write_fake_git(
            tmp.path(),
            "i=0\n\
             while [ $i -lt 400 ]; do\n\
             \techo \"remote: Receiving objects: $i%\" >&2\n\
             \tsleep 0.2\n\
             \ti=$((i+1))\n\
             done",
        );
        let t = test_transport(tmp.path(), fake, DEFAULT_SSH_BASE.into(), 5, 1);

        let started = std::time::Instant::now();
        let err = refresh_retrying_text_busy(&t)
            .await
            .expect_err("总上限必须拦下永不结束的传输");
        assert!(err.contains("总上限"), "错误应说清是总上限: {err}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(8),
            "总上限 1s，判死不该拖这么久: {:?}",
            started.elapsed()
        );
    }

    /// 执行体被**取消**（而不是超时）时，整条进程链也要被带走。
    ///
    /// 线上形态：触发刷新的请求被撤（客户端超时断开，axum 丢掉 handler 的
    /// future），看门狗随 future 一起消失，而 `git → sh → ssh` 还在跑——实测留下
    /// 的是一个静默了十几分钟、rchar 不再增长的 `git fetch` 加它的 `index-pack`。
    /// 这里把看门狗窗口放到远大于测试时长，所以判死只可能来自"future 被丢弃"。
    #[tokio::test]
    async fn cancelling_the_executor_still_kills_the_whole_group() {
        let tmp = tempfile::tempdir().unwrap();
        let pidfile = tmp.path().join("grandchild.pid");
        let fake = write_fake_git(
            tmp.path(),
            &format!(
                "sleep 300 &\n\
                 echo $! > {}\n\
                 sleep 300\n",
                pidfile.display()
            ),
        );
        let t = test_transport(tmp.path(), fake, DEFAULT_SSH_BASE.into(), 30, 60);
        let exec = t.exec.clone();

        let task = tokio::spawn(async move { exec.capture(&["ls-remote", "x"], None).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !pidfile.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .expect("假 git 应写下孙进程 pid")
            .trim()
            .parse()
            .unwrap();
        assert!(process_alive(pid), "孙进程应当先真的起来");

        task.abort();
        let _ = task.await;

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while process_alive(pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            !process_alive(pid),
            "调用者的 future 没了，孙进程 {pid} 也必须一起走——只杀组长会把它留下"
        );
    }

    /// 刷新的执行体要**活得比触发它的请求久**。
    ///
    /// 这条链路上一次克隆是分钟量级，而请求随时可能被撤。执行体跟着请求死掉的
    /// 后果实测：传了几 MB 全白费，写锁还被提前释放，下一个请求对同一个库又起
    /// 一条 fetch。所以取消调用者之后，镜像仍然要走到"新鲜"。
    #[tokio::test]
    async fn refresh_work_outlives_the_caller_that_triggered_it() {
        for _ in 0..2 {
            // 第二次只为绕开"刚写完的脚本被别的测试 fork 出来的进程持着写 fd"
            // 这个与判据无关的微秒级竞态（见 refresh_retrying_text_busy）。
            let tmp = tempfile::tempdir().unwrap();
            let done = tmp.path().join("clone-done");
            let fake = write_fake_git(
                tmp.path(),
                &format!("sleep 1\necho ok > {}\nexit 0\n", done.display()),
            );
            let t = Arc::new(test_transport(
                tmp.path(),
                fake,
                DEFAULT_SSH_BASE.into(),
                30,
                60,
            ));
            let dir = t.mirror_dir("local/repo");

            let caller = {
                let t = t.clone();
                tokio::spawn(async move { t.refresh("local/repo", true).await })
            };
            // 让 clone 真的跑起来，再把调用者撤掉。
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            caller.abort();
            let _ = caller.await;

            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !t.is_fresh(&dir) && std::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if t.is_fresh(&dir) {
                assert!(done.exists(), "传输应当已经自己跑完");
                return;
            }
        }
        panic!("调用者被撤之后，刷新的执行体没能自己跑完");
    }

    /// 半个镜像要**就地在原对象库上续传**，不是清掉重来。
    ///
    /// 这条判据是"链路传不完时系统还能不能前进"的承重墙：清掉重来会在同一条
    /// 传不完的链路上永远从零开始，而就地 fetch 每轮都能把已收到的对象留住、
    /// 把缺的 ref 补上。
    #[tokio::test]
    async fn half_clone_is_resumed_instead_of_recloned() {
        let tmp = tempfile::tempdir().unwrap();
        let upstream_base = tmp.path().join("upstream");
        let upstream = upstream_base.join("local").join("repo.git");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(upstream.parent().unwrap()).unwrap();
        git(
            tmp.path(),
            &[
                "init",
                "-q",
                "--bare",
                "-b",
                "main",
                upstream.to_str().unwrap(),
            ],
        )
        .await;
        git(
            tmp.path(),
            &["init", "-q", "-b", "main", work.to_str().unwrap()],
        )
        .await;
        std::fs::write(work.join("a.txt"), "hello\n").unwrap();
        git(&work, &["add", "."]).await;
        git(&work, &["commit", "-qm", "first"]).await;
        git(&work, &["push", "-q", upstream.to_str().unwrap(), "main"]).await;
        let tip = git(&work, &["rev-parse", "HEAD"]).await;

        // 造"被中断的 clone"：骨架齐全、没有任何 ref、HEAD 停在默认初始分支、
        // 还留着一个上一次传输的 tmp_pack。
        let root = tmp.path().join("mirror-root");
        let dir = root.join("github").join("local").join("repo.git");
        std::fs::create_dir_all(dir.join("objects").join("pack")).unwrap();
        std::fs::create_dir_all(dir.join("refs").join("heads")).unwrap();
        std::fs::write(dir.join("HEAD"), "ref: refs/heads/master\n").unwrap();
        let leftover = dir.join("objects").join("pack").join("tmp_pack_LEFT");
        std::fs::write(&leftover, "half a pack").unwrap();

        // ssh_base 指向本地裸仓：这一测只验"半个库能不能就地续传"，不碰网络。
        let t = test_transport(
            &root,
            PathBuf::from("git"),
            format!("{}/", upstream_base.display()),
            180,
            1800,
        );
        assert_eq!(mirror_state(&dir), MirrorState::Usable);

        t.refresh("local/repo", true).await.expect("续传应当成功");

        assert_eq!(
            git(&dir, &["rev-parse", "refs/heads/main"]).await,
            tip,
            "续传后镜像必须有上游的 ref"
        );
        // HEAD 悬空时按远端默认分支修正：留着它的后果是"静默空克隆"。
        assert_eq!(
            std::fs::read_to_string(dir.join("HEAD")).unwrap(),
            "ref: refs/heads/main\n"
        );
    }

    /// 镜像没有私钥时不该被选中——这是"部署事实 vs 瞬时故障"的边界：
    /// 缺私钥若被当成故障去重试，会把每个请求都拖到超时。
    #[tokio::test]
    async fn serve_refuses_paths_outside_owner_repo_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let t = GitTransport::new(GitMirrorConfig::from_parts(
            tmp.path().to_path_buf(),
            None,
            DEFAULT_SSH_BASE.into(),
        ));
        let err = t
            .serve(MirrorRequest {
                path: "/../../etc/passwd.git/info/refs",
                query: Some("service=git-upload-pack"),
                git_protocol: None,
                is_get: true,
                body: &[],
            })
            .await
            .expect_err("路径穿越必须被拒");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }
}
