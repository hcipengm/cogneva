//! 主线跟踪自动部署器：让集群自治跟踪公版 main。
//!
//! 构建侧（进化 Pod 内，[`MainlineDeployer`] + [`run_mainline_loop`]）：
//! 周期检测集群内 bare 仓库（/host-git）的 main 前进 → 沙盒源码树 reset 到
//! 新 rev → cargo build（PVC target 增量缓存）→ buildah 基于"当前在跑的
//! 不可变 tag"打最小 overlay → 推集群内 registry 的 `main-<rev12>` 不可变
//! tag → 组装该 rev 的清单包（发布集里除四个 deployment 外的支撑资源 +
//! 镜像已改写为本次目标的 deployment 清单）发布成 per-rev ConfigMap →
//! 派独立 Job 跑滚动 → 滚动收敛后才把 registry 浮动签 `:local` 前移
//! 到本 rev（`:local` 是静态清单/GitOps apply 的回退锚点，构建期就推会让
//! 失败回滚的坏镜像成为浮动签权威）。
//!
//! 滚动侧（Job 内，[`RolloutExecutor`]，二进制子命令 `cogneva mainline-rollout`）：
//! 挂载了清单目录时先 apply 支撑资源（新镜像启动所需的 RBAC/ConfigMap/
//! Service 等先就位），再按固定顺序滚动四个 deployment（网关代理面先行、
//! 进化宿主最后）——目标有随镜像下发的清单则 apply 整份清单，否则回落
//! set image；每个部署等 rollout 完成（收敛判据归到本次滚动自己的副本上，
//! 未就绪的新副本按就绪探针自身的判定周期给预算），全部滚完后 soak
//! 观察窗复查；任一失败把已滚部署反向 set image 回 prev tag。
//!
//! 清单随镜像走解决的是拓扑滞后：只 set image 时，chart 里与工作负载一起
//! 演进的 ConfigMap/Service/PVC 变更永远到不了集群（没人 apply 新清单），
//! 新二进制配旧拓扑跑。清单包只含命名空间级的非权限面资源：Secret 硬性
//! 拒绝（零带外凭证红线）；集群级 kind 与 Role/RoleBinding 跳过——K8s
//! 反提权规定 apply 一个 Role 要求发起者已持有其中全部权限，部署面永远
//! 不该持有发布集里管理类 Role 的权限，给自己发 bind/escalate 又等于
//! 自治系统自我提权，所以权限边界变更必须走安装面/人工核准。
//!
//! 滚动必须放 Job 而不是进化 Pod 自身：cogneva-evolution 是 Recreate 单副本，
//! 部署器就跑在里面，对自己 set image 会立刻杀掉门禁/回滚逻辑，新镜像
//! crashloop 时无人 undo，进化面永久宕。Job 用新镜像跑还顺带 smoke test：
//! 新二进制起不来则一次 set image 都不会发生。

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use cog_core::{SFError, SFResult, ShutdownSignal};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::{MainlineDeployerConfig, RolloutTargetConfig};

/// buildah 存储库放 sandbox PVC：与金丝雀 publisher 共享基镜像层缓存，
/// Pod 重启不丢。
const BUILDAH_STORAGE: &str = "/opt/cogneva/sandbox/containers/storage";
const BUILDAH_RUNROOT: &str = "/opt/cogneva/sandbox/containers/run";
/// cargo registry 缓存同样落 PVC：镜像里的 /usr/local/cargo 是容器可写层，
/// Pod 一重建（主线滚动最后一个目标就是 evolution 自己）索引与 crate 缓存
/// 全丢，每次构建都要在家庭网络上重拉整个 crates.io 索引。
const CARGO_HOME_PVC: &str = "/opt/cogneva/sandbox/cargo-home";

// ---------------------------------------------------------------------------
// 纯函数（无 IO，单测覆盖）
// ---------------------------------------------------------------------------

/// 完整 rev 取短 id（git 短 sha 惯例 12 字符）。
fn rev12(rev: &str) -> &str {
    if rev.len() >= 12 {
        &rev[..12]
    } else {
        rev
    }
}

/// 不可变主线镜像引用：`<registry>/cogneva:main-<rev12>`。
pub fn main_image(registry: &str, rev: &str) -> String {
    format!(
        "{}/cogneva:main-{}",
        registry.trim_end_matches('/'),
        rev12(rev)
    )
}

/// 浮动签镜像引用：`<registry>/cogneva:local`。
pub fn local_image(registry: &str) -> String {
    format!("{}/cogneva:local", registry.trim_end_matches('/'))
}

/// 从镜像引用解析 `main-<rev>` 的 rev 片段；非主线 tag（:local、promote-*、
/// 节点 localhost/cogneva:local 等）返回 None。
fn parse_main_rev(image: &str) -> Option<&str> {
    let tag = image.rsplit(':').next()?;
    tag.strip_prefix("main-")
}

/// 滚动 Job 名（含 rev，天然幂等键）。
pub fn job_name(rev: &str) -> String {
    format!("cogneva-mainline-{}", rev12(rev))
}

/// `host:port` 端点拆成 (host, port)。buildah 强制 registry 端点带端口，
/// 集群内 registry 因此永远可解析。
fn endpoint_host_port(endpoint: &str) -> Option<(&str, u16)> {
    let (host, port) = endpoint.trim_end_matches('/').rsplit_once(':')?;
    Some((host, port.parse().ok()?))
}

/// 单平台 manifest 的 config blob digest（多平台 index 没有这一层）。
fn config_digest_of(manifest: &serde_json::Value) -> Option<String> {
    manifest
        .get("config")
        .and_then(|c| c.get("digest"))
        .and_then(|d| d.as_str())
        .map(|d| d.to_string())
}

/// 多平台 index 里第一个子 manifest 的 digest（各平台镜像由同一次构建产出，
/// rev 标签一致，取哪个都行）。
fn first_manifest_digest(index: &serde_json::Value) -> Option<String> {
    index
        .get("manifests")
        .and_then(|m| m.as_array())
        .and_then(|a| a.first())
        .and_then(|m| m.get("digest"))
        .and_then(|d| d.as_str())
        .map(|d| d.to_string())
}

/// 从 image config blob 里取构建期写入的 rev 标签。
fn revision_of_config_blob(blob: &serde_json::Value) -> Option<String> {
    blob.get("config")?
        .get("Labels")?
        .get("org.opencontainers.image.revision")?
        .as_str()
        .map(|s| s.to_string())
}

/// 从裸 HTTP 响应里切出状态码与 body（按 `Content-Length` 截断；缺失则取
/// 剩余全部）。registry 的 JSON 响应永远带 Content-Length。
fn parse_http_response(raw: &[u8]) -> Option<(u16, Vec<u8>)> {
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..split]).ok()?;
    let mut lines = head.lines();
    let status = lines.next()?.split_whitespace().nth(1)?.parse().ok()?;
    let len = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok());
    let body = &raw[split + 4..];
    let body = match len {
        Some(n) if n <= body.len() => &body[..n],
        _ => body,
    };
    Some((status, body.to_vec()))
}

/// 这些 tag 的镜像内容与 rev 一一对应（不可变），其余（浮动签）不能由
/// tag 反推 rev——必须问 registry 当前内容是什么版本。
fn tag_is_immutable_for_rev(tag: &str) -> bool {
    tag.starts_with("main-")
}

/// 浮动签是否就是目标 rev：仅当 registry 上该 tag 的镜像当前确实构建自
/// `bare_rev` 时才成立。只看清单里的 tag 字符串会把"标签还在、内容已被
/// 重新播种成旧二进制"当成已收敛，浮动签随即前移，旧二进制被固化成
/// 静态清单 apply 的回退锚点。
fn floating_pin_is_converged(declared_rev: Option<&str>, bare_rev: &str) -> bool {
    matches!(declared_rev, Some(r) if rev12(r) == rev12(bare_rev))
}

/// 命中即无自救可能的 Pod 等待态：拉不到镜像、镜像引用非法、挂载/配置
/// 错误、容器反复崩溃退出。出现这些状态的新副本永远不会 ready，等再久
/// 也只会烧 rollout 超时，必须立即判败触发回滚。
const FATAL_WAITING_REASONS: &[&str] = &[
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CrashLoopBackOff",
];

/// 滚动内部轮询间隔：探测、致命态复查、部署态查询共用同一节拍。
const ROLLOUT_POLL_SECS: u64 = 5;

/// 超时诊断里保留的事件条数。错误记录不是日志转储：只要够指出病因。
const DIAGNOSIS_EVENT_LIMIT: usize = 3;

/// 就绪门禁在探针判定周期之外额外给出的余量：镜像已在节点上时，容器从起进程
/// 到开始监听这一段的实测开销。集群实证（网关 Pod，探针 5|10|1|3）：起容器到
/// 1/1 相隔 42 秒，而它的判定周期是 5 + 3×(10+1) = 38 秒，差值约 4 秒；这里取
/// 15 秒是给这一段留的宽裕上界——它探针一次都还没开始判，不能算进探针预算。
const CONTAINER_STARTUP_ALLOWANCE_SECS: u64 = 15;

/// 就绪探针自己的四个旋钮。门禁的预算从这里推，不拍固定秒数：探针调周期，
/// 预算跟着动；判据的宽度窄于 kubelet 自己的判定周期，就会重演「比探针更早
/// 开枪、把一个正在起来的版本判死」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeCycle {
    initial_delay: u64,
    period: u64,
    timeout: u64,
    failure_threshold: u64,
}

impl ProbeCycle {
    /// 缺格兜底值取 k8s 默认探针的同值（initialDelay 默认 0）。它只在采样行
    /// 确实拿到了、但某些格为空时逐格生效：一个部署没有就绪探针时四格全空，
    /// 而 running 即 ready 的容器本来就不存在"探针还没通过"的窗口。
    const DEFAULT: Self = Self {
        initial_delay: 0,
        period: 10,
        timeout: 1,
        failure_threshold: 3,
    };

    /// 解析采样行 `initialDelay|period|timeout|failureThreshold`。逐格兜底：
    /// 探针只写了部分字段（或该部署根本没有就绪探针）时，缺的那格用默认值，
    /// 不整条丢弃——丢掉一整格会让预算突然缩回默认值。
    fn parse(out: &str) -> Self {
        let f = sample_fields(out);
        let get = |i: usize, dflt: u64| f.get(i).and_then(|v| v.parse().ok()).unwrap_or(dflt);
        Self {
            initial_delay: get(0, Self::DEFAULT.initial_delay),
            period: get(1, Self::DEFAULT.period),
            timeout: get(2, Self::DEFAULT.timeout),
            failure_threshold: get(3, Self::DEFAULT.failure_threshold),
        }
    }

    /// 门禁预算 = 探针自己的判定周期 + 容器进程启动开销。
    ///
    /// 判定周期 = initialDelay（之前 kubelet 一次都不探）+ failureThreshold ×
    /// (period + timeout)（每次探测最多花 timeout，两次之间隔 period；阈值满之前
    /// kubelet 不会下「这容器一直不 ready」的结论）。这就是"探针还没通过"这件事
    /// 在 kubelet 眼里的正常时长，门禁不该比它更早开枪。
    fn budget(&self) -> Duration {
        let judgements = self
            .failure_threshold
            .saturating_mul(self.period.saturating_add(self.timeout));
        let cycle = self.initial_delay.saturating_add(judgements);
        Duration::from_secs(cycle.saturating_add(CONTAINER_STARTUP_ALLOWANCE_SECS))
    }
}

/// 把 kubectl 采样输出的一行切成字段。所有采样查询都用竖线显式占位，所以
/// 「字段缺失」表现为空串而不是少一列——这一条约定只在这里写一次。
fn sample_fields(line: &str) -> Vec<&str> {
    line.trim().split('|').map(str::trim).collect()
}

/// 一条 Pod 现场。每行形如
/// `name|phase|ready|waitingReason|waitingMessage|startedAt|deletionTimestamp`，
/// 竖线显式占位：未起来的容器没有 startedAt、健康的容器没有 waiting，omitempty
/// 的字段缺失时不会顶掉后面字段的位置。后两列用 `get` 取，所以只有前五列的
/// 采样行照样能解析。
///
/// 这是诊断与就绪门禁**共用**的一批现场——两者问的本来就是同一件事：这个 Pod
/// 现在卡在哪一步。拆成两条查询会让同一个 Pod 在两条路径上给出不同说法。
struct PodSample {
    name: String,
    phase: String,
    ready: bool,
    waiting_reason: String,
    waiting_message: String,
    /// 主容器当前的启动时刻；容器还没起来时为 None。
    started_at: Option<DateTime<Utc>>,
    /// 正在删除中：旧副本 Terminating。它可能仍然 ready，但不属于本次滚动。
    terminating: bool,
}

impl PodSample {
    /// 主容器已运行的时长；容器还没起来时为 None——「没起来」与「刚起来」是
    /// 两件事，前者不该显示成 0s。
    fn uptime(&self, now: DateTime<Utc>) -> Option<Duration> {
        self.started_at
            .map(|t| now.signed_duration_since(t))
            .and_then(|d| d.to_std().ok())
    }
}

/// 解析 Pod 采样输出。字段残缺的半行整条丢弃。
fn pod_samples(out: &str) -> Vec<PodSample> {
    let mut v = Vec::new();
    for line in out.lines() {
        let f = sample_fields(line);
        if f.len() < 5 || f[0].is_empty() {
            continue;
        }
        v.push(PodSample {
            name: f[0].to_string(),
            phase: f[1].to_string(),
            ready: f[2] == "true",
            waiting_reason: f[3].to_string(),
            waiting_message: f[4].to_string(),
            started_at: f
                .get(5)
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|t| t.with_timezone(&Utc)),
            terminating: f.get(6).is_some_and(|s| !s.is_empty()),
        });
    }
    v
}

/// 把 Pod 采样折成诊断。空字段是「没有该信号」而不是「信号为空」，不进诊断
/// ——否则满行空的 `waiting=` 会把真正的 `Pending` 病因埋掉。Terminating 的旧
/// 副本单独标出来：它的 not-ready 是正常的关停过程，不是本次滚动的病情。
fn summarize_pod_states(samples: &[PodSample], now: DateTime<Utc>) -> String {
    let mut parts: Vec<String> = Vec::new();
    for p in samples {
        let mut seg = format!(
            "{} {}",
            p.name,
            if p.phase.is_empty() { "?" } else { &p.phase }
        );
        seg.push_str(if p.ready { " ready" } else { " not-ready" });
        if !p.waiting_reason.is_empty() {
            seg.push_str(&format!(" waiting={}", p.waiting_reason));
        }
        if !p.waiting_message.is_empty() {
            seg.push_str(&format!(" ({})", p.waiting_message));
        }
        if let Some(ran) = p.uptime(now) {
            seg.push_str(&format!(" ran={}s", ran.as_secs()));
        }
        if p.terminating {
            seg.push_str(" terminating");
        }
        parts.push(seg);
    }
    parts.join("; ")
}

/// 本次滚动自己的 Pod（排除正在删除的旧副本）是否全部就绪。
///
/// deployment 的计数器凑得出「滚完了」：旧副本 Terminating 但还 Ready 时它仍
/// 可能计入 readyReplicas，而新副本尚未 ready 却已算进 updatedReplicas，两个数
/// 一凑就是假收敛。收敛必须归到这次滚动自己的副本上，所以再直接看一眼 Pod。
///
/// 采样为空（一次都没取到）返回 None：没有观测就不下结论，收敛与否交回外层预算
/// 与 soak 复查——空集合是「没有证据」，不是「健康」。
fn rollout_pods_ready(samples: &[PodSample]) -> Option<bool> {
    let own: Vec<&PodSample> = samples.iter().filter(|p| !p.terminating).collect();
    if own.is_empty() {
        return None;
    }
    Some(own.iter().all(|p| p.ready))
}

/// 自己的副本里有没有必死等待态（拉不到镜像、配置错误、CrashLoop）。这些副本
/// 永远等不到 ready，等下去只会把预算烧完；判据与超时路径同一份枚举。
fn rollout_pods_fatal(samples: &[PodSample]) -> Option<String> {
    samples
        .iter()
        .filter(|p| !p.terminating)
        .find(|p| FATAL_WAITING_REASONS.contains(&p.waiting_reason.as_str()))
        .map(|p| format!("{} waiting={}", p.name, p.waiting_reason))
}

/// 就绪预算的起算点：自己的、尚未就绪的副本里**最近**启动的那个容器。取最近的
/// 是因为门禁问的是「最新副本给了多久」——按更老的副本起算会让新副本刚起来就被
/// 判超时。容器还没起来（无 startedAt）就不起算：那一段是调度与拉镜像，由外层
/// 预算兜底。
fn readiness_anchor(samples: &[PodSample]) -> Option<DateTime<Utc>> {
    samples
        .iter()
        .filter(|p| !p.terminating && !p.ready)
        .filter_map(|p| p.started_at)
        .max()
}

/// 未就绪的副本是否已经用完整段就绪预算。没有起算点时一律不判超预算。
fn readiness_overdue(samples: &[PodSample], budget: Duration, now: DateTime<Utc>) -> bool {
    let Some(anchor) = readiness_anchor(samples) else {
        return false;
    };
    now.signed_duration_since(anchor)
        .to_std()
        .map(|ran| ran >= budget)
        .unwrap_or(false)
}

/// 把事件行折成诊断，只留与本次部署相关的最近几条。
///
/// 全命名空间的事件绝大多数与这次滚动无关，不过滤就会把真正的
/// `FailedScheduling` 挤出记录。相关性按**名字精确匹配**判：Deployment 本身的
/// 事件，或作用对象是这次采样到的那个 Pod。不能用「部署名前缀」匹配——`cogneva`
/// 的前缀能套住 `cogneva-evolution` / `cogneva-sandbox-executor` 的全部 Pod，
/// 那会把别的部署的病因记到这次滚动头上。
///
/// 每行形如 `kind|name|reason|message`。
fn summarize_events(out: &str, deployment: &str, pod_names: &[String], limit: usize) -> String {
    let mut lines: Vec<String> = Vec::new();
    for line in out.lines() {
        let f = sample_fields(line);
        if f.len() < 4 || f[2].is_empty() {
            continue;
        }
        let (kind, name) = (f[0], f[1]);
        let related = (kind == "Deployment" && name == deployment)
            || (kind == "Pod" && pod_names.iter().any(|p| p == name));
        if !related {
            continue;
        }
        let text = format!("{} {}: {}", name, f[2], f[3]);
        if !lines.contains(&text) {
            lines.push(text);
        }
    }
    let start = lines.len().saturating_sub(limit);
    lines[start..].join("; ")
}

/// 「观测能力故障」的标记与识别。apiserver 不可达、证书握手超时、连接被拒、
/// 查询整体超时，说的是**我们看不到集群**，不是**看清楚了集群里的版本有问题**。
/// 两者混进同一个判据，一次网络抖动就会把刚推上去的好版本回滚回旧版；这台
/// 机器上部署器自己的构建负载正是抖动来源之一（4C 满载时 apiserver 会短暂
/// 掉握手）。带标记的错误由调用方归入「本轮没有观测」，不作版本结论。
const CLUSTER_UNREACHABLE_MARKER: &str = "cluster unreachable";

const CLUSTER_UNREACHABLE_PATTERNS: &[&str] = &[
    CLUSTER_UNREACHABLE_MARKER,
    "Unable to connect to the server",
    "TLS handshake timeout",
    "connection refused",
    "no route to host",
    "i/o timeout",
    "Client.Timeout",
    "timed out after",
];

/// 错误文本是否是集群访问故障（而非版本缺陷）。纯函数：判据只认文本，
/// 不依赖任何现场状态。
fn is_cluster_unreachable(msg: &str) -> bool {
    CLUSTER_UNREACHABLE_PATTERNS.iter().any(|p| msg.contains(p))
}

/// 「集群放不下新 Pod」的标记，与 cluster-unreachable 同一类：它说的是**这次
/// 滚动没有败在版本上**。判据来自调度器自己的类型化判决——新 Pod 的
/// `PodScheduled=False / Unschedulable`，不是从散文里猜的措辞。
///
/// 排不上队时新 Pod 一个都没起来，谈不上新版本的好坏：回滚要把旧镜像重新
/// 调度一遍，而它带着同样的放置面，同样排不进去，只会多一轮 churn 并把
/// 一个可能正常的版本换掉。真正的边界在版本**改没改放置面**上——改了
/// （这次上线把 requests 调大、加了排不上的 nodeSelector）就是版本的事，该
/// 回滚；没改就与版本无关。
const PLACEMENT_BLOCKED_MARKER: &str = "placement blocked";

fn is_placement_blocked(msg: &str) -> bool {
    msg.contains(PLACEMENT_BLOCKED_MARKER)
}

/// 「观测工具本身起不来」的标记，与前两类同一族：说的是**我们没能观测**，不是
/// 观测到版本有毛病。kubectl 二进制缺失、没有可执行位、正被写入（ETXTBSY）这
/// 类 exec 失败下一次查询都没发出去。
///
/// 单列一类而不是并进 cluster-unreachable：那一类的语义是"重试可能等到"
/// （apiserver 抖动、握手超时），`probe` 会按轮询节拍在预算内重试；工具起不来
/// 在本进程生命周期里重试多少次都一样，必须就地返回——但归的还是环境类，
/// 因为它同样说不出新版本的好坏，而回滚只退镜像，修不好一个坏掉的工具路径。
const OBSERVATION_TOOL_MARKER: &str = "observation tool unavailable";

fn is_observation_tool_failure(msg: &str) -> bool {
    msg.contains(OBSERVATION_TOOL_MARKER)
}

/// 镜像一次都还没动时失败的归类。此时没有回滚对象，要判的只是"这次失败说不说
/// 得出新版本的问题"：观测能力故障说不出，归环境类，否则归版本类。
///
/// 这里只有这两类观测故障会出现——调度器判决得等新 Pod 出现，快照阶段还没有
/// 新 Pod。归环境类是为了不占尝试预算：拿一次纯粹的可达性抖动或一个坏掉的工具
/// 路径去消耗本 rev 的尝试，会按上限把一个本来正常的版本搁置到下个 rev。
fn classify_before_any_change(e: SFError) -> RolloutFailure {
    let msg = e.to_string();
    if is_cluster_unreachable(&msg) || is_observation_tool_failure(&msg) {
        RolloutFailure::environment(e)
    } else {
        RolloutFailure::version(e)
    }
}

/// 滚动 Job 的进程退出码里「环境类失败」那一档。Job 内的判定进程按这个码告诉
/// 部署器：这次失败说不出新版本的好坏。取值沿用 sysexits 的 EX_TEMPFAIL——
/// 语义正是"此刻不成、换个时刻可能就成了"，与"任何非零即失败"（版本类走 1）不
/// 冲突。部署器只在 Job 已失败时读一次终止码，正常路径不多花一次 kubectl。
///
/// 只认这一个确切的码：137/143 这类信号终止码同时对应 OOM、驱逐与
/// `activeDeadlineSeconds` 到点，判不出类别，按版本类靠。
const ROLLOUT_EXIT_ENVIRONMENT: i32 = 75;

/// 滚动失败的类别。分的是**这次失败说不说得出新版本的问题**，部署器据此决定
/// 要不要把这次失败记进本 rev 的尝试预算。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FailureClass {
    /// 观测到的版本缺陷：探针不过、启动卡死、支撑清单 apply 失败、镜像拉不下来。
    /// 该回滚，也该占一次尝试——同一处反复失败就该停下来等人。
    #[default]
    Version,
    /// 集群环境问题：调度器放不下新 Pod，或我们看不到集群。两种都说不出新版本
    /// 的好坏，Job 不回滚，部署器也不该把它记成"这个版本试过一次"——否则一次
    /// 纯容量问题就会按尝试上限把一个本来正常的版本搁置，而要等下一版才解封。
    Environment,
}

impl FailureClass {
    fn as_str(self) -> &'static str {
        match self {
            FailureClass::Version => "version",
            FailureClass::Environment => "environment",
        }
    }
}

/// 滚动失败连同它的类别一起带出 `RolloutExecutor::run`。判定进程与部署器在
/// 同一个 crate，类别走类型而不是让部署器回头解析 Job 日志里的字符串。
#[derive(Debug)]
pub struct RolloutFailure {
    /// 这次失败属于哪一类。部署器据此决定要不要计入本 rev 的尝试预算。
    pub class: FailureClass,
    /// 原始错误，措辞与判据都不变（Job 日志、错误文本原样保留）。
    pub error: SFError,
}

impl RolloutFailure {
    fn version(error: SFError) -> Self {
        Self {
            class: FailureClass::Version,
            error,
        }
    }

    fn environment(error: SFError) -> Self {
        Self {
            class: FailureClass::Environment,
            error,
        }
    }
}

impl std::fmt::Display for RolloutFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for RolloutFailure {}

/// 把部署 `.spec` 归一化成可逐字节比较的「放置面」。
///
/// 只抹掉两处不影响放置的易变内容：容器镜像（本次上线改的正是它，留着就每次都算
/// "改过"）与 Pod 模板的 annotations（重启戳、配置校验和，纯噪声）。其余全部留下，
/// 包括 requests / limits、nodeSelector、affinity、tolerations、init 容器、侧车、
/// replicas 与滚动策略的 maxSurge——它们都能让新 Pod 排不进去。
///
/// 解析不了就原样返回，调用方按"与快照不同"处理。
fn normalize_placement_shape(raw: &str) -> String {
    let trimmed = raw.trim();
    let Ok(mut v) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return trimmed.to_string();
    };
    if let Some(m) = v
        .pointer_mut("/template/metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        m.remove("annotations");
    }
    for path in [
        "/template/spec/containers",
        "/template/spec/initContainers",
        "/template/spec/ephemeralContainers",
    ] {
        if let Some(list) = v
            .pointer_mut(path)
            .and_then(serde_json::Value::as_array_mut)
        {
            for c in list.iter_mut() {
                if let Some(o) = c.as_object_mut() {
                    o.remove("image");
                }
            }
        }
    }
    v.to_string()
}

/// 这条超时算不算"集群放不下"（环境类）；是就给出排不上队的 Pod 与调度器消息。
///
/// 环境类只在两个条件同时成立时给出：调度器确实判了排不上队，且部署的放置面
/// 与滚动前逐字节一致——放置面没变，说明不是这次上线把 Pod 顶出节点的。
/// 任一侧读空、或读不到当前放置面（`None`）一律不给环境结论：判不准就往版本侧
/// 靠，宁可多回滚一次，不放走"新版本自己加了排不上的约束"。
fn environment_class(
    blocked: &[(String, String)],
    prev_shape: &str,
    now_shape: Option<&str>,
) -> Option<String> {
    if blocked.is_empty() || prev_shape.is_empty() || now_shape != Some(prev_shape) {
        return None;
    }
    Some(
        blocked
            .iter()
            .map(|(n, m)| {
                if m.is_empty() {
                    n.clone()
                } else {
                    format!("{n}: {m}")
                }
            })
            .collect::<Vec<_>>()
            .join("; "),
    )
}

/// 从 Pod 采样行里挑出被调度器判为排不上队的 Pod，连同它的消息。
///
/// 每行形如 `name|phase|scheduledStatus|reason|message`。只认
/// `PodScheduled=False` 且 `reason=Unschedulable` 的组合：这是调度器给出的
/// 结论；Pod 还 Pending 但未被判决（刚创建、条件未上报）不算，字段缺失的
/// 半行整条丢弃。判据落在 reason 字段上，不靠字段个数：调度器没给 message 时
/// 那一格是空的，按个数卡会把一条真的排不上队丢掉，反而误判成版本缺陷。
fn unschedulable_pods(out: &str) -> Vec<(String, String)> {
    let mut v = Vec::new();
    for line in out.lines() {
        let f = sample_fields(line);
        if f.len() < 4 || f[0].is_empty() {
            continue;
        }
        if f[2] == "False" && f[3] == "Unschedulable" {
            v.push((
                f[0].to_string(),
                f.get(4).copied().unwrap_or("").to_string(),
            ));
        }
    }
    v
}

/// 一条准入被拒的 ReplicaSet 现场。
///
/// ReplicaSet 是唯一知道「Pod 压根没被建出来」的对象：配额超限、校验失败这类
/// 准入拒绝发生在 API 层，失败落在 RS 的 `ReplicaFailure` 条件上，而**没有 Pod
/// 对象**——只看 Pod 的采样一条都取不到，等回到集群时那条 `FailedCreate` 事件
/// 也早过了一小时窗口。
struct ReplicaSetFailure {
    name: String,
    reason: String,
    message: String,
}

impl ReplicaSetFailure {
    /// 条件里没给的字段留空，拼接时不留空段——`FailedCreate: ` 后面跟空串比只
    /// 写 reason 更难读。
    fn detail(&self) -> String {
        match (self.reason.is_empty(), self.message.is_empty()) {
            (false, false) => format!("{}: {}", self.reason, self.message),
            (false, true) => self.reason.clone(),
            (true, false) => self.message.clone(),
            (true, true) => String::new(),
        }
    }
}

/// 从 ReplicaSet 采样行里挑出**本次滚动**里准入被拒的副本集。
///
/// 每行形如 `name|specReplicas|failureStatus|reason|message`，查询已按条件类型
/// 过滤到 `ReplicaFailure`。两个判据缺一不可：
///
/// - `failureStatus == True`：条件以 `False` 留存说的是那次失败已经过去。RS 是
///   长期对象，一条历史失败会一直挂在它上面，把它记成本次病因就是拿旧账当新病情。
/// - `specReplicas != 0`：同一个部署的历代 RS 都带这套标签，选择器会全捞回来，
///   而只有期望副本数不为零的那个才是这次上线要的。读不出副本数的残行同样不算。
///
/// 第二条判据**依赖当前四个部署的滚动策略都是「先缩旧再建新」**（`maxSurge=0/
/// maxUnavailable=1` 或 `Recreate`），旧 RS 在超时时已被缩到 0，所以非零的只剩
/// 本次那个。若将来哪个目标改成 `maxSurge>0`，新旧 RS 会同时非零，这里会把上一
/// 代的历史失败一起记成病因——届时判据要再加上「RS 的 Pod 模板与目标镜像同源」。
fn replicaset_failures(out: &str) -> Vec<ReplicaSetFailure> {
    let mut v = Vec::new();
    for line in out.lines() {
        let f = sample_fields(line);
        if f.len() < 4 || f[0].is_empty() {
            continue;
        }
        let desired: u32 = match f[1].parse() {
            Ok(n) => n,
            Err(_) => continue,
        };
        if desired == 0 || f[2] != "True" {
            continue;
        }
        v.push(ReplicaSetFailure {
            name: f[0].to_string(),
            reason: f[3].to_string(),
            message: f.get(4).copied().unwrap_or("").to_string(),
        });
    }
    v
}

/// 追加一段现场到诊断串：段间用 `; ` 分隔，空段不占位。空字段是「没有该信号」
/// 而不是「信号为空」，留一个空段只会把真正的病因埋进一串分隔符里。
fn push_diagnosis_segment(out: &mut String, label: &str, body: &str) {
    if body.is_empty() {
        return;
    }
    if !out.is_empty() {
        out.push_str("; ");
    }
    out.push_str(label);
    out.push_str(body);
}

/// deployment 是否已滚完：observedGeneration 追上 generation，且
/// updated/ready 副本数达期望。解析不出（滚动交替期的空输出、omitempty
/// 字段缺失）一律当未收敛，由外层按预算继续轮询。纯函数：只看 kubectl
/// 的输出文本。
fn rollout_converged(out: &str) -> bool {
    let parts: Vec<&str> = out.split('|').collect();
    if parts.len() < 5 {
        return false;
    }
    let gen: u64 = parts[0].parse().unwrap_or(0);
    let obs: u64 = parts[1].parse().unwrap_or(0);
    let spec: u32 = parts[2].parse().unwrap_or(0);
    let updated: u32 = parts[3].parse().unwrap_or(0);
    let ready: u32 = parts[4].parse().unwrap_or(0);
    // unavailable 必须为 0：RollingUpdate 新旧副本并存时，旧副本仍 ready 会让
    // ready==spec 提前成立，但新崩溃副本计入 unavailable，不能判完成。
    let unavailable: u32 = parts.get(5).and_then(|v| v.parse().ok()).unwrap_or(0);
    obs >= gen && gen > 0 && updated == spec && ready == spec && unavailable == 0 && spec > 0
}

/// Pod 双标签选择器：主应用/网关/执行器的 name 标签都是 `cogneva`，
/// 单标签会跨部署误判（gitops puller 旧代码只用 name= 的同源缺陷）。
fn pod_selector(name: &str, component: &str) -> String {
    format!(
        "app.kubernetes.io/name={},app.kubernetes.io/component={}",
        name, component
    )
}

/// 四部署当前镜像的归类。
#[derive(Debug, Clone, PartialEq, Eq)]
enum DeployedState {
    /// 四部署统一跑在 `main-<rev>` 上。
    Main(String),
    /// 全都不是主线 tag（迁移前的 localhost/cogneva:local 时代）：允许首轮
    /// 以 registry :local 为基底前进。
    Legacy,
    /// 混合态（部分主线、部分旧 tag，或主线 rev 不一致）。有在飞滚动时表示
    /// 上一轮滚动未收敛，绝不触发新一轮；无在飞滚动时是外部写入造成的非一致，
    /// 由 [`normalize_deployed`] 归一后放行。
    Mixed,
}

fn classify_deployed(images: &[String]) -> DeployedState {
    let mut revs: Vec<String> = Vec::new();
    let mut non_main = 0usize;
    for img in images {
        match parse_main_rev(img) {
            Some(r) => revs.push(r.to_string()),
            None => non_main += 1,
        }
    }
    if revs.is_empty() {
        return DeployedState::Legacy;
    }
    if non_main > 0 {
        return DeployedState::Mixed;
    }
    let first = revs[0].clone();
    if revs.iter().all(|r| r == &first) {
        DeployedState::Main(first)
    } else {
        DeployedState::Mixed
    }
}

/// 归一混合态与"在飞滚动"的关系。
///
/// 混合态的原义是"上一轮的滚动还没收敛"——那只在自己真有滚动在飞时成立。
/// 没有在飞滚动却出现混合态，只可能是外部写入造成的（清单被重下发、部分 apply、
/// GitOps 金丝雀、手工 `set image`），此时按 Mixed 一直拒会让部署器**永久静默停摆**
/// （只有一行 INFO，没有任何自愈路径）。这种混合态按 Legacy 放行，本轮滚动把四部署
/// 重新 pin 回同一个 rev 即自愈。
///
/// 有在飞滚动时保持 Mixed 原义：绝不叠加新一轮。
fn normalize_deployed(deployed: &DeployedState, has_in_flight: bool) -> DeployedState {
    match deployed {
        DeployedState::Mixed if !has_in_flight => DeployedState::Legacy,
        other => other.clone(),
    }
}

/// 前进判定（纯函数）。`is_ancestor` = deployed rev 是 bare rev 的祖先
/// （git merge-base 判定结果，作为参数传入保持本函数无 IO）。
#[derive(Debug, PartialEq, Eq)]
enum AdvanceDecision {
    Advance,
    SameRev,
    NotAncestor,
    Mixed,
    InCooldown,
    MaxAttempts,
}

/// 上一条失败 rev 的记账。冷却、次数、上限回答的是同一个问题——"这个 rev 现在
/// 还能不能再试"——所以合成一格，免得三个数各走各的。
#[derive(Debug, Clone, Copy)]
struct RetryBudget {
    /// 冷却窗截止时刻（unix 秒）。
    cooldown_until: i64,
    /// 当前 bare rev 就是刚失败的那个（状态里的 `failed_rev` 命中了它）。
    retry_of_failed_rev: bool,
    /// 已记的**版本类**失败次数；环境类失败不占这个预算。
    attempts: u32,
    max_attempts: u32,
}

fn evaluate_advance(
    bare_rev: &str,
    deployed: &DeployedState,
    is_ancestor: bool,
    now_ts: i64,
    retry: RetryBudget,
) -> AdvanceDecision {
    match deployed {
        DeployedState::Legacy => AdvanceDecision::Advance,
        DeployedState::Mixed => AdvanceDecision::Mixed,
        DeployedState::Main(d) => {
            if rev12(d) == rev12(bare_rev) {
                return AdvanceDecision::SameRev;
            }
            if !is_ancestor {
                return AdvanceDecision::NotAncestor;
            }
            // 冷却挡的是"重试刚失败的这个 rev"，由 failed_rev 判，不能借 attempts:
            // 环境类失败不占尝试预算（attempts 保持 0），而它恰恰是最该被冷却挡住、
            // 等节点腾出空间的那一档。冷却若连新 rev 一起挡，fix-forward 提交
            // （修的正是上次失败原因）会被无谓延迟一个冷却窗。
            if retry.retry_of_failed_rev && now_ts < retry.cooldown_until {
                return AdvanceDecision::InCooldown;
            }
            if retry.attempts >= retry.max_attempts {
                return AdvanceDecision::MaxAttempts;
            }
            AdvanceDecision::Advance
        }
    }
}

/// 构建锁陈旧判定：持锁进程已死，或锁龄超过构建超时（进程僵死/被杀）。
fn lock_is_stale(age_secs: u64, timeout_secs: u64, pid_alive: bool) -> bool {
    !pid_alive || age_secs > timeout_secs
}

// ---------------------------------------------------------------------------
// 持久状态（state.json，tmp+rename 原子写；权威事实是集群，PVC 丢了能重建）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum Phase {
    SourceReady,
    Built,
    Pushed,
    Dispatched,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InFlight {
    rev: String,
    phase: Phase,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct MainlineState {
    last_good_tag: Option<String>,
    last_good_rev: Option<String>,
    in_flight: Option<InFlight>,
    failed_rev: Option<String>,
    failed_cooldown_until: i64,
    /// **版本类**失败次数；环境类失败不占这个预算（见 [`FailureClass`]）。
    failed_attempts: u32,
    /// `failed_rev` 那次失败的类别。决定重试时能否复用镜像，old state.json
    /// 没有这一格，缺省按版本类（保守：宁可重建）。
    #[serde(default)]
    failed_class: FailureClass,
}

// ---------------------------------------------------------------------------
// 构建侧
// ---------------------------------------------------------------------------

pub struct MainlineDeployer {
    cfg: MainlineDeployerConfig,
    /// 部署器独占一棵稳定路径的工作树。与进化任务的工作树互不干涉：这里是
    /// 唯一能自由 `reset --hard` 的检出，任何第三方检出停在哪里都不影响它。
    workspaces: std::sync::Arc<crate::workspace::WorkspaceManager>,
}

impl MainlineDeployer {
    pub fn new(
        cfg: MainlineDeployerConfig,
        workspaces: std::sync::Arc<crate::workspace::WorkspaceManager>,
    ) -> Self {
        Self { cfg, workspaces }
    }

    /// 部署器工作树路径（稳定）。
    pub fn workdir(&self) -> PathBuf {
        self.workspaces.deployer_workspace()
    }

    /// 外部共享的 CARGO_TARGET_DIR：工作树可整棵重建而不丢增量编译缓存。
    pub fn target_dir(&self) -> PathBuf {
        self.workspaces.target_dir().to_path_buf()
    }

    fn state_path(&self) -> PathBuf {
        Path::new(&self.cfg.state_dir).join("state.json")
    }

    fn lock_path(&self) -> PathBuf {
        Path::new(&self.cfg.state_dir).join("build.lock")
    }

    fn load_state(&self) -> MainlineState {
        match std::fs::read_to_string(self.state_path()) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                warn!(error = %e, "mainline state.json corrupt; starting fresh");
                MainlineState::default()
            }),
            Err(_) => MainlineState::default(),
        }
    }

    /// 打一条 INFO 心跳：bare HEAD + 持久化状态摘要。bare 读取失败只降级
    /// 成占位文本，不影响主循环——心跳本身绝不能成为故障源。
    async fn log_heartbeat(&self) {
        let bare = self
            .bare_main_rev()
            .await
            .unwrap_or_else(|e| format!("unreadable({e})"));
        let summary = heartbeat_message(&self.load_state(), &bare, chrono::Utc::now().timestamp());
        info!(heartbeat = %summary, "mainline deployer heartbeat");
    }

    fn save_state(&self, state: &MainlineState) -> SFResult<()> {
        std::fs::create_dir_all(&self.cfg.state_dir)
            .map_err(|e| SFError::IO(format!("create state dir {}: {e}", self.cfg.state_dir)))?;
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(state)?;
        std::fs::write(&tmp, text).map_err(|e| SFError::IO(format!("write state tmp: {e}")))?;
        std::fs::rename(&tmp, &path).map_err(|e| SFError::IO(format!("rename state: {e}")))?;
        Ok(())
    }

    /// 构建串行锁：4C/7.5G 节点禁并发构建。锁陈旧（进程死/超构建超时）
    /// 可抢占；返回 None 表示有其他构建在跑。
    fn acquire_lock(&self) -> Option<BuildLock> {
        let path = self.lock_path();
        if let Ok(text) = std::fs::read_to_string(&path) {
            let pid: u32 = text
                .lines()
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            let age = std::fs::metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let pid_alive = pid != 0 && Path::new(&format!("/proc/{pid}")).exists();
            if !lock_is_stale(age, self.cfg.build_timeout_secs, pid_alive) {
                info!(
                    pid,
                    age_secs = age,
                    "mainline build lock held by another process"
                );
                return None;
            }
            warn!(pid, age_secs = age, "mainline build lock stale; preempting");
            let _ = std::fs::remove_file(&path);
        }
        let _ = std::fs::create_dir_all(&self.cfg.state_dir);
        std::fs::write(&path, format!("{}\n", std::process::id())).ok()?;
        Some(BuildLock { path })
    }

    async fn run_cmd(
        &self,
        program: &str,
        args: &[&str],
        workdir: Option<&Path>,
        timeout_secs: u64,
    ) -> SFResult<String> {
        let cmdline = format!("{} {}", program, args.join(" "));
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args).kill_on_drop(true);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let fut = cmd.output();
        let output = tokio::time::timeout(Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| SFError::IO(format!("failed to run {program}: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(SFError::IO(format!(
                "{cmdline} failed: {}{}",
                stderr, stdout
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// buildah 子命令统一加 PVC 存储全局参数（全局参数必须在子命令前）。
    async fn buildah(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["--root", BUILDAH_STORAGE, "--runroot", BUILDAH_RUNROOT];
        full.extend_from_slice(args);
        self.run_cmd(&self.cfg.builder_bin, &full, None, timeout_secs)
            .await
    }

    async fn kubectl(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["-n", &self.cfg.namespace];
        full.extend_from_slice(args);
        self.run_cmd(&self.cfg.kubectl_bin, &full, None, timeout_secs)
            .await
    }

    async fn git_src(&self, args: &[&str]) -> SFResult<String> {
        let workdir = self.workdir();
        self.run_cmd("git", args, Some(&workdir), 120).await
    }

    /// bare 仓库指定分支的完整 rev。
    async fn bare_main_rev(&self) -> SFResult<String> {
        let out = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "rev-parse",
                    &self.cfg.branch,
                ],
                None,
                30,
            )
            .await?;
        Ok(out.trim().to_string())
    }

    /// older 是否 newer 的祖先（拒倒退/分叉）。
    async fn is_ancestor(&self, older: &str, newer: &str) -> bool {
        tokio::process::Command::new("git")
            .args([
                "--git-dir",
                &self.cfg.bare_repo,
                "merge-base",
                "--is-ancestor",
                older,
                newer,
            ])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// buildah 在 Pod 内 push/from 的端点（集群 DNS，http）。
    fn push_endpoint(&self) -> String {
        if self.cfg.registry.trim().is_empty() {
            format!(
                "cogneva-registry.{}.svc.cluster.local:5000",
                self.cfg.namespace
            )
        } else {
            self.cfg.registry.trim_end_matches('/').to_string()
        }
    }

    /// kubelet 在节点上 pull 的镜像引用端点（NodePort localhost；节点不解析
    /// 集群 DNS）。Job manifest 镜像与 set image 引用必须用这个端点。
    fn pull_endpoint(&self) -> String {
        if self.cfg.local_registry.trim().is_empty() {
            "localhost:30500".to_string()
        } else {
            self.cfg.local_registry.trim_end_matches('/').to_string()
        }
    }

    /// 四个 deployment 当前在跑的镜像（按 cfg.targets 顺序）。
    async fn deployed_images(&self) -> SFResult<Vec<String>> {
        let mut images = Vec::new();
        for t in &self.cfg.targets {
            let jsonpath = format!(
                "jsonpath={{.spec.template.spec.containers[?(@.name==\"{}\")].image}}",
                t.container
            );
            let img = self
                .kubectl(&["get", "deployment", &t.deployment, "-o", &jsonpath], 30)
                .await?;
            images.push(img.trim().to_string());
        }
        Ok(images)
    }

    /// 集群内 registry 的最小只读客户端：明文 HTTP、同命名空间 DNS、
    /// 无凭证（insecure registry，buildah 走的就是这条通道）。
    async fn registry_get(&self, path: &str, accept: &[&str]) -> SFResult<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let endpoint = self.push_endpoint();
        let (host, port) = endpoint_host_port(&endpoint).ok_or_else(|| {
            SFError::Agent(format!("registry endpoint {endpoint:?} is not host:port"))
        })?;
        let accepted = if accept.is_empty() {
            String::new()
        } else {
            format!("Accept: {}\r\n", accept.join(", "))
        };
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n{accepted}Connection: close\r\n\r\n");
        let mut stream = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::net::TcpStream::connect((host, port)),
        )
        .await
        .map_err(|_| SFError::IO(format!("registry {endpoint} connect timed out")))?
        .map_err(|e| SFError::IO(format!("registry {endpoint} connect failed: {e}")))?;
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("registry request write failed: {e}")))?;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut raw))
            .await
            .map_err(|_| SFError::IO("registry read timed out".into()))?
            .map_err(|e| SFError::IO(format!("registry read failed: {e}")))?;
        let (status, body) = parse_http_response(&raw)
            .ok_or_else(|| SFError::IO("registry returned a malformed HTTP response".into()))?;
        if status != 200 {
            return Err(SFError::IO(format!("registry GET {path} -> {status}")));
        }
        Ok(body)
    }

    /// registry 上某 tag 当前内容构建自哪个 rev：manifest → config blob →
    /// `org.opencontainers.image.revision` 标签。多平台 index 多一跳，先下
    /// 第一个子 manifest 取它的 config digest（各平台同 rev，标签一致）。
    async fn registry_tag_revision(&self, tag: &str) -> SFResult<Option<String>> {
        const MANIFEST_ACCEPT: &[&str] = &[
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        ];
        let manifest = self.registry_manifest(tag, MANIFEST_ACCEPT).await?;
        let config_digest = match config_digest_of(&manifest) {
            Some(d) => d,
            None => {
                let Some(child) = first_manifest_digest(&manifest) else {
                    return Ok(None);
                };
                let child_manifest = self.registry_manifest(&child, MANIFEST_ACCEPT).await?;
                let Some(d) = config_digest_of(&child_manifest) else {
                    return Ok(None);
                };
                d
            }
        };
        let blob: serde_json::Value = serde_json::from_slice(
            &self
                .registry_get(&format!("/v2/cogneva/blobs/{config_digest}"), &[])
                .await?,
        )
        .map_err(|e| {
            SFError::IO(format!(
                "registry config blob {config_digest} is not JSON: {e}"
            ))
        })?;
        Ok(revision_of_config_blob(&blob))
    }

    /// 取一个 manifest（tag 或 digest 引用皆可）。
    async fn registry_manifest(
        &self,
        reference: &str,
        accept: &[&str],
    ) -> SFResult<serde_json::Value> {
        serde_json::from_slice(
            &self
                .registry_get(&format!("/v2/cogneva/manifests/{reference}"), accept)
                .await?,
        )
        .map_err(|e| SFError::IO(format!("registry manifest {reference} is not JSON: {e}")))
    }

    /// registry 上某 tag 是否存在（manifest 可取）。用于"不重建也能修"的
    /// 快路：不可变 tag 内容与 rev 一一对应，存在即可直接滚动。
    async fn registry_tag_exists(&self, tag: &str) -> bool {
        self.registry_get(
            &format!("/v2/cogneva/manifests/{tag}"),
            &["application/vnd.docker.distribution.manifest.v2+json"],
        )
        .await
        .is_ok()
    }

    /// 四部署当前声明的镜像对应哪个 rev。不可变 `main-<rev>` 由 tag 直接
    /// 给出（tag 与内容一一对应）；浮动签必须问 registry 当前内容构建自
    /// 哪个 rev——tag 字符串本身不含 rev，而内容随时可能被重新播种。四部署
    /// 不一致或查不到一律 None（未知绝不当作已收敛）。
    async fn declared_image_rev(&self, images: &[String]) -> Option<String> {
        let first = images.first()?;
        if !images.iter().all(|i| i == first) {
            return None;
        }
        let tag = first.rsplit(':').next()?;
        if tag_is_immutable_for_rev(tag) {
            return parse_main_rev(first).map(|s| s.to_string());
        }
        self.registry_tag_revision(tag).await.ok().flatten()
    }

    /// 叠层基底（buildah from，Pod 内走 push 端点）：已在主线 tag 上则用
    /// 不可变 main-<prev>；迁移首轮（Legacy）用 registry :local
    /// （swap-image/bootstrap 已播种）。绝不基于节点 localhost/cogneva:local
    /// ——进化 Pod 没有节点 containerd socket，且浮动签脱节会自我放大旧镜像。
    fn resolve_base(&self, deployed: &DeployedState) -> String {
        let endpoint = self.push_endpoint();
        match deployed {
            DeployedState::Main(rev) => main_image(&endpoint, rev),
            _ => local_image(&endpoint),
        }
    }

    /// 一轮轮询。
    pub async fn poll_once(&self) -> SFResult<()> {
        let mut state = self.load_state();

        let bare = self.bare_main_rev().await?;
        let images = self.deployed_images().await?;
        let deployed = classify_deployed(&images);

        // 在飞任务收敛/终态处理。
        if let Some(inflight) = state.in_flight.clone() {
            match &deployed {
                DeployedState::Main(d) if rev12(d) == rev12(&inflight.rev) => {
                    info!(rev = %rev12(&inflight.rev), "mainline rollout converged");
                    // 浮动签只在收敛后前移，失败回滚的坏镜像绝不进 :local。
                    self.promote_local_tag(&inflight.rev).await?;
                    state.last_good_rev = Some(inflight.rev.clone());
                    state.last_good_tag = Some(main_image(&self.pull_endpoint(), &inflight.rev));
                    state.in_flight = None;
                    state.failed_rev = None;
                    state.failed_attempts = 0;
                    state.failed_class = FailureClass::default();
                    state.failed_cooldown_until = 0;
                    self.save_state(&state)?;
                    return Ok(());
                }
                _ => {}
            }
            if inflight.phase == Phase::Dispatched {
                match self.job_status(&job_name(&inflight.rev)).await? {
                    JobStatus::Complete => {
                        // Job 成功退出意味着滚动要么收敛、要么已回滚（回滚是非零
                        // 退出，记 Failed）。这里镜像仍不是目标 tag，只可能是
                        // Job 跑完后被外部 apply/GitOps 打回：kubectl apply 同名
                        // Job 是 no-op 不会重跑，必须删掉重新派发，否则永久卡
                        // "等待收敛"。镜像已是目标 tag 则只是收敛尾巴，下轮再判。
                        let target_tag = main_image(&self.pull_endpoint(), &inflight.rev);
                        let all_on_target = self
                            .deployed_images()
                            .await?
                            .iter()
                            .all(|i| i == &target_tag);
                        if all_on_target {
                            info!(rev = %rev12(&inflight.rev), "rollout job complete; awaiting deployment convergence");
                            return Ok(());
                        }
                        warn!(rev = %rev12(&inflight.rev), "rollout job complete but deployments not on target tag (reverted by an apply?); redispatching");
                        self.dispatch_job(&inflight.rev, &target_tag).await?;
                        return Ok(());
                    }
                    JobStatus::Failed => {
                        // 类别取自 Job 的终止码（环境类不回滚也不计尝试），不是
                        // 从 Job 日志里找字符串。
                        let class = self.job_failure_class(&job_name(&inflight.rev)).await;
                        let same_rev = state.failed_rev.as_deref() == Some(inflight.rev.as_str());
                        let attempts = match (class, same_rev) {
                            // 环境类失败不是版本结论：不占尝试预算，只设冷却，
                            // 等节点腾出空间后在冷却窗后再来。
                            (FailureClass::Environment, true) => state.failed_attempts,
                            (FailureClass::Environment, false) => 0,
                            (FailureClass::Version, true) => state.failed_attempts + 1,
                            (FailureClass::Version, false) => 1,
                        };
                        warn!(
                            rev = %rev12(&inflight.rev),
                            class = class.as_str(),
                            attempts,
                            "mainline rollout job failed (rollback, if any, handled by the job itself)"
                        );
                        state.failed_rev = Some(inflight.rev.clone());
                        state.failed_class = class;
                        state.failed_attempts = attempts;
                        state.failed_cooldown_until =
                            chrono::Utc::now().timestamp() + self.cfg.failure_cooldown_secs as i64;
                        state.in_flight = None;
                        self.save_state(&state)?;
                        return Ok(());
                    }
                    JobStatus::Running | JobStatus::NotFound => return Ok(()),
                }
            }
            // phase < Dispatched：上轮在构建中途重启，落到下方构建流程
            // 幂等重跑（同 tag buildah/push 可重复）。
        }

        // 静态清单/GitOps apply 把四部署 pin 到 registry 浮动签 :local（见
        // chart/k3s 清单）；:local 只在收敛后前移，所以 pin 命中 last_good 且
        // bare 未再前进时就是"以浮动签形态收敛"，不重建重派。
        let local_pin = local_image(&self.pull_endpoint());
        let on_local_pin = !images.is_empty() && images.iter().all(|i| i == &local_pin);
        if state.last_good_rev.as_deref() == Some(bare.as_str()) && on_local_pin {
            // 声明态只是"清单里写的是 :local"。浮动签的内容可以被重新播种
            // （bootstrap/swap-image 从本机镜像重推），此时节点会随清单滚动
            // 落到旧二进制，而 tag 字符串一个字都没变——只看清单就会把"退回
            // 旧版"判成"无事可做"。必须问 registry 当前内容构建自哪个 rev。
            let running_rev = self.declared_image_rev(&images).await;
            if floating_pin_is_converged(running_rev.as_deref(), &bare) {
                info!(rev = %rev12(&bare), "deployments pinned to floating :local carrying the current mainline; nothing to do");
                return Ok(());
            }
            warn!(
                rev = %rev12(&bare),
                running_rev = ?running_rev,
                "floating :local no longer carries the current mainline (re-seeded?); re-pinning"
            );
            // 落到下方构建流程：reset 到 bare、复用已有不可变镜像或重建，
            // 再派 Job 把四部署 pin 回 `main-<rev>`。
        }

        let normalized = normalize_deployed(&deployed, state.in_flight.is_some());
        if normalized != deployed {
            warn!(
                images = ?images,
                "deployments sit on mixed images with no rollout in flight (an external partial apply?); converging them onto one revision"
            );
        }
        let deployed = normalized;

        let retry_of_failed_rev = state.failed_rev.as_deref() == Some(bare.as_str());
        let attempts = if retry_of_failed_rev {
            state.failed_attempts
        } else {
            0
        };
        let is_ancestor = match &deployed {
            DeployedState::Main(d) => self.is_ancestor(d, &bare).await,
            _ => true,
        };
        let now = chrono::Utc::now().timestamp();
        let decision = evaluate_advance(
            &bare,
            &deployed,
            is_ancestor,
            now,
            RetryBudget {
                cooldown_until: state.failed_cooldown_until,
                retry_of_failed_rev,
                attempts,
                max_attempts: self.cfg.max_attempts_per_rev,
            },
        );
        match decision {
            AdvanceDecision::Advance => {}
            AdvanceDecision::SameRev => return Ok(()),
            other => {
                info!(decision = ?other, "mainline advance skipped");
                return Ok(());
            }
        }

        let _lock = match self.acquire_lock() {
            Some(lock) => lock,
            None => return Ok(()),
        };

        // 同一镜像两个引用端点：buildah 在 Pod 内走集群 DNS push/from；
        // Job manifest 与 set image 走节点 NodePort（kubelet 不解析集群 DNS）。
        let push_tag = main_image(&self.push_endpoint(), &bare);
        let pull_tag = main_image(&self.pull_endpoint(), &bare);
        let base_tag = self.resolve_base(&deployed);

        // 不可变 tag 与 rev 一一对应：registry 上已有 `main-<rev>` 就说明
        // 该 rev 早已构建过（清单被重下发打回浮动签后，四部署只是需要重新
        // pin 回去），直接派 Job 滚动即可，4C 机器上省掉一次全程构建。
        // 该 rev 此前**版本类**失败过就老老实实重建——半推成功留下的坏 tag
        // 不能靠复用来"修复"，否则会在同一处反复失败。环境类失败不在此列：
        // 那一支的构建与推送都成功了（卡住的是调度），重试复用同一个 tag 是
        // 对的，也免得在本来就排不进 Pod 的节点上再跑一遍全程构建。
        if (!retry_of_failed_rev || state.failed_class == FailureClass::Environment)
            && self.registry_tag_exists(&push_tag).await
        {
            info!(rev = %rev12(&bare), tag = %push_tag, "immutable image already in registry; re-pinning without a rebuild");
            state.in_flight = Some(InFlight {
                rev: bare.clone(),
                phase: Phase::Pushed,
            });
            self.save_state(&state)?;
            self.dispatch_job(&bare, &pull_tag).await?;
            state.in_flight = Some(InFlight {
                rev: bare.clone(),
                phase: Phase::Dispatched,
            });
            self.save_state(&state)?;
            return Ok(());
        }

        info!(rev = %rev12(&bare), base = %base_tag, "mainline advance: building");

        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::SourceReady,
        });
        self.save_state(&state)?;

        // 1. 把独占工作树对齐到新 rev。树是部署器自己的，可无条件 reset。
        self.ensure_source_at(&bare).await?;

        // 2. cargo build --release（target/ 在 source PVC 上增量缓存）。
        self.build_binary(&bare).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Built,
        });
        self.save_state(&state)?;

        // 3. buildah 叠层并推 registry（只推不可变 tag；:local 收敛后前移）。
        self.build_and_push(&bare, &base_tag, &push_tag).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Pushed,
        });
        self.save_state(&state)?;

        // 4. 派滚动 Job（explosion radius 外；新镜像 smoke test）。Job 启动后
        // 自己快照各部署当前镜像作为回滚目标，无需部署器推导 prev。
        self.dispatch_job(&bare, &pull_tag).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Dispatched,
        });
        self.save_state(&state)?;
        info!(rev = %rev12(&bare), tag = %pull_tag, "mainline rollout job dispatched");
        Ok(())
    }

    /// 把部署器独占的工作树对齐到目标 rev。这里没有守卫也没有"忙则跳过"：
    /// 树是自己的，不存在需要保护的在途工作，损坏就重建。
    async fn ensure_source_at(&self, rev: &str) -> SFResult<()> {
        let spec = crate::workspace::WorkspaceSpec::persistent(
            "mainline",
            crate::workspace::WorkspaceKind::Deployer,
            crate::workspace::BaseRef::Commit(rev.to_string()),
        );
        self.workspaces.ensure_persistent(spec).await?;
        self.git_src(&["reset", "--hard", rev]).await?;
        // target 目录在工作树之外，clean 只清源码，不丢增量编译缓存。
        self.git_src(&["clean", "-ffdx"]).await?;
        Ok(())
    }

    async fn build_binary(&self, rev: &str) -> SFResult<()> {
        let jobs = self.cfg.cargo_build_jobs.to_string();
        let cmdline = format!("cargo build --release --bin cogneva (jobs={jobs})");
        // CARGO_HOME 换 PVC 后，镜像 /usr/local/cargo/config.toml 里的 sparse
        // 镜像配置（受限网络构建注入）不会自动继承；缺失会直连 crates.io，
        // 家庭网络上索引拉取极慢。一次性把镜像内配置带到 PVC。
        let pvc_config = std::path::Path::new(CARGO_HOME_PVC).join("config.toml");
        if !tokio::fs::try_exists(&pvc_config).await.unwrap_or(false) {
            let img_config = std::path::Path::new("/usr/local/cargo/config.toml");
            if tokio::fs::try_exists(img_config).await.unwrap_or(false) {
                if let Ok(body) = tokio::fs::read(img_config).await {
                    let _ = tokio::fs::create_dir_all(CARGO_HOME_PVC).await;
                    let _ = tokio::fs::write(&pvc_config, body).await;
                }
            }
        }
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(["build", "--release", "--bin", "cogneva"])
            .current_dir(self.workdir())
            .env("CARGO_BUILD_JOBS", &jobs)
            .env("CARGO_HOME", CARGO_HOME_PVC)
            // 工作树只放源码；产物落在共享 target，工作树重建也不用冷编译。
            .env("CARGO_TARGET_DIR", self.target_dir())
            // build.rs 回退只嵌 7 位短 sha，叠层后的 --version 校验匹配 12
            // 位前缀会必败；显式注入完整 rev（与 swap-image 双保险同源）。
            .env("COGNEVA_GIT_REVISION", rev)
            .kill_on_drop(true);
        let fut = cmd.output();
        let output = tokio::time::timeout(Duration::from_secs(self.cfg.build_timeout_secs), fut)
            .await
            .map_err(|_| {
                SFError::IO(format!(
                    "{cmdline} timed out after {}s",
                    self.cfg.build_timeout_secs
                ))
            })?
            .map_err(|e| SFError::IO(format!("failed to run cargo: {e}")))?;
        if !output.status.success() {
            return Err(SFError::Agent(format!(
                "mainline cargo build failed:\n{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        // strip 失败不致命（二进制可跑，只是体积大）。
        let bin = self.target_dir().join("release/cogneva");
        let _ = self
            .run_cmd("strip", &[bin.to_str().unwrap_or("")], None, 60)
            .await;
        Ok(())
    }

    /// buildah 叠层：FROM 当前在跑 tag → 换二进制 + migrations → --version
    /// 校验内嵌 rev → commit 不可变 tag → 只推不可变 tag（:local 收敛后推）。
    async fn build_and_push(&self, rev: &str, base: &str, new_tag: &str) -> SFResult<()> {
        // 集群内 registry 是纯 HTTP，from 拉基镜像默认试 HTTPS 会报
        // "http: server gave HTTP response to HTTPS client"，与 push 一样
        // 必须显式关 TLS 校验。
        let ctr = self
            .buildah(&["from", "--tls-verify=false", base], 1800)
            .await?;
        let ctr = ctr.trim().to_string();
        let result = self.buildah_steps(&ctr, rev, new_tag).await;
        if let Err(e) = self.buildah(&["rm", &ctr], 60).await {
            warn!(error = %e, "buildah rm failed after build");
        }
        result?;
        Ok(())
    }

    async fn buildah_steps(&self, ctr: &str, rev: &str, new_tag: &str) -> SFResult<()> {
        let bin = self.target_dir().join("release/cogneva");
        let migrations = self.workdir().join("crates/cog-storage/migrations");
        self.buildah(
            &["copy", ctr, bin.to_str().unwrap(), "/opt/cogneva/cogneva"],
            300,
        )
        .await?;
        self.buildah(
            &[
                "copy",
                ctr,
                migrations.to_str().unwrap(),
                "/opt/cogneva/crates/cog-storage/migrations",
            ],
            300,
        )
        .await?;

        // 换版即验证：新二进制必须能自报版本，且内嵌 rev 是目标 rev。
        let version = self
            .buildah(
                &["run", ctr, "--", "/opt/cogneva/cogneva", "--version"],
                120,
            )
            .await?;
        if !version.contains(rev12(rev)) {
            return Err(SFError::Agent(format!(
                "built binary --version {version:?} does not contain target rev {}",
                rev12(rev)
            )));
        }

        self.buildah(
            &[
                "config",
                "--label",
                &format!("org.opencontainers.image.revision={rev}"),
                ctr,
            ],
            60,
        )
        .await?;
        self.buildah(&["commit", ctr, new_tag], 600).await?;

        // 只推不可变 tag；浮动签 :local 在滚动收敛后由 promote_local_tag 前移，
        // 防止构建失败/回滚的坏镜像成为静态清单 apply 的回退锚点。
        self.buildah(&["push", "--tls-verify=false", new_tag], 900)
            .await?;
        info!(image = %new_tag, "mainline overlay image pushed to registry");
        Ok(())
    }

    /// 滚动收敛后把 registry 浮动签 `:local` 前移到指定 rev。buildah 镜像库
    /// 在 sandbox PVC 上，正常情况刚构建的不可变 tag 还在本地；Pod 重建后
    /// 本地丢失则先从 registry 拉回（同 registry 秒回）再打签推送。
    async fn promote_local_tag(&self, rev: &str) -> SFResult<()> {
        let immutable = main_image(&self.push_endpoint(), rev);
        let local = local_image(&self.push_endpoint());
        let present = self
            .buildah(&["images", "-q", &immutable], 30)
            .await?
            .trim()
            .to_string();
        if present.is_empty() {
            self.buildah(&["pull", "--tls-verify=false", &immutable], 900)
                .await?;
        }
        self.buildah(&["tag", &immutable, &local], 60).await?;
        self.buildah(&["push", "--tls-verify=false", &local], 900)
            .await?;
        info!(rev = %rev12(rev), tag = %local, "floating :local advanced to converged revision");
        Ok(())
    }

    /// 派滚动 Job。同名 Job 已在跑时视为已派发（幂等）；同名 Job 已结束
    /// （成功后镜像被 apply 打回、或失败冷却后重试）必须先删除再 apply——
    /// `kubectl apply` 是 upsert，不会重新执行已完成的 Job。new_tag 必须
    /// 是节点 pull 端点引用（kubelet 经 NodePort 拉取）。
    async fn dispatch_job(&self, rev: &str, new_tag: &str) -> SFResult<()> {
        match self.job_status(&job_name(rev)).await? {
            JobStatus::Running | JobStatus::NotFound => {}
            JobStatus::Complete | JobStatus::Failed => {
                info!(rev = %rev12(rev), "replacing finished rollout job before dispatch");
                self.kubectl(&["delete", "job", &job_name(rev), "--ignore-not-found"], 60)
                    .await?;
            }
        }
        let mut args: Vec<String> = vec![
            "mainline-rollout".into(),
            "--tag".into(),
            new_tag.to_string(),
            "--ns".into(),
            self.cfg.namespace.clone(),
            "--soak-secs".into(),
            self.cfg.soak_secs.to_string(),
            "--restart-threshold".into(),
            self.cfg.restart_threshold.to_string(),
            "--timeout".into(),
            self.cfg.rollout_timeout_secs.to_string(),
            "--startup-timeout".into(),
            self.cfg.startup_timeout_secs.to_string(),
        ];
        let mut volumes: Vec<serde_json::Value> = Vec::new();
        let mut mounts: Vec<serde_json::Value> = Vec::new();
        if self.cfg.deliver_manifests {
            // 组包失败直接报错：不派 Job，in_flight 停在 Pushed，下轮 poll
            // 重试——发布集坏（清单缺失/Secret 混入）时宁可不上线也不能
            // 静默回落纯 set image，那会让拓扑滞后悄悄回来。
            let bundle = self.build_bundle_at(rev, new_tag).await?;
            self.publish_manifests_configmap(rev, &bundle).await?;
            args.push("--manifests-dir".into());
            args.push("/manifests".into());
            volumes.push(serde_json::json!({
                "name": "manifests",
                "configMap": { "name": manifests_configmap_name(rev) }
            }));
            mounts.push(serde_json::json!({
                "name": "manifests",
                "mountPath": "/manifests",
                "readOnly": true
            }));
        }
        // Job Pod 也需要 kubectl：镜像不内置，挂载宿主 k3s 多调用二进制
        // （argv[0]=kubectl 即 kubectl）。配置为空（镜像自带/标准 K8s）时不挂。
        if !self.cfg.kubectl_host_path.is_empty() {
            volumes.push(serde_json::json!({
                "name": "kubectl-bin",
                "hostPath": { "path": &self.cfg.kubectl_host_path, "type": "File" }
            }));
            mounts.push(serde_json::json!({
                "name": "kubectl-bin",
                "mountPath": "/usr/local/bin/kubectl",
                "readOnly": true
            }));
        }
        let mut manifest = serde_json::json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name(rev),
                "namespace": self.cfg.namespace,
                "labels": {
                    "app.kubernetes.io/name": "cogneva",
                    "app.kubernetes.io/component": "mainline-rollout",
                },
            },
            "spec": {
                "backoffLimit": 0,
                // 上界必须覆盖最坏情况：每个目标最多吃 startup + rollout + 15s
                // 宽限（到点即判败回滚）；Job 被 activeDeadlineSeconds 杀掉
                // 走不到 Job 自己的回滚，留短了会把集群停在半滚状态。
                "activeDeadlineSeconds": (self.cfg.startup_timeout_secs
                    + self.cfg.rollout_timeout_secs
                    + 15)
                    * self.cfg.targets.len().max(1) as u64
                    + self.cfg.soak_secs,
                "ttlSecondsAfterFinished": 86400,
                "template": {
                    "metadata": {
                        "labels": {
                            "app.kubernetes.io/name": "cogneva",
                            "app.kubernetes.io/component": "mainline-rollout",
                        },
                    },
                    "spec": {
                        "serviceAccountName": "cogneva-evolution",
                        "restartPolicy": "Never",
                        "containers": [{
                            "name": "mainline-rollout",
                            "image": new_tag,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/opt/cogneva/cogneva"],
                            "args": args,
                            // 不声明资源时 QoS 是 BestEffort——节点内存压力下最先
                            // 被驱逐的一档，而这个容器偏偏是决定"回滚不回滚"的判定
                            // 进程：它被驱逐，部署器就把一次观测中断记成一次版本失败，
                            // 集群还停在滚到一半的状态（Job 没跑完，它自己的回滚也没走）。
                            "resources": {
                                "requests": {
                                    "cpu": self.cfg.job_cpu_request,
                                    "memory": self.cfg.job_memory_request,
                                },
                                "limits": {
                                    "cpu": self.cfg.job_cpu_limit,
                                    "memory": self.cfg.job_memory_limit,
                                },
                            },
                        }],
                    },
                },
            },
        });
        if !volumes.is_empty() {
            manifest["spec"]["template"]["spec"]["volumes"] = serde_json::Value::Array(volumes);
        }
        if !mounts.is_empty() {
            manifest["spec"]["template"]["spec"]["containers"][0]["volumeMounts"] =
                serde_json::Value::Array(mounts);
        }
        let body = serde_json::to_vec_pretty(&manifest)?;
        self.apply_stdin(&body, "rollout job").await
    }

    /// `kubectl apply -f -`，清单经 stdin 传入（Job 与清单包 ConfigMap 共用）。
    async fn apply_stdin(&self, body: &[u8], what: &str) -> SFResult<()> {
        let mut child = tokio::process::Command::new(&self.cfg.kubectl_bin)
            .args(["-n", &self.cfg.namespace, "apply", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| SFError::IO(format!("spawn kubectl apply: {e}")))?;
        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| SFError::IO("kubectl stdin unavailable".into()))?;
            stdin.write_all(body).await?;
            stdin.shutdown().await?;
        }
        let output = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
            .await
            .map_err(|_| SFError::IO("kubectl apply timed out".into()))?
            .map_err(|e| SFError::IO(format!("kubectl apply: {e}")))?;
        if !output.status.success() {
            return Err(SFError::IO(format!(
                "kubectl apply {what} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }

    /// 从 bare 仓库读指定 rev 下的文件内容（不触碰沙盒工作树，构建与
    /// 组包互不干扰）。
    async fn git_show(&self, rev: &str, path: &str) -> SFResult<String> {
        let spec = format!("{rev}:{path}");
        self.run_cmd(
            "git",
            &["--git-dir", &self.cfg.bare_repo, "show", &spec],
            None,
            30,
        )
        .await
    }

    /// 读 rev 处的发布清单并组装清单包。image 必须是节点 pull 端点引用
    /// （kubelet 经 NodePort 拉取），与 set image 路径同一约束。
    async fn build_bundle_at(&self, rev: &str, image: &str) -> SFResult<RolloutBundle> {
        let dir = self.cfg.manifest_dir.trim_end_matches('/');
        let kustomization = self
            .git_show(rev, &format!("{dir}/kustomization.yaml"))
            .await?;
        let resources = parse_kustomization_resources(&kustomization)?;
        let mut files = BTreeMap::new();
        for res in &resources {
            let content = self.git_show(rev, &format!("{dir}/{res}")).await?;
            files.insert(res.clone(), content);
        }
        let bundle = build_rollout_bundle(&files, &kustomization, &self.cfg.targets, image)?;
        info!(
            rev = %rev12(rev),
            support_bytes = bundle.support_yaml.len(),
            target_manifests = bundle.targets.len(),
            "manifest bundle assembled"
        );
        Ok(bundle)
    }

    /// 清单包发布成 per-rev ConfigMap（Job 挂载消费）。先按 label 清理
    /// 旧包——名字含 rev 猜不得，label 是稳定选择器；清理失败只 warn
    /// （新包 apply 不受影响，残留由下次发布再清）。
    async fn publish_manifests_configmap(&self, rev: &str, bundle: &RolloutBundle) -> SFResult<()> {
        if let Err(e) = self
            .kubectl(
                &[
                    "delete",
                    "configmap",
                    "-l",
                    "app.kubernetes.io/component=mainline-manifests",
                    "--ignore-not-found",
                ],
                60,
            )
            .await
        {
            warn!(error = %e, "stale manifest configmap cleanup failed (best effort)");
        }
        let mut data = serde_json::Map::new();
        data.insert(
            "support.yaml".into(),
            serde_json::Value::String(bundle.support_yaml.clone()),
        );
        for t in &bundle.targets {
            data.insert(t.key.clone(), serde_json::Value::String(t.yaml.clone()));
        }
        let cm = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": manifests_configmap_name(rev),
                "namespace": self.cfg.namespace,
                "labels": {
                    "app.kubernetes.io/name": "cogneva",
                    "app.kubernetes.io/component": "mainline-manifests",
                },
            },
            "data": serde_json::Value::Object(data),
        });
        let body = serde_json::to_vec_pretty(&cm)?;
        self.apply_stdin(&body, "manifests configmap").await?;
        info!(rev = %rev12(rev), name = %manifests_configmap_name(rev), "manifest bundle published");
        Ok(())
    }

    async fn job_status(&self, name: &str) -> SFResult<JobStatus> {
        let out = match self
            .kubectl(
                &[
                    "get",
                    "job",
                    name,
                    "-o",
                    // 分隔符必须显式占位：空格 + split_whitespace 会吞掉缺失
                    // 字段，失败任务 succeeded 缺省时 failed 值顶到第一位，
                    // 会被误判成 Complete。
                    "jsonpath={.status.succeeded}|{.status.failed}|{.status.active}",
                ],
                30,
            )
            .await
        {
            Ok(o) => o,
            Err(e) if e.to_string().contains("not found") || e.to_string().contains("NotFound") => {
                return Ok(JobStatus::NotFound)
            }
            Err(e) => return Err(e),
        };
        let parts: Vec<&str> = out.split('|').collect();
        let succeeded = parts
            .first()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let failed = parts
            .get(1)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        if succeeded >= 1 {
            Ok(JobStatus::Complete)
        } else if failed >= 1 {
            Ok(JobStatus::Failed)
        } else {
            Ok(JobStatus::Running)
        }
    }

    /// Job 已失败时，问它的判定进程**是哪一类**失败。Job 的 Pod 模板是
    /// `backoffLimit: 0` + `restartPolicy: Never`，一个 Pod 一次运行，终止码
    /// 就是这个进程的退出码。
    ///
    /// 采不到（Pod 已删、查询失败、码不可解析）按**版本类**靠：类别判不准
    /// 时，把环境类误记成版本类只是多花一次尝试预算，反过来则是一个真坏的
    /// 版本被无限重试、永不停下——代价不对称，往记账侧取。
    async fn job_failure_class(&self, name: &str) -> FailureClass {
        let out = match self
            .kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &format!("job-name={name}"),
                    "-o",
                    "jsonpath={range .items[*]}{.status.containerStatuses[0].state.terminated.exitCode}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
        {
            Ok(o) => o,
            Err(e) => {
                warn!(
                    job = %name,
                    error = %e,
                    "could not read the rollout job's exit code; treating its failure as version-class"
                );
                return FailureClass::Version;
            }
        };
        let code = out
            .lines()
            .filter_map(|l| l.trim().parse::<i32>().ok())
            .next_back();
        if code == Some(ROLLOUT_EXIT_ENVIRONMENT) {
            FailureClass::Environment
        } else {
            FailureClass::Version
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum JobStatus {
    Complete,
    Failed,
    Running,
    NotFound,
}

/// 构建锁守卫：Drop 时删锁文件。
struct BuildLock {
    path: PathBuf,
}

impl Drop for BuildLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// 心跳摘要（纯函数便于测试）：一行覆盖空闲态全部关键状态。SameRev 收敛
/// 路径静默返回，没有这条摘要时部署器存活无法从日志证明。
fn heartbeat_message(state: &MainlineState, bare_rev: &str, now_unix: i64) -> String {
    let in_flight = state
        .in_flight
        .as_ref()
        .map(|f| format!("{}@{:?}", rev12(&f.rev), f.phase))
        .unwrap_or_else(|| "none".into());
    format!(
        "bare={} last_good={} in_flight={} failed_rev={} failed_class={} failed_attempts={} cooldown_remaining_secs={}",
        rev12(bare_rev),
        state.last_good_rev.as_deref().map(rev12).unwrap_or("none"),
        in_flight,
        state.failed_rev.as_deref().map(rev12).unwrap_or("none"),
        // 环境类失败不计尝试次数，`failed_rev` 与 `failed_attempts=0` 会同时出现；
        // 不说出类别，这一行读起来就像记账坏了。
        state
            .failed_rev
            .as_ref()
            .map(|_| state.failed_class.as_str())
            .unwrap_or("none"),
        state.failed_attempts,
        (state.failed_cooldown_until - now_unix).max(0),
    )
}

/// 构建侧后台循环入口（插件 spawn）。
pub async fn run_mainline_loop(
    deployer: std::sync::Arc<MainlineDeployer>,
    shutdown: ShutdownSignal,
) {
    // 宿主 bare 仓库（/host-git）与工作树属主/挂载场景会撞 git
    // dubious-ownership；safe.directory 只有 global 配置被采信（与
    // gitops puller 同源处理），启动时幂等写入。临时工作树路径启动时还
    // 不存在，由分配器在创建时按需注册。
    let workdir = deployer.workdir();
    for dir in [
        deployer.cfg.bare_repo.as_str(),
        deployer.workspaces.root().to_str().unwrap_or(""),
        workdir.to_str().unwrap_or(""),
    ] {
        let _ = tokio::process::Command::new("git")
            .args(["config", "--global", "--add", "safe.directory", dir])
            .output()
            .await;
    }
    // 崩溃残留的临时工作树与裸仓库里的孤儿登记在循环启动时清一次。
    let _ = deployer.workspaces.prune().await;
    match deployer.workspaces.gc_stale().await {
        Ok(reclaimed) if !reclaimed.is_empty() => {
            info!(
                count = reclaimed.len(),
                "reclaimed leaked mainline workspaces"
            )
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "workspace gc failed"),
    }
    let interval = Duration::from_secs(deployer.cfg.poll_interval_secs.max(30));
    info!(
        interval_secs = interval.as_secs(),
        push_endpoint = %deployer.push_endpoint(),
        pull_endpoint = %deployer.pull_endpoint(),
        "Mainline deployer loop started"
    );
    let mut ticker = tokio::time::interval(interval);
    // 空闲心跳：SameRev 路径静默返回，靠周期性 INFO 摘要证明部署器存活。
    let heartbeat_every = Duration::from_secs(deployer.cfg.heartbeat_log_secs);
    let mut last_heartbeat: Option<tokio::time::Instant> = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                if let Err(e) = deployer.poll_once().await {
                    warn!(error = %e, "mainline deployer poll failed");
                }
                let due = last_heartbeat
                    .map(|t| t.elapsed() >= heartbeat_every)
                    .unwrap_or(true);
                if due {
                    deployer.log_heartbeat().await;
                    last_heartbeat = Some(tokio::time::Instant::now());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 清单包组装（纯函数，单测覆盖）
// ---------------------------------------------------------------------------

/// 一次滚动携带的发布清单全集：support 是四个 deployment 之外的支撑资源
/// （命名空间级），targets 是镜像已改写为本次滚动引用的 deployment 清单。
/// 滚动侧先 apply support，再按 targets 顺序逐个 apply。
#[derive(Debug)]
pub struct RolloutBundle {
    pub support_yaml: String,
    pub targets: Vec<TargetManifest>,
}

#[derive(Debug)]
pub struct TargetManifest {
    pub deployment: String,
    /// ConfigMap data / 挂载目录里的文件名。
    pub key: String,
    pub yaml: String,
}

/// 目标 deployment 清单在包内的固定文件名（滚动侧按此约定找文件）。
fn target_manifest_key(deployment: &str) -> String {
    format!("deploy-{deployment}.yaml")
}

/// 清单包 ConfigMap 名（per-rev，不可变；旧包在下次发布前按 label 清理）。
fn manifests_configmap_name(rev: &str) -> String {
    format!("mainline-manifests-{}", rev12(rev))
}

/// 集群级 kind：进化 SA 只有命名空间级 Role，apply 这些必然被拒；它们由
/// 安装面（bootstrap/管理员 kubeconfig）管理，不进部署链路。
const CLUSTER_SCOPED_KINDS: &[&str] = &[
    "Namespace",
    "StorageClass",
    "PersistentVolume",
    "ClusterRole",
    "ClusterRoleBinding",
    "CustomResourceDefinition",
    "PriorityClass",
    "IngressClass",
    "RuntimeClass",
    "APIService",
    "ValidatingWebhookConfiguration",
    "MutatingWebhookConfiguration",
];

fn is_cluster_scoped_kind(kind: &str) -> bool {
    CLUSTER_SCOPED_KINDS.contains(&kind)
}

/// 解析 kustomization.yaml 的 resources 列表——发布集的权威定义。
fn parse_kustomization_resources(text: &str) -> SFResult<Vec<String>> {
    let v: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|e| SFError::Config(format!("parse kustomization.yaml: {e}")))?;
    let resources = v
        .get("resources")
        .and_then(|r| r.as_sequence())
        .ok_or_else(|| SFError::Config("kustomization.yaml has no resources list".into()))?;
    resources
        .iter()
        .map(|r| {
            r.as_str()
                .map(str::to_string)
                .ok_or_else(|| SFError::Config("kustomization resources entry not a string".into()))
        })
        .collect()
}

/// 权限面 kind：与集群级 kind 一样不经部署面下发。K8s 反提权规定 apply
/// 一个 Role 要求发起者已持有其中全部权限——发布集里存在部署器 SA 永远
/// 不该持有的权限（如读 Secret 的管理 Role），apply 必被拒；给部署器发
/// bind/escalate 或那些权限本身则等于让自治系统给自己提权。权限边界的
/// 变更必须走安装面/人工核准（bootstrap 以管理员身份 apply 全量清单）。
const RBAC_KINDS: &[&str] = &["Role", "RoleBinding"];

/// 拆分多文档 YAML 并过滤进支撑包：Secret 硬报错（零带外凭证红线，密钥
/// 永不进清单链路）；集群级 kind 与权限面 kind（Role/RoleBinding）跳过
/// 并记日志（前者由安装面管理，后者见 [`RBAC_KINDS`] 的反提权理由）；
/// 空文档（`---` 分隔产生）跳过。
fn namespace_docs(yaml_text: &str, origin: &str) -> SFResult<Vec<serde_yaml::Value>> {
    let mut docs = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(yaml_text) {
        let v = serde_yaml::Value::deserialize(doc)
            .map_err(|e| SFError::Config(format!("{origin}: invalid YAML document: {e}")))?;
        if v.is_null() {
            continue;
        }
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind == "Secret" {
            return Err(SFError::Config(format!(
                "{origin}: Secret in manifest bundle is forbidden; secrets never travel through manifests"
            )));
        }
        if is_cluster_scoped_kind(kind) {
            info!(origin = %origin, kind = %kind, "manifest bundle: skipping cluster-scoped kind");
            continue;
        }
        if RBAC_KINDS.contains(&kind) {
            warn!(origin = %origin, kind = %kind, "manifest bundle: skipping RBAC kind; permission changes must be applied out-of-band");
            continue;
        }
        docs.push(v);
    }
    Ok(docs)
}

/// 改写单个 Deployment 文档里指定容器的 image。容器名显式校验且必须恰好
/// 命中一个——错改比不改危险，宁可整个发布失败。
fn patch_container_image(
    v: &mut serde_yaml::Value,
    origin: &str,
    container: &str,
    image: &str,
) -> SFResult<()> {
    // serde_yaml::Value 没有 JSON Pointer 辅助，逐层 get_mut 下钻；任一层
    // 结构缺失都报硬错误（发布集里的 deployment 清单结构异常不该静默放行）。
    let containers = v
        .get_mut("spec")
        .and_then(|s| s.get_mut("template"))
        .and_then(|t| t.get_mut("spec"))
        .and_then(|s| s.get_mut("containers"))
        .and_then(|c| c.as_sequence_mut())
        .ok_or_else(|| {
            SFError::Config(format!(
                "{origin}: no containers list under spec.template.spec"
            ))
        })?;
    let mut patched = 0usize;
    for c in containers.iter_mut() {
        if c.get("name").and_then(|n| n.as_str()) == Some(container) {
            if let Some(map) = c.as_mapping_mut() {
                map.insert(
                    serde_yaml::Value::String("image".into()),
                    serde_yaml::Value::String(image.to_string()),
                );
                patched += 1;
            }
        }
    }
    if patched != 1 {
        return Err(SFError::Config(format!(
            "{origin}: expected exactly one container named {container}, found {patched}"
        )));
    }
    Ok(())
}

/// 把目标清单里指定容器的 image 改写为本次滚动引用。
///
/// 目标清单允许多文档（如 Deployment + 配套 Service 同文件）：目标
/// Deployment 必须恰好出现一个且名字精确匹配，其余命名空间级文档原样
/// 随目标下发；与支撑包同一套红线——Secret 硬报错、集群级 kind 与
/// RBAC kind 跳过。单文档 `from_str` 会在多文档文件上报错并卡死整条
/// 发布链路（旧版二进制的实机事故形态），故按文档流解析。
fn patch_deployment_image(
    yaml_text: &str,
    origin: &str,
    expect_deployment: &str,
    container: &str,
    image: &str,
) -> SFResult<String> {
    let mut out_docs: Vec<serde_yaml::Value> = Vec::new();
    let mut deployments = 0usize;
    for doc in serde_yaml::Deserializer::from_str(yaml_text) {
        let mut v = serde_yaml::Value::deserialize(doc)
            .map_err(|e| SFError::Config(format!("{origin}: invalid YAML document: {e}")))?;
        if v.is_null() {
            continue;
        }
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind == "Secret" {
            return Err(SFError::Config(format!(
                "{origin}: Secret in manifest bundle is forbidden; secrets never travel through manifests"
            )));
        }
        if is_cluster_scoped_kind(kind) {
            info!(origin = %origin, kind = %kind, "manifest bundle: skipping cluster-scoped kind");
            continue;
        }
        if RBAC_KINDS.contains(&kind) {
            warn!(origin = %origin, kind = %kind, "manifest bundle: skipping RBAC kind; permission changes must be applied out-of-band");
            continue;
        }
        if kind == "Deployment" {
            let name = v
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            if name != expect_deployment {
                return Err(SFError::Config(format!(
                    "{origin}: deployment name {name} does not match rollout target {expect_deployment}"
                )));
            }
            patch_container_image(&mut v, origin, container, image)?;
            deployments += 1;
        }
        out_docs.push(v);
    }
    if deployments != 1 {
        return Err(SFError::Config(format!(
            "{origin}: expected exactly one Deployment named {expect_deployment}, found {deployments}"
        )));
    }
    let mut out = String::new();
    for d in &out_docs {
        out.push_str("---\n");
        out.push_str(
            &serde_yaml::to_string(d).map_err(|e| {
                SFError::Config(format!("{origin}: serialize patched manifest: {e}"))
            })?,
        );
    }
    Ok(out)
}

/// 从发布集文件内容（kustomization resources 里的相对路径 → 文件文本）
/// 组装清单包。目标 deployment 的清单必须在发布集里，缺文件 / 名字对不上
/// 都是硬错误——静默回落 set image 会掩盖发布集漂移，让拓扑滞后悄悄回来。
/// targets 输出保持调用方给的滚动顺序（与 kustomization 里的文件顺序无关）。
pub fn build_rollout_bundle(
    files: &BTreeMap<String, String>,
    kustomization: &str,
    targets: &[RolloutTargetConfig],
    image: &str,
) -> SFResult<RolloutBundle> {
    let resources = parse_kustomization_resources(kustomization)?;
    if let Some(dup) = duplicate_resources(&resources) {
        return Err(SFError::Config(format!(
            "duplicate resource {dup} in kustomization resources"
        )));
    }
    let mut patched: BTreeMap<String, String> = BTreeMap::new();
    let mut support_docs: Vec<serde_yaml::Value> = Vec::new();
    for res in &resources {
        let content = files.get(res).ok_or_else(|| {
            SFError::Config(format!(
                "kustomization resource {res} missing from bundle files"
            ))
        })?;
        if let Some(t) = targets
            .iter()
            .find(|t| t.manifest.as_deref() == Some(res.as_str()))
        {
            let yaml = patch_deployment_image(content, res, &t.deployment, &t.container, image)?;
            patched.insert(res.clone(), yaml);
        } else {
            support_docs.extend(namespace_docs(content, res)?);
        }
    }
    let mut target_manifests = Vec::new();
    for t in targets {
        let Some(m) = t.manifest.as_deref() else {
            // 未声明清单的目标由滚动侧回落 set image（版本偏差兼容路径）。
            continue;
        };
        if !resources.iter().any(|r| r == m) {
            return Err(SFError::Config(format!(
                "manifest {m} of rollout target {} is not in kustomization resources",
                t.deployment
            )));
        }
        let yaml = patched.remove(m).ok_or_else(|| {
            SFError::Config(format!(
                "manifest {m} of target {} was not patched",
                t.deployment
            ))
        })?;
        target_manifests.push(TargetManifest {
            deployment: t.deployment.clone(),
            key: target_manifest_key(&t.deployment),
            yaml,
        });
    }
    // 声明了清单的目标之间不允许共用同一文件：patched 里同名条目会被
    // remove 吃掉，第二个目标报"was not patched"硬错误，不会静默错配。
    let mut support_yaml = String::new();
    for d in &support_docs {
        support_yaml.push_str("---\n");
        support_yaml.push_str(
            &serde_yaml::to_string(d)
                .map_err(|e| SFError::Config(format!("serialize support doc: {e}")))?,
        );
    }
    Ok(RolloutBundle {
        support_yaml,
        targets: target_manifests,
    })
}

/// 发布集去重校验：kustomization resources 不允许重复条目（重复会让
/// patch/support 双路都处理同一文件，support 里出现两份相同资源）。
fn duplicate_resources(resources: &[String]) -> Option<&str> {
    let mut seen: HashSet<&str> = HashSet::new();
    resources
        .iter()
        .map(|s| s.as_str())
        .find(|s| !seen.insert(s))
}

// ---------------------------------------------------------------------------
// 滚动侧（Job 内执行）
// ---------------------------------------------------------------------------

pub struct RolloutTarget {
    pub deployment: String,
    pub container: String,
    pub component: String,
    pub name: String,
}

pub struct RolloutPlan {
    /// 新镜像引用（节点 pull 端点，kubelet 经 NodePort 拉取）。
    pub tag: String,
    pub targets: Vec<RolloutTarget>,
    /// 随镜像下发的发布清单目录（Job 挂载点，可选）。目录在时：先 apply
    /// support.yaml，目标存在 `deploy-<deployment>.yaml` 则 apply 整份清单
    /// （镜像已改写为 tag），否则回落 set image。缺省走纯 set image 旧路
    /// ——派发侧是旧版二进制（版本偏差）时 Job 也能滚。
    pub manifests_dir: Option<String>,
}

impl RolloutPlan {
    pub fn from_config(cfg: &MainlineDeployerConfig, tag: String) -> Self {
        let targets = cfg
            .targets
            .iter()
            .map(|t: &RolloutTargetConfig| RolloutTarget {
                deployment: t.deployment.clone(),
                container: t.container.clone(),
                component: t.component.clone(),
                name: t.name.clone(),
            })
            .collect();
        Self {
            tag,
            targets,
            manifests_dir: None,
        }
    }
}

pub struct RolloutExecutor {
    kubectl: String,
    ns: String,
    soak_secs: u64,
    restart_threshold: u32,
    rollout_timeout_secs: u64,
    startup_timeout_secs: u64,
}

impl RolloutExecutor {
    pub fn new(
        kubectl: impl Into<String>,
        ns: impl Into<String>,
        soak_secs: u64,
        restart_threshold: u32,
        rollout_timeout_secs: u64,
        startup_timeout_secs: u64,
    ) -> Self {
        Self {
            kubectl: kubectl.into(),
            ns: ns.into(),
            soak_secs,
            restart_threshold,
            rollout_timeout_secs,
            startup_timeout_secs,
        }
    }

    async fn run_kubectl(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let cmdline = format!("kubectl {}", args.join(" "));
        let mut full: Vec<&str> = vec!["-n", &self.ns];
        full.extend_from_slice(args);
        let fut = tokio::process::Command::new(&self.kubectl)
            .args(&full)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| {
                SFError::IO(format!(
                    "{OBSERVATION_TOOL_MARKER}: cannot run {}: {e}",
                    self.kubectl
                ))
            })?;
        if !output.status.success() {
            return Err(SFError::IO(format!(
                "{cmdline} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn set_image(&self, t: &RolloutTarget, image: &str) -> SFResult<()> {
        let image_arg = format!("{}={}", t.container, image);
        self.run_kubectl(
            &[
                "set",
                "image",
                &format!("deployment/{}", t.deployment),
                &image_arg,
            ],
            60,
        )
        .await?;
        Ok(())
    }

    /// 把单个目标滚到新镜像：优先 apply 随镜像下发的整份 deployment 清单
    /// （携带拓扑/配置漂移修正），清单缺失才回落 set image（只动 image 字段）。
    /// 两条路径都只负责"提交变更"，收敛判定与回滚仍由调用方统一处理。
    async fn apply_target(&self, plan: &RolloutPlan, t: &RolloutTarget) -> SFResult<()> {
        if let Some(dir) = &plan.manifests_dir {
            let path = Path::new(dir).join(target_manifest_key(&t.deployment));
            if path.is_file() {
                let path_arg = path.to_string_lossy().to_string();
                info!(deployment = %t.deployment, manifest = %path_arg, "mainline rollout: apply target manifest");
                return self
                    .run_kubectl(&["apply", "-f", &path_arg], 60)
                    .await
                    .map(|_| ());
            }
            warn!(deployment = %t.deployment, "no target manifest in bundle; falling back to set image");
        }
        self.set_image(t, &plan.tag).await
    }

    /// 快照单个部署当前在跑的镜像（回滚目标）。
    async fn current_image(&self, t: &RolloutTarget) -> SFResult<String> {
        let jsonpath = format!(
            "jsonpath={{.spec.template.spec.containers[?(@.name==\"{}\")].image}}",
            t.container
        );
        let img = self
            .run_kubectl(&["get", "deployment", &t.deployment, "-o", &jsonpath], 30)
            .await?;
        let img = img.trim().to_string();
        if img.is_empty() {
            return Err(SFError::Agent(format!(
                "deployment/{} has no image for container {}",
                t.deployment, t.container
            )));
        }
        Ok(img)
    }

    /// 带集群访问容忍的查询：把「查不到集群」与「查到的结果不健康」分开。
    /// 前者就地在 budget_secs 内按轮询节拍重试，不产生任何滚动结论；预算耗尽
    /// 仍不可达才返回带标记的错误，调用方据此判「本轮没有观测到任何东西」。
    /// 后者（查询成功但输出表明版本有问题）立即返回，那是真的版本结论。
    /// 非集群访问类的失败（选择器非法、资源不存在）同样立即返回——它们也是
    /// 明确的观测结果，不该被静默重试吞掉。
    async fn probe(&self, args: &[&str], timeout_secs: u64, budget_secs: u64) -> SFResult<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(budget_secs);
        loop {
            match self.run_kubectl(args, timeout_secs).await {
                Ok(out) => return Ok(out),
                Err(e) if is_cluster_unreachable(&e.to_string()) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(SFError::IO(format!("{CLUSTER_UNREACHABLE_MARKER}: {e}")));
                    }
                    tokio::time::sleep(Duration::from_secs(ROLLOUT_POLL_SECS)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 选择器命中的 Pod 的 init 容器进度：每个 init 容器一行，形如
    /// `<finishedAt>|`，未结束的只有分隔符。空输出表示没有任何 init 容器
    /// （无 init 的部署恒为空）。未完成用分隔符而不是空行标记，因为
    /// run_kubectl 会 trim 掉首尾空白，纯空行到不了这里。查询失败时调用方
    /// 沿用当前相位继续计预算，不因一次查询抖动把预算放宽。
    async fn init_containers_progress(&self, t: &RolloutTarget) -> SFResult<String> {
        let selector = pod_selector(&t.name, &t.component);
        self.run_kubectl(
            &[
                "get",
                "pods",
                "-l",
                &selector,
                "-o",
                "jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.state.terminated.finishedAt}{\"|\"}{end}{end}",
            ],
            30,
        )
        .await
    }

    /// 这个部署带不带 init 容器。没有 init 容器的部署不存在启动阶段，一上来
    /// 就该吃就绪预算。查询失败按"带"处理：宁可把一次慢 init 记在启动预算上
    /// 晚一点判败（版本仍有机会被判清白），也不要把它算成就绪超时、把一个
    /// 好版本判成败。
    async fn init_containers_exist(&self, t: &RolloutTarget) -> SFResult<bool> {
        let out = self
            .run_kubectl(
                &[
                    "get",
                    "deployment",
                    &t.deployment,
                    "-o",
                    "jsonpath={.spec.template.spec.initContainers[*].name}",
                ],
                30,
            )
            .await?;
        Ok(!out.trim().is_empty())
    }

    /// 轮询 deployment rollout 完成：observedGeneration 追上 generation 且
    /// updated/ready 副本数达期望（短查询，Job 在爆炸半径外不怕被杀）。
    /// 轮询同时查 Pod 致命等待态：崩溃镜像永远不会 ready，干等 rollout 超时
    /// （默认 300s）既拖慢回滚又让故障窗口白白拉长，命中即早退触发回滚。
    ///
    /// 两段预算：Pod 的 init 容器还没结束时是**启动阶段**，只吃
    /// startup_timeout_secs；init 全部结束后才开始吃 rollout_timeout_secs
    /// 的**就绪**预算。种子这类 init 步骤必须早于主容器结束，但它的耗时
    /// 由外部网络决定、与本次要上线的版本无关，算进就绪预算就会让一次慢
    /// 克隆把好版本判成败。两段都仍需有界：主容器永不 ready 的场景不能
    /// 无限等，Job 被 activeDeadlineSeconds 杀掉不会走 Job 自己的回滚。
    ///
    /// 超时那一刻再分一次因：新 Pod 被调度器判为 Unschedulable 且本次滚动没有
    /// 改动这个部署的放置面时，超时说的是**集群放不下**，不是版本不好（见
    /// PLACEMENT_BLOCKED_MARKER）。`prev_shape` 是 apply 之前快照的放置面。
    async fn wait_rollout_complete(&self, t: &RolloutTarget, prev_shape: &str) -> SFResult<()> {
        let startup_deadline =
            std::time::Instant::now() + Duration::from_secs(self.startup_timeout_secs);
        // 延迟到首次进入就绪阶段才起算：启动阶段的耗时不算在内。
        let mut readiness_deadline: Option<std::time::Instant> = None;
        let has_init_containers = self.init_containers_exist(t).await.unwrap_or(true);
        // 探针配置在整段滚动里不变，进循环前取一次（放在循环里会让每次轮询都多
        // 一条 kubectl）；取不到就一直是 None，到真的要用它判超预算时再补读。
        let mut readiness_probe = self.readiness_probe(t).await;
        // 是否亲眼见过 init 还在跑。刚 apply 时选择器匹配到的仍是旧 Pod，它的
        // init 早已结束，"当前没有未完成的 init"于是立刻成立、就绪预算从 t0
        // 起算，然后整段耗在一次仍在外网克隆的 init 上——好版本被判超时回滚。
        // 只有先见过 init 在跑，后面的"没有未完成的 init"才真的意味着 init 结束。
        let mut seen_init_running = false;
        // 是否成功取到过部署态。一次都没取到时，"预算耗尽"说明的是我们看不到
        // 集群，不是这个版本不收敛——那种结论必须带标记交回调用方，不能变成
        // 回滚指令。
        let mut observed_ever = false;
        let mut last_unreachable = String::new();
        loop {
            // 拿不到观测时沿用当前相位计预算：还没进就绪阶段就仍然是启动阶段。
            let mut starting = readiness_deadline.is_none();
            let mut note = String::new();
            match self
                .run_kubectl(
                    &[
                        "get",
                        "deployment",
                        &t.deployment,
                        "-o",
                        // 竖线显式占位：缺失字段（omitempty 的 updatedReplicas
                        // 等）不能顶掉后续字段的位置。
                        "jsonpath={.metadata.generation}|{.status.observedGeneration}|{.spec.replicas}|{.status.updatedReplicas}|{.status.readyReplicas}|{.status.unavailableReplicas}",
                    ],
                    30,
                )
                .await
            {
                Ok(out) => {
                    observed_ever = true;
                    if rollout_converged(&out) {
                        let samples = self.sample_rollout_pods(t).await;
                        match rollout_pods_ready(&samples) {
                            Some(true) => return Ok(()),
                            Some(false) => {
                                if let Some(fatal) = rollout_pods_fatal(&samples) {
                                    return Err(SFError::Agent(format!(
                                        "pod of deployment/{} in fatal waiting state {fatal}",
                                        t.deployment
                                    )));
                                }
                                self.fatal_pod_state(t).await?;
                                // 新副本还在起来。按就绪探针自己的判定周期给预算，
                                // 预算内不算失败（见 ProbeCycle::budget）——门禁比
                                // kubelet 更早开枪，就会把一个十秒后就就绪的正常
                                // 版本判死。
                                if readiness_probe.is_none() {
                                    readiness_probe = self.readiness_probe(t).await;
                                }
                                if let Some(cycle) = readiness_probe {
                                    let budget = cycle.budget();
                                    if readiness_overdue(&samples, budget, Utc::now()) {
                                        let err = self
                                            .readiness_gate_error(t, budget.as_secs())
                                            .await;
                                        return Err(err);
                                    }
                                }
                            }
                            // 采样取不到：没有观测就不下结论，收敛与否交给外层
                            // 预算与 soak 复查兜底。
                            None => {}
                        }
                        // 收敛态下 init 必然已结束，不重进启动阶段。
                        starting = false;
                        note = out;
                    } else {
                        // 滚动中新旧 Pod 交替、containerStatuses 可能暂时缺失，
                        // 空输出/查询失败在这里不当致命（与 pods_healthy 不同），
                        // 只认明确的致命等待态。
                        self.fatal_pod_state(t).await?;
                        if !has_init_containers {
                            // 没有 init 容器就没有启动阶段，直接吃就绪预算。
                            starting = false;
                        } else if let Ok(progress) = self.init_containers_progress(t).await {
                            if progress.lines().any(|l| l.trim() == "|") {
                                seen_init_running = true;
                                starting = true;
                            } else {
                                // 没有未完成的 init，但没见过它跑过就不算结束：匹配到的
                                // 可能是旧 Pod，也可能是新 Pod 的 init 状态还没上报。
                                starting = !seen_init_running;
                            }
                        }
                        note = out;
                    }
                }
                Err(e) if is_cluster_unreachable(&e.to_string()) => {
                    last_unreachable = e.to_string();
                }
                Err(e) => return Err(e),
            }
            let now = std::time::Instant::now();
            // 就绪预算从首次进入该阶段起算，启动阶段的耗时不算在内。
            let (budget, phase) = if starting {
                (self.startup_timeout_secs, "startup")
            } else {
                (self.rollout_timeout_secs, "readiness")
            };
            let deadline = if starting {
                startup_deadline
            } else {
                *readiness_deadline
                    .get_or_insert_with(|| now + Duration::from_secs(self.rollout_timeout_secs))
            };
            if now >= deadline {
                // 超时才采样：正常路径一次 kubectl 都不多花。
                let diagnosis = self.rollout_diagnosis(t).await;
                let suffix = if diagnosis.is_empty() {
                    String::new()
                } else {
                    format!("; pods: {diagnosis}")
                };
                // 排不上队：只有"这次上线没动放置面"才归环境。
                let blocked = self.sample_unschedulable_pods(t).await;
                let now_shape = self.current_placement_shape(t).await.ok();
                let environment = environment_class(&blocked, prev_shape, now_shape.as_deref());
                return Err(if !observed_ever {
                    // 一次都没看到过部署态：这是观测能力故障，不是版本结论。
                    SFError::IO(format!(
                        "{CLUSTER_UNREACHABLE_MARKER}: rollout of deployment/{} did not complete \
                         within {}s and its {phase} phase was never observed ({last_unreachable})",
                        t.deployment, budget
                    ))
                } else if let Some(detail) = environment {
                    SFError::IO(format!(
                        "{PLACEMENT_BLOCKED_MARKER}: rollout of deployment/{} did not complete \
                         within {}s ({phase} phase) — the scheduler never placed its pod(s) \
                         ({detail}) and this revision did not change the deployment's placement \
                         shape, so the revision is not what failed (last: {note}{suffix})",
                        t.deployment, budget
                    ))
                } else if starting {
                    SFError::Agent(format!(
                        "deployment/{} stuck in startup phase after {}s \
                         (init containers not finished; last deployment state: {note}{suffix})",
                        t.deployment, self.startup_timeout_secs
                    ))
                } else {
                    SFError::Agent(format!(
                        "rollout of deployment/{} did not complete within {}s (last: {note}{suffix})",
                        t.deployment, self.rollout_timeout_secs
                    ))
                });
            }
            tokio::time::sleep(Duration::from_secs(ROLLOUT_POLL_SECS)).await;
        }
    }

    /// 只查致命等待态（拉不到镜像、配置错误、CrashLoop），init 容器与主
    /// 容器一并查：init 拉不到镜像时主容器只报 PodInitializing，不算致命，
    /// 只看主容器就会把启动阶段的上界白白耗光。滚动交替期 containerStatuses
    /// 缺失或查询临时失败均返回 Ok——由调用方的超时与后续 pods_healthy 兜底，
    /// 这里只负责让"必死"的滚动快速失败。
    async fn fatal_pod_state(&self, t: &RolloutTarget) -> SFResult<()> {
        let selector = pod_selector(&t.name, &t.component);
        let out = match self
            .run_kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.state.waiting.reason}{\"\\n\"}{end}{.status.containerStatuses[0].state.waiting.reason}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
        {
            Ok(out) => out,
            Err(_) => return Ok(()),
        };
        for line in out.lines() {
            let reason = line.trim();
            if FATAL_WAITING_REASONS.contains(&reason) {
                return Err(SFError::Agent(format!(
                    "pod of deployment/{} in fatal waiting state {reason}",
                    t.deployment
                )));
            }
        }
        Ok(())
    }

    /// 本次部署的 Pod 现场采样：相位、ready、等待原因/消息、主容器启动时刻、
    /// 是否正在删除。诊断与就绪门禁共用这一条查询——两者要的是同一批现场，
    /// 拆成两条会让同一个 Pod 在两条路径上给出不同说法。
    ///
    /// 采样尽力而为：取不到就返回空，观测失败不产生第二个错误。
    async fn sample_rollout_pods(&self, t: &RolloutTarget) -> Vec<PodSample> {
        let selector = pod_selector(&t.name, &t.component);
        let out = self
            .run_kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{.metadata.name}|{.status.phase}|\
                     {.status.containerStatuses[0].ready}|\
                     {.status.containerStatuses[0].state.waiting.reason}|\
                     {.status.containerStatuses[0].state.waiting.message}|\
                     {.status.containerStatuses[0].state.running.startedAt}|\
                     {.metadata.deletionTimestamp}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
            .unwrap_or_default();
        pod_samples(&out)
    }

    /// 主力容器的就绪探针参数。
    ///
    /// 查询拿到行（哪怕字段全空，即该部署没有就绪探针）就逐格兜底解析：没有探针
    /// 的容器 running 即 ready，本来就不存在"探针还没通过"的窗口，用默认值算出的
    /// 预算不影响结论。查询本身失败则返回 None——**不**退回默认值，因为默认预算
    /// （48s）比真实部署的预算（网关 53s）短，拿它判就等于门禁比 kubelet 更早
    /// 开枪，正是这条判定要消灭的错误。读不到就不判，交回外层 deadline。
    async fn readiness_probe(&self, t: &RolloutTarget) -> Option<ProbeCycle> {
        let out = self
            .run_kubectl(
                &[
                    "get",
                    "deployment",
                    &t.deployment,
                    "-o",
                    "jsonpath={.spec.template.spec.containers[0].readinessProbe.initialDelaySeconds}|\
                     {.spec.template.spec.containers[0].readinessProbe.periodSeconds}|\
                     {.spec.template.spec.containers[0].readinessProbe.timeoutSeconds}|\
                     {.spec.template.spec.containers[0].readinessProbe.failureThreshold}",
                ],
                30,
            )
            .await
            .ok()?;
        // 空输出是查询没生效（路径写错、对象不存在），不是「这个部署没有探针」：
        // 后者仍会返回一行的空字段（竖线占位）。空输出同样不判。
        if out.trim().is_empty() {
            return None;
        }
        Some(ProbeCycle::parse(&out))
    }

    /// 就绪门禁判败的错误：说明判败依据是探针自己的判定周期用完了，并带上现场。
    async fn readiness_gate_error(&self, t: &RolloutTarget, budget_secs: u64) -> SFError {
        let msg = format!(
            "rollout of deployment/{} converged but its own pod(s) are still not ready after the \
             readiness probe's own budget ({}s = initialDelay + failureThreshold x (period + \
             timeout) + container startup)",
            t.deployment, budget_secs
        );
        self.with_scene(t, msg).await
    }

    /// 滚动超时时采样现场：Pod 相位、容器等待原因/消息、准入被拒的副本集、
    /// 本次部署最近的事件。
    ///
    /// 副本计数（`spec|updated|ready|unavailable`）只给结论不给原因——「Pod
    /// 排不上队一直 Pending」与「容器起来了但一直不 ready」在这个向量里长得
    /// 一模一样，处置却相反。事件窗口只有一小时，不留现场就只能等人回到集群
    /// 去猜，那时连证据都过期了。
    ///
    /// 采样是尽力而为：任何一步取不到都留空，观测失败不变成第二个错误。
    async fn rollout_diagnosis(&self, t: &RolloutTarget) -> String {
        let samples = self.sample_rollout_pods(t).await;
        let replicasets = self.sample_replicaset_failures(t).await;
        // 只取 Warning：正常滚动事件（ScalingReplicaSet 等）说明不了病因，
        // 排不上队与探针不过都落在 Warning 里。
        let events = self
            .run_kubectl(
                &[
                    "get",
                    "events",
                    "--field-selector",
                    "type=Warning",
                    "--sort-by=.lastTimestamp",
                    "-o",
                    "jsonpath={range .items[*]}{.involvedObject.kind}|{.involvedObject.name}|\
                     {.reason}|{.message}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
            .unwrap_or_default();
        let mut out = summarize_pod_states(&samples, Utc::now());
        let rs = replicasets
            .iter()
            .map(|r| {
                let d = r.detail();
                if d.is_empty() {
                    r.name.clone()
                } else {
                    format!("{} {}", r.name, d)
                }
            })
            .collect::<Vec<_>>()
            .join("; ");
        push_diagnosis_segment(&mut out, "replicasets: ", &rs);
        let names: Vec<String> = samples.iter().map(|p| p.name.clone()).collect();
        let ev = summarize_events(&events, &t.deployment, &names, DIAGNOSIS_EVENT_LIMIT);
        push_diagnosis_segment(&mut out, "events: ", &ev);
        out
    }

    /// 本次部署里被调度器判为排不上队的 Pod。判据取调度器写在 Pod 条件上的
    /// `PodScheduled=False / Unschedulable`，与诊断采样走两条路：诊断是给人看
    /// 的现场，这里是机器判据，不能靠读诊断文本反推。
    ///
    /// 采样尽力而为：取不到就返回空（当作"没有这个信号"），观测失败不产生
    /// 第二个错误。
    async fn sample_unschedulable_pods(&self, t: &RolloutTarget) -> Vec<(String, String)> {
        let selector = pod_selector(&t.name, &t.component);
        let out = self
            .run_kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{.metadata.name}|{.status.phase}|\
                     {.status.conditions[?(@.type==\"PodScheduled\")].status}|\
                     {.status.conditions[?(@.type==\"PodScheduled\")].reason}|\
                     {.status.conditions[?(@.type==\"PodScheduled\")].message}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
            .unwrap_or_default();
        unschedulable_pods(&out)
    }

    /// 本次部署里准入被拒的 ReplicaSet（配额超限、校验失败）。判据取 RS 自己
    /// 上报的类型化条件 `ReplicaFailure`，不靠读事件文本反推。
    ///
    /// 单列一条查询的理由：准入被拒时 **Pod 对象根本不存在**，Pod 采样一条都
    /// 取不到，而那条 `FailedCreate` 的 Warning 事件挂在 RS 上，事件过滤只留
    /// 同名 Deployment 与采样到的 Pod，会被整条丢掉。没有这条查询，一次因配额
    /// 拒绝而超时的滚动在记录里只剩副本计数，病因是空的。
    ///
    /// 采样尽力而为：取不到就返回空，观测失败不产生第二个错误。
    async fn sample_replicaset_failures(&self, t: &RolloutTarget) -> Vec<ReplicaSetFailure> {
        let selector = pod_selector(&t.name, &t.component);
        let out = self
            .run_kubectl(
                &[
                    "get",
                    "replicasets",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{.metadata.name}|{.spec.replicas}|\
                     {.status.conditions[?(@.type==\"ReplicaFailure\")].status}|\
                     {.status.conditions[?(@.type==\"ReplicaFailure\")].reason}|\
                     {.status.conditions[?(@.type==\"ReplicaFailure\")].message}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
            .unwrap_or_default();
        replicaset_failures(&out)
    }

    /// 快照部署当前的放置面，供判定「排不上队是不是这次上线改出来的」。
    /// 取整份 `.spec` 归一化（见 normalize_placement_shape）而不是只读某个容器的
    /// requests：能把 Pod 顶出节点的字段远不止 requests。
    async fn current_placement_shape(&self, t: &RolloutTarget) -> SFResult<String> {
        let out = self
            .run_kubectl(
                &["get", "deployment", &t.deployment, "-o", "jsonpath={.spec}"],
                30,
            )
            .await?;
        Ok(normalize_placement_shape(&out))
    }

    /// Pod 健康信号：双标签选择器，查 restartCount/ready/waiting reason。
    /// 致命等待态（拉不到镜像、配置错误、CrashLoop）与非零重启立即判病。
    /// 空输出是选择器失效，不能当健康。
    ///
    /// 只用于 soak 复查：滚动期的「新副本还没就绪」由 wait_rollout_complete
    /// 按探针周期给预算，那里的宽限是有刻度的，这里的 fixed 判病只面对已经
    /// 滚完并静置了一段的目标。
    ///
    /// 查询走 probe：集群不可达时就地重试到就绪预算耗尽，耗尽才带标记返回
    /// 「这一轮什么都没看到」。它**不是**健康结论——把它当结论会让一次抖动
    /// 直接触发回滚，把刚推上去的四部署又拽回旧版。重试预算沿用就绪预算，
    /// 不新开第二套旋钮：它已经是可配的「愿意等多久」上界。
    async fn pods_healthy(&self, t: &RolloutTarget) -> SFResult<()> {
        let selector = pod_selector(&t.name, &t.component);
        let out = self
            .probe(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{' '}{.status.containerStatuses[0].ready}{' '}{.status.containerStatuses[0].state.waiting.reason}{\"\\n\"}{end}",
                ],
                30,
                self.rollout_timeout_secs,
            )
            .await?;
        if out.trim().is_empty() {
            return Err(SFError::Agent(format!(
                "no pods found for selector {selector} (deployment/label mismatch?)"
            )));
        }
        for line in out.lines() {
            let mut parts = line.split_whitespace();
            let restarts: u32 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            let ready = parts.next().unwrap_or("false");
            let waiting_reason = parts.next().unwrap_or("");
            if FATAL_WAITING_REASONS.contains(&waiting_reason) {
                let msg = format!(
                    "pod of deployment/{} in fatal waiting state {waiting_reason}: {line}",
                    t.deployment
                );
                return Err(self.with_scene(t, msg).await);
            }
            if ready != "true" {
                let msg = format!("pod of deployment/{} not ready: {line}", t.deployment);
                return Err(self.with_scene(t, msg).await);
            }
            if restarts > self.restart_threshold {
                let msg = format!(
                    "pod of deployment/{} restarting ({} restarts, threshold {}): {line}",
                    t.deployment, restarts, self.restart_threshold
                );
                return Err(self.with_scene(t, msg).await);
            }
        }
        Ok(())
    }

    /// 给判败记录补上现场：与超时路径同一套采样（Pod 名/相位/ready/等待原因与
    /// 消息/容器已运行时长）。判败是这条判定唯一的产出，只留一串内部采样列
    /// （`0 false`）说不出病因，等人回到集群时事件早已过期。
    async fn with_scene(&self, t: &RolloutTarget, msg: String) -> SFError {
        let diagnosis = self.rollout_diagnosis(t).await;
        if diagnosis.is_empty() {
            SFError::Agent(msg)
        } else {
            SFError::Agent(format!("{msg}; pods: {diagnosis}"))
        }
    }

    /// 先落地支撑清单（如随镜像下发），再按计划顺序滚动四部署，逐部署等
    /// rollout 完成，全滚完后 soak 复查；任一失败把已滚目标反向 set image 回
    /// prev tag（不用 rollout undo——多目标无事务性，undo 还会连带回退
    /// 其他字段）。回滚只回退 image：支撑资源不反向删改（apply 是幂等
    /// upsert，旧镜像配新支撑资源可运行；删改支撑资源反而可能把在跑
    /// 集群打坏）。
    pub async fn run(&self, plan: &RolloutPlan) -> Result<(), RolloutFailure> {
        // 支撑资源先于任何镜像变更就位：新二进制启动依赖的 RBAC/ConfigMap/
        // Service 若晚于滚动落地，新 Pod 会因缺依赖 crashloop 触发无谓回滚。
        // 失败直接中止，此时一个镜像都没动（线上原样）：清单确实坏就归版本类，
        // 但如果根本没看清集群（不可达/工具起不来）就不作版本结论。
        if let Some(dir) = &plan.manifests_dir {
            let support = Path::new(dir).join("support.yaml");
            if support.is_file() && support.metadata().map(|m| m.len() > 0).unwrap_or(false) {
                let support_arg = support.to_string_lossy().to_string();
                info!(manifest = %support_arg, "mainline rollout: applying support manifests");
                self.run_kubectl(&["apply", "-f", &support_arg], 120)
                    .await
                    .map_err(classify_before_any_change)?;
            }
        }
        // 任何镜像变更之前先快照各部署当前镜像作为回滚目标：per-target、
        // 端点零歧义，Legacy 首轮（节点 localhost/cogneva:local）也能精确回退。
        // 快照失败则一次变更都不发生（线上原样）。
        let mut prevs: Vec<(String, String)> = Vec::new();
        let mut prev_shapes: Vec<(String, String)> = Vec::new();
        for t in &plan.targets {
            let img = self
                .current_image(t)
                .await
                .map_err(classify_before_any_change)?;
            // 放置面与镜像一起快照：apply 之后才分得清"排不上队"是这次上线
            // 自己加了排不上的约束（版本的事），还是节点本来就满（不是）。
            let shape = self
                .current_placement_shape(t)
                .await
                .map_err(classify_before_any_change)?;
            info!(deployment = %t.deployment, prev = %img, "mainline rollout: snapshot prev image");
            prevs.push((t.deployment.clone(), img));
            prev_shapes.push((t.deployment.clone(), shape));
        }
        let mut done: Vec<&RolloutTarget> = Vec::new();
        for target in &plan.targets {
            let prev_shape = prev_shapes
                .iter()
                .find(|(d, _)| d == &target.deployment)
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            if let Err(e) = self.apply_target(plan, target).await {
                return self.fail_without_blind_rollback(e, &done, &prevs).await;
            }
            if let Err(e) = self.wait_rollout_complete(target, prev_shape).await {
                done.push(target);
                return self.fail_without_blind_rollback(e, &done, &prevs).await;
            }
            done.push(target);
        }

        info!(
            soak_secs = self.soak_secs,
            "mainline rollout: all targets updated, soaking"
        );
        tokio::time::sleep(Duration::from_secs(self.soak_secs)).await;
        for target in &plan.targets {
            if let Err(e) = self.pods_healthy(target).await {
                return self.fail_without_blind_rollback(e, &done, &prevs).await;
            }
        }
        info!(tag = %plan.tag, "mainline rollout complete and healthy");
        Ok(())
    }

    /// 失败收尾：只有**观测到的版本缺陷**才回滚。三类失败不构成版本结论：
    ///
    /// - 集群访问故障（apiserver 不可达、握手超时）：我们看不到集群，既不足以
    ///   判版本好，回滚也同样要经 apiserver、多半一起失败，而这个动作本身还会
    ///   把一个可能已经正常收敛的版本拽回旧的。
    /// - 集群放不下新 Pod：新 Pod 一个都没起来，谈不上新版本的好坏；回滚要把
    ///   旧镜像重新调度一遍，而它带着同样的放置面，同样排不进去。只有本次
    ///   上线自己改动了放置面时才归版本（那一支带的是普通超时错误，不进这里）。
    /// - 观测工具本身起不来（kubectl 缺失/没有可执行位/被占用）：我们连一次查询
    ///   都没发出去，说不出新版本的好坏；而且回滚只退镜像，修不好一个坏掉的工具
    ///   路径，只会把新版本换掉而又重复失败一轮。
    ///
    /// 三类都让 Job 非零退出（部署器下轮重试），集群保持在刚推上去的新版本上。
    ///
    /// 这里是**唯一**给失败定类别的地方：两支不回滚的判成环境类，其余（含
    /// 回滚过的那一支）判成版本类。类别随 [`RolloutFailure`] 带到 CLI，翻成
    /// 进程退出码交给部署器，部署器据此决定占不占尝试预算。
    async fn fail_without_blind_rollback(
        &self,
        e: SFError,
        done: &[&RolloutTarget],
        prevs: &[(String, String)],
    ) -> Result<(), RolloutFailure> {
        let msg = e.to_string();
        if is_cluster_unreachable(&msg) {
            warn!(
                error = %e,
                "cluster unreachable during the rollout; keeping the new revision (no rollback)"
            );
            return Err(RolloutFailure::environment(e));
        }
        if is_placement_blocked(&msg) {
            warn!(
                error = %e,
                "the scheduler never placed the new pods and this revision did not change the \
                 deployment's placement shape; keeping the new revision (no rollback)"
            );
            return Err(RolloutFailure::environment(e));
        }
        if is_observation_tool_failure(&msg) {
            warn!(
                error = %e,
                "the observation tool itself could not be run; keeping the new revision (no \
                 rollback) — rolling the image back cannot repair a broken tool path"
            );
            return Err(RolloutFailure::environment(e));
        }
        self.rollback(&e, done, prevs).await;
        Err(RolloutFailure::version(e))
    }

    /// 尽力回滚：已滚目标按快照的各自 prev 镜像反向 set image 并等收敛
    /// （不用 rollout undo——多目标无事务性，undo 还会连带回退其他字段）。
    /// 回滚本身失败只 warn（人工介入兜底），不掩盖原始错误。
    async fn rollback(&self, cause: &SFError, done: &[&RolloutTarget], prevs: &[(String, String)]) {
        warn!(
            count = done.len(),
            error = %cause,
            "mainline rollout failed; rolling back"
        );
        for t in done.iter().rev() {
            let Some(prev) = prevs
                .iter()
                .find(|(d, _)| d == &t.deployment)
                .map(|(_, i)| i.as_str())
            else {
                warn!(deployment = %t.deployment, "no prev snapshot; skip rollback");
                continue;
            };
            // 回退前的放置面：与新版本那一侧同一判据，好让"回退的 Pod 也排
            // 不进去"在日志里读成同一件事（节点满了），不被当成回退本身失败。
            let live_shape = self.current_placement_shape(t).await.unwrap_or_default();
            if let Err(e) = self.set_image(t, prev).await {
                warn!(deployment = %t.deployment, error = %e, "rollback set image failed");
                continue;
            }
            if let Err(e) = self.wait_rollout_complete(t, &live_shape).await {
                warn!(deployment = %t.deployment, error = %e, "rollback wait failed");
            }
        }
    }
}

/// `cogneva mainline-rollout` 子命令入口（Job Pod 内执行）。targets 用
/// 内置默认四条（Job 不挂 configmap，与部署侧配置默认值同源）；回滚目标
/// 由 Job 启动时快照各部署当前镜像得到，不通过参数传入。
pub async fn run_rollout_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Job Pod 直接调这个子命令，不经 run_app；不初始化订阅者的话滚动/回滚
    // 日志全部不落，Job 失败时 kubectl logs 是空的，无法诊断。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args: Vec<String> = std::env::args().skip(2).collect();
    let mut tag = String::new();
    let mut ns = "cogneva".to_string();
    let mut soak_secs = 120u64;
    let mut restart_threshold = 1u32;
    let mut timeout = 300u64;
    let mut startup_timeout = 900u64;
    let mut kubectl = "kubectl".to_string();
    let mut manifests_dir: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        let value = |i: usize| -> Result<String, Box<dyn std::error::Error>> {
            args.get(i + 1)
                .cloned()
                .ok_or_else(|| format!("--{} requires a value", args[i]).into())
        };
        match args[i].as_str() {
            "--tag" => {
                tag = value(i)?;
                i += 2;
            }
            "--ns" => {
                ns = value(i)?;
                i += 2;
            }
            "--soak-secs" => {
                soak_secs = value(i)?.parse()?;
                i += 2;
            }
            "--restart-threshold" => {
                restart_threshold = value(i)?.parse()?;
                i += 2;
            }
            "--timeout" => {
                timeout = value(i)?.parse()?;
                i += 2;
            }
            "--startup-timeout" => {
                startup_timeout = value(i)?.parse()?;
                i += 2;
            }
            "--kubectl" => {
                kubectl = value(i)?;
                i += 2;
            }
            "--manifests-dir" => {
                manifests_dir = Some(value(i)?);
                i += 2;
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    if tag.is_empty() {
        return Err("--tag is required".into());
    }
    let cfg = MainlineDeployerConfig::default();
    let mut plan = RolloutPlan::from_config(&cfg, tag);
    plan.manifests_dir = manifests_dir;
    let executor = RolloutExecutor::new(
        kubectl,
        ns,
        soak_secs,
        restart_threshold,
        timeout,
        startup_timeout,
    );
    match executor.run(&plan).await {
        Ok(()) => Ok(()),
        // 环境类失败：用独立退出码告诉部署器"这次失败说不出新版本的好坏"，
        // 让它不把这次失败记进本 rev 的尝试预算。`process::exit` 不走返回路径，
        // 因为 Box<dyn Error> 出去一律是 1，区分不出类别。
        Err(f) if f.class == FailureClass::Environment => {
            tracing::error!(error = %f, exit_code = ROLLOUT_EXIT_ENVIRONMENT, "mainline rollout failed for environment reasons");
            std::process::exit(ROLLOUT_EXIT_ENVIRONMENT);
        }
        Err(f) => Err(Box::new(f)),
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rev12_truncates() {
        assert_eq!(rev12("abcdef0123456789"), "abcdef012345");
        assert_eq!(rev12("short"), "short");
    }

    #[test]
    fn endpoint_host_port_splits_registry_endpoint() {
        assert_eq!(
            endpoint_host_port("cogneva-registry.cogneva.svc.cluster.local:5000"),
            Some(("cogneva-registry.cogneva.svc.cluster.local", 5000))
        );
        assert_eq!(
            endpoint_host_port("reg.local:5000/"),
            Some(("reg.local", 5000))
        );
        assert_eq!(endpoint_host_port("no-port"), None);
        assert_eq!(endpoint_host_port("host:notaport"), None);
    }

    #[test]
    fn pod_diagnosis_drops_empty_waiting_fields_and_keeps_the_cause() {
        let now = Utc::now();
        let out = "p-a|Running|true|||\n\
                   p-b|Pending|false|Unschedulable|0/1 nodes are available: 1 Insufficient memory\n";
        let s = summarize_pod_states(&pod_samples(out), now);
        assert!(s.contains("p-a Running ready"));
        assert!(s.contains("p-b Pending not-ready waiting=Unschedulable (0/1 nodes are available: 1 Insufficient memory)"));
        // 只有 Pending 那个 Pod 带等待原因：空等待字段不落进诊断。
        assert_eq!(s.matches("waiting=").count(), 1);
        // 半行（字段缺失）不进诊断，也不让后面的行顶掉位置。
        assert!(summarize_pod_states(&pod_samples("broken|Pending\n"), now).is_empty());
    }

    /// 判败记录要能分开「新副本还在起来」与「旧副本正在关停」：两者都是
    /// not-ready，处置却相反。前者带运行时长，后者带 terminating 标记。
    #[test]
    fn pod_diagnosis_carries_uptime_and_flags_terminating_replicas() {
        let now = DateTime::parse_from_rfc3339("2026-09-17T15:47:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let out = "gw-7tpmm|Running|false|ContainersNotReady|containers with unready status: [gw]|\
                   2026-09-17T15:46:29Z|\n\
                   gw-old|Running|false|||2026-09-17T12:00:00Z|2026-09-17T15:46:27Z\n";
        let s = summarize_pod_states(&pod_samples(out), now);
        let segs: Vec<&str> = s.split("; ").collect();
        assert_eq!(segs.len(), 2, "{s}");
        // 新副本：起来 32 秒还没就绪，且没有 terminating 标记——这是本次滚动的病情。
        assert!(
            segs[0].contains("gw-7tpmm Running not-ready waiting=ContainersNotReady"),
            "{s}"
        );
        assert!(segs[0].contains("ran=32s"), "{s}");
        assert!(!segs[0].contains("terminating"), "{s}");
        // 旧副本：正在关停，被单独标出，不能与前者混为一谈。
        assert!(segs[1].contains("gw-old Running not-ready"), "{s}");
        assert!(segs[1].contains("ran=13621s"), "{s}");
        assert!(segs[1].contains("terminating"), "{s}");
    }

    /// 就绪门禁的预算从探针配置推出来，不是固定秒数：探针调周期，预算跟着动。
    #[test]
    fn readiness_budget_follows_the_probe_configuration() {
        // 集群实测的三个探针（initialDelay 5 / period 10 / timeout 1 / threshold 3）：
        // 5 + 3 × (10 + 1) + 15 = 53s。网关那个 Pod 起容器到就绪实测 42s，落在里面。
        let live = ProbeCycle {
            initial_delay: 5,
            period: 10,
            timeout: 1,
            failure_threshold: 3,
        };
        assert_eq!(live.budget(), Duration::from_secs(53));
        assert_eq!(ProbeCycle::parse("5|10|1|3"), live);
        // 探针调慢，预算跟着长；探针调快，预算跟着短。四项都得进预算：
        // 少算 initialDelay 或 timeout 都会让门禁窄于 kubelet 自己的判定周期。
        let slower = ProbeCycle { period: 30, ..live };
        assert_eq!(slower.budget(), Duration::from_secs(5 + 3 * 31 + 15));
        assert!(slower.budget() > live.budget());
        let no_delay = ProbeCycle {
            initial_delay: 0,
            ..live
        };
        assert!(no_delay.budget() < live.budget());
        let longer_timeout = ProbeCycle { timeout: 5, ..live };
        assert!(longer_timeout.budget() > live.budget());
        let eager = ProbeCycle {
            initial_delay: 0,
            period: 1,
            timeout: 1,
            failure_threshold: 1,
        };
        assert_eq!(eager.budget(), Duration::from_secs(17));
        assert!(eager.budget() < live.budget());
    }

    /// 读不到探针配置（没有就绪探针、查询失败）时逐格兜底到 k8s 默认探针同值，
    /// 而不是整体退化成一个拍出来的常数。
    #[test]
    fn a_missing_probe_configuration_falls_back_per_field() {
        // 完全没有探针：四格都空。
        assert_eq!(ProbeCycle::parse("|||"), ProbeCycle::DEFAULT);
        assert_eq!(ProbeCycle::parse(""), ProbeCycle::DEFAULT);
        assert_eq!(ProbeCycle::DEFAULT.budget(), Duration::from_secs(48));
        // 只写了部分字段：缺的那格单独兜底，已有的那格照样生效。
        assert_eq!(
            ProbeCycle::parse("|30|1|3"),
            ProbeCycle {
                period: 30,
                ..ProbeCycle::DEFAULT
            }
        );
        assert_eq!(
            ProbeCycle::parse("|||5").failure_threshold,
            5,
            "写了一格就取那一格"
        );
    }

    /// 预算内不算失败、预算耗尽才判败，且起算点是「最近启动的那个未就绪副本」。
    #[test]
    fn readiness_overdue_only_fires_after_the_containers_own_budget() {
        let budget = ProbeCycle::DEFAULT.budget();
        let started = DateTime::parse_from_rfc3339("2026-09-17T15:46:29Z")
            .unwrap()
            .with_timezone(&Utc);
        let row = |name: &str, ready: bool, started_at: &str, deleting: &str| {
            format!(
                "{name}|Running|{ready}|||{started_at}|{deleting}\n",
                ready = if ready { "true" } else { "false" }
            )
        };
        let at = |secs: i64| started + chrono::Duration::seconds(secs);

        // 起了 32 秒：预算（兜底探针 48s）还没用完——正是那个被误杀的版本当时的处境。
        let fresh = pod_samples(&row("gw-1", false, "2026-09-17T15:46:29Z", ""));
        assert!(!readiness_overdue(&fresh, budget, at(32)));
        // 就用满预算那一刻才算超。
        assert!(!readiness_overdue(&fresh, budget, at(47)));
        assert!(readiness_overdue(&fresh, budget, at(48)));
        assert!(readiness_overdue(&fresh, budget, at(600)));

        // 起算点取最近的未就绪副本：更老的副本不能把新副本拖进超时。
        let mixed = pod_samples(&format!(
            "{}{}",
            row("gw-old", false, "2026-09-17T15:00:00Z", ""),
            row("gw-new", false, "2026-09-17T15:46:29Z", "")
        ));
        assert!(!readiness_overdue(&mixed, budget, at(32)));

        // 已经就绪的副本不参与起算；正在关停的旧副本同样不参与。
        let ready = pod_samples(&row("gw-1", true, "2026-09-17T15:00:00Z", ""));
        assert!(!readiness_overdue(&ready, budget, at(3600)));
        let terminating = pod_samples(&row(
            "gw-old",
            false,
            "2026-09-17T15:00:00Z",
            "2026-09-17T15:46:27Z",
        ));
        assert!(!readiness_overdue(&terminating, budget, at(3600)));
        // 容器还没起来（无 startedAt）：不起算，交给外层预算。
        let not_started = pod_samples(&row("gw-1", false, "", ""));
        assert!(!readiness_overdue(&not_started, budget, at(600)));
    }

    /// 收敛必须归到本次滚动自己的副本上：旧副本 Terminating 但还 ready 不能算数，
    /// 它正是把「新副本还没就绪」凑成「滚完了」的那一半。
    #[test]
    fn convergence_only_counts_this_rollouts_own_replicas() {
        let rows = |s: &str| pod_samples(s);
        // 只有旧副本（Terminating，还 ready）：没有本次滚动的副本可判，不给结论。
        assert_eq!(
            rollout_pods_ready(&rows(
                "gw-old|Running|true|||2026-09-17T15:00:00Z|2026-09-17T15:46:27Z\n"
            )),
            None
        );
        // 采样取不到（查询失败/选择器失效）：空集合是「没有证据」，不是「健康」。
        assert_eq!(rollout_pods_ready(&[]), None);
        // 旧副本 ready + 新副本 not-ready：这曾经凑成假收敛，必须判未就绪。
        let mixed = rows(
            "gw-old|Running|true|||2026-09-17T15:00:00Z|2026-09-17T15:46:27Z\n\
             gw-new|Running|false|ContainersNotReady|x|2026-09-17T15:46:29Z|\n",
        );
        assert_eq!(rollout_pods_ready(&mixed), Some(false));
        // 新副本就绪、旧副本还在关停：本次滚动自己的副本齐了，算收敛。
        let done = rows(
            "gw-old|Running|false|||2026-09-17T15:00:00Z|2026-09-17T15:46:27Z\n\
             gw-new|Running|true|||2026-09-17T15:46:29Z|\n",
        );
        assert_eq!(rollout_pods_ready(&done), Some(true));
    }

    /// 必死等待态立即判败（不必等预算烧完），且不认正在关停的旧副本。
    #[test]
    fn fatal_pods_are_caught_immediately_and_only_on_own_replicas() {
        assert_eq!(
            rollout_pods_fatal(&pod_samples(
                "gw-1|Running|false|CrashLoopBackOff|back-off|2026-09-17T15:46:29Z|\n"
            )),
            Some("gw-1 waiting=CrashLoopBackOff".to_string())
        );
        // 旧副本的必死态不是本次滚动的病情：它正在被替换掉。
        assert_eq!(
            rollout_pods_fatal(&pod_samples(
                "gw-old|Running|false|CrashLoopBackOff|x|2026-09-17T15:00:00Z|2026-09-17T15:46:27Z\n"
            )),
            None
        );
        // 正在起来（等待原因是正常的创建中）不算必死。
        assert_eq!(
            rollout_pods_fatal(&pod_samples("gw-1|Pending|false|ContainerCreating|||\n")),
            None
        );
        assert_eq!(
            rollout_pods_fatal(&pod_samples("gw-1|Running|true|||2026-09-17T15:46:29Z|\n")),
            None
        );
    }

    #[test]
    fn unschedulable_pods_takes_the_scheduler_verdict_only() {
        let out = "p-a|Running|True||\n\
                   p-b|Pending|False|Unschedulable|0/1 nodes are available: 1 Insufficient memory\n\
                   p-c|Pending|False|NotReady|some other condition\n\
                   p-d|Pending|||\n\
                   p-e|Pending|False|Unschedulable|\n";
        let got = unschedulable_pods(out);
        // 只有被调度器判为 Unschedulable 的整行进判据。调度器没给 message 的那条
        // （p-e）照样算数：判据在 reason 上，不在字段个数上。
        assert_eq!(
            got,
            vec![
                (
                    "p-b".to_string(),
                    "0/1 nodes are available: 1 Insufficient memory".to_string()
                ),
                ("p-e".to_string(), String::new()),
            ]
        );
        // 字段残缺、名字为空、已调度（True）、别的 reason、条件未上报一律不算。
        assert!(unschedulable_pods("p-f|Pending|False\n").is_empty());
        assert!(unschedulable_pods("|Pending|False|Unschedulable|\n").is_empty());
        assert!(unschedulable_pods("p-a|Running|True||\n").is_empty());
        assert!(unschedulable_pods("").is_empty());
    }

    #[test]
    fn replicaset_failures_take_this_rollouts_admission_rejection_only() {
        let out = "gw-new|1|True|FailedCreate|pods \"gw-new-x\" is forbidden: exceeded quota: cogneva-quota\n\
                   gw-old|0|True|FailedCreate|pods \"gw-old-x\" is forbidden: exceeded quota: cogneva-quota\n\
                   gw-healed|1|False|FailedCreate|pods \"gw-healed-x\" is forbidden\n\
                   gw-ok|1|||\n";
        let got = replicaset_failures(out);
        // 只有「期望副本数不为零 + 条件为 True」的那条进现场：旧 RS 的失败是
        // 上一轮的账，条件被翻回 False 说明那次拒绝已过去。
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "gw-new");
        assert_eq!(got[0].reason, "FailedCreate");
        assert_eq!(
            got[0].detail(),
            "FailedCreate: pods \"gw-new-x\" is forbidden: exceeded quota: cogneva-quota"
        );
        // 条件里没给 message 时只留 reason，不留 `FailedCreate: ` 这样的空尾巴。
        let no_msg = replicaset_failures("gw-nodesired|1|True|FailedCreate|\n");
        assert_eq!(no_msg[0].detail(), "FailedCreate");
        // 残行、名字为空、副本数读不出、查询整体为空：一律不算。
        assert!(replicaset_failures("gw-x|1|True\n").is_empty());
        assert!(replicaset_failures("|1|True|FailedCreate|m\n").is_empty());
        assert!(replicaset_failures("gw-x||True|FailedCreate|m\n").is_empty());
        assert!(replicaset_failures("").is_empty());
    }

    #[test]
    fn environment_class_needs_an_unchanged_placement_shape() {
        let blocked = vec![(
            "cogneva-abc-x".to_string(),
            "0/1 nodes are available: 1 Insufficient memory".to_string(),
        )];
        let shape = "{\"replicas\":1,\"template\":{\"spec\":{\"containers\":[{}]}}}";
        // 调度器判了排不上队 + 这次上线没动放置面 ⇒ 环境类，给出 Pod 与消息。
        let v = environment_class(&blocked, shape, Some(shape)).expect("environment class");
        assert!(v.contains("cogneva-abc-x"));
        assert!(v.contains("Insufficient memory"));
        // 放置面变了 ⇒ 版本类，照常回滚。
        assert!(environment_class(&blocked, shape, Some("{\"replicas\":2}")).is_none());
        // 读不到当前放置面、或快照本身是空的 ⇒ 判不准就往版本侧靠。
        assert!(environment_class(&blocked, shape, None).is_none());
        assert!(environment_class(&blocked, "", Some("")).is_none());
        // 没有排不上队这个信号 ⇒ 这条超时与放置无关。
        assert!(environment_class(&[], shape, Some(shape)).is_none());
    }

    /// 这条是「集群放不下」判定面的核心回归锁：能把 Pod 顶出节点的改动远不止
    /// 某个容器的 requests。只比 requests 会把「新版本给自己加了排不上的约束」
    /// 误判成「集群满了」，于是不回滚一个真的坏了的版本——那是全量停机。
    #[test]
    fn placement_shape_only_ignores_the_image_and_restart_stamps() {
        let base = r#"{"replicas":1,"template":{"metadata":{"annotations":{"cogneva.io/restartedAt":"1"},"labels":{"app":"x"}},"spec":{"nodeSelector":{"disk":"ssd"},"containers":[{"name":"c","image":"old","resources":{"requests":{"cpu":"100m"}}}]}}}"#;
        let new_image = base.replace("\"image\":\"old\"", "\"image\":\"new\"");
        let new_stamp = base.replace("\"restartedAt\":\"1\"", "\"restartedAt\":\"2\"");
        assert_eq!(
            normalize_placement_shape(base),
            normalize_placement_shape(&new_image)
        );
        // 重启戳与配置校验和是纯噪声，改动它不构成版本变化。
        assert_eq!(
            normalize_placement_shape(base),
            normalize_placement_shape(&new_stamp)
        );
        // 其余任何一处改动都必须留下差异：requests、nodeSelector、多一个侧车、
        // replicas、maxSurge——它们都能让新 Pod 排不进去而 requests 一个字不变。
        for changed in [
            base.replace("\"cpu\":\"100m\"", "\"cpu\":\"2\""),
            base.replace("\"disk\":\"ssd\"", "\"disk\":\"hdd\""),
            base.replace(
                "{\"name\":\"c\"",
                "{\"name\":\"side\",\"image\":\"s\"},{\"name\":\"c\"",
            ),
            base.replace("\"replicas\":1", "\"replicas\":2"),
            base.replace(
                "\"spec\":{",
                "\"spec\":{\"affinity\":{\"nodeAffinity\":{}},",
            ),
            base.replace(
                "\"replicas\":1",
                "\"replicas\":1,\"strategy\":{\"rollingUpdate\":{\"maxSurge\":1}}",
            ),
        ] {
            assert_ne!(
                normalize_placement_shape(base),
                normalize_placement_shape(&changed),
                "改动必须留下差异: {changed}"
            );
        }
        // 解析不了就原样返回：非空即与快照不同，判不准往版本侧靠。
        assert_eq!(normalize_placement_shape(" not json "), "not json");
        assert_eq!(normalize_placement_shape(""), "");
        // 归一化结果里不再有镜像字段。
        assert!(!normalize_placement_shape(base).contains("image"));
    }

    #[test]
    fn placement_marker_is_recognized_and_distinct_from_unreachable() {
        let e = format!("{PLACEMENT_BLOCKED_MARKER}: rollout of deployment/x did not complete");
        assert!(is_placement_blocked(&e));
        // 放不下与看不到是两回事：一个说集群满了，一个说我们没看见集群，
        // 判据不能互相命中，否则其中一类的处置会被另一类借走。
        assert!(!is_cluster_unreachable(&e));
        assert!(!is_placement_blocked(&format!(
            "{CLUSTER_UNREACHABLE_MARKER}: never observed"
        )));
    }

    /// 观测工具起不来是第三类，谁也不许借走它的判定：它既不是"看不到集群"
    /// （那一类会按轮询节拍重试，而工具起不来重试多少次都一样），也不是版本结论。
    #[test]
    fn the_observation_tool_marker_is_its_own_class() {
        let e = format!(
            "{OBSERVATION_TOOL_MARKER}: cannot run /tmp/fake-kubectl: Permission denied (os error 13)"
        );
        assert!(is_observation_tool_failure(&e));
        assert!(!is_cluster_unreachable(&e));
        assert!(!is_placement_blocked(&e));
        // 真实 exec 失败文本里没有任何访问故障措辞，所以它必须靠自己的标记被认出。
        assert!(!is_cluster_unreachable(
            "failed to run kubectl: Text file busy (os error 26)"
        ));
    }

    /// 采样行里的 Pod 名，供事件相关性过滤用（诊断走的是同一条：从 PodSample 取名字）。
    fn pod_names(pods: &str) -> Vec<String> {
        pod_samples(pods).iter().map(|p| p.name.clone()).collect()
    }

    #[test]
    fn events_diagnosis_keeps_only_this_deployment() {
        let out = "\
Deployment|other-deployment|ScalingReplicaSet|unrelated scale
Pod|other-app-abc-z|FailedScheduling|unrelated scheduling
Pod|cogneva-sandbox-executor-abc-x|FailedScheduling|0/1 nodes are available: 1 Insufficient memory
Pod|cogneva-sandbox-executor-abc-y|Unhealthy|Readiness probe failed
";
        let pods = "cogneva-sandbox-executor-abc-x|Pending|false||\n\
                    cogneva-sandbox-executor-abc-y|Running|false||\n";
        let s = summarize_events(out, "cogneva-sandbox-executor", &pod_names(pods), 3);
        assert!(s.contains("FailedScheduling"));
        assert!(s.contains("Readiness probe failed"));
        assert!(!s.contains("unrelated"));
    }

    #[test]
    fn events_diagnosis_keeps_the_latest_bounded_number() {
        let out = "\
Pod|cogneva-sandbox-executor-abc-x|FailedScheduling|oldest
Pod|cogneva-sandbox-executor-abc-x|Unhealthy|middle
Pod|cogneva-sandbox-executor-abc-y|BackOff|newest
";
        let pods = "cogneva-sandbox-executor-abc-x|Pending|false||\n\
                    cogneva-sandbox-executor-abc-y|Running|false||\n";
        let s = summarize_events(out, "cogneva-sandbox-executor", &pod_names(pods), 2);
        assert!(!s.contains("oldest"));
        assert!(s.contains("middle"));
        assert!(s.contains("newest"));
    }

    /// 前缀匹配会把 `cogneva` 的前缀套到 `cogneva-evolution` /
    /// `cogneva-sandbox-executor` 的全部 Pod 上，把别的部署的病因记到本次滚动
    /// 头上。相关性必须按采样到的确切 Pod 名判。
    #[test]
    fn events_diagnosis_does_not_borrow_other_deployments_pods() {
        let out = "\
Pod|cogneva-evolution-abc-x|BackOff|evolution crashed
Pod|cogneva-sandbox-executor-abc-y|Unhealthy|executor probe failed
";
        let pods = "cogneva-5c75d664c6-8hq7t|Running|true||\n";
        let s = summarize_events(out, "cogneva", &pod_names(pods), 5);
        assert!(s.is_empty(), "{s}");
    }

    /// Deployment 自身的事件（ScalingReplicaSet 等）不因名字不是 Pod 名而被丢。
    #[test]
    fn events_diagnosis_keeps_the_deployment_own_events() {
        let out = "Deployment|cogneva-sandbox-executor|ScalingReplicaSet|Scaled up replica set\n";
        let s = summarize_events(out, "cogneva-sandbox-executor", &[], 5);
        assert!(s.contains("Scaled up replica set"));
    }

    #[test]
    fn manifest_helpers_split_single_manifest_from_index() {
        let single: serde_json::Value = serde_json::json!({"config": {"digest": "sha256:cfg1"}});
        assert_eq!(config_digest_of(&single).as_deref(), Some("sha256:cfg1"));
        assert_eq!(first_manifest_digest(&single), None);

        let index: serde_json::Value = serde_json::json!({
            "manifests": [{"digest": "sha256:plat"}, {"digest": "sha256:plat2"}]
        });
        assert_eq!(config_digest_of(&index), None);
        assert_eq!(
            first_manifest_digest(&index).as_deref(),
            Some("sha256:plat")
        );

        assert_eq!(config_digest_of(&serde_json::json!({"layers": []})), None);
    }

    #[test]
    fn revision_label_read_from_config_blob() {
        let blob = serde_json::json!({
            "config": {"Labels": {"org.opencontainers.image.revision": "deadbeefcafe"}}
        });
        assert_eq!(
            revision_of_config_blob(&blob).as_deref(),
            Some("deadbeefcafe")
        );
        assert_eq!(
            revision_of_config_blob(&serde_json::json!({"config": {"Labels": {}}})),
            None
        );
        assert_eq!(revision_of_config_blob(&serde_json::json!({})), None);
    }

    #[test]
    fn http_response_body_cut_at_content_length() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n{\"a\":1}trailing";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"{\"a\":1}");

        // 无 Content-Length（或短于实际）时取剩余全部——registry 的响应
        // 一律带长度，这条只是不让解析在异常响应上 panic。
        let raw = b"HTTP/1.1 404 Not Found\r\n\r\nnope";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 404);
        assert_eq!(body, b"nope");
    }

    #[test]
    fn floating_pin_convergence_compares_content_not_the_tag_string() {
        let bare = "4dfd51ff1209abcdef";
        assert!(floating_pin_is_converged(Some("4dfd51ff1209abcdef"), bare));
        // registry 上的内容标签是短 id 也能对上。
        assert!(floating_pin_is_converged(Some("4dfd51ff1209"), bare));
        // 浮动签被重新播种成别的 rev：清单里 tag 一个字没变，但没收敛。
        assert!(!floating_pin_is_converged(Some("000000000000abcd"), bare));
        // 读不到内容版本一律不当作已收敛。
        assert!(!floating_pin_is_converged(None, bare));
    }

    #[test]
    fn only_main_prefixed_tags_imply_a_rev() {
        assert!(tag_is_immutable_for_rev("main-4dfd51ff1209"));
        assert!(!tag_is_immutable_for_rev("local"));
        assert!(!tag_is_immutable_for_rev("promote-p-1"));
    }

    #[test]
    fn heartbeat_message_summarizes_idle_state() {
        let state = MainlineState {
            last_good_tag: Some("cogneva:main-4dfd51ff1209".into()),
            last_good_rev: Some("4dfd51ff1209abcdef".into()),
            in_flight: None,
            failed_rev: None,
            failed_cooldown_until: 0,
            failed_attempts: 0,
            failed_class: FailureClass::Version,
        };
        let msg = heartbeat_message(&state, "4dfd51ff1209abcdef", 100);
        assert!(msg.contains("bare=4dfd51ff1209"), "{msg}");
        assert!(msg.contains("last_good=4dfd51ff1209"), "{msg}");
        assert!(msg.contains("in_flight=none"), "{msg}");
        assert!(msg.contains("failed_rev=none"), "{msg}");
        assert!(msg.contains("failed_class=none"), "{msg}");
        assert!(msg.contains("failed_attempts=0"), "{msg}");
        assert!(msg.contains("cooldown_remaining_secs=0"), "{msg}");
    }

    #[test]
    fn heartbeat_message_shows_inflight_and_cooldown() {
        let state = MainlineState {
            last_good_tag: None,
            last_good_rev: None,
            in_flight: Some(InFlight {
                rev: "aabbccddeeff0011".into(),
                phase: Phase::Pushed,
            }),
            failed_rev: Some("112233445566aabb".into()),
            failed_cooldown_until: 1500,
            failed_attempts: 2,
            failed_class: FailureClass::Version,
        };
        let msg = heartbeat_message(&state, "aabbccddeeff0011", 1000);
        assert!(msg.contains("in_flight=aabbccddeeff@Pushed"), "{msg}");
        assert!(msg.contains("failed_rev=112233445566"), "{msg}");
        assert!(msg.contains("failed_class=version"), "{msg}");
        assert!(msg.contains("failed_attempts=2"), "{msg}");
        assert!(msg.contains("cooldown_remaining_secs=500"), "{msg}");
    }

    /// 环境类失败不计尝试次数，`failed_rev` 与 `failed_attempts=0` 会同时出现：
    /// 心跳必须说出类别，否则这一行读起来像记账坏了。
    #[test]
    fn heartbeat_message_names_an_environment_class_failure() {
        let state = MainlineState {
            failed_rev: Some("112233445566aabb".into()),
            failed_cooldown_until: 1500,
            failed_attempts: 0,
            failed_class: FailureClass::Environment,
            ..Default::default()
        };
        let msg = heartbeat_message(&state, "112233445566aabb", 1000);
        assert!(msg.contains("failed_rev=112233445566"), "{msg}");
        assert!(msg.contains("failed_class=environment"), "{msg}");
        assert!(msg.contains("failed_attempts=0"), "{msg}");
    }

    #[test]
    fn heartbeat_message_survives_unreadable_bare_rev() {
        // 心跳本身绝不能成为故障源：bare 读取失败时降级为占位文本。
        let msg = heartbeat_message(&MainlineState::default(), "unreadable(git failed)", 0);
        assert!(msg.contains("bare=unreadable("), "{msg}");
        assert!(msg.contains("last_good=none"), "{msg}");
    }

    #[test]
    fn image_refs_and_rev_parsing() {
        let img = main_image("cogneva-registry.cogneva.svc:5000", "abcdef0123456789");
        assert_eq!(
            img,
            "cogneva-registry.cogneva.svc:5000/cogneva:main-abcdef012345"
        );
        assert_eq!(parse_main_rev(&img), Some("abcdef012345"));
        assert_eq!(
            parse_main_rev("localhost/cogneva:local"),
            None,
            "node-local floating tag must not parse as mainline"
        );
        assert_eq!(parse_main_rev("cogneva-registry:5000/cogneva:local"), None);
        assert_eq!(
            parse_main_rev("cogneva-registry:5000/cogneva:promote-p-1"),
            None
        );
        assert_eq!(local_image("reg:5000/"), "reg:5000/cogneva:local");
        assert_eq!(
            job_name("abcdef0123456789"),
            "cogneva-mainline-abcdef012345"
        );
    }

    #[test]
    fn selector_uses_both_labels() {
        let s = pod_selector("cogneva", "gateway");
        assert_eq!(
            s,
            "app.kubernetes.io/name=cogneva,app.kubernetes.io/component=gateway"
        );
    }

    #[test]
    fn classify_deployed_states() {
        let reg = "r:5000";
        let main = |rev: &str| main_image(reg, rev);
        // 统一主线
        assert_eq!(
            classify_deployed(&[main("aa1111111111"), main("aa1111111111")]),
            DeployedState::Main("aa1111111111".into())
        );
        // 全非主线（迁移前）
        assert_eq!(
            classify_deployed(&[
                "localhost/cogneva:local".into(),
                "localhost/cogneva:local".into()
            ]),
            DeployedState::Legacy
        );
        // 混合
        assert_eq!(
            classify_deployed(&[main("aa1111111111"), "localhost/cogneva:local".into()]),
            DeployedState::Mixed
        );
        // 主线 rev 不一致（滚动未收敛）
        assert_eq!(
            classify_deployed(&[main("aa1111111111"), main("bb2222222222")]),
            DeployedState::Mixed
        );
    }

    /// 混合态只在真有滚动在飞时才算"上一轮未收敛"；无在飞滚动时是外部写入造成的
    /// 非一致，必须归一放行，否则部署器永久停摆。
    #[test]
    fn mixed_state_only_blocks_while_a_rollout_is_in_flight() {
        let mixed = DeployedState::Mixed;
        assert_eq!(
            normalize_deployed(&mixed, true),
            DeployedState::Mixed,
            "有在飞滚动：保持原义，绝不叠加新一轮"
        );
        assert_eq!(
            normalize_deployed(&mixed, false),
            DeployedState::Legacy,
            "无在飞滚动：外部写入的非一致，放行让本轮把它收敛回单一 rev"
        );
        // 归一不动其它态：正常主线不会被降级成 Legacy。
        assert_eq!(
            normalize_deployed(&DeployedState::Main("aa1111111111".into()), false),
            DeployedState::Main("aa1111111111".into())
        );
        assert_eq!(
            normalize_deployed(&DeployedState::Legacy, true),
            DeployedState::Legacy
        );
    }

    #[test]
    fn advance_decisions() {
        let bare = "bb2222222222";
        let main = |r: &str| DeployedState::Main(r.into());
        let now = 1000i64;
        let other = main("aa1111111111");
        let retry = |cooldown_until, retry_of_failed_rev, attempts| RetryBudget {
            cooldown_until,
            retry_of_failed_rev,
            attempts,
            max_attempts: 2,
        };
        // 同 rev 不前进
        assert_eq!(
            evaluate_advance(bare, &main(bare), true, now, retry(0, false, 0)),
            AdvanceDecision::SameRev
        );
        // 非祖先（分叉/倒退）拒
        assert_eq!(
            evaluate_advance(bare, &other, false, now, retry(0, false, 0)),
            AdvanceDecision::NotAncestor
        );
        // 冷却挡的是"重试刚失败的这个 rev"，由 failed_rev 判而非 attempts：
        // attempts=0 正是环境类失败（不占预算）的形状，它同样要被冷却挡住。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(2000, true, 0)),
            AdvanceDecision::InCooldown
        );
        // 版本类失败同样在冷却窗内被挡
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(2000, true, 1)),
            AdvanceDecision::InCooldown
        );
        // 冷却窗内推进到新 rev（fix-forward）不挡：新提交可能正是修复
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(2000, false, 0)),
            AdvanceDecision::Advance
        );
        // 环境类失败重试不占预算：冷却过了就照常再试，不会撞上尝试上限。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(0, true, 0)),
            AdvanceDecision::Advance
        );
        // 超次数
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(0, true, 2)),
            AdvanceDecision::MaxAttempts
        );
        // 正常前进
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(0, true, 1)),
            AdvanceDecision::Advance
        );
        // 迁移首轮（Legacy）直接前进
        assert_eq!(
            evaluate_advance(bare, &DeployedState::Legacy, false, now, retry(0, false, 0)),
            AdvanceDecision::Advance
        );
        // 混合态不前进
        assert_eq!(
            evaluate_advance(bare, &DeployedState::Mixed, true, now, retry(0, false, 0)),
            AdvanceDecision::Mixed
        );
    }

    #[test]
    fn lock_staleness() {
        assert!(!lock_is_stale(10, 3600, true));
        assert!(lock_is_stale(4000, 3600, true), "age over timeout is stale");
        assert!(lock_is_stale(10, 3600, false), "dead pid is stale");
    }

    #[test]
    fn state_serde_roundtrip() {
        let state = MainlineState {
            last_good_tag: Some("r:5000/cogneva:main-aa".into()),
            last_good_rev: Some("aa".into()),
            in_flight: Some(InFlight {
                rev: "bb".into(),
                phase: Phase::Pushed,
            }),
            failed_rev: None,
            failed_cooldown_until: 0,
            failed_attempts: 0,
            failed_class: FailureClass::Environment,
        };
        let text = serde_json::to_string(&state).unwrap();
        let back: MainlineState = serde_json::from_str(&text).unwrap();
        assert_eq!(state, back);
    }

    #[test]
    fn rollout_plan_order_evolution_last() {
        let cfg = MainlineDeployerConfig::default();
        let plan = RolloutPlan::from_config(&cfg, "new".into());
        assert_eq!(plan.targets.len(), 4);
        assert_eq!(plan.targets[0].deployment, "cogneva-security-gateway");
        assert_eq!(plan.targets[1].deployment, "cogneva-sandbox-executor");
        assert_eq!(plan.targets[2].deployment, "cogneva");
        assert_eq!(plan.targets[3].deployment, "cogneva-evolution");
    }

    // --- 命令层测试：真实 git 仓库 + fake buildah/kubectl/cargo/strip ---

    use crate::test_support::write_executable as write_fake_bin;

    async fn real_git(dir: &Path, args: &[&str]) {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// 搭 bare + 工作树：工作树在 rev A，bare/main 在 rev B（A 是 B 祖先）。
    /// 返回 (bare_dir, work_dir, rev_a, rev_b)。
    async fn setup_repos(root: &Path) -> (PathBuf, PathBuf, String, String) {
        let bare = root.join("bare.git");
        let work = root.join("work");
        real_git(root, &["init", "--bare", bare.to_str().unwrap()]).await;
        real_git(
            root,
            &["clone", bare.to_str().unwrap(), work.to_str().unwrap()],
        )
        .await;
        real_git(&work, &["config", "user.email", "t@t.com"]).await;
        real_git(&work, &["config", "user.name", "T"]).await;
        std::fs::write(work.join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::create_dir_all(work.join("crates/cog-storage/migrations")).unwrap();
        std::fs::write(work.join("crates/cog-storage/migrations/001.sql"), "").unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "a"]).await;
        // clone 后默认分支名随 git 配置（master/main），统一推到 bare 的 main。
        real_git(&work, &["push", "origin", "HEAD:main"]).await;
        real_git(&work, &["checkout", "-B", "main"]).await;
        real_git(&work, &["remote", "add", "local", bare.to_str().unwrap()]).await;
        let rev_a = real_git_stdout(&work, &["rev-parse", "HEAD"]).await;

        std::fs::write(work.join("lib.rs"), "fn b() {}\n").unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "b"]).await;
        real_git(&work, &["push", "origin", "main"]).await;
        let rev_b = real_git_stdout(
            &bare,
            &["--git-dir", bare.to_str().unwrap(), "rev-parse", "main"],
        )
        .await;
        // 工作树回到 A（模拟已部署 A，bare 前进到 B）。
        real_git(&work, &["reset", "--hard", &rev_a]).await;
        (bare, work, rev_a, rev_b)
    }

    async fn real_git_stdout(dir: &Path, args: &[&str]) -> String {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 命令层测试会改进程级 PATH（cargo/strip 靠 PATH 查找），用静态锁串行化，
    /// 避免并行测试互相串改环境。用异步锁是因为持有期必须覆盖被测命令的 await，
    /// 同步锁守卫跨 await 持锁会触发 clippy::await_holding_lock。
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// fake buildah：记录调用，from 输出容器名，run --version 输出带目标 rev
    /// 的版本串（rev 直接写进脚本，不走进程 env，避免并行竞态）。
    fn fake_buildah(dir: &Path, version_rev: &str) -> String {
        let log = dir.join("buildah.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "from" ]; then echo "ctr-test-123"; exit 0; fi
  if [ "$prev" = "run" ]; then echo "cogneva 0.5.7 (rev {version_rev})"; exit 0; fi
  prev="$a"
done
exit 0
"#,
            log = log.display(),
            version_rev = version_rev
        );
        write_fake_bin(dir, "fake-buildah", &script);
        dir.join("fake-buildah").to_string_lossy().to_string()
    }

    /// fake kubectl：deployment 镜像查询输出写死的 deployed_image（四部署同值），
    /// job 查询报 NotFound，apply 把 stdin 的 manifest 也落日志（job 名在
    /// manifest 里，不在 argv），其余成功。`-o` 参数必须带 `jsonpath=` 前缀
    /// （真 kubectl 对裸模板报 "unable to match a printer"，fake 同样拒绝，
    /// 否则这类漏前缀单测抓不到）。
    fn fake_kubectl(dir: &Path, deployed_image: &str) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get deployment"*) echo "{deployed_image}" ;;
  *"get job"*) echo "Error: jobs.batch \"x\" not found" >&2; exit 1 ;;
  *"get pods"*) echo "0 true " ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            deployed_image = deployed_image
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// fake kubectl：按 deployment 名字分别返回镜像，用来构造"四部署镜像不一致"
    /// 的现场（清单被部分重下发 / 手工 set image）。名字后的空格是必要边界：
    /// `cogneva ` 不会匹配上 `cogneva-evolution `。
    fn fake_kubectl_per_deployment(dir: &Path, first: &str, second: &str) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get deployment cogneva-security-gateway "*) echo '{first}' ;;
  *"get deployment cogneva "*) echo '{first}' ;;
  *"get deployment "*" jsonpath="*) echo '{second}' ;;
  *"get job"*) echo "Error: jobs.batch \"x\" not found" >&2; exit 1 ;;
  *"get pods"*) echo "0 true " ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            first = first,
            second = second
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// fake kubectl：pods 查询输出从文件读（测试逐例改写文件模拟不同 Pod 态）。
    /// `-o` 同样强制 jsonpath= 前缀（见 fake_kubectl 注释）。
    fn fake_kubectl_pods_from_file(dir: &Path, pods_file: &Path) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get pods"*) cat '{pods_file}' ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            pods_file = pods_file.display()
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    fn test_config(
        root: &Path,
        bare: &Path,
        buildah: &str,
        kubectl: &str,
    ) -> MainlineDeployerConfig {
        MainlineDeployerConfig {
            enabled: true,
            poll_interval_secs: 60,
            bare_repo: bare.to_string_lossy().into_owned(),
            branch: "main".into(),
            registry: "reg.local:5000".into(),
            local_registry: "localhost:30500".into(),
            namespace: "cogneva".into(),
            builder_bin: buildah.into(),
            kubectl_bin: kubectl.into(),
            kubectl_host_path: String::new(),
            state_dir: root.join("state").to_string_lossy().into_owned(),
            build_timeout_secs: 60,
            cargo_build_jobs: 2,
            soak_secs: 1,
            restart_threshold: 1,
            failure_cooldown_secs: 60,
            max_attempts_per_rev: 2,
            rollout_timeout_secs: 60,
            startup_timeout_secs: 900,
            job_cpu_request: "7m".into(),
            job_memory_request: "21Mi".into(),
            job_cpu_limit: "333m".into(),
            job_memory_limit: "199Mi".into(),
            heartbeat_log_secs: 3600,
            manifest_dir: "deploy/k3s".into(),
            // 测试夹具仓库没有 deploy/k3s 清单树；这些用例走 set image 旧路径。
            deliver_manifests: false,
            targets: MainlineDeployerConfig::default().targets,
        }
    }

    /// 部署器工作树的分配器；工作树与 target 都落在测试临时目录内。
    fn test_workspaces(
        root: &Path,
        bare: &Path,
    ) -> std::sync::Arc<crate::workspace::WorkspaceManager> {
        std::sync::Arc::new(crate::workspace::WorkspaceManager::new(
            bare,
            root.join("workspaces"),
            root.join("target"),
        ))
    }

    /// fake cargo：build_binary 靠 PATH 查找 "cargo"，假二进制必须叫这个名。
    /// 产物写进外置的共享 target 目录（工作树里不再有 target/）；接收的
    /// COGNEVA_GIT_REVISION 落盘（构建侧必须显式注入完整 rev）。
    fn fake_cargo(dir: &Path, target_dir: &Path) {
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
echo "$COGNEVA_GIT_REVISION" >> '{envlog}'
mkdir -p '{target}/release'
echo 'fake-binary' > '{target}/release/cogneva'
chmod +x '{target}/release/cogneva'
exit 0
"#,
            log = dir.join("cargo.log").display(),
            envlog = dir.join("cargo-env.log").display(),
            target = target_dir.display()
        );
        write_fake_bin(dir, "cargo", &script);
    }

    fn fake_strip(dir: &Path) {
        write_fake_bin(dir, "strip", "#!/bin/sh\nexit 0\n");
    }

    #[tokio::test]
    // ENV_LOCK 是进程级 PATH 串行锁：PATH 是进程全局状态，必须跨 await 持有
    // 直到被测命令跑完。
    async fn poll_once_builds_pushes_and_dispatches_on_new_main() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, ws);

        // cargo/strip 靠 PATH 查找，bin_dir 前置；ENV_LOCK 保证无并行测试串改。
        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));

        deployer.poll_once().await.unwrap();

        std::env::set_var("PATH", old_path);

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains("from --tls-verify=false reg.local:5000/cogneva:local"),
            "base pull from in-cluster http registry must skip TLS verify: {buildah_calls}"
        );
        // buildah commit/tag/push 走 Pod 内 push 端点（集群 DNS）。
        let push_tag = main_image("reg.local:5000", &rev_b);
        assert!(
            buildah_calls.contains(&format!("commit ctr-test-123 {push_tag}")),
            "{buildah_calls}"
        );
        assert!(
            buildah_calls.contains("push --tls-verify=false"),
            "{buildah_calls}"
        );
        assert!(buildah_calls.contains(&push_tag), "{buildah_calls}");

        // 浮动签 :local 只在滚动收敛后前移：构建阶段允许 FROM registry :local
        // （Legacy 基底），但绝不允许 tag/push 它，否则失败回滚的坏镜像会成为
        // 静态清单 apply 的回退锚点。
        let local_tag = local_image("reg.local:5000");
        let moves_local = buildah_calls.lines().any(|l| {
            (l.contains(" tag ") || l.contains(" push ")) && l.trim_end().ends_with(&local_tag)
        });
        assert!(
            !moves_local,
            "floating :local must not be tagged/pushed before rollout converges: {buildah_calls}"
        );

        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            kubectl_calls.contains("apply -f -"),
            "job should be dispatched: {kubectl_calls}"
        );
        // apply 的 stdin manifest 也落了日志，job 名（在 manifest 里）可验。
        assert!(
            kubectl_calls.contains(&job_name(&rev_b)),
            "job manifest missing: {kubectl_calls}"
        );
        // Job manifest 镜像与 --tag 必须是节点 pull 端点（kubelet 不解析集群 DNS），
        // 绝不能把 Pod 内 push 端点写进 image 引用。
        let pull_tag = main_image("localhost:30500", &rev_b);
        assert!(
            kubectl_calls.contains(&pull_tag),
            "job image must use node pull endpoint: {kubectl_calls}"
        );
        assert!(
            !kubectl_calls.contains(&format!("--tag {push_tag}")),
            "rollout --tag must not use push endpoint: {kubectl_calls}"
        );
        // 两段预算必须一起下发：只带就绪预算会让种子的网络耗时重新算进
        // 就绪预算（本次故障的成因），只带启动预算则主容器永不 ready 时
        // 没有上界。JSON 里数组元素各占一行，去空白后校验紧邻关系。
        let compact: String = kubectl_calls
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            compact.contains("\"--timeout\",\"60\",\"--startup-timeout\",\"900\""),
            "job args must carry both the readiness and the startup budget: {kubectl_calls}"
        );

        // 沙盒构建必须显式注入完整 rev：build.rs 回退只嵌 7 位短 sha，
        // 叠层 --version 的 12 位前缀校验会必败。
        let cargo_env = std::fs::read_to_string(bin_dir.join("cargo-env.log")).unwrap();
        assert!(
            cargo_env.trim().starts_with(&rev_b),
            "cargo must receive full COGNEVA_GIT_REVISION: {cargo_env:?}"
        );

        let state: MainlineState =
            serde_json::from_str(&std::fs::read_to_string(root.join("state/state.json")).unwrap())
                .unwrap();
        assert_eq!(state.in_flight.unwrap().phase, Phase::Dispatched);
    }

    /// 本次死结的回归：第三方工作树停在无关提交（一条与 main 无共同祖先的
    /// 独立历史）时，部署器照常推进。旧设计里被占用的那棵树就是部署器唯一的
    /// 共享工作树，祖先守卫会把部署永久卡在"每轮静默跳过"。
    #[tokio::test]
    async fn deployer_not_wedged_by_foreign_worktree() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, _rev_a, rev_b) = setup_repos(root).await;
        let ws = test_workspaces(root, &bare);

        // 与 main 无共同祖先的旁支历史，推给裸仓库。
        real_git(&work, &["checkout", "--orphan", "side"]).await;
        std::fs::write(work.join("side.txt"), "side\n").unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "side"]).await;
        real_git(&work, &["push", "origin", "side"]).await;

        // 模拟智能体占树：另一棵工作树停在无关分支上。
        let foreign = ws
            .acquire_ephemeral(
                "agent-task",
                crate::workspace::BaseRef::Branch("side".into()),
            )
            .await
            .unwrap();
        assert!(foreign.path.exists());
        // 前提校验：这棵树确实停在 main 的非祖先上——正是旧守卫拒绝搬动的条件。
        let is_ancestor = tokio::process::Command::new("git")
            .args([
                "--git-dir",
                bare.to_str().unwrap(),
                "merge-base",
                "--is-ancestor",
                "side",
                "main",
            ])
            .output()
            .await
            .unwrap();
        assert!(
            !is_ancestor.status.success(),
            "setup must reproduce the wedge condition: side is not an ancestor of main"
        );

        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, ws.clone());

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains(&main_image("reg.local:5000", &rev_b)),
            "deployer must keep advancing despite a foreign worktree: {buildah_calls}"
        );
        // 第三方工作树原样保留，部署器不碰它。
        assert!(foreign.path.exists(), "foreign worktree must be left alone");
        assert_eq!(
            real_git_stdout(&foreign.path, &["rev-parse", "HEAD"]).await,
            real_git_stdout(
                &bare,
                &["--git-dir", bare.to_str().unwrap(), "rev-parse", "side"]
            )
            .await
        );
    }

    /// 回归：四部署镜像被外部写入弄成不一致（清单部分重下发、手工 set image），
    /// 且本地没有在飞滚动。旧逻辑按"上一轮未收敛"永久跳过——只有一行 INFO，
    /// 没有任何自愈路径，部署器就此静默停摆。修正后应照常构建并派发滚动 Job，
    /// 由这一轮把四部署重新 pin 回同一个 rev。
    #[tokio::test]
    // 同上：ENV_LOCK 串行化进程级 PATH 修改，需跨 await 持有。
    async fn poll_once_converges_mixed_deployments_with_no_rollout_in_flight() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        // 网关与主应用停在浮动签，执行器与进化停在旧主线 tag。
        let kubectl = fake_kubectl_per_deployment(
            &bin_dir,
            "localhost:30500/cogneva:local",
            "localhost:30500/cogneva:main-000000000000",
        );
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, ws);

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            kubectl_calls.contains("apply -f -"),
            "mixed deployments must not wedge the deployer; a rollout job should be dispatched: {kubectl_calls}"
        );
        assert!(
            kubectl_calls.contains(&job_name(&rev_b)),
            "rollout job must target the bare main rev: {kubectl_calls}"
        );
        // 基底退回浮动签：混合态里没有唯一可信的主线 rev 可作 from。
        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains("from --tls-verify=false reg.local:5000/cogneva:local"),
            "{buildah_calls}"
        );
    }

    #[tokio::test]
    // 同上：ENV_LOCK 串行化进程级 PATH 修改，需跨 await 持有。
    async fn poll_once_noop_when_already_at_main() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        let deployed = main_image("localhost:30500", &rev_b);
        let kubectl = fake_kubectl(&bin_dir, &deployed);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        assert!(
            !bin_dir.join("buildah.log").exists(),
            "no buildah calls expected when already at main rev"
        );
        let kubectl_calls =
            std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap_or_default();
        assert!(
            !kubectl_calls.contains("apply"),
            "no job dispatch expected: {kubectl_calls}"
        );
    }

    /// 极简假 registry：按序应答预置响应，每连接一次。返回 (endpoint, 收到的请求)。
    async fn fake_registry(
        responses: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let mut reqs = Vec::new();
            for resp in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                reqs.push(String::from_utf8_lossy(&buf[..n]).to_string());
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
            reqs
        });
        (format!("127.0.0.1:{port}"), handle)
    }

    fn http_200(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    const HTTP_404: &str = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";

    #[tokio::test]
    async fn registry_tag_revision_reads_label_from_manifest_and_blob() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg1"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"deadbeefcafe"}}}"#,
            ),
        ])
        .await;
        let mut cfg = test_config(root, Path::new("/nonexistent"), "noop", "noop");
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        let rev = deployer.registry_tag_revision("local").await.unwrap();
        assert_eq!(rev.as_deref(), Some("deadbeefcafe"));

        let reqs = handle.await.unwrap();
        assert!(
            reqs[0].contains("/v2/cogneva/manifests/local"),
            "{:?}",
            reqs[0]
        );
        assert!(
            reqs[1].contains("/v2/cogneva/blobs/sha256:cfg1"),
            "{:?}",
            reqs[1]
        );
        // 明文 HTTP：Pod 与集群内 registry 之间不做 TLS。
        assert!(reqs[0].starts_with("GET /v2/"), "{:?}", reqs[0]);
    }

    #[tokio::test]
    async fn registry_tag_revision_follows_a_multi_platform_index() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"manifests":[{"digest":"sha256:plat"}]}"#),
            http_200(r#"{"config":{"digest":"sha256:cfg2"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"cafebabe0011"}}}"#,
            ),
        ])
        .await;
        let mut cfg = test_config(root, Path::new("/nonexistent"), "noop", "noop");
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        let rev = deployer.registry_tag_revision("local").await.unwrap();
        assert_eq!(rev.as_deref(), Some("cafebabe0011"));
        let reqs = handle.await.unwrap();
        assert!(
            reqs[1].contains("/v2/cogneva/manifests/sha256:plat"),
            "{:?}",
            reqs[1]
        );
        assert!(
            reqs[2].contains("/v2/cogneva/blobs/sha256:cfg2"),
            "{:?}",
            reqs[2]
        );
    }

    #[tokio::test]
    async fn declared_image_rev_derives_immutable_tags_without_the_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let mut cfg = test_config(root, Path::new("/nonexistent"), "noop", "noop");
        // 指向不可达端口：不可变 tag 必须不依赖 registry 就能读出 rev。
        cfg.registry = "127.0.0.1:1".into();
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        let imgs = vec![main_image("localhost:30500", "abcdef0123456789"); 4];
        assert_eq!(
            deployer.declared_image_rev(&imgs).await.as_deref(),
            Some("abcdef012345")
        );
        // 四部署不一致：未知，绝不当成已收敛。
        let mixed = vec![
            main_image("localhost:30500", "abcdef0123456789"),
            local_image("localhost:30500"),
        ];
        assert_eq!(deployer.declared_image_rev(&mixed).await, None);
    }

    /// 浮动签被重新播种成别的 rev：清单里的 tag 字符串一个字没变，但节点
    /// 已经随清单滚到旧二进制。必须识别出没收敛并重新 pin 回 `main-<rev>`。
    #[tokio::test]
    async fn floating_pin_drift_is_detected_and_repaired() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "localhost:30500/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        // 三次 registry 命中：读 :local 的 manifest、读其 config blob（rev 是
        // 别的值）、查 main-<rev> 是否存在（不存在 → 必须真重建）。
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg2"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"000000000000abcd"}}}"#,
            ),
            HTTP_404.to_string(),
        ])
        .await;
        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, ws);

        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            last_good_rev: Some(rev_b.clone()),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        handle.await.unwrap();
        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log"))
            .expect("drift must not take the no-op shortcut");
        assert!(
            buildah_calls.contains("push --tls-verify=false"),
            "{buildah_calls}"
        );
        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(kubectl_calls.contains("apply -f -"), "{kubectl_calls}");
    }

    /// 不可变 tag 已在 registry 里：清单重下发把四部署打回浮动签后，只需
    /// 重新 pin 回去，4C 机器上不必再跑一次全程构建。
    #[tokio::test]
    async fn existing_immutable_image_is_repinned_without_a_rebuild() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "localhost:30500/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg3"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"000000000000abcd"}}}"#,
            ),
            // main-<rev> 已存在：直接复用。
            http_200("{}"),
        ])
        .await;
        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, ws);

        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            last_good_rev: Some(rev_b.clone()),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        handle.await.unwrap();
        assert!(
            !bin_dir.join("buildah.log").exists(),
            "an image already in the registry must be reused, not rebuilt"
        );
        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(kubectl_calls.contains("apply -f -"), "{kubectl_calls}");
        assert!(
            kubectl_calls.contains(&main_image("localhost:30500", &rev_b)),
            "job must pin the deployments back to the immutable tag: {kubectl_calls}"
        );
    }

    #[tokio::test]
    async fn convergence_promotes_floating_local_tag() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        // 四部署已经在目标 main tag 上：收敛分支优先于 Job 状态判定。
        let deployed = main_image("localhost:30500", &rev_b);
        let kubectl = fake_kubectl(&bin_dir, &deployed);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            in_flight: Some(InFlight {
                rev: rev_b.clone(),
                phase: Phase::Dispatched,
            }),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        deployer.poll_once().await.unwrap();

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        let push_main = main_image("reg.local:5000", &rev_b);
        let push_local = local_image("reg.local:5000");
        assert!(
            buildah_calls.contains(&format!("tag {push_main} {push_local}")),
            "convergence must retag immutable tag to floating :local: {buildah_calls}"
        );
        assert!(
            buildah_calls.contains(&format!("push --tls-verify=false {push_local}")),
            "floating :local must be pushed on convergence: {buildah_calls}"
        );

        let state: MainlineState =
            serde_json::from_str(&std::fs::read_to_string(state_dir.join("state.json")).unwrap())
                .unwrap();
        assert!(state.in_flight.is_none());
        assert_eq!(state.last_good_rev.as_deref(), Some(rev_b.as_str()));
        assert_eq!(
            state.last_good_tag.as_deref(),
            Some(main_image("localhost:30500", &rev_b).as_str())
        );
    }

    #[tokio::test]
    async fn local_pin_at_last_good_is_noop_after_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        // 外部 apply 把四部署打回静态清单 pin：registry 浮动签 :local。
        let kubectl = fake_kubectl(&bin_dir, "localhost:30500/cogneva:local");
        // registry 上 :local 的内容确实构建自当前 mainline rev：这才叫收敛。
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg1"}}"#),
            http_200(&format!(
                r#"{{"config":{{"Labels":{{"org.opencontainers.image.revision":"{rev_b}"}}}}}}"#
            )),
        ])
        .await;

        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            last_good_rev: Some(rev_b.clone()),
            last_good_tag: Some(main_image("localhost:30500", &rev_b)),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        deployer.poll_once().await.unwrap();
        handle.await.unwrap();

        assert!(
            !bin_dir.join("buildah.log").exists(),
            "apply pin to current :local must not trigger rebuild"
        );
        let kubectl_calls =
            std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap_or_default();
        assert!(
            !kubectl_calls.contains("apply"),
            "apply pin to current :local must not redispatch: {kubectl_calls}"
        );
    }

    #[tokio::test]
    async fn completed_job_with_reverted_images_is_redispatched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let log = bin_dir.join("kubectl.log");
        // 四部署已被外部 apply 打回 :local（Legacy），而同 rev 的 Job 已完成。
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "bad -o arg" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get deployment"*) echo "localhost:30500/cogneva:local" ;;
  *"get job"*) echo "1||" ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *"delete"*) echo "job.batch \"x\" deleted" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        let kubectl = bin_dir.join("fake-kubectl");
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        let buildah = fake_buildah(&bin_dir, "");

        let cfg = test_config(root, &bare, &buildah, &kubectl.to_string_lossy());
        // 预置在飞状态：Job 已派发（Dispatched），避免触发构建流程。
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            in_flight: Some(InFlight {
                rev: rev_b.clone(),
                phase: Phase::Dispatched,
            }),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains(&format!("delete job {}", job_name(&rev_b))),
            "completed job must be deleted before re-dispatch: {calls}"
        );
        assert!(
            calls.matches("apply -f -").count() >= 1,
            "rollout job must be re-dispatched: {calls}"
        );
        let state: MainlineState =
            serde_json::from_str(&std::fs::read_to_string(state_dir.join("state.json")).unwrap())
                .unwrap();
        assert_eq!(state.in_flight.unwrap().phase, Phase::Dispatched);
    }

    /// fake kubectl：四部署停在 old_image，同 rev 的滚动 Job 报失败，其 Pod 的
    /// 终止码按参数给（部署器据此定类别）。
    fn fake_kubectl_failed_job(dir: &Path, old_image: &str, exit_code: &str) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
case "$*" in
  *"job-name="*) echo "{exit_code}" ;;
  *"get job"*) echo "|1|" ;;
  *"get deployment"*) echo "{old_image}" ;;
  *"delete"*) echo 'job.batch "x" deleted' ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 预置 state.json，返回它的路径。
    fn seed_state(root: &Path, state: MainlineState) -> PathBuf {
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();
        state_dir.join("state.json")
    }

    fn read_state(path: &Path) -> MainlineState {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    /// Job 的失败类别取自它自己的终止码。这里是那条确定性通道的入口：
    /// 部署器不读 Job 日志、不解析错误文本。
    #[tokio::test]
    async fn rollout_job_exit_code_carries_the_failure_class() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let fake = PathBuf::from("/nonexistent");
        let class_of = |name: &str, body: &str| {
            let (root, bin_dir, fake) = (root.to_path_buf(), bin_dir.clone(), fake.clone());
            let name = name.to_string();
            let body = body.to_string();
            async move {
                write_fake_bin(&bin_dir, &name, &body);
                let cfg = test_config(&root, &fake, "noop", &bin_dir.join(&name).to_string_lossy());
                MainlineDeployer::new(cfg, test_workspaces(&root, &fake))
                    .job_failure_class("j")
                    .await
            }
        };
        assert_eq!(
            class_of("k75", "#!/bin/sh\necho 75\nexit 0\n").await,
            FailureClass::Environment
        );
        // 1（版本类退出）、空（Pod 还没终止）、137（信号终止：OOM / 驱逐 / 到点
        // 被杀都长这样，类别判不出）一律按版本类靠：判不准就往记账侧取。
        assert_eq!(
            class_of("k1", "#!/bin/sh\necho 1\nexit 0\n").await,
            FailureClass::Version
        );
        assert_eq!(
            class_of("kempty", "#!/bin/sh\nexit 0\n").await,
            FailureClass::Version
        );
        assert_eq!(
            class_of("k137", "#!/bin/sh\necho 137\nexit 0\n").await,
            FailureClass::Version
        );
        // 采样失败（apiserver 不可达）同样按版本类，且不产生第二个错误。
        assert_eq!(
            class_of("kerr", "#!/bin/sh\necho 'no route to host' >&2\nexit 1\n").await,
            FailureClass::Version
        );
    }

    /// 环境类失败不是版本结论：不占本 rev 的尝试次数，但仍设冷却（不空转），
    /// 且 rev 仍记着（下轮走"重试它"这条判据）。
    #[tokio::test]
    async fn an_environment_class_failure_does_not_consume_the_attempt_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let kubectl = fake_kubectl_failed_job(
            &bin_dir,
            &main_image("localhost:30500", &rev_a),
            &ROLLOUT_EXIT_ENVIRONMENT.to_string(),
        );
        let buildah = fake_buildah(&bin_dir, "");
        let cfg = test_config(root, &bare, &buildah, &kubectl);
        // 上一轮已经记过一次版本类失败：环境类这次不该把它推到上限。
        let state_path = seed_state(
            root,
            MainlineState {
                in_flight: Some(InFlight {
                    rev: rev_b.clone(),
                    phase: Phase::Dispatched,
                }),
                failed_rev: Some(rev_b.clone()),
                failed_attempts: 1,
                failed_class: FailureClass::Version,
                ..Default::default()
            },
        );

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let state = read_state(&state_path);
        assert_eq!(state.failed_attempts, 1, "环境类失败不占尝试预算");
        assert_eq!(state.failed_class, FailureClass::Environment);
        assert_eq!(state.failed_rev.as_deref(), Some(rev_b.as_str()));
        assert!(state.in_flight.is_none());
        assert!(
            state.failed_cooldown_until > chrono::Utc::now().timestamp(),
            "环境类失败仍要冷却，否则部署器会在同一个满节点上空转"
        );
        // 类别必须来自 Job 的 Pod 终止码：这条查询没发生就说明判据换了来源。
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            calls.contains(&format!("job-name={}", job_name(&rev_b))),
            "exit code query missing: {calls}"
        );
    }

    /// 版本类失败照旧占一次：同一处反复失败要能停下来等人。
    #[tokio::test]
    async fn a_version_class_failure_consumes_an_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let kubectl =
            fake_kubectl_failed_job(&bin_dir, &main_image("localhost:30500", &rev_a), "1");
        let buildah = fake_buildah(&bin_dir, "");
        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let state_path = seed_state(
            root,
            MainlineState {
                in_flight: Some(InFlight {
                    rev: rev_b.clone(),
                    phase: Phase::Dispatched,
                }),
                failed_rev: Some(rev_b.clone()),
                failed_attempts: 1,
                failed_class: FailureClass::Version,
                ..Default::default()
            },
        );

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let state = read_state(&state_path);
        assert_eq!(state.failed_attempts, 2);
        assert_eq!(state.failed_class, FailureClass::Version);
    }

    /// 环境类失败过的 rev，其镜像构建与推送都成功过（卡住的是调度）：重试直接
    /// 复用 registry 上的不可变 tag，不在本来就把 Pod 排不进去的节点上再跑一遍
    /// 全程构建。
    #[tokio::test]
    async fn an_environment_class_failure_retries_without_a_rebuild() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let kubectl =
            fake_kubectl_failed_job(&bin_dir, &main_image("localhost:30500", &rev_a), "1");
        // 版本号写全 rev：把复用条件改坏时构建会真的跑起来，让"没有重建"这条
        // 断言（而不是构建本身的报错）成为红灯。
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let (endpoint, handle) = fake_registry(vec![http_200("{}")]).await;
        let push_tag = main_image(&endpoint, &rev_b);
        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);
        // 上一轮：同一个 rev 环境类失败，镜像已在 registry 上。
        let state_path = seed_state(
            root,
            MainlineState {
                failed_rev: Some(rev_b.clone()),
                failed_attempts: 0,
                failed_class: FailureClass::Environment,
                ..Default::default()
            },
        );

        let deployer = MainlineDeployer::new(cfg, ws);
        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        assert!(
            !bin_dir.join("buildah.log").exists(),
            "an environment-class retry must reuse the immutable tag, not rebuild"
        );
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            calls.contains("apply -f -"),
            "the rollout job must be re-dispatched: {calls}"
        );
        assert!(
            calls.contains(&main_image("localhost:30500", &rev_b)),
            "re-dispatched job must carry the same immutable tag: {calls}"
        );
        let state = read_state(&state_path);
        assert_eq!(state.in_flight.unwrap().phase, Phase::Dispatched);
        // 假 registry 只被问了一次：就是"这个 tag 在不在"。
        let reqs = handle.await.unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(
            reqs[0].contains(&format!("/v2/cogneva/manifests/{push_tag}")),
            "the one registry read must be the tag-presence probe: {:?}",
            reqs[0]
        );
    }

    #[tokio::test]
    async fn rollout_failure_rolls_back_updated_targets() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 快照（.image jsonpath）返回各部署当前镜像；rollout 查询（generation
        // jsonpath）：第一个目标 security-gateway 永远不完成（observedGeneration
        // 落后），其余正常。set image 都成功。
        let log = bin_dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *generation*)
    case "$*" in
      *cogneva-security-gateway*) echo "1|0|1|0|0|" ;;
      *) echo "1|1|1|1|1|" ;;
    esac ;;
  *"initContainers"*) ;;
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"get pods"*) echo "0 true " ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        // 轮询间隔 5s，超时 1s：首轮即败、醒后第二轮越过死线，快速失败。
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            1,
            1,
        );
        let cfg = MainlineDeployerConfig::default();
        let plan = RolloutPlan::from_config(&cfg, "localhost:30500/cogneva:main-new".into());
        let err = executor.run(&plan).await.unwrap_err();
        assert!(err.to_string().contains("did not complete"), "{err}");

        let calls = std::fs::read_to_string(&log).unwrap();
        // 回滚目标来自 Job 启动时快照（.image 查询），不是参数传入。
        assert!(
            calls.contains("set image deployment/cogneva-security-gateway security-gateway=localhost:30500/cogneva:main-old"),
            "rollback should set failed target back to its snapshotted prev: {calls}"
        );
        // 新镜像引用是节点 pull 端点。
        assert!(
            calls.contains("set image deployment/cogneva-security-gateway security-gateway=localhost:30500/cogneva:main-new"),
            "rollout should set failed target to new pull-endpoint tag: {calls}"
        );
    }

    #[tokio::test]
    async fn crashloop_pod_fails_fast_without_waiting_for_rollout_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let log = bin_dir.join("kubectl.log");
        // security-gateway rollout 永不完成且新 Pod CrashLoopBackOff：
        // wait_rollout_complete 必须在首轮轮询即致命态早退，而不是等满超时。
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "bad -o arg" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *generation*)
    case "$*" in
      *cogneva-security-gateway*) echo "1|0|1|0|0|" ;;
      *) echo "1|1|1|1|1|" ;;
    esac ;;
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"get pods"*)
    case "$*" in
      *component=security-gateway*) echo "CrashLoopBackOff" ;;
      *) echo "0 true " ;;
    esac ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        // 超时给 300s：若致命态早退失效，测试会真的等 300s（暴露问题）。
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            300,
            900,
        );
        let cfg = MainlineDeployerConfig::default();
        let plan = RolloutPlan::from_config(&cfg, "localhost:30500/cogneva:main-new".into());
        let err = executor.run(&plan).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("fatal waiting state CrashLoopBackOff"),
            "{err}"
        );

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains("set image deployment/cogneva-security-gateway security-gateway=localhost:30500/cogneva:main-old"),
            "fatal state must trigger rollback to snapshotted prev: {calls}"
        );
    }

    #[tokio::test]
    async fn job_status_distinguishes_failed_when_succeeded_field_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 真实 kubectl 对失败 Job 的输出形如 "|1|"：succeeded 缺省，
        // 空格分隔解析会把 failed 顶到第一位误判 Complete。
        let script = r#"#!/bin/sh
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in jsonpath=*) ;; *) echo "bad -o arg" >&2; exit 2 ;; esac
  fi
  prev="$a"
done
case "$*" in
  *job-failed*) echo "|1|" ;;
  *job-ok*) echo "1||" ;;
  *job-running*) echo "||1" ;;
  *) echo "not found" >&2; exit 1 ;;
esac
exit 0
"#;
        write_fake_bin(&bin_dir, "fake-kubectl", script);
        let cfg = MainlineDeployerConfig {
            kubectl_bin: bin_dir.join("fake-kubectl").to_string_lossy().to_string(),
            ..Default::default()
        };
        let deployer = MainlineDeployer::new(cfg, test_workspaces(&bin_dir, &bin_dir));
        assert_eq!(
            deployer.job_status("job-failed").await.unwrap(),
            JobStatus::Failed
        );
        assert_eq!(
            deployer.job_status("job-ok").await.unwrap(),
            JobStatus::Complete
        );
        assert_eq!(
            deployer.job_status("job-running").await.unwrap(),
            JobStatus::Running
        );
        assert_eq!(
            deployer.job_status("job-missing").await.unwrap(),
            JobStatus::NotFound
        );
    }

    #[tokio::test]
    async fn pods_healthy_detects_fatal_states() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let pods_file = bin_dir.join("pods.out");
        let kubectl = fake_kubectl_pods_from_file(&bin_dir, &pods_file);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 60, 900);
        let target = RolloutTarget {
            deployment: "cogneva".into(),
            container: "cogneva".into(),
            component: "gateway".into(),
            name: "cogneva".into(),
        };

        // ImagePullBackOff 立即判病。
        std::fs::write(&pods_file, "0 false ImagePullBackOff").unwrap();
        assert!(executor.pods_healthy(&target).await.is_err());

        // 重启超阈值。
        std::fs::write(&pods_file, "2 true ").unwrap();
        assert!(executor.pods_healthy(&target).await.is_err());

        // 空输出（选择器失效）判病。
        std::fs::write(&pods_file, "").unwrap();
        assert!(executor.pods_healthy(&target).await.is_err());

        // 健康。
        std::fs::write(&pods_file, "0 true ").unwrap();
        assert!(executor.pods_healthy(&target).await.is_ok());
    }

    /// fake kubectl：init 容器进度与 deployment 状态按轮次推进。第 1 轮
    /// init 未结束（与真实 jsonpath 缺失 finishedAt 同形：只有分隔符）且未
    /// 收敛，第 2 轮 init 结束且收敛。`rollout_timeout` 传 0 让"任何一次
    /// 就绪预算消耗"立刻判败——用例只可能因为启动阶段不吃就绪预算而通过。
    fn fake_kubectl_slow_init(dir: &Path, init_never_finishes: bool) -> String {
        let log = dir.join("kubectl.log");
        let count = dir.join("poll.count");
        let finish_cond = if init_never_finishes {
            "false"
        } else {
            r#"[ "$n" -ge 2 ]"#
        };
        let init_body = format!(
            r#"n=$(cat '{count}' 2>/dev/null || echo 1)
    if {finish_cond}; then echo "2026-09-17T03:17:10Z|"; else echo "|"; fi
    "#,
            count = count.display(),
            finish_cond = finish_cond
        );
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"terminated.finishedAt"*)
    {init_body}
    ;;
  *"generation"*)
    n=$(cat '{count}' 2>/dev/null || echo 0)
    n=$((n+1)); echo "$n" > '{count}'
    if [ "$n" -ge 2 ]; then echo "1|1|1|1|1|"; else echo "1|1|1|1|0|1"; fi
    ;;
  # Pod 现场查询（就绪门禁与诊断共用）也含 waiting.reason，必须比它先匹配：
  # 被诊断那支接走会返回空串，门禁读成"没有可判的副本"而不判收敛。
  *"deletionTimestamp"*) echo "p-new|Running|true|||2026-09-17T15:46:29Z|" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) ;;
  *"get pods"*) ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            count = count.display(),
            init_body = init_body
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 单个带 init 容器的目标（执行器）：种子步骤是网络克隆，耗时与本次
    /// 上线的版本无关。
    fn sandbox_executor_plan(tag: &str) -> RolloutPlan {
        RolloutPlan {
            tag: tag.to_string(),
            targets: vec![RolloutTarget {
                deployment: "cogneva-sandbox-executor".into(),
                container: "sandbox-executor".into(),
                component: "sandbox-executor".into(),
                name: "cogneva".into(),
            }],
            manifests_dir: None,
        }
    }

    #[tokio::test]
    async fn startup_phase_does_not_consume_the_readiness_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_slow_init(&bin_dir, false);
        // 就绪预算 0：启动阶段一旦被算进就绪预算，首轮即判败回滚。
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 0, 300);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        executor
            .run(&plan)
            .await
            .expect("slow init must not spend the readiness budget");

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains("set image deployment/cogneva-sandbox-executor sandbox-executor=localhost:30500/cogneva:main-old"),
            "no rollback should happen: {calls}"
        );
    }

    #[tokio::test]
    async fn an_init_container_that_never_finishes_hits_the_startup_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_slow_init(&bin_dir, true);
        // 启动预算 0、就绪预算 300：判败必须来自启动阶段的上界，说明两段
        // 预算是分开的——init 卡死不能靠就绪预算兜底（那正是好版本被误回滚
        // 的成因），也不能无限等（等不到干净回滚）。
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 300, 0);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        assert!(err.to_string().contains("stuck in startup phase"), "{err}");
    }

    /// 假 kubectl：第一轮轮询里选择器匹配到的还是旧 Pod（它的 init 早已结束），
    /// 新 Pod 的 init 从第二轮起才在跑，第三轮滚动收敛。
    fn fake_kubectl_old_pod_outlives_the_new_pod_init(dir: &Path) -> String {
        let log = dir.join("kubectl.log");
        let count = dir.join("poll.count");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"terminated.finishedAt"*)
    n=$(cat '{count}' 2>/dev/null || echo 1)
    if [ "$n" -ge 2 ]; then echo "|"; else echo "2026-09-17T03:17:10Z|"; fi
    ;;
  *"generation"*)
    n=$(cat '{count}' 2>/dev/null || echo 0)
    n=$((n+1)); echo "$n" > '{count}'
    if [ "$n" -ge 3 ]; then echo "1|1|1|1|1|"; else echo "1|1|1|1|0|1"; fi
    ;;
  *"deletionTimestamp"*) echo "p-new|Running|true|||2026-09-17T15:46:29Z|" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            count = count.display()
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 集群实证：种子克隆在外网跑了 299s，而就绪预算 300s 从"看到旧 Pod 的
    /// 已结束 init"那一刻起算，克隆一结束、新副本刚起步就被判超时、好版本
    /// 被回滚。旧 Pod 的 init 早就结束，不能当成本次滚动的 init 进度。
    #[tokio::test]
    async fn a_finished_init_from_the_old_pod_does_not_start_the_readiness_clock() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_old_pod_outlives_the_new_pod_init(&bin_dir);
        // 就绪预算 0：只要在旧 Pod 的已结束 init 上起算了就绪预算，首轮即判败回滚。
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 0, 300);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        executor
            .run(&plan)
            .await
            .expect("the old pod's finished init must not be read as this rollout's progress");

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains(
                "set image deployment/cogneva-sandbox-executor sandbox-executor=localhost:30500/cogneva:main-old"
            ),
            "no rollback should happen: {calls}"
        );
    }

    /// 反面：没有 init 容器的部署不存在启动阶段，就绪预算必须照旧从第一轮起算。
    /// "没见过 init 在跑"这条判据只能用在带 init 容器的部署上，否则主容器永不
    /// ready 的版本会被一路拖到启动预算才判败。
    #[tokio::test]
    async fn a_deployment_without_init_containers_still_spends_the_readiness_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let log = bin_dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"initContainers"*) ;;
  *"generation"*) echo "1|1|1|1|0|1" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        // 就绪预算 0、启动预算 300：判败必须来自就绪预算。
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            0,
            1,
            0,
            300,
        );
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        assert!(
            err.to_string().contains(
                "rollout of deployment/cogneva-sandbox-executor did not complete within 0s"
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_fatal_init_container_waiting_state_fails_fast() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let log = bin_dir.join("kubectl.log");
        // init 容器拉不到镜像：主容器只报 PodInitializing（非致命），只看
        // 主容器就会把启动阶段的上界耗光才判败。
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"terminated.finishedAt"*) echo "|" ;;
  *"generation"*) echo "1|1|1|1|0|1" ;;
  *"waiting.reason"*) printf 'ImagePullBackOff\nPodInitializing\n' ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            300,
            300,
        );
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("fatal waiting state ImagePullBackOff"),
            "{err}"
        );
    }

    /// 假 kubectl：deployment 计数器**一开始就报收敛**（这正是集群实证里那个
    /// 假收敛：旧副本 Terminating 但还 ready 计入 readyReplicas，新副本还没
    /// ready 却已算进 updatedReplicas），而 Pod 现场查询里那个副本前
    /// `ready_after - 1` 次轮询是 not-ready，主容器启动时刻由 `started` 决定
    /// （交给 shell 的 date 算，好覆盖「刚起来」与「早就起来」两种处境）。
    fn fake_kubectl_readiness_gate(dir: &Path, started: &str, ready_after: u64) -> String {
        let log = dir.join("kubectl.log");
        let count = dir.join("pods.count");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"deletionTimestamp"*)
    n=$(cat '{count}' 2>/dev/null || echo 0)
    n=$((n+1)); echo "$n" > '{count}'
    started=$(date -u -d '{started}' +%Y-%m-%dT%H:%M:%SZ)
    if [ "$n" -ge {ready_after} ]; then
      echo "gw-1|Running|true|||$started|"
    else
      echo "gw-1|Running|false|ContainersNotReady|containers with unready status: [gw]|$started|"
    fi
    ;;
  *"terminated.finishedAt"*) echo "" ;;
  *"readinessProbe"*) echo "5|10|1|3" ;;
  *"generation"*) echo "1|1|1|1|1|" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) echo "" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            count = count.display(),
            started = started,
            ready_after = ready_after
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 假 kubectl：Pod 现场查询返回两个副本——旧副本已打 deletionTimestamp、
    /// 关停中不再 ready、主容器起于一小时前；新副本（本次滚动的）已 ready 且刚
    /// 起来。部署计数器是收敛的，所以「收敛且自己的副本都就绪」这个结论只能
    /// 从 Pod 现场、且只看非 terminating 的那批推出来。
    fn fake_kubectl_old_replica_terminating(dir: &Path) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"deletionTimestamp"*)
    old=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)
    new=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    echo "gw-old|Running|false|ContainersNotReady|containers with unready status: [gw]|$old|2026-09-17T15:46:00Z"
    echo "gw-new|Running|true|||$new|"
    ;;
  *"terminated.finishedAt"*) echo "" ;;
  *"readinessProbe"*) echo "5|10|1|3" ;;
  *"generation"*) echo "1|1|1|1|1|" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) echo "" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 假 kubectl：就绪探针查询前 `blind_probe_reads` 次失败，之后返回集群实测的
    /// `5|10|1|3`；部署计数器收敛，Pod 现场是一个早已起来、始终 not-ready 的副本。
    fn fake_kubectl_blind_probe_reads(dir: &Path, blind_probe_reads: u64) -> String {
        let log = dir.join("kubectl.log");
        let count = dir.join("probe.count");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"readinessProbe"*)
    n=$(cat '{count}' 2>/dev/null || echo 0)
    n=$((n+1)); echo "$n" > '{count}'
    if [ "$n" -le {blind_probe_reads} ]; then
      echo "error: unable to connect to the server" >&2
      exit 1
    fi
    echo "5|10|1|3"
    ;;
  *"deletionTimestamp"*)
    started=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)
    echo "gw-1|Running|false|ContainersNotReady|containers with unready status: [gw]|$started|"
    ;;
  *"terminated.finishedAt"*) echo "" ;;
  *"generation"*) echo "1|1|1|1|1|" ;;
  *"restartCount"*) echo "0 false " ;;
  *"waiting.reason"*) echo "" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            count = count.display(),
            blind_probe_reads = blind_probe_reads
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 探针配置读不到时不能拿兜底值顶上：兜底预算（48s）比这个部署真实配置出来
    /// 的预算（53s）短，拿它判就是门禁比 kubelet 更早开枪——正是本故事要消灭的
    /// 那类错误，只是换了一条更窄的触发路径。读不到就不判，下一轮补读；补读到
    /// 之后用真实预算判，所以这里的判败文案必须报真实值 53s。
    #[tokio::test]
    async fn an_unreadable_probe_configuration_is_not_replaced_by_a_guess() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_blind_probe_reads(&bin_dir, 1);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 60, 300);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("readiness probe's own budget"), "{msg}");
        assert!(
            msg.contains("53s"),
            "the verdict must use the deployment's real probe config, not a \
             fallback budget: {msg}"
        );
        assert!(!msg.contains("48s"), "{msg}");
    }

    /// 集群实证（2026-09-17 15:46:26Z 上线 2c33cfb）：security-gateway 新 Pod
    /// 15:46:29 起容器，部署器 15:47:01 以 `not ready: 0 false` 判败回滚，而该
    /// Pod 在 15:47:11 就是 1/1——比门禁晚十秒就绪。就绪探针 period=10 ×
    /// failureThreshold=3 是 kubelet 自己判「这容器一直不 ready」的周期，门禁
    /// 不该比它更早开枪：新副本在探针自己的周期内还没就绪，不算失败。
    #[tokio::test]
    async fn a_replica_that_becomes_ready_within_the_probes_own_budget_is_not_judged() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 刚起容器（date 取当前时刻 → 运行时长 ≈ 0，远在 53s 预算内），
        // 第二次轮询就绪。
        let kubectl = fake_kubectl_readiness_gate(&bin_dir, "now", 2);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 60, 60);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        executor
            .run(&plan)
            .await
            .expect("a replica still starting must not fail the rollout");

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains("main-old"),
            "no rollback may be issued while the new replica is within its probe budget: {calls}"
        );
    }

    /// 收敛必须归到本次滚动自己的副本上：旧副本正在删除（deletionTimestamp 已
    /// 打上、关停中已不 ready、起容器于很久以前）时，若把它也算进"本部署的
    /// Pod"，那个陈旧的启动时刻会立刻吃满就绪预算，于是一个已经就绪的新副本
    /// 配一个正在关停的旧副本，会把一次成功的滚动判成失败。
    ///
    /// 部署计数器在这种现场下确实是收敛的（旧副本已被排除在计数之外），所以
    /// 这条只有直接看 Pod、且只看属于本次滚动的 Pod 才判得对——锁的是
    /// `wait_rollout_complete` 里那条归因，不是它下面的纯函数。
    #[tokio::test]
    async fn a_terminating_replica_does_not_condemn_a_rollout_that_is_actually_up() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_old_replica_terminating(&bin_dir);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 60, 60);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        executor
            .run(&plan)
            .await
            .expect("a terminating old replica must not fail the rollout");

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains("main-old"),
            "a rollout whose own replica is ready must not roll back: {calls}"
        );
    }

    /// 反面：预算用完还是没就绪才判败，且判败记录自带现场（Pod 名、相位、
    /// 就绪状态、等待原因与消息、容器已运行时长）——不能只剩内部采样列。
    #[tokio::test]
    async fn a_replica_that_never_readies_inside_the_budget_fails_with_the_scene() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 主容器已起来一小时仍未就绪：远超 period×failureThreshold + 启动开销。
        let kubectl = fake_kubectl_readiness_gate(&bin_dir, "-1 hour", 100_000);
        // 就绪预算 300s：判败必须来自探针自己的预算，不是外层超时。
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 300, 300);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("readiness probe's own budget"), "{msg}");
        // 预算取自那个部署的探针配置（假 kubectl 给的是集群实测值 5|10|1|3），
        // 不是任何写死的秒数。
        assert!(
            msg.contains("53s"),
            "the budget must come from the probe config: {msg}"
        );
        // 现场：Pod 名、相位、ready、等待原因与消息、容器已运行时长。
        assert!(msg.contains("gw-1 Running not-ready"), "{msg}");
        assert!(msg.contains("waiting=ContainersNotReady"), "{msg}");
        assert!(msg.contains("ran=360"), "{msg}");

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            calls.contains(
                "set image deployment/cogneva-sandbox-executor sandbox-executor=localhost:30500/cogneva:main-old"
            ),
            "a replica that never readies is a revision failure and must roll back: {calls}"
        );
    }

    /// 假 kubectl：准入被拒的现场——deployment 永不收敛（期望 1 副本、建成 0），
    /// **一个 Pod 都没有**（配额拒绝发生在建 Pod 之前），RS 上有
    /// `ReplicaFailure=True / FailedCreate`，且同时挂着上一轮那条已被缩到 0 副本
    /// 的旧 RS。
    fn fake_kubectl_admission_rejected(dir: &Path) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"initContainers[*].name"*) echo "" ;;
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"ReplicaFailure"*)
    echo "cogneva-sandbox-executor-6d9f4c|1|True|FailedCreate|pods \"cogneva-sandbox-executor-6d9f4c-9xq2p\" is forbidden: exceeded quota: cogneva-quota, requested: limits.cpu=500m, used: limits.cpu=16500m, limited: limits.cpu=17"
    echo "cogneva-sandbox-executor-57bc1f|0|True|FailedCreate|pods \"cogneva-sandbox-executor-57bc1f-zzzzz\" is forbidden: exceeded quota: cogneva-quota"
    ;;
  *"generation"*) echo "1|1|1|0|0|1" ;;
  *"deletionTimestamp"*) echo "" ;;
  *"PodScheduled"*) echo "" ;;
  *"readinessProbe"*) echo "5|10|1|3" ;;
  *"restartCount"*) echo "" ;;
  *"waiting.reason"*) echo "" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 准入被拒不产生 Pod 对象，Pod 采样一条都取不到；那条 `FailedCreate` 事件
    /// 又挂在 RS 上被事件过滤丢掉。记录必须靠 RS 自己的类型化条件说出「配额
    /// 超限」，否则一次集群放不下新副本的超时在记录里看着就像版本坏了，等人回到
    /// 集群时事件窗口早过了。
    #[tokio::test]
    async fn an_admission_rejection_is_named_in_the_timeout_record() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_admission_rejected(&bin_dir);
        // 就绪预算 0：第一轮轮询即判超时，用例不空等。
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 0, 300);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("replicasets: cogneva-sandbox-executor-6d9f4c FailedCreate"),
            "the record must name the admission rejection: {msg}"
        );
        assert!(msg.contains("exceeded quota"), "{msg}");
        // 旧 RS（期望副本数已被缩到 0）的历史失败不进现场：拿旧账当新病情，
        // 会把上一次的病因记到这一次头上。
        assert!(!msg.contains("57bc1f"), "{msg}");

        // 判定不变：这一轮只改诊断，准入受阻仍按版本类处置（回滚）。把结论
        // 也锁在这里，是为了让「准入受阻算不算环境」那半个问题改判时，
        // 必须显式改这条用例，而不是悄悄漂移。
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            calls.contains(
                "set image deployment/cogneva-sandbox-executor sandbox-executor=localhost:30500/cogneva:main-old"
            ),
            "diagnosis-only change must not alter the verdict: {calls}"
        );
    }

    #[test]
    fn cluster_unreachable_is_not_a_verdict_about_the_revision() {
        // 观测能力故障：apiserver 不可达、握手超时、连接被拒、查询整体超时。
        for msg in [
            "kubectl get deployment x -o jsonpath=... failed: Unable to connect to the server: net/http: TLS handshake timeout",
            "kubectl get pods ... failed: dial tcp 127.0.0.1:6443: connect: connection refused",
            "kubectl get pods ... timed out after 30s",
            "cluster unreachable: nothing was observed",
        ] {
            assert!(is_cluster_unreachable(msg), "{msg}");
        }
        // 真实的版本结论不能被误判成观测故障——否则该回滚的就不回滚了。
        for msg in [
            "pod of deployment/cogneva in fatal waiting state CrashLoopBackOff",
            "Error from server (NotFound): deployments.apps \"cogneva\" not found",
            "deployment/cogneva has no image for container cogneva",
            "rollout of deployment/cogneva did not complete within 300s (last: 1|1|1|1|0|1)",
        ] {
            assert!(!is_cluster_unreachable(msg), "{msg}");
        }
    }

    #[test]
    fn convergence_needs_ready_replicas_and_no_unavailable_ones() {
        assert!(rollout_converged("1|1|1|1|1|"));
        assert!(rollout_converged("1|1|1|1|1|0"));
        // 旧副本仍 ready 让 ready==spec 提前成立，但新副本 unavailable。
        assert!(!rollout_converged("1|1|1|1|1|1"));
        // observedGeneration 没追上 generation：清单刚提交，控制器还没认。
        assert!(!rollout_converged("2|1|1|1|1|0"));
        // 滚动交替期的空输出/缺字段一律当未收敛。
        assert!(!rollout_converged(""));
        assert!(!rollout_converged("1|1|1"));
    }

    /// 假 kubectl：观测类查询（generation）在前 `blind_queries` 次以集群不可达
    /// 失败，之后正常收敛；其余查询一律正常。用来区分「看不到集群」与
    /// 「看清楚了这个版本有问题」。
    fn fake_kubectl_blind_observations(dir: &Path, blind_queries: u64) -> String {
        let log = dir.join("kubectl.log");
        let count = dir.join("blind.count");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"generation"*)
    n=$(cat '{count}' 2>/dev/null || echo 0)
    n=$((n+1)); echo "$n" > '{count}'
    if [ "$n" -le {blind_queries} ]; then
      echo "Unable to connect to the server: net/http: TLS handshake timeout" >&2
      exit 1
    fi
    echo "1|1|1|1|1|" ;;
  *"terminated.finishedAt"*) echo "" ;;
  # Pod 现场查询（就绪门禁与诊断共用）同时含 waiting.reason，必须比它先匹配。
  *"deletionTimestamp"*) echo "p-new|Running|true|||2026-09-17T15:46:29Z|" ;;
  # 健康查询的 jsonpath 同时含 restartCount 与 waiting.reason，必须先前
  # 者优先，否则健康查询被后者接走、返回空串被当成"没有 Pod"。
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) echo "" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            count = count.display(),
            blind_queries = blind_queries
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// 回归：2026-09-17 04:56 集群实证。四目标 apply 完、soak 结束后复查遇到
    /// `Unable to connect to the server: net/http: TLS handshake timeout`
    /// （部署器自己的构建负载把 apiserver 短暂打到掉握手），旧逻辑把它当成
    /// 滚动失败，`rolling back count=4` 把刚推上去的四个部署全滚回旧版。
    /// 观测能力故障不是版本结论，必须既不回滚、也不当作收敛。
    #[tokio::test]
    async fn a_cluster_that_cannot_be_observed_does_not_roll_back_the_pushed_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 永远观测不到部署态：正常路径之外的每一次查询都不可达。
        let kubectl = fake_kubectl_blind_observations(&bin_dir, 100_000);
        // 启动预算 6s：两三轮回合就判「预算耗尽且从未观测到」。
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 6, 6);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor
            .run(&plan)
            .await
            .expect_err("an unobservable cluster must not be reported as a healthy rollout");
        assert!(
            is_cluster_unreachable(&err.to_string()),
            "the failure must carry the unreachable marker so the caller keeps the new revision: {err}"
        );

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains("main-old"),
            "no rollback command may be issued when nothing was observed: {calls}"
        );
        assert!(
            calls.contains("set image deployment/cogneva-sandbox-executor sandbox-executor=localhost:30500/cogneva:main-new"),
            "the revision must still have been pushed before the blackout: {calls}"
        );
    }

    /// 回归（本轮新发现）：观测工具本身起不来时要说得出病因，而且不能当版本
    /// 结论。旧逻辑的 exec 失败文本落在任何判据之外，`fail_without_blind_rollback`
    /// 认不出它，直接回滚刚推上去的好版本——而回滚只退镜像，一个坏掉的工具路径
    /// 下一轮同样起不来，等于用一次无谓回滚换掉一个可能正常的版本。
    ///
    /// 真实形态见过两种：CI 上的 `failed to run kubectl: Text file busy`（文件正
    /// 被写入），以及二进制没有可执行位。这里用后者，稳定可复现。
    #[tokio::test]
    async fn a_rollout_whose_observation_tool_cannot_start_names_it_and_keeps_the_revision() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 文件在、内容也对，就是没有可执行位：spawn 直接 EACCES，一次查询都没发出去。
        let path = bin_dir.join("fake-kubectl");
        std::fs::write(&path, "#!/bin/sh\necho ok\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let executor =
            RolloutExecutor::new(path.to_string_lossy().to_string(), "cogneva", 0, 1, 6, 6);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor
            .run(&plan)
            .await
            .expect_err("a rollout whose observation tool cannot start must fail");
        assert_eq!(
            err.class,
            FailureClass::Environment,
            "a tool that cannot start says nothing about the revision: {err}"
        );
        let msg = err.to_string();
        assert!(
            is_observation_tool_failure(&msg),
            "the failure must carry the observation-tool marker: {msg}"
        );
        assert!(
            msg.contains("fake-kubectl"),
            "the record must name which binary could not be run: {msg}"
        );
    }

    /// 反面：抖动会过去。几次观测失败之后集群恢复，滚动应当照常收敛，既不
    /// 回滚也不误报失败——容忍的是短暂不可达，不是无边界的等待。
    #[tokio::test]
    async fn a_transient_blackout_clearing_up_lets_the_rollout_converge() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_blind_observations(&bin_dir, 2);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 0, 1, 60, 60);
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        executor
            .run(&plan)
            .await
            .expect("a temporary blackout must not fail a rollout that then converges");

        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains("main-old"),
            "a recovered blackout must not leave a rollback behind: {calls}"
        );
    }

    #[test]
    fn manifest_bundle_naming_is_stable() {
        assert_eq!(target_manifest_key("cogneva"), "deploy-cogneva.yaml");
        assert_eq!(
            manifests_configmap_name("0123456789abcdef"),
            "mainline-manifests-0123456789ab"
        );
    }

    #[test]
    fn kustomization_resources_parsed_and_validated() {
        let ok =
            parse_kustomization_resources("resources:\n  - namespace.yaml\n  - deployment.yaml\n")
                .unwrap();
        assert_eq!(ok, vec!["namespace.yaml", "deployment.yaml"]);
        // 缺 resources 列表硬报错（发布集定义缺失不该静默当空集）。
        assert!(parse_kustomization_resources("kind: Kustomization\n").is_err());
        // 条目非字符串硬报错。
        assert!(parse_kustomization_resources("resources:\n  - a: b\n").is_err());
    }

    #[test]
    fn duplicate_resources_detected() {
        let dup = vec![
            "a.yaml".to_string(),
            "b.yaml".to_string(),
            "a.yaml".to_string(),
        ];
        assert_eq!(duplicate_resources(&dup), Some("a.yaml"));
        let uniq = vec!["a.yaml".to_string(), "b.yaml".to_string()];
        assert_eq!(duplicate_resources(&uniq), None);
    }

    #[test]
    fn cluster_scoped_kind_classification() {
        for k in [
            "Namespace",
            "StorageClass",
            "ClusterRole",
            "ClusterRoleBinding",
            "CustomResourceDefinition",
            "ValidatingWebhookConfiguration",
        ] {
            assert!(is_cluster_scoped_kind(k), "{k} should be cluster-scoped");
        }
        for k in [
            "Deployment",
            "ConfigMap",
            "Service",
            "Role",
            "RoleBinding",
            "Secret",
        ] {
            assert!(!is_cluster_scoped_kind(k), "{k} should be namespace-scoped");
        }
    }

    #[test]
    fn namespace_docs_skips_cluster_scoped_and_rbac_and_rejects_secret() {
        // 多文档：Namespace（集群级）与 Role（权限面）跳过，ConfigMap/Service 保留，空文档跳过。
        let yaml = "---\nkind: Namespace\nmetadata:\n  name: x\n---\nkind: ConfigMap\nmetadata:\n  name: c\n---\nkind: Role\nmetadata:\n  name: r\nrules: []\n---\nkind: RoleBinding\nmetadata:\n  name: rb\n---\nkind: Service\nmetadata:\n  name: svc\n---\n";
        let docs = namespace_docs(yaml, "mixed.yaml").unwrap();
        let kinds: Vec<&str> = docs
            .iter()
            .filter_map(|d| d.get("kind").and_then(|k| k.as_str()))
            .collect();
        assert_eq!(kinds, vec!["ConfigMap", "Service"]);
        // Secret 混入是硬错误（零带外凭证红线）。
        let secret = "kind: Secret\nmetadata:\n  name: s\n";
        let err = namespace_docs(secret, "secret.yaml").unwrap_err();
        assert!(err.to_string().contains("forbidden"), "{err}");
    }

    #[test]
    fn patch_deployment_image_rewrites_named_container() {
        let yaml = "kind: Deployment\nmetadata:\n  name: cogneva\nspec:\n  template:\n    spec:\n      containers:\n        - name: sidecar\n          image: old-side\n        - name: cogneva\n          image: old\n";
        let out = patch_deployment_image(yaml, "d.yaml", "cogneva", "cogneva", "new").unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        let containers = v
            .get("spec")
            .unwrap()
            .get("template")
            .unwrap()
            .get("spec")
            .unwrap()
            .get("containers")
            .unwrap()
            .as_sequence()
            .unwrap();
        // sidecar 不动，cogneva 改写。
        assert_eq!(
            containers[0].get("image").unwrap().as_str(),
            Some("old-side")
        );
        assert_eq!(containers[1].get("image").unwrap().as_str(), Some("new"));
    }

    #[test]
    fn patch_deployment_image_guards() {
        // kind 非 Deployment。
        assert!(patch_deployment_image(
            "kind: Service\nmetadata:\n  name: cogneva\n",
            "s",
            "cogneva",
            "cogneva",
            "i"
        )
        .is_err());
        // 名字对不上。
        let d = "kind: Deployment\nmetadata:\n  name: other\nspec:\n  template:\n    spec:\n      containers:\n        - name: cogneva\n          image: old\n";
        assert!(patch_deployment_image(d, "d", "cogneva", "cogneva", "i").is_err());
        // 容器名不存在（命中 0 个）。
        let no_container = "kind: Deployment\nmetadata:\n  name: cogneva\nspec:\n  template:\n    spec:\n      containers:\n        - name: zzz\n          image: old\n";
        assert!(patch_deployment_image(no_container, "d", "cogneva", "cogneva", "i").is_err());
        // 同名容器两个（命中 2 个，歧义拒绝）。
        let dup = "kind: Deployment\nmetadata:\n  name: cogneva\nspec:\n  template:\n    spec:\n      containers:\n        - name: cogneva\n          image: a\n        - name: cogneva\n          image: b\n";
        assert!(patch_deployment_image(dup, "d", "cogneva", "cogneva", "i").is_err());
        // containers 结构缺失。
        let no_spec = "kind: Deployment\nmetadata:\n  name: cogneva\nspec: {}\n";
        assert!(patch_deployment_image(no_spec, "d", "cogneva", "cogneva", "i").is_err());
    }

    fn bundle_targets() -> Vec<RolloutTargetConfig> {
        vec![
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
                name: "cogneva".into(),
                manifest: Some("evolution-deployment.yaml".into()),
            },
        ]
    }

    fn deployment_yaml(name: &str, container: &str) -> String {
        format!(
            "kind: Deployment\nmetadata:\n  name: {name}\nspec:\n  template:\n    spec:\n      containers:\n        - name: {container}\n          image: placeholder\n"
        )
    }

    #[test]
    fn build_rollout_bundle_splits_support_and_targets() {
        let mut files = BTreeMap::new();
        files.insert(
            "namespace.yaml".to_string(),
            "kind: Namespace\nmetadata:\n  name: cogneva\n".to_string(),
        );
        files.insert(
            "configmap.yaml".to_string(),
            "kind: ConfigMap\nmetadata:\n  name: c\ndata:\n  k: v\n".to_string(),
        );
        files.insert(
            "deployment.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        files.insert(
            "evolution-deployment.yaml".to_string(),
            deployment_yaml("cogneva-evolution", "cogneva"),
        );
        // kustomization resources 顺序与 targets 顺序不同，输出按 targets 序。
        let kustomization =
            "resources:\n  - namespace.yaml\n  - evolution-deployment.yaml\n  - configmap.yaml\n  - deployment.yaml\n";
        let bundle = build_rollout_bundle(
            &files,
            kustomization,
            &bundle_targets(),
            "reg/cogneva:main-x",
        )
        .unwrap();
        // support 只含 ConfigMap（Namespace 集群级被跳过）。
        assert!(bundle.support_yaml.contains("kind: ConfigMap"));
        assert!(!bundle.support_yaml.contains("kind: Namespace"));
        // targets 按声明序：cogneva 先，evolution 后；镜像已改写。
        assert_eq!(bundle.targets.len(), 2);
        assert_eq!(bundle.targets[0].deployment, "cogneva");
        assert_eq!(bundle.targets[0].key, "deploy-cogneva.yaml");
        assert!(bundle.targets[0].yaml.contains("reg/cogneva:main-x"));
        assert_eq!(bundle.targets[1].deployment, "cogneva-evolution");
    }

    #[test]
    fn build_rollout_bundle_guards() {
        let mut files = BTreeMap::new();
        files.insert(
            "deployment.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        files.insert(
            "evolution-deployment.yaml".to_string(),
            deployment_yaml("cogneva-evolution", "cogneva"),
        );
        // kustomization 缺 evolution-deployment.yaml：目标声明了清单却不在发布集，硬错误。
        let partial = "resources:\n  - deployment.yaml\n";
        let err = build_rollout_bundle(&files, partial, &bundle_targets(), "img").unwrap_err();
        assert!(
            err.to_string().contains("not in kustomization resources"),
            "{err}"
        );
        // kustomization 引用了 files 里不存在的资源：硬错误。
        let kustomization =
            "resources:\n  - deployment.yaml\n  - evolution-deployment.yaml\n  - missing.yaml\n";
        let err =
            build_rollout_bundle(&files, kustomization, &bundle_targets(), "img").unwrap_err();
        assert!(
            err.to_string().contains("missing from bundle files"),
            "{err}"
        );
    }

    #[test]
    fn build_rollout_bundle_rejects_secret_in_support() {
        let mut files = BTreeMap::new();
        files.insert(
            "deployment.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        files.insert(
            "evolution-deployment.yaml".to_string(),
            deployment_yaml("cogneva-evolution", "cogneva"),
        );
        files.insert(
            "secret.yaml".to_string(),
            "kind: Secret\nmetadata:\n  name: s\n".to_string(),
        );
        let kustomization =
            "resources:\n  - deployment.yaml\n  - evolution-deployment.yaml\n  - secret.yaml\n";
        let err =
            build_rollout_bundle(&files, kustomization, &bundle_targets(), "img").unwrap_err();
        assert!(err.to_string().contains("forbidden"), "{err}");
    }

    /// 目标清单是多文档文件（Deployment + 配套 Service 同文件）时组包必须
    /// 成功：Deployment 改写 image、Service 原样随目标下发。单文档解析会在
    /// 这里报 "more than one document" 并卡死整条发布链路——实机事故回归。
    #[test]
    fn build_rollout_bundle_handles_multidoc_target_manifest() {
        let mut files = BTreeMap::new();
        files.insert(
            "deployment.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        files.insert(
            "evolution-deployment.yaml".to_string(),
            format!(
                "{}---\nkind: Service\nmetadata:\n  name: cogneva-evolution\nspec:\n  ports:\n    - port: 8080\n",
                deployment_yaml("cogneva-evolution", "cogneva")
            ),
        );
        let kustomization = "resources:\n  - deployment.yaml\n  - evolution-deployment.yaml\n";
        let bundle =
            build_rollout_bundle(&files, kustomization, &bundle_targets(), "reg/img:main-x")
                .unwrap();
        let target = bundle
            .targets
            .iter()
            .find(|t| t.deployment == "cogneva-evolution")
            .expect("evolution target present");
        assert!(
            target.yaml.contains("image: reg/img:main-x"),
            "{target_yaml}",
            target_yaml = target.yaml
        );
        assert!(
            target.yaml.contains("kind: Service"),
            "companion Service must ride along the target manifest: {}",
            target.yaml
        );
    }

    /// 目标清单里的红线与支持包一致：Secret 硬报错；名字不匹配的 Deployment
    /// 硬报错（错滚比不滚危险）；没有目标 Deployment 也硬报错。
    #[test]
    fn multidoc_target_manifest_guards() {
        // Secret 混进目标文件：forbidden。
        let with_secret = format!(
            "{}---\nkind: Secret\nmetadata:\n  name: s\n",
            deployment_yaml("cogneva-evolution", "cogneva")
        );
        let err = patch_deployment_image(
            &with_secret,
            "evolution-deployment.yaml",
            "cogneva-evolution",
            "cogneva",
            "img",
        )
        .unwrap_err();
        assert!(err.to_string().contains("forbidden"), "{err}");

        // 名字不匹配的 Deployment：硬错误。
        let wrong_name = format!(
            "{}---\n{}",
            deployment_yaml("cogneva-evolution", "cogneva"),
            deployment_yaml("other-deployment", "cogneva")
        );
        let err = patch_deployment_image(
            &wrong_name,
            "evolution-deployment.yaml",
            "cogneva-evolution",
            "cogneva",
            "img",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("does not match rollout target"),
            "{err}"
        );

        // 只有 Service 没有 Deployment：恰好一个 Deployment 的约束报错。
        let no_deploy = "kind: Service\nmetadata:\n  name: s\nspec:\n  ports:\n    - port: 1\n";
        let err = patch_deployment_image(
            no_deploy,
            "evolution-deployment.yaml",
            "cogneva-evolution",
            "cogneva",
            "img",
        )
        .unwrap_err();
        assert!(err.to_string().contains("found 0"), "{err}");
    }

    #[tokio::test]
    async fn dispatch_with_manifests_publishes_bundle_and_mounts_it() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, _rev_a, rev_b) = setup_repos(root).await;

        // 发布清单树推进 bare/main：组包经 git show 从 bare 读，与工作树无关。
        // setup_repos 把工作树停在 rev_a（bare 在 rev_b），必须先对齐再提交，
        // 否则 push 非快进被拒。
        real_git(&work, &["reset", "--hard", &rev_b]).await;
        let k3s = work.join("deploy/k3s");
        std::fs::create_dir_all(&k3s).unwrap();
        std::fs::write(
            k3s.join("kustomization.yaml"),
            "resources:\n  - namespace.yaml\n  - configmap.yaml\n  - gateway-deployment.yaml\n  - sandbox-executor-deployment.yaml\n  - deployment.yaml\n  - evolution-deployment.yaml\n",
        )
        .unwrap();
        std::fs::write(
            k3s.join("namespace.yaml"),
            "kind: Namespace\nmetadata:\n  name: cogneva\n",
        )
        .unwrap();
        std::fs::write(
            k3s.join("configmap.yaml"),
            "kind: ConfigMap\nmetadata:\n  name: cogneva-config\ndata:\n  k: v\n",
        )
        .unwrap();
        std::fs::write(
            k3s.join("gateway-deployment.yaml"),
            deployment_yaml("cogneva-security-gateway", "security-gateway"),
        )
        .unwrap();
        std::fs::write(
            k3s.join("sandbox-executor-deployment.yaml"),
            deployment_yaml("cogneva-sandbox-executor", "sandbox-executor"),
        )
        .unwrap();
        std::fs::write(
            k3s.join("deployment.yaml"),
            deployment_yaml("cogneva", "cogneva"),
        )
        .unwrap();
        std::fs::write(
            k3s.join("evolution-deployment.yaml"),
            deployment_yaml("cogneva-evolution", "cogneva"),
        )
        .unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "manifest tree"]).await;
        real_git(&work, &["push", "origin", "main"]).await;
        let rev_c = real_git_stdout(
            &bare,
            &["--git-dir", bare.to_str().unwrap(), "rev-parse", "main"],
        )
        .await;

        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_c));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.deliver_manifests = true;
        let deployer = MainlineDeployer::new(cfg, ws);

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        let log = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        let pull_tag = main_image("localhost:30500", &rev_c);
        // 清单包 ConfigMap 先于 Job 发布，且被 Job 以只读卷挂载。
        assert!(
            log.contains(&manifests_configmap_name(&rev_c)),
            "manifests configmap missing: {log}"
        );
        assert!(log.contains("--manifests-dir"), "job args missing: {log}");
        assert!(log.contains("\"mountPath\": \"/manifests\""), "{log}");
        // 判定进程不能是 BestEffort：Job Pod 模板自带显式 requests/limits，
        // 且取值来自配置面（夹具用的是与默认值不同的数，命中即证明没写死）。
        assert!(
            log.contains("\"resources\""),
            "job must declare resources: {log}"
        );
        for needle in [
            "\"cpu\": \"7m\"",
            "\"memory\": \"21Mi\"",
            "\"cpu\": \"333m\"",
            "\"memory\": \"199Mi\"",
        ] {
            assert!(log.contains(needle), "missing {needle}: {log}");
        }
        // support.yaml 只带命名空间级资源（Namespace 集群级被跳过），
        // 四个 deployment 清单镜像全部改写为节点 pull 端点引用。
        assert!(log.contains("kind: ConfigMap"), "{log}");
        assert!(!log.contains("kind: Namespace"), "{log}");
        assert!(
            log.contains(&format!("image: {pull_tag}")),
            "target manifests must carry the pull-endpoint image: {log}"
        );
        // placeholder 镜像不能漏进包里（漏了说明有 deployment 没被改写）。
        assert!(!log.contains("image: placeholder"), "{log}");
    }

    #[tokio::test]
    async fn manifest_bundle_failure_blocks_dispatch() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // 夹具仓库没有 deploy/k3s 清单树：组包必须硬失败，Job 一个都不派。
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.deliver_manifests = true;
        let deployer = MainlineDeployer::new(cfg, ws);

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        let err = deployer.poll_once().await.unwrap_err();
        std::env::set_var("PATH", old_path);

        assert!(
            err.to_string().contains("kustomization.yaml"),
            "bundle failure must name the missing release set: {err}"
        );
        let log = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !log.contains("apply -f -"),
            "no job may be dispatched: {log}"
        );
        // in_flight 停在 Pushed：下轮 poll 走复用路径重试派发，不重建镜像。
        let state = deployer.load_state();
        let inflight = state.in_flight.expect("in_flight must stay for retry");
        assert_eq!(inflight.rev, rev_b);
        assert_eq!(inflight.phase, Phase::Pushed);
    }

    #[test]
    fn build_rollout_bundle_target_without_manifest_is_skipped() {
        let mut files = BTreeMap::new();
        files.insert(
            "deployment.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        // 第二个目标 manifest=None：不进包，滚动侧对它回落 set image。
        let mut targets = bundle_targets();
        targets[1].manifest = None;
        let kustomization = "resources:\n  - deployment.yaml\n";
        let bundle = build_rollout_bundle(&files, kustomization, &targets, "img").unwrap();
        assert_eq!(bundle.targets.len(), 1);
        assert_eq!(bundle.targets[0].deployment, "cogneva");
    }
}
