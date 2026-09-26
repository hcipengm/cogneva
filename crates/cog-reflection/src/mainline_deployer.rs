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
use cog_core::contract::version::VersionId;
use cog_core::{SFError, SFResult, ShutdownSignal};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::{CodePlatform, MainlineDeployerConfig, RolloutTargetConfig};

/// buildah 存储库放 sandbox PVC：与金丝雀 publisher 共享基镜像层缓存，
/// Pod 重启不丢。
const BUILDAH_STORAGE: &str = "/opt/cogneva/sandbox/containers/storage";
const BUILDAH_RUNROOT: &str = "/opt/cogneva/sandbox/containers/run";
/// cargo registry 缓存同样落 PVC：镜像里的 /usr/local/cargo 是容器可写层，
/// Pod 一重建（主线滚动最后一个目标就是 evolution 自己）索引与 crate 缓存
/// 全丢，每次构建都要在家庭网络上重拉整个 crates.io 索引。
const CARGO_HOME_PVC: &str = "/opt/cogneva/sandbox/cargo-home";

/// 镜像里那个二进制的落点。overlay 单独替换它，与资产表不同源：
/// 它来自构建产物目录，不是 rev 检出里的文件。
const OVERLAY_BINARY_DEST: &str = "/opt/cogneva/cogneva";

/// 运行时资产表在检出的位置。真正的表是仓库里的这个文件，见
/// [`MainlineDeployer::asset_list`]：部署器永远比它正在部署的 rev 旧一代，
/// 表若编在它里面，就描述的是上一代的资产。
const OVERLAY_ASSET_LIST_PATH: &str = "deploy/overlay-assets.json";

/// 最终镜像烤了、但 overlay 明确不刷新的资产，形如（镜像内落点，为什么刷不了）。
///
/// 这张表的用途是让「不刷新」成为一条要有人辩护的声明，而不是一次遗漏：
/// 门禁读 Dockerfile 最终阶段的 COPY 行，凡不在资产表里、又不在
/// [`OVERLAY_BINARY_DEST`] 上的落点，必须在这里留下理由，否则测试红。
const OVERLAY_UNREFRESHABLE: &[(&str, &str)] = &[(
    "/opt/cogneva/web",
    "由镜像的 node 阶段从 web/src 构建，overlay 内没有 node 工具链，新 rev 的前端产物无法在 overlay 内生成",
)];

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

/// 拆 `http://host:port[/prefix]` 成 (host, port, prefix)。平台 API 基址来自
/// 配置/env（形如 `http://cogneva-security-gateway:8081/github`），与 registry
/// 端点不同：它带一个路径前缀，请求行要带上前缀才落到透传分支上。
fn split_http_base(base: &str) -> Option<(String, u16, String)> {
    let rest = base.trim().strip_prefix("http://")?;
    let (authority, prefix) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (host, port) = endpoint_host_port(authority)?;
    Some((
        host.to_string(),
        port,
        prefix.trim_end_matches('/').to_string(),
    ))
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

/// 命中即无自救可能的 Pod 等待态：镜像引用非法、挂载/配置错误、容器反复
/// 崩溃退出。出现这些状态的新副本永远不会 ready，等再久也只会烧 rollout
/// 超时，必须立即判败触发回滚。
const FATAL_WAITING_REASONS: &[&str] = &[
    "InvalidImageName",
    "CreateContainerConfigError",
    "CrashLoopBackOff",
];

/// 拉取失败的等待态。**不能**与上面那组并列——它们各自都说不出自己的病因：
/// k8s 的这两个 reason 只说明"有过一次拉取失败"，既可能是镜像源此刻不服务
/// （registry 正在重启、被驱逐、网络闪断，会自愈），也可能是永远不会有这个
/// 镜像。判据只有 reason 一个字符面时两种因同形，判死就会回滚一份完好的版本：
/// 线上实测过一次，一次 support apply 让 registry 与四个后端一起重启，目标 Pod
/// 在 4 秒后被读成 `ErrImagePull` 版本类失败并回滚（那正是 support 等待那次修的
/// 那一段空窗；但空窗不止我们自己造成的那一种）。
///
/// 而这类失败的真实性质是**新版本一次都没跑起来**：它说不出新版本的好坏，
/// 所以归环境类、不回滚，等镜像源恢复后新副本自己就能起来（见
/// IMAGE_SOURCE_UNAVAILABLE_MARKER）。
const IMAGE_PULL_WAITING_REASONS: &[&str] = &["ErrImagePull", "ImagePullBackOff"];

/// 支撑清单 apply 会动到的工作负载种类。滚动目标本身是 Deployment，但目标由
/// 逐目标等待单独负责，这里的判据不覆盖它们。
const SUPPORT_WORKLOAD_KINDS: &[&str] = &["deploy", "statefulset"];

/// How many read-only pre-flight reads may each spend a whole wait budget
/// retrying: the support-workload generations before the apply, the same
/// reading after it, and the workloads' ConfigMap consumers.
///
/// All three are reads taken before anything changes, and all three are
/// allowed to retry for the wait budget when the cluster does not answer (a
/// single expired attempt is a stall, not a verdict). The Job's deadline is
/// derived from this count as well as from the per-target budgets, because a
/// Job killed by its deadline never runs its own rollback — that half-rolled
/// cluster is what the deadline exists to prevent.
const PREFLIGHT_RETRYING_READS: u64 = 3;

/// 支撑工作负载"这次滚动可以往下走了"的读法：代数、观测到的代数、期望副本数、
/// 就绪副本数。竖线显式占位，缺字段（omitempty）不能顶掉后面的位置。
const SUPPORT_SETTLE_JSONPATH: &str =
    "jsonpath={.metadata.generation}|{.status.observedGeneration}|{.spec.replicas}|{.status.readyReplicas}";

/// 每个工作负载读了哪些 ConfigMap 的读法：名字，然后四个消费面各一段，竖线分段。
/// 卷、projected 里的 ConfigMap 源、envFrom、env.valueFrom（容器与 initContainer
/// 各两段）。竖线显式占位：一个没有卷的工作负载整段是空的，不能让缺字段把后面的
/// 段顶掉。
const CONFIG_CONSUMER_JSONPATH: &str = concat!(
    "jsonpath={range .items[*]}{.metadata.name}{'|'}",
    "{range .spec.template.spec.volumes[*]}{.configMap.name}{','}{end}{'|'}",
    "{range .spec.template.spec.volumes[*]}{.projected.sources[*].configMap.name}{','}{end}{'|'}",
    "{range .spec.template.spec.containers[*].envFrom[*]}{.configMapRef.name}{','}{end}{'|'}",
    "{range .spec.template.spec.containers[*].env[*].valueFrom.configMapKeyRef.name}{','}{end}{'|'}",
    "{range .spec.template.spec.initContainers[*].envFrom[*]}{.configMapRef.name}{','}{end}{'|'}",
    "{range .spec.template.spec.initContainers[*].env[*].valueFrom.configMapKeyRef.name}{','}{end}",
    "{'\\n'}{end}"
);

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

/// 自己的副本里有没有必死等待态（配置错误、CrashLoop、镜像引用非法）。这些副本
/// 永远等不到 ready，等下去只会把预算烧完；判据与超时路径同一份枚举。
///
/// 拉取失败的等待态不在这一份里：它判不了死（见 IMAGE_PULL_WAITING_REASONS），
/// 归预算到期那一刻的环境类处置。
fn rollout_pods_fatal(samples: &[PodSample]) -> Option<String> {
    samples
        .iter()
        .filter(|p| !p.terminating)
        .find(|p| FATAL_WAITING_REASONS.contains(&p.waiting_reason.as_str()))
        .map(|p| format!("{} waiting={}", p.name, p.waiting_reason))
}

/// 这批等待原因里有没有「拉取失败」。init 容器与主容器一起看：init 拉不到镜像时
/// 主容器只报 PodInitializing，只看主容器会把这一档整个漏掉。
///
/// 返回命中的那个原因本身（而不是布尔）：判词里要写出是哪一个等待态，人才知道
/// kubelet 停在哪一步。
fn image_pull_blocked(reasons: &[String]) -> Option<&str> {
    reasons
        .iter()
        .map(String::as_str)
        .find(|r| IMAGE_PULL_WAITING_REASONS.contains(r))
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
/// 观测到版本有毛病。kubectl 二进制缺失、没有可执行位这类 exec 失败下一次查询
/// 都没发出去。
///
/// 单列一类而不是并进 cluster-unreachable：那一类的语义是"重试可能等到"
/// （apiserver 抖动、握手超时），`probe` 会按轮询节拍在预算内重试；工具起不来
/// 在本进程生命周期里重试多少次都一样，必须就地返回——但归的还是环境类，
/// 因为它同样说不出新版本的好坏，而回滚只退镜像，修不好一个坏掉的工具路径。
///
/// ETXTBSY（"文件正被写入"）也会落进这个标记，但它是这一类里唯一一个"重试就
/// 会好"的成员——起因是装可执行文件时把写描述符漏给了并发 fork 的兄弟进程，
/// 已在落位路径上从源头消除。真在生产上撞到一次，代价也只是本 rev 按环境类
/// 失败返回，不回滚、不占尝试预算。
const OBSERVATION_TOOL_MARKER: &str = "observation tool unavailable";

fn is_observation_tool_failure(msg: &str) -> bool {
    msg.contains(OBSERVATION_TOOL_MARKER)
}

/// 「集群不许我们做这件事」的标记：apiserver 的 RBAC 拒绝。发布集里出现这个 SA
/// 不该持有的对象时，apply 在动镜像之前就被拒；此时新版本的好坏一个字都没读到，
/// 真正要改的是发布集或权限授予（两者都在安装面/人工那一侧）。
///
/// 判据只认 apiserver 自己的授权措辞，不用裸 `forbidden` 这个词：组包侧自己的
/// 「Secret in manifest bundle is forbidden」也含它，判据混在一起会把组包错误读成
/// 授权拒绝。授权的动词面是封闭的（get/list/watch/create/update/patch/delete/
/// deletecollection），逐个列全，免得某一种动词被漏掉后掉回版本类。
const AUTHORIZATION_DENIED_MARKER: &str = "authorization denied";

const AUTHORIZATION_DENIED_PATTERNS: &[&str] = &[
    AUTHORIZATION_DENIED_MARKER,
    "is forbidden: User",
    "cannot get resource",
    "cannot list resource",
    "cannot watch resource",
    "cannot create resource",
    "cannot update resource",
    "cannot patch resource",
    "cannot delete resource",
    "cannot deletecollection resource",
];

fn is_authorization_denied(msg: &str) -> bool {
    AUTHORIZATION_DENIED_PATTERNS
        .iter()
        .any(|p| msg.contains(p))
}

/// 「集群此刻装不下这份请求」：apiserver 的准入拒绝，主体是容量或策略约束，不是
/// "你没权限"。配额打满（ResourceQuota）、单容器资源越出区间（LimitRange）、被
/// PodSecurity 拒掉，读到的都是集群此刻的状态。与授权拒绝同一族——新版本的好坏
/// 一个字都没读到，要改的是集群容量或策略面，两者都在安装面/人工那一侧。
///
/// 单列一类而不是并进授权拒绝：两者的措辞面不重叠（授权拒绝带 `User "..."` 主体、
/// 动词面是封闭枚举），合成一条后回归测试就没法逐项断言是哪条判据接住的。
///
/// 判据同样不认裸 `forbidden`：组包侧自己的措辞也含它，混在一起会把组包错误读成
/// 环境类，反而放走一个真坏的版本。
const ADMISSION_POLICY_DENIED_PATTERNS: &[&str] = &[
    // ResourceQuota：`... is forbidden: exceeded quota: cogneva-quota, requested: ...`
    "exceeded quota",
    // LimitRange：`... is forbidden: [maximum cpu usage per Container is 2, but limit is 4]`
    "usage per Container is",
    // PodSecurity：`... violates PodSecurity "restricted:latest": ...`
    "violates PodSecurity",
];

/// 手上的原文（kubectl 的原话）里有没有准入拒绝的措辞。只用于镜像一次都还没动
/// 时的归类——那一步除了上游原文没有别的观测面。
fn is_admission_policy_denied(msg: &str) -> bool {
    ADMISSION_POLICY_DENIED_PATTERNS
        .iter()
        .any(|p| msg.contains(p))
}

/// 判定进程给「准入面把新副本挡在建 Pod 之前」打的标记，与调度器判决的
/// PLACEMENT_BLOCKED 互斥：前者 Pod 对象压根没被创建，后者 Pod 建了但排不上队。
///
/// 与上面那组措辞刻意分成两条判据。超时记录里除了标记还附着一份给人看的现场采样
/// （ReplicaSet 的 `FailedCreate` 消息），措辞与上游原文完全同形；若用措辞做第二
/// 遍判据，「这次上线自己把 requests 调过了配额」那一支也会被读成环境类——而环境
/// 类不占尝试预算，一个真坏的版本会就此无限重试下去。
const ADMISSION_DENIED_MARKER: &str = "admission policy denied";

fn is_admission_denied(msg: &str) -> bool {
    msg.contains(ADMISSION_DENIED_MARKER)
}

/// 新副本卡在拉镜像上、整段预算里一次都没跑起来：这次失败**没有观测到版本**，
/// 说不出新版本的好坏——镜像源自己可能就是病因。带这个标记的失败按环境类处理
/// 且不回滚：回滚到一个更旧的镜像并不能让镜像源恢复，而留在那里的新版本在镜像
/// 源恢复后自己就能起来；判成版本类则会把这个 rev 记成"坏"而不再重试。
///
/// 措辞只说"没拿到证据"，不说断言：镜像到底在不在，这条判定不负责回答。
const IMAGE_SOURCE_UNAVAILABLE_MARKER: &str = "image source unavailable";

fn is_image_source_unavailable(msg: &str) -> bool {
    msg.contains(IMAGE_SOURCE_UNAVAILABLE_MARKER)
}

/// 这次失败**坏在哪**。类别说"这次失败说不说得出新版本的问题"，落点说的是
/// "坏在哪一处"——后者是判"同 rev 反复失败是不是同一处坏"的证据。
///
/// 落点由代码在识别处给出，不从错误措辞里解析：措辞里带着 Pod 名、耗时、端口
/// 这类每次都变的噪声，拿它做证据会得出"每次都是新失败"，于是永远停不下来。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureLocus {
    /// 集群不可达：看不到集群，说不出新版本好坏。
    Unreachable,
    /// 准入面拒绝（配额打满、LimitRange/PodSecurity 越界）：新容器一次都没起来。
    Admission,
    /// 调度器放不下：节点装不下，不是这份变更的事。
    Placement,
    /// 观测工具本身起不来：一次查询都没发出去。
    Tool,
    /// 授权被拒：读不到要看的东西。
    AuthDenied,
    /// 镜像源没供上镜像：新副本卡在拉取上，一次都没跑起来。
    ImageSource,
    /// 其余按"观测到的版本缺陷"论，含回滚过的那一支。
    Observed,
}

impl FailureLocus {
    /// 全部落点，供读回签名时校验值域用。
    const ALL: [FailureLocus; 7] = [
        FailureLocus::Unreachable,
        FailureLocus::Admission,
        FailureLocus::Placement,
        FailureLocus::Tool,
        FailureLocus::AuthDenied,
        FailureLocus::ImageSource,
        FailureLocus::Observed,
    ];

    fn as_str(self) -> &'static str {
        match self {
            FailureLocus::Unreachable => "unreachable",
            FailureLocus::Admission => "admission",
            FailureLocus::Placement => "placement",
            FailureLocus::Tool => "tool",
            FailureLocus::AuthDenied => "auth",
            FailureLocus::ImageSource => "image-source",
            FailureLocus::Observed => "observed",
        }
    }

    /// 这次落点说不说得出版本的问题。`before_any_change` = 一个镜像都还没动
    /// （此时没有回滚对象，判的只是"这次失败有没有信息量"）。
    ///
    /// 两个阶段判据的集合有意不同：滚动中撞上授权拒绝仍要回滚（改过的东西得退
    /// 回去），而滚动中认准入只看判定进程打的标记、不认措辞——超时记录里附着的
    /// 现场采样与上游原文同形，用措辞再判一遍会把"这次上线自己把 requests 调过了
    /// 配额"那一支也读成环境类，那一个真坏的版本就再也等不到回滚。
    ///
    /// 镜像源同理属环境：它说的是"新版本一次都没跑起来"，不是版本好坏。
    fn class(self, before_any_change: bool) -> FailureClass {
        let environment = if before_any_change {
            self != FailureLocus::Observed
        } else {
            matches!(
                self,
                FailureLocus::Unreachable
                    | FailureLocus::Admission
                    | FailureLocus::Placement
                    | FailureLocus::Tool
                    | FailureLocus::ImageSource
            )
        };
        if environment {
            FailureClass::Environment
        } else {
            FailureClass::Version
        }
    }
}

/// 镜像一次都还没动时的落点。快照阶段只有这四类非版本失败会出现——调度器判决
/// 得等新 Pod 出现，这里还没有新 Pod。
fn locate_before_any_change(msg: &str) -> FailureLocus {
    if is_cluster_unreachable(msg) {
        FailureLocus::Unreachable
    } else if is_observation_tool_failure(msg) {
        FailureLocus::Tool
    } else if is_authorization_denied(msg) {
        FailureLocus::AuthDenied
    } else if is_admission_policy_denied(msg) {
        FailureLocus::Admission
    } else {
        FailureLocus::Observed
    }
}

/// 镜像一次都还没动时失败的归类，外加这次失败在发布流程里的位置。位置（阶段 +
/// 目标）与落点一起构成这次失败的签名，部署器拿它判"同 rev 反复失败是不是同一处
/// 坏"——同一处坏两次是可复现的确定性失败，再滚只会得到同一份证据。
fn classify_before_any_change(stage: &str, target: &str, e: SFError) -> RolloutFailure {
    let locus = locate_before_any_change(&e.to_string());
    RolloutFailure::at(stage, target, locus, true, e)
}

/// 一次失败的签名：类别 + 在发布流程里的位置（阶段与目标）+ 落点。同 rev 两次
/// 失败签名相同，说明是同一处坏、可复现；签名变了说明情况在动，还值得再看一次。
fn failure_signature(
    class: FailureClass,
    stage: &str,
    target: &str,
    locus: FailureLocus,
) -> String {
    format!("{}:{stage}:{target}:{}", class.as_str(), locus.as_str())
}

/// 这次失败是不是已经见过的那一处坏。证据取不到（`None`）时不做区分：读不出
/// 落点的失败恰是"判不准"，它不排除"和上次同因"，按同因记——判不准就往停下的一
/// 侧取，误停只是等一个新 rev，误放是无限重滚一个真坏的 rev。
fn failure_repeats(seen: &[Option<String>], now: Option<&str>) -> bool {
    seen.iter().any(|prev| match (prev.as_deref(), now) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    })
}

/// 从终止消息里读回失败签名。只认我们自己写下的形状（`类别:阶段:目标:落点`，
/// 四个字段各在值域内）：容器可能因为别的原因死掉，别的进程也可能往这条通道里
/// 写过东西，把一段陌生文本当签名会让"这次和上次是不是同一处坏"变成掷骰子。
fn parse_failure_signature(msg: &str) -> Option<String> {
    let text = msg.lines().next()?.trim();
    let parts: Vec<&str> = text.split(':').collect();
    if parts.len() != 4 {
        return None;
    }
    let known_class = matches!(
        parts.first().copied(),
        Some("version") | Some("environment")
    );
    let known_locus = FailureLocus::ALL.iter().any(|l| l.as_str() == parts[3]);
    if known_class && known_locus {
        Some(text.to_string())
    } else {
        None
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

/// 滚动失败的类别。分的是**这次失败说不说得出新版本的问题**：版本类是这份
/// 变更的结论，环境类只是集群此刻的样子。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FailureClass {
    /// 观测到的版本缺陷：探针不过、启动卡死、支撑清单 apply 失败、容器反复崩溃。
    /// 该回滚；也是唯一能构成"这个 rev 坏"的结论、进而让重试停下的一类。
    #[default]
    Version,
    /// 集群环境问题：调度器放不下新 Pod、准入面挡在建 Pod 之前、我们看不到集群，
    /// 或新副本卡在拉镜像上（镜像源此刻供不上，会自愈）。这些都说不出新版本的好
    /// 坏，Job 不回滚，重试也不该被当成"这个版本又坏了一次"。
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

/// 滚动失败连同它的类别与签名一起带出 `RolloutExecutor::run`。判定进程与部署器在
/// 同一个 crate，类别走类型而不是让部署器回头解析 Job 日志里的字符串；签名再经
/// 进程的终止消息（k8s 给"这个容器为什么死"留的那条窄通道）过一遍 Job 边界。
#[derive(Debug)]
pub struct RolloutFailure {
    /// 这次失败属于哪一类。
    pub class: FailureClass,
    /// 这次失败坏在哪一处（类别 + 阶段 + 目标 + 落点）。部署器用它判"同 rev
    /// 反复失败是不是同一处坏"。
    pub signature: String,
    /// 原始错误，措辞与判据都不变（Job 日志、错误文本原样保留）。
    pub error: SFError,
}

impl RolloutFailure {
    /// 唯一的失败构造点。类别、落点判据与签名在这里一次成形，两个阶段各自只
    /// 提供"坏在哪"和"处在哪一步"——分开构造会让同一次失败在两处得到两种类别。
    fn at(
        stage: &str,
        target: &str,
        locus: FailureLocus,
        before_any_change: bool,
        error: SFError,
    ) -> Self {
        let class = locus.class(before_any_change);
        Self {
            class,
            signature: failure_signature(class, stage, target, locus),
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

/// 部署器从一个失败的滚动 Job 上读回的东西：类别（终止码）与失败落点（终止消息）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct JobFailure {
    class: FailureClass,
    /// 读不到就是 `None`：没有区分力的证据，不是一种新的失败。
    signature: Option<String>,
}

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

/// 这次上线动没动过部署的放置面。两个"判不准"都算动过：滚动前的快照是空的
/// （没采到），或读不到当前的面。
///
/// 判不准就往版本侧靠——宁可多回滚一次，也不放走"新版本自己把 Pod 顶出节点 /
/// 自己超过了配额"的那一支：环境类不占尝试预算，误放会让一个真坏的版本无限重试。
fn placement_shape_unchanged(prev_shape: &str, now_shape: Option<&str>) -> bool {
    !prev_shape.is_empty() && now_shape == Some(prev_shape)
}

/// 这条超时算不算"集群放不下"（环境类）；是就给出排不上队的 Pod 与调度器消息。
///
/// 环境类只在两个条件同时成立时给出：调度器确实判了排不上队，且部署的放置面
/// 与滚动前逐字节一致——放置面没变，说明不是这次上线把 Pod 顶出节点的。
/// 任一侧读空、或读不到当前放置面（`None`）一律不给环境结论。
fn environment_class(
    blocked: &[(String, String)],
    prev_shape: &str,
    now_shape: Option<&str>,
) -> Option<String> {
    if blocked.is_empty() || !placement_shape_unchanged(prev_shape, now_shape) {
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

/// 本次滚动里被准入面拒掉的新副本，返回可直接读的原因。
///
/// 与 [`environment_class`] 共用同一条边界：只有当这次上线**没动过放置面**时，
/// 拒掉新副本的才是集群此刻的容量或策略面（配额被别的部署占满、LimitRange 被
/// 改窄），而不是这份变更自己。变更自己把 requests 调过了配额、加了个越界的
/// LimitRange 值，都是这份变更的毛病，该回滚也该占一次尝试。
///
/// 判据来自 RS 自己的类型化条件（准入拒绝不产生 Pod 对象，Pod 采样一条都取不到），
/// 不读渲染好的诊断串——那份文本是给人看的现场。
fn admission_rejection(
    failures: &[ReplicaSetFailure],
    prev_shape: &str,
    now_shape: Option<&str>,
) -> Option<String> {
    if !placement_shape_unchanged(prev_shape, now_shape) {
        return None;
    }
    failures
        .iter()
        .find(|f| is_admission_policy_denied(&f.detail()))
        .map(ReplicaSetFailure::detail)
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
///
/// 判据分两层，因为两个问题不是同一个：**要不要动**由部署面回答（认不认得 rev、
/// 是不是同 rev、是不是分叉），**这个 rev 还能不能滚**由失败证据回答。
/// 部署面认不出 rev **不是**"这个 rev 没失败过"的证据——滚动失败后回滚成浮动签
/// 留下的正是认不出 rev 的形态，把第二层挂在第一层的分支里，恰好在失败制造出来的
/// 状态里把安全阀旁路掉，循环会无限重滚一个已知坏的 rev。
#[derive(Debug, PartialEq, Eq)]
enum AdvanceDecision {
    Advance,
    SameRev,
    NotAncestor,
    Mixed,
    InCooldown,
    /// 同一处坏在这个 rev 上复现过：确定性失败，停下等新 rev。
    Repeated,
}

/// 上一条失败 rev 的记账。回答的是同一个问题——"这个 rev 现在还能不能再试"——
/// 所以合成一格，免得几个数各走各的。
#[derive(Debug, Clone, Copy)]
struct RetryBudget {
    /// 环境类失败的限速窗截止时刻（unix 秒）。
    cooldown_until: i64,
    /// 当前 bare rev 就是刚失败的那个（状态里的 `failed_rev` 命中了它）。
    retry_of_failed_rev: bool,
    /// 那次失败的类别。环境类只看限速窗，版本类只看证据有没有复现。
    class: FailureClass,
    /// 同一处坏已经复现过一次（证据在记账处判定，这里只读结论）。
    repeated: bool,
}

fn evaluate_advance(
    bare_rev: &str,
    deployed: &DeployedState,
    is_ancestor: bool,
    now_ts: i64,
    retry: RetryBudget,
) -> AdvanceDecision {
    // 第一层：部署面。只回答"要不要动"，不做记账判据。
    match deployed {
        DeployedState::Mixed => return AdvanceDecision::Mixed,
        DeployedState::Main(d) => {
            if rev12(d) == rev12(bare_rev) {
                return AdvanceDecision::SameRev;
            }
            if !is_ancestor {
                return AdvanceDecision::NotAncestor;
            }
        }
        // 认不出 rev 只说明部署面给不出答案，不构成放行理由——记账判据在下面
        // 按 failed_rev 独立生效。
        DeployedState::Legacy => {}
    }

    // 第二层：这个 rev 还能不能再滚。只看失败记账，与部署面认出没认出 rev 无关。
    if retry.retry_of_failed_rev {
        match retry.class {
            // 环境类不是版本结论：唯一的判据是别在同一个满节点上空转，冷却过了
            // 就再试，没有次数上限——把"集群装不下"读成"这个版本试够了"会让一个
            // 本来正常的版本被搁置到下个 rev。
            FailureClass::Environment => {
                if now_ts < retry.cooldown_until {
                    return AdvanceDecision::InCooldown;
                }
            }
            // 版本类：同一处坏复现过就停。新的 rev 让 `retry_of_failed_rev` 变假，
            // 于是 fix-forward 提交（修的正是上次的失败原因）立即恢复推进，不被
            // 任何时间窗挡——那正是这条判据要保证的事。
            FailureClass::Version => {
                if retry.repeated {
                    return AdvanceDecision::Repeated;
                }
            }
        }
    }
    AdvanceDecision::Advance
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
    /// 环境类失败的限速窗：环境类失败本身不含版本结论，它只决定"隔多久再看一眼
    /// 这台集群"。版本类不走这个窗——那类失败没有时间维度可言，见 `failed_repeated`。
    failed_cooldown_until: i64,
    /// `failed_rev` 上**版本类**失败各自坏在哪（签名去重后的集合，`None` = 有一
    /// 次读不出落点）。这是"这个 rev 还让不让再滚"的全部依据：签名重复出现说明
    /// 同一处坏是可复现的确定性失败，重滚只会拿到同一份证据；签名每次都不同说明
    /// 情况在动（每次滚得比上次远），还值得再看一次。没有次数上限、没有时间窗。
    #[serde(default)]
    failed_signatures: Vec<Option<String>>,
    /// 同一处坏已经复现过一次：确定性失败，停下等人/等新 rev。它由证据推出，
    /// 不是一个能调大调小的预算。
    #[serde(default)]
    failed_repeated: bool,
    /// `failed_rev` 那次失败的类别。决定重试时能否复用镜像，old state.json
    /// 没有这一格，缺省按版本类（保守：宁可重建）。
    #[serde(default)]
    failed_class: FailureClass,
    /// 上游 CI 已经报失败、因此被按住不滚的 rev。没有这一格，"被门禁按住"
    /// 与"没有新 rev 可滚"在心跳上完全同形。
    #[serde(default)]
    ci_hold_rev: Option<String>,
}

// ---------------------------------------------------------------------------
// 构建侧
// ---------------------------------------------------------------------------

pub struct MainlineDeployer {
    cfg: MainlineDeployerConfig,
    /// 部署器独占一棵稳定路径的工作树。与进化任务的工作树互不干涉：这里是
    /// 唯一能自由 `reset --hard` 的检出，任何第三方检出停在哪里都不影响它。
    workspaces: std::sync::Arc<crate::workspace::WorkspaceManager>,
    /// 最近一次上游跟踪的结论，心跳里明说。上游这条链最容易长成"看着在跟、
    /// 其实没跟"：没有它，只跟随 bare 的部署与跟踪坏掉的部署日志一模一样。
    upstream_note: std::sync::Mutex<String>,
    /// 版本契约读数出口。为 None 时判据照跑、只写日志——判据的存在不依赖
    /// 有没有人订阅它的读数。
    metrics: Option<std::sync::Arc<dyn cog_core::MetricsBackend>>,
}

/// 版本契约的读数：与判据分开。
///
/// "离最近一次 release 多远"是读数而不是判据——卡阈值会把正常前进判成违规；
/// 真正让人没法忽略它的是派生标签：距离变了，名字就变了，一个名字不会再覆盖
/// 两个代码状态。
struct VersionReadings {
    /// 最近的 release tag 与它在 tracked main 上的首父提交数。
    nearest_release: Option<(String, u64)>,
    /// tracked main 声明的版本。
    declared: Option<String>,
}

/// git 的 tree-ish 形式：`<rev>:<path>`，路径**不带前导斜杠**。
///
/// 带斜杠的那一版（`<rev>:/<path>`）不是「路径多了一个字符」这么轻：git 会把整串
/// 当成一个 object name 去解，然后报 `Not a valid object name`——于是目录枚举永远
/// 失败，收敛面每轮都停在「没有结论」，`apply` 一次也没跑过。前导斜杠看上去与
/// 工作树里的绝对路径同形，正是它值得一条判据守的原因：这里构造的是 git 的
/// tree-ish，不是文件系统路径。
fn tree_ish_dir(rev: &str, dir: &str) -> String {
    format!("{}:{}/", rev, dir.trim_matches('/'))
}

impl MainlineDeployer {
    pub fn new(
        cfg: MainlineDeployerConfig,
        workspaces: std::sync::Arc<crate::workspace::WorkspaceManager>,
    ) -> Self {
        Self {
            cfg,
            workspaces,
            upstream_note: std::sync::Mutex::new("pending".to_string()),
            metrics: None,
        }
    }

    /// Report the version contract's own readings.
    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn cog_core::MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
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
        let summary = heartbeat_message(
            &self.load_state(),
            &bare,
            &self.upstream_note(),
            chrono::Utc::now().timestamp(),
        );
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

    /// 构建标签：声明版本 + 距最近 release 的提交数 + rev。
    ///
    /// 描述的是工作树而不是某个 commit-ish：`git describe` 不允许 `--dirty` 与
    /// commit-ish 同时出现（实测 `fatal: option '--dirty' and commit-ishes cannot
    /// be used together`），而工作树在 `ensure_source_at` 里刚被 `reset --hard`
    /// 到目标 rev 并 `clean -ffdx`，描述的正是即将编译的那份源码。
    ///
    /// 参数与 `deploy/scripts/version-id.sh` 是同一套——那个脚本是 shell 侧三个
    /// 生产者的唯一一份，这里是 Rust 侧唯一一份。参数不一致不会报错，只会让两边
    /// 报出的名字悄悄不同，所以由门禁逐个核对（`--match` 尤其要紧：裸仓里还有
    /// `promote/*` 这类本地 tag，漏了它会把它们当成最近的 release）。
    ///
    /// 描述到的 rev 不是要构建的那个就报 unknown：标签的全部用处就是区分代码状态，
    /// 一个指向别的提交的名字比"未知"更坏。取不到 tag 同样退化成
    /// `<声明版本>-unknown` 而不失败——这个标签是印章不是闸门，为它停掉整条主线是
    /// 反向的。退化本身可见：读到的距离是"未知"，不是 0。
    async fn git_version_id(&self, rev: &str) -> SFResult<String> {
        let declared = env!("CARGO_PKG_VERSION");
        let unknown = |why: &str| {
            warn!(
                rev = %rev12(rev),
                "git describe {why}; the image will report its build label as unknown distance"
            );
            Ok(format!("v{declared}-unknown"))
        };
        let described = self
            .git_src(&[
                "describe", "--tags", "--long", "--dirty", "--match", "v[0-9]*",
            ])
            .await
            .ok()
            .map(|out| out.trim().to_string())
            .filter(|id| !id.is_empty());
        let Some(id) = described else {
            return unknown("found no reachable release tag");
        };
        // 名字里的 rev 必须就是要构建的那个提交，否则它描述的是另一份源码。
        match VersionId::parse(&id) {
            Ok(parsed) if rev.starts_with(parsed.rev.as_str()) => Ok(id),
            Ok(parsed) => unknown(&format!(
                "described {} while {} is being built",
                parsed.rev,
                rev12(rev)
            )),
            Err(_) => unknown(&format!("output {id} is not a version id")),
        }
    }

    /// bare 仓库指定分支的完整 rev。
    pub(crate) async fn bare_main_rev(&self) -> SFResult<String> {
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

    /// git 中一个 tag 解引用后的提交。
    async fn tag_commit(&self, tag: &str) -> Option<String> {
        self.run_cmd(
            "git",
            &["--git-dir", &self.cfg.bare_repo, "rev-list", "-n1", tag],
            None,
            30,
        )
        .await
        .ok()
        .map(|out| out.trim().to_string())
        .filter(|rev| !rev.is_empty())
    }

    /// `rev` 处工作区声明的版本（`workspace.package.version`）。
    async fn declared_version_at(&self, rev: &str) -> Option<String> {
        let manifest = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "show",
                    &format!("{}:Cargo.toml", rev),
                ],
                None,
                30,
            )
            .await
            .ok()?;
        cog_core::contract::version::declared_version(&manifest).map(|v| v.to_string())
    }

    /// 收集版本契约的证据，全部来自本进程已经在跟的那份历史。
    ///
    /// 声明链的完整性要一起收：走到尽头才能说"这个版本从未被声明"，没走到
    /// 就只能是"读不到"。浅克隆的边界提交在"有没有父提交"上和根提交给出
    /// 同一个答案，所以 completeness 由 `--is-shallow-repository` 判，而不是
    /// 由"最老的声明提交是不是根提交"猜。
    async fn version_evidence(
        &self,
        main: &str,
    ) -> (cog_core::contract::version::Evidence, VersionReadings) {
        use cog_core::contract::version::{DeclarationChange, Evidence, ReleaseTag, TagSet};

        // Cargo.toml 只在被改动的提交上才有新版本值，所以沿这条路径取就够了。
        let touching = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "log",
                    "--first-parent",
                    "--format=%H",
                    main,
                    "--",
                    "Cargo.toml",
                ],
                None,
                120,
            )
            .await
            .unwrap_or_default();

        let mut revs: Vec<&str> = touching
            .split_whitespace()
            .filter(|r| !r.is_empty())
            .collect();
        // git 给的是新→旧；按历史顺序（旧→新）比较，才能把 from/to 说对。
        revs.reverse();
        let mut declarations = Vec::new();
        let mut previous: Option<String> = None;
        let mut current_version: Option<String> = None;
        let mut chain_seen = 0usize;
        for rev in revs {
            chain_seen += 1;
            let Some(version) = self.declared_version_at(rev).await else {
                continue;
            };
            match &previous {
                Some(before) if before != &version => declarations.push(DeclarationChange {
                    rev: rev.to_string(),
                    from: before.clone(),
                    to: version.clone(),
                }),
                None => declarations.push(DeclarationChange {
                    rev: rev.to_string(),
                    from: version.clone(),
                    to: version.clone(),
                }),
                _ => {}
            }
            previous = Some(version.clone());
            current_version = Some(version);
        }

        let shallow = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "rev-parse",
                    "--is-shallow-repository",
                ],
                None,
                30,
            )
            .await
            .map(|out| out.trim() == "true")
            .unwrap_or(true);
        let chain_complete = !shallow && chain_seen > 0;

        // 本仓 refs/tags 里的 release tag：判 tag 忠实与 release 落点。
        let mut releases = Vec::new();
        let mut reachable: Vec<(String, u64)> = Vec::new();
        if let Ok(list) = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "for-each-ref",
                    "--format=%(refname:short)",
                    "refs/tags/v*",
                ],
                None,
                60,
            )
            .await
        {
            for tag in list.split_whitespace() {
                let Some(commit) = self.tag_commit(tag).await else {
                    continue;
                };
                let on_main = self.is_ancestor(&commit, main).await;
                let declared = if on_main {
                    self.declared_version_at(&commit).await.unwrap_or_default()
                } else {
                    String::new()
                };
                if on_main {
                    if let Ok(count) = self
                        .run_cmd(
                            "git",
                            &[
                                "--git-dir",
                                &self.cfg.bare_repo,
                                "rev-list",
                                "--count",
                                &format!("{}..{}", commit, main),
                            ],
                            None,
                            60,
                        )
                        .await
                    {
                        if let Ok(n) = count.trim().parse::<u64>() {
                            reachable.push((tag.to_string(), n));
                        }
                    }
                }
                releases.push(ReleaseTag {
                    tag: tag.to_string(),
                    rev: commit,
                    on_tracked_main: on_main,
                    declared_version: declared,
                });
            }
        }

        // 各平台自己能看到的 release tag：判各点的 tag 集合是否一致。
        let mut tag_sets = Vec::new();
        for up in &self.cfg.upstreams {
            let slug = up.platform.slug();
            let prefix = format!("refs/cogneva/tags/{}/", slug);
            let pattern = format!("{}v*", prefix);
            let Ok(list) = self
                .run_cmd(
                    "git",
                    &[
                        "--git-dir",
                        &self.cfg.bare_repo,
                        "for-each-ref",
                        // 全名，不是 `:short`：`%(refname:short)` 只剥 refs/heads 与
                        // refs/tags 这类众所周知的层级，本仓的 refs/cogneva/... 会
                        // 原样留成 `cogneva/tags/<点>/v0.5.8`——拿完整前缀去剥一个都
                        // 剥不掉，每个点的集合都是空的，而"两个点都空"在判据里读作
                        // 一致。空的读数不能长得像一致的读数。
                        "--format=%(refname)",
                        &pattern,
                    ],
                    None,
                    60,
                )
                .await
            else {
                continue;
            };
            let mut tags: Vec<String> = list
                .split_whitespace()
                .filter_map(|name| name.strip_prefix(prefix.as_str()).map(|s| s.to_string()))
                .collect();
            tags.sort();
            tag_sets.push(TagSet {
                point: slug.to_string(),
                tags,
            });
        }

        // `describe` 取的是最近的 tag（距离最小的那个），读数与它对齐，
        // 免得同一个代码状态在名字里说 93、在读数里说别的。
        let nearest = reachable.into_iter().min_by_key(|(_, distance)| *distance);
        (
            Evidence {
                declarations,
                chain_complete,
                releases,
                tag_sets,
            },
            VersionReadings {
                nearest_release: nearest,
                declared: current_version,
            },
        )
    }

    /// 版本契约：每轮都判、每轮都上报，判据不挂在任何一次推进上。
    ///
    /// 判在部署器里，是因为它持有本集群唯一那份会被推进的历史：三个生产者
    /// 的 push 走三条不同的路（开发会话直推、进化闭环落地、贡献通道），唯一
    /// 共同的汇聚面就是这份历史加 CI。判据挂在这里覆盖前两者，CI 侧跑同一份
    /// 纯函数覆盖后者。
    pub async fn report_version_contract(&self, main: &str) {
        use cog_core::contract::version::{judge, Clause, Verdict};
        use cog_core::metric_names::{
            VERSION_COMMITS_SINCE_RELEASE, VERSION_CONTRACT_CHECKS_TOTAL,
            VERSION_CONTRACT_VIOLATIONS, VERSION_DECLARED_INFO,
        };

        let (evidence, readings) = self.version_evidence(main).await;
        let report = judge(&evidence);
        let violations = report.violations();

        if !report.holds() {
            warn!(
                main = %rev12(main),
                clauses = %violations
                    .iter()
                    .map(|v| format!("{}: {} ({})", v.clause.as_str(), v.subject, v.detail))
                    .collect::<Vec<_>>()
                    .join(" | "),
                unreadable = %report
                    .unreadable()
                    .iter()
                    .map(|(clause, why)| format!("{clause}: {why}"))
                    .collect::<Vec<_>>()
                    .join(" | "),
                "version contract does not hold"
            );
        } else {
            info!(main = %rev12(main), "version contract holds");
        }

        let Some(metrics) = &self.metrics else {
            return;
        };
        let mut labels = std::collections::HashMap::new();
        for clause in Clause::ALL {
            let verdict = report.verdict(clause);
            let count = match verdict {
                Verdict::Violated(vs) => vs.len(),
                _ => 0,
            };
            let outcome = match verdict {
                Verdict::Satisfied => "satisfied",
                Verdict::Violated(_) => "violated",
                Verdict::Unreadable(_) => "unreadable",
            };
            labels.clear();
            labels.insert("clause".to_string(), clause.as_str().to_string());
            let _ = metrics
                .record_gauge(VERSION_CONTRACT_VIOLATIONS, count as f64, labels.clone())
                .await;
            labels.insert("outcome".to_string(), outcome.to_string());
            let _ = metrics
                .record_counter(VERSION_CONTRACT_CHECKS_TOTAL, 1.0, labels.clone())
                .await;
        }
        if let Some((tag, distance)) = &readings.nearest_release {
            let mut labels = std::collections::HashMap::new();
            labels.insert("release".to_string(), tag.clone());
            let _ = metrics
                .record_gauge(VERSION_COMMITS_SINCE_RELEASE, *distance as f64, labels)
                .await;
        }
        if let Some(declared) = &readings.declared {
            let mut labels = std::collections::HashMap::new();
            labels.insert("version".to_string(), declared.clone());
            let _ = metrics
                .record_gauge(VERSION_DECLARED_INFO, 1.0, labels)
                .await;
        }
    }

    fn set_upstream_note(&self, note: &str) {
        if let Ok(mut slot) = self.upstream_note.lock() {
            *slot = note.to_string();
        }
    }

    fn upstream_note(&self) -> String {
        self.upstream_note
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| "unknown".into())
    }

    /// 上游跟踪：把各平台的 main 拉进 bare，再把 bare 的本地分支按祖先关系
    /// 推进到最新的那个 head，返回推进后的 rev（没推进返回 None）。
    ///
    /// 只写 refs 与对象、不碰任何工作树，因此可以和构建并行跑。多个平台之间
    /// 取"是所有其他候选的祖先"的那一个；两个 head 互不为祖先（真分叉）时
    /// 一个都不取，只告警——替分叉猜一个方向，等于用一次分叉决定集群跑谁的
    /// 代码。
    async fn refresh_upstream(&self, current: &str) -> Option<String> {
        if self.cfg.upstreams.is_empty() || self.cfg.git_proxy_base.trim().is_empty() {
            self.set_upstream_note("off");
            return None;
        }
        let base = self.cfg.git_proxy_base.trim_end_matches('/');

        let mut heads: Vec<(CodePlatform, String)> = Vec::new();
        let mut unreachable: Vec<String> = Vec::new();
        let mut unreachable_platforms: Vec<&'static str> = Vec::new();
        for up in &self.cfg.upstreams {
            let repo = up.repo.trim().trim_end_matches(".git");
            let url = format!("{base}/{}/{repo}.git", up.platform.slug());
            let local = format!("refs/cogneva/upstream/{}", up.platform.slug());
            let refspec = format!("+refs/heads/{}:{local}", self.cfg.branch);
            // Release tags are imported per platform as well as into
            // `refs/tags`, because they answer two different questions: the
            // per-platform refs are what the version contract compares between
            // points, while `refs/tags` is what `git describe` -- and therefore
            // every derived version label -- reads. Without this refspec the
            // bare repo keeps only the tags it was seeded with, so the next
            // release tag never arrives and describe keeps naming the previous
            // release.
            //
            // `--no-tags` stays: it turns off the implicit following of tags,
            // not explicit refspecs. `--prune-tags` must never be added here --
            // the bare repo also holds `promote/*` and `gen-*` tags, and
            // pruning would take the baseline-port chain with it.
            let platform_tags =
                format!("+refs/tags/v*:refs/cogneva/tags/{}/v*", up.platform.slug());
            let release_tags = "+refs/tags/v*:refs/tags/v*";
            match self
                .run_cmd(
                    "git",
                    &[
                        "--git-dir",
                        &self.cfg.bare_repo,
                        "fetch",
                        "--no-tags",
                        "--force",
                        &url,
                        &refspec,
                        &platform_tags,
                        release_tags,
                    ],
                    None,
                    self.cfg.upstream_fetch_timeout_secs,
                )
                .await
            {
                Ok(_) => {
                    match self
                        .run_cmd(
                            "git",
                            &["--git-dir", &self.cfg.bare_repo, "rev-parse", &local],
                            None,
                            30,
                        )
                        .await
                    {
                        Ok(rev) => heads.push((up.platform, rev.trim().to_string())),
                        Err(e) => {
                            unreachable.push(format!("{}: {e}", up.platform.slug()));
                            unreachable_platforms.push(up.platform.slug());
                        }
                    }
                }
                Err(e) => {
                    unreachable.push(format!("{}: {e}", up.platform.slug()));
                    unreachable_platforms.push(up.platform.slug());
                }
            }
        }
        if !unreachable.is_empty() {
            warn!(
                upstreams = %unreachable.join("; "),
                "upstream fetch failed; leaving the bare main where it is"
            );
        }
        if heads.is_empty() {
            self.set_upstream_note(&format!("unreachable({})", unreachable.len()));
            return None;
        }

        // 候选 = bare 当前 main 的后代；等于 main 的不算推进，不是后代的不动。
        let mut ahead: Vec<(CodePlatform, String)> = Vec::new();
        for (platform, rev) in heads {
            if rev == current {
                continue;
            }
            if self.is_ancestor(current, &rev).await {
                ahead.push((platform, rev));
            } else {
                warn!(
                    platform = platform.slug(),
                    rev = %rev12(&rev),
                    "upstream main is not a descendant of the bare main; ignoring it"
                );
            }
        }

        let mut best: Option<(CodePlatform, String)> = None;
        let mut diverged = false;
        for (platform, rev) in ahead {
            let Some((bp, brev)) = best.take() else {
                best = Some((platform, rev));
                continue;
            };
            if self.is_ancestor(&brev, &rev).await {
                best = Some((platform, rev));
            } else if self.is_ancestor(&rev, &brev).await {
                best = Some((bp, brev));
            } else {
                diverged = true;
                warn!(
                    a = %format!("{}={}", bp.slug(), rev12(&brev)),
                    b = %format!("{}={}", platform.slug(), rev12(&rev)),
                    "upstream mains diverged; refusing to advance the bare main"
                );
                best = Some((bp, brev));
            }
        }
        if diverged {
            self.set_upstream_note(&upstream_note_with("diverged", &unreachable_platforms));
            return None;
        }
        let Some((platform, rev)) = best else {
            self.set_upstream_note(&upstream_note_with(
                &format!("up-to-date({})", rev12(current)),
                &unreachable_platforms,
            ));
            return None;
        };

        // compare-and-swap：期间有别的写者动过 main 就让这次失败，下轮重来，
        // 不去覆盖别人的结果。
        let branch_ref = format!("refs/heads/{}", self.cfg.branch);
        match self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "update-ref",
                    &branch_ref,
                    &rev,
                    current,
                ],
                None,
                30,
            )
            .await
        {
            Ok(_) => {
                info!(
                    platform = platform.slug(),
                    rev = %rev12(&rev),
                    from = %rev12(current),
                    "upstream main advanced the bare main"
                );
                self.set_upstream_note(&upstream_note_with(
                    &format!("advanced({}={})", platform.slug(), rev12(&rev)),
                    &unreachable_platforms,
                ));
                Some(rev)
            }
            Err(e) => {
                warn!(error = %e, rev = %rev12(&rev), "could not advance the bare main; retrying next poll");
                self.set_upstream_note(&upstream_note_with(
                    "advance-failed",
                    &unreachable_platforms,
                ));
                None
            }
        }
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

    /// 平台 API 的只读 GET：明文 HTTP 打到安全网关的透传端点（凭证由网关在
    /// 出口注入，本进程零 token），与 registry 客户端同形。非 200、不可达、
    /// 响应不是 JSON 一律 Err——调用方按"没有证据"处理，不按失败处理。
    async fn platform_get_json(&self, api_base: &str, path: &str) -> SFResult<serde_json::Value> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (host, port, prefix) = split_http_base(api_base).ok_or_else(|| {
            SFError::Config(format!(
                "platform api base {api_base:?} is not http://host:port[/prefix]"
            ))
        })?;
        let req = format!(
            "GET {prefix}{path} HTTP/1.1\r\nHost: {host}\r\nAccept: application/json\r\n\
             User-Agent: cogneva-mainline-deployer\r\nConnection: close\r\n\r\n"
        );
        let mut stream = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::net::TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| SFError::IO(format!("platform api {host}:{port} connect timed out")))?
        .map_err(|e| SFError::IO(format!("platform api {host}:{port} connect failed: {e}")))?;
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("platform api request write failed: {e}")))?;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut raw))
            .await
            .map_err(|_| SFError::IO("platform api read timed out".into()))?
            .map_err(|e| SFError::IO(format!("platform api read failed: {e}")))?;
        let (status, body) = parse_http_response(&raw)
            .ok_or_else(|| SFError::IO("platform api returned a malformed HTTP response".into()))?;
        if status != 200 {
            return Err(SFError::IO(format!("platform GET {path} -> {status}")));
        }
        serde_json::from_slice(&body)
            .map_err(|e| SFError::IO(format!("platform GET {path} returned non-JSON: {e}")))
    }

    /// 该 rev 在某平台上的 CI 结论：check runs 优先，提交状态兜底，折判据与
    /// 落地通道共用同一份（取不到就是 `None`，两个消费面都不许猜）。
    async fn platform_ci_verdict(&self, api_base: &str, repo: &str, rev: &str) -> Option<bool> {
        let mut conclusions: Vec<String> = Vec::new();
        let mut saw_signal = false;
        let mut pending = false;
        if let Ok(v) = self
            .platform_get_json(api_base, &format!("/repos/{repo}/commits/{rev}/check-runs"))
            .await
        {
            if let Some(runs) = v.get("check_runs").and_then(|r| r.as_array()) {
                for run in runs {
                    saw_signal = true;
                    match run.get("conclusion").and_then(|c| c.as_str()) {
                        Some(c) => conclusions.push(c.to_string()),
                        // 结论为 null = 这条检查还在跑。此刻下结论等于拿半个结果判死刑。
                        None => pending = true,
                    }
                }
            }
        }
        if let Some(verdict) =
            cog_core::contract::ci::fold_ci_signals(saw_signal, pending, &conclusions)
        {
            return Some(verdict);
        }
        // 只用提交状态上报 CI 的仓库（没有 check runs），与落地通道同一条兜底。
        let status = self
            .platform_get_json(api_base, &format!("/repos/{repo}/commits/{rev}/status"))
            .await
            .ok()?;
        match status.get("state").and_then(|s| s.as_str()) {
            Some("success") => Some(true),
            Some("failure") | Some("error") => Some(false),
            _ => None,
        }
    }

    /// 要滚的这个 rev 在上游各平台的 CI 结论。`Some(false)` = 至少一个平台
    /// 给出了明确的失败结论；`None` = 没有证据（没配基址、平台不可达、仓库
    /// 不用 CI、检查还没跑完）。
    async fn ci_verdict_for_rev(&self, rev: &str) -> Option<bool> {
        let mut verdicts = Vec::new();
        for up in &self.cfg.upstreams {
            let Some(base) = up
                .api_base
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let repo = up.repo.trim().trim_end_matches(".git");
            if let Some(v) = self.platform_ci_verdict(base, repo, rev).await {
                verdicts.push(v);
            }
        }
        if verdicts.iter().any(|v| !*v) {
            return Some(false);
        }
        if verdicts.is_empty() {
            None
        } else {
            Some(true)
        }
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

        let mut bare = self.bare_main_rev().await?;
        // 先把上游拉进来再判推进：bare 的 main 由本循环自己从各平台取，
        // 宿主机不再是这条链上的一环。
        if let Some(advanced) = self.refresh_upstream(&bare).await {
            bare = advanced;
        }
        // 版本契约每轮都判，不挂在"这一轮有没有推进"上：一个只在推进时才跑
        // 的判据，在主线停住的时候（恰恰是最需要知道版本分叉没有的时候）不
        // 出声。判在早退之前，后面的 return 都绕不过它。
        self.report_version_contract(&bare).await;
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
                    state.failed_signatures.clear();
                    state.failed_repeated = false;
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
                        // 类别取自 Job 的终止码、落点取自它的终止消息（环境类不回滚
                        // 也不构成版本结论），不是从 Job 日志里找字符串。
                        let failure = self.job_failure(&job_name(&inflight.rev)).await;
                        let class = failure.class;
                        let same_rev = state.failed_rev.as_deref() == Some(inflight.rev.as_str());
                        if !same_rev {
                            // 换 rev 就是换了一份待验的东西：上一份的证据不能拿
                            // 过来用（同一个落点在两个 rev 上不是同一处坏）。
                            state.failed_signatures.clear();
                            state.failed_repeated = false;
                        }
                        if class == FailureClass::Version {
                            // 同一处坏第二次出现 → 可复现的确定性失败。证据取不到
                            // （None）时不做区分：读不出落点的失败恰是"判不准"，
                            // 它不排除"和上次同因"，按同因记。
                            if failure_repeats(
                                &state.failed_signatures,
                                failure.signature.as_deref(),
                            ) {
                                state.failed_repeated = true;
                            } else {
                                state.failed_repeated = false;
                                state.failed_signatures.push(failure.signature.clone());
                            }
                        }
                        warn!(
                            rev = %rev12(&inflight.rev),
                            class = class.as_str(),
                            signature = failure.signature.as_deref().unwrap_or("unknown"),
                            repeats = state.failed_repeated,
                            "mainline rollout job failed (rollback, if any, handled by the job itself)"
                        );
                        state.failed_rev = Some(inflight.rev.clone());
                        state.failed_class = class;
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
                class: state.failed_class,
                repeated: state.failed_repeated,
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

        // 滚动门禁：上游 CI 已经给出明确的失败结论就不滚这个 rev。bare 的
        // main 在上面就推进过了（它只是一份镜像副本，推进无害），所以这里挡
        // 的是"把红 CI 的代码送进集群"，不是"跟踪上游"——上游修好或下一个 rev
        // 追上来，这条路径自己恢复。读不到结论一律放行：拿一次平台抖动停掉
        // 整条主线跟踪，代价比偶尔滚一个恰好红的 rev 更大。
        if self.ci_verdict_for_rev(&bare).await == Some(false) {
            warn!(
                rev = %rev12(&bare),
                "upstream CI reports a failure for this rev; holding the rollout"
            );
            if state.ci_hold_rev.as_deref() != Some(bare.as_str()) {
                state.ci_hold_rev = Some(bare.clone());
                self.save_state(&state)?;
            }
            return Ok(());
        }
        if state.ci_hold_rev.take().is_some() {
            info!(
                rev = %rev12(&bare),
                "upstream CI no longer reports a failure; resuming the rollout"
            );
            self.save_state(&state)?;
        }

        let _lock = match self.acquire_lock() {
            Some(lock) => lock,
            None => return Ok(()),
        };

        // The host build gate as well as the cycle lock, because they bound
        // different things: this lock keeps one deployer's advance from racing
        // its own state file, the gate keeps this advance's compile and image
        // build from landing on top of another builder's. Refusing here rather
        // than waiting is right for a polling cycle -- it never touched state
        // yet, and it comes back on its own.
        let _build_slot = match cog_core::build_gate::try_acquire("mainline advance").await {
            Ok(slot) => slot,
            Err(e) => {
                info!(error = %e, "host is building; deferring the advance to the next cycle");
                return Ok(());
            }
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
        let ws = self.workspaces.ensure_persistent(spec).await?;
        // 与 `refresh` 同一处采样：这棵树也是共享 target 的常驻编译树，索引丢了
        // 一样会把整棵树重写一遍。采样必须在 reset 之前。
        self.workspaces.sample_index_health(&ws).await;
        self.git_src(&["reset", "--hard", rev]).await?;
        // target 目录在工作树之外，clean 只清源码，不丢增量编译缓存。
        self.git_src(&["clean", "-ffdx"]).await?;
        Ok(())
    }

    async fn build_binary(&self, rev: &str) -> SFResult<()> {
        let jobs = self.cfg.cargo_build_jobs.to_string();
        let version_id = self.git_version_id(rev).await?;
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
            // 与 rev 同源注入：源码树在工作树里本来就有 .git，但这层显式注入让
            // 镜像里的名字不依赖 build.rs 当场能否查到 tag。
            .env("COGNEVA_VERSION_ID", &version_id)
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

    /// 这次滚动要重拷的资产表。
    ///
    /// 优先进被部署 rev 的检出：本进程是上一代二进制，编在它里面的表描述的
    /// 是上一代的资产，rev 若新增了一项，交由此表构建的 overlay 就会漏掉它，
    /// 而且不会再有下一次滚动来补——这正是资产陈旧能被拖成永久的那条路。
    /// 检出里没有这个文件（早于该文件的 rev）时才回落到编译进来的那一份。
    ///
    /// 镜像是构建它的那一刻把资产烤进去的，而那一刻的检出不是现在这个 rev，
    /// 不重拷就是新二进制配旧资产跑。失配里 skills 最静默：角色注册表按 skill
    /// 声明的工具名收窄，名字一个都对不上就收窄成空表，请求里连 tools 字段
    /// 都不发，角色于是没有工具可用，而日志里一个字都没有。
    async fn asset_list(&self) -> Vec<crate::runtime_assets::AssetEntry> {
        let path = self.workdir().join(OVERLAY_ASSET_LIST_PATH);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => match crate::runtime_assets::parse_asset_list(&text) {
                Ok(list) => {
                    info!(path = %path.display(), assets = list.len(), "overlay asset list read from the checkout");
                    return list;
                }
                Err(e) => warn!(
                    path = %path.display(),
                    error = %e,
                    "overlay asset list in the checkout is unusable; falling back to the list compiled into this binary, which describes the previous revision's assets"
                ),
            },
            Err(e) => warn!(
                path = %path.display(),
                error = %e,
                "checkout carries no overlay asset list; falling back to the list compiled into this binary, which describes the previous revision's assets"
            ),
        }
        crate::runtime_assets::embedded_asset_list()
    }

    async fn buildah_steps(&self, ctr: &str, rev: &str, new_tag: &str) -> SFResult<()> {
        // 说清楚这次滚动带不动哪些资产：不带，就意味着线上跑的是基底镜像里
        // 那一份，而"哪一份"没有别的面能看出来。
        for (dest, reason) in OVERLAY_UNREFRESHABLE {
            warn!(
                dest = %dest,
                reason = %reason,
                "overlay keeps the base image's copy of this asset"
            );
        }
        let assets = self.asset_list().await;
        let bin = self.target_dir().join("release/cogneva");
        self.buildah(
            &["copy", ctr, bin.to_str().unwrap(), OVERLAY_BINARY_DEST],
            300,
        )
        .await?;
        for entry in &assets {
            let src = self.workdir().join(&entry.from);
            self.buildah(&["copy", ctr, src.to_str().unwrap(), &entry.to], 300)
                .await?;
        }

        // 把这次拷进去的是什么记进镜像本身，好让**跑最新代码的那一侧**——应用
        // 进程——自己判它的资产对不对得上本 rev。部署器判不了这件事：它永远是
        // 旧一代，只能回答上一代的问题。
        let mut digests = std::collections::BTreeMap::new();
        for entry in &assets {
            let src = self.workdir().join(&entry.from);
            let tree = crate::runtime_assets::digest_tree(&src)
                .await
                .map_err(|e| SFError::IO(format!("digest overlay asset {}: {e}", src.display())))?;
            info!(asset = %entry.to, files = tree.files, "overlay refreshed runtime asset");
            digests.insert(entry.to.clone(), tree.digest);
        }
        let manifest =
            crate::runtime_assets::manifest_json(rev, &digests).map_err(SFError::Validation)?;
        std::fs::create_dir_all(&self.cfg.state_dir)
            .map_err(|e| SFError::IO(format!("create state dir {}: {e}", self.cfg.state_dir)))?;
        let manifest_path = Path::new(&self.cfg.state_dir).join("runtime-assets.json");
        std::fs::write(&manifest_path, manifest)
            .map_err(|e| SFError::IO(format!("write {}: {e}", manifest_path.display())))?;
        self.buildah(
            &[
                "copy",
                ctr,
                manifest_path.to_str().unwrap(),
                crate::runtime_assets::RUNTIME_ASSET_MANIFEST_DEST,
            ],
            60,
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
                // 宽限（到点即判败回滚），加上支撑工作负载等就绪的那一段（同为
                // startup 预算）；Job 被 activeDeadlineSeconds 杀掉走不到 Job
                // 自己的回滚，留短了会把集群停在半滚状态。
                //
                // The two support-snapshot reads are in the bound for the same
                // reason: each may retry for a whole wait budget before the
                // first target is touched, and a bound that ignores them is a
                // bound that can expire mid-rollout.
                "activeDeadlineSeconds": (self.cfg.startup_timeout_secs
                    + self.cfg.rollout_timeout_secs
                    + 15)
                    * self.cfg.targets.len().max(1) as u64
                    + self.cfg.soak_secs
                    + self.cfg.startup_timeout_secs
                    + PREFLIGHT_RETRYING_READS * self.cfg.rollout_timeout_secs
                    + 15,
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
    pub(crate) async fn git_show(&self, rev: &str, path: &str) -> SFResult<String> {
        let spec = format!("{rev}:{path}");
        self.run_cmd(
            "git",
            &["--git-dir", &self.cfg.bare_repo, "show", &spec],
            None,
            30,
        )
        .await
    }

    /// rev 下某个目录里的文件名（只一层）。收件面由此**枚举**交付对象，而不是
    /// 拿一份写死的名字清单：清单外的文件会因此被看见，而不是静默缺席。
    pub(crate) async fn git_ls_dir(&self, rev: &str, dir: &str) -> SFResult<Vec<String>> {
        let spec = tree_ish_dir(rev, dir);
        let out = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "ls-tree",
                    "--name-only",
                    &spec,
                ],
                None,
                30,
            )
            .await?;
        Ok(out
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// `kubectl apply -f -` 落到指定命名空间，并把 stdout/stderr 交回调用方。
    ///
    /// 与 [`Self::apply_stdin`] 的差别是**不把失败变成 Err**：收敛面要用 apply
    /// 自己的报错分类成因（命名空间不存在 / CRD 不存在 / 权限被拒），失败本身
    /// 就是读数，包成一句 IO 错误就把它丢了。
    pub(crate) async fn apply_capture(
        &self,
        namespace: &str,
        body: &[u8],
        timeout_secs: u64,
    ) -> SFResult<(bool, String, String)> {
        let mut child = tokio::process::Command::new(&self.cfg.kubectl_bin)
            .args(["-n", namespace, "apply", "-f", "-"])
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
        let output =
            tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
                .await
                .map_err(|_| SFError::IO(format!("kubectl apply timed out after {timeout_secs}s")))?
                .map_err(|e| SFError::IO(format!("kubectl apply: {e}")))?;
        Ok((
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        ))
    }

    /// 读 rev 处的发布清单并组装清单包。image 必须是节点 pull 端点引用
    /// （kubelet 经 NodePort 拉取），与 set image 路径同一约束。
    ///
    /// 目录形态按目录里**有什么**判，不按配置猜：有 `kustomization.yaml` 就是
    /// 发布集目录，否则按平铺的预渲染目录读。两种形态都必须能消费，因为
    /// `manifest_dir` 是配置面，而有的 profile 的权威面就是它自己的预渲染目录
    /// ——`deploy/k3s` 装的是单节点 K3s 的形态，指给标准 K8s 会把 K3s 的宿主
    /// 路径与套接字一并下发。
    async fn build_bundle_at(&self, rev: &str, image: &str) -> SFResult<RolloutBundle> {
        let dir = self.cfg.manifest_dir.trim_end_matches('/');
        // 列不出来（目录在这个 rev 下不存在、rev 本身不可解）与"目录存在但读不出
        // 发布面"是两件事，读数要说清是哪一件，并把底层那条 git 报错带上。
        let names = self.git_ls_dir(rev, dir).await.map_err(|e| {
            SFError::Config(format!(
                "cannot read the release set at {dir} in {}: {e}; expected either a \
                 {KUSTOMIZATION_FILE} listing resources or a flat directory of manifests",
                rev12(rev)
            ))
        })?;
        let set = if names.iter().any(|n| n == KUSTOMIZATION_FILE) {
            let kustomization = self
                .git_show(rev, &format!("{dir}/{KUSTOMIZATION_FILE}"))
                .await?;
            let resources = parse_kustomization_resources(&kustomization)?;
            let mut files = BTreeMap::new();
            for res in &resources {
                let content = self.git_show(rev, &format!("{dir}/{res}")).await?;
                files.insert(res.clone(), content);
            }
            ReleaseSet::from_kustomization(files, resources)?
        } else {
            let mut files = BTreeMap::new();
            for name in names.iter().filter(|n| is_manifest_file(n)) {
                let content = self.git_show(rev, &format!("{dir}/{name}")).await?;
                files.insert(name.clone(), content);
            }
            ReleaseSet::from_flat_dir(files)
        };
        if set.is_empty() {
            return Err(SFError::Config(format!(
                "{dir} holds no manifest at {}: neither a {KUSTOMIZATION_FILE} listing \
                 resources nor a flat directory of manifests",
                rev12(rev)
            )));
        }
        let bundle = build_rollout_bundle(&set, &self.cfg.targets, image)?;
        info!(
            rev = %rev12(rev),
            dir = %dir,
            shape = set.shape(),
            resources = set.len(),
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

    /// Job 已失败时，问它的判定进程**是怎么失败的**。Job 的 Pod 模板是
    /// `backoffLimit: 0` + `restartPolicy: Never`，一个 Pod 一次运行，终止码
    /// 就是这个进程的退出码，终止消息就是它留下的失败落点。
    ///
    /// 一个 Pod 都没有时（终止码读不到、也没得读）改问 Job 的 Failed 条件：准入面
    /// 把 Pod 挡在创建之外时（配额打满、LimitRange 越界），判定进程根本不存在，
    /// 它的退出码永远不会有，而原因就写在条件消息里。这一档能判成环境类就判——
    /// 它和"码读不出来"不是一回事，是**采错了地方**而不是采不到。
    ///
    /// 采不到（Pod 已删、查询失败、码不可解析、条件消息为空）仍按**版本类**靠：
    /// 类别判不准时，把环境类误记成版本类只是多一轮零成本的等待，反过来则是一个
    /// 真坏的版本被无限重试、永不停下——代价不对称，往停下的一侧取。签名同理，
    /// 读不到就返回 `None`（部署器按"没有区分力"处理）。
    async fn job_failure(&self, name: &str) -> JobFailure {
        let missing = JobFailure {
            class: FailureClass::Version,
            signature: None,
        };
        let out = match self
            .kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &format!("job-name={name}"),
                    "-o",
                    "jsonpath={range .items[*]}{.status.containerStatuses[0].state.terminated.exitCode}{\"|\"}{.status.containerStatuses[0].state.terminated.message}{\"\\n\"}{end}",
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
                return missing;
            }
        };
        let mut class = None;
        let mut signature = None;
        for line in out.lines() {
            let mut parts = line.splitn(2, '|');
            let code = parts.next().unwrap_or("").trim().parse::<i32>().ok();
            if let Some(code) = code {
                class = Some(if code == ROLLOUT_EXIT_ENVIRONMENT {
                    FailureClass::Environment
                } else {
                    FailureClass::Version
                });
                signature = parts.next().and_then(parse_failure_signature);
            }
        }
        if let Some(class) = class {
            return JobFailure { class, signature };
        }
        let reason = self.job_failed_condition(name).await.unwrap_or_default();
        if is_cluster_unreachable(&reason)
            || is_observation_tool_failure(&reason)
            || is_authorization_denied(&reason)
            || is_admission_policy_denied(&reason)
        {
            warn!(
                job = %name,
                reason = %reason,
                "rollout job's pod was never created; classified from the job's failure condition"
            );
            return JobFailure {
                class: FailureClass::Environment,
                signature: None,
            };
        }
        missing
    }

    /// Job 的 Failed 条件消息。Pod 被准入面挡在创建之外时，这是唯一留下原因的
    /// 观测面——Job 本身建得出来，Pod 建不出来。
    async fn job_failed_condition(&self, name: &str) -> Option<String> {
        let out = self
            .kubectl(
                &[
                    "get",
                    "job",
                    name,
                    "-o",
                    "jsonpath={.status.conditions[?(@.type==\"Failed\")].message}",
                ],
                30,
            )
            .await
            .ok()?;
        let text = out.trim();
        if text.is_empty() {
            None
        } else {
            Some(text.to_string())
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

/// 上游状态标量（纯函数便于测试）：把"这次比较到底带上了哪些平台"并进结论。
///
/// 一个平台拉不动、另一个照常时，"上游没有更新"与"这次比较根本没算上那个
/// 平台"是两件事。心跳是运维唯一会读的那一行，只报 `up-to-date` 等于替一个
/// 失联的上游作证。全平台都失联时结论本身就是 `unreachable(N)`，不再重复。
fn upstream_note_with(note: &str, unreachable: &[&'static str]) -> String {
    if unreachable.is_empty() {
        note.to_string()
    } else {
        format!("{note}; unreachable={}", unreachable.join(","))
    }
}

/// 心跳摘要（纯函数便于测试）：一行覆盖空闲态全部关键状态。SameRev 收敛
/// 路径静默返回，没有这条摘要时部署器存活无法从日志证明。
fn heartbeat_message(
    state: &MainlineState,
    bare_rev: &str,
    upstream: &str,
    now_unix: i64,
) -> String {
    let in_flight = state
        .in_flight
        .as_ref()
        .map(|f| format!("{}@{:?}", rev12(&f.rev), f.phase))
        .unwrap_or_else(|| "none".into());
    format!(
        "bare={} upstream={} last_good={} in_flight={} ci_hold={} failed_rev={} failed_class={} failed_loci={} failed_repeated={} cooldown_remaining_secs={}",
        rev12(bare_rev),
        upstream,
        state.last_good_rev.as_deref().map(rev12).unwrap_or("none"),
        in_flight,
        state.ci_hold_rev.as_deref().map(rev12).unwrap_or("none"),
        state.failed_rev.as_deref().map(rev12).unwrap_or("none"),
        // 环境类失败不占证据面，`failed_rev` 与 `failed_loci=0` 会同时出现；
        // 不说出类别，这一行读起来就像记账坏了。
        state
            .failed_rev
            .as_ref()
            .map(|_| state.failed_class.as_str())
            .unwrap_or("none"),
        // "停在哪一处、是第一次还是又一处"是这一行的重点：只有这一对数能区分
        // "还在往前走"和"卡死在同一堵墙上"。
        state.failed_signatures.len(),
        state.failed_repeated,
        (state.failed_cooldown_until - now_unix).max(0),
    )
}

/// This loop's name in the liveness census.
pub const MAINLINE_DEPLOYER_LOOP: &str = "mainline_deployer";

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
    // 空闲心跳：SameRev 路径静默返回，靠周期性 INFO 摘要证明部署器存活。
    let heartbeat_every = Duration::from_secs(deployer.cfg.heartbeat_log_secs);
    // The heartbeat log exists to prove this loop is alive, and a log line is not
    // something a rule can read: without a stamp, a deployer that stopped looks
    // exactly like a main that needs no work. The supervised shape adds the other
    // half — a body that panics is run again and the restart is counted.
    let _ = cog_core::loop_health::spawn(
        MAINLINE_DEPLOYER_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        move |beat| {
            let deployer = std::sync::Arc::clone(&deployer);
            let shutdown = shutdown.clone();
            async move {
                let mut ticker = tokio::time::interval(interval);
                // Last heartbeat is per attempt: a restarted loop re-logs on its
                // first tick, which is the honest reading of "the loop started".
                let mut last_heartbeat: Option<tokio::time::Instant> = None;
                loop {
                    // Stamped every cycle, including the SameRev ones that return silently.
                    beat.beat();
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
        },
    )
    .await;
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

/// 发布集目录的形态判据：目录里有它，就按 `resources` 列表读；没有，就按
/// 平铺的预渲染目录读。
const KUSTOMIZATION_FILE: &str = "kustomization.yaml";

/// 解析 kustomization.yaml 的 resources 列表——发布集的权威定义。
fn parse_kustomization_resources(text: &str) -> SFResult<Vec<String>> {
    let v: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|e| SFError::Config(format!("parse {KUSTOMIZATION_FILE}: {e}")))?;
    let resources = v
        .get("resources")
        .and_then(|r| r.as_sequence())
        .ok_or_else(|| SFError::Config(format!("{KUSTOMIZATION_FILE} has no resources list")))?;
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

/// 资源治理 kind：与权限面同路，不经循环面下发。它们的数值由 chart values 与
/// 操作面标定（profile 决定开关与上限），而发布集是从提交好的静态清单组包的——
/// 那是一份钉死的默认值。让循环每 rev 下发它，等于用这份默认值覆盖运维调过的
/// 天花板：调高就是自治系统给自己抬资源上限，调低就是把不认识的运维标定打回去。
/// 创建归安装面（helm / 预渲染 apply），调整归 values 或人工。
const GOVERNANCE_KINDS: &[&str] = &["ResourceQuota", "LimitRange"];

/// 卷声明 kind：创建与调整都归安装面，不进循环面。绑定后的 PVC spec 除了
/// `resources.requests` 之外不可变，而那个例外还要 StorageClass 支持扩容
/// （local-path 不支持）；StorageClass 一旦绑定更是永远改不回来。所以循环
/// 每 rev 重放同一份声明时，只要声明与集群里已绑定的那份不同，apply 必被
/// 拒——那是"这份声明和历史不一致"，不是"新版本不好"，却会让一次本该成功的
/// 滚动整个失败。声明的数值由 chart values 标定、由渲染期门禁与卷声明上限
/// 校核，落盘现状由 `data_volume_over_declared_size` 规则比对声明量发现。
const STORAGE_CLAIM_KINDS: &[&str] = &["PersistentVolumeClaim"];

/// 一份文档在交付面上的归处。
///
/// 拆成一个纯函数是因为它有两个消费面：主线滚动组包（`namespace_docs`）与
/// 可观测性栈的周期收敛。两份实现一定会分叉，而分叉的样子是"同一个对象在一条
/// 路上交付、在另一条路上被跳过"。
///
/// 公开是因为它同时是**授权面**的判据：给收敛循环授什么权，取决于这套清单里
/// 到底有哪些文档真的会被它 apply。授权清单与这张表分叉，会得到"授了权却永不
/// 交付"或"要交付却没权"两种都没人看得见的状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocFate {
    /// 交付。
    Deliver,
    /// 集群级 kind：进化 SA 只有命名空间级 Role，apply 必然被拒，由安装面管理。
    ClusterScoped,
    /// 权限面 kind：一份清单能长出权限就等于权限可以自我扩张。
    Rbac,
    /// 治理 kind：上限是运维标定的，循环每 rev 重放一份钉死的默认值就是拿它
    /// 覆盖运维调过的天花板。
    Governance,
    /// 卷声明 kind：绑定后的 spec 除扩容外不可变，声明与历史不一致会被拒，
    /// 而那不是"新版本不好"。
    StorageClaim,
    /// Secret：零带外凭证红线，密钥永不进清单链路。
    ForbiddenSecret,
}

pub fn classify_doc(kind: &str) -> DocFate {
    if kind == "Secret" {
        return DocFate::ForbiddenSecret;
    }
    if is_cluster_scoped_kind(kind) {
        return DocFate::ClusterScoped;
    }
    if RBAC_KINDS.contains(&kind) {
        return DocFate::Rbac;
    }
    if GOVERNANCE_KINDS.contains(&kind) {
        return DocFate::Governance;
    }
    if STORAGE_CLAIM_KINDS.contains(&kind) {
        return DocFate::StorageClaim;
    }
    DocFate::Deliver
}

/// 拆分多文档 YAML（只拆，不过滤）：空文档（`---` 分隔产生）跳过。
pub(crate) fn split_docs(yaml_text: &str, origin: &str) -> SFResult<Vec<serde_yaml::Value>> {
    let mut docs = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(yaml_text) {
        let v = serde_yaml::Value::deserialize(doc)
            .map_err(|e| SFError::Config(format!("{origin}: invalid YAML document: {e}")))?;
        if v.is_null() {
            continue;
        }
        docs.push(v);
    }
    Ok(docs)
}

/// 拆分多文档 YAML 并过滤进支撑包：Secret 硬报错（零带外凭证红线，密钥
/// 永不进清单链路）；集群级 kind、权限面 kind（Role/RoleBinding）、治理
/// kind（ResourceQuota/LimitRange）与卷声明 kind（PersistentVolumeClaim）
/// 跳过并记日志（由安装面管理，理由见 [`RBAC_KINDS`]、[`GOVERNANCE_KINDS`]
/// 与 [`STORAGE_CLAIM_KINDS`]）；空文档（`---` 分隔产生）跳过。
fn namespace_docs(yaml_text: &str, origin: &str) -> SFResult<Vec<serde_yaml::Value>> {
    let mut docs = Vec::new();
    for v in split_docs(yaml_text, origin)? {
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        match classify_doc(kind) {
            DocFate::Deliver => docs.push(v),
            DocFate::ForbiddenSecret => {
                return Err(SFError::Config(format!(
                    "{origin}: Secret in manifest bundle is forbidden; secrets never travel through manifests"
                )));
            }
            DocFate::ClusterScoped => {
                info!(origin = %origin, kind = %kind, "manifest bundle: skipping cluster-scoped kind")
            }
            DocFate::Rbac => {
                warn!(origin = %origin, kind = %kind, "manifest bundle: skipping RBAC kind; permission changes must be applied out-of-band")
            }
            DocFate::Governance => {
                warn!(origin = %origin, kind = %kind, "manifest bundle: skipping resource governance kind; the ceiling is the operator's and is applied at install time, not by the loop")
            }
            DocFate::StorageClaim => {
                warn!(origin = %origin, kind = %kind, "manifest bundle: skipping volume claim; a bound claim's spec is immutable and is created at install time, not by the loop")
            }
        }
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
/// 随目标下发；与支撑包同一套红线——Secret 硬报错、集群级 kind、RBAC
/// kind、治理 kind 与卷声明 kind 跳过。单文档 `from_str` 会在多文档文件上报错并卡死整条
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
        if GOVERNANCE_KINDS.contains(&kind) {
            warn!(origin = %origin, kind = %kind, "manifest bundle: skipping resource governance kind; the ceiling is the operator's and is applied at install time, not by the loop");
            continue;
        }
        if STORAGE_CLAIM_KINDS.contains(&kind) {
            warn!(origin = %origin, kind = %kind, "manifest bundle: skipping volume claim; a bound claim's spec is immutable and is created at install time, not by the loop");
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

/// 一次滚动的发布资源集：有序的文件名 + 文件内容。
///
/// 两种目录形态都归到这里，消费侧因此不必知道清单来自哪一种。发布集目录用
/// `kustomization.yaml` 的 `resources` 列表；预渲染目录是平铺清单，文件名前缀
/// 是渲染序号，字典序即渲染顺序。差别只在这一层——文件名叫什么由产出侧决定，
/// 所以消费侧不许拿一份手写的「目标 → 文件名」映射去对：渲染器改一次命名，
/// 手写映射就静默失效。
pub struct ReleaseSet {
    resources: Vec<String>,
    files: BTreeMap<String, String>,
    shape: ReleaseSetShape,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReleaseSetShape {
    /// 发布集目录：`kustomization.yaml` 的 `resources` 是权威顺序。
    Kustomization,
    /// 预渲染目录：平铺清单，目录里的每一项都在发布面内。
    FlatDir,
}

impl ReleaseSetShape {
    /// 读数用的形状名（日志里说清这一轮消费的是哪种目录）。
    pub fn as_str(self) -> &'static str {
        match self {
            ReleaseSetShape::Kustomization => "kustomization",
            ReleaseSetShape::FlatDir => "flat",
        }
    }
}

impl ReleaseSet {
    /// 发布集目录形态。`resources` 来自 `kustomization.yaml`；重复条目在这里
    /// 拦下——重复会让后一个目标的清单被前一个吃掉（详见本文件同名测试）。
    pub fn from_kustomization(
        files: BTreeMap<String, String>,
        resources: Vec<String>,
    ) -> SFResult<Self> {
        if let Some(dup) = duplicate_resources(&resources) {
            return Err(SFError::Config(format!(
                "duplicate resource {dup} in kustomization resources"
            )));
        }
        Ok(Self {
            resources,
            files,
            shape: ReleaseSetShape::Kustomization,
        })
    }

    /// 预渲染目录形态：目录里的清单文件全部入发布面，按文件名排序（前缀是
    /// 渲染序号，字典序即渲染顺序）。非清单文件（渲染脚本与说明文档可能同放
    /// 一个目录）不在发布面内。
    pub fn from_flat_dir(files: BTreeMap<String, String>) -> Self {
        let resources = files
            .keys()
            .filter(|n| is_manifest_file(n))
            .cloned()
            .collect();
        Self {
            resources,
            files,
            shape: ReleaseSetShape::FlatDir,
        }
    }

    pub fn shape(&self) -> &'static str {
        self.shape.as_str()
    }

    pub fn len(&self) -> usize {
        self.resources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// 发布集列出但包里没有的文件。发布集目录形态的 `resources` 是人写的，
    /// 缺文件必须当场报出来，不能静默少发一份。
    fn content(&self, res: &str) -> SFResult<&str> {
        self.files.get(res).map(String::as_str).ok_or_else(|| {
            SFError::Config(format!(
                "{res} is listed in the release set but missing from bundle files"
            ))
        })
    }
}

/// 发布面内的文件名。预渲染目录里只有清单，但同一个目录也是人读的地方，
/// 拿后缀划界比拿一份白名单稳。
fn is_manifest_file(name: &str) -> bool {
    name.ends_with(".yaml") || name.ends_with(".yml")
}

/// 从发布资源集组装清单包。目标 deployment 的交付清单必须在集合里，缺文件 /
/// 名字对不上都是硬错误——静默回落 set image 会掩盖发布集漂移，让拓扑滞后
/// 悄悄回来。targets 输出保持调用方给的滚动顺序（与集合里的文件顺序无关）。
pub fn build_rollout_bundle(
    set: &ReleaseSet,
    targets: &[RolloutTargetConfig],
    image: &str,
) -> SFResult<RolloutBundle> {
    let mut claims: Vec<(&RolloutTargetConfig, String)> = Vec::new();
    for t in targets {
        let Some(res) = resolve_target_manifest(set, t)? else {
            // 未声明清单的目标由滚动侧回落 set image（版本偏差兼容路径）。
            continue;
        };
        if let Some((other, _)) = claims.iter().find(|(_, r)| *r == res) {
            return Err(SFError::Config(format!(
                "rollout targets {} and {} are both delivered by the manifest {res}; \
                 one file cannot carry two rollouts",
                other.deployment, t.deployment
            )));
        }
        claims.push((t, res));
    }
    let mut patched: BTreeMap<String, String> = BTreeMap::new();
    let mut support_docs: Vec<serde_yaml::Value> = Vec::new();
    for res in &set.resources {
        let content = set.content(res)?;
        reject_dialect_dependent_scalars(content, res)?;
        match claims.iter().find(|(_, r)| r == res) {
            Some((t, _)) => {
                let yaml =
                    patch_deployment_image(content, res, &t.deployment, &t.container, image)?;
                patched.insert(res.clone(), yaml);
            }
            None => support_docs.extend(namespace_docs(content, res)?),
        }
    }
    let mut target_manifests = Vec::new();
    for (t, res) in &claims {
        let yaml = patched.remove(res).ok_or_else(|| {
            SFError::Config(format!(
                "manifest {res} of target {} was not patched",
                t.deployment
            ))
        })?;
        target_manifests.push(TargetManifest {
            deployment: t.deployment.clone(),
            key: target_manifest_key(&t.deployment),
            yaml,
        });
    }
    let support_yaml = render_docs(&support_docs)?;
    Ok(RolloutBundle {
        support_yaml,
        targets: target_manifests,
    })
}

/// 目标 deployment 由发布集里的哪份清单交付；`None` 表示该目标没声明清单。
///
/// 先按声明名精确匹配——`manifest` 是部署面的声明，确定性判据排在推导之前。
/// 声明名不在集合里时，按清单自己的身份（`kind: Deployment` 加
/// `metadata.name`）反查：预渲染目录的文件名由渲染器生成
/// （`41-deployment-cogneva.yaml`），而滚动目标的身份是集群里的对象名，
/// 后者才是两侧共用的那一半。反查命中多于一份同样是硬错误——那意味着这份
/// 发布集里有两个同名的 Deployment，滚谁都是错的。
fn resolve_target_manifest(
    set: &ReleaseSet,
    target: &RolloutTargetConfig,
) -> SFResult<Option<String>> {
    let Some(declared) = target.manifest.as_deref() else {
        return Ok(None);
    };
    if set.resources.iter().any(|r| r == declared) {
        return Ok(Some(declared.to_string()));
    }
    let mut delivers: Vec<String> = Vec::new();
    for res in &set.resources {
        if carries_deployment(set.content(res)?, res, &target.deployment)? {
            delivers.push(res.clone());
        }
    }
    match delivers.len() {
        1 => Ok(Some(delivers.remove(0))),
        0 => Err(SFError::Config(format!(
            "no manifest in the release set delivers Deployment {}: target declared \
             {declared}, and none of the {} resources is a Deployment by that name",
            target.deployment,
            set.resources.len()
        ))),
        n => Err(SFError::Config(format!(
            "{n} manifests in the release set deliver Deployment {} ({delivers:?}); \
             a rollout target must be delivered by exactly one",
            target.deployment
        ))),
    }
}

/// 这份清单里是否有一个名为 `deployment` 的 Deployment 文档。多文档文件按
/// 文档逐个判：预渲染目录里一个文件一份清单，但发布集目录形态没有这个约束。
fn carries_deployment(yaml_text: &str, origin: &str, deployment: &str) -> SFResult<bool> {
    Ok(split_docs(yaml_text, origin)?.iter().any(|v| {
        v.get("kind").and_then(|k| k.as_str()) == Some("Deployment")
            && v.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                == Some(deployment)
    }))
}

/// 一组文档 → 多文档 YAML（`---` 分隔）。组包与消费侧复核共用同一份序列化。
fn render_docs(docs: &[serde_yaml::Value]) -> SFResult<String> {
    let mut out = String::new();
    for d in docs {
        out.push_str("---\n");
        out.push_str(
            &serde_yaml::to_string(d)
                .map_err(|e| SFError::Config(format!("serialize support doc: {e}")))?,
        );
    }
    Ok(out)
}

/// 下发包在**消费侧**的复核：把包里不属于滚动面的文档（安装面 kind，判定见
/// [`namespace_docs`]）摘掉后写到 `out`，返回可 apply 的路径；全被摘掉时返回
/// None（没有属于滚动面的对象可 apply）。支撑包与目标清单都走这里——它们同源。
///
/// 为什么消费侧要自己复核一遍：两个包都由**部署器**组装，而部署器跑的是当前
/// 已部署的 rev，消费它的 Job 跑的是**本次目标 rev**——两者通常不是同一个
/// 二进制，部署器永远落后于它要上的版本。所以"包里已经清干净了"这个前提，
/// 只在组包者与消费者同 rev 时成立。留下一个安装面对象（例如一份与既有绑定
/// 不一致的卷声明）会让 apply 被准入拒绝，而这一步在"一个镜像都没动"的
/// 中止点上，拒绝会被读成对本次版本的否定——整条落地通道因此停摆。
fn stage_rollout_manifest(text: &str, origin: &str, out: &Path) -> SFResult<Option<PathBuf>> {
    let docs = namespace_docs(text, origin)?;
    if docs.is_empty() {
        return Ok(None);
    }
    std::fs::write(out, render_docs(&docs)?)
        .map_err(|e| SFError::IO(format!("write {}: {e}", out.display())))?;
    Ok(Some(out.to_path_buf()))
}

/// A plain scalar whose meaning depends on which YAML dialect reads it: the
/// manifest is parsed here by a 1.2-style reader and written back out, and the
/// cluster parses that re-emission with a 1.1-style one. The two disagree on
/// the *type* of a small set of bare spellings — `0400` is an octal integer to
/// the cluster and a string to us, `yes`/`on`/`no` are booleans to the cluster
/// and strings to us. Re-emitting "our" reading pins the disagreement: a
/// quoted `'0400'` reaches the API server as a string where it wants an int32,
/// and the rollout dies at apply with the error pointing at our staged copy
/// rather than at the manifest anyone wrote.
///
/// The judgement is made on the source text, not on the parsed tree: in the
/// tree a string `0400` and a deliberately quoted `'0400'` are the same value,
/// and only the spelling tells them apart.
///
/// Returns how each side reads the spelling — (here, there) — or `None` when
/// both readers agree, which is the case for everything ordinary.
fn dialect_readings(tok: &str) -> Option<(&'static str, String)> {
    /// 1.1's boolean words. `true`/`false` are not here: both dialects type
    /// those as booleans, so they read the same either way.
    const BOOLS: [&str; 16] = [
        "y", "Y", "yes", "Yes", "YES", "n", "N", "no", "No", "NO", "on", "On", "ON", "off", "Off",
        "OFF",
    ];
    if BOOLS.contains(&tok) {
        return Some(("string", "boolean".to_string()));
    }
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    // A leading zero is an octal integer to the cluster, a string to us.
    if tok.len() > 1 && digits(tok) && tok.starts_with('0') {
        return Some(("string", format!("octal integer {tok}")));
    }
    // `0o400` is 1.2-only: an integer to us, a string to the cluster.
    if let Some(rest) = tok.strip_prefix("0o").or_else(|| tok.strip_prefix("0O")) {
        if digits(rest) {
            return Some(("integer", "string".to_string()));
        }
    }
    // `1_000`: the cluster reads the underscores as digit separators, we read
    // them as ordinary characters.
    if tok.contains('_') && tok.bytes().all(|b| b.is_ascii_digit() || b == b'_') {
        return Some(("string", format!("integer {}", tok.replace('_', ""))));
    }
    // `1:30` is ninety to the cluster (sexagesimal) and a string to us.
    let parts: Vec<&str> = tok.split(':').collect();
    if parts.len() > 1 && parts.iter().all(|p| digits(p)) {
        return Some(("string", "sexagesimal integer".to_string()));
    }
    None
}

/// The value text a manifest line carries, before any judgement about whether
/// it is a bare scalar. `None` when the line carries no value at all: blank,
/// a comment, or a key that opens a nested mapping.
fn value_of(line: &str) -> Option<&str> {
    let mut rest = line.trim();
    while let Some(after) = rest.strip_prefix("- ") {
        rest = after.trim_start();
    }
    if rest.is_empty() || rest.starts_with('#') {
        return None;
    }
    let value = match rest.find(": ") {
        Some(i) => rest[i + 2..].trim_start(),
        // A trailing colon opens a nested mapping; there is no value here.
        None if rest.ends_with(':') => return None,
        None => rest,
    };
    if value.is_empty() {
        return None;
    }
    Some(value)
}

/// The bare scalar a manifest line carries, if it carries one. Multi-word
/// values never qualify: a space makes the value a string to both dialects.
fn plain_scalar_of(line: &str) -> Option<&str> {
    let value = value_of(line)?;
    // Quoted, block, flow, anchored or tagged: not a bare scalar, so both
    // dialects read it as written.
    if value.starts_with(['"', '\'', '|', '>', '[', '{', '&', '*', '!', '%', '@', '`']) {
        return None;
    }
    let value = match value.find(" #") {
        Some(i) => value[..i].trim_end(),
        None => value,
    };
    if value.is_empty() || value.contains(char::is_whitespace) {
        return None;
    }
    Some(value)
}

/// Whether the line opens a block scalar (`|` or `>` with its indicators):
/// every following line indented deeper than this one is that one string.
fn opens_block_scalar(line: &str) -> bool {
    value_of(line).is_some_and(|v| v.starts_with('|') || v.starts_with('>'))
}

/// Refuse a manifest bundle that carries a bare scalar the two YAML dialects
/// type differently, and say which spellings both readers agree on.
///
/// Checked over every document in the bundle rather than only the ones this
/// rollout applies: the rewrite is per file with one reader, so a single
/// ambiguous spelling anywhere in it makes that rewrite untrustworthy.
///
/// Called on the source text the bundle builder reads out of the revision, not
/// on the manifest staging later hands to `kubectl`: by then the text has been
/// through this process's own reader and writer, and a source `0400` and a
/// source `'0400'` both arrive there as the same quoted string. The source
/// checkout is the last place the two spellings are still distinguishable.
fn reject_dialect_dependent_scalars(text: &str, origin: &str) -> SFResult<()> {
    // Lines inside a block scalar are one string to both readers, so nothing in
    // them is a scalar of the document. They are skipped by indentation: a
    // block runs until a line that indents no deeper than the line opening it.
    // Refusing them would cost a rollout for text the cluster never retypes —
    // and prompts and embedded config live in exactly such blocks.
    let mut in_block: Option<usize> = None;
    for (idx, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        if let Some(opened_at) = in_block {
            if indent > opened_at {
                continue;
            }
            in_block = None;
        }
        if opens_block_scalar(line) {
            in_block = Some(indent);
            continue;
        }
        let Some(tok) = plain_scalar_of(line) else {
            continue;
        };
        let Some((here, there)) = dialect_readings(tok) else {
            continue;
        };
        // A leading-zero spelling has a decimal twin worth naming: it is the
        // spelling the cluster and we agree on, and the one the value usually
        // means (an octal file mode).
        let respell = u32::from_str_radix(tok, 8)
            .ok()
            .filter(|_| tok.starts_with('0') && tok.len() > 1)
            .map(|v| format!(" — `{v}` is the same value and needs no quotes"))
            .unwrap_or_default();
        return Err(SFError::Config(format!(
            "{origin}:{}: the bare value `{tok}` is typed differently by the two YAML \
             dialects this manifest passes through: it is a {here} to this process and a \
             {there} to the cluster, so the staged copy would carry the other type. Write \
             it in a spelling both readers agree on: quote it if it means a string{respell}",
            idx + 1
        )));
    }
    Ok(())
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

/// 支撑清单里可能被 apply 动到的工作负载（后端与集群内 registry）。apply 只改
/// 自己那份 spec：没改动的工作负载原地不动，改动的会滚动重启几秒到几十秒。
#[derive(Debug, Clone, PartialEq, Eq)]
struct SupportWorkload {
    /// kubectl 子命令用的单数资源名（deploy / statefulset）。
    kind: &'static str,
    name: String,
    generation: i64,
}

impl SupportWorkload {
    fn display(&self) -> String {
        format!("{}/{}", self.kind, self.name)
    }
}

/// 这次 apply 真的动过哪些支撑工作负载：代数变了，或 apply 之前根本不在（新增
/// 的工作负载一样要等到就绪）。目标部署不在此列——它们的滚动由逐目标等待负责，
/// 判据与预算都是另一套，在这里再等一遍会让同一件事有两个判据。
fn changed_workloads(
    before: &[SupportWorkload],
    after: &[SupportWorkload],
    targets: &[RolloutTarget],
) -> Vec<SupportWorkload> {
    after
        .iter()
        .filter(|w| {
            !targets
                .iter()
                .any(|t| w.kind == "deploy" && t.deployment == w.name)
        })
        .filter(
            |w| match before.iter().find(|b| b.kind == w.kind && b.name == w.name) {
                Some(b) => b.generation != w.generation,
                None => true,
            },
        )
        .cloned()
        .collect()
}

/// 支撑工作负载是否已经回到"这次滚动可以往下走"：控制器已经处理过这一代 spec
/// （observedGeneration 追平 generation），且就绪副本数达到 spec 要的数目。读法
/// 解析不出来就返回 None（没读到答案，不是"就绪"）。spec.replicas 缺省时按 1
/// 算：Deployment/StatefulSet 的缺省都是 1，读成 0 会把一个单副本工作负载判成
/// "不需要就绪"而直接放行。
fn support_settled(readout: &str) -> Option<bool> {
    let mut parts = readout.split('|');
    let generation: i64 = parts.next()?.trim().parse().ok()?;
    let observed: i64 = parts.next()?.trim().parse().unwrap_or(0);
    let want: i32 = parts
        .next()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1);
    let ready: i32 = parts
        .next()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    Some(observed >= generation && ready >= want)
}

/// 一个工作负载读了哪些 ConfigMap。消费形态取自 Pod 模板本身（卷、projected
/// 卷里的 ConfigMap 源、`envFrom`、`env.valueFrom`，容器与 initContainer 都算），
/// 不写名单：新加一个消费点不需要谁记得来这里补一行。
///
/// 与 `mounters_of_config`（拉取端从清单里读挂载关系）的差别在**读哪一面**：
/// 交付端问的是"我这次要 apply 的清单里谁挂了它"，部署器问的是"集群上现在谁在
/// 读它"——一个是随版本走的声明，一个是实际生效面，部署器要判的正是后者。
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigConsumer {
    /// kubectl 子命令用的单数资源名（deploy / statefulset）。
    kind: String,
    name: String,
    configmaps: Vec<String>,
}

impl ConfigConsumer {
    fn display(&self) -> String {
        format!("{}/{}", self.kind, self.name)
    }
}

/// 把消费面读数（每行一个工作负载，竖线分段、段内逗号分隔）解析成消费者。
///
/// 段序：name|卷里的 ConfigMap|projected 里的 ConfigMap|envFrom|env.valueFrom|
/// initContainer 的 envFrom|initContainer 的 env.valueFrom。空段与空项都要能读：
/// 没有卷的工作负载整段是空的，而不是缺一列——位置由竖线固定，缺字段（omitempty）
/// 不能把后面的段顶掉。
fn config_consumers(readout: &str, kind: &str) -> Vec<ConfigConsumer> {
    let mut out = Vec::new();
    for line in readout.lines() {
        let fields: Vec<&str> = line.split('|').collect();
        let Some(name) = fields.first().map(|s| s.trim()).filter(|s| !s.is_empty()) else {
            continue;
        };
        let mut configmaps: Vec<String> = fields[1..]
            .iter()
            .flat_map(|field| field.split(','))
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect();
        configmaps.sort();
        configmaps.dedup();
        out.push(ConfigConsumer {
            kind: kind.to_string(),
            name: name.to_string(),
            configmaps,
        });
    }
    out
}

/// 这次 apply 之后内容变了的 ConfigMap 名字（新增的也算：上次不在、这次在）。
fn changed_configmaps(
    before: &BTreeMap<String, serde_json::Value>,
    after: &BTreeMap<String, serde_json::Value>,
) -> Vec<String> {
    after
        .iter()
        .filter(|(name, data)| before.get(*name) != Some(*data))
        .map(|(name, _)| name.clone())
        .collect()
}

/// 这份 ConfigMap 的这次内容变化，要不要滚动它的消费者。
///
/// 带分段表的配置文档按表判：只有"启动时才生效"的段变了才值得滚一次，热更新面
/// 覆盖到的段交付即生效，滚了是白重启（还会把一次纯配置改动记成一次服务中断）。
/// 没有分段表的 ConfigMap 只能按内容判——它的读法不在这张表里，说不清，就按安全
/// 侧来：内容变了就滚。
fn config_change_needs_restart(
    name: &str,
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
) -> bool {
    if name != cog_core::config_sections::CONFIG_CONFIGMAP {
        return true;
    }
    let document = |data: Option<&serde_json::Value>| -> Option<serde_json::Value> {
        data.and_then(|d| d.get(cog_core::config_sections::CONFIG_DOCUMENT_KEY))
            .and_then(|text| text.as_str())
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
            .filter(|doc| doc.is_object())
    };
    match (document(before), document(after)) {
        (Some(before), Some(after)) => {
            !cog_core::config_sections::sections_needing_restart_on_change(&before, &after)
                .is_empty()
        }
        // 有一侧读不成文档：这次改了哪几段说不清，按安全侧算——多滚一次，而不是
        // 押"改的都是热段"。
        _ => true,
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
    ///
    /// 清单在 apply 前过一次消费侧复核（同 [`stage_rollout_manifest`]）：组包者跑
    /// 的是当前已部署的 rev，执行这份清单的却是本次目标 rev，两者通常不同——所以
    /// "包里已经清干净了"不能当前提。留下一个安装面对象会让 apply 被准入拒绝，而
    /// 这次拒绝会被读成对目标版本的否定，整条落地通道停在一个没看清集群的中止点上。
    async fn apply_target(&self, plan: &RolloutPlan, t: &RolloutTarget) -> SFResult<()> {
        if let Some(dir) = &plan.manifests_dir {
            let key = target_manifest_key(&t.deployment);
            let path = Path::new(dir).join(&key);
            if path.is_file() {
                let text = tokio::fs::read_to_string(&path)
                    .await
                    .map_err(|e| SFError::IO(format!("read {}: {e}", path.display())))?;
                let staged = stage_rollout_manifest(
                    &text,
                    &key,
                    &std::env::temp_dir().join(format!("mainline-target-{}.yaml", t.deployment)),
                )?;
                if let Some(staged_path) = staged {
                    let path_arg = staged_path.to_string_lossy().to_string();
                    info!(
                        deployment = %t.deployment,
                        source = %path.display(),
                        manifest = %path_arg,
                        "mainline rollout: apply target manifest"
                    );
                    self.clear_superseded_env_values(&staged_path).await?;
                    return self
                        .run_kubectl(&["apply", "-f", &path_arg], 60)
                        .await
                        .map(|_| ());
                }
                warn!(
                    deployment = %t.deployment,
                    source = %path.display(),
                    "target manifest carries no rollout-face object; falling back to set image"
                );
            } else {
                warn!(deployment = %t.deployment, "no target manifest in bundle; falling back to set image");
            }
        }
        self.set_image(t, &plan.tag).await
    }

    /// 采一次命名空间里每份 ConfigMap 的内容（`data` 段），用来在 apply 前后比出
    /// 这次到底改了哪几份。
    ///
    /// 读不到就返回 None：调用方据此走"说不清"的那一支（照样滚），而不是把读不到
    /// 当"没变"——那正好是这条判据要防的静默失效。
    async fn configmap_contents(&self) -> Option<BTreeMap<String, serde_json::Value>> {
        let readout = match self
            .run_kubectl(&["get", "configmap", "-o", "json"], 60)
            .await
        {
            Ok(text) => text,
            Err(e) => {
                warn!(error = %e, "config effect: cannot read the ConfigMaps, treating every consumer as needing a roll");
                return None;
            }
        };
        let parsed: serde_json::Value = match serde_json::from_str(&readout) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "config effect: unreadable ConfigMap listing");
                return None;
            }
        };
        let mut out = BTreeMap::new();
        for item in parsed.get("items").and_then(|v| v.as_array())? {
            let Some(name) = item.pointer("/metadata/name").and_then(|n| n.as_str()) else {
                continue;
            };
            let data = item.get("data").cloned().unwrap_or(serde_json::Value::Null);
            out.insert(name.to_string(), data);
        }
        Some(out)
    }

    /// 集群上每个工作负载读了哪些 ConfigMap。
    ///
    /// Like the support-workload snapshot this is a read taken before anything
    /// changes, so it retries inside the wait budget instead of turning one
    /// expired attempt into a rollout that never started: the same capped and
    /// throttled kubectl child reads both, and both feed the same "no version
    /// conclusion was reached" classification.
    async fn live_config_consumers(&self) -> SFResult<Vec<ConfigConsumer>> {
        let mut out = Vec::new();
        for kind in SUPPORT_WORKLOAD_KINDS {
            let readout = self
                .probe(
                    &["get", kind, "-o", CONFIG_CONSUMER_JSONPATH],
                    30,
                    self.rollout_timeout_secs,
                )
                .await?;
            out.extend(config_consumers(&readout, kind));
        }
        Ok(out)
    }

    /// 让这次 apply 改到的配置真正生效：内容变了、而进程只在启动时读它，工作负载
    /// 的 spec 没动、代数不变，逐目标的滚动与支撑等待都不会碰它——文件是新的、进程
    /// 还是旧的、记录说已生效。这里按内容差自己算出该滚谁，把 restartedAt 打上去
    /// （Pod 模板变了才会滚出新副本），返回滚动名单。
    ///
    /// 读不到内容（None）时按"每份都变了"处理：说不清就有多滚一次，不留静默失效。
    async fn roll_config_change_consumers(
        &self,
        before: Option<&BTreeMap<String, serde_json::Value>>,
        after: Option<&BTreeMap<String, serde_json::Value>>,
    ) -> SFResult<Vec<String>> {
        let empty = BTreeMap::new();
        let before = before.unwrap_or(&empty);
        let consumers = self.live_config_consumers().await?;
        // 后一次读不到：这次 apply 到底落了什么没有第二面可比。此时能拿到的只有
        // "谁在读哪些 ConfigMap"，那就按"每一份都可能变了"算——多滚一次，而不是
        // 一次都不滚。
        let changed: Vec<String> = match after {
            Some(after) => changed_configmaps(before, after),
            None => {
                warn!("config effect: no reading after the apply, rolling every consumer");
                let mut names: Vec<String> = consumers
                    .iter()
                    .flat_map(|c| c.configmaps.iter().cloned())
                    .collect();
                names.sort();
                names.dedup();
                names
            }
        };
        let stamp = cog_core::config_sections::restart_stamp();
        let body = cog_core::config_sections::restart_patch_body(&stamp);
        let needs_restart: Vec<&String> = changed
            .iter()
            .filter(|name| {
                config_change_needs_restart(
                    name,
                    before.get(*name),
                    after.and_then(|a| a.get(*name)),
                )
            })
            .collect();

        // 配置文件有"只有重启才生效"的段变了，却没有任何活着的消费者：这份文档
        // 已经 apply 上去了，而集群里没有任何进程会读它——报成功就是把"文件是新的、
        // 没人读、记录说已生效"记成一次成功的交付。
        let reads_config = |c: &ConfigConsumer| {
            c.configmaps
                .iter()
                .any(|m| m == cog_core::config_sections::CONFIG_CONFIGMAP)
        };
        if needs_restart
            .iter()
            .any(|n| *n == cog_core::config_sections::CONFIG_CONFIGMAP)
            && !consumers.iter().any(reads_config)
        {
            return Err(SFError::Validation(format!(
                "the configuration document changed in sections that only take effect at startup, \
                 but no workload in the namespace reads {}: the ConfigMap is applied and nothing \
                 will pick it up",
                cog_core::config_sections::CONFIG_CONFIGMAP
            )));
        }

        let mut rolled: Vec<String> = Vec::new();
        for name in &needs_restart {
            for consumer in consumers
                .iter()
                .filter(|c| c.configmaps.iter().any(|m| m == *name))
            {
                if rolled.contains(&consumer.name) {
                    continue;
                }
                // 打在 apply 之后的支撑快照之前：这样它是"这次 apply 动过的工作负载"
                // 之一，非目标的那几个由既有的支撑等待一并等它回就绪。
                self.run_kubectl(
                    &[
                        "patch",
                        &consumer.kind,
                        &consumer.name,
                        "--type",
                        "merge",
                        "-p",
                        &body,
                    ],
                    60,
                )
                .await?;
                info!(
                    workload = %consumer.display(),
                    configmap = %name,
                    "config effect: rolled a workload whose configuration is only read at startup"
                );
                rolled.push(consumer.name.clone());
            }
        }
        Ok(rolled)
    }

    /// 采一次命名空间里支撑工作负载的代数，用来在 apply 之后认出这次动过谁。
    async fn support_workloads(&self) -> SFResult<Vec<SupportWorkload>> {
        let mut out = Vec::new();
        for kind in SUPPORT_WORKLOAD_KINDS {
            // The read goes through `probe` rather than straight to
            // `run_kubectl`. This is the gate before anything changes, and it
            // starts a kubectl child inside a container capped at 500m CPU:
            // when the node is busy enough, one 30s attempt can be spent
            // entirely on the child's cold start, and a single expired attempt
            // is not evidence the cluster is unreachable. Five rollout Jobs
            // died with a container lifetime of exactly 30s each and the
            // signature `environment:support-snapshot::unreachable`, at the
            // price of an hour of cooldown and a rollout that never started.
            // A read-only query can be tried again for free, so the wait
            // budget is the readiness allowance that already exists instead of
            // a second "how long am I willing to wait" knob.
            let text = self
                .probe(
                    &[
                        "get",
                        kind,
                        "-o",
                        "jsonpath={range .items[*]}{.metadata.name} {.metadata.generation}{\"\\n\"}{end}",
                    ],
                    30,
                    self.rollout_timeout_secs,
                )
                .await?;
            for line in text.lines() {
                let mut parts = line.split_whitespace();
                let (Some(name), Some(generation)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let Ok(generation) = generation.parse::<i64>() else {
                    continue;
                };
                out.push(SupportWorkload {
                    kind,
                    name: name.to_string(),
                    generation,
                });
            }
        }
        Ok(out)
    }

    /// 等这次 apply 动过的支撑工作负载回到就绪。等不到（或读不到集群）都把错误
    /// 交回调用方按落点分类：读不到集群是环境类，能读却一直起不来是这份发布集的
    /// 事（支撑清单也在本次下发的内容里），此时一个镜像都还没动，谈不上回滚。
    async fn wait_support_settled(&self, workloads: &[SupportWorkload]) -> SFResult<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(self.startup_timeout_secs);
        let mut pending: Vec<&SupportWorkload> = workloads.iter().collect();
        let mut last = String::new();
        while !pending.is_empty() {
            let mut still: Vec<&SupportWorkload> = Vec::new();
            for w in pending {
                let readout = self
                    .run_kubectl(&["get", w.kind, &w.name, "-o", SUPPORT_SETTLE_JSONPATH], 30)
                    .await?;
                if support_settled(&readout) == Some(true) {
                    continue;
                }
                last = format!("{} generation|observed|want|ready={readout}", w.display());
                still.push(w);
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(SFError::Agent(format!(
                    "support workload this rollout restarted did not become ready within {}s: {last}",
                    self.startup_timeout_secs
                )));
            }
            tokio::time::sleep(Duration::from_secs(ROLLOUT_POLL_SECS)).await;
        }
        Ok(())
    }

    /// 清单交付前，先把集群对象上"已被清单的 `valueFrom` 取代"的 env `value` 摘掉。
    ///
    /// 三方合并按 env 条目的 name 合并，清单里消失的字段不会被删掉：该条目于是
    /// 同时带 `value` 与 `valueFrom`、被准入拒绝，本次 apply 停在这一处，而报错
    /// 指向清单——清单是对的。这个对象从此永远 apply 不进去，直到有人手动摘掉
    /// 那个字段；残留物往往正是我们要从清单里拿掉的明文凭证。
    ///
    /// 判据与 patch 序列都在 `cog_core`（纯函数），这里只负责取现状与执行。读不到
    /// 现状（对象还不存在、查询失败）就什么都不做：首次交付本就没有残留，集群不可
    /// 达时紧随其后的 apply 会报出真实错误，不在这里替它下结论。
    async fn clear_superseded_env_values(&self, manifest: &Path) -> SFResult<()> {
        let text = tokio::fs::read_to_string(manifest)
            .await
            .map_err(|e| SFError::IO(format!("read {}: {e}", manifest.display())))?;
        for doc in serde_yaml::Deserializer::from_str(&text) {
            let Ok(doc) = serde_yaml::Value::deserialize(doc) else {
                continue;
            };
            let Ok(desired) = serde_json::to_value(&doc) else {
                continue;
            };
            let Some(workload) = cog_core::contract::env_supersede::workload_identity(&desired)
            else {
                continue;
            };
            let live = match self
                .run_kubectl(
                    &["get", &workload.kind_arg(), &workload.name, "-o", "json"],
                    30,
                )
                .await
            {
                Ok(out) => serde_json::from_str(&out).unwrap_or(serde_json::Value::Null),
                Err(e) => {
                    debug!(
                        workload = %format!("{}/{}", workload.kind, workload.name),
                        error = %e,
                        "cannot read the live object; skipping the superseded-env check"
                    );
                    continue;
                }
            };
            let removals =
                cog_core::contract::env_supersede::superseded_env_values(&desired, &live);
            if removals.is_empty() {
                continue;
            }
            let ops = cog_core::contract::env_supersede::removal_patch_ops(&removals);
            let names: Vec<&str> = removals.iter().map(|r| r.name.as_str()).collect();
            info!(
                workload = %format!("{}/{}", workload.kind, workload.name),
                envs = %names.join(", "),
                "cleared env values the manifest now injects from a source"
            );
            self.run_kubectl(
                &[
                    "patch",
                    &workload.kind_arg(),
                    &workload.name,
                    "--type=json",
                    "-p",
                    &serde_json::Value::Array(ops).to_string(),
                ],
                60,
            )
            .await?;
        }
        Ok(())
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
        // The attempt count and the elapsed time go into the error because the
        // reading has to outlive the process that produced it: one attempt that
        // never got through is a stall (children starting cold, a throttled
        // cgroup), while several spread over the whole budget is a cluster that
        // is down for real. Both would otherwise be recorded as the same
        // "unreachable", and whoever reads the ledger later cannot ask the Job
        // anything — it is gone by then.
        let started = std::time::Instant::now();
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            match self.run_kubectl(args, timeout_secs).await {
                Ok(out) => return Ok(out),
                Err(e) if is_cluster_unreachable(&e.to_string()) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(SFError::IO(format!(
                            "{CLUSTER_UNREACHABLE_MARKER}: {e} \
                             (attempts={attempts} over {}s)",
                            started.elapsed().as_secs()
                        )));
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
    /// 拉取失败的等待态不在此列——它等不到 ready 但会自愈，且没有观测到版本，
    /// 预算到期时按环境类处置（见 IMAGE_PULL_WAITING_REASONS）。
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
                let replicasets = self.sample_replicaset_failures(t).await;
                let diagnosis = self.rollout_diagnosis(t, &replicasets).await;
                let suffix = if diagnosis.is_empty() {
                    String::new()
                } else {
                    format!("; pods: {diagnosis}")
                };
                // 排不上队与准入被拒：都只有"这次上线没动放置面"才归环境。
                let blocked = self.sample_unschedulable_pods(t).await;
                let now_shape = self.current_placement_shape(t).await.ok();
                let environment = environment_class(&blocked, prev_shape, now_shape.as_deref());
                let denied = admission_rejection(&replicasets, prev_shape, now_shape.as_deref());
                // 卡在拉镜像上：整段预算里新版本一次都没跑起来，这次失败没有观测到
                // 版本，说不出它的好坏。先取下来（临时值不能活到下面的格式化里），
                // 命不命中都在这里判，判词里带上是哪一个等待态。
                let pull_blocked =
                    image_pull_blocked(&self.waiting_reasons(t).await).map(|r| r.to_string());
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
                } else if let Some(detail) = denied {
                    SFError::IO(format!(
                        "{ADMISSION_DENIED_MARKER}: rollout of deployment/{} did not complete \
                         within {}s ({phase} phase) — the API server rejected its pod(s) before \
                         any of them were created ({detail}) and this revision did not change the \
                         deployment's placement shape, so the revision is not what failed \
                         (last: {note}{suffix})",
                        t.deployment, budget
                    ))
                } else if let Some(reason) = pull_blocked {
                    // 镜像源的问题不是版本的问题：判词只说"没拿到证据"，不回滚。
                    SFError::IO(format!(
                        "{IMAGE_SOURCE_UNAVAILABLE_MARKER}: rollout of deployment/{} did not \
                         complete within {}s ({phase} phase) — its pod(s) are still waiting on the \
                         image (waiting={reason}) and the new revision has not run once, so this \
                         failure says nothing about it (last: {note}{suffix})",
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

    /// 本次滚动的 Pod 上所有等待原因，init 容器与主容器一并取：init 容器拉不到
    /// 镜像时主容器只报 PodInitializing，只看主容器会把这一档整个漏掉。查询失败
    /// 返回空表——滚动交替期 containerStatuses 本就可能缺失，由调用方的超时兜底。
    async fn waiting_reasons(&self, t: &RolloutTarget) -> Vec<String> {
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
            Err(_) => return Vec::new(),
        };
        out.lines()
            .map(|l| l.trim().to_string())
            .filter(|r| !r.is_empty())
            .collect()
    }

    /// 只查致命等待态（配置错误、CrashLoop、镜像引用非法），查询临时失败返回
    /// Ok——由调用方的超时与后续 pods_healthy 兜底，这里只负责让"必死"的滚动
    /// 快速失败。拉取失败不在这里判（见 IMAGE_PULL_WAITING_REASONS）。
    async fn fatal_pod_state(&self, t: &RolloutTarget) -> SFResult<()> {
        for reason in self.waiting_reasons(t).await {
            if FATAL_WAITING_REASONS.contains(&reason.as_str()) {
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
    /// 准入被拒的副本集由调用方采样后传进来：那一份同时是归类用的机器判据
    /// （[`admission_rejection`]），两次判定不能各查一遍——查两遍就有两种答案。
    ///
    /// 采样是尽力而为：任何一步取不到都留空，观测失败不变成第二个错误。
    async fn rollout_diagnosis(
        &self,
        t: &RolloutTarget,
        replicasets: &[ReplicaSetFailure],
    ) -> String {
        let samples = self.sample_rollout_pods(t).await;
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
        let replicasets = self.sample_replicaset_failures(t).await;
        let diagnosis = self.rollout_diagnosis(t, &replicasets).await;
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
                // 挂载点是只读 ConfigMap，复核后的包落到可写目录再 apply。
                let text = tokio::fs::read_to_string(&support)
                    .await
                    .map_err(|e| SFError::IO(format!("read {}: {e}", support.display())))
                    .map_err(|e| classify_before_any_change("support", "", e))?;
                let staged = stage_rollout_manifest(
                    &text,
                    "support.yaml",
                    &std::env::temp_dir().join("mainline-support.yaml"),
                )
                .map_err(|e| classify_before_any_change("support", "", e))?;
                match staged {
                    Some(path) => {
                        let support_arg = path.to_string_lossy().to_string();
                        // apply 之前先记下支撑工作负载的代数：apply 之后只有代数
                        // 变了的那些是真被这次滚动搅动的，其余原地没动。少了这份
                        // 快照就只能"等所有支撑工作负载"，那会把一个与本次上线
                        // 无关、恰好没起来的后端也算到这次滚动头上。
                        let before = self
                            .support_workloads()
                            .await
                            .map_err(|e| classify_before_any_change("support-snapshot", "", e))?;
                        // 配置文件的现状也要在 apply 之前取：进程读的是 apply 前那份，
                        // 只有拿它当对照才说得清这次改了什么。取不到就走"说不清→都滚"。
                        let configs_before = self.configmap_contents().await;
                        info!(
                            source = %support.display(),
                            manifest = %support_arg,
                            "mainline rollout: applying support manifests"
                        );
                        self.clear_superseded_env_values(&path)
                            .await
                            .map_err(|e| classify_before_any_change("support", "", e))?;
                        self.run_kubectl(&["apply", "-f", &support_arg], 120)
                            .await
                            .map_err(|e| classify_before_any_change("support", "", e))?;
                        let configs_after = self.configmap_contents().await;
                        // 支撑清单里除了 ConfigMap/Service，还有后端与集群内
                        // registry 这些工作负载，而目标部署的镜像要从这个 registry
                        // 拉、启动要连这些后端。apply 只改动的那些会滚动重启几秒到
                        // 几十秒；不等它们回到就绪就滚目标，我们自己制造的这段空窗
                        // 会以 ErrImagePull（registry 正好在重启）或连不上后端的
                        // 身份落到目标 Pod 上，被读成"新版本坏了"并回滚一个完好的
                        // 版本——线上实测过一次：一次 support apply 带上后端探针
                        // 变更，四个后端与 registry 一起重启，目标在 4 秒后被判
                        // ErrImagePull 版本类失败并回滚。
                        //
                        // 配置改动走不了"按代数认人"这条路：apply 一份 ConfigMap 不改
                        // 任何工作负载的 spec，代数不动，逐目标滚动与支撑等待都不会碰
                        // 它的消费者——只改配置的 rev（镜像 tag 与上一版相同时连目标都
                        // 不会滚）就这么静默停用。所以这里按内容差自己判一份该滚的名单，
                        // 滚在拍代数快照之前，让下面的等待一并等它。
                        let config_rolled = self
                            .roll_config_change_consumers(
                                configs_before.as_ref(),
                                configs_after.as_ref(),
                            )
                            .await
                            .map_err(|e| classify_before_any_change("config-effect", "", e))?;
                        if !config_rolled.is_empty() {
                            info!(
                                workloads = %config_rolled.join(","),
                                "mainline rollout: rolled the workloads whose configuration is only read at startup"
                            );
                        }
                        let after = self
                            .support_workloads()
                            .await
                            .map_err(|e| classify_before_any_change("support-snapshot", "", e))?;
                        let restarted = changed_workloads(&before, &after, &plan.targets);
                        if !restarted.is_empty() {
                            info!(
                                workloads = %restarted
                                    .iter()
                                    .map(SupportWorkload::display)
                                    .collect::<Vec<_>>()
                                    .join(","),
                                "mainline rollout: waiting for support workloads this apply restarted"
                            );
                            self.wait_support_settled(&restarted)
                                .await
                                .map_err(|e| classify_before_any_change("support-settle", "", e))?;
                        }
                    }
                    None => warn!(
                        source = %support.display(),
                        "mainline rollout: support bundle carries no rollout-face object; nothing to apply"
                    ),
                }
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
                .map_err(|e| classify_before_any_change("snapshot", &t.deployment, e))?;
            // 放置面与镜像一起快照：apply 之后才分得清"排不上队"是这次上线
            // 自己加了排不上的约束（版本的事），还是节点本来就满（不是）。
            let shape = self
                .current_placement_shape(t)
                .await
                .map_err(|e| classify_before_any_change("snapshot", &t.deployment, e))?;
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
                return self
                    .fail_without_blind_rollback("apply", &target.deployment, e, &done, &prevs)
                    .await;
            }
            if let Err(e) = self.wait_rollout_complete(target, prev_shape).await {
                done.push(target);
                return self
                    .fail_without_blind_rollback("wait", &target.deployment, e, &done, &prevs)
                    .await;
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
                return self
                    .fail_without_blind_rollback("soak", &target.deployment, e, &done, &prevs)
                    .await;
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
    /// - 准入面把新副本挡在建 Pod 之前（配额打满、LimitRange/ PodSecurity 越界）：
    ///   新版本的容器一次都没起来，读到的只是集群此刻装不下，同样要等集群容量或
    ///   策略面变化，不是这份变更的结论。
    /// - 观测工具本身起不来（kubectl 缺失/没有可执行位/被占用）：我们连一次查询
    ///   都没发出去，说不出新版本的好坏；而且回滚只退镜像，修不好一个坏掉的工具
    ///   路径，只会把新版本换掉而又重复失败一轮。
    ///
    /// 四类都让 Job 非零退出（部署器下轮重试），集群保持在刚推上去的新版本上。
    ///
    /// 准入那一类只认判定进程打的标记、不认措辞：超时记录里还附着一份给人看的
    /// 现场采样，措辞与上游原文同形，用措辞再判一遍会把"变更自己把 requests 调过
    /// 了配额"那一支也放成环境类，而环境类不构成"这个 rev 坏"的结论，会无限重试
    /// 一个真坏的版本。
    ///
    /// 不回滚的几支判成环境类，其余（含回滚过的那一支）判成版本类。类别与落点
    /// 随 [`RolloutFailure`] 带到 CLI，翻成进程退出码与终止消息交给部署器——
    /// 部署器据此决定这个 rev 还让不让再滚。
    async fn fail_without_blind_rollback(
        &self,
        stage: &str,
        target: &str,
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
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::Unreachable,
                false,
                e,
            ));
        }
        if is_admission_denied(&msg) {
            warn!(
                error = %e,
                "the API server rejected the new pods and this revision did not change the \
                 deployment's placement shape; keeping the new revision (no rollback)"
            );
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::Admission,
                false,
                e,
            ));
        }
        if is_placement_blocked(&msg) {
            warn!(
                error = %e,
                "the scheduler never placed the new pods and this revision did not change the \
                 deployment's placement shape; keeping the new revision (no rollback)"
            );
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::Placement,
                false,
                e,
            ));
        }
        if is_image_source_unavailable(&msg) {
            warn!(
                error = %e,
                "the new pods never pulled their image, so the new revision has not run once; \
                 keeping the new revision (no rollback) — rolling back to an older image cannot \
                 repair the image source, and the revision left in place starts on its own once \
                 the source answers"
            );
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::ImageSource,
                false,
                e,
            ));
        }
        if is_observation_tool_failure(&msg) {
            warn!(
                error = %e,
                "the observation tool itself could not be run; keeping the new revision (no \
                 rollback) — rolling the image back cannot repair a broken tool path"
            );
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::Tool,
                false,
                e,
            ));
        }
        self.rollback(&e, done, prevs).await;
        // 授权被拒这一类在滚动中仍回滚并记版本类：改过的东西要退回去。落点写成
        // auth 而不是 observed，好让"因为读不到集群而回滚"与"版本真的没起来"在
        // 记账和报告里分得开。
        let locus = if is_authorization_denied(&msg) {
            FailureLocus::AuthDenied
        } else {
            FailureLocus::Observed
        };
        Err(RolloutFailure::at(stage, target, locus, false, e))
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
        // 失败分两条通道过 Job 边界：类别走退出码（环境类用独立的 75，让部署器
        // 知道这次失败说不出新版本的好坏），落点走进程的终止消息——类别只有两档，
        // 而"坏在哪一处"是把两个值都塞进退出码塞不下的东西。终止消息是 k8s 给
        // "这个容器为什么死"留的窄通道，读出端按结构化字段取，不解析日志。
        // `process::exit` 不走返回路径，因为 Box<dyn Error> 出去一律是 1。
        Err(f) => {
            report_failure_signature(&f.signature);
            if f.class == FailureClass::Environment {
                tracing::error!(error = %f, exit_code = ROLLOUT_EXIT_ENVIRONMENT, "mainline rollout failed for environment reasons");
                std::process::exit(ROLLOUT_EXIT_ENVIRONMENT);
            }
            Err(Box::new(f))
        }
    }
}

/// 容器终止消息文件（kubelet 的 `terminationMessagePath` 默认值）。失败落点写在
/// 这里，部署器从 Pod 的 `state.terminated.message` 读回。
const TERMINATION_LOG: &str = "/dev/termination-log";

/// 尽力把失败签名写进终止消息。写不进去不能影响失败本身的交付：类别还走退出码，
/// 签名没了只是让部署器少一份证据（它会往"停下"的一侧取，不会因此多滚一轮）。
fn report_failure_signature(signature: &str) {
    if let Err(e) = write_failure_signature(Path::new(TERMINATION_LOG), signature) {
        tracing::warn!(
            path = TERMINATION_LOG,
            error = %e,
            signature,
            "could not record the rollout failure signature; the deployer will see this failure without evidence of its locus"
        );
    }
}

/// 终止消息只该带一行：kubelet 按 4KiB 截断，多行内容在后端 jsonpath 里也读不利索。
fn write_failure_signature(path: &Path, signature: &str) -> std::io::Result<()> {
    std::fs::write(path, format!("{signature}\n"))
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

    /// 名字不带前导斜杠，目录名前后多余的斜杠由这里收掉。
    #[test]
    fn tree_ish_dir_names_a_directory_inside_the_rev() {
        assert_eq!(tree_ish_dir("HEAD", "deploy/k3s"), "HEAD:deploy/k3s/");
        assert_eq!(tree_ish_dir("HEAD", "/deploy/k3s/"), "HEAD:deploy/k3s/");
    }

    /// 承重的那一条：**带前导斜杠的形式真的会被 git 拒**。
    ///
    /// 只断言拼出来的字符串长什么样的测试，在 git 换个版本改了 tree-ish 解析之后
    /// 仍然绿；这里把两种形式都交给真 git 跑一遍——能列出文件的只有不带斜杠的那
    /// 一种。历史缺陷正是带斜杠的那个：目录枚举每轮失败，收敛面每轮停在「没有
    /// 结论」，apply 一次也没跑到。
    #[test]
    fn only_the_slashes_free_tree_ish_lists_a_directory() {
        let repo = temp_git_repo();
        let listed = git_ls_tree(&repo, &tree_ish_dir("HEAD", "deploy/k3s"));
        assert!(
            listed.contains(&"a.yaml".to_string()),
            "the directory should be listable: {listed:?}"
        );
        // 对照组：同一路径写成 `HEAD:/deploy/k3s/` 时，git 把整串当成一个 object
        // name 去解，于是什么都列不出来——而这里的「什么都列不出来」正是当年线上
        // 那个「这一轮没有结论」。
        let rejected = git_ls_tree(&repo, "HEAD:/deploy/k3s/");
        assert!(
            rejected.is_empty(),
            "git accepted the leading-slash form; the case this guard exists for changed: {rejected:?}"
        );
        let _ = std::fs::remove_dir_all(&repo);
    }

    /// 一个只含 `deploy/k3s/a.yaml` 的临时仓库，用完删掉。
    fn temp_git_repo() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cogneva-tree-ish-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("deploy/k3s")).expect("create temp repo");
        std::fs::write(dir.join("deploy/k3s/a.yaml"), "kind: ConfigMap\n").expect("write file");
        for args in [
            vec!["init", "--quiet"],
            vec!["add", "-A"],
            vec![
                "-c",
                "user.email=gate@example.invalid",
                "-c",
                "user.name=gate",
                "commit",
                "--quiet",
                "-m",
                "seed",
            ],
        ] {
            let out = std::process::Command::new("git")
                .current_dir(&dir)
                .args(&args)
                .output()
                .expect("git runs");
            assert!(
                out.status.success(),
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        dir
    }

    /// 一层目录的文件名；git 不认这个 spec 时按「列不出来」返回空。
    fn git_ls_tree(repo: &std::path::Path, spec: &str) -> Vec<String> {
        let out = std::process::Command::new("git")
            .current_dir(repo)
            .args(["ls-tree", "--name-only", spec])
            .output()
            .expect("git runs");
        if !out.status.success() {
            return Vec::new();
        }
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect()
    }

    /// 最终镜像阶段每条 `COPY`/`ADD` 的落点（shell 与 JSON 两种写法都能读）。
    ///
    /// 只看最后一个 `FROM` 之后的指令：前面那些阶段往镜像里放的东西不经过 run
    /// 阶段的文件系统。反斜杠续行先并成一条逻辑指令，`ADD` 与 `COPY` 同等看待
    /// ——往镜像里烤资产的两条指令里，漏读任何一条，这道门禁就看不见新资产，
    /// 而它失效的方向正是「该红不红」：烘焙了、没人知道、overlay 也刷不到。
    fn image_baked_destinations(dockerfile: &str) -> Vec<String> {
        let mut logical: Vec<String> = Vec::new();
        let mut pending = String::new();
        for raw in dockerfile.lines() {
            let line = raw.trim();
            let continued = line.ends_with('\\');
            let body = line.strip_suffix('\\').unwrap_or(line).trim();
            if !pending.is_empty() {
                pending.push(' ');
            }
            pending.push_str(body);
            if !continued {
                logical.push(std::mem::take(&mut pending));
            }
        }
        if !pending.is_empty() {
            logical.push(pending);
        }

        let mut last_stage: Vec<&str> = Vec::new();
        for line in &logical {
            if line.starts_with("FROM ") {
                last_stage.clear();
            }
            last_stage.push(line.as_str());
        }

        last_stage
            .iter()
            .filter_map(|line| {
                line.strip_prefix("COPY ")
                    .or_else(|| line.strip_prefix("ADD "))
                    .and_then(copy_destination)
            })
            .collect()
    }

    /// 一条 `COPY`/`ADD` 去掉指令名之后的落点：shell 形态取最后那个非 flag 的
    /// token，JSON 形态取数组最后一个元素。
    fn copy_destination(rest: &str) -> Option<String> {
        // 先摘掉 `--from=` / `--chmod=` 这类 flag，剩下的第一个字符才能告诉
        // 我们这是 shell 形态还是 JSON 形态。
        let mut rest = rest.trim();
        while let Some((head, tail)) = rest.split_once(char::is_whitespace) {
            if !head.starts_with("--") {
                break;
            }
            rest = tail.trim();
        }

        // JSON 形态交给 JSON 解析，不按逗号切：路径里的逗号是合法字符，切错了
        // 会把落点读成半截路径——门禁比对不到就红，方向是 "该绿不绿"，还不算
        // 最坏；真正要防的是读出一个对得上的错落点。
        if rest.starts_with('[') {
            if let Ok(items) = serde_json::from_str::<Vec<String>>(rest) {
                return items.last().cloned().filter(|dest| !dest.is_empty());
            }
            return None;
        }

        rest.split_whitespace()
            .next_back()
            .map(|dest| dest.trim_matches(|c| c == '"').to_string())
            .filter(|dest| !dest.is_empty())
    }

    /// 镜像烤进去的运行时资产集，必须与 overlay 刷新的资产集对齐。
    ///
    /// 两个集合各自演进时，新二进制会配着旧资产跑，而失配是静默的：skill 里
    /// 一个工具名对不上注册表，那个角色就收窄成空工具表，日志里没有任何一行
    /// 提示。所以判据不写死资产名，而是读 Dockerfile 最终阶段的 COPY 行——
    /// 谁往镜像里烤了东西，谁就得出现在 overlay 的资产表里，或者进那张要
    /// 写明理由的「刷不了」表。
    #[test]
    fn every_asset_the_image_bakes_is_refreshed_by_the_overlay_or_declared_unreachable() {
        let dockerfile =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Dockerfile"))
                .expect("read workspace Dockerfile");

        let baked = image_baked_destinations(&dockerfile);
        // 解析器自身得先站得住：run 阶段既拷二进制（来自构建产物）又拷 skills
        // （来自构建上下文），两条形态不同的 COPY 都得读到。只断言其中一条时，
        // 另一条的写法一变就是静默漏读——门禁看着全绿，其实已经瞎了。
        for expected in [OVERLAY_BINARY_DEST, "/opt/cogneva/skills"] {
            assert!(
                baked.iter().any(|d| d == expected),
                "the Dockerfile's run stage no longer copies {expected} in a form this gate \
                 can read, so the gate is reading nothing: {baked:?}"
            );
        }

        let list = crate::runtime_assets::embedded_asset_list();
        let refreshed: HashSet<&str> = list.iter().map(|e| e.to.as_str()).collect();
        let unreachable: HashSet<&str> = OVERLAY_UNREFRESHABLE.iter().map(|(to, _)| *to).collect();
        let mut unaccounted: Vec<&str> = baked
            .iter()
            .map(String::as_str)
            .filter(|dest| {
                *dest != OVERLAY_BINARY_DEST
                    && !refreshed.contains(dest)
                    && !unreachable.contains(dest)
            })
            .collect();
        unaccounted.sort_unstable();

        assert!(
            unaccounted.is_empty(),
            "the image bakes runtime assets the overlay never refreshes, so the pod would \
             run the new binary beside a stale copy of them: {unaccounted:?}. Add each to \
             deploy/overlay-assets.json, or to OVERLAY_UNREFRESHABLE with the reason it \
             cannot be refreshed from the checkout."
        );
    }

    /// 资产表的源路径必须真的在检出了才有得拷。
    ///
    /// 表读的是被部署 rev 的检出，所以这里断言的也是「表里每一项的来源在本仓库
    /// 存在」——表是仓库的一部分，仓库里没有就永远拷不到。
    #[test]
    fn every_overlay_asset_source_exists_in_the_checkout() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for entry in crate::runtime_assets::embedded_asset_list() {
            let src = root.join(&entry.from);
            assert!(
                src.exists(),
                "overlay copies {} out of the checkout, but {src:?} does not exist",
                entry.from
            );
        }
    }

    /// 门禁的解析器要认得往镜像里烤资产的每一种写法，否则新资产用另一种写法
    /// 烤进去就是静默漏判（该红不红）。
    #[test]
    fn the_baked_destination_reader_understands_every_copy_form() {
        let dockerfile = r#"
FROM ubuntu:24.04 AS builder
COPY --from=builder /x /opt/never-looked-at
FROM ubuntu:24.04
COPY --from=builder /out/cogneva /opt/cogneva/cogneva
COPY --chmod=755 \
     --from=webbuilder \
     /src/web/dist \
     /opt/cogneva/web
ADD skills /opt/cogneva/skills
COPY ["prompts", "/opt/cogneva/prompts"]
"#;
        let baked = image_baked_destinations(dockerfile);
        assert_eq!(
            baked,
            vec![
                "/opt/cogneva/cogneva",
                "/opt/cogneva/web",
                "/opt/cogneva/skills",
                "/opt/cogneva/prompts",
            ]
        );
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

    /// 拉取失败与致命等待态必须是两份判据：前者说不出病因（镜像源此刻不服务，
    /// 会自愈），判死就会回滚一份完好的版本、并把这个 rev 记成"坏"而不再重试。
    #[test]
    fn a_pull_failure_is_not_a_fatal_waiting_state() {
        // 非空转：致命那一份仍然握着它自己的三种形态。
        for reason in [
            "InvalidImageName",
            "CreateContainerConfigError",
            "CrashLoopBackOff",
        ] {
            assert!(FATAL_WAITING_REASONS.contains(&reason), "{reason}");
        }
        // 镜像引用非法留在致命那一份：它不会自愈，与"镜像源此刻不服务"不同。
        for reason in IMAGE_PULL_WAITING_REASONS {
            assert!(!FATAL_WAITING_REASONS.contains(reason), "{reason}");
        }
        assert!(IMAGE_PULL_WAITING_REASONS.contains(&"ErrImagePull"));
        assert!(IMAGE_PULL_WAITING_REASONS.contains(&"ImagePullBackOff"));

        // 只认拉取那一类，别的等待态不许借走它的处置。
        let pull = vec![
            "ImagePullBackOff".to_string(),
            "PodInitializing".to_string(),
        ];
        assert_eq!(image_pull_blocked(&pull), Some("ImagePullBackOff"));
        assert_eq!(image_pull_blocked(&["CrashLoopBackOff".to_string()]), None);
        assert_eq!(image_pull_blocked(&[]), None);
    }

    /// 镜像源的标记自成一类：不许被别的标记命中，也不许命中别的标记。
    #[test]
    fn the_image_source_marker_is_its_own_class() {
        let e = format!(
            "{IMAGE_SOURCE_UNAVAILABLE_MARKER}: rollout of deployment/x did not complete \
             within 300s (readiness phase) — its pod(s) are still waiting on the image \
             (waiting=ErrImagePull)"
        );
        assert!(is_image_source_unavailable(&e));
        assert!(!is_cluster_unreachable(&e));
        assert!(!is_placement_blocked(&e));
        assert!(!is_admission_denied(&e));
        assert!(!is_observation_tool_failure(&e));
        assert!(!is_image_source_unavailable(&format!(
            "{PLACEMENT_BLOCKED_MARKER}: x"
        )));

        // 落点归环境类（两个阶段都是），才能不回滚、不被记成"这个 rev 坏"。
        assert_eq!(
            FailureLocus::ImageSource.class(false),
            FailureClass::Environment
        );
        assert_eq!(
            FailureLocus::ImageSource.class(true),
            FailureClass::Environment
        );
        assert_eq!(FailureLocus::ImageSource.as_str(), "image-source");
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
            failed_class: FailureClass::Version,
            ci_hold_rev: None,
            ..Default::default()
        };
        let msg = heartbeat_message(&state, "4dfd51ff1209abcdef", "off", 100);
        assert!(msg.contains("bare=4dfd51ff1209"), "{msg}");
        assert!(msg.contains("upstream=off"), "{msg}");
        // 门禁按住与"没有新 rev"必须在这行上分开：两者都不前进。
        assert!(msg.contains("ci_hold=none"), "{msg}");
        assert!(msg.contains("last_good=4dfd51ff1209"), "{msg}");
        assert!(msg.contains("in_flight=none"), "{msg}");
        assert!(msg.contains("failed_rev=none"), "{msg}");
        assert!(msg.contains("failed_class=none"), "{msg}");
        assert!(msg.contains("failed_loci=0"), "{msg}");
        assert!(msg.contains("failed_repeated=false"), "{msg}");
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
            failed_signatures: vec![
                Some("version:wait:cogneva-web:observed".into()),
                Some("version:soak:cogneva-web:observed".into()),
            ],
            failed_repeated: false,
            failed_class: FailureClass::Version,
            ci_hold_rev: Some("deadbeef0011".into()),
        };
        let msg = heartbeat_message(&state, "aabbccddeeff0011", "up-to-date(aabbccddeeff)", 1000);
        assert!(msg.contains("in_flight=aabbccddeeff@Pushed"), "{msg}");
        assert!(msg.contains("upstream=up-to-date(aabbccddeeff)"), "{msg}");
        assert!(msg.contains("failed_rev=112233445566"), "{msg}");
        assert!(msg.contains("failed_class=version"), "{msg}");
        // 两处不同的落点 = 还在往前走；和"复现了"是两件事，不能只看个数。
        assert!(msg.contains("failed_loci=2"), "{msg}");
        assert!(msg.contains("failed_repeated=false"), "{msg}");
        assert!(msg.contains("cooldown_remaining_secs=500"), "{msg}");
        assert!(msg.contains("ci_hold=deadbeef0011"), "{msg}");
    }

    /// 环境类失败不占证据面，`failed_rev` 与 `failed_loci=0` 会同时出现：
    /// 心跳必须说出类别，否则这一行读起来像记账坏了。
    #[test]
    fn heartbeat_message_names_an_environment_class_failure() {
        let state = MainlineState {
            failed_rev: Some("112233445566aabb".into()),
            failed_cooldown_until: 1500,
            failed_class: FailureClass::Environment,
            ..Default::default()
        };
        let msg = heartbeat_message(&state, "112233445566aabb", "diverged", 1000);
        assert!(msg.contains("upstream=diverged"), "{msg}");
        assert!(msg.contains("failed_rev=112233445566"), "{msg}");
        assert!(msg.contains("failed_class=environment"), "{msg}");
        assert!(msg.contains("failed_loci=0"), "{msg}");
    }

    #[test]
    fn a_partly_unreachable_upstream_set_is_not_reported_as_clean() {
        // 全平台可达：结论原样出现，不拖一个空后缀。
        assert_eq!(
            upstream_note_with("up-to-date(aabbccddeeff)", &[]),
            "up-to-date(aabbccddeeff)"
        );
        // 只有一个平台拉得动时，结论必须带着失联平台的名字——否则这一行读起来
        // 就是"两个上游都说没有更新"，而实际上另一个根本没被问到。
        let note = upstream_note_with("up-to-date(aabbccddeeff)", &["github"]);
        assert_eq!(note, "up-to-date(aabbccddeeff); unreachable=github");
        assert!(note.contains("unreachable=github"));
        // 推进路径同样不能吞掉它。
        assert_eq!(
            upstream_note_with("advanced(gitee=aabbccddeeff)", &["github"]),
            "advanced(gitee=aabbccddeeff); unreachable=github"
        );
    }

    #[test]
    fn heartbeat_message_survives_unreadable_bare_rev() {
        // 心跳本身绝不能成为故障源：bare 读取失败时降级为占位文本。
        let msg = heartbeat_message(
            &MainlineState::default(),
            "unreadable(git failed)",
            "unreachable(2)",
            0,
        );
        assert!(msg.contains("bare=unreadable("), "{msg}");
        assert!(msg.contains("upstream=unreachable(2)"), "{msg}");
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
        let retry = |cooldown_until, retry_of_failed_rev, class, repeated| RetryBudget {
            cooldown_until,
            retry_of_failed_rev,
            class,
            repeated,
        };
        let env = FailureClass::Environment;
        let ver = FailureClass::Version;
        // 同 rev 不前进
        assert_eq!(
            evaluate_advance(bare, &main(bare), true, now, retry(0, false, ver, false)),
            AdvanceDecision::SameRev
        );
        // 非祖先（分叉/倒退）拒
        assert_eq!(
            evaluate_advance(bare, &other, false, now, retry(0, false, ver, false)),
            AdvanceDecision::NotAncestor
        );
        // 环境类在限速窗内不重试：别在同一个满节点上空转。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(2000, true, env, false)),
            AdvanceDecision::InCooldown
        );
        // 环境类没有次数上限：窗过了就照常再试。把"集群装不下"读成"这个版本试够了"
        // 会让一个本来正常的版本被搁置到下个 rev。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(0, true, env, false)),
            AdvanceDecision::Advance
        );
        // 版本类不看时间窗：同一处坏没复现过就还值得再看一次（情况可能在动）。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(2000, true, ver, false)),
            AdvanceDecision::Advance
        );
        // 版本类同一处坏复现过：确定性失败，停下等新 rev。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(0, true, ver, true)),
            AdvanceDecision::Repeated
        );
        // 复现结论只对失败的那个 rev 成立：换 rev 就是换了一份待验的东西。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(0, false, ver, true)),
            AdvanceDecision::Advance
        );
        // fix-forward：新 rev 到达时既不撞限速窗也不撞复现结论。
        assert_eq!(
            evaluate_advance(bare, &other, true, now, retry(2000, false, ver, false)),
            AdvanceDecision::Advance
        );
        // 迁移首轮（Legacy）直接前进
        assert_eq!(
            evaluate_advance(
                bare,
                &DeployedState::Legacy,
                false,
                now,
                retry(0, false, ver, false)
            ),
            AdvanceDecision::Advance
        );
        // 认不出 rev（Legacy/Mixed 归一后的形态）不豁免记账判据：滚动失败回滚成
        // 浮动签留下的就是 Legacy + failed_rev==bare，此时判据必须照常生效，否则
        // 循环会无限重滚同一个已知坏的 rev。
        assert_eq!(
            evaluate_advance(
                bare,
                &DeployedState::Legacy,
                true,
                now,
                retry(0, true, ver, true)
            ),
            AdvanceDecision::Repeated
        );
        assert_eq!(
            evaluate_advance(
                bare,
                &DeployedState::Legacy,
                true,
                now,
                retry(2000, true, env, false)
            ),
            AdvanceDecision::InCooldown
        );
        // 但自愈语义不能被误伤：Legacy 且失败的不是这个 rev（外部写入造成的非一致，
        // 或冷启动首轮）仍要放行，否则部署器永久静默停摆。
        assert_eq!(
            evaluate_advance(
                bare,
                &DeployedState::Legacy,
                true,
                now,
                retry(2000, false, ver, false)
            ),
            AdvanceDecision::Advance
        );
        assert_eq!(
            evaluate_advance(
                bare,
                &DeployedState::Legacy,
                true,
                now,
                retry(0, true, ver, false)
            ),
            AdvanceDecision::Advance
        );
        // 混合态不前进
        assert_eq!(
            evaluate_advance(
                bare,
                &DeployedState::Mixed,
                true,
                now,
                retry(0, false, ver, false)
            ),
            AdvanceDecision::Mixed
        );
    }

    /// 失败落点的证据规则：同一处坏第二次出现才算复现；落点读不到时按"同因"记
    /// （判不准就往停下的一侧取）。
    #[test]
    fn failure_evidence_holds_only_when_the_same_locus_repeats() {
        let a = Some("version:wait:cogneva-web:observed".to_string());
        let b = Some("version:apply:cogneva-web:observed".to_string());
        // 第一次失败：没有可比的证据，不算复现。
        assert!(!failure_repeats(&[], a.as_deref()));
        // 同一处坏第二次：复现。
        assert!(failure_repeats(std::slice::from_ref(&a), a.as_deref()));
        // 换了一处坏：不是复现——滚动每次比上次远，说明情况在动。
        assert!(!failure_repeats(std::slice::from_ref(&a), b.as_deref()));
        // 落点读不到（Pod 已删、查询失败）：不排除同因，按同因记。
        assert!(failure_repeats(std::slice::from_ref(&a), None));
        assert!(failure_repeats(&[None], a.as_deref()));
        // 只有过一次读不到的失败、这次仍读不到：同样是复现（否则读不出证据就
        // 变成了"无限重滚"的许可证）。
        assert!(failure_repeats(&[None], None));
    }

    #[test]
    fn failure_signatures_round_trip_through_the_termination_message() {
        let sig = failure_signature(
            FailureClass::Version,
            "wait",
            "cogneva-web",
            FailureLocus::Observed,
        );
        assert_eq!(sig, "version:wait:cogneva-web:observed");
        assert_eq!(
            parse_failure_signature(&format!("{sig}\n")),
            Some(sig.clone())
        );
        // 陌生文本不当证据：容器可能因为别的原因死掉，别的进程也可能往这条通道
        // 里写过东西。把一段陌生字串当签名会让"是不是同一处坏"变成掷骰子。
        assert_eq!(parse_failure_signature(""), None);
        assert_eq!(parse_failure_signature("some other failure"), None);
        assert_eq!(parse_failure_signature("version:wait:web:nonsense"), None);
        assert_eq!(parse_failure_signature("fatal:wait:web:observed"), None);
        assert_eq!(parse_failure_signature("version:wait:web"), None);
        // 多行只取第一行：kubelet 按 4KiB 截断，后面跟着的一般是别的进程的残余。
        assert_eq!(
            parse_failure_signature(&format!("{sig}\ngarbage")),
            Some(sig)
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
            failed_signatures: vec![None, Some("version:wait:web:observed".into())],
            failed_repeated: true,
            failed_class: FailureClass::Environment,
            ci_hold_rev: None,
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
        // 检出自带一份资产表，且比编在二进制里的那份多一项：rev 新增的资产必须
        // 由承载它的那次滚动带上，而不是等下一次。
        std::fs::create_dir_all(work.join("skills")).unwrap();
        std::fs::write(work.join("skills/generator.json"), "[]").unwrap();
        std::fs::create_dir_all(work.join("prompts")).unwrap();
        std::fs::write(work.join("prompts/system.md"), "system\n").unwrap();
        std::fs::create_dir_all(work.join("deploy")).unwrap();
        std::fs::write(
            work.join("deploy/overlay-assets.json"),
            r#"{"assets":[
  {"from": "crates/cog-storage/migrations", "to": "/opt/cogneva/crates/cog-storage/migrations"},
  {"from": "skills", "to": "/opt/cogneva/skills"},
  {"from": "prompts", "to": "/opt/cogneva/prompts"}
]}"#,
        )
        .unwrap();
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

    /// fake kubectl：支撑工作负载的代数查询，前 `failures` 次 `get deploy` 以
    /// 集群不可达的形状失败（真 kubectl 连不上 apiserver 时的 stderr），之后
    /// 正常；statefulset 一直正常。失败次数落盘计数，所以「重试过没有」能从
    /// 日志里读出来，而不用让替身睡够一个尝试超时。
    fn fake_kubectl_failing_times(dir: &Path, failures: u32) -> String {
        let log = dir.join("kubectl.log");
        let count = dir.join("kubectl-failures");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
case "$*" in
  *"get deploy"*)
    n=0
    [ -f '{count}' ] && n=$(cat '{count}')
    if [ "$n" -lt {failures} ]; then
      echo $((n + 1)) > '{count}'
      echo "Unable to connect to the server: dial tcp 10.43.0.1:443: i/o timeout" >&2
      exit 1
    fi
    echo "cogneva 3" ;;
  *"get statefulset"*) echo "pg 7" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            count = count.display(),
            failures = failures
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
            observability_stack: crate::config::ObservabilityStackConfig::default(),
            build_timeout_secs: 60,
            cargo_build_jobs: 2,
            soak_secs: 1,
            restart_threshold: 1,
            failure_cooldown_secs: 60,
            rollout_timeout_secs: 60,
            startup_timeout_secs: 900,
            job_cpu_request: "7m".into(),
            job_memory_request: "21Mi".into(),
            job_cpu_limit: "333m".into(),
            job_memory_limit: "199Mi".into(),
            heartbeat_log_secs: 3600,
            manifest_dir: "deploy/k3s".into(),
            // 上游跟踪在用例里默认关：多数用例只关心 bare 前进后的收敛路径；
            // 跟踪本身的用例自己配 upstreams + git_proxy_base（本地路径当作
            // 透传根，走真实 git fetch，不改这些用例的网络面）。
            upstreams: Vec::new(),
            git_proxy_base: String::new(),
            upstream_fetch_timeout_secs: 30,
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

    /// 在 `proxy/{platform}/owner/repo.git` 造一个上游镜像仓，main 指向给定 rev。
    /// 透传根在用例里就是本地路径，git fetch 走文件传输——测的是真实
    /// refspec/update-ref 路径，不引入网络。
    async fn make_upstream_mirror(root: &Path, platform: &str, repo: &str, src: &Path, rev: &str) {
        let dir = root.join("proxy").join(platform).join(repo);
        std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
        let dir = dir.to_string_lossy().into_owned();
        real_git(root, &["init", "--bare", &dir]).await;
        real_git(
            root,
            &["--git-dir", &dir, "symbolic-ref", "HEAD", "refs/heads/main"],
        )
        .await;
        real_git(src, &["push", &dir, &format!("{rev}:refs/heads/main")]).await;
    }

    fn upstream_config(root: &Path, bare: &Path) -> MainlineDeployerConfig {
        MainlineDeployerConfig {
            upstreams: vec![
                crate::config::UpstreamTrackConfig {
                    platform: crate::config::CodePlatform::Github,
                    repo: "owner/repo".into(),
                    api_base: None,
                },
                crate::config::UpstreamTrackConfig {
                    platform: crate::config::CodePlatform::Gitee,
                    repo: "owner/repo".into(),
                    api_base: None,
                },
            ],
            git_proxy_base: root.join("proxy").to_string_lossy().into_owned(),
            ..test_config(root, bare, "buildah", "kubectl")
        }
    }

    /// 上游 main 前进后由集群内自己拉进 bare 并推进本地 main：这条链上不该
    /// 再有宿主机定时器。两端镜像同一提交时取同一个 rev，谁先到都一样。
    #[tokio::test]
    async fn upstream_main_advances_the_bare_without_a_host_timer() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, rev_a, rev_b) = setup_repos(root).await;
        // bare 还停在 A，平台两端都已前进到 B。
        real_git(
            root,
            &[
                "--git-dir",
                bare.to_str().unwrap(),
                "update-ref",
                "refs/heads/main",
                &rev_a,
            ],
        )
        .await;
        make_upstream_mirror(root, "github", "owner/repo.git", &work, &rev_b).await;
        make_upstream_mirror(root, "gitee", "owner/repo.git", &work, &rev_b).await;

        let deployer =
            MainlineDeployer::new(upstream_config(root, &bare), test_workspaces(root, &bare));
        let advanced = deployer.refresh_upstream(&rev_a).await;

        assert_eq!(advanced.as_deref(), Some(rev_b.as_str()));
        assert_eq!(deployer.bare_main_rev().await.unwrap(), rev_b);
        assert!(
            deployer.upstream_note().contains("advanced"),
            "{}",
            deployer.upstream_note()
        );
    }

    /// 只有一端前进（另一端还停在旧位置）时同样推进：新 head 是旧 head 的
    /// 后代，这正是一个人只推了 gitee 或只推了 github 的形态。
    #[tokio::test]
    async fn a_single_sided_advance_still_advances_the_bare() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, rev_a, rev_b) = setup_repos(root).await;
        real_git(
            root,
            &[
                "--git-dir",
                bare.to_str().unwrap(),
                "update-ref",
                "refs/heads/main",
                &rev_a,
            ],
        )
        .await;
        make_upstream_mirror(root, "github", "owner/repo.git", &work, &rev_a).await;
        make_upstream_mirror(root, "gitee", "owner/repo.git", &work, &rev_b).await;

        let deployer =
            MainlineDeployer::new(upstream_config(root, &bare), test_workspaces(root, &bare));
        assert_eq!(
            deployer.refresh_upstream(&rev_a).await.as_deref(),
            Some(rev_b.as_str())
        );
    }

    /// 两端真的分叉（互不为祖先）时不推进：替分叉猜一个方向，等于用一次
    /// 分叉决定集群跑谁的代码。主线停在原处并留下可查的结论。
    #[tokio::test]
    async fn divergent_upstream_mains_leave_the_bare_alone() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, rev_a, rev_b) = setup_repos(root).await;
        real_git(
            root,
            &[
                "--git-dir",
                bare.to_str().unwrap(),
                "update-ref",
                "refs/heads/main",
                &rev_a,
            ],
        )
        .await;
        make_upstream_mirror(root, "github", "owner/repo.git", &work, &rev_b).await;

        // gitee 从 A 分叉出一条自己的提交。
        let fork = root.join("fork");
        real_git(
            root,
            &[
                "clone",
                "-b",
                "main",
                bare.to_str().unwrap(),
                fork.to_str().unwrap(),
            ],
        )
        .await;
        real_git(&fork, &["config", "user.email", "t@t.com"]).await;
        real_git(&fork, &["config", "user.name", "T"]).await;
        std::fs::write(fork.join("fork.txt"), "fork\n").unwrap();
        real_git(&fork, &["add", "."]).await;
        real_git(&fork, &["commit", "-m", "fork"]).await;
        let rev_c = real_git_stdout(&fork, &["rev-parse", "HEAD"]).await;
        make_upstream_mirror(root, "gitee", "owner/repo.git", &fork, &rev_c).await;

        let deployer =
            MainlineDeployer::new(upstream_config(root, &bare), test_workspaces(root, &bare));
        assert_eq!(deployer.refresh_upstream(&rev_a).await, None);
        assert_eq!(deployer.bare_main_rev().await.unwrap(), rev_a);
        assert_eq!(deployer.upstream_note(), "diverged");
    }

    /// 没有透传根就没有集群内的上游入口：不推进，但结论要落在心跳里——
    /// 只跟随 bare 的部署必须和"跟踪坏了"能从日志上分开。
    #[tokio::test]
    async fn without_a_proxy_base_tracking_is_off() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, rev_a, rev_b) = setup_repos(root).await;
        real_git(
            root,
            &[
                "--git-dir",
                bare.to_str().unwrap(),
                "update-ref",
                "refs/heads/main",
                &rev_a,
            ],
        )
        .await;
        make_upstream_mirror(root, "github", "owner/repo.git", &work, &rev_b).await;

        let cfg = MainlineDeployerConfig {
            git_proxy_base: String::new(),
            ..upstream_config(root, &bare)
        };
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        assert_eq!(deployer.refresh_upstream(&rev_a).await, None);
        assert_eq!(deployer.bare_main_rev().await.unwrap(), rev_a);
        assert_eq!(deployer.upstream_note(), "off");
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

    /// 本轮滚动要重拷的资产表取自**被部署 rev 的检出**，不是编在部署器里的那份。
    ///
    /// 部署器永远是上一代二进制，编在它里面的表描述的是上一代的资产。夹具的检出
    /// 比二进制多一项 `/opt/cogneva/prompts`——`rev 新增的资产必须由承载它的那次
    /// 滚动带上`这件事，只有这样断言得到：若回头去用回退表，这次拷贝会消失，
    /// 而线上没有任何一面说得出来。
    #[tokio::test]
    async fn the_overlay_asset_list_comes_from_the_checkout_not_the_binary() {
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

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        // 这条断言让用例有区分力：回退表里没有这个落点，拷贝只可能来自检出。
        assert!(
            !crate::runtime_assets::embedded_asset_list()
                .iter()
                .any(|e| e.to == "/opt/cogneva/prompts"),
            "回退表不该含 /opt/cogneva/prompts，否则本用例证明不了表来自检出"
        );

        let calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        // 源路径在部署器的检出里、落点是检出那一项独占的：这条拷贝只可能是
        // "读了检出里的表"的结果。
        assert!(
            calls.contains(&format!(
                "copy ctr-test-123 {}/prompts /opt/cogneva/prompts",
                deployer.workdir().display()
            )),
            "检出里那一项没被拷进镜像：{calls}"
        );

        // overlay 把这次拷的是什么记进镜像，供跑最新代码的应用侧自己核对。
        assert!(
            calls.contains(crate::runtime_assets::RUNTIME_ASSET_MANIFEST_DEST),
            "镜像里没有留下拷贝记录，应用侧就无从判断跑的是哪一份：{calls}"
        );
        let manifest = std::fs::read_to_string(root.join("state/runtime-assets.json")).unwrap();
        let parsed = crate::runtime_assets::parse_manifest(&manifest).unwrap();
        assert_eq!(parsed.rev, rev_b);
        assert_eq!(
            parsed.assets.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "/opt/cogneva/crates/cog-storage/migrations",
                "/opt/cogneva/prompts",
                "/opt/cogneva/skills",
            ],
            "拷贝记录必须覆盖本次真正拷进去的每个落点：{manifest}"
        );
        for (dest, digest) in &parsed.assets {
            assert!(
                !digest.is_empty() && digest.len() == 64,
                "{dest} 的摘要不像一份 blake3 十六进制：{digest}"
            );
        }
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

    #[test]
    fn http_base_splits_scheme_host_port_and_prefix() {
        // 平台 API 基址带一个透传前缀，请求行必须带上它才落到网关对应分支。
        assert_eq!(
            split_http_base("http://gw:8081/github"),
            Some(("gw".into(), 8081, "/github".into()))
        );
        assert_eq!(
            split_http_base("http://gw:8081/github/"),
            Some(("gw".into(), 8081, "/github".into()))
        );
        assert_eq!(
            split_http_base("http://gw:8081"),
            Some(("gw".into(), 8081, String::new()))
        );
        // 集群内透传是明文 http；https 基址在这里无法解析，门禁按无证据放行。
        assert_eq!(split_http_base("https://gw:443/github"), None);
    }

    /// 门禁只在拿到**明确的失败结论**时按住。一次检查还在跑、平台不可达、
    /// 没配基址，全都算没有证据——那正是"一次上游抖动不该停掉整条主线跟踪"
    /// 的实现，不是健壮性兜底。
    #[tokio::test]
    async fn ci_verdict_blocks_only_on_a_completed_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (endpoint, handle) = fake_registry(vec![http_200(
            r#"{"check_runs":[{"conclusion":"success"},{"conclusion":"failure"}]}"#,
        )])
        .await;
        let mut cfg = upstream_config(root, Path::new("/nonexistent"));
        cfg.upstreams.truncate(1);
        cfg.upstreams[0].api_base = Some(format!("http://{endpoint}/github"));
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        assert_eq!(deployer.ci_verdict_for_rev("abc123").await, Some(false));

        let reqs = handle.await.unwrap();
        assert!(
            reqs[0].starts_with("GET /github/repos/owner/repo/commits/abc123/check-runs "),
            "{:?}",
            reqs[0]
        );
    }

    #[tokio::test]
    async fn ci_verdict_stays_silent_while_a_check_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"check_runs":[{"conclusion":null}]}"#),
            http_200(r#"{"state":"pending"}"#),
        ])
        .await;
        let mut cfg = upstream_config(root, Path::new("/nonexistent"));
        cfg.upstreams.truncate(1);
        cfg.upstreams[0].api_base = Some(format!("http://{endpoint}/github"));
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        assert_eq!(deployer.ci_verdict_for_rev("abc123").await, None);

        // 检查还在跑时问不出结论，于是退到提交状态兜底这条路径上。
        let reqs = handle.await.unwrap();
        assert!(
            reqs[1].starts_with("GET /github/repos/owner/repo/commits/abc123/status "),
            "{:?}",
            reqs[1]
        );
    }

    #[tokio::test]
    async fn ci_verdict_without_a_reachable_platform_is_no_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // 没配基址：不发请求，也不判失败。
        let cfg = upstream_config(root, Path::new("/nonexistent"));
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));
        assert_eq!(deployer.ci_verdict_for_rev("abc123").await, None);

        // 配了但连不上：同样按没证据处理。
        let mut cfg = upstream_config(root, Path::new("/nonexistent"));
        cfg.upstreams.truncate(1);
        cfg.upstreams[0].api_base = Some("http://127.0.0.1:1/github".into());
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));
        assert_eq!(deployer.ci_verdict_for_rev("abc123").await, None);
    }

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
        let failure_of = |name: &str, body: &str| {
            let (root, bin_dir, fake) = (root.to_path_buf(), bin_dir.clone(), fake.clone());
            let name = name.to_string();
            let body = body.to_string();
            async move {
                write_fake_bin(&bin_dir, &name, &body);
                let cfg = test_config(&root, &fake, "noop", &bin_dir.join(&name).to_string_lossy());
                MainlineDeployer::new(cfg, test_workspaces(&root, &fake))
                    .job_failure("j")
                    .await
            }
        };
        assert_eq!(
            failure_of("k75", "#!/bin/sh\necho 75\nexit 0\n")
                .await
                .class,
            FailureClass::Environment
        );
        // 1（版本类退出）、空（Pod 还没终止）、137（信号终止：OOM / 驱逐 / 到点
        // 被杀都长这样，类别判不出）一律按版本类靠：判不准就往记账侧取。
        assert_eq!(
            failure_of("k1", "#!/bin/sh\necho 1\nexit 0\n").await.class,
            FailureClass::Version
        );
        assert_eq!(
            failure_of("kempty", "#!/bin/sh\nexit 0\n").await.class,
            FailureClass::Version
        );
        assert_eq!(
            failure_of("k137", "#!/bin/sh\necho 137\nexit 0\n")
                .await
                .class,
            FailureClass::Version
        );
        // 采样失败（apiserver 不可达）同样按版本类，且不产生第二个错误。
        assert_eq!(
            failure_of("kerr", "#!/bin/sh\necho 'no route to host' >&2\nexit 1\n")
                .await
                .class,
            FailureClass::Version
        );
        // 一个 Pod 都没有、但 Job 的 Failed 条件写明了准入面拒绝：判定进程压根没被
        // 创建出来，退出码永远不会有，这一档要从条件消息里读成环境类——它与"码读不
        // 出来"不是一回事，是采错了地方。
        assert_eq!(
            failure_of(
                "kquota",
                "#!/bin/sh\ncase \"$*\" in\n  *\"job-name=\"*) exit 0 ;;\n  *\"get job\"*) echo 'Error creating: pods \"j-x\" is forbidden: exceeded quota: cogneva-quota, requested: requests.cpu=200m, used: requests.cpu=6, limited: requests.cpu=6' ;;\n  *) exit 0 ;;\nesac\nexit 0\n"
            )
            .await
            .class,
            FailureClass::Environment
        );
        // 反面：条件消息为空时仍按版本类靠，别把"没读到"读成环境类。
        assert_eq!(
            failure_of(
                "kquotaempty",
                "#!/bin/sh\ncase \"$*\" in\n  *\"job-name=\"*) exit 0 ;;\n  *) exit 0 ;;\nesac\nexit 0\n"
            )
            .await
            .class,
            FailureClass::Version
        );
        // 1（版本类退出）带上终止消息里的落点：签名原样读回，类别仍来自退出码。
        let f = failure_of(
            "ksig",
            "#!/bin/sh\necho '1|version:wait:cogneva-web:observed'\nexit 0\n",
        )
        .await;
        assert_eq!(f.class, FailureClass::Version);
        assert_eq!(
            f.signature.as_deref(),
            Some("version:wait:cogneva-web:observed")
        );
        // 落点不合形状（陌生文本）时签名读成"没有证据"，而不是当成一种新失败。
        let f = failure_of("kjunk", "#!/bin/sh\necho '1|boom'\nexit 0\n").await;
        assert_eq!(f.class, FailureClass::Version);
        assert_eq!(f.signature, None);
    }

    /// 环境类失败不是版本结论：它不推翻本 rev 已有的版本证据，也不设复现结论，
    /// 但仍设限速窗（不在同一个满节点上空转），rev 仍记着（下轮走"重试它"）。
    #[tokio::test]
    async fn an_environment_class_failure_does_not_touch_the_version_evidence() {
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
        // 上一轮已记过一次版本类失败：这次环境类失败不该把它洗掉，也不该就此
        // 判成"复现"。
        let state_path = seed_state(
            root,
            MainlineState {
                in_flight: Some(InFlight {
                    rev: rev_b.clone(),
                    phase: Phase::Dispatched,
                }),
                failed_rev: Some(rev_b.clone()),
                failed_signatures: vec![Some("version:wait:cogneva-web:observed".into())],
                failed_repeated: false,
                failed_class: FailureClass::Version,
                ..Default::default()
            },
        );

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let state = read_state(&state_path);
        assert_eq!(state.failed_class, FailureClass::Environment);
        assert_eq!(
            state.failed_signatures,
            vec![Some("version:wait:cogneva-web:observed".to_string())],
            "环境类失败不是版本结论，不能洗掉版本证据"
        );
        assert!(!state.failed_repeated, "环境类失败不构成复现");
        assert_eq!(state.failed_rev.as_deref(), Some(rev_b.as_str()));
        assert!(state.in_flight.is_none());
        assert!(
            state.failed_cooldown_until > chrono::Utc::now().timestamp(),
            "环境类失败仍要限速，否则部署器会在同一个满节点上空转"
        );
        // 类别必须来自 Job 的 Pod 终止码：这条查询没发生就说明判据换了来源。
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            calls.contains(&format!("job-name={}", job_name(&rev_b))),
            "exit code query missing: {calls}"
        );
    }

    /// 版本类失败：同一处坏第二次出现才判复现（确定性失败，停下等新 rev）。
    #[tokio::test]
    async fn a_repeated_failure_locus_holds_the_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let kubectl = fake_kubectl_failed_job(
            &bin_dir,
            &main_image("localhost:30500", &rev_a),
            "1|version:wait:cogneva-web:observed",
        );
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
                failed_signatures: vec![Some("version:wait:cogneva-web:observed".into())],
                failed_repeated: false,
                failed_class: FailureClass::Version,
                ..Default::default()
            },
        );

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let state = read_state(&state_path);
        assert_eq!(state.failed_class, FailureClass::Version);
        assert!(
            state.failed_repeated,
            "同一处坏第二次出现就是可复现的确定性失败"
        );
        assert_eq!(
            state.failed_signatures.len(),
            1,
            "同一处坏不重复入账：签名集合记的是见过哪些落点"
        );
    }

    /// 版本类失败但落点变了：滚动每次比上次远说明情况在动，还值得再看一次——
    /// 这里不能停下，否则一次半途的失败就把这个 rev 判死。
    #[tokio::test]
    async fn a_new_failure_locus_keeps_the_revision_moving() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let kubectl = fake_kubectl_failed_job(
            &bin_dir,
            &main_image("localhost:30500", &rev_a),
            "1|version:soak:cogneva-web:observed",
        );
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
                failed_signatures: vec![Some("version:wait:cogneva-web:observed".into())],
                failed_repeated: false,
                failed_class: FailureClass::Version,
                ..Default::default()
            },
        );

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let state = read_state(&state_path);
        assert_eq!(state.failed_class, FailureClass::Version);
        assert!(!state.failed_repeated, "换了一处坏不是复现");
        assert_eq!(state.failed_signatures.len(), 2, "新的落点入账");
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

    /// A support-workload read that hits an unreachable cluster is tried again
    /// inside the wait budget instead of ending the rollout: one expired
    /// attempt is a stall, not a verdict, and this read is the gate before
    /// anything changes.
    #[tokio::test]
    async fn an_unreachable_snapshot_read_is_retried_before_any_change() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_failing_times(&bin_dir, 1);
        // Budget 8s: the stall is absorbed within it, and the poll interval is
        // the only real wait this test pays.
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 8, 900);

        let workloads = executor.support_workloads().await.unwrap();
        assert_eq!(workloads.len(), 2, "{workloads:?}");
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert_eq!(
            calls.lines().filter(|l| l.contains("get deploy")).count(),
            2,
            "the read has to be tried again, not turned into a rollout failure: {calls}"
        );
    }

    /// The ConfigMap-consumer read is the third pre-flight read taken before
    /// anything changes, and it is retried for the same reason as the other
    /// two: the failure it would otherwise report is "no conclusion was
    /// reached", not "this version is bad".
    #[tokio::test]
    async fn an_unreachable_config_consumer_read_is_retried_before_any_change() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_failing_times(&bin_dir, 1);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 8, 900);

        executor.live_config_consumers().await.unwrap();
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert_eq!(
            calls.lines().filter(|l| l.contains("get deploy")).count(),
            2,
            "the read has to be tried again, not turned into a rollout failure: {calls}"
        );
    }

    /// A cluster that never answers ends the read, and the error says how many
    /// attempts were spent: "one attempt expired" and "starved for the whole
    /// budget" are different situations, and the Job that produced this reading
    /// is gone by the time anyone reads the ledger.
    #[tokio::test]
    async fn an_unreachable_reading_carries_the_attempt_count() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let kubectl = fake_kubectl_failing_times(&bin_dir, u32::MAX);
        // Budget 0: the first attempt is already at the deadline, so nothing
        // sleeps and the count is pinned to one.
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 0, 900);

        let err = executor.support_workloads().await.unwrap_err().to_string();
        assert!(err.contains("cluster unreachable"), "{err}");
        assert!(err.contains("(attempts=1 over "), "{err}");
    }

    fn support_workload(kind: &'static str, name: &str, generation: i64) -> SupportWorkload {
        SupportWorkload {
            kind,
            name: name.to_string(),
            generation,
        }
    }

    #[test]
    fn changed_workloads_selects_what_this_apply_restarted() {
        let before = vec![
            support_workload("deploy", "cogneva-registry", 1),
            support_workload("deploy", "meilisearch", 3),
            support_workload("statefulset", "postgres", 1),
        ];
        let after = vec![
            // 代数前进：apply 改了 spec，这个会滚动重启。
            support_workload("deploy", "cogneva-registry", 2),
            // 没改：原地不动，不该等。
            support_workload("deploy", "meilisearch", 3),
            // apply 之前不在：新增的工作负载同样要等到就绪。
            support_workload("statefulset", "nats", 1),
            // 目标部署自己：由逐目标等待负责，不在这里再等一遍。
            support_workload("deploy", "cogneva-security-gateway", 9),
            support_workload("statefulset", "postgres", 1),
        ];
        let targets = vec![RolloutTarget {
            deployment: "cogneva-security-gateway".into(),
            container: "security-gateway".into(),
            component: "security-gateway".into(),
            name: "cogneva".into(),
        }];
        let changed: Vec<String> = changed_workloads(&before, &after, &targets)
            .iter()
            .map(SupportWorkload::display)
            .collect();
        assert_eq!(changed, vec!["deploy/cogneva-registry", "statefulset/nats"]);
    }

    #[test]
    fn support_settled_requires_observed_generation_and_ready_replicas() {
        // generation|observed|want|ready
        assert_eq!(support_settled("2|2|1|1|"), Some(true));
        // 控制器还没看到这一代 spec：新副本一个都还没起。
        assert_eq!(support_settled("2|1|1|1|"), Some(false));
        // 滚动中：就绪副本还没补齐。
        assert_eq!(support_settled("2|2|3|2|"), Some(false));
        // 缩到 0 副本：spec 要 0 个，就绪 0 个成立。
        assert_eq!(support_settled("2|2|0|"), Some(true));
        // replicas 缺省（读作 1）而就绪数读不到：不能当成"不需要就绪"。
        assert_eq!(support_settled("2|2||"), Some(false));
        // 读不出来不是"就绪"。
        assert_eq!(support_settled(""), None);
        assert_eq!(support_settled("|2|1|1|"), None);
    }

    /// 支撑清单 apply 会连带重启后端与集群内 registry：目标部署的镜像要从这个
    /// registry 拉、启动要连这些后端，所以必须等它们回到就绪再滚目标。
    ///
    /// 假 kubectl 把这条判据做成硬失败：目标在支撑工作负载就绪之前被 `set image`
    /// 就报错退出。少了这段等待，滚动会直接失败（而不是靠断言顺序来推断）。
    #[tokio::test]
    async fn rollout_waits_for_the_support_workloads_its_own_apply_restarted() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let (manifests, log) = fake_kubectl_support_settle(&bin_dir, 2);

        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            60,
            60,
        );
        let mut plan = RolloutPlan::from_config(
            &MainlineDeployerConfig::default(),
            "localhost:30500/cogneva:main-new".into(),
        );
        plan.manifests_dir = Some(manifests.to_string_lossy().to_string());
        executor.run(&plan).await.unwrap();

        let calls = std::fs::read_to_string(&log).unwrap();
        // 按字节位置比而不是按行：假 kubectl 用 `echo "$@"` 落日志，实参里的
        // `\n`（jsonpath 的换行转义）会被 sh 的 echo 展开成真换行，行切分靠不住。
        let first_target = calls.find("set image ").expect("no target rolled");
        let last_settle_poll = calls
            .rfind("get deploy cogneva-registry")
            .expect("the restarted support workload was never waited on");
        assert!(
            last_settle_poll < first_target,
            "the target rolled before the support workload this apply restarted was ready: {calls}"
        );
        // 没被这次 apply 动过的工作负载不进等待：等它就是把无关的故障算到这次上线头上。
        assert!(
            !calls.contains("get deploy meilisearch"),
            "an untouched support workload must not gate the rollout: {calls}"
        );
    }

    /// 等不到就绪就停下，一个目标都不滚。类别按落点判：集群读得到而工作负载一直
    /// 起不来，怀疑的是这次下发的支撑清单（这份发布集里就有它），判版本类；读不到
    /// 集群才判环境类。此处前者成立，且一个镜像都还没动——所以只停，没有回滚对象。
    #[tokio::test]
    async fn unsettled_support_workload_stops_before_any_target_rolls() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 采样永远落后一代：这个支撑工作负载不会就绪。
        let (manifests, log) = fake_kubectl_support_settle(&bin_dir, 999);

        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            60,
            1,
        );
        let mut plan = RolloutPlan::from_config(
            &MainlineDeployerConfig::default(),
            "localhost:30500/cogneva:main-new".into(),
        );
        plan.manifests_dir = Some(manifests.to_string_lossy().to_string());
        let err = executor.run(&plan).await.unwrap_err();
        assert_eq!(err.class, FailureClass::Version, "{err:?}");
        assert!(
            err.to_string()
                .contains("support workload this rollout restarted did not become ready"),
            "{err}"
        );
        // 这条失败有自己的签名：与目标滚动失败是不同的落点，部署器据此判"同 rev
        // 反复失败是不是同一处坏"。
        assert_eq!(err.signature, "version:support-settle::observed");

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            !calls.contains("set image "),
            "no target may be rolled while the support workload it depends on is not ready: {calls}"
        );
    }

    /// 假 kubectl：支撑清单里 registry 被 apply 改了 spec（代数前进），`set image`
    /// 在它回到就绪之前一律硬失败。`settle_at` 是第几次采样才报就绪。
    fn fake_kubectl_support_settle(dir: &Path, settle_at: u32) -> (PathBuf, PathBuf) {
        let manifests = dir.join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(
            manifests.join("support.yaml"),
            "kind: ConfigMap\nmetadata:\n  name: cogneva-config\n",
        )
        .unwrap();

        let log = dir.join("kubectl.log");
        let applied = dir.join("applied.marker");
        let settled = dir.join("settled.marker");
        let count = dir.join("settle.count");
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
  *"apply -f "*)
    touch '{applied}'
    echo "configmap/cogneva-config configured" ;;
  # 支撑工作负载表：apply 之后 registry 的代数前进，其余没动。
  *"get deploy -o"*)
    if [ -f '{applied}' ]; then echo "cogneva-registry 2"; else echo "cogneva-registry 1"; fi
    echo "meilisearch 1"
    ;;
  *"get statefulset -o"*) ;;
  # 等就绪：第 settle_at 次采样才就绪，之前一直落后一代。
  *"get deploy cogneva-registry -o"*)
    n=$(cat '{count}' 2>/dev/null || echo 0)
    n=$((n+1)); echo "$n" > '{count}'
    if [ "$n" -ge {settle_at} ]; then touch '{settled}'; echo "2|2|1|1|"; else echo "2|1|1|0|"; fi
    ;;
  *"get deployment "*)
    echo "1|1|1|1|1|" ;;
  *"set image "*)
    if [ ! -f '{settled}' ]; then
      echo "target rolled while the support workload this apply restarted was not ready" >&2
      exit 1
    fi
    echo "deployment.apps/x image updated" ;;
  *"terminated.finishedAt"*) ;;
  *"deletionTimestamp"*) echo "p-new|Running|true|||2026-09-17T15:46:29Z|" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) ;;
  *"get pods"*) ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            applied = applied.display(),
            settled = settled.display(),
            count = count.display(),
            settle_at = settle_at
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        (manifests, log)
    }

    /// 消费面读数（与真实 jsonpath 的字段序一致）：卷、projected、envFrom、
    /// env.valueFrom、initContainer 的两段，外加一个什么都不读的工作负载——
    /// 空段不能把后面的段顶掉，没有消费的工作负载也不能被当成读了一堆空名字。
    #[test]
    fn config_consumers_reads_every_consumption_face() {
        let readout = [
            // 卷两个（cogneva-json 与 prompts）、envFrom 一个、initContainer 的 envFrom 一个。
            "cogneva|,cogneva-json,cogneva-prompts,|,,|cogneva-config,|||cogneva-evolution-config,",
            // 什么都不读：整段空，名字后面只有分隔符。
            "postgres||||||",
            "   ",
        ]
        .join("\n");
        let consumers = config_consumers(&readout, "deploy");
        assert_eq!(consumers.len(), 2, "{consumers:?}");
        let cogneva = &consumers[0];
        assert_eq!(cogneva.name, "cogneva");
        assert_eq!(
            cogneva.configmaps,
            vec![
                "cogneva-config".to_string(),
                "cogneva-evolution-config".to_string(),
                "cogneva-json".to_string(),
                "cogneva-prompts".to_string(),
            ]
        );
        assert!(consumers[1].configmaps.is_empty());
    }

    /// 内容变了才算变：新增（上次不在）也算——它落下去的那一刻就是一次新交付。
    #[test]
    fn changed_configmaps_notices_edits_and_additions() {
        let before: BTreeMap<String, serde_json::Value> = [
            (
                "cogneva-json".to_string(),
                serde_json::json!({"cogneva.json": "a"}),
            ),
            ("untouched".to_string(), serde_json::json!({"k": "v"})),
        ]
        .into_iter()
        .collect();
        let after: BTreeMap<String, serde_json::Value> = [
            (
                "cogneva-json".to_string(),
                serde_json::json!({"cogneva.json": "b"}),
            ),
            ("untouched".to_string(), serde_json::json!({"k": "v"})),
            ("brand-new".to_string(), serde_json::json!({"k": "v"})),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            changed_configmaps(&before, &after),
            vec!["brand-new".to_string(), "cogneva-json".to_string()]
        );
    }

    /// 有分段表的配置文件按表判：热更新面覆盖到的段变了不滚（滚了是白重启），
    /// 只有重启才生效的段变了才滚。没有分段表的 ConfigMap 只能按内容判。
    #[test]
    fn a_hot_reloaded_section_does_not_roll_the_consumers() {
        let document = |body: &str| serde_json::json!({ "cogneva.json": body });
        let hot_old = document(r#"{"tuning": {"stream_capacity": 8}}"#);
        let hot_new = document(r#"{"tuning": {"stream_capacity": 16}}"#);
        assert!(!config_change_needs_restart(
            cog_core::config_sections::CONFIG_CONFIGMAP,
            Some(&hot_old),
            Some(&hot_new)
        ));
        let startup_new = document(r#"{"observability": {"alert_rules": []}}"#);
        assert!(config_change_needs_restart(
            cog_core::config_sections::CONFIG_CONFIGMAP,
            Some(&hot_old),
            Some(&startup_new)
        ));
        // 读不出文档（形状不对）：说不清就走安全侧。
        assert!(config_change_needs_restart(
            cog_core::config_sections::CONFIG_CONFIGMAP,
            Some(&serde_json::json!("not an object")),
            Some(&hot_new)
        ));
        // 没有分段表的那几份：内容变了就滚。
        assert!(config_change_needs_restart(
            "cogneva-config",
            Some(&serde_json::json!({"k": "v"})),
            Some(&serde_json::json!({"k": "w"}))
        ));
    }

    /// fake kubectl：support apply 只改 ConfigMap（工作负载代数不动），配置文档里
    /// `observability` 段在 apply 前后不同。两个读它的工作负载（一个目标是
    /// `cogneva`、一个是非目标的 `cogneva-patcher`）都要被 patch 上 restartedAt，
    /// 且都发生在任何 set image 之前；非目标那个还要被支撑等待等到就绪。
    fn fake_kubectl_config_effect(dir: &Path, hot_only: bool) -> (PathBuf, PathBuf) {
        let manifests = dir.join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(
            manifests.join("support.yaml"),
            "kind: ConfigMap\nmetadata:\n  name: cogneva-json\n",
        )
        .unwrap();

        let log = dir.join("kubectl.log");
        let applied = dir.join("applied.marker");
        let patched = dir.join("patched.marker");
        let settled = dir.join("settled.marker");
        // 热更新面覆盖到的段（tuning）变了：没有值得滚的理由，配置文档照样变。
        // 内层文档要按 JSON 字符串嵌进 configmap 列表里，引号得转义。
        let (before_body, after_body) = if hot_only {
            (
                r#"{\"tuning\":{\"stream_capacity\":8}}"#,
                r#"{\"tuning\":{\"stream_capacity\":16}}"#,
            )
        } else {
            (
                r#"{\"observability\":{\"alert_rules\":[]}}"#,
                r#"{\"observability\":{\"alert_rules\":[{\"name\":\"x\"}]}}"#,
            )
        };
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*|json) ;;
      *) echo "error: unable to match a printer" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"apply -f "*)
    touch '{applied}'
    echo "configmap/cogneva-json configured" ;;
  # 配置现状：apply 之前是旧文档，之后是新文档。
  *"get configmap -o json"*)
    if [ -f '{applied}' ]; then
      echo '{{"items":[{{"metadata":{{"name":"cogneva-json"}},"data":{{"cogneva.json":"{after_body}"}}}}]}}'
    else
      echo '{{"items":[{{"metadata":{{"name":"cogneva-json"}},"data":{{"cogneva.json":"{before_body}"}}}}]}}'
    fi ;;
  # 消费面：只有这一种读法会提到 configMapRef（支撑代数读法走下面那一支）。
  *"configMapRef"*)
    echo 'cogneva|cogneva-json|'
    echo 'cogneva-patcher|cogneva-json|'
    echo 'meilisearch||||||' ;;
  *"get deploy -o"*)
    if [ -f '{patched}' ]; then echo "cogneva-patcher 2"; else echo "cogneva-patcher 1"; fi
    echo "meilisearch 1" ;;
  *"get statefulset -o"*) ;;
  # 被 patch 过之后代数才算前进，等它就绪才是"等这次 apply 动过的工作负载"。
  *"get deploy cogneva-patcher -o"*)
    if [ -f '{patched}' ]; then touch '{settled}'; echo "2|2|1|1|"; else echo "1|1|1|1|"; fi
    ;;
  *"patch deploy cogneva-patcher"*) touch '{patched}'; echo "deployment.apps/cogneva-patcher patched" ;;
  *"patch deploy cogneva "*) touch '{patched}'; echo "deployment.apps/cogneva patched" ;;
  *"get deployment "*)
    echo "1|1|1|1|1|" ;;
  *"set image "*)
    if [ "{expect_patch}" = "yes" ] && [ ! -f '{patched}' ]; then
      echo "target rolled while a config consumer had not been rolled yet" >&2
      exit 1
    fi
    echo "deployment.apps/x image updated" ;;
  *"terminated.finishedAt"*) ;;
  *"deletionTimestamp"*) echo "p-new|Running|true|||2026-09-17T15:46:29Z|" ;;
  *"restartCount"*) echo "0 true " ;;
  *"waiting.reason"*) ;;
  *"get pods"*) ;;
  # 只有它不是 jsonpath：`-o jsonpath=...` 也以 `-o json` 开头，所以放最后。
  *" -o json"*) echo '{{}}' ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            applied = applied.display(),
            patched = patched.display(),
            settled = settled.display(),
            before_body = before_body,
            after_body = after_body,
            expect_patch = if hot_only { "no" } else { "yes" }
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        (manifests, log)
    }

    fn config_effect_executor(manifests: &Path) -> RolloutPlan {
        let mut plan = RolloutPlan::from_config(
            &MainlineDeployerConfig::default(),
            "localhost:30500/cogneva:main-new".into(),
        );
        plan.manifests_dir = Some(manifests.to_string_lossy().to_string());
        plan
    }

    /// 只改配置的 rev：工作负载代数一个都没动（apply 一份 ConfigMap 不动任何 spec），
    /// 逐目标滚动与支撑等待都不会碰它。配置文档里只有重启才生效的段变了，读它的
    /// 工作负载就必须被滚——否则文件是新的、进程还是旧的、记录说已生效。
    #[tokio::test]
    async fn a_config_only_change_rolls_the_workloads_that_read_it() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let (manifests, log) = fake_kubectl_config_effect(&bin_dir, false);
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            60,
            60,
        );
        executor
            .run(&config_effect_executor(&manifests))
            .await
            .expect("rollout should succeed");

        let calls = std::fs::read_to_string(&log).unwrap();
        // 按字节位置比而不是按行：假 kubectl 用 `echo "$@"` 落日志，实参里的
        // `\n`（jsonpath 的换行转义）会被 sh 的 echo 展开成真换行，行切分靠不住。
        let first_target = calls.find("set image ").expect("no target rolled");
        for workload in ["cogneva-patcher", "cogneva"] {
            let patch = calls
                .find(&format!("patch deploy {workload} "))
                .unwrap_or_else(|| panic!("{workload} was never rolled: {calls}"));
            assert!(
                patch < first_target,
                "{workload} was rolled after the targets started rolling: {calls}"
            );
        }
        assert!(
            calls.contains("cogneva.io/restartedAt"),
            "the roll has to stamp the pod template, not just the object: {calls}"
        );
        // 非目标那个由支撑等待一并等它回就绪。
        assert!(
            calls.contains("get deploy cogneva-patcher -o"),
            "an off-target config consumer has to be waited on: {calls}"
        );
        // 没读这份配置的工作负载不进滚动名单。
        assert!(
            !calls.contains("patch deploy meilisearch"),
            "a workload that does not read the config must not be rolled: {calls}"
        );
    }

    /// 配置文档变了，但变的都是热更新面覆盖得到的段：交付即生效，滚一次是白重启，
    /// 还会把一个纯配置改动记成一次服务中断。
    #[tokio::test]
    async fn a_hot_reloaded_config_change_rolls_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let (manifests, log) = fake_kubectl_config_effect(&bin_dir, true);
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            60,
            60,
        );
        executor
            .run(&config_effect_executor(&manifests))
            .await
            .expect("rollout should succeed");

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            !calls.contains("restartedAt"),
            "a hot-reloadable change must not cost a restart: {calls}"
        );
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

    /// init 容器拉不到镜像时主容器只报 PodInitializing：这一档必须能读到（判据
    /// 要覆盖 init 容器），但**不能**判成版本坏——新版本一次都没跑起来，镜像源
    /// 自己可能就是病因。线上实测过：registry 与四个后端一起重启，目标 Pod 在
    /// 4 秒后被读成 ErrImagePull 版本类失败并回滚，回滚掉的是一份完好的版本。
    #[tokio::test]
    async fn a_pod_that_cannot_pull_its_image_is_blamed_on_the_source_not_the_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let log = bin_dir.join("kubectl.log");
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
        // 启动预算 0：判败走的是预算到期那一刻的判定，而不是首轮的早退——拉取
        // 失败会自愈，判它快慢没有意义。（这个部署带 init 容器，到期的是启动
        // 预算：init 一直没结束，就绪预算还没起算。）
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            0,
            1,
            300,
            0,
        );
        let plan = sandbox_executor_plan("localhost:30500/cogneva:main-new");

        let err = executor.run(&plan).await.unwrap_err();
        let rendered = format!("{err:?}");
        assert_eq!(err.class, FailureClass::Environment, "{rendered}");
        assert!(
            err.error
                .to_string()
                .contains(IMAGE_SOURCE_UNAVAILABLE_MARKER),
            "{rendered}"
        );
        // 判词要写出是哪一个等待态，人才知道 kubelet 停在哪一步。
        assert!(
            err.error.to_string().contains("waiting=ImagePullBackOff"),
            "{rendered}"
        );
        // 不回滚：回滚到一个更旧的镜像并不能让镜像源恢复。判据是"旧镜像一次都
        // 没被写回去"——正向那次 set image 本来就该发生，所以不能笼统地禁 set
        // image；回滚的两种形状（写回 prev tag、rollout undo）都要禁掉。
        let calls = std::fs::read_to_string(&log).unwrap_or_default();
        assert!(
            !calls.contains("main-old") && !calls.contains("rollout undo"),
            "no rollback should happen: {calls}"
        );
        assert!(
            calls.contains("set image") && calls.contains("main-new"),
            "the new revision must still be the one being rolled out: {calls}"
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

        // 归类：新版本的容器一次都没起来，读出的是集群此刻放不下，不是这份变更的
        // 好坏 ⇒ 环境类，不回滚、不占本 rev 的尝试预算（占满了就会把一份本来正常
        // 的变更搁置到下个 rev）。这条用例此前锁的是相反结论，改判时显式翻过来。
        assert_eq!(err.class, FailureClass::Environment);
        let calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            !calls.contains(
                "set image deployment/cogneva-sandbox-executor sandbox-executor=localhost:30500/cogneva:main-old"
            ),
            "an admission rejection must not roll the revision back: {calls}"
        );
    }

    /// 准入被拒算不算环境类，与「排不上队」共用同一条边界：**只有这次上线没动过
    /// 放置面**时才是集群此刻的问题。变更自己把 requests 调过了配额、加了个越界的
    /// LimitRange 值，都是这份变更的毛病——环境类不占尝试预算，误放会无限重试。
    ///
    /// 判不准（读不到当前面、快照为空）同样往版本侧靠；校验类失败压根不是准入面
    /// 拒绝，不能混进来。
    #[test]
    fn an_admission_rejection_is_environment_only_when_the_shape_did_not_change() {
        let shape = "{\"template\":{\"spec\":{\"containers\":[{}]}}}";
        let quota = vec![ReplicaSetFailure {
            name: "cogneva-x-1a2b3".to_string(),
            reason: "FailedCreate".to_string(),
            message: "pods \"cogneva-x-1a2b3-c4d5e\" is forbidden: exceeded quota: \
                      cogneva-quota, requested: limits.cpu=500m, used: limits.cpu=16500m"
                .to_string(),
        }];
        let denied = admission_rejection(&quota, shape, Some(shape)).expect("environment class");
        assert!(denied.contains("exceeded quota"), "{denied}");

        // 这次上线自己把请求调过了配额 ⇒ 版本类（那一支带的是普通超时错误，标记
        // 不打，照常回滚）。
        let raised = "{\"template\":{\"spec\":{\"containers\":[{\"resources\":\
                      {\"requests\":{\"cpu\":\"4\"}}}]}}}";
        assert!(admission_rejection(&quota, shape, Some(raised)).is_none());
        // 判不准就往版本侧靠。
        assert!(admission_rejection(&quota, shape, None).is_none());
        assert!(admission_rejection(&quota, "", Some(shape)).is_none());
        // 没有准入被拒这个信号 ⇒ 这条超时与准入无关。
        assert!(admission_rejection(&[], shape, Some(shape)).is_none());
        // 校验类失败（无效字段值）不是准入面拒绝：那是这份变更写错了清单。
        let invalid = vec![ReplicaSetFailure {
            name: "cogneva-x-1a2b3".to_string(),
            reason: "FailedCreate".to_string(),
            message: "pods \"cogneva-x-1a2b3-c4d5e\" is invalid: \
                      spec.containers[0].image: Required value"
                .to_string(),
        }];
        assert!(admission_rejection(&invalid, shape, Some(shape)).is_none());
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
    /// 被写入），以及二进制没有可执行位。前者是瞬时的、且起因在测试自己身上（装
    /// 可执行文件的写描述符被并发 fork 的兄弟进程继承走），已改由子进程拷贝落成品
    /// 从源头消除，就不再拿它当回归用例；这里用后者，稳定可复现。
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
    fn namespace_docs_skips_install_surface_kinds_and_rejects_secret() {
        // 多文档：Namespace（集群级）、Role/RoleBinding（权限面）、
        // ResourceQuota/LimitRange（治理面）与 PersistentVolumeClaim（卷声明面）
        // 跳过，ConfigMap/Service 保留，空文档跳过。
        let yaml = "---\nkind: Namespace\nmetadata:\n  name: x\n---\nkind: ConfigMap\nmetadata:\n  name: c\n---\nkind: Role\nmetadata:\n  name: r\nrules: []\n---\nkind: RoleBinding\nmetadata:\n  name: rb\n---\nkind: ResourceQuota\nmetadata:\n  name: cogneva-quota\n---\nkind: LimitRange\nmetadata:\n  name: cogneva-limits\n---\nkind: PersistentVolumeClaim\nmetadata:\n  name: cogneva-data-pvc\nspec:\n  resources:\n    requests:\n      storage: 24Gi\n---\nkind: Service\nmetadata:\n  name: svc\n---\n";
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

    /// 真实事故向量：清单里写着 `defaultMode: 0400`，我们按 1.2 读成字符串、
    /// 重排后加引号写回，集群按 1.1 读成八进制整数 256——于是 apply 阶段报
    /// `unrecognized type: int32`，错误指向我们产出的那份副本，而不是任何人
    /// 手写的那份清单。门禁在**读源文本时**就要拦住它，并给出双方都认的写法。
    #[test]
    fn an_octal_file_mode_is_refused_before_any_rewrite() {
        let text = "kind: Deployment\nmetadata:\n  name: cogneva\nspec:\n  template:\n    spec:\n      volumes:\n        - name: key\n          secret:\n            secretName: git-key\n            defaultMode: 0400\n";
        let err = reject_dialect_dependent_scalars(text, "deployment.yaml").unwrap_err();
        let msg = err.to_string();
        // 行号指到那一行，而不是整份清单。
        assert!(msg.contains("deployment.yaml:11"), "{msg}");
        // 两种读法都写出来，读的人不必自己推。
        assert!(msg.contains("`0400`"), "{msg}");
        assert!(msg.contains("string"), "{msg}");
        assert!(msg.contains("octal integer 0400"), "{msg}");
        // 并给出等价的中性写法，照抄即可修好。
        assert!(msg.contains("`256`"), "{msg}");

        // 两个对照：写出集群要的十进制整数、或显式引号声明这是字符串，都放行。
        let decimal = text.replace("defaultMode: 0400", "defaultMode: 256");
        assert!(reject_dialect_dependent_scalars(&decimal, "deployment.yaml").is_ok());
        let quoted = text.replace("defaultMode: 0400", "defaultMode: \"0400\"");
        assert!(reject_dialect_dependent_scalars(&quoted, "deployment.yaml").is_ok());
    }

    /// 一份清单里可能出现多种两方言不同型的裸写法，每一种都要被认出来；
    /// 而普通值（含两位数的端口、`true`/`false`、带空格的字符串、被引号或
    /// 块标量显式定型的值）一个都不许误伤。
    #[test]
    fn every_dialect_dependent_spelling_is_named_and_ordinary_values_are_not() {
        for (line, expected) in [
            ("enableServiceLinks: yes", "boolean"),
            ("shareProcessNamespace: on", "boolean"),
            ("foo: No", "boolean"),
            ("mode: 0o400", "string"),
            ("millis: 1_000", "string"),
            ("duration: 1:30", "string"),
            ("port: 0755", "octal integer 0755"),
        ] {
            let text = format!("kind: ConfigMap\nmetadata:\n  name: c\n{line}\n");
            let err = reject_dialect_dependent_scalars(&text, "c.yaml")
                .unwrap_err()
                .to_string();
            assert!(err.contains("c.yaml:4"), "{line} -> {err}");
            assert!(err.contains(expected), "{line} -> {err}");
        }
        for line in [
            // 两位十进制、`true`/`false`、两个方言同型。
            "port: 8080",
            "enabled: true",
            "disabled: false",
            // 空格让它成为字符串，两边一致。
            "command: chmod 0400 /key",
            // 引号/块标量/流式/标签显式定型，两边一致。
            "mode: \"0400\"",
            "note: |",
            "ports: [8080, 9090]",
            "tag: !!str 0400",
            "inherit: *anchor",
            // 序列项前缀与注释不该被当成值。
            "- name: x",
            "# defaultMode: 0400",
            "metadata:",
        ] {
            let text = format!("kind: ConfigMap\nmetadata:\n  name: c\n{line}\n");
            assert!(
                reject_dialect_dependent_scalars(&text, "c.yaml").is_ok(),
                "{line} must pass"
            );
        }
    }

    /// 块标量里的每一行都是同一个字符串，集群不会把它重新定型——所以块里的
    /// 裸 `no` / `0400` 一个都不该拦：prompt 与内嵌配置就住在这样的块里，
    /// 拦下来等于为一段谁都不会重新读的文本卡死整条落地通道。
    #[test]
    fn text_inside_a_block_scalar_is_not_a_scalar_of_the_document() {
        let text = "kind: ConfigMap\nmetadata:\n  name: c\ndata:\n  prompt: |\n    answer with yes or no\n    mode: 0400\n    - off\n  other: 1\n";
        assert!(reject_dialect_dependent_scalars(text, "c.yaml").is_ok());
        // 块结束（缩进不再更深）之后的那一行照旧在判据面内。
        let after = text.replace("  other: 1", "  other: 0400");
        let err = reject_dialect_dependent_scalars(&after, "c.yaml")
            .unwrap_err()
            .to_string();
        assert!(err.contains("c.yaml:9"), "{err}");
    }

    /// 门禁的输入是发布集里的文件内容，所以要在**真的会进滚动包的那些清单**上
    /// 验一遍：一个误伤就把整条落地通道卡死在一个与本次版本无关的理由上。
    /// 清单名从 `kustomization.yaml` 反查，不手写文件名——手写的清单会随新增
    /// 资源静默过期。
    #[test]
    fn the_shipped_manifests_pass_the_dialect_gate() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        // `deploy/k3s` 是部署器默认读的发布集；预渲染目录也在内，因为发布集目录
        // 是配置面（`manifest_dir`），把它指到任一预渲染目录是受支持的用法，
        // 那些文件同样会被重排后再 apply。
        for rel in [
            "deploy/k3s",
            "deploy/rendered/k3s-single",
            "deploy/rendered/k3s-multi",
            "deploy/rendered/k8s-standard",
        ] {
            let dir = root.join(rel);
            // 发布集目录用 kustomization 反查；预渲染目录是一堆平铺清单，
            // 目录里的每一项都在发布面内。
            let names: Vec<String> = match std::fs::read_to_string(dir.join("kustomization.yaml")) {
                Ok(k) => parse_kustomization_resources(&k).unwrap(),
                Err(_) => {
                    let mut v: Vec<String> = std::fs::read_dir(&dir)
                        .unwrap_or_else(|e| panic!("read_dir {rel}: {e}"))
                        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                        .filter(|n| n.ends_with(".yaml"))
                        .collect();
                    v.sort();
                    v
                }
            };
            assert!(!names.is_empty(), "{rel} has no manifests to check");
            for name in &names {
                let path = dir.join(name);
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                if let Err(e) = reject_dialect_dependent_scalars(&text, name) {
                    panic!("{rel}/{name} would be refused by the dialect gate: {e}");
                }
            }
            // 对照组：全绿本身不能证明门禁在真的读这些文件——也可能它什么都没
            // 读到。把真实事故那一段追加进目录里第一份清单，必须当场被拦。
            let first = &names[0];
            let text = std::fs::read_to_string(dir.join(first)).unwrap();
            let armed = format!(
                "{text}---\nkind: Deployment\nmetadata:\n  name: cogneva\nspec:\n  template:\n    \
                 spec:\n      volumes:\n        - name: k\n          secret:\n            \
                 secretName: s\n            defaultMode: 0400\n"
            );
            assert!(
                reject_dialect_dependent_scalars(&armed, first).is_err(),
                "{rel}/{first}: the gate read nothing out of a real shipped manifest"
            );
        }
    }

    /// 治理对象就在发布集里（元启动的自建集群走 `kubectl apply -k deploy/k3s`，
    /// 该文件必须留在 `resources` 里），但一份都不许进支撑包：数值来自 chart
    /// values，随包下发就是拿静态清单里的默认值覆盖运维调过的天花板。
    #[test]
    fn build_rollout_bundle_drops_resource_governance_from_support() {
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
            "resource-quota.yaml".to_string(),
            "kind: ResourceQuota\nmetadata:\n  name: cogneva-quota\nspec:\n  hard:\n    pods: \"40\"\n---\nkind: LimitRange\nmetadata:\n  name: cogneva-limits\nspec:\n  limits: []\n"
                .to_string(),
        );
        files.insert(
            "configmap.yaml".to_string(),
            "kind: ConfigMap\nmetadata:\n  name: c\ndata:\n  k: v\n".to_string(),
        );
        let kustomization = "resources:\n  - resource-quota.yaml\n  - configmap.yaml\n  - deployment.yaml\n  - evolution-deployment.yaml\n";
        let bundle = build_rollout_bundle(
            &set_from_kustomization(files, kustomization),
            &bundle_targets(),
            "img",
        )
        .unwrap();
        assert!(bundle.support_yaml.contains("kind: ConfigMap"));
        assert!(
            !bundle.support_yaml.contains("ResourceQuota"),
            "quota must not travel with the rollout: {}",
            bundle.support_yaml
        );
        assert!(
            !bundle.support_yaml.contains("LimitRange"),
            "limits must not travel with the rollout: {}",
            bundle.support_yaml
        );
    }

    /// 卷声明就在发布集里（自建集群走 `kubectl apply -k deploy/k3s` 建卷，该文件
    /// 必须留在 `resources` 里），但一份都不许进支撑包：绑定后的 PVC spec 不可变，
    /// 循环每 rev 重放一份与集群里不同的声明必然被拒，而那不是关于新版本的结论
    /// ——三个 PVC 拒绝曾让落地通道断了一整天。创建与调整归安装面。
    #[test]
    fn build_rollout_bundle_drops_volume_claims_from_support() {
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
            "app-data-pvc.yaml".to_string(),
            "kind: PersistentVolumeClaim\nmetadata:\n  name: cogneva-data-pvc\nspec:\n  resources:\n    requests:\n      storage: 24Gi\n"
                .to_string(),
        );
        files.insert(
            "configmap.yaml".to_string(),
            "kind: ConfigMap\nmetadata:\n  name: c\ndata:\n  k: v\n".to_string(),
        );
        let kustomization = "resources:\n  - app-data-pvc.yaml\n  - configmap.yaml\n  - deployment.yaml\n  - evolution-deployment.yaml\n";
        let bundle = build_rollout_bundle(
            &set_from_kustomization(files, kustomization),
            &bundle_targets(),
            "img",
        )
        .unwrap();
        assert!(bundle.support_yaml.contains("kind: ConfigMap"));
        assert!(
            !bundle.support_yaml.contains("PersistentVolumeClaim"),
            "a claim must not travel with the rollout: {}",
            bundle.support_yaml
        );
        assert!(
            !bundle.support_yaml.contains("24Gi"),
            "the claim's size must not survive into the bundle: {}",
            bundle.support_yaml
        );
    }

    /// 消费侧的包是**落后的组包者**写出来的：卷声明由旧二进制留在包里（它没有
    /// 那条跳过规则），而 apply 它必然被准入拒。复核必须在 apply 之前把这类
    /// 文档摘掉，让一次上线不因"包与历史不一致"整体中止。
    #[test]
    fn staged_rollout_manifest_drops_install_surface_docs() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("support.yaml");
        let text = "\
---
kind: ConfigMap
metadata:
  name: c
data:
  k: v
---
kind: PersistentVolumeClaim
metadata:
  name: cogneva-data-pvc
spec:
  resources:
    requests:
      storage: 24Gi
";
        let path = stage_rollout_manifest(text, "support.yaml", &out)
            .unwrap()
            .expect("configmap survives filtering");
        assert_eq!(path, out);
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("kind: ConfigMap"), "{body}");
        assert!(!body.contains("PersistentVolumeClaim"), "{body}");
        assert!(!body.contains("24Gi"), "{body}");
    }

    /// 目标清单走的是同一个复核：它与支撑包同源，也同样由落后的组包者产出。
    #[test]
    fn staged_rollout_manifest_drops_install_surface_docs_from_a_target_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("target.yaml");
        let text = "\
---
kind: Deployment
metadata:
  name: cogneva
---
kind: PersistentVolumeClaim
metadata:
  name: cogneva-data-pvc
";
        let path = stage_rollout_manifest(text, "deploy-cogneva.yaml", &out)
            .unwrap()
            .expect("deployment survives filtering");
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("kind: Deployment"), "{body}");
        assert!(!body.contains("PersistentVolumeClaim"), "{body}");
    }

    /// 整包都是安装面对象时没有可 apply 的东西——不写空文件（`kubectl apply`
    /// 对空输入报错，那会把"本来无需 apply"变成一次假失败）。
    #[test]
    fn staged_rollout_manifest_reports_nothing_when_only_install_surface() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("support.yaml");
        let text = "kind: PersistentVolumeClaim\nmetadata:\n  name: p\n";
        assert!(stage_rollout_manifest(text, "support.yaml", &out)
            .unwrap()
            .is_none());
        assert!(!out.exists());
    }

    /// 复核不能把 Secret 这道红线放过去：它既不属滚动面，也不许出现在任何清单里。
    #[test]
    fn staged_rollout_manifest_still_rejects_secret() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("support.yaml");
        let text = "kind: Secret\nmetadata:\n  name: s\n";
        let err = stage_rollout_manifest(text, "deploy-cogneva.yaml", &out).unwrap_err();
        assert!(err.to_string().contains("deploy-cogneva.yaml"), "{err}");
    }

    /// 授权被拒不是版本结论：apiserver 说"不许你做"，读到的不是新版本的好坏。
    /// 判据只认 apiserver 的授权措辞，组包侧自己的 "Secret ... is forbidden"
    /// 不会被误读（否则一个组包错误会被记成本 rev 的一次尝试）。
    #[test]
    fn authorization_denial_is_an_environment_failure_not_a_version_verdict() {
        let denial =
            "kubectl apply -f support.yaml failed: resourcequotas \"cogneva-quota\" is forbidden: \
                      User \"system:serviceaccount:cogneva:cogneva-evolution\" cannot get resource \
                      \"resourcequotas\" in API group \"\" in the namespace \"cogneva\"";
        assert!(is_authorization_denied(denial));
        assert_eq!(
            classify_before_any_change("test", "", SFError::IO(denial.to_string())).class,
            FailureClass::Environment
        );
        // 谁也不许借走它的判定。
        assert!(!is_cluster_unreachable(denial));
        assert!(!is_placement_blocked(denial));
        assert!(!is_observation_tool_failure(denial));
        // 动词面逐个列全，任何一种都算授权拒绝。
        for verb in [
            "get",
            "list",
            "watch",
            "create",
            "update",
            "patch",
            "delete",
            "deletecollection",
        ] {
            let t = format!(
                "X is forbidden: User \"u\" cannot {verb} resource \"v\" in API group \"\""
            );
            assert!(is_authorization_denied(&t), "{verb} must be recognized");
        }
        // 反面：组包侧的 forbidden 与普通 apply 失败都不是授权拒绝，仍归版本类。
        let bundle_err = SFError::Config(
            "secret.yaml: Secret in manifest bundle is forbidden; secrets never travel through manifests"
                .into(),
        );
        assert!(!is_authorization_denied(&bundle_err.to_string()));
        assert_eq!(
            classify_before_any_change("test", "", bundle_err).class,
            FailureClass::Version
        );
        assert_eq!(
            classify_before_any_change(
                "test",
                "",
                SFError::IO("kubectl apply -f support.yaml failed: invalid manifest".into()),
            )
            .class,
            FailureClass::Version
        );
    }

    /// 准入面拒绝（配额打满、LimitRange 越界、PodSecurity）不是版本结论：apiserver
    /// 说的是"集群此刻装不下这份请求"，不是"新版本有毛病"。实测集群 `cogneva-quota`
    /// 的 requests.storage 已 110Gi/110Gi 打满——任何带新增 PVC 的发布集都会在这一档
    /// 被拒；把它记成本 rev 的一次尝试，会按上限搁置一个本来正常的版本。
    #[test]
    fn admission_policy_denial_is_an_environment_failure_not_a_version_verdict() {
        let quota = "error when creating \"STDIN\": pods \"cogneva-mainline-x\" is forbidden: \
                    exceeded quota: cogneva-quota, requested: requests.cpu=200m, \
                    used: requests.cpu=2180m, limited: requests.cpu=6";
        assert!(is_admission_policy_denied(quota));
        assert_eq!(
            classify_before_any_change("test", "", SFError::IO(quota.to_string())).class,
            FailureClass::Environment
        );
        // 配额拒绝带 `is forbidden` 却不带 `User "..."` 主体——正因如此它此前从
        // 授权判据的缝里漏过去、掉回了版本类。两条判据各管各的措辞面。
        assert!(!is_authorization_denied(quota));

        for msg in [
            "pods \"x\" is forbidden: [maximum cpu usage per Container is 2, but limit is 4]",
            "pods \"x\" violates PodSecurity \"restricted:latest\": allowPrivilegeEscalation != false",
        ] {
            assert!(is_admission_policy_denied(msg), "{msg}");
            assert_eq!(
                classify_before_any_change("test", "", SFError::IO(msg.into())).class,
                FailureClass::Environment,
                "{msg}"
            );
        }

        // 反面：清单本身坏掉是版本的事。判据不许认裸 `forbidden`，否则组包侧的错误
        // 会被读成环境类，反而放走一个真坏的版本。
        for msg in [
            "kubectl apply -f support.yaml failed: invalid manifest",
            "secret.yaml: Secret in manifest bundle is forbidden; secrets never travel through manifests",
            "error when creating \"STDIN\": Service \"s\" is invalid: spec.ports[0].port: Invalid value",
        ] {
            assert!(!is_admission_policy_denied(msg), "{msg}");
            assert_eq!(
                classify_before_any_change("test", "", SFError::IO(msg.into())).class,
                FailureClass::Version,
                "{msg}"
            );
        }

        // 滚动阶段的归类只认判定进程打的标记，不认措辞：超时记录里附着的现场
        // 采样与上游原文同形，用措辞再判一遍会把「这次上线自己把请求调过了配额」
        // 那一支也放成环境类，而环境类不占尝试预算，一个真坏的版本会无限重试。
        let timeout_with_scene = format!(
            "rollout of deployment/x did not complete within 300s \
             (last: 1|1|0|1|0|1); pods: replicasets: x FailedCreate: {quota}"
        );
        assert!(is_admission_policy_denied(&timeout_with_scene));
        assert!(!is_admission_denied(&timeout_with_scene));
        assert!(is_admission_denied(&format!(
            "{ADMISSION_DENIED_MARKER}: rollout of deployment/x did not complete within 300s"
        )));
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

    /// 发布集目录形态的发布资源集。解析与去重都归包内函数，测试只提供文本。
    fn set_from_kustomization(files: BTreeMap<String, String>, kustomization: &str) -> ReleaseSet {
        ReleaseSet::from_kustomization(
            files,
            parse_kustomization_resources(kustomization).expect("kustomization resources parse"),
        )
        .expect("release set")
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
            &set_from_kustomization(files, kustomization),
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
        // 发布集里没有交付 evolution 目标的清单（`evolution-deployment.yaml` 不在
        // resources 里，也没有第二个同名 Deployment 可反查）：硬错误，不回落到 set image。
        let partial = "resources:\n  - deployment.yaml\n";
        let err = build_rollout_bundle(
            &set_from_kustomization(files.clone(), partial),
            &bundle_targets(),
            "img",
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("no manifest in the release set delivers Deployment cogneva-evolution"),
            "{err}"
        );
        // kustomization 引用了 files 里不存在的资源：硬错误。
        let kustomization =
            "resources:\n  - deployment.yaml\n  - evolution-deployment.yaml\n  - missing.yaml\n";
        let err = build_rollout_bundle(
            &set_from_kustomization(files, kustomization),
            &bundle_targets(),
            "img",
        )
        .unwrap_err();
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
        let err = build_rollout_bundle(
            &set_from_kustomization(files, kustomization),
            &bundle_targets(),
            "img",
        )
        .unwrap_err();
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
        let bundle = build_rollout_bundle(
            &set_from_kustomization(files, kustomization),
            &bundle_targets(),
            "reg/img:main-x",
        )
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

    #[tokio::test]
    async fn a_literal_env_superseded_by_a_source_is_cleared_before_the_apply() {
        // 真实事故形态：集群对象上某个 env 还是明文 value，新清单改成了 valueFrom。
        // 不先摘掉的话 apply 被准入拒绝，而报错指向清单。
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let log = bin_dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
case "$*" in
  *" get "*|get\ *)
    case "$*" in
      *"-o json"*)
        cat <<'JSON'
{{"kind":"Deployment","metadata":{{"name":"cogneva-security-gateway"}},
 "spec":{{"template":{{"spec":{{"containers":[{{"name":"security-gateway",
 "env":[{{"name":"A","value":"1"}},{{"name":"TOKEN","value":"leaked"}}]}}]}}}}}}}}
JSON
        ;;
      *) echo ok ;;
    esac ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        let manifest = tmp.path().join("deploy-cogneva-security-gateway.yaml");
        std::fs::write(
            &manifest,
            "kind: Deployment\nmetadata:\n  name: cogneva-security-gateway\nspec:\n  template:\n    spec:\n      containers:\n        - name: security-gateway\n          env:\n            - name: A\n              value: \"1\"\n            - name: TOKEN\n              valueFrom:\n                secretKeyRef:\n                  name: cogneva-secrets\n                  key: github-token\n",
        )
        .unwrap();
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            60,
            900,
        );
        executor
            .clear_superseded_env_values(&manifest)
            .await
            .unwrap();

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains("--type=json")
                && calls.contains(r#""path":"/spec/template/spec/containers/0/env/1/value""#),
            "the live object's superseded value must be removed by index: {calls}"
        );
        // 清单里那条仍是 value 的条目不该被动：判据只覆盖"被 valueFrom 取代"的。
        assert!(
            !calls.contains(r#""path":"/spec/template/spec/containers/0/env/0/value""#),
            "an env the manifest still writes as a literal is not ours to touch: {calls}"
        );
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
        let bundle = build_rollout_bundle(
            &set_from_kustomization(files, kustomization),
            &targets,
            "img",
        )
        .unwrap();
        assert_eq!(bundle.targets.len(), 1);
        assert_eq!(bundle.targets[0].deployment, "cogneva");
    }

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    /// 盘上的目录 → 平铺形态的发布资源集，与读 rev 的那条路共用同一个构造器。
    fn flat_set_from_disk(dir: &Path) -> ReleaseSet {
        let mut files = BTreeMap::new();
        for entry in
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            if !is_manifest_file(&name) {
                continue;
            }
            let text = std::fs::read_to_string(dir.join(&name))
                .unwrap_or_else(|e| panic!("read {name}: {e}"));
            files.insert(name, text);
        }
        ReleaseSet::from_flat_dir(files)
    }

    /// 发布集目录形态的盘上读法（`kustomization.yaml` 的 resources 是权威顺序）。
    fn kustomization_set_from_disk(dir: &Path) -> ReleaseSet {
        let kustomization = std::fs::read_to_string(dir.join(KUSTOMIZATION_FILE))
            .unwrap_or_else(|e| panic!("read kustomization in {}: {e}", dir.display()));
        let resources = parse_kustomization_resources(&kustomization).expect("resources parse");
        let mut files = BTreeMap::new();
        for res in &resources {
            files.insert(
                res.clone(),
                std::fs::read_to_string(dir.join(res))
                    .unwrap_or_else(|e| panic!("read {res}: {e}")),
            );
        }
        ReleaseSet::from_kustomization(files, resources).expect("release set")
    }

    /// 平铺的预渲染目录必须真的能被消费。渲染产物的文件名由渲染器生成
    /// （`41-deployment-cogneva.yaml`），与滚动目标声明的清单名
    /// （`deployment.yaml`）对不上——名字对不上就整体停下，正是这条能力缺位时
    /// 的样子：配置面把 `manifest_dir` 指向渲染目录，落地通道在组包这一步
    /// 一次都跑不到。
    #[test]
    fn a_rendered_profile_directory_delivers_every_rollout_target() {
        let root = repo_root();
        let targets = MainlineDeployerConfig::default().targets;
        assert_eq!(
            targets.len(),
            4,
            "the default rollout set changed; this test walks all of it"
        );
        for profile in ["k3s-single", "k3s-multi", "k8s-standard"] {
            let dir = root.join("deploy/rendered").join(profile);
            let set = flat_set_from_disk(&dir);
            // 发布面就是目录自己的清单文件：产出侧新加一份，消费侧自动带上，
            // 不靠任何手写清单。
            let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .filter(|n| is_manifest_file(n))
                .collect();
            on_disk.sort();
            assert_eq!(
                set.resources, on_disk,
                "{profile}: release set != directory"
            );
            assert_eq!(set.shape(), "flat", "{profile}: shape reading");
            let bundle = build_rollout_bundle(&set, &targets, "reg/cogneva:main-x")
                .unwrap_or_else(|e| panic!("{profile}: {e}"));
            assert_eq!(
                bundle.targets.len(),
                targets.len(),
                "{profile}: every target must travel with its manifest"
            );
            for t in &bundle.targets {
                assert!(
                    t.yaml.contains("image: reg/cogneva:main-x"),
                    "{profile}: {} was not rewritten: {}",
                    t.deployment,
                    t.yaml
                );
            }
            // 支撑面来自这个目录自己的清单（有内容、且不带 Secret）。
            assert!(
                bundle.support_yaml.contains("kind: ConfigMap"),
                "{profile}: support face is empty"
            );
            assert!(
                !bundle.support_yaml.contains("kind: Secret"),
                "{profile}: secrets never travel through manifests"
            );
            // 判据自证：目录里那 4 个 Deployment 的清单名确实不是目标声明的名字，
            // 所以上面走的是身份反查这条路，不是名字恰好撞上。
            for t in &targets {
                let declared = t
                    .manifest
                    .as_deref()
                    .expect("default targets declare a manifest");
                assert!(
                    !set.resources.iter().any(|r| r.as_str() == declared),
                    "{profile}: {declared} unexpectedly exists; this test would not exercise the identity lookup"
                );
            }
        }
    }

    /// 目标声明的文件名不在集合里、集合里也没有交付它的 Deployment：硬错误，
    /// 报错要带上是哪个 Deployment 与声明的是哪个文件。
    #[test]
    fn a_target_nothing_delivers_is_refused() {
        let mut files = BTreeMap::new();
        files.insert(
            "41-deployment-cogneva.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        let set = ReleaseSet::from_flat_dir(files);
        let targets = bundle_targets();
        let err = build_rollout_bundle(&set, &targets, "img").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no manifest in the release set delivers Deployment cogneva-evolution"),
            "{msg}"
        );
        assert!(msg.contains("evolution-deployment.yaml"), "{msg}");
        assert!(msg.contains("1 resources"), "{msg}");
    }

    /// 集合里有两份同名 Deployment：滚谁都可能是错的，硬错误。
    #[test]
    fn two_manifests_delivering_one_deployment_are_refused() {
        let mut files = BTreeMap::new();
        files.insert(
            "41-deployment-cogneva.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        files.insert(
            "90-deployment-cogneva-copy.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        files.insert(
            "41-deployment-cogneva-evolution.yaml".to_string(),
            deployment_yaml("cogneva-evolution", "cogneva"),
        );
        let set = ReleaseSet::from_flat_dir(files);
        let err = build_rollout_bundle(&set, &bundle_targets(), "img").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("2 manifests in the release set deliver Deployment cogneva"),
            "{msg}"
        );
    }

    /// 两个目标声明同一个名字（在平铺形态下即反查到同一份清单）：一份文件不能
    /// 承载两次滚动，硬错误并点名两个目标。
    #[test]
    fn two_targets_delivered_by_one_manifest_are_refused() {
        let mut files = BTreeMap::new();
        files.insert(
            "41-deployment-cogneva.yaml".to_string(),
            deployment_yaml("cogneva", "cogneva"),
        );
        let mut targets = bundle_targets();
        targets[0].manifest = Some("41-deployment-cogneva.yaml".to_string());
        targets[1].manifest = Some("41-deployment-cogneva.yaml".to_string());
        let set = ReleaseSet::from_flat_dir(files);
        let err = build_rollout_bundle(&set, &targets, "img").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("are both delivered by the manifest"), "{msg}");
        assert!(msg.contains("cogneva and cogneva-evolution"), "{msg}");
    }

    /// 部署面的两个度必须相互成立：每份 profile 的 `mainlineDeployer.manifestDir`
    /// 指向的目录，部署器要真能消费（发布集目录或平铺的预渲染目录），且四个目标
    /// 都能落到清单上。取值与消费分成两个文件写，漂了只有上线那一步才会发现
    /// ——而那时停的是落地通道。
    #[test]
    fn every_profile_points_its_manifest_dir_at_a_consumable_directory() {
        let root = repo_root();
        let chart = root.join("deploy/helm/cogneva");
        let base: serde_yaml::Value = serde_yaml::from_str(
            &std::fs::read_to_string(chart.join("values.yaml")).expect("read values.yaml"),
        )
        .expect("parse values.yaml");
        let default_dir = base
            .get("mainlineDeployer")
            .and_then(|v| v.get("manifestDir"))
            .and_then(|v| v.as_str())
            .expect("values.yaml has mainlineDeployer.manifestDir")
            .to_string();
        let targets = MainlineDeployerConfig::default().targets;
        let mut checked: Vec<(String, String)> = Vec::new();
        let mut profiles: Vec<PathBuf> = std::fs::read_dir(chart.join("profiles"))
            .expect("profiles dir")
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("yaml"))
            .collect();
        profiles.sort();
        for path in profiles {
            let profile = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let values: serde_yaml::Value =
                serde_yaml::from_str(&std::fs::read_to_string(&path).expect("read profile values"))
                    .expect("parse profile values");
            // 只覆盖这一个度：其余键沿用 chart 基础 values。
            let dir = values
                .get("mainlineDeployer")
                .and_then(|v| v.get("manifestDir"))
                .and_then(|v| v.as_str())
                .unwrap_or(&default_dir)
                .to_string();
            let abs = root.join(&dir);
            assert!(abs.is_dir(), "{profile}: manifestDir {dir} does not exist");
            // 发布面不能跨拓扑：这个部署的清单目录要么是它自己的预渲染目录，
            // 要么是与它逐字段对齐的静态基线（`deploy/k3s` 对 k3s-single，两级
            // 对齐由 parity 门禁守）。别的 profile 的目录指过来就是把另一个拓扑的
            // 宿主路径与套接字形态下发上去——那不报错，只是把集群改成另一个样子。
            let expected = if profile == "k3s-single" {
                "deploy/k3s".to_string()
            } else {
                format!("deploy/rendered/{profile}")
            };
            assert_eq!(
                dir, expected,
                "{profile}: the release face moved to another topology's directory"
            );
            let set = if abs.join(KUSTOMIZATION_FILE).exists() {
                kustomization_set_from_disk(&abs)
            } else {
                flat_set_from_disk(&abs)
            };
            assert!(!set.is_empty(), "{profile}: {dir} carries no manifest");
            let bundle = build_rollout_bundle(&set, &targets, "reg/cogneva:main-x")
                .unwrap_or_else(|e| panic!("{profile}: manifestDir {dir}: {e}"));
            assert_eq!(
                bundle.targets.len(),
                targets.len(),
                "{profile}: {dir} does not deliver every rollout target"
            );
            checked.push((profile, dir));
        }
        assert_eq!(checked.len(), 3, "profile set changed: {checked:?}");
    }

    // --- 版本契约：判据跑在真实 git 上 ---

    use cog_core::contract::version::{judge, Clause, DeclarationChange, Verdict};
    use cog_core::metric_names::{
        VERSION_COMMITS_SINCE_RELEASE, VERSION_CONTRACT_CHECKS_TOTAL, VERSION_CONTRACT_VIOLATIONS,
        VERSION_DECLARED_INFO,
    };
    // 读回读数用的是 trait 上的查询方法，实现类型在，方法得靠 trait 进作用域。
    use cog_core::MetricsBackend;

    /// 造一段带版本声明史的历史：c1 首次声明 0.5.7，c2 只改别的文件，c3 声明
    /// 0.5.8。c2 是拿来证伪「凡是提交就重读一遍版本」这类近似的：它既不该进
    /// 声明链，也不能打断 c1 到 c3 的顺序。
    ///
    /// 返回 (bare, work, revs)，revs 按历史顺序。
    async fn version_repo(root: &Path) -> (PathBuf, PathBuf, Vec<String>) {
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

        let mut revs = Vec::new();
        let steps = [
            (Some("0.5.7"), "a.txt"),
            (None, "b.txt"),
            (Some("0.5.8"), "c.txt"),
        ];
        for (declared, touched) in steps {
            if let Some(version) = declared {
                std::fs::write(
                    work.join("Cargo.toml"),
                    format!("[workspace.package]\nversion = \"{version}\"\n"),
                )
                .unwrap();
            }
            std::fs::write(work.join(touched), format!("{touched}\n")).unwrap();
            real_git(&work, &["add", "."]).await;
            real_git(&work, &["commit", "-m", touched]).await;
            revs.push(real_git_stdout(&work, &["rev-parse", "HEAD"]).await);
        }
        real_git(&work, &["push", "origin", "HEAD:main"]).await;
        real_git(&work, &["checkout", "-B", "main"]).await;
        (bare, work, revs)
    }

    /// 在 bare 里打一个附注 release tag——真实 release 就是这个形状，读取要能
    /// 穿过 tag 对象拿到它指的提交。
    async fn tag_release(bare: &Path, tag: &str, rev: &str) {
        real_git(
            bare,
            &[
                "-c",
                "user.email=t@t.com",
                "-c",
                "user.name=T",
                "--git-dir",
                bare.to_str().unwrap(),
                "tag",
                "-a",
                tag,
                rev,
                "-m",
                tag,
            ],
        )
        .await;
    }

    /// 某个上报点（平台）本地存下来的 release tag 引用，形状与上游跟踪时
    /// 用的 refspec 一致。
    async fn tag_at_point(bare: &Path, platform: &str, tag: &str, rev: &str) {
        real_git(
            bare,
            &[
                "--git-dir",
                bare.to_str().unwrap(),
                "update-ref",
                &format!("refs/cogneva/tags/{platform}/{tag}"),
                rev,
            ],
        )
        .await;
    }

    fn version_deployer(root: &Path, bare: &Path) -> MainlineDeployer {
        MainlineDeployer::new(
            test_config(root, bare, "buildah", "kubectl"),
            test_workspaces(root, bare),
        )
    }

    /// 断言某条判据判出违规，且违规对象只有 `subject` 一个。
    fn assert_violation(verdict: &Verdict, subject: &str) {
        match verdict {
            Verdict::Violated(violations) => {
                assert_eq!(violations.len(), 1, "not one violation: {violations:?}");
                assert_eq!(violations[0].subject, subject);
            }
            other => panic!("expected a violation about {subject}, got {other:?}"),
        }
    }

    /// 契约成立时：声明链只含真正改了声明的提交，读数与判据一致。
    ///
    /// 判据与读数是两个出口：卡住「离 release 多远」会把正常前进判成违规，所以
    /// 距离只出现在读数里；这里同时断言两者，防止哪天有人把它挪回判据里。
    #[tokio::test]
    async fn the_version_contract_holds_when_every_release_is_declared_and_tagged() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, revs) = version_repo(root).await;
        tag_release(&bare, "v0.5.7", &revs[0]).await;
        tag_release(&bare, "v0.5.8", &revs[2]).await;

        let (evidence, readings) = version_deployer(root, &bare).version_evidence("main").await;

        assert!(evidence.chain_complete, "the bare repo was cloned in full");
        assert_eq!(
            evidence.declarations,
            vec![
                DeclarationChange {
                    rev: revs[0].clone(),
                    from: "0.5.7".into(),
                    to: "0.5.7".into(),
                },
                DeclarationChange {
                    rev: revs[2].clone(),
                    from: "0.5.7".into(),
                    to: "0.5.8".into(),
                },
            ],
            "the commit that touches no declaration must not appear in the chain"
        );
        assert_eq!(readings.declared.as_deref(), Some("0.5.8"));
        assert_eq!(readings.nearest_release, Some(("v0.5.8".to_string(), 0)));

        let report = judge(&evidence);
        for clause in [
            Clause::DeclarationMonotone,
            Clause::TagFidelity,
            Clause::ReleasePoint,
        ] {
            assert_eq!(
                *report.verdict(clause),
                Verdict::Satisfied,
                "{}",
                clause.as_str()
            );
        }
    }

    /// 一条 tag 指着的提交不是 tracked main 上的 —— 从分叉或已重写的历史里推
    /// 上来的 tag 就是这个形状。这是 tag 自己的事实，不是读数缺口，所以链读全
    /// 了之后它必须判违规，而不是读不到。
    #[tokio::test]
    async fn a_release_tag_outside_the_tracked_main_is_a_violation() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, revs) = version_repo(root).await;
        tag_release(&bare, "v0.5.8", &revs[2]).await;

        // 从 c2 拉一条侧支，声明更高的版本、打 tag、只推分支不并回 main。
        real_git(&work, &["checkout", "-b", "side", &revs[1]]).await;
        std::fs::write(
            work.join("Cargo.toml"),
            "[workspace.package]\nversion = \"0.5.9\"\n",
        )
        .unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "side"]).await;
        real_git(&work, &["push", "origin", "side"]).await;
        let side = real_git_stdout(&work, &["rev-parse", "HEAD"]).await;
        real_git(&work, &["checkout", "main"]).await;
        tag_release(&bare, "v0.5.9", &side).await;

        let (evidence, _) = version_deployer(root, &bare).version_evidence("main").await;

        let foreign = evidence
            .releases
            .iter()
            .find(|r| r.tag == "v0.5.9")
            .expect("the tag is visible in this repo");
        assert!(!foreign.on_tracked_main, "the side branch is not on main");
        assert!(evidence.chain_complete);
        assert_violation(judge(&evidence).verdict(Clause::TagFidelity), "v0.5.9");
    }

    /// 一个版本被打了两次 release —— 两个生产者各自「合入 0.5.8」，就是这个
    /// 形状。两条 tag 都忠实（各自指的提交都声明 0.5.8），所以只有 release 落
    /// 点这条判据该响：同一个版本名不能盖住两个代码状态。
    #[tokio::test]
    async fn two_release_tags_for_one_version_are_a_release_point_violation() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, revs) = version_repo(root).await;
        tag_release(&bare, "v0.5.8", &revs[2]).await;
        tag_release(&bare, "v0.5.8-hotfix", &revs[2]).await;

        let (evidence, _) = version_deployer(root, &bare).version_evidence("main").await;
        let report = judge(&evidence);

        assert_eq!(*report.verdict(Clause::TagFidelity), Verdict::Satisfied);
        assert_violation(report.verdict(Clause::ReleasePoint), "v0.5.8-hotfix");
    }

    /// 同一条 tag（指向 main 上的提交、名字里的版本这个历史从没声明过）在两种
    /// 证据下应当有两种结论：链读全了 = 违规，链被截断 = 读不到。
    ///
    /// 截断在这里是浅克隆造的，不是摆出来的——浅克隆的边界提交在「有没有父提
    /// 交」上和根提交给出同一个答案，正是它会把正常历史判成违规。
    #[tokio::test]
    async fn the_same_untag_reading_is_a_violation_only_when_the_chain_reaches_its_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, revs) = version_repo(root).await;
        tag_release(&bare, "v0.5.8", &revs[2]).await;
        tag_release(&bare, "v0.5.9", &revs[2]).await;

        let full = root.join("full.git");
        real_git(
            root,
            &[
                "clone",
                "--bare",
                "-b",
                "main",
                &format!("file://{}", bare.display()),
                full.to_str().unwrap(),
            ],
        )
        .await;
        // `--depth` 只在 file:// 传输下生效，本地路径克隆会静默地整份拷过来——
        // 那样这条用例就变成「两条都是全量」而永远成立。
        let shallow = root.join("shallow.git");
        real_git(
            root,
            &[
                "clone",
                "--bare",
                "-b",
                "main",
                "--depth",
                "1",
                &format!("file://{}", bare.display()),
                shallow.to_str().unwrap(),
            ],
        )
        .await;
        assert_eq!(
            real_git_stdout(
                root,
                &[
                    "--git-dir",
                    shallow.to_str().unwrap(),
                    "rev-parse",
                    "--is-shallow-repository"
                ]
            )
            .await,
            "true",
            "the depth-1 clone has to actually be shallow for this test to mean anything"
        );

        let (deep_evidence, _) = version_deployer(root, &full).version_evidence("main").await;
        let (cut_evidence, _) = version_deployer(root, &shallow)
            .version_evidence("main")
            .await;

        assert!(deep_evidence.chain_complete);
        assert!(!cut_evidence.chain_complete);
        // 判 tag 是否忠实只用被指提交自己那棵树，链断了也照样读得到——这一条
        // 在两种证据下都该响，正好证明两种结论的差别来自链，而不是判据缺席。
        assert_violation(judge(&deep_evidence).verdict(Clause::TagFidelity), "v0.5.9");
        assert_violation(judge(&cut_evidence).verdict(Clause::TagFidelity), "v0.5.9");
        assert_violation(
            judge(&deep_evidence).verdict(Clause::ReleasePoint),
            "v0.5.9",
        );
        assert!(
            matches!(
                judge(&cut_evidence).verdict(Clause::ReleasePoint),
                Verdict::Unreadable(_)
            ),
            "a truncated chain cannot tell 'never declared' from its own blind spot"
        );
    }

    /// 两个上报点看到的 release tag 必须一致。缺少 tag 的那个点不能靠「它也
    /// 没多出什么」蒙混过去：镜像没同步到 tag，正是发布通道静默失效的样子。
    #[tokio::test]
    async fn a_reporting_point_missing_a_release_tag_is_a_tag_set_violation() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, revs) = version_repo(root).await;
        for tag in ["v0.5.7", "v0.5.8"] {
            tag_at_point(&bare, "github", tag, &revs[2]).await;
        }
        tag_at_point(&bare, "gitee", "v0.5.7", &revs[2]).await;

        let deployer =
            MainlineDeployer::new(upstream_config(root, &bare), test_workspaces(root, &bare));
        let (evidence, _) = deployer.version_evidence("main").await;
        assert_eq!(
            evidence.tag_sets.len(),
            2,
            "both configured points have to be read: {:?}",
            evidence.tag_sets
        );
        // 违规对象是那条 tag，缺它的那个点写在详情里。
        let report = judge(&evidence);
        assert_violation(report.verdict(Clause::TagSetAgreement), "v0.5.8");
        let Verdict::Violated(violations) = report.verdict(Clause::TagSetAgreement) else {
            unreachable!("just asserted")
        };
        assert!(
            violations[0].detail.contains("gitee"),
            "the reading has to name the point that is missing the tag: {}",
            violations[0].detail
        );

        // 对照：把 gitee 补上同一个 tag，同一条判据必须转绿——否则上一条断言
        // 只是「这条判据总是响」，不成立
        tag_at_point(&bare, "gitee", "v0.5.8", &revs[2]).await;
        let (agreed, _) = deployer.version_evidence("main").await;
        assert_eq!(
            *judge(&agreed).verdict(Clause::TagSetAgreement),
            Verdict::Satisfied
        );
    }

    /// 判据的读数必须自成一路：每条判据一个 clause，判了几次与判出几次违规分
    /// 开记。0 与「没报」在读数上要能分开，所以没违规的判据也要有读数。
    #[tokio::test]
    async fn the_version_contract_reports_its_own_readings() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, revs) = version_repo(root).await;
        tag_release(&bare, "v0.5.7", &revs[0]).await;
        tag_release(&bare, "v0.5.8", &revs[2]).await;
        tag_release(&bare, "v0.5.9", &revs[2]).await;

        let metrics = std::sync::Arc::new(cog_storage::MemoryMetricsBackend::new());
        version_deployer(root, &bare)
            .with_metrics(metrics.clone())
            .report_version_contract("main")
            .await;

        let gauges = metrics
            .query_gauge_latest(VERSION_CONTRACT_VIOLATIONS.as_str())
            .await
            .unwrap();
        let expected = [
            (Clause::DeclarationMonotone, 0.0),
            (Clause::TagFidelity, 1.0),
            (Clause::ReleasePoint, 1.0),
            (Clause::TagSetAgreement, 0.0),
        ];
        for (clause, value) in expected {
            let sample = gauges
                .iter()
                .find(|s| s.labels.get("clause").map(String::as_str) == Some(clause.as_str()))
                .unwrap_or_else(|| panic!("no violations reading for {}", clause.as_str()));
            assert_eq!(sample.value, value, "{}", clause.as_str());
        }

        let counters = metrics
            .query_counter_totals(VERSION_CONTRACT_CHECKS_TOTAL.as_str())
            .await
            .unwrap();
        assert_eq!(counters.len(), Clause::ALL.len(), "one series per clause");
        let outcome = |clause: &str| {
            counters
                .iter()
                .find(|s| s.labels.get("clause").map(String::as_str) == Some(clause))
                .unwrap_or_else(|| panic!("no checks reading for {clause}"))
        };
        for (clause, expected_outcome) in [
            ("declaration_monotone", "satisfied"),
            ("tag_fidelity", "violated"),
            ("release_point", "violated"),
            ("tag_set_agreement", "unreadable"),
        ] {
            let sample = outcome(clause);
            assert_eq!(sample.value, 1.0, "{clause}");
            assert_eq!(
                sample.labels.get("outcome").map(String::as_str),
                Some(expected_outcome),
                "{clause}"
            );
        }

        // 读数：最近一次 release 的距离与 main 声明的版本。距离不进判据，只在这
        // 里出现；两个 tag 同距离时取 ref 顺序在前的那个（git 保证顺序）。
        let gauges = metrics
            .query_gauge_latest(VERSION_COMMITS_SINCE_RELEASE.as_str())
            .await
            .unwrap();
        assert_eq!(gauges.len(), 1);
        assert_eq!(
            gauges[0].labels.get("release").map(String::as_str),
            Some("v0.5.8")
        );
        assert_eq!(gauges[0].value, 0.0);

        let declared = metrics
            .query_gauge_latest(VERSION_DECLARED_INFO.as_str())
            .await
            .unwrap();
        assert_eq!(declared.len(), 1);
        assert_eq!(
            declared[0].labels.get("version").map(String::as_str),
            Some("0.5.8")
        );
        assert_eq!(declared[0].value, 1.0);
    }
}
