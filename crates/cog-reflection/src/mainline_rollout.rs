//! 滚动侧（Job 内运行的那一半）：把一份已经发好的清单滚上集群，盯到收敛，失败就回退。
//!
//! 与 [`crate::mainline_deployer`] 的分界是**运行实体**，不是时序，也不是体积：那一半
//! 跑在进化 Pod 里（检测 main 前进、构建镜像、组装清单包、派 Job），这一半跑在 Job 自己
//! 的进程里（子命令 `cogneva mainline-rollout`）。两边不共进程——这一侧的失败在那一侧的
//! 进程里完全看不见，没有任何一个构建、一次测试或一个读者会同时看到两边，这才是拆的理由。
//!
//! 这里放这一侧**独占**的东西：执行器本体，以及只被它读到的诊断解析与判据。两边共用的
//! 类型与纯函数仍留在 `mainline_deployer`，由这里单向引用；反向引用为零，所以这一侧整体
//! 搬走时没有改动那一侧的任何一行语义。
//!
//! **可见性说明**：执行器内部的方法与几个诊断类型的字段在本模块里是 `pub(crate)`，因为
//! 这一侧的用例仍住在 `mainline_deployer::tests` 里（那是搬迁前的唯一一份用例，逐条按
//! 「跑起来是什么样」写，与那一侧的用例共用夹具，拆不开）。`pub(crate)` 只服务那一面，
//! 产出面两边仍然互不调用：`mainline_deployer` 的产出代码只走执行器公开的那几个入口。
//!
//! 清单随镜像走解决的是拓扑滞后：只 `set image` 时，chart 里与工作负载一起演进的
//! ConfigMap/Service/PVC 变更永远到不了集群（没人 apply 新清单），新二进制配旧拓扑跑。
//! 清单包只含命名空间级的非权限面资源：Secret 硬性拒绝（零带外凭证红线）；集群级 kind 与
//! Role/RoleBinding 跳过——K8s 反提权规定 apply 一个 Role 要求发起者已持有其中全部权限，
//! 部署面永远不该持有发布集里管理类 Role 的权限，给自己发 bind/escalate 又等于自治系统
//! 自我提权，所以权限边界变更必须走安装面/人工核准。
//!
//! 滚动必须放 Job 而不是进化 Pod 自身：cogneva-evolution 是 Recreate 单副本，部署器就跑在
//! 里面，对自己 `set image` 会立刻杀掉门禁/回滚逻辑，新镜像 crashloop 时无人 undo，进化面
//! 永久宕。Job 用新镜像跑还顺带 smoke test：新二进制起不来则一次 `set image` 都不会发生。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use cog_core::{SFError, SFResult};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::config::{MainlineDeployerConfig, RolloutTargetConfig};
use crate::mainline_deployer::{
    bounded, carries_deployment, config_consumer_jsonpath, exit_code, failure_signature, field,
    is_admission_policy_denied, is_authorization_denied, is_cluster_unreachable,
    is_observation_tool_failure, namespace_docs, primary_message, render_docs, sample_fields,
    split_docs, target_manifest_key, workload_kind, workload_kind_of_resource, CiNoVerdictReason,
    ConfigConsumer, FailureClass, FailureLocus, MainlineDeployer, PodSample, RolloutTarget,
    WaitingState, WorkloadKind, CLUSTER_UNREACHABLE_MARKER, FATAL_WAITING_REASONS,
    IMAGE_SOURCE_UNAVAILABLE_MARKER, OBSERVATION_TOOL_MARKER, ROLLOUT_EXIT_ENVIRONMENT,
    ROLLOUT_PRECHECK_STAGE, SUPPORT_CAUSE_MESSAGE_CHARS, SUPPORT_CAUSE_SEPARATOR, WORKLOAD_KINDS,
};
use crate::rollout_resources::{self, RolloutResourceReading};

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
pub(crate) const IMAGE_PULL_WAITING_REASONS: &[&str] = &["ErrImagePull", "ImagePullBackOff"];

/// 「落地之前复问一次上游 CI」这一段的墙钟上界（秒）。
///
/// 两头都要用它：这一侧拿它包住整段提问（超时按「没问到」扣住这一轮），派发侧把它
/// 加进 Job 的 `activeDeadlineSeconds`——只封一头，另一头的界就名不副实。
pub(crate) const CI_ASK_BUDGET_SECS: u64 = 180;

/// 上游答「还在跑」时，这一问愿意等多久（秒）再扣住这一轮。
///
/// 派 Job 到 Job 里问这一句只隔着几十秒，而一次 CI 从推上去到出结论要十几分钟：
/// 2026-10-27 实测，Job 在推上去 11 分钟时问，GitHub 答 `still running`，这一轮
/// 扣住、这一版被判成环境失败、主线接着去追下一版；下一版又是同样的十一分钟。
/// 落地节奏快过 CI 判定，于是每一版都死在「还在跑」这一格上，主线三个多小时没有
/// 任何东西上去过。
///
/// 「还在跑」与「没问到」必须分开处置：后者是路不通，等也没用；前者是判词还在
/// 生产，再问一句就有。只等这一种，且等到有上界为止——无限等会让 Job 的期限名不
/// 副实，到点被杀还走不到 Job 自己的回滚。
///
/// 上界要盖住**上游一次完整判定**，不是盖住「第一次答还在跑」：等待从第一次提问
/// 起算，而那时判词还没开始存在，CI 从起跑到最后一条结论的整段都落在等待里。同一
/// 夜连续十版实测这一段是 11.2 / 11.9 / 11.9 / 12.6 / 13.4 / 13.9 / 15.3 / 16.0 /
/// 16.0 / 16.3 分钟（最长 16.3），再加派发侧 30 秒到 10 分钟的滞后——900 秒的界
/// 正好横在中间，镜像构建快的那一版必然在判词出来之前被扣住，而扣住一次这一版就
/// 不再被滚，「界比它等的东西短」于是自己成了停摆的成因。
///
/// 30 分钟是这段实测最长值的 1.8 倍：盖得住最长的一次判定加派发滞后，又不至于让
/// 一条真卡死的流水线把每一轮占住半小时——那种情况下游也是卡死的，多等不额外损失
/// 什么，而每一轮都还能在界内走完自己的回滚。
///
/// 派发侧还有一条：正在滚的那一版**不会**被新提交顶掉（`in_flight` 只在结束后才
/// 前进），所以这段等待等到的结论一定会被用上，不是白等。
pub(crate) const CI_PENDING_WAIT_SECS: u64 = 1800;

/// 等在里面的复问间隔（秒）。一次 CI 判定的粒度是分钟级，问得比这密只是把同一个
/// 答案多读几遍。
pub(crate) const CI_PENDING_REASK_SECS: u64 = 60;

/// 「还在跑」等到了界时，这一轮的判词。
///
/// 判词必须带上「等了多久」与「界是多少」：没有这两个数，「等到界也没等到」与
/// 「问了一句还在跑」在 `state.json` 里是同一句话，而两者下一次要做的事不一样——
/// 前者要么把界放宽、要么去看流水线为什么这么久，后者什么都不用做。判词是唯一会
/// 被下一轮读到的东西（这一轮的日志不会被任何消费者读），所以读数写在这里。
pub(crate) fn wait_exhausted_reason(waited: Duration, why: &str) -> String {
    format!(
        "{why} -- asked again for {}s, the whole {}s this round is allowed to wait, and the \
         answer never finished on its own; the wait itself is the verdict",
        waited.as_secs(),
        CI_PENDING_WAIT_SECS
    )
}

/// 「还在跑」这一答到手之后，这一轮还该不该再问一次。
///
/// 判红的版本等人改代码，没问到的版本等路通，只有「还在跑」是自己会走完的：它说的
/// 是「判词还没产生」，不是「这一版有问题」。所以只有它值得等，且只在
/// [`CI_PENDING_WAIT_SECS`] 之内等——过了界，判词还没来这件事本身就成了结论。
pub(crate) fn still_worth_asking(kind: CiHoldKind, waited: Duration) -> bool {
    kind == CiHoldKind::Pending && waited < Duration::from_secs(CI_PENDING_WAIT_SECS)
}

/// The workload kinds this rollout waits for after the apply: every kind whose
/// pods are long-lived, i.e. every kind a restarted pod leaves serving nothing
/// until the replacement is ready.
///
/// A DaemonSet is in the list and is read by its own pair. It declares no
/// `spec.replicas` — the node count is the controller's to work out — so the
/// replica reading would take the absent field's default of one and compare it
/// against a `readyReplicas` that does not exist either, a comparison that says
/// nothing about any DaemonSet. Reading it as
/// `desiredNumberScheduled`/`numberReady`/`updatedNumberScheduled` is the same
/// question asked in the vocabulary that kind actually speaks.
///
/// Leaving it out was not neutral: 2026-10-26 the rollout reported `Complete`
/// at 20:52:00Z while `daemonset/cogneva-claim-store` — restarted by that same
/// apply — was `desired 1, ready 0`, and it did not answer until 20:53:14Z.
/// Nothing in the release said so, because nothing was reading it.
///
/// CronJob is the one kind left out and stays out: its pods are created per run
/// and vanish, so "wait for the pod that replaces the one I restarted" has no
/// meaning there.
///
/// The rollout targets are Deployments of their own and are waited for one by
/// one; they are not covered here either.
///
/// The words come from [`workload_resource`] rather than being spelled here:
/// this list is read and patched through kubectl, and a face that spelled its
/// own word would name the same object twice under two spellings.
pub(crate) const SUPPORT_WORKLOAD_KINDS: &[&str] = &[
    workload_resource("Deployment"),
    workload_resource("StatefulSet"),
    workload_resource("DaemonSet"),
];

/// 支撑工作负载"这次滚动可以往下走了"的读法：代数、观测到的代数、期望副本数、
/// 就绪副本数、新版本副本数、现有副本总数。竖线显式占位，缺字段（omitempty）不能
/// 顶掉后面的位置。
///
/// 后两个字段是这一条判据的关键，不是陪衬：光看"期望几个、就绪几个"读不出
/// "旧 Pod 正在被换掉"这件事（见 [`support_settled`]）。
const SUPPORT_SETTLE_JSONPATH: &str = "jsonpath={.metadata.generation}|{.status.observedGeneration}|{.spec.replicas}|{.status.readyReplicas}|{.status.updatedReplicas}|{.status.replicas}";

/// DaemonSet 的同一问，用它自己的词表：期望节点数、就绪 Pod 数、已换到新版本的
/// 节点数。它没有 `spec.replicas`，也没有 `status.readyReplicas`——拿上面那份读法
/// 去读它，读回来的是两个不存在的字段，而缺字段的默认值（1 与 0）恰好构成一句
/// 「还没就绪」，于是等待要么永远不成立、要么（缺省按 1 读到 zeros 时）什么都不等。
const DAEMONSET_SETTLE_JSONPATH: &str = "jsonpath={.metadata.generation}|{.status.observedGeneration}|{.status.desiredNumberScheduled}|{.status.numberReady}|{.status.updatedNumberScheduled}";

/// 四个目标部署自己的读数：代数、观测到的代数、期望副本数、新版本副本数、
/// 就绪副本数、不可用副本数。
///
/// 竖线显式占位：缺失字段（omitempty 的 `updatedReplicas` 等）不能顶掉后续字段
/// 的位置。顺序就是 [`rollout_converged`] 那一族按下标读的顺序——查询串在读取端
/// 之外，两边分开写，所以挪字段不会报错，只会在谁也看不见的地方换掉判据读的是
/// 哪一格；把顺序钉住的是 `the_line_readings_walk_their_fields_in_the_order_their_readers_do`。
pub(crate) const ROLLOUT_STATUS_JSONPATH: &str = "jsonpath={.metadata.generation}|{.status.observedGeneration}|{.spec.replicas}|{.status.updatedReplicas}|{.status.readyReplicas}|{.status.unavailableReplicas}";

/// 这个工作负载该用哪份读法。词表只有一处：资源词进 [`workload_kind_of_resource`]
/// 换出清单 kind，再由清单 kind 选读法，不在这里手写第二张对照表。
pub(crate) fn settle_jsonpath(kind: &str) -> &'static str {
    match workload_kind_of_resource(kind).map(|k| k.kind) {
        Some("DaemonSet") => DAEMONSET_SETTLE_JSONPATH,
        _ => SUPPORT_SETTLE_JSONPATH,
    }
}

/// 现场读数带病因，但长度不能由集群里的内容决定：它进日志、进终止消息、进失败
/// 记录。Pod 数、每个 Pod 的容器数、单条消息的字符数各设一个界，超界显式写出来。
pub(crate) const SUPPORT_CAUSE_POD_LIMIT: usize = 3;

const SUPPORT_CAUSE_CONTAINER_LIMIT: usize = 4;

/// 滚动内部轮询间隔：探测、致命态复查、部署态查询共用同一节拍。
const ROLLOUT_POLL_SECS: u64 = 5;

/// 超时诊断里保留的事件条数。错误记录不是日志转储：只要够指出病因。
const DIAGNOSIS_EVENT_LIMIT: usize = 3;

/// 就绪门禁在探针判定周期之外额外给出的余量：镜像已在节点上时，容器从起进程
/// 到开始监听这一段，kubelet 的探针一次都还没判过，所以它不在探针预算里。
/// 集群实证（网关 Pod）：起容器到 1/1 相隔 42 秒，而按它当时的探针配置算出的
/// 判定周期是 38 秒，差值约 4 秒——这就是起进程到监听的实际开销；这里取 15 秒
/// 是给这一段留的宽裕上界。预算的另一半从活的探针配置推（见 [`ProbeCycle`]），
/// 所以换一套探针只动那一半，这个数不跟着变。
const CONTAINER_STARTUP_ALLOWANCE_SECS: u64 = 15;

/// 就绪探针自己的四个旋钮。门禁的预算从这里推，不拍固定秒数：探针调周期，
/// 预算跟着动；判据的宽度窄于 kubelet 自己的判定周期，就会重演「比探针更早
/// 开枪、把一个正在起来的版本判死」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbeCycle {
    pub(crate) initial_delay: u64,
    pub(crate) period: u64,
    pub(crate) timeout: u64,
    pub(crate) failure_threshold: u64,
}

impl ProbeCycle {
    /// 缺格兜底值取 k8s 默认探针的同值（initialDelay 默认 0）。它只在采样行
    /// 确实拿到了、但某些格为空时逐格生效：一个部署没有就绪探针时四格全空，
    /// 而 running 即 ready 的容器本来就不存在"探针还没通过"的窗口。
    pub(crate) const DEFAULT: Self = Self {
        initial_delay: 0,
        period: 10,
        timeout: 1,
        failure_threshold: 3,
    };

    /// 解析采样行 `initialDelay|period|timeout|failureThreshold`。逐格兜底：
    /// 探针只写了部分字段（或该部署根本没有就绪探针）时，缺的那格用默认值，
    /// 不整条丢弃——丢掉一整格会让预算突然缩回默认值。
    pub(crate) fn parse(out: &str) -> Self {
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
    pub(crate) fn budget(&self) -> Duration {
        let judgements = self
            .failure_threshold
            .saturating_mul(self.period.saturating_add(self.timeout));
        let cycle = self.initial_delay.saturating_add(judgements);
        Duration::from_secs(cycle.saturating_add(CONTAINER_STARTUP_ALLOWANCE_SECS))
    }
}

/// 解析 Pod 采样输出。字段残缺的半行整条丢弃。
pub(crate) fn pod_samples(out: &str) -> Vec<PodSample> {
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
            last_message: f.get(7).unwrap_or(&"").to_string(),
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
pub(crate) fn summarize_pod_states(samples: &[PodSample], now: DateTime<Utc>) -> String {
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
pub(crate) fn rollout_pods_ready(samples: &[PodSample]) -> Option<bool> {
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
pub(crate) fn rollout_pods_fatal(samples: &[PodSample]) -> Option<String> {
    samples
        .iter()
        .filter(|p| !p.terminating)
        .find(|p| FATAL_WAITING_REASONS.contains(&p.waiting_reason.as_str()))
        .map(|p| {
            let mut text = format!("{} waiting={}", p.name, p.waiting_reason);
            // 判死只读到「在退避」。容器自己写下的那一句才是死因，接在分隔符之后
            // ——那是集群来的自由文本，判据不许读它（见 `primary_message`）。
            if !p.last_message.is_empty() {
                text.push_str(SUPPORT_CAUSE_SEPARATOR);
                text.push_str(&bounded(&p.last_message, SUPPORT_CAUSE_MESSAGE_CHARS));
            }
            text
        })
}

/// 解析 `原因|死因` 这样的逐行读数。原因取自闭集、永不含 `|`，所以按**第一个**
/// `|` 切分即可；死因是自由文本，里面出现 `|` 不影响切分。死因自带换行时多出来的
/// 那些行没有原因字段，对闭集的匹配自然落空（两个判据都是整词比较）。
pub(crate) fn pod_waiting_states(out: &str) -> Vec<WaitingState> {
    out.lines()
        .filter_map(|line| {
            let (reason, cause) = line.split_once('|').unwrap_or((line, ""));
            let reason = reason.trim();
            if reason.is_empty() {
                return None;
            }
            Some(WaitingState {
                reason: reason.to_string(),
                cause: cause.trim().to_string(),
            })
        })
        .collect()
}

/// 这批等待态里有没有「拉取失败」。init 容器与主容器一起看：init 拉不到镜像时
/// 主容器只报 PodInitializing，只看主容器会把这一档整个漏掉。
///
/// 返回命中的那个原因本身（而不是布尔）：判词里要写出是哪一个等待态，人才知道
/// kubelet 停在哪一步。
pub(crate) fn image_pull_blocked(states: &[WaitingState]) -> Option<&str> {
    states
        .iter()
        .map(|s| s.reason.as_str())
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
pub(crate) fn readiness_overdue(
    samples: &[PodSample],
    budget: Duration,
    now: DateTime<Utc>,
) -> bool {
    let Some(anchor) = readiness_anchor(samples) else {
        return false;
    };
    now.signed_duration_since(anchor)
        .to_std()
        .map(|ran| ran >= budget)
        .unwrap_or(false)
}

/// 判词里附着的现场采样，接在分隔符**之后**。
///
/// 这一段是集群写的：Pod 的相位、事件原文、容器留下的终止消息。判据只读分隔符
/// 之前那一段（见 [`primary_message`]），所以它必须整个待在后面——接在前面时，
/// 新 Pod 探针报的那句 `connection refused`（它只是应用还没开始监听）会被
/// [`is_cluster_unreachable`] 读成"我们看不到集群"，于是这一版被归成环境类：
/// 不回滚、不记账、无限重试。判词本身仍带着现场，剥掉的只是参与判据的那一段。
pub(crate) fn scene_suffix(diagnosis: &str) -> String {
    if diagnosis.is_empty() {
        return String::new();
    }
    let mut text = String::from(SUPPORT_CAUSE_SEPARATOR);
    text.push_str("pods: ");
    text.push_str(diagnosis);
    text
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
pub(crate) fn summarize_events(
    out: &str,
    deployment: &str,
    pod_names: &[String],
    limit: usize,
) -> String {
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

/// 「集群放不下新 Pod」的标记，与 cluster-unreachable 同一类：它说的是**这次
/// 滚动没有败在版本上**。判据来自调度器自己的类型化判决——新 Pod 的
/// `PodScheduled=False / Unschedulable`，不是从散文里猜的措辞。
///
/// 排不上队时新 Pod 一个都没起来，谈不上新版本的好坏：回滚要把旧镜像重新
/// 调度一遍，而它带着同样的放置面，同样排不进去，只会多一轮 churn 并把
/// 一个可能正常的版本换掉。真正的边界在版本**改没改放置面**上——改了
/// （这次上线把 requests 调大、加了排不上的 nodeSelector）就是版本的事，该
/// 回滚；没改就与版本无关。
pub(crate) const PLACEMENT_BLOCKED_MARKER: &str = "placement blocked";

pub(crate) fn is_placement_blocked(msg: &str) -> bool {
    msg.contains(PLACEMENT_BLOCKED_MARKER)
}

/// 判定进程给「准入面把新副本挡在建 Pod 之前」打的标记，与调度器判决的
/// PLACEMENT_BLOCKED 互斥：前者 Pod 对象压根没被创建，后者 Pod 建了但排不上队。
///
/// 与上面那组措辞刻意分成两条判据。超时记录里除了标记还附着一份给人看的现场采样
/// （ReplicaSet 的 `FailedCreate` 消息），措辞与上游原文完全同形；若用措辞做第二
/// 遍判据，「这次上线自己把 requests 调过了配额」那一支也会被读成环境类——而环境
/// 类不占尝试预算，一个真坏的版本会就此无限重试下去。
pub(crate) const ADMISSION_DENIED_MARKER: &str = "admission policy denied";

pub(crate) fn is_admission_denied(msg: &str) -> bool {
    msg.contains(ADMISSION_DENIED_MARKER)
}

pub(crate) fn is_image_source_unavailable(msg: &str) -> bool {
    msg.contains(IMAGE_SOURCE_UNAVAILABLE_MARKER)
}

/// 整段预算里新版本**一个 Pod 都没建成**（`updatedReplicas` 一直是 0）。
///
/// 这不是版本结论：控制器建 Pod 发生在新版本的任何容器跑起来之前，所以这一档
/// 比"新副本卡在拉取上"还要早一步——那时候至少有个 Pod 可以看它的等待原因，
/// 这里连 Pod 都没有。真正能在这里发生的只有控制面自己的事：旧副本还没退场
/// （`maxSurge:0` 下控制器要等它先消失）、控制器自己还没轮到。判成版本类会把
/// 一次"还没轮到"记成"这一版坏了"，并触发一次没有任何证据支持的回滚。
///
/// 措辞只说"没建成"，不说为什么——为什么在这条判定里读不到，编一个原因是
/// 把猜测写成读数。
pub(crate) const NEW_REVISION_NEVER_RAN_MARKER: &str =
    "no pod of the new revision was ever created";

fn is_new_revision_never_ran(msg: &str) -> bool {
    msg.contains(NEW_REVISION_NEVER_RAN_MARKER)
}

/// 整段预算里这台宿主的运行队列一直比它本身宽：那个正在启动的 Pod 拿到的是排队，
/// 不是 CPU。
///
/// 与上面几条同族：它说的不是这一版的好坏。容器起来之后，启动是一件纯计算的活
/// （读模型、建索引、跑迁移、起插件），而机器上还有别的东西在跑；预算按墙上时间
/// 记，宿主不给时间，预算就只是在测别人的负载。
///
/// 2026-10-28 那次滚动的读数：24 核宿主，apply 那一刻 load1 21.9（还没越界），
/// 45 秒后 98，再一分半 140，判死那一刻 161；CPU 95% 忙、dm-0 98.8% 占用、
/// PSI io some 54%。那一版的新 Pod 整段只拿到 0.01–0.09 核、CFS 限流 0.0%
/// （压它的东西在 k8s 的 cgroup 之外）、工作集三分钟一格没涨、端口三百秒没开。
/// 同一张镜像四十分钟前在 load1 15 / CPU 51% 的宿主上启动只用了 31 秒；同一个
/// 滚动后来在 load1 一直低于核数的一小时里从调度到 Ready 用了 207 秒。判成版本类
/// 就是把这台机器的负载记成了代码缺陷，并回滚一份本来会自己起来的版本。
pub(crate) const HOST_LOADED_MARKER: &str = "the host's run queue stayed wider than its core count";

pub(crate) fn is_host_loaded(msg: &str) -> bool {
    msg.contains(HOST_LOADED_MARKER)
}

/// 宿主这一刻的运行队列：`load1` 与核数。
///
/// 两处读的都是 `/proc`，而 `/proc/loadavg` 与 `/proc/cpuinfo` 不随命名空间切片
/// （被切开的只有 PID 号与 cgroup 视图），所以判定 Job 里读到的是**整机**在排多少
/// 活。这正是要的读数：被压住的不是判定进程，是那个正在启动的 Pod，而它分不分得到
/// CPU 取决于整机有多少东西在抢——那件事在它的 cgroup 里读不到（实测它自己的 CFS
/// 限流是 0%，它不是被自己的配额压住的）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct HostRunQueue {
    /// 一分钟平均的可运行任务数（含不可中断睡眠，即等 I/O 的那些）。
    pub load1: f64,
    /// 宿主的 CPU 数。
    pub cores: f64,
}

impl HostRunQueue {
    /// 队列比机器宽，就说明有活在等：一个 Pod 的启动在这个窗口里拿到的是排队时间。
    ///
    /// 界取「每核一个」是因为它就是这台机器的宽度本身——再引一个可调的数，就成了
    /// 第二个要有人去校准的旋钮，而这个判据要回答的只是「有没有东西在等」。
    pub(crate) fn is_loaded(self) -> bool {
        self.load1 > self.cores
    }
}

pub(crate) const LOADAVG_PATH: &str = "/proc/loadavg";
pub(crate) const CPUINFO_PATH: &str = "/proc/cpuinfo";

/// `/proc/loadavg` 的第一列。
fn load1_of(text: &str) -> Option<f64> {
    text.split_whitespace().next()?.parse().ok()
}

/// `/proc/cpuinfo` 里 `processor` 的行数。
fn core_count_of(text: &str) -> Option<f64> {
    let cores = text
        .lines()
        .filter(|l| l.trim_start().starts_with("processor"))
        .count();
    (cores > 0).then_some(cores as f64)
}

/// 读一次宿主的运行队列。读不到就是读不到（返回 `None`），不折成 0：一个读不到的
/// 读数与一台空闲的机器在判据上必须分得开，否则一次读失败会把「没有证据」写成
/// 「宿主很空」。
pub(crate) fn read_host_run_queue(loadavg: &Path, cpuinfo: &Path) -> Option<HostRunQueue> {
    let load1 = load1_of(&std::fs::read_to_string(loadavg).ok()?)?;
    let cores = core_count_of(&std::fs::read_to_string(cpuinfo).ok()?)?;
    Some(HostRunQueue { load1, cores })
}

/// 进程里那一口：读这台机器的两个 `/proc` 文件。
fn read_host_run_queue_now() -> Option<HostRunQueue> {
    read_host_run_queue(Path::new(LOADAVG_PATH), Path::new(CPUINFO_PATH))
}

/// 用例里的默认读数：没有读数。判据读不到宿主就不改判，所以用例停在这条
/// 旧路径上，用哪一份宿主读数由用例自己指定。
fn no_host_reading() -> Option<HostRunQueue> {
    None
}

/// 「这个进程手里一份集群配置都没有」的标记。
///
/// 与 [`CLUSTER_UNREACHABLE_MARKER`] 分成两条判据：那一条说的是**集群那边**此刻
/// 不可达（apiserver 抖动、握手超时），按轮询节拍在预算内重试是真有可能等到的；
/// 这一条说的是本进程拿不出任何凭据——kubectl 于是回落到自己的默认端点
/// `http://localhost:8080`，每一次查询都在本机回环上被拒。重试换不来凭据：一次
/// 300 秒的预算里它会跑几十次，把一次诊断挤成一段时间，而判词写的是「集群不可达」
/// ——读到它的人去看 apiserver，真相在 Pod 自己的凭据面上。
///
/// 判据不靠 kubectl 的措辞：配置根本没读进来、配置齐了而集群没通、配置指向的就是
/// 本机回环，三种情形在它的原文里同形（都带 `connection refused`）。所以本进程在
/// 第一次查询之前自己把四处来源看一遍，全无时就地判死；其余情形照常往下走，而一旦
/// 回落到 `localhost:8080`，那四处来源的读数会附在失败的判词里。
///
/// 那份读数不是装饰。2026-10-25 两趟滚动各烧掉三百秒之后，判词里第一次写明「四处
/// 来源都在、令牌也读得到」，嫌疑才从凭据面挪开，找到真正的原因是调用时多带的一个
/// 旗标（见 `RolloutExecutor::run_kubectl` 的注释）。没有那行读数，同一个问题只能
/// 靠再进一次 Pod 去猜，而那时 Pod 已经不在了。
pub(crate) const CLUSTER_CONFIG_MISSING_MARKER: &str = "no cluster config in this process";

pub(crate) fn is_cluster_config_missing(msg: &str) -> bool {
    msg.contains(CLUSTER_CONFIG_MISSING_MARKER)
}

/// kubectl 一份配置都没读到时的默认端点。只用来决定要不要把配置读数附上判词，
/// 不用来改类别：真有 kubeconfig 指向 `127.0.0.1:8080`（本地 preload 那一档）时
/// 同一行也会这么写，而那是"端点没起"而不是"没有配置"；命令行多带了东西让它不读
/// 配置时更会这么写，而那时配置明明都在。
///
/// 只认 `host:port` 不认带 scheme 的那一整串：kubectl 一次失败会写好几行，请求那
/// 行带 `http://`，而最后那句总结（`The connection to the server localhost:8080 was
/// refused`）不带——按整串比对会漏掉真实的那一半。
const LOCALHOST_FALLBACK_ADDRESS: &str = "localhost:8080";

/// k3s 编译出来的 kubectl 会读的那份 kubeconfig。它只对 k3s 那一版二进制有效，
/// 所以它出现在这里只会让检查**更宽**（少判一次死），不会把一个能用的进程判死。
pub(crate) const K3S_KUBECONFIG: &str = "/etc/rancher/k3s/k3s.yaml";

/// 投影进 Pod 的服务账号令牌：in-cluster 配置的另一半（另一半是环境变量里的端点）。
pub(crate) const SERVICE_ACCOUNT_TOKEN: &str = cog_core::topology::SERVICE_ACCOUNT_TOKEN_FILE;

/// 这个进程能不能指望 `kubectl` 读到集群：kubeconfig 的三处来源与 in-cluster 的
/// 两件套，一处都没有时谁也别怪集群。
///
/// 字段是"看到的样子"，判据是纯函数（[`Self::check`]），真值表因此可以在不碰进程
/// 环境的前提下逐行断言——这份检查一旦误判就会把一个好好的滚动判死，所以它自己
/// 必须能被测出每一行。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ClusterConfigSources {
    /// `KUBECONFIG` 指向的、确实存在的文件（可以多个，冒号分隔）。
    pub kubeconfig_files: Vec<String>,
    /// `$HOME/.kube/config` 在不在。
    pub home_config: bool,
    /// k3s 自带的那份 kubeconfig 在不在。
    pub k3s_config: bool,
    /// `KUBERNETES_SERVICE_HOST` / `_PORT`：in-cluster 的端点。
    pub in_cluster_host: Option<String>,
    pub in_cluster_port: Option<String>,
    /// 投影进来的服务账号令牌在不在。
    pub service_account_token: bool,
    /// `$HOME` 本身；读不到时用它解释"为什么没查那一条"。
    pub home: Option<String>,
}

impl ClusterConfigSources {
    /// 读这个进程与它所在的 Pod 此刻的样子。只读，不改任何东西。
    pub(crate) fn observe() -> Self {
        let env = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let home = env("HOME");
        let kubeconfig_files = env("KUBECONFIG")
            .map(|v| {
                v.split(':')
                    .map(str::trim)
                    .filter(|p| !p.is_empty() && Path::new(p).is_file())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            kubeconfig_files,
            home_config: home
                .as_deref()
                .map(|h| Path::new(h).join(".kube/config").is_file())
                .unwrap_or(false),
            k3s_config: Path::new(K3S_KUBECONFIG).is_file(),
            in_cluster_host: env("KUBERNETES_SERVICE_HOST"),
            in_cluster_port: env("KUBERNETES_SERVICE_PORT"),
            service_account_token: Path::new(SERVICE_ACCOUNT_TOKEN).is_file(),
            home,
        }
    }

    /// `Ok` = 至少有一条路能让 kubectl 读到集群；`Err` = 四处全无，一行说清缺哪。
    pub(crate) fn check(&self) -> Result<(), String> {
        let kubeconfig = !self.kubeconfig_files.is_empty() || self.home_config || self.k3s_config;
        let in_cluster = self.in_cluster_host.is_some()
            && self.in_cluster_port.is_some()
            && self.service_account_token;
        if kubeconfig || in_cluster {
            return Ok(());
        }
        Err(format!(
            "{CLUSTER_CONFIG_MISSING_MARKER}: kubectl would fall back to \
             {LOCALHOST_FALLBACK_ADDRESS} and every query would be refused there. {}",
            self.describe()
        ))
    }

    /// 四处来源各是什么样子，一句话。判词里带上它，读的人就不必再进一次 Pod 才能
    /// 分清"没有配置"与"配置齐了而集群没通"——两者在 kubectl 的原文里同形。
    pub(crate) fn describe(&self) -> String {
        let kubeconfig = if self.kubeconfig_files.is_empty() {
            "KUBECONFIG unset or pointing at no existing file".to_string()
        } else {
            format!("KUBECONFIG → {}", self.kubeconfig_files.join(", "))
        };
        let home = match &self.home {
            Some(h) => format!(
                "{h}/.kube/config {}",
                if self.home_config {
                    "present"
                } else {
                    "missing"
                }
            ),
            None => "HOME unset so $HOME/.kube/config was not looked for".to_string(),
        };
        format!(
            "{kubeconfig}; {home}; {K3S_KUBECONFIG} {}; \
             KUBERNETES_SERVICE_HOST {}, KUBERNETES_SERVICE_PORT {}, \
             {SERVICE_ACCOUNT_TOKEN} {}",
            if self.k3s_config {
                "present"
            } else {
                "missing"
            },
            self.in_cluster_host.as_deref().unwrap_or("unset"),
            self.in_cluster_port.as_deref().unwrap_or("unset"),
            if self.service_account_token {
                "readable"
            } else {
                "missing"
            },
        )
    }
}

/// 镜像一次都还没动时的落点。快照阶段只有这几类非版本失败会出现——调度器判决
/// 得等新 Pod 出现，这里还没有新 Pod。
pub(crate) fn locate_before_any_change(msg: &str) -> FailureLocus {
    let msg = primary_message(msg);
    // 配置缺失排在不可达之前：它自己的措辞里也可能带着 `connection refused`
    // （判词是 us 拼的，但我们同时把 kubectl 原文附在后面），而两者修法不同。
    if is_field_removal_not_taken(msg) {
        // 排在不可达之前：这条判词是我们自己拼的，后面附着的 apply 原文里可能
        // 带任何东西，但它说的是「这次删除没写进去」，修法完全不同。
        FailureLocus::FieldRemoval
    } else if is_cluster_config_missing(msg) {
        FailureLocus::ClusterConfig
    } else if is_cluster_unreachable(msg) {
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
pub(crate) fn classify_before_any_change(stage: &str, target: &str, e: SFError) -> RolloutFailure {
    let locus = locate_before_any_change(&e.to_string());
    RolloutFailure::at(stage, target, locus, true, e)
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
    /// 提前扣住这一轮时，是哪个维度装不下（[`AdmissionShortfall::dimension`]）。
    ///
    /// 只有准入预检那一处填，其余失败为 `None`。签名说「坏在 precheck 的准入这一处」，
    /// 说不出「差在哪一维」，而 CPU 与内存的两条短差修法不同。它经终止消息过 Job 边界，
    /// 部署器读回来记成计数。
    pub admission_hold: Option<&'static str>,
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
            admission_hold: None,
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
pub(crate) fn normalize_placement_shape(raw: &str) -> String {
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
pub(crate) fn environment_class(
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
pub(crate) fn unschedulable_pods(out: &str) -> Vec<(String, String)> {
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
pub(crate) struct ReplicaSetFailure {
    pub(crate) name: String,
    pub(crate) reason: String,
    pub(crate) message: String,
}

impl ReplicaSetFailure {
    /// 条件里没给的字段留空，拼接时不留空段——`FailedCreate: ` 后面跟空串比只
    /// 写 reason 更难读。
    pub(crate) fn detail(&self) -> String {
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
pub(crate) fn replicaset_failures(out: &str) -> Vec<ReplicaSetFailure> {
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
pub(crate) fn admission_rejection(
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
pub(crate) fn rollout_converged(out: &str) -> bool {
    let parts: Vec<&str> = out.split('|').collect();
    if parts.len() < 5 {
        return false;
    }
    let spec: u32 = parts[2].parse().unwrap_or(0);
    let updated = updated_pod_count(out);
    let ready: u32 = parts[4].parse().unwrap_or(0);
    // unavailable 必须为 0：RollingUpdate 新旧副本并存时，旧副本仍 ready 会让
    // ready==spec 提前成立，但新崩溃副本计入 unavailable，不能判完成。
    let unavailable: u32 = parts.get(5).and_then(|v| v.parse().ok()).unwrap_or(0);
    generation_observed(out) && updated == spec && ready == spec && unavailable == 0 && spec > 0
}

/// 控制器**已经处理过当前这一代模板**（`observedGeneration >= generation`，且
/// generation 非零）。
///
/// 这一条是读 `updatedReplicas` 的前提：在控制器处理新模板之前，`status` 里那一段
/// 记的仍是**上一代**的账——上一次滚动完成后 `updatedReplicas` 恒等于副本数，于是
/// apply 之后的第一眼就能读到"新版本建成了 Pod"，而那一刻它数的是旧模板的 Pod。
/// 判"这一版有没有自己的 Pod"必须落在前提成立的那些读数上。
fn generation_observed(out: &str) -> bool {
    let parts: Vec<&str> = out.split('|').collect();
    let gen: u64 = parts
        .first()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let obs: u64 = parts
        .get(1)
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    gen > 0 && obs >= gen
}

/// 部署态读数里 `updatedReplicas`（第 3 段）的值：控制器为新版本**建成**了几个
/// Pod。缺字段（omitempty）与解析不出都是 0——这里问的是"有没有"，不是"有几个"，
/// 读不到就是没读到有。
///
/// 它和 `rollout_converged` 读同一份读数、同一段，所以两处不能各写一份下标：
/// 那会让"收敛"与"新版本建成过没有"在同一次读取上给出互相矛盾的答案。
/// 问"这一版有没有自己的 Pod"时还得先过 [`generation_observed`]——同一段字段在
/// 控制器处理新模板之前记的是上一代的账。
fn updated_pod_count(out: &str) -> u32 {
    out.split('|')
        .nth(3)
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// Pod 双标签选择器：主应用/网关/执行器的 name 标签都是 `cogneva`，
/// 单标签会跨部署误判（gitops puller 旧代码只用 name= 的同源缺陷）。
pub(crate) fn pod_selector(name: &str, component: &str) -> String {
    format!(
        "{}={},{}={}",
        cog_core::topology::LABEL_NAME,
        name,
        cog_core::topology::LABEL_COMPONENT,
        component
    )
}

/// 沿点分路径取一个子值，缺一层就给 `None`。
fn at_path<'a>(v: &'a serde_yaml::Value, path: &str) -> Option<&'a serde_yaml::Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// 一份清单里某个对象相对上一版**少了字段**：删除本该在这次 apply 落到活对象上。
///
/// 只比对象自己的 `spec` 子树：`status` 由控制器写、`metadata` 上有别人加的注解与
/// 标签，拿它们做对照会让这条判据被别的写者点亮，而它要回答的只有一句——这一版
/// 声明要删的东西，集群收到没有。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FieldRemoval {
    /// `kubectl get` 认的那个资源名（小写 kind，带组时是 `kind.group`）。
    pub resource: String,
    pub namespace: String,
    pub name: String,
    /// 相对上一版少掉的字段路径，`spec` 起算，例如 `.spec.headers.customRequestHeaders`。
    pub paths: Vec<String>,
}

/// server-side apply 只删「本次声明过、并且由本 field manager 拥有」的字段：清单里
/// **没写**的字段它根本不碰。于是「从清单里删掉一个字段」在 apply 之后可能一条都没
/// 落——而 apply 退 0、回 `serverside-applied`、`kubectl diff` 输出为空，三项读起来
/// 都像成功。2026-10-22 实测：从 chart 的 traefik `Middleware` 删掉
/// `spec.headers.customRequestHeaders`，主线滚动报健康完成，那个对象的
/// `metadata.generation` 还停在旧值上，一条改动都没上线。
///
/// 判据因此只能问对象自己：清单里少了字段的对象，`metadata.generation` 必须变。
/// 它变，说明这一轮写进去的东西让控制器看见了一份新 spec；它不变而清单里明明少了
/// 字段，就是这次删除没落。
pub(crate) const FIELD_REMOVAL_NOT_TAKEN_MARKER: &str =
    "a field this revision removed is still in the live object";

pub(crate) fn is_field_removal_not_taken(msg: &str) -> bool {
    msg.contains(FIELD_REMOVAL_NOT_TAKEN_MARKER)
}

/// 清单文档的身份与 spec。`resource` 由 kind 与 apiVersion 的组拼出来：
/// `kubectl get` 认 `middleware.traefik.io`、`deployment.apps` 这种写法，而裸
/// kind 在小写之后与它同形，所以两条都用同一种拼法。
fn manifest_identity(doc: &serde_yaml::Value) -> Option<(String, String, Option<String>)> {
    let kind = doc.get("kind")?.as_str()?.to_string();
    let api = doc.get("apiVersion")?.as_str()?.to_string();
    let group = api.split_once('/').map(|(g, _)| g);
    let mut resource = kind.to_ascii_lowercase();
    if let Some(group) = group {
        resource.push('.');
        resource.push_str(group);
    }
    let name = doc.get("metadata")?.get("name")?.as_str()?.to_string();
    let namespace = doc
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|n| n.as_str())
        .map(str::to_string);
    Some((resource, name, namespace))
}

/// 旧文档有、新文档没有的字段路径。值变了不算删除——那是「写上了」，代数是会动的。
/// 整棵子树没了只记一层：路径再往下每一行都是同一件事的重复。
fn removed_field_paths(
    old: &serde_yaml::Value,
    new: &serde_yaml::Value,
    path: &str,
    out: &mut Vec<String>,
) {
    match (old, new) {
        (serde_yaml::Value::Mapping(o), serde_yaml::Value::Mapping(n)) => {
            for (k, ov) in o {
                let key = match k.as_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                match n.get(k) {
                    Some(nv) => removed_field_paths(ov, nv, &format!("{path}.{key}"), out),
                    None => out.push(format!("{path}.{key}")),
                }
            }
        }
        (serde_yaml::Value::Sequence(o), serde_yaml::Value::Sequence(n)) => {
            for (i, ov) in o.iter().enumerate() {
                match n.get(i) {
                    Some(nv) => removed_field_paths(ov, nv, &format!("{path}[{i}]"), out),
                    None => out.push(format!("{path}[{i}]")),
                }
            }
        }
        _ => {}
    }
}

/// 相对上一版，清单里哪些对象的 `spec` 少了字段。
///
/// 只认两版都有、名字相同的对象：新出现的对象没有「删除」可言，消失的对象由别的
/// 路径管（它不在这一版要 apply 的东西里）。
pub(crate) fn objects_that_lost_fields(
    previous: &str,
    current: &str,
    namespace: &str,
) -> Vec<FieldRemoval> {
    let Ok(old_docs) = split_docs(previous, "previous bundle") else {
        return Vec::new();
    };
    let Ok(new_docs) = split_docs(current, "support.yaml") else {
        return Vec::new();
    };
    let new_by_id: BTreeMap<(String, String), &serde_yaml::Value> = new_docs
        .iter()
        .filter_map(|d| {
            let (resource, name, _) = manifest_identity(d)?;
            Some(((resource, name), d))
        })
        .collect();
    let mut out = Vec::new();
    for old in &old_docs {
        let Some((resource, name, ns)) = manifest_identity(old) else {
            continue;
        };
        let Some(new) = new_by_id.get(&(resource.clone(), name.clone())) else {
            continue;
        };
        let (Some(old_spec), Some(new_spec)) = (old.get("spec"), new.get("spec")) else {
            continue;
        };
        let mut paths = Vec::new();
        removed_field_paths(old_spec, new_spec, ".spec", &mut paths);
        if paths.is_empty() {
            continue;
        }
        out.push(FieldRemoval {
            resource,
            namespace: ns.unwrap_or_else(|| namespace.to_string()),
            name,
            paths,
        });
    }
    out
}

/// 哪些「删了字段」的对象**没有变**：事前事后都一样，说明这次删除没落。
///
/// 判不出来的一律放过：读不到代数（对象不存在、权限不够、查询超时）不是「没变」的
/// 证据，把没有证据写成失败会让这条判据在别的故障上误报。只有「两次都读到了、而且
/// 没有增加」才算数。
pub(crate) fn removals_that_did_not_land(
    seen: &[(FieldRemoval, Option<i64>, Option<i64>)],
) -> Vec<FieldRemoval> {
    seen.iter()
        .filter(|(_, before, after)| match (before, after) {
            (Some(b), Some(a)) => a <= b,
            _ => false,
        })
        .map(|(r, _, _)| r.clone())
        .collect()
}

/// 沿一串键取一个字符串字段，缺一层或不是字符串都给 `None`。
fn str_at<'a>(v: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a str> {
    let mut cur = v;
    for p in path {
        cur = cur.get(*p)?;
    }
    cur.as_str()
}

/// 一份清单文档**按名引用**的 ConfigMap。
///
/// 只认工作负载 pod 模板里的引用：`env[].valueFrom.configMapKeyRef`、
/// `envFrom[].configMapRef`、卷上的 `configMap.name` 与 `projected.sources[]` 的
/// 同名字段。别的 kind 不按名引用 ConfigMap（Service 按 label 选、NetworkPolicy
/// 按 label 圈），所以不为它们猜字段——猜出来的名字错了不会报错，只会变成一次判错
/// 方向的扣留。
///
/// **只收 ConfigMap，不收 Secret。** 这个 SA 按设计读不了 Secret：RBAC 不给它任何
/// Secret 动词（给了它就能读值，等于进化 Pod 握着全部凭证，见 `deploy/k3s/evolution-rbac.yaml`
/// 的安全红线）。读不了的引用若进了这道门，按"集群读不到就回答存在"的纪律会变成一次
/// 静默通过——比没有还坏。所以 Secret 那一半**不是滚动侧能问的问题**：谁会建它、建没建，
/// 是安装面的判据。
///
/// 逐个 pod 模板走 [`WORKLOAD_KINDS`] 的表，不另写一份 kind 名单：清单里出现一种
/// 这张表不认识的工作负载，它的引用就没人看得见，而那正是这张表本身要防的那件事。
/// 文档不是工作负载（或没有 pod 模板）时返回空集，不是错误。
fn referenced_configmaps(doc: &serde_yaml::Value) -> Vec<String> {
    let kind = doc.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    let Some(entry) = workload_kind(kind) else {
        return Vec::new();
    };
    let Some(pod) = at_path(doc, entry.pod_template) else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::new();
    let mut push = |name: Option<&str>| {
        if let Some(n) = name {
            if !n.is_empty() {
                out.push(n.to_string());
            }
        }
    };
    for field in ["containers", "initContainers"] {
        for c in pod
            .get(field)
            .and_then(|v| v.as_sequence())
            .into_iter()
            .flatten()
        {
            for e in c
                .get("env")
                .and_then(|v| v.as_sequence())
                .into_iter()
                .flatten()
            {
                push(
                    e.get("valueFrom")
                        .and_then(|v| str_at(v, &["configMapKeyRef", "name"])),
                );
            }
            for ef in c
                .get("envFrom")
                .and_then(|v| v.as_sequence())
                .into_iter()
                .flatten()
            {
                push(str_at(ef, &["configMapRef", "name"]));
            }
        }
    }
    for vol in pod
        .get("volumes")
        .and_then(|v| v.as_sequence())
        .into_iter()
        .flatten()
    {
        push(str_at(vol, &["configMap", "name"]));
        for src in vol
            .get("projected")
            .and_then(|v| v.get("sources"))
            .and_then(|v| v.as_sequence())
            .into_iter()
            .flatten()
        {
            push(str_at(src, &["configMap", "name"]));
        }
    }
    out
}

/// 这批文档里**自己就带着**的对象（kind/name）：本次 apply 会 upsert 出来的那一批。
/// 引用集减掉这一批之后，剩下的每一个都必须在集群里已经存在。
pub(crate) fn defined_objects(
    docs: &[serde_yaml::Value],
) -> std::collections::HashSet<(String, String)> {
    docs.iter()
        .filter_map(|d| {
            let kind = d.get("kind").and_then(|k| k.as_str())?;
            let name = d.get("metadata").and_then(|m| m.get("name"))?.as_str()?;
            Some((kind.to_string(), name.to_string()))
        })
        .collect()
}

/// 这批文档引用、而自己既不带来的 ConfigMap，去重且有序：剩下的每一个都必须在
/// 集群里已经存在。纯函数，判据脱离集群就能读，集群读那一半在
/// [`MainlineDeployer::missing_reference_holds`]。
pub(crate) fn unresolved_configmaps(docs: &[serde_yaml::Value]) -> Vec<String> {
    let defined = defined_objects(docs);
    let mut refs: Vec<String> = docs.iter().flat_map(referenced_configmaps).collect();
    refs.sort();
    refs.dedup();
    refs.retain(|name| !defined.contains(&("ConfigMap".to_string(), name.clone())));
    refs
}

/// The resource word kubectl takes for a manifest kind, read from
/// [`WORKLOAD_KINDS`].
///
/// Const because the faces that name one resource -- a table of what to read, a
/// kubectl argv -- are consts too. A face that spelled its own word is how one
/// resource ends up with two spellings: the table's `deployment` and a
/// hand-written `deploy` are the same object to kubectl, so nothing on the wire
/// objects, and only a reading that goes through one word while a patch goes
/// through the other shows it.
pub(crate) const fn workload_resource(kind: &str) -> &'static str {
    let mut i = 0;
    while i < WORKLOAD_KINDS.len() {
        if same_bytes(WORKLOAD_KINDS[i].kind, kind) {
            return WORKLOAD_KINDS[i].resource;
        }
        i += 1;
    }
    panic!("no workload kind declares this name")
}

/// `str` equality the const evaluator can call (`&str ==` is not const).
const fn same_bytes(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// 一份清单文档里工作负载的 pod 模板，以及它那一行。
fn workload_pod_spec(
    doc: &serde_yaml::Value,
) -> Option<(&'static WorkloadKind, &serde_yaml::Value)> {
    let kind = doc.get("kind").and_then(|k| k.as_str())?;
    let entry = workload_kind(kind)?;
    let mut node = doc;
    for segment in entry.pod_template.split('.') {
        node = node.get(segment)?;
    }
    Some((entry, node))
}

/// 清单里一个容器引用，以及清单声明的 image 值。
///
/// 判据是"清单声明的值"，不是"本仓库的镜像"：apply 把 pod 模板整体 upsert 成清单
/// 那一份，所以**声明值与现场值不同的每一个容器都是这次 apply 会改写的**——包括
/// 清单里顺手更新的第三方镜像（那种改动回滚同样该还）。用一个更窄的判据（只认本
/// 仓库）会漏掉它们，而漏掉的那一项没有读数。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeclaredRef {
    pub(crate) resource: String,
    pub(crate) name: String,
    pub(crate) container: String,
    /// 清单声明的值（pin 之后 apply 出去的那个值）：用来判断现场那一个会不会变。
    pub(crate) image: String,
}

/// 一次 apply 会改写的引用，以及它**改动之前**的现场值。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RewrittenRef {
    pub(crate) resource: String,
    pub(crate) name: String,
    pub(crate) container: String,
    /// apply 之前这个容器指着哪个镜像——回滚要还回去的就是它。
    pub(crate) image: String,
}

/// 这一轮 apply 会改写的整组引用，按面分两处。
///
/// 支撑面在任何镜像变更之前就 apply 了，所以它**一定**被改过，不论目标滚到了
/// 第几个；目标面只有真滚过的那些才还（没滚到的目标一个字节都没动）。
#[derive(Default)]
pub(crate) struct RefSnapshots {
    pub(crate) support: Vec<RewrittenRef>,
    pub(crate) targets: Vec<RewrittenRef>,
}

/// 清单文档里每一个带 image 的容器引用。
///
/// 解析失败是硬错误：这份文档马上就要被 apply 出去，读不出它的引用等于回滚没有
/// 目标——宁可一次变更都不发生（调用方在动任何东西之前就中止）。
pub(crate) fn bundle_declared_containers(yaml_text: &str) -> SFResult<Vec<DeclaredRef>> {
    const POD_LISTS: [&str; 2] = ["containers", "initContainers"];
    let mut out: Vec<DeclaredRef> = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(yaml_text) {
        let v = serde_yaml::Value::deserialize(doc)
            .map_err(|e| SFError::Config(format!("manifest bundle: invalid YAML document: {e}")))?;
        if v.is_null() {
            continue;
        }
        let Some((entry, spec)) = workload_pod_spec(&v) else {
            continue;
        };
        let Some(name) = v
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
        else {
            continue;
        };
        for list_name in POD_LISTS {
            let Some(list) = spec.get(list_name).and_then(|l| l.as_sequence()) else {
                continue;
            };
            for c in list {
                let Some(image) = c.get("image").and_then(|i| i.as_str()) else {
                    continue;
                };
                let Some(container) = c.get("name").and_then(|n| n.as_str()) else {
                    continue;
                };
                out.push(DeclaredRef {
                    resource: entry.resource.to_string(),
                    name: name.to_string(),
                    container: container.to_string(),
                    image: image.to_string(),
                });
            }
        }
    }
    Ok(out)
}

/// 绝不把集群正举着的天花板按回树里那个数。
///
/// 版外那台 Pod（评测台按题起的那台）会按自己的清单把额度抬到装得下，跑完
/// 再放回去；在它抬起到放回之间，集群里的 `hard` 高于树里的声明。这段时间里
/// 把这版声明 apply 下去不是「收敛」：apiserver 拒收一条 `hard` 低于当前用量的
/// 更新，支撑包在**一个镜像都没动**的时候失败，而这次失败会被读成「这一版不
/// 好」。所以按维度取两者之大——被抬高那一维留在集群里，其余维度照常收敛，
/// 两个面不一致这件事由漂移读数说出来，而不是由一次假失败说出来。
///
/// 抬起来的那一维由谁放回去：抬它的那台 Pod 自己（`remove()` 里恢复），以及
/// 它跑完之后第一次没有抬升的交付。所以这里是「不抢」，不是「永不收敛」。
pub(crate) fn keep_raised_ceilings(
    text: &str,
    origin: &str,
    live: &BTreeMap<String, crate::governance_drift::QuotaUsage>,
) -> SFResult<String> {
    use cog_core::claim_footprint::quantity_bytes;
    let mut docs = split_docs(text, origin)?;
    let mut raised: Vec<String> = Vec::new();
    for doc in docs.iter_mut() {
        if doc.get("kind").and_then(|k| k.as_str()) != Some("ResourceQuota") {
            continue;
        }
        let Some(name) = doc
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        let Some(live_hard) = live.get(&format!("resourcequota/{name}")).map(|q| &q.hard) else {
            continue;
        };
        let Some(hard) = doc
            .get_mut("spec")
            .and_then(|s| s.get_mut("hard"))
            .and_then(|h| h.as_mapping_mut())
        else {
            continue;
        };
        for (resource, declared) in hard.iter_mut() {
            let Some(key) = resource.as_str() else {
                continue;
            };
            let Some(live_text) = live_hard.get(key) else {
                continue;
            };
            // A dimension that does not parse on either side is left exactly as
            // declared: this step only knows how to compare, and comparing by
            // text would keep a raised ceiling forever on a spelling difference.
            let (Some(declared_bytes), Some(live_bytes)) = (
                declared.as_str().and_then(quantity_bytes),
                quantity_bytes(live_text),
            ) else {
                continue;
            };
            if live_bytes > declared_bytes {
                *declared = serde_yaml::Value::String(live_text.clone());
                raised.push(format!("{name}.{key}: {declared_bytes} -> {live_bytes}"));
            }
        }
    }
    if !raised.is_empty() {
        tracing::info!(
            raised = %raised.join(", "),
            "rollout: the cluster holds a ceiling above this revision's declaration (a pod \
             outside the release widened it); keeping the higher number so the support apply \
             is not refused for lowering it"
        );
    }
    render_docs(&docs)
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
pub(crate) fn stage_rollout_manifest(
    text: &str,
    origin: &str,
    out: &Path,
) -> SFResult<Option<PathBuf>> {
    let docs = namespace_docs(text, origin)?;
    if docs.is_empty() {
        return Ok(None);
    }
    std::fs::write(out, render_docs(&docs)?)
        .map_err(|e| SFError::IO(format!("write {}: {e}", out.display())))?;
    Ok(Some(out.to_path_buf()))
}

/// Where one manifest package gets staged before it is applied.
///
/// The name has to be different on **every call**: two executors in one process
/// staging the same package (parallel tests are two executors, and a caller
/// that ever rolls two targets at once is no different) write one path, and
/// `std::fs::write` truncates before it writes -- so the reader that wins the
/// race parses a manifest torn in half and treats it as a broken file. The pid
/// separates processes and the counter separates callers inside one; both are
/// needed, since either alone still collides.
///
/// `label` names the package: `target-<deployment>` for one target's manifest,
/// `support` for the support bundle. Both are staged by the same rule because
/// both are written by the same process: a fixed name for the support bundle
/// left two parallel end-to-end tests tearing one file in half, which surfaced
/// as `invalid YAML document: could not find expected ':'` in whichever of them
/// parsed second.
pub(crate) fn staged_manifest_path(label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "mainline-{label}-{}-{seq}.yaml",
        std::process::id()
    ))
}

pub struct RolloutPlan {
    /// 新镜像引用（节点 pull 端点，kubelet 经 NodePort 拉取）。
    pub tag: String,
    pub targets: Vec<RolloutTarget>,
    /// 这一次滚动的因果 id（W3C `traceparent`）。派发侧生成并写进 Job 注解，
    /// Job 里再交回这里：两处不是各写各的值，而是同一个字符串，否则同一条链在
    /// 派发与执行之间断成两条。
    pub traceparent: Option<String>,
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
            traceparent: None,
            manifests_dir: None,
        }
    }
}

/// The `limits` one workload document declares on its pods, summed over every
/// container and init container, keyed the way a `ResourceQuota` spells the same
/// resource (`limits.cpu`, `limits.memory`).
///
/// `None` means the document carries no pod template at all, or a limits value
/// that is not a quantity this crate can read. An unreadable limit is not a
/// limit of zero, and reading one as zero would make a rollout look like it
/// fits; the caller treats `None` as "cannot judge" and lets the rollout through
/// rather than guessing.
///
/// Summing the containers and the init containers together over-counts against
/// what admission charges for init containers (which it takes as a maximum over
/// them, not a sum). That is deliberately the conservative direction, and it
/// cancels in the difference the caller takes whenever a rollout leaves the init
/// containers alone -- the only field evidence this crate has.
pub(crate) fn pod_template_limits(doc: &serde_yaml::Value) -> Option<BTreeMap<String, f64>> {
    let spec = doc.get("spec")?.get("template")?.get("spec")?;
    let mut sums: BTreeMap<String, f64> = BTreeMap::new();
    for list_key in ["containers", "initContainers"] {
        let Some(list) = spec.get(list_key).and_then(|c| c.as_sequence()) else {
            continue;
        };
        for container in list {
            let Some(limits) = container.get("resources").and_then(|r| r.get("limits")) else {
                continue;
            };
            let map = limits.as_mapping()?;
            for (k, v) in map {
                let Some(key) = k.as_str() else { continue };
                let text = crate::governance_drift::quantity_text(v)?;
                let value = cog_core::claim_footprint::quantity_bytes(&text)?;
                *sums.entry(format!("limits.{key}")).or_insert(0.0) += value;
            }
        }
    }
    Some(sums)
}

/// The first resource for which rolling this target's new pod onto the namespace
/// would pass an enforced ceiling, if any, named with the numbers that decided
/// it.
///
/// How much the new pod is charged depends on whether the old one is still
/// there when it is admitted, and that is what `surges` says:
///
/// - `false` — the replacement is admitted only after the old pod is gone, so
///   the highest counted usage is `used - old + new`. This is the shape of
///   `maxSurge: 0`, and only a resource the rollout *raises* can trip it.
/// - `true` — the new pod is admitted beside the old one (`maxSurge ≥ 1`), so
///   the peak is `used + new`, and even an unchanged pod can pass the ceiling
///   because the pair is counted twice. Reading the delta here answers a
///   question nobody asked: it reports headroom for a pod that will never be
///   admitted alone, and the rollout stalls on a refusal whose cause is the
///   surge it was configured with.
///
/// `None` is returned both for "fits" and for "one of the three numbers could
/// not be read": an unreadable figure is not a figure that fits, but it is also
/// not grounds to hold a rollout, and holding a good rollout is the failure this
/// must not introduce.
/// The first enforced ceiling any of these quotas would have this target's new
/// pod pass, if any -- the same walk [`admission_shortfall`] does, kept apart so
/// the reading can be taken twice without the loop being written twice.
fn first_admission_shortfall(
    quotas: &std::collections::BTreeMap<String, crate::governance_drift::QuotaUsage>,
    old_limits: &BTreeMap<String, f64>,
    new_limits: &BTreeMap<String, f64>,
    surges: bool,
) -> Option<AdmissionShortfall> {
    for (object, usage) in quotas {
        if let Some(reason) = admission_shortfall(object, usage, old_limits, new_limits, surges) {
            return Some(reason);
        }
    }
    None
}

/// Names of the pods that are still being charged while they shut down.
///
/// A pod with a deletion timestamp is gone from nobody's accounting yet: the
/// quota keeps charging it until it actually leaves. That is what makes an
/// admission reading taken immediately after this run stopped its own pods say
/// less than it looks like it says.
///
/// `None` when the listing cannot be parsed -- an unreadable figure is not a
/// figure that fits, but it is not grounds to hold a rollout either, which is
/// the discipline the admission judgement itself follows.
pub(crate) fn terminating_pods(pods_json: &str) -> Option<Vec<String>> {
    let v: serde_json::Value = serde_json::from_str(pods_json).ok()?;
    let items = v.get("items")?.as_array()?;
    Some(
        items
            .iter()
            .filter(|p| {
                p.pointer("/metadata/deletionTimestamp")
                    .and_then(|d| d.as_str())
                    .is_some()
            })
            .filter_map(|p| {
                p.pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .map(str::to_owned)
            })
            .collect(),
    )
}

/// A ceiling this target's new pod would pass, and which dimension it is in.
///
/// The dimension rides along from the loop that found it rather than being read
/// back out of [`Self::text`]: the sentence is written for a person and its
/// wording is free to change, while `resource` is the quota key the ceiling is
/// keyed by. A caller that needs "was it cpu or memory" is asking about the key,
/// and re-parsing prose to answer it would break the first time the sentence is
/// reworded -- silently, because a parser that finds nothing answers "other".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmissionShortfall {
    /// The dimension, in a closed set: `cpu`, `memory`, `pods`, `storage`, `other`.
    ///
    /// The repair differs per dimension -- a tight `limits.cpu` is raised in a
    /// different place than `limits.memory` -- and it is the one thing the
    /// failure signature cannot carry (`...:precheck:<target>:admission` says
    /// where it broke, not which ceiling ran out).
    pub dimension: &'static str,
    /// The sentence an operator reads: which object, which resource, by how much.
    pub text: String,
}

/// Which dimension a quota key stands for.
///
/// Keys are `limits.cpu`, `requests.memory`, `pods`, `requests.storage`, ...
/// The prefix (`limits`/`requests`) is not the dimension: the same shortfall in
/// the same resource is repaired in the same place whether it is charged
/// against limits or requests, and splitting them would put two rows on one
/// wall. `other` is a real answer, not a fallback to hide: a name this build has
/// not seen is still a hold, and folding it into a known dimension would name
/// the wrong repair.
pub(crate) fn shortfall_dimension(resource: &str) -> &'static str {
    let resource = resource.trim();
    if resource.ends_with(".cpu") || resource == "cpu" {
        "cpu"
    } else if resource.ends_with(".memory") || resource == "memory" {
        "memory"
    } else if resource == "pods" {
        "pods"
    } else if resource.ends_with(".storage") || resource == "storage" {
        "storage"
    } else {
        "other"
    }
}

pub(crate) fn admission_shortfall(
    object: &str,
    usage: &crate::governance_drift::QuotaUsage,
    old_limits: &BTreeMap<String, f64>,
    new_limits: &BTreeMap<String, f64>,
    surges: bool,
) -> Option<AdmissionShortfall> {
    let mut resources: Vec<&String> = new_limits.keys().collect();
    resources.sort();
    for resource in resources {
        let old = old_limits.get(resource).copied().unwrap_or(0.0);
        let charged = if surges {
            new_limits[resource]
        } else {
            new_limits[resource] - old
        };
        if charged <= 0.0 {
            continue;
        }
        let Some(hard_text) = usage.hard.get(resource) else {
            continue;
        };
        let Some(used_text) = usage.used.get(resource) else {
            continue;
        };
        let (Some(hard), Some(used)) = (
            cog_core::claim_footprint::quantity_bytes(hard_text),
            cog_core::claim_footprint::quantity_bytes(used_text),
        ) else {
            continue;
        };
        if used + charged > hard {
            let text = if surges {
                format!(
                    "{object} {resource}: this rollout surges, so the new pod is admitted beside \
                     the old one and charged {charged} on top of {used_text} used, which passes \
                     the enforced {hard_text}"
                )
            } else {
                format!(
                    "{object} {resource}: the new pod raises it by {charged}, and {used_text} used \
                     + that passes the enforced {hard_text}"
                )
            };
            return Some(AdmissionShortfall {
                dimension: shortfall_dimension(resource),
                text,
            });
        }
    }
    None
}

/// Whether a live deployment admits its replacement beside the old pod, i.e.
/// `maxSurge` resolves to more than zero. A `RollingUpdate` that does not state
/// one surges by a quarter of the replicas (rounded up), which is what the
/// controller does, so an unstated `maxSurge` reads as a surge rather than as
/// none.
pub(crate) fn deployment_surges(live: &serde_json::Value) -> bool {
    let strategy = live.get("spec").and_then(|s| s.get("strategy"));
    let kind = strategy
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("RollingUpdate");
    if kind != "RollingUpdate" {
        return false;
    }
    let replicas = live
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_u64())
        .unwrap_or(1);
    let raw = strategy
        .and_then(|s| s.get("rollingUpdate"))
        .and_then(|r| r.get("maxSurge"));
    let extra = match raw {
        Some(v) => match v.as_u64() {
            Some(n) => n as i64,
            None => match v.as_str().and_then(|t| t.trim().strip_suffix('%')) {
                Some(pct) => match pct.trim().parse::<f64>() {
                    Ok(p) => (p * replicas as f64 / 100.0).ceil() as i64,
                    Err(_) => return true,
                },
                None => return true,
            },
        },
        // Unstated: the controller's own default is 25% of the replicas.
        None => ((replicas as f64) * 0.25).ceil() as i64,
    };
    extra > 0
}

/// 支撑清单里可能被 apply 动到的工作负载（后端、集群内 registry、节点级 DaemonSet）。
/// apply 只改自己那份 spec：没改动的工作负载原地不动，改动的会滚动重启几秒到几十秒。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupportWorkload {
    /// kubectl 子命令用的单数资源名（deployment / statefulset / daemonset）。
    pub(crate) kind: &'static str,
    pub(crate) name: String,
    pub(crate) generation: i64,
}

impl SupportWorkload {
    pub(crate) fn display(&self) -> String {
        format!("{}/{}", self.kind, self.name)
    }
}

/// 这个工作负载是不是本次滚动要逐个滚的目标之一。
///
/// 目标的 Pod 由逐目标循环自己重启：替它再重启一次，就是同一台工作负载同时
/// 叠两份 surge，而准入判据按「一次只有一台在 surge」算账——2026-10-26 实测，
/// 配置效果滚动先给 `deployment/cogneva` 起了一个新 Pod（旧 Pod 因
/// `maxUnavailable: 0` 还留着），逐目标循环再要一份 4 核，预检看到 35100m
/// 之上再加 4000m 撞上 38 核天花板，整轮被判环境类失败、退出码 75。
///
/// 词表只有一处：`kind` 是 kubectl 的资源词，`targets` 里存的是 Deployment 的
/// 对象名，两处各写一遍比较就会在某次改名后静默不匹配。等支撑与被配置滚动
/// 跳过这两条路都走这一个判据。
///
/// 词表读取必须留在取值的这一行（`workload_kind_of_resource(w.kind)`），不能收进
/// 一层只收 `kind` 的 helper：授权面是按这一行的字面量判定「这次 `get` 取的是哪一族
/// 资源」的，藏进 helper，那个普查就读不到这句，判不了这个身份缺不缺那份授权。
pub(crate) fn is_rollout_target(w: &SupportWorkload, targets: &[RolloutTarget]) -> bool {
    workload_kind_of_resource(w.kind).map(|k| k.kind) == Some("Deployment")
        && targets.iter().any(|t| t.deployment == w.name)
}

/// 这个工作负载的目标清单会不会在这次 apply 里真的落到集群上。
///
/// 配置效果滚动只对"清单在手"的目标让路：那种目标被逐目标循环 apply 之后，
/// 清单里的 `checksum/config` 注解随内容一起变，它自己的滚动就把新文档读进去了。
/// 退化成 `set image` 的目标没有这层保证——Pod 模板上唯一会变的是镜像，只改配置
/// 的那一版里镜像没变，让它自己滚等于什么都不发生，配置就没人读。
pub(crate) fn is_manifest_carried(kind: &str, name: &str, carried: &[String]) -> bool {
    workload_kind_of_resource(kind).map(|k| k.kind) == Some("Deployment")
        && carried.iter().any(|d| d == name)
}

/// 本次滚动的目标里，哪些的清单会在 `apply_target` 里被 apply —— 也就是说，
/// 哪些目标的滚动会自己带上新的 `checksum/config` 注解。
///
/// 判据与 `apply_target` 那一路同源：清单目录里有 `deploy-<name>.yaml`，且它
/// 确实带着这个 Deployment。只判"文件在不在"会把一份只有 Service 的清单也算成
/// "apply 了"（那一路 apply 完根本不会滚目标），于是配置改动静默丢失。
pub(crate) async fn manifest_carried_targets(plan: &RolloutPlan) -> Vec<String> {
    let Some(dir) = plan.manifests_dir.as_deref() else {
        return Vec::new();
    };
    let mut carried = Vec::new();
    for t in &plan.targets {
        let key = target_manifest_key(&t.deployment);
        let path = Path::new(dir).join(&key);
        if !path.is_file() {
            continue;
        }
        let Ok(text) = tokio::fs::read_to_string(&path).await else {
            continue;
        };
        if carries_deployment(&text, &key, &t.deployment).unwrap_or(false) {
            carried.push(t.deployment.clone());
        }
    }
    carried
}

/// 这次 apply 真的动过哪些支撑工作负载：代数变了，或 apply 之前根本不在（新增
/// 的工作负载一样要等到就绪）。目标部署不在此列——它们的滚动由逐目标等待负责，
/// 判据与预算都是另一套，在这里再等一遍会让同一件事有两个判据。
pub(crate) fn changed_workloads(
    before: &[SupportWorkload],
    after: &[SupportWorkload],
    targets: &[RolloutTarget],
) -> Vec<SupportWorkload> {
    after
        .iter()
        // A rollout target is a Deployment of its own, so the ones it names
        // are not "some support workload this apply restarted" -- they are
        // waited for one by one.
        .filter(|w| !is_rollout_target(w, targets))
        .filter(
            |w| match before.iter().find(|b| b.kind == w.kind && b.name == w.name) {
                Some(b) => b.generation != w.generation,
                None => true,
            },
        )
        .cloned()
        .collect()
}

/// 支撑工作负载是否已经回到"这次滚动可以往下走"。读法
/// = `generation|observed|want|ready|updated|running`，三条同时成立才算数：
///
/// - `observed >= generation`：控制器已经处理过这一代 spec。
/// - `updated >= want` 且 `running <= updated`：新版本的副本补齐了、旧版本的副本
///   撤干净了。**这一条才是这个等待真正的判据**。只看 `ready >= want` 在换 Pod 的
///   整个窗口里读到的东西与常态一字不差：Deployment 默认先起新副本（`maxSurge`）
///   再撤旧副本，旧副本一路 Ready，`readyReplicas` 全程等于 `want`；StatefulSet 在
///   新旧之间总有一个 Pod 顶着，被判删的那个 Pod 也还带着 Ready 条件。于是"旧 Pod
///   已经下线、新 Pod 还没监听"这一整段——恰恰是这个等待存在的理由——会被判成
///   "就绪"，等待当场放行，而这次 apply 刚重启过的那个支撑负载正是此刻不可用。
///   换成"新版本副本数 + 现有副本总数"，判据在故障期间就动了。
/// - `ready >= want`：新副本还要真的过就绪探针。`updatedReplicas` 数的是新版本的
///   Pod，不看它是否 Ready，单靠它会把"Pod 已创建、探针还没过"判成就绪。
///
/// 读法解析不出来就返回 None（没读到答案，不是"就绪"）。spec.replicas 缺省时按 1
/// 算：Deployment/StatefulSet 的缺省都是 1，读成 0 会把一个单副本工作负载判成
/// "不需要就绪"而直接放行。`updatedReplicas` / `replicas` 缺省按 0 算：这两个字段
/// 都是 optional，缺省即真的为 0，而 0 在 `updated >= want` 一侧是保守方向。
///
/// 读法与判据都按 kind 分派（见 [`settle_jsonpath`]）：上面这一套是副本族的，
/// DaemonSet 走 [`daemonset_settled`]——它没有 `spec.replicas`，同一句判据在它上面
/// 读的是两个不存在的字段。
pub(crate) fn support_settled(kind: &str, readout: &str) -> Option<bool> {
    match workload_kind_of_resource(kind).map(|k| k.kind) {
        Some("DaemonSet") => daemonset_settled(readout),
        _ => replica_settled(readout),
    }
}

/// DaemonSet 的判据：控制器看过这一代 spec，且每个该跑的节点上都有一个就绪的
/// 新版本 Pod。`desiredNumberScheduled` 由控制器按节点选择器数出来，一个节点都
/// 不匹配时它是 0——那种 DaemonSet 确实没有东西要等。
fn daemonset_settled(readout: &str) -> Option<bool> {
    let mut parts = readout.split('|');
    let generation: i64 = parts.next()?.trim().parse().ok()?;
    let observed: i64 = parts.next()?.trim().parse().unwrap_or(0);
    let desired: i32 = parts.next()?.trim().parse().ok()?;
    let ready: i32 = parts.next()?.trim().parse().unwrap_or(0);
    let updated: i32 = parts.next()?.trim().parse().unwrap_or(0);
    Some(observed >= generation && ready >= desired && updated >= desired)
}

fn replica_settled(readout: &str) -> Option<bool> {
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
    let updated: i32 = parts
        .next()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let running: i32 = parts
        .next()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    Some(observed >= generation && ready >= want && updated >= want && running <= updated)
}

/// 一个还没就绪的支撑工作负载，它现在那批 Pod 说了什么。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SupportCause {
    /// 读到了：每个 Pod 一段，逐容器写出它此刻的状态。
    Read(String),
    /// 一个 Pod 都没有。这不是「起不来」：副本数读作 0，或这一版还没被控制器
    /// 创建出来。两种情况下「就绪」这个判据眼下问不出东西，读数得如实这么说。
    NoPods,
    /// 读不到（对象读不到、选择器表达不出来、Pod 列表解析不了）。读不到不等于
    /// 没起来，两者处置不同。
    Unreadable(String),
}

/// 列一个工作负载自己那批 Pod 用的标签选择器，取自它的 `spec.selector.matchLabels`
/// （`k=v,k=v`，按 key 排序）。
///
/// 支撑工作负载没有目标部署那样现成的 name/component 标签可以拼，只有对象自己
/// 声明的选择器是权威。`matchExpressions` 非空时返回 None——那种选择器表达不成
/// `k=v`，拼一个漏掉条件的出来会把别的 Pod 的病因当成它的报，读不到比读错强。
pub(crate) fn support_pod_selector(workload_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(workload_json).ok()?;
    let selector = value.get("spec")?.get("selector")?;
    if selector
        .get("matchExpressions")
        .and_then(|e| e.as_array())
        .is_some_and(|e| !e.is_empty())
    {
        return None;
    }
    let labels = selector.get("matchLabels")?.as_object()?;
    let mut pairs: Vec<String> = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        pairs.push(format!("{key}={}", value.as_str()?));
    }
    if pairs.is_empty() {
        return None;
    }
    pairs.sort();
    Some(pairs.join(","))
}

/// 把一个工作负载当前那批 Pod 的 JSON 折成「为什么没起来」。
///
/// 判死那一刻的四种病因在读数落地之前同形：容器崩（`CrashLoopBackOff`）、镜像
/// 拉不下来（`ImagePullBackOff`）、起来了但探针一直不过（`running not-ready`）、
/// 根本排不上队（Pod 一直 Pending、一个容器状态都没有）。四种的处置各不相同，
/// 而 900s 的等待里它们都只报「没就绪」，事后要回集群去猜，那时事件窗口早过了。
pub(crate) fn support_pod_causes(pods_json: &str) -> SupportCause {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(pods_json) else {
        return SupportCause::Unreadable("the pod list is not JSON".to_string());
    };
    let Some(items) = value.get("items").and_then(|i| i.as_array()) else {
        return SupportCause::Unreadable("the pod list carries no items".to_string());
    };
    if items.is_empty() {
        return SupportCause::NoPods;
    }
    let mut named: Vec<(String, &serde_json::Value)> =
        items.iter().map(|pod| (pod_name(pod), pod)).collect();
    named.sort_by(|a, b| a.0.cmp(&b.0));
    let total = named.len();
    let mut lines: Vec<String> = named
        .iter()
        .take(SUPPORT_CAUSE_POD_LIMIT)
        .map(|(_, pod)| pod_cause(pod))
        .collect();
    if total > SUPPORT_CAUSE_POD_LIMIT {
        lines.push(format!("(+{} more pods)", total - SUPPORT_CAUSE_POD_LIMIT));
    }
    SupportCause::Read(lines.join("; "))
}

fn pod_name(pod: &serde_json::Value) -> String {
    pod.pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .filter(|n| !n.is_empty())
        .unwrap_or("<unnamed pod>")
        .to_string()
}

/// 一个 Pod 一段：相位，加上**逐容器**的状态。
///
/// 多容器 Pod 的「没就绪」不含信息——整 Pod 的就绪是全部容器的与，哪个容器挡住了
/// 它是全部问题。线上实测过一次：主容器一直就绪，边车因为清单里没写子命令而反复
/// 退出，Pod 永远不就绪，而在只读主容器的读数里这看起来像「起来了只是探针不过」。
/// 容器名因此必须进读数。
fn pod_cause(pod: &serde_json::Value) -> String {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|p| p.as_str())
        .filter(|p| !p.is_empty())
        .unwrap_or("<no phase>");
    let mut seg = format!("{} {phase}", pod_name(pod));
    if let Some(reason) = unschedulable_reason(pod) {
        seg.push_str(&format!(", unschedulable: {reason}"));
    }
    let mut containers: Vec<String> = Vec::new();
    for (list, prefix) in [
        ("/status/initContainerStatuses", "init "),
        ("/status/containerStatuses", ""),
    ] {
        let Some(statuses) = pod.pointer(list).and_then(|s| s.as_array()) else {
            continue;
        };
        for status in statuses {
            let name = status
                .get("name")
                .and_then(|n| n.as_str())
                .filter(|n| !n.is_empty())
                .unwrap_or("<unnamed container>");
            containers.push(format!("{prefix}{}", container_cause(name, status)));
        }
    }
    let total = containers.len();
    let shown: Vec<String> = containers
        .into_iter()
        .take(SUPPORT_CAUSE_CONTAINER_LIMIT)
        .collect();
    if shown.is_empty() {
        // kubelet 还没报容器状态（刚创建、还没轮到它）。这是「没有这个信号」，
        // 不是「容器都正常」。
        seg.push_str(", no container status reported yet");
    } else {
        seg.push_str(&format!(", {}", shown.join(", ")));
        if total > SUPPORT_CAUSE_CONTAINER_LIMIT {
            seg.push_str(&format!(
                " (+{} more containers)",
                total - SUPPORT_CAUSE_CONTAINER_LIMIT
            ));
        }
    }
    seg
}

/// 一个容器此刻的状态。等待态写原因，终止态写原因与**退出码**。
///
/// 等待态的原因只说"kubelet 停在哪一步"，退出码说的是"怎么死的"：`1` = 进程自己
/// 非零退出（入口跑错、装配失败），`137` = 被杀（多为超上限），`143` = 被优雅
/// 终止。`CrashLoopBackOff` 尤其如此——它只说"在退避"，真正的死法在 `lastState`
/// 里，所以重启次数与上一次的终止原因/退出码一起写出来。
fn container_cause(name: &str, status: &serde_json::Value) -> String {
    let mut seg = format!("{name}=");
    if let Some(waiting) = status.pointer("/state/waiting") {
        seg.push_str(&format!(
            "waiting {}",
            field(waiting, "reason", "<no reason>")
        ));
        if let Some(message) = waiting
            .get("message")
            .and_then(|m| m.as_str())
            .filter(|m| !m.is_empty())
        {
            seg.push_str(&format!(
                " ({})",
                bounded(message, SUPPORT_CAUSE_MESSAGE_CHARS)
            ));
        }
    } else if let Some(terminated) = status.pointer("/state/terminated") {
        seg.push_str(&format!("terminated {}", terminated_cause(terminated)));
    } else if status.pointer("/state/running").is_some() {
        seg.push_str(
            if status
                .get("ready")
                .and_then(|r| r.as_bool())
                .unwrap_or(false)
            {
                "running ready"
            } else {
                // 容器起来了却一直不就绪：探针没过（或还没过）。与"进程起不来"是两种
                // 病因，判词里必须分得开。
                "running not-ready"
            },
        );
    } else {
        seg.push_str("no state reported");
    }
    let restarts = status
        .get("restartCount")
        .and_then(|r| r.as_u64())
        .unwrap_or(0);
    if restarts > 0 {
        seg.push_str(&format!(" restarts={restarts}"));
    }
    if let Some(last) = status.pointer("/lastState/terminated") {
        seg.push_str(&format!(" last {}", terminated_cause(last)));
    }
    seg
}

/// 终止态的读数：原因、退出码，加上容器自己写下的**终止消息**。
///
/// 退出码只说"怎么死的"（`1` = 进程自己非零退出，`137` = 被杀），到底为什么是 `1`
/// 只有容器自己知道——它把死因写进 `terminationMessagePath`，kubelet 读来放进
/// `state.terminated.message`（退避后的那一次在 `lastState` 里）。判死时刻主容器
/// 多半已经进了 `CrashLoopBackOff`，真正的死因于是落在 `lastState` 上：只读当前态
/// 会把一次「连不上 redis」读成「在退避」，而那两件事要去看的地方相反。
fn terminated_cause(terminated: &serde_json::Value) -> String {
    let mut seg = format!(
        "{} exit {}",
        field(terminated, "reason", "<no reason>"),
        exit_code(terminated)
    );
    if let Some(message) = terminated
        .get("message")
        .and_then(|m| m.as_str())
        .filter(|m| !m.is_empty())
    {
        seg.push_str(&format!(
            " ({})",
            bounded(message, SUPPORT_CAUSE_MESSAGE_CHARS)
        ));
    }
    seg
}

/// 调度器写在 Pod 条件上的判决。排不上队的 Pod 一个容器状态都没有，病因只在这里
/// （`Insufficient cpu`、节点亲和不满足、污点未容忍）。没有这一条，一个 Pending
/// 的 Pod 在读数里只剩「Pending」。
fn unschedulable_reason(pod: &serde_json::Value) -> Option<String> {
    let conditions = pod.pointer("/status/conditions")?.as_array()?;
    for condition in conditions {
        if condition.get("type").and_then(|t| t.as_str()) != Some("PodScheduled") {
            continue;
        }
        if condition.get("status").and_then(|s| s.as_str()) != Some("False") {
            continue;
        }
        let reason = field(condition, "reason", "");
        let message = field(condition, "message", "");
        let text = if message.is_empty() { reason } else { message };
        if text.is_empty() {
            return Some("no message".to_string());
        }
        return Some(bounded(text, SUPPORT_CAUSE_MESSAGE_CHARS));
    }
    None
}

/// 把消费面读数（每行一个工作负载，竖线分段、段内逗号分隔）解析成消费者。
///
/// 段序：name|卷里的 ConfigMap|projected 里的 ConfigMap|envFrom|env.valueFrom|
/// initContainer 的 envFrom|initContainer 的 env.valueFrom。空段与空项都要能读：
/// 没有卷的工作负载整段是空的，而不是缺一列——位置由竖线固定，缺字段（omitempty）
/// 不能把后面的段顶掉。
pub(crate) fn config_consumers(readout: &str, kind: &str) -> Vec<ConfigConsumer> {
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
pub(crate) fn changed_configmaps(
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
pub(crate) fn config_change_needs_restart(
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

/// 一次 kubectl 调用的输出落点：临时目录下两个唯一名字。
///
/// 名字里带进程 id、一个进程内自增号与纳秒时间戳：同一进程里 `probe` 与预热那几条
/// 路径都可能并发起子进程，只按 pid 命名会让两次调用写同一份文件。
fn kubectl_output_paths() -> (PathBuf, PathBuf) {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let base = std::env::temp_dir().join(format!(
        "cogneva-kubectl-{}-{seq}-{stamp}",
        std::process::id()
    ));
    let out = base.with_extension("out");
    let err = base.with_extension("err");
    (out, err)
}

/// 子进程被杀之前写下的话，取最后一段。
///
/// 外层定时器到点是**杀**进程：输出经管道交回时，进程一死管道就断，已经写出来的
/// 那句原因跟着丢，留下的只有一句"timed out after Ns"。2026-10-24 那次滚动就是
/// 这样败的——九次、三百多秒，一次也没留下 kubectl 说的字，事后翻集群日志也判不出
/// 是 apiserver 卡住还是 Pod 自己的网络栈坏了。所以输出落到文件而不是管道：文件在
/// 进程死后仍然可读，判词于是能带上它自己的诊断。
const STDERR_TAIL_CHARS: usize = 600;

fn stderr_tail(text: &str) -> String {
    let trimmed = text.trim();
    let chars: Vec<char> = trimmed.chars().collect();
    if chars.len() <= STDERR_TAIL_CHARS {
        return trimmed.to_string();
    }
    let tail: String = chars[chars.len() - STDERR_TAIL_CHARS..].iter().collect();
    format!("…{tail}")
}

pub struct RolloutExecutor {
    kubectl: String,
    ns: String,
    soak_secs: u64,
    restart_threshold: u32,
    rollout_timeout_secs: u64,
    startup_timeout_secs: u64,
    /// 动手之前要复问的上游 CI。`None` = 这一趟没有上游清单（派发侧没配上游
    /// 或它是旧版二进制），此时不提问、也不假装问过。
    ci_ask: Option<UpstreamCiAsk>,
    /// 读宿主运行队列的那一口。生产里是 [`read_host_run_queue`]，用例里换成固定
    /// 读数：这条判据读的是**跑用例的那台机器**的负载，不换掉，同一份用例在空闲的
    /// 笔记本上与一台正在编译的 CI 机上会给出两种类别。
    host_run_queue: fn() -> Option<HostRunQueue>,
}

/// 一次「落地之前再问一次 CI」的全部输入：要问的 rev，以及问哪些上游。
///
/// 由派发侧（部署器）从它自己那份上游配置里原样带过来，不在这里另立一份：仓库
/// 身份只有一个家，Job 只是拿着同一份清单再问一次。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpstreamCiAsk {
    pub rev: String,
    pub upstreams: Vec<UpstreamCiTarget>,
}

/// 一个上游：平台名（只用于日志与判词落点）、网关透传基址、仓库。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpstreamCiTarget {
    /// `CodePlatform::slug()` 的取值。不参与判据，只用来说清「是谁说的红」。
    pub platform: String,
    pub api_base: String,
    pub repo: String,
}

/// 落地前复问的结论：这一轮为什么不能滚。
///
/// 三种都给同一个动作——**一个字节都不写，等下一轮再问**——但落点不同，落点决定
/// 归属与升级路径：判红是这一版的属性（等人写代码），另外两种不是。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CiHoldKind {
    /// 上游给出了明确的失败结论。
    Red,
    /// 上游有检查，至少一条还没跑完：这一版有没有判词，此刻还没产生。
    Pending,
    /// 这一问没发生：连不上、body 读不出、预算内没答。
    Unasked,
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
            ci_ask: None,
            // 用例里不读跑用例这台机器的负载：同一条判据在一台空闲的机器上与一台
            // 正在编译的机器上会给出两种类别，而这份用例要锁的是判据本身。要用一个
            // 固定的宿主读数，用例显式调 `with_host_run_queue`。
            host_run_queue: if cfg!(test) {
                no_host_reading
            } else {
                read_host_run_queue_now
            },
        }
    }

    /// 挂上"动手之前复问上游 CI"这一趟。只有从命令行起的 Job 会调它。
    pub(crate) fn with_ci_ask(mut self, ask: UpstreamCiAsk) -> Self {
        self.ci_ask = Some(ask);
        self
    }

    /// 换掉宿主运行队列的读数来源。只给用例用。
    #[cfg(test)]
    pub(crate) fn with_host_run_queue(mut self, read: fn() -> Option<HostRunQueue>) -> Self {
        self.host_run_queue = read;
        self
    }

    pub(crate) async fn run_kubectl(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let cmdline = format!("kubectl {}", args.join(" "));
        // 动词与实参之外只加命名空间。**不加 `--request-timeout`**：实测这个
        // k3s 编出来的 kubectl（宿主 `/usr/local/bin/k3s` 挂进来的那一份）一旦
        // 带上这个旗标——`=` 形式或空格形式、放在动词前后都一样——就不再读
        // in-cluster 配置，直接回落到 `http://localhost:8080`，而环境变量与投影
        // 令牌都好好地在（见 [`CLUSTER_CONFIG_MISSING_MARKER`] 记的那一段）。
        // 同一条命令去掉旗标就正常。于是"子进程自己带死线"这条路在这个二进制上
        // 走不通，改用下面的输出落盘：外层定时器照样兜底，但子进程在死之前写下
        // 的话不会再跟着它一起消失。
        let mut full: Vec<&str> = vec!["-n", &self.ns];
        full.extend_from_slice(args);
        let (out_path, err_path) = kubectl_output_paths();
        let spawned = (|| -> std::io::Result<tokio::process::Child> {
            let out = std::fs::File::create(&out_path)?;
            let err = std::fs::File::create(&err_path)?;
            tokio::process::Command::new(&self.kubectl)
                .args(&full)
                .stdout(out)
                .stderr(err)
                .kill_on_drop(true)
                .spawn()
        })();
        let mut child = match spawned {
            Ok(child) => child,
            Err(e) => {
                let _ = std::fs::remove_file(&out_path);
                let _ = std::fs::remove_file(&err_path);
                return Err(SFError::IO(format!(
                    "{OBSERVATION_TOOL_MARKER}: cannot run {}: {e}",
                    self.kubectl
                )));
            }
        };
        let waited = tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait()).await;
        let timed_out = waited.is_err();
        if timed_out {
            // 先杀再读：让它停手，否则读到的是一份还在长的文件。
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        let stdout = std::fs::read_to_string(&out_path).unwrap_or_default();
        let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
        let _ = std::fs::remove_file(&out_path);
        let _ = std::fs::remove_file(&err_path);
        let status = match waited {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => {
                return Err(SFError::IO(format!(
                    "{OBSERVATION_TOOL_MARKER}: cannot run {}: {e}",
                    self.kubectl
                )))
            }
            // 到点被杀：把子进程已经写下的话带上。这正是这条路径存在的理由——
            // 一次九次尝试、三百多秒的停摆如果只留一句「超时」，事后没人说得清
            // 是 apiserver 卡住、Pod 的网络栈坏了，还是配置根本没读进来。
            Err(_) => {
                return Err(SFError::IO(format!(
                    "{} (kubectl was killed{}): {}",
                    crate::cmd_timeout::timed_out(&cmdline, timeout_secs),
                    if stderr.trim().is_empty() {
                        " before it wrote anything"
                    } else {
                        ""
                    },
                    stderr_tail(&stderr),
                )))
            }
        };
        if !status.success() {
            // 回落到 localhost:8080 说明 kubectl 这回一份配置都没读到：可能是这个
            // 进程手里真的没有、可能是那份配置指的就是本机回环、也可能是命令行上多了
            // 个让它不读配置的东西。把四处来源的读数附在后面，判词才有机会自己把这三
            // 种分开——否则读的人还得进一次 Pod，而 Pod 那时已经没了。
            let note = if stderr.contains(LOCALHOST_FALLBACK_ADDRESS) {
                format!(" ({})", ClusterConfigSources::observe().describe())
            } else {
                String::new()
            };
            return Err(SFError::IO(format!("{cmdline} failed: {stderr}{note}")));
        }
        Ok(stdout.trim().to_string())
    }

    /// 落一份清单文件（支撑集、目标部署清单）——走**服务端** apply，与发布那一步
    /// （`apply_stdin_with`）同一个理由。
    ///
    /// 客户端 apply 会把整个对象序列化进
    /// `kubectl.kubernetes.io/last-applied-configuration` 注解，而注解与对象上别的
    /// 一切**合计**有 262144 字节的硬顶。支撑集里装的正是随每一轮发布一起长的配置
    /// 对象：实测那个 `cogneva-json` ConfigMap 一个就是 196164 字节，其中约三分之二
    /// 是告警规则（104 条、每条约 1.3 KB），也就是离顶只剩五十来条规则。真到了那一
    /// 天，装不下它的客户端 apply 会让每一轮滚动都在**第一个动作**上中止，而这一步
    /// 失败按「发布集自己坏了」记成版本缺陷——一份好清单带着"这一版坏了"的判词停下，
    /// 而修法（改交付方式）恰好在被它自己挡住的那条路上。发布那一步已经因为同一个顶
    /// 改走服务端，这里是同一段路的下一截。
    ///
    /// `--force-conflicts` 的对手只可能是这边自己留下的旧对象（安装期的客户端 apply、
    /// 以及按 label 的尽力清扫可能没删掉的残件），冲突方不是别的写者——所以接管是对的。
    /// 缺了它，一条清理失败就会把发布永久卡死，而那正是这条路径要修的那种故障。
    async fn apply_manifest_file(&self, path: &str, timeout_secs: u64) -> SFResult<String> {
        self.run_kubectl(
            &["apply", "-f", path, "--server-side", "--force-conflicts"],
            timeout_secs,
        )
        .await
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
    /// Whether this target's new pod would fail admission against the ceilings
    /// the cluster enforces right now, judged before anything is applied.
    ///
    /// A resource governance object travels in the support bundle and is applied
    /// before any target, so the ceiling this revision declares is in the
    /// cluster by the time this runs. That is why the check reads the live
    /// quota rather than the declaration: it answers "does the workload fit
    /// what is enforced now", which is the question admission asks. Two
    /// failures still land here -- a revision that declares a workload larger
    /// than the ceiling it ships alongside itself, and a support apply that was
    /// skipped or refused (a ceiling below current usage is rejected by the API
    /// server, and the support apply fails before anything rolls). With a
    /// rolling strategy that takes the old pod down first, either would be an
    /// outage. This is the gate the drift reading is not: it holds the rollout
    /// -- the old pod stays -- while the new pod would not fit, and lets it
    /// through once the ceiling catches up.
    ///
    /// `Some(reason)` is a positive finding, named for the object and resource
    /// that decided it. `None` means it fits, no quota constrains the raised
    /// resource, or a side could not be read -- an unreadable figure holds
    /// nothing back.
    async fn admission_would_fail(
        &self,
        plan: &RolloutPlan,
        t: &RolloutTarget,
    ) -> Option<AdmissionShortfall> {
        // The set-image fallback leaves the pod's resources as they are, so
        // there is nothing to compare; only a manifest that rewrites the
        // workload can move its limits.
        let dir = plan.manifests_dir.as_ref()?;
        let key = target_manifest_key(&t.deployment);
        let text = tokio::fs::read_to_string(Path::new(dir).join(&key))
            .await
            .ok()?;
        let docs = split_docs(&text, &key).ok()?;
        let new_limits = docs.iter().find_map(pod_template_limits)?;

        let live_text = self
            .run_kubectl(&["get", "deployment", &t.deployment, "-o", "json"], 60)
            .await
            .ok()?;
        let live_json: serde_json::Value = serde_json::from_str(&live_text).ok()?;
        let surges = deployment_surges(&live_json);
        let live_doc = serde_yaml::to_value(&live_json).ok()?;
        let old_limits = pod_template_limits(&live_doc)?;

        let quota_text = self
            .run_kubectl(&["get", "resourcequota", "-o", "json"], 60)
            .await
            .ok()?;
        let quotas = crate::governance_drift::live_quota_usage(&quota_text).ok()?;
        let verdict = first_admission_shortfall(&quotas, &old_limits, &new_limits, surges);
        verdict.as_ref()?;
        // A refusal is only news if the usage behind it is the usage that will
        // still be there when the new pod is admitted. Earlier targets in this
        // same run stop their own pods, and the quota keeps charging those until
        // they are actually gone -- so the reading taken the instant after they
        // finish is dominated by usage this run just created and is about to
        // give back. 实测 2026-10-26: the main deployment was refused against a
        // usage figure this run had itself just inflated by the pods it replaced,
        // and the same quota measured comfortably under the ceiling a minute
        // later, once they had left. Waiting only when the reading already says
        // "no" costs nothing on the rollouts that were never in doubt.
        if !self.wait_out_terminating_pods().await {
            return verdict;
        }
        let Ok(quota_text) = self
            .run_kubectl(&["get", "resourcequota", "-o", "json"], 60)
            .await
        else {
            return verdict;
        };
        let Ok(quotas) = crate::governance_drift::live_quota_usage(&quota_text) else {
            return verdict;
        };
        first_admission_shortfall(&quotas, &old_limits, &new_limits, surges)
    }

    /// Wait, up to a couple of minutes, for pods that are already shutting down
    /// to leave.
    ///
    /// Returns whether the wait happened at all: `true` means the namespace had
    /// pods with a deletion timestamp and some of them went away, so the caller
    /// should read its figures again. `false` covers both "nothing was
    /// terminating" and "they were still there when the budget ran out" -- in
    /// the second case the reading is the same one that was already taken.
    ///
    /// Bounded on purpose: a pod that never leaves must not turn into a rollout
    /// that never starts. Unreadable listings return immediately, the same way
    /// an unreadable figure holds nothing back elsewhere in this judgement.
    async fn wait_out_terminating_pods(&self) -> bool {
        // The only thing bounding how long a terminating pod may stay is its own
        // grace period, which nothing in this tree sets and so is the platform
        // default of 30s. A budget of a few grace periods outlasts the pods this
        // is waiting for even when their shutdown runs the clock out. Being
        // generous is cheap here because the wait is paid at most once and only
        // on a path that already said "no": if the ceiling really is exceeded,
        // this attempt is refused anyway, so the wait costs seconds, while
        // giving up early costs a whole retry round.
        /// How long one attempt waits for a full ceiling to drain, and how often it looks
        /// while waiting. Waiting on a path that already said no costs seconds; giving up
        /// early costs a whole retry round.
        const BUDGET: std::time::Duration = std::time::Duration::from_secs(120);
        const POLL: std::time::Duration = std::time::Duration::from_secs(5);
        let started = std::time::Instant::now();
        let mut waited = false;
        loop {
            let Ok(text) = self.run_kubectl(&["get", "pods", "-o", "json"], 30).await else {
                return waited;
            };
            let Some(names) = terminating_pods(&text) else {
                return waited;
            };
            if names.is_empty() {
                return waited;
            }
            if started.elapsed() >= BUDGET {
                tracing::warn!(
                    still_terminating = names.len(),
                    waited_secs = started.elapsed().as_secs(),
                    "pods were still terminating when the admission reading was due; judging on \
                     the usage the namespace still charges for them"
                );
                return waited;
            }
            waited = true;
            tokio::time::sleep(POLL).await;
        }
    }

    /// 这次 apply 会引用的 ConfigMap，必须要么随本次 apply 一起来，要么集群里已经在；
    /// 缺一个就扣住，回到 `Some(说明)`。
    ///
    /// 为什么这是一道门而不是一句日志：工作负载按名读的 ConfigMap 缺失时**不报错**。
    /// 键读取上的 `optional` 与卷的缺失都让那个引用静默降级，Pod 照常 Running、就绪
    /// 判据全绿，只有下游功能哑掉，而日志里那句话与"功能没配"逐字同形。
    ///
    /// 边界与理由，写在这里而不是留给读的人猜：
    /// - **只查 ConfigMap**，因为这是这个 SA 有权读的一类（`deploy/k3s/evolution-rbac.yaml`
    ///   授了 `configmaps` 的 `get`）。Secret 不在其列——那个 SA 按设计没有任何 Secret
    ///   动词，读不了；把读不到的引用放进这道门，它会按下面"读不到集群就回答存在"的
    ///   纪律变成一次静默通过，比不查还坏。Secret 的"引用了但没人建"因此不在滚动侧能
    ///   问的问题里，归安装面判。
    /// - 扣住的是**本次要 apply 的这批文档里引用了、而这批文档自己不带**的对象：它就
    ///   不在 apply 会 upsert 的那一集里，所以 apply 不会把它建出来，缺了就得先扣住，
    ///   等建它的那一份（安装面、或另一个 apply）就位。扣住发生在任何 apply 之前，集群
    ///   一个字节都没变，旧 Pod 留着服务，下一轮就位后自己继续。
    ///
    /// 判据两侧都不来自被判方：引用集来自本次要 apply 的这批文档，存在性来自一次
    /// 集群读。读不到集群时回答"存在"（扣不住东西）——与 [`Self::admission_would_fail`]
    /// 同一条纪律：读不到的读数不该扣留发布，真正的集群不可达会在随后的 apply 上报得
    /// 更准、指向更近。
    async fn missing_reference_holds(&self, docs: &[serde_yaml::Value]) -> Option<String> {
        for name in unresolved_configmaps(docs) {
            let exists = match self
                .run_kubectl(&["get", "configmap", &name, "-o", "name"], 60)
                .await
            {
                Ok(_) => true,
                Err(e) => {
                    let msg = e.to_string();
                    !(msg.contains("NotFound") || msg.contains("not found"))
                }
            };
            if !exists {
                return Some(format!(
                    "a workload references ConfigMap/{name}, which this rollout neither applies \
                     nor finds in the cluster; holding before anything is applied so the old pods \
                     keep serving, until whatever owns it creates it"
                ));
            }
        }
        None
    }

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
                    &staged_manifest_path(&format!("target-{}", t.deployment)),
                )?;
                if let Some(staged_path) = staged {
                    let path_arg = staged_path.to_string_lossy().to_string();
                    info!(
                        deployment = %t.deployment,
                        source = %path.display(),
                        manifest = %path_arg,
                        "mainline rollout: apply target manifest"
                    );
                    let outcome = async {
                        self.clear_superseded_env_values(&staged_path).await?;
                        self.apply_manifest_file(&path_arg, 60).await.map(|_| ())
                    }
                    .await;
                    // 这份暂存件只服务这一次 apply，名字又按实例唯一（不再被下一轮覆盖），
                    // 所以用完就删：留着就是随每一次滚动在临时目录里堆一份，而没有任何
                    // 回收方认得它们。失败那一路也删——它派生自包里那份清单，包还在。
                    let _ = tokio::fs::remove_file(&staged_path).await;
                    return outcome;
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
    pub(crate) async fn live_config_consumers(&self) -> SFResult<Vec<ConfigConsumer>> {
        let mut out = Vec::new();
        // Every kind this crate can address, not the ones a roll happens to
        // target: this reading answers "who reads this configuration", and a
        // kind left out of it makes the answer "nobody" -- which the caller
        // treats as a failed delivery, so a narrow reading here turns a
        // workload that does read the document into a rollout error.
        for entry in &WORKLOAD_KINDS {
            let jsonpath = config_consumer_jsonpath(entry.pod_template);
            let readout = self
                .probe(
                    &["get", entry.resource, "-o", &jsonpath],
                    30,
                    self.rollout_timeout_secs,
                )
                .await?;
            out.extend(config_consumers(&readout, entry.resource));
        }
        Ok(out)
    }

    /// 让这次 apply 改到的配置真正生效：内容变了、而进程只在启动时读它，工作负载
    /// 的 spec 没动、代数不变，逐目标的滚动与支撑等待都不会碰它——文件是新的、进程
    /// 还是旧的、记录说已生效。这里按内容差自己算出该滚谁，把 restartedAt 打上去
    /// （Pod 模板变了才会滚出新副本），返回滚动名单。
    ///
    /// 读不到内容（None）时按"每份都变了"处理：说不清就有多滚一次，不留静默失效。
    ///
    /// `carried` 里的工作负载不在这里滚：它们的目标清单这次 apply 会落到集群上，
    /// 清单里带着与本次 ConfigMap 内容对应的 `checksum/config` 注解，被逐目标
    /// 循环滚起来时自然读到新配置；替它先滚一遍则会在准入账上多叠一份 surge
    /// （见 [`is_manifest_carried`]）。
    ///
    /// 清单没到手的目标不在 `carried` 里，照旧在这里滚：它们只会走 `set image`，
    /// Pod 模板上唯一变的是镜像，只改配置的那一版里镜像没变，靠它自己什么都不会发生。
    pub(crate) async fn roll_config_change_consumers(
        &self,
        before: Option<&BTreeMap<String, serde_json::Value>>,
        after: Option<&BTreeMap<String, serde_json::Value>>,
        carried: &[String],
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
                // Which consumers get rolled is narrower than which ones are
                // read, and it is the kind that decides: a pod that keeps
                // running is still serving the configuration it read at
                // startup, while a kind whose pods are created per run reads
                // the new content on its next run and a stamp would replace
                // nothing. The reading above still covers both, so the answer
                // to "does anything read this document" is not decided by
                // whether a roll is worth doing.
                let kind = workload_kind_of_resource(&consumer.kind).ok_or_else(|| {
                    SFError::Config(format!(
                        "config effect: {} is addressed by a resource word no workload kind \
                         declares, so this is not a restart this crate can reason about",
                        consumer.display()
                    ))
                })?;
                if !kind.long_lived_pods {
                    continue;
                }
                // 目标自己的滚动会带上新的 checksum/config 注解，替它先滚一遍
                // 只是同一台工作负载叠两份 surge，准入账按「一次只滚一台」算。
                if is_manifest_carried(&consumer.kind, &consumer.name, carried) {
                    continue;
                }
                // 打在 apply 之后的支撑快照之前：这样它是"这次 apply 动过的工作负载"
                // 之一，由紧跟着的支撑等待一并等它回就绪。
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
    pub(crate) async fn support_workloads(&self) -> SFResult<Vec<SupportWorkload>> {
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

    /// 上一版发布的支撑清单原文。取**最新的、内容与本次不同的那一份**：包按 rev
    /// 各存一份、保留若干代，所以「上一版」不需要谁告诉它，读第二新的那一份就是。
    /// 内容相同的不算上一版——一份没变的清单说不出任何一次删除。
    ///
    /// 「读不到」与「没有上一版」分得开：前者把错误交回调用方留一条日志，后者是
    /// 首装。两者都不改变这一轮的去向——集群里连包都列不出来时拦下每一次滚动，
    /// 代价比这条判据挡不住那一次大——但它们在日志里必须是两句话。
    async fn previous_support_bundle(&self, current: &str) -> SFResult<Option<String>> {
        let text = self
            .probe(
                &[
                    "get",
                    "configmap",
                    "-l",
                    &format!("{}=mainline-manifests", cog_core::topology::LABEL_COMPONENT),
                    "-o",
                    "json",
                ],
                60,
                self.rollout_timeout_secs,
            )
            .await?;
        let listing: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| SFError::IO(format!("manifest bundle listing: {e}")))?;
        let Some(items) = listing["items"].as_array() else {
            return Ok(None);
        };
        let mut items: Vec<(&str, &str)> = items
            .iter()
            .filter_map(|item| {
                let created = item["metadata"]["creationTimestamp"].as_str()?;
                let yaml = item["data"]["support.yaml"].as_str()?;
                Some((created, yaml))
            })
            .collect();
        items.sort_by(|a, b| b.0.cmp(a.0));
        Ok(items
            .into_iter()
            .map(|(_, yaml)| yaml)
            .find(|yaml| *yaml != current)
            .map(str::to_string))
    }

    /// 这次 apply 要删字段的对象，连同它们此刻的代数。取在 apply **之前**：事后
    /// 没有对照，「删没落」与「本来就长这样」就分不开了。
    async fn field_removals_to_verify(&self, current: &str) -> Vec<(FieldRemoval, Option<i64>)> {
        let previous = match self.previous_support_bundle(current).await {
            Ok(Some(previous)) => previous,
            // 没有上一版：首装，没有「相对上一版删掉了什么」可言。
            Ok(None) => return Vec::new(),
            // 读不到上一版：这一轮照滚（列不出包就拦下每一次滚动的代价更大），
            // 但这一次"没查"要留在日志里，不能与"查过、没有删除"同形。
            Err(e) => {
                warn!(
                    error = %e,
                    "mainline rollout: cannot read the previous manifest bundle; this apply's \
                     field removals are not verified"
                );
                return Vec::new();
            }
        };
        let removals = objects_that_lost_fields(&previous, current, &self.ns);
        if removals.is_empty() {
            return Vec::new();
        }
        info!(
            objects = %removals
                .iter()
                .map(|r| format!("{}/{}", r.resource, r.name))
                .collect::<Vec<_>>()
                .join(","),
            "mainline rollout: this apply removes fields from live objects; their generation must move"
        );
        let mut out = Vec::new();
        for r in removals {
            let before = self.generation_of(&r).await;
            out.push((r, before));
        }
        out
    }

    /// 一个对象的 `metadata.generation`。读不到就是读不到——`None` 与「代数为 0」
    /// 在判据上必须分得开。
    async fn generation_of(&self, r: &FieldRemoval) -> Option<i64> {
        let text = self
            .probe(
                &[
                    "get",
                    &r.resource,
                    &r.name,
                    "-n",
                    &r.namespace,
                    "-o",
                    "jsonpath={.metadata.generation}",
                ],
                30,
                self.rollout_timeout_secs,
            )
            .await
            .ok()?;
        text.trim().parse::<i64>().ok()
    }

    /// apply 之后再读一次代数，挑出「清单里删了字段、对象却没变」的那些。
    async fn removals_not_taken(&self, seen: &[(FieldRemoval, Option<i64>)]) -> Vec<FieldRemoval> {
        let mut reread = Vec::new();
        for (r, before) in seen {
            let after = self.generation_of(r).await;
            reread.push((r.clone(), *before, after));
        }
        removals_that_did_not_land(&reread)
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
                    .run_kubectl(&["get", w.kind, &w.name, "-o", settle_jsonpath(w.kind)], 30)
                    .await?;
                if support_settled(w.kind, &readout) == Some(true) {
                    continue;
                }
                last = format!(
                    "{} generation|observed|want|ready|updated|running={readout}",
                    w.display()
                );
                still.push(w);
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                let mut causes: Vec<String> = Vec::new();
                for w in &pending {
                    causes.push(self.support_workload_cause(w).await);
                }
                return Err(SFError::Agent(format!(
                    "support workload this rollout restarted did not become ready within {}s: \
                     {last}{SUPPORT_CAUSE_SEPARATOR}{}",
                    self.startup_timeout_secs,
                    causes.join("; ")
                )));
            }
            tokio::time::sleep(Duration::from_secs(ROLLOUT_POLL_SECS)).await;
        }
        Ok(())
    }

    /// 判死那一刻，一个还没就绪的支撑工作负载的现场。
    ///
    /// 只在这条路径上读：正常滚动每轮多两次 kubectl 是白花的，而病因只有判死时
    /// 才需要写进记录。读数贴进判词的分隔符之后，**不进判据**（见
    /// [`primary_message`]）。
    ///
    /// 读不到就如实报读不到：这一段是附加证据，读失败既不能盖掉「没就绪」这个主
    /// 判据，也不能折成「没起来」。
    async fn support_workload_cause(&self, w: &SupportWorkload) -> String {
        let object = match self
            .run_kubectl(&["get", w.kind, &w.name, "-o", "json"], 30)
            .await
        {
            Ok(text) => text,
            Err(e) => return format!("{}: cause unreadable ({e})", w.display()),
        };
        let Some(selector) = support_pod_selector(&object) else {
            return format!(
                "{}: cause unreadable (no label selector to list its pods by)",
                w.display()
            );
        };
        let pods = match self
            .run_kubectl(&["get", "pods", "-l", &selector, "-o", "json"], 30)
            .await
        {
            Ok(text) => text,
            Err(e) => return format!("{}: cause unreadable ({e})", w.display()),
        };
        let reading = match support_pod_causes(&pods) {
            SupportCause::Read(text) => text,
            SupportCause::NoPods => "no pod exists".to_string(),
            SupportCause::Unreadable(why) => format!("cause unreadable ({why})"),
        };
        format!("{}: {reading}", w.display())
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
    pub(crate) async fn clear_superseded_env_values(&self, manifest: &Path) -> SFResult<()> {
        let text = tokio::fs::read_to_string(manifest)
            .await
            .map_err(|e| SFError::IO(format!("read {}: {e}", manifest.display())))?;
        // 逐文档读，**坏文档不许"跳过它继续下一条"**：`serde_yaml` 解析出错时交回的
        // 迭代项不推进游标，同一条错误文档会被反复送回来，一次 `continue` 就是永不返回
        // 的循环——烧满一核、零系统调用、零日志，在观测面上与"很慢"同形。`split_docs`
        // 把错误 `?` 出去，于是读不出来就不交付这一份：随后的 apply 本就会被准入拒绝，
        // 而那次拒绝会被读成"这一版坏了"，比在这里停下更贵。
        let origin = manifest.display().to_string();
        for doc in split_docs(&text, &origin)? {
            let desired = serde_json::to_value(&doc)
                .map_err(|e| SFError::Config(format!("{origin}: document is not JSON: {e}")))?;
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
        let jsonpath = crate::deployment_image::container_image_query(&t.container);
        let img = self
            .run_kubectl(&["get", "deployment", &t.deployment, "-o", &jsonpath], 30)
            .await?;
        crate::deployment_image::container_image(&img, &t.deployment, &t.container)
    }

    /// 带集群访问容忍的查询：把「查不到集群」与「查到的结果不健康」分开。
    /// 前者就地在 budget_secs 内按轮询节拍重试，不产生任何滚动结论；预算耗尽
    /// 仍不可达才返回带标记的错误，调用方据此判「本轮没有观测到任何东西」。
    /// 后者（查询成功但输出表明版本有问题）立即返回，那是真的版本结论。
    /// 非集群访问类的失败（选择器非法、资源不存在）同样立即返回——它们也是
    /// 明确的观测结果，不该被静默重试吞掉。
    pub(crate) async fn probe(
        &self,
        args: &[&str],
        timeout_secs: u64,
        budget_secs: u64,
    ) -> SFResult<String> {
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
        // 整段预算里有没有见过属于新版本的一个 Pod。没有的话，超时那一刻能采到的
        // 现场（Pod 等待原因、RS 拒绝条件）全是**旧版本**的，下面那几条环境判据
        // 一条都够不着——预算到期只会走到最后的通用分支，被记成"这一版坏了"。
        let mut new_pod_seen = false;
        // 整段预算里宿主排队排了多久、最挤的一刻有多挤。只在预算到期判死时用它
        // 回答一个问题：这段时间到底是这一版没起来，还是这台机器没给它时间。
        // 正常路径上它只是每轮一次 `/proc` 读，不进日志。
        let mut host_polls = 0u32;
        let mut queued_polls = 0u32;
        let mut loaded_peak: Option<HostRunQueue> = None;
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
                        ROLLOUT_STATUS_JSONPATH,
                    ],
                    30,
                )
                .await
            {
                Ok(out) => {
                    observed_ever = true;
                    // 只认控制器处理过当前这一代模板的读数：apply 之后的第一眼
                    // 往往早于控制器，那一段 `updatedReplicas` 仍是上一代的账
                    // （上一轮滚完时它恒等于副本数），拿它置位会让这条"新版本
                    // 一个 Pod 都没建成"的出口**整段预算都够不着**——而它正是
                    // 为这一种现场写的。
                    new_pod_seen |= generation_observed(&out) && updated_pod_count(&out) > 0;
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
                                        let err =
                                            self.readiness_gate_error(t, budget.as_secs()).await;
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
            // 判死那一刻的读数只说明最后一秒，而这段预算要回答的是「整段里有没有
            // 给过它时间」，所以要记下排队的**采样点占比**，不是一次尖峰：一次
            // 尖峰压不住一段启动，半程以上都在排队才是这台机器没给时间。
            if let Some(rq) = (self.host_run_queue)() {
                host_polls += 1;
                if rq.is_loaded() {
                    queued_polls += 1;
                    if loaded_peak.is_none_or(|p| rq.load1 / rq.cores > p.load1 / p.cores) {
                        loaded_peak = Some(rq);
                    }
                }
            }
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
                let suffix = scene_suffix(&diagnosis);
                // 排不上队与准入被拒：都只有"这次上线没动放置面"才归环境。
                let blocked = self.sample_unschedulable_pods(t).await;
                let now_shape = self.current_placement_shape(t).await.ok();
                let environment = environment_class(&blocked, prev_shape, now_shape.as_deref());
                let denied = admission_rejection(&replicasets, prev_shape, now_shape.as_deref());
                // 卡在拉镜像上：整段预算里新版本一次都没跑起来，这次失败没有观测到
                // 版本，说不出它的好坏。先取下来（临时值不能活到下面的格式化里），
                // 命不命中都在这里判，判词里带上是哪一个等待态。
                let pull_blocked =
                    image_pull_blocked(&self.waiting_states(t).await).map(|r| r.to_string());
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
                } else if !new_pod_seen {
                    // 整段预算里没建成过新版本的 Pod：能采到的现场都是旧版本的，
                    // 上面几条（排不上队、准入被拒、拉取卡住）都要求有一个属于
                    // 新版本的现场才能成立。这一档说不出新版本的好坏——没有回滚
                    // 对象，也没有任何新版本的证据——所以按环境类处置、不回滚。
                    SFError::IO(format!(
                        "{NEW_REVISION_NEVER_RAN_MARKER}: rollout of deployment/{} did not \
                         complete within {}s and its updatedReplicas never left 0 — no pod of \
                         the new revision was ever created, so this timeout says nothing about \
                         it (last: {note}{suffix})",
                        t.deployment, budget
                    ))
                } else if let Some(rq) =
                    loaded_peak.filter(|_| queued_polls.saturating_mul(2) >= host_polls)
                {
                    // 半程以上的采样点都在排队：这段时间测的是别人的负载，不是
                    // 这一版的启动。留在集群上不回滚，宿主松下来它自己会起来。
                    SFError::IO(format!(
                        "{HOST_LOADED_MARKER}: rollout of deployment/{} did not complete \
                         within {}s ({phase} phase) — the host's run queue was past one task per \
                         core on {queued_polls} of {host_polls} samples, peaking at load1 {:.2} \
                         against {} cores, so the new pod spent the budget queued behind work \
                         that is not part of this revision instead of running, and this timeout \
                         says nothing about the revision (last: {note}{suffix})",
                        t.deployment, budget, rq.load1, rq.cores
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

    /// 本次滚动的 Pod 上所有等待态，init 容器与主容器一并取：init 容器拉不到
    /// 镜像时主容器只报 PodInitializing，只看主容器会把这一档整个漏掉。查询失败
    /// 返回空表——滚动交替期 containerStatuses 本就可能缺失，由调用方的超时兜底。
    ///
    /// 每一态都把**那个容器自己写下的死因**一并取回：判死那一刻，这条读数往往是
    /// 崩溃版本留下的唯一一句话（见 [`Self::fatal_pod_state`]）。
    async fn waiting_states(&self, t: &RolloutTarget) -> Vec<WaitingState> {
        let selector = pod_selector(&t.name, &t.component);
        let out = match self
            .run_kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{range .status.initContainerStatuses[*]}{.state.waiting.reason}|{.lastState.terminated.message}{\"\\n\"}{end}{.status.containerStatuses[0].state.waiting.reason}|{.status.containerStatuses[0].lastState.terminated.message}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
        {
            Ok(out) => out,
            Err(_) => return Vec::new(),
        };
        pod_waiting_states(&out)
    }

    /// 只查致命等待态（配置错误、CrashLoop、镜像引用非法），查询临时失败返回
    /// Ok——由调用方的超时与后续 pods_healthy 兜底，这里只负责让"必死"的滚动
    /// 快速失败。拉取失败不在这里判（见 IMAGE_PULL_WAITING_REASONS）。
    ///
    /// 判词接上容器自己写下的死因（分隔符之后、不进判据，见 [`primary_message`]）：
    /// 另一个会读死因的判死（`rollout_pods_fatal`）只在一个"已收敛"的分支里跑，
    /// 而致命等待态的容器一律未就绪，那条判据此刻恒假——崩溃的滚动只能走到这
    /// 一条，容器留下的那句话就只剩这一个落点。
    pub(crate) async fn fatal_pod_state(&self, t: &RolloutTarget) -> SFResult<()> {
        for state in self.waiting_states(t).await {
            if !FATAL_WAITING_REASONS.contains(&state.reason.as_str()) {
                continue;
            }
            let mut text = format!(
                "pod of deployment/{} in fatal waiting state {}",
                t.deployment, state.reason
            );
            if !state.cause.is_empty() {
                text.push_str(SUPPORT_CAUSE_SEPARATOR);
                text.push_str(&bounded(&state.cause, SUPPORT_CAUSE_MESSAGE_CHARS));
            }
            return Err(SFError::Agent(text));
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
                     {.metadata.deletionTimestamp}|\
                     {.status.containerStatuses[0].lastState.terminated.message}{\"\\n\"}{end}",
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
    pub(crate) async fn pods_healthy(&self, t: &RolloutTarget) -> SFResult<()> {
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
            SFError::Agent(format!("{msg}{}", scene_suffix(&diagnosis)))
        }
    }

    /// 一份清单文档里**这次 apply 会改写**的那些引用，在 apply **之前**的现场值。
    ///
    /// 两个面各读一次，缺一个都等于回滚只还了一半：声明面取清单文档自己（它会
    /// upsert 出哪些容器、每个容器声明成什么镜像），取值面现场读同一个容器的现值。
    ///
    /// 每个工作负载一次读取，不按容器逐个读——同一个 pod 模板的值要成组取，否则两次
    /// 读取之间发生的变化会被拼成一份不存在的现场。
    ///
    /// 现场值恰好等于清单声明的值 ⇒ 这次 apply 不会改动它，不记（记了也只是
    /// 一条无事的 `set image`）。现场没有这个容器 ⇒ 它是这一版**新加**的引用，没有
    /// 前值可还，也不记。读不到就报错，由调用方在动任何东西之前中止。
    pub(crate) async fn snapshot_rewritten_refs(
        &self,
        yaml_text: &str,
        origin: &str,
    ) -> SFResult<Vec<RewrittenRef>> {
        let declared = bundle_declared_containers(yaml_text)?;
        let mut workloads: Vec<(String, String)> = Vec::new();
        for r in &declared {
            if !workloads
                .iter()
                .any(|(res, n)| res == &r.resource && n == &r.name)
            {
                workloads.push((r.resource.clone(), r.name.clone()));
            }
        }
        let mut out: Vec<RewrittenRef> = Vec::new();
        for (resource, name) in &workloads {
            // 名字问集群、不问清单：对象可能还没建（首次安装），`--ignore-not-found`
            // 让"不存在"退 0 且无输出，与"读不到"分开——后者必须是错误。
            let Some(entry) = workload_kind_of_resource(resource) else {
                // 快照按声明面走，资源词由上面那张表给出，正常到不了这里；真到了
                // 就当作读不到（一次变更都不许发生），而不是静默少记一个引用。
                return Err(SFError::Config(format!(
                    "{origin}: {resource}/{name} is not a workload kind this snapshot can read"
                )));
            };
            let spec = format!(".{}", entry.pod_template);
            let jsonpath = format!(
                "jsonpath={{range {spec}.containers[*]}}{{.name}}={{.image}}{{\"\\n\"}}{{end}}{{range {spec}.initContainers[*]}}{{.name}}={{.image}}{{\"\\n\"}}{{end}}"
            );
            let readout = self
                .run_kubectl(
                    &["get", resource, name, "--ignore-not-found", "-o", &jsonpath],
                    30,
                )
                .await
                .map_err(|e| {
                    SFError::IO(format!(
                        "{origin}: reading the references of {resource}/{name}: {e}"
                    ))
                })?;
            let live: Vec<(&str, &str)> = readout
                .lines()
                .filter_map(|l| l.split_once('='))
                .map(|(n, i)| (n.trim(), i.trim()))
                .collect();
            for r in declared
                .iter()
                .filter(|r| &r.resource == resource && &r.name == name)
            {
                let Some((_, image)) = live.iter().find(|(n, _)| *n == r.container) else {
                    info!(
                        workload = %format!("{resource}/{name}"),
                        container = %r.container,
                        "mainline rollout: this revision adds a reference that has no previous image to restore"
                    );
                    continue;
                };
                if image.is_empty() || *image == r.image {
                    continue;
                }
                out.push(RewrittenRef {
                    resource: resource.clone(),
                    name: name.clone(),
                    container: r.container.clone(),
                    image: image.to_string(),
                });
            }
        }
        if !out.is_empty() {
            info!(
                origin = %origin,
                references = out.len(),
                "mainline rollout: snapshotted the references this apply will rewrite"
            );
        }
        Ok(out)
    }

    /// 把一组引用还回 apply 之前的值，返回**没能还成的工作负载数**。
    ///
    /// 每个工作负载一条命令、一条命令里带该工作负载的每一个 `容器=镜像`：分成多条
    /// 会让同一个 pod 模板被改两次、滚两次，而第二遍只是把第一遍刚起的 Pod 再换一次。
    ///
    /// 只还镜像、不还文档：回滚是幂等 upsert 的反向，反向删改支撑拓扑比留着新拓扑
    /// 更危险（旧镜像配新支撑资源能跑）。这里还的每一个引用都是这次 apply 自己改过
    /// 的那一组，不碰别的东西。
    pub(crate) async fn restore_rewritten_refs(&self, refs: &[RewrittenRef]) -> usize {
        let mut workloads: Vec<(String, String)> = Vec::new();
        for r in refs {
            if !workloads
                .iter()
                .any(|(res, n)| res == &r.resource && n == &r.name)
            {
                workloads.push((r.resource.clone(), r.name.clone()));
            }
        }
        let mut failed = 0usize;
        for (resource, name) in workloads {
            let specs: Vec<String> = refs
                .iter()
                .filter(|r| r.resource == resource && r.name == name)
                .map(|r| format!("{}={}", r.container, r.image))
                .collect();
            let target = format!("{resource}/{name}");
            let mut args: Vec<&str> = vec!["set", "image", &target];
            args.extend(specs.iter().map(|s| s.as_str()));
            match self.run_kubectl(&args, 60).await {
                Ok(_) => info!(
                    workload = %target,
                    containers = %specs.join(","),
                    "mainline rollback: references restored to the images they had before this rollout"
                ),
                Err(e) => {
                    failed += 1;
                    warn!(
                        workload = %target,
                        error = %e,
                        "mainline rollback: could not restore these references; they stay on the failed revision"
                    );
                }
            }
        }
        failed
    }

    /// 落地之前复问上游 CI：`Some((flavor, reason))` = 这一轮不能滚，`flavor` 说
    /// 是哪一种不能滚。
    ///
    /// 判据不在这里另写一份：每个上游的结论都交给
    /// [`MainlineDeployer::platform_ci_verdict`]——同一个函数服务派发侧那两次提问，
    /// 里面就是 `fold_ci_signals`。滚动侧若自己写一份折叠规则，「一条已经给出的
    /// 失败不能被还在跑压住」这类判断就会有两个版本。
    ///
    /// 返回的原因里带着报失败的检查名：只说「有一项红」时，定位要绕到仓库外去问，
    /// 而名字在这一次读里本来就有。
    async fn upstream_ci_hold(&self) -> Option<(CiHoldKind, String)> {
        let ask = self.ci_ask.as_ref()?;
        let waiting_since = std::time::Instant::now();
        loop {
            // 整段提问有自己的上界：每次 HTTP 读自带 30 秒连接 + 60 秒读，而上游可以
            // 有多个、报失败的检查还要逐条问工作流身份，不封顶就会被一次慢网关把
            // Job 的期限吃光——那时失败会以"被判进程没跑完"的形状出现，谁也看不出
            // 是这一步。超时按「没问到」处置：这一轮不滚，下一轮再问。
            let hold = match tokio::time::timeout(
                Duration::from_secs(CI_ASK_BUDGET_SECS),
                self.ask_upstreams(ask),
            )
            .await
            {
                Ok(hold) => hold,
                Err(_) => {
                    warn!(
                        rev = %ask.rev,
                        budget_secs = CI_ASK_BUDGET_SECS,
                        "the pre-apply CI question did not answer within its budget; holding \
                         this rollout, the next round asks again"
                    );
                    return Some((
                        CiHoldKind::Unasked,
                        format!(
                            "upstream CI did not answer within {CI_ASK_BUDGET_SECS}s for {}; \
                             nothing is applied this round",
                            ask.rev
                        ),
                    ));
                }
            };
            match hold {
                Some((kind, _)) if still_worth_asking(kind, waiting_since.elapsed()) => {
                    // 这一句要印出来：一次等待与一次扣住在派发侧同形（都是这一轮
                    // 没滚），只有这一行说得清「为什么多等了这几分钟」。
                    info!(
                        rev = %ask.rev,
                        waited_secs = waiting_since.elapsed().as_secs(),
                        reask_in_secs = CI_PENDING_REASK_SECS,
                        wait_budget_secs = CI_PENDING_WAIT_SECS,
                        "upstream CI is still running on this revision; asking again before \
                         holding the round, because this answer finishes on its own"
                    );
                    tokio::time::sleep(Duration::from_secs(CI_PENDING_REASK_SECS)).await;
                }
                Some((kind, why)) if kind == CiHoldKind::Pending => {
                    // 等到了界：这一句要单独印，因为「还在跑」这一次不是判词还没产生，
                    // 而是这一轮不再等它了——两者的下游动作完全不同。
                    warn!(
                        rev = %ask.rev,
                        waited_secs = waiting_since.elapsed().as_secs(),
                        wait_budget_secs = CI_PENDING_WAIT_SECS,
                        "the upstream CI answer stayed \"still running\" past the wait bound; \
                         the wait itself is the verdict for this round"
                    );
                    return Some((kind, wait_exhausted_reason(waiting_since.elapsed(), &why)));
                }
                other => return other,
            }
        }
    }

    /// 这一版在上游各平台的 CI 结论，折成「能不能滚」。`None` = 每一个上游都答了
    /// 放行（或这一版根本没有东西可判），可以滚。
    pub(crate) async fn ask_upstreams(&self, ask: &UpstreamCiAsk) -> Option<(CiHoldKind, String)> {
        let mut failures: usize = 0;
        // 判词是失败、但上游没有逐条给名字的那几条（结论来自提交状态兜底）。
        let mut failed: Vec<String> = Vec::new();
        // 问了、但这一轮不能放行的两种：上游还在跑，以及这一问根本没有发生。
        let mut still_running: Vec<String> = Vec::new();
        let mut not_asked: Vec<String> = Vec::new();
        for up in &ask.upstreams {
            let (verdict, reason, names, excluded) =
                MainlineDeployer::platform_ci_verdict(&up.api_base, &up.repo, &ask.rev).await;
            if !excluded.is_empty() {
                // 排掉的那几条要留话：这个 rev 上游确实有一条红检查，只是那条不判
                // 这棵树，所以它没有进判词。不写这一行，事后只剩一个"放行"。
                info!(
                    rev = %ask.rev,
                    platform = %up.platform,
                    excluded = %excluded
                        .iter()
                        .map(|(n, w)| format!("{n}@{w}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    "checks that do not judge this tree were left out of the CI verdict"
                );
            }
            match verdict {
                Some(false) => {
                    failures += 1;
                    failed.extend(names.into_iter().map(|n| format!("{}:{n}", up.platform)));
                }
                Some(true) => {}
                None => {
                    // 「没有判词」不是一个结论，是三种现场：还在跑、问到了没有东西、没
                    // 问到。第三种与「绿」之间没有任何共同点，把它读成放行，就是
                    // 2026-10-27 那次带红上线的形状：安全网关正被同一个滚动换副本，两个
                    // 平台的 CI 接口都连不上（`connect_failed`），判据把「一次都没问到」
                    // 读成了「问了、没问题」，于是那个 rev 滚上去了，二十分钟后 CI 红，
                    // 而没有任何东西会在落地之后再把它撤下来。
                    match reason {
                        Some(CiNoVerdictReason::Pending) => still_running.push(up.platform.clone()),
                        Some(r) if r.is_absent_answer() => {
                            not_asked.push(format!("{}:{}", up.platform, r.label()))
                        }
                        other => info!(
                            rev = %ask.rev,
                            platform = %up.platform,
                            reason = %other.map(|r| r.label()).unwrap_or("unknown"),
                            "upstream CI answered and has nothing to judge this revision; letting the rollout continue"
                        ),
                    }
                }
            }
        }
        if failures > 0 {
            let reason = if failed.is_empty() {
                // 结论来自提交状态兜底时没有可命名的检查——那是"这个平台不逐条给名字"，
                // 不是"没有失败项"。
                format!(
                    "upstream CI reports a failure for {} (no check runs named the failure)",
                    ask.rev
                )
            } else {
                format!(
                    "upstream CI reports a failure for {}: {}",
                    ask.rev,
                    failed.join(",")
                )
            };
            return Some((CiHoldKind::Red, reason));
        }
        if still_running.is_empty() && not_asked.is_empty() {
            return None;
        }
        // 没判红但也没放行。两种现场可能同时发生（一个平台还在跑、另一个连不上），
        // 所以合成一句话说完：分成两段返回会把另一半现场丢掉。
        let mut why: Vec<String> = Vec::new();
        if !not_asked.is_empty() {
            why.push(format!("could not be asked ({})", not_asked.join(",")));
        }
        if !still_running.is_empty() {
            why.push(format!("still running on {}", still_running.join(",")));
        }
        // 归属取在场的那一种里**此刻有人能动手**的那一种：连不上要修那条出网的路，
        // 还在跑只能等。两者同时在时按前者归，后者写在这句话里，不丢。
        let flavor = if not_asked.is_empty() {
            CiHoldKind::Pending
        } else {
            CiHoldKind::Unasked
        };
        Some((
            flavor,
            format!(
                "upstream CI has no verdict for {} yet: {}; nothing is applied this round, the \
                 next round asks again",
                ask.rev,
                why.join("; ")
            ),
        ))
    }

    /// 先落地支撑清单（如随镜像下发），再按计划顺序滚动四部署，逐部署等
    /// rollout 完成，全滚完后 soak 复查；任一失败把**这次 apply 改写过的整组引用**
    /// 还原回改动之前的现场值（不用 rollout undo——多目标无事务性，undo 还会连带
    /// 回退其他字段）。只还原得动的那几处，要还原的组由 `RefSnapshots` 给。
    /// 回滚只回退 image：支撑资源不反向删改（apply 是幂等 upsert，旧镜像配新支撑
    /// 资源可运行；删改支撑资源反而可能把在跑集群打坏）。
    pub async fn run(&self, plan: &RolloutPlan) -> Result<(), RolloutFailure> {
        // 上游 CI 的复问。派发侧在派 Job 之前问过一次，但提问到落地之间隔着整段
        // 构建——镜像在这里才被拉下来、apply 在这里才发生，那段时间足够一条要红的
        // 检查跑出结论（实测一次带红上线：派发 2 分 47 秒后应用 Pod 才起来，而
        // 第一条红线在那之前 2 分 19 秒就已经存在）。所以在这个进程里、在第一处
        // 写动作之前再问一次；派发侧那一次答不出来，不代表落地时也答不出来。
        //
        // 三种回答都扣住这一轮，但落点不同。
        //
        // 只有「上游判了红」是这一版的属性；另外两种（还在跑、根本没问到）说不出这一版
        // 的任何事，可它们同样不该被读成放行——**放行的代价不对称**：把「问到了没有
        // 东西」读成放行，滚上去的版本仍然是上游如实答过的；把「一次都没问到」读成
        // 放行，滚上去的是一个没人核对过的版本，而落地之后没有任何东西会把它撤下来。
        // 2026-10-27 就是后者：安全网关正被同一个滚动换副本，两个平台的 CI 接口都
        // `connect_failed`，判据把「问不到」与「绿」并成一句，二十分钟后 CI 红。
        if let Some((flavor, reason)) = self.upstream_ci_hold().await {
            warn!(
                reason = %reason,
                flavor = ?flavor,
                "mainline rollout: holding this rollout before it starts; nothing is applied and \
                 the next round asks again"
            );
            let locus = match flavor {
                CiHoldKind::Red => FailureLocus::UpstreamCi,
                CiHoldKind::Pending => FailureLocus::UpstreamCiPending,
                CiHoldKind::Unasked => FailureLocus::UpstreamCiUnasked,
            };
            return Err(RolloutFailure::at(
                ROLLOUT_PRECHECK_STAGE,
                "",
                locus,
                true,
                SFError::Config(reason),
            ));
        }
        // 这次 apply 会改写的整组引用，两个面各一份，回滚按同一组还原。
        let mut refs = RefSnapshots::default();
        // 引用面门：这次 apply 会引用的 ConfigMap 必须要么随本次 apply 一起来、要么
        // 集群里已经在，缺一个就在这里扣住。位置必须在**第一处 apply 之前**——支撑清单
        // 是这条通道上的第一个写动作，过了它集群就不再是原样，回滚也回不到"一个字节
        // 都没动"。判据、边界与理由见 [`Self::missing_reference_holds`]。
        let mut release_docs: Vec<serde_yaml::Value> = Vec::new();
        if let Some(dir) = &plan.manifests_dir {
            let mut sources: Vec<String> = vec!["support.yaml".to_string()];
            for t in &plan.targets {
                sources.push(target_manifest_key(&t.deployment));
            }
            for key in sources {
                // 读不到就是这一份没有：目标清单本来就是可选的（缺了走 set image
                // 兜底），支撑包也可能是空的。真读不出来的那份会在它自己的 apply
                // 上报得更准。
                if let Ok(text) = tokio::fs::read_to_string(Path::new(dir).join(&key)).await {
                    if let Ok(docs) = split_docs(&text, &key) {
                        release_docs.extend(docs);
                    }
                }
            }
        }
        if let Some(reason) = self.missing_reference_holds(&release_docs).await {
            warn!(
                shortfall = %reason,
                "mainline rollout: holding this rollout before it starts; a workload references a \
                 ConfigMap that is neither in this release nor in the cluster, so the old pods stay \
                 and the next round retries once whatever owns it has created it"
            );
            return Err(RolloutFailure::at(
                ROLLOUT_PRECHECK_STAGE,
                "",
                FailureLocus::Reference,
                true,
                SFError::Config(reason),
            ));
        }
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
                // 集群此刻举着的天花板可能高于这一版声明的（版外的 Pod 抬起
                // 来还没放回去）。先读一眼再落地，否则这次 apply 会因为「把
                // hard 降到当前用量以下」被拒，而挡住的是一条本该成功的滚动。
                // 读不到就照原样 apply——老行为，不因为一次读数失败而改道。
                let text = match self
                    .run_kubectl(&["get", "resourcequota", "-o", "json"], 60)
                    .await
                {
                    Ok(live) => match crate::governance_drift::live_quota_usage(&live) {
                        Ok(live) => keep_raised_ceilings(&text, "support.yaml", &live)
                            .map_err(|e| classify_before_any_change("support", "", e))?,
                        Err(_) => text,
                    },
                    Err(_) => text,
                };
                let staged =
                    stage_rollout_manifest(&text, "support.yaml", &staged_manifest_path("support"))
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
                        // 支撑清单里指向本仓库的引用这次 apply 也会改写（pin 是价值
                        // 判断，不认对象名），所以在它之前把现场值取下来：取不到就
                        // 一次变更都不发生——回滚没有目标时，宁可不上线。
                        refs.support = self
                            .snapshot_rewritten_refs(&text, "support.yaml")
                            .await
                            .map_err(|e| classify_before_any_change("ref-snapshot", "", e))?;
                        // 这一版在清单里删掉字段的对象，以及它们此刻的代数：server-side
                        // apply 删不掉「本次没声明、由别的写者拥有」的字段，而那种删除
                        // 失败退 0、回 serverside-applied，事后只能靠代数看出来一个字都
                        // 没写进去。快照必须取在 apply 之前——它是唯一的对照。
                        let field_removals = self.field_removals_to_verify(&text).await;
                        info!(
                            source = %support.display(),
                            manifest = %support_arg,
                            "mainline rollout: applying support manifests"
                        );
                        let outcome = async {
                            self.clear_superseded_env_values(&path).await?;
                            self.apply_manifest_file(&support_arg, 120)
                                .await
                                .map(|_| ())
                        }
                        .await;
                        // 这份暂存件只服务这一次 apply，名字又按实例唯一（不再被下一轮覆盖），
                        // 所以用完就删：留着就是随每一次滚动在临时目录里堆一份，而没有任何
                        // 回收方认得它们。失败那一路也删——它派生自包里那份清单，包还在。
                        let _ = tokio::fs::remove_file(&path).await;
                        outcome.map_err(|e| classify_before_any_change("support", "", e))?;
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
                        // 这一步排在配置效果滚动**之前**，不是它之后：配置效果滚动
                        // 会重启目标自己，而目标启动时要连的正是这批后端。2026-10-26
                        // 实测，配置效果滚动先重启了 `deployment/cogneva`（19:11:12Z），
                        // 而它要连的 `redis-0` 19:11:36Z 才起，`redis` 又是 headless
                        // Service、没有就绪后端时名字根本解析不出来，于是新 Pod 连炸
                        // 三次（`failed to lookup address information: Name or service
                        // not known`）才自己缓过来。先等后端、再滚消费者。
                        let after_support = self
                            .support_workloads()
                            .await
                            .map_err(|e| classify_before_any_change("support-snapshot", "", e))?;
                        let restarted = changed_workloads(&before, &after_support, &plan.targets);
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
                        // 删字段的对象代数没动：这次 apply 说自己成功，而它要删的那几个
                        // 字段还在。判健康会把「一条改动都没上线」写成「上线完成」，
                        // 之后没有任何东西会再看它一眼——这正是 2026-10-22 那一轮。
                        let not_taken = self.removals_not_taken(&field_removals).await;
                        if !not_taken.is_empty() {
                            let detail = not_taken
                                .iter()
                                .map(|r| {
                                    format!(
                                        "{}/{} ({} : {})",
                                        r.resource,
                                        r.name,
                                        r.paths.join(", "),
                                        r.namespace
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("; ");
                            return Err(classify_before_any_change(
                                "field-removal",
                                "",
                                SFError::Agent(format!(
                                    "{FIELD_REMOVAL_NOT_TAKEN_MARKER}: this revision's support \
                                     manifest no longer declares {detail}, but the live object's \
                                     generation did not move — server-side apply only deletes \
                                     fields this field manager owns, so the deletion was never \
                                     written and the cluster still carries them; remove them \
                                     with a json patch instead of by leaving them out of the \
                                     manifest"
                                )),
                            ));
                        }
                        // 配置改动走不了"按代数认人"这条路：apply 一份 ConfigMap 不改
                        // 任何工作负载的 spec，代数不动，逐目标滚动与支撑等待都不会碰
                        // 它的消费者——只改配置的 rev（镜像 tag 与上一版相同时连目标都
                        // 不会滚）就这么静默停用。所以这里按内容差自己判一份该滚的名单。
                        // 名单里不含"清单在手"的目标：它们被逐目标循环 apply 时带着
                        // 新的 `checksum/config` 注解，自己那次滚动就把新配置读进去了。
                        let carried = manifest_carried_targets(plan).await;
                        let configs_after = self.configmap_contents().await;
                        let config_rolled = self
                            .roll_config_change_consumers(
                                configs_before.as_ref(),
                                configs_after.as_ref(),
                                &carried,
                            )
                            .await
                            .map_err(|e| classify_before_any_change("config-effect", "", e))?;
                        if !config_rolled.is_empty() {
                            info!(
                                workloads = %config_rolled.join(","),
                                "mainline rollout: rolled the workloads whose configuration is only read at startup"
                            );
                            // 这些是自己刚重启的，也要等它们回就绪：不等就进逐目标
                            // 循环，目标启动时连的正是它们。
                            let after_config = self.support_workloads().await.map_err(|e| {
                                classify_before_any_change("support-snapshot", "", e)
                            })?;
                            let config_restarted =
                                changed_workloads(&after_support, &after_config, &plan.targets);
                            if !config_restarted.is_empty() {
                                info!(
                                    workloads = %config_restarted
                                        .iter()
                                        .map(SupportWorkload::display)
                                        .collect::<Vec<_>>()
                                        .join(","),
                                    "mainline rollout: waiting for the workloads the configuration roll restarted"
                                );
                                self.wait_support_settled(&config_restarted).await.map_err(
                                    |e| classify_before_any_change("support-settle", "", e),
                                )?;
                            }
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
            // 同一份目标清单里被这次 apply 改写的其它容器（init 容器、边车）也一起
            // 快照：`prevs` 只管那个命名主容器，而 apply 换的是整份文档。没有清单
            // 的那一轮走 set image 兜底，只动主容器，没有别的引用要还。
            if let Some(dir) = &plan.manifests_dir {
                let key = target_manifest_key(&t.deployment);
                match tokio::fs::read_to_string(Path::new(dir).join(&key)).await {
                    Ok(text) => refs.targets.extend(
                        self.snapshot_rewritten_refs(&text, &key)
                            .await
                            .map_err(|e| {
                                classify_before_any_change("snapshot", &t.deployment, e)
                            })?,
                    ),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(classify_before_any_change(
                            "snapshot",
                            &t.deployment,
                            SFError::IO(format!("read {key}: {e}")),
                        ))
                    }
                }
            }
        }
        let mut done: Vec<&RolloutTarget> = Vec::new();
        for target in &plan.targets {
            let prev_shape = prev_shapes
                .iter()
                .find(|(d, _)| d == &target.deployment)
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            // 换旧 Pod 之前先问一次：新 Pod 装不装得下。装不下就当场扣住，
            // 旧 Pod 留着、应用不断——这是量到了漂移却没有任何门去拦的那一处，
            // 补成门。装不下是集群此刻的事，不是这一版的结论，所以判环境类、
            // 不回滚，下一轮天花板跟上了自己会继续。
            if let Some(shortfall) = self.admission_would_fail(plan, target).await {
                warn!(
                    deployment = %target.deployment,
                    dimension = shortfall.dimension,
                    shortfall = %shortfall.text,
                    "mainline rollout: holding this rollout before it starts; the new pod would \
                     not admit against the ceiling the cluster enforces right now, so the old pod \
                     stays and the next round retries"
                );
                let mut failure = RolloutFailure::at(
                    "precheck",
                    &target.deployment,
                    FailureLocus::Admission,
                    done.is_empty(),
                    SFError::Config(shortfall.text),
                );
                // 扣住是**部署器自己决定先不交付**，不是集群拒了交付物：那一条
                // （`mainline_rollout_refused_total`）的标签是集群挡下 Pod 的那堵墙，
                // 这一条是我方量到装不下就停手。两个事实，两条修法，各自一格。维度
                // 随终止消息过 Job 边界，部署器读回来记成计数——于是「部署器被准入
                // 扣住、差在 CPU」在读数面上有名字，不必去翻这个 Job 的日志。
                failure.admission_hold = Some(shortfall.dimension);
                return Err(failure);
            }
            if let Err(e) = self.apply_target(plan, target).await {
                return self
                    .fail_without_blind_rollback(
                        "apply",
                        &target.deployment,
                        e,
                        &done,
                        &prevs,
                        &refs,
                    )
                    .await;
            }
            if let Err(e) = self.wait_rollout_complete(target, prev_shape).await {
                done.push(target);
                return self
                    .fail_without_blind_rollback(
                        "wait",
                        &target.deployment,
                        e,
                        &done,
                        &prevs,
                        &refs,
                    )
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
                    .fail_without_blind_rollback(
                        "soak",
                        &target.deployment,
                        e,
                        &done,
                        &prevs,
                        &refs,
                    )
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
        refs: &RefSnapshots,
    ) -> Result<(), RolloutFailure> {
        let msg = e.to_string();
        let primary = primary_message(&msg);
        // 判据只读**我们自己写的那一段**（分隔符之前）。分隔符之后是判死那一刻
        // 从集群贴上的现场读数，其中包含容器自己留下的终止消息——那是自由文本。
        // 让它参与下面这几条判据，容器一句「connection refused」或「cannot list
        // resource」就能把一次真坏的版本判成环境类：不回滚、不记账，坏版本留在
        // 集群上。分隔符存在的理由就是这个。
        if is_cluster_unreachable(primary) {
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
        if is_admission_denied(primary) {
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
        if is_placement_blocked(primary) {
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
        if is_image_source_unavailable(primary) {
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
        if is_new_revision_never_ran(primary) {
            warn!(
                error = %e,
                "no pod of the new revision was ever created, so this timeout says nothing \
                 about it; keeping the new revision (no rollback) — the controller had not got \
                 round to it, and rolling back would spend a rollback on no evidence at all"
            );
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::NeverRan,
                false,
                e,
            ));
        }
        if is_host_loaded(primary) {
            warn!(
                error = %e,
                "the host's run queue stayed wider than its core count for the whole budget, \
                 so the new pod was queued instead of running; keeping the new revision (no \
                 rollback) — the revision left in place starts on its own once the host is \
                 quiet again, and rolling back would book this machine's load as a defect of \
                 the code"
            );
            return Err(RolloutFailure::at(
                stage,
                target,
                FailureLocus::HostLoaded,
                false,
                e,
            ));
        }
        if is_observation_tool_failure(primary) {
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
        let verdict = self.rollback(&e, done, prevs, refs).await;
        // 回滚没落地这件事必须走进返回给调用方的错误里。回滚后各工作负载都 Ready
        // 是两个版本共有的形状（Ready 只说明它收敛在**某个**镜像上），把它读成
        // 「退回去了」是这一步最贵的误读：调用方会以为自己在旧版本上，而实际留在
        // 失败版本上。判词来自回读到的镜像，不来自就绪。
        let e = match verdict.not_landed_note() {
            Some(note) => SFError::Agent(format!("{e}; {note}")),
            None => e,
        };
        // 授权被拒这一类在滚动中仍回滚并记版本类：改过的东西要退回去。落点写成
        // auth 而不是 observed，好让"因为读不到集群而回滚"与"版本真的没起来"在
        // 记账和报告里分得开。
        let locus = if is_authorization_denied(primary) {
            FailureLocus::AuthDenied
        } else {
            FailureLocus::Observed
        };
        Err(RolloutFailure::at(stage, target, locus, false, e))
    }

    /// 尽力回滚：已滚目标按快照的各自 prev 镜像反向 set image 并等收敛
    /// （不用 rollout undo——多目标无事务性，undo 还会连带回退其他字段）。
    /// 回滚本身失败只 warn（人工介入兜底），不掩盖原始错误。
    ///
    /// 返回的是回滚自己的判词，两条分开：**落地**是回读到的镜像真的变回了
    /// 快照那一份，**收敛**是它后面又起来了。就绪只说后者，而两个版本都能 Ready
    /// ——拿就绪当落地判词，"退回去了"与"原地没动"读起来是同一句话。
    pub(crate) async fn rollback(
        &self,
        cause: &SFError,
        done: &[&RolloutTarget],
        prevs: &[(String, String)],
        refs: &RefSnapshots,
    ) -> RollbackVerdict {
        let mut verdict = RollbackVerdict::default();
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
            // 这个目标的**整组**引用一条命令还回去：主容器（`prevs` 里那一份）
            // 加上同一份文档里被这次 apply 改写的其它引用（init 容器、边车）。
            // 只还主容器的话，失败 rev 的二进制会继续留在启动路径上——现场就是
            // 这么来的：进化的 `seed-source`、redis 的 `aof-repair`、沙盒的
            // `seed-sandbox` 各自停在某一版失败 rev 上，回滚了主容器它们不动。
            // 分成两条命令会让同一个 pod 模板改两次、滚两次。
            let mut here: Vec<RewrittenRef> = vec![RewrittenRef {
                resource: "deployment".to_string(),
                name: t.deployment.clone(),
                container: t.container.clone(),
                image: prev.to_string(),
            }];
            // 主容器在目标清单里也有一份声明，两处都过滤掉：同一条命令里出现两个
            // 同名容器的 `容器=镜像` 时 `set image` 不报错，只会赢一个，而赢哪个
            // 不在这里决定。主容器以 `prevs` 那一份为准。
            here.extend(
                refs.targets
                    .iter()
                    .filter(|r| r.resource == "deployment" && r.name == t.deployment)
                    .filter(|r| r.container != t.container)
                    .cloned(),
            );
            if self.restore_rewritten_refs(&here).await > 0 {
                // 命令没发出去，读回来也只会是失败版本；这一处已经由上面那条
                // warn 点名，不再当成"落地了但没收敛"重复记一遍。
                verdict.not_landed.push(format!(
                    "deployment/{}: restore command failed",
                    t.deployment
                ));
                continue;
            }
            self.confirm_restored(&here, &mut verdict).await;
            if let Err(e) = self.wait_rollout_complete(t, &live_shape).await {
                warn!(deployment = %t.deployment, error = %e, "rollback wait failed");
                verdict.not_converged.push(t.deployment.clone());
            }
        }
        // 支撑面在任何镜像变更之前就 apply 了，所以它的引用一定被改过——不论目标
        // 滚到了第几个。它的工作负载只有一份文档、各自独立，按工作负载一条命令还。
        if self.restore_rewritten_refs(&refs.support).await > 0 {
            verdict
                .not_landed
                .push("support workloads: restore command failed".to_string());
        } else {
            self.confirm_restored(&refs.support, &mut verdict).await;
        }
        verdict
    }

    /// 回读一处工作负载的 Pod 模板，把每个被改写的容器与它应当退回的镜像对上。
    ///
    /// 读不回来、或者读出来认不出形状时进 `unconfirmed` 而不是 `not_landed`：没
    /// 有观测就不下结论，与滚动那一侧的判据同一条。落地判词的误报方向和漏报方向
    /// 不对称——把"读不到"写成"没落地"会让运维去追一个不存在的失败。
    async fn confirm_restored(&self, refs: &[RewrittenRef], verdict: &mut RollbackVerdict) {
        let mut workloads: Vec<(String, String)> = Vec::new();
        for r in refs {
            if !workloads
                .iter()
                .any(|(res, n)| res == &r.resource && n == &r.name)
            {
                workloads.push((r.resource.clone(), r.name.clone()));
            }
        }
        for (resource, name) in workloads {
            let live = match self.live_images(&resource, &name).await {
                Ok(live) => live,
                Err(e) => {
                    verdict.unconfirmed.push(format!(
                        "{resource}/{name}: could not read the images back: {e}"
                    ));
                    continue;
                }
            };
            for r in refs
                .iter()
                .filter(|r| r.resource == resource && r.name == name)
            {
                match live.iter().find(|(c, _)| c == &r.container) {
                    Some((_, actual)) if actual == &r.image => {
                        verdict
                            .landed
                            .push(format!("{resource}/{}={}", name, r.container));
                    }
                    Some((_, actual)) => verdict.not_landed.push(format!(
                        "{resource}/{name} {}: set to {}, reads back {}",
                        r.container, r.image, actual
                    )),
                    None => verdict.unconfirmed.push(format!(
                        "{resource}/{name} {}: not in the pod template any more",
                        r.container
                    )),
                }
            }
        }
    }

    /// 一个工作负载 Pod 模板里每个容器的镜像，`(容器, 镜像)`，init 容器在前。
    ///
    /// 一条命令读两种容器：`seed-source`、`aof-repair` 这些被回滚改写的引用恰好
    /// 都在 init 容器里，只读 `containers[*]` 会让它们全部落到"模板里没有这个容器"。
    async fn live_images(&self, resource: &str, name: &str) -> SFResult<Vec<(String, String)>> {
        let jsonpath = "jsonpath={range .spec.template.spec.initContainers[*]}{.name}={.image}{\"\\n\"}{end}{range .spec.template.spec.containers[*]}{.name}={.image}{\"\\n\"}{end}";
        let out = self
            .run_kubectl(&["get", resource, name, "-o", jsonpath], 30)
            .await?;
        let mut images = Vec::new();
        for line in out.lines() {
            let Some((container, image)) = line.trim().split_once('=') else {
                continue;
            };
            if container.is_empty() || image.is_empty() {
                continue;
            }
            images.push((container.to_string(), image.to_string()));
        }
        Ok(images)
    }
}

/// 一次回滚自己的两条判词，落地与收敛分开记。
///
/// 现场把它读错过一次：`helm rollback --wait` 因为一个与这次回滚无关的工作负载
/// 起不来而返回 failed，而回滚其实已经落地；同一个集群、同一份清单，去掉 `--wait`
/// 就是绿的。反方向同样成立——每个工作负载都 Ready 也可能是回滚命令根本没生效，
/// 因为它 Ready 在失败版本上。所以两条判词分开取：落地看回读到的镜像，收敛看
/// 各工作负载的 Ready。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RollbackVerdict {
    /// 读回来确认回到了快照镜像的 `资源/名字=容器`。
    pub landed: Vec<String>,
    /// 读回来仍是失败版本的那几处。这一条一旦非空，回滚**没有**落地。
    pub not_landed: Vec<String>,
    /// 命令发出去了但读不回来，或读出来的形状认不出——不下结论。
    pub unconfirmed: Vec<String>,
    /// 落地了但没收敛的工作负载。
    pub not_converged: Vec<String>,
}

impl RollbackVerdict {
    /// 回滚没落地时说给调用方听的那一句；落地了就 `None`，原文不变。
    pub(crate) fn not_landed_note(&self) -> Option<String> {
        if self.not_landed.is_empty() {
            return None;
        }
        Some(format!(
            "rollback did not land: {}",
            self.not_landed.join("; ")
        ))
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
    let parsed = parse_rollout_args(&args)?;
    let cfg = MainlineDeployerConfig::default();
    let mut plan = RolloutPlan::from_config(&cfg, parsed.tag.clone());
    plan.manifests_dir = parsed.manifests_dir.clone();
    plan.traceparent = parsed.traceparent.clone();
    let executor = RolloutExecutor::new(
        parsed.kubectl.clone(),
        parsed.ns.clone(),
        parsed.soak_secs,
        parsed.restart_threshold,
        parsed.timeout,
        parsed.startup_timeout,
    );
    let executor = if parsed.ci_upstreams.is_empty() {
        executor
    } else {
        executor.with_ci_ask(UpstreamCiAsk {
            rev: parsed.rev.trim().to_string(),
            upstreams: parsed.ci_upstreams,
        })
    };
    // 第一件事是问自己够不够格提问：一处集群配置都没有时，后面每一次查询都只会
    // 在 localhost:8080 上被拒，而 `probe` 会把"连接被拒"当成集群抖动重试到预算
    // 耗尽（现场 59 次 / 303 秒），最后落一条指向 apiserver 的判词。凭据重试换
    // 不来，所以就地判死，并把缺的是哪一处写进判词。
    let outcome = match ClusterConfigSources::observe().check() {
        Err(note) => Err(classify_before_any_change(
            ROLLOUT_PRECHECK_STAGE,
            "",
            SFError::IO(note),
        )),
        Ok(()) => executor.run(&plan).await,
    };
    match outcome {
        // 成功也留下读数：这一轮跑得离上限有多近，只有跑完的这一刻在同一个进程里
        // 对照得出来（上限写在 Job 清单里，用掉的量在这个容器自己的 cgroup 里）。
        Ok(()) => {
            report_termination_message(None, None);
            Ok(())
        }
        // 失败分两条通道过 Job 边界：类别走退出码（环境类用独立的 75，让部署器
        // 知道这次失败说不出新版本的好坏），落点走进程的终止消息——类别只有两档，
        // 而"坏在哪一处"是把两个值都塞进退出码塞不下的东西。终止消息是 k8s 给
        // "这个容器为什么死"留的窄通道，读出端按结构化字段取，不解析日志。
        // `process::exit` 不走返回路径，因为 Box<dyn Error> 出去一律是 1。
        Err(f) => {
            report_termination_message(Some(&f.signature), f.admission_hold);
            if f.class == FailureClass::Environment {
                tracing::error!(error = %f, exit_code = ROLLOUT_EXIT_ENVIRONMENT, "mainline rollout failed for environment reasons");
                std::process::exit(ROLLOUT_EXIT_ENVIRONMENT);
            }
            Err(Box::new(f))
        }
    }
}

/// `mainline-rollout` 子命令的参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RolloutCliArgs {
    pub tag: String,
    pub ns: String,
    pub soak_secs: u64,
    pub restart_threshold: u32,
    pub timeout: u64,
    pub startup_timeout: u64,
    pub kubectl: String,
    pub manifests_dir: Option<String>,
    /// 派发侧写进 Job 注解的那一个 traceparent。
    pub traceparent: Option<String>,
    /// 要问 CI 的这一版。空 = 不提问（没有随 Job 带过来的上游清单）。
    pub rev: String,
    pub ci_upstreams: Vec<UpstreamCiTarget>,
}

/// 解析 `mainline-rollout` 的参数。
///
/// 从入口里拆出来只有一个理由：让它可测。参数面是这个进程与派发侧之间唯一的契约，
/// 而「拼错一个开关」在这条通道上的后果是安静地少问一次 CI——入口里读
/// `std::env::args()` 的那一版没有任何用例碰得到它。
pub(crate) fn parse_rollout_args(
    args: &[String],
) -> Result<RolloutCliArgs, Box<dyn std::error::Error>> {
    let mut out = RolloutCliArgs {
        tag: String::new(),
        ns: "cogneva".to_string(),
        soak_secs: 120,
        restart_threshold: 1,
        timeout: 300,
        startup_timeout: 900,
        kubectl: "kubectl".to_string(),
        manifests_dir: None,
        traceparent: None,
        rev: String::new(),
        ci_upstreams: Vec::new(),
    };
    let mut i = 0;
    while i < args.len() {
        let value = |i: usize| -> Result<String, Box<dyn std::error::Error>> {
            args.get(i + 1)
                .cloned()
                .ok_or_else(|| format!("--{} requires a value", args[i]).into())
        };
        match args[i].as_str() {
            "--tag" => {
                out.tag = value(i)?;
                i += 2;
            }
            "--ns" => {
                out.ns = value(i)?;
                i += 2;
            }
            "--soak-secs" => {
                out.soak_secs = value(i)?.parse()?;
                i += 2;
            }
            "--restart-threshold" => {
                out.restart_threshold = value(i)?.parse()?;
                i += 2;
            }
            "--timeout" => {
                out.timeout = value(i)?.parse()?;
                i += 2;
            }
            "--startup-timeout" => {
                out.startup_timeout = value(i)?.parse()?;
                i += 2;
            }
            "--kubectl" => {
                out.kubectl = value(i)?;
                i += 2;
            }
            "--manifests-dir" => {
                out.manifests_dir = Some(value(i)?);
                i += 2;
            }
            "--traceparent" => {
                out.traceparent = Some(value(i)?);
                i += 2;
            }
            "--rev" => {
                out.rev = value(i)?;
                i += 2;
            }
            // 形如 `github|http://gw:8081/github|owner/repo`，可重复。刻意与派发侧
            // 那份上游配置同形同值：Job 只是拿着同一份清单再问一次，不在这里重新
            // 推导仓库身份。
            "--ci-upstream" => {
                let raw = value(i)?;
                let mut parts = raw.splitn(3, '|');
                let platform = parts.next().unwrap_or("").trim().to_string();
                let api_base = parts.next().unwrap_or("").trim().to_string();
                let repo = parts.next().unwrap_or("").trim().to_string();
                if platform.is_empty() || api_base.is_empty() || repo.is_empty() {
                    return Err(format!(
                        "--ci-upstream expects platform|api_base|repo, got {raw:?}"
                    )
                    .into());
                }
                out.ci_upstreams.push(UpstreamCiTarget {
                    platform,
                    api_base,
                    repo,
                });
                i += 2;
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    if out.tag.is_empty() {
        return Err("--tag is required".into());
    }
    // 有上游却没有 rev：这份清单没法问（问的是"哪一版的 CI"）。宁可让 Job 起不来
    // 并报错，也不安静地跳过提问——跳过与"问了、答不上来"在日志上同形。
    if !out.ci_upstreams.is_empty() && out.rev.trim().is_empty() {
        return Err("--ci-upstream requires --rev".into());
    }
    Ok(out)
}

/// 容器终止消息文件（kubelet 的 `terminationMessagePath` 默认值）。失败落点与资源读数
/// 都写在这里，部署器从 Pod 的 `state.terminated.message` 读回。
const TERMINATION_LOG: &str = "/dev/termination-log";

/// 尽力把这一轮的结果写进终止消息：落点（若失败）一行，自己的资源读数一行。
///
/// 写不进去不能影响失败本身的交付：类别还走退出码，落点没了只是让部署器少一份证据
/// （它会往"停下"的一侧取，不会因此多滚一轮）。读数同理由：它是这一轮跑得怎么样的一份
/// 证据，不是这一轮成败的判据。
fn report_termination_message(signature: Option<&str>, admission_hold: Option<&str>) {
    let reading = RolloutResourceReading::read_from(
        Path::new(rollout_resources::CGROUP_ROOT),
        chrono::Utc::now().timestamp().max(0) as u64,
    );
    match reading.as_ref() {
        Some(reading) => tracing::info!(reading = %reading, "rollout resource reading"),
        None => tracing::warn!(
            path = rollout_resources::CGROUP_ROOT,
            "no cgroup to read this run's resource use from; the deployer will see this run without a resource reading"
        ),
    }
    if let Err(e) = write_termination_message(
        Path::new(TERMINATION_LOG),
        signature,
        reading.as_ref(),
        admission_hold,
    ) {
        tracing::warn!(
            path = TERMINATION_LOG,
            error = %e,
            signature = signature.unwrap_or("none"),
            "could not record this rollout run's outcome; the deployer will see its result without the evidence it left"
        );
    }
}

/// 终止消息的行布局：**落点必须在第一行**。
///
/// 部署器读落点时只认第一行（`parse_failure_signature` 取 `lines().next()`）：这条通道
/// 上可能有别的进程写过东西，位置固定才谈得上"这是我们写的形状"。读数与扣住的维度排在
/// 后面，于是它们的增删永远不动落点的位置——反过来放，第一行就会变成一段陌生文本，那一轮
/// 失败从此判不出落点。kubelet 默认按 4KiB 截断，三行远在截断线之内。
pub(crate) fn write_termination_message(
    path: &Path,
    signature: Option<&str>,
    reading: Option<&RolloutResourceReading>,
    admission_hold: Option<&str>,
) -> std::io::Result<()> {
    let mut body = String::new();
    if let Some(signature) = signature {
        body.push_str(signature);
        body.push('\n');
    }
    if let Some(reading) = reading {
        body.push_str(&reading.to_line());
        body.push('\n');
    }
    if let Some(dimension) = admission_hold {
        body.push_str(&format!("{ADMISSION_HOLD_PREFIX}{dimension}"));
        body.push('\n');
    }
    std::fs::write(path, body)
}

/// 扣住那一行的前缀，与它读回时认的前缀是同一个常量。
///
/// 值域是 [`shortfall_dimension`] 的闭集。读回的一端只认这个前缀 + 闭集里的值：这条
/// 通道上也可能写着别的东西，把一段陌生文本当成"差在 CPU"会让读数指错修法。
pub(crate) const ADMISSION_HOLD_PREFIX: &str = "admission_held:";

/// 从终止消息里读回扣住的那一维。
///
/// `None` 既是"这一轮不是被准入扣住的"，也是"这一行读不懂"——两者都不该落成一条计数：
/// 前者无事可记，后者记下来就是把一段陌生文本写成一条资源事实。前缀对上、值不在闭集里
/// 的那一行按读不懂处理。
pub(crate) fn admission_hold_dimension(message: &str) -> Option<&'static str> {
    for line in message.lines() {
        let Some(rest) = line.trim().strip_prefix(ADMISSION_HOLD_PREFIX) else {
            continue;
        };
        return match rest.trim() {
            "cpu" => Some("cpu"),
            "memory" => Some("memory"),
            "pods" => Some("pods"),
            "storage" => Some("storage"),
            "other" => Some("other"),
            _ => None,
        };
    }
    None
}

// 这一侧的用例在 `crate::mainline_deployer::tests` 里，不在本模块。它们是搬迁前那唯一
// 一份用例，与构建侧的用例共用夹具（伪造的 kubectl、临时清单目录、`RolloutExecutor`
// 的搭台），按主题交错排布，逐条断的是「跑起来是什么样」——按模块切开反而会把一份场景
// 拆成两半。搬迁保的是**产出面**的边界，没有动用例的住处。
//
// 例外只留给**不搭台**的纯函数：判据全靠输入文本与返回值，借不到共用的夹具也就
// 没有"一份场景被拆开"的风险（见下面 `scene_suffix` 的两条）。

#[cfg(test)]
mod tests {
    use super::*;

    /// 判词里贴上的现场采样，只能待在分隔符**之后**。
    ///
    /// 它带着集群自己的原话：新 Pod 还没开始监听时，探针报的就是
    /// `connect: connection refused`。接在分隔符之前，[`is_cluster_unreachable`]
    /// 会把这次超时读成"我们看不到集群"——环境类不回滚、不记账、无限重试，
    /// 而这次失败说的恰恰是这个版本自己起不来。
    #[test]
    fn a_scene_attached_to_a_verdict_stays_out_of_the_text_the_class_is_read_from() {
        let diagnosis = "cogneva-55cd8bfdff-qkpvj Running not-ready; events: Unhealthy: \
                         Startup probe failed: Get \"http://10.42.0.99:8080/health/live\": \
                         dial tcp 10.42.0.99:8080: connect: connection refused";
        let msg = format!(
            "rollout of deployment/cogneva did not complete within 300s (last: 1|1|1|1|1|1{})",
            scene_suffix(diagnosis)
        );

        assert!(
            !primary_message(&msg).contains("connection refused"),
            "the scene decided the class: {msg}"
        );
        assert!(
            !is_cluster_unreachable(primary_message(&msg)),
            "a pod that has not started listening is not an unreachable cluster: {msg}"
        );
        // 剥掉的只是参与判据的那一半：判词本身仍带着现场，读日志的人看得到。
        assert!(msg.contains("connection refused"), "{msg}");
        assert!(msg.contains("Startup probe failed"), "{msg}");
    }

    /// 没有现场时判词保持原样：不许多出一个空的分隔段。
    #[test]
    fn an_empty_scene_leaves_the_verdict_untouched() {
        assert!(scene_suffix("").is_empty());
    }

    /// 每核一个可运行任务是这条判据的界：等于宽度算不上排队，越过一格才是。
    #[test]
    fn the_run_queue_is_wide_only_past_one_task_per_core() {
        let at_capacity = HostRunQueue {
            load1: 24.0,
            cores: 24.0,
        };
        assert!(!at_capacity.is_loaded(), "a full machine is not a queue");
        let queued = HostRunQueue {
            load1: 98.0,
            cores: 24.0,
        };
        assert!(queued.is_loaded(), "four tasks per core is a queue");
    }

    /// 两个 `/proc` 读数的解析：`loadavg` 取第一列，`cpuinfo` 数 `processor` 行。
    #[test]
    fn the_host_reading_comes_out_of_the_two_proc_files() {
        let dir = tempfile::tempdir().unwrap();
        let loadavg = dir.path().join("loadavg");
        let cpuinfo = dir.path().join("cpuinfo");
        std::fs::write(&loadavg, "140.73 61.02 22.11 12/3456 789012\n").unwrap();
        std::fs::write(
            &cpuinfo,
            "processor\t: 0\nmodel name\t: x\n\nprocessor\t: 1\nmodel name\t: y\n",
        )
        .unwrap();

        let rq = read_host_run_queue(&loadavg, &cpuinfo).expect("both files are readable");
        assert_eq!(rq.load1, 140.73);
        assert_eq!(rq.cores, 2.0);
        assert!(rq.is_loaded());
    }

    /// 读不到的读数不许折成「宿主很空」：那会把「没有证据」写成「没有负载」，
    /// 而这条判据的全部作用就是把这两种情况分开。
    #[test]
    fn an_unreadable_host_says_nothing_rather_than_zero() {
        let dir = tempfile::tempdir().unwrap();
        let loadavg = dir.path().join("loadavg");
        let cpuinfo = dir.path().join("cpuinfo");
        std::fs::write(&loadavg, "0.31 0.28 0.11 1/100 4242\n").unwrap();
        std::fs::write(&cpuinfo, "processor\t: 0\n").unwrap();
        assert!(read_host_run_queue(&loadavg, &cpuinfo).is_some());
        assert!(
            read_host_run_queue(&dir.path().join("missing"), &cpuinfo).is_none(),
            "a missing file is not a quiet host"
        );
        // 空文件、没有 processor 行的 cpuinfo 同样是读不到。
        std::fs::write(&cpuinfo, "model name\t: x\n").unwrap();
        assert!(read_host_run_queue(&loadavg, &cpuinfo).is_none());
    }

    /// 「这一版删掉了哪个字段」只从两版清单的 `spec` 子树差出来：值变了不算删除，
    /// 新出现的对象没有删除可言，`metadata`/`status` 上的差是别的写者的动作。
    #[test]
    fn a_field_that_is_gone_from_the_manifest_is_the_one_this_apply_has_to_delete() {
        let previous = "\
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: cogneva-strip
spec:
  headers:
    customRequestHeaders:
      X-Frame-Options: DENY
    customResponseHeaders:
      X-Frame-Options: DENY
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cogneva-config
data:
  a: '1'
";
        let current = "\
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: cogneva-strip
  labels:
    added-by: someone-else
spec:
  headers:
    customResponseHeaders:
      X-Frame-Options: SAMEORIGIN
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cogneva-config
data:
  a: '2'
";
        let removals = objects_that_lost_fields(previous, current, "cogneva");
        assert_eq!(
            removals,
            vec![FieldRemoval {
                resource: "middleware.traefik.io".into(),
                namespace: "cogneva".into(),
                name: "cogneva-strip".into(),
                paths: vec![".spec.headers.customRequestHeaders".into()],
            }],
            "only the dropped key is a deletion: a changed value or a new label is not"
        );

        // 新对象不是删除；对象消失也不在这条判据里（它不在这一版要 apply 的东西里）。
        let grown =
            format!("{current}---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: new\n");
        assert!(objects_that_lost_fields(current, &grown, "cogneva").is_empty());
        assert!(objects_that_lost_fields(current, previous, "cogneva").is_empty());
        // 文档读不出来时给空集，不是给一个"全都删了"的结论。
        assert!(objects_that_lost_fields("kind: [torn", current, "cogneva").is_empty());
    }

    /// 判断「删没落」只认两次都读到的代数：读不到不是「没变」的证据，把没有证据
    /// 写成失败会让这条判据在别的故障上误报。
    #[test]
    fn a_generation_that_did_not_move_is_the_only_proof_the_deletion_was_never_written() {
        let removed = FieldRemoval {
            resource: "middleware.traefik.io".into(),
            namespace: "cogneva".into(),
            name: "cogneva-strip".into(),
            paths: vec![".spec.headers.customRequestHeaders".into()],
        };
        // 代数没动：这次删除没写进去。
        assert_eq!(
            removals_that_did_not_land(&[(removed.clone(), Some(7), Some(7))]),
            vec![removed.clone()]
        );
        // 代数退回去也是"没往前动"——对象被别的写者改回旧 spec 同样是这一版没落地。
        assert_eq!(
            removals_that_did_not_land(&[(removed.clone(), Some(7), Some(6))]),
            vec![removed.clone()]
        );
        // 代数前进：这次 apply 写进去了，不是失败。
        assert!(removals_that_did_not_land(&[(removed.clone(), Some(7), Some(8))]).is_empty());
        // 任何一侧读不到都放过。
        assert!(removals_that_did_not_land(&[(removed.clone(), None, Some(7))]).is_empty());
        assert!(removals_that_did_not_land(&[(removed.clone(), Some(7), None)]).is_empty());
        assert!(removals_that_did_not_land(&[(removed.clone(), None, None)]).is_empty());
    }

    /// 这条失败是**这一版的属性**：它发生在镜像一个都没动之前，却说得清「这一版的
    /// 清单没在集群上生效」。判环境类会让同一份清单每过一个冷却窗重试一次，永远
    /// 停不下来，读数上还像「集群还没就绪」。
    #[test]
    fn a_field_removal_that_did_not_land_blames_the_revision_at_both_stages() {
        let msg = format!(
            "{FIELD_REMOVAL_NOT_TAKEN_MARKER}: this revision's support manifest no longer \
             declares middleware.traefik.io/cogneva-strip (.spec.headers.customRequestHeaders \
             : cogneva), but the live object's generation did not move"
        );
        let locus = locate_before_any_change(&msg);
        assert_eq!(locus, FailureLocus::FieldRemoval);
        assert_eq!(locus.class(true), FailureClass::Version);
        assert_eq!(locus.class(false), FailureClass::Version);
        // 现场采样里那些环境类的措辞不能把它盖掉：判据只读分隔符之前那一段。
        let with_scene = format!(
            "{msg}{SUPPORT_CAUSE_SEPARATOR}Error from server (Forbidden): middlewares.traefik.io \
             is forbidden; dial tcp 10.43.0.1:443: connect: connection refused"
        );
        assert_eq!(
            locate_before_any_change(&with_scene),
            FailureLocus::FieldRemoval
        );
    }

    /// 宿主排队这条判词必须穿过两种读法：类别从它读出来，而现场采样里那句
    /// `connection refused`（新 Pod 还没监听时探针的原话）不能把它盖成「集群
    /// 不可达」——两条判据的修法不同，一条去查 apiserver，一条等宿主的队列。
    #[test]
    fn a_queued_host_is_its_own_reading_and_the_scene_cannot_override_it() {
        let note = "1|1|1|1|1|1";
        let scene = "cogneva-55cd8bfdff-qkpvj Running not-ready; events: Unhealthy: \
                     Startup probe failed: Get \"http://10.42.0.99:8080/health/live\": \
                     dial tcp 10.42.0.99:8080: connect: connection refused";
        let msg = format!(
            "{HOST_LOADED_MARKER}: rollout of deployment/cogneva did not complete within 300s \
             (last: {note}{})",
            scene_suffix(scene)
        );

        let primary = primary_message(&msg);
        assert!(is_host_loaded(primary), "the marker survived: {msg}");
        assert!(
            !is_cluster_unreachable(primary),
            "the pod's own connect refusal is not an unreachable cluster: {msg}"
        );
        // 判词本身仍带着现场，翻日志的人看得到这一次跑成了什么样。
        assert!(msg.contains("connect: connection refused"), "{msg}");
    }
}
