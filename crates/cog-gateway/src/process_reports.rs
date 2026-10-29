//! 容器的进程表：谁在里面跑着什么。
//!
//! 有两个上报来源，落在同一个地方：
//!
//! 1. **节点采集器**（每节点一个 DaemonSet，`cogneva process-census`）。它带着
//!    hostPID 与宿主 cgroup 的只读挂载，按 `cri-containerd-*.scope` 把宿主上每一个
//!    容器实例的 `cgroup.procs` 读出来，再按 pid 读 `/proc/<pid>/comm`。**第三方镜像
//!    也报**——这一条不依赖容器里跑的是什么，它读的是宿主的账。
//! 2. **容器自报**（跑本平台镜像的进程，逐拍把自己的 `/proc` 推上来）。留着它是因为
//!    有的集群不给 hostPID（受管集群、受限命名空间），那种地方采集器起不来，平台
//!    自己的容器仍然能报。
//!
//! 落点按**容器实例 id**存：Pod 状态里每个容器都带着 `containerID`，宿主 cgroup 路径
//! 的末段也是同一个 id，两边不用交换名字就能对上。Pod 名与容器名只作参考，不参与
//! 索引——采集器读得到 id，读不到名字。
//!
//! 采集器每拍还会报一次「这台节点我在」的活体，按节点记；于是「这一格没有进程表」
//! 能分成两种：这台节点没有采集器（没有人能报），或者采集器在跑而这一格确实读不到
//! （容器刚起、或者已经退出）。两种的修法相反，屏上不该读成同一句话。
//!
//! 自报端点挂在零凭证的集群内面（不是登录态后面，也不是浏览器能到的那一面）：
//! 报的是发起方自己的进程表，最坏的结果是屏幕上多一行它自己写的假数据；要求它
//! 带平台令牌，就得给每个业务 Pod 发一个身份，那比这个风险大得多。规模与形状在
//! 这里收口：条数、名字长度、新鲜度都有上限。

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, RwLock};
use std::time::Duration;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;

/// 一份进程表最多几条。超过这个数不是「进程很多」，是有人在灌数据。
pub const MAX_REPORTS: usize = 64;
/// 名字最长多少个字符。进程名本来就短，长的是命令行——这一面不读命令行。
pub const MAX_COMMAND_CHARS: usize = 64;
/// 一份自报多久之后算过期。容器停掉之后不会再有新的一拍，旧表不能一直当现状显示。
pub const FRESH_FOR: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportedProcess {
    pub pid: u32,
    #[serde(default)]
    pub parent_pid: u32,
    pub command: String,
    #[serde(default)]
    pub rss_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct Report {
    /// 这份表是从哪台节点上读到的。节点采集器把它自报的节点名带在每一份上；
    /// 容器自报的那条路没有节点名，落空串。
    ///
    /// 为什么要按节点记：宿主上的容器总数与「这一面展示了多少个」之间的差，是唯一
    /// 能说明「采集器看得见、而屏上看不见」的数——而它只有按节点才摊得开，一台节点
    /// 上的采集器停了，别的节点上的差不受影响。
    pub node: String,
    /// 这个容器所在 Pod 的 uid。节点采集器从 cgroup 路径里读得到；容器自报的那条路
    /// 不知道，落 `None`。
    ///
    /// 为什么要按 uid 认领：Pod 的沙箱容器（pause）有一份完整的进程表，却不在
    /// `containerStatuses` 里——只按容器 id 认领，它永远认领不上，于是被读成
    /// 「有进程读数、却没有位置」，而屏上那句话是在说它是宿主上的野容器。
    pub pod_uid: Option<String>,
    pub reported_at: DateTime<Utc>,
    pub processes: Vec<ReportedProcess>,
}

/// 进程表的落脚处。按**容器实例 id** 存最近一份——**最近一份就够**：这一面问的
/// 是「现在它里面跑着什么」，历史在别处（如果哪天要历史，那是另一条读数，不是这一格）。
///
/// 键里没有 Pod 名：容器 id 全局唯一，而采集器读得到 id、读不到名字。两个来源报同一个
/// 容器时，后到的那一份覆盖前一份——两边读的是同一份 `/proc`，没有谁更权威的问题。
static REPORTS: LazyLock<RwLock<HashMap<String, Report>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 每台节点上采集器最后一次报到的时刻。没有这一格，就没有「这台节点没人能报」与
/// 「采集器在跑、这一格读不到」的区分。
static CENSUS: LazyLock<RwLock<HashMap<String, DateTime<Utc>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 收下一份自报，回答「这张表跟上一份比变了吗」。没变就不必在总线上落帧：
/// 一个容器每拍都报同一张表，而帧的意义是「屏上该重读了」。
pub fn record(container_id: &str, report: Report) -> bool {
    let mut reports = REPORTS.write().unwrap_or_else(|e| e.into_inner());
    // 先清掉过期的那批：过期的一份本来就读不出来（`latest` 会滤掉），但它**占着内存**。
    // 键是容器实例 id，而采集器按宿主上每一个容器实例入账——评测环境一天起停几百个，
    // 每个最多带 64 条进程；只写不删时这张表跟着开机时长长，而屏上一点都看不出来。
    // 清在写这一侧：写是一分钟一批，读是每次刷新都读。
    reports.retain(|_, previous| fresh(previous));
    let changed = reports
        .get(container_id)
        .map(|previous| previous.processes != report.processes)
        .unwrap_or(true);
    reports.insert(container_id.to_string(), report);
    changed
}

/// 某个容器实例最近一份还没过期的自报。
pub fn latest(container_id: &str) -> Option<Report> {
    let reports = REPORTS.read().unwrap_or_else(|e| e.into_inner());
    reports
        .get(container_id)
        .filter(|report| fresh(report))
        .cloned()
}

fn fresh(report: &Report) -> bool {
    Utc::now()
        .signed_duration_since(report.reported_at)
        .to_std()
        .map(|age| age <= FRESH_FOR)
        .unwrap_or(false)
}

/// 记一笔「这台节点的采集器这一拍在」。空节点名不记：那不是一个节点。
pub fn record_census(node: &str) {
    let node = node.trim();
    if node.is_empty() {
        return;
    }
    let mut census = CENSUS.write().unwrap_or_else(|e| e.into_inner());
    census.insert(node.to_string(), Utc::now());
}

/// 这台节点上采集器最后一次报到的时刻；超过一个新鲜期就当作没有采集器。
///
/// 返回 `None` 读作「这台节点上没有活着的采集器」——不是「这台节点没有进程」。
pub fn census_at(node: &str) -> Option<DateTime<Utc>> {
    let census = CENSUS.read().unwrap_or_else(|e| e.into_inner());
    census.get(node).copied().filter(|at| {
        Utc::now()
            .signed_duration_since(*at)
            .to_std()
            .map(|age| age <= FRESH_FOR)
            .unwrap_or(false)
    })
}

/// 一个名字最多在这几个容器里出现几回——`commands` 去重后最多留几个。
pub const MAX_UNCLAIMED_COMMANDS: usize = 4;

/// 采集器看得见、屏上没有格子的一个容器。
///
/// 只给一个计数时，读者知道「有 3 个没有格子」，却不知道那 3 个是谁。而宿主上那些
/// 没有 Pod 认领的容器恰恰是最吃这台机器的：构建容器、直接起在宿主上的进程。它们的
/// 名字读不到（cgroup 只给 id），但里面的进程名读得到——那就是能说出它们是谁的东西。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnclaimedContainer {
    /// 容器实例 id 的前 12 个字符。完整 id 是六十来位十六进制，这一面没有地方用它
    /// ——读者要认的是「同一台机器上刚才那两个」，不是把它敲进 shell。
    pub id: String,
    /// 里面跑着几个进程。
    pub processes: usize,
    /// 最占内存的那几个进程名，去重后最多 [`MAX_UNCLAIMED_COMMANDS`] 个。
    pub commands: Vec<String>,
}

/// 一台节点上的普查结果：采集器看到了几个容器，其中哪些是这一面读不到的。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CensusGap {
    pub seen: usize,
    /// 按进程数从多到少，同数按 id 排。
    ///
    /// 排序不是为了好看：这张表每次刷新都从哈希表里重建，不排的话顺序每拍都变，
    /// 而「顺序变了」和「内容变了」在屏上是同一件事——读者会看到一行在跳。
    pub unclaimed: Vec<UnclaimedContainer>,
}

/// 屏上已经画着的容器与 Pod。采集器报上来的一个容器，落在这两组里的任何一组，
/// 就有地方可去。
///
/// 两组而不是一组：Pod 的沙箱容器（pause）有自己的一份进程表，却不在
/// `containerStatuses` 里——它的位置只能靠 Pod 的 uid 认。少这一组，每一个 Pod
/// 都会在缺口里留下一条，而那句话在屏上的意思是「宿主上有东西没人认领」。
#[derive(Debug, Default, Clone)]
pub struct Claims {
    /// `containerStatuses[].containerID` / `initContainerStatuses[].containerID`。
    pub containers: HashSet<String>,
    /// `metadata.uid`。
    pub pods: HashSet<String>,
}

/// 某台节点上采集器看到了几个容器，其中哪些**这一面读不到**。
///
/// 对不上的那些，采集器看得见它们、屏上没有它们的位置——宿主上的构建容器、直接起在
/// 宿主上的进程就是这样。
///
/// 返回 `None` 读作「这台节点上没有活着的采集器」：没有人能报，差就是「不知道」，
/// 不是 0——把它写成 0 会让「采集器没装」读成「一个都没漏」。
pub fn census_gap(node: &str, claims: &Claims) -> Option<CensusGap> {
    let node = node.trim();
    if node.is_empty() || census_at(node).is_none() {
        return None;
    }
    let reports = REPORTS.read().unwrap_or_else(|e| e.into_inner());
    let mut seen = 0usize;
    let mut unclaimed = Vec::new();
    for (id, report) in reports.iter() {
        if !fresh(report) || report.node != node {
            continue;
        }
        seen += 1;
        let claimed = claims.containers.contains(id)
            || report
                .pod_uid
                .as_ref()
                .is_some_and(|uid| claims.pods.contains(uid));
        if !claimed {
            unclaimed.push(UnclaimedContainer {
                id: id.chars().take(12).collect(),
                processes: report.processes.len(),
                commands: commands_of(&report.processes),
            });
        }
    }
    unclaimed.sort_by(|a, b| b.processes.cmp(&a.processes).then_with(|| a.id.cmp(&b.id)));
    Some(CensusGap { seen, unclaimed })
}

/// 一张进程表里最占内存的几个名字，去重，保持「谁更大谁在前」。
///
/// 按内存而不是按 pid 排：一个容器里跑着几十个进程时，说得出「它在跑 rustc」
/// 比列出一串 pid 有用地多，而占内存最大的那个通常就是它正在干的事。
fn commands_of(processes: &[ReportedProcess]) -> Vec<String> {
    let mut ordered: Vec<&ReportedProcess> = processes.iter().collect();
    ordered.sort_by(|a, b| {
        b.rss_bytes
            .cmp(&a.rss_bytes)
            .then_with(|| a.pid.cmp(&b.pid))
    });
    let mut commands: Vec<String> = Vec::new();
    for process in ordered {
        let command = process.command.trim();
        if command.is_empty() || commands.iter().any(|seen| seen == command) {
            continue;
        }
        commands.push(command.to_string());
        if commands.len() == MAX_UNCLAIMED_COMMANDS {
            break;
        }
    }
    commands
}

/// 把 Pod 状态里的 `containerd://<id>` 与自报里的 id 折成同一个形状：只要那串
/// id 本身。运行时装前缀不同（`containerd://`、`docker://`、`cri-o://`），
/// 而前面那截说的是「谁在管」，不是「是哪个容器」。
pub fn container_id_of(status_value: &str) -> &str {
    status_value
        .rsplit_once("://")
        .map(|(_, id)| id)
        .unwrap_or(status_value)
        .trim()
}

/// 只留进程表形状的字段，顺手截断与限量：自报方是谁都行，进来的字节就得按上限收。
pub fn sanitize(mut processes: Vec<ReportedProcess>) -> Vec<ReportedProcess> {
    processes.truncate(MAX_REPORTS);
    for process in &mut processes {
        process.command = process
            .command
            .chars()
            .take(MAX_COMMAND_CHARS)
            .collect::<String>()
            .trim()
            .to_string();
    }
    processes.retain(|process| !process.command.is_empty());
    processes
}

#[derive(Debug, Deserialize)]
pub struct ReportBody {
    /// 报上来的那个 Pod。容器自报时就是 kubelet 写进去的主机名；节点采集器不知道
    /// 名字，可以不填——落点按容器 id 索引，所以它只是参考。
    #[serde(default)]
    pub pod: String,
    /// 报上来的那个容器实例（cgroup 里那串 id），与 Pod 状态里的
    /// `containerStatuses[].containerID` 对得上。
    pub container_id: String,
    /// 它所在 Pod 的 uid，采集器从 cgroup 路径里读得到；容器自己报的话可以不填。
    #[serde(default)]
    pub pod_uid: Option<String>,
    pub processes: Vec<ReportedProcess>,
}

/// 节点采集器一拍报上来的一批：这台节点上每一个容器实例各一份，外加节点名本身。
///
/// 为什么是批而不是逐条发：一台节点上几十个容器，逐条发是几十次握手，而对屏来说
/// 它们是**同一拍**的读数——分开到几十次请求里，读的一方就得自己拼「哪些是这一拍的」。
#[derive(Debug, Deserialize)]
pub struct CensusBody {
    /// 报上来的那台节点。空着一批照样收，但这台节点就没有「采集器在」的活体。
    #[serde(default)]
    pub node: String,
    #[serde(default)]
    pub reports: Vec<ReportBody>,
}

/// 端点收到一份自报之后在指标上留下的两格。**closed set**，两格都在启动时写 0。
pub const INGEST_ACCEPTED: &str = "accepted";
pub const INGEST_REFUSED: &str = "refused";

/// 把一格收口结果写进指标。没有指标后端时什么都不写：这条面本身不依赖它。
///
/// 带 `node` 标签：一份不带节点名的自报落 `node=""`，节点采集器那一拍落它自己的节点名。
///
/// 为什么非得按节点分：一份都不进来时，`max(increase(...))` 是 0，可这个 0 分不出
/// 「整条通道断了」与「十台节点里有一台的采集器死了」——后者的读数是另外九台的
/// 流量在撑着，而屏上仍然是九台有进程表、一台的每一格都写着「这一拍没有它」。
/// 采集器的 Pod 是 Ready 的、日志也不报错，能问出「这台停了多久」的只有按节点的
/// 到达时刻。
async fn publish_ingest(
    metrics: &Option<Arc<dyn cog_core::MetricsBackend>>,
    outcome: &str,
    node: &str,
    value: f64,
) {
    let Some(backend) = metrics.as_ref() else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert(crate::LABEL_OUTCOME.to_string(), outcome.to_string());
    labels.insert(crate::LABEL_NODE.to_string(), node.trim().to_string());
    let _ = backend
        .record_counter(
            cog_core::metric_names::PROCESS_REPORT_INGEST_TOTAL,
            value,
            labels,
        )
        .await;
}

/// 启动时把两格写成 0。
///
/// 不做这一步，一个从没收到过自报的网关与一条根本没有这条通道的网关在读数面上
/// 同形：两格都缺席。写了 0 之后，缺席只意味着「这个进程不发布这条读数」，而 0
/// 意味着通道在、但这一窗里一份都没进来——那是两种不同的故障，各自的修法也不同。
pub async fn publish_vocabulary(metrics: &Option<Arc<dyn cog_core::MetricsBackend>>) {
    for outcome in [INGEST_ACCEPTED, INGEST_REFUSED] {
        // 节点名空着：这一格说的是「这个进程有这条通道」，不是任何一台节点报过到。
        publish_ingest(metrics, outcome, "", 0.0).await;
    }
}

/// `POST /api/v1/cluster/process-report`
///
/// 一个容器把自己这一拍的进程表交上来。收下之后在总线上落一帧「集群这一面变了」，
/// 打开着的拓扑面立刻重读——读数由产生它的一方推上来，屏只负责把它画出来。
pub async fn process_report_handler(
    State(state): State<Arc<GatewayState>>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    // 两种形状：一条自报（容器自报），或一拍一整批（节点采集器）。用一个端点承载，
    // 因为这是同一条通道、同一份账；分成两条端点只会让「哪一条被网络策略挡了」
    // 这个问题多出一个答案。
    let parsed = if is_batch(&body) {
        serde_json::from_value::<CensusBody>(body)
    } else {
        serde_json::from_value::<ReportBody>(body).map(|single| CensusBody {
            node: String::new(),
            reports: vec![single],
        })
    };
    let batch = match parsed {
        Ok(batch) => batch,
        Err(error) => {
            publish_ingest(&state.metrics_backend, INGEST_REFUSED, "", 1.0).await;
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("自报的形状不对：{error}") })),
            )
                .into_response();
        }
    };
    ingest(batch, &state.event_tx, &state.metrics_backend).await
}

/// 一批还是一条：看的是有没有 `reports` 这一项，不是看能不能解析出来——一条自报的
/// 形状在「批」那个反序列化里也能过（`reports` 有默认值），不先分就会把每一条都读成
/// 一批空表，而空表是「这一拍什么都没读到」，不是「这条自报的形状」。
fn is_batch(body: &serde_json::Value) -> bool {
    body.get("reports").is_some()
}

/// 收口与作答，不碰 `GatewayState`：这样一条自报的完整处理（含它落下的那一格
/// 读数）在测试里跑得起来。
///
/// 指标那一格不是装饰：一条被网络策略丢掉的请求与一个从没启动过的自报循环，在
/// 屏上都读作「这个容器没有进程表」。有这一格，才分得开「一份都没进来」（通道
/// 断了）与「别人在报、它不报」（是那个容器的事）。
async fn ingest(
    body: CensusBody,
    events: &tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
    metrics: &Option<Arc<dyn cog_core::MetricsBackend>>,
) -> axum::response::Response {
    // 活体先记：一台节点的采集器连进来、但这一拍一个容器都没读到（节点刚起来），
    // 与采集器根本没起来，在屏上是两种不同的空白。
    let node = body.node.trim().to_string();
    record_census(&node);
    let mut accepted = 0usize;
    let mut refused: Vec<String> = Vec::new();
    let mut changed = false;
    for report in body.reports {
        let container_id = container_id_of(&report.container_id).to_string();
        if container_id.is_empty() || container_id.len() > 128 {
            refused.push(format!("容器 id 不成立：`{}`", report.container_id));
            continue;
        }
        let processes = sanitize(report.processes);
        if processes.is_empty() {
            refused.push(format!("{container_id}: 进程表是空的"));
            continue;
        }
        accepted += 1;
        changed |= record(
            &container_id,
            Report {
                node: node.clone(),
                pod_uid: report
                    .pod_uid
                    .as_deref()
                    .map(str::trim)
                    .filter(|uid| !uid.is_empty())
                    // 采集器报的是 `metadata.uid` 的形式，这里再兜一次：systemd 的
                    // slice 名把连字符写成下划线，另一个实现在上游漏了这一步也不会
                    // 让认领悄悄失效——而认领失效在屏上读作「Pod 的沙箱没人认领」。
                    .map(|uid| uid.replace('_', "-")),
                reported_at: Utc::now(),
                processes,
            },
        );
    }
    if accepted > 0 {
        publish_ingest(metrics, INGEST_ACCEPTED, &node, accepted as f64).await;
    }
    if !refused.is_empty() {
        publish_ingest(metrics, INGEST_REFUSED, &node, refused.len() as f64).await;
    }
    let status = if accepted == 0 && !refused.is_empty() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::ACCEPTED
    };
    // 变了才落帧。帧的意思是「屏上该重读了」，而一个安静的容器每拍报的是同一张
    // 表；按拍落帧，屏就按拍重读一次一模一样的快照。
    // 帧里也不带进程表本身：内容由读的一方重读，两处都发一遍，账就有两本了。
    if changed {
        let _ = events.send(cog_core::AgentEvent::ResourceChanged {
            domain: "cluster".to_string(),
            timestamp: Utc::now(),
        });
    }
    (
        status,
        Json(serde_json::json!({
            "accepted": accepted,
            "changed": changed,
            "refused": refused,
            "census": !body.node.trim().is_empty(),
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_is_trimmed_to_its_own_shape() {
        let mut many: Vec<ReportedProcess> = (0..200)
            .map(|i| ReportedProcess {
                pid: i,
                parent_pid: 1,
                command: "x".repeat(200),
                rss_bytes: 1,
            })
            .collect();
        many.push(ReportedProcess {
            pid: 999,
            parent_pid: 1,
            command: "   ".to_string(),
            rss_bytes: 0,
        });
        let kept = sanitize(many);
        assert_eq!(kept.len(), MAX_REPORTS, "条数有上限");
        assert!(kept
            .iter()
            .all(|p| p.command.chars().count() <= MAX_COMMAND_CHARS));
        assert!(
            kept.iter().all(|p| !p.command.trim().is_empty()),
            "空白名字不算进程"
        );
    }

    #[test]
    fn a_report_older_than_the_window_is_not_the_current_state() {
        // 容器停了之后不会再有新的自报；旧的那一份不能一直当作现状显示。
        let container = "container-old";
        record(
            container,
            Report {
                pod_uid: None,
                node: String::new(),
                reported_at: Utc::now() - chrono::Duration::seconds(FRESH_FOR.as_secs() as i64 + 5),
                processes: vec![ReportedProcess {
                    pid: 1,
                    parent_pid: 0,
                    command: "sh".to_string(),
                    rss_bytes: 0,
                }],
            },
        );
        assert!(latest(container).is_none(), "过期的一份读作「没有读数」");
    }

    #[test]
    fn the_latest_report_wins() {
        let container = "c0ffee";
        record(
            container,
            Report {
                pod_uid: None,
                node: String::new(),
                reported_at: Utc::now() - chrono::Duration::seconds(1),
                processes: vec![ReportedProcess {
                    pid: 1,
                    parent_pid: 0,
                    command: "old".to_string(),
                    rss_bytes: 0,
                }],
            },
        );
        record(
            container,
            Report {
                pod_uid: None,
                node: String::new(),
                reported_at: Utc::now(),
                processes: vec![ReportedProcess {
                    pid: 2,
                    parent_pid: 0,
                    command: "new".to_string(),
                    rss_bytes: 0,
                }],
            },
        );
        let report = latest(container).expect("新鲜的这一份");
        assert_eq!(report.processes[0].command, "new");
    }

    #[test]
    fn only_a_changed_table_is_worth_a_frame() {
        // 一个安静的容器每拍报同一张表；每拍落帧就是让屏每拍重读同一份快照。
        let container = "beef";
        let table = vec![ReportedProcess {
            pid: 1,
            parent_pid: 0,
            command: "cogneva".to_string(),
            rss_bytes: 4096,
        }];
        assert!(
            record(
                container,
                Report {
                    pod_uid: None,
                    node: String::new(),
                    reported_at: Utc::now(),
                    processes: table.clone(),
                }
            ),
            "第一次报到，屏上还没有这一格"
        );
        assert!(
            !record(
                container,
                Report {
                    pod_uid: None,
                    node: String::new(),
                    reported_at: Utc::now(),
                    processes: table,
                }
            ),
            "同样的表再来一次，没有要屏重读的东西"
        );
        assert!(
            record(
                container,
                Report {
                    pod_uid: None,
                    node: String::new(),
                    reported_at: Utc::now(),
                    processes: vec![ReportedProcess {
                        pid: 7,
                        parent_pid: 1,
                        command: "sh".to_string(),
                        rss_bytes: 0,
                    }],
                }
            ),
            "多了一个进程，这才是该推的那一帧"
        );
    }

    #[test]
    fn a_container_that_stopped_does_not_keep_its_slot() {
        // 没有这次改动就会红：键是容器实例 id，而采集器按宿主上每一个容器实例入账。
        // 评测环境一天起停几百个容器，只写不删时这张表跟着开机时长长；过期的那份
        // 读不出来，所以屏上没有任何一格能看出它在长。
        let suffix = std::process::id();
        let stale = format!("stopped-{suffix}");
        let live = format!("running-{suffix}");
        record(
            &stale,
            Report {
                pod_uid: None,
                node: String::new(),
                reported_at: Utc::now() - chrono::Duration::seconds(FRESH_FOR.as_secs() as i64 + 5),
                processes: vec![ReportedProcess {
                    pid: 1,
                    parent_pid: 0,
                    command: "sh".to_string(),
                    rss_bytes: 0,
                }],
            },
        );
        record(
            &live,
            Report {
                pod_uid: None,
                node: String::new(),
                reported_at: Utc::now(),
                processes: vec![ReportedProcess {
                    pid: 2,
                    parent_pid: 0,
                    command: "sh".to_string(),
                    rss_bytes: 0,
                }],
            },
        );
        let reports = REPORTS.read().unwrap_or_else(|e| e.into_inner());
        assert!(
            !reports.contains_key(&stale),
            "过期的那一格没人读得到，却一直占着地方"
        );
        assert!(reports.contains_key(&live), "活着的那一格不能被顺手清掉");
    }

    #[test]
    fn a_runtime_prefix_is_not_part_of_the_identity() {
        // Pod 状态写 `containerd://<id>`，容器自己读到的 cgroup 只有 `<id>`。
        // 两处折成同一个形状才谈得上「对得上」。
        assert_eq!(container_id_of("containerd://abc123"), "abc123");
        assert_eq!(container_id_of("docker://abc123"), "abc123");
        assert_eq!(container_id_of("abc123"), "abc123");
        assert_eq!(container_id_of(""), "");
    }

    #[test]
    fn the_gap_counts_the_containers_this_face_cannot_place() {
        // 没有这次改动就会红：采集器按整台宿主入账，这一面按一个命名空间取 Pod。
        // 两边差的那些容器（traefik、coredns、prometheus 这些）在屏上没有任何一格，
        // 于是「全量」这句话在读数面上无从检验——差多少、差在哪个节点，都读不出来。
        let suffix = std::process::id();
        let node = format!("node-gap-{suffix}");
        let placed = format!("placed-{suffix}");
        let elsewhere = format!("elsewhere-{suffix}");
        let other_node = format!("elsewhere-node-{suffix}");
        let another = "another-node".to_string();
        record_census(&node);
        for (id, owner) in [
            (&placed, &node),
            (&elsewhere, &node),
            (&other_node, &another),
        ] {
            record(
                id,
                Report {
                    pod_uid: None,
                    node: owner.clone(),
                    reported_at: Utc::now(),
                    processes: vec![ReportedProcess {
                        pid: 1,
                        parent_pid: 0,
                        command: "sh".to_string(),
                        rss_bytes: 0,
                    }],
                },
            );
        }
        let claims = Claims {
            containers: [placed.clone()].into_iter().collect(),
            pods: HashSet::new(),
        };
        let gap = census_gap(&node, &claims).expect("有采集器的节点要读得出差");
        assert_eq!(
            (gap.seen, gap.unclaimed.len()),
            (2, 1),
            "这台节点上两份表、其中一份没有 Pod 认领；另一台节点的表不算进来"
        );
        assert_eq!(
            gap.unclaimed[0].id,
            elsewhere.chars().take(12).collect::<String>(),
            "缺口里要点名是哪一个容器，不是一个只够相减的数"
        );
        assert_eq!(
            census_gap(&format!("no-collector-{suffix}"), &claims),
            None,
            "没有采集器的节点：「不知道」，不是「一个都没漏」"
        );
    }

    // ---- 收口那一格读数 ----

    use cog_core::MetricsBackend as _;

    fn memory_metrics() -> Arc<crate::test_metrics::TestMetrics> {
        Arc::new(crate::test_metrics::TestMetrics::new())
    }

    fn as_backend(
        metrics: &Arc<crate::test_metrics::TestMetrics>,
    ) -> Option<Arc<dyn cog_core::MetricsBackend>> {
        Some(metrics.clone())
    }

    async fn cell(
        metrics: &Arc<crate::test_metrics::TestMetrics>,
        outcome: &str,
        node: &str,
    ) -> Option<f64> {
        metrics
            .query_counter_totals(cog_core::metric_names::PROCESS_REPORT_INGEST_TOTAL.as_str())
            .await
            .expect("读得到自己刚写的读数")
            .iter()
            .find(|sample| {
                sample.labels.get(crate::LABEL_OUTCOME).map(String::as_str) == Some(outcome)
                    && sample.labels.get(crate::LABEL_NODE).map(String::as_str) == Some(node)
            })
            .map(|sample| sample.value)
    }

    fn body(pod: &str, container_id: &str, processes: Vec<ReportedProcess>) -> CensusBody {
        CensusBody {
            node: String::new(),
            reports: vec![ReportBody {
                pod: pod.to_string(),
                container_id: container_id.to_string(),
                pod_uid: None,
                processes,
            }],
        }
    }

    fn batch(node: &str, reports: Vec<ReportBody>) -> CensusBody {
        CensusBody {
            node: node.to_string(),
            reports,
        }
    }

    fn report(pod: &str, container_id: &str, command: &str) -> ReportBody {
        ReportBody {
            pod_uid: None,
            pod: pod.to_string(),
            container_id: container_id.to_string(),
            processes: vec![ReportedProcess {
                pid: 1,
                parent_pid: 0,
                command: command.to_string(),
                rss_bytes: 4096,
            }],
        }
    }

    fn one_process() -> Vec<ReportedProcess> {
        vec![ReportedProcess {
            pid: 1,
            parent_pid: 0,
            command: "cogneva".to_string(),
            rss_bytes: 4096,
        }]
    }

    #[tokio::test]
    async fn an_accepted_report_leaves_a_cell_naming_it() {
        // 没有这次改动就会红：那时到达与没到达在读数面上都缺席，一条被网络策略
        // 丢掉的请求与一个从没启动过的自报循环同形。
        let metrics = memory_metrics();
        let backend = as_backend(&metrics);
        let (events, _rx) = tokio::sync::broadcast::channel(8);

        let response = ingest(body("pod-a", "a1a1a1", one_process()), &events, &backend).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(cell(&metrics, INGEST_ACCEPTED, "").await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_REFUSED, "").await,
            None,
            "收下的一份不该同时记在拒绝那一格里"
        );
        assert_eq!(latest("a1a1a1").map(|r| r.processes.len()), Some(1));
    }

    #[tokio::test]
    async fn a_batch_is_counted_under_the_node_that_sent_it() {
        // 没有这次改动就会红：那时每一份到达都落在同一格上，一台节点的采集器死了
        // 也被其它来源（容器自报、别的节点）的流量盖住——「这台停了多久」在读数面
        // 上没有答案，而屏上只是它上面每一个容器写着「这一拍没有它」。
        let metrics = memory_metrics();
        let backend = as_backend(&metrics);
        let (events, _rx) = tokio::sync::broadcast::channel(8);

        // 容器 id 与别的用例不重叠：这张表是整个进程一份的静态账，撞了 id 就会
        // 让别的用例读到这一条写下的读数。
        ingest(
            batch("node-a", vec![report("pod-c", "c3c3c3", "sh")]),
            &events,
            &backend,
        )
        .await;
        ingest(body("pod-d", "d4d4d4", one_process()), &events, &backend).await;

        assert_eq!(cell(&metrics, INGEST_ACCEPTED, "node-a").await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_ACCEPTED, "node-b").await,
            None,
            "另一台节点没有报过，那一格必须是缺席而不是 0"
        );
        assert_eq!(
            cell(&metrics, INGEST_ACCEPTED, "").await,
            Some(1.0),
            "不带节点名的自报单独记一格，不被采集器那一拍盖掉"
        );
    }

    #[tokio::test]
    async fn a_refused_report_leaves_the_other_cell() {
        let metrics = memory_metrics();
        let backend = as_backend(&metrics);
        let (events, _rx) = tokio::sync::broadcast::channel(8);

        let response = ingest(body("pod-b", "b2b2b2", Vec::new()), &events, &backend).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(cell(&metrics, INGEST_REFUSED, "").await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_ACCEPTED, "").await,
            None,
            "被拒的那一份没有收下，不该记在收下那一格"
        );
        assert!(latest("b2b2b2").is_none(), "拒收不落库");
    }

    #[tokio::test]
    async fn the_vocabulary_is_written_at_zero_so_absence_means_no_publisher() {
        // 0 与缺席是两件事：0 说通道在、这一窗一份都没进来；缺席说这个进程根本
        // 不发布这条读数。不先写 0，两种故障在读数面上一模一样。
        let metrics = memory_metrics();
        publish_vocabulary(&as_backend(&metrics)).await;
        assert_eq!(cell(&metrics, INGEST_ACCEPTED, "").await, Some(0.0));
        assert_eq!(cell(&metrics, INGEST_REFUSED, "").await, Some(0.0));
    }

    #[tokio::test]
    async fn a_batch_lands_every_container_on_the_node() {
        // 没有这次改动就会红：采集器一拍报几十个容器，而端点只认单条形状时
        // 整批被拒——屏上每一个第三方容器都读作「没有进程表」。
        let (events, _rx) = tokio::sync::broadcast::channel(8);
        let response = ingest(
            batch(
                "node-1",
                vec![
                    report("", "p1", "postgres"),
                    report("", "p2", "nats-server"),
                    report("", "p3", "redis-server"),
                ],
            ),
            &events,
            &None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(latest("p1").map(|r| r.processes.len()), Some(1));
        assert_eq!(
            latest("p2").expect("第二台也在").processes[0].command,
            "nats-server"
        );
        assert!(latest("p3").is_some(), "第三台也在");
        assert!(census_at("node-1").is_some(), "这一拍同时是这台节点的活体");
    }

    #[tokio::test]
    async fn one_bad_report_does_not_take_the_whole_batch_down() {
        // 一个容器读不到不该让同一台节点上另外几十个容器的读数一起消失。
        let (events, _rx) = tokio::sync::broadcast::channel(8);
        let response = ingest(
            batch(
                "node-2",
                vec![
                    report("", "g1", "kubelet"),
                    ReportBody {
                        pod_uid: None,
                        pod: String::new(),
                        container_id: String::new(),
                        processes: one_process(),
                    },
                ],
            ),
            &events,
            &None,
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::ACCEPTED,
            "有一条坏的，其余照收"
        );
        assert!(latest("g1").is_some());
    }

    #[tokio::test]
    async fn a_node_without_a_census_is_not_a_node_without_processes() {
        // 「没有采集器」与「采集器在跑、这一格读不到」是两个修法相反的现场，
        // 靠这一格分开。
        assert!(census_at("node-never-reported").is_none());
        record_census("node-3");
        assert!(census_at("node-3").is_some());
        record_census("   ");
        assert!(census_at("").is_none(), "空节点名不是一个节点");
    }

    #[tokio::test]
    async fn without_a_metrics_backend_the_report_still_lands() {
        // 指标是可选的：没有后端时这一格不存在，但自报照收——读数少一格不该
        // 变成功能少一格。
        let (events, _rx) = tokio::sync::broadcast::channel(8);
        let response = ingest(body("pod-c", "c3c3c3", one_process()), &events, &None).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(latest("c3c3c3").is_some());
    }

    /// 没有这次改动就会红：缺口只报一个数时，读者知道「有几个没有格子」，
    /// 却不知道漏的是谁——而宿主上最吃机器的恰恰是那几个没有 Pod 的。
    #[test]
    fn an_unclaimed_container_is_named_by_the_processes_it_runs() {
        // 节点名与容器 id 都不与别的用例重叠：这张表是整个进程一份的静态账。
        let node = "census-gap-node-unclaimed";
        let claimed_id = "e1e1e1e1e1e1";
        let build_id = "f2f2f2f2f2f2f2";
        let stale_id = "a9a9a9a9a9a9";
        let elsewhere_id = "b7b7b7b7b7b7";
        record_census(node);

        let process = |pid: u32, command: &str, rss_bytes: u64| ReportedProcess {
            pid,
            parent_pid: 1,
            command: command.to_string(),
            rss_bytes,
        };
        record(
            claimed_id,
            Report {
                pod_uid: None,
                node: node.to_string(),
                reported_at: Utc::now(),
                processes: vec![process(1, "nginx", 100)],
            },
        );
        record(
            build_id,
            Report {
                pod_uid: None,
                node: node.to_string(),
                reported_at: Utc::now(),
                // 两个 cargo：去重后只报一次；顺序按内存，不按 pid。
                processes: vec![
                    process(10, "cargo", 900),
                    process(11, "rustc", 800),
                    process(12, "cargo", 700),
                    process(13, "sh", 600),
                    process(14, "ld", 500),
                    process(15, "cc", 400),
                ],
            },
        );
        record(
            stale_id,
            Report {
                pod_uid: None,
                node: node.to_string(),
                reported_at: Utc::now() - chrono::Duration::seconds(FRESH_FOR.as_secs() as i64 + 5),
                processes: vec![process(20, "left-behind", 100)],
            },
        );
        record(
            elsewhere_id,
            Report {
                pod_uid: None,
                node: "census-gap-node-elsewhere".to_string(),
                reported_at: Utc::now(),
                processes: vec![process(30, "other-node", 100)],
            },
        );

        let mut claims = Claims::default();
        claims.containers.insert(claimed_id.to_string());
        let gap = census_gap(node, &claims).expect("采集器报到过，差就该读得出来");

        assert_eq!(gap.seen, 2, "过期的那份与别的节点上的都不算这一拍");
        assert_eq!(gap.unclaimed.len(), 1, "被认领的那个不在缺口里");
        let row = &gap.unclaimed[0];
        assert_eq!(row.id, &build_id[..12], "只给前 12 个字符，够认出同一个");
        assert_eq!(row.processes, 6);
        assert_eq!(
            row.commands,
            vec!["cargo", "rustc", "sh", "ld"],
            "按内存从大到小、去重、封顶"
        );
    }

    /// 没有这次改动就会红：Pod 的沙箱容器（pause）在宿主上有自己的一份进程表，
    /// 但 Kubernetes 不把它写进 `containerStatuses`。只按容器 id 认领时，集群里
    /// 每一个 Pod 都会在缺口里留下一条，而屏上那句话的意思是「宿主上有东西没人
    /// 认领」——一个每个 Pod 都会触发的假读数。
    #[test]
    fn a_pod_sandbox_container_is_claimed_by_the_pod_it_belongs_to() {
        let node = "census-gap-node-sandbox";
        let id = "c7c7c7c7c7c7c7";
        let uid = "1288f852-f85f-4fce-a027-9fffbdcb0f26";
        record_census(node);
        record(
            id,
            Report {
                node: node.to_string(),
                pod_uid: Some(uid.to_string()),
                reported_at: Utc::now(),
                processes: vec![ReportedProcess {
                    pid: 1,
                    parent_pid: 0,
                    command: "pause".to_string(),
                    rss_bytes: 0,
                }],
            },
        );

        let mut claims = Claims::default();
        assert_eq!(
            census_gap(node, &claims).map(|gap| gap.unclaimed.len()),
            Some(1),
            "没有任何 Pod 时它当然没人认领"
        );
        claims.pods.insert(uid.to_string());
        let gap = census_gap(node, &claims).expect("采集器报到过");
        assert_eq!(
            (gap.seen, gap.unclaimed.len()),
            (1, 0),
            "Pod 自己就在屏上，它的沙箱容器有位置可去"
        );
    }

    /// 从采集器报上来到屏上认领的那一整条路：报上来的 uid 是 systemd 的下划线写法，
    /// 认领用的是 `metadata.uid` 的连字符写法，中间这一步没人做就会静默失效。
    #[tokio::test]
    async fn the_reported_pod_uid_survives_the_trip_from_the_collector_to_the_claim() {
        let (events, _rx) = tokio::sync::broadcast::channel(8);
        let node = "census-ingest-pod-uid";
        let id = "9a9a9a9a9a9a9a";
        let response = ingest(
            CensusBody {
                node: node.to_string(),
                reports: vec![ReportBody {
                    pod: String::new(),
                    container_id: id.to_string(),
                    pod_uid: Some("1288f852_f85f_4fce_a027_9fffbdcb0f26".to_string()),
                    processes: one_process(),
                }],
            },
            &events,
            &None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let mut claims = Claims::default();
        claims
            .pods
            .insert("1288f852-f85f-4fce-a027-9fffbdcb0f26".to_string());
        assert_eq!(
            census_gap(node, &claims).map(|gap| gap.unclaimed.len()),
            Some(0),
            "两种写法要折成一种，否则沙箱容器永远认领不上"
        );
    }

    /// 缺口为空与「没有采集器」不是一回事：前者是零个，后者是不知道。
    #[test]
    fn a_node_with_a_census_and_nothing_unclaimed_reads_as_zero_not_as_unknown() {
        let node = "census-gap-node-complete";
        let id = "d5d5d5d5d5d5";
        record_census(node);
        record(
            id,
            Report {
                pod_uid: None,
                node: node.to_string(),
                reported_at: Utc::now(),
                processes: vec![ReportedProcess {
                    pid: 1,
                    parent_pid: 0,
                    command: "app".to_string(),
                    rss_bytes: 1,
                }],
            },
        );
        let mut claims = Claims::default();
        claims.containers.insert(id.to_string());

        let gap = census_gap(node, &claims).expect("有采集器就有读数");
        assert_eq!((gap.seen, gap.unclaimed.len()), (1, 0));
        assert!(census_gap("census-gap-node-absent", &Claims::default()).is_none());
    }
}
