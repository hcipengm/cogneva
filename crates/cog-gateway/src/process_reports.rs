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

use std::collections::HashMap;
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
async fn publish_ingest(
    metrics: &Option<Arc<dyn cog_core::MetricsBackend>>,
    outcome: &str,
    value: f64,
) {
    let Some(backend) = metrics.as_ref() else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert("outcome".to_string(), outcome.to_string());
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
        publish_ingest(metrics, outcome, 0.0).await;
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
            publish_ingest(&state.metrics_backend, INGEST_REFUSED, 1.0).await;
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
    record_census(&body.node);
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
                reported_at: Utc::now(),
                processes,
            },
        );
    }
    if accepted > 0 {
        publish_ingest(metrics, INGEST_ACCEPTED, accepted as f64).await;
    }
    if !refused.is_empty() {
        publish_ingest(metrics, INGEST_REFUSED, refused.len() as f64).await;
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
    fn a_runtime_prefix_is_not_part_of_the_identity() {
        // Pod 状态写 `containerd://<id>`，容器自己读到的 cgroup 只有 `<id>`。
        // 两处折成同一个形状才谈得上「对得上」。
        assert_eq!(container_id_of("containerd://abc123"), "abc123");
        assert_eq!(container_id_of("docker://abc123"), "abc123");
        assert_eq!(container_id_of("abc123"), "abc123");
        assert_eq!(container_id_of(""), "");
    }

    // ---- 收口那一格读数 ----

    use cog_core::MetricsBackend as _;

    fn memory_metrics() -> Arc<cog_storage::mem::MemoryMetricsBackend> {
        Arc::new(cog_storage::mem::MemoryMetricsBackend::new())
    }

    fn as_backend(
        metrics: &Arc<cog_storage::mem::MemoryMetricsBackend>,
    ) -> Option<Arc<dyn cog_core::MetricsBackend>> {
        Some(metrics.clone())
    }

    async fn cell(
        metrics: &Arc<cog_storage::mem::MemoryMetricsBackend>,
        outcome: &str,
    ) -> Option<f64> {
        metrics
            .query_counter_totals(cog_core::metric_names::PROCESS_REPORT_INGEST_TOTAL.as_str())
            .await
            .expect("读得到自己刚写的读数")
            .iter()
            .find(|sample| sample.labels.get("outcome").map(String::as_str) == Some(outcome))
            .map(|sample| sample.value)
    }

    fn body(pod: &str, container_id: &str, processes: Vec<ReportedProcess>) -> CensusBody {
        CensusBody {
            node: String::new(),
            reports: vec![ReportBody {
                pod: pod.to_string(),
                container_id: container_id.to_string(),
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
        assert_eq!(cell(&metrics, INGEST_ACCEPTED).await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_REFUSED).await,
            None,
            "收下的一份不该同时记在拒绝那一格里"
        );
        assert_eq!(latest("a1a1a1").map(|r| r.processes.len()), Some(1));
    }

    #[tokio::test]
    async fn a_refused_report_leaves_the_other_cell() {
        let metrics = memory_metrics();
        let backend = as_backend(&metrics);
        let (events, _rx) = tokio::sync::broadcast::channel(8);

        let response = ingest(body("pod-b", "b2b2b2", Vec::new()), &events, &backend).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(cell(&metrics, INGEST_REFUSED).await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_ACCEPTED).await,
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
        assert_eq!(cell(&metrics, INGEST_ACCEPTED).await, Some(0.0));
        assert_eq!(cell(&metrics, INGEST_REFUSED).await, Some(0.0));
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
}
