//! 容器自报的进程表。
//!
//! 进程表只有容器自己读得全：它跑在自己的 PID 命名空间里，`/proc` 就是它自己的
//! 那一份。让网关伸手进去拿（exec 进容器跑 ps）要换一项对业务容器执行命令的权限，
//! 而且每多一个读者就多敲一次命令；反过来，容器按一拍把自己那一份推上来，读数就
//! 变成了事件流里的一帧，屏上开的几面读的是同一份。
//!
//! 这条通道的边界是诚实的：只有我们自己的镜像会报（跑的是 `cogneva` 这个二进制），
//! 第三方镜像（postgres、nats、registry）不会，那一格显示「不报」——不是「没有进程」。
//!
//! 落点按**容器实例**（Pod 名 + 容器 id）存，不按容器名：报上来的那一方只能读到
//! 自己的 cgroup，读不到自己叫什么名字；而 Pod 状态里每个容器都带着 `containerID`，
//! 于是「谁报的」这件事在渲染那一步对得上，不需要给每个容器注入一个名字变量。
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

/// 进程自报的落脚处。按 (Pod, 容器 id) 存最近一份——**最近一份就够**：这一面问的
/// 是「现在它里面跑着什么」，历史在别处（如果哪天要历史，那是另一条读数，不是这一格）。
static REPORTS: LazyLock<RwLock<HashMap<(String, String), Report>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 收下一份自报，回答「这张表跟上一份比变了吗」。没变就不必在总线上落帧：
/// 一个容器每拍都报同一张表，而帧的意义是「屏上该重读了」。
pub fn record(pod: &str, container_id: &str, report: Report) -> bool {
    let mut reports = REPORTS.write().unwrap_or_else(|e| e.into_inner());
    let key = (pod.to_string(), container_id.to_string());
    let changed = reports
        .get(&key)
        .map(|previous| previous.processes != report.processes)
        .unwrap_or(true);
    reports.insert(key, report);
    changed
}

/// 某个容器实例最近一份还没过期的自报。
pub fn latest(pod: &str, container_id: &str) -> Option<Report> {
    let reports = REPORTS.read().unwrap_or_else(|e| e.into_inner());
    reports
        .get(&(pod.to_string(), container_id.to_string()))
        .filter(|report| {
            Utc::now()
                .signed_duration_since(report.reported_at)
                .to_std()
                .map(|age| age <= FRESH_FOR)
                .unwrap_or(false)
        })
        .cloned()
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
    /// 报上来的那个 Pod。容器里那个变量就是 kubelet 写进去的主机名。
    pub pod: String,
    /// 报上来的那个容器实例（cgroup 里那串 id），与 Pod 状态里的
    /// `containerStatuses[].containerID` 对得上。
    pub container_id: String,
    pub processes: Vec<ReportedProcess>,
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
    Json(body): Json<ReportBody>,
) -> axum::response::Response {
    ingest(body, &state.event_tx, &state.metrics_backend).await
}

/// 收口与作答，不碰 `GatewayState`：这样一条自报的完整处理（含它落下的那一格
/// 读数）在测试里跑得起来。
///
/// 指标那一格不是装饰：一条被网络策略丢掉的请求与一个从没启动过的自报循环，在
/// 屏上都读作「这个容器没有进程表」。有这一格，才分得开「一份都没进来」（通道
/// 断了）与「别人在报、它不报」（是那个容器的事）。
async fn ingest(
    body: ReportBody,
    events: &tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
    metrics: &Option<Arc<dyn cog_core::MetricsBackend>>,
) -> axum::response::Response {
    let pod = body.pod.trim().to_string();
    let container_id = container_id_of(&body.container_id).to_string();
    if pod.is_empty() || container_id.is_empty() || container_id.len() > 128 {
        publish_ingest(metrics, INGEST_REFUSED, 1.0).await;
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "自报要带 pod 与 container_id" })),
        )
            .into_response();
    }
    let processes = sanitize(body.processes);
    if processes.is_empty() {
        publish_ingest(metrics, INGEST_REFUSED, 1.0).await;
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "空的进程表：读 /proc 的进程自己就是一行，一行都没有说明没读成" })),
        )
            .into_response();
    }
    let count = processes.len();
    let changed = record(
        &pod,
        &container_id,
        Report {
            reported_at: Utc::now(),
            processes,
        },
    );
    publish_ingest(metrics, INGEST_ACCEPTED, 1.0).await;
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
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "accepted": count, "changed": changed })),
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
        let key = ("pod-old".to_string(), "container-old".to_string());
        record(
            &key.0,
            &key.1,
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
        assert!(
            latest(&key.0, &key.1).is_none(),
            "过期的一份读作「没有读数」"
        );
    }

    #[test]
    fn the_latest_report_wins() {
        let pod = "pod-latest";
        let container = "c0ffee";
        record(
            pod,
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
            pod,
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
        let report = latest(pod, container).expect("新鲜的这一份");
        assert_eq!(report.processes[0].command, "new");
    }

    #[test]
    fn only_a_changed_table_is_worth_a_frame() {
        // 一个安静的容器每拍报同一张表；每拍落帧就是让屏每拍重读同一份快照。
        let pod = "pod-changed";
        let container = "beef";
        let table = vec![ReportedProcess {
            pid: 1,
            parent_pid: 0,
            command: "cogneva".to_string(),
            rss_bytes: 4096,
        }];
        assert!(
            record(
                pod,
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
                pod,
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
                pod,
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

    fn body(pod: &str, container_id: &str, processes: Vec<ReportedProcess>) -> ReportBody {
        ReportBody {
            pod: pod.to_string(),
            container_id: container_id.to_string(),
            processes,
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

        let response = ingest(body("pod-a", "aaaa", one_process()), &events, &backend).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(cell(&metrics, INGEST_ACCEPTED).await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_REFUSED).await,
            None,
            "收下的一份不该同时记在拒绝那一格里"
        );
        assert_eq!(latest("pod-a", "aaaa").map(|r| r.processes.len()), Some(1));
    }

    #[tokio::test]
    async fn a_refused_report_leaves_the_other_cell() {
        let metrics = memory_metrics();
        let backend = as_backend(&metrics);
        let (events, _rx) = tokio::sync::broadcast::channel(8);

        let response = ingest(body("pod-b", "bbbb", Vec::new()), &events, &backend).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(cell(&metrics, INGEST_REFUSED).await, Some(1.0));
        assert_eq!(
            cell(&metrics, INGEST_ACCEPTED).await,
            None,
            "被拒的那一份没有收下，不该记在收下那一格"
        );
        assert!(latest("pod-b", "bbbb").is_none(), "拒收不落库");
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
    async fn without_a_metrics_backend_the_report_still_lands() {
        // 指标是可选的：没有后端时这一格不存在，但自报照收——读数少一格不该
        // 变成功能少一格。
        let (events, _rx) = tokio::sync::broadcast::channel(8);
        let response = ingest(body("pod-c", "cccc", one_process()), &events, &None).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(latest("pod-c", "cccc").is_some());
    }
}
