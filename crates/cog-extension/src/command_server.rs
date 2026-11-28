//! Standalone sandbox executor service (`cogneva sandbox-executor`).
//!
//! Executes environment payloads (shell commands, file reads/writes) for the
//! cluster's tool layer and streams output back as NDJSON
//! [`cog_core::CommandEvent`] lines. The pod running this service mounts no
//! secrets and holds no credentials — isolation is the pod boundary, so
//! commands may use full shell syntax by design.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use cog_core::contract::observability::METRICS_PATH;
use cog_core::{CommandEvent, SFError, SandboxPayload};
use futures::StreamExt;
use prometheus::{Encoder, HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry, TextEncoder};
use serde::Deserialize;
use tracing::warn;

use crate::hostdocs::{HostDocOp, HostDocPlan, HostDocs};
use crate::lane_sizing::{self, BuildCost, BuildEnd, BuildSample, LanePlan, LaneSource};
use crate::pressure::{self, Pressure};
use crate::runtime::cgroup::CgroupFacts;
use crate::workdir::{self, CarryError, CommandHold, WorkdirRouter};

#[derive(Clone)]
struct AppState {
    /// Per-task worktree router; absent when the executor runs without a
    /// seeded bare repo (tests, embedded usage), keeping legacy cwd semantics.
    workdir: Option<Arc<WorkdirRouter>>,
    /// Host document scopes; absent unless the deployment mounted one, which
    /// keeps the capability off by default rather than one bad path away from
    /// a writable root.
    hostdocs: Option<Arc<HostDocs>>,
    /// Who gets to compile, and who is waiting. See
    /// [`crate::runtime::local::builds_the_tree`] for what this bounds,
    /// [`BuildLane`] for why the queue turns by agent rather than by arrival,
    /// and [`lane_target_dir`] for why a lane is more than a count.
    build_lane: Arc<BuildLane>,
    /// What each request cost the executor. Owned by the state rather than by
    /// the workdir router, because a deployment without one still runs file
    /// operations, and a latency reading that existed only on some deployments
    /// would be quoted as if it covered all of them.
    commands: Arc<CommandMetrics>,
    /// How long this executor's build queue normally waits. The queue is the
    /// one thing on this side that emits no progress, so it is the one thing
    /// still judged by a duration — measured, not declared.
    queue_waits: Arc<BuildQueueWindow>,
    /// The room running commands have claimed on this replica. One ledger for
    /// both paths that admit work, so a command admitted on the tool path is
    /// part of what the build lane plans against.
    pressure: Arc<pressure::Ledger>,
}

/// The concurrency the deployment declared, from
/// `COGNEVA_SANDBOX_BUILD_CONCURRENCY`.
///
/// `None` is a deployment that named no count, which is not the same as naming
/// one: with none, the count comes from what this container measures (see
/// [`crate::lane_sizing::plan_lanes`]), and it reads as one only until a build
/// has been priced -- one is the count that ran before anything was measured.
/// A written zero is how a config spells "I wrote none", so it reads as none
/// too.
fn build_concurrency() -> Option<usize> {
    std::env::var(crate::lane_sizing::BUILD_CONCURRENCY_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
}

/// A build that waited at least this long for its turn is worth a line.
///
/// Below it are hand-offs between builds that were going to compile anyway;
/// above it is a round that may well have spent its wall waiting.
// cap: fixed — 记一笔的阈值，不是容量：它只决定哪些等待值得读，不决定谁等多久
// keep: 删掉后每一次排队交接都记一行，日志被本来就要编的交接刷满；由「只记值得读的等待」兜住
const BUILD_QUEUE_NOTEWORTHY: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a build may queue for a lane before the
/// executor refuses it, as a measurement rather than a number.
///
/// The window is the [`cog_core::progress::SILENCE_QUANTILE`] quantile of the
/// waits this executor has actually admitted. Below
/// [`cog_core::progress::SAMPLE_FLOOR`] samples there is no window and a waiter
/// waits — "we have not measured this machine yet" is not "you are stuck", and
/// the same floor keeps this reader and the movement predicate from disagreeing
/// about what counts as a sample.
///
/// It no longer shares a number with the command's own budget, because there is
/// no command budget any more: a command is stopped when it stops moving, not
/// when a clock runs out. What is left to bound here is a *queue*, which emits
/// no progress by definition, and the honest bound for it is how long this
/// machine's queue normally is.
pub struct BuildQueueWindow {
    inner: std::sync::Mutex<QueueSamples>,
}

#[derive(Default)]
struct QueueSamples {
    ring: std::collections::VecDeque<std::time::Duration>,
    window: Option<std::time::Duration>,
}

impl BuildQueueWindow {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(QueueSamples::default()),
        }
    }

    fn record(&self, waited: std::time::Duration) {
        let mut s = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        s.ring.push_back(waited);
        // Recent behaviour, not all of it: a window computed over the worst
        // hour this executor ever had would only ever widen.
        let capacity = cog_core::progress::SAMPLE_FLOOR * 10;
        while s.ring.len() > capacity {
            s.ring.pop_front();
        }
        s.window = quantile(&s.ring);
    }

    /// The measured window, or `None` when this executor has not admitted
    /// enough builds to know what a normal wait is.
    pub fn window(&self) -> Option<std::time::Duration> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).window
    }
}

impl Default for BuildQueueWindow {
    fn default() -> Self {
        Self::new()
    }
}

fn quantile(
    waits: &std::collections::VecDeque<std::time::Duration>,
) -> Option<std::time::Duration> {
    if waits.len() < cog_core::progress::SAMPLE_FLOOR {
        return None;
    }
    let mut sorted: Vec<_> = waits.iter().copied().collect();
    sorted.sort_unstable();
    let q = cog_core::progress::SILENCE_QUANTILE;
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

/// Port the executor listens on when `SANDBOX_EXECUTOR_PORT` names none.
///
/// The executor's Service targets this same port, so it is fixed in code
/// rather than remade per deployment: a missing or unusable variable binds
/// the port the Service already routes to, while the environment stays
/// authoritative when it names a usable port.
// cap: fixed — 执行器的 Service 路由到的那个口：没有可用环境变量时绑它，而不是每台部署另立一个
// keep: 删掉后没有可用环境变量时执行器绑不到 Service 路由的那个口，健康检查连不上；由「Service 的目标端口」兜住
const DEFAULT_EXECUTOR_PORT: u16 = 9090;

/// Resolve the listen port from `SANDBOX_EXECUTOR_PORT`.
///
/// A set but unusable value -- not a number, out of `u16` range, or 0, which
/// binds no port -- is not a configured port: the default applies instead of
/// failing to start, the same shape `BuildCacheConfig::from_env` uses for a
/// value it cannot parse.
fn executor_port_from_env() -> u16 {
    std::env::var("SANDBOX_EXECUTOR_PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
        .filter(|port| *port != 0)
        .unwrap_or(DEFAULT_EXECUTOR_PORT)
}

#[derive(Deserialize)]
struct ExecuteRequest {
    payload: SandboxPayload,
    task_id: Option<String>,
    agent_id: Option<String>,
}

async fn health() -> &'static str {
    "ok"
}

/// 执行器这一层管得住什么，四个维度各一行。
///
/// 状态词分开写，因为把「部署声明了」读成「这一层会拒」是这张表最容易犯的错：
/// `enforced` 是这一层在请求上真的会拒，拒绝时说得出一句话；`declared` 是部署声明
/// 了这条边界、这一层读不到它执行与否；`none` 是这一层根本不设边界；`unknown` 是
/// 这一层答不出来——「量不到」与「量到就是不设界」必须分得开，否则一条没人声明的
/// 边界会读起来和一条不存在的边界一样。
pub fn boundary_reading(workdir: Option<&WorkdirRouter>) -> cog_core::SandboxBoundary {
    use cog_core::{BoundaryLine, SandboxBoundary};

    let line = |dimension: &str, state: &str, detail: String| BoundaryLine {
        dimension: dimension.to_string(),
        state: state.to_string(),
        detail,
    };
    let facts = crate::runtime::cgroup::read();

    let writable = match workdir {
        Some(router) => {
            let cfg = router.config();
            line(
                "可写路径",
                "enforced",
                format!(
                    "相对路径挂在这棵树上（工作树根 {}，构建写入 {}）；走出这棵树的路径被拒并说出范围",
                    cfg.workspaces_root.display(),
                    cfg.target_dir.display()
                ),
            )
        }
        None => line(
            "可写路径",
            "none",
            "这个执行器没有车道：命令在进程自己的工作目录里跑，文件路径不做限制".to_string(),
        ),
    };

    let network = match std::env::var("SANDBOX_EGRESS_POLICY") {
        Ok(name) if !name.trim().is_empty() => line(
            "网络",
            "declared",
            format!(
                "出口由集群网络策略 {name} 管（审计网关联的模型端口、DNS、443、观测栈）；这一层读不到策略本体"
            ),
        ),
        _ => line(
            "网络",
            "unknown",
            "没有声明这一层该读哪条网络策略：出口是否被集群策略管，这里答不出来".to_string(),
        ),
    };

    let resources = if facts.limit_bytes.is_none() && facts.throttled_usec.is_none() {
        line(
            "资源",
            "unknown",
            "读不到 cgroup（宿主没有挂 memory.max / cpu.stat）".to_string(),
        )
    } else {
        let memory = match facts.limit_bytes {
            Some(bytes) => format!("内存上限 {} MiB", bytes / (1024 * 1024)),
            None => "内存不限（memory.max 是 max）".to_string(),
        };
        let cpu = match facts.throttled_usec {
            Some(usec) => format!("CPU 配额累计压住 {} ms", usec / 1000),
            None => "cpu.stat 读不到".to_string(),
        };
        line(
            "资源",
            "enforced",
            format!("容器 cgroup：{memory}；{cpu}（都是这一层量的）"),
        )
    };

    SandboxBoundary {
        kind: "executor".to_string(),
        isolated: true,
        workdir: workdir.map(|router| router.config().workspaces_root.display().to_string()),
        lines: vec![
            writable,
            network,
            resources,
            crate::runtime::local::silence_line(),
        ],
    }
}

async fn boundary(State(state): State<AppState>) -> Json<cog_core::SandboxBoundary> {
    Json(boundary_reading(state.workdir.as_deref()))
}

/// 这一刻的编译队：谁占着位子、谁排在第几位。只读，不改变任何人等的顺序。
async fn lane(State(state): State<AppState>) -> Json<cog_core::BuildLaneReading> {
    publish_shape_cost(&state);
    Json(build_lane_fairness(&state.build_lane))
}

/// 两种沙箱形态的价格表，从这台执行器量得到的东西算。
///
/// 每一格都在它自己该在的地方量：起停是本进程入口到监听，切树与搬树是
/// workdir 窗口里的中位，卷是最近一次卷走查的字节，等位子是编译队窗口的分位，
/// 一副副本能跑几份是压力除以实测单价。量不到的格子是 `None`，判决停在
/// 「没答案」并点名缺哪条——不拿一个偏好顶上。
fn shape_readings(state: &AppState) -> pressure::ShapeReadings {
    let demand = pressure::Demand::from_samples(&crate::runtime::cgroup::recent_peaks());
    let room = Pressure::from_facts(
        &crate::runtime::cgroup::read(),
        state.pressure.reserved_bytes(),
    );
    pressure::ShapeReadings {
        startup: pressure::startup_seconds(),
        checkout: state
            .workdir
            .as_ref()
            .and_then(|workdir| workdir.checkout_seconds()),
        queue_wait: state.queue_waits.window().map(|wait| wait.as_secs_f64()),
        idle_bytes: state.build_lane.idle_working_set(),
        tree_bytes: state
            .workdir
            .as_ref()
            .and_then(|workdir| workdir.tree_bytes()),
        tasks_per_replica: room.fits(&demand),
        carry: state
            .workdir
            .as_ref()
            .and_then(|workdir| workdir.carry_reading())
            .map(|(seconds, _bytes)| seconds),
    }
}

fn publish_shape_cost(state: &AppState) {
    pressure::publish_shape_cost(&pressure::sandbox_shape_cost(&shape_readings(state)));
}

fn error_response(status: StatusCode, message: String) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(message))
        .expect("static response")
}

/// 「没搬成」是哪一种，落成调用方能分辨的状态码。
///
/// 三件事在上层各有各的下文：没有树（404）不是故障，调用方身上本来就没东西可搬；
/// 树在用（409）等一下再问；名字不能用（400）是调用方的错；其余（500）才是这副
/// 副本自己没做到。折成一个 500 会让「换一副重试」和「改请求」看起来一样。
fn carry_error_response(e: CarryError) -> Response {
    let (status, message) = match e {
        CarryError::NoTree(m) => (StatusCode::NOT_FOUND, m),
        CarryError::Busy(m) => (StatusCode::CONFLICT, m),
        CarryError::Refused(m) => (StatusCode::BAD_REQUEST, m),
        CarryError::Failed(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
    };
    error_response(status, message)
}

/// 把这条 lane 的树打成包交出去，让工作能跟着任务到另一副副本上。
///
/// 幂等：同一棵没变的树打两次包，得到的是同样内容的包；且打包不改变树
/// （快照走临时索引），所以这边被打完包之后照常继续跑命令。
async fn worktree_export_handler(
    State(state): State<AppState>,
    Json(req): Json<WorktreeExportRequest>,
) -> Response {
    let Some(workdir) = state.workdir.as_ref() else {
        return error_response(
            StatusCode::NOT_FOUND,
            "this executor has no worktree router, so there is no tree to export".to_string(),
        );
    };
    match workdir.pack_lane(&req.lane).await {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/x-git-bundle")
            .body(Body::from(bytes))
            .expect("static response"),
        Err(e) => carry_error_response(e),
    }
}

/// 这副副本上有没有这条 lane 的树。
///
/// 问这个的是**刚起来的客户端**：钉子在它进程里，它重启之后手上一条都没有，
/// 而树不随进程消失——它长在某副副本的卷上，那副副本事后仍答得出。404 是
/// 「没有」，与 200 一样是答案；名字不能用是 400，读不出来是 500，只有这两种
/// 值得让调用方停下来。
async fn worktree_has_handler(
    State(state): State<AppState>,
    Query(q): Query<WorktreeHasQuery>,
) -> Response {
    let Some(workdir) = state.workdir.as_ref() else {
        return error_response(
            StatusCode::NOT_FOUND,
            "this executor has no worktree router, so it holds no tree".to_string(),
        );
    };
    match workdir.holds_tree(&q.lane).await {
        Ok(true) => Json(serde_json::json!({"lane": q.lane, "tree": true})).into_response(),
        Ok(false) => error_response(
            StatusCode::NOT_FOUND,
            format!("lane {} has no tree on this replica", q.lane),
        ),
        Err(e) => carry_error_response(e),
    }
}

/// 把带过来的包放进这条 lane 的树里：没有树就先切一棵，然后把树的内容写回工作区。
///
/// 包是任意大的（一棵真实的工作区），而 axum 默认的 2 MiB 请求体上限会把它们
/// 全挡在门外——这条路由自己关掉那个上限；调用方与这里之间是集群内的地址。
async fn worktree_import_handler(
    State(state): State<AppState>,
    Query(q): Query<WorktreeImportQuery>,
    body: Bytes,
) -> Response {
    let Some(workdir) = state.workdir.as_ref() else {
        return error_response(
            StatusCode::NOT_FOUND,
            "this executor has no worktree router, so there is nowhere to put a tree".to_string(),
        );
    };
    match workdir.unpack_lane(&q.lane, &body).await {
        Ok(files) => Json(serde_json::json!({"lane": q.lane, "files": files})).into_response(),
        Err(e) => carry_error_response(e),
    }
}

/// The executor's own timing of what it was asked to do.
///
/// Nothing else times this side of a tool call. A caller can span one from its
/// first frame to its last, but that span also carries the queue in front of
/// the request, the hop to this pod and the agent's own bookkeeping, so a slow
/// tool and a slow path to the tool read the same. Taken at this boundary --
/// request accepted to the last frame the caller received, or to the moment a
/// file operation's answer is ready, since that work happens before its stream
/// exists -- the number answers the question a latency report starts with, and
/// answers it with the side that owns the wait.
///
/// Two labels rather than one. `tool` is which payload came in, named the way
/// the agent names the tool that sends it -- so this joins to the bus's own
/// tool readings without a translation table, and the request a background
/// command keeps open for minutes is counted under the same name its caller
/// used. `outcome` is there because a histogram of durations alone mixes a
/// command that ran for nine minutes with a request that was refused in a
/// microsecond and a stream the caller walked away from; the three mean
/// opposite things about the sandbox and averaging them tells
/// nobody which one happened.
pub struct CommandMetrics {
    registry: Registry,
    seconds: HistogramVec,
    in_flight: IntGaugeVec,
    build_wait: HistogramVec,
    lane_waiting: IntGaugeVec,
    lane_holding: IntGaugeVec,
    lane_position: HistogramVec,
    lane_capacity: IntGaugeVec,
    build_wall: HistogramVec,
}

/// Buckets spanning a file write to a build that runs to its wall.
///
/// Placed where the answers divide rather than on round decades: under a
/// second is file IO, a few seconds is a search or a checkout, tens of seconds
/// is a compile, and the last edge is past the longest wall a caller can ask
/// for, so a command that runs long lands in a bucket instead of leaving every
/// long request to be counted in `+Inf` with the timeouts.
// cap: fixed — 桶边界是测量轴的定义：命中分布量在桶里，桶本身不能由它导出（那是循环），
// not-a-bound: 桶边界是测量轴的定义：命中分布量在桶里，桶本身不能由它导出（那是循环），
// 切点就是读这张图的人要分开的那几类等待
const COMMAND_LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0,
];

/// How a request ended, as the `outcome` label spells it.
///
/// Four states, each one a different claim about the executor. `failed` is a
/// command that ran and returned non-zero: the sandbox did its job. `refused`
/// is a request that never reached a command -- an empty body, a build slot
/// that could not be taken, a task worktree that would not resolve. `abandoned`
/// is a caller that stopped reading: the command may still have run, and the
/// duration is how long the caller waited, not how long the work took. Folding
/// any of them into "not ok" would let a broken caller read as a broken sandbox.
#[derive(Clone, Copy)]
enum Outcome {
    Ok = 1,
    Failed = 2,
    Refused = 3,
    Abandoned = 4,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Failed => "failed",
            Outcome::Refused => "refused",
            Outcome::Abandoned => "abandoned",
        }
    }
}

impl CommandMetrics {
    fn new() -> Self {
        let registry = Registry::new();
        // Names and labels are literals here, so a construction failure is a
        // programming error rather than a deployment's condition; there is no
        // configuration that could make it recoverable.
        let seconds = HistogramVec::new(
            HistogramOpts::new(
                "sandbox_command_seconds",
                "Wall time from accepting a request to delivering its last frame, by payload kind",
            )
            .buckets(COMMAND_LATENCY_BUCKETS.to_vec()),
            &["tool", "outcome"],
        )
        .expect("static metric name and labels");
        let in_flight = IntGaugeVec::new(
            Opts::new(
                "sandbox_commands_in_flight",
                "Requests the executor has accepted and not finished answering, by payload kind",
            ),
            &["tool"],
        )
        .expect("static metric name and labels");
        // The wait is a reading of its own, not a part of the command's wall:
        // a build that spent its whole budget queued and one that compiled
        // slowly land in the same latency bucket, and they want different
        // fixes. `outcome` separates the queue that admitted the command from
        // the one that turned it away without starting anything.
        // `agent` is the caller the request names: two agents queueing behind
        // the same lane are two different waits, and a single histogram would
        // report their sum as if one of them had been slow. The label is an
        // employee id, not a free-form name -- the executor only ever sees the
        // ids the kernel stamps on requests.
        let build_wait = HistogramVec::new(
            HistogramOpts::new(
                "sandbox_build_slot_wait_seconds",
                "How long a compiling command waited for the shared build lane, \
                 by payload kind, agent and whether that lane admitted it",
            )
            .buckets(COMMAND_LATENCY_BUCKETS.to_vec()),
            &["agent", "tool", "outcome"],
        )
        .expect("static metric name and labels");
        // The queue itself: how many requests each agent has waiting, and how
        // many slots each agent is holding. One says who is being kept out,
        // the other says who is keeping them out -- a single depth number
        // cannot be read as either.
        let lane_waiting = IntGaugeVec::new(
            Opts::new(
                "sandbox_build_lane_waiting",
                "Compiling requests currently queued for the shared build lane, by agent",
            ),
            &["agent"],
        )
        .expect("static metric name and labels");
        let lane_holding = IntGaugeVec::new(
            Opts::new(
                "sandbox_build_lane_holding",
                "Compiling requests currently holding a shared build lane slot, by agent",
            ),
            &["agent"],
        )
        .expect("static metric name and labels");
        // Where in line a caller stood when its wait ended. A refusal at
        // position 1 and one at position 12 are the same sentence and
        // different facts: the first says the lane never moved, the second
        // says it moved without this caller.
        let lane_position = HistogramVec::new(
            HistogramOpts::new(
                "sandbox_build_lane_position",
                "The place in line a compiling request occupied when its wait ended, by agent",
            )
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 100.0]),
            &["agent", "outcome"],
        )
        .expect("static metric name and labels");
        registry
            .register(Box::new(seconds.clone()))
            .expect("metric registered once");
        registry
            .register(Box::new(in_flight.clone()))
            .expect("metric registered once");
        registry
            .register(Box::new(build_wait.clone()))
            .expect("metric registered once");
        registry
            .register(Box::new(lane_waiting.clone()))
            .expect("metric registered once");
        registry
            .register(Box::new(lane_holding.clone()))
            .expect("metric registered once");
        // 这一刻能同时编译几条，以及这个数是量出来的还是没人量过。两个标签一起
        // 才有意义：`source="measured", reading="memory"` 的一条「1」是内存只够
        // 一条，`source="unmeasured"` 的一条「1」是还没量到任何代价——同一个数，
        // 两件不同的事。
        let lane_capacity = IntGaugeVec::new(
            Opts::new(
                "sandbox_build_lane_capacity",
                "How many builds this executor will run at once, and which reading decided it",
            ),
            &["source", "reading"],
        )
        .expect("static metric name and labels");
        // 一条编译从占到放开了多久，按当时几条位子并行分组。它是「多一条位子
        // 值不值」的证据：两条并行时每条比独占时慢多少，只有这个读数说得出。
        let build_wall = HistogramVec::new(
            HistogramOpts::new(
                "sandbox_build_wall_seconds",
                "Wall clock a compiling command held its lane, by how many builds shared the container",
            )
            .buckets(COMMAND_LATENCY_BUCKETS.to_vec()),
            &["beside"],
        )
        .expect("static metric name and labels");
        registry
            .register(Box::new(lane_position.clone()))
            .expect("metric registered once");
        registry
            .register(Box::new(lane_capacity.clone()))
            .expect("metric registered once");
        registry
            .register(Box::new(build_wall.clone()))
            .expect("metric registered once");
        Self {
            registry,
            seconds,
            in_flight,
            build_wait,
            lane_waiting,
            lane_holding,
            lane_position,
            lane_capacity,
            build_wall,
        }
    }

    fn render(&self) -> String {
        let mut buf = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .is_ok()
        {
            String::from_utf8_lossy(&buf).into_owned()
        } else {
            String::new()
        }
    }

    fn observe(&self, tool: &str, outcome: Outcome, seconds: f64) {
        self.seconds
            .with_label_values(&[tool, outcome.as_str()])
            .observe(seconds);
    }

    /// How long a compiling command waited for the lane, and what that wait
    /// ended in. Read beside `sandbox_command_seconds`: this one says the
    /// machine made the command queue, that one says the command was slow.
    fn observe_build_wait(&self, agent: &str, tool: &str, ended: LaneWait, seconds: f64) {
        self.build_wait
            .with_label_values(&[agent, tool, ended.as_str()])
            .observe(seconds);
    }

    fn set_lane_waiting(&self, agent: &str, count: usize) {
        self.lane_waiting
            .with_label_values(&[agent])
            .set(count as i64);
    }

    fn set_lane_holding(&self, agent: &str, count: usize) {
        self.lane_holding
            .with_label_values(&[agent])
            .set(count as i64);
    }

    fn observe_lane_position(&self, agent: &str, ended: LaneWait, place: usize) {
        self.lane_position
            .with_label_values(&[agent, ended.as_str()])
            .observe(place as f64);
    }

    /// 发布这一刻的条数与它的出处。先清空再生一条：两个来源同时为 1 会被
    /// 求和读成 2，而「量不到」与「量到 1」正是要靠这两条分开的两件事。
    fn set_lane_capacity(&self, plan: LanePlan) {
        let (source, reading) = lane_labels(plan.source);
        self.lane_capacity.reset();
        self.lane_capacity
            .with_label_values(&[source, reading])
            .set(plan.lanes as i64);
    }

    fn observe_build_wall(&self, beside: usize, seconds: f64) {
        self.build_wall
            .with_label_values(&[&beside.to_string()])
            .observe(seconds);
    }
}

/// 条数出处在读数面上的两个标签。取值都是闭集：一个读数名一种取值，不拼句子。
fn lane_labels(source: LaneSource) -> (&'static str, &'static str) {
    match source {
        LaneSource::Measured(bound) => ("measured", bound),
        LaneSource::Unmeasured(reading) => ("unmeasured", reading),
        LaneSource::OomKilled => ("oom-killed", "none"),
        LaneSource::Overridden(_) => ("overridden", "declared"),
    }
}

/// How a wait for the shared build lane ended, as the `outcome` label spells it.
///
/// Three states, because they want three different fixes: `admitted` is the
/// lane working as designed, `full` is a caller whose command never started and
/// who can see that it never did, and `closed` is the semaphore gone, which no
/// amount of patience fixes.
#[derive(Clone, Copy)]
enum LaneWait {
    Admitted,
    Full,
    Closed,
}

impl LaneWait {
    fn as_str(self) -> &'static str {
        match self {
            LaneWait::Admitted => "admitted",
            LaneWait::Full => "full",
            LaneWait::Closed => "closed",
        }
    }
}

/// One request's reading, recorded when it is dropped.
///
/// Drop rather than an explicit end call because the two hard cases are the
/// ones where no end call would run: a response stream the caller disconnects
/// from, and a handler that returns early on an error. Both drop this, and both
/// are exactly the requests whose absence from a latency reading would be
/// mistaken for having been fast.
struct CommandWatch {
    metrics: Arc<CommandMetrics>,
    tool: &'static str,
    started: Instant,
    /// Set once the request has a verdict; zero until then means the stream
    /// ended without one, which is a caller that left.
    outcome: Arc<AtomicU8>,
}

impl CommandWatch {
    fn start(metrics: Arc<CommandMetrics>, tool: &'static str) -> Self {
        metrics.in_flight.with_label_values(&[tool]).inc();
        Self {
            metrics,
            tool,
            started: Instant::now(),
            outcome: Arc::new(AtomicU8::new(0)),
        }
    }

    fn mark(&self, outcome: Outcome) {
        // Relaxed: this byte orders nothing else. The drop that reads it needs
        // the value, not a fence -- the response body it arrives with was
        // handed over before the write, and no other memory travels with it.
        self.outcome.store(outcome as u8, Ordering::Relaxed);
    }

    /// Record the request as ended now, with a response that does not stream.
    fn end(self, response: Response, outcome: Outcome) -> Response {
        self.mark(outcome);
        drop(self);
        response
    }
}

impl Drop for CommandWatch {
    fn drop(&mut self) {
        let outcome = match self.outcome.load(Ordering::Relaxed) {
            1 => Outcome::Ok,
            2 => Outcome::Failed,
            3 => Outcome::Refused,
            _ => Outcome::Abandoned,
        };
        self.metrics.in_flight.with_label_values(&[self.tool]).dec();
        self.metrics
            .observe(self.tool, outcome, self.started.elapsed().as_secs_f64());
    }
}

/// The tool label for a payload: the agent's own name for what it asked for.
fn tool_label(payload: &SandboxPayload) -> &'static str {
    match payload {
        SandboxPayload::Command { .. } => "run_command",
        SandboxPayload::ReadFile { .. } => "read_file",
        SandboxPayload::WriteFile { .. } => "write_file",
        SandboxPayload::EditFile { .. } => "edit_file",
        SandboxPayload::CaptureCheckpoint { .. } => "capture_checkpoint",
        SandboxPayload::PlanCheckpoint { .. } => "plan_checkpoint",
        SandboxPayload::RestoreCheckpoint { .. } => "restore_checkpoint",
        SandboxPayload::Wasm { .. } => "wasm",
    }
}

fn stream_response(
    rx: tokio::sync::mpsc::Receiver<CommandEvent>,
    announce: Option<CommandEvent>,
) -> Response {
    ndjson_response(rx, None, announce)
}

/// Serialize a command's events as NDJSON for the caller.
///
/// A watch handed in travels inside the stream's own state, so the span ends
/// where the caller stops waiting: at the final event, or at the moment the
/// connection closes with the command still writing. Neither is visible from
/// the handler, which returns before the first frame is read.
///
/// `announce`, when present, is the first line: the checkout the call ran in,
/// whose absolute path only this side of the wire knows.
fn ndjson_response(
    rx: tokio::sync::mpsc::Receiver<CommandEvent>,
    watch: Option<CommandWatch>,
    announce: Option<CommandEvent>,
) -> Response {
    let stream = futures::stream::unfold(
        (rx, watch, announce),
        |(mut rx, watch, pending)| async move {
            let event = match pending {
                Some(event) => event,
                None => rx.recv().await?,
            };
            if let (Some(w), CommandEvent::Exit { code }) = (watch.as_ref(), &event) {
                w.mark(if *code == 0 {
                    Outcome::Ok
                } else {
                    Outcome::Failed
                });
            }
            Some((event, (rx, watch, None)))
        },
    )
    .map(|event| {
        let mut line = serde_json::to_string(&event).expect("CommandEvent serializes");
        line.push('\n');
        Ok::<_, std::convert::Infallible>(line)
    });
    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .expect("stream response")
}

/// Buffer a file operation into the two-event stream shape.
fn single_shot(result: std::io::Result<String>) -> tokio::sync::mpsc::Receiver<CommandEvent> {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    match result {
        Ok(content) => {
            cog_core::ignore_lost!(tx.try_send(CommandEvent::Stdout { data: content }));
            cog_core::ignore_lost!(tx.try_send(CommandEvent::Exit { code: 0 }));
        }
        Err(e) => {
            let _ = tx.try_send(CommandEvent::Stderr {
                data: e.to_string(),
            });
            cog_core::ignore_lost!(tx.try_send(CommandEvent::Exit { code: 1 }));
        }
    }
    rx
}

/// Resolve the checkout for a request. A valid lane anchors the command cwd and
/// relative file paths; routing errors fail the request instead of silently
/// falling back to a shared directory. When the router is enabled, every
/// command additionally receives the shared CARGO_TARGET_DIR.
///
/// The tree belongs to the task. The unit of isolation is the task, so a
/// request that names one works in that task's tree and a request that names
/// only an employee works in the employee's own: a planner's reads, a
/// generator's writes and an evaluator's runs on one task are looking at one
/// set of files, which is the whole point of the tree. Naming the lane is
/// [`cog_core::checkout_lane`]'s job and nobody else's -- the run writes the
/// same function's answer into its system prefix, so the tree the model is told
/// about and the tree its calls land in cannot drift apart.
async fn resolve_workdir(
    state: &AppState,
    task_id: Option<&str>,
    agent_id: Option<&str>,
) -> Result<
    (
        Option<PathBuf>,
        Option<PathBuf>,
        Option<CommandHold>,
        cog_core::WorktreeOrigin,
    ),
    Box<Response>,
> {
    let Some(router) = state.workdir.as_ref() else {
        return Ok((None, None, None, cog_core::WorktreeOrigin::Unknown));
    };
    let lane = cog_core::checkout_lane(task_id, agent_id);
    match lane {
        // The claim travels with the request and is handed to whatever runs the
        // command: the tree is in use from here until that command is over, and
        // a reclamation pass that only knows `last_used_unix` cannot tell the
        // difference on its own.
        Some(lane) => match router.route(&lane).await {
            Ok((dir, claim, origin)) => Ok((
                Some(dir),
                Some(router.target_dir().to_path_buf()),
                Some(router.command_hold(Some(claim)).await),
                origin,
            )),
            Err(e) => {
                router.metrics().inc_error("route");
                Err(Box::new(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("worktree unavailable for {lane}: {e}"),
                )))
            }
        },
        None => {
            // The business side must stamp identity (require_tool_identity);
            // an unstamped request still runs for compatibility, but outside
            // any checkout and is counted for observability. It builds into the
            // shared cache like any other command, so it takes the command slot
            // all the same: a command that is not tracked is a build the
            // reclamation pass cannot see.
            router.metrics().inc_unscoped();
            tracing::warn!(
                "sandbox request naming neither an employee nor a task; running outside any checkout"
            );
            Ok((
                None,
                Some(router.target_dir().to_path_buf()),
                Some(router.command_hold(None).await),
                cog_core::WorktreeOrigin::Unknown,
            ))
        }
    }
}

/// The id a command's progress beacon is filed under.
///
/// A task id when the caller has one, so the executor's judgement and the
/// orchestrator's reclaim read the same attempt. An unstamped request still
/// runs (compatibility); it gets an id of its own so two anonymous commands
/// cannot overwrite each other's beacon.
fn command_progress_id(task_id: Option<&str>) -> String {
    match task_id.map(str::trim).filter(|id| !id.is_empty()) {
        Some(id) => id.to_string(),
        None => {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            format!(
                "executor-command-{}",
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )
        }
    }
}

async fn execute_handler(
    State(state): State<AppState>,
    Json(req): Json<ExecuteRequest>,
) -> Response {
    // The watch starts before routing, not after: resolving a worktree is the
    // step that once cost six seconds of a first tool call, and a reading that
    // began after it would have reported that call as fast.
    let watch = CommandWatch::start(Arc::clone(&state.commands), tool_label(&req.payload));
    let progress_id = command_progress_id(req.task_id.as_deref());
    let (workdir, cargo_target, worktree_claim, origin) =
        match resolve_workdir(&state, req.task_id.as_deref(), req.agent_id.as_deref()).await {
            Ok(resolved) => resolved,
            Err(resp) => return watch.end(*resp, Outcome::Refused),
        };
    // The checkout's absolute path goes in front of every reply: the caller
    // knows the lane's name, only this side knows where that name lives. A
    // call outside any checkout announces nothing rather than a path that
    // would read as "somewhere real".
    let announce = workdir.as_ref().map(|dir| CommandEvent::Workdir {
        path: dir.display().to_string(),
        origin,
    });
    match req.payload {
        SandboxPayload::Command { ref command } => {
            if command.trim().is_empty() {
                return watch.end(
                    error_response(StatusCode::BAD_REQUEST, "empty command".into()),
                    Outcome::Refused,
                );
            }
            tracing::info!(
                task_id = req.task_id.as_deref().unwrap_or(""),
                agent_id = req.agent_id.as_deref().unwrap_or(""),
                workdir = ?workdir,
                command = %command,
                "sandbox executor running command"
            );
            // A command that compiles nothing takes no build slot, and it
            // still asks the question the build lane asks: is there room for
            // it here? One verdict answers both -- see
            // [`crate::pressure::admits`] -- and the place in line belongs to
            // the build queue, not to this path.
            //
            // What it is priced at comes from the commands that have already
            // finished, so a deployment that has priced nothing yet admits
            // without a reservation and says so on the scrape surface.
            let reservation = if crate::runtime::local::builds_the_tree(command) {
                None
            } else {
                let demand =
                    pressure::Demand::from_samples(&crate::runtime::cgroup::recent_peaks());
                match pressure::wait_for_admission(
                    &state.pressure,
                    |reserved| Pressure::from_facts(&crate::runtime::cgroup::read(), reserved),
                    &demand,
                    || state.pressure.open_count() > 0 || state.build_lane.holding() > 0,
                    state.queue_waits.window(),
                )
                .await
                {
                    Ok(admitted) => admitted.into_reservation(),
                    // A wait that outlived this executor's measured queue is a
                    // refusal, not a failure: 503 says ask again, 500 would say
                    // the request was broken.
                    Err(refusal) => {
                        let message = format!(
                            "the container had no room for this command for {}s                              (this executor's queue is normally {}s); ask again later",
                            refusal.waited.as_secs(),
                            refusal.window.as_secs()
                        );
                        return watch.end(
                            error_response(StatusCode::SERVICE_UNAVAILABLE, message),
                            Outcome::Refused,
                        );
                    }
                }
            };
            // A command that compiles waits its turn here, before its child
            // exists. The wall handed to the child starts when the child does,
            // so time spent queued is not time spent compiling: without this,
            // every extra build started beside it shortened the wall the others
            // had left, and each of them timed out having read nothing.
            let build_slot = match build_slot_for(
                &state,
                req.agent_id.as_deref(),
                tool_label(&req.payload),
                command,
            )
            .await
            {
                Ok(slot) => slot,
                // A lane that stays full past the command's own budget is a
                // refusal, not a failure of the executor: 503 says try again,
                // 500 would say this request was broken.
                Err(refusal @ LaneRefusal::Full { .. }) => {
                    return watch.end(
                        error_response(StatusCode::SERVICE_UNAVAILABLE, refusal.to_string()),
                        Outcome::Refused,
                    )
                }
                // The semaphore is never closed in this process; a caller that
                // lands here is told so rather than run unbounded.
                Err(refusal @ LaneRefusal::Closed(_)) => {
                    return watch.end(
                        error_response(StatusCode::INTERNAL_SERVER_ERROR, refusal.to_string()),
                        Outcome::Refused,
                    )
                }
            };
            let build_target = build_target_dir(cargo_target.as_deref(), build_slot.as_ref());
            // 位子自己不知道 target 卷在哪：装配这一步按位子算出了目录，就把
            // 它和卷的根一并告诉队——卷根用于量余量，目录用于量这一条编译写了多少。
            if let Some(base) = cargo_target.as_deref() {
                state.build_lane.note_target_base(base);
            }
            if let (Some(slot), Some(dir)) = (build_slot.as_ref(), build_target.as_deref()) {
                slot.note_target(dir.to_path_buf());
            }
            match crate::runtime::local::spawn_command(
                command,
                &progress_id,
                workdir.as_deref(),
                build_target.as_deref(),
                worktree_claim,
                build_slot,
                reservation,
            ) {
                // The command's outcome is its own exit code, which arrives as
                // the last event; the watch reads it there.
                Ok(rx) => ndjson_response(rx, Some(watch), announce),
                Err(e) => watch.end(
                    error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    Outcome::Refused,
                ),
            }
        }
        SandboxPayload::ReadFile { ref path } => {
            // 有一棵树才有可越的界：路径走出这棵树就拒绝，并说出范围。没有车道
            // 的请求本来就没有界（见 `boundary_reading` 里那一行 `none`），
            // 这里只能原样交给文件系统。
            let anchored = match workdir.as_deref() {
                Some(base) => match workdir::anchor_inside(base, path) {
                    Ok(path) => path,
                    Err(reason) => {
                        return watch.end(
                            error_response(StatusCode::BAD_REQUEST, reason),
                            Outcome::Refused,
                        );
                    }
                },
                None => workdir::anchor_path(None, path),
            };
            let result = tokio::fs::read_to_string(&anchored).await;
            // A file operation's own result is already known here, so the
            // watch is closed now rather than through the buffered stream.
            let outcome = outcome_of(&result);
            watch.end(stream_response(single_shot(result), announce), outcome)
        }
        SandboxPayload::WriteFile {
            ref path,
            ref content,
        } => {
            // 有一棵树才有可越的界：路径走出这棵树就拒绝，并说出范围。没有车道
            // 的请求本来就没有界（见 `boundary_reading` 里那一行 `none`），
            // 这里只能原样交给文件系统。
            let anchored = match workdir.as_deref() {
                Some(base) => match workdir::anchor_inside(base, path) {
                    Ok(path) => path,
                    Err(reason) => {
                        return watch.end(
                            error_response(StatusCode::BAD_REQUEST, reason),
                            Outcome::Refused,
                        );
                    }
                },
                None => workdir::anchor_path(None, path),
            };
            if let Some(parent) = anchored.parent() {
                if !tokio::fs::try_exists(parent).await.unwrap_or(false) {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        return watch.end(
                            stream_response(single_shot(Err(e)), announce),
                            Outcome::Failed,
                        );
                    }
                }
            }
            let result = tokio::fs::write(&anchored, content)
                .await
                .map(|_| String::new());
            let outcome = outcome_of(&result);
            watch.end(stream_response(single_shot(result), announce), outcome)
        }
        SandboxPayload::EditFile {
            ref path,
            ref old_string,
            ref new_string,
            replace_all,
        } => {
            // 有一棵树才有可越的界：路径走出这棵树就拒绝，并说出范围。没有车道
            // 的请求本来就没有界（见 `boundary_reading` 里那一行 `none`），
            // 这里只能原样交给文件系统。
            let anchored = match workdir.as_deref() {
                Some(base) => match workdir::anchor_inside(base, path) {
                    Ok(path) => path,
                    Err(reason) => {
                        return watch.end(
                            error_response(StatusCode::BAD_REQUEST, reason),
                            Outcome::Refused,
                        );
                    }
                },
                None => workdir::anchor_path(None, path),
            };
            // The reply names the path the caller gave, not the anchored one:
            // an agent that asked for `src/main.rs` should not have to strip a
            // worktree prefix off its own answer to recognise it.
            let result = crate::edit::apply(&anchored, old_string, new_string, replace_all)
                .await
                .map(|report| report.describe(path))
                .map_err(std::io::Error::other);
            let outcome = outcome_of(&result);
            watch.end(stream_response(single_shot(result), announce), outcome)
        }
        SandboxPayload::CaptureCheckpoint { ref key }
        | SandboxPayload::PlanCheckpoint { ref key }
        | SandboxPayload::RestoreCheckpoint { ref key } => {
            // 检查点是「这棵工作树」的属性，没有一个检出就没有可记的东西：在一个
            // 没落在任何 lane 上的请求里假装成功，会让调用方以为已经拍下了。
            let Some(root) = workdir.as_deref() else {
                return watch.end(
                    error_response(
                        StatusCode::BAD_REQUEST,
                        "这个请求没有落在任何一棵检出上，拍不了检查点".into(),
                    ),
                    Outcome::Refused,
                );
            };
            let (label, result) = match req.payload {
                SandboxPayload::CaptureCheckpoint { .. } => (
                    "capture",
                    crate::checkpoints::capture(root, key)
                        .await
                        .map(|plan| plan.to_json().to_string()),
                ),
                SandboxPayload::PlanCheckpoint { .. } => (
                    "plan",
                    crate::checkpoints::plan(root, key)
                        .await
                        .map(|plan| plan.to_json().to_string()),
                ),
                _ => (
                    "restore",
                    crate::checkpoints::restore(root, key)
                        .await
                        .map(|plan| plan.to_json().to_string()),
                ),
            };
            let result = result.map_err(std::io::Error::other);
            if let Err(error) = &result {
                tracing::warn!(checkpoint = %key, op = label, %error, "check point operation failed");
            }
            let outcome = outcome_of(&result);
            watch.end(stream_response(single_shot(result), announce), outcome)
        }
        SandboxPayload::Wasm { .. } => watch.end(
            error_response(
                StatusCode::BAD_REQUEST,
                "executor does not run WASM payloads".into(),
            ),
            Outcome::Refused,
        ),
    }
}

/// The outcome label for a file operation's result, before it is buffered into
/// the event stream the caller sees.
fn outcome_of<T>(result: &std::io::Result<T>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(_) => Outcome::Failed,
    }
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    // Price both sandbox shapes against what this executor has measured before
    // rendering, so the gauge belongs to the same momentary reading as the rest
    // of the page rather than to whenever `/lane` was last polled.
    publish_shape_cost(&state);
    // The memory ceiling belongs on the same surface as the worktree counters:
    // it is the other half of "why did this build die", and it applies even
    // where no workdir router was provisioned.
    let mut body = state
        .workdir
        .as_ref()
        .map(|r| r.metrics().render())
        .unwrap_or(Default::default());
    // The build cache this executor's commands write into. Rendered from the
    // readings rather than from the prometheus registry the counters above live
    // in, because the same family is served by the other process that owns a
    // cache and both sides go through one renderer -- see `crate::build_cache`.
    if let Some(router) = state.workdir.as_ref() {
        body.push_str(&crate::build_cache::render(router.build_cache()).await);
    }
    body.push_str(&crate::runtime::cgroup::render());
    // Whether work may start, and what it is priced at: the other half of the
    // memory reading above, and the only place the three verdicts are told
    // apart.
    body.push_str(&crate::pressure::render());
    // What each payload cost, at the boundary that owns the wait. Rendered
    // unconditionally: a deployment without a workdir router still runs file
    // operations, and a latency reading that only existed on some deployments
    // would be quoted as if it covered all of them.
    body.push_str(&state.commands.render());
    // Document access is off unless a scope is configured, so its counters are
    // only worth rendering where something can move them.
    if let Some(hostdocs) = state.hostdocs.as_ref() {
        body.push_str(&hostdocs.metrics());
    }
    // The background loops of this process live outside the workdir registry, and
    // nothing else publishes them: worktree GC, the offline fetch and the volume
    // footprint walk would each stop silently, and the reading each one produces
    // would keep its last value, which is what a healthy quiet loop looks like.
    let loops = cog_core::loop_health::observable();
    match loops.collect_metrics("").await {
        Ok(readings) => body.push_str(&cog_core::observability_text::render_raw_metrics(&readings)),
        Err(e) => warn!(error = %e, "background loop readings unavailable this scrape"),
    }
    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("static response")
}

/// A request that named no agent. It still queues, and its readings are filed
/// here rather than under somebody else's name: an unattributed wait is a
/// fact about the lane, not a fact about a passers-by.
const UNATTRIBUTED: &str = "unattributed";

/// 一条按 agent 轮转的编译队：谁在等、谁占着，都按人归属。
///
/// 一条先进先出的队看不出「谁在等」：一个 agent 连发十条编译，另一个就排在
/// 十条之后，而两条等待在读数上合成一个数——分不出「我慢」与「有人一直占着」。
/// 这里把**位子**与**等的人**分开记：位子有几条由容量决定（每条写自己的
/// target 目录，见 [`lane_target_dir`]），等的人按 agent 轮转，刚被放行的
/// agent 回到自己队的队尾，于是任何一个 agent 连发都占不满别人的机会。
pub struct BuildLane {
    /// The count the deployment wrote down, if it wrote one. When it did, the
    /// count is theirs; when it did not, [`BuildLane::capacity_now`] derives it.
    declared: Option<usize>,
    inner: std::sync::Mutex<LaneState>,
    wake: tokio::sync::Notify,
    commands: Arc<CommandMetrics>,
    /// What finished builds have been measured to cost this container.
    cost: std::sync::Mutex<BuildCost>,
    /// The volume the lanes' target directories live on, learned from the
    /// first compiling command. Free bytes are read off it at planning time;
    /// until it is known, the disk bound is unmeasured.
    target_base: std::sync::Mutex<Option<PathBuf>>,
    /// The room running commands have claimed, as the command path records it.
    /// Planning against the smaller figure is what makes the two paths share
    /// one account instead of each believing it has the container to itself.
    pressure_ledger: Arc<pressure::Ledger>,
}

struct LaneState {
    waiting: Vec<Waiting>,
    holding: Vec<HeldSlot>,
    /// 每个 agent 最近一次被放行的票号。轮转的依据：票号越小越先轮到它。
    last_served: std::collections::HashMap<String, u64>,
    /// 来过这条队的 agent：读数要能把它们的深度退回 0，而不是停在最后一个
    /// 读数上——一个早就走掉的 agent 的「还在等 3 个」与真的在等同形。
    seen_agents: std::collections::HashSet<String>,
    next_ticket: u64,
}

struct Waiting {
    ticket: u64,
    agent: String,
    tool: String,
    arrived: std::time::Instant,
}

struct HeldSlot {
    agent: String,
    ticket: u64,
    since: std::time::Instant,
    /// 占的是第几条位子。位子不再只是「还有没有空」，而是「空的是哪一条」：
    /// 每条编译写自己的 target 目录，两条同时编译才不会卡在 cargo 的同一把锁上。
    index: usize,
    /// 这条位子上的编译期间容器工作集到过的最高点。采样器只在这一条位子
    /// 独占容器时填它：两条编译并行时升高分不清是谁的，分不清就不算给谁。
    peak_bytes: u64,
}

impl LaneState {
    /// 下一个该被放行的人：按「谁的 agent 最久没被服务」排，同一次服务里
    /// 按到达先后。这就是轮转——刚放过谁，谁就排到自己的队尾。
    fn fair_head(&self) -> Option<usize> {
        let mut best: Option<((u64, u64), usize)> = None;
        for (at, waiter) in self.waiting.iter().enumerate() {
            let served = self.last_served.get(&waiter.agent).copied().unwrap_or(0);
            let key = (served, waiter.ticket);
            if best.is_none_or(|(best_key, _)| key < best_key) {
                best = Some((key, at));
            }
        }
        best.map(|(_, at)| at)
    }

    /// 排队里这个人的名次（从 1 数）：在它前面按轮转顺序还排着几个。
    fn place_of(&self, ticket: u64) -> Option<usize> {
        let mut order: Vec<(u64, u64, &Waiting)> = self
            .waiting
            .iter()
            .map(|w| {
                let served = self.last_served.get(&w.agent).copied().unwrap_or(0);
                (served, w.ticket, w)
            })
            .collect();
        order.sort_by_key(|(served, ticket, _)| (*served, *ticket));
        order
            .iter()
            .position(|(_, t, _)| *t == ticket)
            .map(|at| at + 1)
    }
}

/// How often the container's working set is read while builds run.
///
/// The reading answers "what did this build rise by", so it has to be finer
/// than the build: a cargo build's resident set moves over seconds, and a
/// sample a second catches the plateau without reading allocation noise as a
/// second build.
// cap: fixed — 采样间隔按「编译的常驻集在秒级移动」定；更密只是同一平台期的重复
// keep: 删掉后要么量不到这次构建涨了多少，要么每圈读一次把分配噪声读成第二次构建；由「编译常驻集在秒级移动」兜住
// 读数，更疏会漏掉整条编译的峰
const LANE_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

impl BuildLane {
    fn new(
        declared: Option<usize>,
        commands: Arc<CommandMetrics>,
        pressure_ledger: Arc<pressure::Ledger>,
    ) -> Arc<Self> {
        let lane = Arc::new(Self {
            declared,
            inner: std::sync::Mutex::new(LaneState {
                waiting: Vec::new(),
                holding: Vec::new(),
                last_served: std::collections::HashMap::new(),
                seen_agents: std::collections::HashSet::new(),
                next_ticket: 0,
            }),
            wake: tokio::sync::Notify::new(),
            commands,
            cost: std::sync::Mutex::new(BuildCost::default()),
            target_base: std::sync::Mutex::new(None),
            pressure_ledger,
        });
        // 没有运行时就没人采样：单元测试里成本保持「没量到」，规划因此给 1，
        // 与树上没人量过时的答案一致。
        if tokio::runtime::Handle::try_current().is_ok() {
            let weak = Arc::downgrade(&lane);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(LANE_SAMPLE_INTERVAL).await;
                    let Some(lane) = weak.upgrade() else { break };
                    lane.sample_once();
                }
            });
        }
        lane
    }

    /// 采一次容器工作集：没有编译占位时它是空闲基线，恰好一条时它进那一条
    /// 的峰值。两条以上时谁都不是——升高分不清归属，就不算给任何人。
    fn sample_once(&self) {
        let facts = crate::runtime::cgroup::read();
        let Some(working_set) = facts.working_set_bytes() else {
            return;
        };
        let build = {
            let mut state = self.lock();
            match state.holding.len() {
                0 => None,
                1 => {
                    let held = &mut state.holding[0];
                    held.peak_bytes = held.peak_bytes.max(working_set);
                    Some(false)
                }
                _ => Some(true),
            }
        };
        match build {
            // 没有编译在跑：这是空闲工作集。
            None => self.cost().observe(working_set, 0),
            // 恰好一条：采样器自己填过了，这里不重复记。
            Some(false) => {}
            // 多条：基线仍然只在没有编译时才有意义，这里什么都不做。
            Some(true) => {}
        }
    }

    fn cost(&self) -> std::sync::MutexGuard<'_, BuildCost> {
        self.cost.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// 一条编译都不跑时这块容器占着的内存。两种沙箱形态的价格都要算上它：
    /// 一任务一沙箱每个任务付一整份，一副多任务把这一笔摊在任务之间。
    pub fn idle_working_set(&self) -> Option<u64> {
        self.cost().baseline_bytes()
    }

    /// 编译写 target 的那块卷。第一条编译类命令告诉它一次。
    fn note_target_base(&self, base: &std::path::Path) {
        let mut slot = self.target_base.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(base.to_path_buf());
        }
    }

    fn free_bytes(&self) -> Option<u64> {
        let base = self
            .target_base
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()?;
        cog_core::build_cache_readings::volume_free_bytes(&base)
    }

    /// 这一刻的条数：部署写了就是它写的，没写就是量出来的。
    fn plan(&self) -> LanePlan {
        let facts = crate::runtime::cgroup::read();
        self.plan_with(&facts, self.free_bytes())
    }

    /// 同一套算术，读数由调用方给：判据用夹具喂它，不用这台机器的 cgroup。
    fn plan_with(&self, facts: &CgroupFacts, free_bytes: Option<u64>) -> LanePlan {
        let cost = self.cost();
        // The three figures the command path is judged on, read the same way:
        // the container's limit, what is resident with no build running, and
        // what running commands have reserved.
        let pressure = Pressure::new(
            facts.limit_bytes,
            cost.baseline_bytes(),
            self.pressure_ledger.reserved_bytes(),
        );
        let plan = lane_sizing::plan_lanes(&pressure, free_bytes, &cost, self.declared);
        drop(cost);
        self.commands.set_lane_capacity(plan);
        plan
    }

    fn capacity_now(&self) -> usize {
        self.plan().lanes
    }

    /// How many builds hold a lane right now. Asked by the command path when it
    /// decides whether waiting for room could end: a running build is the most
    /// likely thing to give that room back.
    fn holding(&self) -> usize {
        self.lock().holding.len()
    }

    /// 一条编译结束了：量它的代价、记它的结局，再把条数重算一次。
    ///
    /// 代价是「容器工作集比空闲基线高多少」与「这块 target 目录占多少字节」；
    /// 结局是「进队到出队之间 oom_kill 涨没涨」。前者是下一条位子的价格，后者
    /// 是这条位子太多的证据。墙钟按占位时长记，并按当时几条位子在一起分组——
    /// 它是「多一条位子值不值」的证据，但两次不同的编译比墙钟是在比两个问题，
    /// 所以它只发布、不参与算条数。
    fn finish_build(
        &self,
        started: CgroupFacts,
        peak_water: u64,
        beside: usize,
        since: std::time::Instant,
        target: Option<PathBuf>,
    ) {
        let after = crate::runtime::cgroup::read();
        let end = if CgroupFacts::oom_kill_between(&started, &after) {
            BuildEnd::OomKilled
        } else {
            BuildEnd::Completed
        };
        let wall = since.elapsed();
        let target_bytes = target
            .as_deref()
            .and_then(|dir| cog_core::fs_size::dir_size_bytes(dir, &[]).ok());
        let sample = {
            let cost = self.cost();
            let peak_bytes = cost
                .baseline_bytes()
                .filter(|baseline| peak_water > *baseline)
                .map(|baseline| peak_water - baseline);
            BuildSample {
                peak_bytes,
                target_bytes,
                wall,
                beside,
            }
        };
        self.cost().record(sample, end);
        self.commands.observe_build_wall(beside, wall.as_secs_f64());
        // 条数可能因此变了（第一次量到价格，或刚吃了 OOM）：重算并发布，
        // 让「量不到」与「量到就是 1」在读数面上分得开。
        self.plan();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LaneState> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// 把两条读数重新发布一遍：每个来过这条队的 agent 的等待数与占槽数。
    fn republish(&self, state: &LaneState) {
        let mut waiting: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for w in &state.waiting {
            *waiting.entry(w.agent.as_str()).or_default() += 1;
        }
        let mut holding: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for h in &state.holding {
            *holding.entry(h.agent.as_str()).or_default() += 1;
        }
        for agent in &state.seen_agents {
            self.commands
                .set_lane_waiting(agent, waiting.get(agent.as_str()).copied().unwrap_or(0));
            self.commands
                .set_lane_holding(agent, holding.get(agent.as_str()).copied().unwrap_or(0));
        }
    }

    /// 排进队，直到拿到位子、被拒，或等的人自己走了。
    ///
    /// 没有窗口（这台机器还没量出正常队列多长）就一直等：把人拒在一个没人
    /// 量过的数上，与「这台机器就是慢」在读数上分不开。
    async fn acquire(
        self: &Arc<Self>,
        agent: Option<&str>,
        tool: &str,
        window: Option<std::time::Duration>,
    ) -> Result<(BuildSlot, std::time::Duration), LaneRefusal> {
        let agent = agent
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .unwrap_or(UNATTRIBUTED)
            .to_string();
        let arrived = std::time::Instant::now();
        let ticket = {
            let mut state = self.lock();
            state.next_ticket += 1;
            let ticket = state.next_ticket;
            state.seen_agents.insert(agent.clone());
            state.waiting.push(Waiting {
                ticket,
                agent: agent.clone(),
                tool: tool.to_string(),
                arrived,
            });
            self.republish(&state);
            ticket
        };
        let mut queue = QueueGuard {
            lane: Arc::clone(self),
            ticket,
            armed: true,
        };
        let deadline = window.map(|w| tokio::time::Instant::now() + w);
        loop {
            // 先登记的唤醒再查状态：反过来会漏掉「查完、还没 await」之间
            // 别人放出的那一次。
            let notified = self.wake.notified();
            // 条数每次放人前重算：编译的代价随机器与工作负载变，一个只算一次的
            // 数会停在「第一次编译时的容器」上。
            let capacity = self.capacity_now();
            {
                let mut state = self.lock();
                if let Some(at) = state.waiting.iter().position(|w| w.ticket == ticket) {
                    if state.holding.len() < capacity && state.fair_head() == Some(at) {
                        let place = state.place_of(ticket).unwrap_or(at + 1);
                        // 空着的第一条位子：位子数与容量相等，所以「还有空位」
                        // 保证找得到一条；找不到只可能是容量刚被改小，此时退回
                        // 队尾，不抢别人写着的那条目录。
                        let index = (0..capacity)
                            .find(|i| !state.holding.iter().any(|h| h.index == *i))
                            .unwrap_or(state.holding.len());
                        state.waiting.remove(at);
                        let started = crate::runtime::cgroup::read();
                        let since = std::time::Instant::now();
                        state.holding.push(HeldSlot {
                            agent: agent.clone(),
                            ticket,
                            since,
                            index,
                            peak_bytes: 0,
                        });
                        state.last_served.insert(agent.clone(), ticket);
                        self.republish(&state);
                        queue.armed = false;
                        drop(state);
                        self.commands
                            .observe_lane_position(&agent, LaneWait::Admitted, place);
                        // 队头换人了：轮转的顺序要重新争一次。
                        self.wake.notify_waiters();
                        return Ok((
                            BuildSlot {
                                lane: Arc::clone(self),
                                ticket,
                                index,
                                since,
                                started,
                                target: std::sync::Mutex::new(None),
                            },
                            arrived.elapsed(),
                        ));
                    }
                } else {
                    // 不在队里了：只可能是这条 future 正在被丢掉。两种都不
                    // 该再等。
                    break;
                }
            }
            match deadline {
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        let place = self.leave(ticket, &agent, &mut queue);
                        return Err(LaneRefusal::Full {
                            waited: arrived.elapsed(),
                            window: window.unwrap_or_default(),
                            place,
                        });
                    }
                }
                None => notified.await,
            }
        }
        Err(LaneRefusal::Closed("the lane dropped this waiter".into()))
    }

    /// 把票号从队里拿掉并说出它走时排第几；队变了，让后面的人重新争。
    fn leave(&self, ticket: u64, agent: &str, queue: &mut QueueGuard) -> usize {
        let place = {
            let mut state = self.lock();
            let place = state.place_of(ticket).unwrap_or(1);
            state.waiting.retain(|w| w.ticket != ticket);
            self.republish(&state);
            place
        };
        queue.armed = false;
        self.commands
            .observe_lane_position(agent, LaneWait::Full, place);
        // 走的那个人可能正是队头：它的离开会把空出来的机会让给下一个。
        self.wake.notify_waiters();
        place
    }
}

/// 等的人走了（调用方放弃了这次请求）：票号从队里拿掉，让后面的人重新争。
struct QueueGuard {
    lane: Arc<BuildLane>,
    ticket: u64,
    armed: bool,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        {
            let mut state = self.lane.lock();
            state.waiting.retain(|w| w.ticket != self.ticket);
            self.lane.republish(&state);
        }
        self.lane.wake.notify_waiters();
    }
}

/// 一个被放行的人占着的位子。孩子走完才 drop 它，于是下一条编译开始写 target
/// 时目录前一个人已经放开。
pub struct BuildSlot {
    lane: Arc<BuildLane>,
    ticket: u64,
    index: usize,
    /// 拿到位子的时刻：墙钟从这一刻起，不是从命令拿到墙时起。
    since: std::time::Instant,
    /// 进队时的 cgroup 读数，结局靠它和出队时比。
    started: CgroupFacts,
    /// 这条编译最终写的那块目录，装配时才定下来（非编译命令没有）。
    target: std::sync::Mutex<Option<PathBuf>>,
}

impl BuildSlot {
    /// 这条编译占的是第几条位子，从 0 数。它决定这条编译写哪个 target
    /// 目录：同一条位子上的两次编译必须先后写同一个目录，不同位子上的
    /// 两次编译不该互相等 cargo 的锁。
    pub fn index(&self) -> usize {
        self.index
    }

    /// 这条编译写哪块目录。装配这一步知道（它按位子算），位子本身不知道，
    /// 所以由外部告诉它；没有编译的命令永远不会有。
    pub fn note_target(&self, dir: PathBuf) {
        *self.target.lock().unwrap_or_else(|p| p.into_inner()) = Some(dir);
    }
}

impl Drop for BuildSlot {
    fn drop(&mut self) {
        let (beside, peak_bytes) = {
            let mut state = self.lane.lock();
            let beside = state.holding.len();
            let mut peak = 0;
            if let Some(at) = state.holding.iter().position(|h| h.ticket == self.ticket) {
                peak = state.holding[at].peak_bytes;
                state.holding.remove(at);
            }
            self.lane.republish(&state);
            (beside, peak)
        };
        let target = self
            .target
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        self.lane
            .finish_build(self.started, peak_bytes, beside, self.since, target);
        // 空出一个位子：刚才没轮到的、排在后面的人都该再看一眼。
        self.lane.wake.notify_waiters();
    }
}

/// 这一刻的编译队：容量、谁占着、谁在等（按轮转顺序）。
///
/// 与 `/metrics` 的差别是它给的是**此刻的名单**，不是累计分布：一个 agent 在
/// 队里排第几名，只能从这份名单上读。
pub fn build_lane_fairness(lane: &BuildLane) -> cog_core::BuildLaneReading {
    let capacity = lane.plan().lanes;
    let state = lane.lock();
    let mut order: Vec<(u64, u64, &Waiting)> = state
        .waiting
        .iter()
        .map(|w| {
            let served = state.last_served.get(&w.agent).copied().unwrap_or(0);
            (served, w.ticket, w)
        })
        .collect();
    order.sort_by_key(|(served, ticket, _)| (*served, *ticket));
    cog_core::BuildLaneReading {
        capacity: Some(capacity),
        // 同一个对外读数里既有编译位子的队，也有按余量算的队：扩缩、界面都读
        // 这一份，不另开第二个「这副副本忙不忙」的信号。
        room: Some(cog_core::RoomQueue {
            running: lane.pressure_ledger.open_count() as usize,
            waiting: lane.pressure_ledger.waiting_count(),
        }),
        holding: state
            .holding
            .iter()
            .map(|h| cog_core::LaneHolder {
                agent: h.agent.clone(),
                held_ms: h.since.elapsed().as_millis() as u64,
            })
            .collect(),
        waiting: order
            .iter()
            .enumerate()
            .map(|(at, (_, _, w))| cog_core::LaneWaiter {
                place: at + 1,
                agent: w.agent.clone(),
                tool: w.tool.clone(),
                waited_ms: w.arrived.elapsed().as_millis() as u64,
            })
            .collect(),
    }
}

/// The place this command holds among the builds allowed to compile at once.
///
/// `Ok(None)` is a command that compiles nothing: it takes no slot, because
/// holding one for a search would put every search behind a build.
/// 这条编译写哪个 target 目录。
///
/// 第 0 条位子写共享缓存本体——那是这份部署原本唯一的目录，也是热的那一份；
/// 其余位子各写自己的子目录。两条编译同时写同一个目录不会更快：cargo 先把后
/// 一条挡在自己的锁上，等前一条写完，于是并发的第一条编译拿到的是「等待记在
/// 编译时间里」，不是第二条编译的产出。分开目录之后，代价从锁换成了重复编一
/// 遍依赖——那才是第二条位子真实的价格。
fn lane_target_dir(base: &std::path::Path, index: usize) -> std::path::PathBuf {
    if index == 0 {
        base.to_path_buf()
    } else {
        base.join(format!("lane-{index}"))
    }
}

/// 装配这一步：编译类的命令按自己占的位子选目录，不编译的命令仍写共享缓存
/// （它不排队，也就不该被分到一个只属于某条位子的目录）。
fn build_target_dir(
    base: Option<&std::path::Path>,
    slot: Option<&BuildSlot>,
) -> Option<std::path::PathBuf> {
    let base = base?;
    Some(lane_target_dir(
        base,
        slot.map(BuildSlot::index).unwrap_or(0),
    ))
}

async fn build_slot_for(
    state: &AppState,
    agent: Option<&str>,
    tool: &str,
    command: &str,
) -> Result<Option<BuildSlot>, LaneRefusal> {
    if !crate::runtime::local::builds_the_tree(command) {
        return Ok(None);
    }
    let window = state.queue_waits.window();
    let agent_label = agent
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .unwrap_or(UNATTRIBUTED);
    match state.build_lane.acquire(agent, tool, window).await {
        Ok((slot, waited)) => {
            // 放行的等待是这台机器队列长度的样本：窗口就是从这些数里量的。
            state.queue_waits.record(waited);
            state.commands.observe_build_wait(
                agent_label,
                tool,
                LaneWait::Admitted,
                waited.as_secs_f64(),
            );
            // It is still a wait somebody pays for, so it is said out loud as
            // well: a round that spent its whole wait queued and one that
            // compiled slowly read the same on the latency histogram alone.
            if waited >= BUILD_QUEUE_NOTEWORTHY {
                tracing::info!(
                    command = %command,
                    agent = agent_label,
                    waited_secs = waited.as_secs(),
                    waited_ended = LaneWait::Admitted.as_str(),
                    "a build waited its turn for a lane"
                );
            }
            Ok(Some(slot))
        }
        Err(refusal @ LaneRefusal::Full { waited, .. }) => {
            state.commands.observe_build_wait(
                agent_label,
                tool,
                LaneWait::Full,
                waited.as_secs_f64(),
            );
            if waited >= BUILD_QUEUE_NOTEWORTHY {
                tracing::info!(
                    command = %command,
                    agent = agent_label,
                    waited_secs = waited.as_secs(),
                    waited_ended = LaneWait::Full.as_str(),
                    "a build waited its turn for a lane"
                );
            }
            Err(refusal)
        }
        Err(refusal @ LaneRefusal::Closed(_)) => {
            state
                .commands
                .observe_build_wait(agent_label, tool, LaneWait::Closed, 0.0);
            Err(refusal)
        }
    }
}

/// Why a compiling command never reached its child.
#[derive(Debug)]
enum LaneRefusal {
    /// The lane stayed full for longer than this executor's measured queue
    /// window.
    Full {
        waited: std::time::Duration,
        window: std::time::Duration,
        /// 走时排第几。同一个「没拿到」在不同的名次上是两种事实：第 1 名是
        /// 队根本没动，第 12 名是队动了但没轮到它。
        place: usize,
    },
    /// The lane is gone; no amount of waiting admits this command.
    Closed(String),
}

impl std::fmt::Display for LaneRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneRefusal::Full {
                waited,
                window,
                place,
            } => write!(
                f,
                "build lane busy: this command never started. It waited {}s at place {} \
                 in line for a slot among the commands compiling into the shared target \
                 directory, past the {}s this executor has measured its queue to take at \
                 p99.9, and did not get one. Nothing was compiled and nothing was left \
                 behind; ask again later.",
                waited.as_secs(),
                place,
                window.as_secs()
            ),
            LaneRefusal::Closed(e) => write!(f, "build slot unavailable: {e}"),
        }
    }
}

pub fn router() -> Router {
    let commands = Arc::new(CommandMetrics::new());
    let pressure = pressure::Ledger::new();
    app_router(AppState {
        workdir: None,
        hostdocs: None,
        build_lane: BuildLane::new(
            build_concurrency(),
            Arc::clone(&commands),
            Arc::clone(&pressure),
        ),
        commands,
        queue_waits: Arc::new(BuildQueueWindow::new()),
        pressure,
    })
}

/// Full router wiring a provisioned workdir router.
pub fn app_router_with_workdir(workdir: Arc<WorkdirRouter>) -> Router {
    let commands = Arc::new(CommandMetrics::new());
    let pressure = pressure::Ledger::new();
    app_router(AppState {
        workdir: Some(workdir),
        hostdocs: None,
        build_lane: BuildLane::new(
            build_concurrency(),
            Arc::clone(&commands),
            Arc::clone(&pressure),
        ),
        commands,
        queue_waits: Arc::new(BuildQueueWindow::new()),
        pressure,
    })
}

/// Full router with host document scopes enabled.
pub fn app_router_with_hostdocs(
    workdir: Option<Arc<WorkdirRouter>>,
    hostdocs: Arc<HostDocs>,
) -> Router {
    let commands = Arc::new(CommandMetrics::new());
    let pressure = pressure::Ledger::new();
    app_router(AppState {
        workdir,
        hostdocs: Some(hostdocs),
        build_lane: BuildLane::new(
            build_concurrency(),
            Arc::clone(&commands),
            Arc::clone(&pressure),
        ),
        commands,
        queue_waits: Arc::new(BuildQueueWindow::new()),
        pressure,
    })
}

fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/boundary", get(boundary))
        .route("/lane", get(lane))
        .route(METRICS_PATH, get(metrics_handler))
        .route("/execute", post(execute_handler))
        .route("/documents/list", post(documents_list_handler))
        .route("/documents/organize", post(documents_organize_handler))
        .route("/documents/read", post(documents_read_handler))
        .route("/documents/plan", post(documents_plan_handler))
        .route("/documents/stage", post(documents_stage_handler))
        .route("/documents/approve", post(documents_approve_handler))
        .route("/documents/reject", post(documents_reject_handler))
        .route("/documents/staged", get(documents_staged_handler))
        .route("/documents/review", post(documents_review_handler))
        .route("/documents/apply", post(documents_apply_handler))
        .route("/documents/rollback", post(documents_rollback_handler))
        .route("/worktree/has", get(worktree_has_handler))
        .route("/worktree/export", post(worktree_export_handler))
        .route(
            "/worktree/import",
            post(worktree_import_handler).layer(DefaultBodyLimit::disable()),
        )
        .with_state(state)
}

#[derive(Deserialize)]
struct WorktreeExportRequest {
    lane: String,
}

#[derive(Deserialize)]
struct WorktreeImportQuery {
    lane: String,
}

#[derive(Deserialize)]
struct WorktreeHasQuery {
    lane: String,
}

#[derive(Deserialize)]
struct DocumentsPlanRequest {
    scope: String,
    ops: Vec<HostDocOp>,
}

#[derive(Deserialize)]
struct DocumentsListRequest {
    scope: String,
    /// Only entries under this relative path. Absent means the whole scope.
    #[serde(default)]
    prefix: Option<String>,
}

#[derive(Deserialize)]
struct DocumentsReadRequest {
    scope: String,
    path: String,
}

#[derive(Deserialize)]
struct DocumentsRollbackRequest {
    journal_id: String,
}

/// Organize one scope. It carries the scope and nothing else: which files belong in
/// which bucket is a configuration fact on this side (`HOST_DOCS_CLASSIFY_RULES`), and
/// letting the caller send its own table would put the writable path set under the
/// caller's control -- which is exactly what the model station must not have.
#[derive(Deserialize)]
struct DocumentsOrganizeRequest {
    scope: String,
}

#[derive(Deserialize)]
struct DocumentsStageRequest {
    scope: String,
    ops: Vec<HostDocOp>,
}

#[derive(Deserialize)]
struct DocumentsApproveRequest {
    staged_id: String,
    /// Who approved it. The executor **cannot verify** this name (this pod
    /// deliberately holds no credentials); it is recorded so the approval can be
    /// looked up later -- an empty approver is no record at all.
    approver: String,
}

#[derive(Deserialize)]
struct DocumentsRejectRequest {
    staged_id: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct DocumentsReviewRequest {
    staged_id: String,
}

/// Apply an already-approved plan. The plan travels in the request body together
/// with its approval id: the approval id is part of what this call means, and
/// hiding it in a header or query string would keep "who approved this" out of
/// the parts of the call that get logged.
#[derive(Deserialize)]
struct DocumentsApplyRequest {
    plan: HostDocPlan,
    approval_id: String,
}

/// A refused operation is the caller's to fix (a path outside the scope, an
/// overwrite, a stale plan); everything else is this executor's own state.
fn documents_error(error: SFError) -> Response {
    let status = match error {
        SFError::Validation(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    error_response(status, error.to_string())
}

fn documents_disabled() -> Response {
    error_response(
        StatusCode::CONFLICT,
        "host document access is not configured on this executor".into(),
    )
}

/// List a scope's metadata. **Not gated by the egress switch**: the answer is file
/// names and timestamps, not bodies. Gating this step as well would leave a
/// deployment that never turned the switch on unable to see which files exist at
/// all, which is the whole capability off by default.
async fn documents_list_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsListRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.list(&req.scope, req.prefix.as_deref()) {
        Ok(listing) => Json(listing).into_response(),
        Err(e) => documents_error(e),
    }
}

async fn documents_read_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsReadRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.read(&req.scope, &req.path) {
        Ok(read) => Json(read).into_response(),
        // A closed switch means "this is not being done today" (403), not "you sent a
        // bad request" (400). The caller's next move is opposite in the two cases --
        // turn the switch on, or change the request -- so the codes differ too; the
        // verdict (which cell) and the message come from one value, so this does not
        // re-judge it here.
        Err(refusal) if refusal.outcome == "disabled" => {
            error_response(StatusCode::FORBIDDEN, refusal.error.to_string())
        }
        Err(refusal) => documents_error(refusal.error),
    }
}

/// The caller-side pipeline in one call: list, classify by name, stage. **Not gated by
/// the egress switch** -- today it consults no model, so nothing leaves the process, and
/// the switch exists to bound document bodies going out, not names being sorted. The
/// station that does read bodies takes the gate with it when it arrives, and it will be
/// the read that refuses, one step before any request could be built.
///
/// It stages rather than applies: the answer is a plan and an id, and a person still has
/// to approve it. That keeps "the model tidied up my files" from being a thing this
/// endpoint can do at all.
async fn documents_organize_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsOrganizeRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.organize(&req.scope).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(e) => documents_error(e),
    }
}

async fn documents_plan_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsPlanRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.plan(&req.scope, &req.ops) {
        Ok(plan) => Json(plan).into_response(),
        Err(e) => documents_error(e),
    }
}

/// Stage a plan for approval and return its id. **Not gated by the egress switch**:
/// what is stored is a description of the intended change, and no body has moved
/// yet (the same reasoning as listing).
async fn documents_stage_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsStageRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.stage(&req.scope, &req.ops).await {
        Ok(staged) => Json(staged).into_response(),
        Err(e) => documents_error(e),
    }
}

/// The approval face. What defines it is **who calls it**: the caller side (the
/// model path) is meant to produce plans and stage them, while approving and
/// rejecting are human actions. The executor cannot verify identity (a
/// zero-credential deployment), so this boundary rests on reachability and on who
/// is calling -- a convention, not cryptography. Which side can reach it is stated
/// in the design; this does not pretend to hold a wall it does not have.
async fn documents_approve_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsApproveRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.approve(&req.staged_id, &req.approver).await {
        Ok(approved) => Json(approved).into_response(),
        Err(e) => documents_error(e),
    }
}

async fn documents_reject_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsRejectRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.reject(&req.staged_id, req.reason.as_deref()).await {
        Ok(rejected) => Json(rejected).into_response(),
        Err(e) => documents_error(e),
    }
}

/// The pending list: what a reviewer sees is waiting. It carries **no plan body**;
/// the body goes through `/documents/review`.
async fn documents_staged_handler(State(state): State<AppState>) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.staged_plans().await {
        Ok(list) => Json(list).into_response(),
        Err(e) => documents_error(e),
    }
}

/// One staged plan in full, with every operation and its effect: the reviewer
/// decides from this whether to approve.
async fn documents_review_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsReviewRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.staged_plan(&req.staged_id).await {
        Ok(record) => Json(record).into_response(),
        Err(e) => documents_error(e),
    }
}

async fn documents_apply_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsApplyRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.apply(&req.plan, &req.approval_id).await {
        Ok(applied) => Json(applied).into_response(),
        Err(e) => documents_error(e),
    }
}

async fn documents_rollback_handler(
    State(state): State<AppState>,
    Json(req): Json<DocumentsRollbackRequest>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.rollback(&req.journal_id).await {
        Ok(rolled) => Json(rolled).into_response(),
        Err(e) => documents_error(e),
    }
}

/// Entry point for the `sandbox-executor` subcommand.
///
/// The listen port comes from `SANDBOX_EXECUTOR_PORT` through
/// [`executor_port_from_env`]: a missing or unusable value falls back to
/// [`DEFAULT_EXECUTOR_PORT`], the port the executor's Service targets.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // 沙箱「起来多快」是它自己的价格，不是某个任务的：从这一行进来到监听这一段
    // 付一次，之后每个任务都不再重付。记在进程上，形态价格表要用。
    let booted = Instant::now();
    // 订阅者由装配层在派发前装（`cogneva` 的 main.rs）：本 crate 只许依赖
    // cog-core，够不到 cog-observability，而这一行日志要活过 Job 本身，
    // 靠的正是那边装上的 Loki 镜像。
    let port = executor_port_from_env();
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));

    // Host document scopes exist only where the deployment mounted one. An
    // empty scope map keeps the whole capability (routes included) off, so a
    // cluster that never opted in cannot be reached through it at all.
    let hostdocs_cfg = crate::hostdocs::HostDocsConfig::from_env();
    let hostdocs = if hostdocs_cfg.is_enabled() {
        match HostDocs::new(hostdocs_cfg) {
            Ok(docs) => Some(docs),
            Err(e) => {
                tracing::error!(error = %e, "host document scopes unusable; refusing to serve them");
                None
            }
        }
    } else {
        tracing::info!("host document scopes not configured; document routes answer 409");
        None
    };

    // With a provisioned volume the executor seeds its own bare repo and routes
    // every stamped request into a per-task worktree; without it the legacy
    // process-cwd behaviour stays intact for embedded and test usage.
    let workdir = match WorkdirRouter::from_env().await {
        Some(workdir) => {
            if let Err(e) = workdir.recover().await {
                tracing::error!(error = %e, "workdir recovery failed; serving without router");
                None
            } else {
                // Recovery is local and fast and stays on the startup path; the
                // first upstream fetch runs inside spawn_maintenance so a stalled
                // network cannot block the HTTP listener (and liveness probe).
                workdir.spawn_maintenance();
                tracing::info!(
                    workspaces = %workdir.config().workspaces_root.display(),
                    bare = %workdir.config().bare_repo.display(),
                    "per-task workdir router enabled"
                );
                Some(workdir)
            }
        }
        None => None,
    };
    let commands = Arc::new(CommandMetrics::new());
    let pressure = pressure::Ledger::new();
    let app = app_router(AppState {
        workdir,
        hostdocs,
        build_lane: BuildLane::new(
            build_concurrency(),
            Arc::clone(&commands),
            Arc::clone(&pressure),
        ),
        commands,
        queue_waits: Arc::new(BuildQueueWindow::new()),
        pressure,
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "sandbox executor listening");
    // 绑上端口才算「起来了」：在这之前它接不了任务，之后才付得出价格。
    pressure::record_startup(booted.elapsed().as_millis() as u64);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that mutate `SANDBOX_EXECUTOR_PORT`: the variable
    /// is process-global while the harness runs tests on parallel threads.
    static PORT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    /// The build-concurrency variable is process-wide state and tests run in
    /// threads of one process; a test that reads the default has to be the only
    /// one touching it while it does.
    static BUILD_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// 执行器这一侧没有任何「调用方要多久」的入口了：请求体里没有这一格，
    /// 命令只在它停住时被停。旧的 `timeout_ms` 与 30 秒默认墙都拆了。
    #[test]
    fn an_execute_request_carries_no_declared_wall() {
        let body = serde_json::json!({
            "payload": {"type": "command", "command": "echo hi"},
            "task_id": "t-1",
            "agent_id": "a",
        });
        let parsed: ExecuteRequest = serde_json::from_value(body).expect("the request parses");
        assert_eq!(parsed.task_id.as_deref(), Some("t-1"));
        // 旧调用方仍带着 `timeout_ms` 时不至于 400：它只是不再被读。
        let legacy = serde_json::json!({
            "payload": {"type": "command", "command": "echo hi"},
            "timeout_ms": 600000,
        });
        assert!(serde_json::from_value::<ExecuteRequest>(legacy).is_ok());
    }

    /// 部署没写条数时，条数是量出来的，不是代码里写死的一个数。
    ///
    /// 量到之前它读作 1——1 是还没量过任何东西时跑的那条——但读数的出处说
    /// 得出这是「还没量到」，不是「量到只有一条」。
    #[test]
    fn with_no_declared_count_the_lane_count_is_measured_not_written() {
        let _guard = BUILD_ENV_LOCK.lock().unwrap();
        std::env::remove_var("COGNEVA_SANDBOX_BUILD_CONCURRENCY");
        assert_eq!(
            build_concurrency(),
            None,
            "没写条数不是写了一条：写了一条就压过测量"
        );
        std::env::set_var("COGNEVA_SANDBOX_BUILD_CONCURRENCY", "3");
        assert_eq!(build_concurrency(), Some(3));
        std::env::set_var("COGNEVA_SANDBOX_BUILD_CONCURRENCY", "0");
        assert_eq!(build_concurrency(), None, "写 0 是「没写」的另一种拼法");
        std::env::remove_var("COGNEVA_SANDBOX_BUILD_CONCURRENCY");
    }

    /// 同一个 1，两个出处：一个来自「这个容器只够一条」，一个来自「一条都
    /// 没量过」。读数面必须分得开——分不开的话，内存真的紧和闭环根本没读到
    /// 数是同一个读数。
    #[test]
    fn a_measured_one_and_an_unmeasured_one_read_differently() {
        let commands = Arc::new(CommandMetrics::new());
        let lane = BuildLane::new(None, Arc::clone(&commands), pressure::Ledger::new());
        let gib = 1u64 << 30;

        let blind = lane.plan_with(&CgroupFacts::default(), None);
        assert_eq!(blind.lanes, 1);
        assert!(matches!(blind.source, LaneSource::Unmeasured(_)));
        let rendered = commands.render();
        assert!(
            rendered.contains(
                r#"sandbox_build_lane_capacity{reading="memory limit",source="unmeasured"} 1"#
            ),
            "{rendered}"
        );

        {
            let mut cost = lane.cost();
            cost.observe(gib / 2, 0);
            cost.record(
                BuildSample {
                    peak_bytes: Some(4 * gib),
                    target_bytes: Some(gib),
                    wall: std::time::Duration::from_secs(60),
                    beside: 1,
                },
                BuildEnd::Completed,
            );
        }
        let facts = CgroupFacts {
            limit_bytes: Some(8 * gib),
            ..Default::default()
        };
        let measured = lane.plan_with(&facts, Some(100 * gib));
        assert_eq!(measured.lanes, 1);
        assert_eq!(measured.source, LaneSource::Measured("memory"));
        let rendered = commands.render();
        assert!(
            rendered
                .contains(r#"sandbox_build_lane_capacity{reading="memory",source="measured"} 1"#),
            "{rendered}"
        );
        // 两个来源不会同时挂着：一条是「此刻」的答案，不是另一条的补充。
        assert!(!rendered.contains("unmeasured"), "{rendered}");

        // 同样的容器、更便宜的一条编译：同一次算术给出 6 条，而不是停在 1。
        // （另一条队而不是往同一个窗口里再记一条：代价取的是窗口里的最大值，
        // 例如一条 4 GiB 的编译还在窗口里时，新记一条小的不会把价格压下来。）
        let cheaper = BuildLane::new(None, Arc::clone(&commands), pressure::Ledger::new());
        {
            let mut cost = cheaper.cost();
            cost.observe(gib / 2, 0);
            cost.record(
                BuildSample {
                    peak_bytes: Some(gib),
                    target_bytes: Some(gib),
                    wall: std::time::Duration::from_secs(60),
                    beside: 1,
                },
                BuildEnd::Completed,
            );
        }
        assert_eq!(cheaper.plan_with(&facts, Some(100 * gib)).lanes, 6);
    }

    /// 一条编译放下位子时就被记了价：它的 target 目录有多大、它占了多久。
    /// 这两个数就是下一条位子的价格，没有它们，条数永远不会离开 1。
    #[test]
    fn a_finished_build_is_priced_when_it_releases_its_lane() {
        let commands = Arc::new(CommandMetrics::new());
        let lane = BuildLane::new(Some(1), Arc::clone(&commands), pressure::Ledger::new());
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("artifact"), vec![0u8; 123]).expect("write");
        let slot = BuildSlot {
            lane: Arc::clone(&lane),
            ticket: 0,
            index: 0,
            since: std::time::Instant::now(),
            started: CgroupFacts::default(),
            target: std::sync::Mutex::new(None),
        };
        slot.note_target(dir.path().to_path_buf());
        drop(slot);

        let cost = lane.cost();
        assert_eq!(cost.target_bytes(), Some(123));
        assert_eq!(cost.walls().count(), 1, "这条编译的墙钟记了一次");
    }

    fn state_with_build_slots(n: usize) -> AppState {
        let commands = Arc::new(CommandMetrics::new());
        let pressure = pressure::Ledger::new();
        AppState {
            workdir: None,
            hostdocs: None,
            build_lane: BuildLane::new(Some(n), Arc::clone(&commands), Arc::clone(&pressure)),
            commands,
            queue_waits: Arc::new(BuildQueueWindow::new()),
            pressure,
        }
    }

    /// The bound this exists for: a second build does not start while the
    /// first one holds the last slot, a command that compiles nothing is never
    /// made to wait for one, and a wait past this executor's *measured* queue
    /// window is refused by name instead of hanging until the caller gives up.
    ///
    /// No caller declares a wall any more, so the window is planted here the
    /// same way the movement predicate's is: from gaps a test produced.
    #[tokio::test]
    async fn a_build_waits_its_turn_and_a_search_does_not() {
        let state = state_with_build_slots(1);
        for _ in 0..cog_core::progress::SAMPLE_FLOOR {
            state
                .queue_waits
                .record(std::time::Duration::from_millis(50));
        }
        assert_eq!(
            state.queue_waits.window(),
            Some(std::time::Duration::from_millis(50)),
            "the window is the planted p99.9, not a number of its own"
        );

        let held = build_slot_for(
            &state,
            Some("a"),
            "run_command",
            "cargo test -p cogneva --lib",
        )
        .await
        .expect("the semaphore is open")
        .expect("a build takes a slot");

        let queued = tokio::time::timeout(std::time::Duration::from_millis(20), async {
            build_slot_for(
                &state,
                Some("a"),
                "run_command",
                "cargo clippy --all-targets",
            )
            .await
        })
        .await;
        assert!(
            queued.is_err(),
            "a second build must wait while the only slot is held"
        );

        // A search names the same program without running it, and must not
        // queue behind a build to do so.
        let search = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            build_slot_for(&state, Some("a"), "run_command", "grep -rn cargo crates/"),
        )
        .await
        .expect("a search never queues")
        .expect("the semaphore is open");
        assert!(search.is_none(), "a search holds no build slot");

        // Formatting the tree is the same shape: a program that would compile,
        // asked to do something that writes nothing into the target directory.
        let formatting = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            build_slot_for(
                &state,
                Some("a"),
                "run_command",
                "cargo fmt --all -- --check",
            ),
        )
        .await
        .expect("formatting never queues")
        .expect("the semaphore is open");
        assert!(formatting.is_none(), "formatting holds no build slot");

        // A wait past the measured window is a refusal that names both numbers,
        // not a command started only to be killed.
        let refused = build_slot_for(
            &state,
            Some("a"),
            "run_command",
            "cargo clippy --all-targets",
        )
        .await;
        match refused {
            Err(LaneRefusal::Full { waited, window, .. }) => {
                assert!(waited >= window, "the wait outlasted the measured window");
                assert!(
                    LaneRefusal::Full {
                        waited,
                        window,
                        place: 1
                    }
                    .to_string()
                    .contains("never started"),
                    "the refusal says the command did not run"
                );
            }
            Ok(_) => panic!("a full lane past the window must refuse"),
            Err(LaneRefusal::Closed(e)) => panic!("the semaphore was closed: {e}"),
        }

        drop(held);
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            build_slot_for(
                &state,
                Some("a"),
                "run_command",
                "cargo clippy --all-targets",
            ),
        )
        .await
        .expect("the released slot admits the waiting build")
        .expect("the semaphore is open");
        assert!(next.is_some(), "the queued build gets the slot back");

        // With too few samples there is no window, and a waiter waits rather
        // than being refused on a number nobody measured.
        let unmeasured = state_with_build_slots(1);
        assert_eq!(unmeasured.queue_waits.window(), None);
        let _held_again = build_slot_for(
            &unmeasured,
            Some("a"),
            "run_command",
            "cargo test -p cogneva",
        )
        .await
        .unwrap()
        .expect("a build takes a slot");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(60),
                build_slot_for(
                    &unmeasured,
                    Some("a"),
                    "run_command",
                    "cargo clippy --all-targets"
                ),
            )
            .await
            .is_err(),
            "without a measured window the wait is not refused"
        );

        // The wait is published, so a round that queued and a round that
        // compiled slowly are separable without reading the log.
        let rendered = state.commands.render();
        assert!(
            rendered.contains(
                "sandbox_build_slot_wait_seconds_count{agent=\"a\",outcome=\"admitted\",tool=\"run_command\"}"
            ),
            "the wait is a reading of its own: {rendered}"
        );
        assert!(
            rendered.contains(
                "sandbox_build_slot_wait_seconds_count{agent=\"a\",outcome=\"full\",tool=\"run_command\"}"
            ),
            "the reading separates the lane admitting a command from refusing one: {rendered}"
        );
    }

    /// 等到队里排着 `n` 个人：票号按入队先后发，只有所有人都进了队，
    /// 名次才说明问题。
    async fn wait_until_queued(lane: &Arc<BuildLane>, n: usize) {
        for _ in 0..2_000 {
            let queued = build_lane_fairness(lane).waiting.len();
            if queued == n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!("the lane never reached {n} waiters");
    }

    /// 一个 agent 先占着位子、又连排两条，另一个 agent 才排一条：放行的顺序
    /// 是「另一个、这个、这个」——队按 agent 轮转，不按到达先后。
    ///
    /// 消融：把 `LaneState::fair_head` 换回先进先出，放行顺序变成 a、a、b，
    /// 这条当场红。
    #[tokio::test]
    async fn the_lane_turns_by_agent_not_by_arrival() {
        let commands = Arc::new(CommandMetrics::new());
        let lane = BuildLane::new(Some(1), Arc::clone(&commands), pressure::Ledger::new());
        let held = lane
            .acquire(Some("a"), "run_command", None)
            .await
            .expect("an empty lane admits at once")
            .0;

        let (tx, mut admitted) = tokio::sync::mpsc::unbounded_channel::<&'static str>();
        let mut waiters = Vec::new();
        for who in ["a", "a", "b"] {
            let lane = Arc::clone(&lane);
            let tx = tx.clone();
            waiters.push(tokio::spawn(async move {
                let (slot, _) = lane
                    .acquire(Some(who), "run_command", None)
                    .await
                    .expect("the lane admits everyone eventually");
                tx.send(who).expect("the test still reads");
                drop(slot);
            }));
        }
        drop(tx);
        wait_until_queued(&lane, 3).await;

        // b 在 a 的两条之后入队，却排在第一：轮到的是最久没被放行的 agent。
        let reading = build_lane_fairness(&lane);
        assert_eq!(reading.capacity, Some(1));
        let places: Vec<(&str, usize)> = reading
            .waiting
            .iter()
            .map(|waiter| (waiter.agent.as_str(), waiter.place))
            .collect();
        assert_eq!(
            places,
            vec![("b", 1), ("a", 2), ("a", 3)],
            "轮转：b 排在 a 的两条之前"
        );

        drop(held);
        let mut order = Vec::new();
        for _ in 0..3 {
            let who = tokio::time::timeout(std::time::Duration::from_secs(1), admitted.recv())
                .await
                .expect("the lane admits all three")
                .expect("a waiter is still alive");
            order.push(who);
        }
        assert_eq!(
            order,
            vec!["b", "a", "a"],
            "a 被放行一次之后才轮到自己剩下的一条"
        );
        for waiter in waiters {
            waiter.abort();
        }
    }

    /// 位子不再只是「还有没有空」，而是「空的是哪一条」：同时被放行的两条编译
    /// 拿到的位子序号必须不同，否则它们写的还是同一个 target 目录，第二条只是
    /// 排到 cargo 的锁上——两条位子在读数上成了「两条一起等」。
    ///
    /// 消融：放行时把 `index` 写死成 0，这条红。
    #[tokio::test]
    async fn two_builds_at_once_take_different_lane_indices() {
        let state = state_with_build_slots(2);
        let first = build_slot_for(&state, Some("alice"), "run_command", "cargo build")
            .await
            .expect("the lane is open")
            .expect("a build takes a slot");
        let second = build_slot_for(&state, Some("bob"), "run_command", "cargo build")
            .await
            .expect("a second slot is open")
            .expect("a build takes a slot");
        assert_eq!(first.index(), 0);
        assert_eq!(second.index(), 1, "两条位子分到了同一个 target 目录");

        // 两条都占着的时候第三条等：位子数是容量，不是「谁先到谁进」。
        let mut third = Box::pin(build_slot_for(
            &state,
            Some("carol"),
            "run_command",
            "cargo build",
        ));
        assert!(
            futures::poll!(third.as_mut()).is_pending(),
            "两条位子都被占着，第三条不该被放行"
        );

        drop(first);
        let third = third
            .await
            .expect("the lane is open")
            .expect("a build takes a slot");
        assert_eq!(third.index(), 0, "空出来的那条位子被下一个人接着用");
    }

    /// 位子决定写哪个目录：第 0 条写共享缓存本体（已经在里面的依赖是热的），
    /// 其余各写自己的子目录；不编译的命令没有位子，仍写共享缓存。
    ///
    /// 消融：让 `lane_target_dir` 永远返回本体，第二条断言红；让
    /// `build_target_dir` 忽略位子，两条「按位子分目录」的断言红。
    #[tokio::test]
    async fn the_lane_index_decides_the_target_directory() {
        let base = std::path::Path::new("/opt/cogneva/sandbox/target");
        assert_eq!(lane_target_dir(base, 0), base, "第 0 条位子写共享缓存本体");
        assert_eq!(lane_target_dir(base, 1), base.join("lane-1"));
        assert_eq!(lane_target_dir(base, 3), base.join("lane-3"));

        let state = state_with_build_slots(2);
        let first = build_slot_for(&state, Some("alice"), "run_command", "cargo build")
            .await
            .expect("the lane is open")
            .expect("a build takes a slot");
        let second = build_slot_for(&state, Some("bob"), "run_command", "cargo build")
            .await
            .expect("a second slot is open")
            .expect("a build takes a slot");
        assert_eq!(
            build_target_dir(Some(base), Some(&first)).as_deref(),
            Some(base)
        );
        assert_eq!(
            build_target_dir(Some(base), Some(&second)).as_deref(),
            Some(base.join("lane-1").as_path())
        );
        assert_eq!(
            build_target_dir(Some(base), None).as_deref(),
            Some(base),
            "不编译的命令不占位子，也就没有只属于某条位子的目录"
        );
        assert_eq!(build_target_dir(None, None), None);
    }

    /// 一个 agent 一口气排十条、另一个只排一条：后者排第一而不是第十一。
    ///
    /// 消融：先进先出的队里 b 排第十一，这条当场红。
    #[tokio::test]
    async fn ten_builds_from_one_agent_do_not_bury_the_other() {
        let commands = Arc::new(CommandMetrics::new());
        let lane = BuildLane::new(Some(1), Arc::clone(&commands), pressure::Ledger::new());
        let held = lane
            .acquire(Some("a"), "run_command", None)
            .await
            .expect("an empty lane admits at once")
            .0;

        let (tx, mut admitted) = tokio::sync::mpsc::unbounded_channel::<&'static str>();
        let mut waiters = Vec::new();
        let mut queue: Vec<&'static str> = vec!["a"; 10];
        queue.push("b");
        for who in queue {
            let lane = Arc::clone(&lane);
            let tx = tx.clone();
            waiters.push(tokio::spawn(async move {
                let (slot, _) = lane
                    .acquire(Some(who), "run_command", None)
                    .await
                    .expect("the lane admits everyone eventually");
                tx.send(who).expect("the test still reads");
                drop(slot);
            }));
        }
        drop(tx);
        wait_until_queued(&lane, 11).await;

        let reading = build_lane_fairness(&lane);
        let first = reading.waiting.first().expect("b is in line");
        assert_eq!(first.agent, "b", "b 排第一：{reading:?}");
        assert_eq!(first.place, 1, "b 是第一名：{reading:?}");

        drop(held);
        let first = tokio::time::timeout(std::time::Duration::from_secs(1), admitted.recv())
            .await
            .expect("the lane admits someone")
            .expect("a waiter is still alive");
        assert_eq!(first, "b", "十条 a 挡不住 b 的一条");
        for waiter in waiters {
            waiter.abort();
        }
    }

    /// 队把此刻的名单摆出来：谁占着、谁在等、等的人排第几；按 agent 的占用数
    /// 与名次分布进 `/metrics`，被拒的话说出自己被拒在第几名。
    #[tokio::test]
    async fn the_lane_says_who_is_waiting_and_where() {
        let state = state_with_build_slots(1);
        for _ in 0..cog_core::progress::SAMPLE_FLOOR {
            state
                .queue_waits
                .record(std::time::Duration::from_millis(50));
        }
        let _held = build_slot_for(
            &state,
            Some("a"),
            "run_command",
            "cargo test -p cogneva --lib",
        )
        .await
        .expect("the lane is open")
        .expect("a build takes a slot");

        // b 没有窗口，一直等；c 有这台机器的窗口，等在 b 后面。
        let mut b_wait = Box::pin(state.build_lane.acquire(Some("b"), "run_command", None));
        assert!(
            futures::poll!(b_wait.as_mut()).is_pending(),
            "b waits while a holds the only slot"
        );

        let reading = build_lane_fairness(&state.build_lane);
        assert_eq!(reading.capacity, Some(1));
        assert_eq!(
            reading.holding.first().map(|h| h.agent.as_str()),
            Some("a"),
            "谁占着：{reading:?}"
        );
        let waiting = reading.waiting.first().expect("b is in line");
        assert_eq!(waiting.agent, "b", "谁在等：{reading:?}");
        assert_eq!(waiting.place, 1, "等的人排第几：{reading:?}");

        let rendered = state.commands.render();
        assert!(
            rendered.contains("sandbox_build_lane_waiting{agent=\"b\"} 1"),
            "在等的人数按 agent 归属：{rendered}"
        );
        assert!(
            rendered.contains("sandbox_build_lane_holding{agent=\"a\"} 1"),
            "占槽者也按 agent 归属：{rendered}"
        );

        match build_slot_for(
            &state,
            Some("c"),
            "run_command",
            "cargo clippy --all-targets",
        )
        .await
        {
            Err(refusal @ LaneRefusal::Full { place, .. }) => {
                assert_eq!(place, 2, "c 等在 b 之后");
                assert!(
                    refusal.to_string().contains("at place 2"),
                    "被拒的话说出名次：{refusal}"
                );
            }
            Err(LaneRefusal::Closed(e)) => panic!("the lane closed: {e}"),
            Ok(_) => panic!("a full lane past the measured window must refuse"),
        }
        let rendered = state.commands.render();
        assert!(
            rendered.contains("sandbox_build_lane_position_count{agent=\"c\",outcome=\"full\"} 1"),
            "被拒时的名次是一条读数：{rendered}"
        );
        drop(b_wait);
    }

    /// 一副副本对外的读数里既有编译位子的队，也有按余量算的队：在跑的命令与
    /// 在等余量的命令各算一份。只报编译位子，一副挡着命令的副本会被扩缩读成
    /// 闲着——而它把工作挡住，正是「该加副本」的读数。
    #[tokio::test]
    async fn the_lane_reading_carries_commands_waiting_for_room() {
        const GIB: u64 = 1 << 30;
        let ledger = pressure::Ledger::new();
        let lane = BuildLane::new(None, Arc::new(CommandMetrics::new()), Arc::clone(&ledger));
        let demand = pressure::Demand::from_samples(&[8 * GIB]);
        let parked = {
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move {
                pressure::wait_for_admission(
                    &ledger,
                    |reserved| Pressure::new(Some(8 * GIB), Some(GIB), reserved),
                    &demand,
                    || true,
                    None,
                )
                .await
            })
        };
        for _ in 0..200 {
            if ledger.waiting_count() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let held = ledger.reserve(GIB);
        let reading = build_lane_fairness(&lane);
        assert_eq!(
            reading.room,
            Some(cog_core::RoomQueue {
                running: 1,
                waiting: 1
            }),
            "在跑一位、在等一位：{reading:?}"
        );
        drop(held);
        parked.abort();
        cog_core::ignore_lost!(parked.await);
        assert_eq!(
            build_lane_fairness(&lane).room,
            Some(cog_core::RoomQueue {
                running: 0,
                waiting: 0
            }),
            "没人跑也没人等时这一格是量到的零"
        );
    }

    /// `/lane` 是执行器把队摆出来的那一面：编排侧与界面读的是这一份，而不是自己排一条队
    /// 再猜自己在第几位。
    #[tokio::test]
    async fn an_http_reader_can_see_the_lane_the_executor_holds() {
        let state = state_with_build_slots(1);
        let _held = build_slot_for(
            &state,
            Some("alice"),
            "run_command",
            "cargo test -p cogneva",
        )
        .await
        .expect("the lane is open")
        .expect("a build takes a slot");
        let lane = Arc::clone(&state.build_lane);
        let mut bob = Box::pin(lane.acquire(Some("bob"), "run_command", None));
        assert!(futures::poll!(bob.as_mut()).is_pending());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app_router(state)).await.unwrap();
        });

        let reading: cog_core::BuildLaneReading = reqwest::Client::new()
            .get(format!("http://{addr}/lane"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(reading.capacity, Some(1));
        assert_eq!(
            reading.holding.first().map(|h| h.agent.as_str()),
            Some("alice"),
            "占着位子的人是谁：{reading:?}"
        );
        let waiting = reading.waiting.first().expect("bob is in line");
        assert_eq!(waiting.agent, "bob", "在等的人是谁：{reading:?}");
        assert_eq!(waiting.place, 1, "在等的人排第几：{reading:?}");
        drop(bob);
    }

    /// 等在队里的人自己走了（调用方断开）：票号拿掉，读数退回 0，队不卡在
    /// 一个已经不在了的人后面。
    #[tokio::test]
    async fn a_caller_that_leaves_the_line_stops_being_counted() {
        let state = state_with_build_slots(1);
        let held = build_slot_for(
            &state,
            Some("a"),
            "run_command",
            "cargo test -p cogneva --lib",
        )
        .await
        .expect("the lane is open")
        .expect("a build takes a slot");
        let mut b_wait = Box::pin(build_slot_for(
            &state,
            Some("b"),
            "run_command",
            "cargo clippy --all-targets",
        ));
        assert!(futures::poll!(b_wait.as_mut()).is_pending());
        assert!(
            state
                .commands
                .render()
                .contains("sandbox_build_lane_waiting{agent=\"b\"} 1"),
            "b 在队里：{}",
            state.commands.render()
        );

        drop(b_wait);
        let rendered = state.commands.render();
        assert!(
            rendered.contains("sandbox_build_lane_waiting{agent=\"b\"} 0"),
            "走掉的人不该留下一个「还在等」：{rendered}"
        );
        assert_eq!(
            build_lane_fairness(&state.build_lane).waiting.len(),
            0,
            "队里不留走掉的人"
        );

        drop(held);
        let next = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            build_slot_for(
                &state,
                Some("b"),
                "run_command",
                "cargo clippy --all-targets",
            ),
        )
        .await
        .expect("the lane is not stuck behind a caller that left")
        .expect("the lane is open");
        assert!(next.is_some(), "下一个人立刻拿到位子");
    }

    #[test]
    fn executor_port_defaults_when_unset_and_overrides_when_set() {
        let _guard = PORT_ENV_LOCK.lock().unwrap();
        std::env::remove_var("SANDBOX_EXECUTOR_PORT");
        assert_eq!(executor_port_from_env(), DEFAULT_EXECUTOR_PORT);
        std::env::set_var("SANDBOX_EXECUTOR_PORT", " 9191 ");
        assert_eq!(executor_port_from_env(), 9191);
        std::env::set_var("SANDBOX_EXECUTOR_PORT", "not-a-port");
        assert_eq!(executor_port_from_env(), DEFAULT_EXECUTOR_PORT);
        std::env::set_var("SANDBOX_EXECUTOR_PORT", "0");
        assert_eq!(executor_port_from_env(), DEFAULT_EXECUTOR_PORT);
        std::env::set_var("SANDBOX_EXECUTOR_PORT", "70000");
        assert_eq!(executor_port_from_env(), DEFAULT_EXECUTOR_PORT);
        std::env::remove_var("SANDBOX_EXECUTOR_PORT");
    }

    async fn post(addr: std::net::SocketAddr, body: serde_json::Value) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("http://{}/execute", addr))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn events_of(response: reqwest::Response) -> Vec<CommandEvent> {
        let body = response.text().await.unwrap();
        body.lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()
            .expect("server emits valid CommandEvent NDJSON")
    }

    async fn spawn_server() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router()).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn server_executes_full_shell_and_streams_events() {
        let addr = spawn_server().await;
        let events = events_of(post(
            addr,
            serde_json::json!({"payload": {"type": "command", "command": "echo out | tr 'a-z' 'A-Z'; echo err >&2; exit 7"}}),
        ).await).await;
        let stdout: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stdout { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        let stderr: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stderr { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        let exit = events.iter().find_map(|e| match e {
            CommandEvent::Exit { code } => Some(*code),
            _ => None,
        });
        assert_eq!(stdout.trim(), "OUT");
        assert_eq!(stderr.trim(), "err");
        assert_eq!(exit, Some(7));
    }

    /// Whether a running command claims its tree is decided at this seam —
    /// between routing and execution — and a claim that is never taken looks
    /// exactly like one that is. So the seam is driven end to end: a real
    /// router, a command that outlives the assertions, and the LRU cap asked to
    /// reclaim while it runs.
    #[tokio::test]
    async fn a_running_command_holds_its_task_worktree() {
        // 这条命令在等一个文件：它不印东西、也不烧 CPU，按任何一枚窄窗都会被判停——
        // 而这条用例量的是「跑着的命令占着那棵树」，判停不是它要证的事。窗口由本用例
        // 自己落（水库是进程级的，测试二进制里量的是别的活的节奏）。
        let _serial = cog_core::progress::async_test_lock().await;
        cog_core::progress::reset_for_tests();
        cog_core::progress::seed_gaps_for_tests(
            std::time::Duration::from_secs(300),
            cog_core::progress::SAMPLE_FLOOR,
        );

        let tmp = tempfile::tempdir().unwrap();
        let bare = crate::workdir::tests::seed_bare(tmp.path());
        // A cap of one, so that the tree under the command is the only tree
        // the cap can reach: whether it is reached then does not depend on
        // which tree the LRU happened to look at first. With room for another
        // idle tree the cap meets itself by taking that one, and the reading
        // this test is about (`cap_deferred`) stays at zero for a reason that
        // has nothing to do with the claim — readdir order decided it.
        let workdir = crate::workdir::tests::router(
            tmp.path(),
            &bare,
            1,
            std::time::Duration::from_secs(21600),
        );
        let tree = tmp
            .path()
            .join("workspaces")
            .join(cog_core::worktree_task_lane("held"));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = workdir.clone();
        tokio::spawn(async move {
            axum::serve(listener, app_router_with_workdir(serving))
                .await
                .unwrap();
        });

        // The marker is written from inside the tree, so its presence is proof
        // that a command is running there; the claim is taken before the child
        // is even spawned, so it is in place by then.
        //
        // The command outlives the assertions by waiting for a file rather than
        // by sleeping: a fixed span would measure this host's scheduling, not
        // the code, and a busy machine would finish the command between the
        // routes below and the assertion that follows them — the marker would
        // be gone for the innocent reason, and the failure would read as the
        // cap reclaiming a tree out from under a running command. The wait is
        // bounded so that a claim never released still ends the test.
        let running = tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("http://{addr}/execute"))
                .json(&serde_json::json!({
                    "payload": {"type": "command", "command": "touch started; i=0; \
                        while [ $i -lt 120 ]; do [ -f release ] && break; sleep 1; \
                        i=$((i + 1)); done; echo done"},
                    "task_id": "held",
                }))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        for _ in 0..100 {
            if tree.join("started").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(tree.join("started").exists(), "the command never started");

        // Another task against a cap of one: the cap has to take a tree, and
        // the one with a command running in it is not a candidate — so the cap
        // is not met, and the task it could not make room for still runs.
        workdir.route("second").await.unwrap();
        assert!(
            tree.join("started").exists(),
            "the tree a command runs in was reclaimed underneath it"
        );
        assert!(
            tmp.path().join("workspaces").join("second").is_dir(),
            "the cap turned into a refusal: work that does not fit is still work"
        );
        let body = reqwest::Client::new()
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains("sandbox_workspace_cap_deferred_total 1"),
            "the cap could not be met, and that reading has to reach the scrape surface: {body}"
        );

        // The command is over: the tree is an ordinary tree again. Releasing it
        // through the tree is what ends the command, so the span it ran for is
        // however long the assertions above took, and not a number chosen here.
        std::fs::write(tree.join("release"), b"").unwrap();
        let streamed = running.await.unwrap();
        assert!(streamed.contains("done"), "{streamed}");
        // Nothing is deferred any more — the reading says what the cap had to
        // walk past, not what it once did — and the two trees are now ordinary
        // idle trees, so the cap takes both of them. The markers come out first
        // because a tree holding files nobody committed is kept: this test is
        // about the claim, not about that.
        std::fs::remove_file(tree.join("started")).unwrap();
        std::fs::remove_file(tree.join("release")).unwrap();
        workdir.route("fourth").await.unwrap();
        let body = reqwest::Client::new()
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains("sandbox_workspace_cap_deferred_total 1"),
            "a finished command leaves nothing to defer: {body}"
        );
        assert!(
            body.contains("sandbox_workspace_gc_reclaimed_total 2"),
            "the cap did not take the two trees nothing was holding: {body}"
        );
        cog_core::progress::reset_for_tests();
    }

    /// 执行器先报出这条命令落在哪个目录，而那个路径只有它知道。
    ///
    /// 调用方按车道名选树，但车道名不是路径：从名字到目录那一步发生在执行器
    /// 这台机器上。不把结果回带，模型想知道自己在哪就只剩跑一条 `pwd`——用一次
    /// 往返去问一个回复里本来就有的答案。没有车道的调用不许报出一个看着像真的
    /// 路径，所以这里同时钉住「未挂钩时什么都不说」。
    #[tokio::test]
    async fn a_reply_names_the_checkout_it_ran_in() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = crate::workdir::tests::seed_bare(tmp.path());
        let workdir = crate::workdir::tests::router(
            tmp.path(),
            &bare,
            8,
            std::time::Duration::from_secs(21600),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = workdir.clone();
        tokio::spawn(async move {
            axum::serve(listener, app_router_with_workdir(serving))
                .await
                .unwrap();
        });

        let events = events_of(
            post(
                addr,
                serde_json::json!({
                    "payload": {"type": "command", "command": "pwd"},
                    "agent_id": "kai-a9ab11fc",
                }),
            )
            .await,
        )
        .await;
        let lane = cog_core::worktree_lane("kai-a9ab11fc");
        let expected = tmp.path().join("workspaces").join(&lane);
        match events.first() {
            Some(CommandEvent::Workdir { path, .. }) => assert_eq!(
                std::path::Path::new(path),
                expected.as_path(),
                "执行器报的目录不是这条车道的那棵树"
            ),
            other => panic!("回复的第一帧必须报出工作目录：{other:?}"),
        }
        let printed: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stdout { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            printed.trim(),
            expected.to_string_lossy(),
            "pwd 与报出的目录对不上"
        );

        // 无名无任务的调用落在任何树之外：这时「什么都不报」才是真的。
        let events = events_of(
            post(
                addr,
                serde_json::json!({"payload": {"type": "command", "command": "echo hi"}}),
            )
            .await,
        )
        .await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, CommandEvent::Workdir { .. })),
            "没有车道的调用报了工作目录，读者会把它当成一棵真树：{events:?}"
        );
    }

    #[tokio::test]
    async fn server_file_roundtrip() {
        let addr = spawn_server().await;
        let dir = std::env::temp_dir().join(format!("cog-exec-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.txt").to_string_lossy().into_owned();

        let events = events_of(post(
            addr,
            serde_json::json!({"payload": {"type": "write_file", "path": path, "content": "via-server"}}),
        ).await).await;
        assert!(matches!(
            events.last(),
            Some(CommandEvent::Exit { code: 0 })
        ));

        let events = events_of(
            post(
                addr,
                serde_json::json!({"payload": {"type": "read_file", "path": path}}),
            )
            .await,
        )
        .await;
        let content: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stdout { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(content, "via-server");
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
    }

    /// 读不进准入判决：命令把余量占满时，读照样立刻答，也不占一份预留。
    ///
    /// 这条钉住的是「读与写/编译分开计量」的另一半，也是「读改读 agent 侧快照」
    /// 那条路没有走的原因：本机实测（2026-11-18）一条复用的 HTTP 连接上一次
    /// `read_file` 本机实测 0.76 ms，而一份 50 MB 的树打包成 bundle 实测 1.46 s、25 MB——
    /// 快照要比它替代的读贵三个数量级。有人把读塞进 `wait_for_admission`，这里
    /// 就会红：读要么排到窗口尽头，要么留下一份预留。
    #[tokio::test]
    async fn a_read_answers_while_every_byte_of_room_is_reserved() {
        let commands = Arc::new(CommandMetrics::new());
        let pressure = pressure::Ledger::new();
        let held = pressure.reserve(u64::MAX / 4);
        let state = AppState {
            workdir: None,
            hostdocs: None,
            build_lane: BuildLane::new(
                build_concurrency(),
                Arc::clone(&commands),
                Arc::clone(&pressure),
            ),
            commands,
            queue_waits: Arc::new(BuildQueueWindow::new()),
            pressure: Arc::clone(&pressure),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app_router(state)).await.unwrap();
        });

        let dir = std::env::temp_dir().join(format!("cog-exec-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("r.txt").to_string_lossy().into_owned();
        std::fs::write(&path, "answer").unwrap();

        let events = events_of(
            post(
                addr,
                serde_json::json!({"payload": {"type": "read_file", "path": path}}),
            )
            .await,
        )
        .await;
        let content: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stdout { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(content, "answer");
        assert_eq!(pressure.open_count(), 1, "读占了一份预留：读不该进准入判决");
        drop(held);
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
    }

    /// 编辑走的是和读写同一条服务：文件在哪个树上，就在哪里改。
    ///
    /// 两种结局都在这里读：对上了就改一处并说出行号；对上多处就当场拒绝，
    /// 文件一个字节不动——静默挑一处改掉，调用方要到下一次读才发现改错了地方。
    #[tokio::test]
    async fn server_edits_a_file_in_place() {
        let addr = spawn_server().await;
        let dir = std::env::temp_dir().join(format!("cog-exec-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.txt");
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let shown = path.to_string_lossy().into_owned();

        let events = events_of(
            post(
                addr,
                serde_json::json!({"payload": {"type": "edit_file", "path": shown,
                                               "old_string": "two", "new_string": "TWO"}}),
            )
            .await,
        )
        .await;
        assert!(matches!(
            events.last(),
            Some(CommandEvent::Exit { code: 0 })
        ));
        let report: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stdout { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        assert!(report.contains("1 replacement at line 2"), "{report}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "one\nTWO\n");

        std::fs::write(&path, "x\nx\n").unwrap();
        let events = events_of(
            post(
                addr,
                serde_json::json!({"payload": {"type": "edit_file", "path": shown,
                                               "old_string": "x", "new_string": "y"}}),
            )
            .await,
        )
        .await;
        assert!(matches!(
            events.last(),
            Some(CommandEvent::Exit { code: 1 })
        ));
        let reason: String = events
            .iter()
            .filter_map(|e| match e {
                CommandEvent::Stderr { data } => Some(data.as_str()),
                _ => None,
            })
            .collect();
        assert!(reason.contains("appears 2 times"), "{reason}");
        assert!(reason.contains("lines 1, 2"), "{reason}");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "x\nx\n",
            "被拒绝的编辑不能留下半次写入"
        );
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
    }

    #[tokio::test]
    async fn server_rejects_empty_command_and_wasm() {
        let addr = spawn_server().await;
        let response = post(
            addr,
            serde_json::json!({"payload": {"type": "command", "command": "   "}}),
        )
        .await;
        assert_eq!(response.status(), 400);
        let response = post(
            addr,
            serde_json::json!({"payload": {"type": "wasm", "bytes": [], "entry": "main"}}),
        )
        .await;
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn server_health_endpoints() {
        let addr = spawn_server().await;
        for path in ["/health/live", "/health/ready"] {
            let response = reqwest::Client::new()
                .get(format!("http://{}{}", addr, path))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
        }
    }

    async fn spawn_server_with_hostdocs(
        dir: &tempfile::TempDir,
    ) -> (std::net::SocketAddr, std::path::PathBuf) {
        let root = dir.path().join("documents");
        std::fs::create_dir_all(&root).unwrap();
        let cfg = crate::hostdocs::HostDocsConfig::from_spec(
            Some(&format!("alice={}", root.display())),
            dir.path().join("journal"),
            4096,
        );
        let hostdocs = crate::hostdocs::HostDocs::new(cfg).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app_router_with_hostdocs(None, hostdocs))
                .await
                .unwrap();
        });
        (addr, root)
    }

    /// Without a configured scope the routes exist but hand out nothing: the
    /// capability is off, and that state has to be reachable as a refusal
    /// rather than as an empty plan.
    #[tokio::test]
    async fn document_routes_are_refused_when_no_scope_is_configured() {
        let addr = spawn_server().await;
        // One well-formed body per route: a body the route cannot parse would
        // be refused before the capability check, which would prove nothing
        // about the off state.
        let bodies = [
            (
                "/documents/plan",
                serde_json::json!({"scope": "alice", "ops": []}),
            ),
            ("/documents/organize", serde_json::json!({"scope": "alice"})),
            (
                "/documents/stage",
                serde_json::json!({"scope": "alice", "ops": []}),
            ),
            (
                "/documents/approve",
                serde_json::json!({"staged_id": "x", "approver": "y"}),
            ),
            ("/documents/reject", serde_json::json!({"staged_id": "x"})),
            ("/documents/review", serde_json::json!({"staged_id": "x"})),
            (
                "/documents/apply",
                serde_json::json!({
                    "plan": {"scope": "alice", "plan_hash": "x", "ops": []},
                    "approval_id": "x",
                }),
            ),
            (
                "/documents/rollback",
                serde_json::json!({"journal_id": "x"}),
            ),
        ];
        for (path, body) in bodies {
            let response = reqwest::Client::new()
                .post(format!("http://{}{}", addr, path))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 409, "{path}");
            assert!(response.text().await.unwrap().contains("not configured"));
        }
        // The pending list is a bodyless GET, walked separately: it too has to say
        // something clear while the capability is off.
        let listed = reqwest::Client::new()
            .get(format!("http://{}/documents/staged", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(listed.status(), 409);
        assert!(listed.text().await.unwrap().contains("not configured"));
    }

    /// The whole path over HTTP: plan, apply what was planned, then roll back
    /// to the state before, with the refusal of an out-of-scope path in the
    /// same run.
    #[tokio::test]
    async fn document_plan_apply_and_rollback_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (addr, root) = spawn_server_with_hostdocs(&dir).await;
        std::fs::write(root.join("notes.txt"), b"before").unwrap();
        let client = reqwest::Client::new();

        let refused = client
            .post(format!("http://{}/documents/plan", addr))
            .json(&serde_json::json!({
                "scope": "alice",
                "ops": [{"op": "write", "path": "/etc/passwd", "content": "x"}],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 400);
        assert!(refused.text().await.unwrap().contains("absolute path"));

        let plan: serde_json::Value = client
            .post(format!("http://{}/documents/plan", addr))
            .json(&serde_json::json!({
                "scope": "alice",
                "ops": [
                    {"op": "mkdir", "path": "archive"},
                    {"op": "rename", "from": "notes.txt", "to": "archive/notes.txt"},
                    {"op": "write", "path": "archive/notes.txt", "content": "after"},
                ],
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        // Apply before approving: this gate has to really block on the HTTP surface,
        // not only inside the module's unit tests.
        let unapproved = client
            .post(format!("http://{}/documents/apply", addr))
            .json(&serde_json::json!({"plan": plan.clone(), "approval_id": "1700000000000-0-aaaaaaaaaaaa"}))
            .send()
            .await
            .unwrap();
        assert_eq!(unapproved.status(), 400);
        assert!(unapproved.text().await.unwrap().contains("no staged plan"));

        let staged: serde_json::Value = client
            .post(format!("http://{}/documents/stage", addr))
            .json(&serde_json::json!({
                "scope": "alice",
                "ops": [
                    {"op": "mkdir", "path": "archive"},
                    {"op": "rename", "from": "notes.txt", "to": "archive/notes.txt"},
                    {"op": "write", "path": "archive/notes.txt", "content": "after"},
                ],
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(staged["state"], "pending");
        // The reviewer can see which plan is being approved (every operation is
        // there, one by one).
        let reviewed: serde_json::Value = client
            .post(format!("http://{}/documents/review", addr))
            .json(&serde_json::json!({"staged_id": staged["staged_id"]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(reviewed["plan"]["ops"].as_array().unwrap().len(), 3);
        // The listing carries metadata, not bodies.
        let listed: serde_json::Value = client
            .get(format!("http://{}/documents/staged", addr))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(listed["plans"].as_array().unwrap().len(), 1);
        assert!(listed["plans"][0].get("plan").is_none());

        let approved: serde_json::Value = client
            .post(format!("http://{}/documents/approve", addr))
            .json(&serde_json::json!({"staged_id": staged["staged_id"], "approver": "ops"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(approved["state"], "approved");
        assert_eq!(approved["approver"], "ops");

        let applied: serde_json::Value = client
            .post(format!("http://{}/documents/apply", addr))
            .json(&serde_json::json!({
                "plan": plan,
                "approval_id": staged["staged_id"],
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(applied["applied"].as_array().unwrap().len(), 3);
        assert_eq!(
            std::fs::read_to_string(root.join("archive/notes.txt")).unwrap(),
            "after"
        );

        let rolled: serde_json::Value = client
            .post(format!("http://{}/documents/rollback", addr))
            .json(&serde_json::json!({"journal_id": applied["journal_id"]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rolled["restored"], 3);
        assert_eq!(
            std::fs::read_to_string(root.join("notes.txt")).unwrap(),
            "before"
        );
        assert!(!root.join("archive").exists());
    }

    /// The caller-side pipeline over HTTP, end to end: the folder is classified by name,
    /// the plan is staged, and a person approves and applies it.
    ///
    /// The egress switch is off in this harness, and the run working anyway is the point:
    /// sorting by name reads no bodies, so the switch that bounds bodies leaving the
    /// cluster has nothing to say about it. What it does gate is one step earlier than
    /// this test goes.
    #[tokio::test]
    async fn document_organize_stages_a_plan_that_a_person_then_approves() {
        let dir = tempfile::tempdir().unwrap();
        let (addr, root) = spawn_server_with_hostdocs(&dir).await;
        std::fs::write(root.join("notes.md"), b"# notes").unwrap();
        std::fs::write(root.join("photo.JPG"), b"jpeg").unwrap();
        // Nothing in its name says what it is, so it stays where it is -- and the answer
        // has to say so rather than fold it in with the moved files.
        std::fs::write(root.join("IMG_0421"), b"a harbour at dusk").unwrap();
        let client = reqwest::Client::new();

        let organized: serde_json::Value = client
            .post(format!("http://{}/documents/organize", addr))
            .json(&serde_json::json!({"scope": "alice"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(organized["outcome"], "staged");
        assert_eq!(organized["assist"], "switch_off");
        assert_eq!(organized["counts"]["moved_rule"], 2);
        assert_eq!(organized["counts"]["skipped_no_rule"], 1);
        assert_eq!(organized["proposal"]["skipped"]["no_rule"][0], "IMG_0421");
        assert_eq!(organized["staged"]["state"], "pending");
        // Two directories and two moves.
        assert_eq!(organized["staged"]["op_count"], 4);
        // Staging is not doing: the folder is untouched until someone approves.
        assert!(root.join("notes.md").exists());
        assert!(!root.join("documents").exists());

        let refused = client
            .post(format!("http://{}/documents/apply", addr))
            .json(&serde_json::json!({
                "plan": {"scope": "alice", "plan_hash": "x", "ops": []},
                "approval_id": organized["staged"]["staged_id"],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 400);

        let reviewed: serde_json::Value = client
            .post(format!("http://{}/documents/review", addr))
            .json(&serde_json::json!({"staged_id": organized["staged"]["staged_id"]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let plan = reviewed["plan"].clone();
        assert_eq!(plan["ops"].as_array().unwrap().len(), 4);

        client
            .post(format!("http://{}/documents/approve", addr))
            .json(&serde_json::json!({
                "staged_id": organized["staged"]["staged_id"],
                "approver": "ops",
            }))
            .send()
            .await
            .unwrap();
        let applied: serde_json::Value = client
            .post(format!("http://{}/documents/apply", addr))
            .json(&serde_json::json!({
                "plan": plan,
                "approval_id": organized["staged"]["staged_id"],
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(applied["applied"].as_array().unwrap().len(), 4);
        assert_eq!(
            std::fs::read_to_string(root.join("documents/notes.md")).unwrap(),
            "# notes"
        );
        assert!(root.join("images/photo.JPG").exists());
        assert!(root.join("IMG_0421").exists());
    }

    /// The loops this process runs are invisible unless this endpoint publishes
    /// them: the worktree GC, the offline fetch and the volume footprint walk
    /// each keep their last reading when they stop, and that is exactly what a
    /// healthy quiet loop looks like. The census is the only face that says
    /// whether one is still there.
    #[tokio::test]
    async fn metrics_endpoint_carries_the_background_loop_census() {
        // The registry is process-wide, so the probe uses a name of its own and
        // asserts on that rather than on the absence of anything else.
        let probe = format!("census_probe_{}", std::process::id());
        drop(cog_core::loop_health::register(
            probe.clone(),
            cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(30)),
        ));

        let addr = spawn_server().await;
        let body = reqwest::Client::new()
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(
            body.contains(&format!("cogneva_loop_registered{{loop=\"{probe}\"}} 1\n")),
            "the loop census is missing from the scrape: {body}"
        );
        // The period is what an alert compares the age against, so a census
        // without it says a loop exists but not whether it is on time.
        assert!(
            body.contains(&format!(
                "cogneva_loop_period_seconds{{loop=\"{probe}\"}} 30\n"
            )),
            "the loop period is missing from the scrape: {body}"
        );
    }

    /// The reading a latency report starts with, taken at the only boundary
    /// that can attribute the wait to the sandbox.
    ///
    /// One unlabeled histogram over every request would answer "the executor
    /// averages forty seconds" by mixing a compile, a file read and a stream a
    /// caller walked away from -- a number that moves when the traffic mix
    /// moves and says nothing about any one request. So each kind below asserts
    /// its own label pair, and the outcomes are asserted apart: a command that
    /// ran and failed is not a request the executor refused, and neither is a
    /// caller that stopped reading.
    #[tokio::test]
    async fn command_latency_is_recorded_per_tool_and_per_outcome() {
        let addr = spawn_server().await;
        let payload = |p: serde_json::Value| serde_json::json!({ "payload": p });

        // Ran and succeeded.
        events_of(
            post(
                addr,
                payload(serde_json::json!({"type": "command", "command": "true"})),
            )
            .await,
        )
        .await;
        // Ran and returned non-zero: the sandbox did its job.
        events_of(
            post(
                addr,
                payload(serde_json::json!({"type": "command", "command": "exit 7"})),
            )
            .await,
        )
        .await;
        // A file operation that failed, which never enters a command stream.
        events_of(
            post(
                addr,
                payload(serde_json::json!({
                    "type": "read_file",
                    "path": "/nonexistent/cogneva-command-latency-probe",
                })),
            )
            .await,
        )
        .await;
        // Refused before anything ran: an empty command, and a payload this
        // executor does not execute at all. A refusal is a plain body rather
        // than a stream, so it is read as text.
        post(
            addr,
            payload(serde_json::json!({"type": "command", "command": "   "})),
        )
        .await
        .text()
        .await
        .unwrap();
        post(
            addr,
            payload(serde_json::json!({"type": "wasm", "bytes": [], "entry": "main"})),
        )
        .await
        .text()
        .await
        .unwrap();

        let expected = [
            "sandbox_command_seconds_count{outcome=\"ok\",tool=\"run_command\"} 1\n",
            "sandbox_command_seconds_count{outcome=\"failed\",tool=\"run_command\"} 1\n",
            "sandbox_command_seconds_count{outcome=\"failed\",tool=\"read_file\"} 1\n",
            "sandbox_command_seconds_count{outcome=\"refused\",tool=\"run_command\"} 1\n",
            "sandbox_command_seconds_count{outcome=\"refused\",tool=\"wasm\"} 1\n",
            // The gauge has to come back down on its own: it is moved by the
            // same drop that records the duration, and a request whose stream
            // ended without that drop would leave a pod reading as permanently
            // busy.
            "sandbox_commands_in_flight{tool=\"run_command\"} 0\n",
            "sandbox_commands_in_flight{tool=\"read_file\"} 0\n",
        ];
        // Polled rather than read once: the two streaming requests end with
        // the caller's body, which the server drops just after the last frame
        // reaches it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let body = loop {
            let body = reqwest::Client::new()
                .get(format!("http://{}/metrics", addr))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            if expected.iter().all(|line| body.contains(line))
                || std::time::Instant::now() > deadline
            {
                break body;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        for line in expected {
            assert!(
                body.contains(line),
                "the scrape is missing `{line}`; it carried:\n{body}"
            );
        }
    }

    /// The label is a join key: a name only this file knows would leave the
    /// reading impossible to line up with the agent's own tool call. The
    /// spellings below are the registered tool names (`register_builtins`),
    /// not this module's invention.
    #[test]
    fn tool_labels_are_the_names_the_agent_registers() {
        let label = |p: SandboxPayload| tool_label(&p).to_string();
        assert_eq!(
            label(SandboxPayload::Command {
                command: String::new()
            }),
            "run_command"
        );
        assert_eq!(
            label(SandboxPayload::ReadFile {
                path: String::new()
            }),
            "read_file"
        );
        assert_eq!(
            label(SandboxPayload::WriteFile {
                path: String::new(),
                content: String::new()
            }),
            "write_file"
        );
        assert_eq!(
            label(SandboxPayload::EditFile {
                path: String::new(),
                old_string: String::new(),
                new_string: String::new(),
                replace_all: false,
            }),
            "edit_file"
        );
        // Not a registered tool, and deliberately still labeled: the executor
        // refuses these, and a refusal is a request that happened.
        assert_eq!(
            label(SandboxPayload::Wasm {
                bytes: Vec::new(),
                entry: String::new()
            }),
            "wasm"
        );
    }

    /// The variable is process-global; the test that plants a policy name is the
    /// only one reading it while it does.
    static EGRESS_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// 一条声明过的出口策略不是一条被这一层执行的边界：前者是 `declared`，后者
    /// 只有这一层真的会拒时才配叫 `enforced`。没有声明时也不是 `none`——那是
    /// 「这一层不设界」，而这里只是答不出集群里有哪条策略。
    #[test]
    fn a_declared_egress_policy_is_not_read_as_an_enforced_one() {
        let _guard = EGRESS_ENV_LOCK.lock().unwrap();
        std::env::remove_var("SANDBOX_EGRESS_POLICY");
        let unset = boundary_reading(None);
        let network = unset
            .lines
            .iter()
            .find(|line| line.dimension == "网络")
            .expect("four dimensions include the network");
        assert_eq!(network.state, "unknown", "{}", network.detail);

        std::env::set_var("SANDBOX_EGRESS_POLICY", "cogneva-sandbox-executor-egress");
        let declared = boundary_reading(None);
        let network = declared
            .lines
            .iter()
            .find(|line| line.dimension == "网络")
            .expect("four dimensions include the network");
        assert_eq!(network.state, "declared", "{}", network.detail);
        assert!(
            network.detail.contains("cogneva-sandbox-executor-egress"),
            "the reading names the policy the deployment declared: {}",
            network.detail
        );
        std::env::remove_var("SANDBOX_EGRESS_POLICY");
    }

    /// 一条走出一棵树的文件请求在这一层被拒，拒绝里说得出那棵树的根；同一棵树上
    /// 树里的路径照常读。走路由把这条路径真的发出去，而不是只调那个纯函数——这
    /// 一层若不接上它，四行里那句 `enforced` 就是一句没人执行过的话。
    #[tokio::test]
    async fn a_file_request_out_of_the_tree_is_refused_over_the_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = crate::workdir::tests::seed_bare(tmp.path());
        let workdir = crate::workdir::tests::router(
            tmp.path(),
            &bare,
            2,
            std::time::Duration::from_secs(21600),
        );
        let workspaces_root = workdir.config().workspaces_root.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = Arc::clone(&workdir);
        tokio::spawn(async move {
            axum::serve(listener, app_router_with_workdir(serving))
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();

        let refused = client
            .post(format!("http://{addr}/execute"))
            .json(&serde_json::json!({
                "payload": {"type": "read_file", "path": "../../etc/passwd"},
                "task_id": "escape",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), reqwest::StatusCode::BAD_REQUEST);
        let reason = refused.text().await.unwrap();
        let tree = workspaces_root.join(cog_core::worktree_task_lane("escape"));
        assert!(
            reason.contains(&tree.display().to_string()),
            "the refusal names the writable range: {reason}"
        );

        // 同一棵树上，树里的路径照常落到树上：拒绝只对着走出去的那一步。
        let inside = client
            .post(format!("http://{addr}/execute"))
            .json(&serde_json::json!({
                "payload": {"type": "write_file", "path": "notes/inside.txt", "content": "ok"},
                "task_id": "escape",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(inside.status(), reqwest::StatusCode::OK);
        let written = tree.join("notes").join("inside.txt");
        assert!(written.exists(), "a path inside the tree still lands on it");
    }

    /// 执行器答得出自己的边界：四行，且可写路径那一行在有条车道时是 `enforced`
    /// 并说出范围——面板上那句「管住」靠的就是这条读数。
    #[tokio::test]
    async fn the_boundary_route_names_the_writable_range() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = crate::workdir::tests::seed_bare(tmp.path());
        let workdir = crate::workdir::tests::router(
            tmp.path(),
            &bare,
            2,
            std::time::Duration::from_secs(21600),
        );
        let workspaces_root = workdir.config().workspaces_root.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = Arc::clone(&workdir);
        tokio::spawn(async move {
            axum::serve(listener, app_router_with_workdir(serving))
                .await
                .unwrap();
        });

        let reading: cog_core::SandboxBoundary = reqwest::Client::new()
            .get(format!("http://{addr}/boundary"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .expect("the executor answers its boundary as JSON");
        assert_eq!(reading.kind, "executor");
        assert!(reading.isolated);
        assert_eq!(
            reading.workdir.as_deref(),
            Some(workspaces_root.display().to_string().as_str())
        );
        let writable = reading
            .lines
            .iter()
            .find(|line| line.dimension == "可写路径")
            .expect("four dimensions include the writable path");
        assert_eq!(writable.state, "enforced");
        assert!(
            writable
                .detail
                .contains(&workspaces_root.display().to_string()),
            "the writable range names the root a caller is held to: {}",
            writable.detail
        );
        assert_eq!(
            reading.lines.len(),
            4,
            "a missing dimension would read as a boundary that is not there"
        );
    }
    /// 搬运走真路由的两个端点：导出的就是那棵树的内容，导进去以后另一副上
    /// 读得到同一份东西。
    ///
    /// 这条同时钉住请求体上限：一棵真实的工作区远大于 axum 默认的 2 MiB，而那个
    /// 默认值会把每一份像样的包挡成 413——包里装的是三兆的不可压缩字节，能到岸
    /// 就说明这条路由自己把上限关掉了。
    #[tokio::test]
    async fn a_tree_can_be_exported_and_imported_over_http() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let bare_a = crate::workdir::tests::seed_bare(a.path());
        let bare_b = crate::workdir::tests::seed_bare(b.path());
        let ttl = std::time::Duration::from_secs(21600);
        let ra = crate::workdir::tests::router(a.path(), &bare_a, 8, ttl);
        let rb = crate::workdir::tests::router(b.path(), &bare_b, 8, ttl);

        let (tree_a, use_a, _) = ra.route("lane-carry").await.unwrap();
        std::fs::write(tree_a.join("carried.txt"), "hello\n").unwrap();
        // 不可压缩的三兆（xorshift 的字节序列，git 压不动它）：压缩后的包仍在
        // axum 默认的 2 MiB 之上，能到岸就说明这条路由自己把上限关掉了。
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        let big: Vec<u8> = (0..3 * 1024 * 1024)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        std::fs::write(tree_a.join("big.bin"), &big).unwrap();
        drop(use_a);

        let serve = |router: Arc<WorkdirRouter>| async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, app_router_with_workdir(router))
                    .await
                    .unwrap();
            });
            format!("http://{addr}")
        };
        let from = serve(ra.clone()).await;
        let to = serve(rb.clone()).await;

        let client = reqwest::Client::new();
        // 找树那一问的三种答案各认一次：有（200）、没有（404）、名字不能用（400）。
        // 刚起来的客户端靠它决定落在哪副上，三种答案在它眼里是三件事。
        let has_here = client
            .get(format!("{from}/worktree/has"))
            .query(&[("lane", "lane-carry")])
            .send()
            .await
            .unwrap();
        assert_eq!(has_here.status(), reqwest::StatusCode::OK);
        let has_there = client
            .get(format!("{to}/worktree/has"))
            .query(&[("lane", "lane-carry")])
            .send()
            .await
            .unwrap();
        assert_eq!(has_there.status(), reqwest::StatusCode::NOT_FOUND);
        let refused = client
            .get(format!("{from}/worktree/has"))
            .query(&[("lane", "../escape")])
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), reqwest::StatusCode::BAD_REQUEST);
        let exported = client
            .post(format!("{from}/worktree/export"))
            .json(&serde_json::json!({"lane": "lane-carry"}))
            .send()
            .await
            .unwrap();
        assert_eq!(exported.status(), reqwest::StatusCode::OK);
        let bundle = exported.bytes().await.unwrap();
        assert!(!bundle.is_empty(), "导出的包是空的");

        let imported = client
            .post(format!("{to}/worktree/import"))
            .query(&[("lane", "lane-carry")])
            .body(bundle)
            .send()
            .await
            .unwrap();
        assert_eq!(imported.status(), reqwest::StatusCode::OK);
        let answer: serde_json::Value = imported.json().await.unwrap();
        assert_eq!(answer["files"], 2, "写回的是这两处：{answer}");

        let (tree_b, use_b, _) = rb.route("lane-carry").await.unwrap();
        assert_eq!(
            std::fs::read_to_string(tree_b.join("carried.txt")).unwrap(),
            "hello\n"
        );
        assert_eq!(std::fs::read(tree_b.join("big.bin")).unwrap(), big);
        drop(use_b);

        // 那副上没有这条 lane 的树：404，而不是「搬失败了」。
        let nothing = client
            .post(format!("{from}/worktree/export"))
            .json(&serde_json::json!({"lane": "never-routed"}))
            .send()
            .await
            .unwrap();
        assert_eq!(nothing.status(), reqwest::StatusCode::NOT_FOUND);
    }
    /// The two paths share one account: room a running command has reserved
    /// shrinks what the build lane plans, and releasing it gives the lane back.
    /// With a ledger of its own on each side, both would plan against an empty
    /// container and together over-subscribe it.
    #[test]
    fn room_reserved_by_a_running_command_shrinks_what_the_build_lane_plans() {
        const GIB: u64 = 1 << 30;
        let state = state_with_build_slots(4);
        // No declared count: a written one wins over every reading, and this
        // test is about the reading.
        let lane = BuildLane::new(
            None,
            Arc::clone(&state.commands),
            Arc::clone(&state.pressure),
        );
        {
            let mut cost = lane.cost();
            cost.observe(GIB, 0);
            cost.record(
                BuildSample {
                    peak_bytes: Some(2 * GIB),
                    target_bytes: Some(GIB),
                    wall: std::time::Duration::from_secs(60),
                    beside: 1,
                },
                BuildEnd::Completed,
            );
        }
        let facts = CgroupFacts {
            limit_bytes: Some(8 * GIB),
            ..Default::default()
        };
        // 例如：8 GiB 限额、1 GiB 常驻、一条编译 2 GiB——装得下三条，留一条。
        assert_eq!(lane.plan_with(&facts, None).lanes, 2);

        let held = state.pressure.reserve(4 * GIB);
        assert_eq!(
            lane.plan_with(&facts, None).lanes,
            1,
            "the lane planned as if the reserved room were free"
        );
        drop(held);
        assert_eq!(lane.plan_with(&facts, None).lanes, 2);
    }
}
