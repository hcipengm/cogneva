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
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use cog_core::{CommandEvent, SFError, SandboxPayload};
use futures::StreamExt;
use prometheus::{Encoder, HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry, TextEncoder};
use serde::Deserialize;
use tracing::warn;

use crate::hostdocs::{HostDocOp, HostDocPlan, HostDocs};
use crate::workdir::{self, CommandHold, WorkdirRouter};

#[derive(Clone)]
struct AppState {
    /// Per-task worktree router; absent when the executor runs without a
    /// seeded bare repo (tests, embedded usage), keeping legacy cwd semantics.
    workdir: Option<Arc<WorkdirRouter>>,
    /// Host document scopes; absent unless the deployment mounted one, which
    /// keeps the capability off by default rather than one bad path away from
    /// a writable root.
    hostdocs: Option<Arc<HostDocs>>,
    /// How many commands may compile into the shared target directory at once.
    /// See [`crate::runtime::local::builds_the_tree`] for what this bounds and
    /// why a bound is owed at all.
    build_slots: Arc<tokio::sync::Semaphore>,
    /// What each request cost the executor. Owned by the state rather than by
    /// the workdir router, because a deployment without one still runs file
    /// operations, and a latency reading that existed only on some deployments
    /// would be quoted as if it covered all of them.
    commands: Arc<CommandMetrics>,
}

/// How many commands may compile at once when the deployment names none.
///
/// One, because there is one target directory. Cargo takes an exclusive lock on
/// it, so a second slot does not buy a second compiler: it buys a command that
/// is started, is given its wall clock, and then blocks inside cargo's own
/// lockfile wait -- `locks_lock_inode_wait`, no CPU, wall running down --
/// until it is killed having compiled nothing. Measured on the cluster: a
/// clippy parked this way reached its entire 600 s wall while the build in
/// front of it did all the work, and every round queued behind it repeated the
/// same death.
///
/// One slot moves that wait to this semaphore, which is the one place it is
/// free: the wall handed to a command starts when its child starts, and a
/// command waiting for a slot has no child. The commands this bounds all take
/// the same lock, so a single slot costs nothing in throughput -- the twelve
/// commands that once timed out together did so because none of them held a
/// slot at all, not because there was only one slot to hold.
// cap: bound — 排队策略不是容量读数：容量由宿主的装载决定，这个数只决定几条一起等
const DEFAULT_BUILD_CONCURRENCY: usize = 1;

/// The concurrency in force, from `COGNEVA_SANDBOX_BUILD_CONCURRENCY`.
fn build_concurrency() -> usize {
    std::env::var("COGNEVA_SANDBOX_BUILD_CONCURRENCY")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_BUILD_CONCURRENCY)
}

/// A build that waited at least this long for its turn is worth a line.
///
/// Below it are hand-offs between builds that were going to compile anyway;
/// above it is a round that may well have spent its wall waiting.
// cap: bound — 记一笔的阈值，不是容量：它只决定哪些等待值得读，不决定谁等多久
const BUILD_QUEUE_NOTEWORTHY: std::time::Duration = std::time::Duration::from_secs(60);

/// Wall for a command whose caller named none. Long enough for an interactive
/// command to finish, short enough that a caller which stopped listening does not
/// hold the executor; a caller that needs longer asks for it.
// cap: bound — 调用方没给时的一堵墙：交互命令够跑完，调用方不听了也不至于吊住
const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The wall one command runs under: the caller's, or the default when it named
/// none.
///
/// There used to be a second, hardcoded ceiling here (600 s) that clamped every
/// larger request. It was a second source of truth for one number, and it lost
/// silently: an operator who raised `system.shell_timeout_secs` to an hour got
/// ten minutes and a "timed out after 600s" line that named a number appearing
/// nowhere in their configuration. The client's value comes from that one knob,
/// so it is honored as given here.
fn effective_timeout(timeout_ms: Option<u64>) -> std::time::Duration {
    timeout_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or(DEFAULT_TIMEOUT)
}

/// Port the executor listens on when `SANDBOX_EXECUTOR_PORT` names none.
///
/// The executor's Service targets this same port, so it is fixed in code
/// rather than remade per deployment: a missing or unusable variable binds
/// the port the Service already routes to, while the environment stays
/// authoritative when it names a usable port.
// cap: bound — 没有 SANDBOX_EXECUTOR_PORT 时的默认监听口
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
    timeout_ms: Option<u64>,
    task_id: Option<String>,
    agent_id: Option<String>,
}

async fn health() -> &'static str {
    "ok"
}

fn error_response(status: StatusCode, message: String) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(message))
        .expect("static response")
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
}

/// Buckets spanning a file write to a build that runs to its wall.
///
/// Placed where the answers divide rather than on round decades: under a
/// second is file IO, a few seconds is a search or a checkout, tens of seconds
/// is a compile, and the last edge is past the longest wall a caller can ask
/// for, so a command that runs long lands in a bucket instead of leaving every
/// long request to be counted in `+Inf` with the timeouts.
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
        let build_wait = HistogramVec::new(
            HistogramOpts::new(
                "sandbox_build_slot_wait_seconds",
                "How long a compiling command waited for the shared build lane, \
                 by payload kind and whether that lane admitted it",
            )
            .buckets(COMMAND_LATENCY_BUCKETS.to_vec()),
            &["tool", "outcome"],
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
        Self {
            registry,
            seconds,
            in_flight,
            build_wait,
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
    fn observe_build_wait(&self, tool: &str, ended: LaneWait, seconds: f64) {
        self.build_wait
            .with_label_values(&[tool, ended.as_str()])
            .observe(seconds);
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
            let _ = tx.try_send(CommandEvent::Stdout { data: content });
            let _ = tx.try_send(CommandEvent::Exit { code: 0 });
        }
        Err(e) => {
            let _ = tx.try_send(CommandEvent::Stderr {
                data: e.to_string(),
            });
            let _ = tx.try_send(CommandEvent::Exit { code: 1 });
        }
    }
    rx
}

/// Resolve the checkout for a request. A valid lane anchors the command cwd and
/// relative file paths; routing errors fail the request instead of silently
/// falling back to a shared directory. When the router is enabled, every
/// command additionally receives the shared CARGO_TARGET_DIR.
///
/// The tree belongs to the employee, so the agent id decides it and the task id
/// does not. Routing by task id put every role of one task in one checkout --
/// a planner's reads, a generator's writes and an evaluator's runs sharing one
/// working tree -- and it made the tree's lifetime the task's, so a finished
/// task took the files with it while the person was still here. The task id is
/// still read, but only for a request that named no employee: that call has no
/// lane of its own and something has to name it.
async fn resolve_workdir(
    state: &AppState,
    task_id: Option<&str>,
    agent_id: Option<&str>,
) -> Result<(Option<PathBuf>, Option<PathBuf>, Option<CommandHold>), Box<Response>> {
    let Some(router) = state.workdir.as_ref() else {
        return Ok((None, None, None));
    };
    let from_employee = agent_id.and_then(workdir::worktree_for_agent);
    let lane = from_employee.or_else(|| {
        task_id
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
    });
    match lane {
        // The claim travels with the request and is handed to whatever runs the
        // command: the tree is in use from here until that command is over, and
        // a reclamation pass that only knows `last_used_unix` cannot tell the
        // difference on its own.
        Some(lane) => match router.route(&lane).await {
            Ok((dir, claim)) => Ok((
                Some(dir),
                Some(router.target_dir().to_path_buf()),
                Some(router.command_hold(Some(claim)).await),
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
            ))
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
    let timeout = effective_timeout(req.timeout_ms);
    let (workdir, cargo_target, worktree_claim) =
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
            // A command that compiles waits its turn here, before its child
            // exists. The wall handed to the child starts when the child does,
            // so time spent queued is not time spent compiling: without this,
            // every extra build started beside it shortened the wall the others
            // had left, and each of them timed out having read nothing.
            let build_slot =
                match build_slot_for(&state, tool_label(&req.payload), command, timeout).await {
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
            match crate::runtime::local::spawn_command(
                command,
                timeout,
                workdir.as_deref(),
                cargo_target.as_deref(),
                worktree_claim,
                build_slot,
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
            let anchored = workdir::anchor_path(workdir.as_deref(), path);
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
            let anchored = workdir::anchor_path(workdir.as_deref(), path);
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
            let anchored = workdir::anchor_path(workdir.as_deref(), path);
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
    // The memory ceiling belongs on the same surface as the worktree counters:
    // it is the other half of "why did this build die", and it applies even
    // where no workdir router was provisioned.
    let mut body = state
        .workdir
        .as_ref()
        .map(|r| r.metrics().render())
        .unwrap_or_default();
    // The build cache this executor's commands write into. Rendered from the
    // readings rather than from the prometheus registry the counters above live
    // in, because the same family is served by the other process that owns a
    // cache and both sides go through one renderer -- see `crate::build_cache`.
    if let Some(router) = state.workdir.as_ref() {
        body.push_str(&crate::build_cache::render(router.build_cache()).await);
    }
    body.push_str(&crate::runtime::cgroup::render());
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

/// Router without per-task worktrees (tests and any no-volume deployment).
/// The place this command holds among the builds allowed to compile at once.
///
/// `Ok(None)` is a command that compiles nothing: it takes no slot, because
/// holding one for a search would put every search behind a build.
async fn build_slot_for(
    state: &AppState,
    tool: &str,
    command: &str,
    budget: std::time::Duration,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, LaneRefusal> {
    if !crate::runtime::local::builds_the_tree(command) {
        return Ok(None);
    }
    let queued_at = std::time::Instant::now();
    // The queue may cost the caller at most the wall this command was given.
    // Unbounded, a wait is not a wait but a hang: measured on this host
    // 2026-11-08, compiles queued 190 to 1145 seconds behind one held slot, and
    // the caller learned nothing until its own tool call expired -- it could
    // not tell a busy machine from a slow compiler, so it retried into the same
    // queue. Refused with the number, it can see that the command never ran.
    //
    // The budget is the command's own wall rather than a number of its own: the
    // caller already declared how long it is willing to spend here, and the
    // worst case stays two walls -- one queued, one running.
    let acquired =
        tokio::time::timeout(budget, Arc::clone(&state.build_slots).acquire_owned()).await;
    let waited = queued_at.elapsed();
    let ended = match &acquired {
        Ok(Ok(_)) => LaneWait::Admitted,
        Ok(Err(_)) => LaneWait::Closed,
        Err(_) => LaneWait::Full,
    };
    state
        .commands
        .observe_build_wait(tool, ended, waited.as_secs_f64());
    // It is still a wait somebody pays for, so it is said out loud as well: a
    // round that spent its whole wall queued and one that compiled slowly read
    // the same on the latency histogram alone.
    if waited >= BUILD_QUEUE_NOTEWORTHY {
        tracing::info!(
            command = %command,
            waited_secs = waited.as_secs(),
            waited_ended = ended.as_str(),
            "a build waited its turn for the shared target directory"
        );
    }
    match acquired {
        Ok(Ok(permit)) => Ok(Some(permit)),
        Ok(Err(e)) => Err(LaneRefusal::Closed(e.to_string())),
        Err(_) => Err(LaneRefusal::Full { waited, budget }),
    }
}

/// Why a compiling command never reached its child.
#[derive(Debug)]
enum LaneRefusal {
    /// The lane stayed full for the whole wall the caller allotted the command.
    Full {
        waited: std::time::Duration,
        budget: std::time::Duration,
    },
    /// The semaphore is closed; no amount of waiting admits this command.
    Closed(String),
}

impl std::fmt::Display for LaneRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneRefusal::Full { waited, budget } => write!(
                f,
                "build lane busy: this command never started. It waited {}s of the {}s \
                 its own wall allows for a slot among the commands compiling into the \
                 shared target directory, and did not get one. Nothing was compiled and \
                 nothing was left behind; ask again later, or give the command a longer \
                 timeout_ms so it may queue for longer than it may run.",
                waited.as_secs(),
                budget.as_secs()
            ),
            LaneRefusal::Closed(e) => write!(f, "build slot unavailable: {e}"),
        }
    }
}

pub fn router() -> Router {
    app_router(AppState {
        workdir: None,
        hostdocs: None,
        build_slots: Arc::new(tokio::sync::Semaphore::new(build_concurrency())),
        commands: Arc::new(CommandMetrics::new()),
    })
}

/// Full router wiring a provisioned workdir router.
pub fn app_router_with_workdir(workdir: Arc<WorkdirRouter>) -> Router {
    app_router(AppState {
        workdir: Some(workdir),
        hostdocs: None,
        build_slots: Arc::new(tokio::sync::Semaphore::new(build_concurrency())),
        commands: Arc::new(CommandMetrics::new()),
    })
}

/// Full router with host document scopes enabled.
pub fn app_router_with_hostdocs(
    workdir: Option<Arc<WorkdirRouter>>,
    hostdocs: Arc<HostDocs>,
) -> Router {
    app_router(AppState {
        workdir,
        hostdocs: Some(hostdocs),
        build_slots: Arc::new(tokio::sync::Semaphore::new(build_concurrency())),
        commands: Arc::new(CommandMetrics::new()),
    })
}

fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/metrics", get(metrics_handler))
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
        .with_state(state)
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
    let app = app_router(AppState {
        workdir,
        hostdocs,
        build_slots: Arc::new(tokio::sync::Semaphore::new(build_concurrency())),
        commands: Arc::new(CommandMetrics::new()),
    });

    tracing::info!(addr = %addr, "sandbox executor listening");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
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

    /// 调用方要多久就是多久：这一层不再有自己的硬顶。
    ///
    /// 600 秒那道旧硬顶是同一个数的第二个来源，而且它悄悄生效——把
    /// `system.shell_timeout_secs` 抬到一小时的人，拿到的是十分钟和一句
    /// "timed out after 600s"，那个数在他的配置里根本不存在。这条判据钉的是
    /// 抬上去的那一格真的到了执行器。
    #[test]
    fn a_longer_wall_than_six_hundred_seconds_is_honored() {
        assert_eq!(
            effective_timeout(Some(3_600_000)),
            std::time::Duration::from_secs(3600),
            "执行器把调用方要的一小时截回了旧的上限"
        );
        assert_eq!(
            effective_timeout(Some(200_000)),
            std::time::Duration::from_secs(200)
        );
    }

    /// 没写就是那堵 30 秒的墙：调用方不听了，命令不该把执行器吊住。
    #[test]
    fn a_caller_that_names_no_wall_gets_the_short_one() {
        assert_eq!(effective_timeout(None), DEFAULT_TIMEOUT);
        assert_eq!(DEFAULT_TIMEOUT, std::time::Duration::from_secs(30));
    }

    /// The default has to be the number of writers the target directory admits,
    /// and that number is one: cargo takes an exclusive lock on it. A second
    /// slot does not start a second compiler -- it starts a command whose wall
    /// clock runs while it waits inside cargo for a lock it cannot get, and
    /// that command is killed at its wall having compiled nothing.
    #[test]
    fn the_default_leaves_one_writer_for_the_one_target_directory() {
        let _guard = BUILD_ENV_LOCK.lock().unwrap();
        std::env::remove_var("COGNEVA_SANDBOX_BUILD_CONCURRENCY");
        assert_eq!(
            build_concurrency(),
            1,
            "a second slot is a command started only to be killed"
        );
    }

    fn state_with_build_slots(n: usize) -> AppState {
        AppState {
            workdir: None,
            hostdocs: None,
            build_slots: Arc::new(tokio::sync::Semaphore::new(n)),
            commands: Arc::new(CommandMetrics::new()),
        }
    }

    /// The bound this exists for: a second build does not start while the
    /// first one holds the last slot, a command that compiles nothing is never
    /// made to wait for one, and a wait that outlasts the command's own wall is
    /// refused by name instead of hanging until the caller gives up.
    #[tokio::test]
    async fn a_build_waits_its_turn_and_a_search_does_not() {
        let state = state_with_build_slots(1);
        let patience = std::time::Duration::from_secs(60);
        let held = build_slot_for(
            &state,
            "run_command",
            "cargo test -p cogneva --lib",
            patience,
        )
        .await
        .expect("the semaphore is open")
        .expect("a build takes a slot");

        let queued = tokio::time::timeout(std::time::Duration::from_millis(80), async {
            build_slot_for(
                &state,
                "run_command",
                "cargo clippy --all-targets",
                patience,
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
            std::time::Duration::from_millis(80),
            build_slot_for(&state, "run_command", "grep -rn cargo crates/", patience),
        )
        .await
        .expect("a search never queues")
        .expect("the semaphore is open");
        assert!(search.is_none(), "a search holds no build slot");

        // Formatting the tree is the same shape: a program that would compile,
        // asked to do something that writes nothing into the target directory.
        let formatting = tokio::time::timeout(
            std::time::Duration::from_millis(80),
            build_slot_for(
                &state,
                "run_command",
                "cargo fmt --all -- --check",
                patience,
            ),
        )
        .await
        .expect("formatting never queues")
        .expect("the semaphore is open");
        assert!(formatting.is_none(), "formatting holds no build slot");

        // A wait longer than the wall the command was given is a refusal that
        // names both numbers, not a command started only to be killed.
        let refused = build_slot_for(
            &state,
            "run_command",
            "cargo clippy --all-targets",
            std::time::Duration::from_millis(120),
        )
        .await;
        match refused {
            Err(LaneRefusal::Full { waited, budget }) => {
                assert!(waited >= budget, "the wait is the whole budget and more");
                assert!(
                    LaneRefusal::Full { waited, budget }
                        .to_string()
                        .contains("never started"),
                    "the refusal says the command did not run"
                );
            }
            Ok(_) => panic!("a full lane past the budget must refuse"),
            Err(LaneRefusal::Closed(e)) => panic!("the semaphore was closed: {e}"),
        }

        drop(held);
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            build_slot_for(
                &state,
                "run_command",
                "cargo clippy --all-targets",
                patience,
            ),
        )
        .await
        .expect("the released slot admits the waiting build")
        .expect("the semaphore is open");
        assert!(next.is_some(), "the queued build gets the slot back");

        // The wait is published, so a round that queued and a round that
        // compiled slowly are separable without reading the log.
        let rendered = state.commands.render();
        assert!(
            rendered.contains(
                "sandbox_build_slot_wait_seconds_count{outcome=\"admitted\",tool=\"run_command\"}"
            ),
            "the wait is a reading of its own: {rendered}"
        );
        assert!(
            rendered.contains(
                "sandbox_build_slot_wait_seconds_count{outcome=\"full\",tool=\"run_command\"}"
            ),
            "the reading separates the lane admitting a command from refusing one: {rendered}"
        );
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
        let tmp = tempfile::tempdir().unwrap();
        let bare = crate::workdir::tests::seed_bare(tmp.path());
        let workdir = crate::workdir::tests::router(
            tmp.path(),
            &bare,
            2,
            std::time::Duration::from_secs(21600),
        );
        let tree = tmp.path().join("workspaces").join("held");

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

        // Two more tasks against a cap of two: the cap has to take a tree, and
        // the one with a command running in it is not a candidate.
        workdir.route("second").await.unwrap();
        workdir.route("third").await.unwrap();
        assert!(
            tree.join("started").exists(),
            "the tree a command runs in was reclaimed underneath it"
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
            Some(CommandEvent::Workdir { path }) => assert_eq!(
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
        let _ = std::fs::remove_dir_all(&dir);
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
        let _ = std::fs::remove_dir_all(&dir);
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
}
