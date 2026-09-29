//! Standalone sandbox executor service (`cogneva sandbox-executor`).
//!
//! Executes environment payloads (shell commands, file reads/writes) for the
//! cluster's tool layer and streams output back as NDJSON
//! [`cog_core::CommandEvent`] lines. The pod running this service mounts no
//! secrets and holds no credentials — isolation is the pod boundary, so
//! commands may use full shell syntax by design.

use std::path::PathBuf;
use std::sync::Arc;

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
use serde::Deserialize;
use tracing::warn;

use crate::hostdocs::{HostDocOp, HostDocPlan, HostDocs};
use crate::workdir::{self, WorkdirRouter, WorktreeUse};

#[derive(Clone)]
struct AppState {
    /// Per-task worktree router; absent when the executor runs without a
    /// seeded bare repo (tests, embedded usage), keeping legacy cwd semantics.
    workdir: Option<Arc<WorkdirRouter>>,
    /// Host document scopes; absent unless the deployment mounted one, which
    /// keeps the capability off by default rather than one bad path away from
    /// a writable root.
    hostdocs: Option<Arc<HostDocs>>,
}

/// Hard ceiling on command duration regardless of what the client asks for.
const MAX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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

fn stream_response(rx: tokio::sync::mpsc::Receiver<CommandEvent>) -> Response {
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|event| (event, rx))
    })
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

/// Resolve the task worktree for a request. A valid task id anchors the
/// command cwd and relative file paths; routing errors fail the request
/// instead of silently falling back to a shared directory. When the router is
/// enabled, every command additionally receives the shared CARGO_TARGET_DIR.
async fn resolve_workdir(
    state: &AppState,
    task_id: Option<&str>,
) -> Result<(Option<PathBuf>, Option<PathBuf>, Option<WorktreeUse>), Box<Response>> {
    let Some(router) = state.workdir.as_ref() else {
        return Ok((None, None, None));
    };
    match task_id.filter(|id| !id.trim().is_empty()) {
        // The claim travels with the request and is handed to whatever runs the
        // command: the tree is in use from here until that command is over, and
        // a reclamation pass that only knows `last_used_unix` cannot tell the
        // difference on its own.
        Some(id) => match router.route(id).await {
            Ok((dir, claim)) => Ok((
                Some(dir),
                Some(router.target_dir().to_path_buf()),
                Some(claim),
            )),
            Err(e) => {
                router.metrics().inc_error("route");
                Err(Box::new(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("task worktree unavailable: {e}"),
                )))
            }
        },
        None => {
            // The business side must stamp identity (require_tool_identity);
            // an unstamped request still runs for compatibility, but outside
            // any task tree and is counted for observability.
            router.metrics().inc_unscoped();
            tracing::warn!("sandbox request without task id; running outside per-task worktree");
            Ok((None, Some(router.target_dir().to_path_buf()), None))
        }
    }
}

async fn execute_handler(
    State(state): State<AppState>,
    Json(req): Json<ExecuteRequest>,
) -> Response {
    let timeout = req
        .timeout_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or(DEFAULT_TIMEOUT)
        .min(MAX_TIMEOUT);
    let (workdir, cargo_target, worktree_claim) =
        match resolve_workdir(&state, req.task_id.as_deref()).await {
            Ok(resolved) => resolved,
            Err(resp) => return *resp,
        };
    match req.payload {
        SandboxPayload::Command { ref command } => {
            if command.trim().is_empty() {
                return error_response(StatusCode::BAD_REQUEST, "empty command".into());
            }
            tracing::info!(
                task_id = req.task_id.as_deref().unwrap_or(""),
                agent_id = req.agent_id.as_deref().unwrap_or(""),
                workdir = ?workdir,
                command = %command,
                "sandbox executor running command"
            );
            match crate::runtime::local::spawn_command(
                command,
                timeout,
                workdir.as_deref(),
                cargo_target.as_deref(),
                worktree_claim,
            ) {
                Ok(rx) => stream_response(rx),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
        SandboxPayload::ReadFile { ref path } => {
            let anchored = workdir::anchor_path(workdir.as_deref(), path);
            let result = tokio::fs::read_to_string(&anchored).await;
            stream_response(single_shot(result))
        }
        SandboxPayload::WriteFile {
            ref path,
            ref content,
        } => {
            let anchored = workdir::anchor_path(workdir.as_deref(), path);
            if let Some(parent) = anchored.parent() {
                if !tokio::fs::try_exists(parent).await.unwrap_or(false) {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        return stream_response(single_shot(Err(e)));
                    }
                }
            }
            let result = tokio::fs::write(&anchored, content)
                .await
                .map(|_| String::new());
            stream_response(single_shot(result))
        }
        SandboxPayload::Wasm { .. } => error_response(
            StatusCode::BAD_REQUEST,
            "executor does not run WASM payloads".into(),
        ),
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
    body.push_str(&crate::runtime::cgroup::render());
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
pub fn router() -> Router {
    app_router(AppState {
        workdir: None,
        hostdocs: None,
    })
}

/// Full router wiring a provisioned workdir router.
pub fn app_router_with_workdir(workdir: Arc<WorkdirRouter>) -> Router {
    app_router(AppState {
        workdir: Some(workdir),
        hostdocs: None,
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
/// Port from `SANDBOX_EXECUTOR_PORT`, default 9090.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // 独立子命令不经 run_app，需自行初始化日志，否则请求审计行不落地。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let port: u16 = std::env::var("SANDBOX_EXECUTOR_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
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
    let app = app_router(AppState { workdir, hostdocs });

    tracing::info!(addr = %addr, "sandbox executor listening");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let running = tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("http://{addr}/execute"))
                .json(&serde_json::json!({
                    "payload": {"type": "command", "command": "touch started; sleep 3; echo done"},
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

        // The command is over: the tree is an ordinary tree again.
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
}
