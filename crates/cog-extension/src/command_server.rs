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

use crate::hostdocs::{HostDocOp, HostDocPlan, HostDocs};
use crate::workdir::{self, WorkdirRouter};

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
) -> Result<(Option<PathBuf>, Option<PathBuf>), Box<Response>> {
    let Some(router) = state.workdir.as_ref() else {
        return Ok((None, None));
    };
    match task_id.filter(|id| !id.trim().is_empty()) {
        Some(id) => match router.route(id).await {
            Ok(dir) => Ok((Some(dir), Some(router.target_dir().to_path_buf()))),
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
            Ok((None, Some(router.target_dir().to_path_buf())))
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
    let (workdir, cargo_target) = match resolve_workdir(&state, req.task_id.as_deref()).await {
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
        .route("/documents/plan", post(documents_plan_handler))
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
struct DocumentsRollbackRequest {
    journal_id: String,
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

async fn documents_apply_handler(
    State(state): State<AppState>,
    Json(plan): Json<HostDocPlan>,
) -> Response {
    let Some(hostdocs) = state.hostdocs.as_ref() else {
        return documents_disabled();
    };
    match hostdocs.apply(&plan).await {
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
            (
                "/documents/apply",
                serde_json::json!({"scope": "alice", "plan_hash": "x", "ops": []}),
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
        let applied: serde_json::Value = client
            .post(format!("http://{}/documents/apply", addr))
            .json(&plan)
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
}
