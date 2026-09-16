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
    response::Response,
    routing::{get, post},
    Json, Router,
};
use cog_core::{CommandEvent, SandboxPayload};
use futures::StreamExt;
use serde::Deserialize;

use crate::workdir::{self, WorkdirRouter};

#[derive(Clone)]
struct AppState {
    /// Per-task worktree router; absent when the executor runs without a
    /// seeded bare repo (tests, embedded usage), keeping legacy cwd semantics.
    workdir: Option<Arc<WorkdirRouter>>,
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
    let body = state
        .workdir
        .as_ref()
        .map(|r| r.metrics().render())
        .unwrap_or_default();
    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("static response")
}

/// Router without per-task worktrees (tests and any no-volume deployment).
pub fn router() -> Router {
    app_router(AppState { workdir: None })
}

/// Full router wiring a provisioned workdir router.
pub fn app_router_with_workdir(workdir: Arc<WorkdirRouter>) -> Router {
    app_router(AppState {
        workdir: Some(workdir),
    })
}

fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/execute", post(execute_handler))
        .with_state(state)
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

    // With a provisioned volume the executor seeds its own bare repo and routes
    // every stamped request into a per-task worktree; without it the legacy
    // process-cwd behaviour stays intact for embedded and test usage.
    let app = match WorkdirRouter::from_env().await {
        Some(workdir) => {
            if let Err(e) = workdir.recover().await {
                tracing::error!(error = %e, "workdir recovery failed; serving without router");
                router()
            } else {
                workdir.fetch_once().await;
                workdir.spawn_maintenance();
                tracing::info!(
                    workspaces = %workdir.config().workspaces_root.display(),
                    bare = %workdir.config().bare_repo.display(),
                    "per-task workdir router enabled"
                );
                app_router_with_workdir(workdir)
            }
        }
        None => router(),
    };

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
}
