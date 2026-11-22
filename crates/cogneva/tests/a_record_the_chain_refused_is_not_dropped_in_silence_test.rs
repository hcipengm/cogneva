//! A record the audit chain refused is not dropped in silence.
//!
//! The platform keeps its own chain of what it did: a denial, a creation, a
//! save. Routes append to it after doing the thing, and the append's result was
//! thrown away -- `let _ = stream.append(...)` -- so a chain that refused the
//! write left no trace anywhere. That is the one failure this chain cannot
//! afford: a missing record and a thing that never happened read the same when
//! somebody audits the platform afterwards.
//!
//! The suite below hands the routes a chain that refuses every append and reads
//! the log the process emitted. The action itself still succeeds -- the chain is
//! a record of what happened, not a gate in front of it -- but the refusal has
//! to be nameable: which action, which target, what the chain said.

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

use cog_core::{AuditEvent, AuditKind, AuditStream, SFError, SFResult};

use crate::router_state;

/// An audit chain that refuses every record. Reads answer an empty chain, so a
/// suite that only asks "is it empty" is not fooled by this fake.
struct RefusesRecords;

#[async_trait::async_trait]
impl AuditStream for RefusesRecords {
    async fn append(
        &self,
        _kind: AuditKind,
        _actor: &str,
        _target: &str,
        _action: &str,
        _detail: Value,
    ) -> SFResult<AuditEvent> {
        Err(SFError::Database(
            "the audit chain is read-only right now".into(),
        ))
    }

    async fn read_all(&self) -> SFResult<Vec<AuditEvent>> {
        Ok(Vec::new())
    }
}

/// One emitted event, reduced to the fields a refusal record is made of.
#[derive(Clone, Debug)]
struct Recorded {
    message: String,
    action: Option<String>,
    target: Option<String>,
    error: Option<String>,
}

#[derive(Default)]
struct PickFields {
    message: Option<String>,
    action: Option<String>,
    target: Option<String>,
    error: Option<String>,
}

impl PickFields {
    fn set(&mut self, name: &str, plain: String) {
        let slot = match name {
            "message" => &mut self.message,
            "action" => &mut self.action,
            "target" => &mut self.target,
            "error" => &mut self.error,
            _ => return,
        };
        *slot = Some(plain);
    }
}

impl Visit for PickFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%x` reaches a visitor through `record_debug` with the value still
        // debug-quoted, so the quotes come off here.
        let text = format!("{value:?}");
        let plain = text
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .map(str::to_string)
            .unwrap_or(text);
        self.set(field.name(), plain);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field.name(), value.to_string());
    }
}

struct Capture {
    events: Arc<Mutex<Vec<Recorded>>>,
}

impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut pick = PickFields::default();
        event.record(&mut pick);
        self.events.lock().unwrap().push(Recorded {
            message: pick.message.unwrap_or_default(),
            action: pick.action,
            target: pick.target,
            error: pick.error,
        });
    }
}

/// Drive one request with a capturing subscriber installed for its whole life.
///
/// The subscriber is thread-local, so the request runs on a current-thread
/// runtime inside the same closure: that is what makes the handler's own
/// `warn!` land in the capture.
fn with_capture<F>(future: F) -> (StatusCode, String, Vec<Recorded>)
where
    F: std::future::Future<Output = (StatusCode, String)>,
{
    let events: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
        events: events.clone(),
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the current-thread runtime builds");
    let (status, body) =
        tracing::subscriber::with_default(subscriber, move || runtime.block_on(future));
    let events = events.lock().unwrap().clone();
    (status, body, events)
}

async fn create_project(
    state: Arc<cog_gateway::GatewayState>,
    token: &str,
    workspace: &str,
) -> (StatusCode, String) {
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/projects")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "name": "a project", "workspace_id": workspace })
                        .to_string(),
                ))
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// 建项目：项目建成了，审计链没记上，日志里必须说得出是哪一条没记上。
#[test]
fn a_creation_the_chain_refused_is_named_in_the_log() {
    let state = router_state::with_audit_stream(Arc::new(RefusesRecords));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the runtime builds");
    let token = runtime.block_on(router_state::admin_bearer(&state, "audit-probe"));
    let (status, body, events) = with_capture(create_project(state, &token, "tenant-a"));

    assert_eq!(
        status,
        StatusCode::CREATED,
        "审计链拒了这次追加，但项目本身建成了：{body}"
    );
    let named = events
        .iter()
        .find(|e| e.action.as_deref() == Some("project.created"))
        .unwrap_or_else(|| panic!("这一条没记进审计链，日志里也没有它的名字：{events:#?}"));
    assert!(
        named
            .error
            .as_deref()
            .is_some_and(|e| e.contains("read-only")),
        "只说「没记上」说不出链子回了什么，排查的人还得再去猜一遍：{named:#?}"
    );
    assert!(
        !named.target.as_deref().unwrap_or_default().is_empty(),
        "说不出是哪一条没记上：{named:#?}"
    );
    assert!(
        !named.message.is_empty(),
        "这条日志没有正文，读的人只看得到一个字段：{named:#?}"
    );
}

/// 拒掉一次越权请求：拒绝发生了，审计链没记上，日志里必须说得出是哪一条。
#[test]
fn a_denial_the_chain_refused_is_named_in_the_log() {
    let state = router_state::with_audit_stream(Arc::new(RefusesRecords));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the runtime builds");
    let token = runtime.block_on(router_state::admin_bearer_scoped_to(
        &state,
        "audit-probe",
        &["tenant-b"],
    ));
    let (status, body, events) = with_capture(create_project(state, &token, "tenant-a"));

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "越权的写本来就该被拒，这里的判据是拒绝之后的记录：{body}"
    );
    events
        .iter()
        .find(|e| e.action.as_deref() == Some("authz.workspace_denied"))
        .unwrap_or_else(|| panic!("这次拒绝没记进审计链，日志里也没有它的名字：{events:#?}"));
}
