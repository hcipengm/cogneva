//! 一个人只有一个 id：换岗位、换机器都不换。
//!
//! 对外的 `POST /api/v1/agents/register` 原来把 `hostname|pod_ip|role|uuid`
//! 一起哈希成 id，没有 uuid 时还现发一个随机的。两处都让「人」不连续：
//! role 一变就是新 id，名下记着的活、账、提交都留在旧 id 上；随机 uuid 更是
//! 每重装一次就在名册上多一个从没存在过的人。R16 第 4 步的判据就两条：
//!
//! 1. id 只由稳定身份（客户端自报的 uuid）派生，role／hostname／pod_ip 只是
//!    这一次注册行上的可变事实；
//! 2. 不自报稳定身份就拒绝登记，不再替它发明一个。
//!
//! 第三条是普查：派生只有一份实现。原先 `cog-storage` 里另有一份同函数拷贝，
//! 两处各改一次才一致；只要其中一处漏改，两个 crate 对「同一个人」就给出两个
//! id，而这种分家在任何单 crate 的用例里都看不出来。

use crate::common;

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{AgentRegistration, AgentRegistry, MessageBackend, SFResult};
use cog_gateway::{create_router, GatewayState};

use crate::router_state;

/// 一份会记下每一次登记的名册。
struct RecordingRoster {
    rows: Mutex<Vec<AgentRegistration>>,
}

impl RecordingRoster {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            rows: Mutex::new(Vec::new()),
        })
    }

    fn rows(&self) -> Vec<AgentRegistration> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[async_trait::async_trait]
impl AgentRegistry for RecordingRoster {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(registration.clone());
        Ok(())
    }
    async fn heartbeat(&self, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }
    async fn deregister(&self, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }
    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        Ok(self.rows().into_iter().find(|r| r.agent_id == agent_id))
    }
    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        Ok(self.rows())
    }
    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(self.rows().into_iter().filter(|r| r.role == role).collect())
    }
}

fn state(roster: Arc<RecordingRoster>) -> Arc<GatewayState> {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    router_state::with_messaging(backend, roster)
}

async fn register(
    app: axum::Router,
    state: &Arc<GatewayState>,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let token = router_state::admin_bearer(state, "registrar").await;
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/agents/register")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("the request is well formed");
    let response = app.oneshot(request).await.expect("the router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body comes back");
    let parsed = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, parsed)
}

fn row(uuid: Option<&str>, role: &str, hostname: &str, pod_ip: &str) -> serde_json::Value {
    let mut body = serde_json::json!({
        "hostname": hostname,
        "pod_ip": pod_ip,
        "role": role,
        "workspace_id": "ws-1",
    });
    if let Some(uuid) = uuid {
        body["uuid"] = serde_json::json!(uuid);
    }
    body
}

#[tokio::test]
async fn the_same_person_keeps_one_id_whatever_their_post() {
    let roster = RecordingRoster::new();
    let state = state(roster.clone());
    let app = create_router(state.clone());

    let (status, first) = register(
        app.clone(),
        &state,
        row(Some("worker-7f3a"), "planner", "host-a", "10.0.0.1"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "第一次登记成功（{first}）");
    let id_before = first["agent_id"]
        .as_str()
        .expect("an id comes back")
        .to_string();
    assert_eq!(
        id_before,
        cog_core::agent_id_for_identity("worker-7f3a"),
        "id 只由稳定身份派生"
    );

    // 同一个人换了岗位、换了机器、换了 Pod IP，仍然是同一个人。
    let (status, second) = register(
        app,
        &state,
        row(Some("worker-7f3a"), "evaluator", "host-b", "10.0.0.9"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "第二次登记成功（{second}）");
    assert_eq!(
        second["agent_id"].as_str(),
        Some(id_before.as_str()),
        "换岗位、换机器不改 id：改了的话名下的事留在旧人身上"
    );
    assert_eq!(
        second["registration"]["role"].as_str(),
        Some("evaluator"),
        "role 留在注册行上，跟着这一次注册走"
    );
    assert_eq!(
        second["registration"]["pod_ip"].as_str(),
        Some("10.0.0.9"),
        "pod_ip 也是注册行上的可变事实"
    );

    let rows = roster.rows();
    assert_eq!(rows.len(), 2, "两次登记都写进了名册");
    assert!(rows.iter().all(|r| r.agent_id == id_before));
}

#[tokio::test]
async fn a_registration_without_a_stable_identity_is_refused() {
    let roster = RecordingRoster::new();
    let state = state(roster.clone());
    let app = create_router(state.clone());

    for missing in [None, Some(""), Some("   ")] {
        let (status, body) = register(
            app.clone(),
            &state,
            row(missing, "planner", "host-a", "10.0.0.1"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "没有稳定身份（uuid={missing:?}）就拒绝登记，不再随机发一个「人」（{body}）"
        );
    }
    assert!(roster.rows().is_empty(), "被拒的登记不许在名册上留下人");
}

/// 派生只有一份实现，而且它的输入只有稳定身份。
#[test]
fn the_id_is_derived_once_from_identity_alone() {
    let root = common::root::repo_root();
    let sources = common::rust_production::production_sources(&root);
    assert!(
        sources.len() >= common::rust_production::MIN_PRODUCTION_FILES,
        "普查读到的生产源文件太少（{}），这条判据什么都没量到",
        sources.len()
    );

    let mut definitions = Vec::new();
    for path in &sources {
        let text = common::fsio::read(path);
        for needle in ["fn agent_id_for_identity", "fn generate_agent_id"] {
            if text.contains(needle) {
                definitions.push((path.clone(), needle));
            }
        }
    }

    assert_eq!(
        definitions.len(),
        1,
        "agent id 的派生只许有一份实现，实际：{definitions:?}"
    );
    let (path, _) = &definitions[0];
    assert!(
        path.ends_with("crates/cog-core/src/contract/agent.rs"),
        "唯一的实现应在 cog-core 里，实际在 {}",
        path.display()
    );

    // 实现体内不许出现会变的那三样：它们一变，同一个人就换 id。
    let text = common::fsio::read(path);
    let body = function_body(&text, "fn agent_id_for_identity");
    for mutable in ["hostname", "pod_ip", "role"] {
        assert!(
            !body.contains(mutable),
            "`agent_id_for_identity` 的实现体读了 `{mutable}`：它是注册行上的可变事实，不是这个人是谁"
        );
    }
}

/// 从 `signature` 出现处截到第一个成对收尾的花括号。
fn function_body(text: &str, signature: &str) -> String {
    let start = text
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` 不在文件里"));
    let rest = &text[start..];
    let open = rest
        .find('{')
        .unwrap_or_else(|| panic!("`{signature}` 没有函数体"));
    let mut depth = 0usize;
    for (offset, ch) in rest[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return rest[open..open + offset + 1].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("`{signature}` 的函数体没有收尾");
}
