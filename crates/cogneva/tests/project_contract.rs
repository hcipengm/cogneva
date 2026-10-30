//! The project face's write path, end to end through the real router.
//!
//! The four read endpoints were unit-tested at the projection level (the functions
//! that fold a task graph into rows), but nothing asked the router a question:
//! a route that was never registered, a body shape the handler rejects for the
//! wrong reason, or an error that came back as 500 instead of 400 would all have
//! passed. This file is the first reader of those paths that goes through axum.
//!
//! What it is not: a fixture for the projection. The numbers here are three rows
//! and they are read back through the same endpoints a browser calls.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_gateway::GatewayState;

use crate::router_state;

async fn state() -> Arc<GatewayState> {
    router_state::stateless()
}

fn app(state: &Arc<GatewayState>) -> axum::Router {
    cog_gateway::create_router(Arc::clone(state))
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let payload = match body {
        Some(v) => {
            request = request.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(request.body(payload).expect("request builds"))
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body reads");
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

/// A project created through the route, then read back through the listing. The
/// two must agree: a write that lands in a place the read face does not fold is
/// a write nobody can see.
#[tokio::test]
async fn project_contract_create_then_read_back() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract").await;

    let (status, created) = call(
        &app,
        "POST",
        "/api/v1/projects",
        &token,
        Some(serde_json::json!({ "name": "商用验收", "workspace_id": "default" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {created}");
    let id = created["id"]
        .as_str()
        .expect("id in the response")
        .to_string();
    assert_eq!(created["workspace_id"], "default");
    assert!(
        created["source"]["table"].is_string(),
        "每个响应都要说得清自己读的是哪张表"
    );

    let (status, listing) = call(&app, "GET", "/api/v1/projects", &token, None).await;
    assert_eq!(status, StatusCode::OK);
    let found = listing["projects"]
        .as_array()
        .expect("projects is a list")
        .iter()
        .find(|p| p["id"] == serde_json::Value::String(id.clone()))
        .unwrap_or_else(|| panic!("新建的项目必须在列表里: {listing}"));
    assert_eq!(found["kind"], "goal");
    assert_eq!(found["tasks"], 1, "根那一行就是它自己的第一条任务");
}

/// A task placed under the project's root, then read back as a nested tree.
#[tokio::test]
async fn project_contract_a_task_lands_under_its_parent_in_the_tree() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract").await;

    let (_, created) = call(
        &app,
        "POST",
        "/api/v1/projects",
        &token,
        Some(serde_json::json!({ "name": "树", "workspace_id": "default" })),
    )
    .await;
    let project = created["id"].as_str().expect("id").to_string();

    let (status, child) = call(
        &app,
        "POST",
        &format!("/api/v1/projects/{project}/tasks"),
        &token,
        Some(serde_json::json!({
            "title": "第一片",
            "parent_task_id": project,
            "acceptance": ["跑通门测试"],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {child}");
    let child_id = child["id"].as_str().expect("id").to_string();
    assert_eq!(
        child["parent_task_id"],
        serde_json::Value::String(project.clone())
    );

    let (status, wbs) = call(
        &app,
        "GET",
        &format!("/api/v1/projects/{project}/wbs"),
        &token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {wbs}");
    assert_eq!(wbs["total"], 2, "根 + 一片: {wbs}");
    let roots = wbs["roots"].as_array().expect("roots");
    assert_eq!(roots.len(), 1, "只有根是顶层: {wbs}");
    assert_eq!(roots[0]["id"], serde_json::Value::String(project.clone()));
    let children = roots[0]["children"].as_array().expect("children");
    assert_eq!(children.len(), 1);
    assert_eq!(children[0]["id"], serde_json::Value::String(child_id));
    assert_eq!(children[0]["depth"], 1);
    assert_eq!(children[0]["acceptance"], 1);
    assert_eq!(wbs["orphan_parents"], serde_json::json!([]));
    assert_eq!(wbs["cycles"], serde_json::json!([]));
}

/// The board has to hand back the cards it counts. A column that says "3" and
/// carries no cards is a number a reader cannot act on: they know there is work
/// in that column and not which work, so nothing on that screen can be moved.
/// This walks the whole loop -- create, read, move, read again -- because the
/// interesting failure is a card that stays in the old column after a move.
#[tokio::test]
async fn project_contract_the_board_returns_the_cards_it_counts() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract-board").await;

    let (_, created) = call(
        &app,
        "POST",
        "/api/v1/projects",
        &token,
        Some(serde_json::json!({ "name": "看板", "workspace_id": "default" })),
    )
    .await;
    let project = created["id"].as_str().expect("id").to_string();

    let (status, child) = call(
        &app,
        "POST",
        &format!("/api/v1/projects/{project}/tasks"),
        &token,
        Some(serde_json::json!({
            "title": "能被挪的一片",
            "parent_task_id": project,
            "acceptance": ["看板上看得见"],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {child}");
    let child_id = child["id"].as_str().expect("id").to_string();

    let (status, board) = call(
        &app,
        "GET",
        &format!("/api/v1/projects/{project}/board"),
        &token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {board}");

    let column = |name: &str| -> serde_json::Value {
        board["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .find(|c| c["status"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("no column {name}: {board}"))
    };
    let pending = column("pending");
    let cards = pending["cards"].as_array().expect("cards");
    let card = cards
        .iter()
        .find(|c| c["id"] == serde_json::Value::String(child_id.clone()))
        .unwrap_or_else(|| panic!("the new task is not on the board: {pending}"));
    assert_eq!(card["status"], "pending");
    assert_eq!(card["title"], "能被挪的一片");
    assert_eq!(
        card["parent_task_id"],
        serde_json::Value::String(project.clone())
    );
    assert_eq!(card["has_acceptance"], true);
    assert_eq!(card["blocked"], false);
    assert_eq!(pending["cards_hidden"], 0, "三行以内不会被截断");

    let (status, moved) = call(
        &app,
        "POST",
        &format!("/api/v1/tasks/{child_id}/move"),
        &token,
        Some(serde_json::json!({ "status": "scheduled" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {moved}");

    let (status, after) = call(
        &app,
        "GET",
        &format!("/api/v1/projects/{project}/board"),
        &token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let scheduled = after["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .find(|c| c["status"] == "scheduled")
        .expect("scheduled column");
    assert!(
        scheduled["cards"]
            .as_array()
            .expect("cards")
            .iter()
            .any(|c| c["id"] == child_id.as_str()),
        "挪过去的卡片要在新的一列里: {scheduled}"
    );
    let still_pending = after["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .find(|c| c["status"] == "pending")
        .expect("pending column");
    assert!(
        !still_pending["cards"]
            .as_array()
            .expect("cards")
            .iter()
            .any(|c| c["id"] == child_id.as_str()),
        "同一张卡不许同时出现在两列: {still_pending}"
    );
}

/// A parent that belongs to another project is refused with the code the client
/// branches on, not a 500 and not a silent success.
#[tokio::test]
async fn project_contract_a_cross_project_parent_is_refused() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract").await;

    let (_, a) = call(
        &app,
        "POST",
        "/api/v1/projects",
        &token,
        Some(serde_json::json!({ "name": "甲", "workspace_id": "default" })),
    )
    .await;
    let (_, b) = call(
        &app,
        "POST",
        "/api/v1/projects",
        &token,
        Some(serde_json::json!({ "name": "乙", "workspace_id": "default" })),
    )
    .await;
    let a = a["id"].as_str().expect("id").to_string();
    let b = b["id"].as_str().expect("id").to_string();

    let (status, body) = call(
        &app,
        "POST",
        &format!("/api/v1/projects/{a}/tasks"),
        &token,
        Some(serde_json::json!({ "title": "伸出去的一片", "parent_task_id": b })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(
        body["code"], "parent_in_another_project",
        "判词长在稳定的码上，客户端按它分支: {body}"
    );

    // 被拒之后什么都没落下：甲里仍然只有根那一行.
    let (_, wbs) = call(
        &app,
        "GET",
        &format!("/api/v1/projects/{a}/wbs"),
        &token,
        None,
    )
    .await;
    assert_eq!(
        wbs["total"], 1,
        "拒绝必须是拒绝，不是「报错但写进去了」: {wbs}"
    );
}

/// The ungrouped bucket is not a container: a new task has to land in a real
/// project.
#[tokio::test]
async fn project_contract_the_ungrouped_bucket_takes_no_new_tasks() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract").await;

    let bucket = cog_core::UNGROUPED_PROJECT;
    let (status, body) = call(
        &app,
        "POST",
        &format!("/api/v1/projects/{bucket}/tasks"),
        &token,
        Some(serde_json::json!({ "title": "无主的一片" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(body["code"], "ungrouped_is_not_a_container", "{body}");
}

/// A project nobody can see is 404, and so is one that does not exist. The
/// listing must not grow a row for either.
#[tokio::test]
async fn project_contract_reading_an_unknown_project_is_not_found() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract").await;

    for path in [
        "/api/v1/projects/no-such-project/wbs",
        "/api/v1/projects/no-such-project/board",
        "/api/v1/projects/no-such-project/gantt",
        "/api/v1/projects/no-such-project/evm",
    ] {
        let (status, body) = call(&app, "GET", path, &token, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
        assert_eq!(body["error"], "NotFound", "{path}: {body}");
        assert_eq!(body["code"], "not_found", "{path}: {body}");
    }
}

/// Two tenants, one router: a caller scoped to one workspace sees that workspace
/// and not the other, on the listing and on all four views.
///
/// The rows this guards against are the ones with no workspace at all. Before
/// this change an unscoped row was treated as visible to everyone, so one
/// unowned task appeared in every tenant's board — and no tenant could tell it
/// apart from its own work. The operator token in this test is the other half of
/// the same rule: fail-closed means the platform still sees everything, it does
/// not mean the platform stops seeing.
#[tokio::test]
async fn project_contract_workspace_isolation() {
    let state = state().await;
    let app = app(&state);
    let operator = router_state::admin_bearer(&state, "project-contract-operator").await;

    let mut ids = Vec::new();
    for (name, workspace) in [("甲", "alpha"), ("乙", "beta")] {
        let (status, created) = call(
            &app,
            "POST",
            "/api/v1/projects",
            &operator,
            Some(serde_json::json!({ "name": name, "workspace_id": workspace })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "body: {created}");
        ids.push(created["id"].as_str().expect("id").to_string());
    }
    let (alpha, beta) = (ids[0].clone(), ids[1].clone());

    let tenant = router_state::admin_bearer_scoped_to(&state, "tenant-alpha", &["alpha"]).await;

    let (status, listing) = call(&app, "GET", "/api/v1/projects", &tenant, None).await;
    assert_eq!(status, StatusCode::OK, "body: {listing}");
    let seen: Vec<String> = listing["projects"]
        .as_array()
        .expect("projects is a list")
        .iter()
        .filter_map(|p| p["id"].as_str().map(str::to_string))
        .collect();
    assert!(seen.contains(&alpha), "自己的项目必须在列表里: {listing}");
    assert!(
        !seen.contains(&beta),
        "别人的项目不许出现在列表里: {listing}"
    );

    let (_, operator_view) = call(&app, "GET", "/api/v1/projects", &operator, None).await;
    let all: Vec<&str> = operator_view["projects"]
        .as_array()
        .expect("projects is a list")
        .iter()
        .filter_map(|p| p["id"].as_str())
        .collect();
    assert!(
        all.contains(&alpha.as_str()) && all.contains(&beta.as_str()),
        "平台运营方仍然看得到全部——fail-closed 关的是租户，不是平台: {operator_view}"
    );

    for view in ["wbs", "board", "gantt", "evm"] {
        let path = format!("/api/v1/projects/{beta}/{view}");
        let (status, body) = call(&app, "GET", &path, &tenant, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
        assert_eq!(body["code"], "not_found", "{path}: {body}");

        let own = format!("/api/v1/projects/{alpha}/{view}");
        let (status, body) = call(&app, "GET", &own, &tenant, None).await;
        assert_eq!(status, StatusCode::OK, "{own}: {body}");
    }
}

/// One tenant's own writes leave a trail that can be read back by project.
///
/// The audit table and its read endpoint are wired to a process-level sink, so
/// this test installs a recorder where the deployment installs the Postgres one
/// and asks the same two questions a customer's security review asks: did the
/// write leave a row, and does the row say what changed? A write path that
/// forgets to record passes every other test in this file — the response is
/// identical — which is why this one exists.
struct RowRecorder(std::sync::Mutex<Vec<cog_storage::postgres::AuditEntry>>);

#[async_trait::async_trait]
impl cog_storage::postgres::ProjectAuditLog for RowRecorder {
    async fn record(&self, entry: cog_storage::postgres::AuditEntry) -> cog_core::SFResult<()> {
        self.0.lock().unwrap().push(entry);
        Ok(())
    }

    async fn recent(
        &self,
        _project_id: &str,
        _limit: i64,
    ) -> cog_core::SFResult<Vec<cog_storage::postgres::AuditRow>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn project_contract_every_write_leaves_a_row_the_timeline_can_return() {
    let state = state().await;
    let app = app(&state);
    let token = router_state::admin_bearer(&state, "project-contract-audit").await;
    let recorder = std::sync::Arc::new(RowRecorder(std::sync::Mutex::new(Vec::new())));
    cog_gateway::project_audit::install_for_test(Some(recorder.clone()));

    let (_, created) = call(
        &app,
        "POST",
        "/api/v1/projects",
        &token,
        Some(serde_json::json!({ "name": "审计", "workspace_id": "default" })),
    )
    .await;
    let project = created["id"].as_str().expect("id").to_string();
    let (status, child) = call(
        &app,
        "POST",
        &format!("/api/v1/projects/{project}/tasks"),
        &token,
        Some(serde_json::json!({ "title": "要改的一片", "parent_task_id": project })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {child}");
    let child_id = child["id"].as_str().expect("id").to_string();

    let (status, moved) = call(
        &app,
        "POST",
        &format!("/api/v1/tasks/{child_id}/move"),
        &token,
        Some(serde_json::json!({ "status": "scheduled" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "待办挪到排期是相邻的一步，必须落地: {moved}"
    );

    let rows = recorder.0.lock().unwrap();
    let mine: Vec<&cog_storage::postgres::AuditEntry> =
        rows.iter().filter(|e| e.project_id == project).collect();
    let actions: Vec<&str> = mine.iter().map(|e| e.action.as_str()).collect();
    assert!(
        actions.contains(&"project.task_created"),
        "新建一片活要留一行，实际：{actions:?}"
    );
    assert!(
        actions.contains(&"project.created"),
        "建项目要留一行，实际：{actions:?}"
    );
    assert!(
        actions.contains(&"project.card_moved"),
        "挪一张卡也要留一行，否则「谁把它挪过去的」没有答案，实际：{actions:?}"
    );
    let created_row = mine
        .iter()
        .find(|e| e.action == "project.task_created")
        .expect("task_created row");
    assert!(created_row.before.is_null(), "新建的改前是 null");
    assert_eq!(created_row.after["title"], "要改的一片");
    assert_eq!(created_row.resource_id, child_id, "行要指向被改的那一个");
    drop(rows);

    cog_gateway::project_audit::install_for_test(None);
}
