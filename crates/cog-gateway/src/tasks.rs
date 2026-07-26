use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{error::ApiError, GatewayState};
use cog_core::{AgentEvent, Task, TaskType};

fn broadcast_task_status(
    state: &GatewayState,
    task_id: &str,
    status: &str,
    agent_id: Option<String>,
) {
    let event = AgentEvent::TaskStatusChange {
        task_id: task_id.into(),
        status: status.into(),
        agent_id,
        crew_id: None,
        squad_id: None,
        timestamp: chrono::Utc::now(),
    };
    let _ = state.event_tx.send(event);
}

fn parse_task_type(s: &str) -> TaskType {
    match s {
        "planner" => TaskType::Planner,
        "generator" => TaskType::Generator,
        "evaluator" => TaskType::Evaluator,
        "reviewer" => TaskType::Reviewer,
        "llm_call" => TaskType::LlmCall,
        "tool_call" => TaskType::ToolCall,
        "file_op" => TaskType::FileOp,
        "db_transaction" => TaskType::DbTransaction,
        "network_request" => TaskType::NetworkRequest,
        "wasm_skill" => TaskType::WasmSkill,
        "skill" => TaskType::Skill,
        "dag_node" => TaskType::DagNode,
        _ => TaskType::Custom(s.into()),
    }
}

fn task_type_str(tt: &TaskType) -> String {
    match tt {
        TaskType::Planner => "planner".into(),
        TaskType::Generator => "generator".into(),
        TaskType::Evaluator => "evaluator".into(),
        TaskType::Reviewer => "reviewer".into(),
        TaskType::LlmCall => "llm_call".into(),
        TaskType::ToolCall => "tool_call".into(),
        TaskType::FileOp => "file_op".into(),
        TaskType::DbTransaction => "db_transaction".into(),
        TaskType::NetworkRequest => "network_request".into(),
        TaskType::WasmSkill => "wasm_skill".into(),
        TaskType::Skill => "skill".into(),
        TaskType::DagNode => "dag_node".into(),
        TaskType::Custom(s) => s.clone(),
    }
}

async fn record_task_op(_state: &GatewayState, _op: &str) {
    // MetricsExporter currently only exposes encode(); counter recording is
    // deferred until the exporter interface is extended.
}

// ─── Request / Response DTOs ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateTaskRequest {
    #[serde(default)]
    pub goal_id: Option<String>,
    pub goal: String,
    #[serde(default)]
    pub tasks: Option<Vec<TaskItem>>,
    #[serde(default)]
    pub workspace_id: String,
    pub priority: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct TaskItem {
    pub id: String,
    /// Task type. Use "self_evolution" to trigger the collaborative patch
    /// generation and auto-deploy pipeline.
    pub task_type: String,
    pub input: serde_json::Value,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Serialize)]
pub struct CreateTaskResponse {
    pub goal: String,
    pub task_count: usize,
    pub task_ids: Vec<String>,
    pub message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CompleteTaskRequest {
    pub result: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct FailTaskRequest {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct TaskView {
    pub id: String,
    pub task_type: String,
    pub status: String,
    pub input: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub blocked_by: Vec<String>,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,
    pub agent_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Task> for TaskView {
    fn from(t: Task) -> Self {
        Self {
            id: t.id,
            task_type: task_type_str(&t.task_type),
            status: format!("{:?}", t.status),
            input: t.input,
            result: t.result,
            error: t.error,
            blocked_by: t.blocked_by,
            priority: t.priority,
            retry_count: t.retry_count,
            max_retries: t.max_retries,
            agent_id: t.agent_id,
            created_at: t.created_at.to_rfc3339(),
            updated_at: t.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TaskSummary {
    pub total: usize,
    pub ready: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub all_completed: bool,
}

// ─── Handlers ────────────────────────────────────────────────────────

/// 估算一次 goal 提交的 token 规模：goal 文本 + 各 task input 序列化长度 / 4。
fn estimate_goal_tokens(req: &CreateTaskRequest) -> u64 {
    let mut chars = req.goal.len();
    if let Some(ref items) = req.tasks {
        for item in items {
            chars += item.input.to_string().len();
        }
    }
    ((chars / 4) as u64).max(1)
}

pub async fn create_task_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<(StatusCode, Json<CreateTaskResponse>), ApiError> {
    let goal_id = req
        .goal_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // 审计 3.4：workspace 级资源隔离 —— 认证用户只能向自己所属的 workspace 提交。
    if let Some(axum::Extension(c)) = claims.as_ref() {
        if !req.workspace_id.is_empty()
            && !c.workspace_ids.is_empty()
            && !c.workspace_ids.contains(&req.workspace_id)
        {
            if let Some(ref stream) = state.audit_stream {
                let _ = stream
                    .append(
                        cog_core::AuditKind::Authz,
                        &c.sub,
                        &req.workspace_id,
                        "authz.workspace_denied",
                        serde_json::json!({ "goal_id": goal_id }),
                    )
                    .await;
            }
            return Err(ApiError::forbidden(format!(
                "workspace '{}' is outside your workspace scope",
                req.workspace_id
            )));
        }
    }

    // 按任务配额执法（审计 3.6）：创建前经 5 级 HierarchyManager 检查。
    // 无 hierarchy_manager 时保持原有行为（quota_middleware 仍做用户级执法）。
    if let Some(ref hierarchy) = state.hierarchy_manager {
        let user_id = claims.as_ref().map(|axum::Extension(c)| c.sub.clone());
        let quota_ctx = cog_core::QuotaContext {
            user_id,
            workspace_id: Some(req.workspace_id.clone()),
            team_id: None,
            organization_id: None,
            global_id: None,
        };
        let estimated = estimate_goal_tokens(&req);
        let decision = hierarchy.check(&quota_ctx, estimated).await;
        if !decision.allowed {
            if let Some(ref stream) = state.audit_stream {
                let _ = stream
                    .append(
                        cog_core::AuditKind::QuotaEnforcement,
                        quota_ctx.user_id.as_deref().unwrap_or("anonymous"),
                        &goal_id,
                        "quota.task_rejected",
                        serde_json::json!({
                            "estimated_tokens": estimated,
                            "blocked_by": decision.blocked_by.len(),
                        }),
                    )
                    .await;
            }
            return Err(ApiError::too_many_requests(format!(
                "quota exceeded: estimated {} tokens blocked by {} scope(s)",
                estimated,
                decision.blocked_by.len()
            )));
        }
    }

    let tasks: Vec<Task> = if let Some(ref items) = req.tasks {
        items
            .iter()
            .map(|item| {
                let mut task = Task::new(
                    item.id.clone(),
                    parse_task_type(&item.task_type),
                    item.input.clone(),
                );
                task.blocked_by = item.blocked_by.clone();
                task.priority = item.priority;
                task.workspace_id = Some(req.workspace_id.clone());
                task.goal_id = Some(goal_id.clone());
                task
            })
            .collect()
    } else {
        Vec::new()
    };

    // Gateway 零判决：统一通过 submit_goal_auto 透传 goal + tasks。
    // Orchestrator 内的 ActionPlanner 负责根据标记决定分解或直接执行。
    let ids = state
        .orchestrator
        .submit_goal_auto(&req.goal, tasks)
        .await
        .map_err(|e| ApiError::internal(format!("submit_goal_auto failed: {}", e)))?;
    record_task_op(&state, "submit").await;

    Ok((
        StatusCode::CREATED,
        Json(CreateTaskResponse {
            goal: req.goal,
            task_count: ids.len(),
            task_ids: ids,
            message_id: None,
        }),
    ))
}

pub async fn get_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<TaskView>, ApiError> {
    match state.orchestrator.get_task(&id).await {
        Some(task) => {
            let view: TaskView = task.into();
            Ok(Json(view))
        }
        None => Err(ApiError::not_found(format!("task '{}' not found", id))),
    }
}

pub async fn list_tasks_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let tasks = state.orchestrator.get_all_tasks().await;
    let views: Vec<TaskView> = tasks.into_iter().map(|t| t.into()).collect();
    Ok(Json(views))
}

pub async fn get_ready_tasks_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let tasks = state.orchestrator.get_ready_tasks().await;
    let views: Vec<TaskView> = tasks.into_iter().map(|t| t.into()).collect();
    Ok(Json(views))
}

pub async fn schedule_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .schedule_task(&id)
        .await
        .map_err(ApiError::from)?;
    broadcast_task_status(&state, &id, "Scheduled", None);
    Ok(StatusCode::NO_CONTENT)
}

pub async fn start_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .start_task(&id)
        .await
        .map_err(ApiError::from)?;
    broadcast_task_status(&state, &id, "Running", None);
    Ok(StatusCode::NO_CONTENT)
}

pub async fn complete_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
    Json(req): Json<CompleteTaskRequest>,
) -> Result<Json<Vec<String>>, ApiError> {
    let scheduled = state
        .orchestrator
        .complete_task(&id, req.result)
        .await
        .map_err(|e| ApiError::internal(format!("complete failed: {}", e)))?;
    broadcast_task_status(&state, &id, "Completed", None);
    Ok(Json(scheduled))
}

pub async fn fail_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
    Json(req): Json<FailTaskRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (retried, cancelled, dlq) = state
        .orchestrator
        .fail_task(&id, req.error)
        .await
        .map_err(|e| ApiError::internal(format!("fail failed: {}", e)))?;
    broadcast_task_status(&state, &id, "Failed", None);
    Ok(Json(serde_json::json!({
        "retried": retried,
        "cancelled": cancelled,
        "dlq_pushed": dlq,
    })))
}

pub async fn cancel_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<String>>, ApiError> {
    let cancelled = state
        .orchestrator
        .cancel_task(&id)
        .await
        .map_err(|e| ApiError::internal(format!("cancel failed: {}", e)))?;
    broadcast_task_status(&state, &id, "Cancelled", None);
    Ok(Json(cancelled))
}

pub async fn retry_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .retry_task(&id)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn task_summary_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<TaskSummary>, ApiError> {
    let all = state.orchestrator.get_all_tasks().await;
    let ready = state.orchestrator.get_ready_tasks().await;
    let running: Vec<_> = all
        .iter()
        .filter(|t| format!("{:?}", t.status) == "Running")
        .cloned()
        .collect();
    let completed: Vec<_> = all
        .iter()
        .filter(|t| format!("{:?}", t.status) == "Completed")
        .cloned()
        .collect();
    let failed: Vec<_> = all
        .iter()
        .filter(|t| format!("{:?}", t.status) == "Failed")
        .cloned()
        .collect();

    Ok(Json(TaskSummary {
        total: all.len(),
        ready: ready.len(),
        running: running.len(),
        completed: completed.len(),
        failed: failed.len(),
        all_completed: state.orchestrator.all_completed().await,
    }))
}

pub async fn get_task_dependents_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let tasks = state
        .orchestrator
        .get_dependents(&id)
        .await
        .unwrap_or_default();
    let views: Vec<TaskView> = tasks.into_iter().map(|t| t.into()).collect();
    Ok(Json(views))
}

pub async fn get_task_dependencies_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let tasks = state
        .orchestrator
        .get_dependencies(&id)
        .await
        .unwrap_or_default();
    let views: Vec<TaskView> = tasks.into_iter().map(|t| t.into()).collect();
    Ok(Json(views))
}

pub async fn get_task_graph_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (tasks, edges) = state.orchestrator.get_graph().await;
    let nodes: Vec<TaskView> = tasks.into_iter().map(|t| t.into()).collect();
    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    })))
}

pub async fn delete_task_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .orchestrator
        .delete_task(&id)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn check_timeouts_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    let results = state.orchestrator.check_timeouts().await;
    let out: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(id, retried, cancelled, dlq)| {
            serde_json::json!({
                "task_id": id,
                "retried": retried,
                "cancelled": cancelled,
                "dlq_pushed": dlq,
            })
        })
        .collect();
    Ok(Json(out))
}

// ─── Batch ops ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BatchCompleteRequest {
    pub completions: Vec<CompletionItem>,
}

#[derive(Debug, Deserialize)]
pub struct CompletionItem {
    pub task_id: String,
    pub result: serde_json::Value,
}

pub async fn batch_complete_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<BatchCompleteRequest>,
) -> Result<Json<Vec<String>>, ApiError> {
    let mut scheduled = Vec::new();
    for item in req.completions {
        match state
            .orchestrator
            .complete_task(&item.task_id, item.result)
            .await
        {
            Ok(mut s) => scheduled.append(&mut s),
            Err(e) => tracing::warn!("batch complete failed for {}: {}", item.task_id, e),
        }
    }
    Ok(Json(scheduled))
}
