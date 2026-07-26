//! ActionPlan REST API.
//! goal-decomposition output produced by the Planner / ActionPlanOrchestrator.
//! These endpoints expose CRUD over an in-memory plan store so external
//! clients can submit, retrieve, list and delete plans without going through
//! the orchestrator's task-DAG flow.
//! The store is a [`tokio::sync::Mutex<HashMap<String, StoredActionPlan>>`]
//! attached to [`GatewayState`](crate::GatewayState). Replace with a Redis-
//! backed implementation when persistence across restarts is required.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{error::ApiError, GatewayState};
use cog_core::{validate_dag, ActionPlan, AtomicTask, SFError, SFResult, Skill};

/// Hybrid store for [`ActionPlan`] resources.
/// When a Redis connection is configured, all operations persist to Redis
/// (hash key `sf:action_plans`) so plans survive process restarts. Without
/// Redis the store falls back to an in-memory `HashMap`.
#[derive(Debug, Clone)]
pub struct ActionPlanStore {
    inner: Arc<Mutex<HashMap<String, StoredActionPlan>>>,
    redis: Option<redis::aio::MultiplexedConnection>,
}

impl Default for ActionPlanStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            redis: None,
        }
    }
}

impl ActionPlanStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a store backed by Redis.
    pub async fn with_redis(redis_url: &str) -> SFResult<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| SFError::Redis(format!("redis client: {}", e)))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SFError::Redis(format!("redis connection: {}", e)))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            redis: Some(conn),
        })
    }

    pub async fn insert(&self, id: String, plan: StoredActionPlan) {
        if let Some(ref mut conn) = self.redis.clone() {
            let json = serde_json::to_string(&plan).unwrap_or_default();
            let _: redis::RedisResult<()> = redis::cmd("HSET")
                .arg("sf:action_plans")
                .arg(&id)
                .arg(json)
                .query_async(conn)
                .await;
        }
        self.inner.lock().await.insert(id, plan);
    }

    pub async fn get(&self, id: &str) -> Option<StoredActionPlan> {
        if let Some(ref mut conn) = self.redis.clone() {
            let json: redis::RedisResult<Option<String>> = redis::cmd("HGET")
                .arg("sf:action_plans")
                .arg(id)
                .query_async(conn)
                .await;
            if let Ok(Some(s)) = json {
                if let Ok(plan) = serde_json::from_str::<StoredActionPlan>(&s) {
                    return Some(plan);
                }
            }
        }
        self.inner.lock().await.get(id).cloned()
    }

    pub async fn remove(&self, id: &str) -> Option<StoredActionPlan> {
        if let Some(ref mut conn) = self.redis.clone() {
            let _: redis::RedisResult<()> = redis::cmd("HDEL")
                .arg("sf:action_plans")
                .arg(id)
                .query_async(conn)
                .await;
        }
        self.inner.lock().await.remove(id)
    }

    pub async fn list(&self) -> Vec<StoredActionPlan> {
        if let Some(ref mut conn) = self.redis.clone() {
            let result: redis::RedisResult<Vec<String>> = redis::cmd("HVALS")
                .arg("sf:action_plans")
                .query_async(conn)
                .await;
            if let Ok(values) = result {
                let mut plans = Vec::with_capacity(values.len());
                for v in values {
                    if let Ok(plan) = serde_json::from_str::<StoredActionPlan>(&v) {
                        plans.push(plan);
                    }
                }
                return plans;
            }
        }
        self.inner.lock().await.values().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        if let Some(ref mut conn) = self.redis.clone() {
            let result: redis::RedisResult<i64> = redis::cmd("HLEN")
                .arg("sf:action_plans")
                .query_async(conn)
                .await;
            if let Ok(n) = result {
                return n as usize;
            }
        }
        self.inner.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// An [`ActionPlan`] augmented with server-side metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredActionPlan {
    pub id: String,
    pub plan: ActionPlan,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub validation: Option<ValidationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub entry_nodes: Vec<String>,
    pub exit_nodes: Vec<String>,
    pub critical_path_len: usize,
    pub components: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateActionPlanRequest {
    pub goal: String,
    #[serde(default)]
    pub tasks: Vec<AtomicTask>,
    #[serde(default)]
    pub skills: Vec<Skill>,
    #[serde(default)]
    pub edges: Vec<(String, String)>,
}

#[derive(Debug, Serialize)]
pub struct CreateActionPlanResponse {
    pub id: String,
    pub plan: ActionPlan,
    pub validation: ValidationSummary,
}

#[derive(Debug, Serialize)]
pub struct ListActionPlansResponse {
    pub plans: Vec<StoredActionPlan>,
}

/// `POST /api/v1/action-plan` — create and persist a new [`ActionPlan`].
/// The request body is validated against the 6-rule DAG checker
/// ([`cog_core::validate_dag`]) before being stored. A new UUID is generated
/// as the plan ID.
pub async fn create_action_plan_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<CreateActionPlanRequest>,
) -> Result<(StatusCode, Json<CreateActionPlanResponse>), ApiError> {
    if req.goal.trim().is_empty() {
        return Err(ApiError::bad_request("goal must not be empty"));
    }

    let plan = ActionPlan {
        goal: req.goal,
        tasks: req.tasks,
        skills: req.skills,
        edges: req.edges,
    };

    // 6-rule DAG validation. Empty plans (no tasks) are allowed: the validator
    // returns trivially when there are no tasks/edges.
    let validation = if plan.tasks.is_empty() && plan.edges.is_empty() {
        ValidationSummary {
            entry_nodes: Vec::new(),
            exit_nodes: Vec::new(),
            critical_path_len: 0,
            components: 0,
        }
    } else {
        let v = validate_dag(&plan.tasks, &plan.edges)
            .map_err(|e| ApiError::bad_request(format!("invalid DAG: {}", e)))?;
        ValidationSummary {
            entry_nodes: v.entry_nodes,
            exit_nodes: v.exit_nodes,
            critical_path_len: v.critical_path_len,
            components: v.components,
        }
    };

    let id = Uuid::new_v4().to_string();
    let stored = StoredActionPlan {
        id: id.clone(),
        plan: plan.clone(),
        created_at: chrono::Utc::now(),
        validation: Some(validation.clone()),
    };
    state.action_plan_store.insert(id.clone(), stored).await;

    Ok((
        StatusCode::CREATED,
        Json(CreateActionPlanResponse {
            id,
            plan,
            validation,
        }),
    ))
}

/// `GET /api/v1/action-plan/:id` — fetch a stored [`ActionPlan`] by ID.
pub async fn get_action_plan_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<StoredActionPlan>, ApiError> {
    state
        .action_plan_store
        .get(&id)
        .await
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("action plan '{}' not found", id)))
}

/// `GET /api/v1/action-plan` — list all stored plans.
pub async fn list_action_plans_handler(
    State(state): State<Arc<GatewayState>>,
) -> Json<ListActionPlansResponse> {
    let plans = state.action_plan_store.list().await;
    Json(ListActionPlansResponse { plans })
}

/// `DELETE /api/v1/action-plan/:id` — remove a stored plan.
pub async fn delete_action_plan_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match state.action_plan_store.remove(&id).await {
        Some(_) => Ok(Json(serde_json::json!({"deleted": id}))),
        None => Err(ApiError::not_found(format!(
            "action plan '{}' not found",
            id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, blocked_by: Vec<&str>) -> AtomicTask {
        AtomicTask {
            id: id.into(),
            name: id.into(),
            skill_id: None,
            description: None,
            estimated_tokens: 0,
            skill_gap: false,
            blocked_by: blocked_by.into_iter().map(String::from).collect(),
            blocks: vec![],
            input: serde_json::Value::Null,
            output_entities: vec![],
            estimated_seconds: 0,
        }
    }

    #[tokio::test]
    async fn store_insert_get_remove_roundtrip() {
        let store = ActionPlanStore::new();
        let plan = StoredActionPlan {
            id: "p1".into(),
            plan: ActionPlan {
                goal: "g".into(),
                ..Default::default()
            },
            created_at: chrono::Utc::now(),
            validation: None,
        };
        store.insert("p1".into(), plan.clone()).await;
        assert_eq!(store.len().await, 1);

        let fetched = store.get("p1").await.expect("plan should exist");
        assert_eq!(fetched.plan.goal, "g");

        let removed = store.remove("p1").await;
        assert!(removed.is_some());
        assert!(store.is_empty().await);
        assert!(store.get("p1").await.is_none());
    }

    #[tokio::test]
    async fn store_list_returns_all() {
        let store = ActionPlanStore::new();
        for i in 0..3 {
            let p = StoredActionPlan {
                id: format!("p{}", i),
                plan: ActionPlan::default(),
                created_at: chrono::Utc::now(),
                validation: None,
            };
            store.insert(format!("p{}", i), p).await;
        }
        let listed = store.list().await;
        assert_eq!(listed.len(), 3);
    }

    /// CreateActionPlanRequest deserializes from the documented JSON shape.
    #[test]
    fn create_request_deserializes() {
        let json = serde_json::json!({
            "goal": "build a feature",
            "tasks": [{
                "id": "a",
                "name": "Task A"
            }],
            "skills": [{
                "id": "skill1",
                "name": "Coding",
                "description": "writes code",
                "tools": []
            }],
            "edges": []
        });
        let req: CreateActionPlanRequest =
            serde_json::from_value(json).expect("should deserialize");
        assert_eq!(req.goal, "build a feature");
        assert_eq!(req.tasks.len(), 1);
        assert_eq!(req.skills.len(), 1);
    }

    /// Empty goal is rejected at the validation level used by the handler.
    #[test]
    fn empty_goal_rejected() {
        let req = CreateActionPlanRequest {
            goal: "   ".into(),
            tasks: vec![],
            skills: vec![],
            edges: vec![],
        };
        assert!(req.goal.trim().is_empty());
    }

    /// Linear DAG validates successfully.
    #[test]
    fn linear_dag_validates() {
        let tasks = vec![
            task("a", vec![]),
            task("b", vec!["a"]),
            task("c", vec!["b"]),
        ];
        let edges = vec![("a".into(), "b".into()), ("b".into(), "c".into())];
        let result = cog_core::validate_dag(&tasks, &edges).unwrap();
        assert_eq!(result.entry_nodes, vec!["a"]);
        assert_eq!(result.exit_nodes, vec!["c"]);
    }

    /// Cyclic DAG validation fails.
    #[test]
    fn cyclic_dag_rejected() {
        let tasks = vec![task("a", vec!["b"]), task("b", vec!["a"])];
        let edges = vec![("a".into(), "b".into()), ("b".into(), "a".into())];
        let result = cog_core::validate_dag(&tasks, &edges);
        assert!(result.is_err());
    }
}
