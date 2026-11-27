//! ActionPlan REST API.
//! goal-decomposition output produced by the Planner / ActionPlanOrchestrator.
//! These endpoints expose CRUD over an in-memory plan store so external
//! clients can submit, retrieve, list and delete plans without going through
//! the orchestrator's task-DAG flow.
//! The store is a [`tokio::sync::Mutex<HashMap<String, StoredActionPlan>>`]
//! attached to [`crate::GatewayState`], optionally backed by a shared
//! [`cog_core::KeyValueStore`] when one is handed in at construction: that is
//! what makes plans persist across restarts, and without it the map is the
//! whole store.
//!
//! Configured with a shared store, every operation's answer is that store's
//! answer. A write it refused, or a read it could not serve, is an error the
//! routes report; it is never folded into "stored", "no such plan" or "no
//! plans". The in-memory map is not a fallback in that mode -- it holds only
//! what this process has touched, so answering from it would say things about
//! the platform's state that nobody measured.

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
use cog_core::{validate_dag, ActionPlan, AtomicTask, KeyValueStore, SFResult, Skill};

/// Hybrid store for [`ActionPlan`] resources.
/// When a shared key-value store is configured, all operations persist to it
/// (hash key `sf:action_plans`) so plans survive process restarts. Without one
/// the store falls back to an in-memory `HashMap`.
#[derive(Clone)]
pub struct ActionPlanStore {
    inner: Arc<Mutex<HashMap<String, StoredActionPlan>>>,
    shared: Option<Arc<dyn KeyValueStore>>,
}

impl std::fmt::Debug for ActionPlanStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Whether this store persists to a shared store at all is what a reader
        // needs from this type; the store's internals are not.
        f.debug_struct("ActionPlanStore")
            .field("inner", &self.inner)
            .field("shared", &self.shared.as_ref().map(|_| "key-value store"))
            .finish()
    }
}

impl Default for ActionPlanStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            shared: None,
        }
    }
}

impl ActionPlanStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a store that also persists to `store`, the shared key-value face.
    ///
    /// The caller supplies the face rather than a URL: which implementation is
    /// behind it, and which process holds the connection, is not this crate's
    /// fact to know.
    pub fn with_store(store: Arc<dyn KeyValueStore>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            shared: Some(store),
        }
    }

    /// 写一份计划。共享存储拒了这次写入就是个错误，不是一次「记下了」：
    /// 本进程的内存表不进任何询问者的视野，而模块文档说得很清楚——让计划活过
    /// 一次重启的正是这份共享存储。先写它再写内存表，写不成就不留半份。
    pub async fn insert(&self, id: String, plan: StoredActionPlan) -> SFResult<()> {
        if let Some(ref store) = self.shared {
            let json = serde_json::to_string(&plan)?;
            store.hset(PLANS_KEY, &id, &json).await?;
        }
        self.inner.lock().await.insert(id, plan);
        Ok(())
    }

    /// 读一份计划。配了共享存储时它是唯一的答案来源：内存表只有本进程碰过的
    /// 那几行，在它那里读不到并不等于「没有这份计划」。`Ok(None)` 是「存储说
    /// 没有这一行」，读不成则是 `Err`，两者不折叠。
    pub async fn get(&self, id: &str) -> SFResult<Option<StoredActionPlan>> {
        if let Some(ref store) = self.shared {
            return match store.hget(PLANS_KEY, id).await? {
                Some(s) => Ok(Some(serde_json::from_str::<StoredActionPlan>(&s)?)),
                None => Ok(None),
            };
        }
        Ok(self.inner.lock().await.get(id).cloned())
    }

    /// 删一份计划，返回删掉的那一份（没有则 `None`）。
    ///
    /// 先读后删：答复要回答的是「删掉了什么」，而共享存储里的行可能根本没进过
    /// 本进程的内存表（上一次重启之后它就是空的）。只看内存表的话，一次确实
    /// 发生的删除会被答成 `404`。
    pub async fn remove(&self, id: &str) -> SFResult<Option<StoredActionPlan>> {
        if let Some(ref store) = self.shared {
            let existing = match store.hget(PLANS_KEY, id).await? {
                Some(s) => Some(serde_json::from_str::<StoredActionPlan>(&s)?),
                None => None,
            };
            store.hdel(PLANS_KEY, id).await?;
            self.inner.lock().await.remove(id);
            return Ok(existing);
        }
        Ok(self.inner.lock().await.remove(id))
    }

    /// 列出全部计划。读不成就是一个错误：一份读不出来的账答成空表，读者会
    /// 把「问不到」读成「一张都没有」。
    pub async fn list(&self) -> SFResult<Vec<StoredActionPlan>> {
        if let Some(ref store) = self.shared {
            let values = store.hvals(PLANS_KEY).await?;
            let mut plans = Vec::with_capacity(values.len());
            for v in values {
                plans.push(serde_json::from_str::<StoredActionPlan>(&v)?);
            }
            return Ok(plans);
        }
        Ok(self.inner.lock().await.values().cloned().collect())
    }

    pub async fn len(&self) -> SFResult<usize> {
        if let Some(ref store) = self.shared {
            return Ok(store.hlen(PLANS_KEY).await? as usize);
        }
        Ok(self.inner.lock().await.len())
    }

    pub async fn is_empty(&self) -> SFResult<bool> {
        Ok(self.len().await? == 0)
    }
}

/// The hash the plans are kept under when a shared store is configured.
const PLANS_KEY: &str = "sf:action_plans";

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
    // 存储拒了这次写入，就没有「建好了」这句话可说：答复里那个 id 指向的
    // 是一份重启之后谁都不认识的计划。
    state
        .action_plan_store
        .insert(id.clone(), stored)
        .await
        .map_err(|e| {
            ApiError::internal(format!(
                "计划没能写进存储（写不进去的计划不会在重启后还在）：{e}"
            ))
        })?;

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
    match state.action_plan_store.get(&id).await {
        Ok(Some(plan)) => Ok(Json(plan)),
        Ok(None) => Err(ApiError::not_found(format!(
            "action plan '{}' not found",
            id
        ))),
        Err(e) => Err(ApiError::internal(format!(
            "读不到计划存储，所以确认不了这份计划在不在：{e}"
        ))),
    }
}

/// `GET /api/v1/action-plan` — list all stored plans.
pub async fn list_action_plans_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<ListActionPlansResponse>, ApiError> {
    let plans =
        state.action_plan_store.list().await.map_err(|e| {
            ApiError::internal(format!("读不到计划存储，所以列出来的不是全部：{e}"))
        })?;
    Ok(Json(ListActionPlansResponse { plans }))
}

/// `DELETE /api/v1/action-plan/:id` — remove a stored plan.
pub async fn delete_action_plan_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match state.action_plan_store.remove(&id).await {
        Ok(Some(_)) => Ok(Json(serde_json::json!({"deleted": id}))),
        Ok(None) => Err(ApiError::not_found(format!(
            "action plan '{}' not found",
            id
        ))),
        Err(e) => Err(ApiError::internal(format!(
            "计划存储拒了这次删除，所以它还在：{e}"
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
            terminal: String::new(),
            now: String::new(),
            gap: String::new(),
            output_entities: vec![],
            estimated_seconds: 0,
            parent_id: None,
            acceptance: vec![],
            delivers: None,
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
        store
            .insert("p1".into(), plan.clone())
            .await
            .expect("the in-memory store takes the write");
        assert_eq!(store.len().await.expect("the map is readable"), 1);

        let fetched = store
            .get("p1")
            .await
            .expect("the in-memory store is readable")
            .expect("plan should exist");
        assert_eq!(fetched.plan.goal, "g");

        let removed = store.remove("p1").await.expect("the map is readable");
        assert!(removed.is_some());
        assert!(store.is_empty().await.expect("the map is readable"));
        assert!(store
            .get("p1")
            .await
            .expect("the in-memory store is readable")
            .is_none());
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
            store
                .insert(format!("p{}", i), p)
                .await
                .expect("the in-memory store takes the write");
        }
        let listed = store.list().await.expect("the map is readable");
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
