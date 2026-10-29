//! 一个内存 `StateBackend`：契约的实现，不是某个 crate 的实现。
//!
//! 用例要的是「有一张表能存事件、能按实体取回」，不是「存储 crate 的那张表」。
//! 拿产线那份当夹具，被测 crate 的改动会让别的 crate 的用例红，红的理由却与它无关；
//! 同时给被测 crate 添了一条它没必要认识的邻居。所以这里按契约抄一份，语义与契约
//! 一致、与某个后端的判题规则无关。
//!
//! 这份替身不替 `cog-storage` 的测试负责：那边的 case 判的是它自己的实现。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::{
    AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint,
    UpstreamFailure,
};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Default)]
struct MemoryStore {
    agent_states: HashMap<String, AgentState>,
    checkpoints: HashMap<String, TaskCheckpoint>,
    events: HashMap<String, Vec<Event>>,
    boards: HashMap<String, ContextBoard>,
    dag_states: HashMap<String, serde_json::Value>,
    dag_tasks: HashMap<String, HashMap<String, cog_core::Task>>,
    dag_retries: HashMap<String, HashMap<String, Vec<cog_core::RetryAttempt>>>,
    promotions: Vec<cog_core::PromotionRecord>,
}

/// In-memory state backend for testing.
pub struct MemoryStateBackend {
    store: RwLock<MemoryStore>,
}

impl MemoryStateBackend {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(MemoryStore::default()),
        }
    }
}

impl Default for MemoryStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for MemoryStateBackend {
    /// Count the stored tasks by state across every workspace in the store.
    async fn task_status_counts(&self) -> SFResult<Option<cog_core::TaskStatusCounts>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Some(cog_core::TaskStatusCounts::of(
            store
                .dag_tasks
                .values()
                .flat_map(|tasks| tasks.values())
                .map(|task| &task.status),
        )))
    }

    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.agent_states.get(agent_id).copied())
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.agent_states.insert(agent_id.into(), *state);
        Ok(())
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let current = store.agent_states.get(agent_id);
        if current == Some(expected) {
            store.agent_states.insert(agent_id.into(), *new);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.checkpoints.get(task_id).cloned())
    }

    async fn save_checkpoint(&self, checkpoint: &TaskCheckpoint) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .checkpoints
            .insert(checkpoint.task_id.clone(), checkpoint.clone());
        Ok(())
    }

    async fn append_event(&self, task_id: &str, event: &Event) -> SFResult<u64> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let list = store.events.entry(task_id.into()).or_default();
        list.push(event.clone());
        Ok(list.len() as u64)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        match store.events.get(task_id) {
            Some(list) => {
                let start = offset as usize;
                let end = (start + limit).min(list.len());
                if start >= list.len() {
                    Ok(Vec::new())
                } else {
                    Ok(list[start..end].to_vec())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.boards.get(task_id).cloned())
    }

    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let board = store
            .boards
            .entry(task_id.into())
            .or_insert_with(|| ContextBoard {
                task_id: task_id.into(),
                fields: HashMap::new(),
                updated_at: Utc::now(),
            });
        board.fields.insert(field.into(), value.into());
        board.updated_at = Utc::now();
        Ok(())
    }

    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.checkpoints.remove(task_id);
        Ok(())
    }

    async fn delete_board(&self, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.boards.remove(task_id);
        Ok(())
    }

    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(board) = store.boards.get_mut(task_id) {
            board.fields.remove(field);
            board.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn save_dag_state(&self, workspace_id: &str, state: &serde_json::Value) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.dag_states.insert(workspace_id.into(), state.clone());
        Ok(())
    }

    async fn load_dag_state(&self, workspace_id: &str) -> SFResult<Option<serde_json::Value>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.dag_states.get(workspace_id).cloned())
    }

    fn dag_supports_fine_grained(&self) -> bool {
        true
    }

    async fn dag_get_task(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Option<cog_core::Task>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .and_then(|m| m.get(task_id).cloned()))
    }

    async fn dag_set_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        task: &cog_core::Task,
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .dag_tasks
            .entry(workspace_id.into())
            .or_default()
            .insert(task_id.into(), task.clone());
        Ok(())
    }

    async fn dag_transition_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();
        let current = tasks.get(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if !expected.contains(&current.status) {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot transition task in {:?} state", current.status),
            });
        }
        tasks.insert(task_id.into(), updated.clone());
        Ok(())
    }

    async fn dag_transition_owned_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        expected: &[cog_core::TaskStatus],
        updated: &cog_core::Task,
        owner: &str,
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();
        let current = tasks.get(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if current.owner_unit.as_deref() != Some(owner) {
            return Err(SFError::Conflict(format!(
                "task {task_id} is owned by {}, not {owner}",
                current.owner_unit.as_deref().unwrap_or("nobody")
            )));
        }
        if !expected.contains(&current.status) {
            return Err(SFError::Conflict(format!(
                "task {task_id} is in {:?} state, not one of the states this transition accepts",
                current.status
            )));
        }
        // 交接才改归属；一次状态迁移顺手把 owner 换掉，等于那次决定被悄悄做了。
        if updated.owner_unit.as_deref() != Some(owner) {
            return Err(SFError::Conflict(format!(
                "task {task_id}: the updated row names owner {}, not {owner}",
                updated.owner_unit.as_deref().unwrap_or("nobody")
            )));
        }
        tasks.insert(task_id.into(), updated.clone());
        Ok(())
    }

    async fn dag_remove_task(&self, workspace_id: &str, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(m) = store.dag_tasks.get_mut(workspace_id) {
            m.remove(task_id);
        }
        if let Some(m) = store.dag_retries.get_mut(workspace_id) {
            m.remove(task_id);
        }
        Ok(())
    }

    async fn dag_list_tasks(&self, workspace_id: &str) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn dag_get_all_tasks(&self, workspace_id: &str) -> SFResult<Vec<cog_core::Task>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn dag_get_dependencies(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .and_then(|m| m.get(task_id))
            .map(|t| t.blocked_by.clone())
            .unwrap_or_default())
    }

    async fn dag_get_dependents(&self, workspace_id: &str, task_id: &str) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .map(|m| {
                m.values()
                    .filter(|t| t.blocked_by.iter().any(|b| b == task_id))
                    .map(|t| t.id.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn dag_complete_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();

        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if task.status != cog_core::TaskStatus::Running {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Cannot complete task in {:?} state — it may have been handled by timeout or retry",
                    task.status
                ),
            });
        }
        task.status = cog_core::TaskStatus::Completed;
        task.result = Some(result);
        task.updated_at = chrono::Utc::now();

        let dependent_ids: Vec<String> = tasks
            .values()
            .filter(|t| t.blocked_by.iter().any(|b| b == task_id))
            .map(|t| t.id.clone())
            .collect();

        let mut ready = Vec::new();
        for dep_id in dependent_ids {
            let all_ready = tasks
                .get(&dep_id)
                .map(|d| {
                    d.status == cog_core::TaskStatus::Pending
                        && d.blocked_by.iter().all(|b| {
                            tasks
                                .get(b)
                                .map(|t| t.status == cog_core::TaskStatus::Completed)
                                .unwrap_or(false)
                        })
                })
                .unwrap_or(false);
            if all_ready {
                // 只报告就绪，不翻转状态：Scheduled 的单一含义是"已发布到
                // ready 流"，翻转由发布方（publish_ready_tasks → schedule_task）
                // 完成，否则下游会卡在 Scheduled 永远不被发布。
                ready.push(dep_id);
            }
        }

        Ok(ready)
    }

    async fn dag_fail_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        max_retries: u32,
        retry_delay: std::time::Duration,
    ) -> SFResult<(bool, Vec<String>)> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();

        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        task.error = Some(error.clone());
        task.error_cause = cause;
        task.updated_at = chrono::Utc::now();

        if task.retry_count < max_retries {
            task.retry_count += 1;
            task.status = cog_core::TaskStatus::Pending;
            task.retry_not_before = chrono::Duration::from_std(retry_delay)
                .ok()
                .map(|d| chrono::Utc::now() + d);
            return Ok((true, Vec::new()));
        }

        task.status = cog_core::TaskStatus::Failed;
        task.retry_not_before = None;
        let downstream = collect_downstream_derived(tasks, task_id);
        let mut cancelled = Vec::new();
        for dep_id in downstream {
            if let Some(t) = tasks.get_mut(&dep_id) {
                if !matches!(
                    t.status,
                    cog_core::TaskStatus::Cancelled
                        | cog_core::TaskStatus::Failed
                        | cog_core::TaskStatus::Completed
                ) {
                    t.status = cog_core::TaskStatus::Cancelled;
                    t.error = Some(format!(
                        "Cascade cancelled: upstream task '{}' permanently failed with error: {}",
                        task_id, error
                    ));
                    t.error_cause = None;
                    t.updated_at = chrono::Utc::now();
                    cancelled.push(dep_id.clone());
                }
            }
        }
        Ok((false, cancelled))
    }

    async fn dag_cancel_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        reason: String,
    ) -> SFResult<Vec<String>> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();

        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;
        if matches!(
            task.status,
            cog_core::TaskStatus::Cancelled
                | cog_core::TaskStatus::Failed
                | cog_core::TaskStatus::Completed
        ) {
            return Err(SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot cancel task in {:?} state", task.status),
            });
        }
        task.status = cog_core::TaskStatus::Cancelled;
        task.error = Some(reason.clone());
        // 取消的理由是编排层给的，不是上游给的：类型跟着理由一起换掉，
        // 别让上一次失败的 cause 留在一条已取消的记录上。
        task.error_cause = None;
        task.updated_at = chrono::Utc::now();

        let downstream = collect_downstream_derived(tasks, task_id);
        let mut cancelled = Vec::new();
        for dep_id in downstream {
            if let Some(t) = tasks.get_mut(&dep_id) {
                if !matches!(
                    t.status,
                    cog_core::TaskStatus::Cancelled
                        | cog_core::TaskStatus::Failed
                        | cog_core::TaskStatus::Completed
                ) {
                    t.status = cog_core::TaskStatus::Cancelled;
                    t.error = Some(format!(
                        "Cascade cancelled: upstream task '{}' was cancelled",
                        task_id
                    ));
                    t.updated_at = chrono::Utc::now();
                    cancelled.push(dep_id.clone());
                }
            }
        }
        Ok(cancelled)
    }

    async fn dag_append_retry(
        &self,
        workspace_id: &str,
        task_id: &str,
        attempt: &cog_core::RetryAttempt,
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .dag_retries
            .entry(workspace_id.into())
            .or_default()
            .entry(task_id.into())
            .or_default()
            .push(attempt.clone());
        Ok(())
    }

    async fn dag_get_retry_history(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Vec<cog_core::RetryAttempt>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_retries
            .get(workspace_id)
            .and_then(|m| m.get(task_id).cloned())
            .unwrap_or_default())
    }

    async fn dag_clear_workspace(&self, workspace_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.dag_tasks.remove(workspace_id);
        store.dag_retries.remove(workspace_id);
        store.dag_states.remove(workspace_id);
        Ok(())
    }
}

/// BFS over derived dependents (blocked_by 反向)收集全部下游任务 id。
fn collect_downstream_derived(
    tasks: &HashMap<String, cog_core::Task>,
    task_id: &str,
) -> Vec<String> {
    let mut result = Vec::new();
    let mut stack = vec![task_id.to_string()];
    let mut visited = std::collections::HashSet::new();
    visited.insert(task_id.to_string());

    while let Some(current) = stack.pop() {
        for t in tasks.values() {
            if t.blocked_by.iter().any(|b| b == &current) && visited.insert(t.id.clone()) {
                result.push(t.id.clone());
                stack.push(t.id.clone());
            }
        }
    }
    result
}

// ─── PromotionLedger (in-memory) ───

#[async_trait]
impl cog_core::PromotionLedger for MemoryStateBackend {
    async fn record(&self, rec: cog_core::PromotionRecord) -> SFResult<()> {
        let mut guard = self
            .store
            .write()
            .map_err(|_| SFError::Agent("promotion ledger lock poisoned".into()))?;
        guard.promotions.push(rec);
        Ok(())
    }

    async fn update_status(
        &self,
        id: &str,
        status: cog_core::PromotionStatus,
        outcome: &str,
    ) -> SFResult<()> {
        let mut guard = self
            .store
            .write()
            .map_err(|_| SFError::Agent("promotion ledger lock poisoned".into()))?;
        let Some(rec) = guard.promotions.iter_mut().find(|r| r.id == id) else {
            return Err(SFError::Validation(format!(
                "promotion record {id} not found"
            )));
        };
        rec.status = status;
        rec.outcome = outcome.to_string();
        rec.updated_at = Utc::now();
        Ok(())
    }

    async fn count_promoted_since(&self, since: DateTime<Utc>) -> SFResult<u64> {
        let guard = self
            .store
            .read()
            .map_err(|_| SFError::Agent("promotion ledger lock poisoned".into()))?;
        Ok(guard
            .promotions
            .iter()
            .filter(|r| r.status == cog_core::PromotionStatus::Promoted && r.updated_at >= since)
            .count() as u64)
    }

    async fn recent(&self, limit: usize) -> SFResult<Vec<cog_core::PromotionRecord>> {
        let guard = self
            .store
            .read()
            .map_err(|_| SFError::Agent("promotion ledger lock poisoned".into()))?;
        let mut out = guard.promotions.clone();
        out.sort_by_key(|r| std::cmp::Reverse(r.updated_at));
        out.truncate(limit);
        Ok(out)
    }

    async fn awaiting(
        &self,
        cluster: &str,
        limit: usize,
    ) -> SFResult<Vec<cog_core::PromotionRecord>> {
        let guard = self
            .store
            .read()
            .map_err(|_| SFError::Agent("promotion ledger lock poisoned".into()))?;
        let mut out: Vec<_> = guard
            .promotions
            .iter()
            .filter(|r| {
                r.cluster == cluster && r.status == cog_core::PromotionStatus::AwaitingApproval
            })
            .cloned()
            .collect();
        out.sort_by_key(|r| r.updated_at);
        out.truncate(limit);
        Ok(out)
    }
}
