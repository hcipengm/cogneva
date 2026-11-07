//! Process overview — aggregates DAG state, crew (squad) progress, and agent
//! load into a single read-only snapshot for the WebUI Process view.

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::GatewayState;

#[derive(Debug, Serialize)]
pub struct ProcessTask {
    pub id: String,
    pub task_type: String,
    pub status: String,
    pub agent_id: Option<String>,
    pub blocked_by: Vec<String>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct GoalGroup {
    pub goal_id: String,
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub running: usize,
    pub pending: usize,
    pub progress_pct: f32,
    /// 目标行的终答，DAG 写过才有。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_answer: Option<String>,
    /// 目标行是否已收口。不是 `completed == total`：目标可以收在失败上。
    pub finished: bool,
    pub tasks: Vec<ProcessTask>,
}

/// 目标行的终答与收口状态，按任务 id 取。
///
/// 行不存在、或收口了没写答案，都读作没有：按任务条数推出来的「做完」会比
/// DAG 写下的多。
fn goal_row_answer(
    goal_id: &str,
    rows_by_id: &HashMap<&str, &cog_core::Task>,
) -> (Option<String>, bool) {
    match rows_by_id.get(goal_id) {
        Some(row) => (
            row.result
                .as_ref()
                .and_then(|result| result.get("final_answer"))
                .and_then(|answer| answer.as_str())
                .map(str::to_string),
            row.status.is_terminal(),
        ),
        None => (None, false),
    }
}

#[derive(Debug, Serialize)]
pub struct CrewProgress {
    pub crew_id: String,
    pub agent_ids: Vec<String>,
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub progress_pct: f32,
    pub crew_retry_count: u32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct AgentLoad {
    pub agent_id: String,
    pub role: String,
    pub capabilities: Vec<String>,
    /// healthy / suspect / stuck / dead / unknown (no supervisor verdict).
    pub health: String,
    /// Running tasks currently assigned to this agent.
    pub running_tasks: usize,
    /// Latest self-reported load score 0.0–1.0, if a heartbeat exists.
    pub load_score: Option<f32>,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct HealthCounts {
    pub healthy: usize,
    pub suspect: usize,
    pub stuck: usize,
    pub dead: usize,
}

#[derive(Debug, Serialize)]
pub struct ProcessOverview {
    pub timestamp: DateTime<Utc>,
    pub goals: Vec<GoalGroup>,
    pub crews: Vec<CrewProgress>,
    pub agents: Vec<AgentLoad>,
    pub health: HealthCounts,
    pub dlq_len: usize,
}

pub async fn process_overview_handler(
    State(state): State<Arc<GatewayState>>,
) -> Json<ProcessOverview> {
    let tasks = state.orchestrator.get_all_tasks().await;
    let task_status: HashMap<&str, &cog_core::TaskStatus> =
        tasks.iter().map(|t| (t.id.as_str(), &t.status)).collect();
    // 原始行按 id 索引：`ProcessTask` 是给线上看的投影，丢掉了 `result`，
    // 答案只能从这一层读；按 id 而不是按行的 `goal_id` 找，因为后者可能没填。
    let rows_by_id: HashMap<&str, &cog_core::Task> =
        tasks.iter().map(|t| (t.id.as_str(), t)).collect();

    // ── DAG state: group tasks by goal ──
    let mut goals: HashMap<String, Vec<ProcessTask>> = HashMap::new();
    for t in &tasks {
        goals
            .entry(t.goal_id.clone().unwrap_or_else(|| "(ungrouped)".into()))
            .or_default()
            .push(ProcessTask {
                id: t.id.clone(),
                task_type: crate::tasks::task_type_str(&t.task_type),
                status: t.status.as_str().to_string(),
                agent_id: t.agent_id.clone(),
                blocked_by: t.blocked_by.clone(),
                retry_count: t.retry_count,
                max_retries: t.max_retries,
                error: t.error.clone(),
                created_at: t.created_at,
                started_at: t.started_at,
            });
    }
    let mut goal_groups: Vec<GoalGroup> = goals
        .into_iter()
        .map(|(goal_id, tasks)| {
            let total = tasks.len();
            let completed = tasks.iter().filter(|t| t.status == "completed").count();
            let failed = tasks.iter().filter(|t| t.status == "failed").count();
            let running = tasks.iter().filter(|t| t.status == "running").count();
            let pending = total - completed - failed - running;
            let progress_pct = if total > 0 {
                (completed as f32 / total as f32) * 100.0
            } else {
                0.0
            };
            let (final_answer, finished) = goal_row_answer(&goal_id, &rows_by_id);
            GoalGroup {
                goal_id,
                total,
                completed,
                failed,
                running,
                pending,
                progress_pct,
                final_answer,
                finished,
                tasks,
            }
        })
        .collect();
    goal_groups.sort_by(|a, b| b.progress_pct.partial_cmp(&a.progress_pct).unwrap());

    // ── Health verdicts ──
    let mut health_of: HashMap<String, &'static str> = HashMap::new();
    let mut health = HealthCounts {
        healthy: 0,
        suspect: 0,
        stuck: 0,
        dead: 0,
    };
    if let Some(supervisor) = &state.supervisor {
        if let Ok(report) = supervisor.run_health_pass().await {
            health.healthy = report.healthy.len();
            health.suspect = report.suspect.len();
            health.stuck = report.stuck.len();
            health.dead = report.dead.len();
            for id in report.healthy {
                health_of.insert(id, "healthy");
            }
            for id in report.suspect {
                health_of.insert(id, "suspect");
            }
            for id in report.stuck {
                health_of.insert(id, "stuck");
            }
            for id in report.dead {
                health_of.insert(id, "dead");
            }
        }
    }

    // ── Agent load: registry merged with running-task counts + last heartbeat ──
    let mut running_by_agent: HashMap<&str, usize> = HashMap::new();
    for t in &tasks {
        if t.status == cog_core::TaskStatus::Running {
            if let Some(agent_id) = &t.agent_id {
                *running_by_agent.entry(agent_id.as_str()).or_insert(0) += 1;
            }
        }
    }
    let mut agents: Vec<AgentLoad> = Vec::new();
    if let Some(registry) = &state.agent_registry {
        if let Ok(registrations) = registry.list().await {
            for r in registrations {
                let latest_hb = state
                    .heartbeat_history
                    .as_ref()
                    .and_then(|h| h.get_heartbeat_history(&r.agent_id).last().cloned());
                agents.push(AgentLoad {
                    health: health_of
                        .get(&r.agent_id)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                    running_tasks: running_by_agent
                        .get(r.agent_id.as_str())
                        .copied()
                        .unwrap_or(0),
                    load_score: latest_hb.map(|hb| hb.load_score),
                    agent_id: r.agent_id,
                    role: r.role,
                    capabilities: r.capabilities,
                    last_heartbeat: r.last_heartbeat,
                });
            }
        }
    }
    agents.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));

    // ── Crew (squad) progress: registry crews joined with task statuses ──
    let mut crews: Vec<CrewProgress> = state
        .heartbeat_history
        .as_ref()
        .map(|h| {
            h.list_crews()
                .into_iter()
                .map(|c| {
                    let total = c.task_ids.len();
                    let completed = c
                        .task_ids
                        .iter()
                        .filter(|id| {
                            matches!(
                                task_status.get(id.as_str()),
                                Some(cog_core::TaskStatus::Completed)
                            )
                        })
                        .count();
                    let failed = c
                        .task_ids
                        .iter()
                        .filter(|id| {
                            matches!(
                                task_status.get(id.as_str()),
                                Some(cog_core::TaskStatus::Failed)
                            )
                        })
                        .count();
                    CrewProgress {
                        crew_id: c.crew_id,
                        agent_ids: c.agent_ids,
                        total_tasks: total,
                        completed_tasks: completed,
                        failed_tasks: failed,
                        progress_pct: if total > 0 {
                            (completed as f32 / total as f32) * 100.0
                        } else {
                            0.0
                        },
                        crew_retry_count: c.crew_retry_count,
                        updated_at: c.updated_at,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    crews.sort_by(|a, b| a.crew_id.cmp(&b.crew_id));

    let dlq_len = state.orchestrator.dlq_len().await.unwrap_or(0);

    Json(ProcessOverview {
        timestamp: Utc::now(),
        goals: goal_groups,
        crews,
        agents,
        health,
        dlq_len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(tasks: &[cog_core::Task]) -> HashMap<&str, &cog_core::Task> {
        tasks.iter().map(|t| (t.id.as_str(), t)).collect()
    }

    #[test]
    fn a_settled_goal_row_hands_through_the_answer_it_wrote() {
        let mut goal = cog_core::Task::goal_row("goal-a", "ship it");
        goal.status = cog_core::TaskStatus::Completed;
        goal.result = Some(serde_json::json!({ "final_answer": "shipped" }));
        let tasks = vec![goal];
        assert_eq!(
            goal_row_answer("goal-a", &index(&tasks)),
            (Some("shipped".to_string()), true)
        );
    }

    #[test]
    fn a_goal_row_settled_without_an_answer_is_finished_but_says_nothing() {
        let mut goal = cog_core::Task::goal_row("goal-b", "ship it");
        goal.status = cog_core::TaskStatus::Failed;
        goal.error = Some("分解失败".into());
        let tasks = vec![goal];
        assert_eq!(goal_row_answer("goal-b", &index(&tasks)), (None, true));
    }

    #[test]
    fn a_group_whose_goal_row_is_absent_reads_open_and_unanswered() {
        let mut child = cog_core::Task::new(
            "child-1",
            cog_core::TaskType::Custom("change_ledger".into()),
            serde_json::json!({ "goal": "ship it" }),
        );
        child.goal_id = Some("goal-c".into());
        let tasks = vec![child];
        assert_eq!(goal_row_answer("goal-c", &index(&tasks)), (None, false));
    }
}
