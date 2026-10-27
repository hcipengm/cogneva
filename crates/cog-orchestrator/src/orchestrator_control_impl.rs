use async_trait::async_trait;
use cog_core::{OrchestratorControl, SFError, SFResult, Task, UpstreamFailure};
use std::sync::Arc;

/// High-level orchestrator control that composes a [`cog_core::DagExecutor`]
/// and an [`cog_core::ActionPlanner`] to provide the unified
/// [`OrchestratorControl`] interface.
/// This struct lives in `cog-orchestrator` and delegates DAG state
/// transitions to the internally-mutable [`cog_core::DagExecutor`]
/// implementation, while `submit_goal_auto` routes through
/// [`cog_core::ActionPlanner`] for goal decomposition when needed.
pub struct OrchestratorControlImpl {
    dag_executor: Arc<dyn cog_core::DagExecutor>,
    action_planner: Option<Arc<dyn cog_core::ActionPlanner>>,
    skill_registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
    /// 计划基线所在的后端与它所属的 workspace。与 `dag_executor` 用的是同一份
    /// 存储——基线和任务行分家，读面就会拿 A 库的现状去配 B 库的计划。
    /// 没接时 [`OrchestratorControl::task_plans`] 返回 `NotImplemented`，
    /// 而不是一个空集合冒充「都没排期」。
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    workspace_id: String,
}

impl OrchestratorControlImpl {
    pub fn new(dag_executor: Arc<dyn cog_core::DagExecutor>) -> Self {
        Self {
            dag_executor,
            action_planner: None,
            skill_registry: None,
            state_backend: None,
            workspace_id: "default".into(),
        }
    }

    pub fn with_plan_store(
        mut self,
        backend: Arc<dyn cog_core::StateBackend>,
        workspace_id: impl Into<String>,
    ) -> Self {
        self.state_backend = Some(backend);
        self.workspace_id = workspace_id.into();
        self
    }

    pub fn with_action_planner(mut self, planner: Arc<dyn cog_core::ActionPlanner>) -> Self {
        self.action_planner = Some(planner);
        self
    }

    pub fn with_skill_registry(
        mut self,
        registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
    ) -> Self {
        self.skill_registry = registry;
        self
    }
}

#[async_trait]
impl OrchestratorControl for OrchestratorControlImpl {
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()> {
        self.dag_executor.submit_goal(goal, tasks).await
    }

    async fn submit_goal_auto(&self, goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        if let (Some(ref planner), Some(ref skill_registry)) =
            (&self.action_planner, &self.skill_registry)
        {
            let registry = skill_registry.read().await;
            let ids = planner.process_goal(goal, tasks, &registry).await?;
            return Ok(ids);
        }
        // No planner: still answer with what the graph took, not with what was
        // asked of it. `submit_goal` returns nothing and drops the ids the
        // batch insert skipped, so a held id would read back as queued.
        let added = self.dag_executor.add_tasks_batch(tasks).await?;
        tracing::info!(%goal, added_tasks = %added.len(), "submitted goal directly to the DAG");
        Ok(added)
    }

    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()> {
        self.dag_executor.assign_task(task_id, agent_id).await
    }

    async fn add_task(&self, task: Task) -> SFResult<()> {
        self.dag_executor.add_task(task).await
    }

    async fn replace_task(&self, task: Task) -> SFResult<()> {
        self.dag_executor.replace_task(task).await
    }

    /// 写一条基线。与 [`Self::task_plans`] 共用同一个后端与 workspace：读回
    /// 来的必须就是刚写进去的那一条，两边各算一份就会对不上。
    async fn save_task_plan(&self, plan: &cog_core::TaskPlan) -> SFResult<()> {
        match self.state_backend.as_ref() {
            Some(backend) => backend.save_task_plan(plan).await,
            None => Err(SFError::NotImplemented(
                "save_task_plan: this orchestrator holds no plan store, so a baseline \
                 written here would be readable by nobody"
                    .into(),
            )),
        }
    }

    async fn crew_can_retry(&self, task_ids: &[String]) -> bool {
        self.dag_executor.crew_can_retry(task_ids).await
    }

    async fn crew_retry_all(&self, task_ids: &[String]) -> usize {
        self.dag_executor.crew_retry_all(task_ids).await
    }

    async fn get_ready_tasks(&self) -> Vec<Task> {
        self.dag_executor.get_ready_tasks().await
    }

    async fn get_all_tasks(&self) -> Vec<Task> {
        self.dag_executor.get_all_tasks().await
    }

    async fn task_plans(&self) -> SFResult<Vec<cog_core::TaskPlan>> {
        match self.state_backend.as_ref() {
            Some(backend) => backend.task_plans(&self.workspace_id).await,
            None => Err(SFError::NotImplemented(
                "task_plans: this orchestrator holds no plan store, so whether a task was \
                 ever planned cannot be read here"
                    .into(),
            )),
        }
    }

    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool> {
        self.dag_executor.push_to_dlq(task_id, error).await
    }

    async fn retry_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.retry_task(task_id).await
    }

    async fn dlq_len(&self) -> SFResult<usize> {
        self.dag_executor.dlq_len().await
    }

    async fn start_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.start_task(task_id).await
    }

    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        self.dag_executor.complete_task(task_id, result).await
    }

    async fn fail_task(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        self.dag_executor.fail_task(task_id, error, cause).await
    }

    async fn fail_task_after(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        self.dag_executor
            .fail_task_after(task_id, error, cause, retry_after_secs)
            .await
    }

    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>> {
        self.dag_executor.cancel_task(task_id).await
    }

    async fn get_task(&self, task_id: &str) -> Option<Task> {
        self.dag_executor.get_task(task_id).await
    }

    async fn schedule_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.schedule_task(task_id).await
    }

    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        self.dag_executor.check_timeouts().await
    }

    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>> {
        self.dag_executor.get_dependents(task_id).await
    }

    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>> {
        self.dag_executor.get_dependencies(task_id).await
    }

    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
        self.dag_executor.get_graph().await
    }

    async fn delete_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.delete_task(task_id).await
    }

    async fn all_completed(&self) -> bool {
        self.dag_executor.all_completed().await
    }

    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool> {
        self.dag_executor.replay_dlq(task_id).await
    }
}

// ─── 绩效越界的回注 ───────────────────────────────────────────────────────

/// 读得到逐笔 token 账的那一侧判出越界，能在图上造活的是这一侧；两件事各在一
/// 地，所以回注走这个 trait，而不是让判定侧自己拼一条任务行塞进图里（那样判据
/// 与去重会各演一遍，而两边都不知道对方演了一遍）。
#[async_trait]
impl cog_core::ReplanMinter for OrchestratorControlImpl {
    async fn mint_replans(
        &self,
        breaches: &[cog_core::BreachReport],
    ) -> SFResult<Vec<cog_core::ReplanMinting>> {
        if breaches.is_empty() {
            return Ok(Vec::new());
        }
        // 一轮只扫一遍图：判定侧报上来的是一批越界，不是一条。
        let tasks = self.get_all_tasks().await;
        // 基线读不到时一条也不造：没有版本就没有去重键，硬造会让每一轮都多一条
        // 一模一样的活。这个错误往上抛，判成「这一轮判不了」，而不是「没有越界」。
        let plans = self.task_plans().await.map_err(|e| {
            SFError::Database(format!(
                "mint_replans: plan baselines are not readable, so which revision a breach \
                 belongs to cannot be named: {e}"
            ))
        })?;
        let revision_of = current_revisions(&tasks, &plans);
        let now = chrono::Utc::now();

        let mut minted = Vec::with_capacity(breaches.len());
        let mut batch: Vec<Task> = Vec::new();
        let mut batch_at: Vec<usize> = Vec::new();
        for breach in breaches {
            let Some(revision) = revision_of.get(&breach.project) else {
                minted.push(cog_core::ReplanMinting {
                    project: breach.project.clone(),
                    index: breach.index,
                    outcome: cog_core::ReplanOutcome::Refused {
                        reason: format!(
                            "项目 {} 没有任何计划基线：没有版本可挂，越界也就无从比对",
                            breach.project
                        ),
                    },
                });
                continue;
            };
            match crate::replan::replan_task(breach, revision, &tasks, now) {
                crate::replan::Decision::Mint(task) => {
                    batch_at.push(minted.len());
                    minted.push(cog_core::ReplanMinting {
                        project: breach.project.clone(),
                        index: breach.index,
                        outcome: cog_core::ReplanOutcome::Minted {
                            task_id: task.id.clone(),
                        },
                    });
                    batch.push(*task);
                }
                other => minted.push(cog_core::ReplanMinting {
                    project: breach.project.clone(),
                    index: breach.index,
                    outcome: other.into(),
                }),
            }
        }

        // 一批一起入图：同一轮里两处越界各自判过之后才写，写的时候不再重判。
        // 入图返回的是**真正进了图**的 id——一个被同 id 顶掉的行不是新活，按
        // 「造出来了」报出去，读面会去找一条自己看得到的活却什么都没有。
        if !batch.is_empty() {
            let added = self.dag_executor.add_tasks_batch(batch).await?;
            for &at in &batch_at {
                let cog_core::ReplanOutcome::Minted { task_id } = &minted[at].outcome else {
                    continue;
                };
                if added.contains(task_id) {
                    continue;
                }
                // 没进图：同 id 的行已经在里面了。它是不管用还是已经死了，读那一行
                // 自己的状态，不在这里硬写一个 `open: true`——那会把一条走完了的活
                // 报成「结论还在路上」，而读者会一直等一个已经不用等的结果。
                let open = tasks
                    .iter()
                    .find(|t| &t.id == task_id)
                    .is_none_or(|t| !t.status.is_terminal());
                minted[at].outcome = cog_core::ReplanOutcome::AlreadyMinted {
                    task_id: task_id.clone(),
                    open,
                };
            }
        }
        Ok(minted)
    }
}

/// 每个项目当前的基线版本：这个项目的计划行里 `updated_at` 最新的那一行说的那一版。
///
/// 取最新而不是取最早的：重规划写下的是一版新基线，判「这一版有没有过重规划」
/// 要问的是最新那一版。按 `updated_at` 而不是按 `baseline_rev` 的字符串比——
/// 后者是时间戳文本，字符串序只在同一格式同一时区下才对得上。
fn current_revisions(
    tasks: &[Task],
    plans: &[cog_core::TaskPlan],
) -> std::collections::BTreeMap<String, String> {
    use std::collections::BTreeMap;
    let project_of: BTreeMap<&str, &str> = tasks
        .iter()
        .map(|t| (t.id.as_str(), t.project_key()))
        .collect();
    let mut latest: BTreeMap<String, (&chrono::DateTime<chrono::Utc>, &str)> = BTreeMap::new();
    for plan in plans {
        let Some(project) = project_of.get(plan.task_id.as_str()) else {
            // 计划行比任务行活得久（任务归档了），它说不出这个项目现在是哪一版。
            continue;
        };
        let entry = latest
            .entry(project.to_string())
            .or_insert((&plan.updated_at, plan.baseline_rev.as_str()));
        if plan.updated_at >= *entry.0 {
            *entry = (&plan.updated_at, plan.baseline_rev.as_str());
        }
    }
    latest
        .into_iter()
        .map(|(project, (_, rev))| (project, rev.to_string()))
        .collect()
}
