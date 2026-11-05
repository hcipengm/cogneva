use async_trait::async_trait;
use cog_core::{OrchestratorControl, SFError, SFResult, Task, UpstreamFailure};
use std::sync::Arc;
use tracing::{info, warn};

/// 一轮里最多退役多少条旧键的重规划。
///
/// 这条扫描挂在回注这一轮上，一轮的时间要留给越界本身：一次退役几千条会把
/// 这一轮压成一条批量更新，而它们已经在队列里排了很久，晚几分钟没有代价。
/// 剩下的下一轮接着退——判据是行上的那个键，不是轮次，重跑是幂等的。
// cap: bound — 一轮的时间要留给越界本身；剩下的每十分钟退一批，几小时清完
const STALE_REPLANS_PER_PASS: usize = 300;

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

    /// 退役那些把「另一条重规划自己的估算」当成项目基线的重规划行。
    ///
    /// 这些行是旧基线算法的产物：排期给每条新活写一行带当下时刻的估算，而旧
    /// 算法把项目的最新计划行当成当前基线，于是重规划单一造出来就换了键。现在
    /// 基线跳过重规划的计划行（见 [`current_revisions`]），那个键再也算不出来，
    /// 这些行既不会被去重认成「已经造过」，也不会随交付前进，只剩一遍遍排队。
    ///
    /// 退役而不是删：行是判据的来源，理由写在行上，读它的人能看到这件事为什么
    /// 结束。只碰还没走完的行——已经有终态的，它自己的终态比这里更早、也更准确。
    async fn retire_stale_replans(&self, tasks: &[Task]) {
        let stale: Vec<&Task> = tasks
            .iter()
            .filter(|task| !task.status.is_terminal())
            .filter(|task| crate::replan::keyed_on_a_replans_own_estimate(task))
            .collect();
        if stale.is_empty() {
            return;
        }
        let batch = stale.len().min(STALE_REPLANS_PER_PASS);
        for task in &stale[..batch] {
            let reason = format!(
                "this replan was keyed on another replan's own estimate ({}), a baseline this \
                 project control no longer reads: it can never be matched as an attempt already \
                 made and never advances with a delivery, so it is retired instead of run",
                task.input
                    .get(crate::replan::REPLAN_INPUT_KEY)
                    .and_then(|facts| facts.get("baseline_rev"))
                    .and_then(|rev| rev.as_str())
                    .unwrap_or("")
            );
            match self.cancel_task_with_reason(&task.id, reason).await {
                Ok(cascaded) => info!(
                    task_id = %task.id,
                    cascaded = cascaded.len(),
                    "retired a replan whose key was another replan's own estimate"
                ),
                Err(e) => warn!(
                    task_id = %task.id,
                    error = %e,
                    "a stale replan could not be retired; it keeps its place in the queue"
                ),
            }
        }
        if stale.len() > batch {
            info!(
                retired = batch,
                left = stale.len() - batch,
                "more replans are keyed on another replan's own estimate; the rest go next round"
            );
        }
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

    /// The dispatch ids, from the one rule that derives them. This control plane
    /// runs the loop that hands changes over, so it can answer; a stand-in that
    /// cannot keeps the trait's `None` and a reader reports the cost as
    /// unmeasurable rather than as zero.
    fn change_dispatch_ids(&self, identity: &cog_core::ChangeIdentity) -> Option<Vec<String>> {
        Some(crate::change_ledger::dispatch_task_ids(identity))
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

    async fn cancel_task_with_reason(
        &self,
        task_id: &str,
        reason: String,
    ) -> SFResult<Vec<String>> {
        self.dag_executor
            .cancel_task_with_reason(task_id, reason)
            .await
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
        // 顺手退役旧基线算法留下的重规划行：它们的键再也不会被算出来，留着
        // 只会一遍遍排队、每轮占掉一个时段（判据与理由见
        // `replan::keyed_on_a_replans_own_estimate`）。用的是这一轮已经读过的
        // 那份图，不另读一遍。
        self.retire_stale_replans(&tasks).await;
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
///
/// **重规划自己的计划行不参与**：每造一条新活，排期都会给它写一行形如
/// `estimate/work-time-v1/<kind>/n<N>/<当前时刻>` 的计划。重规划单也是新活，
/// 于是它一被造出来就把这个项目的「当前基线版本」推走一次，而下一轮拿去重比
/// 的正是这个键——上一轮造的那条永远认不出来，于是每轮都再造一条。实测
/// 2026-11-04 一个项目三小时造了 62 条重规划，图上排队的行里一半是它们。
///
/// 跳过它也不是放弃「交了货的基线要往前走」：交付物是**别的**任务（重规划自己
/// 的工期不是这个项目的计划），那些行照样把版本推走，下一版仍是新的一轮。
fn current_revisions(
    tasks: &[Task],
    plans: &[cog_core::TaskPlan],
) -> std::collections::BTreeMap<String, String> {
    use std::collections::BTreeMap;
    let project_of: BTreeMap<&str, (&str, bool)> = tasks
        .iter()
        .map(|t| {
            (
                t.id.as_str(),
                (t.project_key(), crate::replan::is_replan_task(t)),
            )
        })
        .collect();
    let mut latest: BTreeMap<String, (&chrono::DateTime<chrono::Utc>, &str)> = BTreeMap::new();
    for plan in plans {
        let Some(&(project, is_replan)) = project_of.get(plan.task_id.as_str()) else {
            // 计划行比任务行活得久（任务归档了），它说不出这个项目现在是哪一版。
            continue;
        };
        if is_replan {
            continue;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use cog_core::{BreachReport, PerformanceIndex, TaskPlan, TaskType};

    /// 一次越界：项目、指数、读数。去重与造活都只读这几格。
    fn breach(project: &str) -> BreachReport {
        BreachReport {
            project: project.to_string(),
            index: PerformanceIndex::Schedule,
            measured: 0.62,
            earned: 6_200,
            against: 10_000,
        }
    }

    fn row(id: &str, project: &str, kind: &str) -> Task {
        let mut task = Task::new(id, TaskType::Custom(kind.into()), serde_json::json!({}));
        task.goal_id = Some(project.to_string());
        task
    }

    /// 一条计划行。`at` 的先后就是「哪一版更新」的判据。
    fn plan_of(task_id: &str, rev: &str, at: i64) -> TaskPlan {
        TaskPlan {
            workspace_id: "ws-replan-key".into(),
            task_id: task_id.into(),
            planned_start: None,
            planned_end: None,
            budget_tokens: 1,
            baseline_rev: rev.into(),
            updated_at: chrono::Utc.timestamp_opt(at, 0).unwrap(),
        }
    }

    /// 重规划单自己排期的那一行，不许把「它将被拿去比对的基线」推走。
    ///
    /// 每造一条新活，排期都会写一行新的计划（`baseline_rev` 带着当下时刻）。
    /// 重规划单也是新活：认它的话，第二轮读到的基线已经是它自己那一行，
    /// 上一轮造的那条按 `is_replan_for` 永远认不出来，于是每轮都再造一条。
    /// 实测 2026-11-04 一个项目三小时造了 62 条重规划。
    #[test]
    fn the_ticket_it_just_minted_does_not_move_the_baseline_it_will_be_judged_against() {
        const PROJECT: &str = "change-ledger:crates";
        const WORK: &str = "change-row-1";

        // 工作行挂在根下面：一个项目只能有一条根行，多一条就成了「两条根，
        // 挑哪条都是猜」——那与这里要问的基线版本是两件事。
        let mut work_row = row(WORK, PROJECT, "change_ledger");
        work_row.parent_task_id = Some("goal-root".into());
        let mut tasks = vec![row("goal-root", PROJECT, "goal"), work_row];
        let mut plans = vec![plan_of(
            WORK,
            "estimate/work-time-v1/change_ledger/n9/2026-11-04T12:26:47Z",
            1_793_094_407,
        )];
        let found = breach(PROJECT);
        let first_wake = chrono::Utc.timestamp_opt(1_793_125_279, 0).unwrap();

        let revision = current_revisions(&tasks, &plans)
            .remove(PROJECT)
            .expect("有基线");
        let first = match crate::replan::replan_task(&found, &revision, &tasks, first_wake) {
            crate::replan::Decision::Mint(task) => *task,
            other => panic!("第一轮就该造出一条：{other:?}"),
        };

        // 造出来之后，排期给它写了计划：时间比工作行更晚，也就是「项目的
        // updated_at 最新的一行」换成了它自己。
        tasks.push(first.clone());
        plans.push(plan_of(
            &first.id,
            "estimate/work-time-v1/replan/n1/2026-11-04T21:07:59Z",
            1_793_125_279,
        ));

        let revision = current_revisions(&tasks, &plans)
            .remove(PROJECT)
            .expect("有基线");
        match crate::replan::replan_task(
            &found,
            &revision,
            &tasks,
            first_wake + chrono::Duration::seconds(60),
        ) {
            crate::replan::Decision::AlreadyMinted { task_id, open } => {
                assert_eq!(task_id, first.id, "认出的是刚造的那一条");
                assert!(open, "它还没走完，第二轮该等它而不是再造一条");
            }
            other => panic!(
                "重规划自己的估算行把基线推走了，于是上一轮那条认不出来、又造了一条：{other:?}"
            ),
        }
    }

    /// 跳过重规划的行不等于放弃「交了货就是一版新基线」：别的任务排的新计划
    /// 照样把版本推走，下一版仍是一轮新的尝试。
    #[test]
    fn an_estimate_from_any_other_task_still_moves_the_baseline() {
        const PROJECT: &str = "change-ledger:crates";
        let tasks = vec![
            row("goal-root", PROJECT, "goal"),
            row("change-row-1", PROJECT, "change_ledger"),
            row("change-row-2", PROJECT, "change_ledger"),
        ];
        let plans = vec![
            plan_of("change-row-1", "rev-old", 1_000),
            plan_of("change-row-2", "rev-new", 2_000),
        ];
        assert_eq!(
            current_revisions(&tasks, &plans)
                .get(PROJECT)
                .map(String::as_str),
            Some("rev-new"),
            "真交付留下的新计划仍然是这个项目当前的那一版"
        );
    }
}

#[cfg(test)]
mod stale_replan_tests {
    use super::*;
    use cog_core::{TaskStatus, TaskType};

    /// 一条重规划行，记着它是按哪一版基线造的。
    fn replan_row(id: &str, revision: &str) -> Task {
        Task::new(
            id,
            TaskType::Custom(crate::replan::REPLAN_TASK_KIND.into()),
            serde_json::json!({
                "goal": "重规划这个项目",
                crate::replan::REPLAN_INPUT_KEY: {
                    "project": "change-ledger:crates",
                    "index": "schedule",
                    "baseline_rev": revision,
                },
            }),
        )
    }

    /// 旧键的重规划行退役，理由写在行上；按真基线记的那条不动。
    ///
    /// 判据只认一种键：基线版本本身是**另一条重规划的估算**。那种键现在的基线
    /// 算法再也不会算出来，所以这种行既不会被去重认下来，也不会随交付前进，
    /// 留着就是每轮再排一次队。按工作行的估算记的那些是真尝试，留着。
    #[tokio::test]
    async fn a_replan_keyed_on_another_replans_estimate_is_retired_not_queued_again() {
        let dag: Arc<dyn cog_core::DagExecutor> =
            Arc::new(crate::DagExecutor::new("ws-stale-replans".to_string()));
        let stale = replan_row(
            "replan-stale",
            "estimate/work-time-v1/replan/n5/2026-11-04T21:07:59Z",
        );
        let live = replan_row(
            "replan-live",
            "estimate/work-time-v1/change_ledger/n9/2026-11-04T12:26:47Z",
        );
        dag.add_task(stale.clone()).await.unwrap();
        dag.add_task(live.clone()).await.unwrap();

        OrchestratorControlImpl::new(dag.clone())
            .retire_stale_replans(&[stale.clone(), live.clone()])
            .await;

        let settled = dag.get_task(&stale.id).await.expect("行还在，退役不删行");
        assert_eq!(settled.status, TaskStatus::Cancelled, "旧键的行该被退役");
        assert!(
            settled
                .error
                .unwrap_or_default()
                .contains("another replan's own estimate"),
            "理由要写在行上：读它的人得知道这条为什么结束"
        );
        assert_eq!(
            dag.get_task(&live.id).await.unwrap().status,
            TaskStatus::Pending,
            "按真基线记的那条是这一版上的一次尝试，不该被退役"
        );
    }
}
