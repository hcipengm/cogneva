use async_trait::async_trait;
use cog_core::{AtomicTask, SFError, SFResult, Task, TaskResult, TaskResultMetadata, TaskType};
use std::sync::Arc;
use tracing::info;

use crate::{
    actors::ModeSelectorActor,
    observable::{
        global_observable, ChangeYieldOutcome, BOUNDARY_NO_HARD_RULES, BOUNDARY_PASSED,
        BOUNDARY_VIOLATED,
    },
    profile::derive_task_profile,
    squad::{SquadConfig, SquadExecutor, SquadResult},
};

/// 一件派给名册上任意员工的活。谁接都行——这件活由材料、做法与验收
/// 判据说了算，不由接活的人的角色说了算。
///
/// 种类名在 `cog_core::contract::work_item`：交活的一方（场景，如 cog-github）
/// 与接活的一方（这里）是两个 crate，两边各拼一次就会有一边改了另一边不认。
pub use cog_core::contract::work_item::PLATFORM_AGENT_TASK;

/// [`cog_core::TaskExecutor`] implementation that routes tasks through
/// `SquadExecutor` + `ModeSelectorActor` for Agent-based collaboration.
pub struct CollaborationExecutor {
    mode_selector: ModeSelectorActor,
    agent_manager: Option<Arc<dyn cog_core::AgentManager>>,
    llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    hook_engine: Option<Arc<dyn cog_core::HookEngine>>,
    object_backend: Option<Arc<dyn cog_core::ObjectBackend>>,
    squad_reflection: Option<Arc<dyn cog_core::SquadReflection>>,
    boundary_config: Option<crate::BoundaryConfig>,
    knowledge_backend: Option<Arc<dyn cog_core::KnowledgeBackend>>,
    change_sinks: Vec<Arc<dyn cog_core::ChangeSink>>,
    reflection_engine: Option<Arc<dyn cog_core::ReflectionEngine>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    pge_schemas: Option<std::collections::HashMap<String, serde_json::Value>>,
    skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    /// 决策结果的写侧：每个任务现建的 SquadExecutor 都要拿到它，否则
    /// 引擎只读不写，表永远空。
    meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
    /// Ralph Loop 预算与停滞窗口（配置面 `ralph` 段；None = 默认）。
    ralph: Option<crate::squad::ralph::RalphLoopConfig>,
    /// 局部修复预算（配置面 `pge` 段；None = [`crate::DEFAULT_LOCAL_REPAIR_MAX`]）。
    local_repair_max: Option<u32>,
    /// 阶段产物的持久化存储；每个任务现建的 SquadExecutor 都要拿到它，
    /// 否则一次运行断在两条调用之间，已经付过费的计划和 diff 就跟着进程没了。
    stage_store: Option<Arc<dyn cog_core::PgeStageStore>>,
    /// 上游池的暂停开关；每个任务现建的 SquadExecutor 都要拿到它，否则那条
    /// 循环在上游停供期间照样一轮一轮地开新轮。
    pause_gate: Option<Arc<dyn cog_core::SchedulerGate>>,
    /// 原则账；生成端事前要看的那几条原则从这里按路径取。没有它时生成端
    /// 仍旧照跑——只是这一次没有任何一条项目原则被递进来，与「这个仓库没有
    /// 原则管着这些文件」不是一回事，所以缺它要留一条具名日志。
    principle_ledger: Option<Arc<dyn cog_core::PrincipleLedgerStore>>,
    /// 派活时的轮转游标。
    ///
    /// 顺序轮流而不是把活都给第一个人：名册上的人都能干同一类活，全都堆在
    /// 一个名下，其余的人就只是名单上的一行，而队列长度会全记在那个人的账上。
    next_worker: std::sync::atomic::AtomicUsize,
}

/// What the producer said about itself, at the two stages the record keeps.
///
/// `kind` is the entry point's own spelling and `intent` is the class the
/// table made of it on the day the change was written. They travel as one value
/// because they are one fact at two moments: a record that keeps only the class
/// stays frozen at whichever table answered that day, while one that keeps the
/// spelling is re-classified by the funnel against today's table. Together they
/// also keep `extract_changes` inside the seven-parameter bound without
/// silencing the lint that noticed.
struct ProducerClass<'a> {
    intent: Option<cog_core::EvolutionIntent>,
    kind: Option<&'a str>,
}

impl CollaborationExecutor {
    /// Create a new collaboration executor.
    pub fn new() -> Self {
        Self {
            mode_selector: ModeSelectorActor::new(),
            agent_manager: None,
            llm_provider: None,
            hook_engine: None,
            object_backend: None,
            squad_reflection: None,
            boundary_config: None,
            knowledge_backend: None,
            change_sinks: Vec::new(),
            reflection_engine: None,
            self_review: None,
            pge_schemas: None,
            skill_registry: None,
            state_backend: None,
            meta_learning: None,
            ralph: None,
            local_repair_max: None,
            stage_store: None,
            pause_gate: None,
            principle_ledger: None,
            next_worker: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Inject an AgentManager for the squad actors a task runs.
    pub fn with_agent_manager(mut self, manager: Arc<dyn cog_core::AgentManager>) -> Self {
        self.agent_manager = Some(manager);
        self
    }

    /// Inject an LLM provider for squad execution.
    pub fn with_llm_provider(mut self, llm: Arc<dyn cog_core::LlmClient>) -> Self {
        self.llm_provider = Some(llm);
        self
    }

    /// Attach a meta-learning engine for predictive PGE mode selection.
    ///
    /// Held on the executor as well as handed to the mode selector: the
    /// selector only reads a recommendation, while the squad executors built
    /// per task are what write the outcome back. Wiring one without the other
    /// leaves the engine reading a table nothing ever fills.
    pub fn with_meta_learning(mut self, engine: Arc<dyn cog_core::MetaLearning>) -> Self {
        self.mode_selector = self.mode_selector.with_meta_learning(engine.clone());
        self.meta_learning = Some(engine);
        self
    }

    /// Attach a hook engine for event-driven observability.
    pub fn with_hook_engine(mut self, hook: Arc<dyn cog_core::HookEngine>) -> Self {
        self.hook_engine = Some(hook);
        self
    }

    /// Inject an object backend for snapshot persistence.
    pub fn with_object_backend(mut self, backend: Arc<dyn cog_core::ObjectBackend>) -> Self {
        self.object_backend = Some(backend);
        self
    }

    /// Attach a squad-level reflection engine.
    pub fn with_squad_reflection(mut self, reflection: Arc<dyn cog_core::SquadReflection>) -> Self {
        self.squad_reflection = Some(reflection);
        self
    }

    /// Set the boundary configuration for dynamic boundary rule evaluation.
    pub fn with_boundary_config(mut self, cfg: crate::BoundaryConfig) -> Self {
        self.boundary_config = Some(cfg);
        self
    }

    /// Attach a unified knowledge backend for historical pattern retrieval.
    pub fn with_knowledge_backend(mut self, backend: Arc<dyn cog_core::KnowledgeBackend>) -> Self {
        self.knowledge_backend = Some(backend);
        self
    }

    /// Attach a change sink for self-evolution generated changes.
    /// Additive: every attached sink receives every generated change
    /// (e.g. EvolutionEngine approval console + GitHub PR publisher).
    pub fn with_change_sink(mut self, sink: Arc<dyn cog_core::ChangeSink>) -> Self {
        self.change_sinks.push(sink);
        self
    }

    /// Attach a reflection engine to record squad/change outcomes.
    pub fn with_reflection_engine(mut self, engine: Arc<dyn cog_core::ReflectionEngine>) -> Self {
        self.reflection_engine = Some(engine);
        self
    }

    /// Enable the self-review quality gate for all PGE actors.
    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach operator-configured JSON Schemas for PGE actor outputs,
    /// keyed by actor name ("planner", "generator", "evaluator",
    /// "moderator", "merger").
    pub fn with_pge_schemas(
        mut self,
        schemas: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        self.pge_schemas = Some(schemas);
        self
    }

    /// Inject a skill registry so squad prompt skills
    /// (`*_skill_id` in SquadConfig) can be resolved.
    pub fn with_skill_registry(
        mut self,
        registry: Arc<dyn cog_core::ExternalSkillRegistry>,
    ) -> Self {
        self.skill_registry = Some(registry);
        self
    }

    /// Inject a state backend so squads can persist RalphLoop iteration
    /// history across restarts.
    pub fn with_state_backend(mut self, backend: Arc<dyn cog_core::StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// Override Ralph Loop budget/stagnation knobs for all squads
    /// (from the `ralph` config section).
    pub fn with_ralph_config(mut self, config: crate::squad::ralph::RalphLoopConfig) -> Self {
        self.ralph = Some(config);
        self
    }

    /// Override the local-repair budget for all squads
    /// (from the `pge` config section).
    pub fn with_local_repair_max(mut self, max: u32) -> Self {
        self.local_repair_max = Some(max);
        self
    }

    /// 每个阶段产物在下一通调用之前记下来的位置。
    pub fn with_stage_store(mut self, store: Arc<dyn cog_core::PgeStageStore>) -> Self {
        self.stage_store = Some(store);
        self
    }

    /// 让每条 Ralph 循环认识上游池的暂停开关。
    pub fn with_pause_gate(mut self, gate: Arc<dyn cog_core::SchedulerGate>) -> Self {
        self.pause_gate = Some(gate);
        self
    }

    /// 接上原则账：生成端在动手前就能看到管着这次要动的文件的那些原则。
    pub fn with_principle_ledger(
        mut self,
        ledger: Arc<dyn cog_core::PrincipleLedgerStore>,
    ) -> Self {
        self.principle_ledger = Some(ledger);
        self
    }
}

impl Default for CollaborationExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl cog_core::TaskExecutor for CollaborationExecutor {
    fn supports(&self, task_type: &TaskType) -> bool {
        !matches!(task_type, TaskType::WasmSkill)
    }

    async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
        // 这一笔任务名下的 agent 只服务这一次执行：结果交出去之后，没有任何
        // 一条路会再回来读它们。回收因此必须落在这个入口上，而不是等调用方
        // 各自的任务终态钩子——分解、原子执行、员工分派三条路都从这个入口出，
        // 但只有 DAG 那一条有终态钩子；另两条（例如分解任务，它连任务表都不进）
        // 永远等不到那一声，每跑一轮就在进程里留下几个只会阻塞在 inbox 上的
        // 消费者，而管理器对它们持有强引用。实测 2026-10-18：进化进程启动五十
        // 八分钟就攒下 3622 个阻塞在 xreadgroup 上的消费者，而 DAG 一共只见过
        // 1792 个任务；分解链与自信号链的 agent 从来没有人退过。
        //
        // 失败也回收，哪怕排队重试会在同一个任务名下再执行一次。那一次重新建
        // 对象，有续跑点就从续跑点恢复（`create_role_agent` 建完即
        // `restore_from_id`），没有就从零重跑——代价是一次重建和一条新连接；
        // 而留着不回收的代价，是每个失败过的任务在名册上永远多几个只会阻塞在
        // inbox 上的消费者。按前者错，错得可恢复。
        //
        // DAG 的终态钩子里写着「重试应该接同一个对象」，那条规则的前提是终态
        // 钩子等得到重试走完；分解链与自信号链根本没有钩子可等，它们的失败会
        // 把对象留在名册里到进程结束。两害相权，取可恢复的那一个。
        let result = self.execute_unretired(task).await;
        self.release_task_agents(&task.id).await;
        result
    }
}

impl CollaborationExecutor {
    /// [`cog_core::TaskExecutor::execute`] 的主体，不含回收。
    ///
    /// 池级闸门在这里，不在各条执行的内部：入口之下三条路（意图判定、分解、原子
    /// 执行）都要调上游，而闸门要挡下的正是“整条链跑起来才发现第一次调用被拒”。
    /// 见 [`crate::upstream_gate`]。
    async fn execute_unretired(&self, task: &Task) -> SFResult<TaskResult> {
        if let Some(wait) = crate::upstream_gate::blocked_now() {
            return Err(crate::upstream_gate::refusal(wait));
        }
        let result = self.run_the_task(task).await;
        match &result {
            // 一次被服务的执行是池子这一面还有成员能干活的最直接的证据。
            Ok(_) => crate::upstream_gate::served(),
            Err(e) => {
                if let Some(wait) = crate::upstream_gate::trip_from(&e.to_string()) {
                    crate::upstream_gate::refused(wait);
                }
            }
        }
        result
    }

    async fn run_the_task(&self, task: &Task) -> SFResult<TaskResult> {
        // A work item goes to an employee on the roster, not through the
        // Squad→PGE→SelfReview codegen pipeline: the item says what to do with
        // its material, and routing it through a squad would spend a full
        // planner/generator/evaluator run on one answer.
        //
        // This kind, and the ones like it (`self_evolution`,
        // `baseline_port_rework`), are platform-internal entry points: they are
        // dispatched by name here, before anything consults the executor table,
        // and they are not rows of it. A row says which kind of hand runs a
        // sub-task; these say which of the platform's own flows produced a task.
        if let TaskType::Custom(kind) = &task.task_type {
            if kind == PLATFORM_AGENT_TASK {
                return self.execute_agent_task(task).await;
            }
        }
        // is_executable == false means this is an original overall task
        // (placeholder injected by ActionPlanner) that needs decomposition.
        // is_executable == true means this is an atomic/executable task that
        // should be executed directly via Squad, where the Planner produces an
        // execution plan for the Generator.
        if !task.is_executable {
            self.execute_decomposition(task).await
        } else {
            self.execute_atomic_via_squad(task).await
        }
    }

    /// 放掉一个任务名下的 agent。
    ///
    /// 没接上管理器就什么都不做：`with_agent_manager` 与其它装配走同一条路，
    /// 缺了这一格回收就不存在——但那与「这个任务没有 agent」在行为上是同一
    /// 件事，只有名册会一直涨，而名册的年龄读数正是为这条缺口准备的。
    async fn release_task_agents(&self, task_id: &str) {
        let Some(manager) = self.agent_manager.as_ref() else {
            return;
        };
        match manager.retire_task_agents(task_id).await {
            Ok(retired) if retired > 0 => tracing::info!(
                task_id,
                retired,
                "released the agents of a task that finished executing"
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!(
                task_id,
                "could not release the agents of a task that finished executing: {e}"
            ),
        }
    }

    /// Hand a work item to an employee on the roster and return its answer.
    ///
    /// Nothing about the work is decided here. The input document carries the
    /// material, what to do with it and how the answer will be judged — a
    /// `contract` on it renders as the run's instructions — while the
    /// employee brings the loop and the tools. A branch that knows how to judge
    /// one kind of material is an executor with a name; a branch that forwards
    /// a work item is dispatch, and dispatch belongs to whoever is on the
    /// roster rather than to a role this crate would have to spell out.
    async fn execute_agent_task(&self, task: &Task) -> SFResult<TaskResult> {
        let Some(manager) = self.agent_manager.as_ref() else {
            return Err(SFError::Agent(
                "platform_agent_task requires an agent manager".into(),
            ));
        };

        let (agent_id, agent) = self.employee_for(manager.as_ref(), task).await?;
        // 记在接活的那一刻，不记在完成时：走了之后才失败的那一件同样占了这个
        // 人的一次轮转，按结局记会把「他接了很多都没做完」读成「他没接活」。
        crate::observable::global_observable().record_work_item_taken(&agent_id);
        // 这件活与这个人手上上一件活无关。对话里留着上一件活的材料与步骤，
        // 这一轮模型就会拿它们当上下文：钱多付一遍，判词也可能被上一件带偏。
        // 重置只清对话，不动它装配时带上的工具、钩子与账。
        agent.reset().await?;
        let answer = agent.prompt_for_task(&task.id, task.input.clone()).await?;
        info!(
            task_id = %task.id,
            agent_id = %agent_id,
            employee = %cog_core::display_name_for(&agent_id),
            "platform_agent_task answered"
        );
        Ok(TaskResult {
            success: true,
            output: answer,
            // The executor of this task is a person on the roster, not the
            // branch that routed it: a result that says `platform_agent_task`
            // names the desk and loses who was sitting at it.
            metadata: TaskResultMetadata::new(format!("agent:{agent_id}")),
        })
    }

    /// The employee that takes this work item.
    ///
    /// The task's own `agent_id` names one, then `input.agent_id`; without
    /// either the work goes to the next employee in rotation, because an item
    /// that only one employee ever picks up is that employee's queue under
    /// another name.
    async fn employee_for(
        &self,
        manager: &dyn cog_core::AgentManager,
        task: &Task,
    ) -> SFResult<(String, Arc<dyn cog_core::Agent>)> {
        let named = task
            .agent_id
            .as_deref()
            .or_else(|| task.input.get("agent_id").and_then(|v| v.as_str()))
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(id) = named {
            let agent = manager.get_agent(id).await?.ok_or_else(|| {
                SFError::Agent(format!(
                    "the work names employee {id}, who is not on the roster"
                ))
            })?;
            return Ok((id.to_string(), agent));
        }

        // 只从**常驻**员工里挑。名册上还挂着为某件 DAG 任务现起的对象：它们正
        // 在那件任务的对话里，接过一件无关的活要么排在它后面，要么让两边读到
        // 彼此的步骤。`owner_task` 为空的那些才是「活着的时间比任何一件活都长」
        // 的员工，也正是这里要找的人。
        let roster: Vec<_> = manager
            .list_workers()
            .await?
            .into_iter()
            .filter(|worker| worker.owner_task.is_none())
            .collect();
        if roster.is_empty() {
            return Err(SFError::Agent(
                "no standing employee is on the roster to take the work item".into(),
            ));
        }
        let start = self
            .next_worker
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for step in 0..roster.len() {
            let pick = &roster[(start + step) % roster.len()];
            // 名册上的一行不等于一个能接活的活对象：退役随时发生，轮到的那个人
            // 可能刚好在这之前走了。跳过它，不要因此丢掉这件活。
            if let Some(agent) = manager.get_agent(&pick.agent_id).await? {
                return Ok((pick.agent_id.clone(), agent));
            }
        }
        Err(SFError::Agent(
            "every employee on the roster had retired before one could take the work".into(),
        ))
    }

    /// Decomposition path: run the Planner and take its atomic task list as the
    /// deliverable. No mode selection, no generator, no evaluator — the
    /// deliverable's contract is structural, so the gate on it is structural too
    /// and there is nothing for the other two roles to do.
    async fn execute_decomposition(&self, task: &Task) -> SFResult<TaskResult> {
        // Global retry budget: if DagExecutor has already retried this task
        // up to max_retries, fail fast to prevent cross-layer retry storms.
        if task.retry_count >= task.max_retries {
            return Err(SFError::Agent(format!(
                "Global retry budget exhausted: retry_count={} >= max_retries={}",
                task.retry_count, task.max_retries
            )));
        }

        let profile = derive_task_profile(task);

        let goal = task
            .input
            .get("goal")
            .and_then(|v| v.as_str())
            .unwrap_or(&task.id)
            .to_string();

        // 这里不问模式选择器：它选的是"哪种 PGE 拓扑更适合这个目标"，而分解
        // 根本不跑拓扑。问一次只会拿到一个不会被执行的选择，再把它记进台账，
        // 于是日志与学习数据里出现一条从没发生过的试验。
        info!(task_id=%task.id, "Decomposition run: planner only, no PGE topology");

        let mut squad_executor = SquadExecutor::new();
        if let Some(ref llm) = self.llm_provider {
            squad_executor = squad_executor.with_llm_provider(llm.clone());
        }
        if let Some(ref manager) = self.agent_manager {
            squad_executor = squad_executor.with_agent_manager(manager.clone());
        }
        if let Some(ref hook) = self.hook_engine {
            squad_executor = squad_executor.with_hook_engine(hook.clone());
        }
        if let Some(ref backend) = self.object_backend {
            squad_executor = squad_executor.with_object_backend(backend.clone());
        }
        if let Some(ref reflection) = self.squad_reflection {
            squad_executor = squad_executor.with_squad_reflection(reflection.clone());
        }
        if let Some(ref kb) = self.knowledge_backend {
            squad_executor = squad_executor.with_knowledge_backend(kb.clone());
        }
        if let Some(ref cfg) = self.self_review {
            squad_executor = squad_executor.with_self_review(cfg.clone());
        }
        if let Some(ref schemas) = self.pge_schemas {
            squad_executor = squad_executor.with_pge_schemas(schemas.clone());
        }
        if let Some(ref registry) = self.skill_registry {
            squad_executor = squad_executor.with_skill_registry(registry.clone());
        }
        if let Some(ref backend) = self.state_backend {
            squad_executor = squad_executor.with_state_backend(backend.clone());
        }
        if let Some(ref meta) = self.meta_learning {
            squad_executor = squad_executor.with_meta_learning(meta.clone());
        }
        if let Some(ralph) = self.ralph {
            squad_executor = squad_executor.with_ralph_config(ralph);
        }
        if let Some(max) = self.local_repair_max {
            squad_executor = squad_executor.with_local_repair_max(max);
        }
        if let Some(ref store) = self.stage_store {
            squad_executor = squad_executor.with_stage_store(store.clone());
        }
        if let Some(ref gate) = self.pause_gate {
            squad_executor = squad_executor.with_pause_gate(gate.clone());
        }

        let result = squad_executor
            .execute_squad(
                task.id.clone(),
                SquadConfig {
                    goal,
                    context: Self::goal_context(task),
                    pge_mode: crate::profile::PgeMode::PlanOnly,
                    max_retries: task.max_retries,
                    profile: Some(profile),
                    context_window_size: None,
                    boundary_config: self.boundary_config.clone(),
                    is_self_evolution: false,
                    planner_skill_id: None,
                    generator_skill_id: None,
                    evaluator_skill_id: None,
                },
            )
            .await;

        if !result.success {
            return Err(self.fail_run(task, "collaboration", &result, "Squad execution failed"));
        }

        info!(task_id=%task.id, "Collaboration decomposition succeeded");
        let atomic_tasks = Self::extract_atomic_tasks(&result);
        self.enforce_boundaries(task, &result, &atomic_tasks)?;
        let score = Self::extract_score(&result);
        let sub_task_types = Self::sub_task_types(&atomic_tasks);

        let output = serde_json::json!({
            "atomic_tasks": atomic_tasks,
            "squad_result": &result,
        });

        let mut metadata = TaskResultMetadata::new("collaboration");
        if let Some(s) = score {
            metadata = metadata.with_score(s);
        }
        let task_result = TaskResult {
            success: true,
            output,
            metadata,
        };
        self.archive_execution(task, &task_result);
        self.archive_decomposition(task, sub_task_types);
        Ok(task_result)
    }

    /// Atomic execution path: run the full Squad → PGE → SelfReview pipeline,
    /// but with the Planner producing an execution plan
    /// (steps, approach, boundaries) rather than decomposing into sub-tasks.
    /// Generator executes the plan, Evaluator assesses quality.
    async fn execute_atomic_via_squad(&self, task: &Task) -> SFResult<TaskResult> {
        if task.retry_count >= task.max_retries {
            return Err(SFError::Agent(format!(
                "Global retry budget exhausted: retry_count={} >= max_retries={}",
                task.retry_count, task.max_retries
            )));
        }

        let profile = derive_task_profile(task);

        let goal = task
            .input
            .get("goal")
            .and_then(|v| v.as_str())
            .unwrap_or(&task.id)
            .to_string();

        let is_self_evolution = task.is_self_evolution();

        // The decision is counted by the selector itself; the stage is logged
        // here as well so a reader of this line can see which rule decided
        // without going to the metric plane.
        let decision = self.mode_selector.select_mode(&goal, &profile).await;
        let pge_mode = decision.mode;
        let reason = &decision.reason;

        info!(
            task_id=%task.id,
            ?pge_mode,
            stage=%decision.stage.as_str(),
            %reason,
            self_evolution=%is_self_evolution,
            "ModeSelectorActor decision (atomic execution)"
        );

        let mut squad_executor = SquadExecutor::new();
        if let Some(ref llm) = self.llm_provider {
            squad_executor = squad_executor.with_llm_provider(llm.clone());
        }
        if let Some(ref manager) = self.agent_manager {
            squad_executor = squad_executor.with_agent_manager(manager.clone());
        }
        if let Some(ref hook) = self.hook_engine {
            squad_executor = squad_executor.with_hook_engine(hook.clone());
        }
        if let Some(ref backend) = self.object_backend {
            squad_executor = squad_executor.with_object_backend(backend.clone());
        }
        if let Some(ref reflection) = self.squad_reflection {
            squad_executor = squad_executor.with_squad_reflection(reflection.clone());
        }
        if let Some(ref kb) = self.knowledge_backend {
            squad_executor = squad_executor.with_knowledge_backend(kb.clone());
        }
        if let Some(ref cfg) = self.self_review {
            squad_executor = squad_executor.with_self_review(cfg.clone());
        }
        if let Some(ref schemas) = self.pge_schemas {
            squad_executor = squad_executor.with_pge_schemas(schemas.clone());
        }
        if let Some(ref registry) = self.skill_registry {
            squad_executor = squad_executor.with_skill_registry(registry.clone());
        }
        if let Some(ref backend) = self.state_backend {
            squad_executor = squad_executor.with_state_backend(backend.clone());
        }
        if let Some(ref meta) = self.meta_learning {
            squad_executor = squad_executor.with_meta_learning(meta.clone());
        }
        if let Some(ralph) = self.ralph {
            squad_executor = squad_executor.with_ralph_config(ralph);
        }
        if let Some(max) = self.local_repair_max {
            squad_executor = squad_executor.with_local_repair_max(max);
        }
        if let Some(ref store) = self.stage_store {
            squad_executor = squad_executor.with_stage_store(store.clone());
        }
        if let Some(ref gate) = self.pause_gate {
            squad_executor = squad_executor.with_pause_gate(gate.clone());
        }

        let mut context = Self::goal_context(task);
        if is_self_evolution {
            context = Self::build_self_evolution_context(context, &goal);
            // 原则随上下文进流水线：那条链把它克隆成管线自己那个任务行的 input，
            // 生成端的契约再从 input 里取出这一块摆到稳定半边。规划与评估拿到的是
            // 同一份上下文，因此也会读到这几条——把它们只递给其中一个角色，就得
            // 在这里另造一份上下文，而那一份下一秒就会与这一份漂开。
            if let Some(principles) = self.principles_governing(task).await {
                context[cog_core::INPUT_PRINCIPLES] = principles;
            }
        }

        let squad_start = std::time::Instant::now();
        let result = squad_executor
            .execute_squad(
                task.id.clone(),
                SquadConfig {
                    goal: goal.clone(),
                    context,
                    pge_mode,
                    max_retries: task.max_retries,
                    profile: Some(profile),
                    context_window_size: None,
                    boundary_config: self.boundary_config.clone(),
                    is_self_evolution,
                    planner_skill_id: None,
                    generator_skill_id: None,
                    evaluator_skill_id: None,
                },
            )
            .await;
        let squad_latency_ms = squad_start.elapsed().as_millis() as u64;

        if is_self_evolution {
            let execution_output = Self::extract_execution_result(&result);
            info!(
                task_id=%task.id,
                success=%result.success,
                error=?result.error,
                execution_output=%serde_json::to_string_pretty(&execution_output).unwrap_or_default(),
                "Self-evolution Squad execution completed"
            );
        }

        // Record the Squad outcome for reflection learning regardless of success.
        if let Some(ref engine) = self.reflection_engine {
            let pge_mode_str = result.pge_mode.as_str();
            let score = Self::extract_score(&result).map(|s| s as f32);
            if let Err(e) = engine
                .record_squad_result(
                    &task.id,
                    &goal,
                    result.success,
                    pge_mode_str,
                    score,
                    squad_latency_ms,
                )
                .await
            {
                tracing::warn!(task_id=%task.id, error=%e, "Failed to record squad result");
            }
        }

        if !result.success {
            return Err(self.fail_run(
                task,
                "collaboration_atomic",
                &result,
                "Squad atomic execution failed",
            ));
        }

        info!(task_id=%task.id, "Atomic task execution via Squad succeeded");
        let execution_output = Self::extract_execution_result(&result);
        let score = Self::extract_score(&result);

        // If this is a self-evolution task, extract generated changes and
        // hand them to every ChangeSink (fan-out).
        let mut change_ids = Vec::new();
        if is_self_evolution {
            let outcome = if self.change_sinks.is_empty() {
                tracing::warn!(
                    task_id=%task.id,
                    "Self-evolution task succeeded but no ChangeSink is configured, so any change it produces is dropped"
                );
                ChangeYieldOutcome::NoSink
            } else {
                match Self::extract_changes(
                    &result,
                    &goal,
                    &Self::pge_mode_str(&result.pge_mode),
                    task.input.get("issue_number").and_then(|v| v.as_u64()),
                    &task.id,
                    task.agent_id.as_deref(),
                    ProducerClass {
                        intent: task.evolution_intent(),
                        kind: task.evolution_kind(),
                    },
                ) {
                    // The cause is what came back with the emptiness, and it is
                    // read here rather than discarded: a run that spends a whole
                    // PGE budget and yields nothing to land is a failed
                    // evolution step, but a reader that could not read the
                    // payload and a pipeline that produced nothing are not the
                    // same failure. Counted so neither hides behind a
                    // successful-looking task result.
                    Err(cause) => {
                        tracing::warn!(
                            task_id=%task.id,
                            outcome=cause.as_str(),
                            "Self-evolution run yielded no change artifact; nothing to land"
                        );
                        cause
                    }
                    Ok(changes) => {
                        for change in changes {
                            for sink in &self.change_sinks {
                                match sink.submit_change(change.clone()).await {
                                    Ok(artifact_id) => {
                                        info!(task_id=%task.id, %artifact_id, "Submitted generated change");
                                        change_ids.push(artifact_id);
                                    }
                                    Err(e) => {
                                        tracing::warn!(task_id=%task.id, error=%e, "Failed to submit generated change");
                                    }
                                }
                            }
                        }
                        if change_ids.is_empty() {
                            ChangeYieldOutcome::SubmitFailed
                        } else {
                            ChangeYieldOutcome::Submitted
                        }
                    }
                }
            };
            crate::observable::global_observable()
                .record_change_yield(outcome)
                .await;
        }

        // The list is published even when it is empty. A successful run whose
        // generated change never reached a sink is otherwise indistinguishable
        // from one that had nothing to submit — the key is simply absent — and
        // that omission is where a produced diff goes missing without a reading.
        let output = serde_json::json!({
            "execution_result": execution_output,
            "squad_result": &result,
            "change_ids": change_ids,
        });

        let mut metadata = TaskResultMetadata::new("collaboration_atomic");
        if let Some(s) = score {
            metadata = metadata.with_score(s);
        }
        let task_result = TaskResult {
            success: true,
            output,
            metadata,
        };
        // The pipeline's plan is a decomposition too when it carries sub-tasks:
        // which topology ran says nothing about whether the goal was split. A
        // plan without them — the shape self-evolution runs have — passes an
        // empty list, which the archive does not record.
        self.archive_execution(task, &task_result);
        self.archive_decomposition(
            task,
            Self::sub_task_types(&Self::extract_atomic_tasks(&result)),
        );
        Ok(task_result)
    }

    /// Archive a run that ended without delivering, and produce the error it
    /// returns.
    ///
    /// A failed run is the only production source of the knowledge layer's
    /// failure-pattern namespace: the archive is what writes it, and every
    /// archive call site up to now built a delivered envelope, so the failure
    /// arm of that write was unreachable however many writers the namespace
    /// has. The archive is dispatched here, before the error is returned, and
    /// in the background like every other knowledge write so it cannot decide
    /// whether the task is done.
    ///
    /// Read the configured boundary rules against the decomposition that was
    /// just produced, at the one point where the declaration and the tasks it
    /// is about are both in hand.
    ///
    /// The rules arrive from the `/boundary` configuration section and are the
    /// only place a task-size budget is declared. They are read here rather
    /// than handed to an agent because a rule typed `hard` is the one kind with
    /// a concrete check in code — passing an oversized plan to a model and
    /// asking whether it is oversized makes the answer a second opinion rather
    /// than a verdict, and a budget that only a sampled judgement enforces is
    /// not a budget.
    ///
    /// A violation refuses the decomposition: the plan is not delivered, the
    /// refused run is archived with the violation as its feedback, and the
    /// error carries the same text. That is what makes the rule actionable —
    /// the planner is the one that chose the sub-tasks and their inputs, so the
    /// refusal has to name the dimension, the task and the number for the retry
    /// to have anything to work from. A refusal that only logged would leave
    /// the same oversized plan to be produced again.
    ///
    /// Skill definitions are not part of the shape this path decomposes — the
    /// plan carries task specs, not the skill graph `SkillBoundary` compares
    /// against — so that dimension finds nothing to look up and reports no
    /// verdict. It is an absent reading, not a passing one, and no other
    /// dimension is affected by the empty registry.
    fn enforce_boundaries(
        &self,
        task: &Task,
        result: &SquadResult,
        atomic_tasks: &[AtomicTask],
    ) -> SFResult<()> {
        let observability = global_observable();
        let rules = match self.boundary_config.as_ref() {
            Some(config) => config.rules.as_slice(),
            None => &[],
        };
        // Only a rule that is enabled *and* hard has a check to run; a soft one
        // is the evaluator's, and a disabled one is a declaration the operator
        // has switched off. Neither is a reason to refuse, and neither is a
        // reason to say the plan was checked.
        if !rules
            .iter()
            .any(|rule| rule.enabled && rule.rule_type == cog_core::RuleType::Hard)
        {
            observability.record_boundary_evaluation(BOUNDARY_NO_HARD_RULES);
            return Ok(());
        }

        let skills = cog_core::SkillRegistry::new();
        let report = cog_core::detect_boundaries(atomic_tasks, &skills, rules);
        if report.passed {
            observability.record_boundary_evaluation(BOUNDARY_PASSED);
            return Ok(());
        }

        observability.record_boundary_evaluation(BOUNDARY_VIOLATED);
        for violation in &report.violations {
            observability.record_boundary_violation(&violation.dimension);
        }
        let reason = Self::boundary_refusal(&report);
        tracing::warn!(task_id=%task.id, rules=rules.len(), violations=report.violations.len(),
            "Boundary rules refused the decomposition");
        let mut metadata = TaskResultMetadata::new("collaboration");
        metadata = metadata.with_feedback(&reason);
        self.archive_execution(
            task,
            &TaskResult {
                success: false,
                output: serde_json::json!({
                    "atomic_tasks": atomic_tasks,
                    "squad_result": result,
                    "boundary_report": &report,
                }),
                metadata,
            },
        );
        Err(SFError::Agent(reason))
    }

    /// The text a boundary refusal hands back as failure feedback: every
    /// violation named by dimension, task and measured value, followed by the
    /// evaluator's own suggestion for each. A refusal a retry cannot act on is
    /// only a stall.
    fn boundary_refusal(report: &cog_core::BoundaryReport) -> String {
        let violations = report
            .violations
            .iter()
            .map(|violation| {
                format!(
                    "{}: task '{}': {}",
                    violation.dimension, violation.task_id, violation.message
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        let mut reason = format!(
            "Boundary rules refused the decomposition, {} violation(s): {}",
            report.violations.len(),
            violations
        );
        if !report.suggestions.is_empty() {
            reason.push_str(". Fix: ");
            reason.push_str(&report.suggestions.join("; "));
        }
        reason
    }

    /// The reason archived is the run's own, never `fallback`: `fallback` is a
    /// sentence for the error this returns, and a sentence invented here would
    /// become the root cause the Evaluator reads back for this class of task.
    /// A run that states no reason leaves the field empty — "no cause recorded"
    /// is a reading, "something failed" is not.
    fn fail_run(
        &self,
        task: &Task,
        executor_id: &str,
        result: &SquadResult,
        fallback: &str,
    ) -> SFError {
        let reason = result.error.clone().unwrap_or_default();
        let mut metadata = TaskResultMetadata::new(executor_id);
        if !reason.is_empty() {
            metadata = metadata.with_feedback(&reason);
        }
        self.archive_execution(
            task,
            &TaskResult {
                success: false,
                output: serde_json::json!({ "squad_result": result }),
                metadata,
            },
        );
        SFError::Agent(if reason.is_empty() {
            fallback.to_string()
        } else {
            reason
        })
    }

    /// Archive one run's outcome into the KnowledgeBackend in the background,
    /// whichever way it ended. Failures are logged but never block the task
    /// result.
    fn archive_execution(&self, task: &Task, result: &TaskResult) {
        let Some(ref kb) = self.knowledge_backend else {
            return;
        };
        let kb = kb.clone();
        let task = task.clone();
        let result = result.clone();
        tokio::spawn(async move {
            if let Err(e) = kb.archive_execution(&task, &result).await {
                tracing::warn!(task_id = %task.id, error = %e, "Failed to archive execution");
            }
        });
    }

    /// Archive a delivered decomposition into the KnowledgeBackend in the
    /// background, like [`Self::archive_execution`] and for the same reason:
    /// what the run produced is already in hand, and a knowledge write must
    /// never decide whether the task is done.
    fn archive_decomposition(&self, task: &Task, sub_task_types: Vec<String>) {
        let Some(ref kb) = self.knowledge_backend else {
            return;
        };
        if sub_task_types.is_empty() {
            // Nothing to say about how this class of goal is split. Recording it
            // would put a row in the namespace for a run that decomposed nothing.
            return;
        }
        let kb = kb.clone();
        let task = task.clone();
        tokio::spawn(async move {
            if let Err(e) = kb.archive_decomposition(&task, &sub_task_types).await {
                tracing::warn!(task_id = %task.id, error = %e, "Failed to archive decomposition");
            }
        });
    }

    /// The task input, plus the class of the goal it carries.
    ///
    /// The class has to travel with the goal because the pipeline re-hosts the
    /// goal on tasks of its own making: the decomposition loop plans a synthetic
    /// task whose only job is to hold the goal text, and that task's type names
    /// the loop, not the work. A reader that falls back to the host's type still
    /// gets an answer — under a class that says nothing — so the field is
    /// written here, where the goal is read off its original carrier.
    fn goal_context(task: &Task) -> serde_json::Value {
        let mut context = task.input.clone();
        context[cog_core::GoalClass::INPUT_FIELD] =
            serde_json::json!(cog_core::GoalClass::of(task).value);
        context
    }

    fn pge_mode_str(mode: &crate::profile::PgeMode) -> String {
        mode.as_str().to_string()
    }

    /// 这条活点名的路径上站着门的原则，交给生成端事前看。
    ///
    /// 问的是原则账里「哪几条原则的门写在这些路径上」——一条原则的门就是它会
    /// 在哪里变红，而那正是生成端最需要先知道的事：改这个文件会让哪一条判据
    /// 不满意。
    ///
    /// 任务没点名路径时返回 `None`，而不是把与路径无关的原则全摆进去。账里
    /// 三千多条原则绝大多数没有记录门在哪里，无法回答「管不管这条路」；把它们
    /// 一次全递给模型，等于把一份四千行的清单当作原则指引，而真正管着这条路的
    /// 那几条会淹没在里面。没有路径就没有这一次的答案，这不等于没有原则。
    async fn principles_governing(&self, task: &Task) -> Option<serde_json::Value> {
        let ledger = self.principle_ledger.as_ref()?;
        let paths = named_paths(&task.input);
        if paths.is_empty() {
            return None;
        }
        // 多问一条：恰好问到上限与「账里还多着」是两种情形，只问上限就分不开。
        // 分不开的后果不是少几条，是读的人以为这就是全部——一个被丢掉的第 13 条
        // 与账里根本没有更多条，在上下文里长得一模一样。
        match ledger.gating_paths(&paths, PRINCIPLES_PER_TASK + 1).await {
            Ok(rows) if rows.is_empty() => {
                // 有路径、账也读到了、一条都没匹配上：这是一次真实的「这些文件
                // 上没有记录门」，不是读不到。留痕，免得与「没接上账」混为一谈。
                tracing::info!(
                    task_id = %task.id,
                    paths = ?paths,
                    "no principle records a gate on the paths this task names"
                );
                None
            }
            Ok(rows) => {
                let (rows, more) = carried(rows, PRINCIPLES_PER_TASK);
                if more {
                    tracing::warn!(
                        task_id = %task.id,
                        carried = PRINCIPLES_PER_TASK,
                        paths = ?paths,
                        "the account records a gate on more principles than this round carries; \
                         the ones naming the most of these paths are the ones kept, and this \
                         round should probably be split"
                    );
                }
                match serde_json::to_value(&rows) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        tracing::warn!(
                            task_id = %task.id,
                            error = %e,
                            "principle rows did not serialize"
                        );
                        None
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task.id,
                    error = %e,
                    "the principle account could not be read; this round writes without \
                     the principles that govern its paths"
                );
                None
            }
        }
    }

    fn build_self_evolution_context(mut base: serde_json::Value, goal: &str) -> serde_json::Value {
        // Only mark the mode; generator/evaluator add their own role-specific
        // schemas. Putting detailed change instructions here leaks into the
        // planner and makes it emit XML/artifact markup instead of a plan.
        base[cog_core::INPUT_EVOLUTION_MODE] =
            serde_json::json!(cog_core::EVOLUTION_MODE_GENERATE_CHANGE);
        if base.get("goal").is_none() {
            base["goal"] = serde_json::json!(goal);
        }
        base
    }

    /// Derive a change's identity from the task that produced it plus a digest
    /// of its content.
    ///
    /// The artifact is always named `changes.diff`, so using that name as the
    /// change id made every generated change share one identity: they wrote
    /// the same file on disk, overwrote each other's engine record, and — once
    /// the first one landed — every later change looked "already landed" to
    /// the landing idempotency check and was silently skipped forever. The
    /// task id keeps the id traceable; the content digest keeps distinct
    /// changes distinct while still collapsing an identical re-submission.
    fn change_id_for(task_id: &str, content: &str) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(content.as_bytes());
        let short = digest
            .iter()
            .take(6)
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        format!("{}-{}", task_id, short)
    }

    /// The PGE payload a squad result carries, with the roundtable's storage
    /// envelope removed.
    ///
    /// A debate records each iteration under a `roundtable` key, and a
    /// successful debate hands back that same envelope as its result — unlike
    /// the pipeline, which hands back the pipeline result itself and keeps the
    /// envelope for the history entry only. Every probe below reads the payload
    /// through here: one that probes the bare shapes sees a debate that
    /// produced a diff as a run that produced nothing, and drops the diff while
    /// the task still reports success.
    fn pge_payload(squad_result: &crate::squad::SquadResult) -> Option<serde_json::Value> {
        let payload = squad_result.result.clone()?;
        Some(payload.get("roundtable").cloned().unwrap_or(payload))
    }

    /// The changes a run produced, or the cause of its producing none.
    ///
    /// The empty returns are not one fact. A run that handed back no payload,
    /// a payload that arrived in neither envelope this build knows, a pipeline
    /// that really produced no artifact, and a run that answered the question
    /// with analysis instead of a diff are four causes with four different
    /// readers, and a bare `Vec::new()` throws the difference away at the one
    /// point where it was still known.
    fn extract_changes(
        squad_result: &crate::squad::SquadResult,
        goal: &str,
        pge_mode: &str,
        issue_number: Option<u64>,
        task_id: &str,
        producer: Option<&str>,
        class: ProducerClass<'_>,
    ) -> Result<Vec<cog_core::GeneratedChange>, ChangeYieldOutcome> {
        let mut changes = Vec::new();
        // `pge_payload` reads the result, so it is only `None` when the run
        // handed back no result at all.
        let Some(result_val) = Self::pge_payload(squad_result) else {
            return Err(ChangeYieldOutcome::PayloadMissing);
        };

        let artifacts: Vec<crate::squad::pge::types::Artifact> = if let Ok(pipeline) =
            serde_json::from_value::<crate::PgePipelineResult>(result_val.clone())
        {
            pipeline.final_generation.artifacts
        } else if let Ok(roundtable) =
            serde_json::from_value::<crate::PgeRoundtableResult>(result_val.clone())
        {
            roundtable.final_generation.artifacts
        } else {
            // A payload this build cannot read is not a run that produced
            // nothing: the envelope it arrived in is what moved.
            return Err(ChangeYieldOutcome::PayloadUnreadable);
        };

        if artifacts.is_empty() {
            return Err(ChangeYieldOutcome::NoArtifacts);
        }

        for artifact in artifacts {
            if !artifact.is_change() {
                continue;
            }

            let affected_files = match cog_core::parse_diff_affected_files(&artifact.content) {
                Ok(files) => files,
                Err(e) => {
                    tracing::warn!(
                        artifact=%artifact.name,
                        error=%e,
                        "Generated change artifact does not look like a valid unified diff"
                    );
                    Vec::new()
                }
            };

            let change_id = Self::change_id_for(task_id, &artifact.content);
            changes.push(cog_core::GeneratedChange {
                change_id,
                goal: goal.into(),
                content: artifact.content,
                affected_files,
                rationale: None,
                pge_mode: pge_mode.into(),
                self_review_score: Self::extract_score(squad_result).map(|s| s as f32),
                issue_number,
                intent: class.intent,
                // The spelling survives alongside the class it produced: the
                // funnel asks the table again whenever the spelling is there,
                // so a record written under a wrong arm is corrected by the
                // fix rather than outliving it.
                kind: class.kind.map(str::to_string),
                producer: producer.map(str::to_string),
            });
        }

        if changes.is_empty() {
            // Artifacts came back and not one of them was a diff, so the run
            // answered a different question than the one it was asked.
            return Err(ChangeYieldOutcome::NonChangeArtifacts);
        }

        Ok(changes)
    }

    fn extract_score(squad_result: &crate::squad::SquadResult) -> Option<f64> {
        if let Some(result_val) = Self::pge_payload(squad_result) {
            // 分解路径的判定是结构性的二值（拿到任务列表 / 没拿到），这里是
            // 把它翻成下游门槛吃的 0..1 分数，不是给产出质量打分。
            if let Ok(plan_run) =
                serde_json::from_value::<crate::squad::PlanRunResult>(result_val.clone())
            {
                return plan_run.evaluation.score.map(|s| s as f64 / 100.0);
            }
            if let Ok(pipeline) =
                serde_json::from_value::<crate::PgePipelineResult>(result_val.clone())
            {
                return pipeline.final_evaluation.score.map(|s| s as f64 / 100.0);
            }
            if let Ok(roundtable) =
                serde_json::from_value::<crate::PgeRoundtableResult>(result_val.clone())
            {
                return roundtable
                    .final_outcome
                    .judgement()
                    .and_then(|e| e.score)
                    .map(|s| s as f64 / 100.0);
            }
        }
        None
    }

    fn extract_execution_result(squad_result: &crate::squad::SquadResult) -> serde_json::Value {
        if let Some(result_val) = Self::pge_payload(squad_result) {
            // 分解路径：计划是交付物，没有生成物可言。
            if let Ok(plan_run) =
                serde_json::from_value::<crate::squad::PlanRunResult>(result_val.clone())
            {
                return serde_json::json!({
                    "plan": plan_run.plan,
                    "evaluation": plan_run.evaluation,
                });
            }
            // Try PgePipelineResult first.
            if let Ok(pipeline) =
                serde_json::from_value::<crate::PgePipelineResult>(result_val.clone())
            {
                return serde_json::json!({
                    "plan": pipeline.final_plan,
                    "generation": pipeline.final_generation,
                    "evaluation": pipeline.final_evaluation,
                });
            }
            // Try PgeRoundtableResult.
            if let Ok(roundtable) =
                serde_json::from_value::<crate::PgeRoundtableResult>(result_val.clone())
            {
                return serde_json::json!({
                    "plan": roundtable.final_plan,
                    "generation": roundtable.final_generation,
                    "outcome": roundtable.final_outcome,
                });
            }
        }
        serde_json::Value::Null
    }

    fn extract_atomic_tasks(squad_result: &crate::squad::SquadResult) -> Vec<AtomicTask> {
        let mut tasks = Vec::new();
        if let Some(result_val) = Self::pge_payload(squad_result) {
            // 分解路径的产物形状：计划本身就是交付物。
            if let Ok(plan_run) =
                serde_json::from_value::<crate::squad::PlanRunResult>(result_val.clone())
            {
                for spec in &plan_run.plan.sub_tasks {
                    tasks.push(Self::task_spec_to_atomic(spec));
                }
                return tasks;
            }
            // Try to parse as PgePipelineResult first.
            if let Ok(pipeline) =
                serde_json::from_value::<crate::PgePipelineResult>(result_val.clone())
            {
                for spec in &pipeline.final_plan.sub_tasks {
                    tasks.push(Self::task_spec_to_atomic(spec));
                }
                return tasks;
            }
            // Try PgeRoundtableResult.
            if let Ok(roundtable) =
                serde_json::from_value::<crate::PgeRoundtableResult>(result_val.clone())
            {
                for spec in &roundtable.final_plan.sub_tasks {
                    tasks.push(Self::task_spec_to_atomic(spec));
                }
            }
        }
        tasks
    }

    /// The sub-task types a decomposition produced, as the namespace records
    /// them. The atomic list is the platform's own reading of "what this goal
    /// was split into", so the pattern is derived from it rather than from the
    /// plan JSON a second time.
    fn sub_task_types(atomic_tasks: &[AtomicTask]) -> Vec<String> {
        atomic_tasks
            .iter()
            .filter_map(|task| task.skill_id.clone())
            .filter(|task_type| !task_type.is_empty())
            .collect()
    }

    fn task_spec_to_atomic(spec: &crate::TaskSpec) -> AtomicTask {
        AtomicTask {
            id: spec.id.clone(),
            name: spec.name.clone(),
            skill_id: Some(spec.task_type.clone()),
            description: None,
            estimated_tokens: Self::estimate_tokens(&spec.input),
            skill_gap: false,
            blocked_by: spec.blocked_by.clone(),
            blocks: vec![],
            input: spec.input.clone(),
            output_entities: vec![],
            estimated_seconds: Self::estimate_seconds(&spec.name, &spec.input),
            // 验收标准随计划一起来：计划说得出每一片怎样算做完，这里就照抄；
            // 说不出就是空的，分解门按它的判据整份打回——空着从来不是可以蒙过去
            // 的形状，只是此前计划这一侧根本没有字段能装它。
            parent_id: None,
            acceptance: spec.acceptance.clone(),
        }
    }

    /// Ceiling on [`Self::estimate_tokens`], so a pathological input cannot
    /// produce an absurd estimate.
    ///
    /// It has to sit above every threshold a boundary rule may declare. The
    /// estimate is the only reading the `TokenBudget` rule compares against, so
    /// a cap below a declared threshold makes that threshold unreachable by
    /// construction: the rule is declared, enabled, evaluated — and can never
    /// fire, which is indistinguishable from a plan that never exceeded it.
    /// Twice the largest threshold the shipped configurations declare.
    // cap: derived — 取的是出货配置里声明过的最大阈值的两倍：一个实打实等于阈值的估计是看不见的失败，与一个从没超过的计划无法区分
    const TOKEN_ESTIMATE_CAP: u64 = 200_000;

    /// Rough token estimate: ~4 chars per token plus 2 000 token overhead for
    /// prompt wrapping / reasoning, capped at [`Self::TOKEN_ESTIMATE_CAP`].
    ///
    /// The cap is the outer bound, not the expected size: it binds only above
    /// 792 000 characters of serialized input, and it used to sit at 50 000,
    /// below the declared budget — which is a cap that saturates exactly in the
    /// regime the budget is about.
    fn estimate_tokens(input: &serde_json::Value) -> u64 {
        let chars = input.to_string().len() as u64;
        (chars / 4)
            .saturating_add(2_000)
            .min(Self::TOKEN_ESTIMATE_CAP)
    }

    /// Rough time estimate based on task name heuristics and input size.
    fn estimate_seconds(name: &str, input: &serde_json::Value) -> u64 {
        let base: u64 = 120;
        let chars = input.to_string().len() as u64;
        let size_factor = chars / 100;
        let type_factor = if name.contains("test") || name.contains("review") {
            180
        } else if name.contains("implement") || name.contains("code") {
            300
        } else {
            240
        };
        base + size_factor + type_factor
    }
}

/// 一次递给生成端的原则条数上限。
///
/// 这是一道**选的界**，不是量出来的容量：这张表记的是哪几条门站在这次要动的
/// 文件上，正常的改动只落在几个文件里，真实匹配数通常是个位数。界在这里是因为
/// 一次任务点名几百个路径时（整仓改名的活会这样）匹配可以是几百条，而几百条
/// 原则正文会把提示词里真正的任务挤到读不到的地方。撞上界说明这次要动的东西
/// 太大，该先拆，不该靠多塞原则解决。
// cap: bound — 选出来的一道界，不是量出来的容量
const PRINCIPLES_PER_TASK: usize = 12;

/// 这一轮真正递出去的那几条，以及账里是不是还多着。
///
/// 两件事分开返回，是因为它们读起来是两件事：`kept` 是这一轮看到的东西，
/// `more` 是「边界之外还有」这句话本身。只返回 `kept` 的话，把全部原则都和盘
/// 托出与只递了前几条，在下游的上下文里一模一样。
fn carried<T>(mut rows: Vec<T>, cap: usize) -> (Vec<T>, bool) {
    if rows.len() <= cap {
        return (rows, false);
    }
    rows.truncate(cap);
    (rows, true)
}

/// 任务点名的那些路径，从哪里点就从哪里读。
///
/// 先读 `named_files`——那是显式声明，一个词就是一个路径。再扫任务输入里其余
/// 的字符串，把看起来像仓库内路径的片段取出来：拒单回炉的任务把编译器指的
/// 位置写在摘要里（`clippy::x at crates/a/src/b.rs:411`），信号链的任务把路径
/// 写在目标描述里，两种都没有 `named_files`，而它们说的是同一件事。
///
/// 只认带扩展名、带一个以上目录分隔符的片段：`main` 这样的词是散文中到处都
/// 有的，认成路径会让每一次任务都匹配到同一批原则。去重并按字典序排，两轮同一
/// 件任务问出同一个顺序——提示词的缓存前缀就靠这一点才留得住。
fn named_paths(input: &serde_json::Value) -> Vec<String> {
    let mut found: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    if let Some(declared) = input.get("named_files").and_then(|v| v.as_array()) {
        for one in declared.iter().filter_map(|v| v.as_str()) {
            if let Some(path) = path_token(one) {
                found.insert(path.to_string());
            }
        }
    }

    let mut scratch = String::new();
    collect_strings(input, &mut scratch);
    for token in scratch.split(|c: char| c.is_whitespace() || "\"'`,;()[]{}<>".contains(c)) {
        if let Some(path) = path_token(token) {
            found.insert(path.to_string());
        }
    }

    // 再把每一个 crate 根也当作一层问法。一条原则记下的门几乎总是 `crates/X/tests/`
    // 里的一个契约测试，而任务点名的是 `crates/X/src/` 里要改的那个文件——两级路径
    // 逐字不同，只按整条路径问，改任何源码都问不出管着这个 crate 的那几条。crate
    // 根是这两级唯一对得上的那一层。
    for crate_root in found
        .iter()
        .filter_map(|path| path.strip_prefix("crates/")?.split('/').next())
        .map(|name| format!("crates/{name}"))
        .collect::<Vec<_>>()
    {
        found.insert(crate_root);
    }

    found.into_iter().collect()
}

/// 一个片段如果是仓库内的路径，返回它的路径本身。
///
/// 编译器与告警把行号缀在路径后面（`crates/a/src/b.rs:411`），那是路径之外的
/// 东西：不剥掉它，扩展名就成了 `rs:411`，一条真实得不能再真实的路径反而不算
/// 路径。只剥纯数字的尾巴，`crates/a/src/window:size` 这种名字一个字符都不动。
fn path_token(token: &str) -> Option<&str> {
    let token = token.trim_matches(|c: char| matches!(c, '.' | ',' | ')' | ';'));
    let token = match token.rsplit_once(':') {
        Some((head, line)) if !line.is_empty() && line.chars().all(|c| c.is_ascii_digit()) => head,
        _ => token,
    };
    let (_, ext) = token.rsplit_once('.')?;
    let known = matches!(
        ext,
        "rs" | "toml" | "yaml" | "yml" | "json" | "md" | "ts" | "tsx" | "js" | "sh" | "sql"
    );
    let looks_right = known
        && token.contains('/')
        && !token.starts_with('/')
        && !token.contains("..")
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.' | '*'));
    looks_right.then_some(token)
}

/// 把一个 JSON 里所有的字符串接起来，用空白隔开——扫描只需要字节。
fn collect_strings(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::String(s) => {
            out.push_str(s);
            out.push(' ');
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_strings(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
                collect_strings(item, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{CollaborationExecutor, ProducerClass, PLATFORM_AGENT_TASK};
    use crate::checkout::repo_root;
    use cog_core::{Observable, Task, TaskType};

    /// Keeps the envelopes the archive is handed, so a test can read back how a
    /// run ended without a store behind it.
    #[derive(Default)]
    struct RecordingArchive {
        archived: std::sync::Mutex<Vec<cog_core::TaskResult>>,
    }

    impl RecordingArchive {
        /// The envelope handed to the archive, once it has arrived. The write is
        /// spawned, so this waits for it rather than assuming it has run.
        async fn archived(&self) -> cog_core::TaskResult {
            for _ in 0..500 {
                if let Some(result) = self.archived.lock().unwrap().first().cloned() {
                    return result;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            panic!("nothing reached the archive");
        }
    }

    /// 计划说得出每一片怎样算做完，这里就照着搬到那一行上。
    ///
    /// 这一格此前是写死的空列表，而计划那一侧根本没有字段能装它：每一片入图
    /// 前都是空验收，分解门按自己的判据整份打回。集群库实测（2026-11-04）六小时
    /// 里 11 条分解全部死在这一条上，没有一条走到入图——而失败原因读起来像
    /// 「planner 质量差」，正是因为它被写成了「这一条路不归门管」的谎。
    #[test]
    fn a_sub_tasks_own_criteria_travel_onto_the_atomic_task() {
        let spec = crate::TaskSpec {
            id: "t1".into(),
            name: "implement the thing".into(),
            task_type: "generator".into(),
            input: serde_json::json!({ "query": "do it" }),
            blocked_by: Vec::new(),
            acceptance: vec!["the test covers both branches".into()],
        };
        let atomic = CollaborationExecutor::task_spec_to_atomic(&spec);
        assert_eq!(
            atomic.acceptance, spec.acceptance,
            "计划说的验收标准必须原样落到任务行上：丢掉它，分解门就永远说不出来这片怎样算做完"
        );
    }

    #[async_trait::async_trait]
    impl cog_core::KnowledgeBackend for RecordingArchive {
        async fn retrieve_relevant(
            &self,
            _task: &cog_core::Task,
            _query: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::KnowledgeEntry>> {
            Ok(Vec::new())
        }

        async fn retrieve_similar_decompositions(
            &self,
            _goal_class: &str,
            _goal: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::TaskDecompositionPattern>> {
            Ok(Vec::new())
        }

        async fn retrieve_similar_implementations(
            &self,
            _task_type: &str,
            _input_summary: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::ImplementationExample>> {
            Ok(Vec::new())
        }

        async fn retrieve_failure_patterns(
            &self,
            _task_type: &str,
            _top_k: usize,
        ) -> cog_core::SFResult<Vec<cog_core::FailurePattern>> {
            Ok(Vec::new())
        }

        async fn retrieve_task_history(
            &self,
            _task_id: &str,
        ) -> cog_core::SFResult<Vec<cog_core::TaskExecutionRecord>> {
            Ok(Vec::new())
        }

        async fn archive_execution(
            &self,
            _task: &cog_core::Task,
            result: &cog_core::TaskResult,
        ) -> cog_core::SFResult<()> {
            self.archived.lock().unwrap().push(result.clone());
            Ok(())
        }

        async fn archive_decomposition(
            &self,
            _task: &cog_core::Task,
            _sub_task_types: &[String],
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    fn squad_failure(reason: Option<&str>) -> crate::squad::SquadResult {
        crate::squad::SquadResult {
            squad_id: "squad-1".into(),
            success: false,
            result: None,
            retry_count: 1,
            error: reason.map(str::to_string),
            pge_mode: crate::profile::PgeMode::Pipeline,
            reflection: None,
        }
    }

    /// A run that failed reaches the archive as a failure, with the reason the
    /// run itself gave. This is the production source of the failure-pattern
    /// namespace: without it the archive's failure arm is unreachable and the
    /// Evaluator's common failures are empty for every task type, in the same
    /// way an empty namespace is.
    #[tokio::test]
    async fn a_failed_run_is_archived_as_a_failure_with_its_own_reason() {
        let archive = std::sync::Arc::new(RecordingArchive::default());
        let executor = CollaborationExecutor::new().with_knowledge_backend(archive.clone());
        let task = cog_core::Task::new(
            "task-1",
            cog_core::TaskType::Generator,
            serde_json::json!({}),
        );

        let error = executor.fail_run(
            &task,
            "collaboration",
            &squad_failure(Some("the planner never produced a task list")),
            "Squad execution failed",
        );

        assert!(
            error
                .to_string()
                .ends_with("the planner never produced a task list"),
            "返回的错误要是这一次运行的原因: {error}"
        );
        let archived = archive.archived().await;
        assert!(!archived.success, "失败要按失败归档");
        assert_eq!(
            archived.metadata.feedback.as_deref(),
            Some("the planner never produced a task list"),
            "失败模式里的因由来自这一次运行，不能换成调用点编的句子"
        );
    }

    /// A run that states no reason has no reason. The sentence the call site
    /// returns as the task's error is not a cause, and archiving it would put a
    /// fabricated root cause in front of the Evaluator for that whole class.
    #[tokio::test]
    async fn a_run_that_states_no_reason_does_not_get_one_invented_for_it() {
        let archive = std::sync::Arc::new(RecordingArchive::default());
        let executor = CollaborationExecutor::new().with_knowledge_backend(archive.clone());
        let task = cog_core::Task::new(
            "task-2",
            cog_core::TaskType::Generator,
            serde_json::json!({}),
        );

        let error = executor.fail_run(
            &task,
            "collaboration_atomic",
            &squad_failure(None),
            "Squad atomic execution failed",
        );

        assert!(
            error.to_string().ends_with("Squad atomic execution failed"),
            "运行没给原因时返回调用点的兜底句子: {error}"
        );
        let archived = archive.archived().await;
        assert!(!archived.success);
        assert_eq!(
            archived.metadata.feedback, None,
            "运行没给因由，归档里就该是空的"
        );
    }

    #[test]
    fn distinct_content_from_distinct_tasks_gets_distinct_ids() {
        let a = CollaborationExecutor::change_id_for("github-pr-58", "diff A");
        let b = CollaborationExecutor::change_id_for("github-pr-59", "diff B");
        assert_ne!(a, b);
        assert!(a.starts_with("github-pr-58-"), "{a}");
    }

    #[test]
    fn the_same_content_from_the_same_task_collapses() {
        // Landing idempotency is keyed on the change id, so a re-submission of
        // an unchanged diff must resolve to the same id.
        assert_eq!(
            CollaborationExecutor::change_id_for("github-pr-58", "diff A"),
            CollaborationExecutor::change_id_for("github-pr-58", "diff A")
        );
    }

    #[test]
    fn changed_content_from_the_same_task_is_a_new_change() {
        // A repair produces different content and must not look "already
        // landed" just because it came from the same task.
        assert_ne!(
            CollaborationExecutor::change_id_for("github-pr-58", "diff A"),
            CollaborationExecutor::change_id_for("github-pr-58", "diff A fixed")
        );
    }

    /// The self-evolution fact has to survive the pipeline re-hosting the goal
    /// on a task of its own making: the actors read `Task::is_self_evolution()`
    /// off that synthetic task, whose type names the loop rather than the work.
    ///
    /// A task can name the fact in its *type* alone — the gateway's create-task
    /// entry point takes `task_type: "self_evolution"` and leaves the input
    /// untouched — so once the rehost has replaced the type, the type no longer
    /// carries it. It has to have been written into the context, where every
    /// later reader looks. Drop that write and the evaluator's self-evolution
    /// path turns back into a paid model call with no line of code changing.
    ///
    /// The rehost below mirrors the loop's: same type substitution, same context
    /// carried as the input.
    #[test]
    fn a_self_evolution_task_that_names_only_its_type_survives_the_rehost() {
        let submitted = Task::new(
            "gw-1",
            TaskType::Custom("self_evolution".into()),
            serde_json::json!({ "goal": "raise the read timeout" }),
        );
        assert!(submitted.is_self_evolution());

        let context = CollaborationExecutor::build_self_evolution_context(
            CollaborationExecutor::goal_context(&submitted),
            "raise the read timeout",
        );
        let rehosted = Task::new(
            "ralph-pipeline-00000000-0000-0000-0000-000000000000",
            TaskType::Custom("ralph_pipeline_goal".into()),
            context,
        );

        assert!(
            rehosted.is_self_evolution(),
            "the marker must reach the rehosted task, or the actors' \
             self-evolution path silently stops firing"
        );
    }

    /// 名册上有一个常驻员工，外加一本按任务回收的账。
    ///
    /// 这两条测的是**入口**上的回收：从 `TaskExecutor::execute` 出去的任何一条
    /// 路，结束时都要按任务名放掉名下对象。员工分派这条路自己不起新对象，所以
    /// 名册写死，账本只记回收。
    struct RetirementRecorder {
        inner: super::employee_dispatch_tests::RosterManager,
        retired: std::sync::Mutex<Vec<String>>,
    }

    impl RetirementRecorder {
        fn new(inner: super::employee_dispatch_tests::RosterManager) -> Self {
            Self {
                inner,
                retired: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn retired(&self) -> Vec<String> {
            self.retired
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::AgentManager for RetirementRecorder {
        async fn create_agent(
            &self,
            agent_id: &str,
            role: &str,
            llm: std::sync::Arc<dyn cog_core::LlmClient>,
            task_id: Option<&str>,
        ) -> cog_core::SFResult<std::sync::Arc<dyn cog_core::Agent>> {
            self.inner.create_agent(agent_id, role, llm, task_id).await
        }

        async fn retire_task_agents(&self, task_id: &str) -> cog_core::SFResult<usize> {
            self.retired
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(task_id.to_string());
            Ok(1)
        }

        async fn dispatch(&self, msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            self.inner.dispatch(msg).await
        }

        async fn list_workers(&self) -> cog_core::SFResult<Vec<cog_core::WorkerInfo>> {
            self.inner.list_workers().await
        }

        async fn shutdown(&self) -> cog_core::SFResult<()> {
            self.inner.shutdown().await
        }

        async fn get_agent(
            &self,
            agent_id: &str,
        ) -> cog_core::SFResult<Option<std::sync::Arc<dyn cog_core::Agent>>> {
            self.inner.get_agent(agent_id).await
        }
    }

    fn work_item() -> cog_core::Task {
        cog_core::Task::new(
            "github-issue-56",
            cog_core::TaskType::Custom(PLATFORM_AGENT_TASK.into()),
            serde_json::json!({ "task": { "kind": "issue", "number": 56 } }),
        )
    }

    /// 一次执行结束（无论成败）就把这次执行名下的对象放掉。
    ///
    /// 判的是**入口**，不是某一条路：分解、原子执行、员工分派都从
    /// `TaskExecutor::execute` 出，而此前只有 DAG 的任务终态钩子会回收，
    /// 分解链与自信号链根本不进任务表，它们的 agent 于是永远留在名册里。
    /// 这条测试走的是最便宜的一条路（员工分派），红了说明回收不在入口上；
    /// 它不区分「回收写在哪条路内部」——那正是要避免的形状。
    #[tokio::test]
    async fn a_run_that_finished_releases_the_task_agents_that_served_it() {
        let employee = std::sync::Arc::new(super::employee_dispatch_tests::RecordingEmployee::new(
            "fix",
        ));
        let manager = std::sync::Arc::new(RetirementRecorder::new(
            super::employee_dispatch_tests::RosterManager::new(vec![("worker-0".into(), employee)]),
        ));
        let executor = CollaborationExecutor::new().with_agent_manager(manager.clone());

        cog_core::TaskExecutor::execute(&executor, &work_item())
            .await
            .expect("名册上有人，一件活要有答复");

        assert_eq!(
            manager.retired(),
            vec!["github-issue-56".to_string()],
            "执行结束了，这次执行名下的对象必须按这个任务的名字被放掉"
        );
    }

    /// 失败的那一次同样要放掉：出错的任务不会再回来用这些对象，留着它们只会
    /// 让重跑接到一个停在半截的旧对象上，而重跑本该从续跑点恢复。
    #[tokio::test]
    async fn a_run_that_failed_releases_the_task_agents_that_served_it() {
        let manager = std::sync::Arc::new(RetirementRecorder::new(
            super::employee_dispatch_tests::RosterManager::new(Vec::new()),
        ));
        let executor = CollaborationExecutor::new().with_agent_manager(manager.clone());

        cog_core::TaskExecutor::execute(&executor, &work_item())
            .await
            .expect_err("名册是空的，没有员工能接这件活");

        assert_eq!(
            manager.retired(),
            vec!["github-issue-56".to_string()],
            "失败也要按任务名放掉；回收挂在成功分支上就等于只有一半"
        );
    }

    // ---- the boundary declaration and the producer it governs -------------

    /// One task spec whose input serializes large enough that the estimator
    /// reports more tokens than `threshold`.
    fn oversized_spec(id: &str, threshold: u64) -> crate::TaskSpec {
        // estimate_tokens is chars/4 + 2000, so this needs more than
        // 4 * (threshold - 2000) characters.
        let chars = (threshold as usize).saturating_mul(4).saturating_add(4_000);
        crate::TaskSpec {
            id: id.to_string(),
            name: "implement the thing".to_string(),
            task_type: "generator".to_string(),
            input: serde_json::json!({ "body": "x".repeat(chars) }),
            blocked_by: Vec::new(),
            acceptance: Vec::new(),
        }
    }

    fn hard_token_budget(threshold: u64, enabled: bool) -> cog_core::BoundaryRule {
        cog_core::BoundaryRule {
            name: "TokenBudget".into(),
            rule_type: cog_core::RuleType::Hard,
            threshold: Some(threshold),
            description: "no task may be estimated above the threshold".into(),
            enabled,
        }
    }

    fn successful_squad() -> crate::squad::SquadResult {
        crate::squad::SquadResult {
            squad_id: "squad-1".into(),
            success: true,
            result: None,
            retry_count: 0,
            error: None,
            pge_mode: crate::profile::PgeMode::PlanOnly,
            reflection: None,
        }
    }

    fn boundary_task() -> cog_core::Task {
        cog_core::Task::new(
            "task-boundary",
            cog_core::TaskType::Generator,
            serde_json::json!({}),
        )
    }

    /// Reads one boundary cell off the process-wide observable. Counters are
    /// monotonic and shared by every test in this binary, so callers compare a
    /// value read before an action against one read after it rather than
    /// asserting a total.
    ///
    /// The comparison has to be `after >= before + 1.0`, not an exact
    /// difference: the outcome is not an argument the caller chooses — it comes
    /// from the closed set the recorder takes by type — so unlike the review
    /// counters, where a test can pass a stage name of its own, there is no way
    /// to give one test a cell no other test writes. Any test that walks a
    /// collaboration records one of these cells too, so an exact delta would be
    /// an assertion about what every other test happened to be doing. The
    /// weaker form still fails a path that records nothing at all, which is the
    /// defect these guard.
    async fn boundary_cell(name: &str, label: &str, value: &str) -> f64 {
        let metrics = crate::observable::global_observable()
            .collect_metrics("D8")
            .await
            .unwrap();
        metrics
            .iter()
            .find(|m| m.name == name && m.labels.get(label).map(String::as_str) == Some(value))
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// The declaration reaches the producer: a hard rule declared in `/boundary`
    /// is read against the decomposition, and a plan that breaks it is refused
    /// with the dimension, the task and the measured value in the text.
    ///
    /// This is the join the earlier defect was missing — the config was loaded,
    /// threaded and stored, and nothing read it, so the two ends could drift
    /// apart with every test still green.
    #[tokio::test]
    async fn a_declared_hard_rule_that_the_decomposition_breaks_is_refused() {
        let threshold = 3_000;
        let archive = std::sync::Arc::new(RecordingArchive::default());
        let executor = CollaborationExecutor::new()
            .with_knowledge_backend(archive.clone())
            .with_boundary_config(crate::BoundaryConfig {
                rules: vec![hard_token_budget(threshold, true)],
            });
        let spec = oversized_spec("t1", threshold);
        let tasks = vec![CollaborationExecutor::task_spec_to_atomic(&spec)];
        assert!(
            tasks[0].estimated_tokens > threshold,
            "the fixture has to actually break the rule: {} vs {threshold}",
            tasks[0].estimated_tokens
        );
        let before: f64 = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_VIOLATED,
        )
        .await;

        let error = executor
            .enforce_boundaries(&boundary_task(), &successful_squad(), &tasks)
            .expect_err("a plan over a declared hard budget must not be delivered");

        let reason = error.to_string();
        assert!(
            reason.contains("TokenBudget"),
            "the dimension is missing: {reason}"
        );
        assert!(
            reason.contains("t1"),
            "the offending task is missing: {reason}"
        );
        assert!(
            reason.contains(&tasks[0].estimated_tokens.to_string())
                && reason.contains(&threshold.to_string()),
            "the refusal has to carry the number it refused over: {reason}"
        );
        assert!(
            reason.contains("Fix:"),
            "the retry needs the evaluator's own suggestion, not just the refusal: {reason}"
        );

        let envelope = archive.archived().await;
        assert!(!envelope.success);
        let feedback = envelope
            .metadata
            .feedback
            .clone()
            .expect("a refused run has to be archived with its reason");
        // The archived text is the refusal itself; the error the caller reads
        // wraps that same text in the variant's own wording.
        assert!(
            reason.ends_with(&feedback),
            "the archive and the caller have to read the same refusal: {feedback:?} vs {reason:?}"
        );
        assert!(feedback.contains("TokenBudget") && feedback.contains("t1"));

        let after: f64 = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_VIOLATED,
        )
        .await;
        assert!(
            after >= before + 1.0,
            "the refusal has to leave its own reading"
        );
        assert!(
            boundary_cell(
                "collab_boundary_violation_total",
                "dimension",
                "TokenBudget"
            )
            .await
                > 0.0,
            "the dimension that refused has to be countable"
        );
    }

    /// The same rule, a plan that fits. Without this the check above would pass
    /// on a gate that refuses everything.
    #[tokio::test]
    async fn a_plan_inside_the_declared_budget_passes() {
        let executor = CollaborationExecutor::new().with_boundary_config(crate::BoundaryConfig {
            rules: vec![hard_token_budget(1_000_000, true)],
        });
        let spec = oversized_spec("t1", 3_000);
        let tasks = vec![CollaborationExecutor::task_spec_to_atomic(&spec)];
        let before = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_PASSED,
        )
        .await;

        executor
            .enforce_boundaries(&boundary_task(), &successful_squad(), &tasks)
            .expect("a plan under the declared budget is delivered");

        let after = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_PASSED,
        )
        .await;
        assert!(after >= before + 1.0);
    }

    /// A rule the operator switched off is not read — and the reading says so,
    /// rather than reporting a plan that was checked. The plan here breaks the
    /// threshold on purpose, so a disabled rule that still fired would fail.
    #[tokio::test]
    async fn a_disabled_hard_rule_is_not_read() {
        let threshold = 3_000;
        let executor = CollaborationExecutor::new().with_boundary_config(crate::BoundaryConfig {
            rules: vec![hard_token_budget(threshold, false)],
        });
        let spec = oversized_spec("t1", threshold);
        let tasks = vec![CollaborationExecutor::task_spec_to_atomic(&spec)];
        let before = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_NO_HARD_RULES,
        )
        .await;

        executor
            .enforce_boundaries(&boundary_task(), &successful_squad(), &tasks)
            .expect("a disabled rule is a declaration, not a check");

        let after = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_NO_HARD_RULES,
        )
        .await;
        assert!(after >= before + 1.0);
    }

    /// A soft rule is the evaluator's, not the code's. Trusting a `soft` name to
    /// mean "check this in Rust" would turn a dimension meant for the model's
    /// judgement into a refusal the operator never asked for.
    #[tokio::test]
    async fn a_soft_rule_is_not_the_code_verdict() {
        let threshold = 3_000;
        let mut rule = hard_token_budget(threshold, true);
        rule.rule_type = cog_core::RuleType::Soft;
        let executor = CollaborationExecutor::new()
            .with_boundary_config(crate::BoundaryConfig { rules: vec![rule] });
        let spec = oversized_spec("t1", threshold);
        let tasks = vec![CollaborationExecutor::task_spec_to_atomic(&spec)];

        executor
            .enforce_boundaries(&boundary_task(), &successful_squad(), &tasks)
            .expect("only a hard rule has a check in code");
    }

    /// No `/boundary` section at all: nothing to read, and the reading says
    /// exactly that instead of "the plan passed".
    #[tokio::test]
    async fn a_plan_is_not_reported_as_checked_when_no_rule_was_read() {
        let executor = CollaborationExecutor::new();
        let tasks = vec![CollaborationExecutor::task_spec_to_atomic(&oversized_spec(
            "t1", 3_000,
        ))];
        let before = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_NO_HARD_RULES,
        )
        .await;

        executor
            .enforce_boundaries(&boundary_task(), &successful_squad(), &tasks)
            .expect("an unconfigured boundary section cannot refuse anything");

        let after = boundary_cell(
            "collab_boundary_evaluation_total",
            "outcome",
            crate::observable::BOUNDARY_NO_HARD_RULES,
        )
        .await;
        assert!(after >= before + 1.0);
    }

    /// The declaration and the producer are joined: every `TokenBudget` the
    /// shipped configurations declare has to be a threshold the estimator can
    /// exceed, otherwise the rule is evaluated on every decomposition and can
    /// never fire — which reads exactly like a plan that never broke it.
    ///
    /// All three carriers of the same declaration are walked, because a
    /// threshold lowered in one and left unreachable in another is the drift
    /// this is here to catch. The k3s carrier is YAML, so its embedded document
    /// is extracted the way the delivery check extracts it.
    #[test]
    fn the_estimator_reaches_every_token_budget_the_shipped_configs_declare() {
        let root = repo_root();
        let k3s_configmap = format!(
            "deploy/k3s/{}-configmap.yaml",
            cog_core::topology::CONFIG_CONFIGMAP
        );
        let carriers = [
            "cogneva.example.json",
            "deploy/helm/cogneva/files/cogneva.json",
            k3s_configmap.as_str(),
        ];
        let mut declared = Vec::new();
        for carrier in carriers {
            let path = root.join(carrier);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
            let document = if carrier.ends_with(".yaml") {
                embedded_json(&text)
            } else {
                text
            };
            let parsed: serde_json::Value = serde_json::from_str(&document)
                .unwrap_or_else(|e| panic!("{carrier} is not readable JSON: {e}"));
            let rules = parsed
                .get("boundary")
                .and_then(|section| section.get("rules"))
                .and_then(|rules| rules.as_array())
                .unwrap_or_else(|| panic!("{carrier} declares no /boundary rules"));
            for rule in rules {
                if rule.get("name").and_then(|n| n.as_str()) != Some("TokenBudget") {
                    continue;
                }
                if rule.get("enabled").and_then(|e| e.as_bool()) != Some(true) {
                    continue;
                }
                if rule.get("rule_type").and_then(|t| t.as_str()) != Some("hard") {
                    continue;
                }
                let threshold = rule
                    .get("threshold")
                    .and_then(|t| t.as_u64())
                    .unwrap_or_else(|| {
                        panic!("{carrier} declares a TokenBudget with no threshold")
                    });
                declared.push((carrier, threshold));
            }
        }
        assert!(
            !declared.is_empty(),
            "the shipped configurations declare no hard TokenBudget rule, so the check \
             in code has no input and this gate would pass over an empty set"
        );
        for (carrier, threshold) in &declared {
            assert!(
                *threshold < CollaborationExecutor::TOKEN_ESTIMATE_CAP,
                "{carrier} declares a TokenBudget of {threshold}, which the estimator cannot \
                 reach: it saturates at {} and the rule compares with a strict greater-than",
                CollaborationExecutor::TOKEN_ESTIMATE_CAP
            );
        }
    }

    /// The check runs on the path that produces the tasks it judges.
    ///
    /// Everything above this calls `enforce_boundaries` directly, so none of it
    /// can tell whether the decomposition path still calls it — a deleted call
    /// site leaves every one of those tests green while the rules go unread
    /// again. The read is of the call, which is the link no unit test can see.
    #[test]
    fn the_decomposition_path_hands_its_tasks_to_the_boundary_gate() {
        let source = include_str!("collaboration_executor.rs");
        // Everything above this module is the production surface; the rest of
        // the file is this test, where the same call appears as a literal.
        let production = source
            .split("\n#[cfg(test)]")
            .next()
            .expect("the file has no production half");
        let after = production
            .split("let atomic_tasks = Self::extract_atomic_tasks(&result);")
            .nth(1)
            .expect("the decomposition path no longer extracts its atomic tasks");
        let call = after
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with("//"))
            .unwrap_or_default();
        assert_eq!(
            call, "self.enforce_boundaries(task, &result, &atomic_tasks)?;",
            "the boundary rules are read somewhere else than where the tasks are produced, \
             or not at all"
        );
        assert_eq!(
            production.matches("self.enforce_boundaries(").count(),
            1,
            "more than one call site means the gate below reads a call and the run uses another"
        );
    }

    /// The document embedded in the k3s ConfigMap: the block scalar under
    /// `cogneva.json:`, dedented by its own indentation.
    fn embedded_json(manifest: &str) -> String {
        let (_, body) = manifest
            .split_once("cogneva.json: |")
            .expect("the k3s carrier holds no cogneva.json block scalar");
        let lines: Vec<&str> = body.lines().skip(1).collect();
        let indent = lines
            .iter()
            .find(|line| !line.trim().is_empty())
            .map(|line| line.len() - line.trim_start().len())
            .expect("the block scalar is empty");
        lines
            .iter()
            .map(|line| line.get(indent..).unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn roundtable_squad_result(
        artifacts: Vec<crate::squad::pge::types::Artifact>,
    ) -> crate::squad::SquadResult {
        use crate::squad::pge::types::{
            EvaluationResult, GeneratorOutput, PlannerOutput, RoundOutcome, Verdict,
        };

        let roundtable = crate::PgeRoundtableResult {
            iterations: 1,
            consensus_reached: true,
            final_plan: PlannerOutput {
                summary: String::new(),
                plan: serde_json::json!({}),
                sub_tasks: Vec::new(),
                acceptance_criteria: Vec::new(),
                targets: Vec::new(),
            },
            final_generation: GeneratorOutput {
                content: serde_json::json!({}),
                artifacts,
            },
            final_outcome: RoundOutcome::Judged {
                evaluation: EvaluationResult {
                    verdict: Verdict::Pass,
                    feedback: "looks good".into(),
                    score: Some(85),
                    criteria: Vec::new(),
                    details: None,
                },
            },
            history: Vec::new(),
            context_board: None,
            terminal_reason: None,
        };

        crate::squad::SquadResult {
            squad_id: "squad-1".into(),
            success: true,
            // A debate hands its rounds back under `roundtable`, and a
            // successful debate hands back that same envelope as its result.
            result: Some(serde_json::json!({ "roundtable": roundtable })),
            retry_count: 0,
            error: None,
            pge_mode: crate::profile::PgeMode::Roundtable,
            reflection: None,
        }
    }

    /// The diff a debate produced has to reach the sink. The envelope above is
    /// the debate's own result, so a probe that only tries the bare shapes
    /// reads the run as having produced nothing: the change is dropped, no
    /// landing record is written, and the task still reports success.
    #[test]
    fn a_roundtable_change_survives_the_envelope_it_arrives_in() {
        use crate::squad::pge::types::Artifact;

        let diff = "--- a/crates/cog-core/src/lib.rs\n\
                    +++ b/crates/cog-core/src/lib.rs\n\
                    @@ -1 +1 @@\n\
                    -old\n\
                    +new\n";
        let squad_result = roundtable_squad_result(vec![Artifact {
            name: "change.diff".into(),
            content: diff.into(),
            artifact_type: "change".into(),
        }]);

        let changes = CollaborationExecutor::extract_changes(
            &squad_result,
            "goal",
            "roundtable",
            None,
            "task-1",
            None,
            ProducerClass {
                intent: None,
                kind: None,
            },
        )
        .expect("the diff the debate produced must reach the sink");

        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].affected_files,
            vec!["crates/cog-core/src/lib.rs".to_string()]
        );
    }

    /// 产出这条变更的员工要跟着变更走。落地发生在任务行消失之后，那一步
    /// 没有别的地方能查到是谁做的；漏记这一位，提交就只剩平台署名。
    #[test]
    fn a_change_carries_the_agent_that_produced_it() {
        use crate::squad::pge::types::Artifact;

        let diff = "--- a/crates/cog-core/src/lib.rs\n\
                    +++ b/crates/cog-core/src/lib.rs\n\
                    @@ -1 +1 @@\n\
                    -old\n\
                    +new\n";
        let squad_result = roundtable_squad_result(vec![Artifact {
            name: "change.diff".into(),
            content: diff.into(),
            artifact_type: "change".into(),
        }]);

        let changes = CollaborationExecutor::extract_changes(
            &squad_result,
            "goal",
            "roundtable",
            None,
            "task-1",
            Some("luna-1822ab0e"),
            ProducerClass {
                intent: None,
                kind: None,
            },
        )
        .expect("the diff the debate produced must reach the sink");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].producer.as_deref(), Some("luna-1822ab0e"));
    }

    /// 分类表的输入要跟着变更一起走，而不是只留下它当天给出的答案。
    /// 漏斗在读的时候会拿这个拼写重问一次表：少了它，修好一条错的分支
    /// 只对新记录生效，盘上那批旧标签就得靠人工回填。
    #[test]
    fn a_change_carries_the_kind_the_table_was_asked_about() {
        use crate::squad::pge::types::Artifact;

        let diff = "--- a/crates/cog-core/src/lib.rs\n\
                    +++ b/crates/cog-core/src/lib.rs\n\
                    @@ -1 +1 @@\n\
                    -old\n\
                    +new\n";
        let squad_result = roundtable_squad_result(vec![Artifact {
            name: "change.diff".into(),
            content: diff.into(),
            artifact_type: "change".into(),
        }]);

        let changes = CollaborationExecutor::extract_changes(
            &squad_result,
            "goal",
            "roundtable",
            None,
            "task-1",
            None,
            ProducerClass {
                intent: Some(cog_core::EvolutionIntent::SelfEvolution),
                kind: Some("change_ledger"),
            },
        )
        .expect("the diff the debate produced must reach the sink");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind.as_deref(), Some("change_ledger"));
        assert_eq!(
            changes[0].intent,
            Some(cog_core::EvolutionIntent::SelfEvolution)
        );
    }

    /// The reading that the dropped diff was missing: the score and the
    /// execution envelope have to survive the same probe, or a run that
    /// produced a change is indistinguishable from one that produced nothing
    /// even after the change itself is recovered.
    #[test]
    fn a_roundtable_run_reports_its_score_and_plan() {
        let squad_result = roundtable_squad_result(Vec::new());

        assert_eq!(
            CollaborationExecutor::extract_score(&squad_result),
            Some(0.85)
        );
        assert_ne!(
            CollaborationExecutor::extract_execution_result(&squad_result),
            serde_json::Value::Null
        );
    }
}

#[cfg(test)]
mod named_paths_tests {
    use super::named_paths;

    /// 显式声明过的路径一个不少地读出来——这条是整件事的主路：任务说了要动
    /// 哪些文件，原则就按那些文件去问。
    #[test]
    fn declared_files_are_read_as_they_are_written() {
        let paths = named_paths(&serde_json::json!({
            "goal": "修一处 lint",
            "named_files": ["crates/bootstrap/src/main.rs", "crates/cog-core/src/types/task.rs"],
        }));
        assert_eq!(
            paths,
            vec![
                "crates/bootstrap".to_string(),
                "crates/bootstrap/src/main.rs".to_string(),
                "crates/cog-core".to_string(),
                "crates/cog-core/src/types/task.rs".to_string(),
            ],
            "点名的文件与它所在的 crate 都该被问一遍：门几乎总记在 crate 的 tests/ 里"
        );
    }

    /// 没声明 `named_files` 的那几种任务，路径写在散文里：拒单回炉把编译器的
    /// 报点抄进摘要，信号链把目标路径写进描述。读不到这两处，绝大多数自进化
    /// 任务就永远问不出任何一条原则。
    #[test]
    fn a_path_written_inside_a_sentence_is_still_a_path() {
        let paths = named_paths(&serde_json::json!({
            "failure_digest": "clippy::unnecessary_map_or at crates/bootstrap/src/main.rs:411",
            "reason": "the check named `deploy/helm/cogneva/values.yaml` twice",
        }));
        assert_eq!(
            paths,
            vec![
                "crates/bootstrap".to_string(),
                "crates/bootstrap/src/main.rs".to_string(),
                "deploy/helm/cogneva/values.yaml".to_string(),
            ]
        );
    }

    /// 散文里的词不是路径。`main`、`src`、`list.rs` 这些一旦被当成路径，每一次
    /// 任务都会匹配到同一批原则，注入就退化成一段与这次改动无关的噪声。
    #[test]
    fn a_word_that_looks_like_a_name_is_not_a_path() {
        let paths = named_paths(&serde_json::json!({
            "goal": "make the main loop stop早退 and keep list.rs correct",
            "notes": ["see 1.2.3 and /etc/passwd and ../outside/lib.rs"],
        }));
        assert!(paths.is_empty(), "散文里的词被读成了路径：{paths:?}");
    }

    /// 同一个文件被点两次只问一次；两轮之间顺序一样，提示词的缓存前缀才留得住。
    #[test]
    fn the_same_path_is_asked_about_once_and_always_in_the_same_order() {
        let a = named_paths(&serde_json::json!({
            "named_files": ["crates/b/src/x.rs", "crates/a/src/y.rs"],
            "goal": "also crates/b/src/x.rs and crates/a/src/y.rs",
        }));
        let b = named_paths(&serde_json::json!({
            "goal": "crates/a/src/y.rs then crates/b/src/x.rs",
            "named_files": ["crates/a/src/y.rs"],
        }));
        assert_eq!(
            a,
            vec![
                "crates/a".to_string(),
                "crates/a/src/y.rs".to_string(),
                "crates/b".to_string(),
                "crates/b/src/x.rs".to_string(),
            ]
        );
        assert_eq!(a, b, "两次问出的顺序不一样，缓存前缀每一轮都会被丢掉");
    }
}

#[cfg(test)]
mod carried_tests {
    use super::carried;

    /// 没撞上界时一条不少，也不说「还多着」。
    #[test]
    fn everything_is_carried_when_it_fits() {
        let (kept, more) = carried(vec![1, 2, 3], 12);
        assert_eq!(kept, vec![1, 2, 3]);
        assert!(!more, "账里只有三条，却说了「还多着」");
    }

    /// 撞上界时递出去的正好是上限条，且「还多着」这句话必须出现：一条被丢掉的
    /// 原则与账里根本没有更多条，在生成端读到的上下文里长得一模一样，静默截断
    /// 就是让生成端以为自己看到了全部。
    #[test]
    fn a_bound_is_carried_and_said() {
        let (kept, more) = carried((0..13).collect(), 12);
        assert_eq!(kept.len(), 12);
        assert!(more, "还有一条没递出去，却什么都没说");
    }

    /// 恰好等于上限不是「还多着」。
    #[test]
    fn exactly_the_bound_is_not_more() {
        let (kept, more) = carried((0..12).collect(), 12);
        assert_eq!(kept.len(), 12);
        assert!(!more, "恰好装下，却报成了截断");
    }
}

#[cfg(test)]
mod employee_dispatch_tests {
    use super::*;

    /// 只答 `prompt` 的员工：`prompt_for_task` 的默认实现会转到这里，
    /// 所以「活有没有到这个人手上」与「他读到的是什么」都能在这里读到。
    pub(super) struct RecordingEmployee {
        answer: String,
        seen: std::sync::Mutex<Vec<serde_json::Value>>,
        /// 每次接活前被要求清空对话的次数，按发生顺序与前缀记。
        resets: std::sync::Mutex<usize>,
    }

    impl RecordingEmployee {
        pub(super) fn new(answer: &str) -> Self {
            Self {
                answer: answer.to_string(),
                seen: std::sync::Mutex::new(Vec::new()),
                resets: std::sync::Mutex::new(0),
            }
        }

        fn seen(&self) -> Vec<serde_json::Value> {
            self.seen.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }

        fn resets(&self) -> usize {
            *self.resets.lock().unwrap_or_else(|e| e.into_inner())
        }
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for RecordingEmployee {
        async fn prompt(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            self.seen
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(input);
            Ok(serde_json::json!({ "answer": self.answer }))
        }

        async fn start(&self) {}

        async fn snapshot(
            &self,
            _task_id: String,
        ) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
            unimplemented!("这段测试不读检查点")
        }

        async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
            unimplemented!("这段测试不恢复检查点")
        }

        async fn continue_(
            &self,
            _input: serde_json::Value,
        ) -> cog_core::SFResult<serde_json::Value> {
            unimplemented!("这段测试只走一次性接活")
        }

        async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
            unimplemented!()
        }

        async fn abort(&self) -> cog_core::SFResult<()> {
            unreachable!()
        }

        async fn reset(&self) -> cog_core::SFResult<()> {
            let mut resets = self.resets.lock().unwrap_or_else(|e| e.into_inner());
            *resets += 1;
            Ok(())
        }

        async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
            Ok(cog_core::AgentState::Idle)
        }

        async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("接活的员工走自己的运行循环，不在这里回话")
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn read_board(
            &self,
            _task_id: &str,
            _field: &str,
        ) -> cog_core::SFResult<Option<String>> {
            Ok(None)
        }

        async fn write_board(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// 一张写死的名册。`create_agent` 交给第一个员工——这两条测试要的是
    /// 「派给谁」与「派活本身」，不是建员工。
    pub(super) struct RosterManager {
        roster: Vec<(String, std::sync::Arc<dyn cog_core::Agent>)>,
        /// 名册上为正某件任务现起的对象：id → 它正在做的那件任务。
        owned: Vec<(String, String)>,
    }

    impl RosterManager {
        pub(super) fn new(roster: Vec<(String, std::sync::Arc<dyn cog_core::Agent>)>) -> Self {
            Self {
                roster,
                owned: Vec::new(),
            }
        }

        /// 把名册上的某个人标成为一件任务现起的，而不是常驻员工。
        fn with_task_owner(mut self, agent_id: &str, task_id: &str) -> Self {
            self.owned.push((agent_id.to_string(), task_id.to_string()));
            self
        }

        fn owner_of(&self, agent_id: &str) -> Option<String> {
            self.owned
                .iter()
                .find(|(id, _)| id == agent_id)
                .map(|(_, task)| task.clone())
        }
    }

    #[async_trait::async_trait]
    impl cog_core::AgentManager for RosterManager {
        async fn create_agent(
            &self,
            _agent_id: &str,
            _role: &str,
            _llm: std::sync::Arc<dyn cog_core::LlmClient>,
            _task_id: Option<&str>,
        ) -> cog_core::SFResult<std::sync::Arc<dyn cog_core::Agent>> {
            self.roster
                .first()
                .map(|(_, agent)| agent.clone())
                .ok_or_else(|| cog_core::SFError::Agent("empty roster".into()))
        }

        async fn retire_task_agents(&self, _task_id: &str) -> cog_core::SFResult<usize> {
            Ok(0)
        }

        async fn dispatch(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn list_workers(&self) -> cog_core::SFResult<Vec<cog_core::WorkerInfo>> {
            Ok(self
                .roster
                .iter()
                .map(|(id, _)| cog_core::WorkerInfo {
                    agent_id: id.clone(),
                    role: "employee".into(),
                    capabilities: vec!["employee".into()],
                    owner_task: self.owner_of(id),
                })
                .collect())
        }

        async fn shutdown(&self) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn get_agent(
            &self,
            agent_id: &str,
        ) -> cog_core::SFResult<Option<std::sync::Arc<dyn cog_core::Agent>>> {
            Ok(self
                .roster
                .iter()
                .find(|(id, _)| id == agent_id)
                .map(|(_, agent)| agent.clone()))
        }
    }

    fn work_item(id: &str, input: serde_json::Value) -> cog_core::Task {
        cog_core::Task::new(
            id,
            cog_core::TaskType::Custom(PLATFORM_AGENT_TASK.into()),
            input,
        )
    }

    /// 指名交给某个员工：活到的是他，别人手上没有，结果里记的也是他。
    ///
    /// 结果是**这个人**而不是分支名：写回任务行时「谁干的」就靠这一个字段，
    /// 写成 `platform_agent_task` 就只剩桌子、没了坐在桌前的人。
    #[tokio::test]
    async fn a_work_item_reaches_the_employee_it_names() {
        let luna = std::sync::Arc::new(RecordingEmployee::new("read it"));
        let max = std::sync::Arc::new(RecordingEmployee::new("grader"));
        let manager = std::sync::Arc::new(RosterManager::new(vec![
            ("squad:luna".to_string(), luna.clone()),
            ("squad:max".to_string(), max.clone()),
        ]));
        let executor = CollaborationExecutor::new().with_agent_manager(manager);

        let result = executor
            .execute_agent_task(&work_item(
                "work-1",
                serde_json::json!({"agent_id": "squad:max", "material": "a conversation"}),
            ))
            .await
            .expect("指名的人在名册上，这件活就要有人接");

        assert_eq!(result.metadata.executor_id, "agent:squad:max");
        assert_eq!(result.output["answer"], "grader");
        assert!(result.success);
        let seen = max.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0]["material"], "a conversation",
            "员工要读到这件活的材料，不是一件空活"
        );
        assert!(luna.seen().is_empty(), "点名的是 max，luna 不该接到");
    }

    /// 不指名时轮流派，而不是全给名册第一名。
    ///
    /// 只断言「有人接了」的话，把活永远派给第一个人的实现同样绿——而那是
    /// 一个人的队列挂了整张名册的招牌。
    #[tokio::test]
    async fn work_without_a_name_rotates_across_the_roster() {
        let luna = std::sync::Arc::new(RecordingEmployee::new("luna"));
        let max = std::sync::Arc::new(RecordingEmployee::new("max"));
        let manager = std::sync::Arc::new(RosterManager::new(vec![
            ("squad:luna".to_string(), luna.clone()),
            ("squad:max".to_string(), max.clone()),
        ]));
        let executor = CollaborationExecutor::new().with_agent_manager(manager);

        for n in 0..4 {
            executor
                .execute_agent_task(&work_item(
                    &format!("work-{n}"),
                    serde_json::json!({"material": n}),
                ))
                .await
                .expect("名册上有人，这件活就该有人接");
        }

        assert_eq!(luna.seen().len(), 2, "两轮下来 luna 该接两件");
        assert_eq!(max.seen().len(), 2, "两轮下来 max 该接两件");
    }

    /// 名册空的时候不假装干完了。
    ///
    /// 这条是给「工作台上一张空名册」准备的读数：活没被接走就要报错，
    /// 返回一个成功而空的答复会让场景侧以为写回已经发生。
    #[tokio::test]
    async fn an_empty_roster_is_an_error_rather_than_a_silent_empty_answer() {
        let executor = CollaborationExecutor::new()
            .with_agent_manager(std::sync::Arc::new(RosterManager::new(Vec::new())));

        let err = executor
            .execute_agent_task(&work_item("work-1", serde_json::json!({})))
            .await
            .expect_err("名册上一个人都没有，这件活接不了");
        assert!(err.to_string().contains("roster"), "{err}");
    }

    /// 为某件任务现起的对象不是员工，不能把无关的活交给它。
    ///
    /// 名册上两类东西混在一起：常驻员工，与正在做那件 DAG 任务时现起的
    /// 作战小队。后者手里有一段与这件活无关的对话，接过去要么排在它后面，
    /// 要么两边读到彼此的步骤。所以不指名的派活只从常驻的那几个里挑；如果
    /// 名册上全是临时的，宁可报错也不占用别人的小队。
    #[tokio::test]
    async fn a_worker_that_belongs_to_a_task_is_not_an_employee() {
        let borrowed = std::sync::Arc::new(RecordingEmployee::new("borrowed"));
        let luna = std::sync::Arc::new(RecordingEmployee::new("luna"));
        let manager = std::sync::Arc::new(
            RosterManager::new(vec![
                ("generator-for-task-9".to_string(), borrowed.clone()),
                ("squad:luna".to_string(), luna.clone()),
            ])
            .with_task_owner("generator-for-task-9", "task-9"),
        );
        let executor = CollaborationExecutor::new().with_agent_manager(manager);

        executor
            .execute_agent_task(&work_item("work-1", serde_json::json!({})))
            .await
            .expect("名册上有常驻员工，这件活就该有人接");
        assert!(borrowed.seen().is_empty(), "临时小队的对象不该接到这件活");
        assert_eq!(luna.seen().len(), 1, "接活的只能是常驻员工");

        // 只有临时对象时不算有员工：报错，而不是借一个人过来。
        let only_borrowed = std::sync::Arc::new(
            RosterManager::new(vec![(
                "generator-for-task-9".to_string(),
                std::sync::Arc::new(RecordingEmployee::new("borrowed"))
                    as std::sync::Arc<dyn cog_core::Agent>,
            )])
            .with_task_owner("generator-for-task-9", "task-9"),
        );
        let err = CollaborationExecutor::new()
            .with_agent_manager(only_borrowed)
            .execute_agent_task(&work_item("work-2", serde_json::json!({})))
            .await
            .expect_err("名册上只有为任务现起的对象，没有员工");
        assert!(err.to_string().contains("standing employee"), "{err}");
    }

    /// 每件活都从一段干净的对话开始。
    ///
    /// 员工是长的，活是短的：上一件活的材料与步骤留在对话里，这一轮模型就会
    /// 拿它们当上下文——多付一遍钱，判词还可能被上一件带偏。
    #[tokio::test]
    async fn a_work_item_starts_on_a_clean_conversation() {
        let luna = std::sync::Arc::new(RecordingEmployee::new("luna"));
        let executor = CollaborationExecutor::new().with_agent_manager(std::sync::Arc::new(
            RosterManager::new(vec![("squad:luna".to_string(), luna.clone())]),
        ));

        executor
            .execute_agent_task(&work_item("work-1", serde_json::json!({"material": "a"})))
            .await
            .expect("这件活该有人接");
        executor
            .execute_agent_task(&work_item("work-2", serde_json::json!({"material": "b"})))
            .await
            .expect("这件活该有人接");

        assert_eq!(luna.resets(), 2, "每件活开跑前都要清一次对话");
    }

    /// 点名的人不在名册上时不要悄悄换人。
    ///
    /// 换人会把一件本该落到某人手上的活交给别人，而调用方从结果里看不出来：
    /// 任务栏上写着成功，某个人却没读到这件活。
    #[tokio::test]
    async fn work_naming_an_absent_employee_is_refused_not_handed_to_someone_else() {
        let luna = std::sync::Arc::new(RecordingEmployee::new("luna"));
        let executor = CollaborationExecutor::new().with_agent_manager(std::sync::Arc::new(
            RosterManager::new(vec![("squad:luna".to_string(), luna.clone())]),
        ));

        let err = executor
            .execute_agent_task(&work_item(
                "work-1",
                serde_json::json!({"agent_id": "squad:nobody"}),
            ))
            .await
            .expect_err("点名的人不在名册上");
        assert!(err.to_string().contains("squad:nobody"), "{err}");
        assert!(luna.seen().is_empty(), "点名的是别人，luna 不该接到");
    }
}
