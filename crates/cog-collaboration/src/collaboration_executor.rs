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

/// Identity carried on the actionability verdict's outbound request. Without it
/// the gateway records the call against its `unknown` default and the verdict's
/// token spend cannot be attributed to any component.
const INTENT_ASSESS_ACTOR: &str = "intent_assess";

/// 可执行性判据的稳定半段：角色、四条判定的定义、答案的形状。
///
/// 它与被判的那条外部意图无关，所以必须逐字节相同地待在整个请求的最前面。
/// 此前它接在**同一条用户消息的末尾**——正文与整段评论线程之后，于是每个意图
/// 的请求从第一个字节起就与别个不同，这段常量被逐个意图重买一遍。留在用户
/// 消息里的是真正会变的东西：这是哪条意图、它的正文、它的媒体。
const INTENT_ASSESS_SYSTEM: &str = "\
You are a careful actionability judge for an autonomous software-engineering \
agent. You output exactly one JSON verdict and nothing else.\n\
\n\
Attached media (screenshots/recordings) are provided as separate content blocks; \
inspect them — a screenshot may contain the exact error and reproduction steps.\n\
\n\
Decide actionability from the WHOLE content of the intent below:\n\
- fix: the intent is clear enough to act on now (a reproducible bug, or a concrete \
request the agent can realize). Code-level security vulnerabilities belong HERE: the \
fix pipeline runs in a sandbox and its output passes self-review and CI gates before \
merge, so a security defect in the code is exactly what the agent can and should fix. \
Do NOT ask again once enough information is present.\n\
- clarify: essential information is still missing (expected vs actual behavior, \
reproduction steps, or version). Put the single most useful question in `question`.\n\
- skip: not worth acting on (out of scope, not a bug, a duplicate, etc.).\n\
- escalate: ONLY when writing code cannot resolve it — it needs human credentials or \
identity, legal/compliance judgment, physical or billing actions, or the request \
itself is malicious (adding backdoors, exfiltrating secrets). Never escalate merely \
because a defect is sensitive or security-related.\n\
\n\
Reply with ONE JSON object only, no prose, no code fence:\n\
{\"decision\": \"fix|clarify|skip|escalate\", \"question\": \"\", \"priority\": 1-5, \"reason\": \"\"}\n\
priority is 1 (highest) to 5 (lowest). When decision is not `clarify`, leave question empty. \
Write the question in the same language the reporter used.";

/// Chat options for the actionability verdict. Split out from the call site so
/// the attribution is pinned by a test rather than by reading the line.
fn intent_assess_options() -> cog_core::ChatOptions {
    cog_core::ChatOptions::default().with_actor(INTENT_ASSESS_ACTOR)
}

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
        }
    }

    /// Inject an AgentManager so that ModeSelectorActor can create an Agent.
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
        self.mode_selector = self.mode_selector.with_knowledge(backend.clone());
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
        // External-intent actionability assessment is a lightweight single-agent
        // multimodal call, not the Squad→PGE→SelfReview codegen pipeline. It must
        // be intercepted before the squad paths (the task carries no
        // evolution_mode marker, but routing it through a squad would waste a full
        // planner/generator/evaluator run on a one-shot JSON verdict).
        if let TaskType::Custom(kind) = &task.task_type {
            if kind == "platform_intent_assess" {
                return self.execute_intent_assess(task).await;
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
}

impl CollaborationExecutor {
    /// Ensure ModeSelectorActor has an Agent wired. If not, try to create one
    /// via AgentManager when both manager and LLM provider are available.
    async fn mode_selector_with_agent(&self) -> ModeSelectorActor {
        let mut ms = self.mode_selector.clone();
        if let (Some(ref manager), Some(ref llm)) = (&self.agent_manager, &self.llm_provider) {
            match manager
                .create_agent("mode-selector", "mode_selector", llm.clone())
                .await
            {
                Ok(agent) => ms = ms.with_agent(agent),
                Err(e) => tracing::warn!("Failed to create ModeSelector agent: {}", e),
            }
        }
        ms
    }

    /// Lightweight single-agent multimodal actionability verdict for an external
    /// intent (a GitHub/Gitee issue or pull request).
    ///
    /// One multimodal agent reads the intent body, the follow-up comment thread,
    /// and any attached screenshots/recordings (fed as real media content blocks
    /// so a vision/audio-capable model actually sees them), and returns a single
    /// JSON object: `{decision, question, priority, reason}` with
    /// `decision ∈ fix|clarify|skip|escalate`. The verdict rides back on the task
    /// result; cog-github acts on it (submit the heavy PGE fix task / ask exactly
    /// one clarification / skip / escalate). This task produces no change and never
    /// enters the self-evolution pipeline.
    async fn execute_intent_assess(&self, task: &Task) -> SFResult<TaskResult> {
        let (Some(ref manager), Some(ref llm)) = (&self.agent_manager, &self.llm_provider) else {
            // No agent/LLM wired: fail loudly so the router DLQs the task and the
            // sensor side degrades to its local model-free heuristic instead of
            // silently guessing.
            return Err(SFError::Agent(
                "platform_intent_assess requires an agent manager and an LLM provider".into(),
            ));
        };
        let agent = manager
            .create_agent(
                &format!("intent-assessor-{}", task.id),
                "intent_assessor",
                llm.clone(),
            )
            .await?;

        let input = &task.input;
        let kind = input
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("issue");
        let number = input.get("number").and_then(|v| v.as_u64()).unwrap_or(0);
        let title = input.get("title").and_then(|v| v.as_str()).unwrap_or("");
        let body = input.get("body").and_then(|v| v.as_str()).unwrap_or("");
        let labels = input
            .get("labels")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|l| l.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let thread = input
            .get("reply_thread")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let mut prompt = String::new();
        prompt.push_str(&format!(
            "You are judging whether an external {kind} on a code platform is actionable for an autonomous coding agent.\n\n"
        ));
        prompt.push_str(&format!("#{number} {title}\n"));
        if !labels.is_empty() {
            prompt.push_str(&format!("Labels: {labels}\n"));
        }
        prompt.push_str("\n## Body\n");
        prompt.push_str(if body.trim().is_empty() {
            "(empty)"
        } else {
            body
        });
        prompt.push_str("\n\n## Follow-up conversation\n");
        prompt.push_str(if thread.trim().is_empty() {
            "(no comments)"
        } else {
            thread
        });

        let mut blocks = vec![cog_core::ContentBlock::text(prompt)];
        if let Some(media) = input.get("media").and_then(|v| v.as_array()) {
            for m in media {
                let data = m.get("data_base64").and_then(|v| v.as_str()).unwrap_or("");
                let mime = m
                    .get("mime_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("application/octet-stream");
                if !data.is_empty() {
                    blocks.push(cog_core::ContentBlock::media(data, mime));
                }
            }
        }

        let messages = vec![
            cog_core::Message::system(INTENT_ASSESS_SYSTEM),
            cog_core::Message::user_blocks(blocks),
        ];

        let stream = agent
            .chat_stream(&messages, &intent_assess_options())
            .await?;
        use futures::StreamExt;
        let mut stream = stream;
        let mut text = String::new();
        let mut stream_error: Option<String> = None;
        while let Some(event) = stream.next().await {
            match event {
                cog_core::AssistantMessageEvent::TextDelta { delta, .. } => {
                    text.push_str(&delta);
                }
                cog_core::AssistantMessageEvent::Done { message, .. } => {
                    let final_text = message.content();
                    if !final_text.trim().is_empty() {
                        text = final_text;
                    }
                }
                cog_core::AssistantMessageEvent::Error { error, .. } => {
                    // The reply is over, but keep reading rather than returning
                    // from this arm. The event carries only the wording of the
                    // failure; the *type* of it — which refusal the upstream
                    // gave, and how long it said to wait — is on the stream's
                    // final response. Returning here throws that away, and every
                    // consumer above can then only read the sentence to guess
                    // whether re-running could ever succeed. Reading to the end
                    // is also what drains the producer, so `result()` resolves.
                    stream_error = Some(error.content());
                }
                _ => {}
            }
        }

        if let Some(error) = stream_error {
            let response = stream.result().await;
            // A provider that classified the refusal (HTTP status -> a typed
            // refusal, plus the wait the upstream stated in its headers) already
            // said what this failure is. Re-stating it as a sentence here is how
            // a quota outage is retried every poll as if it were a transient
            // blip, so the type is carried out the way it arrived.
            return Err(match response.upstream_failure {
                Some(cause) => SFError::upstream_refused_after(
                    cause,
                    format!("intent assess stream error: {error}"),
                    response.retry_after_secs,
                ),
                None => SFError::Agent(format!("intent assess stream error: {error}")),
            });
        }

        let verdict = Self::parse_intent_verdict(&text)?;
        info!(
            task_id = %task.id,
            decision = %verdict.get("decision").and_then(|v| v.as_str()).unwrap_or("?"),
            "platform_intent_assess verdict"
        );
        Ok(TaskResult {
            success: true,
            output: verdict,
            metadata: TaskResultMetadata::new("intent_assess"),
        })
    }

    /// Parse the model's JSON verdict, tolerating a trailing/leading code fence
    /// and surrounding prose. Normalizes `decision` to lowercase and fills safe
    /// defaults for the optional fields.
    fn parse_intent_verdict(text: &str) -> SFResult<serde_json::Value> {
        let trimmed = text.trim();
        let json_str = if let Some(start) = trimmed.find('{') {
            let end = trimmed.rfind('}').ok_or_else(|| {
                SFError::Agent(format!("intent verdict has no closing brace: {trimmed}"))
            })?;
            trimmed[start..=end].trim()
        } else {
            return Err(SFError::Agent(format!(
                "intent verdict is not JSON: {trimmed}"
            )));
        };
        let mut v: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| SFError::Agent(format!("invalid intent verdict JSON: {e}: {json_str}")))?;

        let decision = v
            .get("decision")
            .and_then(|d| d.as_str())
            .map(|s| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        let decision = match decision.as_str() {
            "fix" | "clarify" | "skip" | "escalate" => decision,
            other => {
                return Err(SFError::Agent(format!(
                    "intent verdict has unknown decision: {other:?}"
                )));
            }
        };
        v["decision"] = serde_json::json!(decision);
        if v.get("question").and_then(|q| q.as_str()).is_none() {
            v["question"] = serde_json::json!("");
        }
        if v.get("priority").and_then(|p| p.as_u64()).is_none() {
            v["priority"] = serde_json::json!(3);
        }
        if v.get("reason").and_then(|r| r.as_str()).is_none() {
            v["reason"] = serde_json::json!("");
        }
        Ok(v)
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

        let mode_selector = self.mode_selector_with_agent().await;
        // The decision is counted by the selector itself; the stage is logged
        // here as well so a reader of this line can see which rule decided
        // without going to the metric plane.
        let decision = mode_selector
            .select_mode(&goal, Some(&profile), Some(&task.id))
            .await;
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

        let mut context = Self::goal_context(task);
        if is_self_evolution {
            context = Self::build_self_evolution_context(context, &goal);
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
                    task.evolution_intent(),
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
    /// The reason archived is the run's own, never `fallback`: `fallback` is a
    /// sentence for the error this returns, and a sentence invented here would
    /// become the root cause the Evaluator reads back for this class of task.
    /// A run that states no reason leaves the field empty — "no cause recorded"
    /// is a reading, "something failed" is not.
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

    fn build_self_evolution_context(mut base: serde_json::Value, goal: &str) -> serde_json::Value {
        // Only mark the mode; generator/evaluator add their own role-specific
        // schemas. Putting detailed change instructions here leaks into the
        // planner and makes it emit XML/artifact markup instead of a plan.
        base["evolution_mode"] = serde_json::json!("generate_change");
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
        intent: Option<cog_core::EvolutionIntent>,
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
                intent,
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

#[cfg(test)]
mod tests {
    use super::CollaborationExecutor;
    use cog_core::Observable;

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
    fn the_actionability_verdict_carries_an_actor() {
        // The gateway falls back to "unknown" when the header is absent, which
        // is indistinguishable from a component genuinely named unknown. The
        // verdict path must therefore never send default options.
        let options = super::intent_assess_options();
        assert_eq!(
            options
                .headers
                .get(cog_core::LLM_ACTOR_HEADER)
                .map(String::as_str),
            Some(super::INTENT_ASSESS_ACTOR)
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

    /// An agent standing in for the assess call, in both of the ways it can end.
    ///
    /// With `reply` set it answers with that verdict; without it, it ends the way
    /// a real refused call does — an error event carrying the wording, and a
    /// final response carrying the classified refusal the provider built out of
    /// the HTTP status. Either way it keeps the message list it was handed, so a
    /// test can read what the judge actually sent instead of what it was meant to
    /// send.
    struct ScriptedAssessAgent {
        reply: Option<String>,
        error_text: String,
        cause: Option<cog_core::UpstreamFailure>,
        wait: Option<u64>,
        seen: std::sync::Mutex<Vec<Vec<cog_core::Message>>>,
    }

    impl ScriptedAssessAgent {
        /// An assess call that is refused upstream.
        fn refusing(
            error_text: &str,
            cause: Option<cog_core::UpstreamFailure>,
            wait: Option<u64>,
        ) -> Self {
            Self {
                reply: None,
                error_text: error_text.to_string(),
                cause,
                wait,
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// An assess call that answers with a verdict.
        fn answering(verdict: &str) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                reply: Some(verdict.to_string()),
                error_text: String::new(),
                cause: None,
                wait: None,
                seen: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<Vec<cog_core::Message>> {
            self.seen.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for ScriptedAssessAgent {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }

        async fn start(&self) {}

        async fn snapshot(
            &self,
            _task_id: String,
        ) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
            Ok(cog_core::AgentCheckpoint {
                checkpoint_id: String::new(),
                task_id: String::new(),
                agent_state: serde_json::Value::Null,
                context_window: Vec::new(),
                event_offset: 0,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn continue_(
            &self,
            _input: serde_json::Value,
        ) -> cog_core::SFResult<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }

        async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn abort(&self) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn reset(&self) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
            Ok(cog_core::AgentState::Idle)
        }

        async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }

        async fn chat_stream(
            &self,
            messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.seen
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(messages.to_vec());
            let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(8);
            let _ = producer
                .push(cog_core::AssistantMessageEvent::Start {
                    timestamp: chrono::Utc::now(),
                })
                .await;
            if let Some(ref reply) = self.reply {
                let _ = producer
                    .push(cog_core::AssistantMessageEvent::Done {
                        reason: cog_core::StopReason::Stop,
                        message: cog_core::Message::assistant_text(reply.clone()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(cog_core::ChatResponse {
                    content: vec![cog_core::ContentBlock::Text {
                        text: reply.clone(),
                        text_signature: None,
                    }],
                    ..cog_core::ChatResponse::default()
                });
                return Ok(stream);
            }
            let _ = producer
                .push(cog_core::AssistantMessageEvent::Error {
                    reason: cog_core::StopReason::Error,
                    error: cog_core::Message::assistant_text(self.error_text.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            // The typed refusal rides the final response, exactly as the
            // providers leave it: the event above has the wording only.
            producer.end(cog_core::ChatResponse {
                error_message: Some(self.error_text.clone()),
                stop_reason: cog_core::StopReason::Error,
                upstream_failure: self.cause,
                retry_after_secs: self.wait,
                ..cog_core::ChatResponse::default()
            });
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.chat_stream(&[], &cog_core::ChatOptions::default())
                .await
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

    struct OneAgentManager {
        agent: std::sync::Arc<dyn cog_core::Agent>,
    }

    #[async_trait::async_trait]
    impl cog_core::AgentManager for OneAgentManager {
        async fn create_agent(
            &self,
            _agent_id: &str,
            _role: &str,
            _llm: std::sync::Arc<dyn cog_core::LlmClient>,
        ) -> cog_core::SFResult<std::sync::Arc<dyn cog_core::Agent>> {
            Ok(std::sync::Arc::clone(&self.agent))
        }

        async fn dispatch(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn list_workers(&self) -> cog_core::SFResult<Vec<cog_core::WorkerInfo>> {
            Ok(Vec::new())
        }

        async fn shutdown(&self) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn get_agent(
            &self,
            _agent_id: &str,
        ) -> cog_core::SFResult<Option<std::sync::Arc<dyn cog_core::Agent>>> {
            Ok(None)
        }
    }

    struct UnusedLlm;

    #[async_trait::async_trait]
    impl cog_core::LlmClient for UnusedLlm {
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
            producer.end(cog_core::ChatResponse::default());
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.chat_stream(&[], &cog_core::ChatOptions::default())
                .await
        }

        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            Ok(cog_core::ChatResponse::default())
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn assess_task() -> cog_core::Task {
        cog_core::Task::new(
            "github-issue-56",
            cog_core::TaskType::Custom("platform_intent_assess".into()),
            serde_json::json!({
                "kind": "issue",
                "number": 56,
                "title": "something is off",
                "body": "details",
            }),
        )
    }

    /// 判据的稳定半段逐字节待在请求最前面，且不许有第二个持有者。
    ///
    /// 判据是「每轮都要重发的内容必须逐字节相同地待在请求最前面」。可执行性
    /// 判据的规则、答案 schema 与语言要求与具体哪条意图无关，但此前它们接在
    /// **同一条用户消息的末尾**，前面是正文和整段评论线程——于是每条意图的请求
    /// 从第一个字节起就与别个不同，这段常量被逐个意图重买一遍（判据是一次
    /// 调用一条意图，所以省钱的地方在跨请求的前缀复用上）。
    ///
    /// 两条意图读**同一份**系统消息，才分得开「常量在最前」与「常量这次恰好
    /// 写对了」；只看一次调用的话，一条把本次正文也塞进系统消息的实现在这条
    /// 断言下同样绿。
    #[tokio::test]
    async fn the_actionability_judge_keeps_its_stable_half_ahead_of_the_intent() {
        let agent = ScriptedAssessAgent::answering(
            r#"{"decision":"fix","question":"","priority":2,"reason":"clear repro"}"#,
        );
        let executor = CollaborationExecutor::new()
            .with_llm_provider(std::sync::Arc::new(UnusedLlm))
            .with_agent_manager(std::sync::Arc::new(OneAgentManager {
                agent: agent.clone(),
            }));

        for (number, title, body) in [
            (1u64, "first intent", "the first body"),
            (2, "second intent", "the second body"),
        ] {
            let task = cog_core::Task::new(
                format!("github-issue-{number}"),
                cog_core::TaskType::Custom("platform_intent_assess".into()),
                serde_json::json!({
                    "kind": "issue",
                    "number": number,
                    "title": title,
                    "body": body,
                }),
            );
            executor
                .execute_intent_assess(&task)
                .await
                .expect("一条答复了判词的流要读出判词");
        }

        let calls = agent.calls();
        assert_eq!(calls.len(), 2, "两条意图要读两次");
        for (i, msgs) in calls.iter().enumerate() {
            assert_eq!(
                msgs.len(),
                2,
                "判据请求只该有系统消息与用户消息两条（第 {i} 条）"
            );
            assert_eq!(
                msgs[0].role(),
                "system",
                "稳定半段必须待在整个请求的最前面（第 {i} 条）"
            );
            assert_eq!(msgs[1].role(), "user", "易变半段在用户消息里（第 {i} 条）");
        }

        let stable = calls[0][0].content();
        assert_eq!(
            stable,
            calls[1][0].content(),
            "两条意图的系统消息不同，说明它带了本条意图，缓存不了"
        );
        for word in [
            "- fix:",
            "- clarify:",
            "- skip:",
            "- escalate:",
            "\"decision\": \"fix|clarify|skip|escalate\"",
            "priority is 1 (highest) to 5 (lowest)",
        ] {
            assert!(
                stable.contains(word),
                "系统消息漏了 {word:?}，模型只能猜；系统消息是：{stable}"
            );
        }

        for (i, msgs) in calls.iter().enumerate() {
            let varying = msgs[1].content();
            for word in [
                "Decide actionability from the WHOLE content above",
                "Reply with ONE JSON object only",
                "priority is 1 (highest) to 5 (lowest)",
            ] {
                assert!(
                    !varying.contains(word),
                    "稳定半段在用户消息里还有第二个持有者（{word:?}，第 {i} 条）：{varying}"
                );
            }
            assert!(
                varying.contains("## Body"),
                "用户消息要把这条意图本身带进去（第 {i} 条）"
            );
        }

        assert!(calls[0][1].content().contains("first intent"));
        assert!(calls[0][1].content().contains("the first body"));
        assert!(calls[1][1].content().contains("second intent"));
        assert_ne!(
            calls[0][1].content(),
            calls[1][1].content(),
            "两条的易变半段一样，这条测试什么也没证明"
        );
    }

    async fn assessed(
        error_text: &str,
        cause: Option<cog_core::UpstreamFailure>,
        wait: Option<u64>,
    ) -> cog_core::SFError {
        let agent = std::sync::Arc::new(ScriptedAssessAgent::refusing(error_text, cause, wait));
        let executor = CollaborationExecutor::new()
            .with_llm_provider(std::sync::Arc::new(UnusedLlm))
            .with_agent_manager(std::sync::Arc::new(OneAgentManager { agent }));
        executor
            .execute_intent_assess(&assess_task())
            .await
            .expect_err("a stream that ends in an error must not yield a verdict")
    }

    /// The refusal's type reaches the caller instead of being flattened into a
    /// sentence. Provenance: assess tasks were failing against a refused
    /// upstream with no typed cause recorded and were re-bought on every poll,
    /// because the consumer could only read the wording.
    #[tokio::test]
    async fn an_assess_refusal_carries_its_type_and_the_stated_wait() {
        let error = assessed(
            "API error (HTTP 503): upstreams unavailable",
            Some(cog_core::UpstreamFailure::QuotaExhausted),
            Some(5578),
        )
        .await;

        assert_eq!(
            error.upstream_failure(),
            Some(cog_core::UpstreamFailure::QuotaExhausted)
        );
        assert_eq!(error.retry_after_secs(), Some(5578));
        assert!(
            error.to_string().contains("intent assess stream error"),
            "the wording still has to reach the log: {error}"
        );
        assert!(
            error.is_terminal_upstream_failure(),
            "an exhausted quota cannot clear by re-running on the next poll: {error}"
        );
    }

    /// The other side: a stream error the provider did not classify stays what
    /// it was. Inventing a type for it would make every unclassified stream
    /// failure look terminal.
    #[tokio::test]
    async fn an_unclassified_stream_error_stays_untyped() {
        let error = assessed("stream ended without a reply", None, None).await;

        assert_eq!(error.upstream_failure(), None);
        assert_eq!(error.retry_after_secs(), None);
        assert!(
            error.to_string().contains("stream ended without a reply"),
            "the wording is the only evidence there is: {error}"
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
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let carriers = [
            "cogneva.example.json",
            "deploy/helm/cogneva/files/cogneva.json",
            "deploy/k3s/cogneva-json-configmap.yaml",
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
        )
        .expect("the diff the debate produced must reach the sink");

        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].affected_files,
            vec!["crates/cog-core/src/lib.rs".to_string()]
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
