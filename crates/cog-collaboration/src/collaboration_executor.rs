use async_trait::async_trait;
use cog_core::{AtomicTask, SFError, SFResult, Task, TaskResult, TaskResultMetadata, TaskType};
use std::sync::Arc;
use tracing::info;

use crate::{
    actors::ModeSelectorActor,
    profile::derive_task_profile,
    squad::{SquadConfig, SquadExecutor},
};

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
    patch_sinks: Vec<Arc<dyn cog_core::PatchSink>>,
    reflection_engine: Option<Arc<dyn cog_core::ReflectionEngine>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    pge_schemas: Option<std::collections::HashMap<String, serde_json::Value>>,
    skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
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
            patch_sinks: Vec::new(),
            reflection_engine: None,
            self_review: None,
            pge_schemas: None,
            skill_registry: None,
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
    pub fn with_meta_learning(mut self, engine: Arc<dyn cog_core::MetaLearning>) -> Self {
        self.mode_selector = self.mode_selector.with_meta_learning(engine);
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

    /// Attach a patch sink for self-evolution generated patches.
    /// Additive: every attached sink receives every generated patch
    /// (e.g. EvolutionEngine approval console + GitHub PR publisher).
    pub fn with_patch_sink(mut self, sink: Arc<dyn cog_core::PatchSink>) -> Self {
        self.patch_sinks.push(sink);
        self
    }

    /// Attach a reflection engine to record squad/patch outcomes.
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
        // is_executable == false means this is an original overall task
        // (placeholder injected by ActionPlanner) that needs decomposition.
        // is_executable == true means this is an atomic/executable task that
        // should be executed directly via Squad in execution_mode.
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

    /// Decomposition path: run the full Squad → PGE → SelfReview pipeline
    /// to break a goal into atomic tasks.
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

        let mode_selector = self.mode_selector_with_agent().await;
        let (pge_mode, reason) = mode_selector.select_mode(&goal, Some(&profile)).await;

        info!(task_id=%task.id, ?pge_mode, %reason, "ModeSelectorActor decision");

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

        let result = squad_executor
            .execute_squad(
                task.id.clone(),
                SquadConfig {
                    goal,
                    context: task.input.clone(),
                    pge_mode,
                    max_retries: task.max_retries,
                    profile: Some(profile),
                    context_window_size: None,
                    boundary_config: self.boundary_config.clone(),
                    execution_mode: false,
                    is_self_evolution: false,
                    planner_skill_id: None,
                    generator_skill_id: None,
                    evaluator_skill_id: None,
                },
            )
            .await;

        if !result.success {
            let error = result
                .error
                .unwrap_or_else(|| "Squad execution failed".into());
            return Err(SFError::Agent(error));
        }

        info!(task_id=%task.id, "Collaboration decomposition succeeded");
        let atomic_tasks = Self::extract_atomic_tasks(&result);
        let score = Self::extract_score(&result);

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
        Ok(task_result)
    }

    /// Atomic execution path: run the full Squad → PGE → SelfReview pipeline,
    /// but in execution_mode where the Planner produces an execution plan
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

        let is_self_evolution = Self::is_self_evolution_task(task);

        let mode_selector = self.mode_selector_with_agent().await;
        let (pge_mode, reason) = mode_selector.select_mode(&goal, Some(&profile)).await;

        info!(
            task_id=%task.id,
            ?pge_mode,
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

        let mut context = task.input.clone();
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
                    execution_mode: true,
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
            let pge_mode_str = match result.pge_mode {
                crate::profile::PgeMode::Pipeline => "pipeline",
                crate::profile::PgeMode::Roundtable => "roundtable",
            };
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
            let error = result
                .error
                .unwrap_or_else(|| "Squad atomic execution failed".into());
            return Err(SFError::Agent(error));
        }

        info!(task_id=%task.id, "Atomic task execution via Squad succeeded");
        let execution_output = Self::extract_execution_result(&result);
        let score = Self::extract_score(&result);

        // If this is a self-evolution task, extract generated patches and
        // hand them to every PatchSink (fan-out).
        let mut patch_ids = Vec::new();
        if is_self_evolution {
            if self.patch_sinks.is_empty() {
                tracing::warn!(
                    task_id=%task.id,
                    "Self-evolution task succeeded but no PatchSink is configured"
                );
            } else {
                let patches =
                    Self::extract_patches(&result, &goal, &Self::pge_mode_str(&result.pge_mode));
                for patch in patches {
                    for sink in &self.patch_sinks {
                        match sink.submit_patch(patch.clone()).await {
                            Ok(artifact_id) => {
                                info!(task_id=%task.id, %artifact_id, "Submitted generated patch");
                                patch_ids.push(artifact_id);
                            }
                            Err(e) => {
                                tracing::warn!(task_id=%task.id, error=%e, "Failed to submit generated patch");
                            }
                        }
                    }
                }
            }
        }

        let output = if patch_ids.is_empty() {
            serde_json::json!({
                "execution_result": execution_output,
                "squad_result": &result,
            })
        } else {
            serde_json::json!({
                "execution_result": execution_output,
                "squad_result": &result,
                "patch_ids": patch_ids,
            })
        };

        let mut metadata = TaskResultMetadata::new("collaboration_atomic");
        if let Some(s) = score {
            metadata = metadata.with_score(s);
        }
        let task_result = TaskResult {
            success: true,
            output,
            metadata,
        };
        self.archive_execution(task, &task_result);
        Ok(task_result)
    }

    /// Archive a successful execution into the KnowledgeBackend in the
    /// background. Failures are logged but never block the task result.
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

    fn is_self_evolution_task(task: &Task) -> bool {
        // Explicit self-evolution task type, or any task that opts into the
        // patch-generation flow via the evolution_mode marker in its input
        // (e.g. github_ci_fix / github_issue_fix from the GitHub integration).
        matches!(
            &task.task_type,
            TaskType::Custom(s) if s == "self_evolution"
        ) || task.input.get("evolution_mode").and_then(|v| v.as_str()) == Some("generate_patch")
    }

    fn pge_mode_str(mode: &crate::profile::PgeMode) -> String {
        match mode {
            crate::profile::PgeMode::Pipeline => "pipeline".into(),
            crate::profile::PgeMode::Roundtable => "roundtable".into(),
        }
    }

    fn build_self_evolution_context(mut base: serde_json::Value, goal: &str) -> serde_json::Value {
        // Only mark the mode; generator/evaluator add their own role-specific
        // schemas. Putting detailed patch instructions here leaks into the
        // planner and makes it emit XML/artifact markup instead of a plan.
        base["evolution_mode"] = serde_json::json!("generate_patch");
        if base.get("goal").is_none() {
            base["goal"] = serde_json::json!(goal);
        }
        base
    }

    fn extract_patches(
        squad_result: &crate::squad::SquadResult,
        goal: &str,
        pge_mode: &str,
    ) -> Vec<cog_core::GeneratedPatch> {
        let mut patches = Vec::new();
        let Some(ref result_val) = squad_result.result else {
            return patches;
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
            Vec::new()
        };

        for artifact in artifacts {
            let is_patch = artifact.artifact_type == "patch"
                || artifact.name.to_lowercase().ends_with(".patch");
            if !is_patch {
                continue;
            }

            let affected_files = match cog_core::parse_patch_affected_files(&artifact.content) {
                Ok(files) => files,
                Err(e) => {
                    tracing::warn!(
                        artifact=%artifact.name,
                        error=%e,
                        "Generated patch artifact does not look like a valid unified diff"
                    );
                    Vec::new()
                }
            };

            patches.push(cog_core::GeneratedPatch {
                patch_id: artifact.name.clone(),
                goal: goal.into(),
                content: artifact.content,
                affected_files,
                rationale: None,
                pge_mode: pge_mode.into(),
                self_review_score: Self::extract_score(squad_result).map(|s| s as f32),
            });
        }

        patches
    }

    fn extract_score(squad_result: &crate::squad::SquadResult) -> Option<f64> {
        if let Some(ref result_val) = squad_result.result {
            if let Ok(pipeline) =
                serde_json::from_value::<crate::PgePipelineResult>(result_val.clone())
            {
                return pipeline.final_evaluation.score.map(|s| s as f64 / 100.0);
            }
            if let Ok(roundtable) =
                serde_json::from_value::<crate::PgeRoundtableResult>(result_val.clone())
            {
                return roundtable.final_evaluation.score.map(|s| s as f64 / 100.0);
            }
        }
        None
    }

    fn extract_execution_result(squad_result: &crate::squad::SquadResult) -> serde_json::Value {
        if let Some(ref result_val) = squad_result.result {
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
                    "evaluation": roundtable.final_evaluation,
                });
            }
        }
        serde_json::Value::Null
    }

    fn extract_atomic_tasks(squad_result: &crate::squad::SquadResult) -> Vec<AtomicTask> {
        let mut tasks = Vec::new();
        if let Some(ref result_val) = squad_result.result {
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

    /// Rough token estimate: ~4 chars per token plus 2 000 token overhead for
    /// prompt wrapping / reasoning. Capped at 50 000.
    fn estimate_tokens(input: &serde_json::Value) -> u64 {
        let chars = input.to_string().len() as u64;
        (chars / 4).saturating_add(2_000).min(50_000)
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
    use super::*;

    #[test]
    fn self_evolution_task_type_matches() {
        let task = Task::new(
            "t1",
            TaskType::Custom("self_evolution".into()),
            serde_json::json!({}),
        );
        assert!(CollaborationExecutor::is_self_evolution_task(&task));
    }

    #[test]
    fn evolution_mode_marker_routes_to_patch_flow() {
        let task = Task::new(
            "t2",
            TaskType::Custom("github_ci_fix".into()),
            serde_json::json!({"evolution_mode": "generate_patch"}),
        );
        assert!(CollaborationExecutor::is_self_evolution_task(&task));
    }

    #[test]
    fn plain_task_is_not_self_evolution() {
        let task = Task::new(
            "t3",
            TaskType::Custom("github_ci_fix".into()),
            serde_json::json!({}),
        );
        assert!(!CollaborationExecutor::is_self_evolution_task(&task));
    }
}
