use std::sync::Arc;

use cog_core::{Agent, KnowledgeBackend, Task, TaskType};

use crate::squad::pge::types::PlannerOutput;

/// Planner Actor — semantic wrapper around a `dyn Agent` created via
/// [`AgentManager`](cog_core::AgentManager).
///
/// Responsible for:
/// 1. Querying historical decomposition patterns from [`KnowledgeBackend`].
/// 2. Constructing Planner context.
/// 3. Invoking the underlying agent and parsing strict-schema output.
#[derive(Clone)]
pub struct PlannerActor {
    agent: Arc<dyn Agent>,
    knowledge: Option<Arc<dyn KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
    prompt_skill: Option<cog_core::PromptSkillDef>,
    context_builder: Option<Arc<dyn cog_core::TaskContextBuilder>>,
}

impl PlannerActor {
    pub fn new(agent: Arc<dyn Agent>) -> Self {
        Self {
            agent,
            knowledge: None,
            self_review: None,
            output_schema: None,
            prompt_skill: None,
            context_builder: None,
        }
    }

    pub fn with_knowledge(mut self, knowledge: Arc<dyn KnowledgeBackend>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach a JSON Schema constraining the planner output. When set, the
    /// schema is injected into the prompt context (taking precedence over
    /// the built-in self-evolution schema) and the raw LLM output is
    /// validated against it; failures are logged and lenient parsing applies.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Attach a prompt skill (SKILL.md 模板 + 可选 schema 指导)。
    /// 算子显式配置的 output_schema 优先于 skill 声明的 schema。
    pub fn with_prompt_skill(mut self, skill: cog_core::PromptSkillDef) -> Self {
        self.prompt_skill = Some(skill);
        self
    }

    /// Override the prompt context builder (defaults to
    /// [`crate::actors::StandardTaskContextBuilder`]).
    pub fn with_context_builder(mut self, builder: Arc<dyn cog_core::TaskContextBuilder>) -> Self {
        self.context_builder = Some(builder);
        self
    }

    /// Run the Planner phase: produce a structured plan and sub-tasks.
    pub async fn plan(
        &self,
        task: &Task,
        attempt: u32,
        previous_feedback: Option<&str>,
        previous_score: Option<u32>,
        previous_generation: Option<&serde_json::Value>,
        context_board: Option<&serde_json::Value>,
    ) -> PlannerOutput {
        let goal = task
            .input
            .get("goal")
            .and_then(|g| g.as_str())
            .unwrap_or("");

        let default_builder;
        let builder: &dyn cog_core::TaskContextBuilder = match self.context_builder.as_ref() {
            Some(b) => b.as_ref(),
            None => {
                default_builder = crate::actors::StandardTaskContextBuilder;
                &default_builder
            }
        };
        let mut ctx = cog_core::TaskContextBuilder::build(
            builder,
            cog_core::PgeRole::Planner,
            &cog_core::TaskContextInput {
                task: Some(task),
                attempt,
                generation: previous_generation,
                previous_feedback,
                previous_score,
                context_board,
                ..Default::default()
            },
        );

        // For self-evolution tasks, the planner must emit a JSON plan that the
        // downstream PGE pipeline can parse. Patch artifacts are produced by the
        // Generator later, so we explicitly tell the planner not to emit XML or
        // patch content here.
        let is_self_evolution = matches!(
            &task.task_type,
            TaskType::Custom(s) if s == "self_evolution"
        ) || task
            .input
            .get("evolution_mode")
            .and_then(|v| v.as_str())
            .map(|s| s == "generate_patch")
            .unwrap_or(false);

        if is_self_evolution {
            ctx["evolution_mode"] = serde_json::json!("generate_patch");
            ctx["response_format"] = serde_json::json!("json");
            ctx["output_schema"] = serde_json::json!({
                "summary": "string: concise plan summary",
                "plan": "object: structured plan details (may be empty for self-evolution execution)",
                "sub_tasks": "array: empty for self-evolution execution, otherwise TaskSpec objects"
            });
            ctx["example"] = serde_json::json!({
                "summary": "Print the Cogneva version at startup by reading the version from Cargo.toml in main.rs",
                "plan": { "approach": "add a version log line in the binary entry point" },
                "sub_tasks": []
            });
            ctx["instructions"] = serde_json::json!(
                "You are the Planner actor. Your job is to produce a plan, NOT the patch. \
                 Emit ONLY a single JSON object matching the output_schema. No markdown, no XML, no code fences, no patch content. \
                 Do not output artifact tags or unified diffs; the Generator actor will create the patch later."
            );
        }

        // A configured output schema takes precedence over the built-in
        // self-evolution schema: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            ctx["output_schema"] = schema.clone();
            ctx["response_format"] = serde_json::json!("json");
        }

        // Prompt skill（SKILL.md 模板 + schema 指导）：算子 schema 优先于 skill schema。
        if let Some(ref skill) = self.prompt_skill {
            crate::actors::apply_prompt_skill(&mut ctx, skill, self.output_schema.as_ref());
        }

        // Inject historical decomposition patterns if knowledge backend is wired.
        if let Some(ref k) = self.knowledge {
            match k.retrieve_similar_decompositions(goal, 3).await {
                Ok(patterns) if !patterns.is_empty() => {
                    ctx["historical_decompositions"] = serde_json::json!(patterns);
                }
                Err(e) => {
                    tracing::warn!("Planner knowledge query failed: {}", e);
                }
                _ => {}
            }
        }

        let input = serde_json::json!({
            "task": task,
            "context": ctx,
        });

        let mut output = match self.agent.prompt(input).await {
            Ok(result) => {
                // 校验 schema：算子显式配置优先，否则用 skill 声明的 schema（均仅告警）。
                let effective_schema = self.output_schema.as_ref().or_else(|| {
                    self.prompt_skill
                        .as_ref()
                        .and_then(|s| s.output_schema.as_ref())
                });
                if let Some(schema) = effective_schema {
                    crate::actors::validate_against_schema(schema, &result.to_string(), "planner");
                }
                crate::squad::pge::parse_planner_output(&result, goal)
            }
            Err(e) => {
                tracing::warn!("Planner prompt failed: {}", e);
                PlannerOutput {
                    summary: format!("Fallback plan for: {}", goal),
                    plan: serde_json::json!({}),
                    sub_tasks: Vec::new(),
                }
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            "planner",
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = crate::squad::pge::parse_planner_output(&value, goal);
            }
        }
        output
    }
}
