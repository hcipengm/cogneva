use std::sync::Arc;

use cog_core::{Agent, KnowledgeBackend, Task};

use crate::squad::pge::types::GeneratorOutput;

/// Context from a previous generation attempt, fed back into repair
/// iterations so the Generator fixes its own output instead of starting over.
#[derive(Debug, Clone, Copy, Default)]
pub struct PreviousAttempt<'a> {
    /// Last evaluation (verdict/feedback/score), if any.
    pub evaluation: Option<&'a serde_json::Value>,
    /// Last generation output, if any.
    pub generation: Option<&'a serde_json::Value>,
    /// Evaluator feedback targeted at repair, if any.
    pub repair_feedback: Option<&'a str>,
}

/// Generator Actor — semantic wrapper around a `dyn Agent` created via
/// [`AgentManager`](cog_core::AgentManager).
///
/// Responsible for:
/// 1. Querying historical implementations from [`KnowledgeBackend`].
/// 2. Constructing Generator context.
/// 3. Invoking the underlying agent and parsing strict-schema output.
#[derive(Clone)]
pub struct GeneratorActor {
    agent: Arc<dyn Agent>,
    knowledge: Option<Arc<dyn KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
    prompt_skill: Option<cog_core::PromptSkillDef>,
    context_builder: Option<Arc<dyn cog_core::TaskContextBuilder>>,
}

impl GeneratorActor {
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

    /// Attach a JSON Schema constraining the generator output. When set, the
    /// schema is injected into the prompt context and the raw LLM output is
    /// validated against it; failures are logged and lenient parsing applies.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Attach a prompt skill（算子显式 output_schema 优先于 skill schema）。
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

    /// Run the Generator phase: execute the plan and produce artifacts.
    pub async fn generate(
        &self,
        task: &Task,
        plan: &serde_json::Value,
        attempt: u32,
        previous: PreviousAttempt<'_>,
        context_board: Option<&serde_json::Value>,
    ) -> GeneratorOutput {
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
            cog_core::PgeRole::Generator,
            &cog_core::TaskContextInput {
                task: Some(task),
                attempt,
                plan: Some(plan),
                generation: previous.generation,
                previous_evaluation: previous.evaluation,
                repair_feedback: previous.repair_feedback,
                context_board,
                ..Default::default()
            },
        );

        // Inject self-evolution patch-generation instructions when requested.
        let is_self_evolution = matches!(
            &task.task_type,
            cog_core::TaskType::Custom(s) if s == "self_evolution"
        ) || ctx
            .get("evolution_mode")
            .and_then(|v| v.as_str())
            .map(|s| s == "generate_patch")
            .unwrap_or(false);

        if is_self_evolution {
            ctx["patch_generation"] = serde_json::json!({
                "output_format": "unified_diff",
                "response_format": "json",
                "schema": {
                    "content": "string: concise summary of the change",
                    "artifacts": [
                        {
                            "artifact_type": "patch",
                            "name": "changes.patch",
                            "content": "valid git unified diff starting with 'diff --git'"
                        }
                    ]
                },
                "artifact_instructions": "Output the code change as a single artifact with artifact_type='patch', name='changes.patch', content being a valid git unified diff starting with 'diff --git'. Do not wrap in markdown fences. This is a Rust workspace; include only source file modifications under src/ directories within crates/**/*.rs."
            });
        } else if self.output_schema.is_none() && self.prompt_skill.is_none() {
            // Built-in contract for standard execution. Lowest precedence:
            // operator schema > prompt skill > built-in.
            ctx["response_format"] = serde_json::json!("json");
            ctx["output_schema"] = serde_json::json!({
                "content": "string: the produced result — the actual answer or deliverable for the goal",
                "artifacts": [{"name": "string", "content": "string", "artifact_type": "string"}]
            });
            ctx["instructions"] = serde_json::json!(
                "You are the Generator actor in a Plan-Generate-Evaluate pipeline. \
                 Execute the plan in context.plan against the goal in context.goal and produce the deliverable. \
                 Put the primary result in content (the real answer, not a description of what you would do). \
                 Use artifacts only for named files/deliverables; an empty array is fine. \
                 Emit ONLY a single JSON object matching output_schema. No markdown, no code fences, no commentary."
            );
        }

        // A configured output schema takes precedence over built-in prompt
        // contracts: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            ctx["output_schema"] = schema.clone();
            ctx["response_format"] = serde_json::json!("json");
        }

        // Prompt skill（SKILL.md 模板 + schema 指导）：算子 schema 优先于 skill schema。
        if let Some(ref skill) = self.prompt_skill {
            crate::actors::apply_prompt_skill(&mut ctx, skill, self.output_schema.as_ref());
        }

        // Inject reference implementations if knowledge backend is wired.
        if let Some(ref k) = self.knowledge {
            let task_type = format!("{:?}", task.task_type);
            let input_summary = serde_json::to_string(&task.input).unwrap_or_default();
            match k
                .retrieve_similar_implementations(&task_type, &input_summary, 3)
                .await
            {
                Ok(examples) if !examples.is_empty() => {
                    ctx["reference_implementations"] = serde_json::json!(examples);
                }
                Err(e) => {
                    tracing::warn!("Generator knowledge query failed: {}", e);
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
                let effective_schema = self.output_schema.as_ref().or_else(|| {
                    self.prompt_skill
                        .as_ref()
                        .and_then(|s| s.output_schema.as_ref())
                });
                if let Some(schema) = effective_schema {
                    crate::actors::validate_against_schema(
                        schema,
                        &result.to_string(),
                        "generator",
                    );
                }
                crate::squad::pge::parse_generator_output(&result)
            }
            Err(e) => {
                tracing::warn!("Generator prompt failed: {}", e);
                GeneratorOutput {
                    content: serde_json::Value::Null,
                    artifacts: Vec::new(),
                }
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        // Skip self-review for self-evolution patch generation. Reasoning-only
        // models often return natural-language explanations instead of strict
        // JSON, so the self-review reformat step can hang for the full timeout
        // without adding value once the patch has been extracted.
        if !is_self_evolution {
            if let Some(revised) = crate::actors::maybe_self_review(
                self.agent.as_ref(),
                &self.self_review,
                &output_str,
                "generator",
            )
            .await
            {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                    output = crate::squad::pge::parse_generator_output(&value);
                }
            }
        }
        output
    }
}
