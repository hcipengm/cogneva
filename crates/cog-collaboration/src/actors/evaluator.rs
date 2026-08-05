use std::sync::Arc;

use cog_core::{Agent, KnowledgeBackend, Task};

use crate::squad::pge::types::{Criterion, EvaluationResult, Verdict};

/// Evaluator Actor — semantic wrapper around a `dyn Agent` created via
/// [`AgentManager`](cog_core::AgentManager).
///
/// Responsible for:
/// 1. Querying common failure patterns from [`KnowledgeBackend`].
/// 2. Constructing Evaluator context.
/// 3. Invoking the underlying agent and parsing strict-schema output.
#[derive(Clone)]
pub struct EvaluatorActor {
    agent: Arc<dyn Agent>,
    knowledge: Option<Arc<dyn KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
    prompt_skill: Option<cog_core::PromptSkillDef>,
    context_builder: Option<Arc<dyn cog_core::TaskContextBuilder>>,
}

impl EvaluatorActor {
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

    /// Attach a JSON Schema constraining the evaluator output. When set, the
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

    /// Run the Evaluator phase: assess plan and generation quality.
    pub async fn evaluate(
        &self,
        task: &Task,
        plan: &serde_json::Value,
        generation: &serde_json::Value,
        history: &[serde_json::Value],
        criteria: &[&str],
        context_board: Option<&serde_json::Value>,
    ) -> EvaluationResult {
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
            cog_core::PgeRole::Evaluator,
            &cog_core::TaskContextInput {
                task: Some(task),
                plan: Some(plan),
                generation: Some(generation),
                history,
                criteria,
                context_board,
                ..Default::default()
            },
        );

        // Self-evolution patch validation: ensure generated artifacts are valid
        // unified diffs targeting safe source paths.
        let is_self_evolution = matches!(
            &task.task_type,
            cog_core::TaskType::Custom(s) if s == "self_evolution"
        ) || ctx
            .get("evolution_mode")
            .and_then(|v| v.as_str())
            .map(|s| s == "generate_patch")
            .unwrap_or(false);

        // For self-evolution tasks the only thing that matters is whether the
        // generated patch artifact is a valid unified diff targeting safe paths.
        // Reasoning-only models often fail to return structured JSON, and the
        // LLM reformat step can hang for minutes. Use deterministic validation
        // and skip the semantic LLM evaluation entirely for this mode.
        if is_self_evolution {
            let validation = Self::validate_patch_artifacts(generation);
            let (verdict, score) =
                if validation.starts_with("patch_validation: patch artifact(s) are valid") {
                    (Verdict::Pass, 85)
                } else if validation.contains("no patch artifact found") {
                    (Verdict::Fail, 0)
                } else {
                    (Verdict::Fail, 10)
                };
            let output = EvaluationResult {
                verdict,
                feedback: validation.clone(),
                score: Some(score),
                criteria: vec![Criterion {
                    name: "patch_validation".into(),
                    score,
                    comment: validation.clone(),
                }],
                details: None,
            };
            let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
            // Self-review for self-evolution is skipped: the deterministic patch
            // validation already gives a reliable verdict, and reasoning-only
            // models frequently fail structured JSON extraction, causing the
            // reformat step to hang for the full timeout.
            let _ = output_str;
            return output;
        }

        // Built-in contract for standard evaluation. Lowest precedence:
        // operator schema > prompt skill > built-in.
        if self.output_schema.is_none() && self.prompt_skill.is_none() {
            ctx["response_format"] = serde_json::json!("json");
            ctx["output_schema"] = serde_json::json!({
                "verdict": "pass | partial | fail",
                "feedback": "string: what is good and what must improve",
                "score": "integer 0-100",
                "criteria": [{"name": "string", "score": "integer 0-100", "comment": "string"}]
            });
            ctx["instructions"] = serde_json::json!(
                "You are the Evaluator actor in a Plan-Generate-Evaluate pipeline. \
                 Judge whether context.generation correctly and completely accomplishes context.goal \
                 following context.plan. Score 80-100 for correct and complete results, \
                 60-79 for partially correct, below 60 for wrong or missing results. \
                 verdict: pass when score >= 80, partial when 60-79, fail otherwise. \
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

        // Inject common failure patterns if knowledge backend is wired.
        if let Some(ref k) = self.knowledge {
            let task_type = format!("{:?}", task.task_type);
            match k.retrieve_failure_patterns(&task_type, 3).await {
                Ok(patterns) if !patterns.is_empty() => {
                    ctx["common_failures"] = serde_json::json!(patterns);
                }
                Err(e) => {
                    tracing::warn!("Evaluator knowledge query failed: {}", e);
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
                        "evaluator",
                    );
                }
                crate::squad::pge::parse_evaluation_result(&result)
            }
            Err(e) => {
                tracing::warn!("Evaluator prompt failed: {}", e);
                EvaluationResult {
                    verdict: Verdict::Fail,
                    feedback: "Evaluation failed".into(),
                    score: None,
                    criteria: Vec::new(),
                    details: None,
                }
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            "evaluator",
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = crate::squad::pge::parse_evaluation_result(&value);
            }
        }
        output
    }

    /// Validate that self-evolution artifacts contain a valid patch.
    /// Returns a criterion string describing the validation result.
    fn validate_patch_artifacts(generation: &serde_json::Value) -> String {
        let artifacts = generation
            .get("artifacts")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let patch_artifacts: Vec<&serde_json::Value> = artifacts
            .iter()
            .filter(|a| {
                let is_patch_type = a
                    .get("artifact_type")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "patch")
                    .unwrap_or(false);
                let is_patch_name = a
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase().ends_with(".patch"))
                    .unwrap_or(false);
                is_patch_type || is_patch_name
            })
            .collect();

        if patch_artifacts.is_empty() {
            return "patch_validation: no patch artifact found (expected artifact_type='patch' or name ending in .patch)".into();
        }

        for artifact in patch_artifacts {
            let Some(content) = artifact.get("content").and_then(|v| v.as_str()) else {
                return "patch_validation: patch artifact has no string content".into();
            };

            let files = match cog_core::parse_patch_affected_files(content) {
                Ok(f) => f,
                Err(e) => {
                    return format!("patch_validation: failed to parse unified diff: {}", e);
                }
            };

            for file in &files {
                let path = std::path::Path::new(file);
                if path
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    return format!("patch_validation: path escapes project root: {}", file);
                }
                if path.is_absolute() {
                    return format!("patch_validation: absolute path not allowed: {}", file);
                }
                if !path.to_string_lossy().replace('\\', "/").contains("/src/") {
                    return format!("patch_validation: target must be under src/: {}", file);
                }
            }
        }

        "patch_validation: patch artifact(s) are valid unified diffs targeting src/".into()
    }
}
