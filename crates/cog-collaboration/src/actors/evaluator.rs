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

        // Self-evolution change validation: ensure generated artifacts are valid
        // unified diffs naming paths the apply gate will also accept.
        let is_self_evolution = task.is_self_evolution();

        // For self-evolution tasks the only thing that matters is whether the
        // generated change artifact is a valid unified diff over safe paths.
        // Reasoning-only models often fail to return structured JSON, and the
        // LLM reformat step can hang for minutes. Use deterministic validation
        // and skip the semantic LLM evaluation entirely for this mode.
        if is_self_evolution {
            let validation = Self::validate_change_artifacts(generation);
            let (verdict, score) =
                if validation.starts_with("change_validation: change artifact(s) are valid") {
                    (Verdict::Pass, 85)
                } else if validation.contains("no change artifact found") {
                    (Verdict::Fail, 0)
                } else {
                    (Verdict::Fail, 10)
                };
            let output = EvaluationResult {
                verdict,
                feedback: validation.clone(),
                score: Some(score),
                criteria: vec![Criterion {
                    name: "change_validation".into(),
                    score,
                    comment: validation.clone(),
                }],
                details: None,
            };
            let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
            // Self-review for self-evolution is skipped: the deterministic change
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
                 When context.criteria lists acceptance criteria, you MUST judge every criterion \
                 individually: emit one entry in criteria per acceptance criterion, using the criterion \
                 text as name and setting score 100 only when the generation verifiably satisfies it. \
                 In that case verdict may be pass ONLY if every acceptance criterion scores 100. \
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

        let mut output = match self.agent.prompt_for_task(&task.id, input).await {
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

    /// Validate that self-evolution artifacts contain a valid change.
    /// Returns a criterion string describing the validation result.
    fn validate_change_artifacts(generation: &serde_json::Value) -> String {
        let artifacts = generation
            .get("artifacts")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let change_artifacts: Vec<&serde_json::Value> = artifacts
            .iter()
            .filter(|a| {
                crate::squad::pge::types::is_change_artifact(
                    a.get("artifact_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                    a.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                )
            })
            .collect();

        if change_artifacts.is_empty() {
            return "change_validation: no change artifact found (expected artifact_type='change' or name ending in .diff)".into();
        }

        for artifact in change_artifacts {
            let Some(content) = artifact.get("content").and_then(|v| v.as_str()) else {
                return "change_validation: change artifact has no string content".into();
            };

            // Judge every target the apply gate will judge, deletions included:
            // a deletion names its file on the side that disappears, and
            // deleting a protected file is the same offence as rewriting it.
            let targets = cog_core::parse_diff_targets(content);
            if targets.is_empty() {
                return "change_validation: failed to parse unified diff: no file paths found"
                    .into();
            }

            for target in &targets {
                // The same question the apply gate asks, from the same place,
                // so a change this evaluator passes is not one the gate refuses
                // for a reason the generator was never told about.
                if let Some(reason) = cog_core::forbidden_target_reason(&target.path) {
                    return format!("change_validation: {}", reason);
                }
            }
        }

        "change_validation: change artifact(s) are valid unified diffs".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation_with_diff(diff: &str) -> serde_json::Value {
        serde_json::json!({
            "artifacts": [{
                "artifact_type": "change",
                "name": "changes.diff",
                "content": diff,
            }]
        })
    }

    fn modify_diff(path: &str) -> String {
        format!(
            "diff --git a/{p} b/{p}\n--- a/{p}\n+++ b/{p}\n@@ -1 +1 @@\n-a\n+b\n",
            p = path
        )
    }

    fn create_diff(path: &str) -> String {
        format!(
            "diff --git a/{p} b/{p}\nnew file mode 100644\n--- /dev/null\n+++ b/{p}\n\
             @@ -0,0 +1,1 @@\n+new\n",
            p = path
        )
    }

    fn delete_diff(path: &str) -> String {
        format!(
            "diff --git a/{p} b/{p}\ndeleted file mode 100644\n--- a/{p}\n+++ /dev/null\n\
             @@ -1 +0,0 @@\n-gone\n",
            p = path
        )
    }

    fn verdict_for(diff: &str) -> String {
        EvaluatorActor::validate_change_artifacts(&generation_with_diff(diff))
    }

    #[test]
    fn a_change_outside_src_is_accepted() {
        // The apply gate warns about these and allows them. An evaluator that
        // refused them would reject a change the contract told the generator it
        // could write, and hand back a reason the contract never mentioned.
        for path in [
            "prompts/system.md",
            "deploy/k3s/notes.md",
            "skills/generator.json",
        ] {
            let verdict = verdict_for(&modify_diff(path));
            assert!(
                verdict.contains("are valid unified diffs"),
                "{path} was refused: {verdict}"
            );
        }
    }

    #[test]
    fn a_created_file_outside_src_is_accepted() {
        let verdict = verdict_for(&create_diff("crates/cogneva/src/new_module.rs"));
        assert!(
            verdict.contains("are valid unified diffs"),
            "a creation was refused: {verdict}"
        );
    }

    #[test]
    fn a_protected_file_is_refused_however_it_is_touched() {
        for diff in [
            modify_diff("Cargo.toml"),
            create_diff("Cargo.toml"),
            delete_diff("Cargo.toml"),
        ] {
            let verdict = verdict_for(&diff);
            assert!(
                verdict.contains("protected file: Cargo.toml"),
                "a protected file slipped through: {verdict}"
            );
        }
    }

    #[test]
    fn a_credential_is_refused_however_it_is_touched() {
        for diff in [
            modify_diff("certs/server.pem"),
            delete_diff("certs/server.pem"),
        ] {
            let verdict = verdict_for(&diff);
            assert!(
                verdict.contains("protected file extension: .pem"),
                "a credential slipped through: {verdict}"
            );
        }
    }

    #[test]
    fn a_path_that_escapes_the_root_is_refused() {
        for diff in [
            modify_diff("../../etc/passwd"),
            create_diff("../outside.rs"),
        ] {
            let verdict = verdict_for(&diff);
            assert!(
                verdict.contains("escapes project root"),
                "an escape slipped through: {verdict}"
            );
        }
        assert!(verdict_for(&modify_diff("/etc/passwd")).contains("absolute path"));
    }

    #[test]
    fn a_diff_naming_no_file_is_refused() {
        let verdict = verdict_for("not a diff at all\n");
        assert!(verdict.contains("no file paths found"), "{verdict}");
    }

    #[test]
    fn the_src_only_wording_the_contract_no_longer_promises_is_gone() {
        // The generator's contract says only the protected lists are refused.
        // A refusal naming a rule that is nowhere in the contract leaves the
        // generator nothing to correct, which is the failure this replaced.
        let all = format!(
            "{}{}{}{}",
            verdict_for(&modify_diff("docs/a.md")),
            verdict_for(&create_diff("docs/b.md")),
            verdict_for(&delete_diff("docs/c.md")),
            verdict_for(&modify_diff("src/lib.rs"))
        );
        assert!(!all.contains("must be under src"), "{all}");
    }
}
