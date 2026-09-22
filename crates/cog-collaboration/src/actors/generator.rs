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

        // Inject self-evolution change-generation instructions when requested.
        let is_self_evolution = task.is_self_evolution();

        if is_self_evolution {
            ctx["change_generation"] = change_generation_contract();
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
                        "generator",
                    );
                }
                crate::squad::pge::parse_generator_output(&result)
            }
            Err(e) => {
                tracing::warn!("Generator prompt failed: {}", e);
                // A prompt that never reached its upstream is not the generator
                // choosing to produce nothing. Carry the real error through
                // content: the in-band token keeps is_terminal_env_failure true,
                // so the pipeline still stops instead of paying for identical
                // retries, while feedback and the learning chain name the actual
                // cause instead of a generator defect that is not there.
                GeneratorOutput {
                    content: serde_json::Value::String(format!("environment_error: {e}")),
                    artifacts: Vec::new(),
                }
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        // Skip self-review for self-evolution change generation. Reasoning-only
        // models often return natural-language explanations instead of strict
        // JSON, so the self-review reformat step can hang for the full timeout
        // without adding value once the change has been extracted.
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

/// The prompt contract the Generator gets for a self-evolution task.
///
/// The change this asks for is judged deterministically: the diff runs against a
/// checkout of the repository through `git apply --check` and then compiles. So
/// the contract has to state the things the gate will check, in the terms the
/// gate checks them — read before writing, hunk counts equal to the body, the
/// diff terminated, context matching the file byte for byte. A diff written from
/// memory is the most expensive failure on this path: the whole generation and
/// evaluation round is paid for and nothing is produced.
fn change_generation_contract() -> serde_json::Value {
    serde_json::json!({
        "output_format": "unified_diff",
        "response_format": "json",
        "schema": {
            "content": "string: concise summary of the change",
            "artifacts": [
                {
                    "artifact_type": "change",
                    "name": "changes.diff",
                    "content": "valid git unified diff starting with 'diff --git'"
                }
            ]
        },
        "artifact_instructions": "Output exactly one artifact: artifact_type='change', name='changes.diff', content being the raw unified diff whose first line is 'diff --git a/<path> b/<path>'. No markdown fences, no commentary inside content.",
        "grounding": "Your diff is validated with `git apply --check` against a checkout of this repository, then applied to it and compiled. It must therefore describe the files as they actually are, not as you remember them. You have file tools in this run — use them before writing: list the directories you intend to touch, then read every file you change in full (read_file, or a shell command such as `sed -n '1,400p' <path>`). Address files by repository-relative path (for example crates/cog-core/src/lib.rs) — the same path that appears in the '+++ b/' line — and those paths resolve against the checkout. Never guess a path: a path that is not in the checkout is rejected unless the diff itself declares it as created.",
        "diff_grammar": "Every hunk header '@@ -<start>,<count> +<start>,<count> @@' must declare exactly the number of lines its body carries, and the diff must end with a newline. Every context line and every removed line has to match the file byte for byte, including indentation and trailing whitespace, and the start line numbers must be the real line numbers in the file you read. Keep hunks narrow and anchor them on context that is unique in the file: one hunk whose context cannot be located fails the entire change.",
        "creating_a_file": "To add a file, declare it as a creation: 'diff --git a/<path> b/<path>', then 'new file mode 100644', '--- /dev/null', '+++ b/<path>', and a hunk header '@@ -0,0 +1,<n> @@' whose body is n '+' lines. Creating a file that already exists fails, and so does rewriting a file that does not exist without declaring it as a creation.",
        "scope": "Stay inside the checkout, and prefer paths under crates/*/src/. The gate rejects changes to build and deployment manifests (Cargo.toml, Cargo.lock, Dockerfile, Containerfile, docker-compose.yml, setup.sh) and to configuration or credential files (cogneva.json, .env, .envrc, *.pem, *.key, *.crt, *.p12); deletions are judged by the same rules as edits. The applied change is compiled and tested, so it must be complete and self-consistent — no placeholder or unimplemented bodies."
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every rule here is one the gate enforces, so dropping any of them costs a
    /// whole paid round for nothing. The prose is free to change; these claims
    /// are not.
    #[test]
    fn the_change_contract_states_the_rules_the_gate_judges() {
        let contract = change_generation_contract();
        let text = contract.to_string();

        for (claim, needle) in [
            (
                "the diff is checked against a checkout",
                "git apply --check",
            ),
            (
                "the model must read before writing",
                "read every file you change in full",
            ),
            ("paths are repository-relative", "repository-relative path"),
            ("a guessed path is rejected", "Never guess a path"),
            (
                "hunk counts must equal the body",
                "number of lines its body carries",
            ),
            ("the diff must be terminated", "must end with a newline"),
            (
                "context must match byte for byte",
                "match the file byte for byte",
            ),
            ("creating a file has its own shape", "--- /dev/null"),
            (
                "the creation shape names the mode line",
                "new file mode 100644",
            ),
            (
                "the creation shape names its hunk header",
                "@@ -0,0 +1,<n> @@",
            ),
            (
                "an existing file may not be recreated",
                "Creating a file that already exists fails",
            ),
            (
                "an absent file may not be rewritten silently",
                "rewriting a file that does not exist without declaring it as a creation",
            ),
            (
                "the protected-file set is named",
                "configuration or credential files",
            ),
            (
                "the build manifests are named",
                "build and deployment manifests",
            ),
            (
                "the deploy manifests are named",
                "Dockerfile, Containerfile",
            ),
            ("the deploy manifests are named", "setup.sh"),
            (
                "line numbers must be the file's real ones",
                "must be the real line numbers in the file you read",
            ),
            (
                "hunks must be narrow and uniquely anchored",
                "narrow and anchor them on context that is unique in the file",
            ),
            (
                "deletions are judged as edits are",
                "deletions are judged by the same rules as edits",
            ),
            (
                "the directories must be listed before reading",
                "list the directories you intend to touch",
            ),
        ] {
            assert!(
                text.contains(needle),
                "contract no longer says {claim}: {needle}"
            );
        }

        // The old wording told the model to only ever edit existing sources
        // under src/, which is narrower than the gate: creations are allowed and
        // so is anything else inside the checkout.
        assert!(
            !text.contains("include only source file modifications"),
            "the contract is back to forbidding everything the gate allows"
        );
    }

    /// The contract names every file the gate refuses, derived from the policy
    /// rather than restated here.
    ///
    /// The test above pins the wording the model reads; this one pins the list
    /// against the gate it is a promise about. They fail in different
    /// directions and neither replaces the other: a name added to the policy
    /// without a word in the contract is a change the generator is invited to
    /// write and the gate then refuses — a round paid for nothing, and the
    /// exact divergence that let a contract-legal change die at evaluation.
    #[test]
    fn the_change_contract_names_every_file_the_policy_refuses() {
        let text = change_generation_contract().to_string();

        // A needle that is a prefix of another entry (`.env` inside `.envrc`)
        // must be followed by something other than a name character, or `.envrc`
        // alone would satisfy the check for `.env` and hide the omission.
        let names = |needle: &str| {
            text.match_indices(needle).any(|(start, _)| {
                match text[start + needle.len()..].chars().next() {
                    Some(c) => !c.is_ascii_alphanumeric(),
                    None => true,
                }
            })
        };

        for name in cog_core::PROTECTED_FILE_NAMES {
            assert!(
                names(name),
                "the contract never names protected file {name}"
            );
        }
        for ext in cog_core::PROTECTED_FILE_EXTENSIONS {
            assert!(
                names(ext),
                "the contract never names protected extension .{ext}"
            );
        }
    }
}
