use std::sync::Arc;

use cog_core::{Agent, KnowledgeBackend, Task, TaskType};

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
    /// 判据审：另一次调用，只读「意图 + 每条的终态」，答三问——
    /// 是不是一个「做完的样子」、能不能被证据判、是不是意图要的那件事。
    ///
    /// 这是「终态写得对不对」的**他律**：写终态的与审终态的不是同一次调用。
    /// 它不看树、不判完成，所以很便宜：两段文字进，一串判词出。答不出判词的
    /// 条目按**不成立**读——判不了不是通过。
    pub async fn review_criteria(
        &self,
        goal: &str,
        items: &[cog_core::contract::ledger_item::LedgerItem],
    ) -> Vec<crate::squad::pge::types::CriterionVerdict> {
        use crate::squad::pge::types::CriterionVerdict;
        if items.is_empty() {
            return Vec::new();
        }
        let payload = serde_json::json!({
            "goal": goal,
            "items": items
                .iter()
                .map(|i| {
                    serde_json::json!({
                        "id": &i.id,
                        "terminal": &i.terminal,
                        // 「现在」与「差距」也一起给判者：终态立不立得住，要看它离
                        // 「现在」有多远；只看终态那一格，判的是一段孤立的话。
                        "now": &i.now,
                        "gap": &i.gap,
                    })
                })
                .collect::<Vec<_>>(),
            "instructions": "Judge against this run's working tree, not against the prose alone: \
                read the files the terminal and the now cell name (read_file) before you answer, \
                and judge (1) whether the terminal describes a finished state rather than an \
                activity, (2) whether what you actually read makes it hold, (3) whether it is \
                what the goal asks for, (4) whether the now cell reads like what the tree \
                shows - a reader following it would land on the same place, (5) whether the \
                gap is exactly what the terminal still lacks and nothing else. \
                Answer one JSON object: {\"verdicts\": [{\"id\": \"...\", \
                \"ok\": true|false, \"reason\": \"...\"}]}. The reason says what you read: a file and \
                what it showed, or that you could not read it - an item you could not check is \
                not ok, because \"could not look\" and \"looked and it holds\" cannot read the same.",
        });
        let task = Task::new(
            "criterion-review",
            TaskType::Custom("criterion_review".into()),
            payload,
        );
        let answer = match self
            .agent
            .prompt_for_task(&task.id, task.input.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                // 审不到不是通过：一次没到上游的调用与「全部立得住」在读数上不能同形。
                return items
                    .iter()
                    .map(|i| CriterionVerdict {
                        id: i.id.clone(),
                        ok: false,
                        reason: format!("the criterion review did not reach a model: {e}"),
                    })
                    .collect();
            }
        };
        let given: Vec<CriterionVerdict> = answer
            .get("verdicts")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value::<CriterionVerdict>(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();
        items
            .iter()
            .map(|i| match given.iter().find(|v| v.id == i.id) {
                Some(v) => v.clone(),
                None => CriterionVerdict {
                    id: i.id.clone(),
                    ok: false,
                    reason: "the review answered nothing for this item".into(),
                },
            })
            .collect()
    }

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

        // For self-evolution tasks the questions with a measurement surface are
        // whether the generated artifact is a valid unified diff over safe
        // paths, and whether it touches the paths the plan said it would — the
        // structural criteria, the apply gate, CI and the plan's declared
        // targets all answer those, deterministically. Asking a model to guess
        // an answer that is already measurable adds a second, less reliable
        // answer to the same question, so this mode is judged by the measurement
        // and the semantic evaluation is skipped.
        //
        // What nothing here asks — and what has no measurement surface today —
        // is whether this change should be made at all, and whether it passed by
        // loosening a gate.
        if is_self_evolution {
            let validation = Self::validate_change_artifacts(generation);
            let (output, cause) = Self::judge_self_evolution_change(&validation, generation, plan);
            // The verdict's cell is the only reading of what the plan bought on
            // this path: the planner's calls are a line in the ledger either
            // way, and without this one there is nothing to weigh them against.
            crate::observable::global_observable().record_self_evolution_verdict(cause);
            let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
            // Self-review for self-evolution is skipped for the same reason: the
            // deterministic change validation already answers the measurable
            // question, and a model asked it again would add a second, less
            // reliable answer. Counted, so the calls this early return saves are
            // a reading rather than an absence.
            crate::actors::note_self_review_settled_without_a_call(
                cog_core::roles::EVALUATOR,
                crate::observable::SELF_REVIEW_DECIDED_BY_SELF_EVOLUTION,
            )
            .await;
            let _ = output_str;
            return output;
        }

        // The stable half: the answer contract. See `actor_input` — the contract
        // and this attempt's content must be assembled apart, or key order sorts
        // `attempt` first and the contract never reaches the cacheable prefix.
        let mut contract = serde_json::json!({});

        // Built-in contract for standard evaluation. Lowest precedence:
        // operator schema > prompt skill > built-in.
        //
        // Only the answer's shape: the standard the judgement is held to comes
        // from the task (`review_spec`), and how the work measures up against it
        // is the judgement this actor was called for. A paragraph here fixing the
        // bands would be the platform scoring its own case before reading it.
        if self.output_schema.is_none() && self.prompt_skill.is_none() {
            contract["response_format"] = serde_json::json!("json");
            contract["output_schema"] = serde_json::json!({
                "verdict": "pass | partial | fail",
                "feedback": "string: what is good and what must improve",
                "score": "integer 0-100",
                "criteria": [{"name": "string", "score": "integer 0-100", "comment": "string"}]
            });
        }

        // A configured output schema takes precedence over built-in prompt
        // contracts: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            contract["output_schema"] = schema.clone();
            contract["response_format"] = serde_json::json!("json");
        }

        // Prompt skill（SKILL.md 模板 + schema 指导）：算子 schema 优先于 skill schema。
        if let Some(ref skill) = self.prompt_skill {
            crate::actors::apply_prompt_skill(&mut contract, skill, self.output_schema.as_ref());
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

        let input = crate::actors::actor_input(task, contract, ctx);

        let (mut output, review_basis) = match self.agent.prompt_for_task(&task.id, input).await {
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
                        cog_core::roles::EVALUATOR,
                    );
                }
                let output = crate::squad::pge::parse_evaluation_result(&result);
                // Same reason as the planner and the generator: an envelope whose
                // own payload names a deterministic cause is a budget the loop
                // spent, not a judgement — the roundtable already stops on it —
                // so a self-review here would be grading the absence of a verdict.
                let basis = crate::actors::review_basis(
                    task,
                    crate::actors::ReviewFacts {
                        upstream_failed: output.is_terminal_env_failure(),
                        ..Default::default()
                    },
                    criteria,
                );
                (output, basis)
            }
            Err(e) => {
                tracing::warn!("Evaluator prompt failed: {}", e);
                // 和 planner、generator 同一条：没到上游的 prompt 不是对作品的
                // 判断。凭空写一个 `Fail` 会让 pipeline 的终止守卫看不见这次
                // 环境故障，照常买完一轮本地修复与全部重试；台账与共识票也会把
                // 「评委没读到作品」记成「作品判不合格」。真因按同一份线格带在
                // `details` 里，「没判定」这件事才在判定面上跟「判了不合格」分开。
                (
                    EvaluationResult {
                        verdict: Verdict::Fail,
                        feedback: format!("Evaluator prompt did not reach its upstream: {e}"),
                        score: None,
                        criteria: Vec::new(),
                        details: Some(serde_json::Value::String(
                            cog_core::contract::outcome::environment_error_content(&e),
                        )),
                    },
                    crate::actors::ReviewBasis::Passed(
                        crate::observable::SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE,
                    ),
                )
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            cog_core::roles::EVALUATOR,
            review_basis,
            &task.id,
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = crate::squad::pge::parse_evaluation_result(&value);
            }
        }
        output
    }

    /// The deterministic judgement of a self-evolution change: the artifact's
    /// own validity, and how much of the plan's declared target set it bought.
    ///
    /// Pure, and separate from [`Self::evaluate`], because this is the whole
    /// judgement on that path — no agent is consulted, so a test can assert the
    /// verdict, the per-target scores and the feedback the repair loop will read
    /// without standing up a model.
    ///
    /// The second half exists because the acceptance criteria are not read here.
    /// On this path the criteria are free text with no judge, so "the change is
    /// about what the plan said it was about" can only come from the plan's
    /// declared target set, compared against the diff's own targets. That set is
    /// the plan's own statement of intent, so the judgement reports what the
    /// change bought of it — the count and the ratio — rather than only whether
    /// the change cleared the floor. A plan that declares none leaves the vectors
    /// empty and changes nothing below.
    pub(crate) fn judge_self_evolution_change(
        validation: &str,
        generation: &serde_json::Value,
        plan: &serde_json::Value,
    ) -> (EvaluationResult, &'static str) {
        // Both readings come out of the one cell: the judgement a caller acts on
        // and the published name of the cell it landed in. Deriving the name
        // here, from the same value the score came from, is what keeps the
        // observability side of this path from being a second opinion.
        let (output, cell) = Self::judge_with_cell(validation, generation, plan);
        (output, cell.cause())
    }

    /// Map the measurements onto the one cell they name.
    ///
    /// The whole mapping lives here so that no caller can pair a verdict with
    /// another cell's reading: `uncovered` only means anything once there is a
    /// well-formed artifact to place, and that is the branch that reads it.
    fn judged_cell(validation: &str, declared: &[String], uncovered: &[String]) -> JudgedCell {
        if !validation.starts_with("change_validation: change artifact(s) are valid") {
            return if validation.contains("no change artifact found") {
                JudgedCell::NoArtifact
            } else {
                JudgedCell::Unappliable
            };
        }
        if declared.is_empty() {
            JudgedCell::NothingDeclared
        } else if uncovered.is_empty() {
            JudgedCell::Covered {
                declared: declared.len(),
            }
        } else {
            JudgedCell::Uncovered {
                bought: declared.len() - uncovered.len(),
                declared: declared.len(),
            }
        }
    }

    /// The judgement together with the cell it landed in.
    ///
    /// One reading rather than two, so a cell a test names and a score it
    /// asserts cannot come from different places.
    fn judge_with_cell(
        validation: &str,
        generation: &serde_json::Value,
        plan: &serde_json::Value,
    ) -> (EvaluationResult, JudgedCell) {
        use crate::squad::pge::types::{change_targets, declared_targets, uncovered_targets};

        let declared = declared_targets(plan);
        let well_formed = validation.starts_with("change_validation: change artifact(s) are valid");
        let uncovered = if well_formed {
            uncovered_targets(generation, &declared)
        } else {
            // A broken diff is not a scope miss: there is no change to place, so
            // coverage stays inert instead of being counted as zero bought.
            Vec::new()
        };
        let cell = Self::judged_cell(validation, &declared, &uncovered);
        let (verdict, score) = cell.verdict_and_score();

        let mut criteria = vec![Criterion {
            name: "change_validation".into(),
            score,
            comment: validation.to_string(),
        }];
        for target in &declared {
            let touched = !uncovered.contains(target);
            criteria.push(Criterion {
                name: format!("plan_target: {target}"),
                score: if touched { 100 } else { 0 },
                comment: if touched {
                    format!("the change touches {target}")
                } else {
                    format!("the change never touches {target}, which the plan named as its target")
                },
            });
        }
        // What the change bought of the plan, as its own criterion. Absent when
        // the plan declared nothing, which keeps that path at the single
        // `change_validation` criterion it has always had.
        let coverage = match cell {
            JudgedCell::Covered { declared } => Some((declared, declared)),
            JudgedCell::Uncovered { bought, declared } => Some((bought, declared)),
            _ => None,
        };
        if let Some((bought, planned)) = coverage {
            let missed = if uncovered.is_empty() {
                "none".to_string()
            } else {
                uncovered.join(", ")
            };
            criteria.push(Criterion {
                name: "plan_coverage".into(),
                score: 100 * bought as u32 / planned.max(1) as u32,
                comment: format!(
                    "the plan named {planned} target(s) and the change bought {bought} of them (missed: {missed})"
                ),
            });
        }

        let feedback = if let JudgedCell::Uncovered { bought, declared } = cell {
            // The repair loop reads this, so the count the score grades is
            // written out: a score of 46 does not say "2 of 3" on its own.
            let touched = change_targets(generation);
            format!(
                "{}; the plan named {} target(s) and the change bought {} of them, missing {} (the diff's targets are: {})",
                validation,
                declared,
                bought,
                uncovered.join(", "),
                if touched.is_empty() {
                    "none".to_string()
                } else {
                    touched.join(", ")
                }
            )
        } else {
            validation.to_string()
        };

        (
            EvaluationResult {
                verdict,
                feedback,
                score: Some(score),
                criteria,
                details: None,
            },
            cell,
        )
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

/// One outcome of the deterministic self-evolution judgement.
///
/// Named cells rather than an if/else chain over measurements, so every verdict
/// and score the judgement can return is decided in one place. The coverage
/// cells carry the counts because a repair loop reading score 20 has to be able
/// to tell buying none of a plan from buying most of it: the grade is the ratio,
/// and the counts are what the criterion and the feedback restate in words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JudgedCell {
    /// No change artifact was produced at all.
    NoArtifact,
    /// A change artifact that is not a unified diff over admissible paths.
    Unappliable,
    /// A valid change under a plan that declared no targets.
    NothingDeclared,
    /// A valid change that touches every target the plan named.
    Covered { declared: usize },
    /// A valid change that bought some of the plan's targets, perhaps none.
    ///
    /// This direction only. Touching paths the plan did not name is allowed on
    /// purpose (the planner is told so): the plan bounds what the change must
    /// cover, not what it may touch, and the apply gate is what bounds the
    /// latter.
    Uncovered { bought: usize, declared: usize },
}

impl JudgedCell {
    /// The published name of this cell.
    ///
    /// The five cells and the five published names have to be one set: they are
    /// counted on the observability side, where a verdict with no name is money
    /// spent with nothing recorded about what it bought. `is_empty`-style
    /// re-derivation is avoided on purpose — a reader that re-derived the cell
    /// from the verdict would fold the vacuous pass (`NothingDeclared`) into the
    /// earned one (`Covered`) and report a planner at work on plans that named
    /// nothing.
    fn cause(self) -> &'static str {
        use crate::observable::{
            SELF_EVOLUTION_VERDICT_AIMED_ELSEWHERE, SELF_EVOLUTION_VERDICT_NO_ARTIFACT,
            SELF_EVOLUTION_VERDICT_NO_TARGETS_DECLARED, SELF_EVOLUTION_VERDICT_UNPARSEABLE,
            SELF_EVOLUTION_VERDICT_WELL_FORMED,
        };
        match self {
            JudgedCell::NoArtifact => SELF_EVOLUTION_VERDICT_NO_ARTIFACT,
            JudgedCell::Unappliable => SELF_EVOLUTION_VERDICT_UNPARSEABLE,
            JudgedCell::NothingDeclared => SELF_EVOLUTION_VERDICT_NO_TARGETS_DECLARED,
            JudgedCell::Covered { .. } => SELF_EVOLUTION_VERDICT_WELL_FORMED,
            JudgedCell::Uncovered { .. } => SELF_EVOLUTION_VERDICT_AIMED_ELSEWHERE,
        }
    }

    /// The verdict and the score this outcome is worth.
    ///
    /// Every cell that existed before the coverage grade keeps its exact score
    /// (0 / 10 / 85), so a plan that declares no targets judges a change exactly
    /// as it did before. A partial purchase is graded by ratio into 20..60: above
    /// the unappliable-diff cell, because the artifact is deliverable in form,
    /// and strictly below the evaluator's own 60 boundary, so a partial purchase
    /// never reads as a pass. Buying none keeps exactly 20.
    fn verdict_and_score(self) -> (Verdict, u32) {
        match self {
            JudgedCell::NoArtifact => (Verdict::Fail, 0),
            JudgedCell::Unappliable => (Verdict::Fail, 10),
            JudgedCell::NothingDeclared | JudgedCell::Covered { .. } => (Verdict::Pass, 85),
            JudgedCell::Uncovered { bought, declared } => {
                let graded = 40 * bought as u32 / declared.max(1) as u32;
                (Verdict::Fail, 20 + graded)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个只会照念一份答案的替身：这条用例判的是**机器判据**在模型说 `ok` 之后
    /// 还能不能把它压回不成立，所以替身必须说 `ok`。
    struct FixedAnswer(serde_json::Value);

    #[async_trait::async_trait]
    impl cog_core::Agent for FixedAnswer {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Ok(self.0.clone())
        }
        async fn start(&self) {}
        async fn snapshot(
            &self,
            _task_id: String,
        ) -> cog_core::SFResult<cog_core::snapshot::AgentCheckpoint> {
            Err(cog_core::SFError::NotImplemented("snapshot".into()))
        }
        async fn restore(
            &self,
            _snapshot: &cog_core::snapshot::AgentCheckpoint,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn continue_(
            &self,
            _input: serde_json::Value,
        ) -> cog_core::SFResult<serde_json::Value> {
            Ok(self.0.clone())
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
            Err(cog_core::SFError::NotImplemented("state".into()))
        }
        async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
            Err(cog_core::SFError::NotImplemented("restore_from_id".into()))
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
            Err(cog_core::SFError::NotImplemented("chat_stream".into()))
        }
        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::NotImplemented("complete_stream".into()))
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
        async fn review_and_revise(
            &self,
            output: &str,
            _config: &cog_core::SelfReviewConfig,
        ) -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
            Ok((
                output.to_string(),
                cog_core::SelfReviewResult::Pass {
                    score: 0.9,
                    summary: "fixed".into(),
                },
            ))
        }
    }


    /// 判据审现在要**去树上取证**：它读终态与「现在」点名的文件，判词说它读到了什么。
    ///
    /// 这条钉的是请求的形状，不是模型的判断：请求里必须出现「读这一轮的工作树」与
    /// 「读不出来就不算通过」。没有这两句，判词只能对着描述说，而描述与树是两件事。
    #[tokio::test]
    async fn the_review_asks_the_judge_to_read_the_tree() {
        use std::sync::{Arc, Mutex};
        struct Recorder(Arc<Mutex<Vec<String>>>);
        #[async_trait::async_trait]
        impl cog_core::Agent for Recorder {
            async fn prompt(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
                self.0
                    .lock()
                    .unwrap()
                    .push(input.get("instructions").and_then(|v| v.as_str()).unwrap_or("").to_string());
                Ok(serde_json::json!({"verdicts": []}))
            }
            async fn start(&self) {}
            async fn snapshot(&self, _t: String) -> cog_core::SFResult<cog_core::snapshot::AgentCheckpoint> {
                Err(cog_core::SFError::NotImplemented("snapshot".into()))
            }
            async fn restore(&self, _s: &cog_core::snapshot::AgentCheckpoint) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn continue_(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
                Ok(input)
            }
            async fn steer(&self, _i: String) -> cog_core::SFResult<()> { Ok(()) }
            async fn abort(&self) -> cog_core::SFResult<()> { Ok(()) }
            async fn reset(&self) -> cog_core::SFResult<()> { Ok(()) }
            async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
                Err(cog_core::SFError::NotImplemented("state".into()))
            }
            async fn wait_for_idle(&self) -> cog_core::SFResult<()> { Ok(()) }
            async fn restore_from_id(&self, _c: &str) -> cog_core::SFResult<()> { Ok(()) }
            fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
                let (_tx, rx) = tokio::sync::broadcast::channel(1);
                rx
            }
            async fn chat_stream(&self, _m: &[cog_core::Message], _o: &cog_core::ChatOptions)
                -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
                Err(cog_core::SFError::NotImplemented("chat_stream".into()))
            }
            async fn complete_stream(&self, _p: &str, _o: &cog_core::CompleteOptions)
                -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
                Err(cog_core::SFError::NotImplemented("complete_stream".into()))
            }
            async fn read_board(&self, _t: &str, _f: &str) -> cog_core::SFResult<Option<String>> { Ok(None) }
            async fn write_board(&self, _t: &str, _f: &str, _v: &str) -> cog_core::SFResult<()> { Ok(()) }
            async fn receive_message(&self, _m: cog_core::InboxMessage) -> cog_core::SFResult<()> { Ok(()) }
            async fn review_and_revise(&self, output: &str, _c: &cog_core::SelfReviewConfig)
                -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
                Ok((output.to_string(), cog_core::SelfReviewResult::Pass { score: 0.9, summary: "ok".into() }))
            }
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let evaluator = EvaluatorActor::new(Arc::new(Recorder(seen.clone())));
        use cog_core::contract::ledger_item::{LedgerItem, LedgerState};
        let items = vec![LedgerItem {
            id: "N1".into(),
            terminal: "窗口里每一轮都带着它需要的全部".into(),
            now: "还没做".into(),
            gap: "缺的是：地板外置".into(),
            state: LedgerState::Open,
        }];
        let _ = evaluator.review_criteria("把上下文做对", &items).await;
        let instructions = seen.lock().unwrap().join("\n");
        assert!(instructions.contains("working tree"), "请求里没让它去读树：{instructions}");
        assert!(instructions.contains("read_file"), "请求里没让它读文件：{instructions}");
        assert!(
            instructions.contains("not ok"),
            "请求里没写「读不出来就不算通过」：{instructions}"
        );
        // 三格各有它的那一问：「现在」要判「照着它能不能读到同一处」，「差距」要判
        // 「是不是恰好只差那一件」。少任何一问，判者就只判了终态那一格。
        assert!(
            instructions.contains("now cell reads like what the tree"),
            "请求里没让判者判「现在」是不是树上读来的：{instructions}"
        );
        assert!(
            instructions.contains("gap is exactly what the terminal still lacks"),
            "请求里没让判者判「差距是不是只差那一件」：{instructions}"
        );
    }

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

    fn plan_naming(paths: &[&str]) -> serde_json::Value {
        serde_json::json!({ "targets": paths })
    }

    fn judged(diff: &str, plan: &serde_json::Value) -> EvaluationResult {
        judged_with_cell(diff, plan).0
    }

    /// The judgement and the cell it landed in, from one reading, so a test can
    /// assert both without the two drifting apart.
    fn judged_with_cell(diff: &str, plan: &serde_json::Value) -> (EvaluationResult, JudgedCell) {
        let generation = generation_with_diff(diff);
        let validation = EvaluatorActor::validate_change_artifacts(&generation);
        EvaluatorActor::judge_with_cell(&validation, &generation, plan)
    }

    fn criterion<'a>(result: &'a EvaluationResult, name: &str) -> &'a Criterion {
        result
            .criteria
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no criterion named {name}: {:?}", result.criteria))
    }

    /// The case the whole check exists for: the plan said the change is about an
    /// existing document, the change created an unrelated source file instead.
    /// The artifact is a perfectly valid diff, so nothing else on this path can
    /// tell the two apart.
    #[test]
    fn a_change_that_never_touches_the_plans_named_file_fails_under_that_criterion() {
        let plan = plan_naming(&["README.md"]);
        let result = judged(
            &create_diff("crates/cogneva/src/windows_quickstart.rs"),
            &plan,
        );

        assert_eq!(result.verdict, Verdict::Fail);
        assert_eq!(criterion(&result, "plan_target: README.md").score, 0);
        // The repair loop reads the feedback, so the unmet target and the paths
        // the diff actually took both have to be in it — a verdict alone would
        // leave the generator guessing which of its files was the wrong one.
        assert!(result.feedback.contains("README.md"), "{}", result.feedback);
        assert!(
            result
                .feedback
                .contains("crates/cogneva/src/windows_quickstart.rs"),
            "{}",
            result.feedback
        );
    }

    #[test]
    fn a_change_that_touches_the_plans_named_file_passes_even_with_extra_files() {
        let plan = plan_naming(&["README.md"]);
        let both = format!(
            "{}{}",
            modify_diff("README.md"),
            create_diff("crates/cogneva/src/quickstart.rs")
        );
        let result = judged(&both, &plan);

        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(criterion(&result, "plan_target: README.md").score, 100);
    }

    /// A plan that declares nothing must leave the judgement byte-for-byte what
    /// it was before targets existed: every runtime whose planner does not emit
    /// them has to keep working unchanged.
    #[test]
    fn a_plan_declaring_no_targets_judges_the_change_as_before() {
        let result = judged(
            &create_diff("crates/cogneva/src/new_module.rs"),
            &serde_json::json!({}),
        );

        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(result.score, Some(85));
        assert_eq!(result.criteria.len(), 1);
        assert_eq!(result.criteria[0].name, "change_validation");
    }

    /// A structurally broken diff is judged as broken, not as a scope miss: the
    /// target check only has meaning once there is a change to place.
    #[test]
    fn an_unappliable_diff_keeps_its_own_cause_whatever_the_plan_declared() {
        let plan = plan_naming(&["README.md"]);
        let generation = generation_with_diff("not a diff at all\n");
        let validation = EvaluatorActor::validate_change_artifacts(&generation);
        let (result, _) =
            EvaluatorActor::judge_self_evolution_change(&validation, &generation, &plan);

        assert_eq!(result.verdict, Verdict::Fail);
        assert_eq!(result.score, Some(10));
        assert!(
            result.feedback.contains("no file paths found"),
            "{}",
            result.feedback
        );
        assert!(
            !result.feedback.contains("does not touch"),
            "{}",
            result.feedback
        );
    }

    /// 五格是一个划分：每一次判定恰好落进一格，五格之和就是判定次数。
    ///
    /// 这条读数是 planner 在这条路上唯一的产出度量——它的调用在账上一直看得见，
    /// 买到了什么一直是空白，于是「planner 有没有提高一次做对」谁答都是猜。
    /// 每个格子都用一具真标本走到，不是断言常量等于自己。
    #[test]
    fn every_judged_change_lands_in_exactly_one_cell() {
        use crate::observable::{
            SELF_EVOLUTION_VERDICT_AIMED_ELSEWHERE, SELF_EVOLUTION_VERDICT_CAUSES,
            SELF_EVOLUTION_VERDICT_NO_ARTIFACT, SELF_EVOLUTION_VERDICT_NO_TARGETS_DECLARED,
            SELF_EVOLUTION_VERDICT_UNPARSEABLE, SELF_EVOLUTION_VERDICT_WELL_FORMED,
        };

        let plan = plan_naming(&["README.md"]);
        let cells = [
            judged_with_cell(&create_diff("crates/cogneva/src/elsewhere.rs"), &plan)
                .1
                .cause(),
            judged_with_cell(&modify_diff("README.md"), &plan).1.cause(),
            judged_with_cell(
                &create_diff("crates/cogneva/src/elsewhere.rs"),
                &serde_json::json!({}),
            )
            .1
            .cause(),
            judged_with_cell("not a diff at all\n", &plan).1.cause(),
        ];
        // 没有产物那一格走不到 diff 上：给它一份 artifacts 空表。
        let empty = serde_json::json!({ "artifacts": [] });
        let validation = EvaluatorActor::validate_change_artifacts(&empty);
        let no_artifact = EvaluatorActor::judge_self_evolution_change(&validation, &empty, &plan).1;

        assert_eq!(cells[0], SELF_EVOLUTION_VERDICT_AIMED_ELSEWHERE);
        assert_eq!(cells[1], SELF_EVOLUTION_VERDICT_WELL_FORMED);
        assert_eq!(cells[2], SELF_EVOLUTION_VERDICT_NO_TARGETS_DECLARED);
        assert_eq!(cells[3], SELF_EVOLUTION_VERDICT_UNPARSEABLE);
        assert_eq!(no_artifact, SELF_EVOLUTION_VERDICT_NO_ARTIFACT);

        // 五格是闭集：一个判定若要写第六个格子，这里先红。
        for cause in cells.into_iter().chain([no_artifact]) {
            assert!(
                SELF_EVOLUTION_VERDICT_CAUSES.contains(&cause),
                "{cause} is not one of the published cells"
            );
        }
        // 「点名了目标且都碰到」与「没点名任何目标」必须是两格：并成一格时，
        // 一个什么都没做的 planner 会被读成一直在干活。
        assert_ne!(cells[1], cells[2]);
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

    /// Buying none of the plan and buying most of it are different outcomes, and
    /// the score has to say so: one flat cell for both leaves the repair loop
    /// unable to tell a change aimed elsewhere from one that missed one target.
    #[test]
    fn buying_none_and_buying_most_of_the_plan_land_in_different_cells() {
        let plan = plan_naming(&["README.md", "docs/a.md", "docs/b.md"]);
        let none = judged_with_cell(&create_diff("crates/cogneva/src/elsewhere.rs"), &plan);
        let most = judged_with_cell(
            &format!("{}{}", modify_diff("README.md"), modify_diff("docs/a.md")),
            &plan,
        );

        assert_eq!(
            none.1,
            JudgedCell::Uncovered {
                bought: 0,
                declared: 3
            }
        );
        assert_eq!(
            most.1,
            JudgedCell::Uncovered {
                bought: 2,
                declared: 3
            }
        );
        assert_eq!(none.0.verdict, Verdict::Fail);
        assert_eq!(most.0.verdict, Verdict::Fail);
        // Buying none keeps exactly the score this cell has always had.
        assert_eq!(none.0.score, Some(20));
        // Two of three lands above it, and still strictly below a pass.
        assert_eq!(most.0.score, Some(46));
        assert!(
            most.0.score > none.0.score,
            "a partial purchase must outscore buying none: {:?} vs {:?}",
            most.0.score,
            none.0.score
        );
        assert!(
            most.0.score.unwrap() < 60,
            "a partial purchase must not read as a pass: {:?}",
            most.0.score
        );
    }

    /// Full coverage is still the pass it always was, and now says so in a
    /// criterion: `plan_coverage` carries the bought fraction, not a second
    /// opinion on the verdict.
    #[test]
    fn full_coverage_is_still_a_pass_and_reads_as_the_whole_plan() {
        let plan = plan_naming(&["README.md", "docs/a.md"]);
        let both = format!("{}{}", modify_diff("README.md"), modify_diff("docs/a.md"));
        let (result, cell) = judged_with_cell(&both, &plan);

        assert_eq!(cell, JudgedCell::Covered { declared: 2 });
        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(result.score, Some(85));
        assert_eq!(criterion(&result, "plan_coverage").score, 100);
    }

    /// The criterion's score is the bought fraction, and its comment names the
    /// targets that were not bought — a ratio with no names would leave the
    /// repair loop to guess which of the plan's files it never touched.
    #[test]
    fn plan_coverage_carries_the_bought_fraction_and_the_missed_targets() {
        let plan = plan_naming(&["README.md", "docs/a.md", "docs/b.md"]);
        let (result, _) = judged_with_cell(&modify_diff("README.md"), &plan);

        let coverage = criterion(&result, "plan_coverage");
        assert_eq!(coverage.score, 33);
        assert!(
            coverage.comment.contains("bought 1 of them"),
            "{}",
            coverage.comment
        );
        for missed in ["docs/a.md", "docs/b.md"] {
            assert!(
                coverage.comment.contains(missed),
                "{missed} is missing from: {}",
                coverage.comment
            );
        }
        // The per-target criteria stay what they were: 100 for a path the change
        // reached, 0 for one it did not.
        assert_eq!(criterion(&result, "plan_target: README.md").score, 100);
        assert_eq!(criterion(&result, "plan_target: docs/a.md").score, 0);
        // The repair loop is told the count the score grades, not left to infer it.
        assert!(
            result.feedback.contains("bought 1 of them"),
            "{}",
            result.feedback
        );
    }

    /// A broken diff makes no coverage claim: there is no change to place, so
    /// "bought none of the plan" would be a second, misleading cause.
    #[test]
    fn a_malformed_diff_makes_no_coverage_claim() {
        let plan = plan_naming(&["README.md"]);
        let (result, cell) = judged_with_cell("not a diff at all\n", &plan);

        assert_eq!(cell, JudgedCell::Unappliable);
        assert_eq!(result.score, Some(10));
        assert!(
            !result.criteria.iter().any(|c| c.name == "plan_coverage"),
            "{:?}",
            result.criteria
        );
        assert!(!result.feedback.contains("bought"), "{}", result.feedback);
    }
}
