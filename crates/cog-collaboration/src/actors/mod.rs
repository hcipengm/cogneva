pub mod context_builder;
pub mod evaluator;
pub mod generator;
pub mod merger;
pub mod mode_selector;
pub mod moderator;
pub mod planner;
pub mod prompt_skill;

pub use context_builder::StandardTaskContextBuilder;
pub use evaluator::EvaluatorActor;
pub use generator::{GeneratorActor, PreviousAttempt};
pub use merger::{fallback_best_branch, parse_merge_result, MergeResult, MergerActor};
pub use mode_selector::ModeSelectorActor;
pub use moderator::{parse_moderator_output, ModeratorActor, ModeratorDecision, ModeratorOutput};
pub use planner::PlannerActor;
pub use prompt_skill::{apply_prompt_skill, resolve_prompt_skill, SKILL_SCHEMA_RESOURCE};

/// Assemble the document handed to the agent: the two halves must arrive as two
/// halves.
///
/// `contract` is everything that states what to produce and does not move from
/// one attempt to the next — the role's instructions, its output schema, the
/// change-format contract. `context` is everything about *this* attempt. The
/// runtime renders the contract first (see `cog_core::contract::prompt`); the
/// two must not be flattened into one map, because a map serializes in key
/// order and the attempt-varying fields sort to the front, which would put a
/// byte that changes on every attempt inside the model's first few characters.
pub(crate) fn actor_input(
    task: &cog_core::Task,
    contract: serde_json::Value,
    context: serde_json::Value,
) -> serde_json::Value {
    with_contract(
        serde_json::json!({
            "task": task,
            "context": context,
        }),
        contract,
    )
}

/// Attach the stable half to a document that is not one of task and context.
///
/// Which half a field belongs to is a property of the request, not of how the
/// varying fields happen to be arranged. An actor whose document is a single
/// flat map still has a half that never moves between attempts — its
/// instructions, its answer contract — and a half that does: the goal, the
/// debate so far, this round's branches. Written under
/// [`cog_core::contract::prompt::PROMPT_CONTRACT_KEY`], the runtime renders the
/// stable half first and drops it from the user message, so the varying fields
/// cannot sit in front of it. Left in the map they are serialized in key order,
/// where the field that changes sorts wherever its name happens to fall —
/// `goal` sorts before `instruction`, and the constant sentence never enters the
/// cacheable prefix at all.
pub(crate) fn with_contract(
    mut document: serde_json::Value,
    contract: serde_json::Value,
) -> serde_json::Value {
    if let Some(fields) = document.as_object_mut() {
        fields.insert(
            cog_core::contract::prompt::PROMPT_CONTRACT_KEY.to_string(),
            contract,
        );
    }
    document
}

/// The specification a review of this task's output is held to.
///
/// The standard has to come from outside the review — a review compared against
/// nothing returns a score the review itself wrote, which no input can
/// contradict. What the actor was asked to satisfy is in hand at every call
/// site, so it is read from the task rather than left empty. `declared` is the
/// criteria the caller handed the actor, when it handed any: for an evaluation
/// those criteria are the standard the judgement must apply.
pub(crate) fn review_spec(task: &cog_core::Task, declared: &[&str]) -> String {
    let goal = task
        .input
        .get("goal")
        .and_then(|g| g.as_str())
        .unwrap_or("")
        .trim();
    let criteria = declared
        .iter()
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>()
        .join("; ");
    match (goal.is_empty(), criteria.is_empty()) {
        (false, false) => format!("{goal}\n\nAcceptance criteria: {criteria}"),
        (false, true) => goal.to_string(),
        (true, false) => format!("Acceptance criteria: {criteria}"),
        (true, true) => String::new(),
    }
}

/// Count a review that was not run, for call sites that stop before the funnel.
///
/// An actor that returns early — a self-evolution evaluation whose verdict is
/// already deterministic, for instance — never reaches [`maybe_self_review`],
/// and the calls it saved would otherwise be indistinguishable from a stage
/// that stopped being reached at all.
pub(crate) fn note_self_review_skip(stage: &str, reason: &'static str) {
    crate::observable::global_observable().record_self_review_skip(stage, reason);
}

/// What a review of an actor's output would be held to, or why it is not run
/// at all.
pub(crate) enum ReviewBasis {
    /// The actor's own output, held to what it was asked to satisfy (see
    /// [`review_spec`]).
    HeldTo(String),
    /// No review of this output, for the stated reason from
    /// [`crate::observable`]'s skip vocabulary.
    ///
    /// The two common reasons are an actor that never reached its upstream —
    /// what the funnel was handed is a placeholder, and reviewing it would buy
    /// two calls for the same unavailable upstream while a revision of a
    /// placeholder replaces a real cause with a plausible-looking answer — and
    /// a stage whose output is deliberately outside the review's remit. Both
    /// are savings, so both are counted where the review would have been.
    Skipped(&'static str),
}

/// Optional self-review helper shared by all actors.
/// Returns the revised output text when the review loop produced a revision;
/// callers are responsible for re-parsing it into their structured output.
///
/// The basis is taken only when the configuration declares no specification of
/// its own. Either way the outcome is counted on the metric plane — the verdict
/// when a review ran, the skip and its reason when one did not — so a stage
/// reviewing with nothing but its own output to compare against, and a stage
/// that was not reviewed at all, are readings rather than assumptions.
pub(crate) async fn maybe_self_review(
    agent: &dyn cog_core::Agent,
    config: &Option<cog_core::SelfReviewConfig>,
    output: &str,
    agent_kind: &str,
    basis: ReviewBasis,
) -> Option<String> {
    let observable = crate::observable::global_observable();
    let Some(config) = config.as_ref() else {
        observable
            .record_self_review_skip(agent_kind, crate::observable::SELF_REVIEW_SKIP_DISABLED);
        return None;
    };
    let spec = match basis {
        ReviewBasis::HeldTo(spec) => spec,
        ReviewBasis::Skipped(reason) => {
            observable.record_self_review_skip(agent_kind, reason);
            return None;
        }
    };
    let cfg = config.clone().with_declared_spec(&spec);
    match agent.review_and_revise(output, &cfg).await {
        Ok((revised, cog_core::SelfReviewResult::Pass { score, summary })) => {
            observable.record_self_review_verdict(
                agent_kind,
                &cfg,
                crate::observable::SELF_REVIEW_VERDICT_PASS,
            );
            tracing::info!(
                agent_kind = %agent_kind,
                score = %score,
                summary = %summary,
                "Self-review passed"
            );
            (revised != output).then_some(revised)
        }
        Ok((
            revised,
            cog_core::SelfReviewResult::NeedRevision {
                critique,
                suggestions,
                score,
            },
        )) => {
            observable.record_self_review_verdict(
                agent_kind,
                &cfg,
                crate::observable::SELF_REVIEW_VERDICT_NEED_REVISION,
            );
            // How the revision step ended. A revision that handed back the
            // text it was given leaves the loop nothing to ask, so the review
            // ends there rather than buying another critique and comparison
            // over the identical text; this cell is those rounds, plus the
            // reviews that reached their last iteration with nothing rewritten.
            // The other cell is recorded too — a loop that never reached a
            // revision and one whose every revision rewrote the text must not
            // read the same.
            observable.record_self_review_revision(
                agent_kind,
                if revised == output {
                    crate::observable::SELF_REVIEW_REVISION_UNCHANGED
                } else {
                    crate::observable::SELF_REVIEW_REVISION_CHANGED
                },
            );
            tracing::warn!(
                agent_kind = %agent_kind,
                score = %score,
                critique = %critique,
                suggestions = ?suggestions,
                "Self-review flagged for revision"
            );
            (revised != output).then_some(revised)
        }
        Err(e) => {
            observable.record_self_review_verdict(
                agent_kind,
                &cfg,
                crate::observable::SELF_REVIEW_VERDICT_FAILED,
            );
            tracing::warn!(
                agent_kind = %agent_kind,
                "Self-review failed: {}",
                e
            );
            None
        }
    }
}

/// Validate raw actor output against a configured JSON Schema.
///
/// Returns `true` when the output parses as JSON and satisfies the schema.
/// Failures are logged as warnings; callers always keep their legacy
/// lenient parsing so a mis-configured schema can never break the pipeline.
pub(crate) fn validate_against_schema(
    schema: &serde_json::Value,
    output: &str,
    agent_kind: &str,
) -> bool {
    let value: serde_json::Value = match serde_json::from_str(output) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                agent_kind = %agent_kind,
                error = %e,
                "Actor output is not valid JSON; cannot apply configured schema"
            );
            return false;
        }
    };

    let validator = match jsonschema::validator_for(schema) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                agent_kind = %agent_kind,
                error = %e,
                "Configured PGE output schema is invalid; ignoring it"
            );
            return true;
        }
    };

    let errors: Vec<String> = validator
        .iter_errors(&value)
        .map(|e| e.to_string())
        .collect();
    if errors.is_empty() {
        true
    } else {
        tracing::warn!(
            agent_kind = %agent_kind,
            errors = ?errors,
            "Actor output failed configured schema validation"
        );
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{maybe_self_review, review_spec, validate_against_schema};
    use crate::observable::{
        global_observable, SELF_REVIEW_CRITERIA_ABSENT, SELF_REVIEW_CRITERIA_DECLARED,
        SELF_REVIEW_SKIP_DISABLED, SELF_REVIEW_SKIP_UPSTREAM_UNAVAILABLE,
        SELF_REVIEW_VERDICT_NEED_REVISION, SELF_REVIEW_VERDICT_PASS,
    };
    use cog_core::observability::Observable;
    use std::sync::{Arc, Mutex};

    fn task_with_goal(goal: &str) -> cog_core::Task {
        cog_core::Task::new(
            "t-1",
            cog_core::TaskType::Planner,
            serde_json::json!({ "goal": goal }),
        )
    }

    /// A stage name this one test owns, for reading a counter the production
    /// paths also write.
    ///
    /// The self-review counters live on the process-global observable, and every
    /// test in this binary runs in that one process, so a difference read of a
    /// cell a production path also writes is a read of a counter other tests may
    /// be moving at the same moment: reading `{stage="moderator",
    /// reason="disabled"}` against its own earlier value once landed five ahead
    /// of what the call under test had left there. The role and the case both go
    /// into the name, so no two tests write the same cell; a name no production
    /// path passes also proves the stage that was recorded came from the
    /// argument rather than from a constant somewhere.
    fn own_stage(role: &str, case: &str) -> String {
        format!("{role} ({case}; own stage)")
    }

    /// What one cell of the review counter reads. The stage has to be one the
    /// caller owns (see [`own_stage`]): an exact count of a cell that other
    /// tests also write is a count of what they wrote.
    async fn cell(stage: &str, criteria: &str, verdict: &str) -> f64 {
        global_observable()
            .collect_metrics("D8")
            .await
            .unwrap()
            .iter()
            .find(|m| {
                m.name == "self_review_verdict_total"
                    && m.labels.get("stage").map(String::as_str) == Some(stage)
                    && m.labels.get("criteria").map(String::as_str) == Some(criteria)
                    && m.labels.get("verdict").map(String::as_str) == Some(verdict)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// What one cell of the skip counter reads, on the same terms as [`cell`]:
    /// the stage has to be one the caller owns.
    async fn skip_cell(stage: &str, reason: &str) -> f64 {
        global_observable()
            .collect_metrics("D8")
            .await
            .unwrap()
            .iter()
            .find(|m| {
                m.name == "self_review_skipped_total"
                    && m.labels.get("stage").map(String::as_str) == Some(stage)
                    && m.labels.get("reason").map(String::as_str) == Some(reason)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// What one cell of the revision counter reads, on the same terms as
    /// [`cell`]: the stage has to be one the caller owns.
    async fn revision_cell(stage: &str, outcome: &str) -> f64 {
        global_observable()
            .collect_metrics("D8")
            .await
            .unwrap()
            .iter()
            .find(|m| {
                m.name == "self_review_revision_total"
                    && m.labels.get("stage").map(String::as_str) == Some(stage)
                    && m.labels.get("outcome").map(String::as_str) == Some(outcome)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// The agent under review, recording the configuration each review was
    /// actually handed — the only place a filled-in specification is visible
    /// from outside the review.
    struct ReviewRecorder {
        seen: Seen,
        pass: bool,
        /// The text the revision step hands back, when the review asks for one.
        /// `None` is the revision that rewrote nothing — the text it was given,
        /// unchanged — which is a different ending from a rewrite.
        revised: Option<String>,
    }

    /// 记录表放在替身外面：断言经 `dyn Agent` 读的是同一份。
    type Seen = Arc<Mutex<Vec<cog_core::SelfReviewConfig>>>;

    impl ReviewRecorder {
        fn recording(pass: bool) -> (Arc<dyn cog_core::Agent>, Seen) {
            Self::recording_with_revision(pass, None)
        }

        fn recording_with_revision(
            pass: bool,
            revised: Option<&str>,
        ) -> (Arc<dyn cog_core::Agent>, Seen) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let agent = ReviewRecorder {
                seen: seen.clone(),
                pass,
                revised: revised.map(str::to_string),
            };
            (Arc::new(agent), seen)
        }
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for ReviewRecorder {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Err(cog_core::SFError::NotImplemented("prompt".into()))
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
            Err(cog_core::SFError::NotImplemented("continue_".into()))
        }
        async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
            Err(cog_core::SFError::NotImplemented("steer".into()))
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
            config: &cog_core::SelfReviewConfig,
        ) -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
            self.seen.lock().unwrap().push(config.clone());
            let result = if self.pass {
                cog_core::SelfReviewResult::Pass {
                    score: 0.9,
                    summary: "mock review".into(),
                }
            } else {
                cog_core::SelfReviewResult::NeedRevision {
                    critique: "no rollback step".into(),
                    suggestions: vec!["name the file".into()],
                    score: 0.2,
                }
            };
            let revised = self.revised.clone().unwrap_or_else(|| output.to_string());
            Ok((revised, result))
        }
    }

    /// A review of an actor's output is held to what that actor was asked to
    /// do, taken from the task rather than left empty: a comparison with
    /// nothing outside the review in it returns a score the review itself
    /// wrote, and the pass/fail decision reads exactly that number.
    #[tokio::test]
    async fn a_review_is_held_to_the_task_it_was_asked_to_satisfy() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let stage = own_stage("planner", "held to the task");
        let before = cell(
            &stage,
            SELF_REVIEW_CRITERIA_DECLARED,
            SELF_REVIEW_VERDICT_PASS,
        )
        .await;

        let _ = maybe_self_review(
            agent.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            &stage,
            crate::actors::ReviewBasis::HeldTo(review_spec(
                &task_with_goal("add a rollback step"),
                &["the plan names the file"],
            )),
        )
        .await;

        let handed = seen.lock().unwrap().clone();
        assert_eq!(handed.len(), 1, "one review was run");
        assert_eq!(
            handed[0].spec.as_deref(),
            Some("add a rollback step\n\nAcceptance criteria: the plan names the file"),
            "the task's own words are the specification the review is held to"
        );
        assert_eq!(
            cell(
                &stage,
                SELF_REVIEW_CRITERIA_DECLARED,
                SELF_REVIEW_VERDICT_PASS
            )
            .await,
            before + 1.0,
            "a review with a standard in it is counted as such, under the stage it was given"
        );
    }

    /// A specification configured deliberately is the standard that was meant
    /// to be applied, so the task's own words do not displace it.
    #[tokio::test]
    async fn a_configured_specification_outranks_the_tasks_own_words() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let config = cog_core::SelfReviewConfig {
            spec: Some("the operator's standard".into()),
            ..Default::default()
        };

        let _ = maybe_self_review(
            agent.as_ref(),
            &Some(config),
            "draft",
            "merger",
            crate::actors::ReviewBasis::HeldTo(review_spec(
                &task_with_goal("merge the branches"),
                &[],
            )),
        )
        .await;

        assert_eq!(
            seen.lock().unwrap()[0].spec.as_deref(),
            Some("the operator's standard")
        );
    }

    /// A task that declares nothing leaves the review with no external
    /// criterion, and that is a reading rather than a silent default: the
    /// verdict is counted under the cell that says so.
    #[tokio::test]
    async fn a_review_with_nothing_declared_is_counted_as_running_without_one() {
        let (agent, seen) = ReviewRecorder::recording(false);
        let stage = own_stage("moderator", "nothing declared");
        let before = cell(
            &stage,
            SELF_REVIEW_CRITERIA_ABSENT,
            SELF_REVIEW_VERDICT_NEED_REVISION,
        )
        .await;

        let _ = maybe_self_review(
            agent.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            &stage,
            crate::actors::ReviewBasis::HeldTo(review_spec(
                &cog_core::Task::new("t-2", cog_core::TaskType::Planner, serde_json::json!({})),
                &[],
            )),
        )
        .await;

        assert_eq!(seen.lock().unwrap()[0].spec, None);
        assert_eq!(
            cell(
                &stage,
                SELF_REVIEW_CRITERIA_ABSENT,
                SELF_REVIEW_VERDICT_NEED_REVISION
            )
            .await,
            before + 1.0,
            "the gate objected, and the stage had nothing but its own text to object to"
        );
    }

    /// 改写这一步买到了什么，两种结局各自成格。
    ///
    /// 「循环从没走到改写」与「改写每次都在重写文本」在判定面上完全同形，
    /// 而前者省不下任何东西、后者省下的正是下一轮那两次调用。
    #[tokio::test]
    async fn a_revision_is_counted_by_what_it_rewrote() {
        let (_agent_rewriting, _) = ReviewRecorder::recording_with_revision(false, Some("better"));
        let agent_rewriting = _agent_rewriting;
        let _ = maybe_self_review(
            agent_rewriting.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            "revision_probe_rewrote",
            crate::actors::ReviewBasis::HeldTo("spec".into()),
        )
        .await;
        assert_eq!(
            revision_cell(
                "revision_probe_rewrote",
                crate::observable::SELF_REVIEW_REVISION_CHANGED
            )
            .await,
            1.0
        );
        assert_eq!(
            revision_cell(
                "revision_probe_rewrote",
                crate::observable::SELF_REVIEW_REVISION_UNCHANGED
            )
            .await,
            0.0
        );

        let (agent_noop, _) = ReviewRecorder::recording(false);
        let _ = maybe_self_review(
            agent_noop.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            "revision_probe_noop",
            crate::actors::ReviewBasis::HeldTo("spec".into()),
        )
        .await;
        assert_eq!(
            revision_cell(
                "revision_probe_noop",
                crate::observable::SELF_REVIEW_REVISION_UNCHANGED
            )
            .await,
            1.0,
            "改写交回原文 = 循环就此收尾，这一格是没买的那些轮"
        );
        assert_eq!(
            revision_cell(
                "revision_probe_noop",
                crate::observable::SELF_REVIEW_REVISION_CHANGED
            )
            .await,
            0.0
        );
    }

    /// An actor that never reached its upstream produced a placeholder, not a
    /// deliverable. The two calls a review would spend would go to the same
    /// unavailable upstream, and a revision of a placeholder replaces a real
    /// cause with a plausible-looking answer — so no review is run, and the
    /// calls it saved are counted rather than silently skipped.
    #[tokio::test]
    async fn a_placeholder_is_not_reviewed_and_the_calls_it_saved_are_counted() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let stage = own_stage("evaluator", "placeholder");
        let before = skip_cell(&stage, SELF_REVIEW_SKIP_UPSTREAM_UNAVAILABLE).await;

        let revised = maybe_self_review(
            agent.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "{\"verdict\":\"Fail\"}",
            &stage,
            crate::actors::ReviewBasis::Skipped(SELF_REVIEW_SKIP_UPSTREAM_UNAVAILABLE),
        )
        .await;

        assert!(
            revised.is_none(),
            "the placeholder is handed back untouched"
        );
        assert!(
            seen.lock().unwrap().is_empty(),
            "no LLM call is bought for an upstream that just failed"
        );
        assert_eq!(
            skip_cell(&stage, SELF_REVIEW_SKIP_UPSTREAM_UNAVAILABLE).await,
            before + 1.0
        );
    }

    /// A stage with reviews turned off is a saving too, and reads as its own
    /// reason: it was not a gate that objected.
    #[tokio::test]
    async fn a_stage_with_reviews_disabled_reports_the_skip_it_is() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let stage = own_stage("moderator", "reviews off");
        let before = skip_cell(&stage, SELF_REVIEW_SKIP_DISABLED).await;

        let revised = maybe_self_review(
            agent.as_ref(),
            &None,
            "draft",
            &stage,
            crate::actors::ReviewBasis::HeldTo(String::new()),
        )
        .await;

        assert!(revised.is_none());
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(
            skip_cell(&stage, SELF_REVIEW_SKIP_DISABLED).await,
            before + 1.0
        );
    }

    #[test]
    fn the_specification_names_the_goal_the_criteria_or_both() {
        assert_eq!(review_spec(&task_with_goal("ship it"), &[]), "ship it");
        assert_eq!(
            review_spec(&task_with_goal(""), &["no rollback step"]),
            "Acceptance criteria: no rollback step"
        );
        assert_eq!(
            review_spec(&task_with_goal("ship it"), &["no rollback step", " "]),
            "ship it\n\nAcceptance criteria: no rollback step",
            "a blank criterion is not a criterion"
        );
        assert_eq!(
            review_spec(&task_with_goal("  "), &[]),
            "",
            "a task that declares nothing leaves the specification empty"
        );
    }

    fn planner_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "required": ["summary", "sub_tasks"],
            "properties": {
                "summary": { "type": "string" },
                "plan": { "type": "object" },
                "sub_tasks": { "type": "array" }
            }
        })
    }

    #[test]
    fn schema_validation_accepts_conforming_output() {
        let output = r#"{"summary": "do the thing", "plan": {}, "sub_tasks": []}"#;
        assert!(validate_against_schema(
            &planner_schema(),
            output,
            "planner"
        ));
    }

    #[test]
    fn schema_validation_rejects_missing_required_fields() {
        let output = r#"{"plan": {}}"#;
        assert!(!validate_against_schema(
            &planner_schema(),
            output,
            "planner"
        ));
    }

    #[test]
    fn schema_validation_rejects_non_json_output() {
        assert!(!validate_against_schema(
            &planner_schema(),
            "not json at all",
            "planner"
        ));
    }

    #[test]
    fn invalid_schema_is_ignored_not_fatal() {
        let bad_schema = serde_json::json!({"type": "nonsense-type"});
        assert!(validate_against_schema(
            &bad_schema,
            r#"{"a": 1}"#,
            "planner"
        ));
    }
}
