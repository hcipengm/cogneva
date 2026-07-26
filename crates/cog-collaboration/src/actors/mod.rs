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

/// Optional self-review helper shared by all actors.
/// Returns the revised output text when the review loop produced a revision;
/// callers are responsible for re-parsing it into their structured output.
pub(crate) async fn maybe_self_review(
    agent: &dyn cog_core::Agent,
    config: &Option<cog_core::SelfReviewConfig>,
    output: &str,
    agent_kind: &str,
) -> Option<String> {
    let cfg = config.as_ref()?;
    match agent.review_and_revise(output, cfg).await {
        Ok((revised, cog_core::SelfReviewResult::Pass { score, summary })) => {
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
    use super::validate_against_schema;

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
