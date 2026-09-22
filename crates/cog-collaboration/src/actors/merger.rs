use std::sync::Arc;

use cog_core::Agent;

use crate::squad::pge::types::{
    GeneratorOutput, PgeBranchResult, PlannerOutput, RoundOutcome, StopCause, StoppedProduct,
};

/// Result of merging parallel PGE branches.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergeResult {
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    /// What the merged round reached: a judgement, or the reason it stopped.
    /// A merge that could not judge anything must say so rather than hand back
    /// a verdict-shaped placeholder.
    pub outcome: RoundOutcome,
    /// The branch carried out; `None` when there was no selection to make.
    #[serde(default)]
    pub selected_branch_id: Option<u32>,
    pub reasoning: String,
}

/// Merger Actor — semantic wrapper around a `dyn Agent`.
///
/// Responsible for synthesizing multiple parallel branch results into a single
/// coherent `PgeRoundtableIteration`. Used when the roundtable's merge strategy
/// is set to `Custom`.
pub struct MergerActor {
    agent: Arc<dyn Agent>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
}

impl MergerActor {
    pub fn new(agent: Arc<dyn Agent>) -> Self {
        Self {
            agent,
            self_review: None,
            output_schema: None,
        }
    }

    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach a JSON Schema constraining the merger output. When set, the
    /// schema is injected into the prompt input and the raw LLM output is
    /// validated against it; failures are logged and lenient parsing applies.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Ask the underlying agent to pick the best branch or synthesize a merged
    /// result from the provided branch outputs.
    pub async fn merge(
        &self,
        task: &cog_core::Task,
        branches: &[PgeBranchResult],
        context_board: &serde_json::Value,
    ) -> MergeResult {
        let branches_json: Vec<serde_json::Value> = branches
            .iter()
            .map(|b| serde_json::to_value(b).unwrap_or_default())
            .collect();

        let mut input = serde_json::json!({
            "goal": task.input.get("goal").cloned().unwrap_or(serde_json::json!(task.task_type)),
            "task_type": format!("{:?}", task.task_type),
            "task_id": task.id,
            "branches": branches_json,
            "context_board": context_board,
        });

        // A configured output schema takes precedence over built-in prompt
        // contracts: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            input["output_schema"] = schema.clone();
            input["response_format"] = serde_json::json!("json");
        }

        let mut output = match self.agent.prompt_for_task(&task.id, input).await {
            Ok(result) => {
                if let Some(ref schema) = self.output_schema {
                    crate::actors::validate_against_schema(schema, &result.to_string(), "merger");
                }
                parse_merge_result(&result, branches)
            }
            Err(e) => {
                tracing::warn!("Merger prompt failed: {}", e);
                fallback_best_branch(branches)
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            "merger",
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = parse_merge_result(&value, branches);
            }
        }
        output
    }
}

/// Parse a raw JSON value into a [`MergeResult`].
///
/// A response that does not match the merge contract leaves the branches as the
/// only thing known to exist, so the deterministic best-branch pick applies
/// instead. That pick carries a judgement a judge actually reached; a
/// verdict-shaped placeholder built from the unparsed text would invent one and
/// send it into the debate history as a real review.
pub fn parse_merge_result(value: &serde_json::Value, branches: &[PgeBranchResult]) -> MergeResult {
    serde_json::from_value(value.clone()).unwrap_or_else(|e| {
        tracing::warn!(
            "Merger output did not match the merge contract ({}); falling back to best branch: {}",
            e,
            value
        );
        fallback_best_branch(branches)
    })
}

/// Fallback merge strategy: pick the branch with the highest evaluation score.
/// Only judged branches carry a score, so only they can be ranked.
pub fn fallback_best_branch(branches: &[PgeBranchResult]) -> MergeResult {
    let judged: Vec<&PgeBranchResult> = branches
        .iter()
        .filter(|b| b.outcome.judgement().is_some())
        .collect();

    match crate::squad::pge::types::best_scored(&judged) {
        Some(best) => MergeResult {
            reasoning: format!("Fallback: selected branch {} by best score", best.branch_id),
            plan: best.plan.clone(),
            generation: best.generation.clone(),
            outcome: best.outcome.clone(),
            selected_branch_id: Some(best.branch_id),
        },
        None => MergeResult {
            plan: PlannerOutput {
                summary: String::new(),
                plan: serde_json::json!({}),
                sub_tasks: Vec::new(),
                acceptance_criteria: Vec::new(),
            },
            generation: GeneratorOutput::none(),
            outcome: RoundOutcome::Stopped {
                cause: StopCause::NotAttempted,
                product: StoppedProduct::None,
            },
            selected_branch_id: None,
            reasoning: "No branch reached a judgement to fall back to".into(),
        },
    }
}
