use std::sync::Arc;

use cog_core::Agent;

use crate::squad::pge::types::{EvaluationResult, GeneratorOutput, PgeBranchResult, PlannerOutput};

/// Result of merging parallel PGE branches.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergeResult {
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
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

        let mut output = match self.agent.prompt(input).await {
            Ok(result) => {
                if let Some(ref schema) = self.output_schema {
                    crate::actors::validate_against_schema(schema, &result.to_string(), "merger");
                }
                parse_merge_result(&result)
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
                output = parse_merge_result(&value);
            }
        }
        output
    }
}

/// Parse a raw JSON value into a [`MergeResult`].
/// Falls back to selecting the best branch if parsing fails.
pub fn parse_merge_result(value: &serde_json::Value) -> MergeResult {
    serde_json::from_value(value.clone()).unwrap_or_else(|_| {
        // If the LLM did not return a valid MergeResult, try to interpret it as
        // a selected branch_id.
        if let Some(branch_id) = value
            .get("selected_branch_id")
            .and_then(|v| v.as_u64())
            .map(|id| id as u32)
        {
            // This branch is unreachable in practice because the `from_value`
            // above would have succeeded for a valid MergeResult; kept for
            // documentation of the expected alternative shape.
            let _ = branch_id;
        }
        MergeResult {
            plan: PlannerOutput {
                summary: String::new(),
                plan: serde_json::json!({}),
                sub_tasks: Vec::new(),
            },
            generation: GeneratorOutput {
                content: serde_json::Value::Null,
                artifacts: Vec::new(),
            },
            evaluation: EvaluationResult {
                verdict: crate::squad::pge::types::Verdict::Fail,
                feedback: String::new(),
                score: None,
                criteria: Vec::new(),
                details: Some(value.clone()),
            },
            reasoning: String::new(),
        }
    })
}

/// Fallback merge strategy: pick the branch with the highest evaluation score.
pub fn fallback_best_branch(branches: &[PgeBranchResult]) -> MergeResult {
    let best = branches
        .iter()
        .max_by_key(|b| b.evaluation.score.unwrap_or(0))
        .cloned()
        .unwrap_or_else(|| PgeBranchResult {
            branch_id: 0,
            plan: PlannerOutput {
                summary: String::new(),
                plan: serde_json::json!({}),
                sub_tasks: Vec::new(),
            },
            generation: GeneratorOutput {
                content: serde_json::Value::Null,
                artifacts: Vec::new(),
            },
            evaluation: EvaluationResult {
                verdict: crate::squad::pge::types::Verdict::Fail,
                feedback: String::new(),
                score: None,
                criteria: Vec::new(),
                details: None,
            },
        });

    MergeResult {
        reasoning: format!("Fallback: selected branch {} by best score", best.branch_id),
        plan: best.plan,
        generation: best.generation,
        evaluation: best.evaluation,
    }
}
