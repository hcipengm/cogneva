use std::sync::Arc;

use cog_core::Agent;

use crate::squad::pge::types::PgeRoundtableIteration;

/// Moderator output for Roundtable debate control.
///
/// The moderator reviews the full debate history and decides whether to
/// continue iterating, change strategy, accept a partial result, or escalate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum ModeratorDecision {
    /// Continue to the next iteration with current strategy.
    #[default]
    Continue,
    /// Pivot the discussion angle (e.g. reframe the goal, introduce new
    /// constraints, or ask agents to focus on a specific weakness).
    ChangeStrategy,
    /// Accept the current best result even if full consensus was not reached.
    /// Useful when further iterations yield diminishing returns.
    AcceptPartial,
    /// Escalate to external review / human-in-the-loop.
    Escalate,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModeratorOutput {
    pub decision: ModeratorDecision,
    pub reasoning: String,
    /// Concrete suggestions for the next iteration (e.g. "focus on error
    /// handling", "re-evaluate the schema design").
    pub suggestions: Vec<String>,
    /// Optional flag indicating whether the moderator believes the current
    /// result is "good enough" despite not reaching formal consensus.
    pub good_enough: bool,
}

impl Default for ModeratorOutput {
    fn default() -> Self {
        Self {
            decision: ModeratorDecision::Continue,
            reasoning: String::new(),
            suggestions: Vec::new(),
            good_enough: false,
        }
    }
}

/// Moderator Actor — semantic wrapper around a `dyn Agent`.
///
/// Responsible for reviewing the full debate history and deciding whether
/// to continue, change strategy, accept a partial result, or escalate.
pub struct ModeratorActor {
    agent: Arc<dyn Agent>,
    knowledge: Option<Arc<dyn cog_core::KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
}

impl ModeratorActor {
    pub fn new(agent: Arc<dyn Agent>) -> Self {
        Self {
            agent,
            knowledge: None,
            self_review: None,
            output_schema: None,
        }
    }

    pub fn with_knowledge(mut self, knowledge: Arc<dyn cog_core::KnowledgeBackend>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach a JSON Schema constraining the moderator output. When set, the
    /// schema is injected into the prompt input and the raw LLM output is
    /// validated against it; failures are logged and lenient parsing applies.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Run the Moderator phase: review debate history and render a decision.
    pub async fn moderate(
        &self,
        task: &cog_core::Task,
        history: &[PgeRoundtableIteration],
        context_board: &serde_json::Value,
        consensus_threshold: f64,
    ) -> ModeratorOutput {
        let history_json: Vec<serde_json::Value> = history
            .iter()
            .map(|h| serde_json::to_value(h).unwrap_or_default())
            .collect();

        let mut input = serde_json::json!({
            "goal": task.input.get("goal").cloned().unwrap_or(serde_json::json!(task.task_type)),
            "task_type": format!("{:?}", task.task_type),
            "task_id": task.id,
            "history": history_json,
            "context_board": context_board,
            "iterations": history.len() as u32,
            "consensus_threshold": consensus_threshold,
        });

        // A configured output schema takes precedence over built-in prompt
        // contracts: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            input["output_schema"] = schema.clone();
            input["response_format"] = serde_json::json!("json");
        }

        // Inject historical task execution records if knowledge backend is wired.
        if let Some(ref k) = self.knowledge {
            match k.retrieve_task_history(&task.id).await {
                Ok(records) if !records.is_empty() => {
                    input["historical_executions"] = serde_json::json!(records);
                }
                Err(e) => {
                    tracing::warn!("Moderator knowledge query failed: {}", e);
                }
                _ => {}
            }
        }

        let mut output = match self.agent.prompt(input).await {
            Ok(result) => {
                if let Some(ref schema) = self.output_schema {
                    crate::actors::validate_against_schema(
                        schema,
                        &result.to_string(),
                        "moderator",
                    );
                }
                parse_moderator_output(&result)
            }
            Err(e) => {
                tracing::warn!("Moderator prompt failed: {}", e);
                ModeratorOutput::default()
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            "moderator",
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = parse_moderator_output(&value);
            }
        }
        output
    }
}

/// Parse a raw JSON value into a [`ModeratorOutput`].
/// Backward-compatible: missing fields get sensible defaults.
pub fn parse_moderator_output(value: &serde_json::Value) -> ModeratorOutput {
    let decision = value
        .get("decision")
        .and_then(|v| v.as_str())
        .and_then(|s| match s.to_lowercase().as_str() {
            "continue" | "continuing" | "next" => Some(ModeratorDecision::Continue),
            "change_strategy" | "change strategy" | "pivot" | "reframe" => {
                Some(ModeratorDecision::ChangeStrategy)
            }
            "accept_partial" | "accept partial" | "accept" | "good enough" => {
                Some(ModeratorDecision::AcceptPartial)
            }
            "escalate" | "escalation" | "human" | "handoff" => Some(ModeratorDecision::Escalate),
            _ => None,
        })
        .unwrap_or_default();

    let reasoning = value
        .get("reasoning")
        .or_else(|| value.get("reason"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let suggestions = value
        .get("suggestions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let good_enough = value
        .get("good_enough")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    ModeratorOutput {
        decision,
        reasoning,
        suggestions,
        good_enough,
    }
}
