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
    ///
    /// `consensus_threshold` travels as context, not as a decision the moderator
    /// owns: the debate applies it to every round's own score before asking, so
    /// what it is being asked is what to do about a debate that did not confirm
    /// — never whether it did.
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

        // The stable half: what this role is being asked, the words its answer
        // is spelled with, and the shape the answer takes. None of it moves from
        // one round to the next, while the debate it is being asked about does —
        // so it is assembled apart from the document and handed over under the
        // contract key, which the runtime renders as the leading message. Left in
        // the map, it would serialize behind `consensus_threshold` and
        // `context_board`, which both move. See `crate::actors::with_contract`.
        //
        // The decision words are spelled the way this actor's parser accepts
        // them, not the way the enum variants are named: `parse_moderator_output`
        // lowercases the reply and matches `continue`, `change_strategy`,
        // `accept_partial`, `escalate`. A contract naming `ChangeStrategy` would
        // be naming a spelling the parse reads as none of the four, and the round
        // would continue by default — a decision taken by a word nobody wrote.
        let mut contract = serde_json::json!({
            "instructions": "You are the Moderator of a roundtable debate. Each agent \
             proposed a plan and a generation, and a judge has ruled on the round. Read the \
             debate so far and decide what happens next: you are not asked whether the round \
             passed — that verdict is already in the history — you are asked what to do about \
             a debate that has not confirmed. `decision` is exactly one of: \
             `continue` — another round with the current strategy; \
             `change_strategy` — same goal, a different angle, named in `suggestions`; \
             `accept_partial` — the current best result stands even though consensus was not \
             reached; `escalate` — this needs an external review or a human.",
            "output_schema": {
                "decision": "string: continue | change_strategy | accept_partial | escalate",
                "reasoning": "string: why this is the right call",
                "suggestions": ["string: concrete focus for the next round; required for change_strategy"],
                "good_enough": "bool: whether the current result stands as it is"
            },
            "response_format": "json"
        });

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
            contract["output_schema"] = schema.clone();
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

        let input = crate::actors::with_contract(input, contract);

        let (mut output, review_basis) = match self.agent.prompt_for_task(&task.id, input).await {
            Ok(result) => {
                if let Some(ref schema) = self.output_schema {
                    crate::actors::validate_against_schema(
                        schema,
                        &result.to_string(),
                        "moderator",
                    );
                }
                (
                    parse_moderator_output(&result),
                    // 自进化任务上不审，与 plan 和生成侧同一条理由：这份输出在到这里
                    // 之前已经被解析成结构体，改写只能重写解析器已经接受过的散文，
                    // 而推理型模型会退回自然语言、把改写步挂满整个超时。跳过用同一个
                    // 具名理由，省下的调用因此是读数而不是缺席。
                    if task.is_self_evolution() {
                        crate::actors::ReviewBasis::Skipped(
                            crate::observable::SELF_REVIEW_SKIP_SELF_EVOLUTION,
                        )
                    } else {
                        crate::actors::ReviewBasis::HeldTo(crate::actors::review_spec(task, &[]))
                    },
                )
            }
            Err(e) => {
                tracing::warn!("Moderator prompt failed: {}", e);
                (
                    ModeratorOutput::default(),
                    crate::actors::ReviewBasis::Skipped(
                        crate::observable::SELF_REVIEW_SKIP_UPSTREAM_UNAVAILABLE,
                    ),
                )
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            "moderator",
            review_basis,
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
