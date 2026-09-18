use cog_core::{
    execute_structured, ChatOptions, ChatResponse, LlmClient, Message, ResponseFormat, SFError,
    SFResult, SelfReviewConfig, SelfReviewResult,
};

/// Observation of an agent output.
#[derive(Debug, Clone)]
pub struct Observation {
    pub output: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Critical assessment produced by the LLM.
#[derive(Debug, Clone)]
pub struct Critique {
    pub issues: Vec<String>,
    pub missing: Vec<String>,
    pub strengths: Vec<String>,
    pub raw: String,
}

/// Comparison against standards / best practices.
#[derive(Debug, Clone)]
pub struct Comparison {
    pub gaps: Vec<String>,
    pub aligned: Vec<String>,
    pub score: f32,
    pub raw: String,
}

// ---------------------------------------------------------------------------
// Structured output schemas for execute_structured
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct CritiqueOutput {
    #[serde(default)]
    issues: Vec<String>,
    #[serde(default)]
    missing: Vec<String>,
    #[serde(default)]
    strengths: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct ComparisonOutput {
    #[serde(default)]
    gaps: Vec<String>,
    #[serde(default)]
    aligned: Vec<String>,
    score: f32,
}

/// Actor label for reviews whose caller did not identify itself. Self-review
/// runs on behalf of every agent, so a fixed label here would merge all of
/// their review spend into one bucket and make a per-agent budget impossible;
/// the label therefore defaults to the component alone.
pub const DEFAULT_SELF_REVIEW_ACTOR: &str = "self_review";

/// The actor label a review on behalf of `role` reports its token spend under.
/// The gateway bounds the label vocabulary by accepting only short lowercase
/// tokens, so a role outside that vocabulary is charged to `unknown` rather
/// than to a bucket nobody asked for.
pub fn self_review_actor(role: &str) -> String {
    format!("{DEFAULT_SELF_REVIEW_ACTOR}:{role}")
}

/// Self-Review Loop: a 5-step quality gate for agent outputs.
/// ```text
/// Observe → Critique → Compare → Decide → Revise → Log
/// ```
#[derive(Debug, Clone)]
pub struct SelfReviewLoop {
    pub config: SelfReviewConfig,
    actor: String,
}

impl SelfReviewLoop {
    pub fn new(config: SelfReviewConfig) -> Self {
        Self {
            config,
            actor: DEFAULT_SELF_REVIEW_ACTOR.to_string(),
        }
    }

    /// Report this loop's token spend under `actor`, so a caller that reviews
    /// on behalf of a specific agent can be charged for it.
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = actor.into();
        self
    }

    /// Run self-review on any serializable output.
    /// Returns the revised output if review succeeds and deserialization
    /// succeeds; otherwise returns the original output unchanged.
    pub async fn review_output<T>(&self, output: &T, llm: &dyn LlmClient) -> SFResult<T>
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de> + std::fmt::Debug + Clone,
    {
        let serialized = match serde_json::to_string_pretty(output) {
            Ok(s) => s,
            Err(_) => return Ok(output.clone()),
        };

        match self.review(&serialized, llm).await {
            Ok((revised_text, _)) => {
                if let Ok(revised) = serde_json::from_str::<T>(&revised_text) {
                    return Ok(revised);
                }
                Ok(output.clone())
            }
            Err(_) => Ok(output.clone()),
        }
    }

    /// Run the full self-review pipeline on an agent output.
    /// Returns the (possibly revised) output and the final review result.
    pub async fn review(
        &self,
        output: &str,
        llm: &dyn LlmClient,
    ) -> SFResult<(String, SelfReviewResult)> {
        let mut current_output = output.to_string();

        for iteration in 0..self.config.max_iterations {
            // Step 1: Observe
            let observation = SelfReviewLoop::observe(&current_output);

            // Step 2: Critique
            let spec = self.config.spec.as_deref().unwrap_or("");
            let critique = SelfReviewLoop::critique(&observation, spec, &self.actor, llm).await?;

            // Step 3: Compare
            let comparison =
                SelfReviewLoop::compare(&critique, &self.config.best_practices, &self.actor, llm)
                    .await?;

            // Step 4: Decide
            let result = SelfReviewLoop::decide(&comparison, self.config.quality_threshold);

            match &result {
                SelfReviewResult::Pass { .. } => {
                    return Ok((current_output, result));
                }
                SelfReviewResult::NeedRevision { .. } => {
                    // Step 5: Revise
                    current_output =
                        SelfReviewLoop::revise(&result, &current_output, &self.actor, llm).await?;

                    // If this was the last allowed iteration, return the revised output
                    // with the last NeedRevision result so callers can log it.
                    if iteration == self.config.max_iterations - 1 {
                        return Ok((current_output, result));
                    }
                }
            }
        }

        // Should never reach here, but fall back to returning the output as-is.
        Ok((
            current_output,
            SelfReviewResult::Pass {
                score: 1.0,
                summary: "Fallback pass after max iterations".into(),
            },
        ))
    }

    /// Step 1: Observe — wrap the agent output.
    pub fn observe(output: &str) -> Observation {
        Observation {
            output: output.to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Step 2: Critique — ask the LLM for a critical assessment.
    pub async fn critique(
        obs: &Observation,
        spec: &str,
        actor: &str,
        llm: &dyn LlmClient,
    ) -> SFResult<Critique> {
        let user_payload = serde_json::json!({
            "output": obs.output,
            "spec": spec,
        });
        let user_msg = Message::user(user_payload.to_string());
        let options = ChatOptions::default().with_actor(actor);

        let output = execute_structured::<CritiqueOutput>(llm, &[user_msg], &options).await?;
        let raw = serde_json::to_string(&output).unwrap_or_default();

        Ok(Critique {
            issues: output.issues,
            missing: output.missing,
            strengths: output.strengths,
            raw,
        })
    }

    /// Step 3: Compare — compare critique against best practices.
    pub async fn compare(
        critique: &Critique,
        best_practices: &[String],
        actor: &str,
        llm: &dyn LlmClient,
    ) -> SFResult<Comparison> {
        let user_payload = serde_json::json!({
            "issues": critique.issues,
            "missing": critique.missing,
            "strengths": critique.strengths,
            "best_practices": best_practices,
        });
        let user_msg = Message::user(user_payload.to_string());
        let options = ChatOptions::default().with_actor(actor);

        let output = execute_structured::<ComparisonOutput>(llm, &[user_msg], &options).await?;
        let raw = serde_json::to_string(&output).unwrap_or_default();

        Ok(Comparison {
            gaps: output.gaps,
            aligned: output.aligned,
            score: output.score.clamp(0.0, 1.0),
            raw,
        })
    }

    /// Step 4: Decide — PASS or NEED_REVISION based on score vs threshold.
    pub fn decide(comparison: &Comparison, threshold: f32) -> SelfReviewResult {
        if comparison.score >= threshold {
            SelfReviewResult::Pass {
                score: comparison.score,
                summary: format!(
                    "Quality score {:.2} meets threshold {:.2}. Gaps: {}. Aligned: {}.",
                    comparison.score,
                    threshold,
                    comparison.gaps.len(),
                    comparison.aligned.len()
                ),
            }
        } else {
            SelfReviewResult::NeedRevision {
                critique: comparison.gaps.join("; "),
                suggestions: comparison.gaps.clone(),
                score: comparison.score,
            }
        }
    }

    /// Step 5: Revise — ask the LLM to improve the output.
    pub async fn revise(
        result: &SelfReviewResult,
        original: &str,
        actor: &str,
        llm: &dyn LlmClient,
    ) -> SFResult<String> {
        let (critique, suggestions) = match result {
            SelfReviewResult::NeedRevision {
                critique,
                suggestions,
                ..
            } => (critique.clone(), suggestions.clone()),
            SelfReviewResult::Pass { .. } => return Ok(original.to_string()),
        };

        let user_payload = serde_json::json!({
            "original": original,
            "critique": critique,
            "suggestions": suggestions,
        });
        let user_msg = Message::user(user_payload.to_string());

        let options = ChatOptions {
            response_format: ResponseFormat::Text,
            ..Default::default()
        }
        .with_actor(actor);

        let response = llm.chat(&[user_msg], &options).await?;
        // A backend that could not serve the request answers with no content and
        // the reason in `error_message`. Returning that as a revision would hand
        // the caller an empty string where the caller had a deliverable, and an
        // empty string is a different string, so it would be applied.
        if let Some(reason) = response.error_message.as_deref() {
            return Err(SFError::LLM(format!("self-review revise failed: {reason}")));
        }
        let revised = extract_text(&response);
        // The same guarantee for a provider that succeeded without producing
        // anything: no revision is not an empty revision.
        if revised.trim().is_empty() {
            return Ok(original.to_string());
        }

        Ok(revised)
    }
}

/// Extract plain text from a ChatResponse.
fn extract_text(response: &ChatResponse) -> String {
    response
        .content
        .iter()
        .filter_map(|b| b.as_text())
        .collect::<Vec<_>>()
        .join("")
}
