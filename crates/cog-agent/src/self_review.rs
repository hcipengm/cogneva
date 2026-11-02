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
    /// The DAG task this review is being run for, when it is being run for one.
    ///
    /// 与 `actor` 分开记，因为两者答的不是同一问：`actor` 答「哪个角色在花」，
    /// 这一格答「哪件活花的」。只有前者时，一次自检的花销落在
    /// `self_review:<role>` 上，按任务看成本的那一面读不到它——账本里这一族
    /// 2026-11-02 实测 354 次成功调用、任务 id 全空。它不是另一个执行者，
    /// 所以不并进 `actor`：并进去等于把一个角色的两件事算成两个人。
    task: Option<String>,
}

impl SelfReviewLoop {
    pub fn new(config: SelfReviewConfig) -> Self {
        Self {
            config,
            actor: DEFAULT_SELF_REVIEW_ACTOR.to_string(),
            task: None,
        }
    }

    /// Report this loop's token spend under `actor`, so a caller that reviews
    /// on behalf of a specific agent can be charged for it.
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = actor.into();
        self
    }

    /// Charge this review's spend to `task`, so "what did this change cost"
    /// counts the calls spent judging it rather than only the ones that wrote it.
    pub fn with_task(mut self, task: impl Into<String>) -> Self {
        let task = task.into();
        if !task.trim().is_empty() {
            self.task = Some(task);
        }
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
            let critique = SelfReviewLoop::critique(
                &observation,
                spec,
                &self.actor,
                self.task.as_deref(),
                llm,
            )
            .await?;

            // Step 3: Compare
            let comparison = SelfReviewLoop::compare(
                &critique,
                spec,
                &self.config.best_practices,
                &self.actor,
                self.task.as_deref(),
                llm,
            )
            .await?;

            // Step 4: Decide
            let result =
                SelfReviewLoop::decide(&critique, &comparison, self.config.quality_threshold);

            match &result {
                SelfReviewResult::Pass { .. } => {
                    return Ok((current_output, result));
                }
                SelfReviewResult::NeedRevision { .. } => {
                    // Step 5: Revise
                    let previous = current_output.clone();
                    current_output = SelfReviewLoop::revise(
                        &result,
                        &current_output,
                        &self.actor,
                        self.task.as_deref(),
                        llm,
                    )
                    .await?;

                    // A revision that rewrote nothing leaves the loop with
                    // nothing left to ask: the next critique and comparison
                    // would be built from the same text, the same spec and the
                    // same best practices as the pair just paid for, so a
                    // second round is a reroll of a question already answered —
                    // and an unchanged text cannot be improved by asking twice.
                    // Stop here and report the judgement already reached. The
                    // caller sees the unchanged text and counts it.
                    if current_output == previous {
                        return Ok((current_output, result));
                    }

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
        task: Option<&str>,
        llm: &dyn LlmClient,
    ) -> SFResult<Critique> {
        let user_payload = serde_json::json!({
            "output": obs.output,
            "spec": spec,
        });
        let user_msg = Message::user(user_payload.to_string());
        let options = stamp(ChatOptions::default(), actor, task);

        let output = execute_structured::<CritiqueOutput>(llm, &[user_msg], &options).await?;
        let raw = serde_json::to_string(&output).unwrap_or_default();

        Ok(Critique {
            issues: output.issues,
            missing: output.missing,
            strengths: output.strengths,
            raw,
        })
    }

    /// Step 3: Compare — compare critique against the standard in force.
    ///
    /// The specification is part of this step's input, not only the critique
    /// step's: this is where the score is produced, and a score judged against
    /// nothing but the reviewer's own findings is a number no caller can
    /// contradict — the loop's pass/fail would rest on it either way.
    pub async fn compare(
        critique: &Critique,
        spec: &str,
        best_practices: &[String],
        actor: &str,
        task: Option<&str>,
        llm: &dyn LlmClient,
    ) -> SFResult<Comparison> {
        let user_payload = serde_json::json!({
            "issues": critique.issues,
            "missing": critique.missing,
            "strengths": critique.strengths,
            "spec": spec,
            "best_practices": best_practices,
        });
        let user_msg = Message::user(user_payload.to_string());
        let options = stamp(ChatOptions::default(), actor, task);

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
    pub fn decide(
        critique: &Critique,
        comparison: &Comparison,
        threshold: f32,
    ) -> SelfReviewResult {
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
            // The revision is told what the critique step found, not a
            // restatement of it: `issues`/`missing` name what is wrong in more
            // detail than the gaps derived from them, and handing `revise` the
            // coarser text left it less informed than the call before it. The
            // gap restatement stays as the fallback, so a critique that found
            // nothing is not an empty instruction.
            let findings: Vec<String> = critique
                .issues
                .iter()
                .chain(critique.missing.iter())
                .cloned()
                .collect();
            let critique_text = if findings.is_empty() {
                comparison.gaps.join("; ")
            } else {
                findings.join("; ")
            };
            SelfReviewResult::NeedRevision {
                critique: critique_text,
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
        task: Option<&str>,
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

        let options = stamp(
            ChatOptions {
                response_format: ResponseFormat::Text,
                ..Default::default()
            },
            actor,
            task,
        );

        // The revision is the only step with no schema to say what it answers:
        // the payload names the text, the critique and the suggestions, but
        // nothing says what to do with them. Asked without that sentence, a
        // model that merely rewords the text is indistinguishable from one that
        // fixed it — the loop then either pays for another round or accepts the
        // text it asked to have changed.
        //
        // It leads the request as a system message for a second reason: the
        // payload below differs on every call, so anything in front of it is
        // the only part an upstream's prefix cache can reuse.
        let instruction = Message::system(
            "Rewrite the original text so that it answers every point in the critique and \
             the suggestions, keeping every part the critique does not object to. Answer with \
             the revised text only: no preamble, no markdown fences, no commentary about the \
             change.",
        );

        let response = llm.chat(&[instruction, user_msg], &options).await?;
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

/// 把「谁在花」与「哪件活花的」两个身份贴到一次请求上。
///
/// 两格都要，因为账本按它们分别取行：只贴角色时，自检这一族 354 次成功调用在
/// 按任务读成本的那一面全是空 id；只贴任务时，「哪个角色在花」就答不出来。
/// 没有任务可贴的那几次（常驻角色、临时评测）不编一个 id——编出来的会读成一件
/// 真花过钱的任务。
fn stamp(options: ChatOptions, actor: &str, task: Option<&str>) -> ChatOptions {
    let options = options.with_actor(actor);
    match task {
        Some(task) => options.with_task(task),
        None => options,
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
