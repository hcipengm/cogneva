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

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------
//
// 这两份请求体用结构体而不是 `serde_json::json!` 拼，因为字段的书写顺序就是
// 线上字节的顺序，而上游的前缀缓存只认从第一个字节起的逐字节相同：`json!`
// 建的是按键排序的 map（没有开 `preserve_order`），`json!({"spec": …, "output": …})`
// 上线是 `{"output": …, "spec": …}`，标准又落回正文后面，等于没改。
// serde 按声明顺序写字段，所以这里的声明顺序就是刻意排的。

/// 评审请求：先是被对照的标准，再是这一轮要审的正文。
#[derive(serde::Serialize)]
struct CritiqueRequest<'a> {
    spec: &'a str,
    output: &'a str,
}

/// 对照请求：先是被对照的标准，再是这一轮重新生成的发现。
#[derive(serde::Serialize)]
struct CompareRequest<'a> {
    spec: &'a str,
    best_practices: &'a [String],
    issues: &'a [String],
    missing: &'a [String],
    strengths: &'a [String],
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
        // 标准在前、被审的正文在后。两者每轮都在变的一次评审里只有标准是
        // 同一份：第二轮审的是改过的正文，但仍是同一件活、同一段标准，把它
        // 放在首位让这一段逐字节相同，上游的前缀缓存才有东西可留。反过来的
        // 顺序在这一族上是实测的形状——24 小时 174 次调用、命中率 0.2%，
        // 因为第一块就是那段只此一份的正文。
        let user_payload = serde_json::to_string(&CritiqueRequest {
            spec,
            output: &obs.output,
        })?;
        let user_msg = Message::user(user_payload);
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
        // 与 critique 同一条规矩：被对照的标准在前，这一轮的发现（`issues`
        // 等，每一轮都由上一轮重新生成）在后。第二轮拿的还是同一段标准，
        // 于是它的请求从第一个字节起就与第一轮相同，直到发现那一块为止。
        let user_payload = serde_json::to_string(&CompareRequest {
            spec,
            best_practices,
            issues: &critique.issues,
            missing: &critique.missing,
            strengths: &critique.strengths,
        })?;
        let user_msg = Message::user(user_payload);
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// 记下每次请求里最后一条用户消息，并按结构化的两种答案作答。
    struct RecordingLlm {
        prompts: Mutex<Vec<String>>,
    }

    impl RecordingLlm {
        fn new() -> Self {
            Self {
                prompts: Mutex::new(Vec::new()),
            }
        }

        fn prompts(&self) -> Vec<String> {
            self.prompts.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LlmClient for RecordingLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("self-review asks non-streaming")
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("self-review asks non-streaming")
        }

        async fn chat(
            &self,
            messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            self.prompts
                .lock()
                .unwrap()
                .push(messages.last().map(|m| m.content()).unwrap_or_default());
            // 一份同时满足 CritiqueOutput 与 ComparisonOutput 的正文：两步问的
            // 是同一段话，这条测试量的是请求里的先后，不是循环走几轮。
            let body =
                r#"{"issues":[],"missing":[],"strengths":[],"gaps":[],"aligned":[],"score":1.0}"#;
            Ok(ChatResponse {
                content: vec![cog_core::ContentBlock::text(body)],
                ..Default::default()
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// 评审的请求以被对照的标准开头，不以这一轮要审的正文开头。
    ///
    /// 第二轮审的是改过的正文，标准却是同一份；正文放在最前面时第一个字节
    /// 就与上一轮不同，上游前缀缓存连一个字都留不下（实测这一族 24 小时
    /// 174 次调用只命中 0.2%）。把标准放在首位，第二轮从开头到正文之前
    /// 逐字节相同，这一段才有得可留。
    #[tokio::test]
    async fn the_standard_a_review_is_held_to_leads_the_payload() {
        const SPEC: &str =
            "SPEC-8f2c: every plan names an acceptance criterion that can be judged true or false";
        const OUTPUT: &str = "OUTPUT-1a77: the plan under review";
        let llm = RecordingLlm::new();

        SelfReviewLoop::critique(
            &SelfReviewLoop::observe(OUTPUT),
            SPEC,
            "self_review:planner",
            None,
            &llm,
        )
        .await
        .unwrap();
        let critique = Critique {
            issues: vec!["ISSUE-c4d1".into()],
            missing: vec![],
            strengths: vec![],
            raw: String::new(),
        };
        SelfReviewLoop::compare(&critique, SPEC, &[], "self_review:planner", None, &llm)
            .await
            .unwrap();

        let prompts = llm.prompts();
        assert_eq!(prompts.len(), 2);
        for (step, prompt) in ["critique", "compare"].iter().zip(&prompts) {
            let spec_at = prompt.find(SPEC).unwrap_or_else(|| {
                panic!("{step} must carry the standard it judges against: {prompt}")
            });
            let varying_at = if *step == "critique" {
                prompt
                    .find(OUTPUT)
                    .expect("the critique carries the text it reviews")
            } else {
                prompt
                    .find("ISSUE-c4d1")
                    .expect("the comparison carries the findings")
            };
            assert!(
                spec_at < varying_at,
                "{step}: the standard must come before what changes every round, \
                 or the shared head is the part that differs first: {prompt}"
            );
        }
    }
}
