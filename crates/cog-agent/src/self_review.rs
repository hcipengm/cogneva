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
        // 打过分的正文里最好的那一份，连同它拿到的分与判词。
        //
        // 循环原来的写法是把最后一轮改写出来的正文直接交出去，而那一份**没有
        // 任何人打过分数**：改写是循环的最后一个动作，判词属于它前面那一版。
        // 于是一次降了分的改写会被原样交出去，而它上一版是这轮评审里最好的
        // 一份——代价付了，质量反而退了。这里把每一版被审过的正文连同分数记下来，
        // 收尾时交最该交的那一份。
        let mut best: Option<(String, f32, SelfReviewResult)> = None;
        // 上一轮打过分的正文拿了多少分，用来判断改写还值不值得再付一次。
        let mut previous_score: Option<f32> = None;

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

            let score = comparison.score;
            if best
                .as_ref()
                .map_or(true, |(_, best_score, _)| score > *best_score)
            {
                best = Some((current_output.clone(), score, result.clone()));
            }

            match &result {
                SelfReviewResult::Pass { .. } => {
                    return Ok((current_output, result));
                }
                SelfReviewResult::NeedRevision { .. } => {
                    // 分数没有往上走，就不再一次改写它。
                    //
                    // 改写一次要重写整篇正文（这一族上实测每次约三千个输出
                    // token），而它存在的理由是「上一版还不够好」；如果这一轮
                    // 拿到的分不比上一轮高，说明这条改写路径已经不再朝评审要的
                    // 方向走，再付一次只是把同一个问题重问一遍，问出来的东西
                    // 也没有人打分。交回已经打过分里最好的那一份，判词也用它
                    // 自己那一轮的。
                    if let Some(previous_score) = previous_score {
                        if score <= previous_score {
                            let (text, _, verdict) = best.expect("刚写进去过");
                            return Ok((text, verdict));
                        }
                    }
                    previous_score = Some(score);

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
                        let (text, _, verdict) = best.expect("刚写进去过");
                        return Ok((text, verdict));
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

    /// 按顺序发分数、按顺序改写：每轮一次 critique、一次 compare、一次 revise。
    ///
    /// `compare` 的分从 `scores` 里按次序取；`critique` 的正文里那个分不被读。
    /// 改写走的是 Text，结构化两步走的是 Json，所以两者在同一个 `chat` 上分得开。
    struct ScoringLlm {
        scores: Mutex<std::collections::VecDeque<f32>>,
        json_calls: std::sync::atomic::AtomicUsize,
        revise_calls: std::sync::atomic::AtomicUsize,
    }

    impl ScoringLlm {
        fn new(scores: &[f32]) -> Self {
            Self {
                scores: Mutex::new(scores.iter().copied().collect()),
                json_calls: std::sync::atomic::AtomicUsize::new(0),
                revise_calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn revise_calls(&self) -> usize {
            self.revise_calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl LlmClient for ScoringLlm {
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
            _messages: &[Message],
            options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            if options.response_format == ResponseFormat::Text {
                let n = self
                    .revise_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                return Ok(ChatResponse {
                    content: vec![cog_core::ContentBlock::text(format!("REV-{n}"))],
                    ..Default::default()
                });
            }
            let n = self
                .json_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // 偶数次是 critique，奇数次是 compare。
            let score = if n % 2 == 1 {
                self.scores
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("每轮 compare 都要有分可发")
            } else {
                0.0
            };
            let body = format!(
                r#"{{"issues":[],"missing":[],"strengths":[],"gaps":[],"aligned":[],"score":{score}}}"#
            );
            Ok(ChatResponse {
                content: vec![cog_core::ContentBlock::text(body)],
                ..Default::default()
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn loop_with_two_rounds() -> SelfReviewLoop {
        SelfReviewLoop::new(SelfReviewConfig {
            max_iterations: 2,
            quality_threshold: 0.8,
            ..Default::default()
        })
    }

    /// 分数没有往上走的那一轮不再改写，交回已经打过分里最好的那一份。
    ///
    /// 改写一次要重写整篇正文，而它存在的理由是「上一版还不够好」。第一轮
    /// 0.6、第二轮 0.5 时，这条路径已经不再朝评审要的方向走：再付一次改写只是
    /// 把同一个问题重问一遍，问出来的正文甚至不会被评分——原来的写法却正是把它
    /// 交了出去，比循环开始时那一份还差。这条测试钉住两件事：只改写了一次，
    /// 交出去的是拿过最高分的那一版。
    #[tokio::test]
    async fn a_revision_that_scored_worse_does_not_buy_another_rewrite() {
        let llm = ScoringLlm::new(&[0.6, 0.5]);

        let (text, verdict) = loop_with_two_rounds()
            .review("ORIGINAL", &llm)
            .await
            .unwrap();

        assert_eq!(llm.revise_calls(), 1, "第二轮分数没涨，就不该再买一次改写");
        assert_eq!(
            text, "ORIGINAL",
            "交出去的是打过分里最好的那一份，不是没人评过分的新改写"
        );
        match verdict {
            SelfReviewResult::NeedRevision { score, .. } => {
                assert!(
                    (score - 0.6).abs() < 1e-6,
                    "判词跟着那一节的分数走：{score}"
                )
            }
            other => panic!("这是一次没过的评审：{other:?}"),
        }
    }

    /// 分数往上走的那一轮照常改写，最后一版交出去。
    ///
    /// 这条是上一条的反面：判据不能把「还在变好」的评审也一并停掉——那样省下的
    /// 就是该花的钱。
    #[tokio::test]
    async fn a_revision_that_scored_better_is_revised_again() {
        let llm = ScoringLlm::new(&[0.6, 0.7]);

        let (text, _) = loop_with_two_rounds()
            .review("ORIGINAL", &llm)
            .await
            .unwrap();

        assert_eq!(llm.revise_calls(), 2, "两轮都在变好，两轮都改写");
        assert_eq!(text, "REV-2", "最后一版是更好的那一版");
    }
}
