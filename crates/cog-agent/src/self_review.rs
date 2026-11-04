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
    /// How well the output meets the standard it was handed, from 0.0 to 1.0.
    ///
    /// Score what that standard states, not how good the output feels: list
    /// the requirements the specification and the best practices name, and
    /// score the fraction of them the output meets. A gap that no stated
    /// requirement covers is still worth naming, but it does not lower the
    /// number -- a reviewer that can always find something to say otherwise
    /// never passes anything, and its number measures its own mood rather than
    /// the output. `1.0` means every stated requirement is met, `0.0` means
    /// none is. Length is not a requirement: padding meets nothing.
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
                .is_none_or(|(_, best_score, _)| score > *best_score)
            {
                best = Some((current_output.clone(), score, result.clone()));
            }

            // Judge-only: hand back the text that was just judged, with the
            // verdict that names what it failed to satisfy.
            //
            // The rewrite below exists to replace an output the review found
            // wanting, but it is judged by nothing: the loop has to pay another
            // critique and comparison to score it, and on this family the
            // rewrite was the single largest line (64% of a review's spend) for
            // text that the producer's own retry path could produce again with
            // the gap list in hand. So the caller gets the judgement and keeps
            // its own text; what to do about the gaps is the caller's decision,
            // made on the same standard the output was already asked to meet.
            if !self.config.revise {
                return Ok((current_output, result));
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

                    // 最后一轮不再买改写。
                    //
                    // 改写是循环里的最后一个动作，之后没有 critique、没有
                    // compare：改写出来的正文不会被任何人打分。按这一族的规矩
                    // ——交出去的只能是打过分的正文——它也不会被交出去。买它
                    // 就是把整篇正文重新生成一遍然后丢掉；生产上这一族没有一次
                    // 达到阈值，所以每一轮评审都在最后一格上付这笔钱。
                    if iteration == self.config.max_iterations - 1 {
                        let (text, _, verdict) = best.expect("刚写进去过");
                        return Ok((text, verdict));
                    }

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
                }
            }
        }

        // A loop that ran zero iterations judged nothing: `best` is empty, and
        // there is no text that carries a score. The fallback that used to sit
        // here returned the untouched output labelled `Pass` with a score of
        // `1.0` -- a verdict and a number nobody produced, published as a
        // review of text no reviewer ever read. A caller that configured zero
        // iterations gets an error it can see, and keeps its own output.
        Err(SFError::Config(
            "self-review ran zero iterations (max_iterations is 0), so no text was judged".into(),
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

        // 分数只在 0.0–1.0 上可比。走出这个区间的数不是「差一点」，是另一把
        // 尺子（8/10、85 分），夹紧之后会变成 1.0——一次越界的读数被读成满分
        // 通过。宁可让这次评审报错，由调用方保留自己的正文，也不要把它当成判据。
        if !(0.0..=1.0).contains(&output.score) {
            return Err(SFError::Validation(format!(
                "comparison scored {}, outside the 0.0-1.0 scale its schema states; \
                 a number on another scale cannot be compared with the threshold",
                output.score
            )));
        }

        Ok(Comparison {
            gaps: output.gaps,
            aligned: output.aligned,
            score: output.score,
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

    /// 记下每次请求里最后一条用户消息与第一条系统消息，并按结构化的两种答案作答。
    struct RecordingLlm {
        prompts: Mutex<Vec<String>>,
        systems: Mutex<Vec<String>>,
    }

    impl RecordingLlm {
        fn new() -> Self {
            Self {
                prompts: Mutex::new(Vec::new()),
                systems: Mutex::new(Vec::new()),
            }
        }

        fn prompts(&self) -> Vec<String> {
            self.prompts.lock().unwrap().clone()
        }

        fn systems(&self) -> Vec<String> {
            self.systems.lock().unwrap().clone()
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
            self.systems
                .lock()
                .unwrap()
                .push(messages.first().map(|m| m.content()).unwrap_or_default());
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

    /// 给分的那一步必须带着一把尺子。
    ///
    /// 请求里只有一份 JSON Schema 与一行「按这个 schema 回答」。schema 里
    /// `score` 若只写一个 `number`，0.8 这个阈值对模型就是一件没听过的事：
    /// 它按自己的心情给一个数，循环拿这个数与阈值比。实测生产 24 小时 226 次
    /// 比对、均分 0.449、达到 0.8 的只有 3 次——分数不是被标准决定的。
    /// 这条测试钉住的是 schema 里那段描述：它把分数定义成「标准列出的要求里
    /// 满足了几成」，并说明篇幅不算要求。
    #[tokio::test]
    async fn the_comparison_is_asked_for_coverage_not_for_an_impression() {
        let llm = RecordingLlm::new();
        let critique = Critique {
            issues: vec![],
            missing: vec![],
            strengths: vec![],
            raw: String::new(),
        };
        SelfReviewLoop::compare(
            &critique,
            "SPEC-8f2c: every plan names an acceptance criterion",
            &[],
            "self_review:planner",
            None,
            &llm,
        )
        .await
        .unwrap();

        let system = llm.systems().pop().expect("compare sends a system message");
        assert!(
            system.contains("fraction of them the output meets"),
            "the score needs a stated ruler, or the threshold compares against a number \
             the model invented: {system}"
        );
        assert!(
            system.contains("Length is not a requirement"),
            "the schema must say that padding meets nothing: {system}"
        );
    }

    /// 越界的分数不是「差一点」，是另一把尺子。
    ///
    /// 模型若按百分制回 85，夹紧之后就是 1.0——一次越界的读数被读成满分通过。
    /// 这一步宁可报错：调用方保留自己的正文，判据面看见的是一次失败的评审，
    /// 而不是一个假的满分。
    #[tokio::test]
    async fn a_score_from_another_scale_is_not_clamped_into_a_pass() {
        let llm = ScoringLlm::new(&[85.0]);
        SelfReviewLoop::critique(
            &SelfReviewLoop::observe("OUTPUT-2b90"),
            "SPEC-4a1d",
            "self_review:planner",
            None,
            &llm,
        )
        .await
        .unwrap();
        let critique = Critique {
            issues: vec![],
            missing: vec![],
            strengths: vec![],
            raw: String::new(),
        };

        let err = SelfReviewLoop::compare(
            &critique,
            "SPEC-4a1d",
            &[],
            "self_review:planner",
            None,
            &llm,
        )
        .await
        .expect_err("85 is not a score on the 0.0-1.0 scale");

        assert!(matches!(err, SFError::Validation(_)), "{err}");
    }

    /// 一轮也没跑的循环没有判词可交。
    ///
    /// 旧实现在循环之外写死了一条 `Pass { score: 1.0 }` 的兜底：`max_iterations`
    /// 配成 0 时它把没被任何人读过的正文标成「通过、满分」交出去。那不是默认
    /// 值，是一条伪造的判定；配置错了就该在调用处看见。
    #[tokio::test]
    async fn a_loop_with_no_iterations_returns_no_verdict() {
        let llm = RecordingLlm::new();
        let config = SelfReviewConfig {
            max_iterations: 0,
            ..SelfReviewConfig::default()
        };

        let err = SelfReviewLoop::new(config)
            .review("ORIGINAL-7f31", &llm)
            .await
            .expect_err("zero iterations cannot produce a verdict");

        assert!(
            matches!(err, SFError::Config(_)),
            "a loop that never ran is a configuration error, not a pass: {err}"
        );
        assert!(
            llm.prompts().is_empty(),
            "and it must not spend a call to say so"
        );
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

        fn json_calls(&self) -> usize {
            self.json_calls.load(std::sync::atomic::Ordering::SeqCst)
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

    fn loop_with_three_rounds() -> SelfReviewLoop {
        SelfReviewLoop::new(SelfReviewConfig {
            max_iterations: 3,
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

    /// 分数往上走的那一轮照常改写。
    ///
    /// 这条是上一条的反面：判据不能把「还在变好」的评审也一并停掉——那样省下的
    /// 就是该花的钱。三轮里两轮的分都在涨，两轮都改写；第三轮的分拿到之后没有
    /// 下一轮可以审改写了，所以交回的是第二轮改写出来的、已经拿过分的
    /// `REV-2`。
    #[tokio::test]
    async fn a_revision_that_scored_better_is_revised_again() {
        let llm = ScoringLlm::new(&[0.6, 0.7, 0.78]);

        let (text, verdict) = loop_with_three_rounds()
            .review("ORIGINAL", &llm)
            .await
            .unwrap();

        assert_eq!(llm.revise_calls(), 2, "前两轮都在变好，那两轮都改写");
        assert_eq!(text, "REV-2", "交出去的是最后一版被审过的正文");
        match verdict {
            SelfReviewResult::NeedRevision { score, .. } => {
                assert!(
                    (score - 0.78).abs() < 1e-6,
                    "判词跟着那一节的分数走：{score}"
                )
            }
            other => panic!("这是一次没过的评审：{other:?}"),
        }
    }

    /// 最后一轮不买改写：改写出来没有人打分，也就交不出去。
    ///
    /// 两轮的循环里，第二轮的分数是最后一次评审读数；这一轮再改写，产出的正文
    /// 既没有 critique 也没有 compare 去评它，所以只会被丢掉。生产上这一族
    /// 没有一次达到阈值，每一轮评审都停在最后一格上——这条测试钉住那一格不再
    /// 付费，而交出去的仍是第一轮改写出来、第二轮打过分的 `REV-1`。
    ///
    /// 只判不写：一次评审只买 critique 与 compare 两次调用。
    ///
    /// 改写是这条流水线上最贵的一步，而它只是把同一个标准再问一遍；配置成
    /// 只判时，交回去的必须是**被审过的那一份原文**——不是改写，也不是空白——
    /// 判词里带着分数与没满足的条目，由调用方自己的重试路径去处理。这条测试
    /// 钉住代价与产出两头：两次调用、零次改写、原文原样交回。
    #[tokio::test]
    async fn judge_only_buys_the_verdict_and_no_rewrite() {
        let llm = ScoringLlm::new(&[0.6]);
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            max_iterations: 2,
            quality_threshold: 0.8,
            revise: false,
            ..Default::default()
        });

        let (text, verdict) = judge_only.review("ORIGINAL", &llm).await.unwrap();

        assert_eq!(
            llm.json_calls(),
            2,
            "只判只买一次 critique 加一次 compare，不是循环跑满两轮"
        );
        assert_eq!(llm.revise_calls(), 0, "只判不买改写");
        assert_eq!(text, "ORIGINAL", "交回去的是被审过的那一份原文");
        match verdict {
            SelfReviewResult::NeedRevision { score, .. } => {
                assert!(
                    (score - 0.6).abs() < 1e-6,
                    "判词带着这一份真实拿到的分：{score}"
                )
            }
            other => panic!("0.6 低于阈值，这是一次没过的评审：{other:?}"),
        }
    }

    /// 只判不写时，过线的输出同样原样交回，不多买一次改写。
    #[tokio::test]
    async fn judge_only_passes_without_a_rewrite() {
        let llm = ScoringLlm::new(&[0.9]);
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            max_iterations: 2,
            quality_threshold: 0.8,
            revise: false,
            ..Default::default()
        });

        let (text, verdict) = judge_only.review("ORIGINAL", &llm).await.unwrap();

        assert_eq!(llm.json_calls(), 2);
        assert_eq!(llm.revise_calls(), 0);
        assert_eq!(text, "ORIGINAL");
        assert!(matches!(verdict, SelfReviewResult::Pass { .. }));
    }

    #[tokio::test]
    async fn the_last_round_buys_no_revision_it_cannot_score() {
        let llm = ScoringLlm::new(&[0.6, 0.7]);

        let (text, verdict) = loop_with_two_rounds()
            .review("ORIGINAL", &llm)
            .await
            .unwrap();

        assert_eq!(
            llm.revise_calls(),
            1,
            "最后一轮的改写不会被评分，也不该被买"
        );
        assert_eq!(text, "REV-1", "交出去的是打过分的最后一版");
        match verdict {
            SelfReviewResult::NeedRevision { score, .. } => {
                assert!(
                    (score - 0.7).abs() < 1e-6,
                    "判词跟着那一节的分数走：{score}"
                )
            }
            other => panic!("这是一次没过的评审：{other:?}"),
        }
    }
}
