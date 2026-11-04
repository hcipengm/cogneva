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

/// 一条要求在这份正文里的三种状态。闭集，没有第四种。
///
/// `undetermined` 不是「没判」，是「从这段正文里判不出来」——「测试通过」这类
/// 要求要的是跑一遍，不是读一遍。让它说判不出来，比让它猜一个答案强：猜出来的
/// 那个分与猜出来的那条理由，下游都当证据用。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
enum RowStatus {
    Met,
    Unmet,
    Undetermined,
}

/// 一条要求、它的状态、以及正文里说到它的那几句话。
///
/// `evidence` 不是给人看的摘要：下面 [`SelfReviewLoop::judge_table`] 会拿它去
/// 正文里逐字找。找不到的 `met` 不算 `met`——这一格是整套一次调用评审的支柱，
/// 把「评审者的判断」换成「可以核对的事实」。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct RequirementRow {
    /// 这条要求，一行，写成一句对正文真假可判的话。
    requirement: String,
    status: RowStatus,
    /// 正文里说到这条要求的原话，逐字，不要转述。
    #[serde(default)]
    evidence: String,
}

/// 一次评审的全部答案：一张表，一个要求一行。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct RequirementTable {
    #[serde(default)]
    rows: Vec<RequirementRow>,
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

/// 判定请求：判定规则、标准、正文，按这个顺序。
///
/// 规则不是一条单独的系统消息，因为有的上游只读第一条系统消息
/// （`crates/cog-llm` 的 anthropic 适配取 `find_map` 的第一条），而
/// `execute_structured` 已经把自己的 schema 放在那一条里——第二条系统消息会
/// 被静默丢掉，判据就没了。放进正文最前面既不丢，也仍是逐字节稳定的前缀：
/// 规则每次相同，标准次之，正文最后。
#[derive(serde::Serialize)]
struct JudgeRequest<'a> {
    rubric: &'a str,
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
        // Judge-only buys one call and hands the text back untouched. It does
        // not enter the loop above because the loop's every other step exists
        // to produce and re-judge a rewrite: with no rewrite there is no second
        // round to run, and a second critique of the same text against the same
        // standard is the same question paid for twice.
        if !self.config.revise {
            let verdict = self.judge_table(output, llm).await?;
            return Ok((output.to_string(), verdict));
        }

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

    /// Judge the text against the requirements it was asked to meet, in one
    /// call, and compute the score here rather than asking for it.
    ///
    /// The two-call shape this replaces asked a model for a number between 0
    /// and 1 and compared that number with a threshold. Measured over a day of
    /// production that number was a mood: 226 reviews, mean 0.449, three of them
    /// at or above 0.8. What replaces it is a table -- one row per stated
    /// requirement, each row `met`, `unmet`, or `undetermined` -- and a fraction
    /// computed from the table. A model can be wrong about a row; it cannot be
    /// vague about a fraction, and the row it is wrong about is a sentence a
    /// person can read and disagree with.
    ///
    /// The evidence column is what stops a `met` from being an opinion. Every
    /// `met` row quotes the text, and [`SelfReviewLoop::quotes_the_text`] looks
    /// for that quote in the text; a quote that is not there does not count as
    /// met, whatever the status says. That is the whole of the independence the
    /// second call used to buy, and it is paid for in bytes compared instead of
    /// in tokens generated.
    async fn judge_table(&self, output: &str, llm: &dyn LlmClient) -> SFResult<SelfReviewResult> {
        let spec = self.config.spec.as_deref().unwrap_or("");
        let user_payload = serde_json::to_string(&JudgeRequest {
            rubric: SelfReviewLoop::judging_rubric(&self.actor),
            spec,
            output,
        })?;
        let messages = vec![Message::user(user_payload)];
        let options = stamp(ChatOptions::default(), &self.actor, self.task.as_deref());

        let table = execute_structured::<RequirementTable>(llm, &messages, &options).await?;

        // 一条要求都没列出来的「评审」不是一次通过的评审，是一次没做的评审。
        // 交回 `Pass` 会把「没人看过」写成「全过」——这正是这张表存在的理由。
        if table.rows.is_empty() {
            return Err(SFError::Validation(
                "the review stated no requirement, so nothing was judged".into(),
            ));
        }

        let mut met = 0usize;
        let mut unmet: Vec<&RequirementRow> = Vec::new();
        let mut undetermined: Vec<&RequirementRow> = Vec::new();
        let mut unsupported: Vec<&RequirementRow> = Vec::new();

        for row in &table.rows {
            match row.status {
                RowStatus::Met => {
                    // A `met` whose quote is not in the text is not a `met`. It
                    // is also not a `unmet` -- the row was never established
                    // either way, and calling it a failure would turn a
                    // hallucinated quote into a defect of the text under review.
                    if SelfReviewLoop::quotes_the_text(&row.evidence, output) {
                        met += 1;
                    } else {
                        unsupported.push(row);
                    }
                }
                RowStatus::Unmet => unmet.push(row),
                RowStatus::Undetermined => undetermined.push(row),
            }
        }

        // 分母是「被判过的条目」：`met`、`unmet`，以及自称 `met` 却没有引文支撑的
        // 那几条。最后一种不能留在分母外——它既不是通过也不是无法判断，是一次没
        // 兑现的声称，把它排除会让一条查无此句的 `met` 白拿一分。
        let judged = met + unmet.len() + unsupported.len();

        // 一条都没判出来时，分数没有分母。这不是 0 分也不是满分，是一次没有结论
        // 的评审：调用方保留自己的正文，读数记在这次失败上。
        if judged == 0 {
            return Err(SFError::Validation(format!(
                "no requirement could be judged from the text: {} undetermined",
                undetermined.len()
            )));
        }

        let score = met as f32 / judged as f32;

        if unmet.is_empty() && unsupported.is_empty() {
            let summary = if undetermined.is_empty() {
                format!("all {} stated requirements are met", met)
            } else {
                format!(
                    "all {} stated requirements that this text can settle are met; \
                     {} cannot be settled from the text",
                    met,
                    undetermined.len()
                )
            };
            return Ok(SelfReviewResult::Pass { score, summary });
        }

        let mut findings: Vec<String> = unmet
            .iter()
            .map(|row| format!("{}: no part of the text carries it", row.requirement))
            .collect();
        findings.extend(unsupported.iter().map(|row| {
            format!(
                "{}: quoted as met, but the quote is not in the text",
                row.requirement
            )
        }));

        Ok(SelfReviewResult::NeedRevision {
            critique: findings.join("; "),
            suggestions: unmet
                .iter()
                .map(|row| row.requirement.clone())
                .chain(unsupported.iter().map(|row| row.requirement.clone()))
                .collect(),
            score,
        })
    }

    /// Whether `evidence` is a quote from `text`, whitespace aside.
    ///
    /// Whitespace is dropped on both sides because a quote copied across a line
    /// break is the same quote, and a check that fails on reflowing would be a
    /// check that punishes formatting. Nothing else is forgiven: case,
    /// punctuation and every word have to be the text's own, or the quote is
    /// the reviewer's paraphrase and the `met` it supports is the reviewer's
    /// opinion.
    fn quotes_the_text(evidence: &str, text: &str) -> bool {
        let tight = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
        let quote = tight(evidence);
        // 空引文在每一段正文里都「找得到」，那不是证据。
        !quote.is_empty() && tight(text).contains(&quote)
    }

    /// What the text under judgement is, and therefore what a `met` means for
    /// it.
    ///
    /// The first version of this review handed every actor the same standard:
    /// the task's own acceptance criteria. For the planner that is the wrong
    /// standard outright -- those criteria describe the deliverable, and a plan
    /// is not the deliverable. Measured on ten consecutive production verdicts,
    /// half of them faulted the plan for "never stating a verdict" or "not
    /// executing", which is what a plan is not supposed to do. A review that
    /// asks a plan to answer the task cannot be passed by any plan, and a gate
    /// that never opens teaches its readers to ignore it.
    ///
    /// The text is chosen by the role the review is charged to. It has to stay
    /// byte-identical across calls, because it is the head of the request and
    /// the only part an upstream prefix cache can hold across tasks.
    fn judging_rubric(actor: &str) -> &'static str {
        let role = actor.rsplit(':').next().unwrap_or(actor);
        match role {
            "planner" => {
                "The text under judgement is a PLAN: a decomposition of a task into sub-tasks. \
                 A plan is not expected to answer the task or to carry out any step -- that is \
                 what its sub-tasks are for -- so \"it does not give the answer\", \"it does not \
                 execute anything\" and \"it defers the work to its sub-tasks\" are not defects \
                 in a plan and must not be scored as such.\n\n\
                 List every requirement the specification states. For each one decide:\n\
                 - met: some sub-task's acceptance criteria carry this requirement, and the words \
                 that carry it are in the text. Quote them.\n\
                 - unmet: no sub-task carries it, or the sub-tasks contradict it or each other.\n\
                 - undetermined: a plan cannot show it either way.\n\n\
                 Quote the text, do not paraphrase it: every `met` is checked character for \
                 character against the text, whitespace aside, and a quote that is not there is \
                 not a `met`."
            }
            "evaluator" => {
                "The text under judgement is a JUDGEMENT about someone else's work. Judge it as \
                 a judgement: whether it names what it assessed, states a verdict, and says what \
                 the verdict rests on. Whether the work itself is good is not this text's \
                 question.\n\n\
                 List every requirement the specification states. For each one decide met, unmet \
                 or undetermined, and for `met` quote the words of the text that carry it, \
                 character for character, whitespace aside."
            }
            "moderator" | "merger" => {
                "The text under judgement is a FINAL ANSWER to the task. List every requirement \
                 the specification states. For each one decide met, unmet or undetermined, and \
                 for `met` quote the words of the text that carry it, character for character, \
                 whitespace aside."
            }
            _ => {
                "The text under judgement is a WORK PRODUCT the task asked for. List every \
                 requirement the specification states. For each one decide met, unmet or \
                 undetermined, and for `met` quote the words of the text that carry it, \
                 character for character, whitespace aside."
            }
        }
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

    /// 按一张固定的要求表作答，并数一次评审买了几次调用。
    struct TableLlm {
        body: &'static str,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl TableLlm {
        fn new(body: &'static str) -> Self {
            Self {
                body,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl LlmClient for TableLlm {
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
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ChatResponse {
                content: vec![cog_core::ContentBlock::text(self.body)],
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
    #[tokio::test]
    async fn judge_only_buys_the_verdict_and_no_rewrite() {
        let llm = TableLlm::new(
            r#"{"rows":[
                 {"requirement":"names a sub-task","status":"met","evidence":"t1: read the file"},
                 {"requirement":"names a rollback","status":"unmet"}]}"#,
        );
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            max_iterations: 2,
            quality_threshold: 0.8,
            revise: false,
            ..Default::default()
        })
        .with_actor("self_review:planner");

        let (text, verdict) = judge_only
            .review("t1: read the file, then t2: deploy", &llm)
            .await
            .unwrap();

        assert_eq!(llm.calls(), 1, "只判不写是一次调用：一张表，一个要求一行");
        assert_eq!(
            text, "t1: read the file, then t2: deploy",
            "交回的是被审过的那一份原文"
        );
        match verdict {
            SelfReviewResult::NeedRevision {
                score,
                suggestions,
                critique,
            } => {
                assert!((score - 0.5).abs() < 1e-6, "两条要求满足一条：{score}");
                assert_eq!(suggestions, vec!["names a rollback".to_string()]);
                assert!(
                    critique.contains("names a rollback"),
                    "判词点名没满足的那一条：{critique}"
                );
            }
            other => panic!("有一条要求没人接住，这是一次没过的评审：{other:?}"),
        }
    }

    /// 每条要求都被引文支撑时才是通过，而且仍然只买一次调用。
    #[tokio::test]
    async fn judge_only_passes_without_a_rewrite() {
        let llm = TableLlm::new(
            r#"{"rows":[
                 {"requirement":"names a sub-task","status":"met","evidence":"t1: read the file"},
                 {"requirement":"names a second step","status":"met","evidence":"then t2: deploy"}]}"#,
        );
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            max_iterations: 2,
            quality_threshold: 0.8,
            revise: false,
            ..Default::default()
        });

        let (text, verdict) = judge_only
            .review("t1: read the file, then t2: deploy", &llm)
            .await
            .unwrap();

        assert_eq!(llm.calls(), 1);
        assert_eq!(text, "t1: read the file, then t2: deploy");
        match verdict {
            SelfReviewResult::Pass { score, .. } => assert!((score - 1.0).abs() < 1e-6),
            other => panic!("每条要求都有引文支撑，这是通过：{other:?}"),
        }
    }

    /// 自称 `met` 的那一条，引文在正文里找不到，就不是 `met`。
    ///
    /// 这是整套一次调用评审的支柱：把「评审者的判断」换成「可以核对的事实」。
    /// 引文查无此句时不能留在分母外——留在外面等于让一条编出来的引文白拿一分。
    #[tokio::test]
    async fn a_met_whose_quote_is_not_in_the_text_does_not_count() {
        let llm = TableLlm::new(
            r#"{"rows":[
                 {"requirement":"names a sub-task","status":"met","evidence":"t1: read the file"},
                 {"requirement":"names a rollback","status":"met","evidence":"t9: roll back"}]}"#,
        );
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            revise: false,
            ..Default::default()
        });

        let (_, verdict) = judge_only
            .review("t1: read the file, then t2: deploy", &llm)
            .await
            .unwrap();

        match verdict {
            SelfReviewResult::NeedRevision {
                score, critique, ..
            } => {
                assert!(
                    (score - 0.5).abs() < 1e-6,
                    "查无此句的那一条仍在分母里：{score}"
                );
                assert!(
                    critique.contains("names a rollback"),
                    "判词要说清它为什么不算：{critique}"
                );
            }
            other => panic!("一条引文在正文里找不到，这次评审不算通过：{other:?}"),
        }
    }

    /// 引文跨行折断仍是同一句：空白不算差别。
    #[tokio::test]
    async fn a_quote_across_a_line_break_is_the_same_quote() {
        let llm = TableLlm::new(
            r#"{"rows":[
                 {"requirement":"names a rollback","status":"met","evidence":"t3: roll\n back to the previous revision"}]}"#,
        );
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            revise: false,
            ..Default::default()
        });

        let (_, verdict) = judge_only
            .review("t1: plan\nt3: roll back to the previous revision", &llm)
            .await
            .unwrap();

        assert!(
            matches!(verdict, SelfReviewResult::Pass { .. }),
            "折行不是转述：{verdict:?}"
        );
    }

    /// 分数是算出来的：回答里自带一个数字也不算数。
    ///
    /// 旧的两步评审把 `score` 交给模型填，实测那是评审者的心情（226 次、均分
    /// 0.449、过 3 次）。这里把 `score` 放进回答正文，表里的行仍按真实状态判，
    /// 算出来的分不该被它带走。
    #[tokio::test]
    async fn a_score_in_the_answer_is_not_the_score() {
        let llm = TableLlm::new(
            r#"{"score":1.0,"rows":[
                 {"requirement":"names a sub-task","status":"met","evidence":"t1: read the file"},
                 {"requirement":"names a rollback","status":"unmet"}]}"#,
        );
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            revise: false,
            ..Default::default()
        });

        let (_, verdict) = judge_only.review("t1: read the file", &llm).await.unwrap();

        match verdict {
            SelfReviewResult::NeedRevision { score, .. } => assert!(
                (score - 0.5).abs() < 1e-6,
                "答案是表里的行算出来的，不是模型写的那个数：{score}"
            ),
            other => panic!("有一行 unmet，这次评审不通过：{other:?}"),
        }
    }

    /// 一条要求都没列出来的「评审」不是通过，是一次没做的评审。
    #[tokio::test]
    async fn a_table_with_no_requirement_is_not_a_pass() {
        let llm = TableLlm::new(r#"{"rows":[]}"#);
        let judge_only = SelfReviewLoop::new(SelfReviewConfig {
            revise: false,
            ..Default::default()
        });

        let outcome = judge_only.review("anything", &llm).await;
        assert!(
            outcome.is_err(),
            "没有任何要求的判词不能读成通过：{outcome:?}"
        );
    }

    /// 判据跟着被审的东西走：计划不因为「没有回答任务」被扣分。
    ///
    /// 实测十次生产判词里有一半是在说「这是计划，不是答案」——拿交付物的标准
    /// 去判计划，计划永远过不了，这道门也就永远开不了。
    #[test]
    fn a_plan_is_judged_as_a_plan() {
        let plan = SelfReviewLoop::judging_rubric("self_review:planner");
        assert!(
            plan.contains("is not expected to answer the task"),
            "计划这条判据必须把「没有回答任务」划出缺陷之外：{plan}"
        );
        assert!(
            !SelfReviewLoop::judging_rubric("self_review:generator")
                .contains("is not expected to answer the task"),
            "生成物就是答案，不能沿用计划那条豁免"
        );
        assert!(
            SelfReviewLoop::judging_rubric("self_review:evaluator").contains("JUDGEMENT"),
            "判词按判词判"
        );
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
