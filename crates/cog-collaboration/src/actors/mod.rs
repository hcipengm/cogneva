pub mod context_builder;
pub mod evaluator;
pub mod generator;
pub mod merger;
pub mod mode_selector;
pub mod moderator;
pub mod planner;
pub mod prompt_skill;

pub use context_builder::StandardTaskContextBuilder;
pub use evaluator::EvaluatorActor;
pub use generator::{GeneratorActor, PreviousAttempt};
pub use merger::{fallback_best_branch, parse_merge_result, MergeResult, MergerActor};
pub use mode_selector::ModeSelectorActor;
pub use moderator::{parse_moderator_output, ModeratorActor, ModeratorDecision, ModeratorOutput};
pub use planner::PlannerActor;
pub use prompt_skill::{apply_prompt_skill, resolve_prompt_skill, SKILL_SCHEMA_RESOURCE};

/// Assemble the document handed to the agent: the two halves must arrive as two
/// halves.
///
/// `contract` is everything that states what to produce and does not move from
/// one attempt to the next — the role's instructions, its output schema, the
/// change-format contract. `context` is everything about *this* attempt. The
/// runtime renders the contract first (see `cog_core::contract::prompt`); the
/// two must not be flattened into one map, because a map serializes in key
/// order and the attempt-varying fields sort to the front, which would put a
/// byte that changes on every attempt inside the model's first few characters.
pub(crate) fn actor_input(
    task: &cog_core::Task,
    contract: serde_json::Value,
    context: serde_json::Value,
) -> serde_json::Value {
    let mut request = request_half(task);
    drop_repeated_goal(&mut request, &context);
    let context = drop_repeated_input(&request, context);
    with_contract(
        serde_json::json!({
            "task": request,
            "context": context,
        }),
        contract,
    )
}

/// 同一段字在一份请求里出现两次时，只留那份没人能删掉的。
///
/// 上下文构建器把整份 `task.input` 抄进了 `context.input`，而它同时又把这些
/// 字段提到 `context` 顶层：2026-11-06 实测一件自进化任务的生成请求体 152,559
/// 字节，其中 32,551 字节的 `goal` 与 32,045 字节的 `failure_log` 各出现两次
/// ——**43% 的请求体是同一段字的第二份**。这不只是多付 token：这份任务输入的
/// 估算占用 66,368 token，超过了 64,000 的窗口，于是窗口的地板（system 与首条
/// user）自己就放不下，之后每一条工具结果加进来都被立刻裁掉，模型每一轮收到的
/// 是同一份请求、看不到自己上一步的任何结果，直到把预算跑光（同一天实测同一个
/// 请求体被原样发了 1,563 次，一小时零产出）。
///
/// 删的是 `context.input` 里那一份，因为另一份在 `context` 顶层或 `task.input`
/// 里——读的人照旧读得到，模型看到的文字不多不少。只在逐字节相同时删：值不同
/// 就是两个字段，不是一个字段的两份。
fn drop_repeated_input(request: &serde_json::Value, mut context: serde_json::Value) -> serde_json::Value {
    let Some(inner) = context.get("input").and_then(|i| i.as_object()) else {
        return context;
    };
    let mut repeated: Vec<String> = Vec::new();
    for (key, value) in inner {
        let elsewhere_in_context = context
            .get(key)
            .is_some_and(|top| top == value);
        let in_request = request
            .get("input")
            .and_then(|i| i.get(key))
            .is_some_and(|top| top == value);
        if elsewhere_in_context || in_request {
            repeated.push(key.clone());
        }
    }
    if repeated.is_empty() {
        return context;
    }
    if let Some(inner) = context.get_mut("input").and_then(|i| i.as_object_mut()) {
        for key in &repeated {
            inner.remove(key);
        }
        // 一条都不剩就不再留一个空壳：`context.input` 在这里是一个值的副本，
        // 副本被删光了，留个 `{}` 只会让读者以为任务输入本来就是空的。
        if inner.is_empty() {
            if let Some(fields) = context.as_object_mut() {
                fields.remove("input");
            }
        }
    }
    context
}

/// 请求半边与上下文半边都写着同一个 goal 时，只留上下文那一份。
///
/// 上下文构建器把 `task.input.goal` 原样抄进 `context.goal`，于是每个请求里
/// 它出现两次——2026-11-02 实测一件自进化任务的 planner 请求体 60,341 字节，
/// 其中 6,322 字节是这同一段文字的第二次。三条角色的答案契约都写着「读
/// `context.goal`」，没有一条读 `task.input.goal`，所以删的是请求那半的这一份：
/// 契约里每一句都仍然指着存在的字段，模型看到的文本一字不少。
///
/// 只在两份逐字节相同时删。自定义上下文构建器写了别的 goal 时不删——那时两份
/// 不是重复，是两个不同的值。
fn drop_repeated_goal(request: &mut serde_json::Value, context: &serde_json::Value) {
    let Some(repeated) = context
        .get("goal")
        .zip(request.get("input").and_then(|i| i.get("goal")))
        .map(|(in_context, in_request)| in_context == in_request)
    else {
        return;
    };
    if !repeated {
        return;
    }
    if let Some(input) = request.get_mut("input").and_then(|i| i.as_object_mut()) {
        input.remove("goal");
    }
}

/// The part of a task that is the request, as opposed to the record of it.
///
/// The whole `Task` used to be written here. It is not a request: it carries the
/// id, the two timestamps, the status, the retry and lease bookkeeping and the
/// dependency lists, none of which the requester authored or the model reads.
/// What it costs is not only the tokens those fields take — it is the cache.
/// Serialization is in key order, and `created_at` sorts ahead of everything the
/// request is made of, so the first byte after `{"blocked_by":[],"blocks":[],` is
/// a timestamp and nothing behind it is ever shared. Measured on two consecutive
/// rounds of one loop, whose request does not move, the rendered halves shared
/// 73 bytes of 708 and the first difference was that timestamp.
///
/// What a requester authored is `input`, and the branches that read it also want
/// the type. Both are the same for every round of one request, which is what
/// makes them cacheable at all.
fn request_half(task: &cog_core::Task) -> serde_json::Value {
    serde_json::json!({
        "input": &task.input,
        "task_type": &task.task_type,
    })
}

/// Attach the stable half to a document that is not one of task and context.
///
/// Which half a field belongs to is a property of the request, not of how the
/// varying fields happen to be arranged. An actor whose document is a single
/// flat map still has a half that never moves between attempts — its
/// instructions, its answer contract — and a half that does: the goal, the
/// debate so far, this round's branches. Written under
/// [`cog_core::contract::prompt::PROMPT_CONTRACT_KEY`], the runtime renders the
/// stable half first and drops it from the user message, so the varying fields
/// cannot sit in front of it. Left in the map they are serialized in key order,
/// where the field that changes sorts wherever its name happens to fall —
/// `goal` sorts before `instruction`, and the constant sentence never enters the
/// cacheable prefix at all.
pub(crate) fn with_contract(
    mut document: serde_json::Value,
    contract: serde_json::Value,
) -> serde_json::Value {
    if let Some(fields) = document.as_object_mut() {
        fields.insert(
            cog_core::contract::prompt::PROMPT_CONTRACT_KEY.to_string(),
            contract,
        );
    }
    document
}

/// The specification a review of this task's output is held to.
///
/// The standard has to come from outside the review — a review compared against
/// nothing returns a score the review itself wrote, which no input can
/// contradict. What the actor was asked to satisfy is in hand at every call
/// site, so it is read from the task rather than left empty. `declared` is the
/// criteria the caller handed the actor, when it handed any: for an evaluation
/// those criteria are the standard the judgement must apply.
pub(crate) fn review_spec(task: &cog_core::Task, declared: &[&str]) -> String {
    let goal = task
        .input
        .get("goal")
        .and_then(|g| g.as_str())
        .unwrap_or("")
        .trim();
    // 任务自己的 `acceptance` 就是它对「怎样算完」的全部声明，必须进规格。
    // 评审表每一行都要引一句规格里的话（`spec_quote`），所以**没写进规格的
    // 要求，评审列不出来**：只交 goal 时，整张表只能从那一句话里拆出来，判词
    // 看着机械不是因为它太死板，是因为它手里确实只有那一句。调用方另外给的
    // criteria 排在后面，是补充而不是替代。
    let mut criteria: Vec<&str> = Vec::new();
    for criterion in task
        .acceptance
        .iter()
        .map(|c| c.trim())
        .chain(declared.iter().map(|c| c.trim()))
    {
        if !criterion.is_empty() && !criteria.contains(&criterion) {
            criteria.push(criterion);
        }
    }
    let criteria = criteria.join("; ");
    match (goal.is_empty(), criteria.is_empty()) {
        (false, false) => format!("{goal}\n\nAcceptance criteria: {criteria}"),
        (false, true) => goal.to_string(),
        (true, false) => format!("Acceptance criteria: {criteria}"),
        (true, true) => String::new(),
    }
}

/// What the caller knows about this output that bears on whether a review of it
/// is worth buying.
///
/// The first stage of a self-review is not the model: it is this program. A
/// stage whose upstream never answered, a change the evolution loop judges by
/// its own deterministic validation, and a plan the caller is about to judge
/// with a structural gate are all cases where a review would be a second answer
/// to a question that already has one. Those are decided here, once, for every
/// actor — five call sites each spelling the ladder out for themselves is five
/// places for the rules to drift apart, and they already had.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReviewFacts {
    /// The actor never reached its upstream, so the output is a placeholder.
    pub upstream_failed: bool,
    /// The work is a self-evolution change, judged by the validation, not read.
    pub self_evolution: bool,
    /// The caller holds a program verdict on this output already.
    pub program_judged: bool,
}

/// Whether this output is reviewed, and what a review of it would be held to.
///
/// The whole of stage two of a self-review: every actor reaches it with the same
/// three facts and gets the same answer, so "which stages are reviewed" is one
/// table a reader can check rather than a ladder per actor. Only when none of
/// the facts holds does this return [`ReviewBasis::HeldTo`], and only that arm
/// costs a model call.
pub(crate) fn review_basis(
    task: &cog_core::Task,
    facts: ReviewFacts,
    declared: &[&str],
) -> ReviewBasis {
    if facts.upstream_failed {
        ReviewBasis::Passed(crate::observable::SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE)
    } else if facts.self_evolution {
        ReviewBasis::Passed(crate::observable::SELF_REVIEW_DECIDED_BY_SELF_EVOLUTION)
    } else if facts.program_judged {
        ReviewBasis::Passed(crate::observable::SELF_REVIEW_DECIDED_BY_PROGRAM_JUDGED)
    } else {
        ReviewBasis::HeldTo(review_spec(task, declared))
    }
}

/// Record a verdict settled without a call, for call sites that stop before the
/// funnel.
///
/// An actor that returns early — a self-evolution evaluation whose verdict is
/// already deterministic, for instance — never reaches [`maybe_self_review`],
/// and the calls it saved would otherwise be indistinguishable from a stage
/// that stopped being reached at all.
pub(crate) async fn note_self_review_settled_without_a_call(stage: &str, reason: &'static str) {
    crate::observable::global_observable()
        .record_self_review_settled_without_a_call(stage, reason)
        .await;
}

/// What a review of an actor's output would be held to, or who settled it
/// without one.
///
/// Both arms are outcomes of the same gate, not "reviewed" and "not reviewed":
/// the second is the pass the fixed program reached before any call was made,
/// and the word it carries is published as `decided_by`.
pub(crate) enum ReviewBasis {
    /// The actor's own output, held to what it was asked to satisfy (see
    /// [`review_spec`]).
    HeldTo(String),
    /// No call: this program settled the artifact, for the stated reason from
    /// [`crate::observable`]'s decided-by vocabulary.
    ///
    /// The two common reasons are an actor that never reached its upstream —
    /// what the funnel was handed is a placeholder, and reviewing it would buy
    /// two calls for the same unavailable upstream while a revision of a
    /// placeholder replaces a real cause with a plausible-looking answer — and
    /// a stage whose output is deliberately outside the review's remit. Both
    /// are settled here, so both are counted where the review would have been.
    Passed(&'static str),
}

/// Optional self-review helper shared by all actors.
/// Returns the revised output text when the review loop produced a revision;
/// callers are responsible for re-parsing it into their structured output.
///
/// A configuration with `revise` off never yields a revision: the judgement
/// still runs and is still counted, the text comes back as it went in, and the
/// caller keeps its own output — the gaps travel in the verdict, not in a
/// rewritten text.
///
/// The basis is taken only when the configuration declares no specification of
/// its own. Either way a verdict is counted on the metric plane — the model's
/// when a review ran, this program's pass and the word that settled it when one
/// did not — so a stage reviewing with nothing but its own output to compare
/// against, and a stage the program already settled, are readings rather than
/// assumptions.
pub(crate) async fn maybe_self_review(
    agent: &dyn cog_core::Agent,
    config: &Option<cog_core::SelfReviewConfig>,
    output: &str,
    agent_kind: &str,
    basis: ReviewBasis,
    task_id: &str,
) -> Option<String> {
    let observable = crate::observable::global_observable();
    let Some(config) = config.as_ref() else {
        observable
            .record_self_review_settled_without_a_call(
                agent_kind,
                crate::observable::SELF_REVIEW_DECIDED_BY_DISABLED,
            )
            .await;
        return None;
    };
    let spec = match basis {
        ReviewBasis::HeldTo(spec) => spec,
        ReviewBasis::Passed(reason) => {
            observable
                .record_self_review_settled_without_a_call(agent_kind, reason)
                .await;
            return None;
        }
    };
    let cfg = config.clone().with_declared_spec(&spec);
    match agent
        .review_and_revise_for_task(output, &cfg, task_id)
        .await
    {
        Ok((revised, cog_core::SelfReviewResult::Pass { score, summary })) => {
            observable.record_self_review_verdict(
                agent_kind,
                &cfg,
                crate::observable::SELF_REVIEW_VERDICT_PASS,
            );
            tracing::info!(
                agent_kind = %agent_kind,
                score = %score,
                summary = %summary,
                "Self-review passed"
            );
            (revised != output).then_some(revised)
        }
        Ok((
            revised,
            cog_core::SelfReviewResult::NeedRevision {
                critique,
                suggestions,
                score,
            },
        )) => {
            observable.record_self_review_verdict(
                agent_kind,
                &cfg,
                crate::observable::SELF_REVIEW_VERDICT_NEED_REVISION,
            );
            // How the revision step ended. A revision that handed back the
            // text it was given leaves the loop nothing to ask, so the review
            // ends there rather than buying another critique and comparison
            // over the identical text; this cell is those rounds, plus the
            // reviews that reached their last iteration with nothing rewritten.
            // The other cell is recorded too — a loop that never reached a
            // revision and one whose every revision rewrote the text must not
            // read the same.
            observable.record_self_review_revision(
                agent_kind,
                if !cfg.revise {
                    crate::observable::SELF_REVIEW_REVISION_JUDGED_ONLY
                } else if revised == output {
                    crate::observable::SELF_REVIEW_REVISION_UNCHANGED
                } else {
                    crate::observable::SELF_REVIEW_REVISION_CHANGED
                },
            );
            tracing::warn!(
                agent_kind = %agent_kind,
                score = %score,
                critique = %critique,
                suggestions = ?suggestions,
                "Self-review flagged for revision"
            );
            (revised != output).then_some(revised)
        }
        Err(e) => {
            observable.record_self_review_verdict(
                agent_kind,
                &cfg,
                crate::observable::SELF_REVIEW_VERDICT_FAILED,
            );
            tracing::warn!(
                agent_kind = %agent_kind,
                "Self-review failed: {}",
                e
            );
            None
        }
    }
}

/// Validate raw actor output against a configured JSON Schema.
///
/// Returns `true` when the output parses as JSON and satisfies the schema.
/// Failures are logged as warnings; callers always keep their legacy
/// lenient parsing so a mis-configured schema can never break the pipeline.
pub(crate) fn validate_against_schema(
    schema: &serde_json::Value,
    output: &str,
    agent_kind: &str,
) -> bool {
    let value: serde_json::Value = match serde_json::from_str(output) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                agent_kind = %agent_kind,
                error = %e,
                "Actor output is not valid JSON; cannot apply configured schema"
            );
            return false;
        }
    };

    let validator = match jsonschema::validator_for(schema) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                agent_kind = %agent_kind,
                error = %e,
                "Configured PGE output schema is invalid; ignoring it"
            );
            return true;
        }
    };

    let errors: Vec<String> = validator
        .iter_errors(&value)
        .map(|e| e.to_string())
        .collect();
    if errors.is_empty() {
        true
    } else {
        tracing::warn!(
            agent_kind = %agent_kind,
            errors = ?errors,
            "Actor output failed configured schema validation"
        );
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{
        actor_input, maybe_self_review, review_basis, review_spec, validate_against_schema,
        ReviewBasis, ReviewFacts,
    };
    use crate::observable::{
        global_observable, SELF_REVIEW_CRITERIA_ABSENT, SELF_REVIEW_CRITERIA_DECLARED,
        SELF_REVIEW_CRITERIA_NOT_ASKED, SELF_REVIEW_DECIDED_BY_DISABLED,
        SELF_REVIEW_DECIDED_BY_MODEL, SELF_REVIEW_DECIDED_BY_PROGRAM_JUDGED,
        SELF_REVIEW_DECIDED_BY_SELF_EVOLUTION, SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE,
        SELF_REVIEW_VERDICT_NEED_REVISION, SELF_REVIEW_VERDICT_NOT_JUDGED,
        SELF_REVIEW_VERDICT_PASS,
    };
    use cog_core::observability::Observable;
    use std::sync::{Arc, Mutex};

    fn task_with_goal(goal: &str) -> cog_core::Task {
        cog_core::Task::new(
            "t-1",
            cog_core::TaskType::Planner,
            serde_json::json!({ "goal": goal }),
        )
    }

    /// 上下文抄了一遍 goal 时，请求那半不再抄第二遍。
    ///
    /// 每一条角色契约都读 `context.goal`，所以删的是 `task.input.goal`：
    /// 契约里指着的那个字段仍在，模型读到的文字一字不少，少付的是重复的那一份。
    #[test]
    fn the_goal_is_in_the_request_once() {
        let goal = "fix the boot phase footprint alert";
        let task = task_with_goal(goal);
        let input = actor_input(
            &task,
            serde_json::json!({ "instructions": "read context.goal" }),
            serde_json::json!({ "goal": goal, "task_id": "t-1" }),
        );

        let rendered = input.to_string();
        let occurrences = rendered.matches(goal).count();
        assert_eq!(
            occurrences, 1,
            "the goal is in the request {occurrences} times: {rendered}"
        );
        assert_eq!(input["context"]["goal"], serde_json::json!(goal));
        assert!(
            input["task"]["input"].get("goal").is_none(),
            "the request half still carries a second copy: {}",
            input["task"]["input"]
        );
        assert_eq!(
            input["task"]["input"]["task_type"],
            serde_json::json!(null),
            "removing the goal must not take anything else with it: {}",
            input["task"]["input"]
        );
    }

    /// 两份 goal 不同时不是重复，是两个值，都得留着。
    #[test]
    fn a_context_goal_that_says_something_else_survives() {
        let task = task_with_goal("the request's goal");
        let input = actor_input(
            &task,
            serde_json::json!({}),
            serde_json::json!({ "goal": "the context's goal" }),
        );

        assert_eq!(
            input["task"]["input"]["goal"],
            serde_json::json!("the request's goal")
        );
        assert_eq!(
            input["context"]["goal"],
            serde_json::json!("the context's goal")
        );
    }

    /// 整份任务输入被抄进上下文时，已经在别处的字段不再抄第二遍。
    ///
    /// 判据是「这段字在这份请求里出现几次」，不是「这个字段叫什么」：字段名
    /// 会随任务的形状变，重复的字节不会。这条用例的题目就是那一件实测任务的
    /// 形状——`goal` 与 `failure_log` 各 32 KB，两份就是整个请求的 43%。
    #[test]
    fn a_context_input_copy_does_not_send_what_the_request_already_sends() {
        let goal = "修好 EVENT_BUS 指标没有播种的那条用例";
        let failure_log = "## Test (job 126812962037)\n".repeat(200);
        let mut task = task_with_goal(goal);
        task.input["failure_log"] = serde_json::json!(failure_log);
        task.input["change_id"] = serde_json::json!("change-ledger-1");

        let input = actor_input(
            &task,
            serde_json::json!({}),
            serde_json::json!({
                "goal": goal,
                "input": {
                    "goal": goal,
                    "failure_log": failure_log,
                    "change_id": "change-ledger-1",
                    "only_in_the_context_input": "keep-me",
                },
            }),
        );

        let rendered = input.to_string();
        assert_eq!(
            rendered.matches(goal).count(),
            1,
            "goal 在请求里出现了不止一次"
        );
        assert_eq!(
            rendered.matches("## Test (job 126812962037)").count(),
            200,
            "failure_log 的第二份没有被删掉: {} 份",
            rendered.matches("## Test (job 126812962037)").count() / 200
        );
        // 别处没有的那一份必须原样留着，否则这条规则会删掉模型唯一能读到的值。
        assert_eq!(
            input["context"]["input"]["only_in_the_context_input"],
            serde_json::json!("keep-me"),
            "上下文输入里唯一的那一份被删了: {}",
            input["context"]["input"]
        );
    }

    /// 两份值不同时不是重复：删任何一份都会改变这次请求的意思。
    #[test]
    fn a_context_input_field_that_says_something_else_survives() {
        let mut task = task_with_goal("请求里的目标");
        task.input["failure_log"] = serde_json::json!("请求里的日志");

        let input = actor_input(
            &task,
            serde_json::json!({}),
            serde_json::json!({
                "goal": "请求里的目标",
                "input": { "failure_log": "上下文里另写的日志" },
            }),
        );

        assert_eq!(
            input["context"]["input"]["failure_log"],
            serde_json::json!("上下文里另写的日志")
        );
        assert_eq!(
            input["task"]["input"]["failure_log"],
            serde_json::json!("请求里的日志")
        );
    }

    /// 上下文里没有 goal 时，请求那半是唯一的一份，动不得。
    #[test]
    fn a_goal_with_nothing_to_compare_against_is_kept() {
        let task = task_with_goal("the only copy");
        let input = actor_input(&task, serde_json::json!({}), serde_json::json!({}));

        assert_eq!(
            input["task"]["input"]["goal"],
            serde_json::json!("the only copy")
        );
    }

    /// A stage name this one test owns, for reading a counter the production
    /// paths also write.
    ///
    /// The self-review counters live on the process-global observable, and every
    /// test in this binary runs in that one process, so a difference read of a
    /// cell a production path also writes is a read of a counter other tests may
    /// be moving at the same moment: reading `{stage="moderator",
    /// reason="disabled"}` against its own earlier value once landed five ahead
    /// of what the call under test had left there. The role and the case both go
    /// into the name, so no two tests write the same cell; a name no production
    /// path passes also proves the stage that was recorded came from the
    /// argument rather than from a constant somewhere.
    fn own_stage(role: &str, case: &str) -> String {
        format!("{role} ({case}; own stage)")
    }

    /// What one cell of the verdict counter reads. The stage has to be one the
    /// caller owns (see [`own_stage`]): an exact count of a cell that other
    /// tests also write is a count of what they wrote.
    async fn cell(stage: &str, criteria: &str, verdict: &str, decided_by: &str) -> f64 {
        global_observable()
            .collect_metrics("D8")
            .await
            .unwrap()
            .iter()
            .find(|m| {
                m.name == "self_review_verdict_total"
                    && m.labels.get(crate::LABEL_STAGE).map(String::as_str) == Some(stage)
                    && m.labels.get(crate::LABEL_CRITERIA).map(String::as_str) == Some(criteria)
                    && m.labels.get(crate::LABEL_VERDICT).map(String::as_str) == Some(verdict)
                    && m.labels.get(crate::LABEL_DECIDED_BY).map(String::as_str) == Some(decided_by)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// What one verdict the program settled reads, on the same terms as
    /// [`cell`]: the stage has to be one the caller owns. Matched on who decided
    /// rather than on the verdict, so the assertion is about the saving and not
    /// about which word the outcome happened to land on.
    async fn settled_cell(stage: &str, decided_by: &str) -> f64 {
        global_observable()
            .collect_metrics("D8")
            .await
            .unwrap()
            .iter()
            .find(|m| {
                m.name == "self_review_verdict_total"
                    && m.labels.get(crate::LABEL_STAGE).map(String::as_str) == Some(stage)
                    && m.labels.get(crate::LABEL_DECIDED_BY).map(String::as_str) == Some(decided_by)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// What one cell of the revision counter reads, on the same terms as
    /// [`cell`]: the stage has to be one the caller owns.
    async fn revision_cell(stage: &str, outcome: &str) -> f64 {
        global_observable()
            .collect_metrics("D8")
            .await
            .unwrap()
            .iter()
            .find(|m| {
                m.name == "self_review_revision_total"
                    && m.labels.get(crate::LABEL_STAGE).map(String::as_str) == Some(stage)
                    && m.labels.get("outcome").map(String::as_str) == Some(outcome)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// The agent under review, recording the configuration each review was
    /// actually handed — the only place a filled-in specification is visible
    /// from outside the review.
    struct ReviewRecorder {
        seen: Seen,
        pass: bool,
        /// The text the revision step hands back, when the review asks for one.
        /// `None` is the revision that rewrote nothing — the text it was given,
        /// unchanged — which is a different ending from a rewrite.
        revised: Option<String>,
    }

    /// 记录表放在替身外面：断言经 `dyn Agent` 读的是同一份。
    type Seen = Arc<Mutex<Vec<cog_core::SelfReviewConfig>>>;

    impl ReviewRecorder {
        fn recording(pass: bool) -> (Arc<dyn cog_core::Agent>, Seen) {
            Self::recording_with_revision(pass, None)
        }

        fn recording_with_revision(
            pass: bool,
            revised: Option<&str>,
        ) -> (Arc<dyn cog_core::Agent>, Seen) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let agent = ReviewRecorder {
                seen: seen.clone(),
                pass,
                revised: revised.map(str::to_string),
            };
            (Arc::new(agent), seen)
        }
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for ReviewRecorder {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Err(cog_core::SFError::NotImplemented("prompt".into()))
        }
        async fn start(&self) {}
        async fn snapshot(
            &self,
            _task_id: String,
        ) -> cog_core::SFResult<cog_core::snapshot::AgentCheckpoint> {
            Err(cog_core::SFError::NotImplemented("snapshot".into()))
        }
        async fn restore(
            &self,
            _snapshot: &cog_core::snapshot::AgentCheckpoint,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn continue_(
            &self,
            _input: serde_json::Value,
        ) -> cog_core::SFResult<serde_json::Value> {
            Err(cog_core::SFError::NotImplemented("continue_".into()))
        }
        async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
            Err(cog_core::SFError::NotImplemented("steer".into()))
        }
        async fn abort(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn reset(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
            Err(cog_core::SFError::NotImplemented("state".into()))
        }
        async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
            Err(cog_core::SFError::NotImplemented("restore_from_id".into()))
        }
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::NotImplemented("chat_stream".into()))
        }
        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::NotImplemented("complete_stream".into()))
        }
        async fn read_board(
            &self,
            _task_id: &str,
            _field: &str,
        ) -> cog_core::SFResult<Option<String>> {
            Ok(None)
        }
        async fn write_board(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn review_and_revise(
            &self,
            output: &str,
            config: &cog_core::SelfReviewConfig,
        ) -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
            self.seen.lock().unwrap().push(config.clone());
            let result = if self.pass {
                cog_core::SelfReviewResult::Pass {
                    score: 0.9,
                    summary: "mock review".into(),
                }
            } else {
                cog_core::SelfReviewResult::NeedRevision {
                    critique: "no rollback step".into(),
                    suggestions: vec!["name the file".into()],
                    score: 0.2,
                }
            };
            let revised = self.revised.clone().unwrap_or_else(|| output.to_string());
            Ok((revised, result))
        }
    }

    /// A review of an actor's output is held to what that actor was asked to
    /// do, taken from the task rather than left empty: a comparison with
    /// nothing outside the review in it returns a score the review itself
    /// wrote, and the pass/fail decision reads exactly that number.
    #[tokio::test]
    async fn a_review_is_held_to_the_task_it_was_asked_to_satisfy() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let stage = own_stage("planner", "held to the task");
        let before = cell(
            &stage,
            SELF_REVIEW_CRITERIA_DECLARED,
            SELF_REVIEW_VERDICT_PASS,
            SELF_REVIEW_DECIDED_BY_MODEL,
        )
        .await;

        let _ = maybe_self_review(
            agent.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            &stage,
            crate::actors::ReviewBasis::HeldTo(review_spec(
                &task_with_goal("add a rollback step"),
                &["the plan names the file"],
            )),
            "t-1",
        )
        .await;

        let handed = seen.lock().unwrap().clone();
        assert_eq!(handed.len(), 1, "one review was run");
        assert_eq!(
            handed[0].spec.as_deref(),
            Some("add a rollback step\n\nAcceptance criteria: the plan names the file"),
            "the task's own words are the specification the review is held to"
        );
        assert_eq!(
            cell(
                &stage,
                SELF_REVIEW_CRITERIA_DECLARED,
                SELF_REVIEW_VERDICT_PASS,
                SELF_REVIEW_DECIDED_BY_MODEL,
            )
            .await,
            before + 1.0,
            "a review with a standard in it is counted as such, under the stage it was given"
        );
    }

    /// A specification configured deliberately is the standard that was meant
    /// to be applied, so the task's own words do not displace it.
    #[tokio::test]
    async fn a_configured_specification_outranks_the_tasks_own_words() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let config = cog_core::SelfReviewConfig {
            spec: Some("the operator's standard".into()),
            ..Default::default()
        };

        let _ = maybe_self_review(
            agent.as_ref(),
            &Some(config),
            "draft",
            "merger",
            crate::actors::ReviewBasis::HeldTo(review_spec(
                &task_with_goal("merge the branches"),
                &[],
            )),
            "t-1",
        )
        .await;

        assert_eq!(
            seen.lock().unwrap()[0].spec.as_deref(),
            Some("the operator's standard")
        );
    }

    /// A task that declares nothing leaves the review with no external
    /// criterion, and that is a reading rather than a silent default: the
    /// verdict is counted under the cell that says so.
    #[tokio::test]
    async fn a_review_with_nothing_declared_is_counted_as_running_without_one() {
        let (agent, seen) = ReviewRecorder::recording(false);
        let stage = own_stage("moderator", "nothing declared");
        let before = cell(
            &stage,
            SELF_REVIEW_CRITERIA_ABSENT,
            SELF_REVIEW_VERDICT_NEED_REVISION,
            SELF_REVIEW_DECIDED_BY_MODEL,
        )
        .await;

        let _ = maybe_self_review(
            agent.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            &stage,
            crate::actors::ReviewBasis::HeldTo(review_spec(
                &cog_core::Task::new("t-2", cog_core::TaskType::Planner, serde_json::json!({})),
                &[],
            )),
            "t-2",
        )
        .await;

        assert_eq!(seen.lock().unwrap()[0].spec, None);
        assert_eq!(
            cell(
                &stage,
                SELF_REVIEW_CRITERIA_ABSENT,
                SELF_REVIEW_VERDICT_NEED_REVISION,
                SELF_REVIEW_DECIDED_BY_MODEL,
            )
            .await,
            before + 1.0,
            "the gate objected, and the stage had nothing but its own text to object to"
        );
    }

    /// 改写这一步买到了什么，两种结局各自成格。
    ///
    /// 「循环从没走到改写」与「改写每次都在重写文本」在判定面上完全同形，
    /// 而前者省不下任何东西、后者省下的正是下一轮那两次调用。
    #[tokio::test]
    async fn a_revision_is_counted_by_what_it_rewrote() {
        let (_agent_rewriting, _) = ReviewRecorder::recording_with_revision(false, Some("better"));
        let agent_rewriting = _agent_rewriting;
        let _ = maybe_self_review(
            agent_rewriting.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            "revision_probe_rewrote",
            crate::actors::ReviewBasis::HeldTo("spec".into()),
            "t-1",
        )
        .await;
        assert_eq!(
            revision_cell(
                "revision_probe_rewrote",
                crate::observable::SELF_REVIEW_REVISION_CHANGED
            )
            .await,
            1.0
        );
        assert_eq!(
            revision_cell(
                "revision_probe_rewrote",
                crate::observable::SELF_REVIEW_REVISION_UNCHANGED
            )
            .await,
            0.0
        );

        let (agent_noop, _) = ReviewRecorder::recording(false);
        let _ = maybe_self_review(
            agent_noop.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "draft",
            "revision_probe_noop",
            crate::actors::ReviewBasis::HeldTo("spec".into()),
            "t-1",
        )
        .await;
        assert_eq!(
            revision_cell(
                "revision_probe_noop",
                crate::observable::SELF_REVIEW_REVISION_UNCHANGED
            )
            .await,
            1.0,
            "改写交回原文 = 循环就此收尾，这一格是没买的那些轮"
        );
        assert_eq!(
            revision_cell(
                "revision_probe_noop",
                crate::observable::SELF_REVIEW_REVISION_CHANGED
            )
            .await,
            0.0
        );
    }

    /// 只判不写的那一次，在改写面上自成格。
    ///
    /// 它不能落进 `unchanged`：那一格记的是一次**付过钱**、交回原文的改写。
    /// 把两者并在一起，这个模式省下来的开销就读成「改写一直没改成」。
    #[tokio::test]
    async fn a_judge_only_review_is_not_counted_as_a_rewrite_that_changed_nothing() {
        let (agent, _) = ReviewRecorder::recording(false);
        let config = cog_core::SelfReviewConfig {
            revise: false,
            ..Default::default()
        };

        let revised = maybe_self_review(
            agent.as_ref(),
            &Some(config),
            "draft",
            "judge_only_probe",
            crate::actors::ReviewBasis::HeldTo("spec".into()),
            "t-1",
        )
        .await;

        assert!(
            revised.is_none(),
            "只判不写：调用方拿回自己的原文，判词里带着没满足的条目"
        );
        assert_eq!(
            revision_cell(
                "judge_only_probe",
                crate::observable::SELF_REVIEW_REVISION_JUDGED_ONLY
            )
            .await,
            1.0
        );
        assert_eq!(
            revision_cell(
                "judge_only_probe",
                crate::observable::SELF_REVIEW_REVISION_UNCHANGED
            )
            .await,
            0.0,
            "没买改写不等于买了一次什么都没改的改写"
        );
        assert_eq!(
            revision_cell(
                "judge_only_probe",
                crate::observable::SELF_REVIEW_REVISION_CHANGED
            )
            .await,
            0.0
        );
    }

    /// An actor that never reached its upstream produced a placeholder, not a
    /// deliverable. The two calls a review would spend would go to the same
    /// unavailable upstream, and a revision of a placeholder replaces a real
    /// cause with a plausible-looking answer — so no review is run, and the
    /// calls it saved are counted rather than silently passed over.
    #[tokio::test]
    async fn a_placeholder_is_not_reviewed_and_the_calls_it_saved_are_counted() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let stage = own_stage("evaluator", "placeholder");
        let before = settled_cell(&stage, SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE).await;

        let revised = maybe_self_review(
            agent.as_ref(),
            &Some(cog_core::SelfReviewConfig::default()),
            "{\"verdict\":\"Fail\"}",
            &stage,
            crate::actors::ReviewBasis::Passed(SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE),
            "t-1",
        )
        .await;

        assert!(
            revised.is_none(),
            "the placeholder is handed back untouched"
        );
        assert!(
            seen.lock().unwrap().is_empty(),
            "no LLM call is bought for an upstream that just failed"
        );
        assert_eq!(
            settled_cell(&stage, SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE).await,
            before + 1.0
        );
        assert_eq!(
            cell(
                &stage,
                SELF_REVIEW_CRITERIA_NOT_ASKED,
                SELF_REVIEW_VERDICT_NOT_JUDGED,
                SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE,
            )
            .await,
            before + 1.0,
            "an outage is not a pass: counted as one it would make the pass rate rise \
             exactly in the hour the upstream was down"
        );
    }

    /// A stage with reviews turned off is a saving too, and reads as its own
    /// decided-by: it was not a gate that objected.
    #[tokio::test]
    async fn a_stage_with_reviews_disabled_is_counted_as_passing_without_a_call() {
        let (agent, seen) = ReviewRecorder::recording(true);
        let stage = own_stage("moderator", "reviews off");
        let before = settled_cell(&stage, SELF_REVIEW_DECIDED_BY_DISABLED).await;

        let revised = maybe_self_review(
            agent.as_ref(),
            &None,
            "draft",
            &stage,
            crate::actors::ReviewBasis::HeldTo(String::new()),
            "t-disabled",
        )
        .await;

        assert!(revised.is_none());
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(
            settled_cell(&stage, SELF_REVIEW_DECIDED_BY_DISABLED).await,
            before + 1.0
        );
    }

    #[test]
    fn the_specification_names_the_goal_the_criteria_or_both() {
        assert_eq!(review_spec(&task_with_goal("ship it"), &[]), "ship it");
        assert_eq!(
            review_spec(&task_with_goal(""), &["no rollback step"]),
            "Acceptance criteria: no rollback step"
        );
        assert_eq!(
            review_spec(&task_with_goal("ship it"), &["no rollback step", " "]),
            "ship it\n\nAcceptance criteria: no rollback step",
            "a blank criterion is not a criterion"
        );
        assert_eq!(
            review_spec(&task_with_goal("  "), &[]),
            "",
            "a task that declares nothing leaves the specification empty"
        );
    }

    /// 这一笔过不过，是 self-review 自己的第一段判决，不是各位员工各自拍的。
    /// 三个事实里任何一个成立，程序就已经下得了「过」；三个都不成立才轮到
    /// 模型。这条表是那一段的全部：改了这里的顺序或条件，五处调用会一起变，
    /// 所以它必须钉在一处。
    #[test]
    fn the_review_gate_is_one_table_for_every_actor() {
        let mut task = task_with_goal("ship it");
        task.acceptance = vec!["no rollback step".to_string()];

        let cases = [
            (
                ReviewFacts {
                    upstream_failed: true,
                    ..Default::default()
                },
                SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE,
            ),
            (
                ReviewFacts {
                    self_evolution: true,
                    ..Default::default()
                },
                SELF_REVIEW_DECIDED_BY_SELF_EVOLUTION,
            ),
            (
                ReviewFacts {
                    program_judged: true,
                    ..Default::default()
                },
                SELF_REVIEW_DECIDED_BY_PROGRAM_JUDGED,
            ),
            // 上游没接住优先于另外两条：产物是占位符时，另外两个理由描述的
            // 是一件根本没发生的事，读数里挂着它们会点错方向。
            (
                ReviewFacts {
                    upstream_failed: true,
                    self_evolution: true,
                    program_judged: true,
                },
                SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE,
            ),
        ];
        for (facts, reason) in cases {
            match review_basis(&task, facts, &[]) {
                ReviewBasis::Passed(held) => assert_eq!(held, reason, "{facts:?}"),
                ReviewBasis::HeldTo(spec) => {
                    panic!("{facts:?} should have concluded a pass, not bought a review: {spec}")
                }
            }
        }

        // 一个事实都不成立：这一笔才真的进模型，规格是任务自己写下的话。
        match review_basis(&task, ReviewFacts::default(), &[]) {
            ReviewBasis::HeldTo(spec) => assert_eq!(
                spec, "ship it\n\nAcceptance criteria: no rollback step",
                "the review is held to what the task itself declared"
            ),
            ReviewBasis::Passed(reason) => panic!("nothing holds, yet it passed by {reason}"),
        }
    }

    /// 提交方自己写的「怎样算完」必须进规格：评审表每一行都要引一句规格里的
    /// 话，规格里没有的要求，评审根本列不出来——只交目标时，整张表只能从那一句
    /// 话里拆出来。这条测试拿一个只写了 acceptance、没写 goal 的任务，是因为
    /// 那种形态下规格非空只可能来自 acceptance。
    #[test]
    fn the_tasks_own_acceptance_reaches_the_specification() {
        let mut task = task_with_goal("");
        task.acceptance = vec![
            "the rollback step names the file".to_string(),
            " ".to_string(),
            "the plan is one page".to_string(),
        ];

        assert_eq!(
            review_spec(&task, &[]),
            "Acceptance criteria: the rollback step names the file; the plan is one page",
            "every criterion the submitter wrote is in the spec, blanks are not"
        );

        // 调用方给的 criteria 是补充：任务自己的排在前，重复的不再列第二遍。
        assert_eq!(
            review_spec(&task, &["the plan is one page", "no new dependency"]),
            "Acceptance criteria: the rollback step names the file; the plan is one page; \
             no new dependency"
        );
    }

    fn planner_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "required": ["summary", "sub_tasks"],
            "properties": {
                "summary": { "type": "string" },
                "plan": { "type": "object" },
                "sub_tasks": { "type": "array" }
            }
        })
    }

    #[test]
    fn schema_validation_accepts_conforming_output() {
        let output = r#"{"summary": "do the thing", "plan": {}, "sub_tasks": []}"#;
        assert!(validate_against_schema(
            &planner_schema(),
            output,
            "planner"
        ));
    }

    #[test]
    fn schema_validation_rejects_missing_required_fields() {
        let output = r#"{"plan": {}}"#;
        assert!(!validate_against_schema(
            &planner_schema(),
            output,
            "planner"
        ));
    }

    #[test]
    fn schema_validation_rejects_non_json_output() {
        assert!(!validate_against_schema(
            &planner_schema(),
            "not json at all",
            "planner"
        ));
    }

    #[test]
    fn invalid_schema_is_ignored_not_fatal() {
        let bad_schema = serde_json::json!({"type": "nonsense-type"});
        assert!(validate_against_schema(
            &bad_schema,
            r#"{"a": 1}"#,
            "planner"
        ));
    }
}
