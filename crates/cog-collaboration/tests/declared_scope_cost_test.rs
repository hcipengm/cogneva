//! What a request that declares its own scope costs, and what it still goes
//! through.
//!
//! The routing rule tiers work by the size a request declares. These tests run
//! the two ends of that tier through the real squad executor — the same one the
//! main flow drives — and count the calls each costs, so "the shortcut is
//! cheaper" and "both still pass the same gates" are readings rather than
//! claims about the code.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cog_collaboration::{derive_task_profile, select_mode, PgeMode, SquadConfig, SquadExecutor};
use cog_core::{
    Agent, AgentCheckpoint, AgentManager, InboxMessage, LlmClient, SelfReviewConfig, Task,
    TaskType, WorkerInfo,
};

// ---------------------------------------------------------------------------
// Call log — one stub answering every role, counted per role
// ---------------------------------------------------------------------------

/// Every prompt a stub was asked for, by role.
///
/// A total alone would not show which stage is missing: the shortcut's whole
/// claim is that one *role* is never asked, and a count that cannot see roles
/// cannot tell that apart from a run that asked everyone cheaper.
#[derive(Default)]
struct CallLog {
    by_role: Mutex<HashMap<String, usize>>,
}

impl CallLog {
    fn record(&self, role: &str) {
        *self
            .by_role
            .lock()
            .unwrap()
            .entry(role.to_string())
            .or_default() += 1;
    }

    fn total(&self) -> usize {
        self.by_role.lock().unwrap().values().sum()
    }

    fn role(&self, role: &str) -> usize {
        self.by_role
            .lock()
            .unwrap()
            .get(role)
            .copied()
            .unwrap_or_default()
    }
}

/// A structurally sound one-line diff: the same artifact comes out of both
/// topologies, so the change-artifact gate both of them run is judged on
/// identical input.
const CHANGE_DIFF: &str = "--- a/docs/quickstart.md\n\
                            +++ b/docs/quickstart.md\n\
                            @@ -1 +1 @@\n\
                            -Install the agent.\n\
                            +Install the agent and start it.\n";

/// The same change as the checkout reports it: `git diff` writes the
/// `diff --git` line first, and a read that does not carry it is not read as
/// the tree's answer at all.
const WORKSPACE_DIFF: &str = "diff --git a/docs/quickstart.md b/docs/quickstart.md\n\
                              index 1111111..2222222 100644\n\
                              --- a/docs/quickstart.md\n\
                              +++ b/docs/quickstart.md\n\
                              @@ -1 +1 @@\n\
                              -Install the agent.\n\
                              +Install the agent and start it.\n";

/// What the checkout behind a round reads back as.
///
/// A producing round is asked about its tree, and the three answers are three
/// different situations: a round that wrote its change, a round that left the
/// tree alone, and a run with no tree to read. They cost differently, so the
/// double says which one it is instead of leaving the default.
#[derive(Clone)]
enum Checkout {
    Changed(String),
    Untouched,
    Unreadable,
}

struct StubAgent {
    role: String,
    response: serde_json::Value,
    log: Arc<CallLog>,
    checkout: Checkout,
}

#[async_trait]
impl Agent for StubAgent {
    async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        self.log.record(&self.role);
        Ok(self.response.clone())
    }

    /// The diff the round left in its checkout, as the real agent harvests it
    /// with `git diff`.
    async fn workspace_change(
        &self,
        _task_id: &str,
        _exclude: &[String],
    ) -> cog_core::SFResult<Option<String>> {
        match &self.checkout {
            Checkout::Changed(diff) => Ok(Some(diff.clone())),
            Checkout::Untouched => Ok(None),
            Checkout::Unreadable => Err(cog_core::SFError::Internal(
                "this run has no checkout to read".into(),
            )),
        }
    }

    async fn start(&self) {}

    async fn snapshot(&self, _task_id: String) -> cog_core::SFResult<AgentCheckpoint> {
        Ok(AgentCheckpoint {
            checkpoint_id: String::new(),
            task_id: String::new(),
            agent_state: serde_json::Value::Null,
            context_window: Vec::new(),
            event_offset: 0,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn restore(&self, _snapshot: &AgentCheckpoint) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn continue_(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }

    async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn abort(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn reset(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        Ok(cog_core::AgentState::Idle)
    }

    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
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
        let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
        producer.end(cog_core::ChatResponse::default());
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(&[], &cog_core::ChatOptions::default())
            .await
    }

    async fn read_board(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
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

    async fn receive_message(&self, _msg: InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }

    /// The self-review loop is itself calls to the same upstream, so a review
    /// counts like any other prompt — leaving it out would understate what a
    /// topology costs and hide what dropping a stage actually buys.
    async fn review_and_revise(
        &self,
        output: &str,
        _config: &SelfReviewConfig,
    ) -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
        self.log.record(&format!("{}:review", self.role));
        Ok((
            output.to_string(),
            cog_core::SelfReviewResult::Pass {
                score: 0.9,
                summary: "stub review".into(),
            },
        ))
    }
}

/// 一个座位被建起来时交出去的那份声明，按交出去的原文记。
///
/// 记的是 `create_agent_declaring` 收到的参数，不是 squad 配置里的字段：中间任何
/// 一层把它丢掉，配置里仍然写着，而判活的尺子上没有。
#[derive(Debug, Clone)]
struct Seat {
    role: String,
    deliverable: cog_core::Deliverable,
    directions: Vec<String>,
}

struct StubAgentManager {
    log: Arc<CallLog>,
    checkout: Checkout,
    seats: Arc<Mutex<Vec<Seat>>>,
}

#[async_trait]
impl AgentManager for StubAgentManager {
    async fn create_agent(
        &self,
        _agent_id: &str,
        role: &str,
        _llm: Arc<dyn LlmClient>,
        _task_id: Option<&str>,
    ) -> cog_core::SFResult<Arc<dyn Agent>> {
        let response = match role {
            "generator" => serde_json::json!({
                "content": { "summary": "wording fix" },
                "artifacts": [{ "name": "change.diff", "content": CHANGE_DIFF, "artifact_type": "change" }],
            }),
            // A pass that judged no declared criterion is downgraded to a fail,
            // and the repair loop then buys another generation -- so the call
            // count this file measures would be counting the stub's laziness
            // rather than the topology. A real evaluator answers the criteria it
            // was given; the double has to do the same.
            "evaluator" => serde_json::json!({
                "verdict": "pass",
                "score": 92,
                "feedback": "the diff applies and matches the request",
                "criteria": [{
                    "name": "the wording is fixed",
                    "score": 90,
                    "comment": "the diff rewords the installation step",
                }],
            }),
            // The moderator keeps the debate going; the second consecutive pass
            // is what ends it, so the round count does not depend on the stub.
            "moderator" => serde_json::json!({
                "decision": "continue",
                "reasoning": "another round",
            }),
            _ => serde_json::json!({ "summary": "plan", "plan": {}, "sub_tasks": [] }),
        };
        Ok(Arc::new(StubAgent {
            role: role.to_string(),
            response,
            log: Arc::clone(&self.log),
            checkout: self.checkout.clone(),
        }))
    }

    async fn create_agent_declaring(
        &self,
        agent_id: &str,
        role: &str,
        llm: Arc<dyn LlmClient>,
        task_id: Option<&str>,
        deliverable: cog_core::Deliverable,
        stated_directions: Vec<String>,
    ) -> cog_core::SFResult<Arc<dyn Agent>> {
        self.seats.lock().unwrap().push(Seat {
            role: role.to_string(),
            deliverable,
            directions: stated_directions,
        });
        self.create_agent(agent_id, role, llm, task_id).await
    }

    async fn dispatch(&self, _msg: InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn list_workers(&self) -> cog_core::SFResult<Vec<WorkerInfo>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn retire_task_agents(&self, _task_id: &str) -> cog_core::SFResult<usize> {
        // 每次调用现造一个替身，没有名册可收。
        Ok(0)
    }

    async fn get_agent(&self, _agent_id: &str) -> cog_core::SFResult<Option<Arc<dyn Agent>>> {
        Ok(None)
    }
}

struct StubLlm;

#[async_trait]
impl LlmClient for StubLlm {
    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
        producer.end(cog_core::ChatResponse::default());
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(&[], &cog_core::ChatOptions::default())
            .await
    }

    async fn chat(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::ChatResponse> {
        Ok(cog_core::ChatResponse::default())
    }

    async fn health_check(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A task of the kind the change pipeline consumes, so a run is judged by the
/// change-artifact gate rather than by a rubric it never reaches.
fn self_evolution_task(goal: &str) -> Task {
    Task::new(
        format!("task-{}", uuid::Uuid::new_v4()),
        TaskType::Custom("self_evolution".into()),
        serde_json::json!({ "goal": goal, "evolution_mode": "generate_change" }),
    )
}

/// A task with no change artifact to validate, which leaves the evaluator as
/// the LLM judge it is everywhere else.
fn plain_task(goal: &str) -> Task {
    Task::new(
        format!("task-{}", uuid::Uuid::new_v4()),
        TaskType::Custom("test".into()),
        serde_json::json!({ "goal": goal }),
    )
}

fn squad_config(goal: &str, task: &Task, pge_mode: PgeMode) -> SquadConfig {
    SquadConfig {
        goal: goal.to_string(),
        context: task.input.clone(),
        pge_mode,
        // No strategy escalation: this test is about the two topologies the
        // routing picked, and a run that escalated would be a third one.
        max_retries: 0,
        profile: Some(derive_task_profile(task)),
        is_self_evolution: task.is_self_evolution(),
        deliverable: task.deliverable(),
        stated_directions: task.stated_directions(),
        ..Default::default()
    }
}

/// Run one task under the topology its own declaration earns, and report the
/// calls it cost.
///
/// The checkout a producing round reads back is the round's own: a round that
/// did its job wrote the change, and this is the case every topology pays for
/// the same way.
async fn run_declared(task: Task) -> (bool, Arc<CallLog>) {
    run_declared_over(Checkout::Changed(WORKSPACE_DIFF.to_string()), task).await
}

/// The same run, with the checkout stated instead of assumed.
async fn run_declared_over(checkout: Checkout, task: Task) -> (bool, Arc<CallLog>) {
    let (success, log, _) = run_declared_recording(checkout, task).await;
    (success, log)
}

/// The same run, reporting the seats it created and the declaration each got.
async fn run_declared_recording(
    checkout: Checkout,
    task: Task,
) -> (bool, Arc<CallLog>, Arc<Mutex<Vec<Seat>>>) {
    let goal = task
        .input
        .get("goal")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let mode = select_mode(&derive_task_profile(&task)).mode;
    let log = Arc::new(CallLog::default());
    let seats: Arc<Mutex<Vec<Seat>>> = Arc::new(Mutex::new(Vec::new()));
    let executor = SquadExecutor::new()
        .with_llm_provider(Arc::new(StubLlm))
        .with_agent_manager(Arc::new(StubAgentManager {
            log: Arc::clone(&log),
            checkout,
            seats: Arc::clone(&seats),
        }))
        .with_self_review(SelfReviewConfig {
            max_iterations: 1,
            quality_threshold: 0.8,
            revise: true,
            spec: None,
            best_practices: Vec::new(),
        });

    let result = executor
        .execute_squad(task.id.clone(), squad_config(&goal, &task, mode))
        .await;

    assert_eq!(
        result.pge_mode, mode,
        "the run must use the routed topology"
    );
    (result.success, log, seats)
}

/// 有人就这件活说过方向，每个座位都带着它去判自己的产出。
///
/// 方向此前只变成一件新活：判这份活的尺子上没有它，「按我说过的算好」与「按默认
/// 算好」在读数上同形。这条测试走真实的派单路径，看的是建每个座位时真正交出去的
/// 那份声明——配置里写着而中间某一层丢掉，正是这条测试要红的那一格。
#[tokio::test]
async fn a_direction_stated_about_the_work_reaches_every_seat_that_judges_it() {
    let mut task = self_evolution_task("quote the failing line in the review");
    task.input[cog_core::INPUT_SIGNAL] = serde_json::json!({
        "kind": cog_core::TASTE_SIGNAL_KIND,
        "taste_kind": "direction_preference",
        "payload": {
            "kind": "direction_preference",
            "preferred": "quoting the failing line",
            "alternative": "a prose verdict",
            "rationale": "prose verdicts could not be checked"
        }
    });

    let (_, _, seats) =
        run_declared_recording(Checkout::Changed(WORKSPACE_DIFF.to_string()), task).await;
    let seats = seats.lock().unwrap().clone();
    assert!(
        !seats.is_empty(),
        "这次运行一个座位都没建：这条判据什么都没在守"
    );
    for seat in &seats {
        assert_eq!(
            seat.directions.len(),
            1,
            "建 {} 时把方向丢了：它判自己的产出时按的是没人说过的那一份标准",
            seat.role
        );
        assert!(
            seat.directions[0].contains("quoting the failing line"),
            "{} 拿到的是概括过的一句，不是提交者的原话：{:?}",
            seat.role,
            seat.directions
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A prose-only edit whose file the request names never asks for a plan: the
/// scope it declares *is* the plan. What remains is unchanged — one producing
/// call, judged by the same change-artifact gate every self-evolution run is
/// judged by.
#[tokio::test]
async fn a_declared_prose_scope_never_asks_for_a_plan() {
    let (success, log) = run_declared(self_evolution_task(
        "fix the wording in `docs/quickstart.md`",
    ))
    .await;

    assert!(
        success,
        "the shortcut still has to produce a passing artifact"
    );
    assert_eq!(
        log.role("planner"),
        0,
        "the declared scope is the plan; nothing asks for another one"
    );
    assert_eq!(
        log.role("generator"),
        1,
        "one producing call, and no retry bought anything: {:?}",
        log.by_role.lock().unwrap()
    );
}

/// A run with no checkout to read is not asked a second time.
///
/// The re-ask exists for a round whose tree came back untouched: the round can
/// write the change into that tree instead of describing it. When the tree
/// could not be read at all there is nothing for the second answer to write
/// into, and the two situations produce the same artifact source — a diff that
/// came from the model — so a rule keyed on the source alone buys a generation
/// that cannot change anything.
#[tokio::test]
async fn a_run_with_no_readable_checkout_is_not_asked_twice() {
    let (_success, log) = run_declared_over(
        Checkout::Unreadable,
        self_evolution_task("fix the wording in `docs/quickstart.md`"),
    )
    .await;

    assert_eq!(
        log.role("generator"),
        1,
        "there is no tree to write into, so a second ask cannot change the answer: {:?}",
        log.by_role.lock().unwrap()
    );
}

/// A round whose tree came back untouched is asked again, once.
///
/// This is the case the re-ask was written for: the tree was read and it is
/// there, so the second answer has somewhere to write the change the first one
/// only described. It is also the half that keeps the rule from being read as
/// "never ask twice" -- the cost the previous test measures is the same round
/// answering a question that had no answer to give.
#[tokio::test]
async fn a_round_that_left_its_checkout_untouched_is_asked_again() {
    let (_success, log) = run_declared_over(
        Checkout::Untouched,
        self_evolution_task("fix the wording in `docs/quickstart.md`"),
    )
    .await;

    assert_eq!(
        log.role("generator"),
        2,
        "the tree was read and had nothing in it, so the round gets one more \
         chance to write: {:?}",
        log.by_role.lock().unwrap()
    );
}

/// The saving is a stage, not the judging: on a task whose evaluator is the LLM
/// judge it is everywhere else, the shortcut still pays for it.
#[tokio::test]
async fn the_shortcut_still_judges_what_it_produced() {
    let (success, log) = run_declared(plain_task("fix the wording in `docs/quickstart.md`")).await;

    assert!(success);
    assert_eq!(log.role("planner"), 0, "still no plan");
    assert!(
        log.role("generator") > 0 && log.role("evaluator") > 0,
        "a generator and an evaluator that did not write the artifact: {:?}",
        log.by_role.lock().unwrap()
    );
}

/// The acceptance the tiering decision names: a text-only change costs
/// significantly fewer calls than a module-level one, and both pass the same
/// gates. The two tasks differ only in what they declare — same stub, same
/// self-review config, same artifact — so the difference in cost is the
/// topology and nothing else.
#[tokio::test]
async fn a_text_only_change_costs_far_less_than_a_module_level_one() {
    let (prose_passed, prose) = run_declared(self_evolution_task(
        "fix the wording in `docs/quickstart.md`",
    ))
    .await;
    let (module_passed, module) = run_declared(self_evolution_task(
        "rework the parser across crates/cog-parser/src/lib.rs, \
         crates/cog-parser/src/lexer.rs, crates/cog-parser/src/ast.rs, \
         crates/cog-parser/src/error.rs",
    ))
    .await;

    assert!(prose_passed, "the text-only change must land");
    assert!(module_passed, "the module-level change must land");

    let (prose_calls, module_calls) = (prose.total(), module.total());
    println!(
        "calls: prose-only {prose_calls} {:?}, module-level {module_calls} {:?}",
        prose.by_role.lock().unwrap(),
        module.by_role.lock().unwrap()
    );
    assert!(
        prose_calls * 2 <= module_calls,
        "a text-only change should cost at most half of a module-level one: \
         {prose_calls} vs {module_calls} ({:?} vs {:?})",
        prose.by_role.lock().unwrap(),
        module.by_role.lock().unwrap()
    );
    assert!(
        module.role("planner") > 0,
        "the module-level change is planned: {:?}",
        module.by_role.lock().unwrap()
    );
}

/// A self-evolution plan is not handed to self-review, for the same reason the
/// change artifact is not: the plan is a JSON value the pipeline parses before
/// the review would run, so a revision can only rewrite prose. The plan is the
/// largest thing either actor hands over, and the review of a document that
/// size is the slowest cell in the ledger — it runs to its timeout when the
/// model answers in prose, which is exactly the answer this path rules out.
#[tokio::test]
async fn a_self_evolution_plan_is_not_self_reviewed() {
    let (passed, log) = run_declared(self_evolution_task(
        "rework the parser across crates/cog-parser/src/lib.rs, \
         crates/cog-parser/src/lexer.rs, crates/cog-parser/src/ast.rs, \
         crates/cog-parser/src/error.rs",
    ))
    .await;

    assert!(passed, "the change still has to land");
    assert!(
        log.role("planner") > 0,
        "this goal is planned, so what is skipped is the review and not the plan: {:?}",
        log.by_role.lock().unwrap()
    );
    assert_eq!(
        log.role("planner:review"),
        0,
        "a parsed plan is not reviewed: the review rewrites prose only, and the \
         document it would rewrite is the largest one on the path"
    );
}
