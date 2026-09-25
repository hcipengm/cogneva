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

struct StubAgent {
    role: String,
    response: serde_json::Value,
    log: Arc<CallLog>,
}

#[async_trait]
impl Agent for StubAgent {
    async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        self.log.record(&self.role);
        Ok(self.response.clone())
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

struct StubAgentManager {
    log: Arc<CallLog>,
}

#[async_trait]
impl AgentManager for StubAgentManager {
    async fn create_agent(
        &self,
        _agent_id: &str,
        role: &str,
        _llm: Arc<dyn LlmClient>,
    ) -> cog_core::SFResult<Arc<dyn Agent>> {
        let response = match role {
            "generator" => serde_json::json!({
                "content": { "summary": "wording fix" },
                "artifacts": [{ "name": "change.diff", "content": CHANGE_DIFF, "artifact_type": "change" }],
            }),
            "evaluator" => serde_json::json!({
                "verdict": "pass",
                "score": 92,
                "feedback": "the diff applies and matches the request",
                "criteria": [],
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
        }))
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
        ..Default::default()
    }
}

/// Run one task under the topology its own declaration earns, and report the
/// calls it cost.
async fn run_declared(task: Task) -> (bool, Arc<CallLog>) {
    let goal = task
        .input
        .get("goal")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let mode = select_mode(&derive_task_profile(&task));
    let log = Arc::new(CallLog::default());
    let executor = SquadExecutor::new()
        .with_llm_provider(Arc::new(StubLlm))
        .with_agent_manager(Arc::new(StubAgentManager {
            log: Arc::clone(&log),
        }))
        .with_self_review(SelfReviewConfig {
            max_iterations: 1,
            quality_threshold: 0.8,
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
    (result.success, log)
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
