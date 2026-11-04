//! Everyone who works in this system can ask to change it — not only the
//! employee sitting in the chat box.
//!
//! The proposal tool was registered on the chat turn's private registry, and
//! that turn copies the process's shared registry into it. So the tool was
//! reachable from exactly one place: somebody typing into the WebUI. The agents
//! that actually do the work are spawned by the task pool out of the shared
//! registry, and none of them could say "I would change this line, here is what
//! it should say, here is why" — the outward loop hands them findings and asks
//! for a proposal they had no way to file.
//!
//! Two halves decide whether that is true, and both are checked here:
//!
//! - **Who asked.** The tool is registered once per process and shared by every
//!   agent, so its handler cannot know the caller; the dispatcher says so around
//!   the call and the handler reads it back. A fixed name would record every
//!   employee's request as the same person's, and the ledger's source column is
//!   exactly "who asked".
//! - **Where it is registered.** On the shared registry, which is the one the
//!   pool builds its workers from — not on the chat turn's private copy.
//!
//! The rest of the tool's contract (a carrier path is refused, the row carries
//! no state, the asker's rejection names the line it refused) is tested in
//! `crates/cogneva/src/self_change.rs` and in the chat turn's own case; nothing
//! here re-states it.

use std::sync::{Arc, Mutex};

use cog_core::{
    ChangeIdentity, ChangeLedgerEvent, ChangeLedgerRow, ChangeLedgerStore, ChangeMeasurement,
    ChangePriority, ChangeReading, ChangeState, SFResult, ToolImplementation,
};
use cogneva::self_change;

/// An account that remembers what was declared and writes nothing else.
///
/// Every other door is unimplemented on purpose: this test is about who a
/// proposal is attributed to, and a stand-in that also measured or delivered
/// would hide a tool that reached a door it should not have.
#[derive(Default)]
struct DeclarationLog {
    declared: Mutex<Vec<ChangeReading>>,
}

#[async_trait::async_trait]
impl ChangeLedgerStore for DeclarationLog {
    async fn declare(&self, readings: &[ChangeReading]) -> SFResult<usize> {
        let mut held = self
            .declared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        held.extend(readings.iter().cloned());
        Ok(readings.len())
    }

    async fn record_readings(
        &self,
        _readings: &[ChangeReading],
        _first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        unimplemented!("a proposal is queued, never folded by the proposer")
    }

    async fn fold_declarations(
        &self,
        _limit: usize,
        _first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        unimplemented!("folding is the resident writer's job")
    }

    async fn fold_retirements(&self, _limit: usize) -> SFResult<usize> {
        unimplemented!("folding is the resident writer's job")
    }

    async fn pending_declarations(&self) -> SFResult<u64> {
        Ok(0)
    }

    async fn pending_retirements(&self) -> SFResult<u64> {
        Ok(0)
    }

    async fn claimed_revisions(
        &self,
        _revisions: &[String],
    ) -> SFResult<std::collections::HashSet<String>> {
        unimplemented!("a proposal claims no revision")
    }

    async fn apply_measurement(&self, _measurement: &ChangeMeasurement) -> SFResult<()> {
        unimplemented!("the proposer never measures its own request")
    }

    async fn outstanding(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the proposer does not read the account")
    }

    async fn events(
        &self,
        _identity: &ChangeIdentity,
        _limit: usize,
    ) -> SFResult<Vec<ChangeLedgerEvent>> {
        unimplemented!("the proposer does not read history")
    }

    async fn owed_by_band(&self) -> SFResult<Vec<(ChangePriority, usize)>> {
        unimplemented!("the proposer does not total the account")
    }

    async fn landed_due(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the proposer does not read landed rows")
    }

    async fn landed_at_revision(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<(ChangeIdentity, Option<String>)>> {
        unimplemented!("the proposer does not read landed rows")
    }

    async fn landed_touching(
        &self,
        _paths: &[String],
        _limit: usize,
    ) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the proposer does not read landed rows")
    }

    async fn landed_awaiting_parent_observation(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<(ChangeIdentity, String)>> {
        unimplemented!("the proposer does not read landed rows")
    }

    async fn record_parent_observation(
        &self,
        _identity: &ChangeIdentity,
        _observation: &str,
    ) -> SFResult<bool> {
        unimplemented!("the proposer does not backfill observations")
    }

    async fn record_reopen(&self, _reopen: &cog_core::ChangeReopen) -> SFResult<()> {
        unimplemented!("the proposer does not reopen rows")
    }

    async fn retired_surface_moved(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the proposer does not read retired rows")
    }

    async fn retired_answered_elsewhere(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the proposer does not read retired rows")
    }

    async fn request_remeasurement(
        &self,
        _identity: &ChangeIdentity,
        _source: &str,
    ) -> SFResult<()> {
        unimplemented!("the proposer does not queue measurements")
    }

    async fn pending_remeasurements(&self, _limit: usize) -> SFResult<Vec<ChangeIdentity>> {
        unimplemented!("the proposer does not read the measurement queue")
    }

    async fn mark_remeasured(&self, _identity: &ChangeIdentity) -> SFResult<()> {
        unimplemented!("the proposer does not fold the measurement queue")
    }

    async fn remeasurement_backlog(&self) -> SFResult<cog_core::RemeasureBacklog> {
        unimplemented!("the proposer does not read the measurement queue")
    }

    async fn record_delivery(&self, _identity: &ChangeIdentity, _rev: &str) -> SFResult<()> {
        unimplemented!("the proposer never delivers")
    }

    async fn stamp_pool_block(&self, _unblock_condition: &str) -> SFResult<u64> {
        unimplemented!("the proposer does not hold the pool")
    }

    async fn clear_pool_block(&self) -> SFResult<u64> {
        unimplemented!("the proposer does not hold the pool")
    }

    async fn pool_wait(&self) -> SFResult<cog_core::PoolWait> {
        unimplemented!("the proposer does not read the pool")
    }

    async fn get(&self, _identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>> {
        unimplemented!("the proposer reads its row back through the chat turn's tool")
    }

    async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>> {
        unimplemented!("the proposer does not total the account")
    }

    async fn record_dispatch_state(&self, _state: &cog_core::DispatchState) -> SFResult<()> {
        unimplemented!("the proposer does not dispatch")
    }
}

fn arguments() -> serde_json::Value {
    serde_json::json!({
        "path": "deploy/helm/cogneva/files/cogneva.json",
        "literal": "\"context_window_size\": 96000",
        "why": "上一轮它撞了上下文天花板，而它读的那份配置里这个数比同事小一半",
    })
}

async fn call(tool: &cog_core::Tool, args: serde_json::Value) -> serde_json::Value {
    let ToolImplementation::Native(handler) = &tool.implementation else {
        panic!("the proposal tool is a native handler");
    };
    handler(args)
        .await
        .expect("a well-formed proposal is not an error")
}

/// The name the dispatcher announced is the name the account records.
///
/// This is the whole reason the tool takes no asker argument: the registry is
/// shared, the handler is one closure, and only the run loop knows whose call
/// this is. A proposal filed under the wrong name is a row nobody can act on —
/// "who wants this" is not derivable from anything else in the row.
#[tokio::test]
async fn the_account_names_the_employee_the_dispatcher_announced() {
    let ledger = Arc::new(DeclarationLog::default());
    let store: Arc<dyn ChangeLedgerStore> = ledger.clone();
    let tool = self_change::tool_for_caller(Some(store));

    let answer = cog_agent::tools::CALLER_AGENT_ID
        .scope("employee-7".to_string(), call(&tool, arguments()))
        .await;

    assert_eq!(
        answer["queued"], true,
        "the proposal did not reach the account: {answer}"
    );
    let declared = ledger
        .declared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(declared.len(), 1, "one proposal queues exactly one row");
    assert_eq!(
        declared[0].source.doc, "self-change:employee-7",
        "the row is filed under somebody else's name: the source column is the \
         only place that says who asked"
    );
}

/// A call the dispatcher never attributed is recorded as unattributed — not
/// under the last name that happened to call, and not under an invented one.
#[tokio::test]
async fn an_unattributed_call_is_not_credited_to_anybody() {
    let ledger = Arc::new(DeclarationLog::default());
    let store: Arc<dyn ChangeLedgerStore> = ledger.clone();
    let tool = self_change::tool_for_caller(Some(store));

    // Outside any `CALLER_AGENT_ID` scope, as a handler invoked directly is.
    let answer = call(&tool, arguments()).await;

    assert_eq!(answer["queued"], true, "nobody said who called: {answer}");
    let declared = ledger
        .declared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(
        declared[0].source.doc,
        format!("self-change:{}", self_change::UNATTRIBUTED_ASKER),
        "an unattributed proposal was credited to a name nobody gave"
    );
}

/// The assembly puts the tool on the registry the workers are built from.
///
/// The chat turn copies the shared registry into a private one before adding
/// its own tools, so a tool registered only on the chat turn is invisible to
/// every agent the pool spawns — and that absence is indistinguishable from a
/// deployment that chose not to offer it.
#[test]
fn the_shared_registry_carries_the_tool() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(
        source.contains("self_change::tool_for_caller("),
        "the assembly never registers the ask-to-change tool on the shared registry: \
         the only agents who can ask to change this system are the ones typing into \
         the chat box"
    );
    assert!(
        source.contains("consume::<cog_agent::ToolRegistry>()"),
        "the tool is registered somewhere other than the process's shared registry: \
         the pool spawns its workers from that one, so anything else is a private \
         copy no worker ever sees"
    );
}
