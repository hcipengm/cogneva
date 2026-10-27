//! Which runtime an agent's teardown work runs on.
//!
//! Dropping an agent still owes the registry a deregistration. That call
//! reaches a backend whose connections are shared with the rest of the process:
//! every clone of a redis connection manager is the same connection manager,
//! and the reconnect future behind those clones is one future. Its retry timer
//! is registered on whichever runtime polls it first, and a runtime built for
//! the drop is one that future outlives. When the reconnect fails -- a redis
//! pod being rebuilt is enough -- the timer is left registered on a runtime
//! that no longer exists, and the next clone to poll it panics inside the
//! shared future. A poisoned shared future re-panics for every holder, so the
//! process keeps running while the loops that held a clone die for good.
//!
//! This measures the arrangement rather than the outage: drop an agent inside a
//! runtime whose identity is known, and ask the store which runtime it was
//! called on. A teardown that builds its own runtime answers with a different
//! identity -- and that is the state this test exists to refuse, whether or not
//! the outage has happened yet.

use async_trait::async_trait;
use cog_agent::Agent;
use cog_core::{
    AgentRegistration, AgentRegistry, AssistantMessageEventStream, ChatOptions, ChatResponse,
    CompleteOptions, LlmClient, Message, RuntimeConfig, SFResult,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Answers nothing: the test never prompts, and a client that could answer
/// would only make the drop under test look like an ordinary run.
struct SilentLlm;

#[async_trait]
impl LlmClient for SilentLlm {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        unimplemented!("teardown never prompts")
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        unimplemented!("teardown never prompts")
    }

    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        unimplemented!("teardown never prompts")
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// Records which runtime the deregistration was called on.
struct RecordingRegistry {
    seen: Arc<Mutex<Option<Option<tokio::runtime::Id>>>>,
}

#[async_trait]
impl AgentRegistry for RecordingRegistry {
    async fn register(&self, _registration: &AgentRegistration) -> SFResult<()> {
        Ok(())
    }

    async fn heartbeat(&self, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn deregister(&self, _agent_id: &str) -> SFResult<()> {
        *self.seen.lock().expect("seen") = Some(
            tokio::runtime::Handle::try_current()
                .ok()
                .map(|handle| handle.id()),
        );
        Ok(())
    }

    async fn get(&self, _agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        Ok(None)
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        Ok(Vec::new())
    }

    async fn list_by_role(&self, _role: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(Vec::new())
    }

    async fn list_by_capability(&self, _capability: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deregistration_runs_on_the_runtime_the_process_already_has() {
    let ambient = tokio::runtime::Handle::current().id();
    let seen = Arc::new(Mutex::new(None));
    let registry = Arc::new(RecordingRegistry { seen: seen.clone() });

    let agent = Agent::new(RuntimeConfig::default(), Arc::new(SilentLlm))
        .with_registry(registry as Arc<dyn AgentRegistry>);
    drop(agent);

    // The drop hands the work off rather than waiting for it, so the assertion
    // is made on the work's own arrival rather than on a fixed sleep.
    for _ in 0..200 {
        if seen.lock().expect("seen").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let at = seen
        .lock()
        .expect("seen")
        .expect("deregister never ran: the drop handed the work to nothing");

    assert_eq!(
        at,
        Some(ambient),
        "deregistration ran on a runtime this process does not have ({at:?} instead of \
         {ambient:?}). The connections it uses are shared with every loop holding a clone, and \
         the retry timer of a failed reconnect outlives a runtime built for the drop: the next \
         clone to poll it panics inside the shared future, which poisons it for everyone."
    );
}
