//! What a reset leaves behind.
//!
//! A reset is about the conversation, not about the object. What makes this
//! object reachable is its command channel and the consumer reading its inbox;
//! a reset that drops both and rebuilds only the command loop leaves an agent
//! that answers a prompt and hears nothing — and nothing calls `start_consumer`
//! again on a live agent, so it stays deaf for the rest of the process.
//!
//! The employee case is exactly this object: a standing agent that takes one
//! work item, clears its conversation, and takes the next. A reset that ends
//! its inbox would retire it without anyone noticing, because it would still
//! look idle.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use cog_agent::Agent;
use cog_core::{
    agent_inbox_stream, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions,
    InboxMessage, LlmClient, Message, MessageBackend, RuntimeConfig, SFResult,
};

/// Answers nothing: this test never prompts. A client that could answer would
/// only make the reset under test look like an ordinary run.
struct SilentLlm;

#[async_trait]
impl LlmClient for SilentLlm {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        unimplemented!("the reset test never prompts")
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        unimplemented!("the reset test never completes")
    }

    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        unimplemented!("the reset test never prompts")
    }

    async fn health_check(&self) -> bool {
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_keeps_the_inbox_consumer_reading() {
    let backend = Arc::new(cog_test_support::MemoryMessageBackend::new());
    let agent_id = "reset-test";
    let config = RuntimeConfig {
        agent_id: agent_id.into(),
        ..Default::default()
    };
    let agent = Agent::new(config, Arc::new(SilentLlm)).with_message_backend(backend.clone());

    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let handler_seen = seen.clone();
    agent
        .start_consumer(move |msg| {
            let seen = handler_seen.clone();
            async move {
                seen.lock().expect("seen").push(format!("{msg:?}"));
                Ok(())
            }
        })
        .await
        .expect("the consumer starts on a configured backend");

    let inbox = agent_inbox_stream(agent_id);
    let message = serde_json::to_vec(&InboxMessage::Prompt {
        goal: serde_json::json!({"work": "before the reset"}),
        reply_stream: None,
    })
    .expect("the inbox wire form");

    backend
        .publish(&inbox, &message)
        .await
        .expect("the first message is published");
    wait_for(&seen, 1).await;

    agent.reset().await.expect("the reset runs");

    backend
        .publish(&inbox, &message)
        .await
        .expect("the second message is published");
    wait_for(&seen, 2).await;

    assert_eq!(
        seen.lock().expect("seen").len(),
        2,
        "a reset left an agent that answers prompts and receives nothing"
    );
}

/// Wait until the handler has seen `want` messages, then one more poll so a
/// duplicate delivery would be visible rather than reported as a race.
async fn wait_for(seen: &Arc<Mutex<Vec<String>>>, want: usize) {
    for _ in 0..300 {
        if seen.lock().expect("seen").len() >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "the handler saw {} messages, expected {want}",
        seen.lock().expect("seen").len()
    );
}
