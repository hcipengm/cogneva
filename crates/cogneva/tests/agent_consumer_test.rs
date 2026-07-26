use cog_agent::consumer::AgentConsumer;
use cog_core::{InboxMessage, MessageBackend};
use cog_stream::MemoryMessageBackend;
use std::sync::Arc;

#[tokio::test]
async fn test_send_message_and_consume() {
    let backend: Arc<dyn MessageBackend> = Arc::new(MemoryMessageBackend::new());
    let consumer = AgentConsumer::new("agent-1", backend.clone());

    let msg = InboxMessage::Prompt {
        goal: serde_json::json!({"task": "hello"}),
        reply_stream: None,
    };
    AgentConsumer::send_message("agent-1", msg.clone(), backend.as_ref())
        .await
        .unwrap();

    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let handler = move |m: InboxMessage| {
        let received = received_clone.clone();
        async move {
            received.lock().unwrap().push(format!("{:?}", m));
            Ok(())
        }
    };

    let run_fut = consumer.run(handler);
    let timeout = tokio::time::timeout(std::time::Duration::from_millis(100), run_fut);
    let _ = timeout.await;

    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert!(received[0].contains("Prompt"));
}
