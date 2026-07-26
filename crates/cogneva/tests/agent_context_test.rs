use cog_agent::context::{estimate_tokens, ContextWindow};
use cog_core::Message;

#[tokio::test]
async fn test_context_window() {
    let tmp = tempfile::tempdir().unwrap();
    let yaml = r#"
prompts:
  agent:default:
    content: "You are a helpful assistant."
    version: "1.0.0"
"#;
    std::fs::write(tmp.path().join("prompts.yaml"), yaml).unwrap();
    let mgr = cog_prompt::PromptManager::from_dir(tmp.path(), cog_prompt::WatchMode::None)
        .await
        .unwrap();

    let mut ctx = ContextWindow::new(1000);
    let default_prompt = mgr
        .get("agent:default")
        .expect("prompt 'agent:default' must be loaded by PromptManager");
    ctx.add_message(Message::system(default_prompt));
    ctx.add_message(Message::user("Hello!"));

    assert_eq!(ctx.messages().len(), 2);
    assert!(ctx.token_count() > 0);
}

#[test]
fn test_token_estimate() {
    assert!(estimate_tokens("Hello world") > 0);
    assert!(estimate_tokens("你好世界") > estimate_tokens("Hello"));
}
