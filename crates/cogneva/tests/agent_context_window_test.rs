use cog_agent::ContextWindow;
use cog_core::Message;

fn default_system_prompt() -> String {
    cog_prompt::global_prompt("agent:default")
        .unwrap_or_else(|| "You are a helpful assistant.".into())
}

#[test]
fn test_context_window_add_messages() {
    let mut ctx = ContextWindow::new(1000);
    ctx.add_message(Message::system(default_system_prompt()));
    ctx.add_message(Message::user("Hello!"));

    assert_eq!(ctx.messages().len(), 2);
    assert!(ctx.token_count() > 0);
}

#[test]
fn test_context_window_system_preserved() {
    let mut ctx = ContextWindow::new(10); // Very small to force trimming
    ctx.add_message(Message::system("System prompt".to_string()));
    ctx.add_message(Message::user("Message 1"));
    ctx.add_message(Message::user("Message 2"));
    ctx.add_message(Message::user("Message 3"));

    // System message should always be preserved
    let messages = ctx.messages();
    assert!(messages.iter().any(|m| matches!(m, Message::System { .. })));
}

#[test]
fn test_context_window_clear() {
    let mut ctx = ContextWindow::new(1000);
    ctx.add_message(Message::system("test".to_string()));
    ctx.clear();
    assert_eq!(ctx.messages().len(), 0);
    assert_eq!(ctx.token_count(), 0);
}

#[test]
fn test_context_window_to_prompt() {
    let mut ctx = ContextWindow::new(1000);
    ctx.add_message(Message::system("sys".to_string()));
    ctx.add_message(Message::user("hello"));

    let prompt = ctx.to_prompt();
    assert!(prompt.contains("[system]"));
    assert!(prompt.contains("[user]"));
}
