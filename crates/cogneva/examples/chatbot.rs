//! 示例 1：Chatbot — 最小 Agent 问答。
//! 运行：`cargo run -p cogneva --example chatbot`

mod common;

use common::MockAgent;

#[tokio::main]
async fn main() -> cog_core::SFResult<()> {
    let agent = MockAgent::json(serde_json::json!({
        "reply": "你好！我是 Cogneva 助手。"
    }));

    let answer = agent
        .prompt(serde_json::json!({
            "task": {"id": "chat-1"},
            "context": {"user": "你好"}
        }))
        .await?;

    println!("user: 你好");
    println!("assistant: {}", answer["reply"]);
    Ok(())
}
