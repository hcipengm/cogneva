//! AgentBench-mini：公开 benchmark 报告框架示例（审计 4.5 代码侧）。
//!
//! 固定任务集 + 确定性 MockAgent，任何环境可复现运行：
//!
//! ```bash
//! cargo run --example agent_bench
//! ```
//!
//! 产出 JSON + Markdown 双工件到 `target/bench-reports/`，
//! Markdown 可直接发布为公开 benchmark 报告。真实 SWE-bench
//! 跑分属于数据侧人工任务，框架保持一致。

mod common;

use std::sync::Arc;
use std::time::Instant;

use cog_reflection::{BenchReport, EvalOutcome, EvalTask};

/// 固定任务集：判定依据为 MockAgent 返回 JSON 中的 `answer` 字段。
fn fixed_suite() -> Vec<(EvalTask, &'static str)> {
    [
        "summarize-log",
        "classify-error",
        "extract-config",
        "plan-rollback",
        "detect-drift",
    ]
    .into_iter()
    .map(|id| {
        (
            EvalTask {
                id: id.to_string(),
                input: serde_json::json!({ "task": id }),
            },
            "ok",
        )
    })
    .collect()
}

#[tokio::main]
async fn main() {
    let started = chrono::Utc::now();
    // 4/5 任务返回正确答案 —— 确定性结果，报告可复现。
    let agent = common::MockAgent::json(serde_json::json!({ "answer": "ok" }));
    let flaky_agent: Arc<dyn cog_core::Agent> = Arc::new(FlakyMock {
        inner: agent,
        fail_task: "detect-drift".to_string(),
    });

    let suite = fixed_suite();
    let mut outcomes = Vec::new();
    for (task, expected) in &suite {
        let begin = Instant::now();
        let result = flaky_agent.prompt(task.input.clone()).await;
        let latency_ms = begin.elapsed().as_millis() as u64;
        let success = result
            .ok()
            .and_then(|v| v.get("answer").and_then(|a| a.as_str().map(String::from)))
            .as_deref()
            == Some(*expected);
        outcomes.push(EvalOutcome {
            task_id: task.id.clone(),
            success,
            latency_ms,
            cost_tokens: (task.input.to_string().len() / 4) as u64,
        });
    }

    let report = BenchReport::new(
        "agentbench-mini",
        env!("CARGO_PKG_VERSION"),
        started,
        outcomes,
    );
    let dir = std::path::Path::new("target/bench-reports");
    let (json_path, md_path) = report
        .write_artifacts(dir)
        .await
        .expect("write bench artifacts");

    println!("suite: agentbench-mini");
    println!(
        "success rate: {:.1}% ({}/{})",
        report.summary.success_rate * 100.0,
        report.summary.succeeded,
        report.summary.total
    );
    println!("mean latency: {:.1} ms", report.summary.mean_latency_ms);
    println!("json: {}", json_path.display());
    println!("markdown: {}", md_path.display());
}

/// 对指定任务返回错误答案，模拟真实的部分失败分布。
struct FlakyMock {
    inner: Arc<dyn cog_core::Agent>,
    fail_task: String,
}

#[async_trait::async_trait]
impl cog_core::Agent for FlakyMock {
    async fn prompt(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        let task = input
            .get("task")
            .and_then(|t| t.as_str())
            .unwrap_or_default();
        if task == self.fail_task {
            return Ok(serde_json::json!({ "answer": "wrong" }));
        }
        self.inner.prompt(input).await
    }

    async fn start(&self) {}
    async fn snapshot(&self, task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        self.inner.snapshot(task_id).await
    }
    async fn restore(&self, snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        self.inner.restore(snapshot).await
    }
    async fn continue_(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        self.inner.continue_(input).await
    }
    async fn steer(&self, instruction: String) -> cog_core::SFResult<()> {
        self.inner.steer(instruction).await
    }
    async fn abort(&self) -> cog_core::SFResult<()> {
        self.inner.abort().await
    }
    async fn reset(&self) -> cog_core::SFResult<()> {
        self.inner.reset().await
    }
    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        self.inner.state().await
    }
    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        self.inner.wait_for_idle().await
    }
    async fn restore_from_id(&self, checkpoint_id: &str) -> cog_core::SFResult<()> {
        self.inner.restore_from_id(checkpoint_id).await
    }
    async fn chat_stream(
        &self,
        messages: &[cog_core::Message],
        options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.inner.chat_stream(messages, options).await
    }
    async fn complete_stream(
        &self,
        prompt: &str,
        options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.inner.complete_stream(prompt, options).await
    }
    async fn read_board(&self, task_id: &str, field: &str) -> cog_core::SFResult<Option<String>> {
        self.inner.read_board(task_id, field).await
    }
    async fn write_board(&self, task_id: &str, field: &str, value: &str) -> cog_core::SFResult<()> {
        self.inner.write_board(task_id, field, value).await
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        self.inner.subscribe()
    }
    async fn receive_message(&self, msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        self.inner.receive_message(msg).await
    }
}
