//! 示例 5：多 Agent 协作 — PgePipeline 编排 Planner → Generator → Evaluator。
//! 运行：`cargo run -p cogneva --example multi_agent_collab`

mod common;

use cog_collaboration::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use cog_collaboration::{PgePipeline, PgePipelineConfig};
use cog_core::{Task, TaskType};
use common::MockAgent;

#[tokio::main]
async fn main() -> cog_core::SFResult<()> {
    let task = Task::new(
        "collab-1",
        TaskType::Custom("multi_agent_collab".into()),
        serde_json::json!({"goal": "设计并实现一个限流中间件"}),
    );

    // 三个角色各自由独立 Agent 承担（此处用 MockAgent 代替真实 LLM）。
    let planner = PlannerActor::new(MockAgent::json(serde_json::json!({
        "summary": "token bucket 限流",
        "plan": {"approach": "token bucket", "rate": "100rps"},
        "sub_tasks": []
    })));
    let generator = GeneratorActor::new(MockAgent::json(serde_json::json!({
        "content": {"module": "rate_limiter.rs", "algorithm": "token_bucket"},
        "artifacts": []
    })));
    let evaluator = EvaluatorActor::new(MockAgent::json(serde_json::json!({
        "verdict": "pass",
        "feedback": "算法选择正确，边界条件覆盖完整",
        "score": 92,
        "criteria": [{"name": "correctness", "score": 92, "comment": "ok"}]
    })));

    let pipeline = PgePipeline::new(PgePipelineConfig {
        max_retries: 2,
        timeout_ms: 10_000,
        local_repair_max: 1,
    });
    let result = pipeline
        .execute_task(
            &task,
            serde_json::json!({"criteria": ["correctness"]}),
            &planner,
            &generator,
            &evaluator,
        )
        .await;

    println!("attempts: {}", result.attempts);
    println!("passed: {}", result.passed);
    println!("plan: {}", result.final_plan.summary);
    println!("generation: {}", result.final_generation.content);
    println!(
        "evaluation: verdict={:?} score={:?}",
        result.final_evaluation.verdict, result.final_evaluation.score
    );
    Ok(())
}
