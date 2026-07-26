//! 示例 3：数据处理管线 — Planner 拆解 → Generator 逐阶段产出。
//! 运行：`cargo run -p cogneva --example data_pipeline`

mod common;

use cog_collaboration::actors::{GeneratorActor, PlannerActor, PreviousAttempt};
use cog_core::{Task, TaskType};
use common::MockAgent;

#[tokio::main]
async fn main() -> cog_core::SFResult<()> {
    let task = Task::new(
        "etl-1",
        TaskType::Custom("data_pipeline".into()),
        serde_json::json!({"goal": "ingest csv → clean → aggregate daily metrics"}),
    );

    let planner = PlannerActor::new(MockAgent::json(serde_json::json!({
        "summary": "three-stage etl",
        "plan": {"stages": ["ingest", "clean", "aggregate"]},
        "sub_tasks": []
    })));
    let plan = planner.plan(&task, 1, None, None, None, None).await;
    println!("plan summary: {}", plan.summary);

    let stages = plan.plan["stages"].as_array().cloned().unwrap_or_default();
    for stage in stages {
        let generator = GeneratorActor::new(MockAgent::json(serde_json::json!({
            "content": {"stage": stage, "rows_out": 1024},
            "artifacts": []
        })));
        let output = generator
            .generate(&task, &plan.plan, 1, PreviousAttempt::default(), None)
            .await;
        println!(
            "stage {} done → rows_out={}",
            output.content["stage"], output.content["rows_out"]
        );
    }
    Ok(())
}
