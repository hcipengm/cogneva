//! 示例 2：代码审查 — Generator 产出修改建议，Evaluator 打分。
//! 运行：`cargo run -p cogneva --example code_review`

mod common;

use cog_collaboration::actors::{EvaluatorActor, GeneratorActor, PreviousAttempt};
use cog_core::{Task, TaskType};
use common::MockAgent;

#[tokio::main]
async fn main() -> cog_core::SFResult<()> {
    let task = Task::new(
        "review-1",
        TaskType::Custom("code_review".into()),
        serde_json::json!({"goal": "review src/lib.rs for correctness"}),
    );

    let generator = GeneratorActor::new(MockAgent::json(serde_json::json!({
        "content": {"review": "lib.rs 第 42 行存在未处理的 None 分支，建议加 .expect(\"msg\")"},
        "artifacts": []
    })));
    let evaluator = EvaluatorActor::new(MockAgent::json(serde_json::json!({
        "verdict": "pass",
        "feedback": "发现真实缺陷，建议可落地",
        "score": 88,
        "criteria": [{"name": "correctness", "score": 88, "comment": "ok"}]
    })));

    let generation = generator
        .generate(
            &task,
            &serde_json::json!({"approach": "static review"}),
            1,
            PreviousAttempt::default(),
            None,
        )
        .await;
    println!("review: {}", generation.content["review"]);

    let evaluation = evaluator
        .evaluate(
            &task,
            &serde_json::json!({"approach": "static review"}),
            &serde_json::json!(generation.content),
            &[],
            &["correctness"],
            None,
        )
        .await;
    println!(
        "verdict: {:?}, score: {:?}, feedback: {}",
        evaluation.verdict, evaluation.score, evaluation.feedback
    );
    Ok(())
}
