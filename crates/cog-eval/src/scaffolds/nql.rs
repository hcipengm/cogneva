//! 薄包真系统：这一行跑的是平台本体，不是在评测台里重写的轻量版。
//!
//! 卖点就是真系统，所以「把一题作为任务投进平台、平台跑完、把答案与 trace 取回来」
//! 这条链路必须是真的。重实现一遍会被问「这跟真平台是不是一个东西」，而那是个无法
//! 回答的问题。
//!
//! 端口开在**平台那一侧**：实现要落在能看见平台类型的地方（组合根），不在这里。
//! 评测台只认 `AgentScaffold`，不认识平台，也不认识平台的任务、总线与 DAG——它们
//! 都在端口后面。这个模块只声明「平台要能收下一题、交回一份带 trace 的答案」。

use std::sync::Arc;

use async_trait::async_trait;

use super::task_text;
use crate::dataset::EvalCase;
use crate::scaffold::{AgentOutput, AgentScaffold, Budget, SolveContext};

/// 平台端口：投一题、等它跑完、把结果取回来。
///
/// **实现在组合根**——评测台只声明这个端口，具体那条走平台 HTTP 路由、把答复投影成
/// `AgentOutput` 的链路落在能看见平台类型的地方，不在这里。这里也不提供任何「缺席时
/// 照样能跑」的默认实现：没有平台时这一行必须表现为「跑不起来」而不是「跑出来一个
/// 低分」，后者的形状是平台被别的东西替掉了。
#[async_trait]
pub trait PlatformRunner: Send + Sync {
    async fn run(&self, request: PlatformRequest) -> anyhow::Result<PlatformReply>;
}

/// 投给平台的一题。
#[derive(Debug, Clone)]
pub struct PlatformRequest {
    /// 题面。逐题投、一题一个任务：把整个数据集合成一个大任务，这一格的分数就
    /// 归因不到任何一道题上。
    pub task: String,
    /// 同一格的三个运行里的哪一个。平台侧要按它分流，三次不能命中同一份缓存——
    /// 那样「三个独立种子」就只是同一个数的三份拷贝。
    pub seed: u64,
    /// 与其它三行同一份预算（步数、上下文上限、温度）。
    pub budget: Budget,
}

/// 平台交回来的一份结果。
#[derive(Debug, Clone)]
pub struct PlatformReply {
    /// 平台侧的任务 id。留着为了可追溯：表里某一行对不上时，要能回去看那一次跑。
    pub task_id: String,
    pub output: AgentOutput,
}

pub struct Nql {
    platform: Arc<dyn PlatformRunner>,
}

impl Nql {
    pub fn new(platform: Arc<dyn PlatformRunner>) -> Self {
        Self { platform }
    }
}

#[async_trait]
impl AgentScaffold for Nql {
    fn name(&self) -> &str {
        "nql"
    }

    async fn solve(&self, case: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        let reply = self
            .platform
            .run(PlatformRequest {
                task: task_text(case),
                seed: ctx.seed,
                budget: ctx.budget,
            })
            .await?;
        // 平台侧的读数（task id）进 trace：一份没有出处的结果在表里是不可追溯的。
        let mut output = reply.output;
        output.trace.insert(
            0,
            crate::metric::StepRecord {
                step_index: 0,
                action_type: "platform_task".into(),
                action_params: serde_json::json!({"task_id": reply.task_id}),
                thought: None,
                duration_ms: 0,
                success: true,
                tool_calls: vec![],
            },
        );
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{FinishReason, NoEnv, ToolSet};
    use crate::scaffolds::test_support::ScriptedLlm;
    use std::sync::Mutex;

    /// 平台替身：只记下收到的请求，回一份固定的答复。
    struct RecordingPlatform {
        seen: Mutex<Vec<PlatformRequest>>,
        answer: String,
    }

    #[async_trait]
    impl PlatformRunner for RecordingPlatform {
        async fn run(&self, request: PlatformRequest) -> anyhow::Result<PlatformReply> {
            let answer = self.answer.clone();
            self.seen.lock().unwrap().push(request);
            Ok(PlatformReply {
                task_id: "task-7".into(),
                output: AgentOutput {
                    final_answer: answer,
                    trace: vec![],
                    tokens: Default::default(),
                    finish: FinishReason::Answered,
                },
            })
        }
    }

    fn case() -> EvalCase {
        EvalCase {
            id: "c1".into(),
            name: "c1".into(),
            input: serde_json::json!("a question"),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    #[tokio::test]
    async fn the_case_is_submitted_with_its_seed_and_the_platform_answer_comes_back() {
        let platform = Arc::new(RecordingPlatform {
            seen: Mutex::new(Vec::new()),
            answer: "the platform's answer".into(),
        });
        let ctx = SolveContext {
            llm: Arc::new(ScriptedLlm::new(vec![])),
            tools: Arc::new(ToolSet::empty()),
            env: Arc::new(NoEnv),
            budget: Budget::default(),
            seed: 2,
        };

        let out = Nql::new(platform.clone())
            .solve(&case(), &ctx)
            .await
            .unwrap();

        assert_eq!(out.final_answer, "the platform's answer");
        let seen = platform.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].task, "a question");
        // 种子与预算要真的投过去：三次运行同一个种子，三个数就是一份拷贝。
        assert_eq!(seen[0].seed, 2);
        assert_eq!(seen[0].budget.max_steps, ctx.budget.max_steps);
        // 平台侧的任务 id 进了 trace，结果才可追溯。
        assert_eq!(out.trace[0].action_type, "platform_task");
        assert_eq!(out.trace[0].action_params["task_id"], "task-7");
    }

    #[tokio::test]
    async fn the_platform_not_running_is_an_error_not_a_low_score() {
        struct Missing;
        #[async_trait]
        impl PlatformRunner for Missing {
            async fn run(&self, _request: PlatformRequest) -> anyhow::Result<PlatformReply> {
                anyhow::bail!("no platform endpoint is wired")
            }
        }
        let ctx = SolveContext {
            llm: Arc::new(ScriptedLlm::new(vec![])),
            tools: Arc::new(ToolSet::empty()),
            env: Arc::new(NoEnv),
            budget: Budget::default(),
            seed: 0,
        };
        let err = Nql::new(Arc::new(Missing))
            .solve(&case(), &ctx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no platform endpoint"), "{err}");
    }
}
