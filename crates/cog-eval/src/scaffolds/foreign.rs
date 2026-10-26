//! 外部流派那一行：把「在题目的机器上执行一支别人写的 agent 程序」包成主表认的一行。
//!
//! 这一行自己不求解任何东西——它把题面交给 [`ExecRunner`]（由**环境**提供，因为跑的
//! 地方就是那道题的那台机器），拿回一个补丁。所以它在本 crate 里是薄的一层，厚的那一层
//! 在拥有机器的那个后端里。
//!
//! 它比别的行多两样东西，都是被「外部程序不是一个 Rust 循环」逼出来的：
//!
//! 1. **定义域**：它吃的是「一棵树 ＋ 一个题面」，吐一个补丁，所以只在有树的列上有定义。
//!    靠 [`AgentScaffold::applies_to`] 说出来，而不是等到跑起来报错——后者会把「这一列
//!    本来就没有这一行」记成一次缺陷。
//! 2. **上限**：循环不在我们手里，步数管不到它，能管的只有时间与这台机器分给它的资源。
//!    上限由组合根一次给定、所有行共用（见 [`ExecLimits`]）。
//!
//! 环境不能托管外部程序时，这一行**报错**而不是交一个空补丁：空补丁在判分器那里长得
//! 就是「没做对」，而真相是这支流派根本没被跑起来——两种读数的处置相反。

use std::time::Instant;

use async_trait::async_trait;

use crate::dataset::EvalCase;
use crate::exec_seam::{ExecLimits, ExecRequest, ExecRunner, ExecSeam};
use crate::metric::StepRecord;
use crate::scaffold::{AgentOutput, AgentScaffold, SolveContext};
use crate::scaffolds::task_text;

/// 一支外部流派在主表里的那一行。
pub struct ForeignFlow {
    /// 表里那一行的名字（`swe-agent` 这种）。进 trace，**不进**外部程序：它不该知道
    /// 自己在这张表上叫什么。
    name: String,
    /// 在这道题的那台机器里怎么起它。由组合根给定——镜像里装在哪、要带哪些环境变量，
    /// 只有摆镜像的那些人知道。
    entrypoint: String,
    /// 它能跑多久、能用多少资源。四行三列共用一份。
    limits: ExecLimits,
    /// 它在哪些列上有定义（基准名）。空 = 哪一列都没有，那样这一行一个格子也不建，
    /// 是声明写错了而不是跑不动。
    columns: Vec<String>,
}

impl ForeignFlow {
    pub fn new(
        name: impl Into<String>,
        entrypoint: impl Into<String>,
        limits: ExecLimits,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            entrypoint: entrypoint.into(),
            limits,
            columns,
        }
    }
}

#[async_trait]
impl AgentScaffold for ForeignFlow {
    fn name(&self) -> &str {
        &self.name
    }

    fn applies_to(&self, benchmark: &str) -> bool {
        self.columns.iter().any(|column| column == benchmark)
    }

    // 动作空间是空的：这支流派用**它自己的**工具，那些工具的名字在这一侧不存在，声明一串
    // 猜来的名字等于给它加工具。并集那一半对它于是是恒等的，它的工具由它的入口点自己备。

    async fn solve(&self, case: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        let runner: std::sync::Arc<dyn ExecRunner> = match ctx.env.exec_seam() {
            ExecSeam::Ready(runner) => runner,
            ExecSeam::Absent(why) => {
                anyhow::bail!("the `{}` row cannot run case {}: {why}", self.name, case.id)
            }
        };

        let problem = task_text(case);
        let started = Instant::now();
        let outcome = runner
            .run(ExecRequest {
                flow: &self.name,
                entrypoint: &self.entrypoint,
                problem: &problem,
                limits: self.limits,
            })
            .await?;
        let duration_ms = started.elapsed().as_millis() as u64;

        Ok(AgentOutput {
            final_answer: outcome.patch,
            trace: vec![StepRecord {
                step_index: 0,
                action_type: "foreign_flow".to_string(),
                action_params: serde_json::json!({
                    "flow": self.name,
                    "entrypoint": self.entrypoint,
                    "wall_clock_secs": self.limits.wall_clock.as_secs(),
                    "cpus": self.limits.cpus,
                    "memory_bytes": self.limits.memory_bytes,
                }),
                // 这一行唯一的「没量到」要写在判词里，不能留白：外部流派的模型调用在容器里
                // 发的，这一侧看不见，所以本行的 token 读数**不在** `AgentOutput.tokens` 上，
                // 它和别的行不在同一本账上——真要数字得去网关的账上取。
                thought: Some(
                    "the flow's own model calls happen inside the case container and are not \
                     visible here: this row's token spend is not what AgentOutput.tokens holds, \
                     read it off the gateway's ledger"
                        .to_string(),
                ),
                duration_ms,
                success: outcome.finish == crate::scaffold::FinishReason::Answered,
                tool_calls: Vec::new(),
            }],
            // 由**它自己的**入口点去调用上游；这一侧没有它的用量，也不许拿一个零冒充量过。
            // 这一格读作「没有读数」，读法在 trace 那一步里写着。
            tokens: cog_core::Usage::default(),
            finish: outcome.finish,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use cog_core::ToolExecutor;

    use super::*;
    use crate::exec_seam::{ExecOutcome, ExecSeam};
    use crate::scaffold::{Budget, CaseEnv, FinishReason, NoEnv, ToolSet};

    /// 一个答应跑什么就回什么的执行面。它记下每一次请求，好让「题面真的到了它手上」可断言。
    struct ScriptedRunner {
        patch: String,
        finish: FinishReason,
        seen: Mutex<Vec<String>>,
        calls: AtomicUsize,
    }

    impl ScriptedRunner {
        fn new(patch: &str, finish: FinishReason) -> Arc<Self> {
            Arc::new(Self {
                patch: patch.to_string(),
                finish,
                seen: Mutex::new(Vec::new()),
                calls: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl ExecRunner for ScriptedRunner {
        async fn run(&self, req: ExecRequest<'_>) -> anyhow::Result<ExecOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(req.problem.to_string());
            Ok(ExecOutcome {
                patch: self.patch.clone(),
                finish: self.finish,
            })
        }
    }

    /// 一台肯托管外部程序的环境。
    struct HostingEnv(Arc<dyn ExecRunner>);

    #[async_trait]
    impl CaseEnv for HostingEnv {
        fn id(&self) -> &str {
            "hosting"
        }
        fn executor(&self) -> Option<Arc<dyn ToolExecutor>> {
            None
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn exec_seam(&self) -> ExecSeam {
            ExecSeam::Ready(self.0.clone())
        }
        async fn teardown(&self) {}
    }

    impl HostingEnv {
        fn boxed(runner: Arc<dyn ExecRunner>) -> Arc<dyn CaseEnv> {
            Arc::new(Self(runner))
        }
    }

    fn case_with(problem: &str) -> EvalCase {
        EvalCase {
            id: "c1".to_string(),
            name: "c1".to_string(),
            input: serde_json::json!(problem),
            expected_output: None,
            expected_tools: None,
            tags: Vec::new(),
            metrics: Vec::new(),
            metadata: Default::default(),
        }
    }

    fn limits() -> ExecLimits {
        ExecLimits::default()
    }

    fn ctx(env: Arc<dyn CaseEnv>) -> SolveContext {
        SolveContext {
            llm: Arc::new(crate::scaffolds::test_support::ScriptedLlm::new(vec![])),
            tools: Arc::new(ToolSet::empty()),
            env,
            budget: Budget::default(),
            seed: 0,
            benchmark: "swe-bench-pro".to_string(),
            scaffold: "swe-agent".to_string(),
        }
    }

    #[tokio::test]
    async fn the_patch_the_runner_returns_is_what_the_row_answers_with() {
        let problem = "修好那道题";
        let runner = ScriptedRunner::new("--- a/x\n+++ b/x\n", FinishReason::Answered);
        let env = HostingEnv::boxed(runner.clone());
        let flow = ForeignFlow::new(
            "swe-agent",
            "sweagent run --problem /tmp/problem.txt",
            limits(),
            vec!["swe-bench-pro".to_string()],
        );

        let out = flow
            .solve(&case_with(problem), &ctx(env))
            .await
            .expect("a hosted flow runs");

        assert_eq!(out.final_answer, "--- a/x\n+++ b/x\n");
        assert_eq!(out.finish, FinishReason::Answered);
        assert_eq!(out.trace.len(), 1);
        assert_eq!(out.trace[0].action_type, "foreign_flow");
        // 题面必须原样到外部程序手上：它看不到判分器，只该拿到这道题。
        assert_eq!(
            runner.seen.lock().unwrap().as_slice(),
            [problem.to_string()]
        );
        // token 这一格不许拿一个零冒充量过：读数不在这里，话要说出来。
        assert_eq!(out.tokens.total_tokens, 0);
        assert!(out.trace[0]
            .thought
            .as_deref()
            .unwrap()
            .contains("not visible here"));
    }

    #[tokio::test]
    async fn a_hit_time_cap_is_not_reported_as_the_flow_failing_by_itself() {
        let runner = ScriptedRunner::new("", FinishReason::ResourceLimit);
        let flow = ForeignFlow::new("swe-agent", "true", limits(), vec!["swe-bench-pro".into()]);
        let out = flow
            .solve(&case_with("x"), &ctx(HostingEnv::boxed(runner)))
            .await
            .unwrap();
        assert_eq!(out.finish, FinishReason::ResourceLimit);
        assert!(!out.trace[0].success);
    }

    #[tokio::test]
    async fn an_env_that_hosts_nothing_is_an_error_and_never_an_empty_patch() {
        let flow = ForeignFlow::new("swe-agent", "true", limits(), vec!["swe-bench-pro".into()]);
        let err = flow
            .solve(&case_with("x"), &ctx(Arc::new(NoEnv)))
            .await
            .expect_err("no host, no run");
        let text = format!("{err}");
        assert!(text.contains("swe-agent"), "{text}");
        // 理由要念出来：读的人据此知道该去修哪一件事。
        assert!(text.contains("hosts no external agent program"), "{text}");
    }

    #[test]
    fn the_row_exists_only_on_the_columns_it_declares() {
        let flow = ForeignFlow::new(
            "swe-agent",
            "true",
            limits(),
            vec!["swe-bench-pro".to_string()],
        );
        assert!(flow.applies_to("swe-bench-pro"));
        assert!(!flow.applies_to("hle"));
        assert!(!flow.applies_to("toolathlon"));
    }
}
