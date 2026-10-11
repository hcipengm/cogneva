//! 规划 → 执行 → 校验 → 重生成，四个角色串成一条流水线，带一份逐题进化的记忆。
//!
//! 四个角色共用**同一个 backbone**，靠提示词分开：把四个角色实现成四个模型会往这一行
//! 里塞进一个评测台不知道的变量（四行本来只允许 backbone 是常数，角色不是）。论文里
//! 那条流程的四个角色是同一模型的四段提示词，这里也一样。
//!
//! 记忆在这一行里的作用是：每个步骤过完之后把「做了什么、判什么、改了什么」攒起来，
//! 喂给后面的步骤。它不是跨题记忆——跨题记忆会让第二题的分数取决于第一题，主表的
//! 每格就不独立了。

use std::time::Instant;

use async_trait::async_trait;
use cog_core::{Message, Usage};

use super::{accumulate, chat_options, observe_call, take_turn, task_text};
use crate::dataset::EvalCase;
use crate::metric::StepRecord;
use crate::scaffold::{AgentOutput, AgentScaffold, FinishReason, SolveContext};

const PLANNER_PROMPT: &str = "\
You plan. Given a task, reply with a numbered list of concrete steps, one per line, and \
nothing else.";

const EXECUTOR_PROMPT: &str = "\
You carry out one step of a plan. Use the tools you are given when they help. Reply with \
what you found or did.";

const VERIFIER_PROMPT: &str = "\
You check one step's result against the task. Reply with `VERDICT: PASS` or `VERDICT: FAIL` \
followed by one sentence saying why.";

const GENERATOR_PROMPT: &str = "\
You produce the deliverable. Given the task, the plan and the step results, reply with the \
final answer and nothing else. If a step failed, repair it as well as you can first.";

pub struct AgentFlow {
    /// 一个步骤最多重生成几次。用尽之后这一步骤按「没做好」记进记忆，流程继续——
    /// 卡在某一步上不动，整题就永远是 0，而其它步骤的产物对判分可能仍然有用。
    max_regenerations: usize,
}

impl AgentFlow {
    pub fn new(max_regenerations: usize) -> Self {
        Self { max_regenerations }
    }
}

impl Default for AgentFlow {
    fn default() -> Self {
        Self::new(2)
    }
}

/// 把规划读成步骤。读不出编号行就整体当成一步——**不能**返回空计划：空计划的
/// 流水线一步不走，交回去的就是空答案，而失败原因会和「规划失败」完全不同形。
pub(crate) fn parse_plan(text: &str) -> Vec<String> {
    let mut steps = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        let rest = trimmed
            .strip_prefix("- ")
            .or_else(|| trimmed.split_once(". ").map(|(_, rest)| rest))
            .or_else(|| trimmed.split_once(") ").map(|(_, rest)| rest));
        if let Some(rest) = rest {
            let rest = rest.trim();
            if !rest.is_empty() {
                steps.push(rest.to_string());
            }
        }
    }
    if steps.is_empty() {
        let whole = text.trim();
        if !whole.is_empty() {
            steps.push(whole.to_string());
        }
    }
    steps
}

/// 校验判词：`VERDICT: PASS|FAIL` 加一句原因。
///
/// 读不出判词按**没过**算：校验器说不清过没过，就不能当成过——把它当过的形状是
/// 「校验器坏了、所有步骤都通过、分数还很高」。
pub(crate) fn parse_verdict(text: &str) -> (bool, String) {
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("VERDICT:") {
            let passed = rest.trim().to_ascii_uppercase().starts_with("PASS");
            return (passed, trimmed.to_string());
        }
    }
    (false, format!("unreadable verdict: {text}"))
}

impl AgentFlow {
    /// 一次带工具的执行：跑到模型不再请求工具为止。
    async fn run_step(
        &self,
        ctx: &SolveContext,
        messages: &mut Vec<Message>,
        options: &cog_core::ChatOptions,
        trace: &mut Vec<StepRecord>,
        tokens: &mut Usage,
        start_index: usize,
    ) -> anyhow::Result<(String, usize)> {
        let mut index = start_index;
        let mut text = String::new();
        for _ in 0..ctx.budget.max_steps {
            let started = Instant::now();
            let turn = take_turn(ctx, messages, options).await?;
            accumulate(tokens, &turn.usage);
            text = turn.text.clone();
            if turn.calls.is_empty() {
                return Ok((text, index));
            }
            messages.push(Message::assistant(turn.content.clone()));
            for (id, name, arguments) in &turn.calls {
                let (_, ok) = observe_call(ctx, messages, id, name, arguments).await;
                trace.push(StepRecord {
                    step_index: index,
                    action_type: name.clone(),
                    action_params: arguments.clone(),
                    thought: Some(turn.text.clone()).filter(|t| !t.trim().is_empty()),
                    duration_ms: started.elapsed().as_millis() as u64,
                    success: ok,
                    tool_calls: vec![name.clone()],
                });
                index += 1;
            }
        }
        Ok((text, index))
    }
}

#[async_trait]
impl AgentScaffold for AgentFlow {
    fn name(&self) -> &str {
        "agentflow"
    }

    async fn solve(&self, case: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        let options = chat_options(ctx);
        let task = task_text(case);
        let mut trace = Vec::new();
        let mut tokens = Usage::default();
        let mut memory: Vec<String> = Vec::new();
        let mut budget_left = ctx.budget.max_steps;
        let mut index = 0usize;

        // 规划。
        let plan_turn = take_turn(
            ctx,
            &[Message::system(PLANNER_PROMPT), Message::user(task.clone())],
            &options,
        )
        .await?;
        accumulate(&mut tokens, &plan_turn.usage);
        let plan = parse_plan(&plan_turn.text);
        trace.push(StepRecord {
            step_index: index,
            action_type: "plan".into(),
            action_params: serde_json::json!({"steps": plan.clone()}),
            thought: Some(plan_turn.text.clone()),
            duration_ms: 0,
            success: !plan.is_empty(),
            tool_calls: vec![],
        });
        index += 1;

        for step in &plan {
            if budget_left == 0 {
                break;
            }
            let mut current = step.clone();
            let mut passed = false;
            let mut attempts = 0usize;
            loop {
                budget_left = budget_left.saturating_sub(1);
                let mut messages = vec![
                    Message::system(EXECUTOR_PROMPT),
                    Message::user(format!(
                        "Task:\n{task}\n\nPlan:\n{}\n\nSo far:\n{}\n\nDo this step:\n{current}",
                        plan.join("\n"),
                        if memory.is_empty() {
                            "(nothing yet)".to_string()
                        } else {
                            memory.join("\n")
                        }
                    )),
                ];
                let (result, next_index) = self
                    .run_step(ctx, &mut messages, &options, &mut trace, &mut tokens, index)
                    .await?;
                index = next_index;

                let verdict_turn = take_turn(
                    ctx,
                    &[
                        Message::system(VERIFIER_PROMPT),
                        Message::user(format!(
                            "Task:\n{task}\n\nStep:\n{current}\n\nResult:\n{result}"
                        )),
                    ],
                    &options,
                )
                .await?;
                accumulate(&mut tokens, &verdict_turn.usage);
                let (ok, note) = parse_verdict(&verdict_turn.text);
                trace.push(StepRecord {
                    step_index: index,
                    action_type: "verify".into(),
                    action_params: serde_json::json!({"step": current, "verdict": note}),
                    thought: Some(verdict_turn.text.clone()),
                    duration_ms: 0,
                    success: ok,
                    tool_calls: vec![],
                });
                index += 1;

                if ok {
                    memory.push(format!("step `{current}`: {result}"));
                    passed = true;
                    break;
                }
                attempts += 1;
                if attempts > self.max_regenerations || budget_left == 0 {
                    memory.push(format!("step `{current}`: failed, last result: {result}"));
                    break;
                }
                let fix_turn = take_turn(
                    ctx,
                    &[
                        Message::system(GENERATOR_PROMPT),
                        Message::user(format!(
                            "The step below did not pass:\n{current}\n\nResult:\n{result}\n\nWhy:\n{note}\n\nReply with a corrected step."
                        )),
                    ],
                    &options,
                )
                .await?;
                accumulate(&mut tokens, &fix_turn.usage);
                trace.push(StepRecord {
                    step_index: index,
                    action_type: "regenerate".into(),
                    action_params: serde_json::json!({"from": current}),
                    thought: Some(fix_turn.text.clone()),
                    duration_ms: 0,
                    success: true,
                    tool_calls: vec![],
                });
                index += 1;
                if fix_turn.text.trim().is_empty() {
                    // 重生成没给出东西：再跑一遍同一个步骤就是空转。
                    memory.push(format!("step `{current}`: failed, no corrected step"));
                    break;
                }
                current = fix_turn.text;
            }
            let _ = passed;
        }

        // 交付：生成器把计划、记忆和判词收成最终答案。
        let final_turn = take_turn(
            ctx,
            &[
                Message::system(GENERATOR_PROMPT),
                Message::user(format!(
                    "Task:\n{task}\n\nPlan:\n{}\n\nStep results:\n{}",
                    plan.join("\n"),
                    memory.join("\n")
                )),
            ],
            &options,
        )
        .await?;
        accumulate(&mut tokens, &final_turn.usage);

        let finish = if budget_left == 0 {
            FinishReason::StepBudget
        } else {
            FinishReason::Answered
        };
        Ok(AgentOutput {
            final_answer: final_turn.text,
            trace,
            tokens,
            finish,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{Budget, NoEnv};
    use crate::scaffolds::test_support::{code_tool, ScriptedLlm, ToolRecorder};
    use std::sync::Arc;

    fn case() -> EvalCase {
        EvalCase {
            id: "c1".into(),
            name: "c1".into(),
            input: serde_json::json!("add 2 and 3"),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    fn ctx_of(llm: Arc<ScriptedLlm>, recorder: &ToolRecorder) -> SolveContext {
        SolveContext {
            llm,
            tools: Arc::new(code_tool(recorder)),
            env: Arc::new(NoEnv),
            budget: Budget::default(),
            seed: 1,
        }
    }

    #[test]
    fn a_plan_is_read_by_its_numbering_and_an_unreadable_one_is_a_single_step() {
        assert_eq!(
            parse_plan("1. do a\n2. do b"),
            vec!["do a".to_string(), "do b".to_string()]
        );
        // 读不出编号不能变成空计划：空计划一步不走，交回去的是空答案。
        assert_eq!(parse_plan("just do it"), vec!["just do it".to_string()]);
        assert!(parse_plan("   ").is_empty());
    }

    #[test]
    fn an_unreadable_verdict_is_not_a_pass() {
        assert!(parse_verdict("VERDICT: PASS all good").0);
        assert!(!parse_verdict("VERDICT: FAIL missing the unit").0);
        // 说不出过没过，就不是过：把它当过的形状是「校验器坏了、分反而更高」。
        assert!(!parse_verdict("hmm, looks fine?").0);
    }

    #[tokio::test]
    async fn a_passing_step_lands_in_memory_and_the_generator_delivers() {
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::text("1. add them"),
            ScriptedLlm::tool_call("python", serde_json::json!({"code": "2+3"})),
            ScriptedLlm::text("the sum is 5"),
            ScriptedLlm::text("VERDICT: PASS correct"),
            ScriptedLlm::text("5"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), &recorder);

        let out = AgentFlow::new(2).solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.final_answer, "5");
        assert_eq!(out.finish, FinishReason::Answered);
        assert_eq!(recorder.count(), 1);
        // trace 里有规划、执行（工具）、校验三种动作。
        let kinds: Vec<&str> = out.trace.iter().map(|s| s.action_type.as_str()).collect();
        assert_eq!(kinds, vec!["plan", "python", "verify"]);
        // 交付时记忆里带着那一步的结果。
        let requests = llm.requests();
        let final_request = crate::scaffolds::test_support::last_text(requests.last().unwrap());
        assert!(final_request.contains("the sum is 5"), "{final_request}");
    }

    #[tokio::test]
    async fn a_failed_step_is_regenerated_and_the_next_verdict_decides() {
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::text("1. answer it"),
            ScriptedLlm::text("I am not sure"),
            ScriptedLlm::text("VERDICT: FAIL no answer given"),
            ScriptedLlm::text("Compute it with the tool."),
            ScriptedLlm::text("it is 5"),
            ScriptedLlm::text("VERDICT: PASS now correct"),
            ScriptedLlm::text("5"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), &recorder);

        let out = AgentFlow::new(2).solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.final_answer, "5");
        let kinds: Vec<&str> = out.trace.iter().map(|s| s.action_type.as_str()).collect();
        assert_eq!(kinds, vec!["plan", "verify", "regenerate", "verify"]);
        assert!(!out.trace[1].success, "第一次校验没过");
        assert!(out.trace[3].success, "重生成之后过了");
    }

    #[tokio::test]
    async fn a_step_that_never_passes_stops_after_the_regeneration_budget() {
        // 规划 → 执行 → 校验失败 → 重生成 → 再执行 → 再失败 → 预算用尽 → 交付。
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::text("1. impossible"),
            ScriptedLlm::text("nope"),
            ScriptedLlm::text("VERDICT: FAIL"),
            ScriptedLlm::text("try again"),
            ScriptedLlm::text("nope"),
            ScriptedLlm::text("VERDICT: FAIL"),
            ScriptedLlm::text("final answer anyway"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm, &recorder);

        let out = AgentFlow::new(1).solve(&case(), &ctx).await.unwrap();

        // 重生成预算用尽就往前走，不卡死：整题永远 0 分比带着瑕疵交卷更差。
        assert_eq!(out.final_answer, "final answer anyway");
        let regenerations = out
            .trace
            .iter()
            .filter(|s| s.action_type == "regenerate")
            .count();
        assert_eq!(regenerations, 1);
    }
}
