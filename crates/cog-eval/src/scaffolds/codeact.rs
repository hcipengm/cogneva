//! CodeAct：把动作空间统一成**可执行代码**。
//!
//! 每一步模型要么写一段代码（我们跑它、把结果喂回去），要么给最终答案。它不挑工具
//! ——挑工具是别的外壳的做法；这里只有「跑代码」这一种动作，所以它只需要基准提供的
//! 那个代码执行工具。

use std::time::Instant;

use async_trait::async_trait;
use cog_core::{Message, Usage};

use super::{
    accumulate, chat_options, first_code_block, json_text, task_text, tool_calls_of, CODE_ARG,
};
use crate::dataset::EvalCase;
use crate::metric::StepRecord;
use crate::scaffold::{AgentOutput, AgentScaffold, FinishReason, SolveContext, ToolSet};

const SYSTEM_PROMPT: &str = "\
You are an agent whose only action is writing code. On each turn, either reply with a \
single fenced code block to be executed, or reply with the final answer and no code block. \
The execution result comes back to you as the next message.";

pub struct CodeAct;

impl CodeAct {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CodeAct {
    fn default() -> Self {
        Self::new()
    }
}

/// 从基准给的这束工具里挑出跑代码的那个。
///
/// CodeAct 的动作空间：一个代码解释器。声明只此一处——`action_space` 与 `code_tool`
/// 都读它，免得「哪几件算解释器」有两份会互相不一致的说法。
const ACTION_SPACE: &[&str] = &["python", "bash"];

/// 没有代码工具就没有 CodeAct 的动作空间——这要**报错**，不能降级成「不带工具直接
/// 问模型」：那样跑出来的是一行别的行，却挂在 CodeAct 名下。
fn code_tool<'a>(tools: &ToolSet, space: &'a [&'static str]) -> Option<&'a str> {
    for candidate in space {
        if tools.definitions().iter().any(|d| d.name == *candidate) {
            return Some(*candidate);
        }
    }
    None
}

#[async_trait]
impl AgentScaffold for CodeAct {
    fn name(&self) -> &str {
        "codeact"
    }

    fn action_space(&self) -> &'static [&'static str] {
        ACTION_SPACE
    }

    async fn solve(&self, case: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        let tool = code_tool(&ctx.tools, ACTION_SPACE).ok_or_else(|| {
            anyhow::anyhow!(
                "this benchmark offers no code tool (`python` or `bash`): the action space of this \
                 scaffold is code, and answering without one would be a different row"
            )
        })?;

        let mut messages = vec![
            Message::system(SYSTEM_PROMPT),
            Message::user(task_text(case)),
        ];
        let mut trace: Vec<StepRecord> = Vec::new();
        let mut tokens = Usage::default();
        let options = chat_options(ctx);

        for step in 0..ctx.budget.max_steps {
            let response = ctx.llm.chat(&messages, &options).await?;
            accumulate(&mut tokens, &response.usage);
            let text = super::text_of(&response.content);
            let calls = tool_calls_of(&response.content);

            // 动作：模型自己点的代码工具调用优先；没有就找围栏代码块。
            let action: Option<(String, Option<String>)> = calls
                .iter()
                .find(|(_, name, _)| name == tool)
                .map(|(id, _, args)| {
                    let code = args
                        .get(CODE_ARG)
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| json_text(args));
                    (code, Some(id.clone()))
                })
                .or_else(|| first_code_block(&text).map(|code| (code, None)));

            let Some((code, call_id)) = action else {
                return Ok(AgentOutput {
                    final_answer: text,
                    trace,
                    tokens,
                    finish: FinishReason::Answered,
                });
            };

            let thought = if text.trim().is_empty() {
                None
            } else {
                Some(text.clone())
            };
            messages.push(Message::assistant(response.content.clone()));

            let started = Instant::now();
            let (observation, ok) = match ctx
                .tools
                .call(tool, serde_json::json!({ CODE_ARG: code }))
                .await
            {
                Ok(value) => (json_text(&value), true),
                Err(e) => (format!("tool error: {e}"), false),
            };
            trace.push(StepRecord {
                step_index: step,
                action_type: tool.to_string(),
                action_params: serde_json::json!({ CODE_ARG: code }),
                thought,
                duration_ms: started.elapsed().as_millis() as u64,
                success: ok,
                tool_calls: vec![tool.to_string()],
            });

            // 代码块不是一次工具调用，就没有可对应的调用 id：那种情况下把结果作为
            // 下一条用户消息喂回去，而不是伪造一个 id。
            messages.push(match &call_id {
                Some(id) => Message::tool_result_text(id, tool, observation),
                None => Message::user(format!("Execution result:\n{observation}")),
            });
        }

        // 撞上限：这一格没过，但**原因**是预算而不是答错，trace 与 finish 都记着。
        Ok(AgentOutput {
            final_answer: String::new(),
            trace,
            tokens,
            finish: FinishReason::StepBudget,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{Budget, NoEnv};
    use crate::scaffolds::test_support::{
        code_tool, last_text, ScriptedLlm, ToolRecorder, CANNED_STDOUT,
    };
    use cog_core::ContentBlock;
    use std::sync::Arc;

    fn case() -> EvalCase {
        EvalCase {
            id: "c1".into(),
            name: "c1".into(),
            input: serde_json::json!("what is 2+2?"),
            expected_output: Some(serde_json::json!("4")),
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    fn ctx_of(llm: Arc<ScriptedLlm>, tools: ToolSet) -> SolveContext {
        SolveContext {
            llm,
            tools: Arc::new(tools),
            env: Arc::new(NoEnv),
            budget: Budget::default(),
            seed: 3,
        }
    }

    #[tokio::test]
    async fn code_is_run_and_the_observation_goes_back_into_the_context() {
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::tool_call("python", serde_json::json!({"code": "print(2+2)"})),
            ScriptedLlm::text("4"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), code_tool(&recorder));

        let out = CodeAct::new().solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.final_answer, "4");
        assert_eq!(out.finish, FinishReason::Answered);
        assert_eq!(out.trace.len(), 1);
        assert_eq!(out.trace[0].action_type, "python");
        assert!(out.trace[0].success);
        assert_eq!(recorder.count(), 1, "工具必须真被调用一次");
        assert_eq!(recorder.calls()[0].1["code"], "print(2+2)");
        // 观察结果回到了上下文：第二次请求的最后一条消息里带着工具的固定输出。
        let second = last_text(&llm.requests()[1]);
        assert!(second.contains(CANNED_STDOUT), "{second}");
    }

    #[tokio::test]
    async fn a_fenced_block_is_also_an_action() {
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::text("think\n```python\nprint(1)\n```"),
            ScriptedLlm::text("done"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), code_tool(&recorder));

        let out = CodeAct::new().solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.final_answer, "done");
        assert_eq!(recorder.count(), 1);
        assert_eq!(recorder.calls()[0].1["code"], "print(1)");
        assert_eq!(
            out.trace[0].thought.as_deref(),
            Some("think\n```python\nprint(1)\n```")
        );
        // 没有工具调用就没有调用 id：结果作为下一条用户消息回去，而不是伪造一个 id。
        let second = last_text(&llm.requests()[1]);
        assert!(second.contains(CANNED_STDOUT), "{second}");
    }

    #[tokio::test]
    async fn hitting_the_step_budget_is_not_reported_as_an_answer() {
        let steps = (0..3)
            .map(|_| ScriptedLlm::tool_call("python", serde_json::json!({"code": "print(1)"})))
            .collect();
        let llm = Arc::new(ScriptedLlm::new(steps));
        let recorder = ToolRecorder::new();
        let mut ctx = ctx_of(llm, code_tool(&recorder));
        ctx.budget = Budget {
            max_steps: 3,
            ..Budget::default()
        };

        let out = CodeAct::new().solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.finish, FinishReason::StepBudget);
        assert_eq!(out.trace.len(), 3);
        assert!(out.final_answer.is_empty(), "没答出来就不能交一个答案回去");
    }

    #[tokio::test]
    async fn a_benchmark_without_a_code_tool_is_refused_not_downgraded() {
        let llm = Arc::new(ScriptedLlm::new(vec![ScriptedLlm::text("42")]));
        let recorder = ToolRecorder::new();
        // 只有搜索工具：CodeAct 的动作空间是代码，拿搜索凑合出来的是另一行。
        let ctx = ctx_of(llm, recorder.toolset(&["search"]));
        let err = CodeAct::new().solve(&case(), &ctx).await.unwrap_err();
        assert!(err.to_string().contains("no code tool"), "{err}");
    }

    #[test]
    fn the_action_space_is_the_interpreter_not_the_search_tool() {
        // 消融按角色切：CodeAct 的动作空间是代码解释器，不是别的方法也能用的搜索工具。
        assert_eq!(CodeAct::new().action_space(), &["python", "bash"][..]);
    }

    #[tokio::test]
    async fn the_assistant_turn_keeps_its_blocks_so_the_tool_result_has_a_call() {
        // 工具调用那一轮的 assistant 消息必须原样进历史：否则下一轮请求里会出现
        // 一条没有对应调用的 tool result，上游会直接拒。
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::tool_call("python", serde_json::json!({"code": "print(1)"})),
            ScriptedLlm::text("done"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), code_tool(&recorder));
        CodeAct::new().solve(&case(), &ctx).await.unwrap();

        let second = &llm.requests()[1];
        let paired = second.iter().any(|m| {
            matches!(m, Message::Assistant { content, .. }
            if content.iter().any(|b| matches!(b, ContentBlock::ToolCall { .. })))
        }) && second
            .iter()
            .any(|m| matches!(m, Message::ToolResult { .. }));
        assert!(paired, "助手那一轮的工具调用与结果必须成对：{second:?}");
    }
}
