//! 单测里反复出现的几件样例：一题、一步、一份结果、一件工具、一份空外壳输出。
//!
//! 它们不是任何一维的题目，是每个维度单测都要先摆好的东西。原先十三个维度文件与
//! `dimensions/mod.rs` 各写了一遍逐字相同的 `mk_case`，适配器那边三份 `tool` 也是
//! 同一件工具——一处改默认值（比如 `id` 从 `t1` 换成别的、工具的参数表从空对象换成
//! 有 schema 的）到不了另外几份，而每一份都还是绿的。
//!
//! 默认值本身就是判据的一部分：`duration_ms: 100` 与 `duration_ms: 0` 是两份不同的
//! 结果（前者是被算过时长的，后者是只带着输出的），所以这里不是「一个构造函数多个
//! 参数」，而是两份结果各有一个名字。

use std::sync::Arc;

use cog_core::{Tool, ToolImplementation};

use crate::dataset::EvalCase;
use crate::metric::{EvalResult, StepRecord};
use crate::scaffold::{AgentOutput, FinishReason};

/// 一题：只有 id 与名字，其余全空。
///
/// 维度算的是结果，不是题目；题目在这里只是 `compute` 要的那个占位。
pub(crate) fn mk_case() -> EvalCase {
    EvalCase {
        metadata: Default::default(),
        id: "t1".into(),
        name: "test".into(),
        input: serde_json::Value::Null,
        expected_output: None,
        expected_tools: None,
        metrics: vec![],
        tags: vec![],
    }
}

/// 一步：一次成功与否的动作，没有工具调用、没有思考、耗时固定。
pub(crate) fn mk_step(action_type: &str, success: bool) -> StepRecord {
    StepRecord {
        step_index: 0,
        action_type: action_type.into(),
        action_params: serde_json::Value::Null,
        thought: None,
        duration_ms: 10,
        success,
        tool_calls: vec![],
    }
}

/// 一份「被算过」的结果：100 ms、50 token、0.001 元，输出为空。
///
/// 时长与开销都不是零，所以按它们算分的维度读到的是一行真被算过的数，而不是
/// 「没有数据」和「零」同形的那个 0。
pub(crate) fn mk_result(steps: Vec<StepRecord>, passed: bool) -> EvalResult {
    EvalResult {
        case_id: "t1".into(),
        passed,
        metrics: vec![],
        agent_output: serde_json::Value::Null,
        duration_ms: 100,
        token_usage: 50,
        cost: 0.001,
        error: None,
        steps,
        trace_json: None,
    }
}

/// 一份只带输出的结果：判分看的就是那段输出，时长与开销是零。
pub(crate) fn mk_result_for_output(output: serde_json::Value) -> EvalResult {
    EvalResult {
        case_id: "t1".into(),
        passed: true,
        metrics: vec![],
        agent_output: output,
        duration_ms: 0,
        token_usage: 0,
        cost: 0.0,
        error: None,
        steps: vec![],
        trace_json: None,
    }
}

/// 一件「交得出去」的工具：实现什么都不做，参数表是一个空对象。
///
/// 适配器的单测判的是**交出去的那一束**（名字、件数、工厂按题造出来的那一束），
/// 不是工具干活的结果，所以实现是个恒返回 `null` 的空壳。
pub(crate) fn mk_tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: name.into(),
        parameters: serde_json::json!({"type": "object"}),
        implementation: ToolImplementation::Native(Arc::new(|_| {
            Box::pin(async move { Ok(serde_json::json!(null)) })
        })),
    }
}

/// 一份空的外壳输出：没有答案、没有过程、没有 token、正常停下。
pub(crate) fn mk_output() -> AgentOutput {
    AgentOutput {
        final_answer: String::new(),
        trace: vec![],
        tokens: cog_core::Usage::default(),
        finish: FinishReason::Answered,
    }
}
