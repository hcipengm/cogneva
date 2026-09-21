//! 只跑 Planner 的分解路径：交付物形状与它的判据。
//!
//! 分解任务的交付物是原子任务列表，判据是纯结构的——有没有拿到任务。这条路径
//! 上生成器根本不参与：让生成器写一份没人读的产物、再让评估器给那份产物打分，
//! 等于拿"被丢弃产物的分数"决定"任务列表的成败"，两者从来不是同一个东西。
//! 于是这里既没有 PGE 拓扑，也没有评估器调用，只有一轮轮向 Planner 要任务列表，
//! 直到拿到、或按 Ralph 的预算与停滞判据止损。
//!
//! 形状与判据定义在产出方（本 crate 的 Ralph Loop）与消费方（本 crate 的 Squad
//! 结果提取器）之间共用的一份里：两处各自拼一个 JSON、各自解一个 JSON，改动时
//! 必然分叉，而分叉的表现是"任务列表解析不出来"这种静默的空结果。

use super::pge::types::{EvaluationResult, PlannerOutput, Verdict};

/// 分解交付物缺失时的病因名。
///
/// 与"生成器交了空信封"同属一类：请求到了上游、模型答了、可交付物是空的。
/// 因此同样不是终止性环境失败——没有任何观测到的证据排除下一轮成功，把它记成
/// 环境故障等于按一个从没看见过的事实结账，还会把读的人引到传输层去。名字点明
/// 是分解这一环，免得读的人去生成器身上找一个从未运行过的角色。
pub const EMPTY_DECOMPOSITION_PREFIX: &str = "empty_decomposition";

/// 分解拿到空交付物时的原因。
pub fn empty_decomposition_reason() -> String {
    format!("{EMPTY_DECOMPOSITION_PREFIX}: the planner returned no atomic sub-tasks")
}

/// 只跑 Planner 的一次运行产物：计划，以及对这份计划确定性的判定。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct PlanRunResult {
    pub plan: PlannerOutput,
    pub evaluation: EvaluationResult,
}

/// 判定一轮分解。
///
/// 交付物的契约本身就是结构性的——下游按 id 建图、按字段解析——所以判据必须是
/// 纯结构函数，不能交给一次 LLM 打分：打分器会把"能不能被执行"变成一次主观
/// 评价，而且它评的那份东西未必就是被消费的那份。这里只判"有没有任务"：id 唯一
/// 性与依赖可达性由拿到类型化任务的那一侧统一判，不在这里做第二份。
///
/// 分数是这条二分判定的 0/100 编码，保持与另外两种模式同形，好让共用同一套
/// 停滞判据的读法不变；它不是质量评分，别在别处当质量读。
pub fn judge_plan(plan: &PlannerOutput) -> EvaluationResult {
    if plan.sub_tasks.is_empty() {
        let reason = empty_decomposition_reason();
        return EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            criteria: vec![super::pge::types::Criterion {
                name: "atomic_sub_tasks_present".into(),
                score: 0,
                comment: reason.clone(),
            }],
            feedback: reason,
            details: None,
        };
    }
    let count = plan.sub_tasks.len();
    EvaluationResult {
        verdict: Verdict::Pass,
        score: Some(100),
        criteria: vec![super::pge::types::Criterion {
            name: "atomic_sub_tasks_present".into(),
            score: 100,
            comment: format!("{count} atomic sub-tasks"),
        }],
        feedback: format!("the decomposition produced {count} atomic sub-tasks"),
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::pge::types::TaskSpec;
    use super::*;

    fn plan_with(sub_tasks: Vec<TaskSpec>) -> PlannerOutput {
        PlannerOutput {
            summary: "s".into(),
            plan: serde_json::json!({}),
            sub_tasks,
            acceptance_criteria: Vec::new(),
        }
    }

    fn spec(id: &str) -> TaskSpec {
        TaskSpec {
            id: id.into(),
            name: format!("task {id}"),
            task_type: "generate".into(),
            input: serde_json::json!({ "query": "do it" }),
            blocked_by: Vec::new(),
        }
    }

    #[test]
    fn a_plan_with_tasks_passes() {
        let evaluation = judge_plan(&plan_with(vec![spec("t1"), spec("t2")]));
        assert_eq!(evaluation.verdict, Verdict::Pass);
        assert!(evaluation.feedback.contains('2'), "{}", evaluation.feedback);
    }

    /// 空交付物是**失败**，但绝不是终止性失败：没有观测到的证据排除下一轮
    /// 成功，记成环境故障会让重试预算按一个从没看见过的事实结账。
    #[test]
    fn an_empty_decomposition_fails_without_claiming_a_terminal_cause() {
        let evaluation = judge_plan(&plan_with(Vec::new()));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert_eq!(evaluation.score, Some(0));
        assert!(
            !cog_core::contract::outcome::is_deterministic_failure(&evaluation.feedback),
            "an empty deliverable names no observed cause, so retries must stay open: {}",
            evaluation.feedback
        );
        assert!(evaluation.feedback.contains(EMPTY_DECOMPOSITION_PREFIX));
    }

    /// 病因名点的是分解这一环，不是生成器：这条路径上根本没有生成器，
    /// 用生成器的名字会把读的人引到一个从未运行过的角色身上。
    #[test]
    fn the_empty_deliverable_names_the_decomposition_not_the_generator() {
        assert!(!empty_decomposition_reason()
            .contains(cog_core::contract::outcome::EMPTY_GENERATION_PREFIX));
    }
}
