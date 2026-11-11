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

/// 计划的依赖表里出现了一个不在这份计划里的 id。
pub const PLAN_DANGLING_DEPENDENCY: &str = "plan_dangling_dependency";
/// 两个子任务用了同一个 id。
pub const PLAN_DUPLICATE_ID: &str = "plan_duplicate_id";
/// 收口的片数不是恰好一片。
pub const PLAN_SINK_COUNT: &str = "plan_sink_count";
/// 有一片说不出怎样算做完。
pub const PLAN_NO_ACCEPTANCE: &str = "plan_no_acceptance";

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

/// 计划这一种产物身上的一处结构缺陷：哪一片、哪一类、差在哪。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanDefect {
    /// 直接被点名的那一片；整份计划的缺陷（例如空交付物）用空串。
    pub task_id: String,
    /// 稳定标识，读面按它取行。改它等于改判据的身份。
    pub code: &'static str,
    /// 具体差在哪。判词要能让生产者照着改，不能只说"不合格"。
    pub detail: String,
}

/// 判一份计划自己声明的结构。
///
/// 判的是**计划这一种产物**能声明的东西：一片交不交得出验收标准、依赖表指不指得到
/// 本份计划里的片、收口是不是恰好一片、id 有没有重。这些全部可由 [`PlannerOutput`]
/// 单独判出来，不需要先把它变成任务图。
///
/// 为什么不留给下游那一道门去判：下游拿到的是入图的**任务批次**，判的是那一批的
/// 父子关系与验收，判不过就整份打回，而判词只进日志——生产者再也没有机会改。
/// 下游那道门同时在替另外两条不经这里的路径（PGE 流水线、圆桌）兜底，所以它必须
/// 留着；但它不是这份计划的**判据所有者**。所有者在这里：判一次、判词原样回到
/// 下一轮的提示里，计划在花掉一次完整重拆之前就有机会把自己补全。
pub fn check_plan(plan: &PlannerOutput) -> Vec<PlanDefect> {
    if plan.sub_tasks.is_empty() {
        return vec![PlanDefect {
            task_id: String::new(),
            code: EMPTY_DECOMPOSITION_PREFIX,
            detail: "the planner returned no atomic sub-tasks".to_string(),
        }];
    }

    let mut defects = Vec::new();
    let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for spec in &plan.sub_tasks {
        if !seen.insert(spec.id.as_str()) {
            defects.push(PlanDefect {
                task_id: spec.id.clone(),
                code: PLAN_DUPLICATE_ID,
                detail: format!("id `{}` 在这份计划里出现了不止一次", spec.id),
            });
        }
    }

    for spec in &plan.sub_tasks {
        for dep in &spec.blocked_by {
            if !seen.contains(dep.as_str()) {
                defects.push(PlanDefect {
                    task_id: spec.id.clone(),
                    code: PLAN_DANGLING_DEPENDENCY,
                    detail: format!("blocked_by 里的 `{dep}` 不在这份计划里"),
                });
            }
        }
    }

    // 收口：没有任何一片把"等某片"写进 blocked_by 的那一片。计划必须恰好给出一片，
    // 因为下游要拿它承载整道目标的答案；零片是各片从不收口，多片是没哪一片算答案。
    let waited_on: std::collections::BTreeSet<&str> = plan
        .sub_tasks
        .iter()
        .flat_map(|s| s.blocked_by.iter().map(String::as_str))
        .collect();
    let sinks: Vec<&str> = plan
        .sub_tasks
        .iter()
        .map(|s| s.id.as_str())
        .filter(|id| !waited_on.contains(id))
        .collect();
    if sinks.len() != 1 {
        defects.push(PlanDefect {
            task_id: String::new(),
            code: PLAN_SINK_COUNT,
            detail: format!(
                "这份计划有 {} 片收口（{}）；必须恰好一片——分支的活要由一片把它们合起来",
                sinks.len(),
                if sinks.is_empty() {
                    "没有一片".to_string()
                } else {
                    sinks.join(", ")
                }
            ),
        });
    }

    // 每一片都要说得出怎样算做完。下游把每一片都当工作包判，没有任何一片能靠
    // "有人挂在我下面"免判——计划这一侧根本没有父指针可声明。
    for spec in &plan.sub_tasks {
        if !spec.acceptance.iter().any(|a| !a.trim().is_empty()) {
            defects.push(PlanDefect {
                task_id: spec.id.clone(),
                code: PLAN_NO_ACCEPTANCE,
                detail: "这一片没有验收标准：acceptance 是空的，做完与没做完没有判据".to_string(),
            });
        }
    }

    defects
}

/// 判定一轮分解。
///
/// 交付物的契约本身就是结构性的——下游按 id 建图、按字段解析——所以判据必须是
/// 纯结构函数，不能交给一次 LLM 打分：打分器会把"能不能被执行"变成一次主观
/// 评价，而且它评的那份东西未必就是被消费的那份。
///
/// 分数是这条二分判定的 0/100 编码，保持与另外两种模式同形，好让共用同一套
/// 停滞判据的读法不变；它不是质量评分，别在别处当质量读。
pub fn judge_plan(plan: &PlannerOutput) -> EvaluationResult {
    let defects = check_plan(plan);
    let count = plan.sub_tasks.len();
    if defects.is_empty() {
        return EvaluationResult {
            verdict: Verdict::Pass,
            score: Some(100),
            criteria: vec![super::pge::types::Criterion {
                name: "atomic_sub_tasks_present".into(),
                score: 100,
                comment: format!("{count} atomic sub-tasks"),
            }],
            feedback: format!("the decomposition produced {count} atomic sub-tasks"),
            details: None,
        };
    }

    let reason = defects
        .iter()
        .map(|d| {
            if d.task_id.is_empty() {
                format!("{}: {}", d.code, d.detail)
            } else {
                format!("{} `{}`: {}", d.code, d.task_id, d.detail)
            }
        })
        .collect::<Vec<_>>()
        .join("; ");
    EvaluationResult {
        verdict: Verdict::Fail,
        score: Some(0),
        criteria: defects
            .iter()
            .map(|d| super::pge::types::Criterion {
                name: d.code.to_string(),
                score: 0,
                comment: if d.task_id.is_empty() {
                    d.detail.clone()
                } else {
                    format!("`{}`: {}", d.task_id, d.detail)
                },
            })
            .collect(),
        feedback: reason,
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
            targets: Vec::new(),
        }
    }

    fn spec(id: &str) -> TaskSpec {
        TaskSpec {
            id: id.into(),
            name: format!("task {id}"),
            task_type: "generate".into(),
            input: serde_json::json!({ "query": "do it" }),
            blocked_by: Vec::new(),
            acceptance: Vec::new(),
            delivers: None,
        }
    }

    /// 一片说得出怎样算做完的活。
    fn done(id: &str) -> TaskSpec {
        TaskSpec {
            acceptance: vec![format!("{id} 的产出能逐条对照成真或假")],
            ..spec(id)
        }
    }

    /// 一片等另一片的活。
    fn after(id: &str, deps: &[&str]) -> TaskSpec {
        TaskSpec {
            blocked_by: deps.iter().map(|d| d.to_string()).collect(),
            ..done(id)
        }
    }

    #[test]
    fn a_plan_whose_leaves_say_how_they_are_done_passes() {
        let evaluation = judge_plan(&plan_with(vec![done("t1"), after("t2", &["t1"])]));
        assert_eq!(evaluation.verdict, Verdict::Pass, "{}", evaluation.feedback);
        assert!(evaluation.feedback.contains('2'), "{}", evaluation.feedback);
    }

    /// 叶子说不出怎样算做完：判据在计划这一侧就要亮，且逐片点名。这道判据的
    /// 全部价值在于判词能原样回到下一轮提示里——留到入图时才判，生产者拿到的是
    /// 一次整份打回，判词只进日志。
    #[test]
    fn a_leaf_without_criteria_is_named_back_to_the_planner() {
        let evaluation = judge_plan(&plan_with(vec![spec("t1"), after("t2", &["t1"])]));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert!(
            evaluation.feedback.contains(PLAN_NO_ACCEPTANCE),
            "{}",
            evaluation.feedback
        );
        assert!(
            evaluation.feedback.contains("`t1`"),
            "{}",
            evaluation.feedback
        );
        assert!(
            !evaluation.feedback.contains("`t2`"),
            "t2 有验收标准，不该被点名：{}",
            evaluation.feedback
        );
    }

    /// 各片从不收口：下游没有哪一片能承载整道目标的答案。
    #[test]
    fn a_plan_that_never_converges_is_refused_and_names_its_ends() {
        let evaluation = judge_plan(&plan_with(vec![done("t1"), done("t2")]));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert!(
            evaluation.feedback.contains(PLAN_SINK_COUNT),
            "{}",
            evaluation.feedback
        );
        assert!(
            evaluation.feedback.contains("t1"),
            "{}",
            evaluation.feedback
        );
        assert!(
            evaluation.feedback.contains("t2"),
            "{}",
            evaluation.feedback
        );
    }

    /// 依赖表指到本份计划里没有的片：那一片永远等不到，整份图静默停滞。
    #[test]
    fn a_dependency_on_a_task_outside_the_plan_is_refused() {
        let evaluation = judge_plan(&plan_with(vec![done("t1"), after("t2", &["t9"])]));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert!(
            evaluation.feedback.contains(PLAN_DANGLING_DEPENDENCY),
            "{}",
            evaluation.feedback
        );
        assert!(
            evaluation.feedback.contains("t9"),
            "{}",
            evaluation.feedback
        );
    }

    /// 两片同名：入图按 id 建行，后一片会把前一片的路由顶掉。
    #[test]
    fn two_sub_tasks_under_one_id_are_refused() {
        let evaluation = judge_plan(&plan_with(vec![done("t1"), after("t1", &["t1"])]));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert!(
            evaluation.feedback.contains(PLAN_DUPLICATE_ID),
            "{}",
            evaluation.feedback
        );
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
