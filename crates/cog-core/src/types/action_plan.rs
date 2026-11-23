use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One row of the executor table, as goal decomposition reads it.
///
/// This is the planner-facing mirror of [`SkillConfig`]: the same row, with the
/// id, the name and the kind it registers — enough for the planner to name the
/// kind of hand for a sub-task. It is not a statement of what the system can do.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct Skill {
    pub id: String,
    pub name: String,
    /// The kind of hand this row registers. For a row mirrored from a
    /// [`SkillConfig`] this is its `role_type`, and it is what the decomposition
    /// payload hands the planner; a bare kind such as `planner`, never a
    /// sentence about capabilities.
    pub description: String,
    pub tools: Vec<String>,
    #[serde(default)]
    pub complexity_score: u32,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub blocks: Vec<String>,
}

/// One row of the executor table, loaded dynamically from JSON.
///
/// A row answers one question: which kind of hand this is (`role_type`). Agents
/// are bound to a row by role (`skill_for_role`) so that a run's outcome can be
/// attributed to the hand that produced it, and goal decomposition is handed the
/// rows so it can name the kind of hand for each sub-task.
///
/// 这一行不回答「这一位能用哪些工具」。工具只有一套：手上有什么由**身体所在的
/// 进程**决定（有没有沙盒、有没有消息队列、有没有出门的客户端），不是由「它是
/// 哪种手」决定。按角色收窄时，同一个进程里两个人对同一张注册表看到两套工具，
/// 而谁被收窄取决于注册表上一个自由标签——换个标签，同一件活就做不成了。
///
/// 这一行也不带人格（`system_prompt`）。它是一个执行体的系统前缀：写明它是谁
/// 在这里是多余的，写错则是把一种流水线的口吻安在另一个人身上。那一轮做什么，
/// 由它拿到的那件活说。
///
/// 这一行更不写轮次上限。它曾经有一个 `max_iterations`：行按角色名取，于是同一
/// 件活换个标签接手就换一个天花板，而「这一轮还要不要接着干」是标签最不该决定
/// 的事。上限现在只有两个来源——操作者在配置面上钉的数（覆盖），和这个进程自己
/// 交卷运行跑出来的轮数（实测，见 `AgentObservable::iteration_budget_for`）。
/// 字段删掉而不是留着不读：一个按标签分发、却没人读的数，比没有它更难发现。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct SkillConfig {
    pub skill_id: String,
    pub name: String,
    /// The kind of hand this row registers — the dispatch key.
    ///
    /// Read in two places: [`crate::SkillRegistry::skill_for_role`] binds an
    /// agent's role to its row, and goal decomposition hands it to the planner as
    /// the row's `description`, which is how a sub-task ends up naming a kind of
    /// hand. It is a kind name (`planner`, `generator`, …), not a capability
    /// claim and not a permission: every row sees the same tool registry, the
    /// one its process installed.
    pub role_type: String,
}

/// An atomic task produced by goal decomposition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct AtomicTask {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub estimated_tokens: u64,
    #[serde(default)]
    pub skill_gap: bool,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub blocks: Vec<String>,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub output_entities: Vec<String>,
    #[serde(default)]
    pub estimated_seconds: u64,
    /// 这一片挂在分解里的哪一片下面。`None` 是顶层工作包。
    ///
    /// 与 `blocked_by` 是两件事：依赖说「谁等谁才能跑」，这里说「谁属于谁」。
    /// 一片可以依赖另一棵子树里的产物而不属于它。分解不带这个指针时，所有子任务
    /// 都挂在目标行下面（一层），带了才是真正的多层 WBS。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// 工作包的验收标准。
    ///
    /// 叶子节点没有它，这一片做完了没有就没有判据——分解门会因此把整份计划打回。
    /// 非叶子（有人挂在它下面）不要求填：它的完成是由子节点汇上来的。
    #[serde(default)]
    pub acceptance: Vec<String>,
    /// 这一片交什么：闭集里的一个词，由分解时读这件活得出。
    ///
    /// 不从 `skill_id` 反推。那个字段说的是「派给哪个座位」，是同一次分解的另一个
    /// 回答；两者一旦互为代理，同一件活换个座位接手就在读数里换了交付物。分解说不出
    /// 来时留空——入口落定的是一份「没人说」，不是一个默认形状。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivers: Option<String>,
}

/// The complete action plan produced by Stage 1 decomposition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, schemars::JsonSchema)]
pub struct ActionPlan {
    pub goal: String,
    pub tasks: Vec<AtomicTask>,
    pub skills: Vec<Skill>,
    #[serde(default)]
    pub edges: Vec<(String, String)>,
}

/// A historical decomposition pattern stored for few-shot retrieval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct DecompositionPattern {
    pub goal_summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_embedding: Option<Vec<f32>>,
    pub decomposition_tree: serde_json::Value,
    #[serde(default)]
    pub skill_set: Vec<String>,
    #[serde(default)]
    pub success_score: f32,
}

/// Rule type: Hard = Rust code has concrete checking logic; Soft = pure prompt-level, LLM judges.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuleType {
    Hard,
    Soft,
}

/// A single boundary rule definition (config-driven, supports dynamic dimensions).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct BoundaryRule {
    pub name: String,
    pub rule_type: RuleType,
    #[serde(default)]
    pub threshold: Option<u64>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Legacy enum for built-in dimensions (kept for backwards compatibility).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryDimension {
    TokenBudget,
    SkillBoundary,
    TimeBoundary,
    StateBoundary,
    DataBoundary,
}

/// A single boundary violation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct BoundaryViolation {
    pub dimension: String,
    pub task_id: String,
    pub message: String,
}

/// Boundary detection report for an action plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct BoundaryReport {
    pub violations: Vec<BoundaryViolation>,
    #[serde(default)]
    pub suggestions: Vec<String>,
    pub passed: bool,
}

impl Default for BoundaryReport {
    fn default() -> Self {
        Self {
            violations: Vec::new(),
            suggestions: Vec::new(),
            passed: true,
        }
    }
}

/// Errors that can occur during DAG validation.
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error, schemars::JsonSchema,
)]
pub enum DagError {
    #[error("cyclic dependency detected")]
    CyclicDependency,
    #[error("no entry node: all tasks have incoming dependencies")]
    NoEntryNode,
    #[error("no exit node: all tasks have outgoing dependencies")]
    NoExitNode,
    #[error("unknown dependency: {0}")]
    UnknownDependency(String),
    #[error("critical path too long: {0} nodes")]
    CriticalPathTooLong(usize),
    #[error("DAG has {0} disconnected components")]
    DisconnectedComponents(usize),
}

/// Result of a successful DAG validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, schemars::JsonSchema)]
pub struct DagValidation {
    pub entry_nodes: Vec<String>,
    pub exit_nodes: Vec<String>,
    pub critical_path_len: usize,
    pub components: usize,
}

/// Validate a DAG against the 6 rules defined in the architecture spec.
pub fn validate_dag(
    tasks: &[AtomicTask],
    edges: &[(String, String)],
) -> Result<DagValidation, DagError> {
    let task_ids: std::collections::HashSet<String> = tasks.iter().map(|t| t.id.clone()).collect();

    // Rule 5: dependency existence check.
    for (from, to) in edges {
        if !task_ids.contains(from) {
            return Err(DagError::UnknownDependency(from.clone()));
        }
        if !task_ids.contains(to) {
            return Err(DagError::UnknownDependency(to.clone()));
        }
    }

    // Build adjacency list.
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for t in tasks {
        adj.entry(t.id.clone()).or_default();
        in_degree.entry(t.id.clone()).or_insert(0);
    }
    for (from, to) in edges {
        adj.entry(from.clone()).or_default().push(to.clone());
        *in_degree.entry(to.clone()).or_insert(0) += 1;
    }

    // Rule 1: cycle detection (DFS coloring).
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }
    let mut colors: HashMap<String, Color> = task_ids
        .iter()
        .map(|id| (id.clone(), Color::White))
        .collect();

    fn dfs(
        node: &str,
        adj: &HashMap<String, Vec<String>>,
        colors: &mut HashMap<String, Color>,
    ) -> bool {
        colors.insert(node.to_string(), Color::Gray);
        if let Some(neighbors) = adj.get(node) {
            for neighbor in neighbors {
                match colors.get(neighbor).copied().unwrap_or(Color::White) {
                    Color::Gray => return true,
                    Color::White => {
                        if dfs(neighbor, adj, colors) {
                            return true;
                        }
                    }
                    Color::Black => {}
                }
            }
        }
        colors.insert(node.to_string(), Color::Black);
        false
    }

    for id in &task_ids {
        if colors.get(id).copied().unwrap_or(Color::White) == Color::White
            && dfs(id, &adj, &mut colors)
        {
            return Err(DagError::CyclicDependency);
        }
    }

    // Rule 3: entry nodes (zero in-degree).
    let entry_nodes: Vec<String> = task_ids
        .iter()
        .filter(|id| *in_degree.get(*id).unwrap_or(&0) == 0)
        .cloned()
        .collect();
    if entry_nodes.is_empty() && !tasks.is_empty() {
        return Err(DagError::NoEntryNode);
    }

    // Rule 4: exit nodes (zero out-degree).
    let exit_nodes: Vec<String> = task_ids
        .iter()
        .filter(|id| adj.get(*id).map(|v| v.is_empty()).unwrap_or(true))
        .cloned()
        .collect();
    if exit_nodes.is_empty() && !tasks.is_empty() {
        return Err(DagError::NoExitNode);
    }

    // Rule 2 & 6: weakly connected components and critical path.
    let mut undirected: HashMap<String, Vec<String>> = HashMap::new();
    for t in tasks {
        undirected.entry(t.id.clone()).or_default();
    }
    for (from, to) in edges {
        undirected.entry(from.clone()).or_default().push(to.clone());
        undirected.entry(to.clone()).or_default().push(from.clone());
    }

    let mut visited = std::collections::HashSet::new();
    let mut components = 0;
    for id in &task_ids {
        if visited.insert(id.clone()) {
            components += 1;
            let mut stack = vec![id.clone()];
            while let Some(cur) = stack.pop() {
                if let Some(neighbors) = undirected.get(&cur) {
                    for n in neighbors {
                        if visited.insert(n.clone()) {
                            stack.push(n.clone());
                        }
                    }
                }
            }
        }
    }

    // Longest path in DAG (critical path length).
    let mut longest: HashMap<String, usize> = HashMap::new();
    fn longest_from(
        node: &str,
        adj: &HashMap<String, Vec<String>>,
        memo: &mut HashMap<String, usize>,
    ) -> usize {
        if let Some(&cached) = memo.get(node) {
            return cached;
        }
        let mut max_child = 0;
        if let Some(neighbors) = adj.get(node) {
            for neighbor in neighbors {
                max_child = max_child.max(longest_from(neighbor, adj, memo));
            }
        }
        let result = 1 + max_child;
        memo.insert(node.to_string(), result);
        result
    }

    let mut critical_path_len = 0;
    for id in &task_ids {
        critical_path_len = critical_path_len.max(longest_from(id, &adj, &mut longest));
    }

    if critical_path_len > 20 {
        return Err(DagError::CriticalPathTooLong(critical_path_len));
    }

    Ok(DagValidation {
        entry_nodes,
        exit_nodes,
        critical_path_len,
        components,
    })
}

/// 一份计划对「这一类」说了什么。
///
/// planner 的产出不是步骤，而是三样东西：这次处理的东西是什么（`input_type`）、
/// 对它这一类**任意实例**都成立的话（`invariants`）、以及这些性质怎么被量出来
/// （`acceptance`）。执行者只允许在它们里面实现；把性质缩窄成这次遇到的样例，
/// 就是「没有计划的实现」。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, schemars::JsonSchema)]
pub struct Plan {
    pub input_type: String,
    pub invariants: Vec<String>,
    pub acceptance: Vec<PlanAcceptance>,
    /// 什么情况下这条性质应当被拒。没有反例的判据，实现里会退化成恒真。
    pub counterexamples: Vec<String>,
    /// 确实只有一种形状时的说明（例如「删掉一个已废弃的函数」）。
    ///
    /// 它是**说明**，不是省略的默认值：写了它才允许只有一种形状，而且它要能回答
    /// 「为什么第二种来源形状不存在」。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub one_shape_because: Option<String>,
}

/// 一条验收：说的是哪种来源形状、怎么量。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct PlanAcceptance {
    /// 这条验收是在哪种输入来源上量的（换一本台账、换一个 worker、换一种格式）。
    ///
    /// 两个几乎一样的样例不算两种形状——形状的区分是**来源类型**的区分。
    pub shape: String,
    pub statement: String,
    /// 从代码树或现网读得出的量法（命令或路径）。人工确认不算。
    pub check: String,
}

/// 计划不成立的原因。每一条都点名是哪一处分不合规。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanDefect {
    /// 一条不变量都没有：这份计划没有说「这一类」是什么。
    NoInvariants,
    /// 一条验收都没有：没有任何东西能在事后说它做对了。
    NoAcceptance,
    /// 只有一种来源形状，又没有说明为什么第二种不存在。
    OneShape(String),
    /// 同一来源形状上的两条验收：那是同一把尺子量两次。
    RepeatedShape(String),
    /// 验收的量法不是可执行的（空串）。
    Unmeasurable(String),
}

impl Plan {
    /// 这份计划能不能进 generator。
    ///
    /// 判据就是设计里那三条：性质非空、验收至少两种**不同来源形状**（或写明为什么
    /// 只有一种）、每条验收都带一条可执行的量法。
    pub fn validate(&self) -> Result<(), PlanDefect> {
        if self.invariants.is_empty() {
            return Err(PlanDefect::NoInvariants);
        }
        if self.acceptance.is_empty() {
            return Err(PlanDefect::NoAcceptance);
        }
        for a in &self.acceptance {
            if a.check.trim().is_empty() {
                return Err(PlanDefect::Unmeasurable(a.shape.clone()));
            }
        }
        let mut shapes: Vec<&str> = self.acceptance.iter().map(|a| a.shape.as_str()).collect();
        shapes.sort_unstable();
        shapes.dedup();
        if shapes.len() == 1 && self.one_shape_because.is_none() {
            return Err(PlanDefect::OneShape(shapes[0].to_string()));
        }
        if shapes.len() != self.acceptance.len() {
            let mut seen = std::collections::BTreeSet::new();
            for a in &self.acceptance {
                if !seen.insert(a.shape.as_str()) {
                    return Err(PlanDefect::RepeatedShape(a.shape.clone()));
                }
            }
        }
        Ok(())
    }

    /// 这份计划点到了每一个给定的身份吗。
    ///
    /// 一条**批次**计划与一条单点计划的区别就在这里：批次的意义是「它们一起成立」，
    /// 所以漏掉一条就不是「计划写得粗」，而是**这一批少了那一件**——而少了的那件
    /// 在读数上与「做完了」同形。判据因此是逐条的，不是「点到了几条就算过」。
    ///
    /// 空清单返回真：没有要覆盖的活时，「每一条都点到了」是真的。
    pub fn names_every(&self, ids: &[String]) -> bool {
        ids.iter().all(|id| {
            self.invariants
                .iter()
                .any(|line| line.contains(id.as_str()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 批次计划漏一条就答否：漏掉的那件在读数上与「做完了」同形。
    #[test]
    fn a_batch_plan_that_misses_an_item_does_not_cover_it() {
        let plan = Plan {
            input_type: "ledger-batch".into(),
            invariants: vec!["S50：流式写入口".into(), "S67：批量入口".into()],
            acceptance: vec![
                PlanAcceptance {
                    shape: "a".into(),
                    statement: "x".into(),
                    check: "cmd".into(),
                },
                PlanAcceptance {
                    shape: "b".into(),
                    statement: "y".into(),
                    check: "cmd".into(),
                },
            ],
            counterexamples: vec![],
            one_shape_because: None,
        };
        assert!(plan.names_every(&["S50".to_string(), "S67".to_string()]));
        assert!(
            !plan.names_every(&["S50".to_string(), "S67".to_string(), "S65".to_string()]),
            "漏掉 S65 却答「覆盖了」——那一条会读作做完了"
        );
        assert!(
            plan.names_every(&[]),
            "没有要覆盖的活时，每一条都点到了是真的"
        );
    }

    fn make_task(id: &str, blocked_by: Vec<&str>, blocks: Vec<&str>) -> AtomicTask {
        AtomicTask {
            id: id.to_string(),
            name: id.to_string(),
            skill_id: None,
            description: None,
            estimated_tokens: 0,
            skill_gap: false,
            blocked_by: blocked_by.into_iter().map(String::from).collect(),
            blocks: blocks.into_iter().map(String::from).collect(),
            input: serde_json::Value::Null,
            output_entities: vec![],
            estimated_seconds: 0,
            parent_id: None,
            acceptance: vec![],
            delivers: None,
        }
    }

    #[test]
    fn test_dag_valid_linear() {
        let tasks = vec![
            make_task("a", vec![], vec!["b"]),
            make_task("b", vec!["a"], vec!["c"]),
            make_task("c", vec!["b"], vec![]),
        ];
        let edges = vec![("a".into(), "b".into()), ("b".into(), "c".into())];
        let result = validate_dag(&tasks, &edges).unwrap();
        assert_eq!(result.entry_nodes, vec!["a"]);
        assert_eq!(result.exit_nodes, vec!["c"]);
        assert_eq!(result.critical_path_len, 3);
        assert_eq!(result.components, 1);
    }

    #[test]
    fn test_dag_cycle_detected() {
        let tasks = vec![
            make_task("a", vec!["c"], vec!["b"]),
            make_task("b", vec!["a"], vec!["c"]),
            make_task("c", vec!["b"], vec!["a"]),
        ];
        let edges = vec![
            ("a".into(), "b".into()),
            ("b".into(), "c".into()),
            ("c".into(), "a".into()),
        ];
        let result = validate_dag(&tasks, &edges);
        assert!(matches!(result, Err(DagError::CyclicDependency)));
    }

    #[test]
    fn test_dag_unknown_dependency() {
        let tasks = vec![make_task("a", vec![], vec![])];
        let edges = vec![("a".into(), "b".into())];
        let result = validate_dag(&tasks, &edges);
        assert!(matches!(result, Err(DagError::UnknownDependency(_))));
    }

    #[test]
    fn test_dag_no_entry_node() {
        let tasks = vec![
            make_task("a", vec!["b"], vec!["b"]),
            make_task("b", vec!["a"], vec!["a"]),
        ];
        let edges = vec![("a".into(), "b".into()), ("b".into(), "a".into())];
        let result = validate_dag(&tasks, &edges);
        assert!(
            matches!(result, Err(DagError::CyclicDependency)),
            "cycle should be detected before no-entry"
        );
    }

    #[test]
    fn test_dag_critical_path_too_long() {
        let mut tasks = Vec::new();
        let mut edges = Vec::new();
        for i in 0..25 {
            tasks.push(make_task(&format!("t{}", i), vec![], vec![]));
            if i > 0 {
                edges.push((format!("t{}", i - 1), format!("t{}", i)));
            }
        }
        let result = validate_dag(&tasks, &edges);
        assert!(matches!(result, Err(DagError::CriticalPathTooLong(25))));
    }

    #[test]
    fn test_boundary_violation_creation() {
        let v = BoundaryViolation {
            dimension: "TokenBudget".into(),
            task_id: "task-1".into(),
            message: "estimated_tokens > 100k".into(),
        };
        assert_eq!(v.dimension, "TokenBudget");
        assert_eq!(v.task_id, "task-1");
    }

    #[test]
    fn test_boundary_rule_creation() {
        let rule = BoundaryRule {
            name: "OutputQuality".into(),
            rule_type: RuleType::Soft,
            threshold: None,
            enabled: true,
        };
        assert_eq!(rule.name, "OutputQuality");
        assert_eq!(rule.rule_type, RuleType::Soft);
        assert!(rule.enabled);
    }

    #[test]
    fn test_boundary_report_default() {
        let report = BoundaryReport::default();
        assert!(report.violations.is_empty());
        assert!(report.suggestions.is_empty());
        assert!(report.passed);
    }

    /// 两种不同来源形状各自有一条可量的验收：计划成立。
    #[test]
    fn two_source_shapes_make_a_plan() {
        let plan = Plan {
            input_type: "台账".into(),
            invariants: vec!["批次＝台账上所有未完成条目".into()],
            acceptance: vec![
                PlanAcceptance {
                    shape: "worker 的 markdown 台账".into(),
                    statement: "缺口被列出".into(),
                    check: "scripts/hooks/family.py --all-todos".into(),
                },
                PlanAcceptance {
                    shape: "平台的变更账本".into(),
                    statement: "pending 条目被逐条点名".into(),
                    check: "cogneva ledger gate".into(),
                },
            ],
            counterexamples: vec!["子集被当成默认批次".into()],
            one_shape_because: None,
        };
        assert_eq!(plan.validate(), Ok(()));
    }

    /// 只有一种形状又没有说明：那是「验了我自己举的例子」，计划不成立。
    #[test]
    fn one_shape_without_a_reason_is_refused() {
        let plan = Plan {
            input_type: "台账".into(),
            invariants: vec!["批次＝全部未完成".into()],
            acceptance: vec![PlanAcceptance {
                shape: "我的 markdown".into(),
                statement: "缺口被列出".into(),
                check: "scripts/hooks/family.py".into(),
            }],
            counterexamples: vec![],
            one_shape_because: None,
        };
        assert_eq!(
            plan.validate(),
            Err(PlanDefect::OneShape("我的 markdown".into()))
        );
    }

    /// 写了说明就允许只有一种形状——但那是说明，不是默认。
    #[test]
    fn one_shape_is_allowed_when_the_absence_is_explained() {
        let plan = Plan {
            input_type: "已废弃函数".into(),
            invariants: vec!["没有调用点".into()],
            acceptance: vec![PlanAcceptance {
                shape: "代码树".into(),
                statement: "零命中".into(),
                check: "grep -r fn_name crates/".into(),
            }],
            counterexamples: vec![],
            one_shape_because: Some("这一类的输入只有代码树一种来源".into()),
        };
        assert_eq!(plan.validate(), Ok(()));
    }

    /// 同一形状上量两次不是两种形状。
    #[test]
    fn two_checks_on_one_shape_are_not_two_shapes() {
        let plan = Plan {
            input_type: "台账".into(),
            invariants: vec!["批次＝全部未完成".into()],
            acceptance: vec![
                PlanAcceptance {
                    shape: "我的 markdown".into(),
                    statement: "a".into(),
                    check: "x".into(),
                },
                PlanAcceptance {
                    shape: "我的 markdown".into(),
                    statement: "b".into(),
                    check: "y".into(),
                },
            ],
            counterexamples: vec![],
            one_shape_because: None,
        };
        assert_eq!(
            plan.validate(),
            Err(PlanDefect::OneShape("我的 markdown".into())),
            "同一来源上的两条验收仍是同一把尺子"
        );
    }

    /// 没有量法的验收不算验收。
    #[test]
    fn an_acceptance_without_a_check_is_refused() {
        let plan = Plan {
            input_type: "台账".into(),
            invariants: vec!["批次＝全部未完成".into()],
            acceptance: vec![PlanAcceptance {
                shape: "我的 markdown".into(),
                statement: "人工确认".into(),
                check: "   ".into(),
            }],
            counterexamples: vec![],
            one_shape_because: Some("x".into()),
        };
        assert_eq!(
            plan.validate(),
            Err(PlanDefect::Unmeasurable("我的 markdown".into()))
        );
    }

    /// 一批的每一条都必须在计划里被点到名：漏一条，这份计划说的就不是「这一批」，
    /// 而份量不足的计划与一份完整的计划在别处长得一样。
    #[test]
    fn a_plan_must_name_every_item_of_its_batch() {
        let plan = Plan {
            input_type: "ledger-batch".into(),
            invariants: vec!["S1：把 A 做完".into(), "S2：再把 B 做完".into()],
            acceptance: Vec::new(),
            counterexamples: Vec::new(),
            one_shape_because: None,
        };
        assert!(plan.names_every(&["S1".into(), "S2".into()]));
        assert!(
            !plan.names_every(&["S1".into(), "S3".into()]),
            "计划没提到 S3 却被判成点齐了"
        );
        assert!(plan.names_every(&[]), "空批次是点齐了，不是漏了");
    }
}
