use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A skill definition in the skill registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct Skill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tools: Vec<String>,
    #[serde(default)]
    pub complexity_score: u32,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub blocks: Vec<String>,
}

/// Agent skill configuration loaded dynamically from JSON.
/// Defines the persona, tools, and runtime parameters for an agent role.
/// Aligns with "Everything is a skill" architecture principle.
/// **Note:** `system_prompt` is deprecated. LLM calls must use structured
/// JSON input via `cog_llm::execute_structured` instead of natural-language
/// prompts. The field is retained only for backward-compatible deserialisation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct SkillConfig {
    pub skill_id: String,
    pub name: String,
    #[serde(default)]
    pub system_prompt: String,
    pub tools: Vec<String>,
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u32,
    pub role_type: String,
}

fn default_max_iterations() -> u32 {
    10
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
    pub description: String,
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

#[cfg(test)]
mod tests {
    use super::*;

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
            description: "代码输出必须包含单元测试".into(),
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
}
