//! 因果推理层 — 从 Schema 实体/关系中提取因果模式。
//! 将知识图谱从"静态事实"提升为"可推理的因果模型"：
//! - CausalExtractor: 从 SchemaEntry 中提取因果断言（X causes Y, X enables Y）
//! - CausalGraph: 内存中的因果图（节点=实体，边=因果强度）
//! - CausalQuery: 查询因果路径、反事实、干预效果

use cog_core::{MemoryBackend, SchemaEntry, SchemaKind};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// 因果断言类型。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CausalAssertion {
    Causes,
    Enables,
    Inhibits,
    Correlates,
}

/// 因果边 — 两个实体之间的因果断言。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalEdge {
    pub from: String,
    pub to: String,
    pub assertion: CausalAssertion,
    pub strength: f64, // 0.0 ~ 1.0
    pub evidence_count: u32,
    pub sources: Vec<String>,
}

/// 因果节点 — 实体在因果图中的投影。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalNode {
    pub entity_id: String,
    pub entity_type: String,
    pub label: String,
}

/// 内存因果图。
pub struct CausalGraph {
    nodes: HashMap<String, CausalNode>,
    edges: Vec<CausalEdge>,
}

impl CausalGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: CausalNode) {
        self.nodes.insert(node.entity_id.clone(), node);
    }

    pub fn add_edge(&mut self, edge: CausalEdge) {
        self.edges.push(edge);
    }

    /// 查找从 `start` 到 `target` 的所有因果路径（DFS）。
    pub fn causal_paths(&self, start: &str, target: &str, max_depth: usize) -> Vec<Vec<String>> {
        let mut paths = vec![];
        let mut visited = HashSet::new();
        let mut current = vec![start.to_string()];
        self.dfs_paths(
            start,
            target,
            max_depth,
            &mut visited,
            &mut current,
            &mut paths,
        );
        paths
    }

    fn dfs_paths(
        &self,
        current: &str,
        target: &str,
        depth: usize,
        visited: &mut HashSet<String>,
        path: &mut Vec<String>,
        results: &mut Vec<Vec<String>>,
    ) {
        if depth == 0 {
            return;
        }
        if current == target && path.len() > 1 {
            results.push(path.clone());
            return;
        }
        visited.insert(current.to_string());
        for edge in &self.edges {
            if edge.from == current && !visited.contains(&edge.to) {
                path.push(edge.to.clone());
                self.dfs_paths(&edge.to, target, depth - 1, visited, path, results);
                path.pop();
            }
        }
        visited.remove(current);
    }

    /// 查询"做什么会导致 X" — 反向因果推理。
    pub fn upstream_causes(&self, target: &str) -> Vec<&CausalEdge> {
        self.edges.iter().filter(|e| e.to == target).collect()
    }

    /// 查询"X 会导致什么" — 正向因果推理。
    pub fn downstream_effects(&self, source: &str) -> Vec<&CausalEdge> {
        self.edges.iter().filter(|e| e.from == source).collect()
    }

    /// 计算干预效果：如果强制改变 `intervention_node`，对 `outcome_node` 的影响概率。
    pub fn intervention_effect(&self, intervention_node: &str, outcome_node: &str) -> f64 {
        let paths = self.causal_paths(intervention_node, outcome_node, 5);
        if paths.is_empty() {
            return 0.0;
        }
        // 简单模型：取最强路径的累积强度
        paths
            .iter()
            .filter_map(|path| {
                let mut strength = 1.0;
                for window in path.windows(2) {
                    let edge = self
                        .edges
                        .iter()
                        .find(|e| e.from == window[0] && e.to == window[1])?;
                    strength *= edge.strength;
                }
                Some(strength)
            })
            .fold(0.0, f64::max)
    }

    pub fn nodes(&self) -> &HashMap<String, CausalNode> {
        &self.nodes
    }

    pub fn edges(&self) -> &[CausalEdge] {
        &self.edges
    }
}

impl Default for CausalGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// 因果提取器 — 从 MemoryBackend 的 Schema 层提取因果图。
pub struct CausalExtractor {
    /// 触发因果提取的关系类型关键词。
    pub causal_relation_keywords: Vec<String>,
}

impl CausalExtractor {
    pub fn new() -> Self {
        Self {
            causal_relation_keywords: vec![
                "causes".into(),
                "enables".into(),
                "inhibits".into(),
                "leads_to".into(),
                "results_in".into(),
                "prevents".into(),
                "improves".into(),
                "degrades".into(),
            ],
        }
    }

    /// 从 MemoryBackend 中提取因果图。
    pub async fn extract_from_backend(
        &self,
        backend: &dyn MemoryBackend,
        namespace: &str,
    ) -> anyhow::Result<CausalGraph> {
        let mut graph = CausalGraph::new();

        // Fetch all schema entries
        let schema_results = backend.search_schema(namespace, "", 10000).await?;

        // First pass: collect all entities as nodes
        for result in &schema_results {
            if result.entry.kind == SchemaKind::Entity {
                let label = result
                    .entry
                    .properties
                    .get("label")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&result.entry.id)
                    .to_string();
                let entity_type = result
                    .entry
                    .properties
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                graph.add_node(CausalNode {
                    entity_id: result.entry.id.clone(),
                    entity_type,
                    label,
                });
            }
        }

        // Second pass: extract causal edges from relations
        for result in &schema_results {
            if result.entry.kind == SchemaKind::Relation {
                if let Some(assertion) = self.infer_assertion(&result.entry) {
                    let from = result
                        .entry
                        .properties
                        .get("from")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let to = result
                        .entry
                        .properties
                        .get("to")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let strength = result
                        .entry
                        .properties
                        .get("strength")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.5);

                    if !from.is_empty() && !to.is_empty() {
                        graph.add_edge(CausalEdge {
                            from,
                            to,
                            assertion,
                            strength,
                            evidence_count: 1,
                            sources: vec![result.entry.id.clone()],
                        });
                    }
                }
            }
        }

        Ok(graph)
    }

    fn infer_assertion(&self, entry: &SchemaEntry) -> Option<CausalAssertion> {
        let relation_type = entry
            .properties
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();

        if relation_type.contains("cause") || relation_type.contains("leads_to") {
            Some(CausalAssertion::Causes)
        } else if relation_type.contains("enable") || relation_type.contains("improve") {
            Some(CausalAssertion::Enables)
        } else if relation_type.contains("inhibit")
            || relation_type.contains("prevent")
            || relation_type.contains("degrade")
        {
            Some(CausalAssertion::Inhibits)
        } else if relation_type.contains("correlate")
            || self
                .causal_relation_keywords
                .iter()
                .any(|kw| relation_type.contains(kw))
        {
            Some(CausalAssertion::Correlates)
        } else {
            None
        }
    }

    /// 从 reflection learnings 中提取因果模式："X 类任务用 Y 模型效果好" → 因果边。
    pub fn extract_from_learning(&self, learning_text: &str, source_id: &str) -> Vec<CausalEdge> {
        let mut edges = vec![];
        // Simple heuristic patterns
        let patterns = [
            ("causes", CausalAssertion::Causes),
            ("enables", CausalAssertion::Enables),
            ("inhibits", CausalAssertion::Inhibits),
            ("improves", CausalAssertion::Enables),
            ("degrades", CausalAssertion::Inhibits),
        ];
        for (keyword, assertion) in &patterns {
            if learning_text.to_lowercase().contains(keyword) {
                // Extract simple "A -> B" patterns
                // MVP: just create a weak correlate edge for the whole learning
                edges.push(CausalEdge {
                    from: format!("learning:{}", source_id),
                    to: learning_text.chars().take(50).collect(),
                    assertion: assertion.clone(),
                    strength: 0.3,
                    evidence_count: 1,
                    sources: vec![source_id.into()],
                });
            }
        }
        edges
    }
}

impl Default for CausalExtractor {
    fn default() -> Self {
        Self::new()
    }
}

/// 因果查询接口 — 封装常用因果推理查询。
pub struct CausalQuery<'a> {
    graph: &'a CausalGraph,
}

impl<'a> CausalQuery<'a> {
    pub fn new(graph: &'a CausalGraph) -> Self {
        Self { graph }
    }

    /// "如果做了 X，Y 会怎样？" — 干预查询。
    pub fn what_if(&self, intervention: &str, outcome: &str) -> f64 {
        self.graph.intervention_effect(intervention, outcome)
    }

    /// "X 的最佳上游干预是什么？" — 反向优化。
    pub fn best_intervention(&self, outcome: &str, candidates: &[&str]) -> Option<(String, f64)> {
        candidates
            .iter()
            .map(|c| (c.to_string(), self.graph.intervention_effect(c, outcome)))
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .filter(|(_, strength)| *strength > 0.0)
    }

    /// "X 和 Y 之间有多少条因果路径？" — 连通性。
    pub fn path_count(&self, from: &str, to: &str, max_depth: usize) -> usize {
        self.graph.causal_paths(from, to, max_depth).len()
    }
}
