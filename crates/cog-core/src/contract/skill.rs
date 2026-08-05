//!Skill system contracts — `cog-core` Domain Kernel.
//!`cog-skill` is the **Markdown skill** management crate:
//!discovery, loading, hot-reload, and content serving.
//!Skills are LLM-readable instruction documents (SKILL.md + scripts/)
//!that extend agent capabilities without recompiling the main binary.

use crate::{
    storage::VectorBackend, AtomicTask, BoundaryReport, BoundaryRule, BoundaryViolation, RuleType,
    SFError, SFResult, Skill, SkillConfig,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

// ==========================================================================
// Skill manifest & metadata
// ==========================================================================

/// Skill metadata (lightweight, used for triggering).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillMetadata {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version: String,
}

/// Parsed YAML frontmatter from SKILL.md.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SkillFrontmatter {
    pub name: String,
    pub description: String,
}

/// Full skill definition (loaded into memory).
pub struct SkillDef {
    pub metadata: SkillMetadata,
    pub skill_md: String,
    pub frontmatter: SkillFrontmatter,
}

/// Prompt-layer skill definition — PGE 输出格式技能化的载体
/// （docs/20250605_squad_pge_architecture_refactor.md §4.3）。
///
/// 把角色的 prompt 模板与输出 schema 从 Rust 代码硬编码迁移为可配置数据：
/// schema 仅作为指导而非紧箍咒，校验失败时调用方回退宽松解析。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PromptSkillDef {
    /// 技能 ID（SkillRegistry 中的目录名）。
    pub skill_id: String,
    /// 角色（planner / generator / evaluator / moderator / merger）。
    pub role: String,
    /// Prompt 模板（SKILL.md 正文），注入 actor 的 prompt ctx。
    pub prompt_template: String,
    /// 期望的输出 schema（指导用，不强制约束 LLM）。
    #[serde(default)]
    pub output_schema: Option<Value>,
    /// 是否要求结构化输出（由 skill 配置决定，而非代码层硬编码）。
    /// None = 保持调用方默认行为。
    #[serde(default)]
    pub use_structured: Option<bool>,
}

// ==========================================================================
// SkillRegistry trait
// ==========================================================================

#[async_trait]
pub trait ExternalSkillRegistry: Send + Sync {
    /// Resolve skill metadata by ID (name + description only).
    async fn resolve_metadata(&self, skill_id: &str) -> crate::SFResult<SkillMetadata>;

    /// Resolve full skill definition by ID (includes SKILL.md body).
    async fn resolve(&self, skill_id: &str) -> crate::SFResult<SkillDef>;

    /// List all loaded skills (metadata only).
    async fn list(&self) -> crate::SFResult<Vec<SkillMetadata>>;

    /// Load a resource file from a skill (references/, agents/, assets/, etc.).
    async fn load_resource(&self, skill_id: &str, resource_path: &str) -> crate::SFResult<String>;

    /// List available scripts for a skill.
    async fn list_scripts(&self, skill_id: &str) -> crate::SFResult<Vec<String>>;

    /// Get absolute path to a script (for execution by the caller).
    async fn script_path(&self, skill_id: &str, script_name: &str) -> crate::SFResult<PathBuf>;

    /// Download a skill from an online source.
    async fn download(&self, source: &str, opts: DownloadOptions) -> crate::SFResult<()>;
}

/// Options for downloading skills.
#[derive(Debug, Clone, Default)]
pub struct DownloadOptions {
    pub branch: Option<String>,
    pub version: Option<String>,
}

/// In-memory registry of skills with discovery and retrieval.
#[derive(Debug, Clone, Default)]
pub struct SkillRegistry {
    skills: HashMap<String, Skill>,
    agent_skills: HashMap<String, SkillConfig>,
    /// Runtime priority overrides for skill selection.
    /// Higher values mean higher priority. Default is 0.
    skill_priorities: HashMap<String, i32>,
}

impl SkillRegistry {
    pub fn new() -> Self {
        Self {
            skills: HashMap::new(),
            agent_skills: HashMap::new(),
            skill_priorities: HashMap::new(),
        }
    }

    pub fn from_skills(skills: Vec<Skill>) -> Self {
        let mut map = HashMap::with_capacity(skills.len());
        for s in skills {
            map.insert(s.id.clone(), s);
        }
        Self {
            skills: map,
            agent_skills: HashMap::new(),
            skill_priorities: HashMap::new(),
        }
    }

    /// Load skills from JSON files in a directory (one skill per file, or an array in a single file).
    pub fn load_from_dir<P: AsRef<Path>>(dir: P) -> SFResult<Self> {
        let mut skills = Vec::new();
        let entries = std::fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = std::fs::read_to_string(&path)?;
                let parsed: Value = serde_json::from_str(&content)?;
                if let Some(arr) = parsed.as_array() {
                    for item in arr {
                        let skill: Skill =
                            serde_json::from_value(item.clone()).map_err(SFError::Serialization)?;
                        skills.push(skill);
                    }
                } else {
                    let skill: Skill =
                        serde_json::from_value(parsed).map_err(SFError::Serialization)?;
                    skills.push(skill);
                }
            }
        }
        Ok(Self::from_skills(skills))
    }

    /// Load skills from a raw JSON string.
    pub fn load_from_json(json: &str) -> SFResult<Self> {
        let skills: Vec<Skill> = serde_json::from_str(json)?;
        Ok(Self::from_skills(skills))
    }

    pub fn get(&self, skill_id: &str) -> Option<&Skill> {
        self.skills.get(skill_id)
    }

    pub fn get_all(&self) -> Vec<&Skill> {
        self.skills.values().collect()
    }

    pub fn insert(&mut self, skill: Skill) {
        self.skills.insert(skill.id.clone(), skill);
    }

    // ── Agent SkillConfig API ────────────────────────────────────────────────

    /// Retrieve an agent skill configuration by ID.
    pub fn get_skill(&self, skill_id: &str) -> Option<&SkillConfig> {
        self.agent_skills.get(skill_id)
    }

    /// Insert an agent skill configuration.
    ///
    /// Also mirrors the config into the planner-facing `skills` map so that
    /// goal decomposition (`get_all`) sees every loaded agent skill — the two
    /// maps must never drift apart ("Everything is a skill").
    pub fn insert_skill_config(&mut self, skill: SkillConfig) {
        let mirrored = Skill {
            id: skill.skill_id.clone(),
            name: skill.name.clone(),
            description: skill.role_type.clone(),
            tools: skill.tools.clone(),
            complexity_score: 0,
            blocked_by: Vec::new(),
            blocks: Vec::new(),
        };
        self.skills.insert(mirrored.id.clone(), mirrored);
        self.agent_skills.insert(skill.skill_id.clone(), skill);
    }

    /// Remove an agent skill configuration by ID.
    pub fn remove_skill_config(&mut self, skill_id: &str) {
        self.agent_skills.remove(skill_id);
        self.skills.remove(skill_id);
    }

    /// Load agent skill configurations from JSON files in a directory.
    /// Each `.json` file should contain either a single `SkillConfig` object
    /// or an array of `SkillConfig` objects.
    pub fn load_skills_from_dir(&mut self, dir: &Path) -> SFResult<()> {
        let entries = std::fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = std::fs::read_to_string(&path)?;
                let parsed: Value = serde_json::from_str(&content)?;
                if let Some(arr) = parsed.as_array() {
                    for item in arr {
                        let skill: SkillConfig =
                            serde_json::from_value(item.clone()).map_err(SFError::Serialization)?;
                        self.insert_skill_config(skill);
                    }
                } else {
                    let skill: SkillConfig =
                        serde_json::from_value(parsed).map_err(SFError::Serialization)?;
                    self.insert_skill_config(skill);
                }
            }
        }
        Ok(())
    }

    /// Load agent skill configurations from a raw JSON string.
    pub fn load_skills_from_json(&mut self, json: &str) -> SFResult<()> {
        let skills: Vec<SkillConfig> =
            serde_json::from_str(json).map_err(SFError::Serialization)?;
        for skill in skills {
            self.insert_skill_config(skill);
        }
        Ok(())
    }

    /// Search for skills by embedding similarity using a vector backend.
    /// The `collection` parameter names the vector collection that holds skill embeddings.
    pub async fn search_by_embedding(
        &self,
        backend: &dyn VectorBackend,
        collection: &str,
        query_embedding: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<(Skill, f32)>> {
        let results = backend.search(collection, query_embedding, top_k).await?;
        let mut scored = Vec::with_capacity(results.len());
        for r in results {
            if let Some(skill_id) = r.metadata.get("skill_id").and_then(|v| v.as_str()) {
                if let Some(skill) = self.skills.get(skill_id) {
                    scored.push((skill.clone(), r.score));
                }
            }
        }
        Ok(scored)
    }

    /// Keyword-based fallback search when embeddings are unavailable.
    pub fn search_by_keyword(&self, query: &str) -> Vec<&Skill> {
        let lower = query.to_lowercase();
        let terms: Vec<&str> = lower.split_whitespace().collect();
        self.skills
            .values()
            .filter(|s| {
                let text = format!("{} {} {}", s.id, s.name, s.description).to_lowercase();
                terms.iter().any(|term| text.contains(term))
            })
            .collect()
    }

    // ── Priority API ─────────────────────────────────────────────────────────

    /// Set the priority for a skill. Higher values mean higher selection priority.
    pub fn set_priority(&mut self, skill_id: &str, priority: i32) {
        self.skill_priorities.insert(skill_id.to_string(), priority);
    }

    /// Get the priority for a skill. Returns 0 if not explicitly set.
    pub fn get_priority(&self, skill_id: &str) -> i32 {
        self.skill_priorities.get(skill_id).copied().unwrap_or(0)
    }

    /// Return all skills sorted by priority (highest first).
    pub fn get_prioritized_skills(&self) -> Vec<(&Skill, i32)> {
        let mut scored: Vec<(&Skill, i32)> = self
            .skills
            .values()
            .map(|s| (s, self.get_priority(&s.id)))
            .collect();
        scored.sort_by_key(|a| std::cmp::Reverse(a.1));
        scored
    }
}

/// Detect boundary violations using dynamically configured rules.
/// Only Hard rules are evaluated here; Soft rules are handled by LLM in Evaluator.
pub fn detect_boundaries(
    tasks: &[AtomicTask],
    skills: &SkillRegistry,
    rules: &[BoundaryRule],
) -> BoundaryReport {
    let mut violations = Vec::new();
    let mut suggestions = Vec::new();

    for rule in rules
        .iter()
        .filter(|r| r.enabled && r.rule_type == RuleType::Hard)
    {
        match rule.name.as_str() {
            "TokenBudget" => {
                let threshold = rule.threshold.unwrap_or(100_000);
                for task in tasks {
                    if task.estimated_tokens > threshold {
                        violations.push(BoundaryViolation {
                            dimension: rule.name.clone(),
                            task_id: task.id.clone(),
                            message: format!(
                                "estimated_tokens {} exceeds limit {}",
                                task.estimated_tokens, threshold
                            ),
                        });
                        suggestions.push(format!(
                            "Split task '{}' vertically into smaller sub-tasks",
                            task.id
                        ));
                    }
                }
            }
            "SkillBoundary" => {
                let threshold = rule.threshold.unwrap_or(2) as usize;
                for task in tasks {
                    if let Some(ref skill_id) = task.skill_id {
                        if let Some(skill) = skills.get(skill_id) {
                            let unrelated_skills: Vec<&str> = task
                                .blocked_by
                                .iter()
                                .filter_map(|dep_id| tasks.iter().find(|t| t.id == *dep_id))
                                .filter_map(|dep| dep.skill_id.as_ref())
                                .filter(|dep_skill_id| {
                                    *dep_skill_id != skill_id
                                        && !skill.blocked_by.contains(&dep_skill_id.to_string())
                                        && !skill.blocks.contains(&dep_skill_id.to_string())
                                })
                                .map(|s| s.as_str())
                                .collect();
                            if unrelated_skills.len() >= threshold {
                                violations.push(BoundaryViolation {
                                    dimension: rule.name.clone(),
                                    task_id: task.id.clone(),
                                    message: format!(
                                        "task spans >={} unrelated skills: {:?}",
                                        threshold, unrelated_skills
                                    ),
                                });
                                suggestions.push(format!(
                                    "Split task '{}' horizontally into parallel skill-specific tasks",
                                    task.id
                                ));
                            }
                        }
                    }
                }
            }
            "TimeBoundary" => {
                let threshold = rule.threshold.unwrap_or(3600);
                for task in tasks {
                    if task.estimated_seconds > threshold {
                        violations.push(BoundaryViolation {
                            dimension: rule.name.clone(),
                            task_id: task.id.clone(),
                            message: format!(
                                "estimated_time {}s exceeds threshold {}s",
                                task.estimated_seconds, threshold
                            ),
                        });
                        suggestions.push(format!(
                            "Break task '{}' into milestone sub-tasks each < 30min",
                            task.id
                        ));
                    }
                }
            }
            "StateBoundary" => {
                let threshold = rule.threshold.unwrap_or(3) as usize;
                let transitions = ["design", "review", "modify", "implement", "test", "deploy"];
                for task in tasks {
                    if let Some(ref desc) = task.description {
                        let found: Vec<&str> = transitions
                            .iter()
                            .filter(|&&t| desc.to_lowercase().contains(t))
                            .copied()
                            .collect();
                        if found.len() >= threshold {
                            violations.push(BoundaryViolation {
                                dimension: rule.name.clone(),
                                task_id: task.id.clone(),
                                message: format!(
                                    "description contains {} state transitions: {:?}",
                                    found.len(),
                                    found
                                ),
                            });
                            suggestions.push(format!(
                                "Split task '{}' at each state transition point",
                                task.id
                            ));
                        }
                    }
                }
            }
            "DataBoundary" => {
                let threshold = rule.threshold.unwrap_or(3) as usize;
                for task in tasks {
                    if task.output_entities.len() >= threshold {
                        violations.push(BoundaryViolation {
                            dimension: rule.name.clone(),
                            task_id: task.id.clone(),
                            message: format!(
                                "task handles {} independent data sources: {:?}",
                                task.output_entities.len(),
                                task.output_entities
                            ),
                        });
                        suggestions.push(format!(
                            "Split task '{}' by data source for parallel execution",
                            task.id
                        ));
                    }
                }
            }
            // Custom hard rules can be added here by name match.
            _ => {
                tracing::debug!("Unknown hard boundary rule '{}' skipped", rule.name);
            }
        }
    }

    BoundaryReport {
        passed: violations.is_empty(),
        violations,
        suggestions,
    }
}

/// Infer dependencies across three layers:
/// A. skill-inherent (static, from skill.blocked_by / skill.blocks)
/// B. data-flow (dynamic, from input/output entity overlap)
/// C. manual-override (highest priority, from explicit overrides)
pub fn infer_dependencies(
    tasks: &[AtomicTask],
    skills: &SkillRegistry,
    manual_overrides: &[(String, String)],
) -> Vec<(String, String)> {
    let mut edges: HashSet<(String, String)> = HashSet::new();

    // Layer C: manual overrides (highest priority).
    for (from, to) in manual_overrides {
        edges.insert((from.clone(), to.clone()));
    }

    // Layer A: skill-inherent dependencies.
    let task_skill_map: HashMap<String, String> = tasks
        .iter()
        .filter_map(|t| t.skill_id.as_ref().map(|sid| (t.id.clone(), sid.clone())))
        .collect();

    for task in tasks {
        if let Some(skill_id) = task_skill_map.get(&task.id) {
            if let Some(skill) = skills.get(skill_id) {
                // If this skill blocks another skill, add edge to tasks using the blocked skill.
                for blocked_skill_id in &skill.blocks {
                    for other in tasks {
                        if other.id != task.id && other.skill_id.as_ref() == Some(blocked_skill_id)
                        {
                            edges.insert((task.id.clone(), other.id.clone()));
                        }
                    }
                }
                // If this skill is blocked_by another skill, add edge from blocking tasks.
                for blocker_skill_id in &skill.blocked_by {
                    for other in tasks {
                        if other.id != task.id && other.skill_id.as_ref() == Some(blocker_skill_id)
                        {
                            edges.insert((other.id.clone(), task.id.clone()));
                        }
                    }
                }
            }
        }
    }

    // Layer B: data-flow dependency (NER-based overlap).
    for a in tasks {
        for b in tasks {
            if a.id == b.id {
                continue;
            }
            // If b's input contains any entity produced by a (in a.output_entities).
            if !a.output_entities.is_empty() {
                let input_text = serde_json::to_string(&b.input)
                    .unwrap_or_default()
                    .to_lowercase();
                for entity in &a.output_entities {
                    if input_text.contains(&entity.to_lowercase()) {
                        edges.insert((a.id.clone(), b.id.clone()));
                        break;
                    }
                }
            }
        }
    }

    edges.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_skill(id: &str, blocked_by: Vec<&str>, blocks: Vec<&str>) -> Skill {
        Skill {
            id: id.to_string(),
            name: id.to_string(),
            description: format!("{} skill", id),
            tools: vec![],
            complexity_score: 1,
            blocked_by: blocked_by.into_iter().map(String::from).collect(),
            blocks: blocks.into_iter().map(String::from).collect(),
        }
    }

    fn default_rules() -> Vec<BoundaryRule> {
        vec![
            BoundaryRule {
                name: "TokenBudget".into(),
                rule_type: RuleType::Hard,
                threshold: Some(100_000),
                description: "单个任务的 estimated_tokens 不得超过阈值".into(),
                enabled: true,
            },
            BoundaryRule {
                name: "TimeBoundary".into(),
                rule_type: RuleType::Hard,
                threshold: Some(3_600),
                description: "单个任务的 estimated_seconds 不得超过阈值".into(),
                enabled: true,
            },
            BoundaryRule {
                name: "StateBoundary".into(),
                rule_type: RuleType::Hard,
                threshold: Some(3),
                description: "单个任务 description 中的状态转换点不得超过阈值".into(),
                enabled: true,
            },
            BoundaryRule {
                name: "DataBoundary".into(),
                rule_type: RuleType::Hard,
                threshold: Some(3),
                description: "单个任务处理的独立数据源数量不得超过阈值".into(),
                enabled: true,
            },
            BoundaryRule {
                name: "OutputQuality".into(),
                rule_type: RuleType::Soft,
                threshold: None,
                description: "生成输出必须满足以下质量标准：1) 代码必须包含单元测试 2) 文档必须完整 3) 无已知安全漏洞".into(),
                enabled: true,
            },
        ]
    }

    fn make_task(
        id: &str,
        skill_id: Option<&str>,
        tokens: u64,
        seconds: u64,
        desc: Option<&str>,
        output_entities: Vec<&str>,
    ) -> AtomicTask {
        AtomicTask {
            id: id.to_string(),
            name: id.to_string(),
            skill_id: skill_id.map(String::from),
            description: desc.map(String::from),
            estimated_tokens: tokens,
            skill_gap: false,
            blocked_by: vec![],
            blocks: vec![],
            input: serde_json::Value::Null,
            output_entities: output_entities.into_iter().map(String::from).collect(),
            estimated_seconds: seconds,
        }
    }

    #[test]
    fn test_skill_registry_load_from_json() {
        let json = r#"[
            {"id":"skill-a","name":"Skill A","description":"does A","tools":["t1"],"complexity_score":2},
            {"id":"skill-b","name":"Skill B","description":"does B","tools":["t2"]}
        ]"#;
        let registry = SkillRegistry::load_from_json(json).unwrap();
        assert_eq!(registry.get_all().len(), 2);
        assert!(registry.get("skill-a").is_some());
        assert!(registry.get("skill-b").is_some());
    }

    #[test]
    fn test_skill_registry_keyword_search() {
        let skills = vec![
            make_skill("db-design", vec![], vec![]),
            make_skill("api-design", vec![], vec![]),
            make_skill("backend-impl", vec![], vec![]),
        ];
        let registry = SkillRegistry::from_skills(skills);
        let results = registry.search_by_keyword("design");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_boundary_token_budget() {
        let tasks = vec![make_task("t1", Some("s1"), 150_000, 0, None, vec![])];
        let registry = SkillRegistry::from_skills(vec![make_skill("s1", vec![], vec![])]);
        let rules = default_rules();
        let report = detect_boundaries(&tasks, &registry, &rules);
        assert!(!report.passed);
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].dimension, "TokenBudget");
    }

    #[test]
    fn test_boundary_time() {
        let tasks = vec![make_task("t1", Some("s1"), 0, 4000, None, vec![])];
        let registry = SkillRegistry::from_skills(vec![make_skill("s1", vec![], vec![])]);
        let rules = default_rules();
        let report = detect_boundaries(&tasks, &registry, &rules);
        assert!(!report.passed);
        assert!(report
            .violations
            .iter()
            .any(|v| v.dimension == "TimeBoundary"));
    }

    #[test]
    fn test_boundary_state() {
        let tasks = vec![make_task(
            "t1",
            Some("s1"),
            0,
            0,
            Some("Design the system, review it, modify the code, implement and test"),
            vec![],
        )];
        let registry = SkillRegistry::from_skills(vec![make_skill("s1", vec![], vec![])]);
        let rules = default_rules();
        let report = detect_boundaries(&tasks, &registry, &rules);
        assert!(!report.passed);
        assert!(report
            .violations
            .iter()
            .any(|v| v.dimension == "StateBoundary"));
    }

    #[test]
    fn test_boundary_data() {
        let tasks = vec![make_task(
            "t1",
            Some("s1"),
            0,
            0,
            None,
            vec!["users", "orders", "payments"],
        )];
        let registry = SkillRegistry::from_skills(vec![make_skill("s1", vec![], vec![])]);
        let rules = default_rules();
        let report = detect_boundaries(&tasks, &registry, &rules);
        assert!(!report.passed);
        assert!(report
            .violations
            .iter()
            .any(|v| v.dimension == "DataBoundary"));
    }

    #[test]
    fn test_infer_dependencies_skill_inherent() {
        let skills = vec![
            make_skill("s-arch", vec![], vec!["s-db"]),
            make_skill("s-db", vec!["s-arch"], vec![]),
        ];
        let registry = SkillRegistry::from_skills(skills);
        let tasks = vec![
            make_task("t-arch", Some("s-arch"), 0, 0, None, vec![]),
            make_task("t-db", Some("s-db"), 0, 0, None, vec![]),
        ];
        let edges = infer_dependencies(&tasks, &registry, &[]);
        assert!(edges.contains(&("t-arch".into(), "t-db".into())));
    }

    #[test]
    fn test_infer_dependencies_data_flow() {
        let skills = vec![make_skill("s1", vec![], vec![])];
        let registry = SkillRegistry::from_skills(skills);
        let t1 = make_task("t1", Some("s1"), 0, 0, None, vec!["architecture-diagram"]);
        let mut t2 = make_task("t2", Some("s1"), 0, 0, None, vec![]);
        t2.input = serde_json::json!({"doc": "use the architecture-diagram"});
        let edges = infer_dependencies(&[t1, t2], &registry, &[]);
        assert!(edges.contains(&("t1".into(), "t2".into())));
    }

    #[test]
    fn test_infer_dependencies_manual_override_priority() {
        let skills = vec![];
        let registry = SkillRegistry::from_skills(skills);
        let tasks = vec![
            make_task("a", None, 0, 0, None, vec![]),
            make_task("b", None, 0, 0, None, vec![]),
        ];
        let edges = infer_dependencies(&tasks, &registry, &[("b".into(), "a".into())]);
        assert!(edges.contains(&("b".into(), "a".into())));
    }

    #[test]
    fn test_skill_registry_load_skills_from_json() {
        let mut registry = SkillRegistry::new();
        let json = r#"[
            {"skill_id":"planner","name":"Planner","tools":["md"],"max_iterations":10,"role_type":"planner"},
            {"skill_id":"evaluator","name":"Evaluator","tools":["test"],"max_iterations":5,"role_type":"evaluator"}
        ]"#;
        registry.load_skills_from_json(json).unwrap();
        assert!(registry.get_skill("planner").is_some());
        assert!(registry.get_skill("evaluator").is_some());
        let planner = registry.get_skill("planner").unwrap();
        assert_eq!(planner.name, "Planner");
        assert_eq!(planner.max_iterations, 10);
        assert_eq!(planner.role_type, "planner");
    }

    #[test]
    fn test_skill_registry_load_skills_from_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        std::fs::write(
            path.join("planner.json"),
            r#"{"skill_id":"planner","name":"Planner","tools":["md"],"max_iterations":10,"role_type":"planner"}"#,
        )
        .unwrap();
        std::fs::write(
            path.join("evaluator.json"),
            r#"{"skill_id":"evaluator","name":"Evaluator","tools":["test"],"max_iterations":5,"role_type":"evaluator"}"#,
        )
        .unwrap();

        let mut registry = SkillRegistry::new();
        registry.load_skills_from_dir(path).unwrap();
        assert!(registry.get_skill("planner").is_some());
        assert!(registry.get_skill("evaluator").is_some());
    }

    #[test]
    fn test_skill_priority() {
        let mut registry = SkillRegistry::from_skills(vec![
            make_skill("s-low", vec![], vec![]),
            make_skill("s-high", vec![], vec![]),
            make_skill("s-default", vec![], vec![]),
        ]);
        registry.set_priority("s-high", 10);
        registry.set_priority("s-low", -5);

        assert_eq!(registry.get_priority("s-high"), 10);
        assert_eq!(registry.get_priority("s-low"), -5);
        assert_eq!(registry.get_priority("s-default"), 0);
        assert_eq!(registry.get_priority("missing"), 0);

        let prioritized = registry.get_prioritized_skills();
        assert_eq!(prioritized.len(), 3);
        assert_eq!(prioritized[0].0.id, "s-high");
        assert_eq!(prioritized[0].1, 10);
        assert_eq!(prioritized[2].0.id, "s-low");
        assert_eq!(prioritized[2].1, -5);
    }
}
