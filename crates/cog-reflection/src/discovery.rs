//! Autonomous capability discovery through exploratory task generation.
//! The engine proactively constructs tasks that exercise untried tool
//! combinations, parameter ranges, cross-domain transfers, and boundary
//! conditions.  Successful discoveries enter the standard five-phase
//! learning pipeline; failures are recorded as negative learnings to avoid
//! repeated exploration.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{DiscoveryStatus, DiscoveryStrategy, DiscoveryTask};
use crate::LearningRecorder;
use cog_core::SFResult;

/// Engine that generates and evaluates exploratory discovery tasks.
pub struct DiscoveryEngine {
    recorder: Arc<dyn LearningRecorder>,
    /// Combinations that have already been explored (tool_set hash).
    explored: Arc<RwLock<HashSet<String>>>,
}

impl std::fmt::Debug for DiscoveryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryEngine").finish()
    }
}

impl DiscoveryEngine {
    pub fn new(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            recorder,
            explored: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Generate a batch of discovery tasks based on the available tools and
    /// skills.  Tasks are filtered to remove already-explored combinations.
    pub async fn generate_tasks(
        &self,
        tool_names: &[String],
        skill_ids: &[String],
        max_tasks: usize,
    ) -> Vec<DiscoveryTask> {
        let mut tasks = Vec::with_capacity(max_tasks);
        let mut guard = self.explored.write().await;

        // Strategy 1: Tool combination exploration (pair-wise).
        for i in 0..tool_names.len() {
            for j in (i + 1)..tool_names.len() {
                let combo = format!("tool:{}+{}", tool_names[i], tool_names[j]);
                if guard.contains(&combo) {
                    continue;
                }
                if tasks.len() >= max_tasks {
                    break;
                }
                guard.insert(combo.clone());
                tasks.push(DiscoveryTask {
                    id: DiscoveryTask::generate_id(),
                    hypothesis: format!(
                        "Combining {} and {} may yield a new composite capability.",
                        tool_names[i], tool_names[j]
                    ),
                    strategy: DiscoveryStrategy::ToolCombination,
                    tools: vec![tool_names[i].clone(), tool_names[j].clone()],
                    prompt_variants: vec![format!(
                        "Use {} then {} to solve a general task.",
                        tool_names[i], tool_names[j]
                    )],
                    evaluation_criteria: vec![
                        "Produces a coherent result".into(),
                        "Does not error".into(),
                    ],
                    status: DiscoveryStatus::Pending,
                    created_at: Utc::now(),
                });
            }
        }

        // Strategy 2: Parameter space exploration for the first tool.
        if let Some(tool) = tool_names.first() {
            let combo = format!("param:{}", tool);
            if !guard.contains(&combo) && tasks.len() < max_tasks {
                guard.insert(combo.clone());
                tasks.push(DiscoveryTask {
                    id: DiscoveryTask::generate_id(),
                    hypothesis: format!(
                        "Varying parameters for {} reveals optimal usage patterns.",
                        tool
                    ),
                    strategy: DiscoveryStrategy::ParameterSpace,
                    tools: vec![tool.clone()],
                    prompt_variants: vec![
                        format!("Use {} with conservative parameters.", tool),
                        format!("Use {} with aggressive parameters.", tool),
                    ],
                    evaluation_criteria: vec![
                        "Result quality improves with parameter change".into()
                    ],
                    status: DiscoveryStatus::Pending,
                    created_at: Utc::now(),
                });
            }
        }

        // Strategy 3: Cross-domain transfer for the first skill.
        if let Some(skill) = skill_ids.first() {
            let combo = format!("xfer:{}", skill);
            if !guard.contains(&combo) && tasks.len() < max_tasks {
                guard.insert(combo.clone());
                tasks.push(DiscoveryTask {
                    id: DiscoveryTask::generate_id(),
                    hypothesis: format!(
                        "Skill {} may be applicable to an unrelated domain.",
                        skill
                    ),
                    strategy: DiscoveryStrategy::CrossDomainTransfer,
                    tools: tool_names.to_vec(),
                    prompt_variants: vec![format!(
                        "Apply the principles of skill {} to a novel problem domain.",
                        skill
                    )],
                    evaluation_criteria: vec!["Skill principles transfer meaningfully".into()],
                    status: DiscoveryStatus::Pending,
                    created_at: Utc::now(),
                });
            }
        }

        // Strategy 4: Boundary stress test.
        let combo = "boundary:stress".to_string();
        if !guard.contains(&combo) && tasks.len() < max_tasks {
            guard.insert(combo.clone());
            tasks.push(DiscoveryTask {
                id: DiscoveryTask::generate_id(),
                hypothesis: "The system handles extreme inputs gracefully.".into(),
                strategy: DiscoveryStrategy::BoundaryStressTest,
                tools: tool_names.to_vec(),
                prompt_variants: vec![
                    "Process an empty input.".into(),
                    "Process a maximally long input.".into(),
                    "Process contradictory instructions.".into(),
                ],
                evaluation_criteria: vec![
                    "Does not panic or hang".into(),
                    "Produces a meaningful error or fallback".into(),
                ],
                status: DiscoveryStatus::Pending,
                created_at: Utc::now(),
            });
        }

        drop(guard);
        info!(generated = tasks.len(), "Generated discovery tasks");
        tasks
    }

    /// Evaluate the result of an executed discovery task and decide whether
    /// it produced a valuable new capability.
    pub async fn evaluate_result(
        &self,
        task: &DiscoveryTask,
        success: bool,
        notes: &str,
    ) -> SFResult<DiscoveryStatus> {
        let status = if success {
            // Heuristic: if the task succeeded and notes are non-empty,
            // consider it validated.
            if !notes.is_empty() && notes.len() > 20 {
                DiscoveryStatus::Validated
            } else {
                DiscoveryStatus::Inconclusive
            }
        } else {
            DiscoveryStatus::Rejected
        };

        match status {
            DiscoveryStatus::Validated => {
                info!(
                    task_id = %task.id,
                    strategy = ?task.strategy,
                    "Discovery validated — entering learning pipeline"
                );
                // Record as a positive learning so the standard 5-phase
                // pipeline can promote it to a skill.
                let learning = cog_core::Learning::new(
                    cog_core::LearningCategory::Insight,
                    cog_core::Priority::High,
                    cog_core::Area::Config,
                    format!("Discovery: {}", task.hypothesis),
                    format!("Result: {}\nNotes: {}", success, notes),
                    "Extract as new skill or best practice",
                    cog_core::LearningSource::SelfReview,
                );
                self.recorder.record_learning(learning).await?;
            }
            DiscoveryStatus::Rejected => {
                debug!(
                    task_id = %task.id,
                    "Discovery rejected — recording negative learning"
                );
                let learning = cog_core::Learning::new(
                    cog_core::LearningCategory::KnowledgeGap,
                    cog_core::Priority::Low,
                    cog_core::Area::Config,
                    format!("Negative discovery: {}", task.hypothesis),
                    format!("Failed with notes: {}", notes),
                    "Avoid this combination in future",
                    cog_core::LearningSource::SelfReview,
                );
                self.recorder.record_learning(learning).await?;
            }
            DiscoveryStatus::Inconclusive => {
                warn!(
                    task_id = %task.id,
                    "Discovery inconclusive — needs more data"
                );
            }
            _ => {}
        }

        Ok(status)
    }

    /// Check whether a combination has already been explored.
    pub async fn is_explored(&self, tool_set: &[String]) -> bool {
        let key = format!("tool:{}", tool_set.join("+"));
        let guard = self.explored.read().await;
        guard.contains(&key)
    }

    /// Mark a combination as explored without generating a task.
    pub async fn mark_explored(&self, tool_set: &[String]) {
        let key = format!("tool:{}", tool_set.join("+"));
        let mut guard = self.explored.write().await;
        guard.insert(key);
    }
}
