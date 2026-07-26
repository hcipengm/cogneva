//! Promotion pipeline: mature learnings are elevated to system-level
//! configurations (skills, prompts, tool definitions, best practices, or wiki entries).
//! The [`LearningPromoter`] trait encapsulates the decision logic and
//! the side-effects of promoting a learning.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use cog_core::{SFResult, SkillConfig, SkillRegistry};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::types::PromotionResult;
use cog_core::{Learning, LearningCategory};

/// Promotes mature learnings to system configurations.
#[async_trait]
pub trait LearningPromoter: Send + Sync {
    /// Attempt to promote `learning` if it meets maturity criteria.
    async fn promote_if_ready(&self, learning: &Learning) -> SFResult<PromotionResult>;

    /// Check whether `learning` is ready for promotion **without** side effects.
    fn should_promote(&self, learning: &Learning) -> bool;
}

/// Default promoter implementing the maturity rules from the
/// `self-improving-agent` skill:
/// - Recurrence count ≥ `min_recurrence`
/// - Related to ≥ `min_tasks` distinct tasks
/// - First seen within `max_age_days` of last seen
///
/// When a [`cog_core::WikiBackend`] is provided, `Insight` and `KnowledgeGap`
/// learnings are automatically written to the wiki as Markdown documents.
type OnPromotedFn = dyn Fn(&PromotionResult) + Send + Sync;

pub struct DefaultLearningPromoter {
    min_recurrence: u32,
    min_tasks: usize,
    max_age_days: i64,
    skill_registry: Arc<RwLock<SkillRegistry>>,
    wiki_adapter: Option<Arc<dyn cog_core::WikiBackend>>,
    on_promoted: Option<Arc<OnPromotedFn>>,
}

impl std::fmt::Debug for DefaultLearningPromoter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultLearningPromoter")
            .field("min_recurrence", &self.min_recurrence)
            .field("min_tasks", &self.min_tasks)
            .field("max_age_days", &self.max_age_days)
            .field("skill_registry", &"<SkillRegistry>")
            .field("wiki_adapter", &self.wiki_adapter.is_some())
            .finish()
    }
}

impl DefaultLearningPromoter {
    pub fn new(skill_registry: Arc<RwLock<SkillRegistry>>) -> Self {
        Self {
            min_recurrence: 3,
            min_tasks: 2,
            max_age_days: 30,
            skill_registry,
            wiki_adapter: None,
            on_promoted: None,
        }
    }

    /// Attach a wiki adapter so that `Insight` / `KnowledgeGap` learnings
    /// are automatically persisted as wiki documents.
    pub fn with_wiki_adapter(mut self, adapter: Arc<dyn cog_core::WikiBackend>) -> Self {
        self.wiki_adapter = Some(adapter);
        self
    }

    /// Override the minimum recurrence threshold.
    pub fn with_min_recurrence(mut self, n: u32) -> Self {
        self.min_recurrence = n;
        self
    }

    /// Override the minimum distinct-task threshold.
    pub fn with_min_tasks(mut self, n: usize) -> Self {
        self.min_tasks = n;
        self
    }

    /// Override the maximum age window in days.
    pub fn with_max_age_days(mut self, days: i64) -> Self {
        self.max_age_days = days;
        self
    }

    /// Register a callback invoked whenever a learning is promoted.
    /// Used by `AgentRuntime` to hot-reload `SkillConfig` into the
    /// running agent without restart.
    pub fn with_on_promoted(
        mut self,
        callback: Arc<dyn Fn(&PromotionResult) + Send + Sync>,
    ) -> Self {
        self.on_promoted = Some(callback);
        self
    }

    /// Generate a Markdown document from a learning entry.
    fn learning_to_markdown(learning: &Learning) -> String {
        let mut md = String::new();
        md.push_str(&format!("# [{}] {}\n\n", learning.id, learning.summary));
        md.push_str(&format!(
            "**Category**: {:?} | **Priority**: {:?} | **Area**: {:?} | **Status**: {:?}\n\n",
            learning.category, learning.priority, learning.area, learning.status
        ));
        md.push_str(&format!(
            "**Recurrence**: {} | **Tasks**: {}\n\n",
            learning.recurrence_count,
            learning.related_tasks.len()
        ));
        md.push_str(&format!(
            "**First seen**: {} | **Last seen**: {}\n\n",
            learning.first_seen.to_rfc3339(),
            learning.last_seen.to_rfc3339()
        ));

        if !learning.tags.is_empty() {
            md.push_str(&format!("**Tags**: {}\n\n", learning.tags.join(", ")));
        }

        md.push_str("## Details\n\n");
        md.push_str(&learning.details);
        md.push_str("\n\n");

        md.push_str("## Suggested Action\n\n");
        md.push_str(&learning.suggested_action);
        md.push_str("\n\n");

        if !learning.related_files.is_empty() {
            md.push_str("## Related Files\n\n");
            for file in &learning.related_files {
                md.push_str(&format!("- `{}`\n", file));
            }
            md.push('\n');
        }

        if !learning.see_also.is_empty() {
            md.push_str("## See Also\n\n");
            for id in &learning.see_also {
                md.push_str(&format!("- `{}`\n", id));
            }
            md.push('\n');
        }

        if let Some(ref pk) = learning.pattern_key {
            md.push_str(&format!("**Pattern Key**: `{}`\n\n", pk));
        }

        md.push_str(&format!(
            "---\n*Auto-generated by cog-reflection at {}*\n",
            Utc::now().to_rfc3339()
        ));
        md
    }

    /// Write a learning to the wiki as a Markdown document.
    async fn write_to_wiki(&self, learning: &Learning) -> SFResult<String> {
        let adapter = match &self.wiki_adapter {
            Some(a) => a,
            None => {
                return Err(cog_core::SFError::Validation(
                    "Wiki adapter not configured".into(),
                ));
            }
        };

        let path = format!("reflections/{}-{:?}.md", learning.id, learning.area);
        let content = Self::learning_to_markdown(learning);

        adapter.ingest_document(&path, &content).await?;
        info!("wrote learning {} to wiki at {}", learning.id, path);
        Ok(path)
    }
}

#[async_trait]
impl LearningPromoter for DefaultLearningPromoter {
    fn should_promote(&self, learning: &Learning) -> bool {
        let age_days = (learning.last_seen - learning.first_seen).num_days();

        let recurrence_ok = learning.recurrence_count >= self.min_recurrence;
        let tasks_ok = learning.related_tasks.len() >= self.min_tasks;
        let age_ok = age_days <= self.max_age_days;

        recurrence_ok && tasks_ok && age_ok
    }

    async fn promote_if_ready(&self, learning: &Learning) -> SFResult<PromotionResult> {
        if !self.should_promote(learning) {
            let reason = format!(
                "Not ready: recurrence={}, tasks={}, age_days={}",
                learning.recurrence_count,
                learning.related_tasks.len(),
                (learning.last_seen - learning.first_seen).num_days()
            );
            return Ok(PromotionResult::NotReady { reason });
        }

        // Map learning category to promotion target.
        let result = match learning.category {
            LearningCategory::BestPractice => {
                // Promote to SkillConfig best_practices field.
                let mut registry = self.skill_registry.write().await;
                let skill_id = format!("bp.{:?}.{}", learning.area, learning.id);
                let config = SkillConfig {
                    skill_id: skill_id.clone(),
                    name: format!("Best Practice: {}", learning.summary),
                    system_prompt: String::new(), // Deprecated per SkillConfig docs.
                    tools: vec![],
                    max_iterations: 10,
                    role_type: "best_practice".into(),
                };
                registry.insert_skill_config(config);
                info!(
                    "promoted learning {} to SkillConfig {}",
                    learning.id, skill_id
                );
                PromotionResult::Promoted {
                    target: "SkillRegistry".into(),
                    value: skill_id,
                }
            }
            LearningCategory::Correction => {
                // Promote to system prompt adjustment (via SkillConfig).
                let mut registry = self.skill_registry.write().await;
                let skill_id = format!("corr.{:?}.{}", learning.area, learning.id);
                let config = SkillConfig {
                    skill_id: skill_id.clone(),
                    name: format!("Correction Rule: {}", learning.summary),
                    system_prompt: String::new(),
                    tools: vec![],
                    max_iterations: 10,
                    role_type: "correction".into(),
                };
                registry.insert_skill_config(config);
                info!(
                    "promoted correction {} to SkillConfig {}",
                    learning.id, skill_id
                );
                PromotionResult::Promoted {
                    target: "SkillRegistry".into(),
                    value: skill_id,
                }
            }
            LearningCategory::Insight | LearningCategory::KnowledgeGap => {
                // Insights and knowledge gaps become wiki / documentation entries.
                match self.write_to_wiki(learning).await {
                    Ok(path) => {
                        info!("promoted learning {} to wiki entry {}", learning.id, path);
                        PromotionResult::Promoted {
                            target: "Wiki".into(),
                            value: path,
                        }
                    }
                    Err(e) => {
                        warn!(
                            "failed to write learning {} to wiki: {}. Falling back to SkillRegistry.",
                            learning.id, e
                        );
                        // Fallback: write to SkillRegistry as a documentation skill.
                        let mut registry = self.skill_registry.write().await;
                        let skill_id = format!("wiki.{:?}.{}", learning.area, learning.id);
                        let config = SkillConfig {
                            skill_id: skill_id.clone(),
                            name: format!("Wiki: {}", learning.summary),
                            system_prompt: String::new(),
                            tools: vec![],
                            max_iterations: 10,
                            role_type: "documentation".into(),
                        };
                        registry.insert_skill_config(config);
                        PromotionResult::Promoted {
                            target: "SkillRegistry(fallback)".into(),
                            value: skill_id,
                        }
                    }
                }
            }
        };

        if let Some(ref cb) = self.on_promoted {
            cb(&result);
        }

        Ok(result)
    }
}
