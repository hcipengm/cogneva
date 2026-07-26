//! Extract mature patterns into concrete [`SkillConfig`] entries.
//! The [`SkillExtractor`] uses an LLM to turn a [`Pattern`] into a
//! well-formed skill configuration, then runs it through a quality gate
//! before inserting it into the [`SkillRegistry`].

use std::sync::Arc;

use cog_core::{
    ChatOptions, LlmClient, Message, ResponseFormat, SFResult, SkillConfig, SkillRegistry,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use cog_core::Pattern;

/// Extracts skills from mature patterns.
pub struct SkillExtractor {
    llm: Arc<dyn LlmClient>,
    skill_registry: Arc<RwLock<SkillRegistry>>,
    prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
}

impl SkillExtractor {
    pub fn new(
        llm: Arc<dyn LlmClient>,
        skill_registry: Arc<RwLock<SkillRegistry>>,
        prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
    ) -> Self {
        Self {
            llm,
            skill_registry,
            prompt_manager,
        }
    }

    /// Generate a [`SkillConfig`] from a [`Pattern`] using the LLM.
    /// Returns `None` if the LLM cannot produce a valid configuration.
    pub async fn extract_skill(&self, pattern: &Pattern) -> SFResult<Option<SkillConfig>> {
        let prompt = format!(
            "Given the following recurring pattern detected in an AI agent system, \
             produce a valid JSON SkillConfig object.\n\n\
             Pattern: {}\n\
             Related learnings: {}\n\n\
             The JSON must contain exactly these fields:\n\
             - skill_id (string, kebab-case, unique)\n\
             - name (string, descriptive)\n\
             - tools (array of strings)\n\
             - max_iterations (number, default 10)\n\
             - role_type (string, e.g. 'planner', 'evaluator', 'best_practice')",
            pattern.description,
            pattern.learning_ids.join(", ")
        );

        let system_prompt = self
            .prompt_manager
            .as_ref()
            .and_then(|pm| pm.get("reflection:skill_extractor"))
            .unwrap_or_else(|| {
                "You are a skill extraction assistant. Respond with valid JSON only.".into()
            });
        let messages = vec![Message::system(system_prompt), Message::user(prompt)];

        let options = ChatOptions {
            response_format: ResponseFormat::Json,
            ..Default::default()
        };

        let response = self.llm.chat(&messages, &options).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        debug!("LLM skill extraction response: {}", text);

        match serde_json::from_str::<SkillConfig>(&text) {
            Ok(skill) => {
                info!(
                    "extracted skill '{}' from pattern '{}'",
                    skill.skill_id, pattern.key
                );
                Ok(Some(skill))
            }
            Err(e) => {
                warn!("failed to parse extracted skill: {}", e);
                Ok(None)
            }
        }
    }

    /// Quality gate: validate an extracted skill before insertion.
    /// Returns `true` if the skill passes all quality checks.
    pub async fn quality_gate(&self, skill: &SkillConfig) -> SFResult<bool> {
        // 1. ID must be non-empty and kebab-case-like.
        if skill.skill_id.is_empty() || skill.skill_id.contains(' ') {
            debug!("quality gate failed: invalid skill_id '{}'", skill.skill_id);
            return Ok(false);
        }

        // 2. Name must be non-empty.
        if skill.name.is_empty() {
            debug!(
                "quality gate failed: empty name for skill '{}'",
                skill.skill_id
            );
            return Ok(false);
        }

        // 3. max_iterations must be reasonable (1–1000).
        if skill.max_iterations == 0 || skill.max_iterations > 1000 {
            debug!(
                "quality gate failed: max_iterations {} out of range for skill '{}'",
                skill.max_iterations, skill.skill_id
            );
            return Ok(false);
        }

        // 4. role_type must be non-empty.
        if skill.role_type.is_empty() {
            debug!(
                "quality gate failed: empty role_type for skill '{}'",
                skill.skill_id
            );
            return Ok(false);
        }

        // 5. No duplicate skill_id in the registry.
        let registry = self.skill_registry.read().await;
        if registry.get_skill(&skill.skill_id).is_some() {
            debug!(
                "quality gate failed: duplicate skill_id '{}'",
                skill.skill_id
            );
            return Ok(false);
        }

        info!("quality gate passed for skill '{}'", skill.skill_id);
        Ok(true)
    }

    /// Extract skill + quality gate + insert into registry in one shot.
    /// Returns `Some(skill_id)` on success.
    pub async fn extract_and_insert(&self, pattern: &Pattern) -> SFResult<Option<String>> {
        if let Some(skill) = self.extract_skill(pattern).await? {
            if self.quality_gate(&skill).await? {
                let id = skill.skill_id.clone();
                let mut registry = self.skill_registry.write().await;
                registry.insert_skill_config(skill);
                info!("inserted extracted skill '{}' into registry", id);
                return Ok(Some(id));
            }
        }
        Ok(None)
    }
}
