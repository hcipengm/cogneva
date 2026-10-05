//! Controlled evolution engine — L1 soft-code evolution + L2 source-code change
//! generation (with human/CI gate).
//! L1 (automatic, low risk):
//!   - Refine an existing skill based on accumulated learnings/errors.
//!   - Synthesize hook definitions from observed event patterns.
//!   - Suggest tool variants from error signatures.
//!
//! L2 (semi-automatic, medium risk):
//!   - Generate Rust code changes and write them to `evolution-changes/`.
//!   - The system **never** auto-merges into `main`; changes await review.

use std::sync::Arc;

use chrono::Utc;
use cog_core::{
    ChatOptions, LlmClient, Message, ResponseFormat, SFResult, SkillConfig, SkillRegistry,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{EvolutionKind, EvolutionResult, EvolutionStatus};

/// What each prompt says when no prompt manager is wired into the engine.
///
/// These are not copies kept for their own sake. The same instruction has to
/// hold on both paths — the declared prompt a running process reads and the
/// text the code sends when there is nothing to read — and a side that is
/// edited alone is how a declaration stops describing the run. The guard in
/// this file's tests renders every declared key and compares it to the text
/// here byte for byte, so an edit to either side that is not mirrored fails the
/// suite instead of shipping as a quiet difference.
mod fallback {
    /// Skill refinement: asks for an improved `SkillConfig` for one skill.
    pub fn skill_refinement(skill_json: &str, skill_id: &str) -> String {
        format!(
            "You are evolving an existing agent skill based on production feedback.\n\n\
             Current skill:\n{}\n\n\
             Generate an improved version as valid JSON with the same schema:\n\
             - skill_id (keep identical: '{}')\n\
             - name (improved if needed)\n\
             - tools (add/remove based on learnings)\n\
             - max_iterations (tune if needed)\n\
             - role_type (keep or refine)\n\
             - system_prompt (the actual prompt text that guides the agent)",
            skill_json, skill_id
        )
    }

    /// Tool variant suggestion: the error patterns observed plus the field list
    /// the caller parses back out of the reply.
    pub fn tool_variant(tool_name: &str, error_patterns: &str) -> String {
        format!(
            "You are a tool design expert for an AI agent system.\n\n\
             Existing tool: {}\n\n\
             Observed error patterns:\n- {}\n\n\
             Generate an improved tool variant as JSON with these fields:\n\
             - name: tool name (suggest a new name like {}_v2 or {}_improved)\n\
             - description: concise description of what the tool does\n\
             - parameters: valid JSON Schema object describing input parameters\n\
             - implementation_hint: string describing implementation approach (e.g., \"native\", \"wasm\", \"rhai\")\n\n\
             Respond with ONLY the JSON object.",
            tool_name, error_patterns, tool_name, tool_name
        )
    }

    pub const SKILL_REFINEMENT_SYSTEM: &str = "Respond with valid JSON SkillConfig only.";
    pub const TOOL_VARIANT_SYSTEM: &str = "Respond with valid JSON tool definition only.";
}

/// Engine that drives controlled self-evolution of the system.
pub struct EvolutionEngine {
    llm: Arc<dyn LlmClient>,
    skill_registry: Arc<RwLock<SkillRegistry>>,
    change_dir: std::path::PathBuf,
    prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
    /// Optional channel to notify an external [`HookEngine`] that a new hook
    /// has been synthesized.  When present the hook JSON is sent immediately
    /// after it passes validation and is written to disk.
    hook_sink: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>>,
    /// Optional channel to notify an external [`ToolRegistry`] that a new tool
    /// variant has been suggested.
    tool_sink: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>>,
    /// Project root for project-context compilation checks.
    /// When `Some`, a change's target paths are validated against the real
    /// workspace instead of an isolated temp crate.
    project_root: Option<std::path::PathBuf>,
    /// In-memory log of all evolution attempts and their current status.
    /// Production systems may additionally persist this to a backend.
    results: Arc<tokio::sync::Mutex<std::collections::HashMap<String, EvolutionResult>>>,
}

/// Hard upper bound on the number of [`EvolutionResult`] entries kept in the
/// engine's in-memory log.
///
/// Each result carries the full text of the artifact it produced in
/// `content`, and the log had no reclaimer: every generation branch inserted
/// for the pod's whole lifetime, so resident history grew with uptime until
/// the evolution worker's normal burst amplitude carried its working set into
/// the OOM band. Durable history is already written to disk (`change_dir`)
/// and the recorder/state backend, so the resident log only needs recent
/// entries and the bound makes worst-case resident usage independent of how
/// many rollout cycles the pod lives through.
pub(crate) const MAX_RESULTS: usize = 256;

/// Evict the results with the earliest timestamps until `results` holds at
/// most [`MAX_RESULTS`] entries. Returns how many entries were dropped.
///
/// This is the only eviction logic in the engine and it runs under the
/// caller's results lock, so the bound cannot be bypassed by a new branch.
fn evict_oldest(results: &mut std::collections::HashMap<String, EvolutionResult>) -> usize {
    let mut evicted = 0;
    while results.len() > MAX_RESULTS {
        let Some(key) = results
            .iter()
            .min_by_key(|(_, value)| value.created_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        results.remove(&key);
        evicted += 1;
    }
    evicted
}

impl std::fmt::Debug for EvolutionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let has_hook_sink = self.hook_sink.lock().map(|g| g.is_some()).unwrap_or(false);
        let has_tool_sink = self.tool_sink.lock().map(|g| g.is_some()).unwrap_or(false);
        f.debug_struct("EvolutionEngine")
            .field("change_dir", &self.change_dir)
            .field("has_hook_sink", &has_hook_sink)
            .field("has_tool_sink", &has_tool_sink)
            .field("project_root", &self.project_root)
            .finish()
    }
}

impl EvolutionEngine {
    pub fn new(
        llm: Arc<dyn LlmClient>,
        skill_registry: Arc<RwLock<SkillRegistry>>,
        prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
    ) -> Self {
        Self {
            llm,
            skill_registry,
            change_dir: std::path::PathBuf::from("evolution-changes"),
            prompt_manager,
            hook_sink: std::sync::Mutex::new(None),
            tool_sink: std::sync::Mutex::new(None),
            project_root: None,
            results: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Set the directory where code changes are written (default:
    /// `./evolution-changes`).
    pub fn with_change_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.change_dir = dir.into();
        self
    }

    /// Inject a channel sink so that synthesized hooks can be auto-registered
    /// by an external hook engine.
    pub fn with_hook_sink(self, tx: tokio::sync::mpsc::UnboundedSender<serde_json::Value>) -> Self {
        if let Ok(mut guard) = self.hook_sink.lock() {
            *guard = Some(tx);
        }
        self
    }

    /// Inject a channel sink so that suggested tool variants can be
    /// auto-registered by an external tool registry.
    pub fn with_tool_sink(self, tx: tokio::sync::mpsc::UnboundedSender<serde_json::Value>) -> Self {
        if let Ok(mut guard) = self.tool_sink.lock() {
            *guard = Some(tx);
        }
        self
    }

    /// Set the project root for real-workspace compilation checks.
    pub fn with_project_root(mut self, root: impl Into<std::path::PathBuf>) -> Self {
        self.project_root = Some(root.into());
        self
    }

    /// Register an externally-produced evolution result (e.g. artifact-level
    /// policy proposals from `ArtifactEvolution`) into the engine's registry
    /// so it appears in `list_results` / the admin change table.
    /// Same `artifact_id` re-registers (latest proposal wins).
    pub async fn register_result(&self, result: EvolutionResult) {
        self.insert_result(result).await;
    }

    /// The single bounded insertion path for [`Self::results`].
    ///
    /// Every result this engine produces is stored through here so the
    /// [`MAX_RESULTS`] cap holds on every branch: after the entry is stored,
    /// oldest entries are evicted under the same lock. Same `artifact_id`
    /// re-registers (latest proposal wins).
    async fn insert_result(&self, result: EvolutionResult) {
        let mut results = self.results.lock().await;
        results.insert(result.artifact_id.clone(), result);
        evict_oldest(&mut results);
    }

    /// Update the status of an evolution result by `artifact_id`.
    /// Returns `true` if the result was found and updated.
    pub async fn update_status(&self, artifact_id: &str, status: EvolutionStatus) -> bool {
        let mut results = self.results.lock().await;
        if let Some(r) = results.get_mut(artifact_id) {
            let old = r.status;
            r.status = status;
            info!(
                artifact_id = %artifact_id,
                old_status = ?old,
                new_status = ?status,
                "Evolution result status updated"
            );
            true
        } else {
            warn!(
                artifact_id = %artifact_id,
                "Evolution result not found for status update"
            );
            false
        }
    }

    /// List all evolution results ordered from newest to oldest.
    pub async fn list_results(&self) -> Vec<EvolutionResult> {
        let results = self.results.lock().await;
        let mut list: Vec<EvolutionResult> = results.values().cloned().collect();
        list.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        list
    }

    // ========================================================================
    // L1 — Skill Refinement
    // ========================================================================

    /// Refine an existing skill by feeding its accumulated learnings and
    /// errors into the LLM and generating an improved `SkillConfig`.
    pub async fn refine_skill(&self, skill_id: &str) -> SFResult<Option<EvolutionResult>> {
        let existing = {
            let reg = self.skill_registry.read().await;
            reg.get_skill(skill_id).cloned()
        };

        let skill = match existing {
            Some(s) => s,
            None => {
                warn!(skill_id, "Cannot refine: skill not found in registry");
                return Ok(None);
            }
        };

        let skill_json = serde_json::to_string_pretty(&skill).unwrap_or_default();
        let prompt = {
            let mut vars = std::collections::HashMap::new();
            vars.insert("skill_json".to_string(), skill_json.clone());
            vars.insert("skill_id".to_string(), skill_id.to_string());
            self.prompt_manager
                .as_ref()
                .and_then(|pm| pm.render("reflection:evolution_refinement", &vars).ok())
                .unwrap_or_else(|| fallback::skill_refinement(&skill_json, skill_id))
        };

        let system_prompt = self
            .prompt_manager
            .as_ref()
            .and_then(|pm| pm.get("reflection:evolution_refinement_system"))
            .unwrap_or_else(|| fallback::SKILL_REFINEMENT_SYSTEM.into());

        let messages = vec![Message::system(system_prompt), Message::user(prompt)];

        let options = ChatOptions {
            response_format: ResponseFormat::Json,
            ..Default::default()
        }
        .with_actor("evolution");

        let response = self.llm.chat(&messages, &options).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        debug!(skill_id, "Refinement LLM response: {}", text);

        match serde_json::from_str::<SkillConfig>(&text) {
            Ok(mut improved) => {
                // Force the skill_id to remain the same so we overwrite rather
                // than create a duplicate.
                improved.skill_id = skill_id.to_string();

                if self.quality_gate(&improved).await? {
                    let mut reg = self.skill_registry.write().await;
                    reg.insert_skill_config(improved);
                    info!(skill_id, "Refined skill inserted into registry");
                    let result = EvolutionResult {
                        kind: EvolutionKind::SkillRefinement,
                        artifact_id: skill_id.to_string(),
                        description: "LLM-refined skill config".into(),
                        content: text,
                        status: EvolutionStatus::Generated,
                        created_at: Utc::now(),
                        eval_summary: None,
                    };
                    self.insert_result(result.clone()).await;
                    Ok(Some(result))
                } else {
                    warn!(skill_id, "Refined skill failed quality gate");
                    let result = EvolutionResult {
                        kind: EvolutionKind::SkillRefinement,
                        artifact_id: skill_id.to_string(),
                        description: "Refined skill failed quality gate".into(),
                        content: text,
                        status: EvolutionStatus::ValidationFailed,
                        created_at: Utc::now(),
                        eval_summary: None,
                    };
                    self.insert_result(result.clone()).await;
                    Ok(Some(result))
                }
            }
            Err(e) => {
                warn!(skill_id, "Failed to parse refined skill: {}", e);
                let result = EvolutionResult {
                    kind: EvolutionKind::SkillRefinement,
                    artifact_id: skill_id.to_string(),
                    description: format!("Failed to parse refined skill: {}", e),
                    content: text,
                    status: EvolutionStatus::ValidationFailed,
                    created_at: Utc::now(),
                    eval_summary: None,
                };
                self.insert_result(result.clone()).await;
                Ok(Some(result))
            }
        }
    }

    // ========================================================================
    // L1 — Hook Synthesis
    // ========================================================================

    /// Synthesize a [`cog_core::HookDef`] from a recurring event pattern using the LLM.
    /// The generated hook is written to `change_dir/hooks/{id}.json` and can be
    /// loaded by the caller into a hook engine.
    pub async fn synthesize_hook(
        &self,
        event_pattern: &str,
        action_outcomes: &[String],
    ) -> SFResult<Option<EvolutionResult>> {
        let outcomes_text = action_outcomes.join("\n- ");
        // 这条提示词只有一个持有者，就是这里。它**不**进 `prompts/system_prompts.yaml`：
        // 正文里指令要模型照抄的动作参数形状（`{url, headers?}`、`{channel}`…）与模板
        // 引擎的变量起始符同形，抄进 YAML 就不再是同一段字——它会先被当成表达式。那时
        // 要么改正文（那是改行为），要么留下一份永远渲染失败的声明（那是假声明）。
        // 此前的调用正是后者：key 从未被声明 ⇒ `render` 每次都失败 ⇒ 这里每次都执行。
        let prompt = format!(
            "You are a hook synthesis expert for an AI agent system.\n\n\
             Observed event pattern:\n{}\n\n\
             Action outcomes:\n- {}\n\n\
             Generate a hook definition as JSON with these fields:\n\
             - id: unique hook identifier (use only lowercase, numbers, hyphens)\n\
             - trigger: one of [on_agent_start, on_agent_end, on_task_complete, on_task_fail, on_crew_complete, on_ralph_pass, on_ralph_unrecoverable, on_squad_retry]\n\
             - scope: one of [global, crew, squad] (default: global)\n\
             - action: object with \"type\" and required fields. Types:\n\
               - webhook {{url, headers?}}\n\
               - redis_stream {{channel}}\n\
               - log {{level: trace|debug|info|warn|error}}\n\
               - notify {{user_id}}\n\
             - rate_limit: optional {{burst, per_second}}\n\
             - timeout_ms: optional integer\n\n\
             Respond with ONLY the JSON object.",
            event_pattern, outcomes_text
        );

        let system_prompt = "Respond with valid JSON HookDef only.".to_string();

        let messages = vec![Message::system(system_prompt), Message::user(prompt)];

        let options = ChatOptions {
            response_format: ResponseFormat::Json,
            temperature: Some(0.3),
            max_tokens: Some(512),
            ..Default::default()
        }
        .with_actor("evolution");

        let response = self.llm.chat(&messages, &options).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        let json_str = Self::extract_json(&text);

        // 答复拿到了、只是这条输入下的回答解不出来——内容类失败，不能记成
        // 上游故障：那会把死信拖成无限延后重投。
        let hook_json: serde_json::Value =
            serde_json::from_str(json_str).map_err(cog_core::SFError::Serialization)?;

        // Validate required fields.
        let id = hook_json.get("id").and_then(|v| v.as_str());
        let trigger = hook_json.get("trigger").and_then(|v| v.as_str());
        let action = hook_json.get("action");
        let (Some(id_str), Some(trigger_str), Some(_action)) = (id, trigger, action) else {
            let result = EvolutionResult {
                kind: EvolutionKind::HookSynthesis,
                artifact_id: format!(
                    "hook-{}",
                    uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
                ),
                description: format!("Hook synthesis for pattern: {}", event_pattern),
                content: text,
                status: EvolutionStatus::ValidationFailed,
                created_at: Utc::now(),
                eval_summary: None,
            };
            self.insert_result(result.clone()).await;
            return Ok(Some(result));
        };

        // Validate trigger value against known enum.
        let valid_triggers = [
            "on_agent_start",
            "on_agent_end",
            "on_task_complete",
            "on_task_fail",
            "on_crew_complete",
            "on_ralph_pass",
            "on_ralph_unrecoverable",
            "on_squad_retry",
        ];
        if !valid_triggers.contains(&trigger_str) {
            let result = EvolutionResult {
                kind: EvolutionKind::HookSynthesis,
                artifact_id: format!(
                    "hook-{}",
                    uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
                ),
                description: format!(
                    "Invalid trigger '{}': must be one of {:?}",
                    trigger_str, valid_triggers
                ),
                content: text,
                status: EvolutionStatus::ValidationFailed,
                created_at: Utc::now(),
                eval_summary: None,
            };
            self.insert_result(result.clone()).await;
            return Ok(Some(result));
        }

        let artifact_id = id_str.to_string();

        // Write to hooks directory.
        let hook_dir = cog_core::config::self_evolution_hook_dir(&self.change_dir);
        let filename = hook_dir.join(format!("{}.json", artifact_id));
        if let Some(parent) = filename.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        tokio::fs::write(
            &filename,
            serde_json::to_string_pretty(&hook_json).unwrap_or_default(),
        )
        .await
        .map_err(|e| {
            cog_core::SFError::IO(format!(
                "Failed to write hook {}: {}",
                filename.display(),
                e
            ))
        })?;

        info!(
            artifact_id = %artifact_id,
            path = %filename.display(),
            "Synthesized hook written to disk"
        );

        // Notify external registrar (e.g. HookEngine) so the hook becomes
        // active immediately without waiting for a file-system scan.
        if let Ok(guard) = self.hook_sink.lock() {
            if let Some(ref tx) = *guard {
                let _ = tx.send(hook_json);
            }
        }

        let result = EvolutionResult {
            kind: EvolutionKind::HookSynthesis,
            artifact_id,
            description: format!("Synthesized hook for pattern: {}", event_pattern),
            content: text,
            status: EvolutionStatus::Generated,
            created_at: Utc::now(),
            eval_summary: None,
        };
        self.insert_result(result.clone()).await;
        Ok(Some(result))
    }

    // ========================================================================
    // L1 — Tool Variant Suggestion
    // ========================================================================

    /// Suggest an improved tool variant based on error patterns using the LLM.
    /// The generated tool schema is written to `change_dir/tools/{name}.json` and
    /// can be registered by the caller into a tool registry.
    pub async fn suggest_tool_variant(
        &self,
        tool_name: &str,
        error_patterns: &[String],
    ) -> SFResult<Option<EvolutionResult>> {
        let errors_text = error_patterns.join("\n- ");
        let prompt = {
            let mut vars = std::collections::HashMap::new();
            vars.insert("tool_name".to_string(), tool_name.to_string());
            vars.insert("error_patterns".to_string(), errors_text.clone());
            self.prompt_manager
                .as_ref()
                .and_then(|pm| pm.render("reflection:evolution_tool", &vars).ok())
                .unwrap_or_else(|| fallback::tool_variant(tool_name, &errors_text))
        };

        let system_prompt = self
            .prompt_manager
            .as_ref()
            .and_then(|pm| pm.get("reflection:evolution_tool_system"))
            .unwrap_or_else(|| fallback::TOOL_VARIANT_SYSTEM.into());

        let messages = vec![Message::system(system_prompt), Message::user(prompt)];

        let options = ChatOptions {
            response_format: ResponseFormat::Json,
            temperature: Some(0.3),
            max_tokens: Some(512),
            ..Default::default()
        }
        .with_actor("evolution");

        let response = self.llm.chat(&messages, &options).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        let json_str = Self::extract_json(&text);

        let tool_json: serde_json::Value =
            serde_json::from_str(json_str).map_err(cog_core::SFError::Serialization)?;

        let name = tool_json.get("name").and_then(|v| v.as_str());
        let description = tool_json.get("description").and_then(|v| v.as_str());
        let parameters = tool_json.get("parameters");
        let (Some(name_str), Some(_description_str), Some(params)) =
            (name, description, parameters)
        else {
            let result = EvolutionResult {
                kind: EvolutionKind::ToolVariant,
                artifact_id: format!("{}-v2", tool_name),
                description: format!("Tool variant suggestion for {}", tool_name),
                content: text,
                status: EvolutionStatus::ValidationFailed,
                created_at: Utc::now(),
                eval_summary: None,
            };
            self.insert_result(result.clone()).await;
            return Ok(Some(result));
        };

        // Validate that parameters looks like a JSON Schema object.
        {
            if params.get("type").and_then(|v| v.as_str()) != Some("object") {
                let result = EvolutionResult {
                    kind: EvolutionKind::ToolVariant,
                    artifact_id: format!("{}-v2", tool_name),
                    description: format!("Tool '{}' parameters must have type='object'", tool_name),
                    content: text.clone(),
                    status: EvolutionStatus::ValidationFailed,
                    created_at: Utc::now(),
                    eval_summary: None,
                };
                self.insert_result(result.clone()).await;
                return Ok(Some(result));
            }
            if !params
                .get("properties")
                .map(|v| v.is_object())
                .unwrap_or(false)
            {
                let result = EvolutionResult {
                    kind: EvolutionKind::ToolVariant,
                    artifact_id: format!("{}-v2", tool_name),
                    description: format!(
                        "Tool '{}' parameters must have a 'properties' object",
                        tool_name
                    ),
                    content: text,
                    status: EvolutionStatus::ValidationFailed,
                    created_at: Utc::now(),
                    eval_summary: None,
                };
                self.insert_result(result.clone()).await;
                return Ok(Some(result));
            }
        }

        let artifact_id = name_str.to_string();

        // Write to tools directory.
        let tool_dir = self.change_dir.join("tools");
        let filename = tool_dir.join(format!("{}.json", artifact_id));
        if let Some(parent) = filename.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        tokio::fs::write(
            &filename,
            serde_json::to_string_pretty(&tool_json).unwrap_or_default(),
        )
        .await
        .map_err(|e| {
            cog_core::SFError::IO(format!(
                "Failed to write tool {}: {}",
                filename.display(),
                e
            ))
        })?;

        info!(
            artifact_id = %artifact_id,
            path = %filename.display(),
            "Suggested tool variant written to disk"
        );

        // Notify external registrar (e.g. ToolRegistry) so the tool becomes
        // discoverable immediately.
        if let Ok(guard) = self.tool_sink.lock() {
            if let Some(ref tx) = *guard {
                let _ = tx.send(tool_json);
            }
        }

        let result = EvolutionResult {
            kind: EvolutionKind::ToolVariant,
            artifact_id,
            description: format!("Improved variant of tool {}", tool_name),
            content: text,
            status: EvolutionStatus::Generated,
            created_at: Utc::now(),
            eval_summary: None,
        };
        self.insert_result(result.clone()).await;
        Ok(Some(result))
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Extract JSON from text, handling markdown fences.
    fn extract_json(text: &str) -> &str {
        let trimmed = text.trim();
        if trimmed.starts_with("```json") {
            trimmed
                .trim_start_matches("```json")
                .trim_end_matches("```")
                .trim()
        } else if trimmed.starts_with("```") {
            trimmed
                .trim_start_matches("```")
                .trim_end_matches("```")
                .trim()
        } else {
            trimmed
        }
    }
    // ========================================================================
    // Quality Gate (mirrors SkillExtractor)
    // ========================================================================

    async fn quality_gate(&self, skill: &SkillConfig) -> SFResult<bool> {
        if skill.skill_id.is_empty() || skill.skill_id.contains(' ') {
            return Ok(false);
        }
        if skill.name.is_empty() {
            return Ok(false);
        }
        if skill.max_iterations == 0 || skill.max_iterations > 1000 {
            return Ok(false);
        }
        if skill.role_type.is_empty() {
            return Ok(false);
        }
        // Allow duplicates during refinement because we intentionally overwrite.
        Ok(true)
    }

    // ========================================================================
    // ChangeSink implementation — receive collaboration-generated .diff files
    // ========================================================================

    fn sanitize_artifact_id(&self, change_id: &str) -> String {
        let safe: String = change_id
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(c, '.' | '_' | '-'))
            .collect();
        if safe.is_empty() || safe == ".diff" {
            let now = chrono::Utc::now();
            return format!(
                "evo-{}-{}",
                now.format("%Y%m%d"),
                uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
            );
        }
        safe
    }

    fn validate_change_paths(
        &self,
        files: &[std::path::PathBuf],
        project_root: Option<&std::path::Path>,
    ) -> cog_core::SFResult<()> {
        let canonical_root = project_root.and_then(|r| r.canonicalize().ok());

        for file in files {
            let path = std::path::Path::new(file);

            if let Some(reason) = cog_core::forbidden_target_reason(&file.to_string_lossy()) {
                return Err(cog_core::SFError::Validation(reason));
            }

            // When a project root is known, verify the target resolves inside it.
            if let Some(ref root) = canonical_root {
                let absolute = root.join(path);
                if let Ok(canonical) = absolute.canonicalize() {
                    if !canonical.starts_with(root) {
                        return Err(cog_core::SFError::Validation(format!(
                            "Target path escapes project root: {}",
                            file.display()
                        )));
                    }
                }
            }

            // Strongly encourage modifications under src/.
            if !path.to_string_lossy().replace('\\', "/").contains("/src/") {
                tracing::warn!(
                    target = %file.display(),
                    "Change target is outside a src directory; allowed but unusual"
                );
            }
        }

        Ok(())
    }

    /// Write a collaboration-generated change to disk and register it as an
    /// EvolutionResult with status `CompileChecked`.
    async fn write_generated_change(
        &self,
        change: &cog_core::GeneratedChange,
    ) -> cog_core::SFResult<String> {
        let artifact_id = self.sanitize_artifact_id(&change.change_id);

        // This sink writes the bytes that reach the pipeline verbatim, so it is
        // the last place a structural defect can be derived back from the body
        // rather than handed to the apply gate.
        let content = repair_generated_diff(&change.content);

        // Derive affected files from the change content if not supplied.
        let affected_files: Vec<std::path::PathBuf> = if change.affected_files.is_empty() {
            cog_core::parse_diff_affected_files(&content)?
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect()
        } else {
            change
                .affected_files
                .iter()
                .map(std::path::PathBuf::from)
                .collect()
        };

        self.validate_change_paths(&affected_files, self.project_root.as_deref())?;

        let filename = self.change_dir.join(format!("{}.diff", artifact_id));
        if let Some(parent) = filename.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                cog_core::SFError::IO(format!(
                    "Failed to create change dir {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        tokio::fs::write(&filename, &content).await.map_err(|e| {
            cog_core::SFError::IO(format!(
                "Failed to write change {}: {}",
                filename.display(),
                e
            ))
        })?;

        info!(
            artifact_id = %artifact_id,
            path = %filename.display(),
            "Collaboration-generated change written to disk"
        );

        let result = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: artifact_id.clone(),
            description: change.goal.clone(),
            content: content.clone(),
            status: EvolutionStatus::CompileChecked,
            created_at: Utc::now(),
            eval_summary: None,
        };
        self.insert_result(result).await;

        Ok(artifact_id)
    }
}

#[async_trait::async_trait]
impl cog_core::ChangeSink for EvolutionEngine {
    async fn submit_change(&self, change: cog_core::GeneratedChange) -> cog_core::SFResult<String> {
        self.write_generated_change(&change).await
    }
}

/// Repair a generated diff's own structure, leaving what it says untouched.
///
/// The `@@` line counts and the final terminator depend on nothing but the
/// body beneath them, so a generator that writes a correct body and miscounts
/// its header — or closes the last line without a terminator, which `git apply`
/// rejects as "corrupt patch at line N" — can have both derived for no tokens.
/// Re-prompting instead tends to reproduce them, because the gate can name a
/// line number but not the mistake. A diff that is already sound comes back
/// byte for byte, so this can never turn a working patch into a broken one.
fn repair_generated_diff(diff: &str) -> String {
    match cog_core::normalize_diff_hunk_headers(diff) {
        Some(repaired) => {
            info!(
                defect = ?cog_core::diff_structural_defect(diff),
                "repaired diff structure from the body before validation"
            );
            repaired
        }
        None => diff.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_strips_markdown_fences() {
        assert_eq!(
            EvolutionEngine::extract_json("```json\n{\"a\":1}\n```"),
            "{\"a\":1}"
        );
        assert_eq!(
            EvolutionEngine::extract_json("```\n{\"a\":1}\n```"),
            "{\"a\":1}"
        );
        assert_eq!(EvolutionEngine::extract_json("{\"a\":1}"), "{\"a\":1}");
    }

    #[test]
    fn a_diff_the_generator_left_unterminated_is_repaired_before_the_gate() {
        // The shape a live run handed to the apply gate, which rejected it as
        // "corrupt patch at line 9": the body is right, the last line has no
        // terminator.
        let text = "diff --git a/crates/bootstrap/src/main.rs b/crates/bootstrap/src/main.rs\n\
                    --- a/crates/bootstrap/src/main.rs\n\
                    +++ b/crates/bootstrap/src/main.rs\n\
                    @@ -411,1 +411,1 @@\n\
                    -        .map_or(false, |status| status.success());\n\
                    +        .is_ok_and(|status| status.success());";
        assert!(
            cog_core::diff_structural_defect(text).is_some(),
            "the sample no longer carries the defect this test is about"
        );

        let repaired = repair_generated_diff(text);
        assert_eq!(cog_core::diff_structural_defect(&repaired), None);
        assert!(repaired.ends_with('\n'));
        // The repair derives the terminator and changes nothing the diff says.
        assert!(repaired.contains("+        .is_ok_and(|status| status.success());"));
        assert!(repaired.contains("-        .map_or(false, |status| status.success());"));
    }

    #[test]
    fn a_hunk_header_that_disagrees_with_its_body_is_repaired_from_the_body() {
        // Two lines in the body, one declared. `git apply` names this as
        // "corrupt patch" too, and the counts are pure arithmetic over a body
        // the model already paid for.
        let diff = "diff --git a/x.rs b/x.rs\n\
                    --- a/x.rs\n\
                    +++ b/x.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    -a\n\
                    -b\n\
                    +c\n\
                    +d\n";
        assert!(cog_core::diff_structural_defect(diff).is_some());
        let repaired = repair_generated_diff(diff);
        assert_eq!(cog_core::diff_structural_defect(&repaired), None);
    }

    fn make_result(id: &str, created_at: chrono::DateTime<chrono::Utc>) -> EvolutionResult {
        EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: id.to_string(),
            description: String::new(),
            content: format!("content-{id}"),
            status: EvolutionStatus::Generated,
            created_at,
            eval_summary: None,
        }
    }

    #[test]
    fn results_at_or_under_the_cap_keep_every_entry() {
        let mut results = std::collections::HashMap::new();
        for i in 0..MAX_RESULTS {
            let id = format!("r-{i}");
            results.insert(id.clone(), make_result(&id, chrono::Utc::now()));
        }
        assert_eq!(
            evict_oldest(&mut results),
            0,
            "nothing at or below the cap may be evicted"
        );
        assert_eq!(results.len(), MAX_RESULTS);
    }

    #[test]
    fn results_beyond_the_cap_drop_the_oldest_entries() {
        let mut results = std::collections::HashMap::new();
        let now = chrono::Utc::now();
        let total = MAX_RESULTS + 5;
        for i in 0..total {
            let id = format!("r-{i}");
            results.insert(
                id.clone(),
                make_result(&id, now + chrono::Duration::milliseconds(i as i64)),
            );
        }
        assert_eq!(
            evict_oldest(&mut results),
            5,
            "the five over-cap entries go"
        );
        assert_eq!(results.len(), MAX_RESULTS, "the cap is a hard bound");
        assert!(
            !results.contains_key("r-4"),
            "the earliest results must be evicted"
        );
        assert!(
            results.contains_key("r-5"),
            "entries newer than the cap floor must survive"
        );
        assert!(results.contains_key(&format!("r-{}", total - 1)));
    }

    #[test]
    fn a_sound_diff_comes_back_byte_for_byte() {
        let diff = "diff --git a/x.rs b/x.rs\n\
                    --- a/x.rs\n\
                    +++ b/x.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    -a\n\
                    +b\n";
        assert_eq!(repair_generated_diff(diff), diff);
    }
}

/// The declared prompt and the code's fallback are one instruction carried
/// twice, and until this guard existed nothing compared them.
///
/// The paths diverge in a way no other gate sees: the code path is the one the
/// tests exercise (every test builds the engine without a prompt manager, so
/// the fallback is what runs), while the declared path is the one production
/// runs (the ConfigMap is what a deployed process loads). An edit to either
/// side alone therefore passes every behavioural test and changes what the
/// model is told in production only.
#[cfg(test)]
mod declared_prompt_matches_fallback {
    use super::fallback;
    use cog_prompt::{PromptManager, TemplateVars, WatchMode};

    /// Every key this guard compares. The list is checked against what
    /// `prompts/` actually declares in both directions, so a declaration added
    /// or removed without touching this guard fails here rather than dropping
    /// out of coverage.
    const COMPARED: [&str; 6] = [
        "agent:default",
        "reflection:evolution_refinement",
        "reflection:evolution_refinement_system",
        "reflection:evolution_tool",
        "reflection:evolution_tool_system",
        "reflection:skill_extractor",
    ];

    /// Load `prompts/` the way a process does: from the directory as shipped,
    /// not from a copy written by the test.
    async fn manager() -> PromptManager {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../prompts");
        PromptManager::from_dir(dir, WatchMode::None)
            .await
            .unwrap_or_else(|e| panic!("prompts/ must load from {dir}: {e}"))
    }

    /// A `|` block scalar keeps one trailing newline; the code's literal has
    /// none. That byte is YAML's, not the instruction's, so it is not a
    /// difference in what the model is told.
    fn same_instruction(rendered: &str, text: &str) -> bool {
        rendered.trim_end_matches('\n') == text.trim_end_matches('\n')
    }

    fn refinement_vars() -> (TemplateVars, &'static str, &'static str) {
        let json = "{\"skill_id\":\"sk-1\"}";
        let id = "sk-1";
        (
            TemplateVars::new()
                .with("skill_json", json)
                .with("skill_id", id),
            json,
            id,
        )
    }

    fn tool_vars() -> (TemplateVars, &'static str, &'static str) {
        let name = "read_file";
        let errors = "timeout\nbad path";
        (
            TemplateVars::new()
                .with("tool_name", name)
                .with("error_patterns", errors),
            name,
            errors,
        )
    }

    #[tokio::test]
    async fn every_declared_prompt_is_the_text_the_code_falls_back_to() {
        let pm = manager().await;
        let mut keys: Vec<String> = {
            let reg = pm.registry.read().expect("registry readable");
            reg.keys().into_iter().cloned().collect()
        };
        keys.sort();
        let compared: Vec<String> = COMPARED.iter().map(|k| k.to_string()).collect();
        assert_eq!(
            keys, compared,
            "prompts/ declares a different set of keys than this guard compares"
        );

        // Each arm reads its key as a literal at the call rather than passing the
        // loop's variable down: the workspace-wide sweep that reconciles every
        // prompt call site against `prompts/` reads the key off the call, and a
        // call it cannot read is a call it cannot check.
        for key in COMPARED {
            // Bootstrap entry owned by cog-prompt; this crate never asks for it
            // and so has no fallback to compare against.
            if key == "agent:default" {
                continue;
            }
            let pairs: Vec<(String, String)> = match key {
                "reflection:skill_extractor" => vec![(
                    pm.get("reflection:skill_extractor").expect("declared"),
                    crate::extractor::SKILL_EXTRACTOR_SYSTEM.to_string(),
                )],
                "reflection:evolution_refinement" => {
                    let (vars, json, id) = refinement_vars();
                    vec![(
                        pm.render("reflection:evolution_refinement", &vars)
                            .expect("renders"),
                        fallback::skill_refinement(json, id),
                    )]
                }
                "reflection:evolution_refinement_system" => vec![(
                    pm.get("reflection:evolution_refinement_system")
                        .expect("declared"),
                    fallback::SKILL_REFINEMENT_SYSTEM.to_string(),
                )],
                "reflection:evolution_tool" => {
                    let (vars, name, errors) = tool_vars();
                    vec![(
                        pm.render("reflection:evolution_tool", &vars)
                            .expect("renders"),
                        fallback::tool_variant(name, errors),
                    )]
                }
                "reflection:evolution_tool_system" => vec![(
                    pm.get("reflection:evolution_tool_system")
                        .expect("declared"),
                    fallback::TOOL_VARIANT_SYSTEM.to_string(),
                )],
                other => panic!(
                    "{other} is declared in prompts/ and this guard has no case for \
                     it. If cog-reflection falls back for it, compare that text \
                     here; if another crate reads it, name that crate in this arm."
                ),
            };
            assert!(
                !pairs.is_empty(),
                "{key} produced no comparison, so its arm asserts nothing"
            );
            for (declared, expected) in pairs {
                assert!(
                    same_instruction(&declared, &expected),
                    "{key}: the declared instruction and the code's fallback differ"
                );
            }
        }
    }
}
