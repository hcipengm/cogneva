//! Controlled evolution engine — L1 soft-code evolution + L2 source-code patch
//! generation (with human/CI gate).
//! L1 (automatic, low risk):
//!   - Refine an existing skill based on accumulated learnings/errors.
//!   - Synthesize hook definitions from observed event patterns.
//!   - Suggest tool variants from error signatures.
//!
//! L2 (semi-automatic, medium risk):
//!   - Generate Rust code patches and write them to `evolution-patches/`.
//!   - The system **never** auto-merges into `main`; patches await review.

use std::sync::Arc;

use chrono::Utc;
use cog_core::{
    ChatOptions, LlmClient, Message, ResponseFormat, SFResult, SkillConfig, SkillRegistry,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{EvolutionKind, EvolutionResult, EvolutionStatus};

/// Engine that drives controlled self-evolution of the system.
pub struct EvolutionEngine {
    llm: Arc<dyn LlmClient>,
    skill_registry: Arc<RwLock<SkillRegistry>>,
    patch_dir: std::path::PathBuf,
    prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
    /// Optional channel to notify an external [`HookEngine`] that a new hook
    /// has been synthesized.  When present the hook JSON is sent immediately
    /// after it passes validation and is written to disk.
    hook_sink: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>>,
    /// Optional channel to notify an external [`ToolRegistry`] that a new tool
    /// variant has been suggested.
    tool_sink: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>>,
    /// Project root for project-context compilation checks.
    /// When `Some`, `generate_code_patch` validates patches against the
    /// real workspace instead of an isolated temp crate.
    project_root: Option<std::path::PathBuf>,
    /// In-memory log of all evolution attempts and their current status.
    /// Production systems may additionally persist this to a backend.
    results: Arc<tokio::sync::Mutex<std::collections::HashMap<String, EvolutionResult>>>,
}

impl std::fmt::Debug for EvolutionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let has_hook_sink = self.hook_sink.lock().map(|g| g.is_some()).unwrap_or(false);
        let has_tool_sink = self.tool_sink.lock().map(|g| g.is_some()).unwrap_or(false);
        f.debug_struct("EvolutionEngine")
            .field("patch_dir", &self.patch_dir)
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
            patch_dir: std::path::PathBuf::from("evolution-patches"),
            prompt_manager,
            hook_sink: std::sync::Mutex::new(None),
            tool_sink: std::sync::Mutex::new(None),
            project_root: None,
            results: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Set the directory where code patches are written (default:
    /// `./evolution-patches`).
    pub fn with_patch_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.patch_dir = dir.into();
        self
    }

    /// Inject a channel sink so that synthesized hooks can be auto-registered
    /// by an external [`HookEngine`].
    pub fn with_hook_sink(self, tx: tokio::sync::mpsc::UnboundedSender<serde_json::Value>) -> Self {
        if let Ok(mut guard) = self.hook_sink.lock() {
            *guard = Some(tx);
        }
        self
    }

    /// Inject a channel sink so that suggested tool variants can be
    /// auto-registered by an external [`ToolRegistry`].
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
    /// so it appears in `list_results` / the admin patch table.
    /// Same `artifact_id` re-registers (latest proposal wins).
    pub async fn register_result(&self, result: EvolutionResult) {
        let mut results = self.results.lock().await;
        results.insert(result.artifact_id.clone(), result);
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
                .unwrap_or_else(|| {
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
                })
        };

        let system_prompt = self
            .prompt_manager
            .as_ref()
            .and_then(|pm| pm.get("reflection:evolution_refinement_system"))
            .unwrap_or_else(|| "Respond with valid JSON SkillConfig only.".into());

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
                    self.results
                        .lock()
                        .await
                        .insert(result.artifact_id.clone(), result.clone());
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
                    self.results
                        .lock()
                        .await
                        .insert(result.artifact_id.clone(), result.clone());
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
                self.results
                    .lock()
                    .await
                    .insert(result.artifact_id.clone(), result.clone());
                Ok(Some(result))
            }
        }
    }

    // ========================================================================
    // L1 — Hook Synthesis
    // ========================================================================

    /// Synthesize a [`HookDef`] from a recurring event pattern using the LLM.
    /// The generated hook is written to `patch_dir/hooks/{id}.json` and can be
    /// loaded by the caller into a [`HookEngine`].
    pub async fn synthesize_hook(
        &self,
        event_pattern: &str,
        action_outcomes: &[String],
    ) -> SFResult<Option<EvolutionResult>> {
        let outcomes_text = action_outcomes.join("\n- ");
        let prompt = {
            let mut vars = std::collections::HashMap::new();
            vars.insert("event_pattern".to_string(), event_pattern.to_string());
            vars.insert("action_outcomes".to_string(), outcomes_text.clone());
            self.prompt_manager
                .as_ref()
                .and_then(|pm| pm.render("reflection:evolution_hook", &vars).ok())
                .unwrap_or_else(|| format!(
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
                ))
        };

        let system_prompt = self
            .prompt_manager
            .as_ref()
            .and_then(|pm| pm.get("reflection:evolution_hook_system"))
            .unwrap_or_else(|| "Respond with valid JSON HookDef only.".into());

        let messages = vec![Message::system(system_prompt), Message::user(prompt)];

        let options = ChatOptions {
            response_format: ResponseFormat::Json,
            temperature: Some(0.3),
            max_tokens: Some(512),
            ..Default::default()
        };

        let response = self.llm.chat(&messages, &options).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        let json_str = Self::extract_json(&text);

        let hook_json: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| cog_core::SFError::LLM(format!("Failed to parse hook JSON: {e}")))?;

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
            self.results
                .lock()
                .await
                .insert(result.artifact_id.clone(), result.clone());
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
            self.results
                .lock()
                .await
                .insert(result.artifact_id.clone(), result.clone());
            return Ok(Some(result));
        }

        let artifact_id = id_str.to_string();

        // Write to hooks directory.
        let hook_dir = self.patch_dir.join("hooks");
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
        self.results
            .lock()
            .await
            .insert(result.artifact_id.clone(), result.clone());
        Ok(Some(result))
    }

    // ========================================================================
    // L1 — Tool Variant Suggestion
    // ========================================================================

    /// Suggest an improved tool variant based on error patterns using the LLM.
    /// The generated tool schema is written to `patch_dir/tools/{name}.json` and
    /// can be registered by the caller into a [`ToolRegistry`].
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
                .unwrap_or_else(|| format!(
                    "You are a tool design expert for an AI agent system.\n\n\
                     Existing tool: {}\n\n\
                     Observed error patterns:\n- {}\n\n\
                     Generate an improved tool variant as JSON with these fields:\n\
                     - name: tool name (suggest a new name like {}_v2 or {}_improved)\n\
                     - description: concise description of what the tool does\n\
                     - parameters: valid JSON Schema object describing input parameters\n\
                     - implementation_hint: string describing implementation approach (e.g., \"native\", \"wasm\", \"rhai\")\n\n\
                     Respond with ONLY the JSON object.",
                    tool_name, errors_text, tool_name, tool_name
                ))
        };

        let system_prompt = self
            .prompt_manager
            .as_ref()
            .and_then(|pm| pm.get("reflection:evolution_tool_system"))
            .unwrap_or_else(|| "Respond with valid JSON tool definition only.".into());

        let messages = vec![Message::system(system_prompt), Message::user(prompt)];

        let options = ChatOptions {
            response_format: ResponseFormat::Json,
            temperature: Some(0.3),
            max_tokens: Some(512),
            ..Default::default()
        };

        let response = self.llm.chat(&messages, &options).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join("");

        let json_str = Self::extract_json(&text);

        let tool_json: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| cog_core::SFError::LLM(format!("Failed to parse tool JSON: {e}")))?;

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
            self.results
                .lock()
                .await
                .insert(result.artifact_id.clone(), result.clone());
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
                self.results
                    .lock()
                    .await
                    .insert(result.artifact_id.clone(), result.clone());
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
                self.results
                    .lock()
                    .await
                    .insert(result.artifact_id.clone(), result.clone());
                return Ok(Some(result));
            }
        }

        let artifact_id = name_str.to_string();

        // Write to tools directory.
        let tool_dir = self.patch_dir.join("tools");
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
        self.results
            .lock()
            .await
            .insert(result.artifact_id.clone(), result.clone());
        Ok(Some(result))
    }

    // ========================================================================
    // L2 — Source Code Patch Generation (with compile validation)
    // ========================================================================

    /// Generate a unified-diff patch, validate it with `git apply --check`,
    /// and write it to the evolution-patches directory as `<patch_id>.patch`.
    /// The patch goes through up to 3 LLM attempts. If validation fails, the
    /// error output is fed back into the next prompt. Final statuses:
    /// - `"compile_checked"` — passed structural + `git apply --check` validation
    /// - `"compile_error"` — failed validation after 3 attempts
    /// - `"awaiting_review"` — no unified diff detected (conceptual answer)
    pub async fn generate_code_patch(
        &self,
        module_description: &str,
        learning_context: &str,
    ) -> SFResult<Option<EvolutionResult>> {
        let mut validation_errors = String::new();
        let mut last_text = String::new();

        for attempt in 1..=3 {
            let prompt = {
                let mut vars = std::collections::HashMap::new();
                vars.insert("learning_context".to_string(), learning_context.to_string());
                vars.insert(
                    "module_description".to_string(),
                    module_description.to_string(),
                );
                if !validation_errors.is_empty() {
                    vars.insert("compile_errors".to_string(), validation_errors.clone());
                }
                self.prompt_manager
                    .as_ref()
                    .and_then(|pm| pm.render("reflection:evolution_patch", &vars).ok())
                    .unwrap_or_else(|| {
                        let error_section = if validation_errors.is_empty() {
                            String::new()
                        } else {
                            format!(
                                "\n\nPrevious attempt failed validation. Errors:\n{}\n\nPlease fix these errors and regenerate the patch.",
                                validation_errors
                            )
                        };
                        format!(
                            "You are an expert Rust engineer improving an AI agent system.\n\n\
                             Context:\n{}\n\n\
                             Requirement:\n{}\
                             {}\n\n\
                             Generate a patch that addresses the requirement. Output ONLY a \
                             unified diff (starting with 'diff --git a/... b/...'), with no \
                             markdown fences and no prose. This is a Rust workspace; modify \
                             only source files under crates/**/*.rs.",
                            learning_context, module_description, error_section
                        )
                    })
            };

            let system_prompt = self
                .prompt_manager
                .as_ref()
                .and_then(|pm| pm.get("reflection:evolution_patch_system"))
                .unwrap_or_else(|| {
                    "Respond with a single unified diff patch and nothing else.".into()
                });

            let messages = vec![Message::system(system_prompt), Message::user(prompt)];

            let options = ChatOptions::default();
            let response = self.llm.chat(&messages, &options).await?;
            let text: String = response
                .content
                .iter()
                .filter_map(|b| b.as_text())
                .collect::<Vec<_>>()
                .join("");
            last_text = text.clone();

            // Extract the unified diff from the response.
            let Some(diff) = Self::extract_unified_diff(&text) else {
                // No diff found — treat as conceptual answer; record it in
                // memory only (no .patch file, so the pipeline ignores it).
                let patch_id = format!(
                    "patch-{}",
                    uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
                );
                let result = EvolutionResult {
                    kind: EvolutionKind::CodePatch,
                    artifact_id: patch_id,
                    description: module_description.into(),
                    content: text,
                    status: EvolutionStatus::AwaitingReview,
                    created_at: Utc::now(),
                    eval_summary: None,
                };
                self.results
                    .lock()
                    .await
                    .insert(result.artifact_id.clone(), result.clone());
                return Ok(Some(result));
            };

            match self.validate_patch(&diff).await {
                (true, _) => {
                    let patch_id = format!(
                        "patch-{}",
                        uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
                    );
                    let filename = self.patch_dir.join(format!("{}.patch", patch_id));
                    if let Some(parent) = filename.parent() {
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                    tokio::fs::write(&filename, &diff).await.map_err(|e| {
                        cog_core::SFError::Agent(format!(
                            "Failed to write patch {}: {}",
                            filename.display(),
                            e
                        ))
                    })?;

                    info!(
                        patch_id = %patch_id,
                        path = %filename.display(),
                        "Generated code patch passed validation"
                    );

                    let result = EvolutionResult {
                        kind: EvolutionKind::CodePatch,
                        artifact_id: patch_id,
                        description: module_description.into(),
                        content: diff,
                        status: EvolutionStatus::CompileChecked,
                        created_at: Utc::now(),
                        eval_summary: None,
                    };
                    self.results
                        .lock()
                        .await
                        .insert(result.artifact_id.clone(), result.clone());
                    return Ok(Some(result));
                }
                (false, output) => {
                    validation_errors = output;
                    warn!(attempt, "Patch failed validation, retrying");
                }
            }
        }

        // All retries exhausted — record the failure in memory only; an
        // invalid patch must never reach the patch directory.
        let patch_id = format!(
            "patch-{}",
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        );
        warn!(
            patch_id = %patch_id,
            "Patch failed validation after 3 attempts"
        );

        let result = EvolutionResult {
            kind: EvolutionKind::CodePatch,
            artifact_id: patch_id,
            description: module_description.into(),
            content: format!(
                "{}\n\n<!-- VALIDATION ERRORS AFTER 3 ATTEMPTS -->\n```\n{}\n```\n",
                last_text, validation_errors
            ),
            status: EvolutionStatus::CompileError,
            created_at: Utc::now(),
            eval_summary: None,
        };
        self.results
            .lock()
            .await
            .insert(result.artifact_id.clone(), result.clone());
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

    /// Extract a unified diff from raw LLM output.
    ///
    /// Accepts a bare diff or one wrapped in markdown fences (```diff /
    /// ```). The diff is taken from the first `diff --git ` line (or, for
    /// plain unified diffs, the first `--- a/` line) to the end, with
    /// trailing blank lines and a closing fence stripped. Returns `None`
    /// when no diff-like content is present or the diff references no files.
    fn extract_unified_diff(text: &str) -> Option<String> {
        let lines: Vec<&str> = text.lines().collect();
        let start = lines
            .iter()
            .position(|l| l.starts_with("diff --git "))
            .or_else(|| lines.iter().position(|l| l.starts_with("--- a/")))?;

        let mut end = lines.len();
        while end > start && (lines[end - 1].trim().is_empty() || lines[end - 1].trim() == "```") {
            end -= 1;
        }
        if end <= start {
            return None;
        }

        let diff = lines[start..end].join("\n");
        cog_core::parse_patch_affected_files(&diff).ok()?;
        Some(diff)
    }

    /// Validate a unified diff before it enters the patch pipeline.
    ///
    /// 1. The diff must reference at least one file and must not touch any
    ///    protected path (see [`Self::validate_patch_paths`]).
    /// 2. When a `project_root` git repository is configured, the patch must
    ///    apply cleanly (`git apply --check`).
    ///
    /// Returns `(success, details)`; `details` feeds the retry loop on
    /// failure. When `git` is unavailable the structural checks alone decide.
    async fn validate_patch(&self, diff: &str) -> (bool, String) {
        let files: Vec<std::path::PathBuf> = match cog_core::parse_patch_affected_files(diff) {
            Ok(f) => f.into_iter().map(std::path::PathBuf::from).collect(),
            Err(e) => return (false, format!("Patch structure invalid: {}", e)),
        };

        if let Err(e) = self.validate_patch_paths(&files, self.project_root.as_deref()) {
            return (false, e.to_string());
        }

        let Some(root) = self.project_root.as_ref() else {
            return (
                true,
                "structural validation passed (no project root)".into(),
            );
        };
        if !root.join(".git").is_dir() {
            return (true, "structural validation passed (not a git repo)".into());
        }

        let tmp = match tempfile::NamedTempFile::new() {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "temp file unavailable; structural validation only");
                return (
                    true,
                    format!("structural validation passed (temp file: {})", e),
                );
            }
        };
        if let Err(e) = std::fs::write(tmp.path(), diff) {
            warn!(error = %e, "temp write failed; structural validation only");
            return (
                true,
                format!("structural validation passed (temp write: {})", e),
            );
        }

        match tokio::process::Command::new("git")
            .args(["apply", "--check", "--verbose"])
            .arg(tmp.path())
            .current_dir(root)
            .output()
            .await
        {
            Ok(out) => {
                let combined = format!(
                    "{}{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                (out.status.success(), combined)
            }
            Err(e) => {
                warn!(error = %e, "git apply --check unavailable; structural validation only");
                (true, format!("structural validation passed (git: {})", e))
            }
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
    // PatchSink implementation — receive collaboration-generated .patch files
    // ========================================================================

    fn sanitize_artifact_id(&self, patch_id: &str) -> String {
        let safe: String = patch_id
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(c, '.' | '_' | '-'))
            .collect();
        if safe.is_empty() || safe == ".patch" {
            let now = chrono::Utc::now();
            return format!(
                "evo-{}-{}",
                now.format("%Y%m%d"),
                uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
            );
        }
        safe
    }

    fn validate_patch_paths(
        &self,
        files: &[std::path::PathBuf],
        project_root: Option<&std::path::Path>,
    ) -> cog_core::SFResult<()> {
        use std::collections::HashSet;

        let forbidden_names: HashSet<&str> = [
            "Cargo.toml",
            "Cargo.lock",
            "cogneva.json",
            ".env",
            ".envrc",
            "Dockerfile",
            "Containerfile",
            "docker-compose.yml",
            "setup.sh",
        ]
        .iter()
        .cloned()
        .collect();

        let forbidden_extensions: HashSet<&str> =
            ["pem", "key", "crt", "p12"].iter().cloned().collect();

        let canonical_root = project_root.and_then(|r| r.canonicalize().ok());

        for file in files {
            let path = std::path::Path::new(file);

            // Reject path traversal.
            if path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(cog_core::SFError::Validation(format!(
                    "Patch path contains parent directory traversal: {}",
                    file.display()
                )));
            }

            if path.is_absolute() {
                return Err(cog_core::SFError::Validation(format!(
                    "Patch path must be relative: {}",
                    file.display()
                )));
            }

            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if forbidden_names.contains(name) {
                    return Err(cog_core::SFError::Validation(format!(
                        "Modifying protected file {} is not allowed",
                        name
                    )));
                }
            }

            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if forbidden_extensions.contains(ext) {
                    return Err(cog_core::SFError::Validation(format!(
                        "Modifying .{} files is not allowed",
                        ext
                    )));
                }
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
                    "Patch target is outside a src directory; allowed but unusual"
                );
            }
        }

        Ok(())
    }

    /// Write a collaboration-generated patch to disk and register it as an
    /// EvolutionResult with status `CompileChecked`.
    async fn write_generated_patch(
        &self,
        patch: &cog_core::GeneratedPatch,
    ) -> cog_core::SFResult<String> {
        let artifact_id = self.sanitize_artifact_id(&patch.patch_id);

        // Derive affected files from the patch content if not supplied.
        let affected_files: Vec<std::path::PathBuf> = if patch.affected_files.is_empty() {
            cog_core::parse_patch_affected_files(&patch.content)?
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect()
        } else {
            patch
                .affected_files
                .iter()
                .map(std::path::PathBuf::from)
                .collect()
        };

        self.validate_patch_paths(&affected_files, self.project_root.as_deref())?;

        let filename = self.patch_dir.join(format!("{}.patch", artifact_id));
        if let Some(parent) = filename.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                cog_core::SFError::IO(format!(
                    "Failed to create patch dir {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        tokio::fs::write(&filename, &patch.content)
            .await
            .map_err(|e| {
                cog_core::SFError::IO(format!(
                    "Failed to write patch {}: {}",
                    filename.display(),
                    e
                ))
            })?;

        info!(
            artifact_id = %artifact_id,
            path = %filename.display(),
            "Collaboration-generated patch written to disk"
        );

        let result = EvolutionResult {
            kind: EvolutionKind::CodePatch,
            artifact_id: artifact_id.clone(),
            description: patch.goal.clone(),
            content: patch.content.clone(),
            status: EvolutionStatus::CompileChecked,
            created_at: Utc::now(),
            eval_summary: None,
        };
        self.results
            .lock()
            .await
            .insert(artifact_id.clone(), result);

        Ok(artifact_id)
    }
}

#[async_trait::async_trait]
impl cog_core::PatchSink for EvolutionEngine {
    async fn submit_patch(&self, patch: cog_core::GeneratedPatch) -> cog_core::SFResult<String> {
        self.write_generated_patch(&patch).await
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
    fn extract_unified_diff_bare() {
        let text = "diff --git a/crates/foo/src/lib.rs b/crates/foo/src/lib.rs\n\
                    index 1111111..2222222 100644\n\
                    --- a/crates/foo/src/lib.rs\n\
                    +++ b/crates/foo/src/lib.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    -old\n\
                    +new\n";
        let diff = EvolutionEngine::extract_unified_diff(text).unwrap();
        assert!(diff.starts_with("diff --git a/crates/foo/src/lib.rs"));
        assert!(diff.contains("+new"));
    }

    #[test]
    fn extract_unified_diff_fenced_with_prose() {
        let text = "Here is the patch you asked for:\n\n\
                    ```diff\n\
                    diff --git a/crates/foo/src/lib.rs b/crates/foo/src/lib.rs\n\
                    --- a/crates/foo/src/lib.rs\n\
                    +++ b/crates/foo/src/lib.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    -old\n\
                    +new\n\
                    ```\n";
        let diff = EvolutionEngine::extract_unified_diff(text).unwrap();
        assert!(diff.starts_with("diff --git"));
        assert!(!diff.contains("```"));
    }

    #[test]
    fn extract_unified_diff_plain_unified() {
        let text = "--- a/crates/foo/src/lib.rs\n\
                    +++ b/crates/foo/src/lib.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    -old\n\
                    +new\n";
        let diff = EvolutionEngine::extract_unified_diff(text).unwrap();
        assert!(diff.starts_with("--- a/"));
    }

    #[test]
    fn extract_unified_diff_none_for_prose() {
        assert!(EvolutionEngine::extract_unified_diff("# Just a header\nNo code here.").is_none());
        // A diff-like block with no +++ file header is also rejected.
        assert!(
            EvolutionEngine::extract_unified_diff("diff --git a/x b/x\n@@ -1 +1 @@\n-a\n+b\n")
                .is_none()
        );
    }
}
