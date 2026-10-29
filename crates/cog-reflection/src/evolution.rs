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

use crate::change_pipeline::write_record_file;
use crate::types::{EvolutionKind, EvolutionResult, EvolutionStatus};

/// What each prompt says when no prompt manager is wired into the engine.
///
/// These are not copies kept for their own sake. The same instruction has to
/// hold on both paths — the declared prompt a running process reads and the
/// text the code sends when there is nothing to read — and a side that is
/// edited alone is how a declaration stops describing the run. A guard in the
/// assembly root's test suite renders every declared key and compares it to the
/// text here byte for byte, so an edit to either side that is not mirrored
/// fails the suite instead of shipping as a quiet difference. That guard reads
/// this module from outside, which is why it is public: the crate that owns the
/// declaration and the crate that owns the loader do not name each other.
pub mod fallback {
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
    /// Resident index of the evolution attempts this process has produced and
    /// their current status, keyed by `artifact_id`. It is an index and not a
    /// copy: [`resident_record`] drops each artifact's text, whose durable copy
    /// lives in the change directory or the store that owns that artifact kind.
    results: Arc<tokio::sync::Mutex<std::collections::HashMap<String, EvolutionResult>>>,
    /// 一份变更最多允许多少有效改动行。判据来自落地侧读的同一个配置键
    /// （[`crate::GenerationLimits`]），在这里先在写盘前用一次：超限的变更不该
    /// 走完构建与沙盒验证、到落地时才被拦下，那时每一次重跑都是白跑的。
    max_changed_lines: usize,
}

/// Move a produced result into the shape the resident index stores.
///
/// The index keeps the artifact's metadata and drops `content`. The full text
/// has a durable copy of its own -- the change directory for a code change,
/// the registries and the artifact store for the rest -- so keeping it here
/// only duplicated it, and it was the *size* of that text, not the number of
/// entries, that made resident usage grow with uptime. A cap on the entry
/// count cannot bound a quantity the entry size sets: "the most recent 256"
/// is unbounded in bytes whenever an entry may be arbitrarily large. So the
/// index holds no fixed cap at all -- what remains is one small record per
/// artifact this process has produced, each mirrored by the store that already
/// owns it.
fn resident_record(mut result: EvolutionResult) -> EvolutionResult {
    result.content.clear();
    result
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
            max_changed_lines: crate::GenerationLimits::default().max_changed_lines,
        }
    }

    /// 设置生成侧的行数上限，取落地侧读的同一个配置键所载的值。
    pub fn with_max_changed_lines(mut self, max_changed_lines: usize) -> Self {
        self.max_changed_lines = max_changed_lines;
        self
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

    /// The single insertion path for [`Self::results`].
    ///
    /// Every result this engine produces is stored through here, so the shape
    /// [`resident_record`] enforces -- metadata, no artifact text -- holds on
    /// every branch and cannot be bypassed by a new one. Same `artifact_id`
    /// re-registers (latest proposal wins).
    async fn insert_result(&self, result: EvolutionResult) {
        let result = resident_record(result);
        let mut results = self.results.lock().await;
        results.insert(result.artifact_id.clone(), result);
    }

    /// Update the status of an evolution result by `artifact_id`.
    /// Returns `true` if the result was found and updated.
    ///
    /// For a code change the new status is written through to the record beside
    /// its `.diff`, so the state a restart reads back is this one rather than
    /// whatever the index held before the change was judged. A change the index
    /// does not know is still answered when its `.diff` is in the queue: the
    /// conclusion is what resolves an unrecorded change, and leaving it in the
    /// index alone would make "no record" the one state that never ends.
    pub async fn update_status(&self, artifact_id: &str, status: EvolutionStatus) -> bool {
        let updated = {
            let mut results = self.results.lock().await;
            match results.get_mut(artifact_id) {
                Some(r) => {
                    let old = r.status;
                    r.status = status;
                    info!(
                        artifact_id = %artifact_id,
                        old_status = ?old,
                        new_status = ?status,
                        "Evolution result status updated"
                    );
                    Some(r.clone())
                }
                None => None,
            }
        };

        match updated {
            Some(record) if matches!(record.kind, EvolutionKind::CodeChange) => {
                self.persist_record(&record).await;
                true
            }
            Some(_) => true,
            None => {
                // Not in the index. A code change whose `.diff` is still in the
                // queue has a state worth recording even though this process
                // never generated it -- that is exactly the change whose record
                // is missing, and the verification's conclusion is what fills it.
                let path = self.change_dir.join(format!("{artifact_id}.diff"));
                if !path.exists() {
                    warn!(
                        artifact_id = %artifact_id,
                        "Evolution result not found for status update"
                    );
                    return false;
                }
                let record = EvolutionResult {
                    kind: EvolutionKind::CodeChange,
                    artifact_id: artifact_id.to_string(),
                    description: String::new(),
                    content: String::new(),
                    status,
                    created_at: crate::change_pipeline::UNKNOWN_CREATED_AT,
                    eval_summary: None,
                    tiering: None,
                };
                self.persist_record(&record).await;
                true
            }
        }
    }

    /// Write `record` to the durable copy beside its change.
    ///
    /// A failure is logged, not propagated: the caller has already changed the
    /// in-memory status and cannot undo it, and the record is rewritten on the
    /// next update. What it costs is that a restart before the next update
    /// reads the older status, which is the status the record had -- not a
    /// wrong one.
    async fn persist_record(&self, record: &EvolutionResult) {
        if let Err(e) = write_record_file(&self.change_dir, record).await {
            warn!(
                artifact_id = %record.artifact_id,
                error = %e,
                "could not write the change record; the resident status stands alone until the next update"
            );
        }
    }

    /// How many *code changes* the resident index holds.
    ///
    /// Only that kind is counted. The other artifacts are keyed by an id that is
    /// stable per artifact, so the index holds one entry for each whether
    /// anything reads it or not; a code change is generated under a fresh id
    /// every time, so its entries are the ones the retirement path bounds and
    /// the only ones that can accumulate. The number is published beside the
    /// queue's file count, which is what turns "the index is remembering more
    /// changes than exist" into a reading.
    pub async fn resident_code_change_len(&self) -> usize {
        self.results
            .lock()
            .await
            .values()
            .filter(|r| matches!(r.kind, EvolutionKind::CodeChange))
            .count()
    }

    /// Drop an artifact's resident record.
    ///
    /// Only allowed once the artifact is out of the pending queue and its own
    /// record sits beside it in `retired/`. The index is otherwise the last
    /// thing holding that change's state, and this is the drop that would take
    /// the state with it -- so the check is here rather than at the call site:
    /// a caller that forgot it would lose the record in silence, and a record
    /// that never moves is a leak this method exists to end.
    pub async fn retire_result(&self, pipeline: &crate::ChangePipeline, artifact_id: &str) -> bool {
        if !pipeline.artifact_is_retired(artifact_id) {
            warn!(
                artifact_id = %artifact_id,
                "refusing to drop the resident record: the change is not retired yet"
            );
            return false;
        }
        let mut results = self.results.lock().await;
        let dropped = results.remove(artifact_id).is_some();
        if dropped {
            debug!(artifact_id = %artifact_id, "resident evolution record retired");
        }
        dropped
    }

    /// Look one result up by `artifact_id`.
    ///
    /// Both readers that name a single change -- `ChangePipeline::pending_changes`
    /// and `EvolutionAdminService::get_policy_result` -- used to call
    /// `list_results` and then pick one entry out of the clone. `pending_changes`
    /// does that inside its per-file loop, so every file in the queue cloned the
    /// whole index and the allocation grew with `files x resident` to answer a
    /// question about one record. A lookup answers it without touching the rest.
    pub async fn get_result(&self, artifact_id: &str) -> Option<EvolutionResult> {
        self.results.lock().await.get(artifact_id).cloned()
    }

    /// List all evolution results ordered from newest to oldest.
    ///
    /// The records carry metadata only: `content` is not populated (see
    /// [`resident_record`]). A caller that needs the artifact text reads it
    /// back from where that artifact lives.
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
                        tiering: None,
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
                        tiering: None,
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
                    tiering: None,
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
                tiering: None,
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
                tiering: None,
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
            tiering: None,
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
                tiering: None,
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
                    tiering: None,
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
                    tiering: None,
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
            tiering: None,
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

        // 行数上限在写盘前判一次。落地侧按同一个数（同一个配置键）判超限，而
        // 到那时变更已经走完构建、测试与沙盒验证——超限的变更在这里被拒，只花
        // 掉生成那一轮；放它过去，花掉的是整条流水线，最后还是落不了地。
        // 计数用 `cog_core::count_diff_lines`：生成侧、晋级门、落地侧必须是同一个
        // 读数，第二份解析器会让同一份 diff 对不同读者给出不同的行数。
        let diff_lines = cog_core::count_diff_lines(&content);
        if diff_lines > self.max_changed_lines {
            return Err(cog_core::SFError::Validation(format!(
                "generated change is {diff_lines} changed lines, over the {} line budget shared with the landing policy; narrow the scope and regenerate rather than submitting an over-budget diff",
                self.max_changed_lines
            )));
        }

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
            tiering: None,
        };

        // The record goes down with the change, or the change does not go down
        // at all. A `.diff` whose record is missing is the state the queue
        // cannot describe, so a generation that leaves one has produced an
        // orphan rather than a change -- and the caller is better served by the
        // error than by a file nothing can say the state of.
        if let Err(e) = write_record_file(&self.change_dir, &result).await {
            if let Err(remove) = tokio::fs::remove_file(&filename).await {
                warn!(
                    path = %filename.display(),
                    error = %remove,
                    "could not remove a change whose record failed to write; it stays as an unrecorded diff"
                );
            }
            return Err(e);
        }

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

    /// principle: feedback_a_format_gate_is_landed_on_the_diff_and_censused_before_it_is_written
    ///
    /// 落一扇「写法」的门之前，先把它的两种形状各量一次：同一句原则既能落成
    /// 「判存量」——照存量字节判——也能落成「判新增」——只判修好之后要写进去的
    /// +/- 行。同一份样本、同一个修订、同一个结构 oracle：存量读数点名缺陷，
    /// 新增读数干净；普查在写之前数，修复不得改变所数的变更行数，已经合身的
    /// diff 一个字节都不许被重写。
    #[test]
    fn a_format_gate_is_censused_in_its_two_shapes_before_it_is_written() {
        // 一份固定样本，在这棵树的同一个修订上量两次：身体是对的，最后一行
        // 没有结束符，git apply 会以 corrupt patch 拒收。
        let stock = "diff --git a/x.rs b/x.rs\n\
                     --- a/x.rs\n\
                     +++ b/x.rs\n\
                     @@ -1,1 +1,1 @@\n\
                     -old\n\
                     +new";
        // 判存量：存量形状在门下必须被点名，而不是被静默放过。
        assert!(
            cog_core::diff_structural_defect(stock).is_some(),
            "the stock reading must name the defect already present in the bytes"
        );

        // 判新增：同一个结构 oracle 判修好之后要新增进树里的形状。
        let repaired = repair_generated_diff(stock);
        assert_eq!(cog_core::diff_structural_defect(&repaired), None);
        assert!(repaired.ends_with('\n'));
        assert!(repaired.contains("-old\n"));
        assert!(repaired.contains("+new\n"));

        // 普查在写之前进行：write_generated_change 先 repair，再用同一个
        // cog_core::count_diff_lines 量修复后的文本。修复只补结构，不许改变
        // 普查所数的变更行数——存量身体两行，新增形状还是两行，门无法借修复
        // 夹带额外的改动过门。
        assert_eq!(
            cog_core::count_diff_lines(stock),
            cog_core::count_diff_lines(&repaired)
        );

        // 已经合身的存量是这扇门的另一种形状：门对它一个字节都不能动。
        let sound = "diff --git a/x.rs b/x.rs\n\
                     --- a/x.rs\n\
                     +++ b/x.rs\n\
                     @@ -1,1 +1,1 @@\n\
                     -old\n\
                     +new\n";
        assert_eq!(repair_generated_diff(sound), sound);
    }

    fn make_result(id: &str, created_at: chrono::DateTime<chrono::Utc>) -> EvolutionResult {
        EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: id.to_string(),
            description: format!("desc-{id}"),
            content: format!("content-{id}"),
            status: EvolutionStatus::Generated,
            created_at,
            eval_summary: None,
            tiering: None,
        }
    }

    #[test]
    fn make_result_carries_text_so_the_drop_test_proves_something() {
        // Guards the guard: if `make_result` ever stopped carrying text, the
        // assertion in the test below would pass without proving anything.
        assert!(!make_result("r-0", chrono::Utc::now()).content.is_empty());
    }

    #[test]
    fn the_resident_record_keeps_the_metadata_and_drops_the_artifact_text() {
        // Resident usage used to grow with the size of the artifact text, which
        // is a quantity an entry-count cap cannot bound. The record the index
        // stores keeps the metadata and drops the text; the durable copy stays
        // where the artifact already lives.
        let stored = resident_record(make_result("r-0", chrono::Utc::now()));
        assert!(
            stored.content.is_empty(),
            "the artifact text must not stay resident"
        );
        assert_eq!(stored.artifact_id, "r-0");
        assert_eq!(
            stored.description, "desc-r-0",
            "the metadata the index exists for must survive the drop"
        );
        assert_eq!(stored.kind, EvolutionKind::CodeChange);
        assert_eq!(stored.status, EvolutionStatus::Generated);
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

    /// The engine is the subject, not the model behind it, so the double never
    /// answers anything.
    struct SilentLlm;

    #[async_trait::async_trait]
    impl cog_core::LlmClient for SilentLlm {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            unimplemented!()
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn engine_on(dir: &std::path::Path) -> EvolutionEngine {
        let registry = Arc::new(RwLock::new(cog_core::SkillRegistry::new()));
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(SilentLlm);
        EvolutionEngine::new(llm, registry, None).with_change_dir(dir)
    }

    fn code_change(id: &str, status: EvolutionStatus) -> EvolutionResult {
        EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: id.to_string(),
            description: format!("goal-{id}"),
            content: String::new(),
            status,
            created_at: chrono::Utc::now(),
            eval_summary: None,
            tiering: None,
        }
    }

    /// The drop of a resident record is guarded on the artifact being out of the
    /// queue with its own record beside it, because until then the index is the
    /// last thing that knows this change's state.
    #[tokio::test]
    async fn a_resident_record_is_dropped_only_after_its_change_is_retired() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let engine = engine_on(&change_dir);
        let pipeline = crate::ChangePipeline::new(temp.path(), &change_dir, true);

        tokio::fs::write(change_dir.join("chg-1.diff"), "not a real diff\n")
            .await
            .unwrap();
        pipeline
            .write_change_record(&code_change("chg-1", EvolutionStatus::CompileChecked))
            .await
            .unwrap();
        engine
            .register_result(code_change("chg-1", EvolutionStatus::CompileChecked))
            .await;

        assert!(
            !engine.retire_result(&pipeline, "chg-1").await,
            "the change is still in the queue: dropping the record now would lose its state"
        );
        assert!(
            engine.get_result("chg-1").await.is_some(),
            "a refused drop must not have dropped anything"
        );

        pipeline.retire_change("chg-1", "landed").await.unwrap();

        assert!(engine.retire_result(&pipeline, "chg-1").await);
        assert!(
            engine.get_result("chg-1").await.is_none(),
            "the state lives in the retired record from here on"
        );
    }

    /// A change the index never saw still gets its conclusion recorded: it is
    /// exactly the change whose record is missing, and leaving it in the index
    /// alone would make "no record" the one state that never ends.
    #[tokio::test]
    async fn a_status_update_answers_a_change_the_index_never_saw() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let engine = engine_on(&change_dir);
        let pipeline = crate::ChangePipeline::new(temp.path(), &change_dir, true);
        tokio::fs::write(change_dir.join("legacy.diff"), "not a real diff\n")
            .await
            .unwrap();

        assert!(engine.get_result("legacy").await.is_none());
        assert!(
            engine
                .update_status("legacy", EvolutionStatus::AwaitingReview)
                .await
        );

        let record = pipeline
            .read_change_record("legacy")
            .await
            .expect("the conclusion has to be on disk, or a restart reads the unknown again");
        assert_eq!(record.status, EvolutionStatus::AwaitingReview);
        assert_eq!(
            record.created_at,
            crate::change_pipeline::UNKNOWN_CREATED_AT,
            "recovering a record must not invent a creation time for it"
        );
    }

    /// Nothing to answer: no record and no file is not a change this engine can
    /// say anything about, and pretending otherwise would write a record for a
    /// change that does not exist.
    #[tokio::test]
    async fn a_status_update_for_a_change_that_is_nowhere_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let engine = engine_on(&change_dir);

        assert!(
            !engine
                .update_status("nobody", EvolutionStatus::AwaitingReview)
                .await
        );
        assert!(!change_dir.join("nobody.json").exists());
    }

    /// The status the index holds is written through to the record, so a restart
    /// reads the state the change reached rather than the one it was written
    /// with.
    #[tokio::test]
    async fn a_status_update_writes_through_to_the_record() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let engine = engine_on(&change_dir);
        let pipeline = crate::ChangePipeline::new(temp.path(), &change_dir, true);
        tokio::fs::write(change_dir.join("chg-2.diff"), "not a real diff\n")
            .await
            .unwrap();
        engine
            .register_result(code_change("chg-2", EvolutionStatus::CompileChecked))
            .await;

        assert!(
            engine
                .update_status("chg-2", EvolutionStatus::AwaitingReview)
                .await
        );

        assert_eq!(
            pipeline.read_change_record("chg-2").await.map(|r| r.status),
            Some(EvolutionStatus::AwaitingReview)
        );
    }

    /// 超限的变更在写盘前被拒，恰好等于上限时放行。
    ///
    /// 落地侧按同一个数判超限，但它判的时候变更已经走完构建、测试与沙盒验证。
    /// 两半都要钉住：只钉「超限被拒」会让边界悄悄挪成「小于」也放行；只钉
    /// 「等于上限放行」则放行一切。
    #[tokio::test]
    async fn a_generated_change_over_the_shared_line_budget_is_refused_before_it_is_written() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let engine = engine_on(&change_dir).with_max_changed_lines(2);

        let diff = |added: usize| {
            let mut s = String::from(
                "diff --git a/crates/cog-agent/src/x.rs b/crates/cog-agent/src/x.rs\n\
                 --- a/crates/cog-agent/src/x.rs\n\
                 +++ b/crates/cog-agent/src/x.rs\n\
                 @@ -1,1 +1,1 @@\n",
            );
            for i in 0..added {
                s.push_str(&format!("+line {i}\n"));
            }
            s
        };
        let mut change = cog_core::GeneratedChange {
            change_id: "over-budget".into(),
            goal: "over budget".into(),
            affected_files: vec!["crates/cog-agent/src/x.rs".into()],
            content: diff(3),
            ..Default::default()
        };
        let err = engine
            .write_generated_change(&change)
            .await
            .expect_err("3 行超过 2 行上限，必须被拒");
        assert!(
            err.to_string().contains("line budget"),
            "拒绝的理由要点名那个预算，生成侧才知道该缩小范围：{err}"
        );
        assert!(
            !change_dir.join("over-budget.diff").exists(),
            "被拒的变更不该留下落盘的 diff"
        );

        change.change_id = "at-budget".into();
        change.content = diff(2);
        assert!(
            engine.write_generated_change(&change).await.is_ok(),
            "恰好等于上限要放行——边界不是「小于」"
        );
    }
}

#[cfg(test)]
mod agent_default_exemption_carries_its_own_witness {
    //! principle: feedback_an_exemption_is_also_a_blind_spot
    //!
    //! The parity guard in the assembly root's test suite skips one key it lists:
    //! `agent:default` is skipped with a comment claiming this crate never asks
    //! for it and that cog-prompt owns the bootstrap prompt. That skip is an exemption, and an exemption is
    //! by construction a blind spot: it rests on a hypothesis about an ambiguous
    //! signal -- "this crate never requests that key" -- which the guard itself
    //! never checks. The day a call site requests `agent:default`, the skip
    //! silently stops meaning what its comment claims. This is the gate the
    //! hypothesis is owed: scan this crate's own shipped source for a `get(`/
    //! `render(` call naming the key, so the exemption can only survive while
    //! its assumption is still true.

    use std::path::{Path, PathBuf};

    const EXEMPTED_KEY: &str = "agent:default";

    #[test]
    fn no_call_site_in_this_crate_requests_the_exempted_prompt_key() {
        let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        let mut files = Vec::new();
        collect_rust_files(root, &mut files);
        assert!(
            !files.is_empty(),
            "{} must hold the sources this witness scans",
            root.display()
        );

        let mut hits = Vec::new();
        for file in &files {
            let text =
                std::fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
            for (lineno, line) in text.lines().enumerate() {
                if requests_exempted_key(line) {
                    hits.push(format!("{}:{}", file.display(), lineno + 1));
                }
            }
        }
        assert!(
            hits.is_empty(),
            "the declared-prompt parity guard skips `{EXEMPTED_KEY}` on the claim that this crate never requests \
             it; these call sites now do, so the exemption is a blind spot until the guard compares that key too:\n{}",
            hits.join("\n")
        );
    }

    fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect_rust_files(&path, out);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    /// Whether one source line requests the exempted key at a prompt call site:
    /// a quoted key whose prefix, whitespace stripped, ends in `.get(` or
    /// `.render(`. The bare string in a key list, or an `if key == ...`
    /// comparison, is not a request, so the skip's own declaration does not
    /// witness against itself.
    fn requests_exempted_key(line: &str) -> bool {
        let needle = format!("\"{EXEMPTED_KEY}\"");
        line.match_indices(needle.as_str()).any(|(at, _)| {
            let before: String = line[..at].chars().filter(|c| !c.is_whitespace()).collect();
            before.ends_with(".get(") || before.ends_with(".render(")
        })
    }
}

#[cfg(test)]
mod a_declarations_identity_is_its_scope_and_prose_cannot_widen_it {
    //! principle: feedback_a_declarations_identity_is_its_scope_and_prose_cannot_widen_it
    //!
    //! A change declaration's identity is its `kind`, its `scope` and its
    //! `literal`, and nothing else. The judge that answers for `kind = grep`
    //! walks the files under `scope` and matches `literal`; the declaration's
    //! trailing description column is prose a person reads, and no sentence in
    //! it -- however wide the surfaces it names -- adds a file to the set the
    //! grep matched. This is the gate that rule is owed: it runs the
    //! declaration as written, a grep over `crates/` for this principle's own
    //! literal in this tree at one revision, and asserts the match set is
    //! exactly what kind, scope and literal produce.

    use std::path::{Path, PathBuf};

    /// The declaration's literal column, as the intake wrote it.
    const LITERAL: &str = "feedback_a_declarations_identity_is_its_scope_and_prose_cannot_widen_it";

    /// The declaration's scope column: the judge walks this root and no other.
    const SCOPE: &str = "crates";

    #[test]
    fn the_greps_match_set_is_what_kind_scope_and_literal_produce() {
        let scope_root =
            std::fs::canonicalize(concat!(env!("CARGO_MANIFEST_DIR"), "/../../crates"))
                .unwrap_or_else(|e| panic!("the declared scope `{SCOPE}` must resolve: {e}"));
        let mut files = Vec::new();
        collect_rust_files(&scope_root, &mut files);
        assert!(
            !files.is_empty(),
            "{} must hold the sources this gate greps",
            scope_root.display()
        );

        // The judge's whole reading of the declaration: the files under the
        // scope whose text carries the literal. Nothing below this line reads
        // the declaration's prose, which is the point the gate exists to pin.
        let mut hits = Vec::new();
        for file in &files {
            let text =
                std::fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
            for (lineno, line) in text.lines().enumerate() {
                if line.contains(LITERAL) {
                    hits.push((file.clone(), lineno + 1, line.trim().to_string()));
                }
            }
        }

        // Every hit stays inside the declared scope by construction of the
        // walk; assert it anyway, because the declaration's description column
        // names surfaces outside `crates/` and prose cannot widen the scope.
        for (file, _, _) in &hits {
            assert!(
                file.starts_with(&scope_root),
                "{} matched the literal while lying outside the declared scope `{SCOPE}`",
                file.display()
            );
        }

        // The literal's only carrier under the scope is this gate: its header
        // declaration and its own references. A second carrier would be part
        // of the declaration's match set that this gate answers for nowhere.
        let this_file = scope_root.join("cog-reflection/src/evolution.rs");
        let mut outside: Vec<String> = hits
            .iter()
            .filter(|(file, _, _)| file != &this_file)
            .map(|(file, lineno, _)| format!("{}:{lineno}", file.display()))
            .collect();
        outside.sort();
        assert!(
            outside.is_empty(),
            "literal matched outside the gate that carries it; kind + scope + literal is the \
             whole identity, so these carriers are the declaration's and this gate answers for \
             none of them:\n{}",
            outside.join("\n")
        );

        // The carrying declaration is itself among the hits: a gate whose own
        // header the grep cannot find answers for a different literal.
        let declared = hits.iter().any(|(file, _, line)| {
            file == &this_file && line.starts_with("//! principle:") && line.contains(LITERAL)
        });
        assert!(
            declared,
            "no `//! principle: {LITERAL}` line matched in {}: a gate whose own header the grep \
             cannot find answers for a different literal",
            this_file.display()
        );
    }

    /// The kind column means what the vocabulary says, not what prose says:
    /// `grep` is a judgeable presence kind in `change-kinds.tsv`, so carrying
    /// it asks for a grep over the scope whatever the description column adds.
    #[test]
    fn the_kind_column_means_what_the_vocabulary_says() {
        let vocabulary =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../cogneva/contracts/change-kinds.tsv");
        let text = std::fs::read_to_string(&vocabulary)
            .unwrap_or_else(|e| panic!("{}: {e}", vocabulary.display()));
        let mut found = None;
        for line in text.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            if cols.first() == Some(&"grep") {
                found = Some((
                    cols.get(1).copied().unwrap_or(""),
                    cols.get(2).copied().unwrap_or(""),
                ));
                break;
            }
        }
        let (direction, state) = found.unwrap_or_else(|| {
            panic!(
                "{} holds no row for `grep`, the kind this declaration carries",
                vocabulary.display()
            )
        });
        assert_eq!(
            (direction, state),
            ("present", "judgeable"),
            "kind `grep` no longer names a presence grep a new declaration may carry"
        );
    }

    fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect_rust_files(&path, out);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
}
