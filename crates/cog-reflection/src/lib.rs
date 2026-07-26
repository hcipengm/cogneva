//! `cog-reflection` — Cross-session learning and self-improvement for Cogneva.
//! This crate provides the **asynchronous, cumulative, knowledge-oriented**
//! reflection layer that complements the existing *synchronous, single-run,
//! output-quality* reflection in `cog-agent` (`SelfReviewLoop`).
//! ## Architecture
//! ```text
//! detector ──→ recorder ──→ matcher ──→ promoter ──→ extractor
//!    ↑                                              │
//!    │  (SelfReview / AgentEvent / conversation)     │
//!    └──────────────────────────────────────────────┘
//!                          (feedback loop to SkillRegistry / AgentConfig)
//! ```
//! ## Phase Roadmap
//! | Phase | Modules | Description |
//! |-------|---------|-------------|
//! | **1** | `types`, `detector`, `recorder` | Core data model, keyword-based detection, in-memory persistence |
//! | **2** | `matcher`, `promoter` | Pattern matching, vector similarity, auto-promotion to SkillRegistry |
//! | **3** | `reviewer`, `extractor` | Periodic review scheduling, LLM-based skill extraction |
//! ## Integration Points
//! - **`cog-agent`** — `HookEngine` triggers `detector` on `AgentEvent::SelfReview` and `AgentEvent::TaskStatusChange`.
//! - **`cog-memory`** — `MemoryBackendRecorder` archives learnings into the three-layer memory system.
//! - **`cog-llm`** — `SkillExtractor` uses an `LLMProvider` to generate `SkillConfig` from mature patterns.
//! - **`cog-core`** — `SkillRegistry` receives promoted `SkillConfig` entries.

pub mod crew;
pub mod detector;
pub mod discovery;
pub mod effectiveness;
pub mod eval_harness;
pub mod evolution;
pub mod evolution_admin;
pub mod evolution_deployer;
pub mod extractor;
pub mod firecracker;
pub mod flywheel;
pub mod image_rollout;
pub mod matcher;
pub mod meta_learning;
pub mod patch_pipeline;
pub mod policy_store;
pub mod promoter;
pub mod recorder;
pub mod reviewer;
pub mod sandbox;
pub mod squad;
pub mod types;

use cog_core::{DecisionCategory, DecisionOutcome, Learning};
pub use detector::{DefaultLearningDetector, LearningDetector};
pub use discovery::DiscoveryEngine;
pub use effectiveness::SkillEffectivenessTracker;
pub use eval_harness::{
    compare, evaluate, two_proportion_z_test, BenchReport, EvalComparison, EvalOutcome,
    EvalSummary, EvalTask, EvalVerdict,
};
pub use evolution::EvolutionEngine;
pub use evolution_admin::EvolutionAdminService;
pub use evolution_deployer::{BuildArtifact, EvolutionDeployer};
pub use extractor::SkillExtractor;
pub use firecracker::{FirecrackerSandbox, MicroVm, MicroVmOutcome};
pub use flywheel::{JsonlFileSink, LearningSink, WarehouseRecorder};
pub use image_rollout::ImageRollout;
pub use matcher::{DefaultLearningMatcher, LearningMatcher};
pub use meta_learning::MetaLearningEngine;
pub use patch_pipeline::{ApplyResult, PatchPipeline};
pub use policy_store::{
    ArtifactEvolution, PolicyArtifact, PolicyCandidate, PolicyProposal, PolicyStore,
};
pub use promoter::{DefaultLearningPromoter, LearningPromoter};
pub use recorder::{InMemoryRecorder, LearningRecorder, MemoryBackendRecorder};
pub use reviewer::PeriodicReviewer;
pub use sandbox::{enforce_sandbox_boundary, BoundaryDecision, SandboxKind, SandboxSignals};
pub use squad::DefaultSquadReflection;
pub use types::*;

pub mod plugin;

use std::sync::Arc;

/// Convenience builder that wires together all Phase-1 components.
pub struct ReflectionEngine {
    pub detector: Arc<dyn LearningDetector>,
    pub recorder: Arc<dyn LearningRecorder>,
    pub matcher: Arc<dyn LearningMatcher>,
    pub promoter: Arc<dyn LearningPromoter>,
    pub reviewer: Option<Arc<PeriodicReviewer>>,
    pub extractor: Option<Arc<SkillExtractor>>,
    /// Deep self-evolution: skill effectiveness tracking.
    pub effectiveness_tracker: Option<Arc<SkillEffectivenessTracker>>,
    /// Deep self-evolution: meta-learning mode selector.
    pub meta_learning: Option<Arc<MetaLearningEngine>>,
    /// Deep self-evolution: controlled evolution engine.
    pub evolution: Option<Arc<EvolutionEngine>>,
    /// Deep self-evolution: autonomous capability discovery.
    pub discovery: Option<Arc<DiscoveryEngine>>,
    /// Per-trigger cooldown tracking to avoid spamming LLM calls.
    evolution_cooldowns:
        Arc<tokio::sync::Mutex<std::collections::HashMap<String, chrono::DateTime<chrono::Utc>>>>,
    /// Minimum interval between two evolution triggers of the same key.
    evolution_cooldown_secs: u64,
    /// In-memory tool-error counters (resets on restart; persistent counts come from recorder).
    tool_error_counts: Arc<tokio::sync::Mutex<std::collections::HashMap<String, u32>>>,
    /// How many repeated errors before suggesting a tool variant.
    tool_error_threshold: u32,
    /// How many recurrences before synthesizing a hook.
    hook_recurrence_threshold: u32,
    /// How many recurrences before generating a code patch.
    patch_recurrence_threshold: u32,
}

impl std::fmt::Debug for ReflectionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReflectionEngine")
            .field("detector", &"<dyn LearningDetector>")
            .field("recorder", &"<dyn LearningRecorder>")
            .field("matcher", &"<dyn LearningMatcher>")
            .field("promoter", &"<dyn LearningPromoter>")
            .field("reviewer", &self.reviewer.is_some())
            .field("extractor", &self.extractor.is_some())
            .field(
                "effectiveness_tracker",
                &self.effectiveness_tracker.is_some(),
            )
            .field("meta_learning", &self.meta_learning.is_some())
            .field("evolution", &self.evolution.is_some())
            .field("discovery", &self.discovery.is_some())
            .field("evolution_cooldown_secs", &self.evolution_cooldown_secs)
            .field("tool_error_threshold", &self.tool_error_threshold)
            .field("hook_recurrence_threshold", &self.hook_recurrence_threshold)
            .field(
                "patch_recurrence_threshold",
                &self.patch_recurrence_threshold,
            )
            .finish()
    }
}

impl ReflectionEngine {
    /// Build a Phase-1 engine with in-memory storage and default thresholds.
    pub fn new_in_memory(
        skill_registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>,
    ) -> Self {
        let recorder: Arc<dyn LearningRecorder> = Arc::new(InMemoryRecorder::new());
        let detector: Arc<dyn LearningDetector> = Arc::new(DefaultLearningDetector::new());
        let matcher: Arc<dyn LearningMatcher> =
            Arc::new(DefaultLearningMatcher::new(recorder.clone(), None));
        let promoter: Arc<dyn LearningPromoter> =
            Arc::new(DefaultLearningPromoter::new(skill_registry));

        Self {
            detector,
            recorder,
            matcher,
            promoter,
            reviewer: None,
            extractor: None,
            effectiveness_tracker: None,
            meta_learning: None,
            evolution: None,
            discovery: None,
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            patch_recurrence_threshold: 5,
        }
    }

    /// Build a **deep self-evolution** engine with all components including
    /// effectiveness tracking, meta-learning, controlled evolution, and
    /// autonomous discovery.
    /// This is the highest-tier constructor for systems that need to improve
    /// themselves across sessions.
    #[allow(clippy::too_many_arguments)]
    pub fn new_self_evolution(
        skill_registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>,
        llm: Arc<dyn cog_core::LlmClient>,
        review_interval: std::time::Duration,
        memory_backend: Arc<dyn cog_core::MemoryBackend>,
        prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
        hook_sink: Option<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>,
        tool_sink: Option<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>,
        project_root: Option<std::path::PathBuf>,
    ) -> Self {
        let recorder: Arc<dyn LearningRecorder> = Arc::new(MemoryBackendRecorder::new(
            memory_backend.clone(),
            "reflection",
        ));
        let detector: Arc<dyn LearningDetector> = Arc::new(DefaultLearningDetector::new());
        let matcher: Arc<dyn LearningMatcher> =
            Arc::new(DefaultLearningMatcher::new(recorder.clone(), None));
        let promoter: Arc<dyn LearningPromoter> =
            Arc::new(DefaultLearningPromoter::new(skill_registry.clone()));
        let reviewer = Arc::new(PeriodicReviewer::new(
            recorder.clone(),
            matcher.clone(),
            promoter.clone(),
            review_interval,
        ));
        let extractor = Arc::new(SkillExtractor::new(
            llm.clone(),
            skill_registry.clone(),
            prompt_manager.clone(),
        ));
        let effectiveness_tracker = Arc::new(SkillEffectivenessTracker::new(recorder.clone()));
        let meta_learning = Arc::new(MetaLearningEngine::new(recorder.clone()));
        let mut evolution =
            EvolutionEngine::new(llm.clone(), skill_registry.clone(), prompt_manager.clone());
        if let Some(tx) = hook_sink {
            evolution = evolution.with_hook_sink(tx);
        }
        if let Some(tx) = tool_sink {
            evolution = evolution.with_tool_sink(tx);
        }
        if let Some(root) = project_root {
            evolution = evolution.with_project_root(root);
        }
        let evolution = Arc::new(evolution);
        let discovery = Arc::new(DiscoveryEngine::new(recorder.clone()));

        Self {
            detector,
            recorder,
            matcher,
            promoter,
            reviewer: Some(reviewer),
            extractor: Some(extractor),
            effectiveness_tracker: Some(effectiveness_tracker),
            meta_learning: Some(meta_learning),
            evolution: Some(evolution),
            discovery: Some(discovery),
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            patch_recurrence_threshold: 5,
        }
    }

    /// Build a production-grade engine with all Phase-1/2/3 components.
    /// Uses [`MemoryBackendRecorder`] so that learnings and errors are persisted
    /// through the three-layer memory pipeline (raw → schema → summary).
    pub fn new_production(
        skill_registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>,
        llm: Arc<dyn cog_core::LlmClient>,
        review_interval: std::time::Duration,
        memory_backend: Arc<dyn cog_core::MemoryBackend>,
        prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
    ) -> Self {
        let recorder: Arc<dyn LearningRecorder> =
            Arc::new(MemoryBackendRecorder::new(memory_backend, "reflection"));
        let detector: Arc<dyn LearningDetector> = Arc::new(DefaultLearningDetector::new());
        let matcher: Arc<dyn LearningMatcher> =
            Arc::new(DefaultLearningMatcher::new(recorder.clone(), None));
        let promoter: Arc<dyn LearningPromoter> =
            Arc::new(DefaultLearningPromoter::new(skill_registry.clone()));
        let reviewer = Arc::new(PeriodicReviewer::new(
            recorder.clone(),
            matcher.clone(),
            promoter.clone(),
            review_interval,
        ));
        let extractor = Arc::new(SkillExtractor::new(llm, skill_registry, prompt_manager));

        Self {
            detector,
            recorder,
            matcher,
            promoter,
            reviewer: Some(reviewer),
            extractor: Some(extractor),
            effectiveness_tracker: None,
            meta_learning: None,
            evolution: None,
            discovery: None,
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            patch_recurrence_threshold: 5,
        }
    }

    /// Build an engine with an arbitrary recorder (e.g. custom backend).
    pub fn new_with_recorder(
        recorder: Arc<dyn LearningRecorder>,
        skill_registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>,
        llm: Arc<dyn cog_core::LlmClient>,
        review_interval: std::time::Duration,
        prompt_manager: Option<Arc<dyn cog_core::PromptProvider>>,
    ) -> Self {
        let detector: Arc<dyn LearningDetector> = Arc::new(DefaultLearningDetector::new());
        let matcher: Arc<dyn LearningMatcher> =
            Arc::new(DefaultLearningMatcher::new(recorder.clone(), None));
        let promoter: Arc<dyn LearningPromoter> =
            Arc::new(DefaultLearningPromoter::new(skill_registry.clone()));
        let reviewer = Arc::new(PeriodicReviewer::new(
            recorder.clone(),
            matcher.clone(),
            promoter.clone(),
            review_interval,
        ));
        let extractor = Arc::new(SkillExtractor::new(llm, skill_registry, prompt_manager));

        Self {
            detector,
            recorder,
            matcher,
            promoter,
            reviewer: Some(reviewer),
            extractor: Some(extractor),
            effectiveness_tracker: None,
            meta_learning: None,
            evolution: None,
            discovery: None,
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            patch_recurrence_threshold: 5,
        }
    }

    /// Convenience: detect + record + match in one call.
    pub async fn process_learning(&self, learning: Learning) -> cog_core::SFResult<Learning> {
        let mut learning = learning;
        self.recorder.record_learning(learning.clone()).await?;
        self.matcher.update_recurrence(&mut learning).await?;
        Ok(learning)
    }

    /// Check whether the evolution trigger keyed by `key` is still in its
    /// cooldown period.  If not, mark it as triggered now and return `true`.
    async fn check_evolution_cooldown(&self, key: &str) -> bool {
        let mut cooldowns = self.evolution_cooldowns.lock().await;
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::seconds(self.evolution_cooldown_secs as i64);
        cooldowns.retain(|_k, v| *v > cutoff);
        match cooldowns.get(key) {
            Some(last)
                if now.signed_duration_since(*last).num_seconds()
                    < self.evolution_cooldown_secs as i64 =>
            {
                tracing::debug!("Evolution trigger '{}' still in cooldown", key);
                false
            }
            _ => {
                cooldowns.insert(key.to_string(), now);
                true
            }
        }
    }

    /// Process an `AgentEvent` through the full pipeline:
    /// detect → record → match → (optionally) promote → evolution triggers.
    pub async fn process_event(&self, event: &cog_core::AgentEvent) -> cog_core::SFResult<()> {
        // 1. Error detection from events
        if let Some(error_entry) = self.detector.detect_error(event) {
            self.recorder.record_error(error_entry).await?;
        }

        // 2. Self-review extraction
        let learnings = self.detector.detect_from_self_review(event);
        for learning in learnings {
            let mut l = learning;
            self.recorder.record_learning(l.clone()).await?;
            self.matcher.update_recurrence(&mut l).await?;

            // Attempt promotion immediately
            let _ = self.promoter.promote_if_ready(&l).await;

            // Evolution triggers for mature learnings (same logic as process_context)
            self.maybe_trigger_evolution_from_learning(&l).await;
        }

        Ok(())
    }

    /// Given a mature learning, trigger synthesize_hook / generate_code_patch
    /// if recurrence thresholds are crossed and cooldown allows.
    async fn maybe_trigger_evolution_from_learning(&self, l: &cog_core::Learning) {
        if l.recurrence_count >= self.hook_recurrence_threshold {
            let hook_key = format!(
                "hook:{}:{:?}",
                l.pattern_key.as_deref().unwrap_or("unknown"),
                l.area
            );
            if self.check_evolution_cooldown(&hook_key).await {
                if let Some(ref evolution) = self.evolution {
                    let event_pattern = format!("{}: {}", l.summary, l.details);
                    let action_outcomes = vec![
                        l.suggested_action.clone(),
                        format!("Area: {:?}, Category: {:?}", l.area, l.category),
                    ];
                    tracing::info!(
                        learning_id = %l.id,
                        recurrence = l.recurrence_count,
                        "Triggering synthesize_hook for mature learning"
                    );
                    if let Err(e) = evolution
                        .synthesize_hook(&event_pattern, &action_outcomes)
                        .await
                    {
                        tracing::warn!("synthesize_hook failed: {}", e);
                    }
                }
            }
        }

        let is_code_related = matches!(l.category, cog_core::LearningCategory::Correction);
        if is_code_related && l.recurrence_count >= self.patch_recurrence_threshold {
            let patch_key = format!(
                "patch:{}:{:?}",
                l.pattern_key.as_deref().unwrap_or("unknown"),
                l.area
            );
            if self.check_evolution_cooldown(&patch_key).await {
                if let Some(ref evolution) = self.evolution {
                    let module_description = format!("{:?} module", l.area);
                    let learning_context = format!(
                        "Recurring {:?} ({}x): {}. Suggested fix: {}",
                        l.category, l.recurrence_count, l.details, l.suggested_action
                    );
                    tracing::info!(
                        learning_id = %l.id,
                        recurrence = l.recurrence_count,
                        "Triggering generate_code_patch for recurring code defect"
                    );
                    if let Err(e) = evolution
                        .generate_code_patch(&module_description, &learning_context)
                        .await
                    {
                        tracing::warn!("generate_code_patch failed: {}", e);
                    }
                }
            }
        }
    }

    /// Process tool execution results for pattern detection.
    /// When a tool fails repeatedly, triggers `suggest_tool_variant` via the
    /// evolution engine so the system can autonomously improve its tooling.
    pub async fn process_tool_result(
        &self,
        tool_name: &str,
        result: &serde_json::Value,
        is_error: bool,
    ) -> cog_core::SFResult<()> {
        if let Some(error_entry) = self
            .detector
            .detect_from_tool_result(tool_name, result, is_error)
        {
            self.recorder.record_error(error_entry).await?;
        }

        if is_error {
            let mut counts = self.tool_error_counts.lock().await;
            let count = counts.entry(tool_name.to_string()).or_insert(0);
            *count += 1;
            let current = *count;
            drop(counts);

            if current >= self.tool_error_threshold {
                let cooldown_key = format!("tool:{}", tool_name);
                if self.check_evolution_cooldown(&cooldown_key).await {
                    if let Some(ref evolution) = self.evolution {
                        let error_patterns =
                            vec![serde_json::to_string(result).unwrap_or_default()];
                        tracing::info!(
                            tool = %tool_name,
                            errors = current,
                            "Triggering suggest_tool_variant for repeatedly failing tool"
                        );
                        if let Err(e) = evolution
                            .suggest_tool_variant(tool_name, &error_patterns)
                            .await
                        {
                            tracing::warn!("suggest_tool_variant failed for {}: {}", tool_name, e);
                        }
                    }
                    // Reset counter so we don't re-trigger immediately.
                    let mut counts = self.tool_error_counts.lock().await;
                    counts.remove(tool_name);
                }
            }
        } else {
            // Success: decay the error count (remove entry to prevent unbounded growth).
            let mut counts = self.tool_error_counts.lock().await;
            counts.remove(tool_name);
        }

        Ok(())
    }

    /// Process a full context window after a run completes.
    /// When learnings reach maturity (recurrence threshold), triggers
    /// `synthesize_hook` and, for code-related issues, `generate_code_patch`.
    pub async fn process_context(&self, messages: &[cog_core::Message]) -> cog_core::SFResult<()> {
        let learnings = self.detector.detect_from_context(messages);
        for learning in learnings {
            let mut l = learning;
            self.recorder.record_learning(l.clone()).await?;
            self.matcher.update_recurrence(&mut l).await?;
            let _ = self.promoter.promote_if_ready(&l).await;
            self.maybe_trigger_evolution_from_learning(&l).await;
        }
        Ok(())
    }

    /// Trigger skill extraction from a mature pattern (called after
    /// SelfReview indicates NEED_REVISION).
    pub async fn extract_from_pattern(
        &self,
        pattern: &cog_core::Pattern,
    ) -> cog_core::SFResult<Option<String>> {
        if let Some(ref extractor) = self.extractor {
            extractor.extract_and_insert(pattern).await
        } else {
            Ok(None)
        }
    }

    /// Start the background periodic reviewer if configured.
    pub fn start_reviewer(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.reviewer.as_ref().map(|r| {
            let r = r.clone();
            tokio::spawn(async move {
                r.run().await;
            })
        })
    }

    #[cfg(test)]
    pub fn set_cooldown_secs(&mut self, secs: u64) {
        self.evolution_cooldown_secs = secs;
    }

    // ========================================================================
    // Deep Self-Evolution Integration Methods
    // ========================================================================

    /// Feed a skill usage outcome into the effectiveness tracker.
    pub async fn process_skill_outcome(
        &self,
        outcome: cog_core::SkillOutcome,
    ) -> cog_core::SFResult<()> {
        if let Some(ref tracker) = self.effectiveness_tracker {
            tracker.record_outcome(outcome).await?;
        }
        Ok(())
    }

    /// Apply effectiveness-tracker recommendations to the skill registry.
    pub async fn apply_effectiveness_actions(
        &self,
        skill_registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>,
        llm: Option<Arc<dyn cog_core::LlmClient>>,
    ) -> cog_core::SFResult<()> {
        if let Some(ref tracker) = self.effectiveness_tracker {
            let skill_ids = tracker.tracked_skill_ids().await;
            for sid in skill_ids {
                tracker
                    .apply_action(&sid, skill_registry.clone(), llm.clone())
                    .await?;
            }
        }
        Ok(())
    }

    /// Recommend a PGE mode using meta-learning.
    pub async fn recommend_mode(
        &self,
        features: &cog_core::TaskFeatures,
    ) -> cog_core::ModeRecommendation {
        match self.meta_learning {
            Some(ref meta) => meta.recommend_mode(features).await,
            None => cog_core::ModeRecommendation::UseDefault,
        }
    }

    /// Record the actual outcome of a mode decision so meta-learning can improve.
    pub async fn record_mode_outcome(
        &self,
        features: &cog_core::TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> cog_core::SFResult<()> {
        if let Some(ref meta) = self.meta_learning {
            meta.record_outcome(features, selected_mode, success, score, latency_ms)
                .await?;
        }
        Ok(())
    }

    /// Generate a batch of discovery tasks.
    pub async fn generate_discovery_tasks(
        &self,
        tool_names: &[String],
        skill_ids: &[String],
        max_tasks: usize,
    ) -> Vec<crate::types::DiscoveryTask> {
        match self.discovery {
            Some(ref disc) => disc.generate_tasks(tool_names, skill_ids, max_tasks).await,
            None => Vec::new(),
        }
    }

    /// Evaluate a discovery task result.
    pub async fn evaluate_discovery_result(
        &self,
        task: &crate::types::DiscoveryTask,
        success: bool,
        notes: &str,
    ) -> cog_core::SFResult<crate::types::DiscoveryStatus> {
        match self.discovery {
            Some(ref disc) => disc.evaluate_result(task, success, notes).await,
            None => Ok(crate::types::DiscoveryStatus::Inconclusive),
        }
    }

    /// Record the result of a Squad run so reflection can learn from
    /// collaboration quality.
    pub async fn record_squad_result(
        &self,
        task_id: &str,
        goal: &str,
        success: bool,
        pge_mode: &str,
        score: Option<f32>,
        latency_ms: u64,
    ) -> cog_core::SFResult<()> {
        let category = if success {
            cog_core::LearningCategory::Insight
        } else {
            cog_core::LearningCategory::Correction
        };
        let priority = if success {
            cog_core::Priority::Medium
        } else {
            cog_core::Priority::High
        };
        let summary = format!(
            "Squad {} for self-evolution task {}",
            if success { "succeeded" } else { "failed" },
            task_id
        );
        let details = format!(
            "Goal: {}\nPGE mode: {}\nScore: {:?}\nLatency: {}ms",
            goal, pge_mode, score, latency_ms
        );
        let mut learning = cog_core::Learning::new(
            category,
            priority,
            cog_core::Area::Backend,
            summary,
            details,
            "Use this outcome to improve future self-evolution patch generation",
            cog_core::LearningSource::SelfReview,
        );
        learning.related_tasks.push(task_id.to_string());
        self.recorder.record_learning(learning.clone()).await?;
        self.matcher.update_recurrence(&mut learning).await?;

        if let Some(ref tracker) = self.effectiveness_tracker {
            let outcome = cog_core::SkillOutcome {
                skill_id: "squad_execution".into(),
                task_signature: format!("self_evolution:{}", goal),
                success,
                score,
                latency_ms,
                token_cost: 0,
                observed_at: chrono::Utc::now(),
            };
            tracker.record_outcome(outcome).await?;
        }

        Ok(())
    }

    /// Record the outcome of a generated patch after it has been applied,
    /// tested, built, or deployed.
    pub async fn record_patch_outcome(
        &self,
        patch_id: &str,
        success: bool,
        test_output: &str,
    ) -> cog_core::SFResult<()> {
        let category = if success {
            cog_core::LearningCategory::BestPractice
        } else {
            cog_core::LearningCategory::Correction
        };
        let priority = if success {
            cog_core::Priority::Medium
        } else {
            cog_core::Priority::High
        };
        let summary = format!(
            "Patch {} {}",
            patch_id,
            if success {
                "deployed successfully"
            } else {
                "failed during apply/test/build/deploy"
            }
        );
        let truncated = if test_output.len() > 2000 {
            &test_output[..2000]
        } else {
            test_output
        };
        let details = format!("Test output summary: {}", truncated);
        let mut learning = cog_core::Learning::new(
            category,
            priority,
            cog_core::Area::Backend,
            summary,
            details,
            "Use this outcome to improve future patch generation and validation",
            cog_core::LearningSource::SelfReview,
        );
        learning.related_tasks.push(patch_id.to_string());
        self.recorder.record_learning(learning.clone()).await?;
        self.matcher.update_recurrence(&mut learning).await?;

        if let Some(ref tracker) = self.effectiveness_tracker {
            let outcome = cog_core::SkillOutcome {
                skill_id: "patch_deployment".into(),
                task_signature: format!("patch:{}", patch_id),
                success,
                score: if success { Some(1.0) } else { Some(0.0) },
                latency_ms: 0,
                token_cost: 0,
                observed_at: chrono::Utc::now(),
            };
            tracker.record_outcome(outcome).await?;
        }

        Ok(())
    }

    /// Trigger skill refinement via the evolution engine.
    pub async fn evolve_skill(
        &self,
        skill_id: &str,
    ) -> cog_core::SFResult<Option<crate::types::EvolutionResult>> {
        match self.evolution {
            Some(ref evo) => evo.refine_skill(skill_id).await,
            None => Ok(None),
        }
    }

    /// Generate a code patch (L2 evolution) and write it to disk.
    pub async fn generate_code_patch(
        &self,
        module_description: &str,
        learning_context: &str,
    ) -> cog_core::SFResult<Option<crate::types::EvolutionResult>> {
        match self.evolution {
            Some(ref evo) => {
                evo.generate_code_patch(module_description, learning_context)
                    .await
            }
            None => Ok(None),
        }
    }

    /// Synthesize a hook definition from an observed event pattern.
    pub async fn synthesize_hook(
        &self,
        event_pattern: &str,
        action_outcomes: &[String],
    ) -> cog_core::SFResult<Option<crate::types::EvolutionResult>> {
        match self.evolution {
            Some(ref evo) => evo.synthesize_hook(event_pattern, action_outcomes).await,
            None => Ok(None),
        }
    }

    /// Suggest an improved tool variant based on observed error patterns.
    pub async fn suggest_tool_variant(
        &self,
        tool_name: &str,
        error_patterns: &[String],
    ) -> cog_core::SFResult<Option<crate::types::EvolutionResult>> {
        match self.evolution {
            Some(ref evo) => evo.suggest_tool_variant(tool_name, error_patterns).await,
            None => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl cog_core::ReflectionEngine for ReflectionEngine {
    async fn process_tool_result(
        &self,
        tool_name: &str,
        result: &serde_json::Value,
        is_error: bool,
    ) -> cog_core::SFResult<()> {
        Self::process_tool_result(self, tool_name, result, is_error).await
    }

    async fn process_context(&self, messages: &[cog_core::Message]) -> cog_core::SFResult<()> {
        Self::process_context(self, messages).await
    }

    async fn process_event(&self, event: &cog_core::AgentEvent) -> cog_core::SFResult<()> {
        Self::process_event(self, event).await
    }

    async fn extract_and_insert(
        &self,
        pattern: &cog_core::Pattern,
    ) -> cog_core::SFResult<Option<String>> {
        Self::extract_from_pattern(self, pattern).await
    }

    async fn process_skill_outcome(
        &self,
        outcome: cog_core::SkillOutcome,
    ) -> cog_core::SFResult<()> {
        Self::process_skill_outcome(self, outcome).await
    }

    async fn record_squad_result(
        &self,
        task_id: &str,
        goal: &str,
        success: bool,
        pge_mode: &str,
        score: Option<f32>,
        latency_ms: u64,
    ) -> cog_core::SFResult<()> {
        Self::record_squad_result(self, task_id, goal, success, pge_mode, score, latency_ms).await
    }

    async fn record_patch_outcome(
        &self,
        patch_id: &str,
        success: bool,
        test_output: &str,
    ) -> cog_core::SFResult<()> {
        Self::record_patch_outcome(self, patch_id, success, test_output).await
    }

    fn start_reviewer(&self) -> Option<tokio::task::JoinHandle<()>> {
        Self::start_reviewer(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Area, Learning, LearningCategory, LearningSource, Priority, SkillRegistry};

    #[tokio::test]
    async fn test_reflection_engine_processes_learning() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let engine = ReflectionEngine::new_in_memory(registry);

        let learning = Learning::new(
            LearningCategory::Correction,
            Priority::High,
            Area::Backend,
            "Test correction",
            "Detailed description of the correction",
            "Fix the issue",
            LearningSource::UserFeedback,
        );

        let processed = engine.process_learning(learning.clone()).await.unwrap();
        assert_eq!(processed.id, learning.id);

        let stored = engine.recorder.get_learning(&learning.id).await.unwrap();
        assert!(stored.is_some());
    }

    #[tokio::test]
    async fn test_detector_finds_corrections() {
        let detector = DefaultLearningDetector::new();
        let messages = vec![cog_core::Message::user(
            "Actually, that's not right. You missed the error handling.",
        )];
        let learnings = detector.detect_correction(&messages);
        assert!(!learnings.is_empty());
        assert_eq!(learnings[0].category, LearningCategory::Correction);
    }

    #[tokio::test]
    async fn test_promoter_thresholds() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let promoter = DefaultLearningPromoter::new(registry)
            .with_min_recurrence(3)
            .with_min_tasks(2)
            .with_max_age_days(30);

        let mut learning = Learning::new(
            LearningCategory::BestPractice,
            Priority::Medium,
            Area::Tests,
            "A best practice",
            "Details",
            "Action",
            LearningSource::SelfReview,
        );
        learning.recurrence_count = 2;
        assert!(!promoter.should_promote(&learning));

        learning.recurrence_count = 3;
        learning.related_tasks = vec!["task-1".into(), "task-2".into()];
        assert!(promoter.should_promote(&learning));
    }

    #[tokio::test]
    async fn test_matcher_detects_similar() {
        let recorder = Arc::new(InMemoryRecorder::new());
        let matcher = DefaultLearningMatcher::new(recorder.clone(), None);

        let l1 = Learning::new(
            LearningCategory::Insight,
            Priority::Medium,
            Area::Backend,
            "Database connection pooling",
            "Use connection pooling for better performance",
            "Implement pool",
            LearningSource::Conversation,
        );
        recorder.record_learning(l1.clone()).await.unwrap();

        let l2 = Learning::new(
            LearningCategory::Insight,
            Priority::Medium,
            Area::Backend,
            "Database connection pooling issue",
            "Connection pooling improves database performance significantly",
            "Add pooling",
            LearningSource::Conversation,
        );

        let similar = matcher.find_similar(&l2).await.unwrap();
        assert!(!similar.is_empty());
        assert_eq!(similar[0].id, l1.id);
    }

    /// Build a fake event stream from a plain text payload.
    fn fake_stream_from_text(
        text: &str,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        let (mut _stream, mut producer) = cog_core::EventStream::with_capacity(1);
        let response = cog_core::ChatResponse {
            content: vec![cog_core::ContentBlock::Text {
                text: text.to_string(),
                text_signature: None,
            }],
            api: "fake".into(),
            provider: "fake".into(),
            model: "fake".into(),
            response_id: None,
            usage: Default::default(),
            stop_reason: cog_core::StopReason::Stop,
            error_message: None,
            timestamp: chrono::Utc::now(),
        };
        let event = cog_core::AssistantMessageEvent::TextEnd {
            content_index: 0,
            content: text.to_string(),
            partial: cog_core::Message::assistant_text("test"),
            timestamp: chrono::Utc::now(),
        };
        let _ = producer.try_push(event);
        producer.end(response);
        Ok(_stream)
    }

    /// A fake LLM that counts chat_stream invocations and returns a minimal
    /// valid JSON payload so EvolutionEngine methods do not panic.
    struct CountingLlm {
        calls: Arc<tokio::sync::Mutex<u32>>,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for CountingLlm {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            let mut calls = self.calls.lock().await;
            *calls += 1;
            drop(calls);
            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::text(
                    r#"{"name":"auto_tool","description":"auto","parameters":{"type":"object"}}"#,
                )],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            let mut calls = self.calls.lock().await;
            *calls += 1;
            drop(calls);
            fake_stream_from_text(
                r#"{"name":"auto_tool","description":"auto","parameters":{"type":"object"}}"#,
            )
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

    #[tokio::test]
    async fn test_process_tool_result_triggers_suggest_tool_variant_after_threshold() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let mut engine = ReflectionEngine::new_in_memory(registry);
        let calls = Arc::new(tokio::sync::Mutex::new(0u32));
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(CountingLlm {
            calls: calls.clone(),
        });
        engine.evolution = Some(Arc::new(EvolutionEngine::new(
            llm,
            Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())),
            None,
        )));
        // Lower threshold so the test doesn't need many iterations.
        engine.tool_error_threshold = 2;
        engine.set_cooldown_secs(0);

        let error_result = serde_json::json!({"error": "connection refused" });

        // First error – no trigger yet.
        engine
            .process_tool_result("test_tool", &error_result, true)
            .await
            .unwrap();
        let c1 = *calls.lock().await;
        assert_eq!(c1, 0, "Should not trigger on first error");

        // Second error – crosses threshold, should trigger suggest_tool_variant.
        engine
            .process_tool_result("test_tool", &error_result, true)
            .await
            .unwrap();
        let c2 = *calls.lock().await;
        assert_eq!(c2, 1, "Should trigger suggest_tool_variant on second error");

        // Success should reset the counter.
        engine
            .process_tool_result("test_tool", &serde_json::json!({"ok": true}), false)
            .await
            .unwrap();

        // Two more errors should trigger again (counter was reset).
        engine
            .process_tool_result("test_tool", &error_result, true)
            .await
            .unwrap();
        let c3 = *calls.lock().await;
        assert_eq!(c3, 1, "Still only 1 after first post-reset error");

        engine
            .process_tool_result("test_tool", &error_result, true)
            .await
            .unwrap();
        let c4 = *calls.lock().await;
        assert_eq!(c4, 2, "Should trigger again after reset + threshold");
    }

    #[tokio::test]
    async fn test_process_context_triggers_synthesize_hook_after_recurrence() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let mut engine = ReflectionEngine::new_in_memory(registry.clone());
        let calls = Arc::new(tokio::sync::Mutex::new(0u32));
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(CountingLlm {
            calls: calls.clone(),
        });
        engine.evolution = Some(Arc::new(EvolutionEngine::new(llm, registry, None)));
        // Lower thresholds.
        engine.hook_recurrence_threshold = 2;
        engine.patch_recurrence_threshold = 5;
        engine.set_cooldown_secs(0);

        // Seed the recorder with a similar learning so recurrence bumps to 2.
        let mut seed = Learning::new(
            LearningCategory::Insight,
            Priority::High,
            Area::Backend,
            "Self-review critique for agent 'test-agent'",
            "The agent keeps making the same mistake.",
            "Add working-memory reminders",
            LearningSource::SelfReview,
        );
        seed.recurrence_count = 1;
        engine.recorder.record_learning(seed.clone()).await.unwrap();

        // Trigger a SelfReview event that produces a matching learning.
        let event = cog_core::AgentEvent::SelfReview {
            agent_id: "test-agent".into(),
            status: "NEED_REVISION".into(),
            score: 0.5,
            critique: Some("The agent keeps making the same mistake.".into()),
            suggestions: Some(vec!["Add working-memory reminders".into()]),
            summary: None,
            timestamp: chrono::Utc::now(),
        };

        engine.process_event(&event).await.unwrap();

        let c = *calls.lock().await;
        assert!(
            c >= 1,
            "Should trigger synthesize_hook when recurrence_count reaches threshold"
        );
    }

    /// A fake LLM that returns a fixed JSON payload for testing channel flows.
    struct FixedResponseLlm {
        response: String,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for FixedResponseLlm {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::text(self.response.clone())],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            fake_stream_from_text(&self.response)
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

    #[tokio::test]
    async fn test_process_tool_result_sends_to_tool_sink() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let mut engine = ReflectionEngine::new_in_memory(registry);
        let (tool_tx, mut tool_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(FixedResponseLlm {
            response: r#"{"name":"test_tool_v2","description":"Improved test tool","parameters":{"type":"object","properties":{}}}"#.to_string(),
        });

        let mut evolution = crate::evolution::EvolutionEngine::new(
            llm,
            Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())),
            None,
        );
        evolution = evolution.with_tool_sink(tool_tx);
        engine.evolution = Some(Arc::new(evolution));
        engine.tool_error_threshold = 1;
        engine.set_cooldown_secs(0);

        engine
            .process_tool_result(
                "test_tool",
                &serde_json::json!({"error": "connection refused"}),
                true,
            )
            .await
            .unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), tool_rx.recv())
            .await
            .unwrap()
            .expect("tool_sink should receive a tool variant JSON");
        assert_eq!(received["name"], "test_tool_v2");
        assert_eq!(received["description"], "Improved test tool");
        assert_eq!(received["parameters"]["type"], "object");
    }

    #[tokio::test]
    async fn test_process_event_sends_to_hook_sink() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let mut engine = ReflectionEngine::new_in_memory(registry.clone());
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(FixedResponseLlm {
            response: r#"{"id":"test-hook","trigger":"on_task_fail","action":{"type":"log","level":"info"}}"#.to_string(),
        });

        let mut evolution = crate::evolution::EvolutionEngine::new(llm, registry, None);
        evolution = evolution.with_hook_sink(hook_tx);
        engine.evolution = Some(Arc::new(evolution));
        engine.hook_recurrence_threshold = 1;
        engine.set_cooldown_secs(0);

        // Seed the recorder with a matching learning so recurrence bumps to threshold.
        let mut seed = Learning::new(
            LearningCategory::Insight,
            Priority::High,
            Area::Backend,
            "Self-review critique for agent 'test-agent'",
            "The agent keeps making the same mistake.",
            "Add working-memory reminders",
            LearningSource::SelfReview,
        );
        seed.recurrence_count = 1;
        engine.recorder.record_learning(seed.clone()).await.unwrap();

        let event = cog_core::AgentEvent::SelfReview {
            agent_id: "test-agent".into(),
            status: "NEED_REVISION".into(),
            score: 0.5,
            critique: Some("The agent keeps making the same mistake.".into()),
            suggestions: Some(vec!["Add working-memory reminders".into()]),
            summary: None,
            timestamp: chrono::Utc::now(),
        };

        engine.process_event(&event).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), hook_rx.recv())
            .await
            .unwrap()
            .expect("hook_sink should receive a hook JSON");
        assert_eq!(received["id"], "test-hook");
        assert_eq!(received["trigger"], "on_task_fail");
        assert_eq!(received["action"]["type"], "log");
    }

    #[tokio::test]
    async fn test_evolution_results_persisted_with_generated_status() {
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(FixedResponseLlm {
            response: r#"{"name":"persisted_tool","description":"A tool","parameters":{"type":"object","properties":{}}}"#.to_string(),
        });
        let evolution = EvolutionEngine::new(
            llm,
            Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())),
            None,
        );

        let result = evolution
            .suggest_tool_variant("base_tool", &["error1".into()])
            .await
            .unwrap()
            .expect("suggest_tool_variant should return a result");

        assert_eq!(result.status, EvolutionStatus::Generated);
        assert_eq!(result.artifact_id, "persisted_tool");

        let listed = evolution.list_results().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].artifact_id, "persisted_tool");
        assert_eq!(listed[0].status, EvolutionStatus::Generated);
    }

    #[tokio::test]
    async fn test_evolution_update_status_transitions_correctly() {
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(FixedResponseLlm {
            response: r#"{"name":"status_tool","description":"A tool","parameters":{"type":"object","properties":{}}}"#.to_string(),
        });
        let evolution = EvolutionEngine::new(
            llm,
            Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())),
            None,
        );

        evolution
            .suggest_tool_variant("base", &["err".into()])
            .await
            .unwrap()
            .expect("should generate");

        let found = evolution
            .update_status("status_tool", EvolutionStatus::Registered)
            .await;
        assert!(found, "update_status should find the result");

        let listed = evolution.list_results().await;
        assert_eq!(listed[0].status, EvolutionStatus::Registered);
    }

    #[tokio::test]
    async fn test_process_tool_result_full_loop_status_lifecycle() {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let mut engine = ReflectionEngine::new_in_memory(registry);
        let (tool_tx, mut tool_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(FixedResponseLlm {
            response: r#"{"name":"lifecycle_tool","description":"Lifecycle test tool","parameters":{"type":"object","properties":{}}}"#.to_string(),
        });

        let mut evolution = crate::evolution::EvolutionEngine::new(
            llm,
            Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())),
            None,
        );
        evolution = evolution.with_tool_sink(tool_tx);
        let evolution_arc = Arc::new(evolution);
        engine.evolution = Some(evolution_arc.clone());
        engine.tool_error_threshold = 1;
        engine.set_cooldown_secs(0);

        engine
            .process_tool_result(
                "test_tool",
                &serde_json::json!({"error": "connection refused"}),
                true,
            )
            .await
            .unwrap();

        // Bridge task: receive from channel and update status to Registered.
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), tool_rx.recv())
            .await
            .unwrap()
            .expect("tool_sink should receive JSON");
        let name = received["name"].as_str().unwrap();
        evolution_arc
            .update_status(name, EvolutionStatus::Registered)
            .await;

        // Verify the result is now Registered.
        let results = evolution_arc.list_results().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].artifact_id, "lifecycle_tool");
        assert_eq!(results[0].status, EvolutionStatus::Registered);
    }

    #[tokio::test]
    async fn test_synthesize_hook_persists_result_with_status() {
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(FixedResponseLlm {
            response:
                r#"{"id":"hook-status-test","trigger":"on_task_fail","action":{"type":"log"}}"#
                    .to_string(),
        });
        let evolution = EvolutionEngine::new(
            llm,
            Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())),
            None,
        );

        let result = evolution
            .synthesize_hook("pattern", &["outcome".into()])
            .await
            .unwrap()
            .expect("synthesize_hook should return a result");

        assert_eq!(result.status, EvolutionStatus::Generated);
        assert_eq!(result.artifact_id, "hook-status-test");

        let listed = evolution.list_results().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, EvolutionStatus::Generated);
    }
}
