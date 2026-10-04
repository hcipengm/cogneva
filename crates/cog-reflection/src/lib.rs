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

/// 反思条目在记忆后端里的命名空间。写入与补齐扫的是同一份归档，两处各写一个
/// 字面量迟早会分叉，而分叉的表现是补齐循环扫到空集合、报「没有孤儿」——正是
/// 它要防的那种静默假阴性。
pub const REFLECTION_NAMESPACE: &str = "reflection";

pub mod auto_promoter;
pub mod baseline_port;
// The readings and the rules of the shared build cache live in cog-core: two
// processes measure one and drop bytes from it (this deployment's builder and
// the sandbox executor), and neither may depend on the other. See the module
// docs for the one thing they do not share, the fact that says a build is
// running.
pub use cog_core::build_cache_readings;
pub use cog_core::build_cache_reclaim;
pub mod buildah_store;
pub mod change_execution;
pub mod change_pipeline;
pub mod change_rework;
pub mod config;
pub mod crew;
pub mod detector;
pub mod diff_fidelity;
pub mod discovery;
pub mod effectiveness;
pub mod eval_harness;
pub mod evolution;
pub mod evolution_admin;
pub mod evolution_build_readings;
pub mod evolution_deployer;
pub mod evolution_flight_readings;
pub mod evolution_queue_readings;
pub mod extractor;
pub mod fault_classifier;
pub mod firecracker;
pub mod flywheel;
pub mod gitops_publisher;
pub mod gitops_puller;
pub mod governance_drift;
pub mod image_rollout;
pub mod mainline_deployer;
pub mod matcher;
pub mod meta_learning;
pub mod observability_stack;
pub mod policy_evolution;
pub mod policy_store;
pub mod promoter;
pub mod promotion_gate;
pub mod promotion_switch;
pub mod promotion_trend;
pub mod recorder;
pub mod registry_footprint;
pub mod reviewer;
pub mod rollout_resources;
pub mod runtime_assets;
pub mod sandbox;
pub mod signal_readings;
pub mod signal_watcher;
pub mod squad;
pub mod types;
pub mod verification_budget;
pub mod version_contract;
pub mod workspace;

#[cfg(test)]
pub(crate) mod test_support;

pub use auto_promoter::{AutoPromoter, PromotionChannel, PromotionSource};
pub use baseline_port::{
    run_baseline_port_loop, AbsorptionStatus, BaselinePorter, PortItemResult, PortOutcome,
    PortPlan, PortPlanItem, PortReport, PortRoute, PromotedChange,
};
pub use change_pipeline::{ApplyResult, ChangePipeline};
use cog_core::{DecisionCategory, DecisionOutcome, Learning};
pub use config::{
    BaselinePortConfig, ChangeJobConfig, GitOpsConfig, MainlineDeployerConfig,
    ObservabilityStackConfig, PromotionGateConfig,
};
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
pub use fault_classifier::RuleBasedFaultClassifier;
pub use firecracker::{FirecrackerSandbox, MicroVm, MicroVmOutcome};
pub use flywheel::{JsonlFileSink, LearningSink, WarehouseRecorder};
pub use gitops_publisher::GitOpsPublisher;
pub use gitops_puller::{
    parse_tag_message, run_puller_loop, GitOpsPuller, PromotionCandidate, CANARY_GATE_BLIND_RULE,
};
pub use image_rollout::ImageRollout;
pub use mainline_deployer::{
    api_resource_of, classify_doc, run_mainline_loop, run_rollout_cli, workload_kind,
    workload_kind_of_resource, DocFate, FailureClass, MainlineDeployer, RolloutExecutor,
    RolloutFailure, RolloutPlan, RolloutTarget, WorkloadKind, DELIVERABLE_MANIFEST_TREES,
    WORKLOAD_KINDS,
};
pub use matcher::{DefaultLearningMatcher, LearningMatcher};
pub use meta_learning::{DecisionStatsSnapshot, MetaLearningEngine};
pub use observability_stack::{StackConvergence, STACK_NOT_CONVERGED_RULE};
pub use policy_evolution::{
    run_policy_evolution_loop, PolicyEvolutionConfig, PolicyEvolutionDriver, PolicyEvolutionOutcome,
};
pub use policy_store::{
    ArtifactEvolution, PolicyArtifact, PolicyCandidate, PolicyProposal, PolicyStore,
};
pub use promoter::{DefaultLearningPromoter, LearningPromoter};
pub use promotion_gate::{classify, count_diff_lines, GateVerdict};
pub use promotion_switch::PromotionSwitch;
pub use promotion_trend::PromotionTrendReporter;
pub use recorder::{InMemoryRecorder, LearningRecorder, MemoryBackendRecorder};
pub use reviewer::PeriodicReviewer;
pub use sandbox::{enforce_sandbox_boundary, BoundaryDecision, SandboxKind, SandboxSignals};
pub use signal_watcher::{spawn_signal_watcher_loop, SignalWatcherConfig};
pub use squad::DefaultSquadReflection;
pub use types::*;

pub mod plugin;

use std::sync::Arc;

/// The recurrence key of a refused change: which criterion, and on what.
///
/// Pure, because the merging rule is the whole contract and has to be checkable
/// without a store: two refusals accumulate into one count exactly when this key
/// matches, the matcher treats an exact key match as similarity 1.0, and that
/// path does not depend on embeddings — of which this deployment has none.
///
/// The files, rather than the goal text, are what "what" means here. The goal
/// is prose, so two refusals about the same file worded differently would take
/// two counts and neither would ever mature; the file set is the same fact
/// however it was described. Sorted and deduplicated so the same set is one key
/// whichever order the diff listed it in. A refusal that never got as far as
/// naming a file keys on the criterion alone, which is what it is: generation
/// emitting artifacts nothing can read.
pub fn refusal_pattern_key(
    cause: cog_core::RejectionCause,
    files: &[std::path::PathBuf],
) -> String {
    format!("change:refused:{}:{}", cause.as_str(), named_files(files))
}

/// How much of a change's outcome evidence a learning can carry.
///
/// The record is read by a generator, not by a person, so this is a budget
/// rather than a storage limit: whatever is dropped here is a thing the next
/// attempt cannot know. The spend is what matters, not the size — see
/// `failure_digest`, which puts the diagnosis inside it. Shared by the refusal
/// path and the general outcome path so one number governs both.
const CHANGE_EVIDENCE_BUDGET: usize = 2000;

/// The files a refusal names, spelled the one way the key and the record both
/// use them: sorted, deduplicated, comma-joined, empty when the refusal never
/// got as far as naming one.
fn named_files(files: &[std::path::PathBuf]) -> String {
    let mut named: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .collect();
    named.sort();
    named.dedup();
    named.join(",")
}

/// What the record says the refusal was about.
///
/// A refusal that named no file says so instead of leaving the field blank: an
/// empty value reads as "no file was involved", while the fact is that nothing
/// about this artifact could be read at all — the difference between a defect
/// in one file and generation emitting something no check can parse.
fn refusal_subject(cause: cog_core::RejectionCause, files: &[std::path::PathBuf]) -> String {
    let named = named_files(files);
    if named.is_empty() {
        format!("no file named (a {} refusal)", cause.as_str())
    } else {
        named
    }
}

/// The task id a hand-off is submitted under.
///
/// Named after the refused change when there is one: the point of the id is that
/// the next attempt is findable from the artifact that was refused, and the
/// change id is what every other reading of that artifact is keyed by. A defect
/// the system noticed on its own has no change yet, so it is keyed by its
/// learning instead.
///
/// The id is stable across retries of the same requirement — the orchestrator
/// treats a re-submission of a held id as an idempotent no-op — so a trigger that
/// fires twice cannot put two attempts on one defect.
fn rework_task_id(l: &cog_core::Learning, refused_by_a_gate: bool) -> String {
    let sanitize = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    };
    let subject = if refused_by_a_gate {
        l.related_tasks
            .first()
            .map(|id| sanitize(id))
            .unwrap_or_else(|| sanitize(&l.id))
    } else {
        sanitize(&l.id)
    };
    format!("rework-{subject}")
}

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
    /// Where a refused change is handed back to the main flow, and what became
    /// of each hand-off.
    ///
    /// Built here rather than passed in because the orchestrator it carries
    /// cannot be consumed until `start()`, while the trigger that reads it is
    /// live from `init()`. The plugin arms the gate and fills the slot; this
    /// engine only asks it whether it may submit.
    pub rework: Arc<crate::change_rework::ChangeReworkGate>,
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
    /// How many recurrences before generating a code change.
    change_recurrence_threshold: u32,
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
            .field("rework_owns_submission", &self.rework.owns_submission())
            .field("evolution_cooldown_secs", &self.evolution_cooldown_secs)
            .field("tool_error_threshold", &self.tool_error_threshold)
            .field("hook_recurrence_threshold", &self.hook_recurrence_threshold)
            .field(
                "change_recurrence_threshold",
                &self.change_recurrence_threshold,
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
            rework: Arc::new(crate::change_rework::ChangeReworkGate::new()),
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            change_recurrence_threshold: 5,
        }
    }

    /// Build a **deep self-evolution** engine with effectiveness tracking,
    /// controlled evolution, and autonomous discovery.
    /// This is the highest-tier constructor for systems that need to improve
    /// themselves across sessions.
    ///
    /// The meta-learning engine is *not* built here: it needs the durable state
    /// path, which belongs to the deployment's data volume rather than to this
    /// constructor, and a second engine built anywhere else would be a second
    /// object that records decisions nobody reads. The deployment installs the
    /// one engine through [`MetaLearningEngine::with_durable_state`].
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
        change_dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        let recorder: Arc<dyn LearningRecorder> = Arc::new(MemoryBackendRecorder::new(
            memory_backend.clone(),
            REFLECTION_NAMESPACE,
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
        let mut evolution =
            EvolutionEngine::new(llm.clone(), skill_registry.clone(), prompt_manager.clone())
                .with_change_dir(change_dir);
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
            meta_learning: None,
            evolution: Some(evolution),
            discovery: Some(discovery),
            rework: Arc::new(crate::change_rework::ChangeReworkGate::new()),
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            change_recurrence_threshold: 5,
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
        let recorder: Arc<dyn LearningRecorder> = Arc::new(MemoryBackendRecorder::new(
            memory_backend,
            REFLECTION_NAMESPACE,
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
            rework: Arc::new(crate::change_rework::ChangeReworkGate::new()),
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            change_recurrence_threshold: 5,
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
            rework: Arc::new(crate::change_rework::ChangeReworkGate::new()),
            evolution_cooldowns: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            evolution_cooldown_secs: 3600,
            tool_error_counts: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            tool_error_threshold: 3,
            hook_recurrence_threshold: 3,
            change_recurrence_threshold: 5,
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

    /// Given a mature learning, trigger synthesize_hook and, for a code defect,
    /// the hand-off to the main flow, if recurrence thresholds are crossed and
    /// cooldown allows.
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
        // Two ways to be mature enough to generate for, because the two kinds of
        // record are not the same kind of thing. A self-review pattern has to be
        // seen `change_recurrence_threshold` times before it is a defect rather
        // than a coincidence. A refusal was already a verdict on one artifact
        // the system chose to submit, and its evidence was a full test run: the
        // first one is worth acting on, and holding it until the count matured
        // is how the same defect got generated, refused and generated again.
        let refused_by_a_gate = matches!(l.source, cog_core::LearningSource::ChangeRefusal);
        if is_code_related
            && (refused_by_a_gate || l.recurrence_count >= self.change_recurrence_threshold)
        {
            let change_key = format!(
                "change:{}:{:?}",
                l.pattern_key.as_deref().unwrap_or("unknown"),
                l.area
            );
            if self.check_evolution_cooldown(&change_key).await {
                self.hand_off_change_rework(l, refused_by_a_gate).await;
            }
        }
    }

    /// Hand a code defect back to the main evolution flow as a task.
    ///
    /// The defect is generated for by the same generator every other entry point
    /// uses, not by a second one living here. What this side owns is the
    /// requirement: which files the gate named, what it said, and which criterion
    /// refused it. Those travel as data in the task payload rather than as prose
    /// in a prompt, so the next attempt is judged against the same facts by the
    /// same gate -- a second generator iterating on its own structural check is
    /// how a change gets generated, refused and generated again from the verdict
    /// text instead of from the tree.
    async fn hand_off_change_rework(&self, l: &cog_core::Learning, refused_by_a_gate: bool) {
        let orchestrator = match self.rework.submission() {
            // The requirement is not lost here: the process that owns submission
            // sees the same learning and hands it off.
            crate::change_rework::Submission::NotOwner => return,
            crate::change_rework::Submission::NoExecutor => {
                self.rework
                    .record(crate::change_rework::ChangeReworkOutcome::NoExecutor);
                tracing::warn!(
                    learning_id = %l.id,
                    "no orchestrator to hand a code defect to; the requirement has no way out"
                );
                return;
            }
            crate::change_rework::Submission::Ready(orchestrator) => orchestrator,
        };

        // The refused files are the requirement when they are known: they are
        // what the gate said was wrong, and naming them is the difference
        // between a next attempt that can find the defect and one that searches
        // a whole crate for it.
        let named_files: Vec<String> = l.related_files.clone();
        let target = if named_files.is_empty() {
            format!("{:?} module", l.area)
        } else {
            named_files.join(", ")
        };
        let goal = format!(
            "Fix a defect the {} check reported, on {}. Regenerate the change so \
             that it applies to the current tree and passes the gate that refused \
             the last attempt. Do not reconstruct the file from the verdict: read \
             the file as it is.\n\nEvidence:\n{}",
            l.rejection_cause
                .map(|c| c.as_str())
                .unwrap_or("change")
                .replace('_', " "),
            target,
            l.details
        );

        let task_id = rework_task_id(l, refused_by_a_gate);
        let task = cog_core::Task::new(
            task_id.clone(),
            cog_core::TaskType::Custom("change_rework".into()),
            serde_json::json!({
                "goal": goal,
                "evolution_mode": "generate_change",
                "task_kind": "change_rework",
                "learning_id": l.id,
                "rejection_cause": l.rejection_cause.map(|c| c.as_str()),
                // The two things the main flow needs, as fields rather than only
                // inside the goal text: the digest is what the refused run
                // actually printed, and the file list is where it printed it.
                "failure_digest": l.details,
                "named_files": named_files,
            }),
        );

        tracing::info!(
            learning_id = %l.id,
            task_id = %task_id,
            recurrence = l.recurrence_count,
            refused_by_a_gate,
            "Handing a code defect back to the main flow"
        );
        match orchestrator.submit_goal_auto(&goal, vec![task]).await {
            Ok(_) => self
                .rework
                .record(crate::change_rework::ChangeReworkOutcome::Submitted),
            Err(e) => {
                self.rework
                    .record(crate::change_rework::ChangeReworkOutcome::SubmitFailed);
                tracing::warn!(task_id = %task_id, error = %e, "rework hand-off refused by the orchestrator");
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
    /// `synthesize_hook` and, for code-related issues, a hand-off to the main
    /// flow's change generation.
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
            "Use this outcome to improve future self-evolution change generation",
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

    /// Record the outcome of a generated change after it has been applied,
    /// tested, built, or deployed.
    pub async fn record_change_outcome(
        &self,
        change_id: &str,
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
            "Change {} {}",
            change_id,
            if success {
                "deployed successfully"
            } else {
                "failed during apply/test/build/deploy"
            }
        );
        // Same spend as a refusal's evidence, and the same reason it is a spend
        // rather than a cut: see `failure_digest`. Slicing at a byte offset also
        // panicked here when the cut landed inside a multi-byte character, which
        // a transcript carrying one localised assertion message will do.
        let digest =
            cog_core::contract::reflection::failure_digest(test_output, CHANGE_EVIDENCE_BUDGET);
        let details = format!("Test output summary: {}", digest);
        let mut learning = cog_core::Learning::new(
            category,
            priority,
            cog_core::Area::Backend,
            summary,
            details,
            "Use this outcome to improve future change generation and validation",
            cog_core::LearningSource::SelfReview,
        );
        learning.related_tasks.push(change_id.to_string());
        self.recorder.record_learning(learning.clone()).await?;
        self.matcher.update_recurrence(&mut learning).await?;

        self.note_change_skill_outcome(change_id, success).await?;

        Ok(())
    }

    /// Record a change that a deterministic gate criterion refused.
    ///
    /// The refusal is the strongest evidence this system produces about its own
    /// generation: an artifact was written, the gate read it, and a check that
    /// needs no judgement said no. Two things make it usable, and neither was
    /// here.
    ///
    /// First, the recurrence key. A learning's count is what decides whether a
    /// defect is recurring enough to generate for, and between two refusals
    /// that count was decided by how much English their dumps happened to share
    /// — two changes stopped by two different checks could accumulate into one
    /// count, and one check failing on two files could fail to accumulate at
    /// all. The key here is built from the two facts that make the count mean
    /// something: which criterion, and which files it was refused on.
    ///
    /// Second, the offer to the triggers. Every other learning path hands its
    /// learning to `maybe_trigger_evolution_from_learning`; this one
    /// ended at the recorder, so the corpus grew a Correction per refused change
    /// while generation was never told about any of them. That is the whole
    /// reason a refusal is recorded at all.
    ///
    /// The general [`Self::record_change_outcome`] deliberately does not call
    /// the trigger: it also carries failures that are already answered by
    /// something bounded — a CI failure is re-driven under a per-cause budget —
    /// and generating from the same evidence as well would spend two budgets on
    /// one defect.
    pub async fn record_change_refusal(
        &self,
        change_id: &str,
        cause: cog_core::RejectionCause,
        files: &[std::path::PathBuf],
        detail: &str,
    ) -> cog_core::SFResult<()> {
        // Spend the budget on the diagnosis rather than on the head of the
        // transcript: a `--workspace` run puts its failing tests hundreds of
        // lines in, so a head-truncated dump hands generation the passing tests
        // and withholds the symptom. See `failure_digest`.
        let digest = cog_core::contract::reflection::failure_digest(detail, CHANGE_EVIDENCE_BUDGET);
        let mut learning = cog_core::Learning::new(
            cog_core::LearningCategory::Correction,
            cog_core::Priority::High,
            cog_core::Area::Backend,
            format!(
                "Change {} was refused by the {} check",
                change_id,
                cause.as_str()
            ),
            format!(
                "Refused on: {}\nEvidence: {}",
                refusal_subject(cause, files),
                digest
            ),
            format!(
                "Read what the {} check reported before generating for this again",
                cause.as_str()
            ),
            cog_core::LearningSource::ChangeRefusal,
        );
        learning.pattern_key = Some(refusal_pattern_key(cause, files));
        learning.rejection_cause = Some(cause);
        // The refused files travel as data, not only inside the prose of
        // `details`, because they are the target of the generation this record
        // may trigger: a requirement that says "Backend module" sends the next
        // attempt looking for the defect anywhere in a crate when the gate
        // named the file. Sorted and deduplicated the way the pattern key
        // spells them, so one refusal is one target set.
        learning.related_files = named_files(files)
            .split(',')
            .filter(|file| !file.is_empty())
            .map(str::to_string)
            .collect();
        learning.related_tasks.push(change_id.to_string());
        self.recorder.record_learning(learning.clone()).await?;
        self.matcher.update_recurrence(&mut learning).await?;
        self.note_change_skill_outcome(change_id, false).await?;

        self.maybe_trigger_evolution_from_learning(&learning).await;
        Ok(())
    }

    /// Feed one change's outcome to the skill effectiveness tracker.
    ///
    /// Shared by every way a change can end so the skill reading counts the
    /// same population whichever path recorded it.
    async fn note_change_skill_outcome(
        &self,
        change_id: &str,
        success: bool,
    ) -> cog_core::SFResult<()> {
        if let Some(ref tracker) = self.effectiveness_tracker {
            let outcome = cog_core::SkillOutcome {
                skill_id: "change_deployment".into(),
                task_signature: format!("change:{}", change_id),
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

    async fn record_change_outcome(
        &self,
        change_id: &str,
        success: bool,
        test_output: &str,
    ) -> cog_core::SFResult<()> {
        Self::record_change_outcome(self, change_id, success, test_output).await
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
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        };
        let event = cog_core::AssistantMessageEvent::TextEnd {
            content_index: 0,
            content: text.to_string(),
            timestamp: chrono::Utc::now(),
        };
        let _ = producer.try_push(event);
        producer.end(response);
        Ok(_stream)
    }

    /// A fake LLM that counts chat_stream invocations and returns a minimal
    /// valid JSON payload so EvolutionEngine methods do not panic.
    ///
    /// It also keeps the prompts when given somewhere to keep them. A test that
    /// only counts calls can say a generation happened but not that it was
    /// asked for the right thing, and "what the requirement named" is the whole
    /// question when a refusal decides what the next attempt is told to fix.
    struct CountingLlm {
        calls: Arc<tokio::sync::Mutex<u32>>,
        prompts: Option<Arc<tokio::sync::Mutex<Vec<String>>>>,
    }

    impl CountingLlm {
        async fn note(&self, messages: &[cog_core::Message]) {
            let mut calls = self.calls.lock().await;
            *calls += 1;
            drop(calls);
            if let Some(prompts) = &self.prompts {
                let rendered = messages
                    .iter()
                    .map(|message| message.content())
                    .collect::<Vec<_>>()
                    .join("\n");
                prompts.lock().await.push(rendered);
            }
        }
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for CountingLlm {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            self.note(_messages).await;
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
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.note(_messages).await;
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
            prompts: None,
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
            prompts: None,
        });
        engine.evolution = Some(Arc::new(EvolutionEngine::new(llm, registry, None)));
        // Lower thresholds.
        engine.hook_recurrence_threshold = 2;
        engine.change_recurrence_threshold = 5;
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
                upstream_failure: None,
                retry_after_secs: None,
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

    /// A refusal is a verdict, not a pattern, so the first one is worth acting
    /// on. Waiting for the count to mature is what left the diagnosis undelivered
    /// — the observed recurrence was one or two refusals, and the threshold is
    /// five — while the same intent was regenerated from scratch by a path that
    /// knew nothing about the refusal.
    #[tokio::test]
    async fn a_refusal_reaches_the_main_flow_on_the_first_one() {
        let (mut engine, submissions, goals) = engine_that_records_rework();
        engine.set_cooldown_secs(0);

        let files = vec![std::path::PathBuf::from("crates/x/src/lib.rs")];
        engine
            .record_change_refusal("c-1", cog_core::RejectionCause::TestsFailed, &files, "boom")
            .await
            .unwrap();
        assert_eq!(
            *submissions.lock().await,
            1,
            "the first refusal is the one that carries evidence nothing else has"
        );

        let asked = goals.lock().await.join("\n");
        assert!(
            asked.contains("crates/x/src/lib.rs"),
            "the requirement has to name the file the gate refused on, not just \
             the area it lives in; got:\n{asked}"
        );
    }

    /// The control for the test above: the threshold has not gone away, it just
    /// no longer applies to a verdict. A self-review pattern still has to be
    /// seen often enough to be a defect rather than a coincidence, and this is
    /// what stops "generate on the first record" from becoming "generate on
    /// every record".
    #[tokio::test]
    async fn a_self_review_pattern_still_waits_for_its_threshold() {
        let (mut engine, submissions, _) = engine_that_records_rework();
        engine.change_recurrence_threshold = 5;
        // The hook trigger shares this path and fires from three occurrences, so
        // it is pushed out of the way: this test is about the change threshold,
        // and a call it did not cause would read as one it did.
        engine.hook_recurrence_threshold = 99;
        engine.set_cooldown_secs(0);

        let mut learning = cog_core::Learning::new(
            cog_core::LearningCategory::Correction,
            cog_core::Priority::High,
            cog_core::Area::Backend,
            "a pattern",
            "seen once",
            "look at it",
            cog_core::LearningSource::SelfReview,
        );
        learning.pattern_key = Some("change:a-pattern:Backend".into());

        for seen in 1..5 {
            learning.recurrence_count = seen;
            engine
                .maybe_trigger_evolution_from_learning(&learning)
                .await;
            assert_eq!(
                *submissions.lock().await,
                0,
                "a self-review pattern seen {seen} times is still below its threshold of 5"
            );
        }

        learning.recurrence_count = 5;
        engine
            .maybe_trigger_evolution_from_learning(&learning)
            .await;
        assert_eq!(
            *submissions.lock().await,
            1,
            "at the threshold it hands the defect to the main flow, as it always did"
        );
    }

    /// Two refusals of different criteria are different defects, and the
    /// cooldown that keeps one defect from being regenerated every cycle must
    /// not swallow the other. The key that separates them is the criterion plus
    /// the files, so this is also the test that the key is what the cooldown
    /// reads.
    #[tokio::test]
    async fn one_criterion_s_cooldown_does_not_swallow_another() {
        let (mut engine, submissions, _) = engine_that_records_rework();
        engine.set_cooldown_secs(3600);

        let files = vec![std::path::PathBuf::from("crates/x/src/lib.rs")];
        engine
            .record_change_refusal("c-1", cog_core::RejectionCause::TestsFailed, &files, "boom")
            .await
            .unwrap();
        assert_eq!(*submissions.lock().await, 1, "the first refusal hands off");

        engine
            .record_change_refusal("c-2", cog_core::RejectionCause::TestsFailed, &files, "boom")
            .await
            .unwrap();
        assert_eq!(
            *submissions.lock().await,
            1,
            "the same defect again inside the cooldown is not handed off again"
        );

        engine
            .record_change_refusal(
                "c-3",
                cog_core::RejectionCause::MalformedDiff,
                &files,
                "boom",
            )
            .await
            .unwrap();
        assert_eq!(
            *submissions.lock().await,
            2,
            "a refusal by a different check is a different defect, and the first \
             one's cooldown does not silence it"
        );
    }

    /// The recurrence key is what merges two refusals into one count, so its
    /// rule is worth pinning on its own: the same criterion and the same files
    /// is one key however the diff happened to order them, and any change to
    /// either half makes it another.
    #[test]
    fn a_refusal_key_is_the_criterion_and_the_files() {
        let a = std::path::PathBuf::from("crates/a/src/lib.rs");
        let b = std::path::PathBuf::from("crates/b/src/lib.rs");
        let cause = cog_core::RejectionCause::TestsFailed;

        let one = std::slice::from_ref(&a);
        let other = std::slice::from_ref(&b);
        assert_eq!(
            refusal_pattern_key(cause, &[a.clone(), b.clone()]),
            refusal_pattern_key(cause, &[b.clone(), a.clone()]),
            "the order the diff listed the files in is not part of the defect"
        );
        assert_eq!(
            refusal_pattern_key(cause, &[a.clone(), a.clone()]),
            refusal_pattern_key(cause, one),
            "the same file named twice is the same defect"
        );
        assert_ne!(
            refusal_pattern_key(cause, one),
            refusal_pattern_key(cause, other),
            "the same criterion on another file is another defect"
        );
        assert_ne!(
            refusal_pattern_key(cause, one),
            refusal_pattern_key(cog_core::RejectionCause::MalformedDiff, one),
            "another criterion on the same file is another defect"
        );
        assert_ne!(
            refusal_pattern_key(cause, &[]),
            refusal_pattern_key(cog_core::RejectionCause::MalformedDiff, &[]),
            "an artifact nothing could read still says which check refused it"
        );
    }

    /// 一个只回答「收下了」的编排器替身。
    ///
    /// 这条支路对编排器做的唯一一件事是 `submit_goal_auto`；其余方法在此
    /// `unimplemented!()`——一个会在测试里悄悄做事的替身，会让断言读到别的东西上去。
    struct RecordingOrchestrator {
        submissions: Arc<tokio::sync::Mutex<u32>>,
        goals: Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl cog_core::OrchestratorControl for RecordingOrchestrator {
        async fn submit_goal(
            &self,
            _goal: &str,
            _tasks: Vec<cog_core::Task>,
        ) -> cog_core::SFResult<()> {
            unimplemented!("the hand-off submits through submit_goal_auto")
        }
        async fn submit_goal_auto(
            &self,
            goal: &str,
            _tasks: Vec<cog_core::Task>,
        ) -> cog_core::SFResult<Vec<String>> {
            *self.submissions.lock().await += 1;
            self.goals.lock().await.push(goal.to_string());
            Ok(Vec::new())
        }
        async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn add_task(&self, _task: cog_core::Task) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
            unimplemented!()
        }
        async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
            unimplemented!()
        }
        async fn get_ready_tasks(&self) -> Vec<cog_core::Task> {
            unimplemented!()
        }
        async fn get_all_tasks(&self) -> Vec<cog_core::Task> {
            unimplemented!()
        }
        async fn push_to_dlq(&self, _task_id: &str, _error: String) -> cog_core::SFResult<bool> {
            unimplemented!()
        }
        async fn retry_task(&self, _task_id: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn dlq_len(&self) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn start_task(&self, _task_id: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn complete_task(
            &self,
            _task_id: &str,
            _result: serde_json::Value,
        ) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn fail_task(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::UpstreamFailure>,
        ) -> cog_core::SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn fail_task_after(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::UpstreamFailure>,
            _retry_after_secs: Option<u64>,
        ) -> cog_core::SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn cancel_task(&self, _task_id: &str) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn get_task(&self, _task_id: &str) -> Option<cog_core::Task> {
            unimplemented!()
        }
        async fn schedule_task(&self, _task_id: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn get_dependents(&self, _task_id: &str) -> Option<Vec<cog_core::Task>> {
            unimplemented!()
        }
        async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<cog_core::Task>> {
            unimplemented!()
        }
        async fn get_graph(&self) -> (Vec<cog_core::Task>, Vec<(String, String)>) {
            unimplemented!()
        }
        async fn delete_task(&self, _task_id: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn all_completed(&self) -> bool {
            unimplemented!()
        }
        async fn replay_dlq(&self, _task_id: &str) -> cog_core::SFResult<bool> {
            unimplemented!()
        }
    }

    /// 一个把「回流到主流程」记下来的引擎：计数说回流发生过，目标文本说这次
    /// 被要求修的是什么——后者正是拒绝应当回答的问题。
    ///
    /// 属主门先武装、槽先填上：不武装的进程按设计不提交，而这里要读的正是
    /// 提交了几次。
    #[allow(clippy::type_complexity)]
    fn engine_that_records_rework() -> (
        ReflectionEngine,
        Arc<tokio::sync::Mutex<u32>>,
        Arc<tokio::sync::Mutex<Vec<String>>>,
    ) {
        let registry = Arc::new(tokio::sync::RwLock::new(SkillRegistry::new()));
        let engine = ReflectionEngine::new_in_memory(registry);
        let submissions = Arc::new(tokio::sync::Mutex::new(0u32));
        let goals = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        engine.rework.arm();
        engine
            .rework
            .set_orchestrator(Arc::new(RecordingOrchestrator {
                submissions: submissions.clone(),
                goals: goals.clone(),
            }));
        (engine, submissions, goals)
    }
}
