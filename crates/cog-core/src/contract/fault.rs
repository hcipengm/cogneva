//! Fault classification contract: categorize a failure by root cause so the
//! runtime can pick a recovery strategy instead of blindly retrying.
//!
//! Classification is rule-based and deterministic (no LLM): the same error
//! text always yields the same category, keeping recovery decisions auditable.

use serde::{Deserialize, Serialize};

/// Root-cause category of a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultCategory {
    /// Network connectivity: timeouts, refused connections, DNS, TLS.
    Network,
    /// Defect in the code itself: panics, type/logic/compile errors.
    Code,
    /// Insufficient compute resources: OOM, disk full, throttling, rate limits.
    Resource,
    /// Bad or missing configuration / credentials / permissions.
    Config,
    /// An upstream third-party service failed (LLM provider, SaaS API).
    ExternalDependency,
    /// Not enough signal to classify.
    Unknown,
}

impl FaultCategory {
    /// All categories in deterministic evaluation order.
    pub const ALL: [FaultCategory; 6] = [
        FaultCategory::Network,
        FaultCategory::Code,
        FaultCategory::Resource,
        FaultCategory::Config,
        FaultCategory::ExternalDependency,
        FaultCategory::Unknown,
    ];

    /// Recovery strategy recommended for this category.
    pub fn default_strategy(&self) -> RecoveryStrategy {
        match self {
            // Transient by nature: retry with backoff.
            FaultCategory::Network => RecoveryStrategy::RetryWithBackoff,
            // Retries cannot fix a code defect: route into self-evolution.
            FaultCategory::Code => RecoveryStrategy::TriggerSelfEvolution,
            // Retrying under exhaustion makes things worse: relieve pressure.
            FaultCategory::Resource => RecoveryStrategy::ScaleOrRebalance,
            // A human or config hot-update must correct the configuration.
            FaultCategory::Config => RecoveryStrategy::FixConfiguration,
            // Out of our control: alert a human instead of burning retries.
            FaultCategory::ExternalDependency => RecoveryStrategy::AlertOperator,
            FaultCategory::Unknown => RecoveryStrategy::Investigate,
        }
    }
}

/// What the runtime should do about a classified failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStrategy {
    /// Retry the failed operation with exponential backoff.
    RetryWithBackoff,
    /// Scale resources up, adjust quotas, or rebalance tasks off the hot spot.
    ScaleOrRebalance,
    /// File a self-evolution task to fix the defect in code.
    TriggerSelfEvolution,
    /// Correct configuration (hot-update where possible), then retry.
    FixConfiguration,
    /// Page a human operator; automatic recovery is unsafe or impossible.
    AlertOperator,
    /// Collect more evidence before acting.
    Investigate,
}

/// Result of classifying one failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultClassification {
    pub category: FaultCategory,
    pub strategy: RecoveryStrategy,
    /// Identifier of the rule that produced this classification (audit trail).
    pub matched_rule: String,
    /// 0.0–1.0 heuristic confidence based on keyword hit count.
    pub confidence: f32,
}

/// Classifies failure text into a root-cause category with a recovery
/// strategy. Implementations must be deterministic and side-effect free.
pub trait FaultClassifier: Send + Sync {
    fn classify(&self, error_text: &str) -> FaultClassification;
}
