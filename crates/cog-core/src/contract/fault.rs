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

#[cfg(test)]
mod classification_stays_an_enum_until_the_boundary {
    //! principle: feedback_category_not_via_strings_across_processes
    //!
    //! 分类结果在进程内必须是 enum 不是字符串。The verdict stays a
    //! [`FaultCategory`] for as long as it is inside the process; text
    //! appears only at the boundary where one process hands it to another.
    //!
    //! This gate holds both ends. In process the value is the enum, so no
    //! decision is made by comparing strings. Across the boundary the form
    //! is closed and total: every variant round-trips back to itself, two
    //! variants never share one spelling, and a spelling that names no
    //! variant is refused rather than read as a default -- a typo must not
    //! become "no signal".

    use super::*;

    /// The wire spelling of every variant, as serde writes it.
    fn wire_forms() -> Vec<String> {
        let mut forms = Vec::new();
        for category in FaultCategory::ALL {
            let form = serde_json::to_string(&category).expect("serializes");
            forms.push(form);
        }
        forms
    }

    #[test]
    fn the_verdict_in_process_is_the_enum_and_not_its_text() {
        // 进程内的字段类型就是 enum：这里取出来的是 `FaultCategory`，不是
        // 它的一段文本，所以进程内的比较是变体之间的比较。
        fn category_of(classification: &FaultClassification) -> FaultCategory {
            classification.category
        }

        let classification = FaultClassification {
            category: FaultCategory::Resource,
            strategy: RecoveryStrategy::ScaleOrRebalance,
            matched_rule: "resource".to_string(),
            confidence: 1.0,
        };
        assert_eq!(category_of(&classification), FaultCategory::Resource);
        assert_eq!(
            category_of(&classification).default_strategy(),
            RecoveryStrategy::ScaleOrRebalance
        );
    }

    #[test]
    fn every_variant_crosses_the_boundary_by_parsing_back_to_itself() {
        let forms = wire_forms();
        // 闭集的规模就是变体表的规模：一个只能由字符串到达的变体在这里
        // 与 `FaultCategory::ALL` 对不上。
        assert_eq!(forms.len(), FaultCategory::ALL.len());
        let mut seen = std::collections::BTreeSet::new();
        for (category, form) in FaultCategory::ALL.into_iter().zip(&forms) {
            let parsed = serde_json::from_str::<FaultCategory>(form).expect("parses back");
            assert_eq!(parsed, category);
            assert!(seen.insert(form.as_str()), "two variants share {form}");
        }
    }

    #[test]
    fn a_form_that_names_no_variant_is_refused_rather_than_defaulted() {
        // 没有静默兜底：读不回来的文本不能落成 `Unknown`，否则跨进程的一次
        // 拼写错误就变成了一次「没有信号」的分类。
        for text in ["\"nonsense\"", "\"\"", "null", "\"Network\""] {
            assert!(
                serde_json::from_str::<FaultCategory>(text).is_err(),
                "{text} names no FaultCategory and must be refused"
            );
        }
        // 未知不是缺席：`Unknown` 只能由显式的 `"unknown"` 得到。
        let unknown = serde_json::from_str::<FaultCategory>("\"unknown\"");
        assert_eq!(unknown.ok(), Some(FaultCategory::Unknown));
    }
}
