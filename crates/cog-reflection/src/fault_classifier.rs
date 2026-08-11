//! Rule-based fault classifier: deterministic keyword rules map failure text
//! to a root-cause category, so the supervisor/orchestrator can pick a
//! recovery strategy that matches the cause instead of blindly retrying.
//!
//! Rules are evaluated per category; the category with the most keyword hits
//! wins (ties break by [`FaultCategory::ALL`] order). Zero hits → `Unknown`.
//! Classification never calls an LLM — it must stay cheap, deterministic,
//! and available even when every upstream provider is down.

use cog_core::{FaultCategory, FaultClassification, FaultClassifier};

/// Keyword rules for one category. Matching is case-insensitive substring.
struct CategoryRule {
    name: &'static str,
    category: FaultCategory,
    keywords: &'static [&'static str],
}

const RULES: &[CategoryRule] = &[
    CategoryRule {
        name: "network",
        category: FaultCategory::Network,
        keywords: &[
            "connection refused",
            "connection reset",
            "timed out",
            "timeout",
            "dns",
            "unreachable",
            "no route to host",
            "tls handshake",
            "certificate",
            "socket",
            "econnrefused",
            "econnreset",
            "network is down",
            "broken pipe",
        ],
    },
    CategoryRule {
        name: "code",
        category: FaultCategory::Code,
        keywords: &[
            "panic",
            "unwrap",
            "index out of bounds",
            "null pointer",
            "segmentation fault",
            "assertion failed",
            "type error",
            "compile error",
            "compilation failed",
            "syntax error",
            "undefined",
            "deadlock",
            "borrow checker",
            "mismatched types",
        ],
    },
    CategoryRule {
        name: "resource",
        category: FaultCategory::Resource,
        keywords: &[
            "out of memory",
            "oomkilled",
            "oom",
            "no space left",
            "disk full",
            "too many open files",
            "cpu throttl",
            "rate limit",
            "resource exhausted",
            "quota exceeded",
            "memory limit",
            "insufficient",
        ],
    },
    CategoryRule {
        name: "config",
        category: FaultCategory::Config,
        keywords: &[
            "missing config",
            "invalid config",
            "unknown field",
            "failed to parse",
            "parse error",
            "not set",
            "not configured",
            "misconfigur",
            "permission denied",
            "unauthorized",
            "forbidden",
            "invalid token",
            "bad credentials",
        ],
    },
    CategoryRule {
        name: "external_dependency",
        category: FaultCategory::ExternalDependency,
        keywords: &[
            "bad gateway",
            "service unavailable",
            "gateway timeout",
            "upstream",
            "provider error",
            "api error",
            "http 502",
            "http 503",
            "http 504",
            "third-party",
            "webhook delivery failed",
        ],
    },
];

/// Deterministic keyword-rule classifier.
#[derive(Debug, Default, Clone, Copy)]
pub struct RuleBasedFaultClassifier;

impl RuleBasedFaultClassifier {
    pub fn new() -> Self {
        Self
    }
}

impl FaultClassifier for RuleBasedFaultClassifier {
    fn classify(&self, error_text: &str) -> FaultClassification {
        let text = error_text.to_lowercase();
        let mut best: Option<(&CategoryRule, usize)> = None;
        for rule in RULES {
            let hits = rule
                .keywords
                .iter()
                .filter(|kw| text.contains(**kw))
                .count();
            if hits == 0 {
                continue;
            }
            let dominated = best
                .as_ref()
                .is_some_and(|(_, best_hits)| *best_hits >= hits);
            if !dominated {
                best = Some((rule, hits));
            }
        }
        match best {
            Some((rule, hits)) => {
                // 1 hit is a weak signal; 3+ hits saturate confidence at 1.0.
                let confidence = (hits as f32 / 3.0).min(1.0);
                FaultClassification {
                    category: rule.category,
                    strategy: rule.category.default_strategy(),
                    matched_rule: rule.name.to_string(),
                    confidence,
                }
            }
            None => FaultClassification {
                category: FaultCategory::Unknown,
                strategy: FaultCategory::Unknown.default_strategy(),
                matched_rule: "none".to_string(),
                confidence: 0.0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::RecoveryStrategy;

    fn classify(text: &str) -> FaultClassification {
        RuleBasedFaultClassifier::new().classify(text)
    }

    #[test]
    fn classifies_network() {
        let c = classify("request failed: connection refused (os error 111)");
        assert_eq!(c.category, FaultCategory::Network);
        assert_eq!(c.strategy, RecoveryStrategy::RetryWithBackoff);
        assert_eq!(c.matched_rule, "network");
    }

    #[test]
    fn classifies_code() {
        let c = classify("thread 'main' panicked at index out of bounds: len is 3 but index is 5");
        assert_eq!(c.category, FaultCategory::Code);
        assert_eq!(c.strategy, RecoveryStrategy::TriggerSelfEvolution);
    }

    #[test]
    fn classifies_resource() {
        let c = classify("container OOMKilled: out of memory, usage exceeded memory limit");
        assert_eq!(c.category, FaultCategory::Resource);
        assert_eq!(c.strategy, RecoveryStrategy::ScaleOrRebalance);
        assert!(c.confidence >= 0.99);
    }

    #[test]
    fn classifies_config() {
        let c = classify("bootstrap failed: invalid config, unknown field `provdier` at line 12");
        assert_eq!(c.category, FaultCategory::Config);
        assert_eq!(c.strategy, RecoveryStrategy::FixConfiguration);
    }

    #[test]
    fn classifies_external_dependency() {
        let c = classify("LLM call failed: upstream provider error, http 503 service unavailable");
        assert_eq!(c.category, FaultCategory::ExternalDependency);
        assert_eq!(c.strategy, RecoveryStrategy::AlertOperator);
    }

    #[test]
    fn unclassified_text_maps_to_unknown_investigate() {
        let c = classify("something odd happened");
        assert_eq!(c.category, FaultCategory::Unknown);
        assert_eq!(c.strategy, RecoveryStrategy::Investigate);
        assert_eq!(c.confidence, 0.0);
    }

    #[test]
    fn case_insensitive_and_strongest_signal_wins() {
        // One network keyword vs two resource keywords → resource wins.
        let c = classify("TIMEOUT while writing: No Space Left on device, disk full");
        assert_eq!(c.category, FaultCategory::Resource);
    }

    #[test]
    fn deterministic_across_calls() {
        let text = "connection reset by peer";
        assert_eq!(classify(text), classify(text));
    }
}
