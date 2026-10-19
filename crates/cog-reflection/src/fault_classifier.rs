//! Rule-based fault classifier: deterministic keyword rules map failure text
//! to a root-cause category, so the supervisor/orchestrator can pick a
//! recovery strategy that matches the cause instead of blindly retrying.
//!
//! Rules are evaluated per category; the category with the most keyword hits
//! wins (ties break by [`FaultCategory::ALL`] order). Zero hits → `Unknown`.
//! Classification never calls an LLM — it must stay cheap, deterministic,
//! and available even when every upstream provider is down.
//!
//! principle: feedback_a_wrong_invocation_must_not_be_reported_as_a_broken_tool
//!
//! 「工具自己坏了」与「你这条命令写错了」必须分开报：健康工具对一条写错的
//! 调用给出的用法报错（未知子命令、无法识别的参数）是调用方的故障，归为
//! `Config` / `FixConfiguration`；绝不能报成工具自己损坏（`Code` /
//! `ExternalDependency`）——后两类才触发自我演化或告警。

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
            "unrecognized",
            "no such command",
            "unexpected argument",
            "usage:",
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
    fn a_wrong_invocation_is_the_callers_fault_not_a_broken_tool() {
        // 健康工具对一条写错的命令的回答：未知子命令、无法识别的参数、用法提示。
        // 这是调用方写错了，必须报成 Config / FixConfiguration。
        for transcript in [
            "error: unrecognized subcommand 'frobnicate'\n\nUsage: cogctl [OPTIONS] <COMMAND>\n\nFor more information, try '--help'.",
            "error: no such command: `flurb`\n\nUsage: cogctl <COMMAND>",
            "error: unexpected argument '--retires' found\n\nUsage: cogctl run [OPTIONS]",
        ] {
            let c = classify(transcript);
            assert_eq!(c.category, FaultCategory::Config, "transcript: {transcript}");
            assert_eq!(c.strategy, RecoveryStrategy::FixConfiguration);
            assert_eq!(c.matched_rule, "config");
            // 绝不能报成工具自己坏了：那两类才会触发自我演化或告警。
            assert_ne!(c.category, FaultCategory::Code);
            assert_ne!(c.category, FaultCategory::ExternalDependency);
            assert_ne!(c.strategy, RecoveryStrategy::TriggerSelfEvolution);
            assert_ne!(c.strategy, RecoveryStrategy::AlertOperator);
        }

        // 配对的另一读：工具真的坏了时仍然报成工具自己的故障，两种转录在
        // 这一个修订上各落各的类，谁也不能被报成另一个。
        let broken = classify("thread 'worker' panicked at index out of bounds");
        assert_eq!(broken.category, FaultCategory::Code);
        assert_eq!(broken.strategy, RecoveryStrategy::TriggerSelfEvolution);

        let dependency =
            classify("LLM call failed: upstream provider error, http 503 service unavailable");
        assert_eq!(dependency.category, FaultCategory::ExternalDependency);
        assert_eq!(dependency.strategy, RecoveryStrategy::AlertOperator);
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
