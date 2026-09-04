//! Issue triage — local, rule-based pre-filters and the no-LLM fallback.
//!
//! This crate is a sensor/actuator: it never calls an LLM directly. The
//! semantic actionability judgment ("can we act on this issue/PR yet?") is
//! dispatched to `cog-collaboration` through the orchestrator as a
//! `platform_intent_assess` task, where a single multimodal agent reads the
//! issue body plus follow-up replies (including screenshots/recordings) and
//! returns a `{decision, question, priority}` JSON verdict.
//!
//! What stays local here by design (no model call):
//! - deterministic label pre-filters (forbidden labels → Skip,
//!   human-required labels → EscalateHuman);
//! - a conservative text-count heuristic used only when no orchestrator is
//!   wired (keeps the loop autonomous without any LLM).

use crate::config::GitHubIntegrationConfig;

use crate::provider::PlatformIssue;

/// Decision produced by [`IssueTriage`].
#[derive(Debug, Clone, PartialEq)]
pub enum TriageDecision {
    /// Not worth fixing or not enough information.
    Skip {
        /// Why the issue was skipped.
        reason: String,
    },
    /// More information is needed from the reporter.
    AskForClarification {
        /// The question to post on the issue.
        question: String,
    },
    /// Suitable for an agent fix — generate a task.
    Fix {
        /// Priority hint: 1 (highest) – 5 (lowest).
        priority: u8,
        /// Short rationale recorded for observability.
        rationale: String,
    },
    /// High risk or sensitive — must be handled by a human.
    EscalateHuman {
        /// Why human handling is required.
        reason: String,
    },
}

/// Local, model-free triage: deterministic label pre-filters plus a text-count
/// heuristic fallback. Semantic (multimodal) actionability is judged upstream
/// in `cog-collaboration`; this type holds no LLM client.
#[derive(Default)]
pub struct IssueTriage;

impl IssueTriage {
    /// Create the local (rules + heuristic) triage. This is the only mode in
    /// this crate — semantic judgment runs in collaboration, not here.
    pub fn rules_only() -> Self {
        Self
    }

    /// Deterministic label pre-filters: forbidden labels → Skip,
    /// human-required labels → EscalateHuman. `None` when no label short-circuits.
    pub fn rules_decision(
        issue: &PlatformIssue,
        config: &GitHubIntegrationConfig,
    ) -> Option<TriageDecision> {
        Self::apply_rules(issue, config)
    }

    /// No-LLM fallback: decide from readable text length only (screenshots and
    /// links carry no text for this judge). Used when no orchestrator is wired
    /// so the loop stays autonomous; the semantic path runs elsewhere.
    pub fn heuristic_decision(issue: &PlatformIssue, reply_thread: &str) -> TriageDecision {
        Self::heuristic(issue, reply_thread)
    }

    /// Evaluate locally (rules then heuristic). This is the model-free fallback
    /// path. `reply_thread` is the rendered clarification conversation; pass an
    /// empty string when there is none (e.g. a fresh PR intent).
    pub async fn evaluate(
        &self,
        issue: &PlatformIssue,
        config: &GitHubIntegrationConfig,
        reply_thread: &str,
    ) -> TriageDecision {
        if let Some(decision) = Self::apply_rules(issue, config) {
            return decision;
        }
        Self::heuristic(issue, reply_thread)
    }

    /// Deterministic pre-filters from the integration config.
    fn apply_rules(
        issue: &PlatformIssue,
        config: &GitHubIntegrationConfig,
    ) -> Option<TriageDecision> {
        if let Some(label) = issue
            .labels
            .iter()
            .find(|l| config.forbidden_labels.contains(l))
        {
            return Some(TriageDecision::Skip {
                reason: format!("forbidden label: {}", label),
            });
        }
        if let Some(label) = issue
            .labels
            .iter()
            .find(|l| config.human_required_labels.contains(l))
        {
            return Some(TriageDecision::EscalateHuman {
                reason: format!("human-required label: {}", label),
            });
        }
        None
    }

    /// Conservative fallback when no LLM is configured: only fix issues that
    /// carry a clear, non-trivial description. The reporter's follow-up replies
    /// count too — a terse issue body can be fully clarified in the comments.
    /// Image embeds and bare URLs carry no readable text for a non-vision judge,
    /// so they are excluded; when readable information is still missing the
    /// fallback asks for the specifics to be pasted as text.
    fn heuristic(issue: &PlatformIssue, reply_thread: &str) -> TriageDecision {
        let combined = format!("{}\n{}", issue.body, reply_thread);
        if readable_len(&combined) < 40 {
            return TriageDecision::AskForClarification {
                question: "能否补充更完整的问题描述（期望行为、实际行为、复现步骤、影响版本）？\
                     如果信息在截图里，请把报错文字/复现步骤贴成文本，我当前无法读取图片内容。"
                    .into(),
            };
        }
        TriageDecision::Fix {
            priority: 3,
            rationale: "rule-based triage: description or replies look actionable".into(),
        }
    }
}

/// Count readable characters (letter/digit/CJK) after stripping markdown image
/// embeds (`![alt](url)`) and bare URL tokens. Used only by the rule-based
/// fallback to decide whether the issue plus replies carry enough to act on —
/// screenshots and links carry no text for a non-vision judge.
fn readable_len(text: &str) -> usize {
    let mut s = text.to_string();
    while let Some(start) = s.find("![") {
        let after = &s[start + 2..];
        let Some(rel_open) = after.find("](") else {
            break;
        };
        let open_abs = start + 2 + rel_open + 2;
        let Some(rel_close) = s[open_abs..].find(')') else {
            break;
        };
        let close_abs = open_abs + rel_close + 1;
        s.replace_range(start..close_abs, " ");
    }
    s.split_whitespace()
        .filter(|w| !w.starts_with("http://") && !w.starts_with("https://"))
        .flat_map(|w| w.chars())
        .filter(|c| c.is_alphanumeric())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn issue(labels: &[&str], body: &str) -> PlatformIssue {
        PlatformIssue {
            number: 1,
            title: "title".into(),
            body: body.into(),
            state: "open".into(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            author: "user".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn forbidden_label_skips() {
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let decision = triage
            .evaluate(&issue(&["wontfix"], "x".repeat(100).as_str()), &config, "")
            .await;
        assert!(matches!(decision, TriageDecision::Skip { .. }));
    }

    #[tokio::test]
    async fn human_required_label_escalates() {
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let decision = triage
            .evaluate(&issue(&["security"], "x".repeat(100).as_str()), &config, "")
            .await;
        assert!(matches!(decision, TriageDecision::EscalateHuman { .. }));
    }

    #[tokio::test]
    async fn short_body_asks_for_clarification() {
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let decision = triage.evaluate(&issue(&[], "too short"), &config, "").await;
        assert!(matches!(
            decision,
            TriageDecision::AskForClarification { .. }
        ));
    }

    #[tokio::test]
    async fn clear_issue_is_fixed() {
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let body = "The /health endpoint responds slowly. Expected: under 50ms. \
                    Actual: 500ms. Reproduce: curl localhost:8080/health repeatedly.";
        let decision = triage.evaluate(&issue(&[], body), &config, "").await;
        assert!(matches!(decision, TriageDecision::Fix { .. }));
    }

    #[tokio::test]
    async fn terse_body_with_substantive_replies_is_actionable() {
        // 原始正文极短（"看代码"），但报告者在评论里补全了复现——heuristic
        // 必须把回复一并计入，判 Fix 而不是对同一句短正文反复追问。
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let replies = "Reporter: 启动就崩溃，期望正常起来，实际 panic。\n\
                       Reporter: 复现：cargo run 后立即 panic，版本 v0.2.0，回溯在 init。";
        let decision = triage
            .evaluate(&issue(&[], "Read code"), &config, replies)
            .await;
        assert!(matches!(decision, TriageDecision::Fix { .. }));
    }

    #[tokio::test]
    async fn image_only_replies_still_ask_for_text() {
        // 只贴截图、没有可读文字时，非视觉 heuristic 无法据此行动，应追问把报错
        // 贴成文本（且受轮次上限约束，不会无限追问）。
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let replies = "Reporter: ![screenshot](https://github.com/u/a.png)";
        let decision = triage.evaluate(&issue(&[], "bug"), &config, replies).await;
        match decision {
            TriageDecision::AskForClarification { question } => {
                assert!(question.contains("文本") || question.contains("截图"));
            }
            other => panic!("expected clarify, got {:?}", other),
        }
    }
}
