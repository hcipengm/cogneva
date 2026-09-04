//! Issue triage — autonomous evaluation of whether an issue should be fixed
//! by the agent, needs clarification, or must go to a human.
//!
//! Rule-based pre-filters always run (forbidden labels → Skip,
//! human-required labels → EscalateHuman).  When an LLM client is available
//! the remaining issues are evaluated semantically; without an LLM the
//! fallback is a conservative heuristic so the loop stays autonomous.

use std::sync::Arc;

use crate::config::GitHubIntegrationConfig;
use cog_core::{ChatOptions, LlmClient, Message};

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

/// Evaluates issues and renders triage decisions.
pub struct IssueTriage {
    llm: Option<Arc<dyn LlmClient>>,
}

impl IssueTriage {
    /// Create a triage evaluator without an LLM (rules + heuristics only).
    pub fn rules_only() -> Self {
        Self { llm: None }
    }

    /// Create a triage evaluator backed by an LLM for semantic judgment.
    pub fn with_llm(llm: Arc<dyn LlmClient>) -> Self {
        Self { llm: Some(llm) }
    }

    /// Evaluate an issue and produce a triage decision.
    ///
    /// `reply_thread` is the rendered clarification conversation (bot questions
    /// and the reporter's follow-up replies, including image/link attachments).
    /// Actionability must be judged from the whole thread, not just the often
    /// terse issue body — a screenshot or follow-up comment can carry the answer.
    /// Pass an empty string when there is no conversation (e.g. a fresh PR intent).
    pub async fn evaluate(
        &self,
        issue: &PlatformIssue,
        config: &GitHubIntegrationConfig,
        reply_thread: &str,
    ) -> TriageDecision {
        if let Some(decision) = Self::apply_rules(issue, config) {
            return decision;
        }

        match &self.llm {
            Some(llm) => {
                match self
                    .evaluate_with_llm(llm.as_ref(), issue, reply_thread)
                    .await
                {
                    Ok(decision) => decision,
                    Err(e) => {
                        tracing::warn!(
                            issue = issue.number,
                            error = %e,
                            "LLM triage failed; falling back to heuristic"
                        );
                        Self::heuristic(issue, reply_thread)
                    }
                }
            }
            None => Self::heuristic(issue, reply_thread),
        }
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

    async fn evaluate_with_llm(
        &self,
        llm: &dyn LlmClient,
        issue: &PlatformIssue,
        reply_thread: &str,
    ) -> cog_core::SFResult<TriageDecision> {
        // The follow-up thread is appended verbatim when present: the reporter's
        // comments — including screenshots/links — are part of the actionable
        // picture. When a screenshot cannot be read and the issue is still not
        // actionable, instruct the bot to ask for the key text rather than to
        // treat the image as an answer or ignore it.
        let thread_section = if reply_thread.trim().is_empty() {
            String::new()
        } else {
            format!(
                "\n\nReporter follow-up replies (newest last; image/link attachments \
                 may contain the answer — if you cannot read an attached screenshot and \
                 the issue is not yet actionable, ask the reporter to paste the error text \
                 and reproduction steps):\n{}",
                reply_thread
            )
        };
        let prompt = format!(
            "You are the triage actor of an autonomous code-evolution system.\n\
             Evaluate this GitHub issue together with the reporter's follow-up replies, and \
             respond with ONLY a JSON object, no markdown fences:\n\
             {{\"decision\": \"skip\"|\"clarify\"|\"fix\"|\"escalate\", \
             \"reason\": \"short rationale\", \
             \"question\": \"clarification question when decision=clarify\", \
             \"priority\": 1-5}}\n\n\
             Criteria: decide `fix` only when the combined information (issue body PLUS replies) \
             is clearly actionable, valuable, and low-risk — a terse body can be fully clarified \
             by follow-up replies or an attached screenshot.\n\
             Escalate when it touches security, deployment, credentials, or data loss.\n\
             Decide `clarify` when the combined information still lacks reproduction steps or \
             expected behavior; phrase a SPECIFIC question about what is still missing.\n\n\
             Issue #{}: {}\nLabels: {}\n\n{}{}",
            issue.number,
            issue.title,
            issue.labels.join(", "),
            issue.body,
            thread_section
        );

        let messages = vec![
            Message::system("Respond with a single JSON object and nothing else."),
            Message::user(prompt),
        ];
        let response = llm.chat(&messages, &ChatOptions::default()).await?;
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect();

        Self::parse_llm_decision(&text)
    }

    /// Parse the LLM JSON verdict. Tolerates markdown fences.
    fn parse_llm_decision(text: &str) -> cog_core::SFResult<TriageDecision> {
        let trimmed = text.trim();
        let stripped = if trimmed.starts_with("```") {
            trimmed
                .trim_start_matches("```json")
                .trim_start_matches("```")
                .trim_end_matches("```")
                .trim()
        } else {
            trimmed
        };

        let value: serde_json::Value = serde_json::from_str(stripped).map_err(|e| {
            cog_core::SFError::Validation(format!("triage LLM output is not JSON: {}", e))
        })?;

        let decision = value
            .get("decision")
            .and_then(|v| v.as_str())
            .unwrap_or("skip");
        let reason = value
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        Ok(match decision {
            "fix" => TriageDecision::Fix {
                priority: value
                    .get("priority")
                    .and_then(|v| v.as_u64())
                    .map(|p| p.clamp(1, 5) as u8)
                    .unwrap_or(3),
                rationale: reason,
            },
            "clarify" => TriageDecision::AskForClarification {
                question: value
                    .get("question")
                    .and_then(|v| v.as_str())
                    .filter(|q| !q.is_empty())
                    .unwrap_or("能否补充期望行为、实际行为与复现步骤？")
                    .to_string(),
            },
            "escalate" => TriageDecision::EscalateHuman { reason },
            _ => TriageDecision::Skip { reason },
        })
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

    #[test]
    fn parses_fenced_llm_json() {
        let text = "```json\n{\"decision\": \"fix\", \"reason\": \"clear\", \"priority\": 2}\n```";
        let decision = IssueTriage::parse_llm_decision(text).unwrap();
        assert_eq!(
            decision,
            TriageDecision::Fix {
                priority: 2,
                rationale: "clear".into()
            }
        );
    }
}
