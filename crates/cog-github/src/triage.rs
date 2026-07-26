//! Issue triage — autonomous evaluation of whether an issue should be fixed
//! by the agent, needs clarification, or must go to a human.
//!
//! Rule-based pre-filters always run (forbidden labels → Skip,
//! human-required labels → EscalateHuman).  When an LLM client is available
//! the remaining issues are evaluated semantically; without an LLM the
//! fallback is a conservative heuristic so the loop stays autonomous.

use std::sync::Arc;

use cog_core::{ChatOptions, GitHubIntegrationConfig, LlmClient, Message};

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
    pub async fn evaluate(
        &self,
        issue: &PlatformIssue,
        config: &GitHubIntegrationConfig,
    ) -> TriageDecision {
        if let Some(decision) = Self::apply_rules(issue, config) {
            return decision;
        }

        match &self.llm {
            Some(llm) => match self.evaluate_with_llm(llm.as_ref(), issue).await {
                Ok(decision) => decision,
                Err(e) => {
                    tracing::warn!(
                        issue = issue.number,
                        error = %e,
                        "LLM triage failed; falling back to heuristic"
                    );
                    Self::heuristic(issue)
                }
            },
            None => Self::heuristic(issue),
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
    /// carry a clear, non-trivial description.
    fn heuristic(issue: &PlatformIssue) -> TriageDecision {
        let body_len = issue.body.trim().len();
        if body_len < 40 {
            return TriageDecision::AskForClarification {
                question: "能否补充更完整的问题描述（期望行为、实际行为、复现步骤、影响版本）？"
                    .into(),
            };
        }
        TriageDecision::Fix {
            priority: 3,
            rationale: "rule-based triage: description looks actionable".into(),
        }
    }

    async fn evaluate_with_llm(
        &self,
        llm: &dyn LlmClient,
        issue: &PlatformIssue,
    ) -> cog_core::SFResult<TriageDecision> {
        let prompt = format!(
            "You are the triage actor of an autonomous code-evolution system.\n\
             Evaluate this GitHub issue and respond with ONLY a JSON object, no markdown fences:\n\
             {{\"decision\": \"skip\"|\"clarify\"|\"fix\"|\"escalate\", \
             \"reason\": \"short rationale\", \
             \"question\": \"clarification question when decision=clarify\", \
             \"priority\": 1-5}}\n\n\
             Criteria: fix only when the issue is clearly actionable, valuable, and low-risk.\n\
             Escalate when it touches security, deployment, credentials, or data loss.\n\
             Ask for clarification when the description lacks reproduction steps or expected behavior.\n\n\
             Issue #{}: {}\nLabels: {}\n\n{}",
            issue.number,
            issue.title,
            issue.labels.join(", "),
            issue.body
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
            .evaluate(&issue(&["wontfix"], "x".repeat(100).as_str()), &config)
            .await;
        assert!(matches!(decision, TriageDecision::Skip { .. }));
    }

    #[tokio::test]
    async fn human_required_label_escalates() {
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let decision = triage
            .evaluate(&issue(&["security"], "x".repeat(100).as_str()), &config)
            .await;
        assert!(matches!(decision, TriageDecision::EscalateHuman { .. }));
    }

    #[tokio::test]
    async fn short_body_asks_for_clarification() {
        let triage = IssueTriage::rules_only();
        let config = GitHubIntegrationConfig::default();
        let decision = triage.evaluate(&issue(&[], "too short"), &config).await;
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
        let decision = triage.evaluate(&issue(&[], body), &config).await;
        assert!(matches!(decision, TriageDecision::Fix { .. }));
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
