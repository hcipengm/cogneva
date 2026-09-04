//! Learning signal detection from conversations and agent events.
//! The [`LearningDetector`] trait identifies moments where the system can
//! learn — corrections, errors, knowledge gaps, and feature requests.

use async_trait::async_trait;
use cog_core::{AgentEvent, Message};
use tracing::{debug, trace};

use crate::types::FeatureRequest;
use cog_core::{ErrorEntry, Learning, LearningCategory, LearningSource, Priority};

/// Detects learning signals from messages and events.
#[async_trait]
pub trait LearningDetector: Send + Sync {
    /// Detect explicit or implicit user corrections in a message thread.
    fn detect_correction(&self, messages: &[Message]) -> Vec<Learning>;

    /// Detect a structured error entry from an agent event.
    fn detect_error(&self, event: &AgentEvent) -> Option<ErrorEntry>;

    /// Detect knowledge gaps expressed by the user or inferred from context.
    fn detect_knowledge_gap(&self, messages: &[Message]) -> Vec<Learning>;

    /// Detect feature requests in user messages.
    fn detect_feature_request(&self, messages: &[Message]) -> Vec<FeatureRequest>;

    /// Extract learnings from a `SelfReview` agent event.
    fn detect_from_self_review(&self, event: &AgentEvent) -> Vec<Learning>;

    /// Detect patterns from a full context window after a run completes.
    /// This catches semantic-level issues (e.g. agent going in circles)
    /// that keyword matching on single messages misses.
    fn detect_from_context(&self, messages: &[Message]) -> Vec<Learning>;

    /// Detect a learning from a single tool execution result.
    fn detect_from_tool_result(
        &self,
        tool_name: &str,
        result: &serde_json::Value,
        is_error: bool,
    ) -> Option<ErrorEntry>;
}

/// Default keyword-based detector with optional LLM-powered refinement.
/// Phase 1 uses fast keyword heuristics. Future phases can layer an
/// LLM-powered `LlmpoweredDetector` that wraps this for higher accuracy.
#[derive(Debug, Clone)]
pub struct DefaultLearningDetector {
    correction_keywords: Vec<String>,
    error_keywords: Vec<String>,
    knowledge_gap_keywords: Vec<String>,
    feature_request_keywords: Vec<String>,
}

impl Default for DefaultLearningDetector {
    fn default() -> Self {
        Self {
            correction_keywords: vec![
                "that's not right".into(),
                "actually".into(),
                "can you also".into(),
                "you missed".into(),
                "wrong".into(),
                "incorrect".into(),
                "no, ".into(),
                "should be".into(),
                "not quite".into(),
            ],
            error_keywords: vec![
                "error".into(),
                "failed".into(),
                "exception".into(),
                "panic".into(),
                "timeout".into(),
            ],
            knowledge_gap_keywords: vec![
                "i don't know how to".into(),
                "how do i".into(),
                "what is".into(),
                "can you explain".into(),
                "unclear".into(),
                "confused".into(),
            ],
            feature_request_keywords: vec![
                "it would be nice".into(),
                "feature request".into(),
                "can you add".into(),
                "would be helpful".into(),
                "support for".into(),
                "please add".into(),
            ],
        }
    }
}

impl DefaultLearningDetector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a detector with custom keyword sets.
    pub fn with_keywords(
        correction: Vec<String>,
        error: Vec<String>,
        knowledge_gap: Vec<String>,
        feature_request: Vec<String>,
    ) -> Self {
        Self {
            correction_keywords: correction,
            error_keywords: error,
            knowledge_gap_keywords: knowledge_gap,
            feature_request_keywords: feature_request,
        }
    }

    fn extract_text(messages: &[Message]) -> String {
        messages
            .iter()
            .filter_map(|m| match m {
                Message::User { content, .. } | Message::Assistant { content, .. } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    if text.is_empty() {
                        None
                    } else {
                        Some(text)
                    }
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    }

    fn has_keyword(text: &str, keywords: &[String]) -> Option<String> {
        keywords
            .iter()
            .find(|k| text.contains(&k.to_lowercase()))
            .cloned()
    }
}

#[async_trait]
impl LearningDetector for DefaultLearningDetector {
    fn detect_correction(&self, messages: &[Message]) -> Vec<Learning> {
        let text = Self::extract_text(messages);
        let mut learnings = Vec::new();

        if let Some(keyword) = Self::has_keyword(&text, &self.correction_keywords) {
            trace!("detected correction signal: {}", keyword);
            learnings.push(Learning::new(
                LearningCategory::Correction,
                Priority::High,
                cog_core::Area::Backend,
                format!("User correction detected via keyword: {}", keyword),
                text,
                "Review the corrected output and update best practices or system prompts accordingly.",
                LearningSource::UserFeedback,
            ));
        }

        debug!("detected {} corrections", learnings.len());
        learnings
    }

    fn detect_error(&self, event: &AgentEvent) -> Option<ErrorEntry> {
        match event {
            AgentEvent::ToolExecutionEnd {
                result,
                is_error: true,
                agent_id,
                tool_call_id,
                ..
            } => {
                let summary = format!(
                    "Tool call '{}' failed on agent '{}'",
                    tool_call_id, agent_id
                );
                Some(ErrorEntry::new(
                    Priority::High,
                    summary,
                    serde_json::to_string_pretty(result).unwrap_or_default(),
                    format!("Agent: {}, ToolCall: {}", agent_id, tool_call_id),
                    "Investigate tool arguments and tool implementation for the failure cause.",
                ))
            }
            AgentEvent::TaskStatusChange {
                status,
                task_id,
                agent_id,
                ..
            } if status.to_lowercase() == "failed" || status.to_lowercase() == "error" => {
                let summary = format!("Task '{}' failed", task_id);
                Some(ErrorEntry::new(
                    Priority::High,
                    summary,
                    format!("Task status changed to: {}", status),
                    format!("Task: {}, Agent: {:?}", task_id, agent_id),
                    "Check task logs and retry with modified parameters or squad composition.",
                ))
            }
            _ => None,
        }
    }

    fn detect_knowledge_gap(&self, messages: &[Message]) -> Vec<Learning> {
        let text = Self::extract_text(messages);
        let mut learnings = Vec::new();

        if let Some(keyword) = Self::has_keyword(&text, &self.knowledge_gap_keywords) {
            trace!("detected knowledge gap signal: {}", keyword);
            learnings.push(Learning::new(
                LearningCategory::KnowledgeGap,
                Priority::Medium,
                cog_core::Area::Docs,
                format!("Knowledge gap detected via keyword: {}", keyword),
                text,
                "Document the missing knowledge or add it to the skill registry / system prompts.",
                LearningSource::Conversation,
            ));
        }

        learnings
    }

    fn detect_feature_request(&self, messages: &[Message]) -> Vec<FeatureRequest> {
        let text = Self::extract_text(messages);
        let mut requests = Vec::new();

        if let Some(keyword) = Self::has_keyword(&text, &self.feature_request_keywords) {
            trace!("detected feature request signal: {}", keyword);
            requests.push(FeatureRequest::new(
                format!("Feature request via keyword: {}", keyword),
                text,
                crate::types::Complexity::Medium,
                "Evaluate feasibility and prioritize in the product backlog.",
                cog_core::Area::Backend,
            ));
        }

        requests
    }

    fn detect_from_self_review(&self, event: &AgentEvent) -> Vec<Learning> {
        let AgentEvent::SelfReview {
            agent_id,
            status,
            score,
            critique,
            suggestions,
            summary,
            ..
        } = event
        else {
            return Vec::new();
        };

        let mut learnings = Vec::new();

        if status == "NEED_REVISION" {
            if let Some(critique_text) = critique {
                learnings.push(Learning::new(
                    LearningCategory::Insight,
                    Priority::Medium,
                    cog_core::Area::Tests,
                    format!("Self-review critique for agent '{}'", agent_id),
                    critique_text.clone(),
                    suggestions
                        .as_ref()
                        .map(|s| s.join("; "))
                        .unwrap_or_default(),
                    LearningSource::SelfReview,
                ));
            }
        } else if let Some(summary_text) = summary {
            // Low-score passes may still indicate a pattern worth tracking.
            if *score < 0.9 {
                learnings.push(Learning::new(
                    LearningCategory::BestPractice,
                    Priority::Low,
                    cog_core::Area::Tests,
                    format!("Self-review marginal pass for agent '{}'", agent_id),
                    summary_text.clone(),
                    "Monitor for recurring marginal scores that could indicate systematic issues.",
                    LearningSource::SelfReview,
                ));
            }
        }

        learnings
    }

    fn detect_from_context(&self, messages: &[Message]) -> Vec<Learning> {
        let text = Self::extract_text(messages);
        let mut learnings = Vec::new();

        // Detect circular reasoning: repeated similar assistant messages
        let assistant_texts: Vec<String> = messages
            .iter()
            .filter_map(|m| match m {
                Message::Assistant { content, .. } => {
                    let t: String = content.iter().filter_map(|b| b.as_text()).collect();
                    if t.len() > 20 {
                        Some(t.to_lowercase())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

        if assistant_texts.len() >= 3 {
            // Simple heuristic: if the last assistant message is highly similar to an earlier one,
            // the agent may be stuck in a loop.
            if let Some(last) = assistant_texts.last() {
                for earlier in &assistant_texts[..assistant_texts.len() - 1] {
                    if last == earlier
                        || (last.len() > 50 && earlier.len() > 50 && last[..50] == earlier[..50])
                    {
                        learnings.push(Learning::new(
                        LearningCategory::Insight,
                        Priority::High,
                        cog_core::Area::Backend,
                        "Agent appears to be repeating itself",
                        format!("Last assistant message closely resembles an earlier turn. Full context: {}", text),
                        "Add working-memory reminders or steering to break the loop.",
                        LearningSource::SelfReview,
                    ));
                        break;
                    }
                }
            }
        }

        // Detect excessive tool retries from the message stream
        let tool_result_count = messages
            .iter()
            .filter(|m| matches!(m, Message::ToolResult { .. }))
            .count();
        if tool_result_count >= 5 {
            learnings.push(Learning::new(
                LearningCategory::Correction,
                Priority::High,
                cog_core::Area::Backend,
                "Excessive tool retries detected",
                format!("{} tool results in context window. Context: {}", tool_result_count, text),
                "Review tool usage pattern; consider adding a tool-use best practice or improving tool descriptions.",
                LearningSource::SelfReview,
            ));
        }

        debug!("detected {} context-level learnings", learnings.len());
        learnings
    }

    fn detect_from_tool_result(
        &self,
        tool_name: &str,
        result: &serde_json::Value,
        is_error: bool,
    ) -> Option<ErrorEntry> {
        if !is_error {
            return None;
        }

        let error_text = serde_json::to_string_pretty(result).unwrap_or_default();

        // Check if this looks like a recurring tool error
        if self
            .error_keywords
            .iter()
            .any(|k| error_text.to_lowercase().contains(k))
        {
            let summary = format!("Tool '{}' failed with error pattern", tool_name);
            Some(ErrorEntry::new(
                Priority::High,
                summary,
                error_text,
                format!("Tool: {}", tool_name),
                "Investigate tool arguments and implementation.",
            ))
        } else {
            None
        }
    }
}
