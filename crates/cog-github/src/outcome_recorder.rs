//! Outcome recorder — feeds PR/CI results back into the reflection engine.
//!
//! Implements the feedback leg of the design doc (§8): poll open PRs created
//! by the bot, and when a PR is merged or closed record the outcome via
//! `cog_core::ReflectionEngine::record_change_outcome` so future triage and
//! generation decisions improve.

use crate::error::Result;
use crate::provider::{CodePlatformProvider, PullRequestDetail};

/// Tracks PRs awaiting an outcome: PR number → change id.
#[derive(Debug, Default)]
pub struct OutcomeRecorder {
    pending: std::collections::HashMap<u64, String>,
}

impl OutcomeRecorder {
    /// Create an empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a PR created for `change_id` so its outcome gets recorded.
    pub fn track(&mut self, pr_number: u64, change_id: impl Into<String>) {
        self.pending.insert(pr_number, change_id.into());
    }

    /// Number of PRs currently tracked.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Poll all tracked PRs once. Terminal states (`merged` / `closed`) are
    /// recorded to the reflection engine and removed from tracking.
    pub async fn poll_once(
        &mut self,
        provider: &dyn CodePlatformProvider,
        reflection: &dyn cog_core::ReflectionEngine,
    ) -> Result<()> {
        let tracked: Vec<(u64, String)> = self
            .pending
            .iter()
            .map(|(pr, change)| (*pr, change.clone()))
            .collect();

        for (pr_number, change_id) in tracked {
            let detail = match provider.get_pull_request(pr_number).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(pr = pr_number, error = %e, "Failed to poll PR state");
                    continue;
                }
            };

            if let Some((success, output)) = Self::terminal_outcome(&detail) {
                if let Err(e) = reflection
                    .record_change_outcome(&change_id, success, &output)
                    .await
                {
                    tracing::warn!(
                        pr = pr_number,
                        change_id = %change_id,
                        error = %e,
                        "Failed to record change outcome"
                    );
                }
                self.pending.remove(&pr_number);
            }
        }
        Ok(())
    }

    /// Map a terminal PR state to `(success, test_output)`; `None` while the
    /// PR is still open.
    fn terminal_outcome(detail: &PullRequestDetail) -> Option<(bool, String)> {
        match detail.state.as_str() {
            "merged" => Some((
                true,
                format!(
                    "PR #{} merged (ci_passed={:?}, changed_lines={})",
                    detail.number, detail.ci_passed, detail.changed_lines
                ),
            )),
            "closed" => Some((false, format!("PR #{} closed without merge", detail.number))),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn detail(state: &str) -> PullRequestDetail {
        PullRequestDetail {
            number: 1,
            title: "t".into(),
            url: String::new(),
            state: state.into(),
            labels: vec![],
            changed_lines: 10,
            affected_files: vec![],
            ci_passed: Some(true),
            review_requested: false,
            head_sha: "x".into(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn terminal_outcome_mapping() {
        assert_eq!(OutcomeRecorder::terminal_outcome(&detail("open")), None);
        assert_eq!(
            OutcomeRecorder::terminal_outcome(&detail("merged")).map(|(s, _)| s),
            Some(true)
        );
        assert_eq!(
            OutcomeRecorder::terminal_outcome(&detail("closed")).map(|(s, _)| s),
            Some(false)
        );
    }

    #[test]
    fn track_and_count() {
        let mut rec = OutcomeRecorder::new();
        rec.track(1, "change-a");
        rec.track(2, "change-b");
        assert_eq!(rec.pending_count(), 2);
    }
}
