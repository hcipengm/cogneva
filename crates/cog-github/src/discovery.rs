//! Issue discovery — actively scans the configured repository for issues
//! that should enter the self-evolution pipeline.
//!
//! Filtering follows `docs/2026-06-28_github_issue_to_pr_integration_design.md`:
//! only issues whose state is in `allowed_issue_states`, that carry none of
//! the `forbidden_labels`, and that were updated since the last poll are
//! returned, capped at `max_issues_per_scan`.

use chrono::{DateTime, Utc};

use crate::config::GitHubIntegrationConfig;

use crate::error::Result;
use crate::provider::{CodePlatformProvider, PlatformIssue};

/// Active scanner for platform issues.
///
/// The scanner is intentionally stateless apart from `last_polled_at`; the
/// discovery loop owns persistence of that watermark.
pub struct IssueDiscovery {
    last_polled_at: Option<DateTime<Utc>>,
}

impl IssueDiscovery {
    /// Create a new discovery scanner with no watermark (first scan returns
    /// every matching issue).
    pub fn new() -> Self {
        Self {
            last_polled_at: None,
        }
    }

    /// Create a scanner resuming from a persisted watermark.
    pub fn with_watermark(last_polled_at: DateTime<Utc>) -> Self {
        Self {
            last_polled_at: Some(last_polled_at),
        }
    }

    /// The current watermark, if any.
    pub fn last_polled_at(&self) -> Option<DateTime<Utc>> {
        self.last_polled_at
    }

    /// Pull candidate issues from the provider and apply the configured
    /// filters. Advances the watermark to the newest `updated_at` seen.
    pub async fn scan(
        &mut self,
        provider: &dyn CodePlatformProvider,
        config: &GitHubIntegrationConfig,
    ) -> Result<Vec<PlatformIssue>> {
        let issues = provider.list_open_issues().await?;
        let filtered = Self::filter(issues, config, self.last_polled_at);

        if let Some(newest) = filtered.iter().map(|i| i.updated_at).max() {
            self.last_polled_at = Some(newest);
        }

        Ok(filtered)
    }

    /// Pure filter: allowed states, no forbidden labels, updated since the
    /// watermark, capped at `max_issues_per_scan` (newest first).
    pub fn filter(
        issues: Vec<PlatformIssue>,
        config: &GitHubIntegrationConfig,
        since: Option<DateTime<Utc>>,
    ) -> Vec<PlatformIssue> {
        let mut out: Vec<PlatformIssue> = issues
            .into_iter()
            .filter(|i| {
                config
                    .allowed_issue_states
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(&i.state))
            })
            .filter(|i| !i.labels.iter().any(|l| config.forbidden_labels.contains(l)))
            .filter(|i| since.map(|s| i.updated_at > s).unwrap_or(true))
            .collect();

        out.sort_by_key(|i| std::cmp::Reverse(i.updated_at));
        out.truncate(config.max_issues_per_scan);
        out
    }
}

impl Default for IssueDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue(
        number: u64,
        state: &str,
        labels: &[&str],
        updated_at: DateTime<Utc>,
    ) -> PlatformIssue {
        PlatformIssue {
            number,
            title: format!("issue {}", number),
            body: String::new(),
            state: state.into(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            author: "someone".into(),
            created_at: updated_at,
            updated_at,
        }
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn filter_applies_state_label_watermark_and_cap() {
        let config = GitHubIntegrationConfig {
            max_issues_per_scan: 2,
            ..Default::default()
        };

        let issues = vec![
            issue(1, "open", &[], ts(100)),
            issue(2, "closed", &[], ts(200)),
            issue(3, "open", &["wontfix"], ts(300)),
            issue(4, "open", &[], ts(400)),
            issue(5, "open", &[], ts(500)),
        ];

        let filtered = IssueDiscovery::filter(issues, &config, Some(ts(150)));
        // #2 wrong state, #3 forbidden label, #1 before watermark.
        // Newest first, capped at 2.
        assert_eq!(
            filtered.iter().map(|i| i.number).collect::<Vec<_>>(),
            vec![5, 4]
        );
    }

    #[test]
    fn filter_without_watermark_keeps_all_matching() {
        let config = GitHubIntegrationConfig::default();
        let issues = vec![
            issue(1, "open", &[], ts(100)),
            issue(2, "open", &[], ts(200)),
        ];
        let filtered = IssueDiscovery::filter(issues, &config, None);
        assert_eq!(filtered.len(), 2);
    }
}
