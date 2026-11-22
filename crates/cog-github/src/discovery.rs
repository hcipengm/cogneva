//! Issue discovery — actively scans the configured repository for issues
//! that should enter the self-evolution pipeline.
//!
//! Filtering: only issues whose state is in `allowed_issue_states`, that
//! carry none of
//! the `forbidden_labels`, and that were updated since the last poll are
//! returned, capped at `max_issues_per_scan` oldest first: the scan remembers
//! the last issue it handed out, so what the cap held back is the next round's
//! window rather than a set that never comes back.

use chrono::{DateTime, Utc};

use crate::config::GitHubIntegrationConfig;

use crate::error::Result;
use crate::provider::{CodePlatformProvider, PlatformIssue};

/// 上一拍扫到了哪里：最后一条交出去的 issue 的更新时刻与编号。
///
/// 两个字段而不是一个时刻，是因为 `updated_at` 按秒记，同一秒里装得下好几条：
/// 只拿时刻当水位，卡在界上那一秒里剩下的几条，下一拍用「晚于」一判就再也
/// 拿不到了。编号补上这一秒内的先后，界因此不再丢掉东西。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    /// 最后一条交出去的 issue 的 `updated_at`。
    pub at: DateTime<Utc>,
    /// 同一时刻里的先后：下一拍从这一条之后接着取。
    pub number: u64,
}

/// Active scanner for platform issues.
///
/// The scanner is intentionally stateless apart from its cursor; the discovery
/// loop owns persistence of that watermark.
pub struct IssueDiscovery {
    cursor: Option<Cursor>,
}

impl IssueDiscovery {
    /// Create a new discovery scanner with no watermark (first scan returns
    /// every matching issue).
    pub fn new() -> Self {
        Self { cursor: None }
    }

    /// Resume from a persisted timestamp: everything updated at or before it
    /// counts as seen. There is no number to resume from, so issues sharing
    /// that second with the watermark count as seen too.
    pub fn with_watermark(last_polled_at: DateTime<Utc>) -> Self {
        Self {
            cursor: Some(Cursor {
                at: last_polled_at,
                number: u64::MAX,
            }),
        }
    }

    /// The current watermark, if any.
    pub fn last_polled_at(&self) -> Option<DateTime<Utc>> {
        self.cursor.map(|c| c.at)
    }

    /// Pull candidate issues from the provider and apply the configured
    /// filters. Advances the watermark to the last issue handed out.
    ///
    /// Not to the newest one seen: the window is taken oldest first, so the
    /// ones the cap left behind are newer than the last one handed out and the
    /// next scan picks them up. Advancing to the newest would skip them for
    /// good — the module's "capped at" would read as "lost beyond".
    pub async fn scan(
        &mut self,
        provider: &dyn CodePlatformProvider,
        config: &GitHubIntegrationConfig,
    ) -> Result<Vec<PlatformIssue>> {
        let issues = provider.list_open_issues().await?;
        let filtered = Self::filter(issues, config, self.cursor);

        if let Some(last) = filtered.last() {
            self.cursor = Some(Cursor {
                at: last.updated_at,
                number: last.number,
            });
        }

        Ok(filtered)
    }

    /// Pure filter: allowed states, no forbidden labels, updated after the
    /// cursor, capped at `max_issues_per_scan` oldest first.
    ///
    /// Oldest first is what makes the cap a window rather than a cut: the
    /// caller's cursor lands on the last row here, and every row the cap held
    /// back is newer than it, so the next scan starts exactly where this one
    /// stopped.
    pub fn filter(
        issues: Vec<PlatformIssue>,
        config: &GitHubIntegrationConfig,
        cursor: Option<Cursor>,
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
            .filter(|i| {
                cursor
                    .map(|c| i.updated_at > c.at || (i.updated_at == c.at && i.number > c.number))
                    .unwrap_or(true)
            })
            .collect();

        out.sort_by(|a, b| {
            a.updated_at
                .cmp(&b.updated_at)
                .then(a.number.cmp(&b.number))
        });
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
            url: String::new(),
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

        let filtered = IssueDiscovery::filter(
            issues,
            &config,
            Some(Cursor {
                at: ts(150),
                number: u64::MAX,
            }),
        );
        // #2 wrong state, #3 forbidden label, #1 before watermark.
        // Oldest first, capped at 2.
        assert_eq!(
            filtered.iter().map(|i| i.number).collect::<Vec<_>>(),
            vec![4, 5]
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

    /// 一个返回固定清单的平台，用来把 `scan` 本身（而不只是纯过滤）走一遍。
    struct FixedIssues(Vec<PlatformIssue>);

    #[async_trait::async_trait]
    impl CodePlatformProvider for FixedIssues {
        async fn list_open_issues(&self) -> Result<Vec<PlatformIssue>> {
            Ok(self.0.clone())
        }
        async fn create_pull_request(
            &self,
            _req: crate::provider::CreatePullRequest,
        ) -> Result<crate::provider::PlatformPullRequest> {
            Err(crate::error::CogGitHubError::Provider("unused".into()))
        }
        async fn comment_on_issue(&self, _n: u64, _body: String) -> Result<()> {
            Ok(())
        }
        async fn merge_pull_request(&self, _n: u64, _sha: String) -> Result<()> {
            Ok(())
        }
        async fn get_pull_request(&self, _n: u64) -> Result<crate::provider::PullRequestDetail> {
            Err(crate::error::CogGitHubError::Provider("unused".into()))
        }
    }

    /// 一次扫描的界是一个窗口，不是一道剪子：卡在上面的那几条要在下一拍回来。
    /// 这里走的是首扫（没有水位）上一条真会发生的形状——仓库里已经开着比界多
    /// 的 issue——并连扫三拍，要求五条一条不落、且没有一条被重复交出去。
    #[tokio::test]
    async fn a_scan_that_hits_the_cap_hands_the_rest_to_the_next_one() {
        let config = GitHubIntegrationConfig {
            max_issues_per_scan: 2,
            ..Default::default()
        };
        let platform = FixedIssues(vec![
            issue(1, "open", &[], ts(100)),
            issue(2, "open", &[], ts(200)),
            issue(3, "open", &[], ts(300)),
            issue(4, "open", &[], ts(400)),
            issue(5, "open", &[], ts(500)),
        ]);

        let mut discovery = IssueDiscovery::new();
        let mut seen: Vec<u64> = Vec::new();
        for _ in 0..3 {
            let window = discovery.scan(&platform, &config).await.expect("a scan");
            seen.extend(window.iter().map(|i| i.number));
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);

        // 第四拍：全都交过了，窗口必须是空的，而不是从头再来一遍。
        let again = discovery.scan(&platform, &config).await.expect("a scan");
        assert!(
            again.is_empty(),
            "re-handed {:?}",
            again.iter().map(|i| i.number).collect::<Vec<_>>()
        );
    }

    /// 同一秒里更新的几条在界上不能丢：水位是时刻加编号，不是只有一个时刻。
    #[test]
    fn a_tie_on_the_second_the_cap_lands_in_loses_nothing() {
        let config = GitHubIntegrationConfig {
            max_issues_per_scan: 2,
            ..Default::default()
        };
        let all = || {
            vec![
                issue(1, "open", &[], ts(100)),
                issue(2, "open", &[], ts(200)),
                issue(3, "open", &[], ts(200)),
                issue(4, "open", &[], ts(200)),
            ]
        };

        let first = IssueDiscovery::filter(all(), &config, None);
        assert_eq!(
            first.iter().map(|i| i.number).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let last = first.last().expect("a window");
        let second = IssueDiscovery::filter(
            all(),
            &config,
            Some(Cursor {
                at: last.updated_at,
                number: last.number,
            }),
        );
        assert_eq!(
            second.iter().map(|i| i.number).collect::<Vec<_>>(),
            vec![3, 4]
        );
    }
}
