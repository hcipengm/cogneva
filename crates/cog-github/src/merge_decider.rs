//! Auto-merge decision for generated PRs.
//!
//! Implements the `AutoMergePolicy` checks from the design doc: CI must pass,
//! no review requested, changed-line budget, forbidden paths, forbidden
//! labels, and a cooldown window. Human review remains an override, not a
//! blocker.

use chrono::Utc;

use cog_core::AutoMergePolicy;

use crate::provider::PullRequestDetail;

/// Outcome of a merge decision.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeDecision {
    /// All policy checks passed — merge automatically.
    AutoMerge,
    /// At least one check failed — wait for humans or for state to change.
    Wait {
        /// Why merging is held back.
        reason: String,
    },
}

/// Stateless policy evaluator.
pub struct MergeDecider;

impl MergeDecider {
    /// Evaluate a PR against the auto-merge policy.
    pub fn can_auto_merge(pr: &PullRequestDetail, policy: &AutoMergePolicy) -> MergeDecision {
        if !policy.enabled {
            return MergeDecision::Wait {
                reason: "auto-merge policy disabled".into(),
            };
        }

        if pr.state != "open" {
            return MergeDecision::Wait {
                reason: format!("PR is not open (state: {})", pr.state),
            };
        }

        if policy.require_ci_pass && pr.ci_passed != Some(true) {
            return MergeDecision::Wait {
                reason: match pr.ci_passed {
                    Some(false) => "CI is failing".into(),
                    Some(true) => unreachable!(),
                    None => "CI status unknown".into(),
                },
            };
        }

        if policy.require_no_review_requested && pr.review_requested {
            return MergeDecision::Wait {
                reason: "human review requested".into(),
            };
        }

        if pr.changed_lines > policy.max_changed_lines {
            return MergeDecision::Wait {
                reason: format!(
                    "changed lines {} exceed budget {}",
                    pr.changed_lines, policy.max_changed_lines
                ),
            };
        }

        for file in &pr.affected_files {
            if policy.forbidden_paths.iter().any(|p| path_matches(p, file)) {
                return MergeDecision::Wait {
                    reason: format!("touches forbidden path: {}", file),
                };
            }
        }

        if let Some(label) = pr
            .labels
            .iter()
            .find(|l| policy.forbidden_labels.contains(l))
        {
            return MergeDecision::Wait {
                reason: format!("forbidden label: {}", label),
            };
        }

        let cooldown = chrono::Duration::hours(policy.cooldown_hours as i64);
        if Utc::now() - pr.created_at < cooldown {
            return MergeDecision::Wait {
                reason: format!("cooldown of {}h not elapsed", policy.cooldown_hours),
            };
        }

        MergeDecision::AutoMerge
    }
}

/// Minimal glob matching for forbidden paths: supports `*` suffix/prefix
/// patterns (`deploy/`, `*.lock`, `.github/workflows`).
fn path_matches(pattern: &str, path: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        return path.starts_with(prefix);
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return path.ends_with(suffix);
    }
    path == pattern || path.starts_with(&format!("{}/", pattern.trim_end_matches('/')))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::PullRequestDetail;

    fn pr() -> PullRequestDetail {
        PullRequestDetail {
            number: 1,
            title: "fix".into(),
            url: String::new(),
            state: "open".into(),
            labels: vec![],
            changed_lines: 50,
            affected_files: vec!["crates/cog-core/src/lib.rs".into()],
            ci_passed: Some(true),
            review_requested: false,
            head_sha: "abc".into(),
            created_at: Utc::now() - chrono::Duration::hours(48),
        }
    }

    #[test]
    fn merges_when_all_checks_pass() {
        assert_eq!(
            MergeDecider::can_auto_merge(&pr(), &AutoMergePolicy::default()),
            MergeDecision::AutoMerge
        );
    }

    #[test]
    fn waits_when_ci_failing_or_unknown() {
        let policy = AutoMergePolicy::default();
        let mut failing = pr();
        failing.ci_passed = Some(false);
        assert!(matches!(
            MergeDecider::can_auto_merge(&failing, &policy),
            MergeDecision::Wait { .. }
        ));
        let mut unknown = pr();
        unknown.ci_passed = None;
        assert!(matches!(
            MergeDecider::can_auto_merge(&unknown, &policy),
            MergeDecision::Wait { .. }
        ));
    }

    #[test]
    fn waits_on_forbidden_path_globs() {
        let policy = AutoMergePolicy::default();
        let mut lock = pr();
        lock.affected_files = vec!["Cargo.lock".into()];
        assert!(matches!(
            MergeDecider::can_auto_merge(&lock, &policy),
            MergeDecision::Wait { .. }
        ));
        let mut workflow = pr();
        workflow.affected_files = vec![".github/workflows/ci.yml".into()];
        assert!(matches!(
            MergeDecider::can_auto_merge(&workflow, &policy),
            MergeDecision::Wait { .. }
        ));
        let mut deploy = pr();
        deploy.affected_files = vec!["deploy/k3s/pod.yaml".into()];
        assert!(matches!(
            MergeDecider::can_auto_merge(&deploy, &policy),
            MergeDecision::Wait { .. }
        ));
    }

    #[test]
    fn waits_on_labels_lines_cooldown_and_review() {
        let policy = AutoMergePolicy::default();

        let mut labeled = pr();
        labeled.labels = vec!["manual-only".into()];
        assert!(matches!(
            MergeDecider::can_auto_merge(&labeled, &policy),
            MergeDecision::Wait { .. }
        ));

        let mut big = pr();
        big.changed_lines = 500;
        assert!(matches!(
            MergeDecider::can_auto_merge(&big, &policy),
            MergeDecision::Wait { .. }
        ));

        let mut fresh = pr();
        fresh.created_at = Utc::now();
        assert!(matches!(
            MergeDecider::can_auto_merge(&fresh, &policy),
            MergeDecision::Wait { .. }
        ));

        let mut reviewed = pr();
        reviewed.review_requested = true;
        let strict = AutoMergePolicy {
            require_no_review_requested: true,
            ..Default::default()
        };
        assert!(matches!(
            MergeDecider::can_auto_merge(&reviewed, &strict),
            MergeDecision::Wait { .. }
        ));
    }

    #[test]
    fn disabled_policy_waits() {
        let policy = AutoMergePolicy {
            enabled: false,
            ..Default::default()
        };
        assert!(matches!(
            MergeDecider::can_auto_merge(&pr(), &policy),
            MergeDecision::Wait { .. }
        ));
    }
}
