//! Owner policy control for the contribution channel.
//!
//! The policy decides what happens to a change that passed the quality gates:
//! `Auto` submits it as a PR right away, `Ask` stages it for explicit owner
//! approval, `Local` keeps it on disk. The controller owns the in-process
//! view (the gateway persists the choice into the cluster Secret) and the
//! live PR sink used to flush the staged backlog on approval.

use std::sync::{Arc, RwLock};

use cog_core::{ContributionControl, ContributionPolicy, GeneratedChange, SFError, SFResult};

use crate::pending_changes;
use crate::pr_publisher::GitHubChangeSink;

/// In-process owner policy + live sink for the contribution channel.
pub struct ContributionController {
    policy: RwLock<ContributionPolicy>,
    sink: RwLock<Option<Arc<GitHubChangeSink>>>,
}

impl ContributionController {
    /// Shared instance with the default policy (`Auto`) and no sink.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Publish (or replace) the live PR sink once the channel is connected.
    pub fn set_sink(&self, sink: Arc<GitHubChangeSink>) {
        *self.sink.write().unwrap_or_else(|e| e.into_inner()) = Some(sink);
    }

    /// Current owner policy.
    pub fn policy(&self) -> ContributionPolicy {
        *self.policy.read().unwrap_or_else(|e| e.into_inner())
    }

    /// True when generated changes must be staged instead of published.
    pub fn should_stage(&self) -> bool {
        self.policy() != ContributionPolicy::Auto
    }
}

impl Default for ContributionController {
    fn default() -> Self {
        Self {
            policy: RwLock::new(ContributionPolicy::Auto),
            sink: RwLock::new(None),
        }
    }
}

#[async_trait::async_trait]
impl ContributionControl for ContributionController {
    fn policy(&self) -> ContributionPolicy {
        ContributionController::policy(self)
    }

    fn set_policy(&self, policy: ContributionPolicy) {
        *self.policy.write().unwrap_or_else(|e| e.into_inner()) = policy;
    }

    async fn pending(&self) -> SFResult<Vec<GeneratedChange>> {
        Ok(pending_changes::load_pending().await)
    }

    async fn flush_pending(&self, change_id: Option<&str>) -> SFResult<usize> {
        let sink = self
            .sink
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                SFError::Config("贡献通道未连接：先连接平台账号，再提交暂存变更".to_string())
            })?;
        let mut flushed = 0;
        for change in pending_changes::load_pending().await {
            if let Some(id) = change_id {
                if change.change_id != id {
                    continue;
                }
            }
            // Owner approval bypasses the policy gate — the click IS the decision.
            match sink.publish_approved(&change).await {
                Ok(_) => {
                    pending_changes::remove_staged(&change.change_id).await;
                    flushed += 1;
                }
                Err(e) => {
                    return Err(SFError::Internal(format!(
                        "变更 {} 提交失败：{e}",
                        change.change_id
                    )));
                }
            }
        }
        Ok(flushed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_auto_and_does_not_stage() {
        let c = ContributionController::new_shared();
        assert_eq!(c.policy(), ContributionPolicy::Auto);
        assert!(!c.should_stage());
    }

    #[test]
    fn ask_and_local_stage_changes() {
        let c = ContributionController::new_shared();
        cog_core::ContributionControl::set_policy(c.as_ref(), ContributionPolicy::Ask);
        assert!(c.should_stage());
        cog_core::ContributionControl::set_policy(c.as_ref(), ContributionPolicy::Local);
        assert!(c.should_stage());
        cog_core::ContributionControl::set_policy(c.as_ref(), ContributionPolicy::Auto);
        assert!(!c.should_stage());
    }

    #[tokio::test]
    async fn flush_without_sink_is_explicit_error() {
        let c = ContributionController::new_shared();
        let err = cog_core::ContributionControl::flush_pending(c.as_ref(), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("贡献通道未连接"));
    }
}
