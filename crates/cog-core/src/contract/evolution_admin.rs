//! Admin-facing contract for the self-evolution pipeline.
//!
//! This trait lives in `cog-core` so the Gateway can expose evolution controls
//! without taking a direct dependency on `cog-reflection`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Summarized view of a single evolution artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionPatchInfo {
    pub id: String,
    pub kind: String,
    pub description: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    /// One-line diff summary (e.g. "3 files, +42 -17"; policy updates show
    /// the version transition). Derived from the artifact content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_summary: Option<String>,
    /// Statistical evaluation verdict (two-proportion z-test) when the
    /// artifact passed through the eval gate, e.g. "Adopt z=2.31 uplift +18%".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval_summary: Option<String>,
}

/// Request to evaluate an artifact-level policy candidate against a baseline
/// (产物级进化 §14.3). The verdict is gated by a two-proportion z-test;
/// an `Adopt` verdict does **not** activate the policy — it stages the
/// candidate at `AwaitingReview` until a human approves it, mirroring the
/// source-level `manual_approve` gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEvalRequest {
    /// Policy name (sanitized to `[A-Za-z0-9._-]`).
    pub name: String,
    /// Baseline outcomes under the current policy.
    pub baseline_outcomes: Vec<bool>,
    /// Candidate policy payload.
    pub candidate_payload: serde_json::Value,
    /// Candidate outcomes observed under the candidate payload.
    pub candidate_outcomes: Vec<bool>,
    /// Human-readable reason for the proposal (recorded in the version chain).
    pub reason: String,
}

/// Response from an explicit apply request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionApplyResponse {
    pub patch_id: String,
    pub test_passed: bool,
    pub test_output: String,
    pub new_status: String,
    pub files_changed: Vec<String>,
}

/// Response from an explicit deploy request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionDeployResponse {
    pub patch_id: String,
    pub commit_hash: String,
    pub staged_binary_path: String,
    pub switched: bool,
}

/// Snapshot of the self-evolution counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionMetricsSnapshot {
    pub events_total: u64,
    pub events_failed: u64,
    pub patches_applied: u64,
    pub patches_failed: u64,
}

/// A single entry in the evolution event stream (artifact lifecycle record).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionEventInfo {
    pub id: String,
    pub kind: String,
    pub description: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

/// Response from an explicit rollback request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionRollbackResponse {
    pub rolled_back: bool,
    pub message: String,
}

/// Admin operations for the self-evolution subsystem.
#[async_trait::async_trait]
pub trait EvolutionAdmin: Send + Sync {
    /// List all known evolution artifacts (newest first).
    async fn list_patches(&self) -> crate::SFResult<Vec<EvolutionPatchInfo>>;

    /// Apply a single patch to the working tree and run the test suite.
    async fn apply_patch(&self, patch_id: &str) -> crate::SFResult<EvolutionApplyResponse>;

    /// Commit and build a patch, then optionally stage and switch to the new binary.
    async fn deploy_patch(&self, patch_id: &str) -> crate::SFResult<EvolutionDeployResponse>;

    /// Human-in-the-loop gate release (Phase 3.1/4.3 `manual_approve`):
    /// approve a patch that passed tests and is held at `AwaitingReview`,
    /// then commit/build/deploy it. Rejects patches not awaiting review.
    /// Default: not supported by this implementation.
    async fn approve_patch(&self, _patch_id: &str) -> crate::SFResult<EvolutionDeployResponse> {
        Err(crate::SFError::NotImplemented("evolution approve".into()))
    }

    /// Roll back to the previously deployed binary and restart.
    /// Default: not supported by this implementation.
    async fn rollback(&self) -> crate::SFResult<EvolutionRollbackResponse> {
        Err(crate::SFError::NotImplemented("evolution rollback".into()))
    }

    /// List recent evolution events (newest first), up to `limit` entries.
    /// Default: not supported by this implementation.
    async fn list_events(&self, _limit: usize) -> crate::SFResult<Vec<EvolutionEventInfo>> {
        Err(crate::SFError::NotImplemented("evolution events".into()))
    }

    /// Evaluate an artifact-level policy candidate (产物级进化).
    /// An `Adopt` verdict stages the candidate at `AwaitingReview`;
    /// `approve_patch` on the returned artifact id hot-swaps the policy.
    /// Default: not supported by this implementation.
    async fn evaluate_policy(
        &self,
        _req: PolicyEvalRequest,
    ) -> crate::SFResult<EvolutionPatchInfo> {
        Err(crate::SFError::NotImplemented("policy evaluate".into()))
    }
}
