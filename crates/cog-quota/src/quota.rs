//! The single-level quota view, answered from the token ledger's projection.
//!
//! This type used to keep an account of its own: one cell per person holding
//! whatever was left of the day's allowance, decremented on the request path
//! and topped up by the recharge route. That writer was removed when it turned
//! out to charge management requests by body length — a number unrelated to
//! what the platform spends — while every reader stayed: four management routes
//! and the supervisor's workspace gate. From then on each of them read the
//! bootstrap default forever, and that default was indistinguishable from a
//! real balance. A customer whose day was already spent saw exactly the number
//! a customer who had spent nothing saw.
//!
//! There is one book. Every metered call is a row in the gateway's token
//! ledger; the hierarchy manager rewrites today's per-person cell from those
//! rows on a period; an enforcement point judges a call against that cell. This
//! type is a reading of the same cells and keeps no counter of its own, so it
//! cannot disagree with what enforcement allowed: a lost cell is repaired by
//! the next projection pass instead of resetting a customer's day to a default.
//!
//! The two writes that remain belong to the one book as well. A recharge raises
//! the declared ceiling — it used to add to a counter no ceiling was compared
//! against, so the "tokens added" it reported could never let a call through
//! and could never stop one. `pre_check` only reads: what a call costs is
//! landed once, as a ledger row, and projected back, so deducting an estimate
//! here would be a second accounting beside the ledger.

use std::sync::Arc;

use crate::error::{QuotaError, QuotaResult};
use crate::observable::QuotaReadings;
use crate::{PreCheckResult, QuotaSummary};
use cog_core::{HierarchyManager, QuotaContext, QuotaLimits, QuotaScope, ScopeStatus};

/// Reads one person's (or one workspace's) day out of the quota hierarchy.
#[derive(Clone)]
pub struct QuotaManager {
    /// The book every answer here is read from. Held as the trait object
    /// because this type never needs to know which store the hierarchy writes
    /// to — only what the cells say right now.
    hierarchy: Arc<dyn HierarchyManager>,
    /// This process's own decisions, counted where they are made.
    ///
    /// Held here rather than read back out of the store at scrape time because
    /// the store is shared with every other replica: counting the fleet's
    /// traffic under one pod's name would make the reading depend on how many
    /// replicas happened to be up.
    readings: Arc<QuotaReadings>,
}

impl std::fmt::Debug for QuotaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaManager")
            .field("hierarchy", &"quota hierarchy")
            .finish()
    }
}

impl QuotaManager {
    pub fn new(hierarchy: Arc<dyn HierarchyManager>) -> Self {
        Self {
            hierarchy,
            readings: Arc::new(QuotaReadings::default()),
        }
    }

    /// This manager's decision counters, for the process's metrics face.
    ///
    /// A clone of the handle, so the reader keeps counting into the same
    /// counters the reader was built over.
    pub fn readings(&self) -> Arc<QuotaReadings> {
        self.readings.clone()
    }

    /// A context holding exactly one cell: the one being read.
    ///
    /// The other four levels are left empty on purpose. Filling them in would
    /// make this reading the minimum over someone else's cells, which is an
    /// answer to a different question than "what does this person's own cell
    /// say" — and it would need a caller that knows the workspace, team and
    /// organization of a person, which the management routes do not have.
    fn context(scope: QuotaScope, target_id: &str) -> QuotaContext {
        let mut ctx = QuotaContext::default();
        let slot = match scope {
            QuotaScope::User => &mut ctx.user_id,
            QuotaScope::Workspace => &mut ctx.workspace_id,
            QuotaScope::Team => &mut ctx.team_id,
            QuotaScope::Organization => &mut ctx.organization_id,
            QuotaScope::Global => &mut ctx.global_id,
        };
        *slot = Some(target_id.to_string());
        ctx
    }

    /// What enforcement would say about one cell right now, with nothing to
    /// add: the ceiling in force and today's projected spend.
    async fn scope_reading(&self, scope: QuotaScope, target_id: &str) -> Option<ScopeStatus> {
        self.hierarchy
            .check(&Self::context(scope, target_id), 0)
            .await
            .scopes
            .into_iter()
            .next()
    }

    async fn spend_today(&self, scope: QuotaScope, target_id: &str) -> u64 {
        self.scope_reading(scope, target_id)
            .await
            .map(|reading| reading.used_today)
            .unwrap_or(0)
    }

    async fn remaining_of(&self, scope: QuotaScope, target_id: &str) -> u64 {
        self.scope_reading(scope, target_id)
            .await
            .map(|reading| reading.remaining)
            .unwrap_or(0)
    }

    /// The ceiling in force for one cell: the declared pair, or the
    /// hierarchy's default when nobody declared one.
    async fn ceiling(&self, scope: QuotaScope, target_id: &str) -> QuotaLimits {
        self.scope_reading(scope, target_id)
            .await
            .map(|reading| reading.limits)
            .unwrap_or_default()
    }

    /// Ask whether a call of `estimated_tokens` fits, without spending
    /// anything.
    ///
    /// The judgement is the same cascade the LLM proxy runs, so a caller that
    /// asks here and then makes the call gets the same answer twice rather
    /// than two policies that drift. Deducting the estimate is deliberately not
    /// part of it: the spend is landed once per call and projected back onto
    /// the cell, and a second deduction from the request path would make the
    /// cell disagree with the rows it is computed from.
    pub async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        let mut ctx = Self::context(QuotaScope::User, user_id);
        ctx.workspace_id = workspace_id.map(str::to_string);
        let decision = self.hierarchy.check(&ctx, estimated_tokens).await;

        // Counted at the decision, which is the only place that knows it: a
        // refusal reaches only the caller it refused, so this counter is the
        // whole of what an operator can learn about the rate.
        if decision.allowed {
            self.readings.record_allowed();
        } else {
            self.readings.record_refused();
        }

        PreCheckResult {
            allowed: decision.allowed,
            // The tightest ceiling the call was judged against: reporting the
            // user cell while a workspace cell is what refused it would name a
            // number the refusal did not use.
            remaining: decision
                .scopes
                .iter()
                .map(|scope| scope.remaining)
                .min()
                .unwrap_or(0),
            estimated_cost: 0.0,
        }
    }

    /// Nothing to reconcile.
    ///
    /// The spend of a call was landed when the call finished, and the cell a
    /// ceiling is judged against takes that total back on the next projection
    /// pass. An estimate charged earlier has nothing to correct, because no
    /// estimate is charged anywhere: the only amount this pipeline knows is the
    /// one the upstream reported.
    pub async fn finalize(
        &self,
        _user_id: &str,
        _workspace_id: Option<&str>,
        _estimated_tokens: u64,
        _actual_tokens: u64,
    ) -> QuotaResult<()> {
        Ok(())
    }

    /// Remaining allowance for a person today: the ceiling minus the projected
    /// spend.
    pub async fn get_remaining(&self, user_id: &str) -> u64 {
        self.remaining_of(QuotaScope::User, user_id).await
    }

    /// Remaining allowance for a workspace today.
    pub async fn get_workspace_remaining(&self, workspace_id: &str) -> u64 {
        self.remaining_of(QuotaScope::Workspace, workspace_id).await
    }

    /// Today's projected spend for a person.
    pub async fn get_used_today(&self, user_id: &str) -> u64 {
        self.spend_today(QuotaScope::User, user_id).await
    }

    /// Today's projected spend for a workspace.
    pub async fn get_workspace_used_today(&self, workspace_id: &str) -> u64 {
        self.spend_today(QuotaScope::Workspace, workspace_id).await
    }

    /// Calls made by a person today.
    ///
    /// The one book counts tokens, not calls: a ledger row carries what the
    /// upstream reported, and the projection writes that total into the cell a
    /// ceiling is judged against. Nothing accumulates a call count, and
    /// inventing one here would be the second book this type exists to not be.
    /// No reader asks this today; it answers zero, which is the shape of "this
    /// count has no source" rather than a measurement.
    pub async fn get_request_count_today(&self, _user_id: &str) -> u64 {
        0
    }

    /// A person's day: the ceiling in force, what was spent, what is left.
    pub async fn get_user_summary(&self, user_id: &str) -> QuotaSummary {
        let reading = self.scope_reading(QuotaScope::User, user_id).await;
        let used_today = reading.as_ref().map(|r| r.used_today).unwrap_or(0);
        let remaining = reading.as_ref().map(|r| r.remaining).unwrap_or(0);
        let total_quota = reading
            .as_ref()
            .map(|r| r.limits.hard_limit)
            .unwrap_or_default();
        QuotaSummary {
            target_id: user_id.to_string(),
            target_type: "user".into(),
            total_quota,
            remaining,
            used_today,
        }
    }

    /// A workspace's day, in the same shape as [`Self::get_user_summary`].
    pub async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary {
        let reading = self
            .scope_reading(QuotaScope::Workspace, workspace_id)
            .await;
        let used_today = reading.as_ref().map(|r| r.used_today).unwrap_or(0);
        let remaining = reading.as_ref().map(|r| r.remaining).unwrap_or(0);
        let total_quota = reading
            .as_ref()
            .map(|r| r.limits.hard_limit)
            .unwrap_or_default();
        QuotaSummary {
            target_id: workspace_id.to_string(),
            target_type: "workspace".into(),
            total_quota,
            remaining,
            used_today,
        }
    }

    /// Raise one cell's ceiling by `tokens`, keeping the soft/hard gap.
    ///
    /// Raising the ceiling is what "give this account more room today" means
    /// once enforcement compares spend against a declared ceiling: the
    /// allowance is not a separate balance that could be added to, it is the
    /// distance between the spend and the ceiling. Writing a balance instead
    /// would leave the enforcement reading a number the top-up never touched.
    async fn raise_ceiling(
        &self,
        scope: QuotaScope,
        target_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        if let Some(until) = valid_until {
            return Err(QuotaError::InvalidArgument(format!(
                "the ceiling is a daily one and resets at UTC midnight, so a top-up cannot expire at {until}; \
                 declare the ceiling for the period you mean with the limits route instead"
            )));
        }
        let current = self.ceiling(scope, target_id).await;
        let raised = QuotaLimits::new(
            current.soft_limit.saturating_add(tokens),
            current.hard_limit.saturating_add(tokens),
        );
        self.hierarchy
            .set_limits(scope, target_id, raised)
            .await
            .map_err(|e| QuotaError::DatabaseError(e.to_string()))
    }

    /// Raise a person's ceiling for today.
    pub async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        self.raise_ceiling(QuotaScope::User, user_id, tokens, valid_until)
            .await
    }

    /// Raise a workspace's ceiling for today.
    pub async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        self.raise_ceiling(QuotaScope::Workspace, workspace_id, tokens, valid_until)
            .await
    }
}

#[async_trait::async_trait]
impl cog_core::WorkspaceQuotaSource for QuotaManager {
    /// The supervisor asks one question: how much room a workspace has left.
    ///
    /// Workspace-level spend has no writer yet — nothing on the platform says
    /// which workspace a given call belongs to — so this answers the declared
    /// ceiling with no spend against it. That is the same number enforcement
    /// would use, which is the point: a gate that reads a counter nothing
    /// writes cannot tell a quiet workspace from an unwired one.
    async fn workspace_remaining(&self, workspace_id: &str) -> u64 {
        self.get_workspace_remaining(workspace_id).await
    }
}

/// Lightweight quota checker for use in middleware and other contexts.
#[derive(Debug, Clone)]
pub struct QuotaChecker {
    manager: QuotaManager,
}

impl QuotaChecker {
    pub fn new(manager: QuotaManager) -> Self {
        Self { manager }
    }

    pub async fn check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        self.manager
            .pre_check(user_id, workspace_id, estimated_tokens)
            .await
    }
}

#[async_trait::async_trait]
impl cog_core::QuotaManager for QuotaManager {
    async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        self.pre_check(user_id, workspace_id, estimated_tokens)
            .await
    }

    async fn finalize(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
        actual_tokens: u64,
    ) -> cog_core::SFResult<()> {
        self.finalize(user_id, workspace_id, estimated_tokens, actual_tokens)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn get_remaining(&self, user_id: &str) -> u64 {
        self.get_remaining(user_id).await
    }

    async fn get_workspace_remaining(&self, workspace_id: &str) -> u64 {
        self.get_workspace_remaining(workspace_id).await
    }

    async fn get_used_today(&self, user_id: &str) -> u64 {
        self.get_used_today(user_id).await
    }

    async fn get_workspace_used_today(&self, workspace_id: &str) -> u64 {
        self.get_workspace_used_today(workspace_id).await
    }

    async fn get_request_count_today(&self, user_id: &str) -> u64 {
        self.get_request_count_today(user_id).await
    }

    async fn get_user_summary(&self, user_id: &str) -> QuotaSummary {
        self.get_user_summary(user_id).await
    }

    async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary {
        self.get_workspace_summary(workspace_id).await
    }

    async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> cog_core::SFResult<()> {
        self.recharge(user_id, tokens, valid_until)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> cog_core::SFResult<()> {
        self.recharge_workspace(workspace_id, tokens, valid_until)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }
}
