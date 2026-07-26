use chrono::{Duration, Timelike, Utc};
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::QuotaResult;
use crate::{PreCheckResult, QuotaSummary};

/// Daily usage entry.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DailyUsage {
    pub date: String, // YYYY-MM-DD
    pub tokens_used: u64,
    pub request_count: u64,
}

/// Manages per-user and per-workspace token quotas backed by Redis.
#[derive(Debug, Clone)]
pub struct QuotaManager {
    redis: Arc<Mutex<redis::aio::MultiplexedConnection>>,
    default_quota: u64,
}

impl QuotaManager {
    pub fn new(redis: redis::aio::MultiplexedConnection, default_quota: u64) -> Self {
        Self {
            redis: Arc::new(Mutex::new(redis)),
            default_quota,
        }
    }

    /// Build the Redis key for a user's remaining quota.
    fn user_key(user_id: &str) -> String {
        format!("quota:remaining:{}", user_id)
    }

    /// Build the Redis key for a workspace's remaining quota.
    fn workspace_key(workspace_id: &str) -> String {
        format!("quota:ws:remaining:{}", workspace_id)
    }

    /// Build the Redis key for a user's daily usage counter.
    fn user_used_key(user_id: &str) -> String {
        format!("quota:used:{}", user_id)
    }

    /// Build the Redis key for a workspace's daily usage counter.
    fn workspace_used_key(workspace_id: &str) -> String {
        format!("quota:used:ws:{}", workspace_id)
    }

    /// Build the Redis key for daily request count per user.
    fn user_requests_key(user_id: &str) -> String {
        format!("quota:requests:{}", user_id)
    }

    /// Get seconds until next midnight (UTC).
    fn seconds_until_midnight() -> u64 {
        let now = Utc::now();
        let seconds_today =
            now.num_seconds_from_midnight() as i64 + now.timestamp_subsec_millis() as i64 / 1000;
        (Duration::seconds(86400).num_seconds() - seconds_today).max(0) as u64
    }

    /// Bootstrap a missing quota key to `default_quota` with TTL until midnight.
    async fn bootstrap_key(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
    ) -> QuotaResult<()> {
        let ttl = Self::seconds_until_midnight();
        if ttl > 0 {
            let _: () = conn.set_ex(key, self.default_quota, ttl).await?;
        } else {
            let _: () = conn.set(key, self.default_quota).await?;
        }
        Ok(())
    }

    /// Pre-check quota before processing a request.
    /// If enough quota exists, pre-deduct the estimated amount.
    pub async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        let mut conn = self.redis.lock().await;

        let user_key = Self::user_key(user_id);
        let user_remaining: Option<i64> = conn.get(&user_key).await.ok();
        let user_remaining = if let Some(v) = user_remaining {
            v.max(0) as u64
        } else {
            // Bootstrap missing key with TTL
            let _ = self.bootstrap_key(&mut conn, &user_key).await;
            self.default_quota
        };

        let ws_remaining = if let Some(ws_id) = workspace_id {
            let ws_key = Self::workspace_key(ws_id);
            let ws: Option<i64> = conn.get(&ws_key).await.ok();
            if let Some(v) = ws {
                v.max(0) as u64
            } else {
                let _ = self.bootstrap_key(&mut conn, &ws_key).await;
                self.default_quota
            }
        } else {
            u64::MAX
        };

        let remaining = user_remaining.min(ws_remaining);
        // remaining == 0 means the quota is exhausted: reject even zero-cost
        // requests so callers get a clear 429 instead of silent pass-through.
        let allowed = remaining > 0 && remaining >= estimated_tokens;

        if allowed {
            // Atomically deduct: read current (or default), compute new value, and set it.
            let current: Option<i64> = conn.get(&user_key).await.ok();
            let current = current.unwrap_or(self.default_quota as i64);
            let new_remaining = (current - estimated_tokens as i64).max(0);
            let _: Result<(), _> = conn.set(&user_key, new_remaining).await;

            if let Some(ws_id) = workspace_id {
                let ws_key = Self::workspace_key(ws_id);
                let current: Option<i64> = conn.get(&ws_key).await.ok();
                let current = current.unwrap_or(self.default_quota as i64);
                let new_remaining = (current - estimated_tokens as i64).max(0);
                let _: Result<(), _> = conn.set(&ws_key, new_remaining).await;
            }
        }

        PreCheckResult {
            allowed,
            remaining,
            estimated_cost: 0.0,
        }
    }

    /// Finalize quota after actual consumption.
    /// Adjusts the pre-deduction to match actual usage.
    pub async fn finalize(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
        actual_tokens: u64,
    ) -> QuotaResult<()> {
        let mut conn = self.redis.lock().await;

        let diff = estimated_tokens as i64 - actual_tokens as i64;

        if diff > 0 {
            // Over-deducted: refund the difference
            let user_key = Self::user_key(user_id);
            let _: Result<(), _> = conn.incr(&user_key, diff as u64).await;
            if let Some(ws_id) = workspace_id {
                let ws_key = Self::workspace_key(ws_id);
                let _: Result<(), _> = conn.incr(&ws_key, diff as u64).await;
            }
        } else if diff < 0 {
            // Under-deducted: deduct additional
            let additional = (-diff) as u64;
            let user_key = Self::user_key(user_id);
            let _: Result<(), _> = conn.decr(&user_key, additional).await;
            if let Some(ws_id) = workspace_id {
                let ws_key = Self::workspace_key(ws_id);
                let _: Result<(), _> = conn.decr(&ws_key, additional).await;
            }
        }

        // Record actual usage in daily counters with TTL
        let ttl = Self::seconds_until_midnight();
        if ttl > 0 {
            let user_used_key = Self::user_used_key(user_id);
            let _: Result<(), _> = conn.incr(&user_used_key, actual_tokens).await;
            let _: Result<(), _> = conn.expire(&user_used_key, ttl as i64).await;

            let user_req_key = Self::user_requests_key(user_id);
            let _: Result<(), _> = conn.incr(&user_req_key, 1u64).await;
            let _: Result<(), _> = conn.expire(&user_req_key, ttl as i64).await;

            if let Some(ws_id) = workspace_id {
                let ws_used_key = Self::workspace_used_key(ws_id);
                let _: Result<(), _> = conn.incr(&ws_used_key, actual_tokens).await;
                let _: Result<(), _> = conn.expire(&ws_used_key, ttl as i64).await;
            }
        }

        Ok(())
    }

    /// Get remaining quota for a user.
    pub async fn get_remaining(&self, user_id: &str) -> u64 {
        let mut conn = self.redis.lock().await;
        let key = Self::user_key(user_id);
        let remaining: Option<i64> = conn.get(&key).await.ok();
        remaining.unwrap_or(self.default_quota as i64).max(0) as u64
    }

    /// Get remaining quota for a workspace.
    pub async fn get_workspace_remaining(&self, workspace_id: &str) -> u64 {
        let mut conn = self.redis.lock().await;
        let key = Self::workspace_key(workspace_id);
        let remaining: Option<i64> = conn.get(&key).await.ok();
        remaining.unwrap_or(self.default_quota as i64).max(0) as u64
    }

    /// Get today's used tokens for a user.
    pub async fn get_used_today(&self, user_id: &str) -> u64 {
        let mut conn = self.redis.lock().await;
        let key = Self::user_used_key(user_id);
        let used: Option<i64> = conn.get(&key).await.ok();
        used.unwrap_or(0).max(0) as u64
    }

    /// Get today's used tokens for a workspace.
    pub async fn get_workspace_used_today(&self, workspace_id: &str) -> u64 {
        let mut conn = self.redis.lock().await;
        let key = Self::workspace_used_key(workspace_id);
        let used: Option<i64> = conn.get(&key).await.ok();
        used.unwrap_or(0).max(0) as u64
    }

    /// Get today's request count for a user.
    pub async fn get_request_count_today(&self, user_id: &str) -> u64 {
        let mut conn = self.redis.lock().await;
        let key = Self::user_requests_key(user_id);
        let count: Option<i64> = conn.get(&key).await.ok();
        count.unwrap_or(0).max(0) as u64
    }

    /// Get a full quota summary for a user.
    pub async fn get_user_summary(&self, user_id: &str) -> QuotaSummary {
        let remaining = self.get_remaining(user_id).await;
        let used_today = self.get_used_today(user_id).await;
        let total = remaining + used_today;
        QuotaSummary {
            target_id: user_id.to_string(),
            target_type: "user".into(),
            total_quota: total,
            remaining,
            used_today,
        }
    }

    /// Get a full quota summary for a workspace.
    pub async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary {
        let remaining = self.get_workspace_remaining(workspace_id).await;
        let used_today = self.get_workspace_used_today(workspace_id).await;
        let total = remaining + used_today;
        QuotaSummary {
            target_id: workspace_id.to_string(),
            target_type: "workspace".into(),
            total_quota: total,
            remaining,
            used_today,
        }
    }

    /// Recharge a user's quota.
    pub async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        let mut conn = self.redis.lock().await;
        let key = Self::user_key(user_id);

        let current: Option<u64> = conn.get(&key).await.ok();
        let new_amount = current.unwrap_or(0) + tokens;

        let ttl = if let Some(until) = valid_until {
            let now = chrono::Utc::now();
            let diff = until.signed_duration_since(now);
            diff.num_seconds().max(0) as u64
        } else {
            Self::seconds_until_midnight()
        };

        if ttl > 0 {
            let _: () = conn.set_ex(&key, new_amount, ttl).await?;
        } else {
            let _: () = conn.set(&key, new_amount).await?;
        }
        Ok(())
    }

    /// Recharge a workspace's quota.
    pub async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        let mut conn = self.redis.lock().await;
        let key = Self::workspace_key(workspace_id);

        let current: Option<u64> = conn.get(&key).await.ok();
        let new_amount = current.unwrap_or(0) + tokens;

        let ttl = if let Some(until) = valid_until {
            let now = chrono::Utc::now();
            let diff = until.signed_duration_since(now);
            diff.num_seconds().max(0) as u64
        } else {
            Self::seconds_until_midnight()
        };

        if ttl > 0 {
            let _: () = conn.set_ex(&key, new_amount, ttl).await?;
        } else {
            let _: () = conn.set(&key, new_amount).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl cog_core::WorkspaceQuotaSource for QuotaManager {
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
