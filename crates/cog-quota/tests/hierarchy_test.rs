//! Integration tests for the 5-level quota hierarchy.
//!
//! The manager speaks the core key-value contract, so these tests run against
//! an in-memory implementation and need no Redis: the same suite runs on a
//! developer machine, in CI and in the evolution sandbox instead of skipping
//! wherever no server happens to be up. Each test builds its own store, so
//! there is nothing to clean up between them.

use cog_core::{KeyValueStore, QuotaContext, QuotaLimits, QuotaScope};
use cog_quota::HierarchyManager;
use std::sync::{Arc, Mutex};

/// An in-memory [`KeyValueStore`] for the hierarchy suite.
///
/// Values are stored without expiry and the set and hash operations are not
/// implemented: the hierarchy manager keeps counters and JSON limit documents,
/// so those are the shapes this provides. Expiry is the driver's behaviour and
/// its tests belong with the driver.
#[derive(Default)]
struct FakeStore {
    strings: Mutex<std::collections::HashMap<String, String>>,
}

#[async_trait::async_trait]
impl KeyValueStore for FakeStore {
    async fn get(&self, key: &str) -> cog_core::SFResult<Option<String>> {
        Ok(self.strings.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> cog_core::SFResult<()> {
        self.strings
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> cog_core::SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, key: &str) -> cog_core::SFResult<()> {
        self.strings.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> cog_core::SFResult<i64> {
        Ok(-1)
    }

    async fn incr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        let mut strings = self.strings.lock().unwrap();
        let next = strings
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        strings.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> cog_core::SFResult<i64> {
        unreachable!("the hierarchy manager expires counters separately")
    }

    async fn decr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use sets")
    }

    async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use sets")
    }

    async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("the hierarchy manager does not use sets")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hget(&self, _key: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hgetall(&self, _key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
        unreachable!("the session store does not use whole-hash reads")
    }

    async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
        unreachable!("the hierarchy manager does not use hash fields")
    }
}

/// A hierarchy manager over a fresh in-memory store. Default limits are
/// soft 80 / hard 100, which the cases below are written against.
fn fresh_manager() -> HierarchyManager {
    HierarchyManager::new(Arc::new(FakeStore::default()), QuotaLimits::new(80, 100))
}

#[tokio::test]
async fn test_quota_limits_clamps_soft_above_hard() {
    let limits = QuotaLimits::new(200, 100);
    assert_eq!(limits.soft_limit, 100);
    assert_eq!(limits.hard_limit, 100);
}

#[tokio::test]
async fn test_quota_limits_from_hard_default_ratio() {
    let limits = QuotaLimits::from_hard(1000, 0.8);
    assert_eq!(limits.soft_limit, 800);
    assert_eq!(limits.hard_limit, 1000);
}

#[tokio::test]
async fn test_cascade_order_is_user_to_global() {
    let order = QuotaScope::cascade_order();
    assert_eq!(order[0], QuotaScope::User);
    assert_eq!(order[1], QuotaScope::Workspace);
    assert_eq!(order[2], QuotaScope::Team);
    assert_eq!(order[3], QuotaScope::Organization);
    assert_eq!(order[4], QuotaScope::Global);
}

#[tokio::test]
async fn test_quota_context_target_lookup() {
    let ctx = QuotaContext {
        user_id: Some("u1".into()),
        workspace_id: None,
        team_id: Some("t1".into()),
        organization_id: None,
        global_id: Some("global".into()),
    };
    assert_eq!(ctx.target(QuotaScope::User), Some("u1"));
    assert_eq!(ctx.target(QuotaScope::Workspace), None);
    assert_eq!(ctx.target(QuotaScope::Team), Some("t1"));
    assert_eq!(ctx.target(QuotaScope::Organization), None);
    assert_eq!(ctx.target(QuotaScope::Global), Some("global"));
}

#[tokio::test]
async fn test_check_allows_when_below_soft_limit() {
    let prefix = "ti1";
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some(format!("{}:user", prefix)),
        workspace_id: Some(format!("{}:ws", prefix)),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 10).await;
    assert!(decision.allowed);
    assert!(decision.warnings.is_empty());
    assert!(decision.blocked_by.is_empty());
    assert_eq!(decision.scopes.len(), 2);
}

#[tokio::test]
async fn test_check_warns_at_soft_limit() {
    let prefix = "ti2";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    // Default soft=80, hard=100. Consume 80 → next 5 would project 85 ≥ 80
    // but < 100, so we should warn but not block.
    mgr.consume(&ctx, 80).await.expect("consume ok");
    let decision = mgr.check(&ctx, 5).await;
    assert!(decision.allowed);
    assert_eq!(decision.warnings.len(), 1);
    assert!(decision.blocked_by.is_empty());
}

#[tokio::test]
async fn test_check_blocks_at_hard_limit() {
    let prefix = "ti3";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    mgr.consume(&ctx, 95).await.expect("consume ok");
    let decision = mgr.check(&ctx, 10).await;
    assert!(!decision.allowed);
    assert_eq!(decision.blocked_by.len(), 1);
    assert_eq!(decision.blocked_by[0].scope, QuotaScope::User);
}

#[tokio::test]
async fn test_set_limits_overrides_default() {
    let prefix = "ti4";
    let mgr = fresh_manager();
    let team = format!("{}:team", prefix);
    mgr.set_limits(QuotaScope::Team, &team, QuotaLimits::new(40, 50))
        .await
        .expect("set ok");
    let limits = mgr.get_limits(QuotaScope::Team, &team).await;
    assert_eq!(limits.soft_limit, 40);
    assert_eq!(limits.hard_limit, 50);
}

#[tokio::test]
async fn test_full_hierarchy_blocks_at_team() {
    let prefix = "ti5";
    let mgr = fresh_manager();
    let team = format!("{}:team", prefix);
    // Force the team to a tiny limit so it dominates.
    mgr.set_limits(QuotaScope::Team, &team, QuotaLimits::new(5, 10))
        .await
        .expect("set ok");
    let ctx = QuotaContext {
        user_id: Some(format!("{}:u", prefix)),
        workspace_id: Some(format!("{}:w", prefix)),
        team_id: Some(team.clone()),
        organization_id: Some(format!("{}:o", prefix)),
        global_id: Some(format!("{}:g", prefix)),
    };
    let d = mgr.check(&ctx, 12).await;
    assert!(!d.allowed);
    assert!(d.blocked_by.iter().any(|s| s.scope == QuotaScope::Team));
}

#[tokio::test]
async fn test_consume_increments_history() {
    let prefix = "ti6";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    mgr.consume(&ctx, 10).await.expect("consume ok");
    mgr.consume(&ctx, 25).await.expect("consume ok");
    let history = mgr
        .history(QuotaScope::User, &user, 1)
        .await
        .expect("history ok");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].tokens_used, 35);
}

#[tokio::test]
async fn test_refund_undoes_consumption() {
    let prefix = "ti7";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    mgr.consume(&ctx, 30).await.expect("consume ok");
    mgr.refund(&ctx, 20).await.expect("refund ok");
    let d = mgr.check(&ctx, 0).await;
    assert!(d.allowed);
    let user_status = d
        .scopes
        .iter()
        .find(|s| s.scope == QuotaScope::User)
        .expect("user scope present");
    assert_eq!(user_status.used_today, 10);
}

#[tokio::test]
async fn test_pre_deduct_returns_decision_when_allowed() {
    let prefix = "ti8";
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some(format!("{}:user", prefix)),
        ..Default::default()
    };
    let d = mgr.pre_deduct(&ctx, 10).await.expect("ok");
    assert!(d.allowed);
}

#[tokio::test]
async fn test_pre_deduct_returns_err_when_blocked() {
    let prefix = "ti9";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    mgr.consume(&ctx, 99).await.expect("consume ok");
    let res = mgr.pre_deduct(&ctx, 10).await;
    assert!(
        res.is_err(),
        "should error when projected exceeds hard limit"
    );
}

/// A blocked decision names the narrowest ceiling that stopped it, with the
/// figures the decision was made from and the wait until the counters reset.
///
/// Both facts are what a request-facing caller cannot derive from the decision
/// itself: the scopes are a list, and the reset is a clock fact rather than a
/// stored one. Naming the first blocking scope (cascade order) is what makes the
/// refusal point at the one allowance its owner can raise.
#[tokio::test]
async fn test_refusal_names_the_blocked_scope_and_the_reset() {
    let prefix = "ti10";
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some(format!("{}:user", prefix)),
        workspace_id: Some(format!("{}:ws", prefix)),
        ..Default::default()
    };
    mgr.consume(&ctx, 95).await.expect("consume ok");

    let decision = mgr.check(&ctx, 10).await;
    assert!(!decision.allowed);
    // Noon UTC: the pair below is the rest of the day.
    let noon = 1_600_000_000 - 1_600_000_000 % 86_400 + 12 * 3600;
    let refusal = cog_quota::refusal_for(&decision, 10, noon).expect("blocked");

    assert_eq!(
        refusal.scope,
        QuotaScope::User,
        "the narrowest blocked scope"
    );
    assert_eq!(refusal.target_id, format!("{}:user", prefix));
    assert_eq!(refusal.cap_tokens, 100);
    assert_eq!(refusal.spent_tokens, 95);
    assert_eq!(refusal.estimated_tokens, 10);
    assert_eq!(refusal.retry_after_secs, 12 * 3600);
}

/// An allowed decision has no refusal to report: `None`, not a zeroed one.
#[tokio::test]
async fn test_allowed_decision_has_no_refusal() {
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some("ti11:user".into()),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 10).await;
    assert!(decision.allowed);
    assert_eq!(cog_quota::refusal_for(&decision, 10, 0), None);
}
