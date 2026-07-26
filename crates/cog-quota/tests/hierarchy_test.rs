//! Integration tests for the 5-level quota hierarchy.
//! These tests connect to a Redis instance configured by the
//! `COGNEVA_TEST_REDIS_URL` env var (default: `redis://127.0.0.1:6379/`). Each
//! test uses a unique target ID prefix so parallel tests do not collide,
//! and cleans up the keys it creates.

use cog_core::{QuotaContext, QuotaLimits, QuotaScope};
use cog_quota::HierarchyManager;

use redis::AsyncCommands;

fn redis_url() -> String {
    std::env::var("COGNEVA_TEST_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

async fn fresh_manager(prefix: &str) -> Option<(HierarchyManager, redis::Client)> {
    let client = match redis::Client::open(redis_url()) {
        Ok(c) => c,
        Err(_) => return None,
    };
    let conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return None,
    };
    let mgr = HierarchyManager::new(conn.clone(), QuotaLimits::new(80, 100));
    cleanup(&client, prefix).await;
    Some((mgr, client))
}

async fn cleanup(client: &redis::Client, prefix: &str) {
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let keys: Vec<String> = conn
        .keys(format!("quota:*:*:{}*", prefix))
        .await
        .unwrap_or_default();
    for k in keys {
        let _: Result<(), _> = conn.del(k).await;
    }
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
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_check_warns_at_soft_limit() {
    let prefix = "ti2";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_check_blocks_at_hard_limit() {
    let prefix = "ti3";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_set_limits_overrides_default() {
    let prefix = "ti4";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
    let team = format!("{}:team", prefix);
    mgr.set_limits(QuotaScope::Team, &team, QuotaLimits::new(40, 50))
        .await
        .expect("set ok");
    let limits = mgr.get_limits(QuotaScope::Team, &team).await;
    assert_eq!(limits.soft_limit, 40);
    assert_eq!(limits.hard_limit, 50);
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_full_hierarchy_blocks_at_team() {
    let prefix = "ti5";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_consume_increments_history() {
    let prefix = "ti6";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_refund_undoes_consumption() {
    let prefix = "ti7";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_pre_deduct_returns_decision_when_allowed() {
    let prefix = "ti8";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
    let ctx = QuotaContext {
        user_id: Some(format!("{}:user", prefix)),
        ..Default::default()
    };
    let d = mgr.pre_deduct(&ctx, 10).await.expect("ok");
    assert!(d.allowed);
    cleanup(&client, prefix).await;
}

#[tokio::test]
async fn test_pre_deduct_returns_err_when_blocked() {
    let prefix = "ti9";
    let Some((mgr, client)) = fresh_manager(prefix).await else {
        eprintln!("skipping: redis not available");
        return;
    };
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
    cleanup(&client, prefix).await;
}
