//! 错误注入测试（审计 Phase 1 任务 1.3）：模拟 PostgreSQL / Redis
//! 不可用，验证 StoragePlugin 的降级与硬失败边界：
//! - strict_persistence=true：PostgreSQL 不可达 → init 硬失败；
//! - 非 strict：PostgreSQL 降级为内存后端继续，但 Redis 连接是
//!   硬依赖（AgentRegistry），不可达时 init 失败。

use cog_core::{PluginContext, SystemPlugin};

fn config(pg_url: &str, redis_url: &str, strict: bool) -> cog_core::Config {
    let mut c = cog_core::Config::default();
    c.providers
        .pg
        .options
        .insert("connection".into(), serde_json::json!(pg_url));
    c.dag_executor.redis_url = redis_url.into();
    c.system.strict_persistence = strict;
    c.system.pg_acquire_timeout_secs = 1;
    c.system.pg_min_connections = 1;
    c.system.pg_max_connections = 2;
    // 测试环境不落盘 raw log。
    c.raw_logger.enabled = false;
    c
}

#[tokio::test]
async fn strict_persistence_fails_hard_when_postgres_unreachable() {
    let ctx = PluginContext::new(config(
        "postgres://127.0.0.1:1/cogneva",
        "redis://127.0.0.1:1",
        true,
    ));
    let mut plugin = cog_storage::plugin::StoragePlugin::new();
    let err = plugin
        .init(&ctx)
        .await
        .expect_err("strict_persistence must turn PostgreSQL failure into a hard error");
    assert!(
        err.to_string().contains("strict_persistence"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn non_strict_postgres_degrades_but_redis_is_hard_dependency() {
    let ctx = PluginContext::new(config(
        "postgres://127.0.0.1:1/cogneva",
        "redis://127.0.0.1:1",
        false,
    ));
    let mut plugin = cog_storage::plugin::StoragePlugin::new();
    let err = plugin
        .init(&ctx)
        .await
        .expect_err("Redis is a hard dependency; init must fail when it is unreachable");
    assert!(err.to_string().contains("Redis"), "unexpected error: {err}");
}
