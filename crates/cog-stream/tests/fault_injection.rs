//! 错误注入测试（审计 Phase 1 任务 1.3）：模拟 NATS / Redis 不可用，
//! 验证 StreamPlugin 按设计降级（NATS → Redis → in-memory），
//! 且 `strict_persistence=true` 时硬失败而非静默降级。

use std::sync::Arc;

use cog_core::{MessageBackend, PluginContext, SystemPlugin};

fn config(nats_urls: Vec<String>, redis_url: &str, strict: bool) -> cog_core::Config {
    let mut c = cog_core::Config::default();
    c.dag_executor.nats.urls = nats_urls;
    c.dag_executor.redis_url = redis_url.into();
    c.system.strict_persistence = strict;
    c
}

#[tokio::test]
async fn nats_and_redis_down_degrades_to_memory_backend() {
    let ctx = PluginContext::new(config(
        vec!["nats://127.0.0.1:1".into()],
        "redis://127.0.0.1:1",
        false,
    ));
    let mut plugin = cog_stream::plugin::StreamPlugin::new();
    plugin
        .init(&ctx)
        .await
        .expect("init must degrade to in-memory, not fail");

    let backend: Arc<dyn MessageBackend> = ctx
        .consume_service()
        .expect("a message backend must be published after degradation");
    backend.publish("fault.injection", b"ping").await.unwrap();
}

#[tokio::test]
async fn redis_down_without_nats_degrades_to_memory_backend() {
    let ctx = PluginContext::new(config(vec![], "redis://127.0.0.1:1", false));
    let mut plugin = cog_stream::plugin::StreamPlugin::new();
    plugin
        .init(&ctx)
        .await
        .expect("init must degrade to in-memory, not fail");

    let backend: Arc<dyn MessageBackend> = ctx
        .consume_service()
        .expect("a message backend must be published after degradation");
    backend.publish("fault.injection", b"ping").await.unwrap();
}

#[tokio::test]
async fn strict_persistence_fails_hard_when_nats_unreachable() {
    let ctx = PluginContext::new(config(
        vec!["nats://127.0.0.1:1".into()],
        "redis://127.0.0.1:1",
        true,
    ));
    let mut plugin = cog_stream::plugin::StreamPlugin::new();
    let err = plugin
        .init(&ctx)
        .await
        .expect_err("strict_persistence must turn degradation into a hard error");
    assert!(
        err.to_string().contains("strict_persistence"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn strict_persistence_fails_hard_when_redis_unreachable() {
    let ctx = PluginContext::new(config(vec![], "redis://127.0.0.1:1", true));
    let mut plugin = cog_stream::plugin::StreamPlugin::new();
    let err = plugin
        .init(&ctx)
        .await
        .expect_err("strict_persistence must turn degradation into a hard error");
    assert!(
        err.to_string().contains("strict_persistence"),
        "unexpected error: {err}"
    );
}
