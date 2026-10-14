//! LLM 上游池的网关侧装配原语：故障转移路由构造与池快照写入。
//!
//! 网关是唯一持有上游凭证的进程，由它判定上游池能否服务。429/402 的逐请求
//! 故障转移逻辑在 [`cog_llm::RoutingProvider`] 内，本模块只负责两件装配工作：
//! 按配置构造那个 Provider，以及把 [`cog_core::LlmPoolStatus`] 快照带 TTL
//! 写入共享键值面（现网是 Redis）的键 [`cog_core::LLM_POOL_STATUS_KEY`]，
//! 供调度侧的 `cog_supervisor::llm_pool_guard` 读取。写的是
//! [`cog_core::KeyValueStore`] 而不是某个驱动：这条通道的读者在另一个进程里，
//! 但两个进程之间同意的是一把键和一条载荷，不是谁在实现它。

use std::sync::Arc;

use cog_llm::RoutingProvider;

/// 按已按优先级排好的后端列表（索引 0 为主）构造多后端故障转移 Provider。
///
/// `RoutingProvider` 每次请求都从主后端开始，主上游恢复后自然切回。
pub fn build_routing_provider(
    backends: Vec<Arc<dyn cog_core::LlmClient>>,
    max_attempts: u32,
    retry_on_429: bool,
    retry_on_402: bool,
) -> Arc<RoutingProvider> {
    Arc::new(RoutingProvider::new(
        backends,
        max_attempts,
        retry_on_429,
        retry_on_402,
    ))
}

/// 用 `llm_routing` 配置段里的故障转移开关构造路由 Provider。
pub fn routing_provider_from_config(
    backends: Vec<Arc<dyn cog_core::LlmClient>>,
    config: &cog_llm::LLMRoutingConfig,
) -> Arc<RoutingProvider> {
    build_routing_provider(
        backends,
        config.max_failover_attempts,
        config.retry_on_429,
        config.retry_on_402,
    )
}

/// 由当前嫌疑上游列表汇总池级快照。
///
/// 池大小为 0 时不产出“全灭”结论：未配置上游与全部上游故障不是一回事，
/// 读这个快照的消费者靠它决定要不要暂停工作。其余字段是各上游的实证
/// 输入，由探测侧补齐，这里不替它们说话。
pub fn pool_snapshot(
    pool_size: usize,
    unavailable_upstreams: Vec<String>,
) -> cog_core::LlmPoolStatus {
    let unavailable = pool_size > 0 && unavailable_upstreams.len() >= pool_size;
    cog_core::LlmPoolStatus {
        unavailable,
        unavailable_upstreams,
        pool_size,
        ..Default::default()
    }
}

/// 把池快照序列化后带 TTL 写入共享键值面，返回是否送达。
///
/// 判定向上、向下两个方向必须由同一个写者落同一条载荷，所以这里只做
/// “写 `status` 给定的值”这一件事，不参与判定。TTL 至少 1 秒：写入进程
/// 崩溃后，陈旧的“全灭”结论会自己过期，不会把调度器永久楔死在暂停态。
pub async fn write_pool_status(
    store: &dyn cog_core::KeyValueStore,
    status: &cog_core::LlmPoolStatus,
    ttl_secs: u64,
) -> bool {
    let payload = match serde_json::to_string(status) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "池状态序列化失败，跳过跨进程发布");
            return false;
        }
    };
    match store
        .set_ex(cog_core::LLM_POOL_STATUS_KEY, &payload, ttl_secs.max(1))
        .await
    {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(error = %e, "池状态写入共享键值面失败");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_pool_is_not_marked_unavailable() {
        let status = pool_snapshot(0, Vec::new());
        assert!(!status.unavailable);
        assert_eq!(status.pool_size, 0);
    }

    #[test]
    fn all_suspect_upstreams_mark_the_pool_down() {
        let status = pool_snapshot(2, vec!["a|m".into(), "b|m".into()]);
        assert!(status.unavailable);
        assert_eq!(status.unavailable_upstreams.len(), 2);
    }

    #[test]
    fn one_healthy_upstream_keeps_the_pool_up() {
        let status = pool_snapshot(2, vec!["a|m".into()]);
        assert!(!status.unavailable);
    }

    #[test]
    fn an_empty_backend_list_still_builds() {
        let _ = routing_provider_from_config(
            Vec::new(),
            &cog_llm::LLMRoutingConfig {
                max_failover_attempts: 3,
                retry_on_429: true,
                retry_on_402: true,
                ..Default::default()
            },
        );
    }
}
