//! LLM 上游池的网关侧装配原语：故障转移路由构造与池快照写入。
//!
//! 网关是唯一持有上游凭证的进程，由它判定上游池能否服务。429/402 的逐请求
//! 故障转移逻辑在 [`cog_llm::RoutingProvider`] 内，本模块只负责两件装配工作：
//! 按配置构造那个 Provider，以及把 [`cog_core::LlmPoolStatus`] 快照带 TTL
//! 写入共享键值面（现网是 Redis）的键 [`cog_core::LLM_POOL_STATUS_KEY`]，
//! 供调度侧的 `cog_supervisor::llm_pool_guard` 读取。写的是
//! [`cog_core::KeyValueStore`] 而不是某个驱动：这条通道的读者在另一个进程里，
//! 但两个进程之间同意的是一把键和一条载荷，不是谁在实现它。
//!
//! **这里只搬字节，不判定**：`unavailable` 由 `security_gateway` 按
//! 「可服务的上游数 < 声明的下限」算出来（下限缺失时回落成 1，见
//! [`cog_core::LlmPoolStatus::min_healthy`]），而且它是**锁存**的——低于下限就置位，
//! 只有上游实证成功才解除，嫌疑窗到期不解除。所以它**不是** (池大小, 嫌疑列表) 的
//! 纯函数，本模块不该再长出一个「由嫌疑列表现算池判定」的构造函数：那样的函数
//! 会在嫌疑窗到期时自己翻回「可用」，把锁存让掉（2026-10-17 之前这里有一个
//! `pool_snapshot`，零生产调用点，算的正是那条旧规则，已删）。

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
