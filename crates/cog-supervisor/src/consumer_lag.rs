//! 广播消费者落后时，把丢掉的条数送到一个活得比日志久的读面上。
//!
//! supervisor 这一层有两条互不相干的订阅：一条读 gateway 发布的 `AgentEvent`
//! 广播（事件聚合器与自主协作环各订阅一次），一条读 supervisor 自己发布的
//! `SupervisorEvent`（告警存储）。它们的落后是四件不同的事，各自有自己的 owner，
//! 所以各有一个 `consumer` 标签值——读数面上分开，修的时候才知道该去改哪条。
//!
//! 这个函数的第二个用途是**把「落后」与「关了」分开**：本 crate 里自主环与告警
//! 存储两条循环原写成 `while let Ok(x) = rx.recv().await`，而 `Lagged` 也是一个
//! `Err`——于是广播里一次普通的背压，会被它们读成「通道关了」，**整条循环静静
//! 退出**；事件聚合器走的是 `try_recv` 的 drain，它把 `Lagged` 累加进 `stats.dropped`
//! 而没有丢过循环，缺的只是那个数的读者。丢几条事件与从此不再消费，是两种量级
//! 完全不同的故障。

use std::collections::HashMap;
use std::sync::Arc;

use cog_core::MetricsBackend;

/// 事件聚合器：读 `AgentEvent` 广播、把事件转发给可观测性网关。
pub const LAG_CONSUMER_SUPERVISOR_EVENT_AGGREGATOR: &str = "supervisor_event_aggregator";

/// 自主协作环：读同一条 `AgentEvent` 广播、驱动交接与失败恢复。
///
/// 与聚合器分开命名，因为它们是两个独立的订阅：一条落后不代表另一条落后。
pub const LAG_CONSUMER_AUTONOMOUS_EVENT_LOOP: &str = "autonomous_event_loop";

/// 记下消费者落后广播、丢掉 `skipped` 条事件这件事。
///
/// 没有后端不是错误（嵌入式运行就没有），写失败只 `warn!`：事件已经丢了，为了
/// 记账把消费者的失败升级一次，只会换来更大的损失。
pub async fn record_consumer_lag(
    metrics: &Option<Arc<dyn MetricsBackend>>,
    consumer: &str,
    skipped: u64,
) {
    let Some(backend) = metrics.as_ref() else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert("consumer".to_string(), consumer.to_string());
    if let Err(e) = backend
        .record_counter(
            cog_core::metric_names::EVENT_BUS_LAGGED_TOTAL,
            skipped as f64,
            labels,
        )
        .await
    {
        tracing::warn!(error = %e, consumer, "could not record the events this consumer missed");
    }
}
