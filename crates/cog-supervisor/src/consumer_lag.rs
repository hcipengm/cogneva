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
///
/// 标识符**故意不以 `_LOOP` 结尾**：这条常量装的是一个**标签值**，不是一条环的名字，
/// 而循环门把「凡 `pub const …_LOOP: &str = "…"`」读成一条环的名字声明——它的值与本文件
/// 外那条真环（`crate::autonomous::AUTONOMOUS_EVENT_LOOP`）同字面，正是这个同字面让两处
/// 读法可以并排；标识符跟着叫 `…_LOOP` 则会让门看到「同一个名字被两条环声明」而红。
/// 值必须保持同字面，名字不能让出去。
pub const LAG_CONSUMER_AUTONOMOUS_EVENT: &str = "autonomous_event_loop";

/// 告警存储：读 supervisor 发布的 `SupervisorEvent` 广播、把值得告警的事件留在内存表里。
///
/// 它与上面两条读的**不是同一条广播**（那条是 gateway 发的事件，这条是 supervisor
/// 自己的事件），所以它落后与它们落后是两件事。
pub const LAG_CONSUMER_SUPERVISOR_ALERT_STORE: &str = "supervisor_alert_store";

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

/// 记下这一趟 drain 取走了几条、又交出几条。
///
/// 每趟写一次，写入的值是这一趟取走 / 交出的事件条数。`consumed` 那本登记表的 HELP
/// 明说按事件写的族不许混进按趟写的量（同一个名字下 `rate()` 会变成两种意思），而这条
/// drain 一趟最多取 1024 条，按事件写就是每个事件两次 SQL——一条把被测量者拖慢的读数。
/// 单位仍是事件数，所以同窗口 `taken_sum` 与 `attempted_sum` 可以直接比，也可以与存储
/// 侧的失败计数相减。
///
/// 两个 stage 是一对：`taken` 大于 `attempted` 就是「这一趟取到了、没交出去」——
/// 只看 lag 计数器，它与「这一趟没事干」同形；只看下游表的行数，它与「上游从没
/// 发过」同形。交出后有没有落库不在这里判：那一片由存储侧的
/// `cogneva_observability_event_persist_failed_total` 记账，两者相减才是进表的条数。
pub async fn record_handoff(
    metrics: &Option<Arc<dyn MetricsBackend>>,
    consumer: &str,
    taken: u64,
    attempted: u64,
) {
    let Some(backend) = metrics.as_ref() else {
        return;
    };
    for (stage, count) in [("taken", taken), ("attempted", attempted)] {
        let labels = HashMap::from([
            ("consumer".to_string(), consumer.to_string()),
            ("stage".to_string(), stage.to_string()),
        ]);
        if let Err(e) = backend
            .record_counter(
                cog_core::metric_names::EVENT_BUS_HANDOFF_TOTAL,
                count as f64,
                labels,
            )
            .await
        {
            tracing::warn!(error = %e, consumer, stage, "cannot record a drain's handoff");
        }
    }
}
