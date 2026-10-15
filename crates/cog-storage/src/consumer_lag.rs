//! 广播消费者落后时，把丢掉的条数送到一个活得比日志久的读面上。
//!
//! 这个 crate 里有两个可观测性网关（Postgres 版与内存版），它们把同一条广播的
//! `Lagged` 都就地吃掉：网关的 `subscribe_events` 只能返回一条事件流，没有第二条
//! 通道可以顺手把「我漏了几条」带出去，于是那个数一直只活在调用方的错觉里——订阅
//! 方看到的是**少了的**事件，而少掉这件事本身没有任何地方记着。
//!
//! 两个实现共用这一个函数而不是各写一份：它们实现的是同一个 trait 的同一个方法，
//! 两条流不可能同时在被消费，所以**它们是同一个消费者**，也就该是同一个标签值。
//! 分两份写，读数面上就会按实现分出两条曲线，而「谁落后了」这个问题问的是消费者，
//! 不是它的后端。

use std::collections::HashMap;
use std::sync::Arc;

use cog_core::MetricsBackend;

/// `consumer` 标签值，写在 [`cog_core::metric_names::EVENT_BUS_LAGGED_TOTAL`] 上。
///
/// 名字点的是**订阅方**（谁在这条流后面追），不是产生事件的网关：同一个进程里
/// 订阅这条广播的还有别的消费者，各自有自己的落后读数。
pub(crate) const LAG_CONSUMER_OBSERVABILITY_EVENT_STREAM: &str = "observability_event_stream";

/// 记下订阅流落后广播、丢掉 `skipped` 条事件这件事。
///
/// 没有后端不是错误（嵌入式运行就没有），写失败只 `warn!`：事件已经丢了，为了
/// 记账把消费者的失败升级一次，只会换来更大的损失。
pub(crate) async fn record_consumer_lag(metrics: &Option<Arc<dyn MetricsBackend>>, skipped: u64) {
    let Some(backend) = metrics.as_ref() else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert(
        "consumer".to_string(),
        LAG_CONSUMER_OBSERVABILITY_EVENT_STREAM.to_string(),
    );
    if let Err(e) = backend
        .record_counter(
            cog_core::metric_names::EVENT_BUS_LAGGED_TOTAL,
            skipped as f64,
            labels,
        )
        .await
    {
        tracing::warn!(error = %e, "could not record the events this subscriber missed");
    }
}
