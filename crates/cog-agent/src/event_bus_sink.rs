//! 事件总线发布端：把 AgentEnd 从事件产生边缘异步写入持久总线。
//!
//! 事件产生处（forward 循环）不能同步 await 总线发布——总线故障会把
//! AgentEnd 热路径卡住。这里用有界 mpsc 缓冲解耦：发布任务从缓冲取事件、
//! 失败时原地指数退避重试（队头阻塞即背压）；缓冲排满时新事件打 ERROR
//! 并丢弃计数——缓冲绝不无界增长，溢出响亮可见。
//!
//! 重试分两类，因为「等一等会好」与「再等也一样」不是同一件事：瞬时故障
//! （总线挂了、超时）原地退避重试，队头阻塞在这里是**背压**；永久拒绝
//! （载荷超过传输层的字节上限）**当次消费掉**——它不会因为等待而变小，
//! 而把队头交给它，代价是后面每一条都永远轮不到，且发布口只有这一个。
//!
//! 两种丢弃都落在同一个计数器上、按 `cause` 分开，因为「缓冲忙了一下」与
//! 「这条对话没了」不是同一件事，共用一个数就分不出来。装上投递口之后
//! `AgentEnd` 只走总线、agent 侧既没接 WAL 也没接 raw 日志，所以这条计数
//! 是丢掉的那份**唯一**耐久副本留下的读数。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, warn};

use cog_core::MetricsBackend;

/// 丢弃的成因词表。闭集，判定点在这个文件里：计数写在这里，标签也写在这里，
/// 调用方不得自造标签值（自造会让「读那一格」与「写那一格」对不上）。
pub mod cause {
    /// 入口的**有界缓冲**排满了：发布任务正在退避重试一个会自愈的故障，这一拍
    /// 收不下，事件在进队前被丢。总线恢复后不再产生——容量读数，不是故障读数。
    pub const BUFFER_FULL: &str = "buffer_full";
    /// 传输层**永久拒绝**了这条载荷（字节数超过它声明的帧上限）。再等多久也
    /// 不会变小，投递口把它当次消费掉。不可自愈——丢掉的是这条对话的唯一副本。
    pub const PAYLOAD_REJECTED: &str = "payload_rejected";
}

/// 两个成因各自的累计丢弃数。分开存，因为合起来那个数答不了「是忙了一下还是
/// 丢了一条对话」。指标计数与这里的原子量同源，同一个事件只加一次。
#[derive(Default)]
struct DropCounts {
    buffer_full: AtomicUsize,
    payload_rejected: AtomicUsize,
}

/// AgentEnd 的总线投递口。`Clone` 廉价（mpsc sender + 共享计数器 + 句柄）。
#[derive(Clone)]
pub struct EventBusSink {
    tx: mpsc::Sender<cog_core::AgentEvent>,
    drops: Arc<DropCounts>,
    /// 丢弃读数的落地处。嵌入式 / 测试装配里没有指标后端时为 `None`，此时丢弃
    /// 只剩 ERROR 行——这正是没有后端时的行为，不是一条被吞掉的读数。
    metrics: Option<Arc<dyn MetricsBackend>>,
    /// 记录指标要 await，而「缓冲排满」是在**同步**的投递入口发现的。借一个
    /// 运行期句柄把那一笔读数交出去：句柄取自 `spawn`（那里本来就在运行期里，
    /// 否则它自己那句 `tokio::spawn` 就失败了）。
    handle: tokio::runtime::Handle,
}

impl EventBusSink {
    /// 创建投递口并启动后台发布任务。任务退出条件：所有 sender 句柄
    /// 被 drop（进程/插件关停）。
    pub fn spawn(
        publisher: Arc<dyn cog_core::EventPublisher>,
        buffer_capacity: usize,
        retry_base_delay_ms: u64,
        metrics: Option<Arc<dyn MetricsBackend>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<cog_core::AgentEvent>(buffer_capacity);
        let drops = Arc::new(DropCounts::default());
        let sink = Self {
            tx,
            drops: Arc::clone(&drops),
            metrics: metrics.clone(),
            handle: tokio::runtime::Handle::current(),
        };
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let mut backoff_ms = retry_base_delay_ms;
                loop {
                    match publisher.publish(&event).await {
                        Ok(()) => break,
                        Err(e) if e.is_permanent_rejection() => {
                            // 这条再发多少次也还是这么大。丢掉它、继续下一条：
                            // 留着它只是把队头占住，而它身后的每一条都在等一个
                            // 永远不会到来的成功。丢的代价是真的——装上投递口后
                            // AgentEnd 不走 broadcast，也没有第二条耐久面——所以
                            // 这个计数是它留下的唯一读数。
                            let n = drops.payload_rejected.fetch_add(1, Ordering::Relaxed) + 1;
                            error!(
                                "Event bus refused this event permanently, dropping it rather than blocking the queue (total dropped: {n}): {e}"
                            );
                            record_drop(&metrics, cause::PAYLOAD_REJECTED).await;
                            break;
                        }
                        Err(e) => {
                            warn!("Event bus publish failed: {e}; retrying in {backoff_ms}ms");
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            // 退避上限 60s：总线长时间故障时事件在缓冲里堆积，
                            // 重试频率降下来了，缓冲溢出由 send 侧 ERROR 报告。
                            backoff_ms = backoff_ms.saturating_mul(2).min(60_000);
                        }
                    }
                }
            }
        });
        sink
    }

    /// 非阻塞投递。缓冲满时丢弃并打 ERROR，并按 `buffer_full` 记一笔读数。
    ///
    /// 丢掉的这一份就是**没有**的那一份：装上投递口之后 `AgentEnd` 不再走
    /// broadcast，而 agent 侧的 `wal` 与 `raw_logger` 都是 `Option`，生产装配里
    /// 两者都没接（`Agent::with_wal` 没有生产调用方）⇒ 总线这一份是 `AgentEnd`
    /// **唯一**的耐久副本，丢了没有第二条路能补回来。
    pub fn send(&self, event: cog_core::AgentEvent) {
        if let Err(e) = self.tx.try_send(event) {
            let n = self.drops.buffer_full.fetch_add(1, Ordering::Relaxed) + 1;
            error!("Event bus buffer full, dropped event (total dropped: {n}): {e}");
            // 同步入口不能 await：把这一笔读数交给运行期。指标写失败只留一行
            // warn（事件已经丢了，计数写不成不能再搭上别的）。
            if let Some(metrics) = self.metrics.clone() {
                self.handle
                    .spawn(async move { record_drop(&Some(metrics), cause::BUFFER_FULL).await });
            }
        }
    }

    /// 入口缓冲排满丢掉的条数（测试用）。
    pub fn buffer_full_drops(&self) -> usize {
        self.drops.buffer_full.load(Ordering::Relaxed)
    }

    /// 被传输层永久拒绝、当次消费掉的条数（测试用）。它记的是**真的丢了**的
    /// 对话数，与入口缓冲那格不能相加：两者的成因、修复、代价都不同。
    pub fn payload_rejected_drops(&self) -> usize {
        self.drops.payload_rejected.load(Ordering::Relaxed)
    }
}

/// 把一笔丢弃记到闭集计数器上，成因走 `cause` 标签。
///
/// 没有后端时什么都不做（嵌入式 / 测试）。后端有但写失败时只打一行 warn：
/// 事件已经丢了，这条读数写不进去是又一次损失，不能让它反过来影响投递路径。
async fn record_drop(metrics: &Option<Arc<dyn MetricsBackend>>, cause: &'static str) {
    let Some(metrics) = metrics.as_ref() else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert("cause".to_string(), cause.to_string());
    if let Err(e) = metrics
        .record_counter(cog_core::metric_names::EVENT_BUS_DROPPED_TOTAL, 1.0, labels)
        .await
    {
        warn!(error = %e, "event bus sink: could not record the dropped event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 记录每次发布的间谍发布器；可注入失败观察重试。
    struct SpyPublisher {
        published: Mutex<Vec<String>>,
        fail_times: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl cog_core::EventPublisher for SpyPublisher {
        async fn publish(&self, event: &cog_core::AgentEvent) -> cog_core::SFResult<()> {
            let mut fails = self.fail_times.lock().unwrap();
            if *fails > 0 {
                *fails -= 1;
                return Err(cog_core::SFError::Agent("injected failure".into()));
            }
            let agent_id = match event {
                cog_core::AgentEvent::AgentEnd { agent_id, .. } => agent_id.clone(),
                _ => "other".into(),
            };
            self.published.lock().unwrap().push(agent_id);
            Ok(())
        }
    }

    /// 对某个指定 `agent_id` **永远**回永久拒绝的发布器。用来钉住「永久错
    /// 当次消费」：它自己不会被重试磨掉，只能被丢掉。
    struct PermanentRejectionPublisher {
        rejected_id: String,
        published: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl cog_core::EventPublisher for PermanentRejectionPublisher {
        async fn publish(&self, event: &cog_core::AgentEvent) -> cog_core::SFResult<()> {
            let agent_id = match event {
                cog_core::AgentEvent::AgentEnd { agent_id, .. } => agent_id.clone(),
                _ => "other".into(),
            };
            if agent_id == self.rejected_id {
                return Err(cog_core::SFError::PayloadTooLarge {
                    size: 2_000_000,
                    limit: Some(1_048_576),
                });
            }
            self.published.lock().unwrap().push(agent_id);
            Ok(())
        }
    }

    fn agent_end(agent_id: &str) -> cog_core::AgentEvent {
        cog_core::AgentEvent::AgentEnd {
            agent_id: agent_id.into(),
            messages: vec![],
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        }
    }

    /// 记下每一次计数的后端：断言的读数是「哪条序列、带什么标签」，不是内部原子。
    #[derive(Default)]
    struct RecordingMetrics(Mutex<Vec<(cog_core::MetricName, HashMap<String, String>)>>);

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for RecordingMetrics {
        async fn record_gauge(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            _value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.0.lock().unwrap().push((name, labels));
            Ok(())
        }
        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// 记录里的成因标签集合，去掉序列名（那由 `EVENT_BUS_DROPPED_TOTAL` 断言）。
    fn recorded_causes(metrics: &RecordingMetrics) -> Vec<String> {
        metrics
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(_, labels)| labels.get("cause").cloned().unwrap_or_default())
            .collect()
    }

    /// 失败重试：发布器前两次失败，事件必须最终发布成功且不丢。
    #[tokio::test]
    async fn retries_until_publish_succeeds() {
        let spy = Arc::new(SpyPublisher {
            published: Mutex::new(vec![]),
            fail_times: Mutex::new(2),
        });
        let sink = EventBusSink::spawn(spy.clone(), 8, 1, None);
        sink.send(agent_end("a-retry"));

        for _ in 0..100 {
            if !spy.published.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("event must eventually publish after transient failures");
    }

    /// 有界缓冲溢出：发布器被第一次失败卡住（退避中），容量 1 的缓冲灌
    /// 第二条时只能进一条，其余丢弃且计数器可查。
    #[tokio::test]
    async fn overflow_drops_loudly_with_counter() {
        let spy = Arc::new(SpyPublisher {
            published: Mutex::new(vec![]),
            fail_times: Mutex::new(1),
        });
        // 发布任务处理第一条时先撞失败退避，缓冲随即被后续事件占满。
        let sink = EventBusSink::spawn(spy.clone(), 1, 50, None);
        sink.send(agent_end("a-0"));
        for _ in 0..50 {
            tokio::task::yield_now().await;
            if sink.buffer_full_drops() > 0 {
                break;
            }
            sink.send(agent_end("a-x"));
        }
        assert!(sink.buffer_full_drops() > 0, "overflow must be counted");
    }

    /// 永久拒绝当次消费：被拒的那一条丢掉，它**身后**的每一条都必须照常发出。
    ///
    /// 这是发布任务只此一个的必然要求：把队头交给一个重试不会改变结果的失败，
    /// 代价不是丢一条，而是它后面全部永远轮不到。所以断言分成两半——后一条
    /// 必须发出去（队列没被堵），被拒的那条必须计入丢弃（代价没被悄悄咽掉）。
    #[tokio::test]
    async fn a_permanently_rejected_event_does_not_block_the_queue() {
        let spy = Arc::new(PermanentRejectionPublisher {
            rejected_id: "too-big".into(),
            published: Mutex::new(vec![]),
        });
        let sink = EventBusSink::spawn(spy.clone(), 8, 1, None);
        sink.send(agent_end("first"));
        sink.send(agent_end("too-big"));
        sink.send(agent_end("last"));

        for _ in 0..100 {
            if spy.published.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(
            *spy.published.lock().unwrap(),
            vec!["first".to_string(), "last".to_string()],
            "被永久拒绝的那条必须被跨过去，而不是把它身后的都堵在后面"
        );
        assert_eq!(sink.payload_rejected_drops(), 1, "丢掉的代价要留在读数上");
    }

    /// 永久拒绝这一笔要落到闭集计数器上、标签是 `payload_rejected`，且**不是**
    /// 入口缓冲那一格：两种成因共用一个数就分不出「忙了一下」与「对话没了」。
    #[tokio::test]
    async fn a_permanent_rejection_is_recorded_under_its_own_cause() {
        let spy = Arc::new(PermanentRejectionPublisher {
            rejected_id: "too-big".into(),
            published: Mutex::new(vec![]),
        });
        let metrics = Arc::new(RecordingMetrics::default());
        let sink = EventBusSink::spawn(spy.clone(), 8, 1, Some(metrics.clone()));

        sink.send(agent_end("too-big"));

        for _ in 0..100 {
            if !metrics.0.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let recorded = metrics.0.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1, "一笔丢弃对应一笔读数");
        assert_eq!(
            recorded[0].0.as_str(),
            "cogneva_event_bus_dropped_total",
            "读数要落在闭集里那条序列上"
        );
        assert_eq!(
            recorded[0].1.get("cause").map(String::as_str),
            Some("payload_rejected"),
            "成因必须在标签上"
        );
        assert_eq!(sink.buffer_full_drops(), 0, "两者不能混进对方那一格");
    }

    /// 入口缓冲排满这一笔落在**另一个**成因格上。与上一条一起钉住「分得出」：
    /// 两条断言各自的 cause 值不同，把两格合成一个数就会有一条红。
    #[tokio::test]
    async fn a_buffer_overflow_is_recorded_under_its_own_cause() {
        let spy = Arc::new(SpyPublisher {
            published: Mutex::new(vec![]),
            fail_times: Mutex::new(1),
        });
        let metrics = Arc::new(RecordingMetrics::default());
        let sink = EventBusSink::spawn(spy.clone(), 1, 50, Some(metrics.clone()));
        sink.send(agent_end("a-0"));
        for _ in 0..50 {
            tokio::task::yield_now().await;
            if sink.buffer_full_drops() > 0 {
                break;
            }
            sink.send(agent_end("a-x"));
        }

        for _ in 0..100 {
            if !metrics.0.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let causes = recorded_causes(&metrics);
        assert!(
            !causes.is_empty() && causes.iter().all(|c| c.as_str() == cause::BUFFER_FULL),
            "入口溢出只该记 buffer_full，实际记了 {causes:?}"
        );
        assert_eq!(sink.payload_rejected_drops(), 0, "它不碰另一格");
    }
}
