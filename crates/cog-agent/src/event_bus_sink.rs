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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, warn};

/// AgentEnd 的总线投递口。`Clone` 廉价（mpsc sender + 共享计数器）。
#[derive(Clone)]
pub struct EventBusSink {
    tx: mpsc::Sender<cog_core::AgentEvent>,
    dropped: Arc<AtomicUsize>,
}

impl EventBusSink {
    /// 创建投递口并启动后台发布任务。任务退出条件：所有 sender 句柄
    /// 被 drop（进程/插件关停）。
    pub fn spawn(
        publisher: Arc<dyn cog_core::EventPublisher>,
        buffer_capacity: usize,
        retry_base_delay_ms: u64,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<cog_core::AgentEvent>(buffer_capacity);
        let dropped = Arc::new(AtomicUsize::new(0));
        let sink = Self {
            tx,
            dropped: Arc::clone(&dropped),
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
                            // 永远不会到来的成功。丢的代价是真的（总线这一份
                            // 记录没有了，且装上投递口后 AgentEnd 不走 broadcast），
                            // 所以这条日志与计数是它留下的唯一读数。
                            let dropped = dropped.fetch_add(1, Ordering::Relaxed) + 1;
                            error!(
                                "Event bus refused this event permanently, dropping it rather than blocking the queue (total dropped: {dropped}): {e}"
                            );
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

    /// 非阻塞投递。缓冲满时丢弃并打 ERROR——事件仍在 WAL 与日志链路里，
    /// 丢弃只影响总线这一份持久副本。（装上投递口之后 `AgentEnd` 不再走
    /// broadcast，所以这条副本没有第二条路可以补回来。）
    pub fn send(&self, event: cog_core::AgentEvent) {
        if let Err(e) = self.tx.try_send(event) {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            error!("Event bus buffer full, dropped event (total dropped: {dropped}): {e}");
        }
    }

    /// 累计丢弃数（测试与告警探针用）。两种成因共用这一个数——缓冲排满，与
    /// 载荷被传输层永久拒绝。两者都兑现成同一件事：这条事件的总线副本没有了，
    /// 成因在各自的 ERROR 行里，不靠这个数区分。
    pub fn dropped_count(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
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

    /// 失败重试：发布器前两次失败，事件必须最终发布成功且不丢。
    #[tokio::test]
    async fn retries_until_publish_succeeds() {
        let spy = Arc::new(SpyPublisher {
            published: Mutex::new(vec![]),
            fail_times: Mutex::new(2),
        });
        let sink = EventBusSink::spawn(spy.clone(), 8, 1);
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
        let sink = EventBusSink::spawn(spy.clone(), 1, 50);
        sink.send(agent_end("a-0"));
        for _ in 0..50 {
            tokio::task::yield_now().await;
            if sink.dropped_count() > 0 {
                break;
            }
            sink.send(agent_end("a-x"));
        }
        assert!(sink.dropped_count() > 0, "overflow must be counted");
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
        let sink = EventBusSink::spawn(spy.clone(), 8, 1);
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
        assert_eq!(sink.dropped_count(), 1, "丢掉的代价要留在读数上");
    }
}
