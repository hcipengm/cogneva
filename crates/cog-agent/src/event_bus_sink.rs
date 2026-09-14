//! 事件总线发布端：把 AgentEnd 从事件产生边缘异步写入持久总线。
//!
//! 事件产生处（forward 循环）不能同步 await 总线发布——总线故障会把
//! AgentEnd 热路径卡住。这里用有界 mpsc 缓冲解耦：发布任务从缓冲取事件、
//! 失败时原地指数退避重试（队头阻塞即背压）；缓冲排满时新事件打 ERROR
//! 并丢弃计数——缓冲绝不无界增长，溢出响亮可见。

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
        let sink = Self {
            tx,
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let mut backoff_ms = retry_base_delay_ms;
                loop {
                    match publisher.publish(&event).await {
                        Ok(()) => break,
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

    /// 非阻塞投递。缓冲满时丢弃并打 ERROR——事件仍在 broadcast/WAL/日志
    /// 链路里，丢弃只影响总线这一份持久副本。
    pub fn send(&self, event: cog_core::AgentEvent) {
        if let Err(e) = self.tx.try_send(event) {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            error!("Event bus buffer full, dropped event (total dropped: {dropped}): {e}");
        }
    }

    /// 累计丢弃数（测试与告警探针用）。
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
}
