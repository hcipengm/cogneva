use std::sync::Arc;
use tokio::task::JoinHandle;

/// 循环名：钩子事件转发环，报在 `cogneva_loop_*` 的 `loop` 标签上。
///
/// 前缀是本仓库已有的约定（[`crate::executor::COLLABORATION_LISTENER_LOOP`] 也叫
/// `gateway_*`）：进程里可能有同名的一族环，前缀说的是它在哪个组件里。与本文件
/// 落后时写的 `consumer` 标签值（[`crate::LAG_CONSUMER_HOOK_FORWARDER`]）只在
/// 后半段同字面——两个面说的是同一个消费者，用后缀对起来。
pub const HOOK_FORWARDER_LOOP: &str = "gateway_hook_forwarder";

/// Forwards all HookEngine events to the WebSocket broadcast channel
/// (and optionally to a downstream [`cog_core::EventPublisher`] for cross-process consumers).
/// Subscribes to the HookEngine's internal event broadcast and re-publishes
/// each hook event as an `AgentEvent::TaskStatusChange` so WebSocket clients
/// receive them.
pub struct HookToWsForwarder;

impl HookToWsForwarder {
    pub fn spawn(
        hook_engine: Arc<dyn cog_core::HookEngine>,
        event_tx: tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
        event_publisher: Option<Arc<dyn cog_core::EventPublisher>>,
        metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
        stop: cog_core::ShutdownSignal,
    ) -> JoinHandle<()> {
        // 订阅一次、跨重启共用：body 每次尝试都重建，重新订阅会落在通道当前的
        // 尾巴上，环死期间触发的钩子事件就再也补不回来——而那正是它最该补上的
        // 那一段。锁只护着 `recv()`，处理本身不持锁。
        let rx = Arc::new(tokio::sync::Mutex::new(hook_engine.subscribe()));

        // 这条转发以前是个裸 `tokio::spawn`：它退出之后读数面上什么都没有——钩子
        // 事件不再进 WebSocket，而看起来与「没有钩子触发」同形。登记之后，它的退出
        // 落进 `cogneva_loop_deaths_total`，停与不动在读数面上才分得开。
        cog_core::loop_health::spawn(
            HOOK_FORWARDER_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            stop.clone(),
            // 每次尝试都重建，所以它消费的东西在这里克隆。
            move |beat| {
                let rx = Arc::clone(&rx);
                let event_tx = event_tx.clone();
                let event_publisher = event_publisher.clone();
                let metrics = metrics.clone();
                let stop = stop.clone();
                async move {
                    // 三种结局分开写：`Lagged` 也是一个 `Err`，`while let Ok(..)` 会把广播
                    // 里一次普通的背压读成「通道关了」，于是这条转发**静静退出**——钩子事件
                    // 从此不再进 WebSocket，与「没有钩子触发」在读数面上同形。
                    loop {
                        // 每一轮都盖一次章，无论队列里有没有东西：事件驱动的环大部分
                        // 时间停在空队列上，只在有事件时才盖，报出来的是队列的安静。
                        beat.beat();
                        // 锁只在取事件这一段里：句柄跨尝试共用是为了让重启落在
                        // 原来的位置上，不是为了把处理也串进去。取到之后立刻放掉，
                        // 一次重启要等的最多只有一次 `recv()`。
                        let received = {
                            let mut rx = rx.lock().await;
                            tokio::select! {
                                biased;
                                _ = stop.wait() => return,
                                received = rx.recv() => received,
                            }
                        };
                        let hook_event = match received {
                            Ok(hook_event) => hook_event,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(
                                    skipped,
                                    "hook forwarder fell behind; it keeps forwarding"
                                );
                                crate::record_consumer_lag(
                                    &metrics,
                                    crate::LAG_CONSUMER_HOOK_FORWARDER,
                                    skipped,
                                )
                                .await;
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                        };
                        let event = cog_core::AgentEvent::TaskStatusChange {
                            task_id: hook_event
                                .task_id
                                .clone()
                                .unwrap_or_else(|| "hook".to_string()),
                            status: "hook_event".to_string(),
                            agent_id: hook_event.agent_id.clone(),
                            crew_id: hook_event.crew_id.clone(),
                            squad_id: hook_event.squad_id.clone(),
                            timestamp: chrono::Utc::now(),
                        };

                        // Publish to downstream event bus (e.g. MQ) if configured.
                        if let Some(ref publisher) = event_publisher {
                            let publisher = publisher.clone();
                            let event_clone = event.clone();
                            tokio::spawn(async move {
                                if let Err(e) = publisher.publish(&event_clone).await {
                                    tracing::warn!("Failed to publish hook AgentEvent: {}", e);
                                }
                            });
                        }

                        // Broadcast to WebSocket subscribers; ignore send errors (no receivers).
                        let _ = event_tx.send(event);
                    }
                }
            },
        )
    }
}
