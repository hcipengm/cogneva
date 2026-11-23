//! 对话作为意图入口：进来的一句先落成一条意图，再去回答。
//!
//! 在此之前，属主在对话里说的话只有一处落点——对话记录。记录回答「这里有过一段
//! 对话」，不回答「这里有一件要办的事」，而两者在库里长得一样：一句「把这批门禁
//! 的归属改回来」与一句「今天几号」都是同样的一行。于是需要有人去做的那句话，只要
//! 属主不再提起，就再没有任何一方会去做它。
//!
//! 所以这一句进门时先落到事件流上（`intent_received`，原文不改写、一句话一行），
//! 由把意图变成待办的那条循环接手（`intent_triaged`）。落不进去也不让这一轮对话
//! 失败——它已经在上游答了一半——但不许静默：失败进计数，计数进 `/metrics`。
//!
//! 接线做成进程级单例而不是 `GatewayState` 的一个槽：那个结构每加一格，生产装配与
//! 五个夹具都要跟着改一遍，而漏掉的那一处不会报错，它会让入口安静地不写。接线在
//! 网关插件里发生一次。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use chrono::Utc;
use cog_core::{IntentEvents, IntentRow};
use tracing::warn;

/// 装好的那个写者。`None` 是「这个进程没接上意图落点」，与「接上了但一句没说」不同。
fn sink_slot() -> &'static RwLock<Option<Arc<dyn IntentEvents>>> {
    static SLOT: OnceLock<RwLock<Option<Arc<dyn IntentEvents>>>> = OnceLock::new();
    SLOT.get_or_init(|| RwLock::new(None))
}

/// 接线：网关插件从存储插件手里拿到写者时调一次。
pub fn install(sink: Arc<dyn IntentEvents>) {
    if let Ok(mut slot) = sink_slot().write() {
        *slot = Some(sink);
    }
}

/// 这台进程的写者，没接上时 `None`。
pub fn sink() -> Option<Arc<dyn IntentEvents>> {
    sink_slot().read().ok().and_then(|s| s.clone())
}

/// 只给测试用：装上/摘下写者，让同一进程里的用例互不污染。
#[doc(hidden)]
pub fn install_for_test(sink: Option<Arc<dyn IntentEvents>>) {
    if let Ok(mut slot) = sink_slot().write() {
        *slot = sink;
    }
}

/// 三个计数。
///
/// `received` 是被要求落下的句数，`failures` 是其中没落成的，`unwired` 是根本没有
/// 写者的。三个分开是因为修法不同：`unwired` 去查装配，`failures` 去查库，
/// `received` 只是分母——没有它，「失败 3 次」说不出是千分之三还是全部。
#[derive(Debug, Default)]
pub struct IntentWrites {
    pub received: AtomicU64,
    pub failures: AtomicU64,
    pub unwired: AtomicU64,
}

pub fn counters() -> &'static IntentWrites {
    static C: OnceLock<IntentWrites> = OnceLock::new();
    C.get_or_init(IntentWrites::default)
}

pub const RECEIVED_TOTAL: &str = "cogneva_intent_received_total";
pub const FAILURES_TOTAL: &str = "cogneva_intent_receive_failures_total";
pub const UNWIRED_TOTAL: &str = "cogneva_intent_unwired_total";

/// 那一组计数的 Prometheus 文本。
pub fn readings() -> Vec<cog_core::observability::RawMetric> {
    let c = counters();
    vec![
        cog_core::observability::RawMetric::new(
            RECEIVED_TOTAL,
            c.received.load(Ordering::Relaxed) as f64,
        ),
        cog_core::observability::RawMetric::new(
            FAILURES_TOTAL,
            c.failures.load(Ordering::Relaxed) as f64,
        ),
        cog_core::observability::RawMetric::new(
            UNWIRED_TOTAL,
            c.unwired.load(Ordering::Relaxed) as f64,
        ),
    ]
}

/// 一句话的身份：会话 ＋ 原文的稳定摘要。
///
/// 后缀取自原文而不是序号或时刻：同一句重投要落在同一行，否则重连一次、重试一次
/// 就在事件流里多出一条「属主又说了一遍」。不取时刻的代价是：同一段会话里隔很久
/// 一字不差地说两遍，只会有一行——那一行仍然在，仍然会被接手，只是第二遍不另算。
pub fn intent_identity(session_id: &str, text: &str) -> String {
    format!("chat:{session_id}:{:016x}", fnv1a(text))
}

/// FNV-1a：只要求稳定与散得开，不要求抗碰撞——它是行身份的后缀，不是凭证。
fn fnv1a(bytes: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// 记下这一句。调用方在回声与回答之前调它，**不**因为它失败而中止这一轮。
///
/// 空白的一句不是一次到达：没有内容可办，记下来只会让接手那一侧每次都要判一次
/// 「这是空的」。发出去的那个 `/命令` 是界面动作，不是一句要说的话。
pub async fn record_turn(session_id: &str, said_by: &str, addressed_to: &str, text: &str) {
    if text.trim().is_empty() {
        return;
    }
    let c = counters();
    c.received.fetch_add(1, Ordering::Relaxed);
    let row = IntentRow {
        identity: intent_identity(session_id, text),
        source: "chat".into(),
        said_by: said_by.to_string(),
        addressed_to: addressed_to.to_string(),
        text: text.to_string(),
        parent: None,
        at: Utc::now(),
    };
    match sink() {
        Some(sink) => {
            if let Err(e) = sink.record(&row).await {
                c.failures.fetch_add(1, Ordering::Relaxed);
                warn!(
                    error = %e,
                    identity = %row.identity,
                    "chat: this turn did not land as an intent; it stays a conversation only"
                );
            }
        }
        None => {
            c.unwired.fetch_add(1, Ordering::Relaxed);
            warn!(
                identity = %row.identity,
                "chat: no intent sink in this process; this turn stays a conversation only"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// 写者与三个计数都是进程级的：并行跑的用例会互相装上/摘下写者、把失败数加在
    /// 别人的分母上。用例一个接一个地跑，同一条线程上的装配才说得清是谁装上的。
    fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
        static GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        GATE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[derive(Default)]
    struct Recorder {
        rows: Mutex<Vec<IntentRow>>,
        fail: bool,
    }

    #[async_trait]
    impl IntentEvents for Recorder {
        fn writer(&self) -> &str {
            "test"
        }
        async fn record(&self, row: &IntentRow) -> cog_core::SFResult<()> {
            if self.fail {
                return Err(cog_core::SFError::Database("boom".into()));
            }
            self.rows.lock().unwrap().push(row.clone());
            Ok(())
        }
        async fn mark_triaged(
            &self,
            _identities: &[String],
            _task_id: &str,
            _actor: &str,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// 回答落在「已经记下」之后：这一轮先成为一条意图，再去回答它。
    #[tokio::test]
    async fn a_turn_is_recorded_before_it_is_answered() {
        let _gate = one_at_a_time();
        let recorder = Arc::new(Recorder::default());
        install_for_test(Some(recorder.clone()));
        record_turn("s1", "owner", "", "把这批门禁的归属改回来").await;
        install_for_test(None);
        let rows = recorder.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text, "把这批门禁的归属改回来");
        assert_eq!(rows[0].source, "chat");
        assert_eq!(rows[0].said_by, "owner");
    }

    /// 同一句重投是同一行：身份只取会话与原文。
    #[test]
    fn the_same_sentence_in_the_same_session_has_one_identity() {
        let _gate = one_at_a_time();
        assert_eq!(
            intent_identity("s1", "把这批门禁的归属改回来"),
            intent_identity("s1", "把这批门禁的归属改回来")
        );
        assert_ne!(
            intent_identity("s1", "把这批门禁的归属改回来"),
            intent_identity("s2", "把这批门禁的归属改回来")
        );
        assert_ne!(
            intent_identity("s1", "把这批门禁的归属改回来"),
            intent_identity("s1", "把门禁的归属改回来")
        );
    }

    /// 空白的一句不是到达；写不进去记成失败，不让它静默变成「没有说过」。
    #[tokio::test]
    async fn an_empty_turn_is_not_an_intent_and_a_failure_is_counted() {
        let _gate = one_at_a_time();
        let recorder = Arc::new(Recorder {
            rows: Mutex::default(),
            fail: true,
        });
        install_for_test(Some(recorder.clone()));
        let before = counters().received.load(Ordering::Relaxed);
        record_turn("s1", "owner", "", "   ").await;
        assert_eq!(counters().received.load(Ordering::Relaxed), before);
        let failures = counters().failures.load(Ordering::Relaxed);
        record_turn("s1", "owner", "", "这是一句话").await;
        install_for_test(None);
        assert!(counters().failures.load(Ordering::Relaxed) > failures);
        assert!(recorder.rows.lock().unwrap().is_empty());
    }
}
