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
use std::sync::{Arc, OnceLock};

use cog_core::{IntentEvents, IntentRow};
use tracing::warn;

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

/// 把一行意图写到这个进程的同一个落点。
///
/// 写者由调用方交进来（`GatewayState.intent_events`、对话那条路手里的同一份），
/// 这里不再藏一个进程级全局槽：同一份 sink 只有一个入口，漏装时 `None` 会计进
/// `unwired`，不会让某一条路安静地不写。
///
/// 空白的一句不是一次到达；写不进去也不中止调用方那一轮，但失败与未接线分开计数。
pub async fn record(sink: Option<&Arc<dyn IntentEvents>>, row: IntentRow) {
    if row.text.trim().is_empty() {
        return;
    }
    let c = counters();
    c.received.fetch_add(1, Ordering::Relaxed);
    match sink {
        Some(sink) => {
            if let Err(e) = sink.record(&row).await {
                c.failures.fetch_add(1, Ordering::Relaxed);
                warn!(
                    error = %e,
                    source = %row.source,
                    identity = %row.identity,
                    "this intent did not land; it stays only where it was said"
                );
            }
        }
        None => {
            c.unwired.fetch_add(1, Ordering::Relaxed);
            warn!(
                source = %row.source,
                identity = %row.identity,
                "no intent sink in this process; this turn stays a conversation only"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// 三个计数是进程级的：并行用例会把失败数加在别人的分母上。
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

    fn chat_row(session: &str, owner: &str, addressed_to: &str, text: &str) -> IntentRow {
        IntentRow::new(
            "chat",
            cog_core::chat_intent_identity(session, text),
            owner,
            addressed_to,
            text,
            None,
        )
    }

    /// 一行交给同一个落点；写进去的是入口给的收件人，不是空的。
    #[tokio::test]
    async fn a_row_reaches_the_one_sink_with_its_addressee() {
        let _gate = one_at_a_time();
        let recorder: Arc<dyn IntentEvents> = Arc::new(Recorder::default());
        record(
            Some(&recorder),
            chat_row("s1", "owner", "luna-1", "把这批门禁的归属改回来"),
        )
        .await;
        let rows = recorder.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source, "chat");
        assert_eq!(rows[0].said_by, "owner");
        assert_eq!(rows[0].addressed_to, "luna-1");
        assert_eq!(rows[0].text, "把这批门禁的归属改回来");
    }

    /// 空白的一句不是到达；写不进去记成失败，不让它静默变成「没有说过」。
    #[tokio::test]
    async fn an_empty_turn_is_not_an_intent_and_a_failure_is_counted() {
        let _gate = one_at_a_time();
        let recorder: Arc<dyn IntentEvents> = Arc::new(Recorder {
            rows: Mutex::default(),
            fail: true,
        });
        let before = counters().received.load(Ordering::Relaxed);
        record(Some(&recorder), chat_row("s1", "owner", "luna-1", "   ")).await;
        assert_eq!(counters().received.load(Ordering::Relaxed), before);
        let failures = counters().failures.load(Ordering::Relaxed);
        record(
            Some(&recorder),
            chat_row("s1", "owner", "luna-1", "这是一句话"),
        )
        .await;
        assert!(counters().failures.load(Ordering::Relaxed) > failures);
        assert!(recorder.rows.lock().unwrap().is_empty());
    }

    /// 没有装写者与写失败是两件事：前者是装配，后者是库。
    #[tokio::test]
    async fn an_unwired_process_counts_the_miss() {
        let _gate = one_at_a_time();
        let before = counters().unwired.load(Ordering::Relaxed);
        record(None, chat_row("s1", "owner", "luna-1", "把这件事办了")).await;
        assert!(counters().unwired.load(Ordering::Relaxed) > before);
    }
}
