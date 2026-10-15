//! The security gateway's own readings, produced by the process that has them.
//!
//! The pool readings have a durable face: the same gauge is written a second
//! time into a shared metrics store so the reading outlives the process that
//! produced it. Every one of those writes can fail -- the store refuses it, or it
//! does not answer within the bound the publishing loop allows -- and the
//! process-local reading is unaffected either way, so a gateway that has lost its
//! durable face looks exactly like one that has not: the same series, the same
//! values, the same `/metrics`. What differs is how long those readings outlive
//! the pod, which is the entire reason the durable face exists.
//!
//! So this is the reading that separates the two, at the only place that can see
//! it. It is a scrape-face reading and it dies with the pod -- a process that
//! cannot write cannot record its own death. What it answers is the question
//! asked while the pod is up: is the durable copy being written, or has this
//! process been dropping writes.
//!
//! A failure is published by reason rather than folded into one count. A store
//! that rejects a write and one that never answers have different remedies -- the
//! first is the store answering, the second may be the store never being reached
//! -- and a single total would report that something is wrong without saying
//! which of the two to go and look at.
//!
//! Both cells are published from startup, zeros included, whenever a durable face
//! exists at all: "no failed writes" is a reading, and a series that appears only
//! once the first failure lands reads as a producer that arrived late. A process
//! with no durable face publishes nothing under this name, so "there is no
//! writer" and "the writer has failed nothing" stay apart -- the first is a
//! configuration, the second is a measurement.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Durable writes that did not land, by how they failed.
///
/// The name ends in `_total` because it is cumulative since this process
/// started; the renderer reads the type from that suffix rather than from a
/// second declaration beside it.
pub const DURABLE_WRITE_FAILURES_TOTAL: &str = "cogneva_gateway_durable_write_failures_total";

/// Which way the write failed. The values are the series' identity.
const REASON_LABEL: &str = "reason";

/// How one durable write failed.
///
/// The two are kept apart because the operator's next move differs: a store
/// error is the backend's own answer (a closed connection, a missing table, a
/// rejected statement) and carries text saying which, while a timeout is the
/// absence of an answer and says only that the bound was reached. Folding them
/// into one cell would publish "something failed here" and leave the reader to
/// find out which by digging through logs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum DurableWriteFailure {
    /// The store answered, and the answer was a failure.
    StoreError = 0,
    /// The store did not answer within the bound the caller allows.
    Timeout = 1,
}

impl DurableWriteFailure {
    /// Every value, in the order the counters are indexed by. The order is
    /// checked against the label each value publishes rather than trusted.
    pub const ALL: [Self; 2] = [Self::StoreError, Self::Timeout];

    /// The value the `reason` label carries.
    pub const fn label(self) -> &'static str {
        match self {
            Self::StoreError => "store_error",
            Self::Timeout => "timeout",
        }
    }
}

/// Durable writes this process did not land, by reason.
///
/// Process-local and cumulative: it counts attempts, not episodes. One store
/// outage that spans twenty publishing ticks is twenty cells of lost durability
/// rather than one event, because that is what the store is missing -- and the
/// once-per-outage warning the caller logs stays the thing that bounds the log.
#[derive(Default)]
pub struct DurableWriteFailures {
    /// Indexed by the discriminant of [`DurableWriteFailure`], which
    /// [`DurableWriteFailure::ALL`] enumerates in the same order.
    counts: [AtomicU64; 2],
}

impl DurableWriteFailures {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one write that did not land.
    pub fn record(&self, failure: DurableWriteFailure) {
        self.counts[failure as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// What has been recorded so far, one cell per reason.
    pub fn counts(&self) -> [(DurableWriteFailure, u64); 2] {
        DurableWriteFailure::ALL.map(|f| (f, self.counts[f as usize].load(Ordering::Relaxed)))
    }
}

#[async_trait]
impl Observable for DurableWriteFailures {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        // Every cell, the zero ones included: a reason that has not failed yet is
        // a reading about this process, while an absent cell is a reading about
        // nothing at all.
        Ok(self
            .counts()
            .into_iter()
            .map(|(failure, count)| {
                RawMetric::new(DURABLE_WRITE_FAILURES_TOTAL, count as f64)
                    .with_label(REASON_LABEL, failure.label())
            })
            .collect())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// 这两格是进程级累计值，哪个维度问都是同一个数，所以不按维度分支：采集侧
    /// 只采一次，而不是在被点名的那几个维度下各回一份同样的读数。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn published(readings: &[RawMetric]) -> Vec<(String, String, f64)> {
        readings
            .iter()
            .map(|m| {
                (
                    m.name.clone(),
                    m.labels.get(REASON_LABEL).cloned().unwrap_or_default(),
                    m.value,
                )
            })
            .collect()
    }

    /// 这一族的判据是「哪个理由失败了几次」，所以两格必须各自可读，且名字与标签
    /// 从产出点取（改名会在这里红），不是测试自己抄的一份。
    #[tokio::test]
    async fn each_reason_is_a_series_of_its_own() {
        let failures = DurableWriteFailures::new();
        failures.record(DurableWriteFailure::StoreError);
        failures.record(DurableWriteFailure::StoreError);
        failures.record(DurableWriteFailure::Timeout);

        let rows = published(&failures.collect_metrics("").await.unwrap());
        assert_eq!(
            rows,
            vec![
                (
                    DURABLE_WRITE_FAILURES_TOTAL.to_string(),
                    "store_error".to_string(),
                    2.0
                ),
                (
                    DURABLE_WRITE_FAILURES_TOTAL.to_string(),
                    "timeout".to_string(),
                    1.0
                ),
            ]
        );
    }

    /// 一次都没失败过也要发布：这一族要分开的是「耐久面在写、没写砸」与「这个进程
    /// 根本没有耐久面」，而后者由产出者缺席表达，不由零表达。按需发布的版本会把
    /// 两者都渲染成同一个画面——没有这一条序列。
    #[tokio::test]
    async fn a_process_that_has_failed_nothing_still_publishes_both_cells() {
        let failures = DurableWriteFailures::new();
        let readings = failures.collect_metrics("").await.unwrap();

        assert_eq!(readings.len(), 2, "两格都要在：{:?}", published(&readings));
        assert!(
            readings
                .iter()
                .all(|m| m.name == DURABLE_WRITE_FAILURES_TOTAL && m.value == 0.0),
            "零值也要是这个族自己的序列：{:?}",
            published(&readings)
        );
    }

    /// 每个枚举值都上了标签，而且标签互不相同——两个值共用一个标签会让两格在
    /// 暴露面上合成一条序列，合计值是真的，分开读是假的。
    #[tokio::test]
    async fn every_reason_has_a_label_of_its_own() {
        let labels: Vec<&str> = DurableWriteFailure::ALL.iter().map(|f| f.label()).collect();
        let unique: BTreeSet<&str> = labels.iter().copied().collect();
        assert_eq!(labels.len(), unique.len(), "标签有重复：{labels:?}");

        let failures = DurableWriteFailures::new();
        for failure in DurableWriteFailure::ALL {
            failures.record(failure);
        }
        let rows = published(&failures.collect_metrics("").await.unwrap());
        assert_eq!(
            rows.iter()
                .map(|(_, label, _)| label.as_str())
                .collect::<BTreeSet<_>>(),
            labels.iter().copied().collect::<BTreeSet<_>>()
        );
    }
}
