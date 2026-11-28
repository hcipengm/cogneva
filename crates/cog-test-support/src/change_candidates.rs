//! 待落地队列的内存替身：夹具里的一条变更不需要一个库就能走完写入 / 定案 / 读取。
//!
//! 它实现的是**同一条契约**（`cog_core::ChangeCandidateStore`），语义与库端一致：
//!
//! * 同一个 `artifact_id` 记两次只留一条——库里靠 `(writer, idempotency_key)` 的唯一索引，
//!   这里靠它们都被拼成候选身份；
//! * 定案**不改候选本身**，而是记下「后来怎么了」；读出来的那一行由两半拼成，与库端
//!   那条 `DISTINCT ON` 查询算的是同一件事；
//! * 同一候选被定案两次只留第一次——第二次写不进去（第二条事件带着同一个幂等键）。
//!
//! 为什么它只能在夹具里：一行内存只在进程里活着，重启就没了——那正是这次要收掉的
//! 「记录活不过写它的进程」那个病。生产写者是 `cog-storage` 里的库端实现。

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use cog_core::{CandidateOutcome, ChangeCandidate, ChangeCandidateStore, SFError, SFResult};

/// 一条候选在内存里的两半：它被记下时的样子，与它被定案时的样子。
#[derive(Debug, Clone, Default)]
struct Entry {
    candidate: Option<ChangeCandidate>,
    /// 这条变更自己走到哪一步：最新那条状态事件说的那个词。
    status_note: Option<String>,
    decision: Option<(CandidateOutcome, String)>,
}

impl Entry {
    fn row(&self, artifact_id: &str) -> Option<ChangeCandidate> {
        let mut row = self.candidate.clone()?;
        row.artifact_id = artifact_id.to_string();
        if let Some(status) = &self.status_note {
            row.recorded_status = status.clone();
        }
        match &self.decision {
            Some((outcome, reason)) => {
                row.status = match outcome {
                    CandidateOutcome::Accepted => "accepted",
                    CandidateOutcome::Rejected => "rejected",
                }
                .to_string();
                row.decision_reason = Some(reason.clone());
            }
            None => {
                row.status = "candidate".to_string();
                row.decision_reason = None;
            }
        }
        Some(row)
    }
}

/// 内存里的候选队列。见模块注释。
#[derive(Debug, Default)]
pub struct MemoryCandidateStore {
    entries: Mutex<BTreeMap<String, Entry>>,
}

impl MemoryCandidateStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记下来的候选，按 `created_at` 新的在前（测试读回用）。
    pub fn recorded(&self) -> Vec<ChangeCandidate> {
        let entries = self
            .entries
            .lock()
            .expect("the candidate store was poisoned");
        Self::sorted(entries.values().filter_map(|entry| entry.candidate.clone()))
    }

    /// 一条候选被记下的时间（用于「最新的在前」）。库端按 `created_at` 排，这里同一条规则。
    fn sorted(rows: impl Iterator<Item = ChangeCandidate>) -> Vec<ChangeCandidate> {
        let mut rows: Vec<ChangeCandidate> = rows.collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        rows
    }
}

#[async_trait]
impl ChangeCandidateStore for MemoryCandidateStore {
    async fn record(&self, candidate: &ChangeCandidate) -> SFResult<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Internal("the candidate store was poisoned".into()))?;
        let entry = entries.entry(candidate.artifact_id.clone()).or_default();
        if entry.candidate.is_none() {
            let mut row = candidate.clone();
            row.status = "candidate".to_string();
            row.decision_reason = None;
            entry.candidate = Some(row);
        }
        Ok(())
    }

    async fn transition(&self, artifact_id: &str, status: &str, _note: &str) -> SFResult<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Internal("the candidate store was poisoned".into()))?;
        let entry = entries.entry(artifact_id.to_string()).or_default();
        // 最新那条状态事件是这一行此刻的状态：与库端「取最后一条事件」同一条规则。
        entry.status_note = Some(status.to_string());
        Ok(())
    }

    async fn decide(
        &self,
        artifact_id: &str,
        outcome: CandidateOutcome,
        reason: &str,
    ) -> SFResult<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Internal("the candidate store was poisoned".into()))?;
        let entry = entries.entry(artifact_id.to_string()).or_default();
        if entry.candidate.is_none() {
            return Err(SFError::Validation(format!(
                "no candidate {artifact_id} to decide"
            )));
        }
        if entry.decision.is_none() {
            entry.decision = Some((outcome, reason.to_string()));
        }
        Ok(())
    }

    async fn pending(&self) -> SFResult<Vec<ChangeCandidate>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Internal("the candidate store was poisoned".into()))?;
        Ok(Self::sorted(entries.iter().filter_map(|(id, entry)| {
            entry.row(id).filter(ChangeCandidate::is_pending)
        })))
    }

    async fn all(&self) -> SFResult<Vec<ChangeCandidate>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Internal("the candidate store was poisoned".into()))?;
        Ok(Self::sorted(
            entries.iter().filter_map(|(id, entry)| entry.row(id)),
        ))
    }

    async fn read(&self, artifact_id: &str) -> SFResult<Option<ChangeCandidate>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Internal("the candidate store was poisoned".into()))?;
        Ok(entries
            .get(artifact_id)
            .and_then(|entry| entry.row(artifact_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn candidate(id: &str, minute: u32) -> ChangeCandidate {
        ChangeCandidate {
            unit_of_work: None,
            artifact_id: id.to_string(),
            kind: "code_change".to_string(),
            description: format!("why {id}"),
            content: format!("--- a\n+++ b\n{id}\n"),
            created_at: Utc.with_ymd_and_hms(2026, 11, 17, 0, minute, 0).unwrap(),
            recorded_status: "compile_checked".to_string(),
            eval_summary: None,
            tiering: None,
            status: "candidate".to_string(),
            decision_reason: None,
            event_id: format!("event-{id}"),
        }
    }

    /// 同一条候选记两次只留一条：库端靠幂等键的唯一索引，这里靠同一个键。
    #[tokio::test]
    async fn recording_the_same_candidate_twice_leaves_one_row() {
        let store = MemoryCandidateStore::new();
        store.record(&candidate("c1", 1)).await.unwrap();
        store.record(&candidate("c1", 1)).await.unwrap();
        assert_eq!(store.pending().await.unwrap().len(), 1);
    }

    /// 定案是又一条事件：候选正文不变，状态与原因从定案那半读出来。
    #[tokio::test]
    async fn a_decision_changes_the_row_without_rewriting_the_candidate() {
        let store = MemoryCandidateStore::new();
        store.record(&candidate("c1", 1)).await.unwrap();
        store
            .decide("c1", CandidateOutcome::Rejected, "tests failed")
            .await
            .unwrap();

        assert!(store.pending().await.unwrap().is_empty());
        let row = store.read("c1").await.unwrap().expect("the row reads");
        assert_eq!(row.status, "rejected");
        assert_eq!(row.decision_reason.as_deref(), Some("tests failed"));
        assert_eq!(row.content, "--- a\n+++ b\nc1\n");
    }

    /// 同一候选不会被接受两次：第二次定案不覆盖第一次。
    #[tokio::test]
    async fn a_candidate_is_decided_once() {
        let store = MemoryCandidateStore::new();
        store.record(&candidate("c1", 1)).await.unwrap();
        store
            .decide("c1", CandidateOutcome::Rejected, "first")
            .await
            .unwrap();
        store
            .decide("c1", CandidateOutcome::Accepted, "second")
            .await
            .unwrap();
        let row = store.read("c1").await.unwrap().expect("the row reads");
        assert_eq!(row.status, "rejected");
        assert_eq!(row.decision_reason.as_deref(), Some("first"));
    }
}
