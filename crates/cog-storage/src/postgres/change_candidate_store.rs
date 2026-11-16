//! PostgreSQL-backed 候选变更队列：正本是事件流，队列是它的读数。
//!
//! 为什么不是一份自己维护的记录文件：文件把「文件在不在」当成了「变更在不在」，
//! 于是重启后一条已经判过的变更与一条刚生成的变更读起来一样；记录只有那一个写者，
//! 没有重放，也没人核过它与真相是否一致。这里写的是 `cogneva_event` 里的三种事件
//! （`change_candidate` / `change_accepted` / `change_rejected`），队列是「每个候选
//! 身份的最后一条事件」这条查询。
//!
//! 投影**没有自己的表**：它是这条查询本身，因而不可能落后于流。重建就是重放——
//! 同一段事件读多少遍都是同一行，这也正是这里不需要水位与重建命令的理由。
//!
//! 三条事件在信封里的位置：
//!
//! * `change_candidate`：`step = generated`、`outcome = ok`——候选是「生成」这一步的
//!   产物，它自己的描述、正文、评估结论全在 `attributes` 里；
//! * `change_accepted`：`step = landed`、`outcome = ok`；
//! * `change_rejected`：`step = evaluated`、`outcome = failed`——拒绝是「这一步没成」，
//!   与续跑读到的形状一致：下一次尝试从原处重来，而不是这条变更凭空消失。
//!
//! 幂等键按候选身份拼：同一候选记两次、定案两次都只留一条。这正是「同一候选不会被
//! 接受两次」在库里的写法——不是读一遍再判一次，而是让第二条写不进去。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use cog_core::{CandidateOutcome, ChangeCandidate, ChangeCandidateStore, SFError, SFResult};

const CANDIDATE_KIND: &str = "change_candidate";
const MEASURED_KIND: &str = "change_measured";
const ACCEPTED_KIND: &str = "change_accepted";
const REJECTED_KIND: &str = "change_rejected";
const SUBJECT_KIND: &str = "change";

/// 这一条变更自己的全部事件种类。投影只在它们里面找「最后一条」。
const CHANGE_KINDS: [&str; 4] = [CANDIDATE_KIND, MEASURED_KIND, ACCEPTED_KIND, REJECTED_KIND];

/// 写事件的那条 SQL。信封的每一列都写，因为默认值只覆盖「服务端时间」与「第一次尝试」
/// 这两样，其余每一列缺了都少一个能力（见迁移 117 的逐列注释）。
const INSERT_SQL: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, attributes) \
VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $9, NULL, $10, $11, $12, 1, $13) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

/// 一个身份的最后一条事件，连同它那条 `change_candidate`。
///
/// 两半分别取：候选的正文与描述只写在 `change_candidate` 上，而定案事件只说明
/// 「后来怎么了」。按身份各取最后一条再拼起来，是「这一行现在长什么样」的最小查询。
const LAST_EVENT_SQL: &str = "\
WITH latest AS (\
    SELECT DISTINCT ON (subject_id) subject_id, event_id, kind, attributes\
      FROM cogneva_event\
     WHERE kind = ANY($1)\
     ORDER BY subject_id, event_id DESC\
), origin AS (\
    SELECT DISTINCT ON (subject_id) subject_id, attributes\
      FROM cogneva_event\
     WHERE kind = $2\
     ORDER BY subject_id, event_id DESC\
) \
SELECT latest.subject_id, latest.event_id, latest.kind, latest.attributes, origin.attributes \
  FROM latest JOIN origin USING (subject_id)";

type RowSnapshot = (String, String, String, String, String);

fn to_sf(err: sqlx::Error) -> SFError {
    SFError::Database(err.to_string())
}

/// 候选队列的库端实现。
///
/// `actor` 与 `writer` 在构造时定下来：它们是归属与「值变了还是写者换了」的答案，
/// 不是每一条事件各自猜一个。`writer` 要能指名一个进程（Pod + 镜像 rev），因为
/// 幂等键的作用域就是它——两个进程用同一个键写同一条事件，第二条会被当成重复。
#[derive(Debug)]
pub struct PgChangeCandidateStore {
    pool: PgPool,
    actor: String,
    writer: String,
    scope: String,
    ids: std::sync::Mutex<cog_core::EventIdFactory>,
}

impl PgChangeCandidateStore {
    /// 造一个写者。`writer` 应当带进程身份；`ids` 是这一进程的顺序身份生成器。
    pub fn new(
        pool: PgPool,
        actor: impl Into<String>,
        writer: impl Into<String>,
        scope: impl Into<String>,
        ids: cog_core::EventIdFactory,
    ) -> Self {
        Self {
            pool,
            actor: actor.into(),
            writer: writer.into(),
            scope: scope.into(),
            ids: std::sync::Mutex::new(ids),
        }
    }

    fn next_event_id(&self) -> String {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        self.ids
            .lock()
            .expect("the id factory is only poisoned by a panic in next()")
            .next(now_ms)
    }

    async fn append(
        &self,
        artifact_id: &str,
        event: (&str, &str, &str),
        idempotency_key: &str,
        causation_id: Option<&str>,
        attributes: &str,
    ) -> SFResult<()> {
        let (kind, step, outcome) = event;
        let event_id = self.next_event_id();
        sqlx::query(INSERT_SQL)
            .bind(&event_id)
            .bind(kind)
            .bind(SUBJECT_KIND)
            .bind(artifact_id)
            .bind(artifact_id)
            .bind(step)
            .bind(outcome)
            .bind(idempotency_key)
            .bind(causation_id)
            .bind(&self.actor)
            .bind(&self.writer)
            .bind(&self.scope)
            .bind(attributes)
            .execute(&self.pool)
            .await
            .map_err(to_sf)?;
        Ok(())
    }

    async fn rows(&self, kinds: &[&str]) -> SFResult<Vec<ChangeCandidate>> {
        let kinds: Vec<String> = kinds.iter().map(|kind| (*kind).to_string()).collect();
        let rows: Vec<RowSnapshot> = sqlx::query_as(LAST_EVENT_SQL)
            .bind(&kinds)
            .bind(CANDIDATE_KIND)
            .fetch_all(&self.pool)
            .await
            .map_err(to_sf)?;
        rows.into_iter()
            .map(|(subject_id, event_id, kind, attributes, origin)| {
                candidate_from_events(&subject_id, &event_id, &kind, &attributes, &origin)
            })
            .collect()
    }
}

/// 把「一个身份的两条事件」拼成一行候选。
///
/// 读不懂的载荷不是错误也不是空行：它是「这一行说过什么，而这一版代码读不懂」——
/// 于是正文留空、描述说清楚，状态照常从事件种类读出来。丢掉整行会让一条被拒绝的
/// 变更重新出现在待办里，那比读不懂更坏。
fn candidate_from_events(
    subject_id: &str,
    event_id: &str,
    kind: &str,
    decision_attributes: &str,
    origin_attributes: &str,
) -> SFResult<ChangeCandidate> {
    let status = match kind {
        ACCEPTED_KIND => "accepted",
        REJECTED_KIND => "rejected",
        _ => "candidate",
    };

    // 读不懂的载荷不是错误，也不是空行：它是「这一行说过什么，而这一版代码读不懂」。
    // 丢掉整行会让一条被拒绝的变更重新出现在待办里——那比读不懂更坏。所以正文留空、
    // 状态标 `undecodable`，状态词照常从事件种类读出来。
    let (Ok(origin), Ok(decision)) = (
        serde_json::from_str::<serde_json::Value>(origin_attributes),
        serde_json::from_str::<serde_json::Value>(decision_attributes),
    ) else {
        return Ok(ChangeCandidate {
            artifact_id: subject_id.to_string(),
            kind: String::new(),
            description: String::new(),
            content: String::new(),
            created_at: DateTime::<Utc>::MIN_UTC,
            eval_summary: None,
            tiering: None,
            recorded_status: "undecodable".to_string(),
            status: status.to_string(),
            decision_reason: None,
            event_id: event_id.to_string(),
        });
    };
    let text = |value: &serde_json::Value, key: &str| -> Option<String> {
        value.get(key).and_then(|v| v.as_str()).map(str::to_string)
    };

    Ok(ChangeCandidate {
        artifact_id: subject_id.to_string(),
        kind: text(&origin, "kind").unwrap_or_default(),
        description: text(&origin, "description").unwrap_or_default(),
        content: text(&origin, "content").unwrap_or_default(),
        created_at: text(&origin, "created_at")
            .and_then(|at| DateTime::parse_from_rfc3339(&at).ok())
            .map(|at| at.with_timezone(&Utc))
            .unwrap_or(DateTime::<Utc>::MIN_UTC),
        eval_summary: origin
            .get("eval_summary")
            .filter(|value| !value.is_null())
            .and_then(|value| serde_json::from_value(value.clone()).ok()),
        tiering: origin
            .get("tiering")
            .filter(|value| !value.is_null())
            .map(|value| value.to_string()),
        recorded_status: text(&decision, "status")
            .or_else(|| text(&origin, "status"))
            .unwrap_or_default(),
        status: status.to_string(),
        // 候选那条事件上没有原因；定案那条才有。
        decision_reason: if status == "candidate" {
            None
        } else {
            text(&decision, "reason")
        },
        event_id: event_id.to_string(),
    })
}

#[async_trait]
impl ChangeCandidateStore for PgChangeCandidateStore {
    async fn record(&self, candidate: &ChangeCandidate) -> SFResult<()> {
        let attributes = serde_json::json!({
            "kind": candidate.kind,
            "status": candidate.recorded_status,
            "description": candidate.description,
            "content": candidate.content,
            "created_at": candidate.created_at.to_rfc3339(),
            "eval_summary": candidate.eval_summary,
            "tiering": candidate
                .tiering
                .as_deref()
                .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok()),
        })
        .to_string();
        self.append(
            &candidate.artifact_id,
            (CANDIDATE_KIND, "generated", "ok"),
            &format!("{CANDIDATE_KIND}:{}", candidate.artifact_id),
            None,
            &attributes,
        )
        .await
    }

    async fn transition(&self, artifact_id: &str, status: &str, note: &str) -> SFResult<()> {
        self.append(
            artifact_id,
            (MEASURED_KIND, "verified", "ok"),
            &format!("{MEASURED_KIND}:{artifact_id}:{status}"),
            None,
            &serde_json::json!({ "status": status, "note": note }).to_string(),
        )
        .await
    }

    async fn decide(
        &self,
        artifact_id: &str,
        outcome: CandidateOutcome,
        reason: &str,
    ) -> SFResult<()> {
        let (step, resolution) = match outcome {
            CandidateOutcome::Accepted => ("landed", "ok"),
            CandidateOutcome::Rejected => ("evaluated", "failed"),
        };
        let kind = outcome.kind();
        self.append(
            artifact_id,
            (kind, step, resolution),
            &format!("{kind}:{artifact_id}"),
            None,
            &serde_json::json!({ "reason": reason }).to_string(),
        )
        .await
    }

    async fn pending(&self) -> SFResult<Vec<ChangeCandidate>> {
        // 状态事件也算「最后一条」：读待办的人要的是这一行**此刻**的状态词，
        // 不是它生成时的那一个。
        let mut rows = self.rows(&CHANGE_KINDS).await?;
        rows.retain(ChangeCandidate::is_pending);
        rows.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        Ok(rows)
    }

    async fn all(&self) -> SFResult<Vec<ChangeCandidate>> {
        let mut rows = self.rows(&CHANGE_KINDS).await?;
        rows.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        Ok(rows)
    }

    async fn read(&self, artifact_id: &str) -> SFResult<Option<ChangeCandidate>> {
        Ok(self
            .all()
            .await?
            .into_iter()
            .find(|row| row.artifact_id == artifact_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 三种事件各读成一个状态词，且**只有定案事件带原因**——候选那条事件上没有
    /// 原因可读，把它读成空串会让「为什么被拒」看起来是「没人说」。
    #[test]
    fn each_event_kind_reads_as_its_state_word() {
        let origin = serde_json::json!({
            "kind": "code_change",
            "description": "d",
            "content": "--- a\n+++ b\n",
            "created_at": "2026-11-17T00:00:00Z",
        })
        .to_string();

        let pending = candidate_from_events("c1", "e1", "change_candidate", &origin, &origin)
            .expect("a candidate row reads");
        assert_eq!(pending.status, "candidate");
        assert!(pending.is_pending());
        assert_eq!(pending.decision_reason, None);

        let rejected = candidate_from_events(
            "c1",
            "e2",
            "change_rejected",
            &serde_json::json!({ "reason": "tests failed" }).to_string(),
            &origin,
        )
        .expect("a rejected row reads");
        assert_eq!(rejected.status, "rejected");
        assert_eq!(rejected.decision_reason.as_deref(), Some("tests failed"));
        assert_eq!(rejected.content, "--- a\n+++ b\n");
    }

    /// 载荷读不懂时，这一行仍然读得出来，并标明它读不懂。
    ///
    /// 这正是「不猜不丢」：丢掉整行会让一条被拒绝的变更重新出现在待办里，而把它读成
    /// 一条空候选会让「这一版代码读不懂」看起来像「这条变更没有内容」。
    #[test]
    fn an_undecodable_payload_keeps_its_row_and_says_so() {
        let row = candidate_from_events("c9", "e9", "change_rejected", "not json", "not json")
            .expect("an unreadable payload is not an unreadable row");
        assert_eq!(row.status, "rejected");
        assert_eq!(row.recorded_status, "undecodable");
        assert!(row.content.is_empty());
        assert_eq!(row.event_id, "e9");
    }

    /// 正文里的时间读不懂时，这一行仍然读得出来：丢掉整行会让一条被拒绝的变更
    /// 重新出现在待办里。时间为最小值，因而排在所有说出时间的行之后。
    #[test]
    fn a_row_with_an_unreadable_time_still_reads() {
        let origin = serde_json::json!({
            "kind": "code_change",
            "description": "d",
            "content": "x",
            "created_at": "not a time",
        })
        .to_string();
        let row = candidate_from_events("c1", "e1", "change_candidate", &origin, &origin)
            .expect("an unreadable time is not an unreadable row");
        assert_eq!(row.created_at, DateTime::<Utc>::MIN_UTC);
        assert_eq!(row.content, "x");
    }
}
