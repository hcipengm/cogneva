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
    SELECT DISTINCT ON (subject_id) subject_id, event_id, kind, attributes, payload_version\
      FROM cogneva_event\
     WHERE kind = ANY($1)\
     ORDER BY subject_id, event_id DESC\
), origin AS (\
    SELECT DISTINCT ON (subject_id) subject_id, attributes, payload_version\
      FROM cogneva_event\
     WHERE kind = $2\
     ORDER BY subject_id, event_id DESC\
) \
SELECT latest.subject_id, latest.event_id, latest.kind, latest.attributes, latest.payload_version, \
       origin.attributes, origin.payload_version \
  FROM latest JOIN origin USING (subject_id)";

type RowSnapshot = (String, String, String, String, i32, String, i32);

/// 这一版代码读得懂的载荷版本，按 kind 一张表。
///
/// 表里没有的 (kind, version) 组合**不是错误**：它是「这条事件是后来的版本写的」。
/// 不猜、不丢——行照样读出来，标 `undecodable`，原文仍在库里（事件只追加，没人改它）。
/// 把读不懂的行按当前格式硬解释，得到的是一组静默的错误值；那比缺一行更坏。
const READABLE_PAYLOAD_VERSIONS: [(&str, i32); 4] = [
    (CANDIDATE_KIND, 1),
    (MEASURED_KIND, 1),
    (ACCEPTED_KIND, 1),
    (REJECTED_KIND, 1),
];

/// 这一版代码读不读得懂这一条事件的载荷。
fn payload_is_readable(kind: &str, version: i32) -> bool {
    READABLE_PAYLOAD_VERSIONS
        .iter()
        .any(|(known_kind, known_version)| *known_kind == kind && *known_version == version)
}

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
            .map(
                |(subject_id, event_id, kind, attributes, version, origin, origin_version)| {
                    candidate_from_events(&RowInput {
                        subject_id: &subject_id,
                        event_id: &event_id,
                        kind: &kind,
                        decision_attributes: &attributes,
                        decision_version: version,
                        origin_attributes: &origin,
                        origin_version,
                    })
                },
            )
            .collect()
    }
}

/// 把「一个身份的两条事件」拼成一行候选。
///
/// 读不懂的载荷不是错误也不是空行：它是「这一行说过什么，而这一版代码读不懂」——
/// 于是正文留空、描述说清楚，状态照常从事件种类读出来。丢掉整行会让一条被拒绝的
/// 变更重新出现在待办里，那比读不懂更坏。
/// 拼一行要的那几条事件字段。参数多于七个时按名字分组，读的人不必数位置。
struct RowInput<'a> {
    subject_id: &'a str,
    event_id: &'a str,
    kind: &'a str,
    decision_attributes: &'a str,
    decision_version: i32,
    origin_attributes: &'a str,
    origin_version: i32,
}

/// 一条候选写进载荷的字段。目录在 `cog_observability::event_stream::PAYLOAD_CATALOG`，
/// 由测试对账：这里多一个键或少一个键都会红——载荷存 text，没有类型能替人记住它有什么。
fn candidate_attributes(candidate: &ChangeCandidate) -> serde_json::Value {
    serde_json::json!({
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
}

/// 一条状态事件的载荷。
fn measured_attributes(status: &str, note: &str) -> serde_json::Value {
    serde_json::json!({ "status": status, "note": note })
}

/// 一条定案事件的载荷（接受与拒绝同形，只有原因）。
fn decision_attributes(reason: &str) -> serde_json::Value {
    serde_json::json!({ "reason": reason })
}

/// 读不懂的一行：状态照常从事件种类读出来，内容留空，并标明这一版读不懂。
fn undecodable_row(subject_id: &str, event_id: &str, status: &str) -> ChangeCandidate {
    ChangeCandidate {
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
    }
}

fn candidate_from_events(input: &RowInput<'_>) -> SFResult<ChangeCandidate> {
    let RowInput {
        subject_id,
        event_id,
        kind,
        decision_attributes,
        decision_version,
        origin_attributes,
        origin_version,
    } = *input;
    let status = match kind {
        ACCEPTED_KIND => "accepted",
        REJECTED_KIND => "rejected",
        _ => "candidate",
    };

    // 读不懂的载荷不是错误，也不是空行：它是「这一行说过什么，而这一版代码读不懂」。
    // 两件事都算读不懂：载荷解不出来，或者它是这一版没有解码器的版本。丢掉整行会让
    // 一条被拒绝的变更重新出现在待办里——那比读不懂更坏。所以正文留空、状态标
    // `undecodable`，状态词照常从事件种类读出来，原文仍在库里。
    let readable = payload_is_readable(CANDIDATE_KIND, origin_version)
        && payload_is_readable(kind, decision_version);
    let parsed = (
        serde_json::from_str::<serde_json::Value>(origin_attributes),
        serde_json::from_str::<serde_json::Value>(decision_attributes),
    );
    let (Ok(origin), Ok(decision)) = parsed else {
        return Ok(undecodable_row(subject_id, event_id, status));
    };
    if !readable {
        return Ok(undecodable_row(subject_id, event_id, status));
    }
    let text = |value: &serde_json::Value, key: &str| -> Option<String> {
        value.get(key).and_then(|v| v.as_str()).map(str::to_string)
    };
    // 这一版写下的载荷恒有这三个键（`PAYLOAD_CATALOG` 的测试对着它）。缺一个，
    // 说明这一行不是能按本版读的载荷：把它读成空串，「读不懂」就长成了「一条
    // 内容为空的候选」，与这张表不猜不丢的规矩相反。
    let (Some(kind), Some(description), Some(content)) = (
        text(&origin, "kind"),
        text(&origin, "description"),
        text(&origin, "content"),
    ) else {
        return Ok(undecodable_row(subject_id, event_id, status));
    };

    Ok(ChangeCandidate {
        artifact_id: subject_id.to_string(),
        kind,
        description,
        content,
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
        let attributes = candidate_attributes(candidate).to_string();
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
            &measured_attributes(status, note).to_string(),
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
            &decision_attributes(reason).to_string(),
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

        let pending = candidate_from_events(&RowInput {
            subject_id: "c1",
            event_id: "e1",
            kind: CANDIDATE_KIND,
            decision_attributes: &origin,
            decision_version: 1,
            origin_attributes: &origin,
            origin_version: 1,
        })
        .expect("a candidate row reads");
        assert_eq!(pending.status, "candidate");
        assert!(pending.is_pending());
        assert_eq!(pending.decision_reason, None);

        let decision = serde_json::json!({ "reason": "tests failed" }).to_string();
        let rejected = candidate_from_events(&RowInput {
            subject_id: "c1",
            event_id: "e2",
            kind: REJECTED_KIND,
            decision_attributes: &decision,
            decision_version: 1,
            origin_attributes: &origin,
            origin_version: 1,
        })
        .expect("a rejected row reads");
        assert_eq!(rejected.status, "rejected");
        assert_eq!(rejected.decision_reason.as_deref(), Some("tests failed"));
        assert_eq!(rejected.content, "--- a\n+++ b\n");
    }

    /// 写者写出去的键与目录逐个对得上：载荷存 text，没有类型能替人记住它有什么。
    #[test]
    fn the_writer_and_the_catalog_name_the_same_fields() {
        use cog_core::ChangeCandidate;
        let candidate = ChangeCandidate {
            artifact_id: "c1".into(),
            kind: "code_change".into(),
            description: "d".into(),
            content: "x".into(),
            created_at: Utc::now(),
            eval_summary: None,
            tiering: None,
            recorded_status: "compile_checked".into(),
            status: "candidate".into(),
            decision_reason: None,
            event_id: String::new(),
        };
        let written = |value: &serde_json::Value| -> std::collections::BTreeSet<String> {
            value
                .as_object()
                .expect("a payload is an object")
                .keys()
                .cloned()
                .collect()
        };
        let catalogue = |kind: &str| -> std::collections::BTreeSet<String> {
            cog_core::payload_fields(kind, 1)
                .unwrap_or_else(|| panic!("{kind} v1 is not catalogued"))
                .iter()
                .map(|(field, _)| (*field).to_string())
                .collect()
        };

        assert_eq!(
            written(&candidate_attributes(&candidate)),
            catalogue("change_candidate")
        );
        assert_eq!(
            written(&measured_attributes("verified", "")),
            catalogue("change_measured")
        );
        assert_eq!(
            written(&decision_attributes("why")),
            catalogue("change_accepted")
        );
        assert_eq!(
            written(&decision_attributes("why")),
            catalogue("change_rejected")
        );
    }

    /// 载荷读不懂时，这一行仍然读得出来，并标明它读不懂。
    ///
    /// 这正是「不猜不丢」：丢掉整行会让一条被拒绝的变更重新出现在待办里，而把它读成
    /// 一条空候选会让「这一版代码读不懂」看起来像「这条变更没有内容」。
    #[test]
    fn an_undecodable_payload_keeps_its_row_and_says_so() {
        let row = candidate_from_events(&RowInput {
            subject_id: "c9",
            event_id: "e9",
            kind: REJECTED_KIND,
            decision_attributes: "not json",
            decision_version: 1,
            origin_attributes: "not json",
            origin_version: 1,
        })
        .expect("an unreadable payload is not an unreadable row");
        assert_eq!(row.status, "rejected");
        assert_eq!(row.recorded_status, "undecodable");
        assert!(row.content.is_empty());
        assert_eq!(row.event_id, "e9");
    }

    /// 版本 99 的载荷：这一版没有它的解码器。行照样读出来、标 `undecodable`，
    /// 原文仍在库里——不猜、不丢。
    #[test]
    fn a_payload_from_a_later_version_is_kept_and_marked_undecodable() {
        let attributes = serde_json::json!({ "kind": "code_change", "content": "x" }).to_string();
        let row = candidate_from_events(&RowInput {
            subject_id: "c8",
            event_id: "e8",
            kind: CANDIDATE_KIND,
            decision_attributes: &attributes,
            decision_version: 99,
            origin_attributes: &attributes,
            origin_version: 99,
        })
        .expect("a later version is not an error");
        assert_eq!(row.recorded_status, "undecodable");
        assert_eq!(row.status, "candidate");
        assert!(
            row.content.is_empty(),
            "reading a version this build has no decoder for would be guessing"
        );
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
        let row = candidate_from_events(&RowInput {
            subject_id: "c1",
            event_id: "e1",
            kind: CANDIDATE_KIND,
            decision_attributes: &origin,
            decision_version: 1,
            origin_attributes: &origin,
            origin_version: 1,
        })
        .expect("an unreadable time is not an unreadable row");
        assert_eq!(row.created_at, DateTime::<Utc>::MIN_UTC);
        assert_eq!(row.content, "x");
    }

    /// 这一版写下的载荷恒有 `kind` / `description` / `content`。缺了一个，是这一行
    /// 不按本版读得懂，不是一条内容为空的候选——空串会把「读不懂」写成「没有内容」。
    #[test]
    fn a_payload_missing_a_required_field_is_undecodable_not_empty() {
        for missing in ["kind", "description", "content"] {
            let mut origin = serde_json::json!({
                "kind": "code_change",
                "description": "d",
                "content": "--- a\n+++ b\n",
                "created_at": "2026-11-17T00:00:00Z",
            });
            origin.as_object_mut().unwrap().remove(missing);
            let origin = origin.to_string();
            let row = candidate_from_events(&RowInput {
                subject_id: "c7",
                event_id: "e7",
                kind: CANDIDATE_KIND,
                decision_attributes: &origin,
                decision_version: 1,
                origin_attributes: &origin,
                origin_version: 1,
            })
            .expect("an unreadable payload is not an unreadable row");
            assert_eq!(
                row.recorded_status, "undecodable",
                "缺 {missing} 的载荷必须标成读不懂，不能读成空字段"
            );
            assert_eq!(row.status, "candidate", "状态词照样从事件种类读出来");
        }
    }
}
