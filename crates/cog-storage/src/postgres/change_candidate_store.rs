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
use sqlx::{PgPool, Row};

use cog_core::{
    CandidateOutcome, ChangeCandidate, ChangeCandidateStore, EventStore, SFError, SFResult,
};

const CANDIDATE_KIND: &str = "change_candidate";
const MEASURED_KIND: &str = "change_measured";
const ACCEPTED_KIND: &str = "change_accepted";
const REJECTED_KIND: &str = "change_rejected";
const SUBJECT_KIND: &str = "change";

/// 这一条变更自己的全部事件种类。投影只在它们里面找「最后一条」。
fn change_kinds() -> Vec<String> {
    // 一族的词表只在一处：`change_*` 四个细词由族表给，新写的聚合步也在里面。
    cog_core::kinds_of("change")
        .unwrap_or(&[])
        .iter()
        .map(|kind| (*kind).to_string())
        .collect()
}

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

/// 同一个目标要的变更：候选事件的 `attributes->>'unit_of_work'` 等于它。
///
/// 身份不是 `scope`（那是进程级的一个字符串），也不是信封的 `unit_of_work`
/// （那一格是这条变更自己的 id）。目标写在候选自己的载荷里，所以这条读法能回答
/// 「这个目标要过哪些变更」，再按 `artifact_id` 对落地账（`landing_states`）拼出
/// 「落地没有」。
const UNIT_CANDIDATES_SQL: &str = "\
WITH latest AS (\
    SELECT DISTINCT ON (subject_id) subject_id, event_id, kind, attributes, payload_version\
      FROM cogneva_event\
     WHERE kind = ANY($1)\
     ORDER BY subject_id, event_id DESC\
), origin AS (\
    SELECT DISTINCT ON (subject_id) subject_id, attributes, payload_version\
      FROM cogneva_event\
     WHERE kind = $2 AND attributes->>'unit_of_work' = $3\
     ORDER BY subject_id, event_id DESC\
) \
SELECT latest.subject_id, latest.event_id, latest.kind, latest.attributes, latest.payload_version, \
       origin.attributes, origin.payload_version \
  FROM latest JOIN origin USING (subject_id)";

/// 一个目标要的变更，按候选身份各自最新一版读出。
pub async fn unit_candidates(
    pool: &PgPool,
    kinds: &[String],
    unit_of_work: &str,
) -> SFResult<Vec<ChangeCandidate>> {
    let rows: Vec<RowSnapshot> = sqlx::query_as(UNIT_CANDIDATES_SQL)
        .bind(kinds)
        .bind(CANDIDATE_KIND)
        .bind(unit_of_work)
        .fetch_all(pool)
        .await
        .map_err(to_sf)?;
    let mut out = Vec::with_capacity(rows.len());
    for (subject_id, event_id, kind, attributes, version, origin, origin_version) in rows {
        out.push(candidate_from_events(&RowInput {
            subject_id: &subject_id,
            event_id: &event_id,
            kind: &kind,
            decision_attributes: &attributes,
            decision_version: version,
            origin_attributes: &origin,
            origin_version,
        })?);
    }
    Ok(out)
}

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
pub struct PgChangeCandidateStore {
    pool: PgPool,
    actor: String,
    writer: String,
    scope: String,
    ids: std::sync::Mutex<cog_core::EventIdFactory>,
    /// 发布读数的句柄。可选：没有它，读不懂的行照样标 `undecodable`，只是没有那条
    /// 计数——「读不懂的越来越多了」这件事就得一条一条翻才看得见。
    ///
    /// 结构体因此不再 `derive(Debug)`：trait object 没有 `Debug`。一个带发布句柄的
    /// store 本来就打印不出什么有用的东西，而「打印不出来」不该由句柄决定。
    metrics: Option<std::sync::Arc<dyn cog_core::MetricsBackend>>,
}

/// 手写的 `Debug`：发布句柄是 trait object，没有 `Debug`，而契约要求的正是这一行。
/// 打印的是这个 store 的身份（池、写者、作用域），不打印它接没接读数面——
/// 「打印不出来」不该由句柄决定，而「接没接」是一件该由读数回答的事。
impl std::fmt::Debug for PgChangeCandidateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgChangeCandidateStore")
            .field("actor", &self.actor)
            .field("writer", &self.writer)
            .field("scope", &self.scope)
            .field("metrics", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
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
            metrics: None,
        }
    }

    /// 接上读数面。构造函数不动：一个没有读数面的部署照样能读，只是读不懂的行
    /// 不会自己报到。
    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn cog_core::MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 读不懂的行报到读数面：`kind` 与 `version` 决定该补哪个解码器。
    async fn note_undecodable(&self, kind: &str, version: i32) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        let mut labels = std::collections::HashMap::new();
        labels.insert(crate::LABEL_KIND.to_string(), kind.to_string());
        labels.insert(crate::LABEL_VERSION.to_string(), version.to_string());
        let _ = metrics
            .record_counter(cog_core::metric_names::EVENT_UNDECODABLE_TOTAL, 1.0, labels)
            .await;
    }

    fn next_event_id(&self) -> String {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        self.ids
            .lock()
            .expect("the id factory is only poisoned by a panic in next()")
            .next(now_ms)
    }

    async fn append(&self, payload: cog_core::EventPayload) -> SFResult<()> {
        let mut event = cog_core::FactEvent::new(payload);
        event.event_id = Some(self.next_event_id());
        let store = super::event_log::EventLogStore::new(self.pool.clone(), self.writer.clone());
        store.append(event).await
    }

    async fn rows(&self, kinds: &[String]) -> SFResult<Vec<ChangeCandidate>> {
        let rows: Vec<RowSnapshot> = sqlx::query_as(LAST_EVENT_SQL)
            .bind(&kinds)
            .bind(CANDIDATE_KIND)
            .fetch_all(&self.pool)
            .await
            .map_err(to_sf)?;
        let mut out = Vec::with_capacity(rows.len());
        for (subject_id, event_id, kind, attributes, version, origin, origin_version) in rows {
            let candidate = candidate_from_events(&RowInput {
                subject_id: &subject_id,
                event_id: &event_id,
                kind: &kind,
                decision_attributes: &attributes,
                decision_version: version,
                origin_attributes: &origin,
                origin_version,
            })?;
            // 读不懂的行在这一次读里就报一声：计数是「该补解码器了」的唯一信号，
            // 而它必须跟着读走——放在写入口只会数到我们写坏的，数不到别人写对的而
            // 这一版读不懂的。
            if candidate.recorded_status == "undecodable" {
                self.note_undecodable(&kind, version).await;
            }
            out.push(candidate);
        }
        Ok(out)
    }
}

/// 一个目标要的变更，连同每条变更此刻的落地状态。
#[derive(Debug, Clone)]
pub struct LandingChainRow {
    pub candidate: ChangeCandidate,
    pub landing: Option<crate::postgres::landing_states::LandingStateRow>,
}

/// 「目标 → 变更 → 落地没有」：先按目标读候选，再按变更 id 对落地账。
///
/// 两半各自已经有读法（`unit_candidates` 与 `landing_states`），这里只把它们按
/// 变更 id 拼成一条链——拼在 Rust 里而不是 SQL 里，是因为落地账的读法已经把
/// 「最后一条事实」折好了，再写一遍会多出第二份折法。
pub async fn unit_landing_chain(
    pool: &PgPool,
    kinds: &[String],
    unit_of_work: &str,
) -> SFResult<Vec<LandingChainRow>> {
    let candidates = unit_candidates(pool, kinds, unit_of_work).await?;
    let landings = crate::postgres::landing_states::landing_states(pool).await?;
    let mut by_change: std::collections::HashMap<
        &str,
        &crate::postgres::landing_states::LandingStateRow,
    > = std::collections::HashMap::new();
    for landing in &landings {
        by_change.insert(landing.change_id.as_str(), landing);
    }
    Ok(candidates
        .into_iter()
        .map(|candidate| LandingChainRow {
            landing: by_change
                .get(candidate.artifact_id.as_str())
                .map(|l| (*l).clone()),
            candidate,
        })
        .collect())
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
    // 键名只在一处：契约层的那一份，写者与读者共用。
    cog_core::candidate_attributes(candidate)
}

/// 读不懂的一行：状态照常从事件种类读出来，内容留空，并标明这一版读不懂。
fn undecodable_row(subject_id: &str, event_id: &str, status: &str) -> ChangeCandidate {
    ChangeCandidate {
        unit_of_work: None,
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
        unit_of_work: origin
            .get("unit_of_work")
            .and_then(|value| value.as_str())
            .map(str::to_string),
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
        self.append(cog_core::EventPayload::ChangeCandidate {
            candidate: candidate.clone(),
            actor: self.actor.clone(),
            scope: self.scope.clone(),
        })
        .await
    }

    async fn transition(&self, artifact_id: &str, status: &str, note: &str) -> SFResult<()> {
        self.append(cog_core::EventPayload::ChangeMeasured {
            artifact_id: artifact_id.to_string(),
            status: status.to_string(),
            note: note.to_string(),
            actor: self.actor.clone(),
            scope: self.scope.clone(),
        })
        .await
    }

    async fn decide(
        &self,
        artifact_id: &str,
        outcome: CandidateOutcome,
        reason: &str,
    ) -> SFResult<()> {
        let payload = match outcome {
            CandidateOutcome::Accepted => cog_core::EventPayload::ChangeAccepted {
                artifact_id: artifact_id.to_string(),
                reason: reason.to_string(),
                actor: self.actor.clone(),
                scope: self.scope.clone(),
            },
            CandidateOutcome::Rejected => cog_core::EventPayload::ChangeRejected {
                artifact_id: artifact_id.to_string(),
                reason: reason.to_string(),
                actor: self.actor.clone(),
                scope: self.scope.clone(),
            },
        };
        self.append(payload).await
    }

    async fn pending(&self) -> SFResult<Vec<ChangeCandidate>> {
        // 状态事件也算「最后一条」：读待办的人要的是这一行**此刻**的状态词，
        // 不是它生成时的那一个。
        let kinds = change_kinds();
        let mut rows = self.rows(&kinds).await?;
        rows.retain(ChangeCandidate::is_pending);
        rows.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        Ok(rows)
    }

    async fn all(&self) -> SFResult<Vec<ChangeCandidate>> {
        let kinds = change_kinds();
        let mut rows = self.rows(&kinds).await?;
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
mod divergence_tests {
    use super::CandidateDivergence;

    /// 两种分歧各数各的：合成一个数就分不出该把判词放宽还是收紧。
    #[test]
    fn the_two_directions_are_counted_apart() {
        let reading = CandidateDivergence {
            adopt_accepted: 2,
            adopt_rejected: 3,
            reject_accepted: 1,
            reject_rejected: 4,
            inconclusive_accepted: 0,
            inconclusive_rejected: 5,
            unjudged: 1,
        };
        assert_eq!(reading.divergent(), 4);
        assert_eq!(reading.total(), 16);
        assert_eq!(CandidateDivergence::default().divergent(), 0);
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
            unit_of_work: None,
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
        let measured = cog_core::EventPayload::ChangeMeasured {
            artifact_id: "a".into(),
            status: "verified".into(),
            note: String::new(),
            actor: "actor".into(),
            scope: "scope".into(),
        };
        let accepted = cog_core::EventPayload::ChangeAccepted {
            artifact_id: "a".into(),
            reason: "why".into(),
            actor: "actor".into(),
            scope: "scope".into(),
        };
        let rejected = cog_core::EventPayload::ChangeRejected {
            artifact_id: "a".into(),
            reason: "why".into(),
            actor: "actor".into(),
            scope: "scope".into(),
        };
        for payload in [measured, accepted, rejected] {
            let fields = payload.envelope("e1", "w");
            assert_eq!(
                written(&serde_json::from_str::<serde_json::Value>(&fields.attributes).unwrap()),
                catalogue(payload.kind()),
                "{} 的载荷键与目录不一致",
                payload.kind()
            );
        }
    }

    /// 载荷读不懂时，这一行仍然读得出来，并标明它读不懂。
    ///
    /// 这正是「不猜不丢」：丢掉整行会让一条被拒绝的变更重新出现在待办里，而把它读成
    /// 读不懂的行要报到读数面，带 `kind` 与 `version`：补哪个解码器由这两个标签答。
    #[tokio::test]
    async fn an_undecodable_row_is_counted_with_the_kind_and_version_it_could_not_read() {
        use std::sync::Arc;
        let metrics = Arc::new(crate::mem::MemoryMetricsBackend::new());
        let store = PgChangeCandidateStore::new(
            sqlx::PgPool::connect_lazy("postgres://unused/unused").expect("lazy pool"),
            "actor",
            "writer",
            "scope",
            cog_core::EventIdFactory::seeded(1, [0u8; 10]),
        )
        .with_metrics(metrics.clone());

        store.note_undecodable(REJECTED_KIND, 99).await;

        use cog_core::MetricsBackend as _;
        let series = metrics
            .query_counter_totals("cogneva_event_undecodable_total")
            .await
            .expect("the counter must be readable");
        assert_eq!(series.len(), 1, "一次读不懂只该有一条序列：{series:?}");
        assert_eq!(
            series[0].labels.get("kind").map(String::as_str),
            Some(REJECTED_KIND)
        );
        assert_eq!(
            series[0].labels.get("version").map(String::as_str),
            Some("99")
        );
        assert!(series[0].value >= 1.0, "计数没有加上去：{series:?}");
    }

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

/// 判词与门的结局分歧：评估门说该收/该拒，而最终是拒/收。
///
/// 两种读数都要：**判词说收而门拒**（判得太松或门太紧）与**判词说拒而门收**
/// （判得太紧或门太松）。合成一个数会让两种修法变成一个。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CandidateDivergence {
    pub adopt_accepted: u64,
    pub adopt_rejected: u64,
    pub reject_accepted: u64,
    pub reject_rejected: u64,
    pub inconclusive_accepted: u64,
    pub inconclusive_rejected: u64,
    pub unjudged: u64,
}

impl CandidateDivergence {
    /// 判词与结局不一致的条数。
    pub fn divergent(&self) -> u64 {
        self.adopt_rejected + self.reject_accepted
    }

    pub fn total(&self) -> u64 {
        self.adopt_accepted
            + self.adopt_rejected
            + self.reject_accepted
            + self.reject_rejected
            + self.inconclusive_accepted
            + self.inconclusive_rejected
            + self.unjudged
    }
}

/// 从流上数一遍：每个候选的判词与它最后的结局。
pub async fn candidate_divergence(pool: &PgPool) -> SFResult<CandidateDivergence> {
    let rows = sqlx::query(
        "SELECT subject_id, kind, attributes::jsonb AS attributes FROM cogneva_event \
          WHERE kind = ANY($1) ORDER BY seq",
    )
    .bind(vec![
        CANDIDATE_KIND.to_string(),
        ACCEPTED_KIND.to_string(),
        REJECTED_KIND.to_string(),
        MEASURED_KIND.to_string(),
    ])
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read candidate divergence: {e}")))?;
    let mut verdicts: std::collections::HashMap<String, Option<String>> = Default::default();
    let mut outcomes: std::collections::HashMap<String, String> = Default::default();
    for row in rows {
        let subject: String = row.get("subject_id");
        let kind: String = row.get("kind");
        let attributes: serde_json::Value = row.get("attributes");
        match kind.as_str() {
            CANDIDATE_KIND => {
                let verdict = attributes
                    .get("eval_summary")
                    .and_then(|summary| summary.get("verdict"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                verdicts.insert(subject, verdict);
            }
            ACCEPTED_KIND => {
                outcomes.insert(subject, "accepted".into());
            }
            REJECTED_KIND => {
                outcomes.insert(subject, "rejected".into());
            }
            _ => {}
        }
    }
    let mut out = CandidateDivergence::default();
    for (subject, verdict) in verdicts {
        let Some(outcome) = outcomes.get(&subject) else {
            out.unjudged += 1;
            continue;
        };
        let accepted = outcome == "accepted";
        match verdict.as_deref() {
            Some("adopt") if accepted => out.adopt_accepted += 1,
            Some("adopt") => out.adopt_rejected += 1,
            Some("reject") if accepted => out.reject_accepted += 1,
            Some("reject") => out.reject_rejected += 1,
            Some("inconclusive") if accepted => out.inconclusive_accepted += 1,
            Some("inconclusive") => out.inconclusive_rejected += 1,
            _ => out.unjudged += 1,
        }
    }
    Ok(out)
}
