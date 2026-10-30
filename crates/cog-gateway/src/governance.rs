//! The governance read face: the face roster, its current verdict, and the
//! import that seeds it.
//!
//! All three answer from the task graph. There is no governance table behind
//! these handlers -- a face is a node the task system already keeps -- so the
//! routes are a view, and a caller that wants to know who owns a face gets the
//! same row the scheduler sees.
//!
//! The verdict is recomputed per request rather than served from a stored
//! reading. A stored reading would be a claim about a moment, and a route that
//! printed it later would be reporting the past as the present; the only
//! freshness rule worth having is "this answer was computed now". What a
//! stopped loop means is answered separately, by the loop's own liveness
//! metric, so the two questions do not get one answer.

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{error::ApiError, GatewayState};

/// What `GET /api/v1/governance/faces` returns.
#[derive(Debug, Serialize)]
pub struct FacesResponse {
    pub generated_at: DateTime<Utc>,
    /// Counts by kind and by staffing, so a reader does not have to re-apply
    /// the filters and get one of them subtly wrong.
    pub census: cog_core::FaceCensus,
    pub faces: Vec<cog_core::Face>,
}

/// What `GET /api/v1/governance/drift` returns.
#[derive(Debug, Serialize)]
pub struct DriftResponse {
    pub generated_at: DateTime<Utc>,
    pub readings: Vec<cog_core::FaceDriftReading>,
}

fn store(state: &GatewayState) -> Result<&Arc<dyn cog_core::GovernanceStore>, ApiError> {
    state.governance_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable(
            "no shared database is published, so there is no face graph to read",
        )
    })
}

pub async fn faces_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<FacesResponse>, ApiError> {
    let store = store(&state)?;
    let nodes = store.face_nodes().await?;
    let faces: Vec<cog_core::Face> = nodes.iter().filter_map(cog_core::Face::from_task).collect();
    let now = Utc::now();
    Ok(Json(FacesResponse {
        generated_at: now,
        census: cog_core::census(&faces, now),
        faces,
    }))
}

pub async fn drift_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<DriftResponse>, ApiError> {
    let store = store(&state)?;
    let now = Utc::now();
    let readings = cog_core::read_drift(&**store, state.agent_registry.as_deref(), now).await?;
    Ok(Json(DriftResponse {
        generated_at: now,
        readings,
    }))
}

/// One row of an import.
///
/// It carries no revision and no timestamp: a file describes a state, and the
/// graph decides how many times that state has been moved since. Requiring the
/// caller to supply either would invite an import that overwrites a handover
/// by presenting a newer number.
#[derive(Debug, Deserialize)]
pub struct ImportFaceRequest {
    pub face_id: String,
    pub owner_unit: Option<String>,
    #[serde(flatten)]
    pub spec: cog_core::FaceSpec,
}

pub async fn import_face_handler(
    State(state): State<Arc<GatewayState>>,
    Json(request): Json<ImportFaceRequest>,
) -> Result<Json<cog_core::FaceImportOutcome>, ApiError> {
    let store = store(&state)?;
    if request.face_id.trim().is_empty() {
        return Err(ApiError::bad_request("a face must have an id"));
    }
    if request.owner_unit.is_none() && !request.spec.has_named_vacancy() {
        return Err(ApiError::bad_request(format!(
            "face {} has no owner and no complete vacancy; an empty owner without an expiry, \
             an assigner and a gap task is not a vacancy, it is a face nobody has taken",
            request.face_id
        )));
    }
    if request.owner_unit.is_some() && request.owner_unit == request.spec.deputy_unit {
        return Err(ApiError::bad_request(format!(
            "face {} names the same unit as owner and deputy; one name written twice is not a \
             second pair of eyes",
            request.face_id
        )));
    }
    let face = cog_core::Face {
        face_id: request.face_id,
        owner_unit: request.owner_unit,
        // An import describes ownership; the handover that would justify a
        // move is a separate signed row and is not written here.
        bound_by_handover_id: None,
        spec: request.spec,
        updated_at: Utc::now(),
    };
    let outcome = cog_core::import_face(&**store, &face).await?;
    Ok(Json(outcome))
}

/// What `GET /api/v1/governance/principles` returns.
///
/// Three readings, not one number: how many rows sit in each judged state, how
/// many carry each kind of gate, and which principles have actually stopped
/// something. A single "principles: N" would answer none of the questions a
/// reader comes with -- it cannot tell a corpus with no gate from one whose
/// gates never fire, and those two want opposite work.
#[derive(Debug, Serialize)]
pub struct PrinciplesResponse {
    pub generated_at: DateTime<Utc>,
    pub census: cog_core::PrincipleCensus,
    pub check_kinds: cog_core::PrincipleCheckKindCensus,
    /// The principles a gate has refused something for, most refused first.
    pub refused: Vec<RefusedPrinciple>,
    /// How many principles have been refused at all, and the sum of their
    /// counts. Both are totals over the whole set, not over the list below it:
    /// a total taken from a page is an answer to a different question than the
    /// one the label asks.
    pub refused_principles: u64,
    pub refusals_total: i64,
}

/// One principle a gate has stopped something for.
#[derive(Debug, Serialize)]
pub struct RefusedPrinciple {
    pub slug: String,
    pub lead: Option<String>,
    pub refusals: i64,
    pub last_refused_at: Option<DateTime<Utc>>,
    /// 它会在哪儿红：载体自己写下的门与判据。空着表示还没有人说过，
    /// 与「这一条没有门」是两个不同的结论，界面不能把两者画成同一格。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub where_red: Option<String>,
    /// 这条原则的载体（记忆文件）。点开一条被拒过的原则，第一件要问的是它出自哪一页。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carrier_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_gate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_gate: Option<String>,
    /// 检查档位：形状 / 树 / 现场 / 人判 / 未声明。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<String>,
    /// 判读状态。空着表示还没有一次判读落过地。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    pub priority: String,
}

/// 一行台账 → 一条被拒原则的读数。
///
/// 抽成函数只为让这几列可测：它们都是从行上原样搬过来的，而「搬的时候掉了一列」
/// 是这里唯一真会发生的错——行上有 where_red，读面没有，界面就只能给一个 slug。
fn refused_view(row: cog_core::PrincipleRow) -> RefusedPrinciple {
    RefusedPrinciple {
        slug: row.slug,
        lead: row.lead,
        refusals: row.refusals,
        last_refused_at: row.last_refused_at,
        where_red: row.where_red,
        carrier_path: row.carrier_path,
        host_gate: row.host_gate,
        platform_gate: row.platform_gate,
        check_kind: row.check_kind.map(|kind| kind.as_str().to_string()),
        state: row.state.map(|state| state.as_str().to_string()),
        priority: row.priority.as_str().to_string(),
    }
}

/// How many refused principles the route returns in the list. The totals are
/// taken before the truncation, so this bounds the list without making either
/// total a page count.
// cap: bound
const REFUSED_PAGE: usize = 50;
/// How many refused rows are read to compute the totals. The selection is
/// `refusals > 0`, so this is a bound on how many principles have ever been
/// refused by a single deployment; reading the whole set is what lets the two
/// totals describe the set rather than the page.
// cap: bound
const REFUSED_READ_CAP: usize = 10_000;

fn principles(state: &GatewayState) -> Result<&Arc<dyn cog_core::PrincipleLedgerStore>, ApiError> {
    state.principle_ledger.as_ref().ok_or_else(|| {
        ApiError::service_unavailable(
            "no shared database is published, so there is no principle ledger to read",
        )
    })
}

pub async fn principles_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<PrinciplesResponse>, ApiError> {
    let ledger = principles(&state)?;
    let now = Utc::now();
    let census = ledger.census().await?;
    let check_kinds = ledger.check_kind_census().await?;
    let mut refused: Vec<RefusedPrinciple> = ledger
        .refused(REFUSED_READ_CAP)
        .await?
        .into_iter()
        .map(refused_view)
        .collect();
    // The store selects `refusals > 0` and hands back whatever order the table
    // gives; a list ordered by nothing reads as "these are the ones", when the
    // next page would have named a different set.
    refused.sort_by(|a, b| b.refusals.cmp(&a.refusals).then(a.slug.cmp(&b.slug)));
    let refused_principles = refused.len() as u64;
    let refusals_total: i64 = refused.iter().map(|row| row.refusals).sum();
    refused.truncate(REFUSED_PAGE);

    Ok(Json(PrinciplesResponse {
        generated_at: now,
        census,
        check_kinds,
        refused_principles,
        refusals_total,
        refused,
    }))
}

/// What `GET /api/v1/governance/changes` returns.
///
/// The change account's state, and the rows it hands out next. Four readings
/// rather than one number: how many rows sit in each state, how much outstanding
/// work each priority band holds, how much intake is queued and not yet folded
/// into the account, and the head of the queue itself. A single "changes: N"
/// cannot separate a corpus whose demands are all landed from one whose demands
/// were never looked at, and those two want opposite work.
#[derive(Debug, Serialize)]
pub struct ChangesResponse {
    pub generated_at: DateTime<Utc>,
    /// Every row the account holds, whatever its state.
    pub total: u64,
    /// One cell per state, in `ChangeState::ALL` order. A state with no rows is
    /// present with zero: a missing cell and an empty state read the same to a
    /// caller that has to guess which one it was not told, and the guess it
    /// makes about a terminal state is the expensive one.
    pub census: Vec<ChangeStateCount>,
    /// Outstanding rows per band, every band present. Summed, this is the same
    /// population the non-terminal cells of `census` sum to; the two are
    /// published together so that a disagreement between them is visible
    /// instead of being resolved silently in favour of one.
    pub owed_by_band: Vec<ChangeBandCount>,
    /// Readings queued and not yet folded into the account.
    ///
    /// The one reading that says whether "the ledger was told" happened: a
    /// change handed over and not yet folded appears here and nowhere else, so
    /// zero is the only value at which the account is known to have caught up.
    pub pending_declarations: u64,
    /// Retirements queued and not yet folded, the same reading for the other
    /// intake.
    pub pending_retirements: u64,
    /// How many rows are outstanding, over the whole set rather than the list
    /// below it. `outstanding.len()` is a page length and answering this
    /// question with it would make the backlog look as small as the page.
    pub outstanding_total: u64,
    /// The rows the ledger hands out next, most urgent band first.
    pub outstanding: Vec<ChangeLedgerRowView>,
}

/// One state's row count.
#[derive(Debug, Serialize)]
pub struct ChangeStateCount {
    pub state: String,
    /// Whether a row in this state is still work. Printed rather than left for
    /// the reader to derive from the state's name: the two terminal states are
    /// exactly the ones a reader is most likely to miscount.
    pub outstanding: bool,
    pub rows: u64,
}

/// One priority band's outstanding row count.
#[derive(Debug, Serialize)]
pub struct ChangeBandCount {
    pub band: String,
    pub rows: u64,
}

/// One outstanding row, as the operator reads it.
///
/// Not the stored row verbatim: the identity is flattened into the words that
/// name it, and the columns no landed row has are left off, so a reader sees
/// what to do rather than a record of every measurement ever taken.
#[derive(Debug, Serialize)]
pub struct ChangeLedgerRowView {
    /// The flattened key, for display and for matching the repository views.
    ///
    /// Not the key to hand back: a literal routinely contains colons, so the
    /// rendered form cannot be split into its parts again. The three columns
    /// below are what a caller sends when it wants one row's history.
    pub identity: String,
    pub kind: String,
    pub scope: String,
    pub literal: String,
    pub state: String,
    pub title: Option<String>,
    pub assertion: Option<String>,
    /// The documents that ask for this change.
    pub sources: Vec<String>,
    pub first_seen_at: DateTime<Utc>,
    /// `None` while nothing has measured the row. Kept distinct from an old
    /// measurement: "nobody has looked" and "looked at a while ago" want
    /// different work, and a filled-in timestamp makes them one row.
    pub measured_at: Option<DateTime<Utc>>,
    pub measured_rev: Option<String>,
    /// The commit that delivered this change, once one has. A row can be
    /// delivered and still owed: the diff reached the line and the property it
    /// was meant to establish does not hold there. Leaving this off the read
    /// made those two rows indistinguishable from one nothing has landed.
    pub delivered_rev: Option<String>,
    /// Whether this row is standing in front of the rest of the account: it was
    /// delivered and its claim is still owed. Derived from the two facts above
    /// rather than stored, so it goes false the moment the claim holds.
    pub holds_the_line: bool,
    /// Why the row will never settle by retrying, when that is the case. Read
    /// off the row itself: an undecided row whose reason is not shown reads as
    /// one that is merely waiting.
    pub unsettled_reason: Option<String>,
    /// The band to act on, as the documents asking for this change named it.
    pub priority: String,
    /// The face this change is filed under, or `none` when the documents that
    /// ask for it name none or name different ones.
    pub block: String,
}

/// One ledger row as the changes read hands it out.
///
/// Split from the handler so the mapping has a reader that needs no database:
/// which facts a row view keeps is about the row, not about whether the store
/// answered.
fn change_row_view(row: cog_core::ChangeLedgerRow) -> ChangeLedgerRowView {
    let priority = row.priority().as_str().to_string();
    let block = row.block().as_str().to_string();
    let holds_the_line = row.holds_the_line();
    ChangeLedgerRowView {
        identity: row.identity.as_key(),
        kind: row.identity.kind.clone(),
        scope: row.identity.scope.clone(),
        literal: row.identity.literal.clone(),
        state: row.state.as_str().to_string(),
        title: row.title,
        assertion: row.assertion,
        sources: row
            .sources
            .iter()
            .map(|source| source.doc.clone())
            .collect(),
        first_seen_at: row.first_seen_at,
        measured_at: row.measured_at,
        measured_rev: row.measured_rev,
        delivered_rev: row.delivered_rev,
        holds_the_line,
        unsettled_reason: row.unsettled_reason,
        priority,
        block,
    }
}

/// How many outstanding rows the route returns. The account can hold thousands;
/// this bounds the body without bounding `outstanding_total`, which is taken
/// from the census.
// cap: bound
const OUTSTANDING_PAGE: usize = 100;

fn changes(state: &GatewayState) -> Result<&Arc<dyn cog_core::ChangeLedgerStore>, ApiError> {
    state.change_ledger.as_ref().ok_or_else(|| {
        ApiError::service_unavailable(
            "no shared database is published, so there is no change ledger to read",
        )
    })
}

pub async fn changes_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<ChangesResponse>, ApiError> {
    let ledger = changes(&state)?;
    let now = Utc::now();
    let counted: std::collections::HashMap<cog_core::ChangeState, u64> =
        ledger.census().await?.into_iter().collect();
    let by_band: std::collections::HashMap<cog_core::ChangePriority, usize> =
        ledger.owed_by_band().await?.into_iter().collect();
    let pending_declarations = ledger.pending_declarations().await?;
    let pending_retirements = ledger.pending_retirements().await?;
    let rows = ledger.outstanding(OUTSTANDING_PAGE).await?;

    let census: Vec<ChangeStateCount> = cog_core::ChangeState::ALL
        .iter()
        .map(|state| ChangeStateCount {
            state: state.as_str().to_string(),
            outstanding: state.is_outstanding(),
            rows: counted.get(state).copied().unwrap_or(0),
        })
        .collect();
    let total: u64 = census.iter().map(|cell| cell.rows).sum();
    let outstanding_total: u64 = census
        .iter()
        .filter(|cell| cell.outstanding)
        .map(|cell| cell.rows)
        .sum();
    let owed_by_band: Vec<ChangeBandCount> = cog_core::ChangePriority::ALL
        .iter()
        .map(|band| ChangeBandCount {
            band: band.as_str().to_string(),
            rows: by_band.get(band).copied().unwrap_or(0) as u64,
        })
        .collect();

    let outstanding = rows.into_iter().map(change_row_view).collect();

    Ok(Json(ChangesResponse {
        generated_at: now,
        total,
        census,
        owed_by_band,
        pending_declarations,
        pending_retirements,
        outstanding_total,
        outstanding,
    }))
}

/// What `GET /api/v1/governance/changes/events` takes.
///
/// The identity arrives in its three parts, never as the rendered key: the key
/// is a display form, and a literal containing colons makes two different
/// splits render identically. A caller that only had the key would get an empty
/// history back and read it as "this row never moved", which is the one answer
/// that is always wrong.
#[derive(Debug, Deserialize)]
pub struct ChangeTimelineQuery {
    pub kind: String,
    pub scope: String,
    pub literal: String,
    pub limit: Option<usize>,
}

/// One row's history: how it got to where it is.
#[derive(Debug, Serialize)]
pub struct ChangeTimelineResponse {
    pub identity: String,
    pub kind: String,
    pub scope: String,
    pub literal: String,
    /// Newest first, the order the store returns.
    pub events: Vec<cog_core::ChangeLedgerEvent>,
}

/// How many moves one history returns unless the caller asks for fewer. The
/// steps come newest first, so the bound drops the oldest ones -- the account
/// can be years deep and the question is "how did this get here".
// cap: bound
const TIMELINE_PAGE: usize = 50;
// cap: bound — widest history a caller may ask for in one read
const TIMELINE_MAX: usize = 200;

/// The identity a timeline request names, or the reason it names none.
///
/// Split out from the handler so the rule has a reader that does not need a
/// database: what makes a request bad is about the request, not about whether
/// the ledger answered.
fn timeline_identity(query: &ChangeTimelineQuery) -> Result<cog_core::ChangeIdentity, ApiError> {
    if query.kind.trim().is_empty() || query.scope.trim().is_empty() {
        return Err(ApiError::bad_request(
            "kind and scope are what the identity is keyed by; an empty one matches rows \
             that were never named that way",
        ));
    }
    // An empty literal is legitimate: some checks name a whole scope and no
    // literal at all, and refusing those would make their history unreadable.
    Ok(cog_core::ChangeIdentity::new(
        query.kind.as_str(),
        query.scope.as_str(),
        query.literal.as_str(),
    ))
}

fn timeline_limit(asked: Option<usize>) -> usize {
    asked.unwrap_or(TIMELINE_PAGE).clamp(1, TIMELINE_MAX)
}

pub async fn change_timeline_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<ChangeTimelineQuery>,
) -> Result<Json<ChangeTimelineResponse>, ApiError> {
    let identity = timeline_identity(&query)?;
    let ledger = changes(&state)?;
    let events = ledger
        .events(&identity, timeline_limit(query.limit))
        .await?;
    Ok(Json(ChangeTimelineResponse {
        identity: identity.as_key(),
        kind: identity.kind,
        scope: identity.scope,
        literal: identity.literal,
        events,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn row() -> cog_core::PrincipleRow {
        let at = Utc.with_ymd_and_hms(2026, 10, 25, 8, 0, 0).unwrap();
        cog_core::PrincipleRow {
            slug: "feedback_example".into(),
            lead: Some("一句原则".into()),
            carrier_path: Some("memory/feedback_example.md".into()),
            carrier_sha256: None,
            host_gate: Some("principle-gate.sh".into()),
            where_red: Some("crates/x/src/y.rs:12".into()),
            platform_gate: Some("check-before-push.sh".into()),
            check_kind: Some(cog_core::PrincipleCheckKind::Tree),
            priority: cog_core::ChangePriority::P1,
            state: Some(cog_core::PrincipleState::Enforced),
            refusals: 3,
            last_refused_at: Some(at),
            first_seen_at: at,
            measured_at: Some(at),
            measured_rev: Some("abc123".into()),
        }
    }

    /// 台账行上有「它在哪儿会红」，读面以前只交了 slug 与次数：界面上点开一条
    /// 被拒过的原则，看不出它出自哪一页、由哪扇门守、判读到什么状态。
    #[test]
    fn a_refused_principle_carries_where_it_would_go_red() {
        let view = refused_view(row());
        assert_eq!(view.slug, "feedback_example");
        assert_eq!(view.refusals, 3);
        assert_eq!(view.where_red.as_deref(), Some("crates/x/src/y.rs:12"));
        assert_eq!(
            view.carrier_path.as_deref(),
            Some("memory/feedback_example.md")
        );
        assert_eq!(view.host_gate.as_deref(), Some("principle-gate.sh"));
        assert_eq!(view.platform_gate.as_deref(), Some("check-before-push.sh"));
        assert_eq!(view.check_kind.as_deref(), Some("tree"));
        assert_eq!(view.state.as_deref(), Some("enforced"));
        assert_eq!(view.priority, "p1");
    }

    fn ledger_row() -> cog_core::ChangeLedgerRow {
        let at = Utc.with_ymd_and_hms(2026, 10, 29, 8, 0, 0).unwrap();
        cog_core::ChangeLedgerRow {
            identity: cog_core::ChangeIdentity::new(
                "grep",
                "crates/cog-gateway/src/governance.rs",
                "change_timeline_handler",
            ),
            state: cog_core::ChangeState::Pending,
            title: Some("变更面点得进去".into()),
            assertion: Some("单条变更读得回它的来路".into()),
            sources: vec![cog_core::ChangeSource {
                doc: "specs/example.md".into(),
                verdict: None,
                priority: cog_core::ChangePriority::P1,
                block: cog_core::ChangeBlock::named("change").expect("a face name"),
            }],
            first_seen_at: at,
            measured_at: Some(at),
            measured_rev: Some("abc123".into()),
            delivered_rev: Some("def456".into()),
            landed_at: None,
            unsettled_reason: Some("scope_absent_in_revision".into()),
            retire_reason: None,
            retired_at: None,
        }
    }

    /// 一行被交付了却还欠着，与一行什么都没落过，在变更列表上曾经同形：
    /// 读面只交出了 `measured_rev`，把「哪个提交把它送上了线」和「它为什么还不闭」
    /// 都丢在台账行里。看见这两格才知道该去修树还是该去修判据。
    #[test]
    fn a_row_that_was_delivered_and_is_still_owed_says_so() {
        let view = change_row_view(ledger_row());
        assert_eq!(view.delivered_rev.as_deref(), Some("def456"));
        assert!(view.holds_the_line);
        assert_eq!(
            view.unsettled_reason.as_deref(),
            Some("scope_absent_in_revision")
        );
        assert_eq!(view.priority, "p1");
        assert_eq!(view.block, "change");
        // 交付不改判据：读的是量到的那一版，不是送来那一版。
        assert_eq!(view.measured_rev.as_deref(), Some("abc123"));
    }

    /// 没有交付过的那一行，`holds_the_line` 必须是假的：扣着线的行会按 P0 领序并挡住
    /// 同一 rev 上的其它行，多判一条就会把主线停在一笔从没送上去的变更后面。
    #[test]
    fn a_row_nothing_delivered_does_not_hold_the_line() {
        let mut row = ledger_row();
        row.delivered_rev = None;
        let view = change_row_view(row);
        assert_eq!(view.delivered_rev, None);
        assert!(!view.holds_the_line);
    }

    fn query(kind: &str, scope: &str, literal: &str) -> ChangeTimelineQuery {
        ChangeTimelineQuery {
            kind: kind.into(),
            scope: scope.into(),
            literal: literal.into(),
            limit: None,
        }
    }

    /// 时间线的身份按三段给，不按渲染键反解：字面量里常有冒号，`kind:scope:literal`
    /// 这个渲染式拆不回去。拿它反解出来的会是一条永远为空的历史，而空历史读作
    /// 「这条行从来没动过」——恰恰是从来不对的那个答案。
    #[test]
    fn a_timeline_identity_is_three_parts_and_a_colon_in_the_literal_survives() {
        let identity = timeline_identity(&query(
            "grep",
            "crates/cog-gateway/src/x.rs",
            "route(\"/a:b\", get(h))",
        ))
        .expect("three parts is all a timeline needs");
        assert_eq!(identity.kind, "grep");
        assert_eq!(identity.scope, "crates/cog-gateway/src/x.rs");
        assert_eq!(identity.literal, "route(\"/a:b\", get(h))");
        // 渲染式是两个不同身份可以撞在一起的地方，这里只用来显示。
        assert_eq!(
            identity.as_key(),
            "grep:crates/cog-gateway/src/x.rs:route(\"/a:b\", get(h))"
        );
    }

    /// 一个 `absent` 式检查只点名一个范围、没有字面量；拒绝它会让这类行的历史
    /// 永远读不出来。
    #[test]
    fn an_empty_literal_is_a_real_identity_and_an_empty_kind_is_not() {
        assert!(timeline_identity(&query("absent", "deploy/k3s", "")).is_ok());
        assert!(timeline_identity(&query("", "deploy/k3s", "x")).is_err());
        assert!(timeline_identity(&query("grep", "   ", "x")).is_err());
    }

    #[test]
    fn a_timeline_page_is_bounded_at_both_ends() {
        assert_eq!(timeline_limit(None), TIMELINE_PAGE);
        assert_eq!(timeline_limit(Some(0)), 1);
        assert_eq!(timeline_limit(Some(10_000)), TIMELINE_MAX);
    }
}
