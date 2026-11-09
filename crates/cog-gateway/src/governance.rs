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
// cap: bound — 一页拒绝记录的条数，界的是单次响应的体积
const REFUSED_PAGE: usize = 50;
/// How many refused rows are read to compute the totals. The selection is
/// `refusals > 0`, so this is a bound on how many principles have ever been
/// refused by a single deployment; reading the whole set is what lets the two
/// totals describe the set rather than the page.
// cap: bound — 算拒绝总数最多读多少行；读到这里就按已读部分外推，不把整表读进内存
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
    /// Why the per-row cost column is empty, when it is.
    ///
    /// Null means every row below carries a measured figure. A string means
    /// none of them does, and names the reason: a page of nulls with nothing
    /// said about why reads as a fleet of changes that all cost nothing, which
    /// is the one conclusion the column must never invite.
    pub cost_unreadable: Option<String>,
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
    /// What this one change cost, over every handover its identity fixes.
    ///
    /// `None` when the column could not be measured for this page at all; the
    /// response's `cost_unreadable` says why. `Some(0)` is a real answer -- a
    /// change nothing ever picked up -- and the two are kept apart for the same
    /// reason `spend_by_task` keeps them apart.
    pub cost: Option<u64>,
}

/// One ledger row as the changes read hands it out.
///
/// Split from the handler so the mapping has a reader that needs no database:
/// which facts a row view keeps is about the row, not about whether the store
/// answered.
fn change_row_view(row: cog_core::ChangeLedgerRow, cost: Option<u64>) -> ChangeLedgerRowView {
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
        cost,
    }
}

/// What each row of one page cost, keyed by identity, or the reason the whole
/// column could not be measured.
///
/// One ledger read for the page rather than one per row: the rows are known at
/// once, the ids are fixed by their identities, and a per-row read would put a
/// hundred round trips behind a single screen.
///
/// Nothing here invents a zero. A row whose ids this control plane cannot name,
/// a page with no ledger behind it, and a failed read all produce `None` and a
/// reason; only an answered read produces figures.
async fn page_costs(
    ledger: Option<&Arc<dyn cog_core::LlmUsageLedger>>,
    orchestrator: &Arc<dyn cog_core::OrchestratorControl>,
    rows: &[cog_core::ChangeLedgerRow],
) -> Result<std::collections::BTreeMap<String, u64>, String> {
    let mut per_row: Vec<(String, Vec<String>)> = Vec::with_capacity(rows.len());
    for row in rows {
        let ids = orchestrator
            .change_dispatch_ids(&row.identity)
            .ok_or_else(|| "this control plane does not dispatch changes".to_string())?;
        per_row.push((row.identity.as_key(), ids));
    }
    let Some(ledger) = ledger else {
        return Err("this process holds no token ledger".into());
    };
    let all: Vec<String> = per_row.iter().flat_map(|(_, ids)| ids.clone()).collect();
    let spent: std::collections::BTreeMap<String, u64> =
        ledger.spend_by_task(&all).await?.into_iter().collect();
    Ok(per_row
        .into_iter()
        .map(|(key, ids)| {
            let total = ids
                .iter()
                .map(|id| spent.get(id).copied().unwrap_or(0))
                .sum();
            (key, total)
        })
        .collect())
}

/// How many outstanding rows the route returns. The account can hold thousands;
/// this bounds the body without bounding `outstanding_total`, which is taken
/// from the census.
// cap: bound — 一页未结事项的条数，与拒绝页同量级，界的是单次响应体积
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

    let costs = page_costs(state.llm_usage.as_ref(), &state.orchestrator, &rows).await;
    let (cost_of, cost_unreadable) = match costs {
        Ok(by_identity) => (Some(by_identity), None),
        Err(reason) => {
            tracing::warn!(%reason, "governance: per-change cost is not measurable on this page");
            (None, Some(reason))
        }
    };
    let outstanding = rows
        .into_iter()
        .map(|row| {
            let cost = cost_of.as_ref().map(|by_identity| {
                by_identity
                    .get(&row.identity.as_key())
                    .copied()
                    .unwrap_or(0)
            });
            change_row_view(row, cost)
        })
        .collect();

    Ok(Json(ChangesResponse {
        generated_at: now,
        total,
        census,
        owed_by_band,
        pending_declarations,
        pending_retirements,
        cost_unreadable,
        outstanding_total,
        outstanding,
    }))
}

/// What `GET /api/v1/governance/changes/funnel` takes.
#[derive(Debug, Deserialize)]
pub struct ChangeFunnelQuery {
    /// How many UTC days back to read. The store answers with the days it has
    /// and never pads a missing one with zeroes, so a caller that asks for more
    /// than the account has lived gets a shorter series rather than a series
    /// whose first half claims nothing happened.
    #[serde(default = "default_funnel_days")]
    pub days: u32,
}

/// A fortnight: long enough to see a week's shape twice, short enough that the
/// query stays inside the event table's hot window.
fn default_funnel_days() -> u32 {
    14
}

/// The attempt-to-landing ladder, one row per UTC day.
///
/// Separate from `changes_handler` because the two answer questions that must
/// not be confused: that one says what the account *holds*, this one says what
/// it *did*. A cumulative census cannot yield the second -- a day on which
/// nothing landed and a day on which a thousand did leave the same mark on it.
#[derive(Debug, Serialize)]
pub struct ChangeFunnelResponse {
    pub days: Vec<FunnelDayView>,
    /// What [`Self::days`] adds up to. Computed here, not in the page: the unit
    /// cost is a ratio over a denominator, and a page that picks the
    /// denominator for itself can pick one that makes the number say something
    /// the days never said.
    pub summary: FunnelSummary,
}

/// The returned days, summed -- the figures the page shows once above the table.
///
/// **Two unit costs, not one.** The numerator is the whole deployment's spend:
/// every caller's calls, not only the ones that moved a change. There are two
/// defensible denominators for it and they answer different questions:
///
/// - over *every* closure the ledger closed: what one closed item costs. Most
///   of those closures were asks the corpus already satisfied -- they shipped
///   nothing and cost almost nothing -- so this figure is the cheapest reading
///   of the same spend.
/// - over the closures that carried a delivered revision: what one shipped
///   change costs. This is the figure the account's own target is stated in.
///
/// The gap between them is exactly [`Self::rev_coverage`], and printing only
/// one of them without it is what makes the panel misread: a reader who takes
/// the per-closure figure for the per-shipped-change figure is off by that
/// ratio, and a reader who takes the other one without seeing the coverage
/// cannot tell whether it is large because changes are expensive or because
/// almost nothing shipped. So both are carried, and the coverage that relates
/// them is carried with them.
/// Why a window's money could not be stated.
///
/// Three causes, three sentences: a page that collapses them into one prints
/// "no price recorded" over a day whose usage rows were never read, which reads
/// as a measurement rather than as a gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CostUnstated {
    /// At least one day's usage rows could not be read. Nothing about the
    /// window's spend is known, including whether any was.
    UsageUnread,
    /// Every day was read, but at least one day's priced calls span more than
    /// one currency. A sum over two currencies is not a sum, and this service
    /// holds no exchange rate.
    MixedCurrency,
    /// Every day was read and none is mixed, but no call in the window carried
    /// a price. The total is zero and there is no unit to state it in.
    NothingPriced,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FunnelSummary {
    pub attempted: u64,
    pub rounds: u64,
    pub passed: u64,
    /// Every row the ledger closed in the window. Not output: see
    /// [`cog_core::ChangeFunnelDay::landed`].
    pub landed: u64,
    /// Closures that carried a delivered revision -- the ones that shipped.
    pub landed_with_rev: u64,
    /// `landed_with_rev / landed`. `null` when nothing landed: a ratio whose
    /// denominator is zero does not exist, and printing it as zero would read
    /// as "nothing shipped" on a window where nothing closed at all.
    pub rev_coverage: Option<f64>,
    /// `null` when any returned day could not read the usage table, or when
    /// there are no days. A sum that quietly skips a day reads as "that day
    /// cost nothing", which is the one thing an unmeasured day must not say.
    pub llm_calls: Option<u64>,
    /// What the window's priced calls cost, in [`Self::cost_currency`]'s
    /// millionths. `null` when some day could not be read or some day's priced
    /// calls span two currencies; [`Self::cost_unstated`] says which.
    ///
    /// A day with no calls, or with calls that were all served unpriced, is
    /// **not** a hole here: it adds zero to the priced total, and the floor it
    /// hides is [`Self::unpriced_calls`]'s to state.
    pub cost_micros: Option<i64>,
    /// The single currency [`Self::cost_micros`] is in, as the price book
    /// names it. Never picked by the page.
    pub cost_currency: Option<String>,
    /// Why [`Self::cost_micros`] is `null`, when it is. `null` itself is the
    /// only field a page cannot restate: "no money figure" has three causes and
    /// they call for three different sentences, so the cause travels with the
    /// absence instead of being re-derived from the days by every reader.
    pub cost_unstated: Option<CostUnstated>,
    /// Calls served with no price recorded, over the same days. Any of them
    /// makes every money figure here a floor rather than a total.
    pub unpriced_calls: Option<u64>,
    /// `llm_calls / landed`: what one closed item costs to buy. Left as a
    /// fraction rather than rounded here -- a fraction of a call per closure is
    /// the useful form on a window with many landings, and a page that wants a
    /// whole number can round it where it prints.
    pub calls_per_landing: Option<f64>,
    /// `llm_calls / landed_with_rev`: what one shipped change costs to buy.
    pub calls_per_shipped_change: Option<f64>,
    /// `cost_micros / landed`, in the currency's millionths.
    pub cost_per_landing_micros: Option<i64>,
    /// `cost_micros / landed_with_rev`, in the currency's millionths. This is
    /// the denominator the account's own target is stated over.
    pub cost_per_shipped_change_micros: Option<i64>,
}

impl FunnelSummary {
    /// Sums exactly the days the route returns -- so the summary and the table
    /// under it can never be about different windows.
    pub fn of(days: &[FunnelDayView]) -> Self {
        let sum_u64 = |f: fn(&FunnelDayView) -> u64| days.iter().map(f).sum::<u64>();
        let attempted = sum_u64(|d| d.attempted);
        let rounds = sum_u64(|d| d.rounds);
        let passed = sum_u64(|d| d.passed);
        let landed = sum_u64(|d| d.landed);
        let landed_with_rev = sum_u64(|d| d.landed_with_rev);

        // Every column that comes from the usage table is all-or-nothing over
        // the window: one unread day and the sum is not the window's total.
        // An empty window is a measured zero, not an unread one: every day it
        // has -- none -- was read. `all` on an empty slice is exactly that.
        let calls_read = days.iter().all(|d| d.llm_calls.is_some());
        let llm_calls = calls_read.then(|| days.iter().filter_map(|d| d.llm_calls).sum::<u64>());

        let unpriced_read = days.iter().all(|d| d.unpriced_calls.is_some());
        let unpriced_calls =
            unpriced_read.then(|| days.iter().filter_map(|d| d.unpriced_calls).sum::<u64>());

        // A day whose money cannot be stated makes the window's money a
        // non-number, not a smaller number. Which shapes count is the whole
        // question, and getting it wrong turns a quiet day into a hole:
        //
        // - a day whose usage rows could not be read is unstated, always;
        // - a day whose priced calls span two currencies is unstated: there is
        //   no exchange rate here, so there is no sum;
        // - a day with **no calls at all** is not unstated. Its priced spend is
        //   zero, and zero sums. Reading it as a hole is what makes one quiet
        //   day hide the window's whole cost;
        // - a day whose calls were all served unpriced is not unstated either:
        //   it adds zero to the priced total, and `unpriced_calls` already says
        //   the total is a floor rather than a total.
        //
        // The day shapes that reach here with priced calls and no figure are
        // exactly the mixed-currency ones, because the store states a day only
        // when one currency priced at least one call.
        let unstated_day = days.iter().any(|d| {
            d.llm_calls.is_none()
                || (d.cost_micros.is_none()
                    && d.unpriced_calls == Some(0)
                    && d.llm_calls.unwrap_or(0) > 0)
        });
        let mut currencies: Vec<String> = days
            .iter()
            .filter_map(|d| d.cost_currency.clone())
            .collect();
        currencies.sort();
        currencies.dedup();
        let cost_currency = (currencies.len() == 1).then(|| currencies[0].clone());
        let cost_unstated = if !calls_read || !unpriced_read {
            Some(CostUnstated::UsageUnread)
        } else if unstated_day || currencies.len() > 1 {
            // One day priced in two currencies, or two days priced in
            // different ones: both are sums this service cannot take, and a
            // window that mixes them is the same non-sum as a day that does.
            Some(CostUnstated::MixedCurrency)
        } else if cost_currency.is_none() {
            // Every day was read and none is mixed, yet no currency named the
            // window: no call in it was ever priced. The total is zero and
            // there is no unit to state it in.
            Some(CostUnstated::NothingPriced)
        } else {
            None
        };
        let cost_micros = match (&cost_unstated, &cost_currency) {
            (None, Some(_)) => Some(days.iter().map(|d| d.cost_micros.unwrap_or(0)).sum::<i64>()),
            _ => None,
        };

        // A denominator of zero does not make the ratio zero -- it makes it not
        // exist, and every caller here has to render that as "no such figure",
        // not as a cheap one.
        let per = |total: Option<f64>, denominator: u64| -> Option<f64> {
            if denominator == 0 {
                return None;
            }
            Some(total? / denominator as f64)
        };

        Self {
            attempted,
            rounds,
            passed,
            landed,
            landed_with_rev,
            rev_coverage: (landed > 0).then(|| landed_with_rev as f64 / landed as f64),
            llm_calls,
            cost_micros,
            cost_currency,
            cost_unstated,
            unpriced_calls,
            calls_per_landing: per(llm_calls.map(|c| c as f64), landed),
            calls_per_shipped_change: per(llm_calls.map(|c| c as f64), landed_with_rev),
            cost_per_landing_micros: per(cost_micros.map(|m| m as f64), landed)
                .map(|v| v.round() as i64),
            cost_per_shipped_change_micros: per(cost_micros.map(|m| m as f64), landed_with_rev)
                .map(|v| v.round() as i64),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FunnelDayView {
    pub day: String,
    pub attempted: u64,
    pub rounds: u64,
    pub passed: u64,
    pub landed: u64,
    pub landed_with_rev: u64,
    /// `null` when the usage table could not be read -- not `0`.
    pub llm_calls: Option<u64>,
    /// `null` when the usage table could not be read -- not `0`.
    pub tokens: Option<u64>,
    /// What the day's priced calls cost, in `cost_currency`'s millionths. `null`
    /// when the day's money cannot be stated: the usage table could not be read,
    /// nothing that day was priced, or the day was priced in more than one
    /// currency.
    pub cost_micros: Option<i64>,
    /// The currency `cost_micros` is in, taken from the price book's label. `null`
    /// when the day has no priced call or no single currency -- the page must not
    /// pick a symbol of its own.
    pub cost_currency: Option<String>,
    /// Calls that day served with no price recorded. Any of them makes
    /// `cost_micros` a floor; the page has to say so rather than print it as the
    /// total.
    pub unpriced_calls: Option<u64>,
}

pub async fn changes_funnel_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<ChangeFunnelQuery>,
) -> Result<Json<ChangeFunnelResponse>, ApiError> {
    // Absent store is a failure, not an empty series: an empty series would be
    // the claim that no day had any activity.
    let ledger = changes(&state)?;
    let days = ledger.funnel(q.days).await?;
    let days: Vec<FunnelDayView> = days
        .into_iter()
        .map(|d| FunnelDayView {
            day: d.day,
            attempted: d.attempted,
            rounds: d.rounds,
            passed: d.passed,
            landed: d.landed,
            landed_with_rev: d.landed_with_rev,
            llm_calls: d.llm_calls,
            tokens: d.tokens,
            cost_micros: d.cost_micros,
            cost_currency: d.cost_currency,
            unpriced_calls: d.unpriced_calls,
        })
        .collect();
    let summary = FunnelSummary::of(&days);
    Ok(Json(ChangeFunnelResponse { days, summary }))
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
    /// What this one change cost, measured over its own handovers.
    pub cost: ChangeCost,
}

/// What one change cost, summed over every handover it could have made.
///
/// The figure is the change's own, not a fleet share: it is the ledger rows
/// filed under the ids this change's identity fixes, so a change that took
/// three handovers sums three attempts and a change nobody picked up reads
/// zero. Dividing a day's spend by the day's landings answers a different
/// question -- what the fleet costs per landing -- and cannot be read as what
/// any particular change cost, because the two numbers move independently.
#[derive(Debug, Serialize)]
pub struct ChangeCost {
    /// How many ids the sum was taken over -- the whole closed set a change can
    /// be worked under, not the number of handovers it actually made.
    ///
    /// The denominator travels with the figure because a reading of one
    /// generation is not a reading of a change: a change that was re-armed two
    /// times and reports its first attempt's tokens reads exactly like a cheap
    /// change. Named for what it counts, so a reader cannot mistake it for a
    /// count of attempts.
    pub ids_covered: usize,
    /// Tokens spent under those ids, or `None` when the ledger could not be
    /// read. `None` is not zero: a process with no ledger and a change that
    /// cost nothing would otherwise be one row, and the first is a fault that
    /// never clears on its own.
    pub tokens: Option<u64>,
    /// Why the figure is missing, when it is.
    pub unreadable: Option<String>,
}

/// One change's cost, read from the ledger under the ids its identity fixes.
///
/// The ids come from the control plane rather than being recomputed here: the
/// hash that turns an identity into a task id is one rule, and this crate
/// reaches every other crate through `cog-core`, so a second copy would be the
/// only way to have it here at all.
pub async fn change_cost(
    ledger: Option<&Arc<dyn cog_core::LlmUsageLedger>>,
    ids: Option<Vec<String>>,
) -> ChangeCost {
    let Some(ids) = ids else {
        return ChangeCost {
            ids_covered: 0,
            tokens: None,
            unreadable: Some("this control plane does not dispatch changes".into()),
        };
    };
    let ids_covered = ids.len();
    let Some(ledger) = ledger else {
        return ChangeCost {
            ids_covered,
            tokens: None,
            unreadable: Some("this process holds no token ledger".into()),
        };
    };
    match ledger.spend_by_task(&ids).await {
        Ok(rows) => ChangeCost {
            ids_covered,
            tokens: Some(rows.iter().map(|(_, tokens)| *tokens).sum()),
            unreadable: None,
        },
        Err(e) => ChangeCost {
            ids_covered,
            tokens: None,
            unreadable: Some(e),
        },
    }
}

/// How many moves one history returns unless the caller asks for fewer. The
/// steps come newest first, so the bound drops the oldest ones -- the account
/// can be years deep and the question is "how did this get here".
// cap: bound — 一页时间线的条数，界的是单次响应体积
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
    let cost = change_cost(
        state.llm_usage.as_ref(),
        state.orchestrator.change_dispatch_ids(&identity),
    )
    .await;
    Ok(Json(ChangeTimelineResponse {
        identity: identity.as_key(),
        kind: identity.kind,
        scope: identity.scope,
        literal: identity.literal,
        events,
        cost,
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
        let view = change_row_view(ledger_row(), None);
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
        let view = change_row_view(row, None);
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

    fn funnel_day(landed: u64, landed_with_rev: u64, micros: Option<i64>) -> FunnelDayView {
        FunnelDayView {
            day: "2026-11-09".into(),
            attempted: 10,
            rounds: 20,
            passed: 8,
            landed,
            landed_with_rev,
            llm_calls: Some(100),
            tokens: Some(1000),
            cost_micros: micros,
            cost_currency: Some("CNY".into()),
            unpriced_calls: Some(0),
        }
    }

    /// 两个分母都要印，而且都要用手算得出的数。
    ///
    /// 2026-11-10 的页面上只有一个「每笔交付成本」，分母只取带 rev 的那几条：分母是
    /// 全部关掉条数的十六分之一，同一个花销看起来贵了十六倍。反过来只取全部关掉条数
    /// 也一样坏——那会把大量「本来就成立、什么都没送出」的条算进分母，读起来像便宜。
    /// 这一格要的是：两个数都按各自的定义算出来，谁都别冒充另一个。
    #[test]
    fn both_unit_costs_are_stated_over_their_own_denominator() {
        let days = vec![funnel_day(160, 10, Some(1_600_000))];
        let s = FunnelSummary::of(&days);
        assert_eq!(s.landed, 160);
        assert_eq!(s.landed_with_rev, 10);
        assert_eq!(s.rev_coverage, Some(0.0625));
        assert_eq!(
            s.cost_per_landing_micros,
            Some(10_000),
            "每笔落地成本 = 花销 / 全部落地：1600000 / 160"
        );
        assert_eq!(
            s.cost_per_shipped_change_micros,
            Some(160_000),
            "每笔送出变更成本 = 花销 / 带 rev 的落地：1600000 / 10"
        );
        assert_eq!(s.calls_per_landing, Some(0.625));
        assert_eq!(s.calls_per_shipped_change, Some(10.0));
    }

    /// 分母为零时比值不存在。印 `0` 会把「一条都没送出」读成「送出得非常便宜」，
    /// 而这一面最需要说清的恰恰是「一条都没送出」。
    #[test]
    fn a_zero_denominator_yields_no_ratio_rather_than_zero() {
        let days = vec![funnel_day(0, 0, Some(500_000))];
        let s = FunnelSummary::of(&days);
        assert_eq!(s.rev_coverage, None);
        assert_eq!(s.cost_per_landing_micros, None);
        assert_eq!(s.cost_per_shipped_change_micros, None);
        assert_eq!(s.calls_per_shipped_change, None);
        assert_eq!(
            s.cost_micros,
            Some(500_000),
            "钱照旧是说得出的那一个数：缺的是分母，不是花销"
        );
    }

    /// 结算面上的一条硬规矩：窗口里有一天没读到用量表，整窗的钱就不是一个数。
    /// 少加一天会读成「那天没花钱」，而这一列的用途正是判断「今天是不是没产出」。
    #[test]
    fn one_unread_day_makes_the_window_money_unknown() {
        let mut unread = funnel_day(5, 2, None);
        unread.llm_calls = None;
        unread.unpriced_calls = None;
        unread.cost_currency = None;
        let days = vec![funnel_day(5, 2, Some(400_000)), unread];
        let s = FunnelSummary::of(&days);
        assert_eq!(s.llm_calls, None);
        assert_eq!(s.unpriced_calls, None);
        assert_eq!(s.cost_micros, None);
        assert_eq!(s.cost_unstated, Some(CostUnstated::UsageUnread));
        assert_eq!(s.cost_per_landing_micros, None);
        assert_eq!(
            s.landed, 10,
            "数得出来的那一半照常相加：没读懂的是钱，不是落地数"
        );
    }

    /// 窗口里有一天完全没调用（夜里没跑、网关没起），整窗的钱照样说得出来。
    ///
    /// 那一天的花销是零，零能相加。把它当成「没读到」是这一格要挡的错：一个安静的
    /// 日子会让整窗的成本变成「未记价」，于是面板上「今天没花钱」与「今天读不到」
    /// 分不开，而这一列的用途正是判断今天是不是没产出。
    #[test]
    fn a_quiet_day_adds_zero_and_does_not_hide_the_rest_of_the_window() {
        let mut quiet = funnel_day(0, 0, None);
        quiet.llm_calls = Some(0);
        quiet.tokens = Some(0);
        quiet.cost_currency = None;
        let days = vec![funnel_day(5, 2, Some(400_000)), quiet];
        let s = FunnelSummary::of(&days);
        assert_eq!(s.llm_calls, Some(100), "安静的零调用日子照样是量到的");
        assert_eq!(s.unpriced_calls, Some(0));
        assert_eq!(s.cost_micros, Some(400_000));
        assert_eq!(s.cost_currency.as_deref(), Some("CNY"));
        assert_eq!(s.cost_unstated, None);
        assert_eq!(
            s.cost_per_shipped_change_micros,
            Some(200_000),
            "分母只数带 rev 的：400000 / 2"
        );
    }

    /// 全窗一次有价调用都没有：钱是零，但没有任何币种能把它说出来。这种「说不出来」
    /// 与「没读到」是两件事，印成同一句会让一次成功的读取看起来像一次失败。
    #[test]
    fn a_window_with_nothing_priced_says_so_rather_than_claiming_it_was_unread() {
        let mut unpriced = funnel_day(5, 2, None);
        unpriced.cost_currency = None;
        unpriced.unpriced_calls = Some(100);
        let s = FunnelSummary::of(&[unpriced]);
        assert_eq!(s.llm_calls, Some(100));
        assert_eq!(s.unpriced_calls, Some(100));
        assert_eq!(s.cost_micros, None);
        assert_eq!(s.cost_unstated, Some(CostUnstated::NothingPriced));
    }

    /// 跨币种的和不是和。两天各自说得出钱，加起来说不出来——这一格要的是那句
    /// 「未记价」，不是「两天加起来的数」。
    #[test]
    fn two_currencies_do_not_add_up() {
        let mut other = funnel_day(5, 2, Some(300_000));
        other.cost_currency = Some("USD".into());
        let days = vec![funnel_day(5, 2, Some(400_000)), other];
        let s = FunnelSummary::of(&days);
        assert_eq!(s.cost_micros, None);
        assert_eq!(s.cost_currency, None);
        assert_eq!(s.cost_unstated, Some(CostUnstated::MixedCurrency));
        assert_eq!(s.cost_per_landing_micros, None);
    }

    /// 一天之内跨币种与两天之间跨币种是同一种「说不出来」：两条都要落进
    /// `MixedCurrency`，不能因为币种集合只数到一种就把跨币种那天读成「未记价」。
    #[test]
    fn a_day_priced_in_two_currencies_is_mixed_even_when_its_neighbours_are_not() {
        let mut mixed = funnel_day(5, 2, None);
        mixed.cost_currency = None;
        mixed.unpriced_calls = Some(0);
        let days = vec![funnel_day(5, 2, Some(400_000)), mixed];
        let s = FunnelSummary::of(&days);
        assert_eq!(s.cost_micros, None);
        assert_eq!(s.cost_unstated, Some(CostUnstated::MixedCurrency));
    }
}
