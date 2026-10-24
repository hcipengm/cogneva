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

use axum::{extract::State, Json};
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
    state.governance_store.as_ref().ok_or_else(|| ApiError {
        error: "ServiceUnavailable".into(),
        message: "no shared database is published, so there is no face graph to read".into(),
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
}

/// How many refused principles the route returns in the list. The totals are
/// taken before the truncation, so this bounds the list without making either
/// total a page count.
const REFUSED_PAGE: usize = 50;
/// How many refused rows are read to compute the totals. The selection is
/// `refusals > 0`, so this is a bound on how many principles have ever been
/// refused by a single deployment; reading the whole set is what lets the two
/// totals describe the set rather than the page.
const REFUSED_READ_CAP: usize = 10_000;

fn principles(state: &GatewayState) -> Result<&Arc<dyn cog_core::PrincipleLedgerStore>, ApiError> {
    state.principle_ledger.as_ref().ok_or_else(|| ApiError {
        error: "ServiceUnavailable".into(),
        message: "no shared database is published, so there is no principle ledger to read".into(),
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
        .map(|row| RefusedPrinciple {
            slug: row.slug,
            lead: row.lead,
            refusals: row.refusals,
            last_refused_at: row.last_refused_at,
        })
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
    pub identity: String,
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
}

/// How many outstanding rows the route returns. The account can hold thousands;
/// this bounds the body without bounding `outstanding_total`, which is taken
/// from the census.
const OUTSTANDING_PAGE: usize = 100;

fn changes(state: &GatewayState) -> Result<&Arc<dyn cog_core::ChangeLedgerStore>, ApiError> {
    state.change_ledger.as_ref().ok_or_else(|| ApiError {
        error: "ServiceUnavailable".into(),
        message: "no shared database is published, so there is no change ledger to read".into(),
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

    let outstanding = rows
        .into_iter()
        .map(|row| ChangeLedgerRowView {
            identity: row.identity.as_key(),
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
        })
        .collect();

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
