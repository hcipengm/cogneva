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
        spec: request.spec,
        updated_at: Utc::now(),
    };
    let outcome = cog_core::import_face(&**store, &face).await?;
    Ok(Json(outcome))
}
