//! Hand a face to a unit that is here, when the unit it names is gone.
//!
//! A face row carries a unit name, and that name is a declaration that ages:
//! the unit it wrote down leaves, and the row keeps saying it owns the face.
//! Nothing in the tree could move the row afterwards. A handover needs the
//! outgoing owner's signature, and a unit that is gone cannot give one -- which
//! is why the contract grew `from_absent`: an observation of absence, re-read
//! when the move is applied, stands in for that one signature. What it did not
//! grow is anyone to write the row. Every move was still a person typing
//! `governance handover open`, then `sign`, then `apply`, once per face.
//!
//! This pass is that someone. It reads the same graph the drift reading reads,
//! asks the heartbeat registry which units are here, and moves a face only when
//! both halves are true and the absence has held long enough to not be a
//! restart. The move is a handover row like any other: it names the reading it
//! rested on, the absent unit, the unit taking the face, and -- because no unit
//! agreed to take it -- the platform as the caster of both signatures.
//!
//! Two rules keep this from becoming a churn machine:
//!
//! - A face is handed only to a unit that holds presence on the slow clock.
//!   Planners and other in-process agents register for the length of their own
//!   run; they are alive, and they are not holders. Their records were measured
//!   at a TTL of about half a minute, which is also why the registry's own
//!   listing is the right question and the index's membership is not.
//! - The absence is not judged in one pass. The first pass that sees a face's
//!   owner gone writes a draft naming the unit that would take it; a later pass
//!   signs and applies it only after the owner's presence key has had time to
//!   expire more than once. The draft is the pass's memory, which is why this
//!   loop holds none of its own: a restarted pass reads the same draft row and
//!   picks up where the previous one stopped.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use cog_core::{
    AgentRegistry, Face, GovernanceStore, Handover, HandoverSpec, HandoverState, Signature,
    TaskStatus, HANDOVER_TASK_TYPE, OWNER_IDLE_AFTER_SECS,
};

use crate::governance_acts::{handover_task, moved_face_row, sign_spec};

/// The name this pass signs under.
///
/// It is not a unit: it is the platform acting on a face no unit can act on,
/// and the handover row says so instead of borrowing a unit's name for an
/// agreement that unit never gave.
pub const STAFFING_ACTOR: &str = "governance:auto-staffing";

/// The `source` every row written by this pass carries.
///
/// The value is how a later pass tells its own draft from a handover a person
/// opened for the same face: those two must not compete for the row.
pub const STAFFING_SOURCE: &str = "presence-loop";

/// The role a unit carries when it holds presence through the slow heartbeat.
///
/// Written by the command a unit runs to make itself addressable. A unit with
/// any other role is an in-process agent whose registration lasts as long as
/// the run it was created for; handing it a face would move the face again
/// when that run ends.
pub const FACE_HOLDER_ROLE: &str = "external-worker";

/// How long an owner must be gone before its face moves.
///
/// Twice the idle window the reading uses. The presence key an external worker
/// leaves behind outlives its last heartbeat by one and a half windows, so a
/// single window would move faces during the key's own lifetime -- turning a
/// restart into a handover. Two windows is past the expiry with room for one
/// missed renewal on top.
pub const STAFFING_GRACE_SECS: i64 = OWNER_IDLE_AFTER_SECS * 2;

/// Why the pass withdrew a draft it had opened.
pub const OWNER_RETURNED: &str = "the owner came back before the move";
/// Why the pass withdrew a draft naming a unit that has since left.
pub const TARGET_LEFT: &str = "the unit named to take the face left before the move";

/// One write to make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaffingStep {
    /// Record that the face's owner is gone and name the unit that would take
    /// it. Nothing moves until a later pass signs this.
    Open {
        face_id: String,
        from_unit: String,
        to_unit: String,
    },
    /// The absence has held long enough: sign the draft and apply it.
    Move { handover_id: String },
    /// The absence still holds, but the unit the draft named is gone too.
    /// Keep the observation and name another holder.
    Retarget {
        handover_id: String,
        to_unit: String,
    },
    /// The move no longer holds.
    Withdraw {
        handover_id: String,
        reason: &'static str,
    },
}

/// What one pass should write.
///
/// Pure: the caller supplies the three readings (graph rows, present units,
/// and which of those can hold a face) and gets back the writes. Nothing here
/// knows what time it is beyond the `now` it was handed, so the rule can be
/// read and tested without a clock or a store.
pub fn plan_staffing(
    faces: &[Face],
    handovers: &[Handover],
    live: &[String],
    holders: &[String],
    now: DateTime<Utc>,
) -> Vec<StaffingStep> {
    // The graph's own order is not a promise. Load spread is decided as the
    // plan is built -- the second face sees where the first one went -- so two
    // passes that read the same rows in different orders would hand the same
    // faces to different units. Sorted here, the plan is a function of the
    // readings alone.
    let mut faces: Vec<&Face> = faces.iter().collect();
    faces.sort_by(|a, b| a.face_id.cmp(&b.face_id));

    let mut load: BTreeMap<&str, usize> = holders.iter().map(|unit| (unit.as_str(), 0)).collect();
    for face in &faces {
        if let Some(owner) = face.owner_unit.as_deref() {
            if let Some(count) = load.get_mut(owner) {
                *count += 1;
            }
        }
    }

    let mut steps = Vec::new();
    for face in faces {
        let Some(owner) = face.owner_unit.as_deref() else {
            // A vacancy has no outgoing owner to be absent: filling one is a
            // different act and is not this pass's to take.
            continue;
        };
        let ours: Vec<&Handover> = handovers
            .iter()
            .filter(|row| {
                row.spec.face_id == face.face_id
                    && row.spec.source.as_deref() == Some(STAFFING_SOURCE)
                    && matches!(row.state(), HandoverState::Draft | HandoverState::Signed)
            })
            .collect();

        if live.iter().any(|unit| unit == owner) {
            for row in &ours {
                steps.push(StaffingStep::Withdraw {
                    handover_id: row.id.clone(),
                    reason: OWNER_RETURNED,
                });
            }
            continue;
        }

        // A handover somebody opened by hand for this face is a decision
        // already being made. Two writers racing for one face is how a move
        // gets signed against a reading the other writer never saw.
        let foreign_open = handovers.iter().any(|row| {
            row.spec.face_id == face.face_id
                && matches!(row.state(), HandoverState::Draft | HandoverState::Signed)
                && row.spec.source.as_deref() != Some(STAFFING_SOURCE)
        });
        if foreign_open {
            continue;
        }

        if let Some(row) = ours.iter().min_by(|a, b| a.id.cmp(&b.id)) {
            let target_live = holders.contains(&row.spec.to_unit);
            if !target_live {
                // What the draft recorded is the owner's absence, and that is
                // still true; what changed is the unit that was to take the
                // face. Withdrawing the draft here would throw the wait away,
                // and a later pass would start the grace over -- so a face
                // whose would-be takers churn faster than the grace would
                // never move at all. The unsigned draft keeps its
                // `from_absent` and changes only its target.
                //
                // A signed draft is not rewritten like this: its signatures
                // cover the payload, and one of them is the target's own
                // agreement. A unit that is gone cannot have agreed to
                // anything, so that agreement is withdrawn, not edited.
                if row.state() == HandoverState::Draft {
                    if let Some(to_unit) = pick_target(owner, holders, &load) {
                        *load
                            .get_mut(to_unit.as_str())
                            .expect("holders seeded the load") += 1;
                        steps.push(StaffingStep::Retarget {
                            handover_id: row.id.clone(),
                            to_unit,
                        });
                        continue;
                    }
                }
                steps.push(StaffingStep::Withdraw {
                    handover_id: row.id.clone(),
                    reason: TARGET_LEFT,
                });
                continue;
            }
            let waited = row
                .spec
                .from_absent
                .map(|seen| (now - seen).num_seconds() >= STAFFING_GRACE_SECS)
                .unwrap_or(false);
            if waited || row.state() == HandoverState::Signed {
                steps.push(StaffingStep::Move {
                    handover_id: row.id.clone(),
                });
            }
            continue;
        }

        let Some(to_unit) = pick_target(owner, holders, &load) else {
            continue;
        };
        *load
            .get_mut(to_unit.as_str())
            .expect("holders seeded the load") += 1;
        steps.push(StaffingStep::Open {
            face_id: face.face_id.clone(),
            from_unit: owner.to_string(),
            to_unit,
        });
    }
    steps
}

/// The live holder with the fewest faces, ties settled by name.
///
/// By load rather than by declaration: a face row no longer carries the roster
/// that named its owner, and the graph is the only account of who is carrying
/// what. The name is the tie-break so two passes reading the same graph pick
/// the same unit -- a choice that varied between passes would sign one move and
/// apply another.
fn pick_target(owner: &str, holders: &[String], load: &BTreeMap<&str, usize>) -> Option<String> {
    holders
        .iter()
        .filter(|unit| unit.as_str() != owner)
        .min_by(|a, b| {
            let load_a = load.get(a.as_str()).copied().unwrap_or(0);
            let load_b = load.get(b.as_str()).copied().unwrap_or(0);
            load_a.cmp(&load_b).then_with(|| a.cmp(b))
        })
        .cloned()
}

/// What one pass did, for the caller that wants to log or test it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StaffingReport {
    pub opened: usize,
    pub retargeted: usize,
    pub moved: usize,
    pub withdrawn: usize,
}

/// Run one staffing pass.
///
/// Not started, or started without a registry, is not a fault: without a
/// presence reading there is no way to tell a unit that left from one whose
/// heartbeat was briefly unreadable, and a pass that cannot tell must not move
/// anything.
pub async fn staff_faces(
    store: &dyn GovernanceStore,
    agents: &dyn AgentRegistry,
    live: &[String],
    holders: &[String],
    now: DateTime<Utc>,
) -> cog_core::SFResult<StaffingReport> {
    if holders.is_empty() {
        return Ok(StaffingReport::default());
    }
    let face_rows = store.face_nodes().await?;
    let faces: Vec<Face> = face_rows.iter().filter_map(Face::from_task).collect();
    let handovers: Vec<Handover> = store
        .governance_nodes(HANDOVER_TASK_TYPE)
        .await?
        .iter()
        .filter_map(Handover::from_task)
        .collect();

    let steps = plan_staffing(&faces, &handovers, live, holders, now);
    let mut report = StaffingReport::default();
    for step in steps {
        match step {
            StaffingStep::Open {
                face_id,
                from_unit,
                to_unit,
            } => {
                let spec = draft_spec(&face_id, &from_unit, &to_unit, live, now);
                let id = format!(
                    "handover:{}:{}:{}",
                    face_id,
                    to_unit,
                    now.timestamp_millis()
                );
                // The id has to be unique per row and the rows are written by
                // one leased pass; two faces never share an id because the
                // face id is in it. Two opens of the same face in one pass
                // cannot happen -- the planner emits one step per face.
                store
                    .save_governance_node(&handover_task(&id, spec, now))
                    .await?;
                report.opened += 1;
                info!(
                    face = %face_id,
                    from = %from_unit,
                    to = %to_unit,
                    grace_secs = STAFFING_GRACE_SECS,
                    "face owner is gone; a draft names the unit that would take it"
                );
            }
            StaffingStep::Move { handover_id } => {
                let Some(row) = handovers.iter().find(|row| row.id == handover_id) else {
                    continue;
                };
                let Some(from_unit) = row.spec.from_unit.as_deref() else {
                    continue;
                };
                // The absence the draft recorded is re-read here, one write
                // before the move. The registry changing its mind between the
                // listing and this line is exactly the case the re-read exists
                // for: the owner came back, so the face stays with it.
                match agents.get(from_unit).await {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        withdraw(store, row, OWNER_RETURNED, now).await?;
                        report.withdrawn += 1;
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            face = %row.spec.face_id,
                            from = %from_unit,
                            error = %e,
                            "whether the face's owner is still gone could not be read; no move \
                             is made this pass"
                        );
                        continue;
                    }
                }
                let mut spec = row.spec.clone();
                if !spec.is_fully_signed() {
                    let to_unit = spec.to_unit.clone();
                    sign_spec(&mut spec, &to_unit, Some(STAFFING_ACTOR.into()), now)
                        .map_err(cog_core::SFError::Validation)?;
                    sign_spec(&mut spec, STAFFING_ACTOR, Some(STAFFING_ACTOR.into()), now)
                        .map_err(cog_core::SFError::Validation)?;
                }
                if !spec.is_fully_signed() {
                    warn!(
                        handover = %handover_id,
                        "a staffing draft is not signed by every required unit; no move is made"
                    );
                    continue;
                }
                // Matched by id rather than by position: `faces` drops rows
                // whose payload does not parse, so an index into it is not an
                // index into `face_rows`.
                let face_task = face_rows.iter().find(|task| {
                    Face::from_task(task).map(|row| row.face_id) == Some(spec.face_id.clone())
                });
                let Some(face_task) = face_task else {
                    warn!(
                        face = %spec.face_id,
                        "the face a staffing draft names no longer has a row; nothing to move"
                    );
                    continue;
                };
                let (moved_face, expected_revision) =
                    moved_face_row(face_task, &spec, handover_id.as_str(), now)
                        .map_err(cog_core::SFError::Validation)?;
                let mut applied = (*row).clone();
                applied.spec = spec;
                applied.status = HandoverState::Applied.status();
                let mut task = handover_task(&applied.id, applied.spec.clone(), now);
                task.status = TaskStatus::Completed;
                task.result = Some(serde_json::json!({
                    "outcome": "applied",
                    "face_id": applied.spec.face_id,
                    "to_unit": applied.spec.to_unit,
                    "cast_by": STAFFING_ACTOR,
                }));
                match store
                    .apply_handover(&task, &moved_face, expected_revision)
                    .await?
                {
                    Some(rebound) => {
                        report.moved += 1;
                        info!(
                            face = %applied.spec.face_id,
                            from = %applied.spec.from_unit.as_deref().unwrap_or("-"),
                            to = %applied.spec.to_unit,
                            rebound,
                            "face handed to a unit that is present; the row and the work moved \
                             together"
                        );
                    }
                    None => warn!(
                        handover = %handover_id,
                        face = %applied.spec.face_id,
                        "the face moved or the handover changed while it was being applied; the \
                         next pass reads the graph again"
                    ),
                }
            }
            StaffingStep::Retarget {
                handover_id,
                to_unit,
            } => {
                let Some(row) = handovers.iter().find(|row| row.id == handover_id) else {
                    continue;
                };
                let mut spec = row.spec.clone();
                let from_unit = spec.from_unit.clone().unwrap_or_default();
                spec.to_unit = to_unit.clone();
                spec.withdraw_condition = Some(format!(
                    "{from_unit} returns to the heartbeat registry, or {to_unit} leaves before \
                     the move"
                ));
                // The terms changed, so a digest or a signature over the old
                // ones would now be a claim about a payload nobody read. The
                // planner only retargets unsigned drafts; clearing here is
                // what makes that a checked fact rather than a promise by the
                // caller.
                spec.payload_sha256 = None;
                spec.signed_by.clear();
                let mut task = handover_task(&row.id, spec, now);
                task.updated_at = now;
                if store
                    .save_governance_node_if_status(&task, &[HandoverState::Draft.status()])
                    .await?
                {
                    report.retargeted += 1;
                    info!(
                        handover = %handover_id,
                        face = %row.spec.face_id,
                        from = %from_unit,
                        to = %to_unit,
                        absent_since = ?row.spec.from_absent,
                        "the unit named to take the face left; the draft names another and the \
                         absence keeps its original timestamp"
                    );
                }
            }
            StaffingStep::Withdraw {
                handover_id,
                reason,
            } => {
                let Some(row) = handovers.iter().find(|row| row.id == handover_id) else {
                    continue;
                };
                if withdraw(store, row, reason, now).await? {
                    report.withdrawn += 1;
                }
            }
        }
    }
    Ok(report)
}

/// The row a first sighting writes: everything a later pass needs to decide
/// whether the move still holds, and nothing that claims agreement yet.
fn draft_spec(
    face_id: &str,
    from_unit: &str,
    to_unit: &str,
    live: &[String],
    now: DateTime<Utc>,
) -> HandoverSpec {
    HandoverSpec {
        face_id: face_id.to_string(),
        from_unit: Some(from_unit.to_string()),
        from_absent: Some(now),
        to_unit: to_unit.to_string(),
        witness_unit: STAFFING_ACTOR.to_string(),
        source: Some(STAFFING_SOURCE.to_string()),
        criterion_reading: Some(format!(
            "the heartbeat registry does not list {from_unit}; {} unit(s) hold presence: {}",
            live.len(),
            if live.is_empty() {
                "-".to_string()
            } else {
                live.join(",")
            }
        )),
        reading_at: Some(now),
        open_actions: Vec::new(),
        open_task_ids: Vec::new(),
        permissions_context: None,
        artifact_path: None,
        artifact_expires_at: None,
        // A draft that nobody signs is the record of an absence, not a
        // promise; it dies with the vacancy it described. Left open forever it
        // would stop the next pass from ever drafting a move for this face.
        expires_when: Some(now + chrono::Duration::days(1)),
        withdraw_condition: Some(format!(
            "{from_unit} returns to the heartbeat registry, or {to_unit} leaves before the move"
        )),
        payload_sha256: None,
        signed_by: Vec::new(),
    }
}

/// Mark an open draft withdrawn, if it is still open.
async fn withdraw(
    store: &dyn GovernanceStore,
    row: &Handover,
    reason: &'static str,
    now: DateTime<Utc>,
) -> cog_core::SFResult<bool> {
    let mut task = handover_task(&row.id, row.spec.clone(), now);
    task.result = Some(serde_json::json!({
        "outcome": "withdrawn",
        "by_unit": STAFFING_ACTOR,
        "reason": reason,
    }));
    task.updated_at = now;
    task.status = HandoverState::Withdrawn.status();
    let saved = store
        .save_governance_node_if_status(
            &task,
            &[
                HandoverState::Draft.status(),
                HandoverState::Signed.status(),
            ],
        )
        .await?;
    if saved {
        info!(
            handover = %row.id,
            face = %row.spec.face_id,
            %reason,
            "staffing draft withdrawn"
        );
    }
    Ok(saved)
}

/// The signature a handover carries when the platform cast it for a unit.
///
/// Exposed because a reader of the ledger should be able to name the shape
/// without re-deriving it: a row whose signatures all say the same caster was
/// written by this pass, not agreed to by two units.
pub fn platform_signature(unit: &str, at: DateTime<Utc>) -> Signature {
    Signature {
        unit: unit.to_string(),
        at,
        cast_by: Some(STAFFING_ACTOR.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::FaceKind;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn face(id: &str, owner: Option<&str>) -> Face {
        Face {
            face_id: id.to_string(),
            owner_unit: owner.map(str::to_string),
            bound_by_handover_id: None,
            spec: cog_core::FaceSpec::first(id, FaceKind::Face),
            updated_at: at(0),
        }
    }

    fn units(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn draft(face_id: &str, from: &str, to: &str, seen: DateTime<Utc>) -> Handover {
        let mut spec = draft_spec(face_id, from, to, &units(&[to]), seen);
        spec.from_absent = Some(seen);
        Handover {
            id: format!("handover:{face_id}:{to}:1"),
            status: TaskStatus::Pending,
            spec,
            updated_at: seen,
        }
    }

    #[test]
    fn a_face_whose_owner_is_gone_gets_a_draft_naming_a_present_holder() {
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[],
            &units(&["worker", "alice-1"]),
            &units(&["worker"]),
            at(0),
        );
        assert_eq!(
            steps,
            vec![StaffingStep::Open {
                face_id: "通信".into(),
                from_unit: "f5".into(),
                to_unit: "worker".into(),
            }]
        );
    }

    #[test]
    fn a_face_whose_owner_is_present_is_left_alone() {
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[],
            &units(&["f5", "worker"]),
            &units(&["f5", "worker"]),
            at(0),
        );
        assert!(steps.is_empty(), "{steps:?}");
    }

    #[test]
    fn an_absent_owner_with_nobody_to_take_the_face_moves_nothing() {
        // Present, but not a holder: an in-process planner is alive and is not
        // somebody a face can be handed to.
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[],
            &units(&["alice-1"]),
            &units(&[]),
            at(0),
        );
        assert!(steps.is_empty(), "{steps:?}");
    }

    #[test]
    fn the_draft_is_signed_only_after_the_absence_has_held_past_the_grace() {
        let early = plan_staffing(
            &[face("通信", Some("f5"))],
            &[draft("通信", "f5", "worker", at(0))],
            &units(&["worker"]),
            &units(&["worker"]),
            at(STAFFING_GRACE_SECS - 1),
        );
        assert!(early.is_empty(), "{early:?}");

        let late = plan_staffing(
            &[face("通信", Some("f5"))],
            &[draft("通信", "f5", "worker", at(0))],
            &units(&["worker"]),
            &units(&["worker"]),
            at(STAFFING_GRACE_SECS),
        );
        assert_eq!(
            late,
            vec![StaffingStep::Move {
                handover_id: "handover:通信:worker:1".into()
            }]
        );
    }

    #[test]
    fn an_owner_that_comes_back_withdraws_the_draft_instead_of_moving_it() {
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[draft("通信", "f5", "worker", at(0))],
            &units(&["f5", "worker"]),
            &units(&["f5", "worker"]),
            at(STAFFING_GRACE_SECS * 2),
        );
        assert_eq!(
            steps,
            vec![StaffingStep::Withdraw {
                handover_id: "handover:通信:worker:1".into(),
                reason: OWNER_RETURNED,
            }]
        );
    }

    #[test]
    fn a_target_that_left_hands_the_draft_to_another_holder() {
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[draft("通信", "f5", "worker", at(0))],
            &units(&["f6"]),
            &units(&["f6"]),
            at(STAFFING_GRACE_SECS / 2),
        );
        assert_eq!(
            steps,
            vec![StaffingStep::Retarget {
                handover_id: "handover:通信:worker:1".into(),
                to_unit: "f6".into(),
            }],
            "the wait the draft already served is not thrown away because its target left"
        );
    }

    #[test]
    fn a_retargeted_draft_moves_on_the_original_absences_clock() {
        // A draft minted when `worker` was the target, rewritten to `f6`. The
        // id keeps naming the draft, not the unit that ends up taking it.
        let mut moved = draft("通信", "f5", "worker", at(0));
        moved.spec.to_unit = "f6".into();
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[moved],
            &units(&["f6"]),
            &units(&["f6"]),
            at(STAFFING_GRACE_SECS),
        );
        assert_eq!(
            steps,
            vec![StaffingStep::Move {
                handover_id: "handover:通信:worker:1".into()
            }]
        );
    }

    #[test]
    fn a_target_that_left_with_nobody_else_to_take_the_face_withdraws_the_draft() {
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[draft("通信", "f5", "worker", at(0))],
            &units(&["alice-1"]),
            &units(&[]),
            at(STAFFING_GRACE_SECS * 2),
        );
        assert_eq!(
            steps,
            vec![StaffingStep::Withdraw {
                handover_id: "handover:通信:worker:1".into(),
                reason: TARGET_LEFT,
            }]
        );
    }

    #[test]
    fn a_handover_somebody_opened_by_hand_keeps_the_face_to_itself() {
        let mut manual = draft("通信", "f5", "operator", at(0));
        manual.spec.source = Some("the owner is the session actually on duty".into());
        let steps = plan_staffing(
            &[face("通信", Some("f5"))],
            &[manual],
            &units(&["worker"]),
            &units(&["worker"]),
            at(STAFFING_GRACE_SECS * 2),
        );
        assert!(steps.is_empty(), "{steps:?}");
    }

    #[test]
    fn faces_spread_over_the_present_holders_instead_of_piling_onto_one() {
        let faces = vec![
            face("通信", Some("f5")),
            face("资源管理", Some("f5")),
            face("可观测与告警", Some("f5")),
        ];
        let steps = plan_staffing(
            &faces,
            &[],
            &units(&["worker", "operator"]),
            &units(&["worker", "operator"]),
            at(0),
        );
        let targets: Vec<&str> = steps
            .iter()
            .map(|step| match step {
                StaffingStep::Open { to_unit, .. } => to_unit.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(targets, vec!["operator", "worker", "operator"]);
    }

    #[test]
    fn a_vacant_face_is_not_this_pass_to_fill() {
        let steps = plan_staffing(
            &[face("通信", None)],
            &[],
            &units(&["worker"]),
            &units(&["worker"]),
            at(0),
        );
        assert!(steps.is_empty(), "{steps:?}");
    }

    #[test]
    fn the_same_graph_planned_twice_gives_the_same_steps() {
        let faces = vec![face("b", Some("gone")), face("a", Some("gone"))];
        let live = units(&["x", "y"]);
        let first = plan_staffing(&faces, &[], &live, &live, at(0));
        let second = plan_staffing(
            &faces.clone().into_iter().rev().collect::<Vec<_>>(),
            &[],
            &live,
            &live,
            at(0),
        );
        assert_eq!(first, second);
    }

    #[test]
    fn a_platform_signature_names_the_unit_and_the_caster() {
        let signature = platform_signature("worker", at(0));
        assert_eq!(signature.unit, "worker");
        assert_eq!(signature.cast_by.as_deref(), Some(STAFFING_ACTOR));
    }

    /// A governance store holding rows in memory, with the one transactional
    /// write this pass depends on: a handover and a face row move together, and
    /// the face write is guarded by the revision the pass read.
    #[derive(Default)]
    struct MemoryStore {
        rows: std::sync::Mutex<Vec<cog_core::Task>>,
    }

    impl MemoryStore {
        fn with(rows: Vec<cog_core::Task>) -> Self {
            Self {
                rows: std::sync::Mutex::new(rows),
            }
        }

        fn row(&self, id: &str) -> cog_core::Task {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.id == id)
                .cloned()
                .unwrap_or_else(|| panic!("no row {id}"))
        }

        fn of_type(&self, task_type: &str) -> Vec<cog_core::Task> {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .filter(|row| {
                    matches!(&row.task_type, cog_core::TaskType::Custom(kind) if kind == task_type)
                })
                .cloned()
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl GovernanceStore for MemoryStore {
        async fn face_nodes(&self) -> cog_core::SFResult<Vec<cog_core::Task>> {
            Ok(self.of_type("face"))
        }

        async fn open_face_bound_tasks(&self) -> cog_core::SFResult<Vec<cog_core::Task>> {
            Ok(Vec::new())
        }

        async fn save_face_node(&self, task: &cog_core::Task) -> cog_core::SFResult<()> {
            self.save_governance_node(task).await
        }

        async fn save_face_node_if_revision(
            &self,
            task: &cog_core::Task,
            expected: i64,
        ) -> cog_core::SFResult<bool> {
            let _ = expected;
            self.save_governance_node(task).await?;
            Ok(true)
        }

        async fn governance_nodes(
            &self,
            task_type: &str,
        ) -> cog_core::SFResult<Vec<cog_core::Task>> {
            Ok(self.of_type(task_type))
        }

        async fn save_governance_node(&self, task: &cog_core::Task) -> cog_core::SFResult<()> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(slot) => *slot = task.clone(),
                None => rows.push(task.clone()),
            }
            Ok(())
        }

        async fn save_governance_node_if_status(
            &self,
            task: &cog_core::Task,
            expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(slot) if expected.contains(&slot.status) => {
                    *slot = task.clone();
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        async fn save_governance_node_with_event(
            &self,
            _event: &cog_core::Task,
            _node: &cog_core::Task,
            _expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            Ok(false)
        }

        async fn apply_handover(
            &self,
            handover: &cog_core::Task,
            face: &cog_core::Task,
            expected_revision: i64,
        ) -> cog_core::SFResult<Option<usize>> {
            let mut rows = self.rows.lock().unwrap();
            let Some(face_slot) = rows.iter().position(|row| row.id == face.id) else {
                return Ok(None);
            };
            let current = Face::from_task(&rows[face_slot]).unwrap();
            if current.revision() != expected_revision {
                return Ok(None);
            }
            rows[face_slot] = face.clone();
            match rows.iter_mut().find(|row| row.id == handover.id) {
                Some(slot) => *slot = handover.clone(),
                None => rows.push(handover.clone()),
            }
            Ok(Some(0))
        }

        async fn reassign_face_work(
            &self,
            _face_id: &str,
            _to_unit: &str,
        ) -> cog_core::SFResult<usize> {
            Ok(0)
        }

        async fn release_face_work(&self, _face_id: &str) -> cog_core::SFResult<usize> {
            Ok(0)
        }
    }

    /// The heartbeat registry as this pass reads it: one answer per unit.
    struct Registry {
        present: Vec<String>,
    }

    #[async_trait::async_trait]
    impl AgentRegistry for Registry {
        async fn register(
            &self,
            _registration: &cog_core::AgentRegistration,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn heartbeat(&self, _agent_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn deregister(&self, _agent_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn get(
            &self,
            agent_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::AgentRegistration>> {
            Ok(self
                .present
                .iter()
                .find(|unit| unit.as_str() == agent_id)
                .map(|unit| {
                    cog_core::AgentRegistration::new(
                        unit.clone(),
                        "host",
                        "",
                        FACE_HOLDER_ROLE,
                        "default",
                        Vec::new(),
                        cog_core::ResourceInfo::default(),
                    )
                }))
        }

        async fn list(&self) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(self
                .present
                .iter()
                .map(|unit| {
                    cog_core::AgentRegistration::new(
                        unit.clone(),
                        "host",
                        "",
                        FACE_HOLDER_ROLE,
                        "default",
                        Vec::new(),
                        cog_core::ResourceInfo::default(),
                    )
                })
                .collect())
        }

        async fn list_by_role(
            &self,
            _role: &str,
        ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            self.list().await
        }

        async fn list_by_capability(
            &self,
            _capability: &str,
        ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn the_first_pass_records_an_absence_and_moves_nothing() {
        let store = MemoryStore::with(vec![face("通信", Some("f5")).to_task(None)]);
        let registry = Registry {
            present: units(&["worker"]),
        };
        let report = staff_faces(
            &store,
            &registry,
            &units(&["worker"]),
            &units(&["worker"]),
            at(0),
        )
        .await
        .unwrap();
        assert_eq!(report.opened, 1);
        assert_eq!(report.moved, 0);

        let hands = store.of_type(HANDOVER_TASK_TYPE);
        let drafted = Handover::from_task(&hands[0]).unwrap();
        assert_eq!(drafted.state(), HandoverState::Draft);
        assert_eq!(drafted.spec.from_unit.as_deref(), Some("f5"));
        assert_eq!(drafted.spec.from_absent, Some(at(0)));
        assert_eq!(drafted.spec.to_unit, "worker");
        assert_eq!(drafted.spec.source.as_deref(), Some(STAFFING_SOURCE));
        assert!(drafted.spec.signed_by.is_empty());
        assert_eq!(
            Face::from_task(&store.row(&face("通信", None).to_task(None).id))
                .unwrap()
                .owner_unit
                .as_deref(),
            Some("f5"),
            "a draft is a reading, not a move"
        );
    }

    #[tokio::test]
    async fn once_the_grace_has_passed_the_face_and_the_signatures_land_together() {
        let store = MemoryStore::with(vec![face("通信", Some("f5")).to_task(None)]);
        let absent = Registry {
            present: units(&["worker"]),
        };
        staff_faces(
            &store,
            &absent,
            &units(&["worker"]),
            &units(&["worker"]),
            at(0),
        )
        .await
        .unwrap();

        let report = staff_faces(
            &store,
            &absent,
            &units(&["worker"]),
            &units(&["worker"]),
            at(STAFFING_GRACE_SECS),
        )
        .await
        .unwrap();
        assert_eq!(report.moved, 1);

        let moved = Face::from_task(&store.row(&face("通信", None).to_task(None).id)).unwrap();
        assert_eq!(moved.owner_unit.as_deref(), Some("worker"));
        assert_eq!(moved.revision(), 2);

        let hands = store.of_type(HANDOVER_TASK_TYPE);
        assert_eq!(hands.len(), 1, "one draft, one move: no second row");
        let applied = Handover::from_task(&hands[0]).unwrap();
        assert_eq!(applied.state(), HandoverState::Applied);
        assert_eq!(
            moved.bound_by_handover_id.as_deref(),
            Some(applied.id.as_str())
        );
        let casters: Vec<Option<&str>> = applied
            .spec
            .signed_by
            .iter()
            .map(|signature| signature.cast_by.as_deref())
            .collect();
        assert_eq!(
            casters,
            vec![Some(STAFFING_ACTOR), Some(STAFFING_ACTOR)],
            "the platform signed; the row must not read as two units agreeing"
        );
        assert!(applied.spec.is_fully_signed());
    }

    #[tokio::test]
    async fn a_draft_whose_target_left_is_rewritten_and_keeps_its_absence_timestamp() {
        let store = MemoryStore::with(vec![face("通信", Some("f5")).to_task(None)]);
        staff_faces(
            &store,
            &Registry {
                present: units(&["worker"]),
            },
            &units(&["worker"]),
            &units(&["worker"]),
            at(0),
        )
        .await
        .unwrap();

        // Only the target changed: `worker` is gone, `f6` is here. The
        // absence is still f5's, and it is the same absence.
        let later = Registry {
            present: units(&["f6"]),
        };
        let report = staff_faces(
            &store,
            &later,
            &units(&["f6"]),
            &units(&["f6"]),
            at(STAFFING_GRACE_SECS / 2),
        )
        .await
        .unwrap();
        assert_eq!(report.retargeted, 1);
        assert_eq!(report.moved, 0);

        let hands = store.of_type(HANDOVER_TASK_TYPE);
        assert_eq!(
            hands.len(),
            1,
            "a retarget rewrites the draft, it does not fork it"
        );
        let rewritten = Handover::from_task(&hands[0]).unwrap();
        assert_eq!(rewritten.state(), HandoverState::Draft);
        assert_eq!(rewritten.spec.to_unit, "f6");
        assert_eq!(
            rewritten.spec.from_absent,
            Some(at(0)),
            "the absence began half a grace ago; a retarget must not restart that clock"
        );

        let report = staff_faces(
            &store,
            &later,
            &units(&["f6"]),
            &units(&["f6"]),
            at(STAFFING_GRACE_SECS),
        )
        .await
        .unwrap();
        assert_eq!(
            report.moved, 1,
            "the move lands one grace after the absence, not two"
        );
        let moved = Face::from_task(&store.row(&face("通信", None).to_task(None).id)).unwrap();
        assert_eq!(moved.owner_unit.as_deref(), Some("f6"));
    }

    #[tokio::test]
    async fn an_owner_that_comes_back_between_the_draft_and_the_move_keeps_the_face() {
        let store = MemoryStore::with(vec![face("通信", Some("f5")).to_task(None)]);
        let absent = Registry {
            present: units(&["worker"]),
        };
        staff_faces(
            &store,
            &absent,
            &units(&["worker"]),
            &units(&["worker"]),
            at(0),
        )
        .await
        .unwrap();

        // The listing still says f5 is gone, but the re-read one write before
        // the move finds it: the fact the draft rested on expired.
        let back = Registry {
            present: units(&["worker", "f5"]),
        };
        let report = staff_faces(
            &store,
            &back,
            &units(&["worker"]),
            &units(&["worker"]),
            at(STAFFING_GRACE_SECS),
        )
        .await
        .unwrap();
        assert_eq!(report.moved, 0);
        assert_eq!(report.withdrawn, 1);

        let kept = Face::from_task(&store.row(&face("通信", None).to_task(None).id)).unwrap();
        assert_eq!(kept.owner_unit.as_deref(), Some("f5"));
        let hands = store.of_type(HANDOVER_TASK_TYPE);
        assert_eq!(
            Handover::from_task(&hands[0]).unwrap().state(),
            HandoverState::Withdrawn
        );
    }
}
