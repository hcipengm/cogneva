//! `cogneva governance handover|vote` — moves and questions, kept where the
//! faces already live.
//!
//! A handover is one task row: it names the face and the two units, carries
//! the signatures in its payload, and stops being a draft once all three
//! required units signed. Applying it writes two rows in one transaction — the
//! handover moves to its applied state and the face's owner moves — so a reader
//! can never see the face moved with the paper still in draft.
//!
//! A question is one decision row and one vote row per voter. The rules that
//! settle it are written when it opens and are not rewritten; the tally is
//! computed from the votes every time it is asked for, so no stored count can
//! disagree with the rows it summarises. A principle that settles the question
//! outright produces a resolved decision with no votes at all.
//!
//! These are rows of the task graph, in the same reserved workspace as the
//! faces, and they are never executable: nothing schedules them.

use std::error::Error;

use chrono::{DateTime, Utc};

use cog_core::{
    append_decision_event, close_decision_with_event, verify_decision_chain, ChainVerdict,
    CloseOutcome, Decision, DecisionMode, DecisionSpec, DecisionState, GovernanceStore, Handover,
    HandoverSpec, HandoverState, Signature, TaskStatus, TieBreak, Vote, DECISION_TASK_TYPE,
    HANDOVER_TASK_TYPE, VOTE_TASK_TYPE,
};
use cog_orchestrator::governance_acts::{
    backfill_ballot, close_ballot, decision_task, handover_task, tally, validate_ballot,
    validate_principle, vote_id, vote_task,
};

pub const USAGE: &str = "\
cogneva governance <handover|vote> <SUBCOMMAND> [OPTIONS]

HANDOVER — moving a face from one unit to another. All three units must sign.
    open --face <face> --to <unit> --witness <unit> [--from <unit>]
         [--source <text>] [--criterion <reading>] [--reading-at <rfc3339>]
         [--open-action <text>]... [--permissions-context <text>]
         [--artifact <path>] [--artifact-expires <rfc3339>]
         [--expires-when <rfc3339>] [--withdraw-condition <text>]
         Seven cells: where the reading came from, the reading and when it was
         taken, unfinished actions, permissions, witness, the artifact and how
         long it stays readable, when the handover dies and what withdraws it.
         --open-action may repeat; each one is an obligation the new owner
         inherits and a place the next reader will look for it.
    sign --id <handover> --unit <unit>
    apply --id <handover>
    withdraw --id <handover> --unit <unit>
    list [--face <face>]

VOTE — a question the principles do not settle on their own.
    open --subject <text> --kind <kind> --option <opt> --option <opt> ...
         (--status-quo <opt> --eligible <u1,u2,...> --quorum <n>
          --deadline <rfc3339> --tie-break status_quo|second_round|escalate_named_body)
         | (--principle <slug> --rationale <text> --result <opt>)
    cast --decision <id> --unit <unit> --option <opt>
    withdraw --decision <id> --unit <unit>
    close --decision <id>
    verify --decision <id>
         Recompute the decision's history and report whether it still verifies.
         An event that was edited, removed or reordered exits non-zero.
    list [--json]
    backfill --replies <dir> --subject <text> --kind <kind> --option <opt> ...
             --status-quo <opt> --deadline <rfc3339>
         One file per unit, named <unit>-vote.md, first `vote:` line is the
         choice. The record is marked provenance=backfilled, predeclared=false,
         tie_break=not_predeclared: the count is real, the rules were not
         declared before the votes were cast.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Act {
    HandoverOpen {
        face: String,
        from: Option<String>,
        to: String,
        witness: String,
        source: Option<String>,
        criterion: Option<String>,
        reading_at: Option<DateTime<Utc>>,
        open_actions: Vec<String>,
        permissions_context: Option<String>,
        artifact: Option<String>,
        artifact_expires_at: Option<DateTime<Utc>>,
        expires_when: Option<DateTime<Utc>>,
        withdraw_condition: Option<String>,
    },
    HandoverSign {
        id: String,
        unit: String,
    },
    HandoverApply {
        id: String,
    },
    HandoverWithdraw {
        id: String,
        unit: String,
    },
    HandoverList {
        face: Option<String>,
    },
    VoteOpen {
        subject: String,
        kind: String,
        options: Vec<String>,
        status_quo: Option<String>,
        eligible: Vec<String>,
        quorum: u32,
        deadline: Option<DateTime<Utc>>,
        tie_break: Option<TieBreak>,
        principle: Option<String>,
        rationale: Option<String>,
        result: Option<String>,
    },
    VoteCast {
        decision: String,
        unit: String,
        option: String,
    },
    VoteWithdraw {
        decision: String,
        unit: String,
    },
    VoteClose {
        decision: String,
    },
    VoteList {
        json: bool,
    },
    /// Recompute a decision's history and say whether it still verifies.
    VoteVerify {
        decision: String,
    },
    /// Put a vote that predates the ledger into it, with its provenance said
    /// out loud. The replies are the bytes that were actually cast; the rules
    /// section the row carries is reconstructed and marked as such.
    VoteBackfill {
        replies: String,
        subject: String,
        kind: String,
        options: Vec<String>,
        status_quo: String,
        deadline: DateTime<Utc>,
    },
}

/// Parse the arguments after `governance`.
pub fn parse_args(args: &[String]) -> Result<Act, String> {
    let family = args.first().ok_or("handover or vote is required")?;
    let sub = args.get(1).ok_or("a subcommand is required")?;
    let mut opts: Vec<(String, String)> = Vec::new();
    let mut flags: Vec<String> = Vec::new();
    let mut iter = args[2..].iter();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            flags.push(arg.clone());
            continue;
        }
        if !arg.starts_with("--") {
            return Err(format!("unexpected argument {arg}"));
        }
        let value = iter
            .next()
            .ok_or_else(|| format!("{arg} needs a value"))?
            .clone();
        opts.push((arg.clone(), value));
    }
    let one = |name: &str| -> Result<Option<String>, String> {
        let mut found = opts.iter().filter(|(k, _)| k == name);
        let first = found.next().map(|(_, v)| v.clone());
        if found.next().is_some() {
            return Err(format!("{name} may only be given once"));
        }
        Ok(first)
    };
    let many = |name: &str| -> Vec<String> {
        opts.iter()
            .filter(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .collect()
    };
    let required = |name: &str| -> Result<String, String> {
        one(name)?.ok_or_else(|| format!("{name} is required"))
    };

    match (family.as_str(), sub.as_str()) {
        ("handover", "open") => Ok(Act::HandoverOpen {
            face: required("--face")?,
            from: one("--from")?,
            to: required("--to")?,
            witness: required("--witness")?,
            source: one("--source")?,
            criterion: one("--criterion")?,
            reading_at: parse_time(&one("--reading-at")?)?,
            open_actions: many("--open-action"),
            permissions_context: one("--permissions-context")?,
            artifact: one("--artifact")?,
            artifact_expires_at: parse_time(&one("--artifact-expires")?)?,
            expires_when: parse_time(&one("--expires-when")?)?,
            withdraw_condition: one("--withdraw-condition")?,
        }),
        ("handover", "sign") => Ok(Act::HandoverSign {
            id: required("--id")?,
            unit: required("--unit")?,
        }),
        ("handover", "apply") => Ok(Act::HandoverApply {
            id: required("--id")?,
        }),
        ("handover", "withdraw") => Ok(Act::HandoverWithdraw {
            id: required("--id")?,
            unit: required("--unit")?,
        }),
        ("handover", "list") => Ok(Act::HandoverList {
            face: one("--face")?,
        }),
        ("vote", "open") => {
            let quorum = match one("--quorum")? {
                Some(raw) => raw
                    .parse::<u32>()
                    .map_err(|e| format!("--quorum is not a number: {e}"))?,
                None => 0,
            };
            let tie_break = match one("--tie-break")? {
                Some(raw) => Some(
                    TieBreak::parse(&raw).ok_or_else(|| format!("unknown --tie-break {raw}"))?,
                ),
                None => None,
            };
            let eligible: Vec<String> = match one("--eligible")? {
                Some(raw) => raw
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                None => Vec::new(),
            };
            Ok(Act::VoteOpen {
                subject: required("--subject")?,
                kind: required("--kind")?,
                options: many("--option"),
                status_quo: one("--status-quo")?,
                eligible,
                quorum,
                deadline: parse_time(&one("--deadline")?)?,
                tie_break,
                principle: one("--principle")?,
                rationale: one("--rationale")?,
                result: one("--result")?,
            })
        }
        ("vote", "cast") => Ok(Act::VoteCast {
            decision: required("--decision")?,
            unit: required("--unit")?,
            option: required("--option")?,
        }),
        ("vote", "withdraw") => Ok(Act::VoteWithdraw {
            decision: required("--decision")?,
            unit: required("--unit")?,
        }),
        ("vote", "close") => Ok(Act::VoteClose {
            decision: required("--decision")?,
        }),
        ("vote", "list") => Ok(Act::VoteList {
            json: flags.iter().any(|f| f == "--json"),
        }),
        ("vote", "verify") => Ok(Act::VoteVerify {
            decision: required("--decision")?,
        }),
        ("vote", "backfill") => Ok(Act::VoteBackfill {
            replies: required("--replies")?,
            subject: required("--subject")?,
            kind: required("--kind")?,
            options: many("--option"),
            status_quo: required("--status-quo")?,
            deadline: parse_time(&one("--deadline")?)?
                .ok_or_else(|| "--deadline is required for a backfilled ballot".to_string())?,
        }),
        ("handover" | "vote", other) => Err(format!("unknown {family} subcommand {other}")),
        (other, _) => Err(format!("unknown governance family {other}")),
    }
}

fn parse_time(raw: &Option<String>) -> Result<Option<DateTime<Utc>>, String> {
    match raw {
        None => Ok(None),
        Some(text) => Ok(Some(
            DateTime::parse_from_rfc3339(text)
                .map_err(|e| format!("not an rfc3339 time: {e}"))?
                .with_timezone(&Utc),
        )),
    }
}

/// Run one handover or vote command.
pub async fn run(
    act: Act,
    store: &dyn GovernanceStore,
    now: DateTime<Utc>,
) -> Result<(), Box<dyn Error>> {
    match act {
        Act::HandoverOpen {
            face,
            from,
            to,
            witness,
            source,
            criterion,
            reading_at,
            open_actions,
            permissions_context,
            artifact,
            artifact_expires_at,
            expires_when,
            withdraw_condition,
        } => {
            let spec = HandoverSpec {
                face_id: face.clone(),
                from_unit: from,
                to_unit: to.clone(),
                witness_unit: witness,
                source,
                criterion_reading: criterion,
                reading_at,
                open_actions,
                open_task_ids: Vec::new(),
                permissions_context,
                artifact_path: artifact,
                artifact_expires_at,
                expires_when,
                withdraw_condition,
                payload_sha256: None,
                signed_by: Vec::new(),
            };
            let id = format!("handover:{}:{}:{}", face, to, now.timestamp_millis());
            let task = handover_task(&id, spec, now);
            store.save_governance_node(&task).await?;
            println!("{id}");
        }
        Act::HandoverSign { id, unit } => {
            let mut task = find_node(store, HANDOVER_TASK_TYPE, &id).await?;
            let mut handover =
                Handover::from_task(&task).ok_or_else(|| format!("{id} is not a handover"))?;
            if handover.state() != HandoverState::Draft {
                return Err(format!("{id} is {}, not a draft", handover.state().as_str()).into());
            }
            if handover.spec.signed_by.iter().any(|s| s.unit == unit) {
                println!("{}: already signed by {unit}", handover.state().as_str());
                return Ok(());
            }
            // The first signature fixes the terms. A later signature over a
            // payload that has since changed would attest to something nobody
            // read, so refuse instead of quietly re-hashing.
            if !handover.spec.signature_covers_payload() {
                return Err(format!(
                    "{id} changed after it was signed; the terms must be re-read before another signature"
                )
                .into());
            }
            if handover.spec.payload_sha256.is_none() {
                handover.spec.payload_sha256 = Some(handover.spec.payload_digest());
            }
            let expected = task.status;
            handover.spec.signed_by.push(Signature {
                unit: unit.clone(),
                at: now,
            });
            let state = if handover.spec.is_fully_signed() {
                HandoverState::Signed
            } else {
                HandoverState::Draft
            };
            task.input
                .as_object_mut()
                .ok_or("the handover payload is not an object")?
                .insert(
                    cog_core::HANDOVER_INPUT_KEY.to_string(),
                    serde_json::to_value(&handover.spec)?,
                );
            task.status = state.status();
            task.updated_at = now;
            if !store
                .save_governance_node_if_status(&task, &[expected])
                .await?
            {
                return Err(format!("{id} changed while it was being signed; re-read it").into());
            }
            println!("{}: {}", id, state.as_str());
        }
        Act::HandoverApply { id } => {
            let mut task = find_node(store, HANDOVER_TASK_TYPE, &id).await?;
            let handover =
                Handover::from_task(&task).ok_or_else(|| format!("{id} is not a handover"))?;
            if handover.state() != HandoverState::Signed {
                return Err(format!(
                    "{id} is {}, not signed; nothing to apply",
                    handover.state().as_str()
                )
                .into());
            }
            // Applying a row whose terms no longer match its signatures would
            // move a face on the strength of documents the signers never saw.
            if !handover.spec.signature_covers_payload() {
                return Err(format!(
                    "{id} no longer matches what its signers attested to; refusing to apply"
                )
                .into());
            }
            let mut face_task =
                find_node(store, cog_core::FACE_TASK_TYPE, &handover.spec.face_id).await?;
            let face = cog_core::Face::from_task(&face_task)
                .ok_or_else(|| format!("{} is not a face", handover.spec.face_id))?;
            let expected_revision = face.revision();
            let mut spec = face.spec.clone();
            spec.revision += 1;
            // Filling a declared vacancy ends it. The three cells name who was
            // going to fill the seat, by when, and with which task; once
            // somebody is in the seat they describe a search that is over. Left
            // behind, `gap_task` keeps pointing at a closing task that may never
            // have existed while the row looks staffed and nobody re-checks it.
            if handover.spec.from_unit.is_none() {
                spec.vacancy_until = None;
                spec.vacancy_assigner = None;
                spec.gap_task_id = None;
            }
            face_task
                .input
                .as_object_mut()
                .ok_or("the face payload is not an object")?
                .insert(
                    cog_core::FACE_INPUT_KEY.to_string(),
                    serde_json::to_value(&spec)?,
                );
            face_task.owner_unit = Some(handover.spec.to_unit.clone());
            face_task.bound_by_handover_id = Some(id.clone());
            face_task.updated_at = now;

            task.status = HandoverState::Applied.status();
            task.result = Some(serde_json::json!({
                "outcome": "applied",
                "face_id": handover.spec.face_id,
                "to_unit": handover.spec.to_unit,
            }));
            task.updated_at = now;

            let Some(rebound) = store
                .apply_handover(&task, &face_task, expected_revision)
                .await?
            else {
                return Err(format!(
                    "{id} could not be applied: the face moved or the handover changed"
                )
                .into());
            };
            println!(
                "applied: {} now owned by {} (revision {}); {} unstarted task(s) rebound",
                handover.spec.face_id, handover.spec.to_unit, spec.revision, rebound
            );
        }
        Act::HandoverWithdraw { id, unit } => {
            let mut task = find_node(store, HANDOVER_TASK_TYPE, &id).await?;
            let handover =
                Handover::from_task(&task).ok_or_else(|| format!("{id} is not a handover"))?;
            if !matches!(
                handover.state(),
                HandoverState::Draft | HandoverState::Signed
            ) {
                return Err(format!("{id} is already {}", handover.state().as_str()).into());
            }
            let expected = task.status;
            task.status = HandoverState::Withdrawn.status();
            task.result = Some(serde_json::json!({
                "outcome": "withdrawn",
                "by_unit": unit,
            }));
            task.updated_at = now;
            if !store
                .save_governance_node_if_status(&task, &[expected])
                .await?
            {
                return Err(format!("{id} changed while it was being withdrawn").into());
            }
            println!("withdrawn: {id}");
        }
        Act::HandoverList { face } => {
            let mut rows: Vec<Handover> = store
                .governance_nodes(HANDOVER_TASK_TYPE)
                .await?
                .iter()
                .filter_map(Handover::from_task)
                .filter(|h| face.as_deref().map(|f| h.spec.face_id == f).unwrap_or(true))
                .collect();
            rows.sort_by(|a, b| a.id.cmp(&b.id));
            println!("id\tface\tfrom\tto\tstate\tsigned");
            for handover in &rows {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    handover.id,
                    handover.spec.face_id,
                    handover.spec.from_unit.as_deref().unwrap_or("-"),
                    handover.spec.to_unit,
                    handover.state().as_str(),
                    handover.spec.signed_by.len(),
                );
            }
            println!(
                "# generated_at={} handovers={}",
                now.to_rfc3339(),
                rows.len()
            );
        }
        Act::VoteOpen {
            subject,
            kind,
            options,
            status_quo,
            eligible,
            quorum,
            deadline,
            tie_break,
            principle,
            rationale,
            result,
        } => {
            let mode = if principle.is_some() {
                DecisionMode::Principle
            } else {
                DecisionMode::Ballot
            };
            let state = if mode == DecisionMode::Principle {
                DecisionState::Resolved
            } else {
                DecisionState::Open
            };
            let spec = DecisionSpec {
                kind: kind.clone(),
                subject: subject.clone(),
                options: options.clone(),
                status_quo,
                mode,
                state,
                eligible_units: eligible,
                quorum,
                tie_break,
                deadline,
                principle_slug: principle,
                principle_rationale: rationale,
                result_option: if mode == DecisionMode::Principle {
                    result
                } else {
                    None
                },
                result_reason: None,
                supersedes: None,
                provenance: Some("live".into()),
                predeclared: true,
            };
            match mode {
                DecisionMode::Ballot => validate_ballot(&spec)?,
                DecisionMode::Principle => validate_principle(&spec)?,
            }
            let id = format!("decision:{}:{}", kind, now.timestamp_millis());
            let task = decision_task(&id, spec.clone(), state.status(), now);
            store.save_governance_node(&task).await?;
            append_decision_event(
                store,
                &id,
                "opened",
                None,
                Some(&format!(
                    "mode={} state={}",
                    spec.mode.as_str(),
                    state.as_str()
                )),
                now,
            )
            .await?;
            println!("{id}");
        }
        Act::VoteCast {
            decision,
            unit,
            option,
        } => {
            let task = find_node(store, DECISION_TASK_TYPE, &decision).await?;
            let row = Decision::from_task(&task)
                .ok_or_else(|| format!("{decision} is not a decision"))?;
            if row.spec.state != DecisionState::Open {
                return Err(format!("{decision} is {}, not open", row.spec.state.as_str()).into());
            }
            if row.spec.mode != DecisionMode::Ballot {
                return Err(
                    format!("{decision} was settled by a principle; nothing to vote").into(),
                );
            }
            if !row.spec.options.iter().any(|o| o == &option) {
                return Err(format!("{option} is not one of this decision's options").into());
            }
            if !row.spec.eligible_units.iter().any(|u| u == &unit) {
                return Err(format!("{unit} is not on this decision's roster").into());
            }
            let vote = vote_task(&decision, &unit, &option, now);
            store.save_governance_node(&vote).await?;
            append_decision_event(
                store,
                &decision,
                "vote_cast",
                Some(&unit),
                Some(&option),
                now,
            )
            .await?;
            println!("{}: {unit} -> {option}", decision);
        }
        Act::VoteWithdraw { decision, unit } => {
            let decision_row = find_node(store, DECISION_TASK_TYPE, &decision).await?;
            let row = Decision::from_task(&decision_row)
                .ok_or_else(|| format!("{decision} is not a decision"))?;
            if row.spec.state != DecisionState::Open {
                return Err(format!("{decision} is {}, not open", row.spec.state.as_str()).into());
            }
            let id = vote_id(&decision, &unit);
            let mut vote = find_node(store, VOTE_TASK_TYPE, &id).await?;
            if vote.status == TaskStatus::Cancelled {
                println!("{id}: already withdrawn");
                return Ok(());
            }
            vote.status = TaskStatus::Cancelled;
            vote.updated_at = now;
            store.save_governance_node(&vote).await?;
            append_decision_event(store, &decision, "vote_withdrawn", Some(&unit), None, now)
                .await?;
            println!("{id}: withdrawn");
        }
        Act::VoteClose { decision } => {
            let mut task = find_node(store, DECISION_TASK_TYPE, &decision).await?;
            let mut row = Decision::from_task(&task)
                .ok_or_else(|| format!("{decision} is not a decision"))?;
            if row.spec.state != DecisionState::Open {
                return Err(format!("{decision} is already {}", row.spec.state.as_str()).into());
            }
            let votes: Vec<Vote> = store
                .governance_nodes(VOTE_TASK_TYPE)
                .await?
                .iter()
                .filter_map(Vote::from_task)
                .filter(|v| v.decision_id == decision)
                .collect();
            let outcome = close_ballot(&row.spec, &votes, now);
            if outcome.state == DecisionState::Open {
                println!("{decision}: open — {}", outcome.reason);
                return Ok(());
            }
            // Read the history before writing the verdict: a chain that does
            // not verify means somebody edited the record this close would be
            // the latest step in, and appending would vouch for the edit.
            match verify_decision_chain(store, &decision).await? {
                ChainVerdict::Intact { .. } => {}
                ChainVerdict::Broken { at_seq, reason } => {
                    return Err(format!(
                        "{decision} cannot be closed: its history is broken at seq {} ({reason}); \
                         re-read the record before resolving it",
                        at_seq
                            .map(|seq| seq.to_string())
                            .unwrap_or_else(|| "?".into())
                    )
                    .into())
                }
            }
            row.spec.state = outcome.state;
            row.spec.result_option = outcome.option.clone();
            row.spec.result_reason = Some(outcome.reason.clone());
            task.input
                .as_object_mut()
                .ok_or("the decision payload is not an object")?
                .insert(
                    cog_core::DECISION_INPUT_KEY.to_string(),
                    serde_json::to_value(&row.spec)?,
                );
            let expected = task.status;
            task.status = outcome.state.status();
            task.result = Some(serde_json::json!({
                "state": outcome.state.as_str(),
                "option": outcome.option,
                "reason": outcome.reason,
                "counts": outcome.tally.by_option,
                "voters": outcome.tally.voters,
            }));
            task.updated_at = now;
            // The verdict and the step that records it are one write: a row
            // that reads closed with no step for it, or a step for a close
            // that lost its race, would each be a history nobody can rebuild.
            let detail = format!(
                "{}: {}",
                row.spec.state.as_str(),
                row.spec
                    .result_reason
                    .clone()
                    .unwrap_or_else(|| "no reason recorded".into())
            );
            match close_decision_with_event(
                store,
                &decision,
                &task,
                &[expected],
                Some(&detail),
                now,
            )
            .await?
            {
                CloseOutcome::Closed => {}
                CloseOutcome::LostRace => {
                    return Err(format!("{decision} changed while it was being closed").into())
                }
                CloseOutcome::ChainBroken { at_seq, reason } => {
                    return Err(format!(
                        "{decision}: broken at seq {}: {reason}",
                        at_seq
                            .map(|seq| seq.to_string())
                            .unwrap_or_else(|| "?".into())
                    )
                    .into())
                }
            }
            println!(
                "{}: {} — {}",
                decision,
                outcome.state.as_str(),
                outcome.reason
            );
        }
        Act::VoteList { json } => {
            let decisions: Vec<_> = store
                .governance_nodes(DECISION_TASK_TYPE)
                .await?
                .iter()
                .filter_map(Decision::from_task)
                .collect();
            let votes: Vec<Vote> = store
                .governance_nodes(VOTE_TASK_TYPE)
                .await?
                .iter()
                .filter_map(Vote::from_task)
                .collect();
            if json {
                let rows: Vec<serde_json::Value> = decisions
                    .iter()
                    .map(|decision| {
                        let tally = tally(
                            &decision.spec,
                            &votes
                                .iter()
                                .filter(|v| v.decision_id == decision.id)
                                .cloned()
                                .collect::<Vec<_>>(),
                        );
                        serde_json::json!({
                            "id": decision.id,
                            "state": decision.spec.state.as_str(),
                            "mode": decision.spec.mode.as_str(),
                            "subject": decision.spec.subject,
                            "result_option": decision.spec.result_option,
                            "voters": tally.voters,
                            "counts": tally.by_option,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&rows)?);
                return Ok(());
            }
            println!("id\tstate\tmode\tresult\tvoters\tsubject");
            for decision in &decisions {
                let mine: Vec<Vote> = votes
                    .iter()
                    .filter(|v| v.decision_id == decision.id)
                    .cloned()
                    .collect();
                let tally = tally(&decision.spec, &mine);
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    decision.id,
                    decision.spec.state.as_str(),
                    decision.spec.mode.as_str(),
                    decision.spec.result_option.as_deref().unwrap_or("-"),
                    tally.voters.len(),
                    decision.spec.subject,
                );
            }
            println!(
                "# generated_at={} decisions={}",
                now.to_rfc3339(),
                decisions.len()
            );
        }
        Act::VoteVerify { decision } => {
            // The chain is the only thing that says the record was not edited
            // in place, so this arm just re-derives it and reports. A broken
            // chain is an error exit: a caller that checks the status must see
            // the edit, not a printed line it may not read.
            match verify_decision_chain(store, &decision).await? {
                ChainVerdict::Intact { events } => {
                    println!("{decision}: intact events={events}");
                }
                ChainVerdict::Broken { at_seq, reason } => {
                    return Err(format!(
                        "{decision}: broken at seq {}: {reason}",
                        at_seq
                            .map(|seq| seq.to_string())
                            .unwrap_or_else(|| "?".into())
                    )
                    .into())
                }
            }
        }
        Act::VoteBackfill {
            replies,
            subject,
            kind,
            options,
            status_quo,
            deadline,
        } => {
            // Each reply file is one unit's ballot: its name is the unit and
            // its first `vote:` line is the option. Reading the bytes instead
            // of a remembered tally is the point -- the remembered count is a
            // second ledger that would drift the moment somebody edits a file.
            let dir = std::path::Path::new(&replies);
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.ends_with("-vote.md"))
                })
                .collect();
            entries.sort();
            if entries.is_empty() {
                return Err(format!("no *-vote.md files under {}", dir.display()).into());
            }
            let mut replies_by_unit: Vec<(String, String)> = Vec::new();
            let mut cast_at: std::collections::BTreeMap<String, DateTime<Utc>> =
                std::collections::BTreeMap::new();
            for path in &entries {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or("a reply file name is not UTF-8")?;
                let unit = name.trim_end_matches("-vote.md").to_string();
                let text = std::fs::read_to_string(path)
                    .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
                let option = text
                    .lines()
                    .find_map(|line| line.strip_prefix("vote:"))
                    .map(str::trim)
                    .filter(|option| !option.is_empty())
                    .ok_or_else(|| {
                        format!("{} has no 'vote:' line; refusing to guess", path.display())
                    })?;
                let at = std::fs::metadata(path)
                    .and_then(|meta| meta.modified())
                    .map(DateTime::<Utc>::from)
                    .unwrap_or(now);
                replies_by_unit.push((unit.clone(), option.to_string()));
                cast_at.insert(unit, at);
            }
            let ballot = backfill_ballot(
                &kind,
                &subject,
                &options,
                Some(status_quo),
                deadline,
                &replies_by_unit,
            )
            .map_err(|e| -> Box<dyn Error> { e.into() })?;
            // The id is derived from the question and the deadline, so a rerun
            // is the same row rather than a second copy of the same vote.
            let id = format!(
                "decision:{}:backfill:{}",
                kind,
                deadline.format("%Y%m%dT%H%M%SZ")
            );
            if store
                .governance_nodes(DECISION_TASK_TYPE)
                .await?
                .iter()
                .any(|task| task.id == id)
            {
                println!("{id}: already backfilled; nothing written");
                return Ok(());
            }
            let state = ballot.spec.state;
            let task = decision_task(&id, ballot.spec.clone(), state.status(), now);
            store.save_governance_node(&task).await?;
            for (unit, option) in &ballot.votes {
                let at = cast_at.get(unit).copied().unwrap_or(now);
                store
                    .save_governance_node(&vote_task(&id, unit, option, at))
                    .await?;
            }
            // One step for the whole reconstruction rather than one per vote:
            // the votes are their own rows, and this event says the row was
            // assembled from files after the fact -- which is the fact a reader
            // must not mistake for a ballot that ran live.
            append_decision_event(
                store,
                &id,
                "backfilled",
                None,
                Some(&format!("votes={} predeclared=false", ballot.votes.len())),
                now,
            )
            .await?;
            let counts: Vec<String> = options
                .iter()
                .map(|option| {
                    format!(
                        "{option}={}",
                        ballot
                            .votes
                            .iter()
                            .filter(|(_, chosen)| chosen == option)
                            .count()
                    )
                })
                .collect();
            println!(
                "{}: {} — {} [{}] votes={} provenance=backfilled predeclared=false",
                id,
                state.as_str(),
                ballot.spec.result_reason.as_deref().unwrap_or("-"),
                counts.join(" "),
                ballot.votes.len(),
            );
        }
    }
    Ok(())
}

async fn find_node(
    store: &dyn GovernanceStore,
    task_type: &str,
    id: &str,
) -> Result<cog_core::Task, Box<dyn Error>> {
    store
        .governance_nodes(task_type)
        .await?
        .into_iter()
        .find(|task| task.id == id)
        .ok_or_else(|| format!("no {task_type} row named {id}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn every_subcommand_parses_with_its_operands() {
        assert!(matches!(
            parse_args(&args(&[
                "handover",
                "open",
                "--face",
                "f",
                "--to",
                "a",
                "--witness",
                "b"
            ]))
            .unwrap(),
            Act::HandoverOpen { .. }
        ));
        assert!(matches!(
            parse_args(&args(&["handover", "sign", "--id", "h", "--unit", "a"])).unwrap(),
            Act::HandoverSign { .. }
        ));
        assert!(matches!(
            parse_args(&args(&["handover", "apply", "--id", "h"])).unwrap(),
            Act::HandoverApply { .. }
        ));
        assert!(matches!(
            parse_args(&args(&["handover", "list"])).unwrap(),
            Act::HandoverList { .. }
        ));
        assert!(matches!(
            parse_args(&args(&[
                "vote",
                "open",
                "--subject",
                "s",
                "--kind",
                "k",
                "--option",
                "a",
                "--option",
                "b",
                "--status-quo",
                "a",
                "--eligible",
                "u1,u2",
                "--quorum",
                "1",
                "--deadline",
                "2026-10-20T00:00:00Z",
                "--tie-break",
                "status_quo"
            ]))
            .unwrap(),
            Act::VoteOpen { quorum: 1, .. }
        ));
        assert!(matches!(
            parse_args(&args(&[
                "vote",
                "cast",
                "--decision",
                "d",
                "--unit",
                "u",
                "--option",
                "a"
            ]))
            .unwrap(),
            Act::VoteCast { .. }
        ));
        assert!(matches!(
            parse_args(&args(&["vote", "list", "--json"])).unwrap(),
            Act::VoteList { json: true }
        ));
        assert!(matches!(
            parse_args(&args(&["vote", "verify", "--decision", "d"])).unwrap(),
            Act::VoteVerify { decision } if decision == "d"
        ));
        assert!(matches!(
            parse_args(&args(&[
                "vote",
                "backfill",
                "--replies",
                "/tmp/replies",
                "--subject",
                "s",
                "--kind",
                "k",
                "--option",
                "a",
                "--option",
                "b",
                "--status-quo",
                "a",
                "--deadline",
                "2026-10-20T00:00:00Z"
            ]))
            .unwrap(),
            Act::VoteBackfill { .. }
        ));
    }

    #[test]
    fn a_missing_operand_is_refused_rather_than_defaulted() {
        assert!(parse_args(&args(&["handover", "open", "--face", "f"])).is_err());
        assert!(parse_args(&args(&["vote", "open", "--subject", "s"])).is_err());
        assert!(parse_args(&args(&["vote", "cast", "--decision", "d"])).is_err());
        assert!(parse_args(&args(&["vote", "verify"])).is_err());
        assert!(parse_args(&args(&["vote", "backfill", "--subject", "s"])).is_err());
        assert!(parse_args(&args(&["nonsense", "list"])).is_err());
        assert!(parse_args(&args(&["vote"])).is_err());
    }

    #[test]
    fn a_ballot_missing_its_tie_break_or_quorum_is_refused_at_open_time() {
        let act = parse_args(&args(&[
            "vote",
            "open",
            "--subject",
            "s",
            "--kind",
            "k",
            "--option",
            "a",
            "--option",
            "b",
            "--status-quo",
            "a",
            "--eligible",
            "u1,u2",
            "--quorum",
            "1",
            "--deadline",
            "2026-10-20T00:00:00Z",
        ]))
        .unwrap();
        let Act::VoteOpen {
            options,
            status_quo,
            eligible,
            quorum,
            deadline,
            tie_break,
            ..
        } = act
        else {
            panic!("expected a vote open");
        };
        let spec = DecisionSpec {
            kind: "k".into(),
            subject: "s".into(),
            options,
            status_quo,
            mode: DecisionMode::Ballot,
            state: DecisionState::Open,
            eligible_units: eligible,
            quorum,
            tie_break,
            deadline,
            principle_slug: None,
            principle_rationale: None,
            result_option: None,
            result_reason: None,
            supersedes: None,
            provenance: None,
            predeclared: false,
        };
        assert!(validate_ballot(&spec).is_err());
    }

    #[test]
    fn a_principle_decision_without_its_basis_is_refused() {
        let act = parse_args(&args(&[
            "vote",
            "open",
            "--subject",
            "s",
            "--kind",
            "k",
            "--option",
            "a",
            "--option",
            "b",
            "--principle",
            "some-principle",
            "--result",
            "a",
        ]))
        .unwrap();
        let Act::VoteOpen {
            options,
            principle,
            rationale,
            result,
            ..
        } = act
        else {
            panic!("expected a vote open");
        };
        let spec = DecisionSpec {
            kind: "k".into(),
            subject: "s".into(),
            options,
            status_quo: None,
            mode: DecisionMode::Principle,
            state: DecisionState::Resolved,
            eligible_units: Vec::new(),
            quorum: 0,
            tie_break: None,
            deadline: None,
            principle_slug: principle,
            principle_rationale: rationale,
            result_option: result,
            result_reason: None,
            supersedes: None,
            provenance: None,
            predeclared: false,
        };
        assert!(validate_principle(&spec).is_err());
    }

    #[test]
    fn a_repeated_single_valued_option_is_refused() {
        assert!(parse_args(&args(&[
            "handover",
            "open",
            "--face",
            "f",
            "--face",
            "g",
            "--to",
            "a",
            "--witness",
            "b"
        ]))
        .is_err());
    }

    /// The rows a backfill writes, with no database behind them. What is under
    /// test is the reading of the reply files and the shape of the rows, not
    /// the SQL that carries them there.
    struct MemoryStore {
        rows: std::sync::Mutex<Vec<cog_core::Task>>,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self {
                rows: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn of_type(&self, task_type: &str) -> Vec<cog_core::Task> {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .filter(|task| task.task_type == cog_core::TaskType::Custom(task_type.into()))
                .cloned()
                .collect()
        }

        fn row(&self, id: &str) -> cog_core::Task {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .find(|task| task.id == id)
                .cloned()
                .expect("the row exists")
        }
    }

    #[async_trait::async_trait]
    impl cog_core::GovernanceStore for MemoryStore {
        async fn face_nodes(&self) -> cog_core::SFResult<Vec<cog_core::Task>> {
            Ok(self.of_type(cog_core::FACE_TASK_TYPE))
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
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(row)
                    if cog_core::Face::from_task(row)
                        .map(|face| face.revision() == expected)
                        .unwrap_or(false) =>
                {
                    *row = task.clone();
                    Ok(true)
                }
                _ => Ok(false),
            }
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
                Some(row) => *row = task.clone(),
                None => rows.push(task.clone()),
            }
            Ok(())
        }

        async fn save_governance_node_if_status(
            &self,
            task: &cog_core::Task,
            expected: &[cog_core::TaskStatus],
        ) -> cog_core::SFResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(row) if expected.contains(&row.status) => {
                    *row = task.clone();
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        async fn save_governance_node_with_event(
            &self,
            event: &cog_core::Task,
            node: &cog_core::Task,
            expected: &[cog_core::TaskStatus],
        ) -> cog_core::SFResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            let Some(slot) = rows.iter_mut().find(|row| row.id == node.id) else {
                return Ok(false);
            };
            if !expected.contains(&slot.status) {
                return Ok(false);
            }
            *slot = node.clone();
            match rows.iter_mut().find(|row| row.id == event.id) {
                Some(entry) => *entry = event.clone(),
                None => rows.push(event.clone()),
            }
            Ok(true)
        }

        async fn apply_handover(
            &self,
            handover: &cog_core::Task,
            face: &cog_core::Task,
            expected_revision: i64,
        ) -> cog_core::SFResult<Option<usize>> {
            let Some(spec) = cog_core::Handover::from_task(handover).map(|h| h.spec) else {
                return Ok(None);
            };
            let mut rows = self.rows.lock().unwrap();

            let stored_revision = rows
                .iter()
                .find(|row| row.id == face.id)
                .and_then(cog_core::Face::from_task)
                .map(|face| face.revision());
            if stored_revision != Some(expected_revision) {
                return Ok(None);
            }
            let Some(stored_handover) = rows.iter_mut().find(|row| row.id == handover.id) else {
                return Ok(None);
            };
            if stored_handover.status != cog_core::TaskStatus::Scheduled {
                return Ok(None);
            }
            *stored_handover = handover.clone();

            if let Some(stored_face) = rows.iter_mut().find(|row| row.id == face.id) {
                *stored_face = face.clone();
            }

            let mut rebound = 0;
            if let Some(from) = spec.from_unit.as_deref() {
                for row in rows.iter_mut() {
                    if row.id == face.id
                        || row.face_id.as_deref() != Some(spec.face_id.as_str())
                        || row.owner_unit.as_deref() != Some(from)
                        || !matches!(
                            row.status,
                            cog_core::TaskStatus::Pending | cog_core::TaskStatus::Scheduled
                        )
                    {
                        continue;
                    }
                    row.owner_unit = Some(spec.to_unit.clone());
                    row.bound_by_handover_id = Some(handover.id.clone());
                    // These name the old owner's worker; leaving them would keep
                    // a claim the new owner cannot act on.
                    row.agent_id = None;
                    row.lease_owner = None;
                    row.updated_at = handover.updated_at;
                    rebound += 1;
                }
            }
            Ok(Some(rebound))
        }
    }

    fn write_replies(dir: &std::path::Path, pairs: &[(&str, &str)]) {
        for (unit, option) in pairs {
            std::fs::write(
                dir.join(format!("{unit}-vote.md")),
                format!("vote: {option}\nthe reasons, which the ledger does not count\n"),
            )
            .unwrap();
        }
    }

    #[tokio::test]
    async fn a_backfill_reads_the_reply_files_and_a_rerun_writes_the_same_row_once() {
        let dir = tempfile::tempdir().unwrap();
        write_replies(
            dir.path(),
            &[("a", "operator"), ("b", "8e"), ("c", "operator")],
        );
        let act = Act::VoteBackfill {
            replies: dir.path().to_string_lossy().into_owned(),
            subject: "who approves writes".into(),
            kind: "who-approves-writes".into(),
            options: vec!["operator".into(), "8e".into(), "6e".into()],
            status_quo: "operator".into(),
            deadline: DateTime::parse_from_rfc3339("2026-10-19T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        };
        let store = MemoryStore::new();
        let now = Utc::now();
        run(act.clone(), &store, now).await.unwrap();

        let decisions = store.of_type(DECISION_TASK_TYPE);
        assert_eq!(decisions.len(), 1);
        assert_eq!(
            decisions[0].id,
            "decision:who-approves-writes:backfill:20261019T120000Z"
        );
        let decision = Decision::from_task(&decisions[0]).unwrap();
        assert_eq!(decision.spec.result_option.as_deref(), Some("operator"));
        assert_eq!(decision.spec.provenance.as_deref(), Some("backfilled"));

        let votes = store.of_type(VOTE_TASK_TYPE);
        assert_eq!(votes.len(), 3);
        let units: Vec<&str> = votes
            .iter()
            .filter_map(|vote| vote.owner_unit.as_deref())
            .collect();
        assert_eq!(units, vec!["a", "b", "c"]);

        run(act, &store, now).await.unwrap();
        assert_eq!(store.of_type(DECISION_TASK_TYPE).len(), 1);
        assert_eq!(store.of_type(VOTE_TASK_TYPE).len(), 3);
    }

    #[tokio::test]
    async fn a_backfill_refuses_a_file_with_no_vote_line_instead_of_guessing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a-vote.md"), "I have not decided yet\n").unwrap();
        let act = Act::VoteBackfill {
            replies: dir.path().to_string_lossy().into_owned(),
            subject: "s".into(),
            kind: "k".into(),
            options: vec!["operator".into(), "8e".into()],
            status_quo: "operator".into(),
            deadline: Utc::now(),
        };
        let store = MemoryStore::new();
        let error = run(act, &store, Utc::now()).await.unwrap_err();
        assert!(error.to_string().contains("vote:"), "{error}");
        assert!(store.of_type(DECISION_TASK_TYPE).is_empty());
    }

    #[tokio::test]
    async fn a_second_ballot_from_one_unit_changes_the_vote_rather_than_counting_twice() {
        let store = MemoryStore::new();
        let now = Utc::now();
        let spec = DecisionSpec {
            kind: "k".into(),
            subject: "s".into(),
            options: vec!["a".into(), "b".into()],
            status_quo: Some("a".into()),
            mode: DecisionMode::Ballot,
            state: DecisionState::Open,
            eligible_units: vec!["u".into()],
            quorum: 1,
            tie_break: Some(TieBreak::StatusQuo),
            deadline: Some(now + chrono::Duration::hours(1)),
            principle_slug: None,
            principle_rationale: None,
            result_option: None,
            result_reason: None,
            supersedes: None,
            provenance: None,
            predeclared: true,
        };
        store
            .save_governance_node(&decision_task("d", spec.clone(), TaskStatus::Pending, now))
            .await
            .unwrap();

        for option in ["a", "b"] {
            run(
                Act::VoteCast {
                    decision: "d".into(),
                    unit: "u".into(),
                    option: option.into(),
                },
                &store,
                now,
            )
            .await
            .unwrap();
        }

        let votes: Vec<Vote> = store
            .of_type(VOTE_TASK_TYPE)
            .iter()
            .filter_map(Vote::from_task)
            .collect();
        assert_eq!(votes.len(), 1, "a change of mind is not a second ballot");
        let tally = tally(&spec, &votes);
        assert_eq!(tally.voters, vec!["u".to_string()], "one unit, one ballot");
        assert_eq!(tally.by_option.get("b"), Some(&vec!["u".to_string()]));
        assert_eq!(tally.by_option.get("a"), None, "the first choice is gone");
    }

    #[tokio::test]
    async fn verifying_a_decision_passes_an_untouched_history_and_fails_an_edited_one() {
        let store = MemoryStore::new();
        let now = Utc::now();
        for detail in ["opened", "a vote was cast", "closed"] {
            cog_core::append_decision_event(&store, "d", "step", None, Some(detail), now)
                .await
                .unwrap();
        }
        run(
            Act::VoteVerify {
                decision: "d".into(),
            },
            &store,
            now,
        )
        .await
        .expect("a history nobody touched verifies");

        // The edit that matters is the one made in place: the event keeps its
        // position and its link, and only its words change.
        {
            let mut rows = store.rows.lock().unwrap();
            let event = rows
                .iter_mut()
                .find(|row| row.id.ends_with(":000001"))
                .expect("the second event");
            event.input[cog_core::DECISION_EVENT_INPUT_KEY]["detail"] =
                serde_json::json!("something that was never said");
        }

        let error = run(
            Act::VoteVerify {
                decision: "d".into(),
            },
            &store,
            now,
        )
        .await
        .expect_err("an edited event must not verify");
        assert!(error.to_string().contains("broken at seq 1"), "{error}");
    }

    #[tokio::test]
    async fn applying_a_handover_moves_the_face_and_the_work_that_had_not_started() {
        let store = MemoryStore::new();
        let now = Utc::now();
        let face_id = "\u{4efb}\u{52a1}\u{7ba1}\u{7406}";

        let face = cog_core::Face {
            face_id: face_id.to_string(),
            owner_unit: Some("worker".into()),
            bound_by_handover_id: None,
            spec: cog_core::FaceSpec::first(face_id, cog_core::FaceKind::Face),
            updated_at: now,
        };
        store
            .save_governance_node(&face.to_task(None))
            .await
            .unwrap();

        let work = |id: &str, status| {
            let mut task = cog_core::Task::new(
                id,
                cog_core::TaskType::Custom("code".into()),
                serde_json::json!({}),
            );
            task.status = status;
            task.face_id = Some(face_id.to_string());
            task.owner_unit = Some("worker".into());
            task
        };
        let pending = work("work-pending", cog_core::TaskStatus::Pending);
        store.save_governance_node(&pending).await.unwrap();
        let mut running = work("work-running", cog_core::TaskStatus::Running);
        running.agent_id = Some("agent-of-worker".into());
        running.lease_owner = Some("worker-process".into());
        store.save_governance_node(&running).await.unwrap();

        run(
            Act::HandoverOpen {
                face: face_id.to_string(),
                from: Some("worker".into()),
                to: "6e".into(),
                witness: "8b2".into(),
                source: None,
                criterion: None,
                reading_at: None,
                open_actions: Vec::new(),
                permissions_context: None,
                artifact: None,
                artifact_expires_at: None,
                expires_when: Some(now + chrono::Duration::days(1)),
                withdraw_condition: None,
            },
            &store,
            now,
        )
        .await
        .unwrap();
        let id = store.of_type(HANDOVER_TASK_TYPE)[0].id.clone();
        for unit in ["worker", "6e", "8b2"] {
            run(
                Act::HandoverSign {
                    id: id.clone(),
                    unit: unit.into(),
                },
                &store,
                now,
            )
            .await
            .unwrap();
        }
        run(Act::HandoverApply { id: id.clone() }, &store, now)
            .await
            .unwrap();

        assert_eq!(store.row(&id).status, cog_core::TaskStatus::Completed);
        let moved = cog_core::Face::from_task(&store.row(face_id)).unwrap();
        assert_eq!(moved.owner_unit.as_deref(), Some("6e"));
        assert_eq!(moved.revision(), 2);
        assert_eq!(moved.bound_by_handover_id.as_deref(), Some(id.as_str()));

        // Unstarted work follows the face, and stops carrying the old owner's
        // worker, which the new owner could not execute it with.
        let followed = store.row("work-pending");
        assert_eq!(followed.owner_unit.as_deref(), Some("6e"));
        assert_eq!(followed.bound_by_handover_id.as_deref(), Some(id.as_str()));
        assert_eq!(followed.agent_id, None);

        // A run in flight is not moved under its lease.
        let still_running = store.row("work-running");
        assert_eq!(still_running.owner_unit.as_deref(), Some("worker"));
        assert_eq!(still_running.lease_owner.as_deref(), Some("worker-process"));

        // Until that run ends, the face is red for the right reason; after it
        // does, the reading is green without anyone editing the ledger.
        let in_flight = vec![followed.clone(), still_running];
        let reading = cog_core::observe(cog_core::FaceObservation {
            face_id,
            face: Some(&moved),
            open_tasks: &in_flight,
            signed_handovers: &[],
            owner_liveness: cog_core::OwnerLiveness::Seen(now),
            now,
        });
        assert_eq!(reading.red_rules, vec!["drifted_binding".to_string()]);

        let settled = vec![followed];
        let reading = cog_core::observe(cog_core::FaceObservation {
            face_id,
            face: Some(&moved),
            open_tasks: &settled,
            signed_handovers: &[],
            owner_liveness: cog_core::OwnerLiveness::Seen(now),
            now,
        });
        assert_eq!(reading.state, cog_core::DriftState::OnTarget);
    }

    // Importing the same roster row twice must be a no-op that says so. The
    // `Unchanged` outcome is what a caller uses to report "nothing moved"; if
    // the timestamp were stamped before the comparison, every import would
    // write and the promise would only hold in effect, never in the answer.
    #[tokio::test]
    async fn importing_the_same_row_twice_is_unchanged_and_writes_nothing() {
        let store = MemoryStore::new();
        let now = Utc::now();
        let face = |at| cog_core::Face {
            face_id: "face".into(),
            owner_unit: Some("b4".into()),
            bound_by_handover_id: None,
            spec: cog_core::FaceSpec::first("face", cog_core::FaceKind::Face),
            updated_at: at,
        };

        let first = cog_core::import_face(&store, &face(now)).await.unwrap();
        assert_eq!(first, cog_core::FaceImportOutcome::Inserted);
        let before = store.row("face").updated_at;

        let again = cog_core::import_face(&store, &face(now)).await.unwrap();
        assert_eq!(again, cog_core::FaceImportOutcome::Unchanged);
        assert_eq!(store.row("face").updated_at, before);
    }

    #[tokio::test]
    async fn filling_a_vacancy_clears_the_search_that_was_looking_for_someone() {
        let store = MemoryStore::new();
        let now = Utc::now();
        let face_id = "\u{5b89}\u{5168}\u{4e0e}\u{51ed}\u{8bc1}";

        let mut spec = cog_core::FaceSpec::first(face_id, cog_core::FaceKind::Face);
        spec.vacancy_until = Some(now + chrono::Duration::days(1));
        spec.vacancy_assigner = Some("operator".into());
        // A gap task that was never created: the import path checks only that
        // the three cells are non-empty, so a name that resolves to nothing
        // reads exactly like a real one.
        spec.gap_task_id = Some("SEC-BASELINE".into());
        let face = cog_core::Face {
            face_id: face_id.to_string(),
            owner_unit: None,
            bound_by_handover_id: None,
            spec,
            updated_at: now,
        };
        store
            .save_governance_node(&face.to_task(None))
            .await
            .unwrap();

        let id = run(
            Act::HandoverOpen {
                face: face_id.to_string(),
                from: None,
                to: "b4".into(),
                witness: "38".into(),
                source: None,
                criterion: None,
                reading_at: None,
                open_actions: Vec::new(),
                permissions_context: None,
                artifact: None,
                artifact_expires_at: None,
                expires_when: Some(now + chrono::Duration::days(1)),
                withdraw_condition: None,
            },
            &store,
            now,
        )
        .await
        .map(|_| store.of_type(HANDOVER_TASK_TYPE)[0].id.clone())
        .unwrap();
        for unit in ["b4", "38"] {
            run(
                Act::HandoverSign {
                    id: id.clone(),
                    unit: unit.into(),
                },
                &store,
                now,
            )
            .await
            .unwrap();
        }
        run(Act::HandoverApply { id }, &store, now).await.unwrap();

        let filled = cog_core::Face::from_task(&store.row(face_id)).unwrap();
        assert_eq!(filled.owner_unit.as_deref(), Some("b4"));
        assert!(!filled.spec.has_named_vacancy());
        assert_eq!(filled.spec.vacancy_until, None);
        assert_eq!(filled.spec.vacancy_assigner, None);
        assert_eq!(filled.spec.gap_task_id, None);
        assert!(filled.is_staffed());
        assert!(!filled.vacancy_expired_at(now + chrono::Duration::days(2)));
    }

    // The seven cells include "what the new owner inherits as unfinished
    // business" and "anything they need to know about permissions". Neither
    // had a flag, so both could only be written into a free-text field that
    // the next reader has no reason to search.
    #[test]
    fn a_handover_carries_its_open_actions_and_permissions() {
        let parsed = parse_args(&args(&[
            "handover",
            "open",
            "--face",
            "f",
            "--to",
            "a",
            "--witness",
            "b",
            "--open-action",
            "first",
            "--open-action",
            "second",
            "--permissions-context",
            "p",
        ]))
        .unwrap();
        match parsed {
            Act::HandoverOpen {
                open_actions,
                permissions_context,
                ..
            } => {
                assert_eq!(
                    open_actions,
                    vec!["first".to_string(), "second".to_string()]
                );
                assert_eq!(permissions_context.as_deref(), Some("p"));
            }
            other => panic!("expected a handover open, got {other:?}"),
        }
    }
}
