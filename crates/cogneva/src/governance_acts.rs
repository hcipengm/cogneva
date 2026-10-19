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
    Decision, DecisionMode, DecisionSpec, DecisionState, GovernanceStore, Handover, HandoverSpec,
    HandoverState, Signature, TaskStatus, TieBreak, Vote, DECISION_TASK_TYPE, HANDOVER_TASK_TYPE,
    VOTE_TASK_TYPE,
};
use cog_orchestrator::governance_acts::{
    close_ballot, decision_task, handover_task, tally, validate_ballot, validate_principle,
    vote_id, vote_task,
};

pub const USAGE: &str = "\
cogneva governance <handover|vote> <SUBCOMMAND> [OPTIONS]

HANDOVER — moving a face from one unit to another. All three units must sign.
    open --face <face> --to <unit> --witness <unit> [--from <unit>]
         [--criterion <text>] [--expires-when <rfc3339>]
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
    list [--json]";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Act {
    HandoverOpen {
        face: String,
        from: Option<String>,
        to: String,
        witness: String,
        criterion: Option<String>,
        expires_when: Option<DateTime<Utc>>,
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
            criterion: one("--criterion")?,
            expires_when: parse_time(&one("--expires-when")?)?,
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
            criterion,
            expires_when,
        } => {
            let spec = HandoverSpec {
                face_id: face.clone(),
                from_unit: from,
                to_unit: to.clone(),
                witness_unit: witness,
                criterion_reading: criterion,
                reading_at: None,
                open_actions: Vec::new(),
                open_task_ids: Vec::new(),
                permissions_context: None,
                expires_when,
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
            let mut face_task =
                find_node(store, cog_core::FACE_TASK_TYPE, &handover.spec.face_id).await?;
            let face = cog_core::Face::from_task(&face_task)
                .ok_or_else(|| format!("{} is not a face", handover.spec.face_id))?;
            let expected_revision = face.revision();
            let mut spec = face.spec.clone();
            spec.revision += 1;
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

            if !store
                .apply_handover(&task, &face_task, expected_revision)
                .await?
            {
                return Err(format!(
                    "{id} could not be applied: the face moved or the handover changed"
                )
                .into());
            }
            println!(
                "applied: {} now owned by {} (revision {})",
                handover.spec.face_id, handover.spec.to_unit, spec.revision
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
            };
            match mode {
                DecisionMode::Ballot => validate_ballot(&spec)?,
                DecisionMode::Principle => validate_principle(&spec)?,
            }
            let id = format!("decision:{}:{}", kind, now.timestamp_millis());
            let task = decision_task(&id, spec, state.status(), now);
            store.save_governance_node(&task).await?;
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
            if !store
                .save_governance_node_if_status(&task, &[expected])
                .await?
            {
                return Err(format!("{decision} changed while it was being closed").into());
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
    }

    #[test]
    fn a_missing_operand_is_refused_rather_than_defaulted() {
        assert!(parse_args(&args(&["handover", "open", "--face", "f"])).is_err());
        assert!(parse_args(&args(&["vote", "open", "--subject", "s"])).is_err());
        assert!(parse_args(&args(&["vote", "cast", "--decision", "d"])).is_err());
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
}
