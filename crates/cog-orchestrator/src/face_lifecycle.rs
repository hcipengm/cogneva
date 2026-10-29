//! Faces earn their place by coverage: nominate one when unclaimed work piles up,
//! propose retiring one that has held nothing for a reporting period.
//!
//! A face is a heading in the account, and a heading is only worth its line when
//! work is filed under it. The census already measures the two halves that
//! answer this -- how much work is filed under no face at all, and how much each
//! named face holds -- so the decision is read off the account rather than
//! declared somewhere else. Nothing here writes a face list: a new face is born
//! the pass a document first names it (the column carries the name, not a
//! membership), and a face that has stopped being named stops being counted.
//!
//! What this adds is the step a reading cannot take by itself: a goal, so that
//! somebody is asked. Two findings, one goal each per day, submitted through the
//! same entry every other goal uses -- the action planner decomposes them and
//! the ordinary executors carry the pieces. The ids are derived from the day and
//! the face rather than being random, so a pass that runs every few minutes does
//! not ask the same question forty times an hour, while tomorrow's pass asks
//! again: a face still without work tomorrow is a finding nobody acted on, not
//! the same submission.
//!
//! Neither threshold is a number picked here. Unclaimed work is worth a heading
//! of its own when it is larger than the pile of any single named face -- the
//! account says what "large" means this week, and the rule keeps holding as it
//! grows. A face is done rather than idle when none of its rows has moved for a
//! full reporting period, which is the unit the account is already projected and
//! reported in.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use cog_core::{BlockCensus, ChangeBlock, OrchestratorControl, PersistedAlert, Task};

/// How long a face has to hold nothing before it is proposed for retirement.
///
/// One day, for the reason the threshold below is a comparison rather than a
/// number: the account is projected into a day file and its remediation goals
/// are one per face per day, so a day is the period this account already has an
/// answer for. A face whose last row moved inside that period may simply not
/// have been written to yet today.
pub const FACE_RETIRE_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// The goal id for one day's "name a face for the unclaimed work".
pub fn propose_goal_id(day: &str) -> String {
    format!("face-propose-{day}")
}

/// The goal id for one day's retirement proposal about one face.
pub fn retire_goal_id(face: &str, day: &str) -> String {
    format!("face-retire-{}-{day}", crate::face_drift::face_slug(face))
}

/// Which reading a pile of unclaimed work was counted in.
///
/// The name travels with the finding because the answer is different for each:
/// a change row is filed under a heading by editing one document, while a rule
/// that fires with nobody's name on it is handed to a face by naming it where
/// the rule is written. Whoever is asked needs to know which of the two piles
/// they are being asked about.
pub const LEDGER_SOURCE: &str = "变更账";
/// Firing alerts that carry no face.
pub const ALERTS_SOURCE: &str = "告警";

/// A pile of work that nobody's name is on, and the yardstick it is measured
/// against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nomination {
    /// The reading this pile was counted in.
    pub source: &'static str,
    /// Items in that reading filed under no face at all.
    pub uncovered: u64,
    /// What the largest named face holds in the same reading, for the reader who
    /// wants to check the comparison rather than take the verdict.
    pub largest_named: u64,
    /// How long the least-recently-observed of those items has waited.
    pub oldest_unmeasured_secs: Option<u64>,
}

impl Nomination {
    /// Whether this pile is large enough to be worth a heading of its own.
    ///
    /// Larger than every named face **of the same reading**, not merely
    /// non-zero: one unreported item is a document that forgot a column, and a
    /// heading invented for it would be work about work. The bar is that
    /// reading's own spread, so it rises with the account instead of being a
    /// number that was right once -- and two readings of different things (rows
    /// of an account, alerts that are ringing now) never share a bar.
    fn worth_a_heading(&self) -> bool {
        self.uncovered > 0 && self.uncovered > self.largest_named
    }
}

/// A face that has held nothing for a full reporting period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retirement {
    pub face: ChangeBlock,
    /// Rows the face still has, all of them retired: the count says the heading
    /// is not merely unwritten, it is finished.
    pub retired: u64,
    /// How long since anything in the face moved.
    pub idle_secs: Option<u64>,
}

/// What this pass would ask for, read from the census alone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaceLifecycle {
    pub nominate: Option<Nomination>,
    pub retire: Vec<Retirement>,
}

/// The pile the change account itself carries: rows filed under no face.
fn ledger_pile(census: &[BlockCensus]) -> Option<Nomination> {
    let uncovered = census.iter().find(|face| !face.block.is_named())?;
    Some(Nomination {
        source: LEDGER_SOURCE,
        uncovered: uncovered.owed,
        largest_named: census
            .iter()
            .filter(|face| face.block.is_named())
            .map(|face| face.owed)
            .max()
            .unwrap_or(0),
        oldest_unmeasured_secs: uncovered.oldest_unmeasured_secs,
    })
}

/// The pile the alert surface carries: firing alerts with no face on them.
///
/// Only *firing* alerts count. A rule that is silent is not work anybody owes,
/// and counting every rule that names no face would nominate a heading for a
/// rulebook rather than for work. The pile is the firing alerts with no face;
/// the yardstick is the largest pile any one named face holds, so the two sides
/// of the comparison are the same kind of thing.
///
/// The face comes from the label the mirror writes when the rule declared one.
/// A label that is present but empty is the same as absent -- an empty name is
/// exactly what the writer drops rather than writes.
pub fn alert_pile(alerts: &[PersistedAlert], now: DateTime<Utc>) -> Nomination {
    let mut named: std::collections::HashMap<&str, u64> = std::collections::HashMap::new();
    let mut uncovered = 0u64;
    let mut oldest: Option<u64> = None;
    for alert in alerts {
        let face = alert
            .labels
            .get(cog_core::alerts::ALERT_FACE_LABEL)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|face| !face.is_empty());
        match face {
            Some(face) => *named.entry(face).or_default() += 1,
            None => {
                uncovered += 1;
                let age = now
                    .signed_duration_since(alert.fired_at)
                    .num_seconds()
                    .max(0) as u64;
                oldest = Some(oldest.map_or(age, |seen: u64| seen.max(age)));
            }
        }
    }
    Nomination {
        source: ALERTS_SOURCE,
        uncovered,
        largest_named: named.values().copied().max().unwrap_or(0),
        oldest_unmeasured_secs: oldest,
    }
}

/// Read the two findings off the census, plus whatever the alert surface said.
///
/// A census that could not be read is `None` at the call site rather than an
/// empty slice, and neither is the same as an account with nothing in it: the
/// empty slice here means the store answered and the answer was "no faces",
/// which cannot happen while `none` is always printed -- so anything that
/// arrives empty is treated as nothing to say rather than as a finding.
///
/// The two piles are compared separately and the larger winner is returned: one
/// counts rows of the account, the other counts alerts ringing now, and a count
/// of one is not a bar for the other. Whichever pile is bigger is the one worth
/// somebody's attention first.
pub fn read_face_lifecycle(census: &[BlockCensus], alerts: Option<&Nomination>) -> FaceLifecycle {
    let nominate = ledger_pile(census)
        .into_iter()
        .chain(alerts.cloned())
        .filter(Nomination::worth_a_heading)
        .max_by_key(|pile| pile.uncovered);
    let retire = census
        .iter()
        .filter(|face| face.block.is_named())
        .filter(|face| face.owed == 0 && face.landed == 0 && face.retired > 0)
        .filter(|face| {
            face.newest_activity_secs
                .is_some_and(|secs| secs >= FACE_RETIRE_WINDOW.as_secs())
        })
        .map(|face| Retirement {
            face: face.block.clone(),
            retired: face.retired,
            idle_secs: face.newest_activity_secs,
        })
        .collect();
    FaceLifecycle { nominate, retire }
}

/// The text a nomination is handed to the planner with.
///
/// It names the count and the wait and stops there: deciding which heading the
/// unclaimed rows belong under is the work, and a loop that already knew the
/// answer would be doing the reading it exists to delegate.
pub fn propose_goal_text(nomination: &Nomination) -> String {
    let waited = match nomination.oldest_unmeasured_secs {
        Some(secs) => format!("{} 秒", secs),
        None => "未知（这些条目都已量过，或从未量过）".to_string(),
    };
    // The two readings are cleared in different places, so the ask names the
    // one this pile came from rather than describing both and leaving the
    // reader to guess which half they are being asked about.
    let what = match nomination.source {
        ALERTS_SOURCE => {
            "有若干条**正在响的告警**没有挂面：没有面就没有人去动它，响着的规则和没写的规则在这里\
             是同一件事。请读这些告警的规则与消息，找出它们共同关心的那个面：如果确实构成一个新面，\
             就在写这些规则的地方把它补成一个面名；如果它们分属已有面，就在规则上补上漏写的面。"
                .to_string()
        }
        _ => "请读这些行的文档与标题，找出它们共同关心的那个面：如果确实构成一个新面，就在提出这些\
              变更的文档里写下那个面名——账认名字本身，不需要改代码、不需要迁移；\
              如果它们分属已有面，就补上那些文档漏写的面。"
            .to_string(),
    };
    format!(
        "Name a face for the unclaimed work\n\n\
         {} 里有 {} 条不属于任何面，比同一个读数里任何一个具名面此刻持有的都多（最大的具名面 {} 条）；\
         其中最久的那条已经等了 {}。{}\
         做完之后，这个数应当落到任何具名面之下。",
        nomination.source, nomination.uncovered, nomination.largest_named, waited, what,
    )
}

/// The text a retirement proposal is handed to the planner with.
pub fn retire_goal_text(retirement: &Retirement) -> String {
    let idle = match retirement.idle_secs {
        Some(secs) => format!("{} 秒", secs),
        None => "未知".to_string(),
    };
    format!(
        "Retire face \"{}\" or give it work\n\n\
         面「{}」名下 {} 条变更全部已退场，没有一条还在欠账或已完成；最近一次动静在 {} 之前，\
         已经超过一个完整的报告周期。一个面存在的理由是它有活儿：\
         如果这一面确实做完了，就把它从面册里退掉；如果它还有该做的事，就把那些事写进文档。\
         两种答案都要能让下一拍读数看到它变了——继续挂着才是唯一不对的答案。",
        retirement.face.as_str(),
        retirement.face.as_str(),
        retirement.retired,
        idle,
    )
}

/// The row a finding is submitted under.
///
/// Never stamped with a face, not even the retirement's own: the row would then
/// count as work on that heading, and a proposal about a face with no work would
/// be the work that keeps it alive. The row is the goal's identity marker and is
/// never executed; the planner decomposes it into the steps that actually answer
/// the finding.
fn lifecycle_row(id: String, text: &str) -> Task {
    let mut row = Task::goal_row(id, text);
    row.is_executable = false;
    row
}

/// Submit this pass's findings, at most once a day each.
///
/// `census` is `None` when the store could not be read: a failed read asks for
/// nothing, because "nobody could look" and "nothing is there" are different
/// findings and only the second is one of these.
pub async fn dispatch_face_lifecycle(
    orchestrator: &dyn OrchestratorControl,
    census: Option<&[BlockCensus]>,
    alerts: Option<&Nomination>,
    now: DateTime<Utc>,
) {
    // The retirement question is the account's alone: a face stops being worth
    // its line when nothing in the account moves, and the alert surface has no
    // rows to ask that about. So a pass with no census asks nothing -- but it
    // still must not skip a pile of unowned alerts it can see.
    let empty: &[BlockCensus] = &[];
    let plan = read_face_lifecycle(census.unwrap_or(empty), alerts);
    if plan.nominate.is_none() && plan.retire.is_empty() {
        return;
    }
    let day = now.format(cog_core::DAY_KEY_FORMAT).to_string();
    if let Some(ref nomination) = plan.nominate {
        let id = propose_goal_id(&day);
        submit(
            orchestrator,
            id.clone(),
            &propose_goal_text(nomination),
            "unclaimed work",
        )
        .await;
    }
    for retirement in &plan.retire {
        let id = retire_goal_id(retirement.face.as_str(), &day);
        submit(
            orchestrator,
            id.clone(),
            &retire_goal_text(retirement),
            retirement.face.as_str(),
        )
        .await;
    }
    info!(
        nominated = plan.nominate.is_some(),
        retiring = plan.retire.len(),
        "face lifecycle: this pass asked the account's two questions"
    );
}

async fn submit(orchestrator: &dyn OrchestratorControl, id: String, text: &str, what: &str) {
    match orchestrator
        .submit_goal_auto(text, vec![lifecycle_row(id.clone(), text)])
        .await
    {
        Ok(added) if added.is_empty() => {
            info!(goal_id = %id, what, "face lifecycle: today's question is already queued")
        }
        Ok(added) => {
            info!(goal_id = %id, what, tasks = added.len(), "face lifecycle: handed to the action planner")
        }
        Err(e) => warn!(
            goal_id = %id,
            what,
            error = %e,
            "face lifecycle: the question could not be submitted; the census still reads as it was"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn face(block: &str, landed: u64, owed: u64, retired: u64, idle: Option<u64>) -> BlockCensus {
        BlockCensus {
            block: ChangeBlock::named(block).expect("a face name"),
            landed,
            owed,
            retired,
            undecided: 0,
            oldest_unmeasured_secs: None,
            newest_activity_secs: idle,
        }
    }

    const DAY: u64 = 24 * 60 * 60;

    #[test]
    fn unclaimed_work_is_nominated_only_when_it_outgrows_every_named_face() {
        // More unclaimed rows than the biggest face holds: the account is
        // saying the headings no longer cover the work.
        let plan = read_face_lifecycle(
            &[
                face("none", 0, 9, 0, Some(0)),
                face("tasks", 10, 8, 2, Some(0)),
                face("token", 0, 3, 0, Some(0)),
            ],
            None,
        );
        assert_eq!(
            plan.nominate,
            Some(Nomination {
                source: LEDGER_SOURCE,
                uncovered: 9,
                largest_named: 8,
                oldest_unmeasured_secs: None
            })
        );

        // Equal is not larger: a pile the same size as a face nobody made a
        // heading for is a document that forgot a column, not a missing face.
        let plan = read_face_lifecycle(
            &[
                face("none", 0, 8, 0, Some(0)),
                face("tasks", 0, 8, 0, Some(0)),
            ],
            None,
        );
        assert_eq!(plan.nominate, None);

        // Nothing unclaimed at all.
        let plan = read_face_lifecycle(
            &[
                face("none", 4, 0, 1, Some(0)),
                face("tasks", 0, 1, 0, Some(0)),
            ],
            None,
        );
        assert_eq!(plan.nominate, None);

        // No named face at all: every row is unclaimed, so there is more
        // unclaimed work than any face holds -- this is an account whose
        // headings have not been written yet, and the first one is what it
        // needs.
        let plan = read_face_lifecycle(&[face("none", 0, 1, 0, Some(0))], None);
        assert_eq!(
            plan.nominate,
            Some(Nomination {
                source: LEDGER_SOURCE,
                uncovered: 1,
                largest_named: 0,
                oldest_unmeasured_secs: None
            })
        );

        // An account with no rows at all has nothing to say: `none` is always
        // printed by the store, so an empty census is a reader that was handed
        // nothing rather than an account that holds nothing.
        assert_eq!(read_face_lifecycle(&[], None).nominate, None);
    }

    fn alert(face: Option<&str>, age_secs: i64) -> PersistedAlert {
        let mut labels = serde_json::Map::new();
        if let Some(face) = face {
            labels.insert(
                cog_core::alerts::ALERT_FACE_LABEL.to_string(),
                serde_json::Value::String(face.to_string()),
            );
        }
        PersistedAlert {
            rule: "some_rule".into(),
            dedup_key: format!("some_rule/{face:?}/{age_secs}"),
            severity: "warning".into(),
            state: "firing".into(),
            message: "something is wrong".into(),
            labels: serde_json::Value::Object(labels),
            fired_at: Utc::now() - chrono::Duration::seconds(age_secs),
            last_seen_at: None,
            acknowledged_at: None,
            acknowledged_by: None,
        }
    }

    #[test]
    fn firing_alerts_with_no_face_are_counted_against_the_faces_that_do_carry_one() {
        let pile = alert_pile(
            &[
                alert(None, 30),
                alert(Some(""), 10),
                alert(Some("  "), 10),
                alert(Some("tasks"), 5),
                alert(Some("tasks"), 5),
                alert(Some("observability"), 5),
            ],
            Utc::now(),
        );
        // Absent, empty and blank all mean the same thing to the writer that
        // stores these rows -- no name -- so they are one pile here too.
        assert_eq!(pile.source, ALERTS_SOURCE);
        assert_eq!(pile.uncovered, 3);
        assert_eq!(pile.largest_named, 2);
        // The oldest unowned alert is the one that has waited longest with
        // nobody's name on it; a named one's age says nothing about that wait.
        let waited = pile.oldest_unmeasured_secs.expect("two unclaimed ages");
        assert!((25..=30).contains(&waited), "waited {waited}");

        // Nothing ringing means nothing to nominate, not a division by zero.
        let quiet = alert_pile(&[], Utc::now());
        assert_eq!(quiet.uncovered, 0);
        assert_eq!(quiet.oldest_unmeasured_secs, None);

        // A rulebook is not a pile of work: named alerts alone never nominate.
        let owned = alert_pile(&[alert(Some("tasks"), 5), alert(Some("s"), 5)], Utc::now());
        assert_eq!(owned.uncovered, 0);
    }

    #[test]
    fn an_unowned_alert_pile_nominates_on_its_own_bar() {
        // The account is quiet: nothing unclaimed in it at all.
        let census = [
            face("none", 0, 0, 0, Some(0)),
            face("tasks", 3, 2, 0, Some(0)),
        ];

        // Three unowned alerts against a biggest named pile of two: the alert
        // surface is the reading that nominates, and it says so.
        let alerts = alert_pile(
            &[
                alert(None, 5),
                alert(None, 5),
                alert(None, 5),
                alert(Some("tasks"), 5),
                alert(Some("tasks"), 5),
            ],
            Utc::now(),
        );
        let plan = read_face_lifecycle(&census, Some(&alerts));
        let nomination = plan.nominate.expect("the alert pile outgrew its bar");
        assert_eq!(nomination.source, ALERTS_SOURCE);
        assert_eq!(nomination.uncovered, 3);
        assert_eq!(nomination.largest_named, 2);

        // The other direction: an alert pile that is smaller than the named
        // pile of the *same* reading does not nominate, however large the
        // account's own unclaimed count is -- the two bars are not shared.
        let small = alert_pile(
            &[
                alert(None, 5),
                alert(Some("tasks"), 5),
                alert(Some("tasks"), 5),
            ],
            Utc::now(),
        );
        let plan = read_face_lifecycle(&census, Some(&small));
        assert_eq!(plan.nominate, None);

        // With no alert reading at all, the account's own pile is still read.
        let owed = [
            face("none", 0, 4, 0, Some(0)),
            face("tasks", 0, 2, 0, Some(0)),
        ];
        let plan = read_face_lifecycle(&owed, None);
        assert_eq!(
            plan.nominate.map(|n| (n.source, n.uncovered)),
            Some((LEDGER_SOURCE, 4))
        );
    }

    #[test]
    fn the_larger_of_the_two_piles_is_the_one_asked_about() {
        let census = [
            face("none", 0, 6, 0, Some(0)),
            face("tasks", 0, 4, 0, Some(0)),
        ];
        let alerts = alert_pile(
            &[
                alert(None, 5),
                alert(None, 5),
                alert(None, 5),
                alert(None, 5),
                alert(None, 5),
                alert(None, 5),
                alert(None, 5),
                alert(Some("tasks"), 5),
                alert(Some("tasks"), 5),
            ],
            Utc::now(),
        );
        let plan = read_face_lifecycle(&census, Some(&alerts));
        // Seven unowned alerts beat six unowned rows: two findings, one goal.
        assert_eq!(
            plan.nominate.map(|n| (n.source, n.uncovered)),
            Some((ALERTS_SOURCE, 7))
        );
    }

    #[test]
    fn a_face_with_nothing_live_and_no_movement_for_a_day_is_proposed_for_retirement() {
        let census = [
            face("none", 0, 1, 0, Some(0)),
            face("done", 0, 0, 4, Some(DAY + 1)),
            face("busy", 2, 3, 0, Some(DAY + 1)),
            face("just_retired", 0, 0, 2, Some(60)),
            face("landed_only", 5, 0, 0, Some(DAY + 1)),
        ];
        let plan = read_face_lifecycle(&census, None);
        let retired: Vec<&str> = plan.retire.iter().map(|r| r.face.as_str()).collect();
        assert_eq!(retired, vec!["done"]);
        assert_eq!(plan.retire[0].retired, 4);
        assert_eq!(plan.retire[0].idle_secs, Some(DAY + 1));

        // A face that still owes work is not idle, however long since it moved:
        // an old debt is the face doing its job, not a face that is finished.
        let plan = read_face_lifecycle(&[face("owed", 0, 1, 3, Some(DAY * 30))], None);
        assert!(plan.retire.is_empty());

        // A face with nothing in it at all is not a census row: the census is
        // measured from rows, so a heading nobody ever named is not reported
        // here as a face to retire.
        let plan = read_face_lifecycle(&[], None);
        assert_eq!(plan, FaceLifecycle::default());
    }

    #[test]
    fn the_ids_name_the_day_and_the_face_and_are_safe_as_task_ids() {
        assert_eq!(propose_goal_id("2026-10-24"), "face-propose-2026-10-24");
        assert_eq!(
            retire_goal_id("delivery", "2026-10-24"),
            "face-retire-delivery-2026-10-24"
        );
        // A face id in another script is escaped rather than folded: two names
        // that differ only outside the safe set must not produce one id, or one
        // face's question would be refused as already asked by another.
        assert_eq!(
            retire_goal_id("可评估", "2026-10-24"),
            "face-retire-~53ef~8bc4~4f30-2026-10-24"
        );
        assert_ne!(
            retire_goal_id("可评估", "2026-10-24"),
            retire_goal_id("可評估", "2026-10-24")
        );
    }

    #[test]
    fn the_texts_lead_with_what_is_being_asked_and_carry_the_numbers_behind_it() {
        let nomination = Nomination {
            source: LEDGER_SOURCE,
            uncovered: 12,
            largest_named: 7,
            oldest_unmeasured_secs: Some(3600),
        };
        let text = propose_goal_text(&nomination);
        assert!(text.starts_with("Name a face for the unclaimed work"));
        assert!(text.contains("12"));
        assert!(text.contains("7"));

        // The ask has to say which reading the pile came from: clearing a row
        // of the account and clearing an alert that is ringing now are two
        // different pieces of work, and the reader is handed one of them.
        let from_alerts = Nomination {
            source: ALERTS_SOURCE,
            ..nomination.clone()
        };
        let alert_text = propose_goal_text(&from_alerts);
        assert!(alert_text.contains(ALERTS_SOURCE));
        assert!(alert_text.contains("告警"));
        assert!(!text.contains(ALERTS_SOURCE));

        let retirement = Retirement {
            face: ChangeBlock::named("delivery").expect("a face name"),
            retired: 3,
            idle_secs: Some(DAY * 2),
        };
        let text = retire_goal_text(&retirement);
        assert!(text.starts_with("Retire face \"delivery\""));
        assert!(text.contains("3"));
    }
}
