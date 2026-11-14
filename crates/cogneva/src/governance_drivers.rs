//! `cogneva governance drivers` — the roster of drivers, read from the graph.
//!
//! Why the roster is a graph node and not a file: `scripts/data/self-sufficiency.drivers.tsv`
//! decided, by hand, which unit drives which line of management and what its
//! silence would look like. Nothing behind that table said whether a row was
//! still true — the row and the fact were the same edit — so a unit that was
//! installed, renamed or retired left the table saying whatever the last person
//! typed. The units themselves know: each one carries its front, kind, hosts and
//! stopped-reading in its own systemd file (`X-Cogneva-*`), and the task graph is
//! where that survives as a governed row. The roster is the projection of those
//! rows; the file is gone, not seeded from somewhere else.
//!
//! Why an existing node type rather than a table: faces, handovers, decisions and
//! readings already live on the same channel, and `governance_nodes(type)` is the
//! read on it. A second account of "what drives what" is the thing this module
//! removes.
//!
//! One unit is one node. A unit can drive several fronts, and the table said
//! something different about it on each one (the same worker's silence is read
//! from its transcript for the task account and from its unit state for the
//! principle account), so the per-front reading lives on the front entry, not in
//! a single field on the node. Folding them together would silently pick one
//! front's reading for all of them.
//!
//! The five columns below are the gate's contract, unchanged: it parses this
//! rendering exactly as it parsed the file.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

use cog_core::{GovernanceStore, Task, TaskStatus, TaskType, GOVERNANCE_WORKSPACE};

/// The task type of a driver node.
pub const DRIVER_TASK_TYPE: &str = "driver";

/// The closed set of driver kinds the gate judges. A kind outside it would be a
/// row the gate can read but not act on, which is worse than a refusal here.
pub const KINDS: &[&str] = &["machine", "claude", "claude-retired", "claude-owner"];

/// The closed set of hosts a row can describe. `any` is the default: a row that
/// says nothing about placement is judged wherever the gate runs, which is what
/// the old empty fifth column meant.
pub const HOSTS: &[&str] = &["any", "platform", "dev"];

/// One row of the roster: the five columns the gate reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverRow {
    pub front: String,
    pub kind: String,
    pub unit: String,
    pub stopped_reading: String,
    pub host: String,
}

/// The id a unit's node carries. Namespaced so a driver row cannot collide with
/// a face, a handover or a reading that happens to share the id.
pub fn node_id(unit: &str) -> String {
    format!("driver:{unit}")
}

/// One front a unit drives, with the reading that says this unit has stopped
/// driving it. Kept per front because the table said something different about
/// the same unit on different fronts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DriverFront {
    front: String,
    stopped_reading: String,
    host: String,
}

/// Every row of one unit, folded into the node's shape.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DriverUnit {
    unit: String,
    kind: String,
    fronts: Vec<DriverFront>,
}

pub fn parse_roster(text: &str) -> Result<Vec<DriverRow>, String> {
    let mut rows: Vec<DriverRow> = Vec::new();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let at = index + 1;
        let columns: Vec<&str> = line.split('\t').collect();
        if columns.len() < 4 {
            return Err(format!(
                "line {at}: fewer than four columns \
                 (front<TAB>kind<TAB>unit<TAB>stopped-reading[<TAB>host])"
            ));
        }
        let front = columns[0].trim();
        if front.is_empty() {
            return Err(format!("line {at}: no front (the first column names one)"));
        }
        let kind = columns[1].trim();
        if !KINDS.contains(&kind) {
            return Err(format!(
                "line {at}: kind `{kind}` is not one of {}",
                KINDS.join("|")
            ));
        }
        let unit = columns[2].trim();
        if unit.is_empty() {
            return Err(format!("line {at}: front `{front}` names no driver"));
        }
        // A unit name cannot carry whitespace; a row that does is a sign the
        // columns were shifted, and the gate would then judge a name no unit has.
        if unit.chars().any(char::is_whitespace) || unit.contains('/') {
            return Err(format!(
                "line {at}: driver `{unit}` is not a unit name (no whitespace, no `/`)"
            ));
        }
        let stopped_reading = columns[3].trim().to_string();
        let host = columns
            .get(4)
            .map(|h| h.trim())
            .filter(|h| !h.is_empty())
            .unwrap_or("any")
            .to_string();
        if !HOSTS.contains(&host.as_str()) {
            return Err(format!(
                "line {at}: host `{host}` is not one of {}",
                HOSTS.join("|")
            ));
        }
        // One front says one thing about one unit. Two rows for the same pair
        // would leave the import to pick one, and the gate would judge whichever
        // happened to sort last.
        if !seen.insert((front.to_string(), unit.to_string())) {
            return Err(format!(
                "line {at}: `{unit}` is already declared on `{front}` — one unit states its \
                 stopped-reading once per front"
            ));
        }
        rows.push(DriverRow {
            front: front.to_string(),
            kind: kind.to_string(),
            unit: unit.to_string(),
            stopped_reading,
            host,
        });
    }
    Ok(rows)
}

/// Fold the rows into one node per unit. A unit that is two kinds at once would
/// be judged by the gate as whichever row it read first, so it is refused rather
/// than resolved.
fn fold(rows: &[DriverRow]) -> Result<Vec<DriverUnit>, String> {
    let mut units: Vec<DriverUnit> = Vec::new();
    let mut at: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows {
        match at.get(row.unit.as_str()) {
            None => {
                at.insert(&row.unit, units.len());
                units.push(DriverUnit {
                    unit: row.unit.clone(),
                    kind: row.kind.clone(),
                    fronts: vec![DriverFront {
                        front: row.front.clone(),
                        stopped_reading: row.stopped_reading.clone(),
                        host: row.host.clone(),
                    }],
                });
            }
            Some(&index) => {
                let unit = &mut units[index];
                if unit.kind != row.kind {
                    return Err(format!(
                        "`{}` is declared `{}` and `{}` — one unit has one kind",
                        row.unit, unit.kind, row.kind
                    ));
                }
                unit.fronts.push(DriverFront {
                    front: row.front.clone(),
                    stopped_reading: row.stopped_reading.clone(),
                    host: row.host.clone(),
                });
            }
        }
    }
    Ok(units)
}

/// The node a folded unit is stored as. Kept whole on rewrite: only the fields
/// this module owns are set, so a field somebody else added survives the import.
fn to_task(unit: &DriverUnit, existing: Option<Task>) -> Task {
    let mut task = existing.unwrap_or_else(|| {
        Task::new(
            node_id(&unit.unit),
            TaskType::Custom(DRIVER_TASK_TYPE.into()),
            serde_json::json!({}),
        )
    });
    task.id = node_id(&unit.unit);
    task.task_type = TaskType::Custom(DRIVER_TASK_TYPE.into());
    task.status = TaskStatus::Pending;
    task.is_executable = false;
    task.workspace_id = Some(GOVERNANCE_WORKSPACE.to_string());
    let mut input = match std::mem::take(&mut task.input) {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    input.insert("unit".into(), unit.unit.clone().into());
    input.insert("kind".into(), unit.kind.clone().into());
    input.insert(
        "fronts".into(),
        serde_json::Value::Array(
            unit.fronts
                .iter()
                .map(|front| {
                    serde_json::json!({
                        "front": front.front,
                        "stopped_reading": front.stopped_reading,
                        "host": front.host,
                    })
                })
                .collect(),
        ),
    );
    task.input = serde_json::Value::Object(input);
    task
}

/// A stored node back into roster rows, one per front it drives. `None` for a
/// node that is not a driver row at all — the caller skips it rather than
/// inventing a unit.
fn from_task(task: &Task) -> Option<Vec<DriverRow>> {
    if !matches!(&task.task_type, TaskType::Custom(kind) if kind == DRIVER_TASK_TYPE) {
        return None;
    }
    let unit = task.input.get("unit")?.as_str()?.to_string();
    let kind = task.input.get("kind")?.as_str()?.to_string();
    let fronts = task.input.get("fronts")?.as_array()?;
    if fronts.is_empty() {
        return None;
    }
    let mut rows: Vec<DriverRow> = Vec::new();
    for front in fronts {
        let name = front.get("front")?.as_str()?.to_string();
        let stopped_reading = front
            .get("stopped_reading")
            .and_then(|r| r.as_str())
            .unwrap_or("")
            .to_string();
        let host = front
            .get("host")
            .and_then(|h| h.as_str())
            .unwrap_or("any")
            .to_string();
        rows.push(DriverRow {
            front: name,
            kind: kind.clone(),
            unit: unit.clone(),
            stopped_reading,
            host,
        });
    }
    Some(rows)
}

/// The five columns, one row per (front, unit), sorted so the output does not
/// depend on the order the graph returned the nodes in.
pub fn render(rows: &[DriverRow]) -> String {
    let mut sorted: Vec<&DriverRow> = rows.iter().collect();
    sorted.sort_by(|a, b| {
        (a.front.as_str(), a.unit.as_str(), a.kind.as_str()).cmp(&(
            b.front.as_str(),
            b.unit.as_str(),
            b.kind.as_str(),
        ))
    });
    let mut out = String::new();
    for row in sorted {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            row.front, row.kind, row.unit, row.stopped_reading, row.host
        ));
    }
    out
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportSummary {
    /// Roster rows read.
    pub rows: usize,
    /// Distinct units they name, i.e. nodes written.
    pub units: usize,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    /// Units the graph held and this input no longer names. A unit that was
    /// uninstalled must leave the roster, and the import is the only act that
    /// says so — so it is authoritative in both directions.
    pub retired: usize,
}

impl ImportSummary {
    pub fn line(&self) -> String {
        format!(
            "imported {} rows into {} driver nodes: inserted={} updated={} unchanged={} retired={}",
            self.rows, self.units, self.inserted, self.updated, self.unchanged, self.retired
        )
    }
}

/// Put a complete roster into the graph: units it names become pending nodes,
/// units it no longer names are cancelled. Idempotent — a second run over the
/// same input retires and inserts nothing.
///
/// This is the shape of a full replacement: the one-time move of the legacy
/// declaration table, or a deliberate act that narrows the roster.
pub async fn import_from_text(
    store: &dyn GovernanceStore,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    import(store, text, false).await
}

/// Put the units an input names into the graph without retiring the others.
///
/// This is the shape of the host's own reconciliation: a unit file says which
/// front it drives, and every install re-reads the units that exist. But a unit
/// that is not installed is not a unit that has nothing to do — for a driver the
/// absence is exactly what the gate must red, and a retired role stays a row
/// saying "this unit must not run". Retiring on install would silently drop both.
pub async fn upsert_from_text(
    store: &dyn GovernanceStore,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    import(store, text, true).await
}

async fn import(
    store: &dyn GovernanceStore,
    text: &str,
    upsert_only: bool,
) -> Result<ImportSummary, Box<dyn Error>> {
    let rows = parse_roster(text)?;
    let units = fold(&rows)?;
    let mut existing: BTreeMap<String, Task> = BTreeMap::new();
    for node in store.governance_nodes(DRIVER_TASK_TYPE).await? {
        if from_task(&node).is_some() {
            existing.insert(node.id.clone(), node);
        }
    }
    let mut summary = ImportSummary {
        rows: rows.len(),
        units: units.len(),
        ..ImportSummary::default()
    };
    for unit in &units {
        let id = node_id(&unit.unit);
        match existing.remove(&id) {
            None => {
                store.save_governance_node(&to_task(unit, None)).await?;
                summary.inserted += 1;
            }
            Some(prior) => {
                let next = to_task(unit, Some(prior.clone()));
                if next.input == prior.input && prior.status == TaskStatus::Pending {
                    summary.unchanged += 1;
                } else {
                    store.save_governance_node(&next).await?;
                    summary.updated += 1;
                }
            }
        }
    }
    if upsert_only {
        return Ok(summary);
    }
    // What is left was named by an earlier import and is named by nothing now.
    for (_, mut task) in existing {
        if task.status == TaskStatus::Cancelled {
            continue;
        }
        task.status = TaskStatus::Cancelled;
        store.save_governance_node(&task).await?;
        summary.retired += 1;
    }
    Ok(summary)
}

/// The roster as the gate parses it: pending rows only, five columns.
pub async fn roster(store: &dyn GovernanceStore) -> Result<String, Box<dyn Error>> {
    let mut rows: Vec<DriverRow> = Vec::new();
    for node in store.governance_nodes(DRIVER_TASK_TYPE).await? {
        if node.status != TaskStatus::Pending {
            continue;
        }
        if let Some(node_rows) = from_task(&node) {
            rows.extend(node_rows);
        }
    }
    Ok(render(&rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Rows are listed in the order the roster renders them (front, then unit);
    // the round-trip test would otherwise be asserting that render sorts, not
    // that it preserves.
    const ROSTER: &str = "\
# a header line the gate also skips
任务管理\tclaude-retired\tclaude-worker.service\ttranscript 最新 record 时刻\tany
原则管理\tclaude-retired\tclaude-worker.service\t`systemctl --user show ... -p ActiveState`\tany
原则管理\tmachine\tcogneva-principle-producer.timer\t`systemctl --user show ... -p LoadState`\tplatform
资源管理\tmachine\tcluster:cogneva-host-ledger\t读数停在旧值\tplatform
";

    #[test]
    fn a_roster_survives_parse_and_render_unchanged() {
        crate::roster_test_support::a_round_trip_moves_no_byte(
            "driver roster",
            ROSTER,
            parse_roster,
            render,
        );
    }

    #[test]
    fn comments_and_blank_lines_are_not_rows() {
        crate::roster_test_support::comments_and_blank_lines_are_not_rows(
            "driver roster",
            parse_roster,
        );
    }

    // Two rows for one (front, unit) would leave the import to pick one and the
    // gate to judge whichever sorted last. The pair is the identity.
    #[test]
    fn a_unit_declared_twice_on_one_front_is_refused() {
        let text = "任务管理\tclaude\tc.service\tx\n任务管理\tclaude\tc.service\ty\n";
        let why = parse_roster(text).expect_err("refused");
        assert!(why.contains("already declared"), "{why}");
    }

    #[test]
    fn a_kind_outside_the_closed_set_is_refused() {
        let why = parse_roster("任务管理\trobot\tu.service\tx\n").expect_err("refused");
        assert!(why.contains("not one of"), "{why}");
        assert!(why.contains("claude-retired"), "{why}");
    }

    #[test]
    fn a_host_outside_the_closed_set_is_refused() {
        let why = parse_roster("任务管理\tclaude\tu.service\tx\tcloud\n").expect_err("refused");
        assert!(why.contains("host `cloud`"), "{why}");
    }

    #[test]
    fn an_empty_fifth_column_means_any_host() {
        let rows = parse_roster("任务管理\tclaude\tu.service\tx\t\n").expect("parses");
        assert_eq!(rows[0].host, "any");
    }

    #[test]
    fn a_row_with_three_columns_is_refused() {
        let why = parse_roster("任务管理\tclaude\tu.service\n").expect_err("refused");
        assert!(why.contains("fewer than four columns"), "{why}");
    }

    // A unit name with a space is a sign the columns were shifted; the gate would
    // then judge a name no unit has.
    #[test]
    fn a_driver_that_is_not_a_unit_name_is_refused() {
        for unit in ["claude worker.service", "units/claude.service"] {
            let why = parse_roster(&format!("任务管理\tclaude\t{unit}\tx\n")).expect_err("refused");
            assert!(why.contains("not a unit name"), "{unit}: {why}");
        }
    }

    // One unit is one node: two rows that disagree about its kind would be judged
    // as whichever the gate read first.
    #[test]
    fn one_unit_declared_as_two_kinds_is_refused() {
        let text = "任务管理\tclaude\tc.service\tx\n原则管理\tclaude-retired\tc.service\ty\n";
        let rows = parse_roster(text).expect("rows parse");
        let why = fold(&rows).expect_err("refused");
        assert!(why.contains("one unit has one kind"), "{why}");
    }

    // The node is a governance row like a face: not executable, carried in the
    // governance workspace, and typed so `governance_nodes("driver")` finds it.
    #[test]
    fn the_rows_of_one_unit_become_one_node_and_come_back() {
        let rows = parse_roster(ROSTER).expect("parses");
        let units = fold(&rows).expect("folds");
        let worker = units
            .iter()
            .find(|u| u.unit == "claude-worker.service")
            .unwrap();
        let task = to_task(worker, None);
        assert_eq!(task.id, "driver:claude-worker.service");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(!task.is_executable);
        assert_eq!(task.workspace_id.as_deref(), Some(GOVERNANCE_WORKSPACE));
        let mut back = from_task(&task).expect("reads back");
        back.sort_by(|a, b| a.front.cmp(&b.front));
        let mut expected: Vec<DriverRow> = rows
            .iter()
            .filter(|row| row.unit == "claude-worker.service")
            .cloned()
            .collect();
        expected.sort_by(|a, b| a.front.cmp(&b.front));
        assert_eq!(back, expected);
        // Both fronts keep their own reading: folding them into one field would
        // silently pick one front's answer for the other.
        assert_ne!(back[0].stopped_reading, back[1].stopped_reading);
    }

    #[test]
    fn a_node_of_another_type_is_not_a_driver_row() {
        let mut task = Task::new(
            "reading:x",
            TaskType::Custom("reading".into()),
            serde_json::json!({"unit": "u", "kind": "claude", "fronts": []}),
        );
        task.input = serde_json::json!({"unit": "u", "kind": "claude", "fronts": []});
        assert_eq!(from_task(&task), None);
    }

    #[test]
    fn the_rendering_is_sorted_so_two_reads_compare_byte_for_byte() {
        crate::roster_test_support::a_reordered_roster_renders_the_same_bytes(
            "driver roster",
            ROSTER,
            parse_roster,
            render,
        );
    }

    // An empty reading is the gate's third state, not something the import may
    // paper over: filling it here would turn "nobody decided" into a reading.
    #[test]
    fn an_empty_stopped_reading_survives_the_round_trip() {
        let rows = parse_roster("任务管理\tclaude\tu.service\t\n").expect("parses");
        assert_eq!(rows[0].stopped_reading, "");
        let units = fold(&rows).expect("folds");
        let back = from_task(&to_task(&units[0], None)).expect("reads back");
        assert_eq!(back[0].stopped_reading, "");
    }

    #[test]
    fn the_summary_line_names_every_direction() {
        let summary = ImportSummary {
            rows: 5,
            units: 3,
            inserted: 2,
            updated: 1,
            unchanged: 0,
            retired: 1,
        };
        assert_eq!(
            summary.line(),
            "imported 5 rows into 3 driver nodes: inserted=2 updated=1 unchanged=0 retired=1"
        );
    }

    /// A governance store that keeps rows in a vector. Only the two methods this
    /// module calls are real; reaching any other would be a bug.
    #[derive(Default)]
    struct Rows(std::sync::Mutex<Vec<Task>>);

    impl Rows {
        fn dump(&self) -> Vec<Task> {
            self.0.lock().expect("unpoisoned").clone()
        }
    }

    #[async_trait::async_trait]
    impl GovernanceStore for Rows {
        async fn governance_nodes(&self, task_type: &str) -> cog_core::SFResult<Vec<Task>> {
            Ok(self
                .dump()
                .into_iter()
                .filter(
                    |task| matches!(&task.task_type, TaskType::Custom(kind) if kind == task_type),
                )
                .collect())
        }

        async fn save_governance_node(&self, task: &Task) -> cog_core::SFResult<()> {
            let mut rows = self.0.lock().expect("unpoisoned");
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(existing) => *existing = task.clone(),
                None => rows.push(task.clone()),
            }
            Ok(())
        }

        async fn face_nodes(&self) -> cog_core::SFResult<Vec<Task>> {
            unreachable!("drivers never touch faces")
        }

        async fn open_face_bound_tasks(&self) -> cog_core::SFResult<Vec<Task>> {
            unreachable!("drivers never touch faces")
        }

        async fn save_face_node(&self, _task: &Task) -> cog_core::SFResult<()> {
            unreachable!("drivers never touch faces")
        }

        async fn save_face_node_if_revision(
            &self,
            _task: &Task,
            _expected: i64,
        ) -> cog_core::SFResult<bool> {
            unreachable!("drivers never touch faces")
        }

        async fn save_governance_node_if_status(
            &self,
            _task: &Task,
            _expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            unreachable!("a roster import writes unconditionally")
        }

        async fn save_governance_node_with_event(
            &self,
            _event: &Task,
            _node: &Task,
            _expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            unreachable!("a roster import carries no event")
        }

        async fn apply_handover(
            &self,
            _handover: &Task,
            _face: &Task,
            _expected_revision: i64,
        ) -> cog_core::SFResult<Option<usize>> {
            unreachable!("drivers never move hands")
        }

        async fn reassign_face_work(
            &self,
            _face_id: &str,
            _to_unit: &str,
        ) -> cog_core::SFResult<usize> {
            unreachable!("drivers never move work")
        }

        async fn release_face_work(&self, _face_id: &str) -> cog_core::SFResult<usize> {
            unreachable!("drivers never release work")
        }

        async fn ensure_face_duty(&self, _row: &Task) -> cog_core::SFResult<bool> {
            unreachable!("drivers are not duties")
        }
    }

    // The input is re-read on every beat, so importing twice must move no byte
    // the second time: a row that flip-flopped would look like a roster nobody is
    // editing and everybody is rewriting.
    #[tokio::test]
    async fn importing_the_same_roster_twice_is_a_fixed_point() {
        let store = Rows::default();
        let first = import_from_text(&store, ROSTER).await.expect("imports");
        assert_eq!(first.rows, 4);
        assert_eq!(first.units, 3);
        assert_eq!(first.inserted, 3);
        let before = roster(&store).await.expect("reads");
        let second = import_from_text(&store, ROSTER).await.expect("imports");
        assert_eq!(second.inserted, 0);
        assert_eq!(second.updated, 0);
        assert_eq!(second.unchanged, 3);
        assert_eq!(second.retired, 0);
        assert_eq!(roster(&store).await.expect("reads"), before);
    }

    // A unit that was uninstalled leaves the input, and the import is the only
    // place that says so. An import that only ever added nodes would keep the
    // gate judging a driver that no longer exists.
    #[tokio::test]
    async fn a_unit_the_input_stops_naming_leaves_the_roster() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let shortened = "任务管理\tclaude-retired\tclaude-worker.service\ttranscript 最新 record 时刻\tany\n原则管理\tclaude-retired\tclaude-worker.service\t`systemctl --user show ... -p ActiveState`\tany\n";
        let summary = import_from_text(&store, shortened).await.expect("imports");
        assert_eq!(summary.retired, 2);
        assert_eq!(summary.unchanged, 1);
        let now = roster(&store).await.expect("reads");
        assert!(!now.contains("cogneva-principle-producer.timer"), "{now}");
        assert!(!now.contains("cluster:cogneva-host-ledger"), "{now}");
        assert!(now.contains("claude-worker.service"), "{now}");
    }

    // The host re-reads the units that exist; a unit that is not installed is
    // absent from that input. Upsert must leave it alone — retiring it would drop
    // both the machine leg whose absence is a red and the retired role whose row
    // is the thing that notices a revival.
    #[tokio::test]
    async fn upsert_leaves_units_the_input_does_not_name_in_place() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let partial = "任务管理\tclaude-retired\tclaude-worker.service\ttranscript 最新 record 时刻\tany\n原则管理\tclaude-retired\tclaude-worker.service\t`systemctl --user show ... -p ActiveState`\tany\n";
        let summary = upsert_from_text(&store, partial).await.expect("upserts");
        assert_eq!(summary.retired, 0);
        assert_eq!(summary.unchanged, 1);
        let now = roster(&store).await.expect("reads");
        assert!(now.contains("cogneva-principle-producer.timer"), "{now}");
        assert!(now.contains("cluster:cogneva-host-ledger"), "{now}");
    }

    #[tokio::test]
    async fn changing_a_reading_reaches_the_roster() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let edited = ROSTER.replace(
            "transcript 最新 record 时刻",
            "transcript 里最新一条 record",
        );
        let summary = import_from_text(&store, &edited).await.expect("imports");
        assert_eq!(summary.updated, 1);
        assert_eq!(summary.inserted, 0);
        assert!(roster(&store)
            .await
            .expect("reads")
            .contains("transcript 里最新一条 record"));
    }

    // A retired unit stays in the graph as a cancelled node; rendering it would
    // put the gate back on the uninstalled driver one beat after the import that
    // retired it.
    #[tokio::test]
    async fn a_cancelled_row_is_not_part_of_the_roster() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let mut rows = store.dump();
        // A node with one front, so a cancelled unit removes exactly its row.
        let single = rows
            .iter()
            .position(|row| row.id == "driver:cogneva-principle-producer.timer")
            .expect("the producer node was written");
        rows[single].status = TaskStatus::Cancelled;
        *store.0.lock().expect("unpoisoned") = rows;
        let now = roster(&store).await.expect("reads");
        let named = ROSTER.lines().filter(|line| !line.starts_with('#')).count();
        assert_eq!(now.lines().count(), named - 1, "{now}");
    }

    #[tokio::test]
    async fn an_input_that_does_not_parse_writes_nothing() {
        let store = Rows::default();
        let why = import_from_text(&store, "任务管理\trobot\tu.service\tx\n")
            .await
            .expect_err("refused");
        assert!(why.to_string().contains("not one of"), "{why}");
        assert!(store.dump().is_empty());
    }

    // A unit with two kinds parses row by row but must not be written: one of the
    // two would be silently the one the gate judges.
    #[tokio::test]
    async fn one_unit_declared_as_two_kinds_writes_nothing() {
        let store = Rows::default();
        let why = import_from_text(
            &store,
            "任务管理\tclaude\tc.service\tx\n原则管理\tclaude-retired\tc.service\ty\n",
        )
        .await
        .expect_err("refused");
        assert!(why.to_string().contains("one unit has one kind"), "{why}");
        assert!(store.dump().is_empty());
    }
}
