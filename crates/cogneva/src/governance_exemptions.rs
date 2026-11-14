//! `cogneva governance exemptions` — time-boxed exemptions, read from the graph.
//!
//! Why an exemption is a governed row and not a line in a table: every
//! exemption says the same three things — what it lets through, why that is
//! right today, and when it stops being right — and a hand-edited table carries
//! only as much of that as the last person typed. The expiry is the part that
//! decays: a table row with no date stays a hole until somebody remembers it,
//! while a row on the platform can be read by whoever judges, together with the
//! date, and judged again next week without anyone re-reading prose.
//!
//! One node holds one kind's rows. A kind is a contract with one consumer: the
//! columns, the closed sets inside them, and what the consumer does with a
//! match are all fixed here, so an import that carries a row the consumer
//! cannot use is refused at declaration time rather than discovered as a gate
//! that silently stopped matching. The whole kind is rewritten by one import
//! because "the exemptions in force" is one answer; `--upsert` exists for the
//! daily act of adding or renewing one row without restating the rest.

use std::collections::BTreeSet;
use std::error::Error;

use chrono::NaiveDate;

use cog_core::{
    day_key, GovernanceStore, Task, TaskStatus, TaskType, DAY_KEY_FORMAT, GOVERNANCE_WORKSPACE,
};

/// The task type of an exemption node.
pub const EXEMPTION_TASK_TYPE: &str = "exemption";

/// One kind of exemption: the columns its rows carry, and how many leading
/// columns identify the row. Everything after those is the reason the row is
/// allowed to exist; changing it is a new argument, not a new exemption.
pub struct Kind {
    pub name: &'static str,
    pub columns: &'static [&'static str],
    pub claims: usize,
}

/// The closed set of kinds. A kind is added with its consumer and removed with
/// it; a kind nobody reads would be a place to file a hole where no one looks.
pub const KINDS: &[Kind] = &[
    Kind {
        name: "append-only-exception",
        columns: &["path", "key", "reason"],
        claims: 2,
    },
    Kind {
        name: "label-key-excuse",
        columns: &["layer", "rule", "metric", "key", "code", "why"],
        claims: 4,
    },
    Kind {
        name: "carrier",
        columns: &["face", "reason"],
        claims: 1,
    },
    Kind {
        name: "face-links-gap",
        columns: &["face", "reason"],
        claims: 1,
    },
];

/// The layers a label-key excuse can describe. A row for a layer nobody judges
/// would sit there looking like an answer while the failing layer stays red.
const LABEL_LAYERS: &[&str] = &["prometheus", "infra_watch"];

/// The closed set of reasons a label key can be absent from every live series.
/// Free text here would let "the selector is wrong" be filed as a reason, which
/// is exactly the defect the excuse is meant to keep visible.
const LABEL_CODES: &[&str] = &[
    "object_absent",
    "component_absent",
    "feature_off",
    "collector_off",
    "exporter_version",
    "series_renamed",
    "known_defect",
    "lazy_counter",
    "error_only_state",
    "code_not_deployed",
];

/// One exemption row as the consumer reads it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExemptionRow {
    pub kind: String,
    /// Values in the kind's `columns` order.
    pub cells: Vec<String>,
    /// The last day the row is in force, as `YYYY-MM-DD`.
    pub expires_at: String,
}

impl ExemptionRow {
    pub fn cell(&self, column: &str) -> Option<&str> {
        let kind = kind_of(&self.kind)?;
        let index = kind.columns.iter().position(|c| *c == column)?;
        self.cells.get(index).map(String::as_str)
    }

    /// The columns that identify the row. Two rows with the same claims are the
    /// same exemption stated twice, which the import refuses rather than picks
    /// between.
    fn claims(&self, kind: &Kind) -> Vec<String> {
        self.cells.iter().take(kind.claims).cloned().collect()
    }

    /// The expiry day itself is still inside: a row expiring today is in force
    /// for today and gone tomorrow. Comparing ISO dates as bytes is the same
    /// comparison a reader looking at the calendar would make.
    pub fn expired_on(&self, today: &str) -> bool {
        self.expires_at.as_str() < today
    }
}

/// The id a kind's node carries. Namespaced so an exemption cannot collide
/// with a face, a driver, a front or a reading that happens to share the name.
pub fn node_id(kind: &str) -> String {
    format!("exemption:{kind}")
}

fn kind_of(name: &str) -> Option<&'static Kind> {
    KINDS.iter().find(|kind| kind.name == name)
}

/// Parse the import input: one row per line, kind first, expiry last.
///
/// Comments and blank lines are skipped. Every row is checked against its
/// kind's contract before anything is written; a row the consumer cannot use
/// is refused here, where the operator still has the file in hand.
pub fn parse_roster(text: &str) -> Result<Vec<ExemptionRow>, String> {
    let mut rows: Vec<ExemptionRow> = Vec::new();
    let mut seen: BTreeSet<(String, Vec<String>)> = BTreeSet::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let at = index + 1;
        let fields: Vec<&str> = line.split('\t').collect();
        let name = fields[0].trim();
        let Some(kind) = kind_of(name) else {
            return Err(format!(
                "line {at}: `{name}` is not one of {}",
                kind_names()
            ));
        };
        let expected = 1 + kind.columns.len() + 1;
        if fields.len() != expected {
            return Err(format!(
                "line {at}: kind `{}` takes {} then expires_at ({expected} columns), got {}",
                kind.name,
                kind.columns.join(" "),
                fields.len()
            ));
        }
        let mut cells: Vec<String> = Vec::with_capacity(kind.columns.len());
        for (column, value) in kind.columns.iter().zip(&fields[1..]) {
            let value = value.trim();
            if value.is_empty() {
                return Err(format!("line {at}: `{}` names no {column}", kind.name));
            }
            cells.push(value.to_string());
        }
        let expires_at = fields[fields.len() - 1].trim();
        let date = NaiveDate::parse_from_str(expires_at, DAY_KEY_FORMAT)
            .map_err(|_| format!("line {at}: `{expires_at}` is not a YYYY-MM-DD date"))?;
        if day_key(date) != expires_at {
            return Err(format!(
                "line {at}: `{expires_at}` is not a YYYY-MM-DD date (write the zero-padded form)"
            ));
        }
        if kind.name == "label-key-excuse" {
            let layer = cells[0].as_str();
            if !LABEL_LAYERS.contains(&layer) {
                return Err(format!(
                    "line {at}: layer `{layer}` is not one of {}",
                    LABEL_LAYERS.join("|")
                ));
            }
            let code = cells[4].as_str();
            if !LABEL_CODES.contains(&code) {
                return Err(format!(
                    "line {at}: code `{code}` is not one of {}",
                    LABEL_CODES.join("|")
                ));
            }
        }
        let row = ExemptionRow {
            kind: kind.name.to_string(),
            cells,
            expires_at: expires_at.to_string(),
        };
        if !seen.insert((row.kind.clone(), row.claims(kind))) {
            return Err(format!(
                "line {at}: `{}` is declared twice for the same {}",
                kind.name,
                kind.columns[..kind.claims].join(", ")
            ));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn kind_names() -> String {
    KINDS.iter().map(|k| k.name).collect::<Vec<_>>().join("|")
}

/// One kind's node, carrying every row of that kind.
fn to_task(kind: &Kind, rows: &[ExemptionRow], existing: Option<Task>) -> Task {
    let mut task = existing.unwrap_or_else(|| {
        Task::new(
            node_id(kind.name),
            TaskType::Custom(EXEMPTION_TASK_TYPE.into()),
            serde_json::json!({}),
        )
    });
    task.id = node_id(kind.name);
    task.task_type = TaskType::Custom(EXEMPTION_TASK_TYPE.into());
    task.status = TaskStatus::Pending;
    task.is_executable = false;
    task.workspace_id = Some(GOVERNANCE_WORKSPACE.to_string());
    let mut input = match std::mem::take(&mut task.input) {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    input.insert("kind".into(), kind.name.into());
    let encoded: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut object = serde_json::Map::new();
            for (column, value) in kind.columns.iter().zip(&row.cells) {
                object.insert((*column).into(), value.as_str().into());
            }
            object.insert("expires_at".into(), row.expires_at.as_str().into());
            serde_json::Value::Object(object)
        })
        .collect();
    input.insert("rows".into(), serde_json::Value::Array(encoded));
    task.input = serde_json::Value::Object(input);
    task
}

/// A stored node back into rows. An object missing a contract column is skipped
/// rather than repaired: a row the import never wrote is not one to invent.
fn from_task(kind: &Kind, task: &Task) -> Vec<ExemptionRow> {
    if !matches!(&task.task_type, TaskType::Custom(k) if k == EXEMPTION_TASK_TYPE) {
        return Vec::new();
    }
    let Some(rows) = task.input.get("rows").and_then(|r| r.as_array()) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            let object = row.as_object()?;
            let mut cells = Vec::with_capacity(kind.columns.len());
            for column in kind.columns {
                cells.push(object.get(*column)?.as_str()?.to_string());
            }
            let expires_at = object.get("expires_at")?.as_str()?.to_string();
            Some(ExemptionRow {
                kind: kind.name.to_string(),
                cells,
                expires_at,
            })
        })
        .collect()
}

/// The rows, sorted so two reads of the same graph are byte-identical and a
/// consumer can compare a run against the last one. Order is not a judgment:
/// the kind is the first column, and the rest follows the declaration.
pub fn render(rows: &[ExemptionRow]) -> String {
    let mut sorted: Vec<&ExemptionRow> = rows.iter().collect();
    sorted.sort();
    let mut out = String::new();
    for row in sorted {
        out.push_str(&row.kind);
        for cell in &row.cells {
            out.push('\t');
            out.push_str(cell);
        }
        out.push('\t');
        out.push_str(&row.expires_at);
        out.push('\n');
    }
    out
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportSummary {
    pub rows: usize,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    /// Rows the graph held and this input no longer states. The set in force is
    /// authoritative in both directions: an exemption that was dropped must
    /// stop excusing anything, and only an import can say so.
    pub retired: usize,
}

impl ImportSummary {
    pub fn line(&self) -> String {
        format!(
            "imported {} exemption rows into the graph: inserted={} updated={} unchanged={} retired={}",
            self.rows, self.inserted, self.updated, self.unchanged, self.retired
        )
    }
}

/// Put the whole set in force into the graph: kinds and rows the input no
/// longer states are removed from their nodes. Idempotent.
pub async fn import_from_text(
    store: &dyn GovernanceStore,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    import(store, text, false).await
}

/// State the rows an input names without withdrawing the others. The shape of
/// an operator adding one exemption, or renewing one expiry, without restating
/// the rest of the set.
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
    let mut summary = ImportSummary {
        rows: rows.len(),
        ..ImportSummary::default()
    };
    let mut nodes: Vec<Task> = store.governance_nodes(EXEMPTION_TASK_TYPE).await?;
    for kind in KINDS {
        let wanted: Vec<ExemptionRow> = rows
            .iter()
            .filter(|row| row.kind == kind.name)
            .cloned()
            .collect();
        let prior = nodes
            .iter()
            .position(|node| node.id == node_id(kind.name))
            .map(|at| nodes.remove(at));
        let existing = match &prior {
            Some(node) => from_task(kind, node),
            None => Vec::new(),
        };
        let mut next: Vec<ExemptionRow> = if upsert_only {
            let mut merged = existing.clone();
            for row in &wanted {
                match merged
                    .iter()
                    .position(|old| old.claims(kind) == row.claims(kind))
                {
                    Some(at) => merged[at] = row.clone(),
                    None => merged.push(row.clone()),
                }
            }
            merged
        } else {
            wanted
        };
        next.sort();
        if prior.is_none() && next.is_empty() {
            continue;
        }
        let changed = prior
            .as_ref()
            .map(|node| from_task(kind, node) != next)
            .unwrap_or(true);
        if !changed && prior.as_ref().map(|n| n.status) == Some(TaskStatus::Pending) {
            summary.unchanged += next.len();
            continue;
        }
        for row in &next {
            match existing
                .iter()
                .find(|old| old.claims(kind) == row.claims(kind))
            {
                None => summary.inserted += 1,
                Some(old) if old == row => summary.unchanged += 1,
                Some(_) => summary.updated += 1,
            }
        }
        if !upsert_only {
            summary.retired += existing
                .iter()
                .filter(|old| !next.iter().any(|row| row.claims(kind) == old.claims(kind)))
                .count();
        }
        store
            .save_governance_node(&to_task(kind, &next, prior))
            .await?;
    }
    Ok(summary)
}

/// Every row in force, every kind, one line each. Order is `render`'s.
pub async fn exemptions(store: &dyn GovernanceStore) -> Result<String, Box<dyn Error>> {
    let mut rows: Vec<ExemptionRow> = Vec::new();
    for node in store.governance_nodes(EXEMPTION_TASK_TYPE).await? {
        if node.status != TaskStatus::Pending {
            continue;
        }
        let Some(kind) = node
            .input
            .get("kind")
            .and_then(|k| k.as_str())
            .and_then(kind_of)
        else {
            continue;
        };
        rows.extend(from_task(kind, &node));
    }
    Ok(render(&rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SFResult;

    const ROSTER: &str = "\
# the tables this replaced
append-only-exception\tscripts/data/host-readings.tsv\t*\tThe whole table retired; the roster is read from the units now.\t2026-11-30
label-key-excuse\tprometheus\tCognevaDown\tup\tcontainer\tknown_defect\tThe exporter drops the label while the pod restarts.\t2027-01-15
carrier\t可评估\tNo alert rule names this face yet; the benchmark arm publishes no series.\t2027-02-01
";

    #[test]
    fn a_roster_survives_parse_and_render() {
        let rows = parse_roster(ROSTER).expect("parses");
        assert_eq!(rows.len(), 3);
        let rendered = render(&rows);
        let again = parse_roster(&rendered).expect("parses");
        assert_eq!(render(&again), rendered);
        // Sorted by kind, so the two carriers sort by the two kinds' names.
        assert!(rendered.starts_with("append-only-exception\t"));
        assert_eq!(rows[2].cell("face"), Some("可评估"));
    }

    #[test]
    fn comments_and_blank_lines_are_not_rows() {
        assert!(parse_roster("\n# only a comment\n\n")
            .expect("parses")
            .is_empty());
    }

    #[test]
    fn an_unknown_kind_is_refused_with_the_closed_set() {
        let why = parse_roster("nonsense\ta\tb\t2026-12-01\n").expect_err("refused");
        assert!(why.contains("append-only-exception"), "{why}");
    }

    #[test]
    fn a_row_of_the_wrong_width_names_the_columns_it_expected() {
        let why = parse_roster("carrier\t可评估\t2026-12-01\n").expect_err("refused");
        assert!(why.contains("face reason then expires_at"), "{why}");
    }

    #[test]
    fn a_row_without_a_date_is_refused() {
        let why = parse_roster("carrier\t可评估\treason\tsoon\n").expect_err("refused");
        assert!(why.contains("YYYY-MM-DD"), "{why}");
    }

    #[test]
    fn a_date_written_without_padding_is_refused_rather_than_compared_wrong() {
        let why = parse_roster("carrier\t可评估\treason\t2026-1-5\n").expect_err("refused");
        assert!(why.contains("zero-padded"), "{why}");
    }

    #[test]
    fn an_empty_cell_is_refused_rather_than_imported_as_a_hole() {
        let why = parse_roster("carrier\t\treason\t2026-12-01\n").expect_err("refused");
        assert!(why.contains("names no face"), "{why}");
    }

    #[test]
    fn one_exemption_stated_twice_is_refused() {
        let why = parse_roster("carrier\t可评估\ta\t2026-12-01\ncarrier\t可评估\tb\t2027-01-01\n")
            .expect_err("refused");
        assert!(why.contains("declared twice"), "{why}");
    }

    #[test]
    fn a_label_excuse_outside_the_closed_sets_is_refused() {
        let why = parse_roster(
            "label-key-excuse\tprometheus\tR\tm\tk\tbecause\twhy\treason\t2026-12-01\n",
        );
        assert!(why.is_err());
        let why = parse_roster(
            "label-key-excuse\tprometheus\tR\tm\tk\tbecause\tThe key is fine.\t2026-12-01\n",
        )
        .expect_err("refused");
        assert!(why.contains("not one of"), "{why}");
    }

    #[test]
    fn the_expiry_day_itself_is_still_in_force() {
        let rows = parse_roster("carrier\t可评估\treason\t2026-12-01\n").expect("parses");
        assert!(!rows[0].expired_on("2026-12-01"));
        assert!(rows[0].expired_on("2026-12-02"));
    }

    // A node is a governance row like a face: not executable, in the governance
    // workspace, typed so `governance_nodes("exemption")` finds it.
    #[test]
    fn a_kind_becomes_one_node_and_comes_back() {
        let rows = parse_roster(ROSTER).expect("parses");
        let carriers: Vec<ExemptionRow> = rows
            .iter()
            .filter(|row| row.kind == "carrier")
            .cloned()
            .collect();
        let task = to_task(kind_of("carrier").expect("known"), &carriers, None);
        assert_eq!(task.id, "exemption:carrier");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(!task.is_executable);
        assert_eq!(task.workspace_id.as_deref(), Some(GOVERNANCE_WORKSPACE));
        assert_eq!(
            from_task(kind_of("carrier").expect("known"), &task),
            carriers
        );
    }

    #[test]
    fn a_node_of_another_type_is_not_an_exemption() {
        let task = Task::new(
            "front:x",
            TaskType::Custom("front".into()),
            serde_json::json!({"rows": []}),
        );
        assert!(from_task(kind_of("carrier").expect("known"), &task).is_empty());
    }

    #[test]
    fn the_rendering_is_sorted_so_two_reads_compare_byte_for_byte() {
        let rows = parse_roster(ROSTER).expect("parses");
        let mut reversed = rows.clone();
        reversed.reverse();
        assert_eq!(render(&rows), render(&reversed));
    }

    #[test]
    fn the_summary_line_names_every_direction() {
        let summary = ImportSummary {
            rows: 3,
            inserted: 2,
            updated: 1,
            unchanged: 0,
            retired: 1,
        };
        assert_eq!(
            summary.line(),
            "imported 3 exemption rows into the graph: inserted=2 updated=1 unchanged=0 retired=1"
        );
    }

    #[derive(Default)]
    struct Nodes(std::sync::Mutex<Vec<Task>>);

    #[async_trait::async_trait]
    impl GovernanceStore for Nodes {
        async fn governance_nodes(&self, task_type: &str) -> SFResult<Vec<Task>> {
            Ok(self
                .0
                .lock()
                .expect("unpoisoned")
                .iter()
                .filter(|task| matches!(&task.task_type, TaskType::Custom(k) if k == task_type))
                .cloned()
                .collect())
        }

        async fn save_governance_node(&self, task: &Task) -> SFResult<()> {
            let mut nodes = self.0.lock().expect("unpoisoned");
            match nodes.iter_mut().find(|node| node.id == task.id) {
                Some(existing) => *existing = task.clone(),
                None => nodes.push(task.clone()),
            }
            Ok(())
        }

        async fn face_nodes(&self) -> SFResult<Vec<Task>> {
            unreachable!("exemptions never touch faces")
        }

        async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>> {
            unreachable!("exemptions never touch faces")
        }

        async fn save_face_node(&self, _task: &Task) -> SFResult<()> {
            unreachable!("exemptions never touch faces")
        }

        async fn save_face_node_if_revision(&self, _task: &Task, _expected: i64) -> SFResult<bool> {
            unreachable!("exemptions never touch faces")
        }

        async fn save_governance_node_if_status(
            &self,
            _task: &Task,
            _expected: &[TaskStatus],
        ) -> SFResult<bool> {
            unreachable!("an exemption import writes unconditionally")
        }

        async fn save_governance_node_with_event(
            &self,
            _event: &Task,
            _node: &Task,
            _expected: &[TaskStatus],
        ) -> SFResult<bool> {
            unreachable!("an exemption import writes unconditionally")
        }

        async fn apply_handover(
            &self,
            _handover: &Task,
            _face: &Task,
            _expected_revision: i64,
        ) -> SFResult<Option<usize>> {
            unreachable!("exemptions never move hands")
        }

        async fn reassign_face_work(&self, _face_id: &str, _to_unit: &str) -> SFResult<usize> {
            unreachable!("exemptions never move work")
        }

        async fn release_face_work(&self, _face_id: &str) -> SFResult<usize> {
            unreachable!("exemptions never release work")
        }

        async fn ensure_face_duty(&self, _row: &Task) -> SFResult<bool> {
            unreachable!("exemptions are not duties")
        }
    }

    #[tokio::test]
    async fn a_full_import_retires_the_rows_it_no_longer_states() {
        let store = Nodes::default();
        import_from_text(
            &store,
            "carrier\t可评估\treason\t2026-12-01\ncarrier\t安全与凭证\treason\t2026-12-01\n",
        )
        .await
        .expect("first import");
        let summary = import_from_text(&store, "carrier\t可评估\treason\t2027-01-01\n")
            .await
            .expect("second import");
        assert_eq!(summary.rows, 1);
        assert_eq!(summary.updated, 1);
        assert_eq!(summary.retired, 1);
        let read = exemptions(&store).await.expect("reads");
        assert_eq!(
            read, "carrier\t可评估\treason\t2027-01-01\n",
            "the row that left the set must no longer be exempt"
        );
    }

    #[tokio::test]
    async fn upsert_adds_one_row_without_withdrawing_the_others() {
        let store = Nodes::default();
        import_from_text(&store, "carrier\t可评估\treason\t2026-12-01\n")
            .await
            .expect("first import");
        let summary = upsert_from_text(&store, "carrier\t安全与凭证\treason\t2026-12-01\n")
            .await
            .expect("upsert");
        assert_eq!(summary.inserted, 1);
        assert_eq!(summary.retired, 0);
        let read = exemptions(&store).await.expect("reads");
        assert!(read.contains("carrier\t可评估\treason\t2026-12-01"));
        assert!(read.contains("carrier\t安全与凭证\treason\t2026-12-01"));
    }

    #[tokio::test]
    async fn a_second_identical_import_writes_nothing() {
        let store = Nodes::default();
        import_from_text(&store, ROSTER)
            .await
            .expect("first import");
        let summary = import_from_text(&store, ROSTER)
            .await
            .expect("second import");
        assert_eq!(summary.unchanged, 3);
        assert_eq!(summary.inserted + summary.updated + summary.retired, 0);
    }

    #[tokio::test]
    async fn a_node_of_another_type_is_left_alone() {
        let store = Nodes::default();
        let other = Task::new(
            "front:x",
            TaskType::Custom("front".into()),
            serde_json::json!({"front": "x", "what": "y"}),
        );
        store.save_governance_node(&other).await.expect("seed");
        import_from_text(&store, "carrier\t可评估\treason\t2026-12-01\n")
            .await
            .expect("import");
        let nodes = store.governance_nodes("front").await.expect("reads");
        assert_eq!(nodes, vec![other]);
    }
}
