//! `cogneva governance declarations` — the tables a host reading judges
//! against, read from the graph.
//!
//! Why a declaration is a governed row: every one of these tables states a
//! decision — which face a principle theme belongs to, how a face's missing
//! reading is recognised as failing, how long it has to stay that way before
//! the number is worth printing. A hand-edited copy in the tree is a second
//! answer to a question the platform already answers, and the two drift in
//! silence: a row edited in the file and a row imported into the graph read the
//! same until somebody compares them, and nobody does.
//!
//! One node holds one kind's rows, so what a reader gets is one answer per
//! kind rather than a row that exists in one place and not the other. The
//! contract lives here, next to the closed sets: an import that carries a row
//! the consumer cannot use is refused while the operator still has the file in
//! hand, instead of turning into a reading that quietly stopped matching.
//!
//! Rows are positional and their width may stop early, because the trailing
//! columns are refinements of the row before them: a criteria row with no
//! probe still says how the reading fails, it just cannot say for how long.
//! A gap in the middle is refused — an empty cell between two filled ones is a
//! hole somebody forgot to fill, not an omitted refinement.

use std::collections::BTreeSet;
use std::error::Error;

use cog_core::{GovernanceStore, Task, TaskStatus, TaskType, GOVERNANCE_WORKSPACE};

/// The task type of a declaration node.
pub const DECLARATION_TASK_TYPE: &str = "declaration";

/// A column whose value is one of a fixed list.
pub struct ClosedSet {
    pub column: usize,
    pub values: &'static [&'static str],
}

/// One kind of declaration table: the columns its rows carry, how many leading
/// columns a row must fill, which columns identify the row, and the closed sets
/// inside it.
pub struct Kind {
    pub name: &'static str,
    pub columns: &'static [&'static str],
    /// Columns a row must fill. Everything after may be omitted from the end.
    pub required: usize,
    /// The columns that identify a row. Two rows sharing them are one
    /// declaration stated twice, which the import refuses rather than picks
    /// between.
    pub ident: &'static [usize],
    pub closed: &'static [ClosedSet],
}

/// The closed set of kinds. A kind is added with its consumer and removed with
/// it: a table nobody reads would be a place to file a decision where no one
/// looks.
pub const KINDS: &[Kind] = &[
    // How a face's owed reading is recognised as failing, and how long it has
    // been failing. One row per face; the first column carries the face name
    // exactly as the face node spells it, spaces included.
    Kind {
        name: "face-reading-criteria",
        columns: &["face", "expr", "probe", "hours"],
        required: 2,
        ident: &[0],
        closed: &[],
    },
    // Which face a principle theme or slug belongs to. A theme or slug belongs
    // to exactly one face: `ident` is (kind, key), not the leading (face,
    // kind), because a second face claiming the same theme is the defect the
    // census reading counts, not a second declaration of it.
    Kind {
        name: "principle-face-map",
        columns: &["face", "kind", "key"],
        required: 3,
        ident: &[1, 2],
        closed: &[ClosedSet {
            column: 1,
            values: &["theme", "slug"],
        }],
    },
];

pub fn kind_by_name(name: &str) -> Option<&'static Kind> {
    KINDS.iter().find(|kind| kind.name == name)
}

pub fn kind_names() -> String {
    KINDS.iter().map(|k| k.name).collect::<Vec<_>>().join("|")
}

/// One row of one kind, as the consumer reads it. `cells` holds the values that
/// were stated; a trailing column the row omitted is simply not there.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeclarationRow {
    pub kind: String,
    pub cells: Vec<String>,
}

impl DeclarationRow {
    pub fn cell(&self, column: &str) -> Option<&str> {
        let kind = kind_by_name(&self.kind)?;
        let index = kind.columns.iter().position(|c| *c == column)?;
        self.cells.get(index).map(String::as_str)
    }

    fn ident(&self, kind: &Kind) -> Vec<String> {
        kind.ident
            .iter()
            .map(|index| self.cells.get(*index).cloned().unwrap_or_default())
            .collect()
    }
}

/// The id a kind's node carries. Namespaced so a declaration cannot collide
/// with a face, a driver, a front, a reading or an exemption.
pub fn node_id(kind: &str) -> String {
    format!("declaration:{kind}")
}

/// Parse a kind's table: one row per line, columns in the kind's order.
///
/// Comments and blank lines are skipped, so the projection can carry a header
/// and the operator's file can carry the reason the table exists. Every row is
/// checked against the kind's contract before anything is written.
pub fn parse_table(kind: &Kind, text: &str) -> Result<Vec<DeclarationRow>, String> {
    let mut rows: Vec<DeclarationRow> = Vec::new();
    let mut seen: BTreeSet<Vec<String>> = BTreeSet::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let at = index + 1;
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() > kind.columns.len() {
            return Err(format!(
                "line {at}: `{}` takes at most {} columns ({}), got {}",
                kind.name,
                kind.columns.len(),
                kind.columns.join(" "),
                fields.len()
            ));
        }
        let mut cells: Vec<String> = Vec::with_capacity(fields.len());
        for (position, value) in fields.iter().enumerate() {
            let value = value.trim();
            if value.is_empty() {
                return Err(format!(
                    "line {at}: `{}` has an empty {}",
                    kind.name, kind.columns[position]
                ));
            }
            cells.push(value.to_string());
        }
        if cells.len() < kind.required {
            return Err(format!(
                "line {at}: `{}` needs {} ({} columns), got {}",
                kind.name,
                kind.columns[..kind.required].join(" "),
                kind.required,
                cells.len()
            ));
        }
        for set in kind.closed {
            let value = cells[set.column].as_str();
            if !set.values.contains(&value) {
                return Err(format!(
                    "line {at}: {} `{value}` is not one of {}",
                    kind.columns[set.column],
                    set.values.join("|")
                ));
            }
        }
        if kind.name == "face-reading-criteria" {
            if let Some(hours) = cells.get(3) {
                match hours.parse::<f64>() {
                    Ok(n) if n > 0.0 => {}
                    _ => {
                        return Err(format!(
                            "line {at}: hours `{hours}` is not a positive number of hours"
                        ))
                    }
                }
            }
        }
        let row = DeclarationRow {
            kind: kind.name.to_string(),
            cells,
        };
        if !seen.insert(row.ident(kind)) {
            return Err(format!(
                "line {at}: `{}` is declared twice for the same {}",
                kind.name,
                kind.ident
                    .iter()
                    .map(|index| kind.columns[*index])
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        rows.push(row);
    }
    Ok(rows)
}

/// One kind's node, carrying every row of that kind.
fn to_task(kind: &Kind, rows: &[DeclarationRow], existing: Option<Task>) -> Task {
    let mut task = existing.unwrap_or_else(|| {
        Task::new(
            node_id(kind.name),
            TaskType::Custom(DECLARATION_TASK_TYPE.into()),
            serde_json::json!({}),
        )
    });
    task.id = node_id(kind.name);
    task.task_type = TaskType::Custom(DECLARATION_TASK_TYPE.into());
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
            serde_json::Value::Object(object)
        })
        .collect();
    input.insert("rows".into(), serde_json::Value::Array(encoded));
    task.input = serde_json::Value::Object(input);
    task
}

/// A stored node back into rows. A row missing a required column is skipped
/// rather than repaired: a row the import never wrote is not one to invent.
fn from_task(kind: &Kind, task: &Task) -> Vec<DeclarationRow> {
    if !matches!(&task.task_type, TaskType::Custom(k) if k == DECLARATION_TASK_TYPE) {
        return Vec::new();
    }
    let Some(rows) = task.input.get("rows").and_then(|r| r.as_array()) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            let object = row.as_object()?;
            let mut cells: Vec<String> = Vec::with_capacity(kind.columns.len());
            for (position, column) in kind.columns.iter().enumerate() {
                match object.get(*column).and_then(|v| v.as_str()) {
                    Some(value) => cells.push(value.to_string()),
                    None if position < kind.required => return None,
                    // A trailing column the row omitted; stop here, because a
                    // later column with no earlier one to sit behind would be
                    // a hole this reader would otherwise hand on.
                    None => break,
                }
            }
            Some(DeclarationRow {
                kind: kind.name.to_string(),
                cells,
            })
        })
        .collect()
}

/// The rows, sorted so two reads of the same graph are byte-identical and a
/// consumer can compare a run against the last one. Order is not a judgment;
/// sorting by the row keeps the table stable when a row is added.
pub fn render(rows: &[DeclarationRow]) -> String {
    let mut sorted: Vec<&DeclarationRow> = rows.iter().collect();
    sorted.sort();
    let mut out = String::new();
    for row in sorted {
        out.push_str(&row.cells.join("\t"));
        out.push('\n');
    }
    out
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportSummary {
    pub kind: String,
    pub rows: usize,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    /// Rows the graph held and this input no longer states. The set in force is
    /// authoritative in both directions: a decision that was dropped must stop
    /// being read as one, and only an import can say so.
    pub retired: usize,
}

impl ImportSummary {
    pub fn line(&self) -> String {
        format!(
            "imported {} {} rows into the graph: inserted={} updated={} unchanged={} retired={}",
            self.rows, self.kind, self.inserted, self.updated, self.unchanged, self.retired
        )
    }
}

/// Put the whole set in force into the graph: rows the input no longer states
/// are removed. Idempotent.
pub async fn import_from_text(
    store: &dyn GovernanceStore,
    kind: &Kind,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    import(store, kind, text, false).await
}

/// State the rows an input names without withdrawing the others. The shape of
/// an operator adding one row, or revising one face's criteria, without
/// restating the whole table.
pub async fn upsert_from_text(
    store: &dyn GovernanceStore,
    kind: &Kind,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    import(store, kind, text, true).await
}

async fn import(
    store: &dyn GovernanceStore,
    kind: &Kind,
    text: &str,
    upsert_only: bool,
) -> Result<ImportSummary, Box<dyn Error>> {
    let wanted = parse_table(kind, text)?;
    let mut summary = ImportSummary {
        kind: kind.name.to_string(),
        rows: wanted.len(),
        ..ImportSummary::default()
    };
    let nodes = store.governance_nodes(DECLARATION_TASK_TYPE).await?;
    let prior = nodes.into_iter().find(|node| node.id == node_id(kind.name));
    let existing = match &prior {
        Some(node) => from_task(kind, node),
        None => Vec::new(),
    };
    let mut next: Vec<DeclarationRow> = if upsert_only {
        let mut merged = existing.clone();
        for row in &wanted {
            match merged
                .iter()
                .position(|old| old.ident(kind) == row.ident(kind))
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
    // A full import always writes its kind's node, even with no rows: stating
    // that a table has nothing in it is a declaration, and it is the only way
    // to tell it apart from a platform that never received the table at all.
    if upsert_only && prior.is_none() && next.is_empty() {
        return Ok(summary);
    }
    let changed = prior
        .as_ref()
        .map(|node| from_task(kind, node) != next)
        .unwrap_or(true);
    if !changed && prior.as_ref().map(|n| n.status) == Some(TaskStatus::Pending) {
        summary.unchanged += next.len();
        return Ok(summary);
    }
    for row in &next {
        match existing
            .iter()
            .find(|old| old.ident(kind) == row.ident(kind))
        {
            None => summary.inserted += 1,
            Some(old) if old == row => summary.unchanged += 1,
            Some(_) => summary.updated += 1,
        }
    }
    if !upsert_only {
        summary.retired += existing
            .iter()
            .filter(|old| !next.iter().any(|row| row.ident(kind) == old.ident(kind)))
            .count();
    }
    store
        .save_governance_node(&to_task(kind, &next, prior))
        .await?;
    Ok(summary)
}

/// One kind's table, every row, one line each. Order is `render`'s.
pub async fn table(store: &dyn GovernanceStore, kind: &Kind) -> Result<String, Box<dyn Error>> {
    Ok(declared_table(store, kind).await?.unwrap_or_default())
}

/// The kind's table and whether the graph states one at all.
///
/// `None` is not the same answer as an empty table: "nobody declared a row" and
/// "this platform never received the declaration" are two different things to
/// the reader, and a projection that wrote a header over no rows would merge
/// them. A kind that was never imported therefore has no file beside the faces,
/// which a reader reports as unread rather than as a table with nothing in it.
pub async fn declared_table(
    store: &dyn GovernanceStore,
    kind: &Kind,
) -> Result<Option<String>, Box<dyn Error>> {
    let node = store
        .governance_nodes(DECLARATION_TASK_TYPE)
        .await?
        .into_iter()
        .find(|node| node.id == node_id(kind.name));
    Ok(match node {
        Some(node) if node.status == TaskStatus::Pending => Some(render(&from_task(kind, &node))),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SFResult;

    const CRITERIA: &str = "\
# face\texpr\tprobe\thours
token 管理\tllm_pool_available == 0\tllm_pool_available == 0\t2
通信\tincrease(cogneva_alert_notifications_total{outcome=~\"failed\"}[30m]) > 0
";

    const MAP: &str = "\
原则管理\ttheme\tgates-and-commits
原则管理\tslug\tverify-before-green
变更管理\ttheme\tcanonical-flow
";

    fn criteria() -> &'static Kind {
        kind_by_name("face-reading-criteria").expect("known")
    }

    fn face_map() -> &'static Kind {
        kind_by_name("principle-face-map").expect("known")
    }

    #[test]
    fn a_table_survives_parse_and_render() {
        let rows = parse_table(criteria(), CRITERIA).expect("parses");
        assert_eq!(rows.len(), 2);
        let rendered = render(&rows);
        let again = parse_table(criteria(), &rendered).expect("parses");
        assert_eq!(render(&again), rendered);
        assert!(rendered.contains("通信\tincrease("), "{rendered}");
    }

    #[test]
    fn the_face_name_keeps_its_space() {
        let rows = parse_table(criteria(), CRITERIA).expect("parses");
        assert_eq!(rows[0].cell("face"), Some("token 管理"));
        assert_eq!(rows[0].cell("hours"), Some("2"));
    }

    #[test]
    fn a_row_may_stop_after_its_required_columns() {
        let rows = parse_table(criteria(), "通信\tx > 0\n").expect("parses");
        assert_eq!(rows[0].cells, vec!["通信", "x > 0"]);
        assert_eq!(rows[0].cell("probe"), None);
    }

    #[test]
    fn a_gap_in_the_middle_is_refused() {
        let why = parse_table(criteria(), "通信\tx > 0\t\t2\n").expect_err("refused");
        assert!(why.contains("empty probe"), "{why}");
    }

    #[test]
    fn a_row_shorter_than_its_required_columns_is_refused() {
        let why = parse_table(criteria(), "通信\n").expect_err("refused");
        assert!(why.contains("needs face expr"), "{why}");
    }

    #[test]
    fn an_hour_count_that_is_not_a_positive_number_is_refused() {
        for bad in ["soon", "0", "-1"] {
            let why = parse_table(criteria(), &format!("通信\tx > 0\tx > 0\t{bad}\n"))
                .expect_err("refused");
            assert!(why.contains("positive number"), "{why}");
        }
    }

    #[test]
    fn a_kind_outside_its_closed_set_is_refused() {
        let why = parse_table(face_map(), "原则管理\ttopic\tx\n").expect_err("refused");
        assert!(why.contains("not one of theme|slug"), "{why}");
    }

    #[test]
    fn one_key_under_two_faces_is_refused() {
        let why = parse_table(
            face_map(),
            "原则管理\ttheme\tgates\n变更管理\ttheme\tgates\n",
        )
        .expect_err("refused");
        assert!(
            why.contains("declared twice for the same kind, key"),
            "{why}"
        );
    }

    #[test]
    fn the_same_key_may_be_a_theme_and_a_slug() {
        let rows = parse_table(face_map(), "原则管理\ttheme\tx\n原则管理\tslug\tx\n")
            .expect("the identity is kind plus key");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn comments_and_blank_lines_are_not_rows() {
        assert!(parse_table(criteria(), "\n# only a comment\n\n")
            .expect("parses")
            .is_empty());
    }

    // A node is a governance row like a face: not executable, in the governance
    // workspace, typed so `governance_nodes("declaration")` finds it.
    #[test]
    fn a_kind_becomes_one_node_and_comes_back() {
        let rows = parse_table(criteria(), CRITERIA).expect("parses");
        let task = to_task(criteria(), &rows, None);
        assert_eq!(task.id, "declaration:face-reading-criteria");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(!task.is_executable);
        assert_eq!(task.workspace_id.as_deref(), Some(GOVERNANCE_WORKSPACE));
        assert_eq!(from_task(criteria(), &task), rows);
    }

    #[test]
    fn a_node_of_another_type_is_not_a_declaration() {
        let task = Task::new(
            "exemption:carrier",
            TaskType::Custom("exemption".into()),
            serde_json::json!({"rows": []}),
        );
        assert!(from_task(criteria(), &task).is_empty());
    }

    #[test]
    fn the_rendering_is_sorted_so_two_reads_compare_byte_for_byte() {
        let rows = parse_table(criteria(), CRITERIA).expect("parses");
        let mut reversed = rows.clone();
        reversed.reverse();
        assert_eq!(render(&rows), render(&reversed));
    }

    #[test]
    fn the_summary_line_names_every_direction() {
        let summary = ImportSummary {
            kind: "principle-face-map".into(),
            rows: 3,
            inserted: 2,
            updated: 1,
            unchanged: 0,
            retired: 1,
        };
        assert_eq!(
            summary.line(),
            "imported 3 principle-face-map rows into the graph: \
             inserted=2 updated=1 unchanged=0 retired=1"
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
            unreachable!("declarations never touch faces")
        }

        async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>> {
            unreachable!("declarations never touch faces")
        }

        async fn save_face_node(&self, _task: &Task) -> SFResult<()> {
            unreachable!("declarations never touch faces")
        }

        async fn save_face_node_if_revision(&self, _task: &Task, _expected: i64) -> SFResult<bool> {
            unreachable!("declarations never touch faces")
        }

        async fn save_governance_node_if_status(
            &self,
            _task: &Task,
            _expected: &[TaskStatus],
        ) -> SFResult<bool> {
            unreachable!("a declaration import writes unconditionally")
        }

        async fn save_governance_node_with_event(
            &self,
            _event: &Task,
            _node: &Task,
            _expected: &[TaskStatus],
        ) -> SFResult<bool> {
            unreachable!("a declaration import writes unconditionally")
        }

        async fn apply_handover(
            &self,
            _handover: &Task,
            _face: &Task,
            _expected_revision: i64,
        ) -> SFResult<Option<usize>> {
            unreachable!("declarations never move hands")
        }

        async fn reassign_face_work(&self, _face_id: &str, _to_unit: &str) -> SFResult<usize> {
            unreachable!("declarations never move work")
        }

        async fn release_face_work(&self, _face_id: &str) -> SFResult<usize> {
            unreachable!("declarations never release work")
        }

        async fn ensure_face_duty(&self, _row: &Task) -> SFResult<bool> {
            unreachable!("declarations are not duties")
        }
    }

    #[tokio::test]
    async fn a_full_import_retires_the_rows_it_no_longer_states() {
        let store = Nodes::default();
        import_from_text(
            &store,
            face_map(),
            "原则管理\ttheme\ta\n变更管理\ttheme\tb\n",
        )
        .await
        .expect("first import");
        let summary = import_from_text(&store, face_map(), "原则管理\ttheme\ta\n")
            .await
            .expect("second import");
        assert_eq!(summary.rows, 1);
        assert_eq!(summary.unchanged, 1);
        assert_eq!(summary.retired, 1);
        let read = table(&store, face_map()).await.expect("reads");
        assert_eq!(read, "原则管理\ttheme\ta\n");
    }

    #[tokio::test]
    async fn upsert_adds_one_row_without_withdrawing_the_others() {
        let store = Nodes::default();
        import_from_text(&store, criteria(), "通信\tx > 0\n")
            .await
            .expect("first import");
        let summary = upsert_from_text(&store, criteria(), "安全与凭证\ty > 0\n")
            .await
            .expect("upsert");
        assert_eq!(summary.inserted, 1);
        assert_eq!(summary.retired, 0);
        let read = table(&store, criteria()).await.expect("reads");
        assert!(read.contains("通信\tx > 0"));
        assert!(read.contains("安全与凭证\ty > 0"));
    }

    #[tokio::test]
    async fn a_second_identical_import_writes_nothing() {
        let store = Nodes::default();
        import_from_text(&store, face_map(), MAP)
            .await
            .expect("first");
        let summary = import_from_text(&store, face_map(), MAP)
            .await
            .expect("second");
        assert_eq!(summary.unchanged, 3);
        assert_eq!(summary.inserted + summary.updated + summary.retired, 0);
    }

    #[tokio::test]
    async fn a_kind_that_was_never_imported_reads_empty_and_is_not_declared() {
        let store = Nodes::default();
        assert_eq!(table(&store, criteria()).await.expect("reads"), "");
        assert_eq!(
            declared_table(&store, criteria()).await.expect("reads"),
            None,
            "a platform that never received the kind must not look like an empty table"
        );
    }

    #[tokio::test]
    async fn a_kind_that_was_imported_empty_is_declared_and_empty() {
        let store = Nodes::default();
        import_from_text(&store, criteria(), "# nobody wrote a criterion yet\n")
            .await
            .expect("imports");
        assert_eq!(
            declared_table(&store, criteria()).await.expect("reads"),
            Some(String::new())
        );
    }

    #[tokio::test]
    async fn one_kind_is_left_alone_by_an_import_of_another() {
        let store = Nodes::default();
        import_from_text(&store, criteria(), "通信\tx > 0\n")
            .await
            .expect("criteria");
        import_from_text(&store, face_map(), MAP)
            .await
            .expect("map");
        let read = table(&store, criteria()).await.expect("reads");
        assert_eq!(read, "通信\tx > 0\n");
    }
}
