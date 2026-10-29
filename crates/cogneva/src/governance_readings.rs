//! `cogneva governance readings` — the roster of host readings, read from the graph.
//!
//! Why the roster is a graph node and not a file: the host aggregator used to be
//! told what to run by `scripts/data/host-readings.tsv`, which was typed by hand.
//! A hand-typed roster has no source of truth behind it — the row saying a reading
//! exists and the fact that it exists are the same edit — so every change had to
//! be made twice, once in that file and once in the script, and nothing could tell
//! the two apart once they disagreed. Faces are already not done that way: a face
//! is a node in the task graph and the file is a one-off declaration. Readings
//! take the same route: one producer is one governance node of type `reading`,
//! whose input carries the script path, the note and the series families it emits.
//!
//! Why an existing node type rather than a table: the graph already has a channel
//! for arbitrary governance rows — faces, handovers and decisions all use it, and
//! `governance_nodes(type)` is the read on that channel. A new table would be a
//! second account of "what exists", which is the thing this module removes.
//!
//! The four columns below are the aggregator's contract, unchanged: it parses
//! this rendering exactly as it parsed the file, so switching the source is a
//! change in one place rather than a rewrite of the reconciliation around it.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

use cog_core::{GovernanceStore, Task, TaskStatus, TaskType, GOVERNANCE_WORKSPACE};

/// The task type of a host-reading producer row.
pub const READING_TASK_TYPE: &str = "reading";

/// One row of the roster, the same four columns the aggregator reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadingRow {
    pub producer: String,
    pub path: String,
    pub note: String,
    /// Comma-separated series families. A `=verdict` suffix marks a family whose
    /// reason to exist is to trigger action rather than to be looked at.
    pub series: Vec<String>,
}

/// The id a producer's node carries. Namespaced so a reading row cannot collide
/// with a face or a handover that happens to share the id.
pub fn node_id(producer: &str) -> String {
    format!("reading:{producer}")
}

pub fn parse_roster(text: &str) -> Result<Vec<ReadingRow>, String> {
    let mut rows: Vec<ReadingRow> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let at = index + 1;
        let columns: Vec<&str> = line.split('\t').collect();
        if columns.len() < 2 {
            return Err(format!(
                "line {at}: fewer than two columns (producer<TAB>script[TAB]note[TAB]series)"
            ));
        }
        let producer = columns[0].trim();
        if producer.is_empty()
            || !producer.as_bytes()[0].is_ascii_lowercase()
            || !producer
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(format!(
                "line {at}: producer `{producer}` is not `[a-z][a-z0-9_]*`"
            ));
        }
        if !seen.insert(producer.to_string()) {
            return Err(format!(
                "line {at}: producer `{producer}` appears twice — two nodes with one id, and the \
                 second would silently overwrite the first"
            ));
        }
        let path = columns[1].trim();
        if path.is_empty() {
            return Err(format!("line {at}: producer `{producer}` names no script"));
        }
        let note = columns.get(2).map(|n| n.trim()).unwrap_or("").to_string();
        let series: Vec<String> = columns
            .get(3)
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        rows.push(ReadingRow {
            producer: producer.to_string(),
            path: path.to_string(),
            note,
            series,
        });
    }
    Ok(rows)
}

/// The node a row is stored as. Kept whole on rewrite: only the fields this
/// module owns are set, so a field somebody else added survives the import.
pub fn to_task(row: &ReadingRow, existing: Option<Task>) -> Task {
    let mut task = existing.unwrap_or_else(|| {
        Task::new(
            node_id(&row.producer),
            TaskType::Custom(READING_TASK_TYPE.into()),
            serde_json::json!({}),
        )
    });
    task.id = node_id(&row.producer);
    task.task_type = TaskType::Custom(READING_TASK_TYPE.into());
    task.status = TaskStatus::Pending;
    task.is_executable = false;
    task.workspace_id = Some(GOVERNANCE_WORKSPACE.to_string());
    let mut input = match std::mem::take(&mut task.input) {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    input.insert("producer".into(), row.producer.clone().into());
    input.insert("path".into(), row.path.clone().into());
    input.insert("note".into(), row.note.clone().into());
    input.insert(
        "series".into(),
        serde_json::Value::Array(row.series.iter().cloned().map(Into::into).collect()),
    );
    task.input = serde_json::Value::Object(input);
    task
}

/// A stored row back into the four columns. `None` for a node that is not a
/// reading row at all — the caller skips it rather than inventing a producer.
pub fn from_task(task: &Task) -> Option<ReadingRow> {
    if !matches!(&task.task_type, TaskType::Custom(kind) if kind == READING_TASK_TYPE) {
        return None;
    }
    let producer = task.input.get("producer")?.as_str()?.to_string();
    let path = task.input.get("path")?.as_str()?.to_string();
    let note = task
        .input
        .get("note")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    let series: Vec<String> = task
        .input
        .get("series")
        .and_then(|s| s.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|s| s.as_str())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    Some(ReadingRow {
        producer,
        path,
        note,
        series,
    })
}

/// The four columns, one row per producer, sorted so the output does not depend
/// on the order the graph returned the rows in.
pub fn render(rows: &[ReadingRow]) -> String {
    let mut sorted: Vec<&ReadingRow> = rows.iter().collect();
    sorted.sort_by(|a, b| a.producer.cmp(&b.producer));
    let mut out = String::new();
    for row in sorted {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            row.producer,
            row.path,
            row.note,
            row.series.join(",")
        ));
    }
    out
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportSummary {
    pub rows: usize,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    /// Rows the graph held and this file no longer names. A producer whose
    /// script was deleted must leave the roster, and the file is the only place
    /// that says so — so the import is authoritative in both directions.
    pub retired: usize,
}

impl ImportSummary {
    pub fn line(&self) -> String {
        format!(
            "imported {} rows: inserted={} updated={} unchanged={} retired={}",
            self.rows, self.inserted, self.updated, self.unchanged, self.retired
        )
    }
}

/// Put a roster file into the graph: rows it names become pending nodes, rows it
/// no longer names are cancelled. Idempotent — a second run over the same file
/// retires and inserts nothing.
pub async fn import_from_text(
    store: &dyn GovernanceStore,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    let rows = parse_roster(text)?;
    let mut existing: BTreeMap<String, Task> = BTreeMap::new();
    for node in store.governance_nodes(READING_TASK_TYPE).await? {
        if from_task(&node).is_some() {
            existing.insert(node.id.clone(), node);
        }
    }
    let mut summary = ImportSummary {
        rows: rows.len(),
        ..ImportSummary::default()
    };
    for row in &rows {
        let id = node_id(&row.producer);
        match existing.remove(&id) {
            None => {
                store.save_governance_node(&to_task(row, None)).await?;
                summary.inserted += 1;
            }
            Some(prior) => {
                let next = to_task(row, Some(prior.clone()));
                if next.input == prior.input && prior.status == TaskStatus::Pending {
                    summary.unchanged += 1;
                } else {
                    store.save_governance_node(&next).await?;
                    summary.updated += 1;
                }
            }
        }
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

/// The roster as the aggregator parses it: pending rows only, four columns.
pub async fn roster(store: &dyn GovernanceStore) -> Result<String, Box<dyn Error>> {
    let mut rows: Vec<ReadingRow> = Vec::new();
    for node in store.governance_nodes(READING_TASK_TYPE).await? {
        if node.status != TaskStatus::Pending {
            continue;
        }
        if let Some(row) = from_task(&node) {
            rows.push(row);
        }
    }
    Ok(render(&rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROSTER: &str = "\
# a header line the aggregator also skips
debt_edge_hook\tscripts/host/cogneva-debt-edge-reading.sh\t依赖边账的读数\tcogneva_debt_edge_records,cogneva_debt_edge_verdicts=verdict
face_symmetry\tscripts/host/cogneva-face-symmetry-reading.sh\t面表对称\tcogneva_face_symmetry_unknown=verdict
";

    // The aggregator parses four columns; a roster that renders back to the same
    // rows it was imported from is the only proof that switching the source
    // changes nothing downstream. Comments do not survive — they are not rows,
    // and the aggregator skips them too, so the roster it parses is unchanged.
    #[test]
    fn a_roster_survives_parse_and_render_unchanged() {
        let rows = parse_roster(ROSTER).expect("parses");
        let expected: String = ROSTER
            .lines()
            .filter(|line| !line.starts_with('#'))
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(render(&rows), expected);
        // And it is a fixed point: a second trip through parse and render moves
        // no byte, so a reader cannot tell an imported row from a re-rendered one.
        let again = parse_roster(&render(&rows)).expect("parses");
        assert_eq!(render(&again), render(&rows));
    }

    #[test]
    fn comments_and_blank_lines_are_not_rows() {
        let rows = parse_roster("\n# only a comment\n\n").expect("parses");
        assert!(rows.is_empty());
    }

    // Two rows with one producer would be two nodes with one id: the second
    // import would overwrite the first and the roster would silently lose a
    // script. The name is the identity, so a repeat is refused rather than
    // deduplicated.
    #[test]
    fn a_producer_named_twice_is_refused() {
        let text = "a\tscripts/a.sh\tx\t\nb\tscripts/b.sh\ty\t\na\tscripts/c.sh\tz\t\n";
        let why = parse_roster(text).expect_err("refused");
        assert!(why.contains("appears twice"), "{why}");
    }

    #[test]
    fn a_producer_that_is_not_a_label_is_refused() {
        let why = parse_roster("DeadgEdge\tscripts/a.sh\tx\t\n").expect_err("refused");
        assert!(why.contains("not `[a-z][a-z0-9_]*`"), "{why}");
    }

    #[test]
    fn a_row_with_one_column_is_refused() {
        let why = parse_roster("debt_edge_hook\n").expect_err("refused");
        assert!(why.contains("fewer than two columns"), "{why}");
    }

    // The node is a governance row like a face: not executable, carried in the
    // governance workspace, and typed so `governance_nodes("reading")` finds it.
    #[test]
    fn a_row_becomes_a_governance_node_and_back() {
        let rows = parse_roster(ROSTER).expect("parses");
        let task = to_task(&rows[0], None);
        assert_eq!(task.id, "reading:debt_edge_hook");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(!task.is_executable);
        assert_eq!(task.workspace_id.as_deref(), Some(GOVERNANCE_WORKSPACE));
        assert_eq!(from_task(&task).as_ref(), Some(&rows[0]));
    }

    #[test]
    fn a_node_of_another_type_is_not_a_reading_row() {
        let mut task = Task::new(
            "face",
            TaskType::Custom("face".into()),
            serde_json::json!({"producer": "x", "path": "y"}),
        );
        task.input = serde_json::json!({"producer": "x", "path": "y"});
        assert_eq!(from_task(&task), None);
    }

    // Only pending rows are the roster: a cancelled row is a producer that was
    // retired by an import, and rendering it would send the aggregator back to a
    // script that no longer exists.
    #[test]
    fn the_rendering_is_sorted_so_two_reads_compare_byte_for_byte() {
        let rows = parse_roster(ROSTER).expect("parses");
        let mut reversed = rows.clone();
        reversed.reverse();
        assert_eq!(render(&rows), render(&reversed));
    }

    #[test]
    fn series_are_split_and_rejoined_without_their_spacing() {
        let row = parse_roster("p\ts\tn\ta=verdict , b , c\n").expect("parses");
        assert_eq!(row[0].series, vec!["a=verdict", "b", "c"]);
        assert_eq!(render(&row), "p\ts\tn\ta=verdict,b,c\n");
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
            "imported 3 rows: inserted=2 updated=1 unchanged=0 retired=1"
        );
    }
}
