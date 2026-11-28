//! `cogneva governance fronts` — the closed set of fronts, read from the graph.
//!
//! Why this is a graph node and not a file: `scripts/data/fronts.tsv` said, by
//! hand, which faces the self-sufficiency gate judges. The gate's own scope was
//! therefore an edit away from being narrowed until it passed, and the file's
//! owner column was a second answer to a question the face node already answers.
//! A front is a declaration -- "these faces are the lines somebody must drive" --
//! and declarations belong on the governed channel with every other one.
//!
//! What a front node carries is only the closure itself: its name and what it
//! covers. Ownership is not copied here. The face node owns it, a signed
//! handover moves it, and a projection measures it; a third copy on the front row
//! would be one more place where a stale name looks like a live answer.
//!
//! The two columns are the gate's contract: it pairs the first column with the
//! driver roster's first column, exactly as it did with the file.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

use cog_core::{GovernanceStore, Task, TaskStatus, TaskType, GOVERNANCE_WORKSPACE};

/// The task type of a front node.
pub const FRONT_TASK_TYPE: &str = "front";

/// One row of the closed set: the name the gate pairs on, and what it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontRow {
    pub name: String,
    pub what: String,
}

/// The id a front's node carries, namespaced so it cannot collide with a face,
/// a driver, a handover or a reading that happens to share the name.
pub fn node_id(name: &str) -> String {
    format!("front:{name}")
}

/// Parse the import input. Columns after the second are accepted and ignored:
/// the historical file carried owner and deputy there, and those are the face
/// node's to answer. Accepting the file whole is what lets the one-time move be
/// `git show <rev>:scripts/data/fronts.tsv | import-fronts --from -` without a
/// hand edit in between; ignoring the columns is what keeps the import from
/// creating a second ownership truth.
pub fn parse_roster(text: &str) -> Result<(Vec<FrontRow>, usize), String> {
    let mut rows: Vec<FrontRow> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut ignored_ownership = 0;
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let at = index + 1;
        let columns: Vec<&str> = line.split('\t').collect();
        if columns.len() < 2 {
            return Err(format!(
                "line {at}: fewer than two columns (front<TAB>what-it-covers)"
            ));
        }
        let name = columns[0].trim();
        if name.is_empty() {
            return Err(format!("line {at}: no front name"));
        }
        if columns.len() > 2 {
            ignored_ownership += 1;
        }
        if !seen.insert(name.to_string()) {
            return Err(format!(
                "line {at}: `{name}` is declared twice — the closed set names a front once"
            ));
        }
        rows.push(FrontRow {
            name: name.to_string(),
            what: columns[1].trim().to_string(),
        });
    }
    Ok((rows, ignored_ownership))
}

/// The node a row is stored as. Kept whole on rewrite: only the fields this
/// module owns are set, so a field somebody else added survives the import.
fn to_task(row: &FrontRow, existing: Option<Task>) -> Task {
    let mut task = existing.unwrap_or_else(|| {
        Task::new(
            node_id(&row.name),
            TaskType::Custom(FRONT_TASK_TYPE.into()),
            serde_json::json!({}),
        )
    });
    task.id = node_id(&row.name);
    task.task_type = TaskType::Custom(FRONT_TASK_TYPE.into());
    task.status = TaskStatus::Pending;
    task.is_executable = false;
    task.workspace_id = Some(GOVERNANCE_WORKSPACE.to_string());
    let mut input = match std::mem::take(&mut task.input) {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    input.insert("front".into(), row.name.clone().into());
    input.insert("what".into(), row.what.clone().into());
    task.input = serde_json::Value::Object(input);
    task
}

/// A stored node back into a row. `None` for a node that is not a front row --
/// the caller skips it rather than inventing a front.
fn from_task(task: &Task) -> Option<FrontRow> {
    if !matches!(&task.task_type, TaskType::Custom(kind) if kind == FRONT_TASK_TYPE) {
        return None;
    }
    let name = task.input.get("front")?.as_str()?.to_string();
    let what = task
        .input
        .get("what")
        .and_then(|w| w.as_str())
        .unwrap_or("")
        .to_string();
    Some(FrontRow { name, what })
}

/// The two columns, sorted by name so the output does not depend on the order
/// the graph returned the nodes in. Order is not a judgment: the gate asks
/// "which fronts are in scope", not "in what order were they declared".
pub fn render(rows: &[FrontRow]) -> String {
    let mut sorted: Vec<&FrontRow> = rows.iter().collect();
    sorted.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
    let mut out = String::new();
    for row in sorted {
        out.push_str(&format!("{}\t{}\n", row.name, row.what));
    }
    out
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportSummary {
    /// Rows read from the input.
    pub rows: usize,
    /// Rows whose columns past the second were ignored (the historical
    /// ownership columns). Counted so the operator sees that the file carried
    /// them and the graph did not take them.
    pub ignored_ownership: usize,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    /// Fronts the graph held and this input no longer names. The closed set is
    /// authoritative in both directions: a front that left the set must leave
    /// the gate's scope, and only an import can say so.
    pub retired: usize,
}

impl ImportSummary {
    pub fn line(&self) -> String {
        format!(
            "imported {} rows into front nodes: inserted={} updated={} unchanged={} retired={} ignored-ownership-columns={}",
            self.rows,
            self.inserted,
            self.updated,
            self.unchanged,
            self.retired,
            self.ignored_ownership
        )
    }
}

/// Put a complete closed set into the graph: fronts it names become pending
/// nodes, fronts it no longer names are cancelled. Idempotent.
pub async fn import_from_text(
    store: &dyn GovernanceStore,
    text: &str,
) -> Result<ImportSummary, Box<dyn Error>> {
    import(store, text, false).await
}

/// Put the fronts an input names into the graph without retiring the others.
/// The shape of a platform declaration that adds one front without restating
/// the whole set; a front nobody named yet is not a front to remove.
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
    let (rows, ignored_ownership) = parse_roster(text)?;
    let mut existing: BTreeMap<String, Task> = BTreeMap::new();
    for node in store.governance_nodes(FRONT_TASK_TYPE).await? {
        if from_task(&node).is_some() {
            existing.insert(node.id.clone(), node);
        }
    }
    let mut summary = ImportSummary {
        rows: rows.len(),
        ignored_ownership,
        ..ImportSummary::default()
    };
    for row in &rows {
        let id = node_id(&row.name);
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
    if upsert_only {
        return Ok(summary);
    }
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

/// The closed set as the gate parses it: pending rows only, two columns.
pub async fn fronts(store: &dyn GovernanceStore) -> Result<String, Box<dyn Error>> {
    let mut rows: Vec<FrontRow> = Vec::new();
    for node in store.governance_nodes(FRONT_TASK_TYPE).await? {
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
# the file this replaced
变更管理\t变更从提出到落地的全过程\tf5\t8b2
原则管理\t原则的产生、投影、消费与废退\tf5\t70
任务管理\t待办从哪里来、做完怎么记\tf5\t8b2
";

    #[test]
    fn a_roster_survives_parse_and_render_without_its_ownership_columns() {
        let (rows, ignored) = parse_roster(ROSTER).expect("parses");
        assert_eq!(ignored, 3);
        // Sorted by the name's bytes, so the output does not depend on the
        // order the graph returned the nodes in.
        let expected = "\
任务管理\t待办从哪里来、做完怎么记
原则管理\t原则的产生、投影、消费与废退
变更管理\t变更从提出到落地的全过程
";
        assert_eq!(render(&rows), expected);
        let (again, _) = parse_roster(&render(&rows)).expect("parses");
        assert_eq!(render(&again), expected);
    }

    #[test]
    fn comments_and_blank_lines_are_not_rows() {
        let (rows, ignored) = parse_roster("\n# only a comment\n\n").expect("parses");
        assert!(rows.is_empty());
        assert_eq!(ignored, 0);
    }

    #[test]
    fn a_row_with_one_column_is_refused() {
        let why = parse_roster("任务管理\n").expect_err("refused");
        assert!(why.contains("fewer than two columns"), "{why}");
    }

    #[test]
    fn a_front_declared_twice_is_refused() {
        let why = parse_roster("任务管理\ta\n任务管理\tb\n").expect_err("refused");
        assert!(why.contains("declared twice"), "{why}");
    }

    #[test]
    fn an_empty_name_is_refused() {
        let why = parse_roster("\twhat\n").expect_err("refused");
        assert!(why.contains("no front name"), "{why}");
    }

    // The description is a reading of the declaration, not something the import
    // may invent: a row that said nothing keeps saying nothing.
    #[test]
    fn an_empty_description_survives_the_round_trip() {
        let (rows, _) = parse_roster("通信\t\n").expect("parses");
        assert_eq!(rows[0].what, "");
        let back = from_task(&to_task(&rows[0], None)).expect("reads back");
        assert_eq!(back.what, "");
    }

    // A node is a governance row like a face: not executable, in the governance
    // workspace, typed so `governance_nodes("front")` finds it.
    #[test]
    fn a_row_becomes_one_node_and_comes_back() {
        let (rows, _) = parse_roster("通信\t节点之间与对外的那条信道\n").expect("parses");
        let task = to_task(&rows[0], None);
        assert_eq!(task.id, "front:通信");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(!task.is_executable);
        assert_eq!(task.workspace_id.as_deref(), Some(GOVERNANCE_WORKSPACE));
        assert_eq!(from_task(&task), Some(rows[0].clone()));
    }

    #[test]
    fn a_node_of_another_type_is_not_a_front_row() {
        let task = Task::new(
            "driver:u",
            TaskType::Custom("driver".into()),
            serde_json::json!({"front": "任务管理", "what": "x"}),
        );
        assert_eq!(from_task(&task), None);
    }

    #[test]
    fn the_rendering_is_sorted_so_two_reads_compare_byte_for_byte() {
        let (rows, _) = parse_roster(ROSTER).expect("parses");
        let mut reversed = rows.clone();
        reversed.reverse();
        assert_eq!(render(&rows), render(&reversed));
    }

    #[test]
    fn the_summary_line_names_every_direction() {
        let summary = ImportSummary {
            rows: 7,
            ignored_ownership: 7,
            inserted: 6,
            updated: 1,
            unchanged: 0,
            retired: 0,
        };
        assert_eq!(
            summary.line(),
            "imported 7 rows into front nodes: inserted=6 updated=1 unchanged=0 retired=0 ignored-ownership-columns=7"
        );
    }

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
            unreachable!("fronts never touch faces")
        }

        async fn open_face_bound_tasks(&self) -> cog_core::SFResult<Vec<Task>> {
            unreachable!("fronts never touch faces")
        }

        async fn save_face_node(&self, _task: &Task) -> cog_core::SFResult<()> {
            unreachable!("fronts never touch faces")
        }

        async fn save_face_node_if_revision(
            &self,
            _task: &Task,
            _expected: i64,
        ) -> cog_core::SFResult<bool> {
            unreachable!("fronts never touch faces")
        }

        async fn save_governance_node_if_status(
            &self,
            _task: &Task,
            _expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            unreachable!("a front import writes unconditionally")
        }

        async fn save_governance_node_with_event(
            &self,
            _event: &Task,
            _node: &Task,
            _expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            unreachable!("a front import carries no event")
        }

        async fn apply_handover(
            &self,
            _handover: &Task,
            _face: &Task,
            _expected_revision: i64,
        ) -> cog_core::SFResult<Option<usize>> {
            unreachable!("fronts never move hands")
        }

        async fn reassign_face_work(
            &self,
            _face_id: &str,
            _to_unit: &str,
        ) -> cog_core::SFResult<usize> {
            unreachable!("fronts never move work")
        }

        async fn release_face_work(&self, _face_id: &str) -> cog_core::SFResult<usize> {
            unreachable!("fronts never release work")
        }

        async fn ensure_face_duty(&self, _row: &Task) -> cog_core::SFResult<bool> {
            unreachable!("fronts are not duties")
        }
    }

    #[tokio::test]
    async fn importing_the_same_set_twice_is_a_fixed_point() {
        let store = Rows::default();
        let first = import_from_text(&store, ROSTER).await.expect("imports");
        assert_eq!(first.rows, 3);
        assert_eq!(first.inserted, 3);
        assert_eq!(first.ignored_ownership, 3);
        let before = fronts(&store).await.expect("reads");
        let second = import_from_text(&store, ROSTER).await.expect("imports");
        assert_eq!(second.inserted, 0);
        assert_eq!(second.updated, 0);
        assert_eq!(second.unchanged, 3);
        assert_eq!(second.retired, 0);
        assert_eq!(fronts(&store).await.expect("reads"), before);
    }

    // The closed set is the gate's scope; an import that only added would keep
    // judging a front that was removed from the set.
    #[tokio::test]
    async fn a_front_the_input_stops_naming_leaves_the_set() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let summary = import_from_text(&store, "通信\t节点之间与对外的那条信道\n")
            .await
            .expect("imports");
        assert_eq!(summary.retired, 3);
        let now = fronts(&store).await.expect("reads");
        assert_eq!(now, "通信\t节点之间与对外的那条信道\n");
    }

    // Adding one front must not narrow the set to that one front.
    #[tokio::test]
    async fn upsert_leaves_fronts_the_input_does_not_name_in_place() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let summary = upsert_from_text(&store, "通信\t节点之间与对外的那条信道\n")
            .await
            .expect("upserts");
        assert_eq!(summary.retired, 0);
        assert_eq!(summary.inserted, 1);
        let now = fronts(&store).await.expect("reads");
        assert!(now.contains("任务管理"), "{now}");
        assert!(now.contains("通信"), "{now}");
    }

    #[tokio::test]
    async fn changing_a_description_reaches_the_set() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let edited = ROSTER.replace("原则的产生、投影、消费与废退", "原则的产生与废退");
        let summary = import_from_text(&store, &edited).await.expect("imports");
        assert_eq!(summary.updated, 1);
        assert!(fronts(&store)
            .await
            .expect("reads")
            .contains("原则的产生与废退"));
    }

    #[tokio::test]
    async fn a_cancelled_front_is_not_part_of_the_set() {
        let store = Rows::default();
        import_from_text(&store, ROSTER).await.expect("imports");
        let mut rows = store.dump();
        let index = rows
            .iter()
            .position(|row| row.id == "front:任务管理")
            .expect("written");
        rows[index].status = TaskStatus::Cancelled;
        *store.0.lock().expect("unpoisoned") = rows;
        let now = fronts(&store).await.expect("reads");
        assert!(!now.contains("任务管理"), "{now}");
        assert_eq!(now.lines().count(), 2);
    }

    #[tokio::test]
    async fn an_input_that_does_not_parse_writes_nothing() {
        let store = Rows::default();
        let why = import_from_text(&store, "任务管理\ta\n任务管理\tb\n")
            .await
            .expect_err("refused");
        assert!(why.to_string().contains("declared twice"), "{why}");
        assert!(store.dump().is_empty());
    }
}
