//! Compile a face's `SOP.md` into verified DAG tasks.
//!
//! The document a person edits carries its machine-readable half in a fenced
//! `yaml` block. The block is not the first one in the file — a face's SOP holds
//! more than one, and the tasks are not always the one that comes first — so the
//! extractor looks for the block whose top level carries `tasks:`, not for "the
//! first fenced block". Every task is handed to the planner already verified, so
//! the planner's existing "all verified" branch injects them straight into the
//! DAG and skips decomposition entirely.

use chrono::Utc;
use cog_core::{ActionPlannerMeta, ActionPlannerSource, Task, TaskType};
use serde::Deserialize;
use std::collections::HashSet;
use thiserror::Error;

/// One configured task face: where its SOP lives and how often to re-read it.
///
/// The `repo` field holds an `owner/repo` pair rather than a URL, because the
/// fetch composes it into a platform endpoint — a value that reads as a URL
/// invites a caller to use it as one, and the path it would need appended is not
/// the path it carries. The `id` names the face on the readings; it is the
/// configuration's own identity and is available even when the document cannot
/// be fetched or parsed, so a failure is attributable to a named face.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TaskFaceSource {
    pub id: String,
    pub repo: String,
    pub sop_path: String,
    pub cadence_secs: u64,
}

/// The machine-readable half of a face SOP.
#[derive(Debug, Clone, Deserialize)]
pub struct SopDoc {
    /// The face these tasks belong to. It prefixes every task id and every
    /// dependency reference, so a compiled task never collides with a task from
    /// another face that happens to reuse an id.
    pub face: String,
    pub tasks: Vec<SopTask>,
}

/// One task as the SOP declares it.
#[derive(Debug, Clone, Deserialize)]
pub struct SopTask {
    pub id: String,
    pub name: String,
    pub face: String,
    pub role: String,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    pub product: String,
    #[serde(default)]
    pub gate: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Why a SOP document could not be compiled.
///
/// Each variant names the document's own defect rather than a downstream
/// symptom: an empty `tasks` list and a `tasks` key that was never there are
/// different mistakes with different fixes, and folding them into one "invalid
/// SOP" would make the log line useless to whoever edits the document.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SopCompileError {
    #[error("no fenced yaml block with a top-level `tasks:` key was found")]
    MissingTaskBlock,
    #[error("the tasks block is not a valid SOP: {0}")]
    InvalidYaml(String),
    #[error("the SOP declares an empty face")]
    EmptyFace,
    #[error("the SOP declares no tasks")]
    NoTasks,
    #[error("the SOP declares the task id `{0}` more than once")]
    DuplicateId(String),
    #[error("task `{task}` is blocked by `{dep}`, which this SOP does not declare")]
    UnknownDependency { task: String, dep: String },
}

/// The info string of the fenced block the tasks live in.
const YAML_FENCE: &str = "yaml";

/// Return the fenced YAML block whose top level carries a `tasks:` key.
///
/// A SOP file holds several fenced blocks; the tasks are identified by their
/// content, not their position, so a block added above them does not silently
/// become the one that is compiled.
fn extract_tasks_block(sop_md: &str) -> Option<&str> {
    let mut candidates: Vec<&str> = Vec::new();
    let mut open_at: Option<usize> = None;
    let mut cursor = 0usize;
    for line in sop_md.split_inclusive('\n') {
        let line_end = cursor + line.len();
        let trimmed = line.trim_end_matches(['\n', '\r']);
        match open_at {
            None => {
                let info = trimmed.trim_start();
                if let Some(rest) = info.strip_prefix("```") {
                    if rest.trim() == YAML_FENCE {
                        open_at = Some(line_end);
                    }
                }
            }
            Some(start) => {
                if trimmed.trim() == "```" {
                    candidates.push(&sop_md[start..cursor]);
                    open_at = None;
                }
            }
        }
        cursor = line_end;
    }
    candidates.into_iter().find(|block| {
        serde_yaml::from_str::<serde_yaml::Value>(block)
            .ok()
            .is_some_and(|doc| doc.get("tasks").is_some())
    })
}

/// Compile `sop_md` into the tasks the DAG should run.
///
/// The whole batch is verified as one: each task carries `verified: true` and the
/// document's content hash as its version, so a re-compile of an unchanged SOP
/// produces byte-identical task identities and a re-compile of a changed one
/// carries a new version — which is what lets the DAG's idempotent injection tell
/// "the same plan again" from "a new plan" without the caller tracking either.
///
/// Nothing here detects cycles between tasks: that is the DAG's own validation,
/// reached the moment the batch is admitted, and duplicating it would give two
/// answers to one question. The gates kept here are the ones the DAG cannot see:
/// a document that references a task it never declares would otherwise inject a
/// dependency that can never be satisfied and a batch that silently never runs.
pub fn compile(face_source: &TaskFaceSource, sop_md: &str) -> Result<Vec<Task>, SopCompileError> {
    let block = extract_tasks_block(sop_md).ok_or(SopCompileError::MissingTaskBlock)?;
    let doc: SopDoc =
        serde_yaml::from_str(block).map_err(|e| SopCompileError::InvalidYaml(e.to_string()))?;

    if doc.face.trim().is_empty() {
        return Err(SopCompileError::EmptyFace);
    }
    if doc.tasks.is_empty() {
        return Err(SopCompileError::NoTasks);
    }

    let mut seen: HashSet<&str> = HashSet::with_capacity(doc.tasks.len());
    for task in &doc.tasks {
        if !seen.insert(task.id.as_str()) {
            return Err(SopCompileError::DuplicateId(task.id.clone()));
        }
    }
    for task in &doc.tasks {
        for dep in &task.blocked_by {
            if !seen.contains(dep.as_str()) {
                return Err(SopCompileError::UnknownDependency {
                    task: task.id.clone(),
                    dep: dep.clone(),
                });
            }
        }
    }

    let sop_hash = blake3::hash(sop_md.as_bytes()).to_hex().to_string();
    let goal_id = format!("face:{}", doc.face);
    let now = Utc::now();
    let mut out = Vec::with_capacity(doc.tasks.len());
    for task in &doc.tasks {
        let id = format!("{}:{}", doc.face, task.id);
        let input = serde_json::json!({
            "face": doc.face,
            "sop_task_id": task.id,
            "name": task.name,
            "role": task.role,
            "product": task.product,
            "gate": task.gate,
            "note": task.note,
            "repo": face_source.repo,
        });
        let mut compiled = Task::new(id, TaskType::Custom(task.name.clone()), input);
        compiled.blocked_by = task
            .blocked_by
            .iter()
            .map(|dep| format!("{}:{}", doc.face, dep))
            .collect();
        compiled.is_executable = true;
        compiled.goal_id = Some(goal_id.clone());
        compiled.action_planner_meta = Some(ActionPlannerMeta {
            verified: true,
            version: Some(sop_hash.clone()),
            note: None,
            source: Some(ActionPlannerSource::UserProvided),
            confidence: None,
            timestamp: Some(now),
        });
        out.push(compiled);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::TaskStatus;

    fn source() -> TaskFaceSource {
        TaskFaceSource {
            id: "example-face".into(),
            repo: "example/sop-repo".into(),
            sop_path: "SOP.md".into(),
            cadence_secs: 3600,
        }
    }

    /// A SOP with two blocks, the tasks one placed after a readings block: the
    /// extractor must key on the `tasks:` content, not on position.
    const TWO_BLOCK_SOP: &str = r#"
# Face plan

Some prose.

```yaml
face: example-face
readings:
  - fact.verify
  - topic.select
```

## Tasks

```yaml
face: example-face
ledger_key: task.id
tasks:
  - id: A1
    name: fact.verify
    face: facts
    role: fact-checker
    blocked_by: []
    product: facts/facts.yaml
    gate: G4
  - id: A2
    name: topic.select
    face: claims
    role: topic-steward
    blocked_by: [A1]
    product: claims/claim.yaml
    gate: G5
```
"#;

    #[test]
    fn compiles_the_declared_tasks_into_verified_rows() {
        let tasks = compile(&source(), TWO_BLOCK_SOP).expect("compiles");
        assert_eq!(tasks.len(), 2);

        let ids: Vec<&str> = tasks.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["example-face:A1", "example-face:A2"]);

        let a1 = &tasks[0];
        assert_eq!(a1.task_type, TaskType::Custom("fact.verify".into()));
        assert!(a1.is_executable);
        assert_eq!(a1.status, TaskStatus::Pending);
        assert_eq!(a1.goal_id.as_deref(), Some("face:example-face"));
        assert!(a1.blocked_by.is_empty());
        let meta = a1.action_planner_meta.as_ref().expect("verified meta");
        assert!(meta.verified);
        assert_eq!(meta.source, Some(ActionPlannerSource::UserProvided));
        assert!(meta.version.is_some(), "the SOP hash is the version");
        assert_eq!(a1.input["face"], serde_json::json!("example-face"));
        assert_eq!(a1.input["repo"], serde_json::json!("example/sop-repo"));
        assert_eq!(a1.input["sop_task_id"], serde_json::json!("A1"));
        assert_eq!(a1.input["product"], serde_json::json!("facts/facts.yaml"));
        assert_eq!(a1.input["gate"], serde_json::json!("G4"));

        let a2 = &tasks[1];
        assert_eq!(a2.blocked_by, ["example-face:A1"]);
        assert!(a2.action_planner_meta.as_ref().unwrap().verified);
    }

    /// The tasks block is chosen by content: a readings block above it must not
    /// be mistaken for the plan.
    #[test]
    fn the_tasks_block_is_chosen_by_content_not_position() {
        let tasks = compile(&source(), TWO_BLOCK_SOP).expect("compiles");
        assert!(
            tasks.iter().all(|t| t.input.get("readings").is_none()),
            "the readings block must not be the one compiled"
        );
    }

    /// The same document compiles to the same version, and a changed document
    /// compiles to a different one — the version is what tells "same plan again"
    /// from "a new plan" at injection time.
    #[test]
    fn the_version_tracks_the_document_content() {
        let first = compile(&source(), TWO_BLOCK_SOP).expect("compiles");
        let again = compile(&source(), TWO_BLOCK_SOP).expect("compiles");
        let a = first[0]
            .action_planner_meta
            .as_ref()
            .unwrap()
            .version
            .clone();
        let b = again[0]
            .action_planner_meta
            .as_ref()
            .unwrap()
            .version
            .clone();
        assert_eq!(a, b);

        let changed = TWO_BLOCK_SOP.replace("gate: G5", "gate: G6");
        let other = compile(&source(), &changed).expect("compiles");
        assert_ne!(a, other[0].action_planner_meta.as_ref().unwrap().version);
    }

    #[test]
    fn a_dependency_on_an_undeclared_task_is_refused() {
        let sop = r#"
```yaml
face: example-face
tasks:
  - id: A1
    name: fact.verify
    face: facts
    role: fact-checker
    blocked_by: [Z9]
    product: facts/facts.yaml
```
"#;
        assert_eq!(
            compile(&source(), sop),
            Err(SopCompileError::UnknownDependency {
                task: "A1".into(),
                dep: "Z9".into(),
            })
        );
    }

    #[test]
    fn a_document_with_no_tasks_is_refused() {
        let sop = "```yaml\nface: example-face\ntasks: []\n```\n";
        assert_eq!(compile(&source(), sop), Err(SopCompileError::NoTasks));
    }

    #[test]
    fn a_duplicate_task_id_is_refused() {
        let sop = r#"
```yaml
face: example-face
tasks:
  - id: A1
    name: fact.verify
    face: facts
    role: r
    product: p
  - id: A1
    name: topic.select
    face: claims
    role: r
    product: p
```
"#;
        assert_eq!(
            compile(&source(), sop),
            Err(SopCompileError::DuplicateId("A1".into()))
        );
    }

    #[test]
    fn a_document_with_no_tasks_block_is_refused() {
        let sop = "# no fenced plan here\n\n```yaml\nreadings: [x]\n```\n";
        assert_eq!(
            compile(&source(), sop),
            Err(SopCompileError::MissingTaskBlock)
        );
    }
}
