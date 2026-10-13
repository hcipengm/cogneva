//! End-to-end: a face's document becomes tasks in the DAG.
//!
//! The two halves the dispatch loop joins — compiling a document into rows, and
//! the planner putting verified rows into the DAG — are tested apart elsewhere.
//! What this covers is the seam between them, because that is where a mistake
//! costs nothing visible: a row that compiles but is never queued, or a
//! dependency that is dropped on the way in, both leave the loop reporting a
//! face as kept while its plan sits nowhere.
//!
//! The document is a local fixture. Nothing here reaches a platform endpoint;
//! the fetch side is exercised against a canned client in the module's own
//! tests.

use std::sync::Arc;

use cog_core::{ActionPlanner, SkillRegistry, TaskStatus};
use cog_orchestrator::sop::{compile, TaskFaceSource};
use cog_orchestrator::{ActionPlanOrchestrator, DagExecutor};

/// A face whose second task waits on the first, so the mapping of `blocked_by`
/// onto the DAG is observable rather than assumed.
const FIXTURE: &str = r#"
Some prose above the block that is not part of the plan.

```yaml
readings:
  - name: build_seconds
```

```yaml
face: release-checks
ledger_key: task.id
tasks:
  - id: build
    name: build.check
    face: build
    role: builder
    product: out/build.json
  - id: verify
    name: build.verify
    face: verify
    role: verifier
    blocked_by: [build]
    product: out/verify.json
```
"#;

fn source() -> TaskFaceSource {
    TaskFaceSource {
        id: "release-checks".into(),
        repo: "example/SOP".into(),
        sop_path: "SOP.md".into(),
        cadence_secs: 60,
    }
}

/// The fixture's two rows land in the DAG under the face's own namespace, with
/// the dependency pointing at the sibling's namespaced id and not at the bare
/// one the document spelled.
#[tokio::test]
async fn a_fixture_document_lands_its_tasks_in_the_dag() {
    let dag = Arc::new(DagExecutor::new("ws-sop-integration".to_string()));
    let planner = ActionPlanOrchestrator::new().with_dag_executor(dag.clone());
    let registry = SkillRegistry::new();

    let tasks = compile(&source(), FIXTURE).expect("the fixture compiles");
    let queued = planner
        .process_goal("face:release-checks", tasks, &registry)
        .await
        .expect("verified rows are injected");

    let queued: std::collections::BTreeSet<String> = queued.into_iter().collect();
    assert_eq!(
        queued,
        [
            "release-checks:build".to_string(),
            "release-checks:verify".to_string()
        ]
        .into_iter()
        .collect(),
        "a verified row the planner does not queue is a plan that never runs"
    );

    let built = dag
        .get_task("release-checks:build")
        .await
        .expect("the first row is in the DAG");
    assert!(built.is_executable, "a task the document declared is work");
    assert_eq!(built.status, TaskStatus::Pending);
    assert!(built.blocked_by.is_empty());

    let verify = dag
        .get_task("release-checks:verify")
        .await
        .expect("the dependent row is in the DAG");
    assert_eq!(
        verify.blocked_by,
        vec!["release-checks:build".to_string()],
        "the dependency has to name the id the DAG holds, not the one the document wrote"
    );
}

/// Same document twice is one plan. The loop re-reads the document on every
/// tick and only the text's hash stops it re-injecting; a call that got past
/// that guard must still not queue a second copy, or a restart that lost the
/// hash would duplicate the whole plan onto a live DAG.
#[tokio::test]
async fn injecting_the_same_document_twice_queues_nothing_the_second_time() {
    let dag = Arc::new(DagExecutor::new("ws-sop-idempotent".to_string()));
    let planner = ActionPlanOrchestrator::new().with_dag_executor(dag.clone());
    let registry = SkillRegistry::new();

    let first = planner
        .process_goal(
            "face:release-checks",
            compile(&source(), FIXTURE).unwrap(),
            &registry,
        )
        .await
        .expect("the first pass queues the plan");
    assert_eq!(first.len(), 2);

    let second = planner
        .process_goal(
            "face:release-checks",
            compile(&source(), FIXTURE).unwrap(),
            &registry,
        )
        .await
        .expect("a re-injection is recognized, not rejected as a duplicate");
    assert!(
        second.is_empty(),
        "the same rows are already held; echoing them back would read as queued just now"
    );
}
