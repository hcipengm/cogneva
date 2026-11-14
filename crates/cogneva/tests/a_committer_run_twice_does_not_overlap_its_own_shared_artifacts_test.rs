//! Gate: a change's artifacts are named by the change, not by the slot the run
//! that committed them happened to write into.
//!
//! The queue is one directory shared by the whole deployment, and every run of
//! the step that commits a change -- the `.diff` text plus the metadata record
//! beside it -- writes into that same directory. What keeps two runs apart is
//! only the name each artifact is given: the change's own id. Collapse that to
//! a fixed name ("the newest change", one `.json` per cycle) and the second run
//! does not add artifacts, it overwrites the first run's -- and every id but
//! the last one then reads back as "no such change", which is also the answer a
//! change nobody ever wrote gets. Neither file looks wrong on its own; the
//! queue simply holds one change fewer than the runs that committed into it,
//! and nothing notices until the missing one is looked for and cannot be found.
//!
//! So nothing here counts files, and nothing here asserts that the directory
//! "holds N entries": that shape stays green through exactly the overwrite this
//! file is named after, because one file is still one file. Each run is read
//! back instead through the readers the product itself uses, by its own id: the
//! record must be the one that run wrote (its id, its description, its status)
//! and the diff text must be that run's bytes. Two ids answering with their own
//! text is the same claim as "the two runs named two different files" -- one
//! file cannot answer for both -- and it is a claim about the artifacts rather
//! than about the number of them.
//!
//! What this cannot see: whether either run's artifacts are whole enough to
//! land -- that is the promotion gate's reading, over the artifact's bytes --
//! and whether the runs share anything *outside* this directory, such as a
//! staging binary or a backup slot. Those are written and pinned beside the
//! code that writes them, not here.

use std::path::Path;

use cog_reflection::{ChangePipeline, EvolutionKind, EvolutionResult, EvolutionStatus};

/// One run's change: the id it is named by, the status the run reached, and the
/// description the run's own step put there.
fn run(id: &str, status: EvolutionStatus) -> EvolutionResult {
    EvolutionResult {
        kind: EvolutionKind::CodeChange,
        artifact_id: id.to_string(),
        description: format!("goal-{id}"),
        content: String::new(),
        status,
        created_at: chrono::Utc::now(),
        eval_summary: None,
        tiering: None,
    }
}

/// The diff text a run writes under its own name.
fn diff_text(id: &str) -> String {
    format!("diff --git a/{id} b/{id}\n")
}

/// Commit one run's pair of artifacts into the shared queue directory.
async fn commit(pipeline: &ChangePipeline, queue: &Path, result: &EvolutionResult) {
    let id = result.artifact_id.as_str();
    tokio::fs::write(queue.join(format!("{id}.diff")), diff_text(id))
        .await
        .expect("the run writes its own diff text into the queue");
    pipeline
        .write_change_record(result)
        .await
        .expect("the run writes its own record into the queue");
}

#[tokio::test]
async fn a_committer_run_twice_does_not_overlap_its_own_shared_artifacts() {
    let temp = tempfile::tempdir().expect("temp dir");
    let queue = temp.path().join("changes");
    tokio::fs::create_dir_all(&queue).await.expect("queue dir");
    let pipeline = ChangePipeline::new(temp.path(), queue.to_path_buf(), false);

    let first = run("chg-first", EvolutionStatus::CompileChecked);
    let second = run("chg-second", EvolutionStatus::AwaitingReview);

    // Two runs over the one directory the deployment shares, both in flight at
    // once: the second is not waiting for the first to finish.
    tokio::join!(
        commit(&pipeline, &queue, &first),
        commit(&pipeline, &queue, &second)
    );

    for ran in [&first, &second] {
        let id = ran.artifact_id.as_str();

        let text = pipeline
            .read_change_content(id)
            .await
            .unwrap_or_else(|| panic!("{id}: its diff text is not in the shared queue"));
        assert_eq!(
            text,
            diff_text(id),
            "{id}: the queue answered with another run's diff text"
        );

        let record = pipeline.read_change_record(id).await.unwrap_or_else(|| {
            panic!("{id}: its record is gone -- another run wrote over its name")
        });
        assert_eq!(
            record.artifact_id, id,
            "{id}: the record read back under this id belongs to another change"
        );
        assert_eq!(
            record.description, ran.description,
            "{id}: the record lost what this run said the change was for"
        );
        assert_eq!(
            record.status, ran.status,
            "{id}: the record read back carries the other run's status"
        );
    }
}
