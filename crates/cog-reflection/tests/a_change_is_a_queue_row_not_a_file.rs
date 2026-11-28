//! 一条变更在队列里是**一行事件**，不是盘上一份 `.json`。
//!
//! 这不是「换个存法」：记录文件曾经是「文件在不在」被读成「变更在不在」的地方。
//! 一次重启让一条已经判过的变更与一条刚生成的变更读起来一样，于是一条永远不可能
//! 应用的变更每轮重试；记录只有那一个写者，没有重放，也没人核过它与真相是否一致。
//!
//! 两条判据，缺一条就还留着一条路回去：
//!
//! * 行为的：记下一条候选之后，它旁边不多出任何 `.json` 文件，而候选读得回来；
//! * 结构的：写队列的那个模块里没有任何一处拼出 `.json` 路径——一份可以再长出来
//!   的写法，只要还留在那个模块里，下一次「补一份缓存」就会把它带回来。

use std::sync::Arc;

use cog_core::{CandidateOutcome, ChangeCandidate, ChangeCandidateStore};

fn candidate(id: &str) -> ChangeCandidate {
    ChangeCandidate {
        unit_of_work: None,
        artifact_id: id.to_string(),
        kind: "code_change".to_string(),
        description: "why this change".to_string(),
        content: "--- a/x\n+++ b/x\n".to_string(),
        created_at: chrono::Utc::now(),
        recorded_status: "compile_checked".to_string(),
        status: "candidate".to_string(),
        decision_reason: None,
        event_id: String::new(),
        eval_summary: None,
        tiering: None,
    }
}

/// 记下候选之后，队列目录里只有 `.diff`，没有任何 `.json`。
#[tokio::test]
async fn a_recorded_candidate_leaves_no_json_file_beside_its_diff() {
    let temp = tempfile::tempdir().unwrap();
    let change_dir = temp.path().join("changes");
    tokio::fs::create_dir_all(&change_dir).await.unwrap();
    tokio::fs::write(change_dir.join("chg-1.diff"), "--- a/x\n+++ b/x\n")
        .await
        .unwrap();

    let store: Arc<dyn ChangeCandidateStore> =
        Arc::new(cog_test_support::MemoryCandidateStore::new());
    let pipeline = cog_reflection::ChangePipeline::new(temp.path(), &change_dir, true)
        .with_candidates(store.clone());
    pipeline
        .write_change_record(&cog_reflection::types::EvolutionResult {
            unit_of_work: None,
            kind: cog_reflection::types::EvolutionKind::CodeChange,
            artifact_id: "chg-1".to_string(),
            description: "why this change".to_string(),
            content: String::new(),
            status: cog_reflection::types::EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
            tiering: None,
        })
        .await
        .unwrap();

    let mut names: Vec<String> = Vec::new();
    let mut entries = tokio::fs::read_dir(&change_dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().to_string());
    }
    names.sort();
    assert_eq!(
        names,
        vec!["chg-1.diff".to_string()],
        "队列的写者只追加事件：盘上不该再出现一份记录"
    );

    let row = pipeline
        .read_change_record("chg-1")
        .await
        .expect("the candidate is readable from the queue");
    assert_eq!(
        row.status,
        cog_reflection::types::EvolutionStatus::CompileChecked
    );
    assert_eq!(row.description, "why this change");
}

/// 定案之后那条候选仍然读得出来，且带着定案的结局——盘上依然没有 `.json`。
#[tokio::test]
async fn a_decided_candidate_stays_readable_without_a_file() {
    let temp = tempfile::tempdir().unwrap();
    let change_dir = temp.path().join("changes");
    tokio::fs::create_dir_all(&change_dir).await.unwrap();
    tokio::fs::write(change_dir.join("chg-2.diff"), "--- a/x\n+++ b/x\n")
        .await
        .unwrap();

    let store: Arc<dyn ChangeCandidateStore> =
        Arc::new(cog_test_support::MemoryCandidateStore::new());
    store.record(&candidate("chg-2")).await.unwrap();
    let pipeline = cog_reflection::ChangePipeline::new(temp.path(), &change_dir, true)
        .with_candidates(store.clone());
    pipeline
        .retire_change("chg-2", CandidateOutcome::Rejected, "tests failed")
        .await
        .unwrap();

    assert!(pipeline.pending_changes(None).await.unwrap().is_empty());
    let row = store
        .read("chg-2")
        .await
        .unwrap()
        .expect("the row survives");
    assert_eq!(row.status, "rejected");
    assert_eq!(row.decision_reason.as_deref(), Some("tests failed"));
    assert!(
        !change_dir.join("chg-2.json").exists(),
        "定案是又一条事件，不是一份新文件"
    );
}

/// 把盘上那份 `.diff` 删掉，同一段正文仍然读得回来——它已经在流里。
///
/// `.diff` 从「正本」降为「导出工件」，判据就是这一条：盘丢了，事实还在。
#[tokio::test]
async fn a_diff_deleted_from_disk_is_still_readable_from_the_stream() {
    let temp = tempfile::tempdir().unwrap();
    let change_dir = temp.path().join("changes");
    tokio::fs::create_dir_all(&change_dir).await.unwrap();
    let diff = "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n";
    tokio::fs::write(change_dir.join("chg-3.diff"), diff)
        .await
        .unwrap();

    let store: Arc<dyn ChangeCandidateStore> =
        Arc::new(cog_test_support::MemoryCandidateStore::new());
    let mut row = candidate("chg-3");
    row.content = diff.to_string();
    store.record(&row).await.unwrap();
    let pipeline = cog_reflection::ChangePipeline::new(temp.path(), &change_dir, true)
        .with_candidates(store.clone());

    tokio::fs::remove_file(change_dir.join("chg-3.diff"))
        .await
        .unwrap();

    assert_eq!(
        pipeline.read_change_content("chg-3").await.as_deref(),
        Some(diff),
        "the diff text travels with the candidate; the file on disk is an export"
    );
}

/// 写队列的那个模块里没有任何一处拼出 `.json` 路径。
///
/// 这是「不会再长回来」的那一半：行为测试只说得出现在这条路上没有文件，说不出一份
/// 缓存会不会被下一个人加回去。路径拼写就是那条路的形式，它必须不在这个模块里。
#[test]
fn the_queue_module_spells_no_record_file_path() {
    // 判的是**拼出来的**路径：这个模块当然还会点名别的 `.json`（chart 的配置、
    // package.json），那些不是队列；把产物 id 拼成一个文件名的写法才是。
    let source = include_str!("../src/change_pipeline.rs");
    for (number, line) in source.lines().enumerate() {
        assert!(
            !(line.contains("format!") && line.contains(".json")),
            "line {}: the queue's module still builds a record file path out of an id ({line}): \
             a change is an event, and a path here is the shape that reads \"the file exists\" \
             as \"the change exists\"",
            number + 1
        );
    }
}
