//! The Generator's `reference_implementations` and the Evaluator's failure
//! patterns were reads against namespaces nothing wrote to. Both come back
//! empty when the namespace is empty, so the defect was invisible from the
//! read side; these tests drive a task through archival and then through the
//! same retrieval calls the actors make, so a write that no query can reach
//! fails here rather than showing up as "no relevant experience".

use std::sync::Arc;

use cog_core::{KnowledgeBackend, MemoryBackend, Task, TaskResult, TaskResultMetadata, TaskType};
use cog_wiki::UnifiedKnowledgeBackend;

fn backend() -> (Arc<dyn KnowledgeBackend>, Arc<dyn MemoryBackend>) {
    let memory: Arc<dyn MemoryBackend> = Arc::new(cog_memory::backend::MemoryMemoryBackend::new());
    let unified = UnifiedKnowledgeBackend::new().with_memory(memory.clone());
    (Arc::new(unified), memory)
}

/// The task type spelling the caller and the archival both use.
fn task_type_of(task: &Task) -> String {
    format!("{:?}", task.task_type)
}

fn result_of(success: bool, score: f64, feedback: Option<&str>, output: &str) -> TaskResult {
    let mut metadata = TaskResultMetadata::new("test").with_score(score);
    metadata.feedback = feedback.map(str::to_string);
    TaskResult {
        success,
        output: serde_json::json!({ "content": output }),
        metadata,
    }
}

#[tokio::test]
async fn a_successful_task_becomes_a_reference_implementation_for_its_task_type() {
    let (knowledge, _memory) = backend();
    let task = Task::new(
        "task-1",
        TaskType::Generator,
        serde_json::json!({ "goal": "summarise the failing builds" }),
    );

    knowledge
        .archive_execution(&task, &result_of(true, 0.8, None, "a summary"))
        .await
        .unwrap();

    // Exactly the call the generator makes: the task type plus the serialized
    // input. Passing the two concatenated as one query is what made the
    // retrieval unreachable even once rows existed, so the round trip is only
    // proven by going through the same shape.
    let input_summary = serde_json::to_string(&task.input).unwrap();
    let examples = knowledge
        .retrieve_similar_implementations(&task_type_of(&task), &input_summary, 3)
        .await
        .unwrap();

    assert_eq!(examples.len(), 1, "一次成功应留下一条参考实现");
    assert_eq!(examples[0].task_type, task_type_of(&task));
    assert_eq!(examples[0].observed_count, 1);
    assert!(examples[0].output_summary.contains("a summary"));
}

#[tokio::test]
async fn a_second_success_of_the_same_type_carries_the_count_forward() {
    let (knowledge, _memory) = backend();
    let task = Task::new("task-2", TaskType::Generator, serde_json::json!({}));

    for score in [0.4_f64, 0.9_f64] {
        knowledge
            .archive_execution(&task, &result_of(true, score, None, "out"))
            .await
            .unwrap();
    }

    let examples = knowledge
        .retrieve_similar_implementations(&task_type_of(&task), "", 3)
        .await
        .unwrap();

    assert_eq!(examples.len(), 1, "同一任务类型只应有一行");
    assert_eq!(examples[0].observed_count, 2, "第二次成功要把计数带上来");
    assert_eq!(examples[0].score, 0.9, "留下的是最近一次，不是第一次");
}

#[tokio::test]
async fn a_failed_task_becomes_a_failure_pattern_for_its_task_type() {
    let (knowledge, _memory) = backend();
    let task = Task::new("task-3", TaskType::Evaluator, serde_json::json!({}));

    knowledge
        .archive_execution(
            &task,
            &result_of(false, 0.0, Some("the diff touched a protected file"), "x"),
        )
        .await
        .unwrap();

    let patterns = knowledge
        .retrieve_failure_patterns(&task_type_of(&task), 3)
        .await
        .unwrap();

    assert_eq!(patterns.len(), 1, "一次失败应留下一条失败模式");
    assert_eq!(patterns[0].occurrence_count, 1);
    assert!(
        patterns[0].root_cause.contains("protected file"),
        "失败的因由来自执行元数据，不能丢: {:?}",
        patterns[0]
    );
}

#[tokio::test]
async fn a_task_type_that_never_ran_retrieves_nothing() {
    let (knowledge, _memory) = backend();
    let ran = Task::new("task-4", TaskType::Generator, serde_json::json!({}));
    let never_ran = Task::new("task-5", TaskType::Reviewer, serde_json::json!({}));

    knowledge
        .archive_execution(&ran, &result_of(true, 0.5, None, "out"))
        .await
        .unwrap();

    // The control: a namespace with rows in it still answers nothing for a
    // type that never ran, so the retrieval above is not returning whatever
    // happens to be stored.
    assert!(knowledge
        .retrieve_similar_implementations(&task_type_of(&never_ran), "", 3)
        .await
        .unwrap()
        .is_empty());
    assert!(knowledge
        .retrieve_failure_patterns(&task_type_of(&never_ran), 3)
        .await
        .unwrap()
        .is_empty());

    // And a success is not a failure: the two namespaces stay apart.
    assert!(knowledge
        .retrieve_failure_patterns(&task_type_of(&ran), 3)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_failure_and_a_success_of_one_type_do_not_overwrite_each_other() {
    let (knowledge, _memory) = backend();
    let task = Task::new("task-6", TaskType::DagNode, serde_json::json!({}));

    knowledge
        .archive_execution(&task, &result_of(true, 0.7, None, "ok"))
        .await
        .unwrap();
    knowledge
        .archive_execution(&task, &result_of(false, 0.0, Some("timeout"), "no"))
        .await
        .unwrap();

    let examples = knowledge
        .retrieve_similar_implementations(&task_type_of(&task), "", 3)
        .await
        .unwrap();
    let patterns = knowledge
        .retrieve_failure_patterns(&task_type_of(&task), 3)
        .await
        .unwrap();

    assert_eq!(examples.len(), 1, "成功的那次还在");
    assert_eq!(patterns.len(), 1, "失败的那次也在");
    assert_eq!(patterns[0].root_cause, "timeout");
}
