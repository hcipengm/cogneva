//! The Generator's `reference_implementations`, the Evaluator's failure
//! patterns and the Planner's similar decompositions were reads against
//! namespaces nothing wrote to. All of them come back empty when the namespace
//! is empty, so the defect was invisible from the read side; these tests drive a
//! task through archival and then through the same retrieval calls the actors
//! make, so a write that no query can reach fails here rather than showing up as
//! "no relevant experience".

use std::sync::Arc;

use cog_core::{
    GoalClass, KnowledgeBackend, MemoryBackend, Task, TaskResult, TaskResultMetadata, TaskType,
};
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

// ---------------------------------------------------------------------------
// Decompositions
// ---------------------------------------------------------------------------

/// The class a goal was submitted under, and the task the decomposition loop
/// actually plans on: a synthetic task whose own type names the loop rather
/// than the work, which is why the class has to travel with the goal.
const CARRIED_CLASS: &str = "Custom(\"platform_ci_fix\")";

fn planning_task(id: &str, goal: &str) -> Task {
    Task::new(
        id,
        TaskType::Custom("ralph_plan_goal".into()),
        serde_json::json!({
            "goal": goal,
            (GoalClass::INPUT_FIELD): CARRIED_CLASS,
        }),
    )
}

#[tokio::test]
async fn a_decomposition_becomes_a_pattern_for_its_goal_class() {
    let (knowledge, _memory) = backend();
    let task = planning_task("task-7", "fix the red build");

    knowledge
        .archive_decomposition(&task, &["generate".to_string(), "evaluate".to_string()])
        .await
        .unwrap();

    // Exactly the call the planner makes, on the class the goal carried.
    let patterns = knowledge
        .retrieve_similar_decompositions(CARRIED_CLASS, "fix the red build", 3)
        .await
        .unwrap();

    assert_eq!(patterns.len(), 1, "一次分解应留下一条模式");
    assert_eq!(patterns[0].goal_summary, "fix the red build");
    assert_eq!(patterns[0].task_types, vec!["evaluate", "generate"]);
    assert_eq!(patterns[0].used_count, 1);
    assert_eq!(patterns[0].avg_sub_task_count, 2.0);
}

/// The retrieval is keyed on the class, not on the goal text: a goal of the same
/// class that shares no wording still retrieves. This is the control that says
/// the round trip above goes through the query dimension the write side keys on
/// — matching on the goal string would make the namespace look empty again the
/// first time a goal is phrased differently.
#[tokio::test]
async fn another_goal_of_the_same_class_still_retrieves() {
    let (knowledge, _memory) = backend();
    knowledge
        .archive_decomposition(
            &planning_task("task-8", "fix the red build"),
            &["generate".into()],
        )
        .await
        .unwrap();

    let patterns = knowledge
        .retrieve_similar_decompositions(CARRIED_CLASS, "make the pipeline green again", 3)
        .await
        .unwrap();

    assert_eq!(patterns.len(), 1, "同一类的另一个目标也要能取到");
}

/// And the converse: a query built from the goal text reaches nothing, because
/// nothing is keyed on it. The write and the read have to agree on the dimension
/// or the namespace is empty from both ends — which is the shape this defect had.
#[tokio::test]
async fn a_query_built_from_the_goal_text_retrieves_nothing() {
    let (knowledge, _memory) = backend();
    knowledge
        .archive_decomposition(
            &planning_task("task-9", "fix the red build"),
            &["generate".into()],
        )
        .await
        .unwrap();

    assert!(
        knowledge
            .retrieve_similar_decompositions("fix the red build", "fix the red build", 3)
            .await
            .unwrap()
            .is_empty(),
        "目标文本不是写入侧的键，按它查应当查不到"
    );
}

/// A goal that carries no class is recorded under the type of the task that
/// holds it. That is a coarser class, not an absent one, so the row is still
/// reachable — and the metric `collab_goal_class_source_total{source="host_type"}`
/// is what makes the difference visible, since the rows alone cannot tell a
/// carried class from a fallback.
#[tokio::test]
async fn a_goal_without_a_carried_class_is_keyed_on_its_host_type() {
    let (knowledge, _memory) = backend();
    let task = Task::new(
        "task-10",
        TaskType::Generator,
        serde_json::json!({ "goal": "summarise the builds" }),
    );

    knowledge
        .archive_decomposition(&task, &["generate".into()])
        .await
        .unwrap();

    let host_class = TaskType::Generator.retrieval_class();
    assert_eq!(GoalClass::of(&task).value, host_class);
    assert_eq!(
        knowledge
            .retrieve_similar_decompositions(&host_class, "summarise the builds", 3)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        knowledge
            .retrieve_similar_decompositions(CARRIED_CLASS, "summarise the builds", 3)
            .await
            .unwrap()
            .is_empty(),
        "没有携带类的目标不该落进别的类"
    );
}

/// A run that delivered no sub-tasks records nothing: it is the absence of a
/// decomposition, not a decomposition of width zero. Recording it would open a
/// row that counts a run which decomposed nothing.
#[tokio::test]
async fn a_run_that_decomposed_nothing_records_nothing() {
    let (knowledge, _memory) = backend();
    let task = planning_task("task-11", "fix the red build");

    knowledge.archive_decomposition(&task, &[]).await.unwrap();
    knowledge
        .archive_decomposition(&task, &[String::new()])
        .await
        .unwrap();

    assert!(knowledge
        .retrieve_similar_decompositions(CARRIED_CLASS, "fix the red build", 3)
        .await
        .unwrap()
        .is_empty());
}

/// The second decomposition of a class folds into the row the first one opened:
/// the count moves, the widths average over recorded runs, and the types of both
/// runs stay because a class of goals is split in more than one way.
#[tokio::test]
async fn a_second_decomposition_of_the_same_class_folds_into_the_row() {
    let (knowledge, _memory) = backend();
    let task = planning_task("task-12", "fix the red build");

    knowledge
        .archive_decomposition(&task, &["generate".into()])
        .await
        .unwrap();
    knowledge
        .archive_decomposition(&task, &["generate".into(), "review".into()])
        .await
        .unwrap();

    let patterns = knowledge
        .retrieve_similar_decompositions(CARRIED_CLASS, "fix the red build", 3)
        .await
        .unwrap();

    assert_eq!(patterns.len(), 1, "同一类只有一行");
    assert_eq!(patterns[0].used_count, 2, "第二次分解要把计数带上来");
    assert_eq!(patterns[0].task_types, vec!["generate", "review"]);
    assert_eq!(
        patterns[0].avg_sub_task_count, 1.5,
        "宽度按已记录的次数取均值"
    );
}
