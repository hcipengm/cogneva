use cog_core::{DagMessage, MessageBackend, ShutdownSignal, Task, TaskStatus};
use cog_orchestrator::dag_executor::{DagExecutorConfig, DagExecutorRuntime};
use cog_stream::MemoryMessageBackend;
use futures::StreamExt;

fn make_test_task(id: &str) -> Task {
    Task {
        id: id.into(),
        task_type: cog_core::TaskType::DagNode,
        status: TaskStatus::Pending,
        input: serde_json::json!({"goal": "test"}),
        result: None,
        error: None,
        blocked_by: vec![],
        blocks: vec![],
        priority: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        agent_id: None,
        workspace_id: None,
        retry_count: 0,
        max_retries: 3,
        started_at: None,
        timeout_seconds: 300,
        action_planner_meta: None,
        goal_id: None,
        parent_task_id: None,
        is_executable: true,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_loop_updates_state_from_results() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-1".into(),
            consumer_group: "cg-1".into(),
            max_retries: 3,
        },
        backend.clone(),
    );

    let task = make_test_task("task-1");
    runtime
        .submit_goal("test-goal", vec![task.clone()])
        .await
        .unwrap();

    // Publish ready tasks to the ready stream.
    runtime.publish_ready_tasks().await.unwrap();

    // Task must be started before it can be completed.
    runtime.orchestrator().start_task("task-1").await.unwrap();

    // Simulate TaskExecutorRouter: read from ready stream, execute, publish result.
    let result_stream = "orchestrator:results:ws-1";
    let msg = DagMessage::TaskComplete {
        message_id: "msg-1".into(),
        timestamp: chrono::Utc::now(),
        task_id: "task-1".into(),
        result: serde_json::json!({"status": "ok"}),
        sender: "executor".into(),
        recipient: "orchestrator".into(),
    };
    backend
        .publish(result_stream, &serde_json::to_vec(&msg).unwrap())
        .await
        .unwrap();

    let shutdown = ShutdownSignal::new();
    let s = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        s.trigger();
    });

    runtime.run_consumer(shutdown).await.unwrap();

    let t = runtime.orchestrator().get_task("task-1").await.unwrap();
    assert_eq!(t.status, TaskStatus::Completed);
    assert_eq!(t.result, Some(serde_json::json!({"status": "ok"})));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_loop_retries_on_task_failed_result() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-2".into(),
            consumer_group: "cg-2".into(),
            max_retries: 3,
        },
        backend.clone(),
    );

    let task = make_test_task("task-fail");
    runtime
        .submit_goal("fail-goal", vec![task.clone()])
        .await
        .unwrap();

    runtime.publish_ready_tasks().await.unwrap();
    runtime
        .orchestrator()
        .start_task("task-fail")
        .await
        .unwrap();

    let result_stream = "orchestrator:results:ws-2";
    let msg = DagMessage::TaskFailed {
        message_id: "msg-fail".into(),
        timestamp: chrono::Utc::now(),
        task_id: "task-fail".into(),
        error: "simulated failure".into(),
        sender: "executor".into(),
        recipient: "orchestrator".into(),
    };
    backend
        .publish(result_stream, &serde_json::to_vec(&msg).unwrap())
        .await
        .unwrap();

    let shutdown = ShutdownSignal::new();
    let s = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        s.trigger();
    });

    runtime.run_consumer(shutdown).await.unwrap();

    let t = runtime.orchestrator().get_task("task-fail").await.unwrap();
    // Task is retried (retry_count incremented) and then re-published by
    // publish_ready_tasks, so its final status is Scheduled.
    assert_eq!(t.status, TaskStatus::Scheduled);
    assert_eq!(t.retry_count, 1);
}

#[tokio::test]
async fn consumer_loop_respects_shutdown_signal() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-3".into(),
            consumer_group: "cg-3".into(),
            max_retries: 3,
        },
        backend.clone(),
    );

    let shutdown = ShutdownSignal::new();
    shutdown.trigger(); // Trigger immediately.

    let start = std::time::Instant::now();
    runtime.run_consumer(shutdown).await.unwrap();
    let elapsed = start.elapsed();

    // Should return almost immediately since shutdown is already triggered.
    assert!(elapsed < tokio::time::Duration::from_millis(500));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_publishes_ready_tasks() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-4".into(),
            consumer_group: "cg-4".into(),
            max_retries: 3,
        },
        backend.clone(),
    );

    let task = make_test_task("task-4");
    runtime
        .submit_goal("event-goal", vec![task.clone()])
        .await
        .unwrap();

    runtime.publish_ready_tasks().await.unwrap();

    // Subscribe to the ready stream to verify the task was published.
    let ready_stream = "orchestrator:ready:ws-4";
    let mut ready = backend.subscribe(ready_stream, "test-cg").await.unwrap();
    let mut found = false;
    while let Ok(Some(Ok((_, bytes)))) =
        tokio::time::timeout(tokio::time::Duration::from_millis(100), ready.next()).await
    {
        let t: Task = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if t.id == "task-4" {
            found = true;
            break;
        }
    }
    assert!(found, "Ready task should be published to ready stream");

    // Verify orchestrator state: task should be Scheduled.
    let t = runtime.orchestrator().get_task("task-4").await.unwrap();
    assert_eq!(t.status, TaskStatus::Scheduled);
}
