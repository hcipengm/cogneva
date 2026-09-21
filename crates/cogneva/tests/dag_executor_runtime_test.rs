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
        error_cause: None,
        blocked_by: vec![],
        blocks: vec![],
        priority: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        agent_id: None,
        workspace_id: None,
        retry_count: 0,
        max_retries: 3,
        retry_not_before: None,
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
            ..DagExecutorConfig::default()
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
            ..DagExecutorConfig::default()
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
        error_cause: None,
        retry_after_secs: None,
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
    // A transient failure stays retryable, so the retry budget is spent on it —
    // but the retry is held behind the task type's backoff rather than pushed
    // straight back onto the ready stream: the task is Pending again and carries
    // the deadline the publisher filters on. Going back to Scheduled here would
    // mean the retry ignored its own backoff.
    assert_eq!(t.status, TaskStatus::Pending);
    assert_eq!(t.retry_count, 1);
    assert!(
        t.retry_not_before
            .is_some_and(|due| due > chrono::Utc::now()),
        "a retried task must carry a future backoff deadline, got {:?}",
        t.retry_not_before
    );
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
            ..DagExecutorConfig::default()
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
            ..DagExecutorConfig::default()
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

/// 变更生成的产物只有产出它的进程读得到（写进本进程的 change_dir，扫它的
/// 循环也在同一个进程里），所以它不能走所有 worker 竞争的共享队列：谁先读到
/// 谁执行，跑到别的进程里就产出一份没有读者存在的东西，且账单照付。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn change_generation_goes_to_the_process_local_queue() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-local".into(),
            consumer_group: "cg-local".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );

    let mut ordinary = make_test_task("task-ordinary");
    ordinary.task_type = cog_core::TaskType::Custom("unit_of_work".into());

    let mut generation = make_test_task("task-generate");
    generation.task_type = cog_core::TaskType::Custom("platform_issue_fix".into());
    generation.input = serde_json::json!({"goal": "fix it", "evolution_mode": "generate_change"});

    runtime
        .submit_goal("mixed-goal", vec![ordinary, generation])
        .await
        .unwrap();
    runtime.publish_ready_tasks().await.unwrap();

    async fn drain(stream: &str, group: &str, backend: &MemoryMessageBackend) -> Vec<String> {
        let mut sub = backend.subscribe(stream, group).await.unwrap();
        let mut ids = Vec::new();
        while let Ok(Some(Ok((_, bytes)))) =
            tokio::time::timeout(tokio::time::Duration::from_millis(200), sub.next()).await
        {
            if let Ok(t) = serde_json::from_slice::<Task>(&bytes) {
                ids.push(t.id);
            }
        }
        ids
    }

    let shared = drain("orchestrator:ready:ws-local", "grp-shared", &backend).await;
    let local = drain("orchestrator:ready:ws-local:local", "grp-local", &backend).await;

    assert_eq!(
        shared,
        vec!["task-ordinary".to_string()],
        "a task whose result travels over the bus stays on the shared queue"
    );
    assert_eq!(
        local,
        vec!["task-generate".to_string()],
        "change generation belongs on the queue only the executor role reads"
    );
}
