mod common;

use cog_agent::GlobalAgentManager;
use cog_core::{
    AgentManager, DagMessage, MessageBackend, ShutdownSignal, StateBackend, Task, TaskStatus,
};
use cog_orchestrator::dag_executor::{DagExecutorConfig, DagExecutorRuntime};
use cog_storage::{MemoryAgentRegistry, MemoryStateBackend};
use cog_stream::MemoryMessageBackend;
use common::mocks::MockLLMProvider;
use futures::StreamExt;
use std::sync::Arc;

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
        lease_owner: None,
        lease_expires_at: None,
        action_planner_meta: None,
        goal_id: None,
        parent_task_id: None,
        sink_task_id: None,
        face_id: None,
        owner_unit: None,
        bound_by_handover_id: None,
        is_executable: true,
        acceptance: vec![],
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

/// 回队列的重试不是终态：这一次失败之后还要用同一个 agent 再跑一遍，所以名册
/// 里那一个必须留着。
///
/// 没有这一条时，「回收只挂在终态路径上」只是读代码读出来的结论——把回收挪到
/// 失败分支上，别的用例一条都不会红。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_failure_keeps_the_agent_it_will_run_with_again() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-retry".into(),
            consumer_group: "cg-retry".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );

    let messages: Arc<dyn MessageBackend> = Arc::new(backend.clone());
    let state: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let manager = Arc::new(GlobalAgentManager::new(
        Arc::new(MemoryAgentRegistry::new()),
        messages,
        state,
    ));
    runtime
        .orchestrator()
        .attach_agent_manager(manager.clone() as Arc<dyn AgentManager>);

    let name = cog_core::agent_id_for("task-retried", "executor");
    manager
        .create_agent(
            &name,
            "executor",
            Arc::new(MockLLMProvider::new()),
            Some("task-retried"),
        )
        .await
        .expect("create");
    assert_eq!(
        manager.list_workers().await.len(),
        1,
        "the task starts with one agent"
    );

    runtime
        .submit_goal("retry-goal", vec![make_test_task("task-retried")])
        .await
        .unwrap();
    runtime.publish_ready_tasks().await.unwrap();
    runtime
        .orchestrator()
        .start_task("task-retried")
        .await
        .unwrap();

    backend
        .publish(
            "orchestrator:results:ws-retry",
            &serde_json::to_vec(&DagMessage::TaskFailed {
                message_id: "msg-retried".into(),
                timestamp: chrono::Utc::now(),
                task_id: "task-retried".into(),
                error: "simulated transient failure".into(),
                error_cause: None,
                retry_after_secs: None,
                sender: "executor".into(),
                recipient: "orchestrator".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let shutdown = ShutdownSignal::new();
    let s = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        s.trigger();
    });
    runtime.run_consumer(shutdown).await.unwrap();

    let t = runtime
        .orchestrator()
        .get_task("task-retried")
        .await
        .unwrap();
    assert_eq!(t.retry_count, 1, "the failure must have been a retry");
    assert_eq!(t.status, TaskStatus::Pending);
    let live = manager.list_workers().await;
    assert_eq!(
        live.len(),
        1,
        "a retry is not a terminal state: retiring here costs the retry the \
         object it was going to run with, and the run after it builds another"
    );
    assert!(!live[0].agent.is_retired());
}

/// 终态任务都要收，不能只收汇点：一件没有声明 `sink_task_id` 的任务跑完了，
/// 它名下的对象也得放掉。
///
/// 汇点只是终态任务里的一小类；把回收排在 `parent_task_id` 的提前返回之后，
/// 其它终态任务就全部漏收，而漏收在读数面上长得和「本来就没多少任务结束」一样。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_terminal_task_that_is_not_a_sink_releases_its_agent() {
    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-plain".into(),
            consumer_group: "cg-plain".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );

    let messages: Arc<dyn MessageBackend> = Arc::new(backend.clone());
    let manager = Arc::new(GlobalAgentManager::new(
        Arc::new(MemoryAgentRegistry::new()),
        messages,
        Arc::new(MemoryStateBackend::new()) as Arc<dyn StateBackend>,
    ));
    runtime
        .orchestrator()
        .attach_agent_manager(manager.clone() as Arc<dyn AgentManager>);

    let name = cog_core::agent_id_for("task-plain", "executor");
    manager
        .create_agent(
            &name,
            "executor",
            Arc::new(MockLLMProvider::new()),
            Some("task-plain"),
        )
        .await
        .expect("create");

    runtime
        .submit_goal("plain-goal", vec![make_test_task("task-plain")])
        .await
        .unwrap();
    runtime.publish_ready_tasks().await.unwrap();
    runtime
        .orchestrator()
        .start_task("task-plain")
        .await
        .unwrap();

    backend
        .publish(
            "orchestrator:results:ws-plain",
            &serde_json::to_vec(&DagMessage::TaskComplete {
                message_id: "msg-plain".into(),
                timestamp: chrono::Utc::now(),
                task_id: "task-plain".into(),
                result: serde_json::json!({"status": "ok"}),
                sender: "executor".into(),
                recipient: "orchestrator".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let shutdown = ShutdownSignal::new();
    let s = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        s.trigger();
    });
    runtime.run_consumer(shutdown).await.unwrap();

    let t = runtime.orchestrator().get_task("task-plain").await.unwrap();
    assert_eq!(t.status, TaskStatus::Completed);
    assert!(
        t.sink_task_id.is_none() && t.parent_task_id.is_none(),
        "this task declares no sink: it is the case that a sink-only hook misses"
    );
    assert!(
        manager.list_workers().await.is_empty(),
        "a task in a terminal state still holds its agent: the release is on \
         the sink path rather than on every terminal path"
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

/// 池的窗口只剩一件任务的额度时，别的就绪任务不许上线：它们留在 Pending，
/// 等窗口能付得起再来。上线了也是一条注定被网关按同一组数字拒绝的请求，
/// 而拒绝不是吞吐。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spent_window_holds_ready_tasks_back_until_it_can_pay() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// 只报一个上游、只改额度的桩：测试要动的是「窗口还剩多少」。
    struct ScriptedWindow {
        allowance: Arc<AtomicU64>,
        spent: u64,
        reserve: u64,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmPoolStatusSource for ScriptedWindow {
        async fn status(&self) -> Option<cog_core::LlmPoolStatus> {
            Some(cog_core::LlmPoolStatus {
                pace: cog_core::LlmWindowPace {
                    measured_upstreams: 1,
                    allowance_tokens: self.allowance.load(Ordering::Relaxed),
                    spent_tokens: self.spent,
                    reserve_tokens: self.reserve,
                    reserve_population: 5,
                    window_secs: 604_800,
                    window_start_unix: 0,
                },
                pool_size: 1,
                ..Default::default()
            })
        }
    }

    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-admit".into(),
            consumer_group: "cg-admit".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );
    let allowance = Arc::new(AtomicU64::new(3_000_000));
    runtime.attach_pool_status_source(Arc::new(ScriptedWindow {
        allowance: allowance.clone(),
        spent: 2_500_000,
        reserve: 500_000,
    }));

    let tasks: Vec<Task> = (0..4)
        .map(|i| make_test_task(&format!("adm-{i}")))
        .collect();
    runtime.submit_goal("admission-goal", tasks).await.unwrap();

    // 还剩 500_000，正好一件。任务表按 id 顺序提交，准入截的是就绪表尾部。
    runtime.publish_ready_tasks().await.unwrap();
    let scheduled = count_status(&runtime, TaskStatus::Scheduled).await;
    let pending = count_status(&runtime, TaskStatus::Pending).await;
    assert_eq!(scheduled, 1, "one job of allowance admits exactly one task");
    assert_eq!(pending, 3, "the rest stay ready for a pass that can pay");

    // 第二拍：窗口没变，已欠一件，所以一件也不放行——不是每拍都重放一次额度。
    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        count_status(&runtime, TaskStatus::Scheduled).await,
        1,
        "a bound compared with what is owed does not re-admit the same allowance"
    );

    // 额度涨到能付四件（4_500_000 − 2_500_000 = 4 × 500_000）：已欠一件，
    // 这一拍补三件。
    allowance.store(4_500_000, Ordering::Relaxed);
    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        count_status(&runtime, TaskStatus::Scheduled).await,
        4,
        "once the window can pay, the held tasks go out"
    );
    assert_eq!(count_status(&runtime, TaskStatus::Pending).await, 0);
}

async fn count_status(runtime: &DagExecutorRuntime, status: TaskStatus) -> usize {
    runtime
        .orchestrator()
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| t.status == status)
        .count()
}
