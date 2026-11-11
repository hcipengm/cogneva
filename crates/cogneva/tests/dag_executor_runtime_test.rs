use crate::common;

use cog_agent::GlobalAgentManager;
use cog_core::{
    AgentManager, DagMessage, MessageBackend, MetricsBackend, ShutdownSignal, StateBackend, Task,
    TaskStatus,
};
use cog_orchestrator::dag_executor::{DagExecutorConfig, DagExecutorRuntime};
use cog_storage::{MemoryAgentRegistry, MemoryStateBackend};
use cog_stream::MemoryMessageBackend;
use common::mocks::MockLLMProvider;
use futures::StreamExt;
use std::sync::Arc;

fn make_test_task(id: &str) -> Task {
    Task {
        submitted_by: None,
        id: id.into(),
        task_type: cog_core::TaskType::DagNode,
        status: TaskStatus::Pending,
        input: serde_json::json!({"goal": "test"}),
        result: None,
        error: None,
        error_cause: None,
        failure_payer: None,
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
        declared_duration_seconds: 300,
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
        // 可执行的活必须说得出“怎样算做完”，否则调度面会把它按在 Pending 上：
        // 这一条判据是准入，不是注释。夹具跟着它走，测的仍是消费者/发布者那半边。
        acceptance: vec!["the task reaches a terminal state the consumer loop records".into()],
        venue: None,
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
        failure_payer: None,
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
                failure_payer: None,
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

/// 池说了「到某个时刻我再值得被问一次」之后，发布者在那之前不该再扫一遍就绪
/// 队列、再写一遍拒绝行。
///
/// 停摆的那几个小时里每一拍都写一次准入读数，读起来和「一直在试」同形：能分开
/// 两者的只有「上一拍写的是什么」与「那一拍之后有没有新的一拍」。这里用准入面
/// 自己的写次数判：退避期间的拍一次都不写，池子回来说可用时立刻写回并放行。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_waits_for_the_instant_the_pool_named() {
    use std::sync::atomic::{AtomicBool, Ordering};

    /// 池自报不可用，并把下次探测时刻写在快照里；开关让用例把它翻回可用。
    struct ScriptedPool {
        unavailable: Arc<AtomicBool>,
        next_attempt_unix: Arc<std::sync::atomic::AtomicI64>,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmPoolStatusSource for ScriptedPool {
        async fn status(&self) -> Option<cog_core::LlmPoolStatus> {
            Some(cog_core::LlmPoolStatus {
                unavailable: self.unavailable.load(Ordering::Relaxed),
                next_attempt_unix: self.next_attempt_unix.load(Ordering::Relaxed),
                pool_size: 1,
                ..Default::default()
            })
        }
    }

    async fn admission_writes(metrics: &cog_test_support::MemoryMetricsBackend) -> usize {
        let now = chrono::Utc::now();
        metrics
            .query_gauge_range(
                cog_core::metric_names::READY_ADMISSION_READINGS.as_str(),
                now - chrono::Duration::hours(1),
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("the in-memory board answers")
            .len()
    }

    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-backoff".into(),
            consumer_group: "cg-backoff".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );
    let metrics = Arc::new(cog_test_support::MemoryMetricsBackend::new());
    runtime.orchestrator().attach_metrics(metrics.clone());

    // 第一拍就是「池子说了不可用，但那个时刻已经过去了」：这一拍该照常判、照常写，
    // 判词是按下不动。退避只认**未来**的时刻，一个过去的时刻不是闭窗。
    let unavailable = Arc::new(AtomicBool::new(true));
    let next_attempt_unix = Arc::new(std::sync::atomic::AtomicI64::new(0));
    runtime.attach_pool_status_source(Arc::new(ScriptedPool {
        unavailable: unavailable.clone(),
        next_attempt_unix: next_attempt_unix.clone(),
    }));

    runtime
        .submit_goal("backoff-goal", vec![make_test_task("backoff-task")])
        .await
        .unwrap();

    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        count_status(&runtime, TaskStatus::Pending).await,
        1,
        "an unavailable pool admits nothing"
    );
    let after_deciding = admission_writes(&metrics).await;
    assert!(
        after_deciding > 0,
        "the pass that decided the hold is written down"
    );

    // 池子把复问时刻推到一小时后：这之间的每一拍都不该再扫队列、再写一遍读数。
    next_attempt_unix.store(chrono::Utc::now().timestamp() + 3_600, Ordering::Relaxed);
    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        admission_writes(&metrics).await,
        after_deciding,
        "a pass that skipped itself must not write an admission reading: it decided nothing"
    );
    assert_eq!(count_status(&runtime, TaskStatus::Pending).await, 1);

    // 池子提前回来说可用：退避跟着池子的判定走，不由那个时刻独占。
    unavailable.store(false, Ordering::Relaxed);
    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        count_status(&runtime, TaskStatus::Scheduled).await,
        1,
        "the first pass after the pool is back admits the held task"
    );
    assert!(
        admission_writes(&metrics).await > after_deciding,
        "the pass that admitted work is written down again"
    );
}

/// 停摆期间，账上要同时有「我们什么时候再问」和「上游说什么时候回来」两个时刻。
///
/// 只有前者的时候，一次月度配额耗尽读起来和一次五秒钟的轮询失败同形：日志里
/// 每一拍都写着五秒后再看，而真正该等的那个日子在别处。2026-11-03 就是这样被
/// 读错的——每一拍都写 `next_look_unix` 五秒后，上游其实已经说了
/// 2026-11-04 16:00。这里判的是那份证据有没有进到准入读数里，以及没有上游说过
/// 时刻时它写成 0 而不是沿用上一拍的数。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hold_publishes_the_instant_the_upstream_named_apart_from_our_own_retry() {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

    /// 池自报不可用；`next_attempt_unix` 一直留在过去，所以每一拍都真的判一遍
    /// （退避只认未来的时刻），而 `evidenced_recovery_unix` 是上游自己说的那个
    /// 日子，只由开关翻动。
    struct ScriptedPool {
        unavailable: Arc<AtomicBool>,
        recovery_unix: Arc<AtomicI64>,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmPoolStatusSource for ScriptedPool {
        async fn status(&self) -> Option<cog_core::LlmPoolStatus> {
            Some(cog_core::LlmPoolStatus {
                unavailable: self.unavailable.load(Ordering::Relaxed),
                next_attempt_unix: 0,
                evidenced_recovery_unix: self.recovery_unix.load(Ordering::Relaxed),
                pool_size: 1,
                ..Default::default()
            })
        }
    }

    /// 一个 `kind` 在准入读数里的最新值，`None` 表示这一拍没写它。
    async fn cell(metrics: &cog_test_support::MemoryMetricsBackend, kind: &str) -> Option<f64> {
        let now = chrono::Utc::now();
        metrics
            .query_gauge_range(
                cog_core::metric_names::READY_ADMISSION_READINGS.as_str(),
                now - chrono::Duration::hours(1),
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("the in-memory board answers")
            .into_iter()
            .filter(|sample| {
                sample
                    .labels
                    .get(cog_orchestrator::LABEL_KIND)
                    .is_some_and(|value| value == kind)
            })
            .max_by_key(|sample| sample.timestamp)
            .map(|sample| sample.value)
    }

    let backend = MemoryMessageBackend::new();
    let runtime = DagExecutorRuntime::new_with_backend(
        DagExecutorConfig {
            redis_url: "redis://localhost".into(),
            workspace_id: "ws-recovery".into(),
            consumer_group: "cg-recovery".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );
    let metrics = Arc::new(cog_test_support::MemoryMetricsBackend::new());
    runtime.orchestrator().attach_metrics(metrics.clone());

    let unavailable = Arc::new(AtomicBool::new(true));
    let recovery_unix = Arc::new(AtomicI64::new(1_793_807_999));
    runtime.attach_pool_status_source(Arc::new(ScriptedPool {
        unavailable: unavailable.clone(),
        recovery_unix: recovery_unix.clone(),
    }));
    runtime
        .submit_goal("recovery-goal", vec![make_test_task("recovery-task")])
        .await
        .unwrap();

    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        cell(&metrics, "recovery_at_unix").await,
        Some(1_793_807_999.0),
        "上游说了它的额度什么时候回来，这一格却没写进去：读者只剩我们自己那个五秒的轮询间隔"
    );
    assert_eq!(
        cell(&metrics, "retry_at_unix").await,
        Some(0.0),
        "两个时刻混成了一个：这一次没有待探测的时刻，而它不该被上游那个日子填上"
    );

    // 池子回来、上游也不再说明时刻：这一格要写回 0。仪表不写就留着上一拍的值，
    // 而留下的那个值在账面上与「上游刚说过」同形。
    unavailable.store(false, Ordering::Relaxed);
    recovery_unix.store(0, Ordering::Relaxed);
    runtime.publish_ready_tasks().await.unwrap();
    assert_eq!(
        cell(&metrics, "recovery_at_unix").await,
        Some(0.0),
        "没有上游报时刻的这一拍要写 0，否则上一拍那个日子会一直挂在账上"
    );
}

/// 只回答「最近落地里门类占多少」的账。
///
/// 其余的门一个都不实现：发布者只问这一个问题，而一个把别的答案也编出来的夹具
/// 会让测试在它从未走过的路上给出一个看起来像读数的结果。
struct ScriptedLandedShare {
    gate: u64,
    total: u64,
}

#[async_trait::async_trait]
impl cog_core::ChangeLedgerStore for ScriptedLandedShare {
    async fn landed_tally_since(
        &self,
        _since: chrono::DateTime<chrono::Utc>,
        _prefix: &str,
    ) -> cog_core::SFResult<cog_core::LandedTally> {
        Ok(cog_core::LandedTally {
            matching: self.gate,
            total: self.total,
        })
    }

    async fn record_readings(
        &self,
        _readings: &[cog_core::ChangeReading],
        _first_seen_rev: Option<&str>,
    ) -> cog_core::SFResult<usize> {
        unimplemented!("准入只问落地占比")
    }
    async fn declare(&self, _readings: &[cog_core::ChangeReading]) -> cog_core::SFResult<usize> {
        unimplemented!("准入只问落地占比")
    }
    async fn fold_declarations(
        &self,
        _limit: usize,
        _first_seen_rev: Option<&str>,
    ) -> cog_core::SFResult<usize> {
        unimplemented!("准入只问落地占比")
    }
    async fn fold_retirements(&self, _limit: usize) -> cog_core::SFResult<usize> {
        unimplemented!("准入只问落地占比")
    }
    async fn pending_declarations(&self) -> cog_core::SFResult<u64> {
        unimplemented!("准入只问落地占比")
    }
    async fn pending_retirements(&self) -> cog_core::SFResult<u64> {
        unimplemented!("准入只问落地占比")
    }
    async fn claimed_revisions(
        &self,
        _revisions: &[String],
    ) -> cog_core::SFResult<std::collections::HashSet<String>> {
        unimplemented!("准入只问落地占比")
    }
    async fn apply_measurement(
        &self,
        _measurement: &cog_core::ChangeMeasurement,
    ) -> cog_core::SFResult<()> {
        unimplemented!("准入只问落地占比")
    }
    async fn outstanding(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeLedgerRow>> {
        unimplemented!("准入只问落地占比")
    }
    async fn events(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeLedgerEvent>> {
        unimplemented!("准入只问落地占比")
    }
    async fn owed_by_band(&self) -> cog_core::SFResult<Vec<(cog_core::ChangePriority, usize)>> {
        unimplemented!("准入只问落地占比")
    }
    async fn landed_due(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeLedgerRow>> {
        unimplemented!("准入只问落地占比")
    }
    async fn landed_at_revision(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<(cog_core::ChangeIdentity, Option<String>)>> {
        unimplemented!("准入只问落地占比")
    }
    async fn landed_touching(
        &self,
        _paths: &[String],
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeLedgerRow>> {
        unimplemented!("准入只问落地占比")
    }
    async fn landed_awaiting_parent_observation(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<(cog_core::ChangeIdentity, String)>> {
        unimplemented!("准入只问落地占比")
    }
    async fn record_parent_observation(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _observation: &str,
    ) -> cog_core::SFResult<bool> {
        unimplemented!("准入只问落地占比")
    }
    async fn record_reopen(&self, _reopen: &cog_core::ChangeReopen) -> cog_core::SFResult<()> {
        unimplemented!("准入只问落地占比")
    }
    async fn retired_surface_moved(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeLedgerRow>> {
        unimplemented!("准入只问落地占比")
    }
    async fn retired_answered_elsewhere(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeLedgerRow>> {
        unimplemented!("准入只问落地占比")
    }
    async fn request_remeasurement(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _source: &str,
    ) -> cog_core::SFResult<()> {
        unimplemented!("准入只问落地占比")
    }
    async fn pending_remeasurements(
        &self,
        _limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::ChangeIdentity>> {
        unimplemented!("准入只问落地占比")
    }
    async fn mark_remeasured(
        &self,
        _identity: &cog_core::ChangeIdentity,
    ) -> cog_core::SFResult<()> {
        unimplemented!("准入只问落地占比")
    }
    async fn remeasurement_backlog(&self) -> cog_core::SFResult<cog_core::RemeasureBacklog> {
        unimplemented!("准入只问落地占比")
    }
    async fn record_delivery(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _rev: &str,
    ) -> cog_core::SFResult<()> {
        unimplemented!("准入只问落地占比")
    }
    async fn stamp_pool_block(&self, _unblock_condition: &str) -> cog_core::SFResult<u64> {
        unimplemented!("准入只问落地占比")
    }
    async fn clear_pool_block(&self) -> cog_core::SFResult<u64> {
        unimplemented!("准入只问落地占比")
    }
    async fn pool_wait(&self) -> cog_core::SFResult<cog_core::PoolWait> {
        unimplemented!("准入只问落地占比")
    }
    async fn get(
        &self,
        _identity: &cog_core::ChangeIdentity,
    ) -> cog_core::SFResult<Option<cog_core::ChangeLedgerRow>> {
        unimplemented!("准入只问落地占比")
    }
    async fn census(&self) -> cog_core::SFResult<Vec<(cog_core::ChangeState, u64)>> {
        unimplemented!("准入只问落地占比")
    }
    async fn funnel(&self, _days: u32) -> cog_core::SFResult<Vec<cog_core::ChangeFunnelDay>> {
        unimplemented!("准入只问落地占比")
    }
    async fn record_dispatch_state(
        &self,
        _state: &cog_core::DispatchState,
    ) -> cog_core::SFResult<()> {
        unimplemented!("准入只问落地占比")
    }
}

/// 门类与产品活抢同一批座位时，产品活先走，门类按它实测的落地占比拿座位。
///
/// 现场取自 2026-11-06：门类占了就绪队列的 86%（1,636/1,978 条），而落到
/// `landed` 的账行里门类只占 5%（5/100）。没有这一刀时，批里的前 10 条原样
/// 进队列，门类那 3 条排在产品活前面；有了它，产品活先占满这 10 个座位，
/// 门类只拿到 ceil(10 × 5%) = 1 个。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principle_gate_queues_behind_product_work() {
    struct TenJobWindow;
    #[async_trait::async_trait]
    impl cog_core::LlmPoolStatusSource for TenJobWindow {
        async fn status(&self) -> Option<cog_core::LlmPoolStatus> {
            Some(cog_core::LlmPoolStatus {
                pace: cog_core::LlmWindowPace {
                    measured_upstreams: 1,
                    allowance_tokens: 5_000_000,
                    spent_tokens: 0,
                    reserve_tokens: 500_000,
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
            workspace_id: "ws-gate".into(),
            consumer_group: "cg-gate".into(),
            max_retries: 3,
            ..DagExecutorConfig::default()
        },
        backend.clone(),
    );
    runtime.attach_pool_status_source(Arc::new(TenJobWindow));
    runtime.attach_change_ledger(Arc::new(ScriptedLandedShare {
        gate: 5,
        total: 100,
    }));

    // 门类排在最前：就绪表接着提交顺序走，所以不重排的话，截出来的前缀
    // 里全是门类，而这份测试要看的正是那之后谁先走。
    let mut tasks = Vec::new();
    for i in 0..3 {
        let mut task = make_test_task(&format!("gate-{i}"));
        task.input = serde_json::json!({
            "goal": "Give the principle X the gate it is owed",
            "change_sources": ["principle_ledger"],
        });
        tasks.push(task);
    }
    for i in 0..20 {
        tasks.push(make_test_task(&format!("product-{i}")));
    }
    runtime.submit_goal("gate-goal", tasks).await.unwrap();

    runtime.publish_ready_tasks().await.unwrap();

    let scheduled: Vec<String> = runtime
        .orchestrator()
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|task| task.status == TaskStatus::Scheduled)
        .map(|task| task.id)
        .collect();
    assert_eq!(scheduled.len(), 10, "窗口只付得起十件");
    let gates = scheduled
        .iter()
        .filter(|id| id.starts_with("gate-"))
        .count();
    assert_eq!(
        gates, 1,
        "门类按实测的 5% 拿座位：10 × 5% = 1；没有这一刀时是 3 条（它们排在最前）"
    );
    assert_eq!(
        scheduled
            .iter()
            .filter(|id| id.starts_with("product-"))
            .count(),
        9,
        "其余九个座位留给产品活：门类不是被切掉，而是只坐它那一份"
    );
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
