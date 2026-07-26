//! Redis Streams cluster test — 3 DagExecutorRuntime instances share a
//! Redis-backed MessageBackend and verify task hand-off consistency.

use chrono::Utc;
use cog_core::{DagMessage, MessageBackend, SFResult, Task, TaskStatus, TaskType};
use cog_orchestrator::{DagExecutorConfig, DagExecutorRuntime};
use cog_stream::RedisMessageBackend;
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const REDIS_URL: &str = "redis://127.0.0.1:6379";

/// Skip the whole test file if Redis is not reachable.
async fn redis_available() -> bool {
    redis::Client::open(REDIS_URL)
        .ok()
        .and_then(|c| {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(c.get_multiplexed_async_connection()).ok()
            })
            .join()
            .ok()
            .flatten()
        })
        .is_some()
}

fn make_task(id: &str, task_type: TaskType, blocked_by: Vec<String>, workspace: &str) -> Task {
    let now = Utc::now();
    Task {
        id: id.into(),
        task_type,
        status: TaskStatus::Pending,
        input: serde_json::json!({}),
        result: None,
        error: None,
        blocked_by,
        blocks: vec![],
        priority: 1,
        created_at: now,
        updated_at: now,
        agent_id: None,
        workspace_id: Some(workspace.into()),
        retry_count: 0,
        max_retries: 2,
        started_at: None,
        timeout_seconds: 30,
        action_planner_meta: None,
        goal_id: None,
        parent_task_id: None,
        is_executable: true,
    }
}

/// Build a runtime connected to the shared Redis backend.
async fn make_runtime(id: &str, workspace: &str) -> SFResult<DagExecutorRuntime> {
    let backend = RedisMessageBackend::new(REDIS_URL).await?;
    let config = DagExecutorConfig {
        redis_url: REDIS_URL.into(),
        workspace_id: workspace.into(),
        consumer_group: format!("cg-{}", id),
        max_retries: 2,
    };
    Ok(DagExecutorRuntime::new_with_backend(config, backend))
}

/// Clean up Redis streams for a given workspace.
async fn cleanup_streams(workspace: &str) {
    let client = match redis::Client::open(REDIS_URL) {
        Ok(c) => c,
        _ => return,
    };
    if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
        let streams: Vec<String> = vec![
            format!("orchestrator:ready:{}", workspace),
            format!("orchestrator:results:{}", workspace),
        ];
        for s in &streams {
            let _: redis::RedisResult<()> = redis::cmd("DEL").arg(s).query_async(&mut conn).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Test 1: One producer submits tasks, three consumers share the ready queue.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_three_instances_share_ready_queue() {
    if !redis_available().await {
        eprintln!("SKIP: Redis not available");
        return;
    }
    let ws = "cluster-test-ws-1";
    cleanup_streams(ws).await;

    let rt_a = Arc::new(make_runtime("a", ws).await.expect("runtime a"));
    let _rt_b = Arc::new(make_runtime("b", ws).await.expect("runtime b"));
    let _rt_c = Arc::new(make_runtime("c", ws).await.expect("runtime c"));

    // Instance B and C must create consumer groups BEFORE messages are
    // published — Redis XGROUP CREATE with "$" only tracks messages
    // arriving after creation.
    let ready_stream = format!("orchestrator:ready:{}", ws);

    let backend_b = RedisMessageBackend::new(REDIS_URL).await.unwrap();
    backend_b
        .create_consumer_group(&ready_stream, "cg-b")
        .await
        .unwrap();
    let mut stream_b = backend_b.subscribe(&ready_stream, "cg-b").await.unwrap();

    let backend_c = RedisMessageBackend::new(REDIS_URL).await.unwrap();
    backend_c
        .create_consumer_group(&ready_stream, "cg-c")
        .await
        .unwrap();
    let mut stream_c = backend_c.subscribe(&ready_stream, "cg-c").await.unwrap();

    // Instance A submits a goal with two independent tasks.
    let tasks = vec![
        make_task("t1", TaskType::DagNode, vec![], ws),
        make_task("t2", TaskType::DagNode, vec![], ws),
    ];
    rt_a.submit_goal("g1", tasks).await.unwrap();
    rt_a.publish_ready_tasks().await.unwrap();

    let mut b_received = Vec::new();
    let mut c_received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    while Instant::now() < deadline && (b_received.len() < 2 || c_received.len() < 2) {
        if b_received.len() < 2 {
            if let Ok(Some(Ok((_, bytes)))) =
                tokio::time::timeout(Duration::from_millis(300), stream_b.next()).await
            {
                b_received.push(bytes);
            }
        }
        if c_received.len() < 2 {
            if let Ok(Some(Ok((_, bytes)))) =
                tokio::time::timeout(Duration::from_millis(300), stream_c.next()).await
            {
                c_received.push(bytes);
            }
        }
    }

    // Both consumers should have received both ready tasks (broadcast semantics
    // via distinct consumer groups).
    assert_eq!(
        b_received.len(),
        2,
        "consumer B should see 2 ready tasks, got {}",
        b_received.len()
    );
    assert_eq!(
        c_received.len(),
        2,
        "consumer C should see 2 ready tasks, got {}",
        c_received.len()
    );

    cleanup_streams(ws).await;
}

// ---------------------------------------------------------------------------
// Test 2: Task-complete message published by one instance is seen by all.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_task_complete_propagates_to_all_instances() {
    if !redis_available().await {
        eprintln!("SKIP: Redis not available");
        return;
    }
    let ws = "cluster-test-ws-2";
    cleanup_streams(ws).await;

    let rt_a = Arc::new(make_runtime("a", ws).await.expect("runtime a"));
    let _rt_b = Arc::new(make_runtime("b", ws).await.expect("runtime b"));
    let _rt_c = Arc::new(make_runtime("c", ws).await.expect("runtime c"));

    // Create consumer groups for B and C BEFORE publishing — Redis only
    // tracks messages arriving after group creation when using "$".
    let result_stream = format!("orchestrator:results:{}", ws);

    let backend_b = RedisMessageBackend::new(REDIS_URL).await.unwrap();
    backend_b
        .create_consumer_group(&result_stream, "cg-b-res")
        .await
        .unwrap();
    let mut stream_b = backend_b
        .subscribe(&result_stream, "cg-b-res")
        .await
        .unwrap();

    let backend_c = RedisMessageBackend::new(REDIS_URL).await.unwrap();
    backend_c
        .create_consumer_group(&result_stream, "cg-c-res")
        .await
        .unwrap();
    let mut stream_c = backend_c
        .subscribe(&result_stream, "cg-c-res")
        .await
        .unwrap();

    // Submit and ready a task on instance A.
    let tasks = vec![make_task("tc1", TaskType::DagNode, vec![], ws)];
    rt_a.submit_goal("g-tc", tasks).await.unwrap();
    rt_a.publish_ready_tasks().await.unwrap();

    // Instance A simulates completion and publishes the result.
    let msg = DagMessage::TaskComplete {
        message_id: Uuid::new_v4().to_string(),
        timestamp: Utc::now(),
        task_id: "tc1".into(),
        result: serde_json::json!({"status": "ok"}),
        sender: "agent-a".into(),
        recipient: "orchestrator".into(),
    };
    let payload = serde_json::to_vec(&msg).unwrap();

    let backend_pub = RedisMessageBackend::new(REDIS_URL).await.unwrap();
    backend_pub.publish(&result_stream, &payload).await.unwrap();

    let b_msg = tokio::time::timeout(Duration::from_secs(3), stream_b.next())
        .await
        .ok()
        .flatten();
    let c_msg = tokio::time::timeout(Duration::from_secs(3), stream_c.next())
        .await
        .ok()
        .flatten();

    assert!(
        b_msg.is_some(),
        "instance B should receive the TaskComplete message"
    );
    assert!(
        c_msg.is_some(),
        "instance C should receive the TaskComplete message"
    );

    // Verify payload content.
    let (_, bytes_b) = b_msg.unwrap().unwrap();
    let decoded: DagMessage = serde_json::from_slice(&bytes_b).unwrap();
    match decoded {
        DagMessage::TaskComplete { task_id, .. } => assert_eq!(task_id, "tc1"),
        _ => panic!("expected TaskComplete"),
    }

    cleanup_streams(ws).await;
}

// ---------------------------------------------------------------------------
// Test 3: State consistency — orchestrator snapshot survives across instances.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_orchestrator_state_shared_via_backend() {
    if !redis_available().await {
        eprintln!("SKIP: Redis not available");
        return;
    }
    let ws = "cluster-test-ws-3";
    cleanup_streams(ws).await;

    // Instance A creates a task DAG.
    let rt_a = Arc::new(make_runtime("a", ws).await.expect("runtime a"));
    let tasks = vec![
        make_task("s1", TaskType::DagNode, vec![], ws),
        make_task("s2", TaskType::DagNode, vec!["s1".into()], ws),
    ];
    rt_a.submit_goal("g-state", tasks).await.unwrap();

    // Query orchestrator state from instance A.
    let ready_on_a = rt_a.orchestrator().find_ready_tasks().await.len();
    assert_eq!(ready_on_a, 1, "only s1 should be ready");

    // Instance B creates its own runtime but should be able to inspect the
    // same workspace stream (it does not share memory state, but the
    // message backend carries the events).
    let rt_b = Arc::new(make_runtime("b", ws).await.expect("runtime b"));

    // To truly share state we would need a StateBackend; for this test we
    // verify that both instances can at least publish/consume on the same
    // workspace stream without collision.
    rt_b.publish_ready_tasks().await.unwrap(); // should be a no-op (no tasks in B's mem orch)

    // Confirm B can read the ready task from the shared stream.
    // Consumer group must be created BEFORE publish_ready_tasks() is called.
    let ready_stream = format!("orchestrator:ready:{}", ws);
    let backend_b = RedisMessageBackend::new(REDIS_URL).await.unwrap();
    backend_b
        .create_consumer_group(&ready_stream, "cg-b-state")
        .await
        .unwrap();
    let mut stream_b = backend_b
        .subscribe(&ready_stream, "cg-b-state")
        .await
        .unwrap();

    // Instance A publishes ready tasks.
    rt_a.publish_ready_tasks().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg = tokio::time::timeout(Duration::from_secs(3), stream_b.next())
        .await
        .ok()
        .flatten();
    assert!(
        msg.is_some(),
        "B should see A's ready task on the shared stream"
    );

    cleanup_streams(ws).await;
}
