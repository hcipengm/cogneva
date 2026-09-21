use async_trait::async_trait;
use cog_core::{
    DagMessage, MessageBackend, OrchestratorControl, SFError, SFResult, ShutdownSignal, Task,
    TaskExecutor, TaskResult, TaskType,
};
use futures::{FutureExt, StreamExt};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

// Pending 恢复：pod 在执行途中死掉时，消息在组内永 pending，
// `subscribe`（XREADGROUP ">"）只读新消息，永远不会重投——旧消息会
// 堵住依赖它的下游任务。后台清扫器周期性 XAUTOCLAIM 认领 idle 超
// 阈值的 pending 消息并重走完整执行管线。
// 阈值必须大于最长任务执行时长，否则正在执行的长任务会被误判死亡
// 而并发重投（at-least-once：下游 complete_task 需容忍重复）。
const PENDING_IDLE_MS: u64 = 10 * 60 * 1000;
const CLAIM_INTERVAL: Duration = Duration::from_secs(60);
const CLAIM_BATCH: usize = 16;
/// 认领消息的执行并发上限。tick 只负责认领和派发，不能在某条认领
/// 消息的执行上 await：agent 任务可能跑几十分钟，串行处理会让整个
/// pending 恢复循环停摆（实测一条任务执行 90+ 分钟，期间零
/// XAUTOCLAIM，恢复网形同虚设）。
///
/// 进程级上限：本进程读的每条 ready 队列共用同一份许可，多读一条队列
/// 不该把在途执行翻倍。
const CLAIM_CONCURRENCY: usize = 4;

/// Dispatcher that routes ready tasks to the first [`TaskExecutor`] whose
/// [`TaskExecutor::supports`] returns `true`.
/// Decouples the orchestrator from concrete execution crates
/// (`cog-collaboration`, `cog-extension`) so new backends can be registered
/// at start-up without modifying the orchestrator.
#[derive(Clone)]
pub struct TaskExecutorRouter {
    executors: Arc<tokio::sync::RwLock<Vec<Arc<dyn TaskExecutor>>>>,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
}

impl TaskExecutorRouter {
    pub fn new() -> Self {
        Self {
            executors: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            orchestrator: None,
        }
    }

    pub async fn with_executor(self, executor: Arc<dyn TaskExecutor>) -> Self {
        self.executors.write().await.push(executor);
        self
    }

    pub fn with_orchestrator(mut self, orchestrator: Arc<dyn OrchestratorControl>) -> Self {
        self.orchestrator = Some(orchestrator);
        self
    }

    pub async fn register(&self, executor: Arc<dyn TaskExecutor>) {
        self.executors.write().await.push(executor);
    }

    /// Find a matching executor and run the task.
    /// Returns [`SFError::Agent`] when no registered executor claims the task
    /// type.
    pub async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
        let executor = self
            .executors
            .read()
            .await
            .iter()
            .find(|e| e.supports(&task.task_type))
            .cloned()
            .ok_or_else(|| {
                SFError::Agent(format!(
                    "No executor supports task type {:?}",
                    task.task_type
                ))
            })?;
        executor.execute(task).await
    }

    /// Consume ready tasks from the message backend, execute them, and publish
    /// results back to the results stream.
    ///
    /// Reads every queue [`crate::ready_queue::consumed_ready_streams`] names
    /// for this deployment. The queues share one in-flight budget: the
    /// concurrency ceiling is per process, so reading a second queue must not
    /// double the number of tasks this process runs at once.
    pub async fn run_consumer(
        &self,
        task_backend: Arc<dyn MessageBackend>,
        result_backend: Arc<dyn MessageBackend>,
        workspace_id: &str,
        owns_executor_role: bool,
        shutdown: ShutdownSignal,
    ) -> SFResult<()> {
        // 主订阅循环与清扫器共用的执行许可：新鲜投递与 idle 认领合计最多
        // CLAIM_CONCURRENCY 条在途执行。
        let claim_slots = Arc::new(tokio::sync::Semaphore::new(CLAIM_CONCURRENCY));

        let loops = crate::ready_queue::consumed_ready_streams(workspace_id, owns_executor_role)
            .into_iter()
            .map(|ready_stream| {
                self.run_one_ready_stream(
                    task_backend.clone(),
                    result_backend.clone(),
                    ready_stream,
                    workspace_id.to_string(),
                    claim_slots.clone(),
                    shutdown.clone(),
                )
            });

        for outcome in futures::future::join_all(loops).await {
            if let Err(e) = outcome {
                tracing::warn!("ready stream consumer exited: {e}");
            }
        }
        Ok(())
    }

    async fn run_one_ready_stream(
        &self,
        task_backend: Arc<dyn MessageBackend>,
        result_backend: Arc<dyn MessageBackend>,
        ready_stream: String,
        workspace_id: String,
        claim_slots: Arc<tokio::sync::Semaphore>,
        shutdown: ShutdownSignal,
    ) -> SFResult<()> {
        // Use a stable group name per workspace so that pod restacks do not
        // leak an unbounded number of consumer groups and so messages are not
        // lost when a new pod takes over. The group name intentionally omits
        // a random UUID; Redis Streams consumer groups are persistent and a
        // restarted consumer will resume from the last acknowledged ID.
        let group = format!("executor-loop-{workspace_id}");

        task_backend
            .create_consumer_group(&ready_stream, &group)
            .await?;

        let pipe = Arc::new(ReadyPipeline {
            task_backend: task_backend.clone(),
            result_backend: result_backend.clone(),
            ready_stream: ready_stream.clone(),
            group: group.clone(),
            workspace_id,
        });

        {
            let sweeper = self.clone();
            let sweep_task_backend = task_backend.clone();
            let sweep_shutdown = shutdown.clone();
            let sweep_pipe = pipe.clone();
            let sweep_slots = claim_slots.clone();
            tokio::spawn(async move {
                // 观测面：清扫器是唯一同时知道"扫哪条流"和"按什么阈值扫"的地方，
                // 由它把 pending 状态报给指标面。先量一次再进循环，让系列在首个
                // 节拍前就存在——否则一个从没量过的流和一个量过发现干净的流
                // 在抓取面上都是"没有证据"。
                let observer = crate::observable::stream_pending_observable();
                observer
                    .measure(
                        &*sweep_task_backend,
                        &sweep_pipe.ready_stream,
                        &sweep_pipe.group,
                        PENDING_IDLE_MS,
                        CLAIM_INTERVAL.as_secs(),
                    )
                    .await;
                let mut ticker = tokio::time::interval(CLAIM_INTERVAL);
                loop {
                    tokio::select! {
                        biased;
                        _ = sweep_shutdown.wait() => break,
                        _ = ticker.tick() => {
                            match sweep_task_backend
                                .claim_pending(
                                    &sweep_pipe.ready_stream,
                                    &sweep_pipe.group,
                                    PENDING_IDLE_MS,
                                    CLAIM_BATCH,
                                )
                                .await
                            {
                                Ok(claimed) if !claimed.is_empty() => {
                                    tracing::warn!(
                                        stream = %sweep_pipe.ready_stream,
                                        count = claimed.len(),
                                        "Claimed idle pending ready messages for re-execution"
                                    );
                                    for (msg_id, bytes) in claimed {
                                        // 先拿许可再派发：限制重执行并发，shutdown
                                        // 时也不再起新执行；许可在处理任务内持有到
                                        // 执行结束。
                                        let permit = tokio::select! {
                                            _ = sweep_shutdown.wait() => break,
                                            acquired = sweep_slots.clone().acquire_owned() => match acquired {
                                                Ok(permit) => permit,
                                                Err(_) => break,
                                            },
                                        };
                                        sweeper.spawn_ready_processing(
                                            sweep_pipe.clone(),
                                            msg_id,
                                            bytes,
                                            permit,
                                        );
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        stream = %sweep_pipe.ready_stream,
                                        "Pending claim sweep failed: {e}"
                                    );
                                }
                            }
                            // 每轮必量，包括认领失败那一轮：报出去的是这一轮扫完仍然
                            // 超龄的那部分，也就是清扫器没能收回的工作。认领失败时这
                            // 个数最该被看见，走到 continue 就会把它漏掉。
                            observer
                                .measure(
                                    &*sweep_task_backend,
                                    &sweep_pipe.ready_stream,
                                    &sweep_pipe.group,
                                    PENDING_IDLE_MS,
                                    CLAIM_INTERVAL.as_secs(),
                                )
                                .await;
                        }
                    }
                }
            });
        }

        // Resubscribe on stream failure/end instead of exiting the spawned
        // task: a single transient read error historically ended the loop and
        // froze the ready group for days until the next pod restart.
        'subscribe: loop {
            let mut stream = match task_backend.subscribe(&ready_stream, &group).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("ready stream subscribe failed, retrying: {e}");
                    tokio::select! {
                        _ = shutdown.wait() => break 'subscribe,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => continue 'subscribe,
                    }
                }
            };

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait() => break 'subscribe,
                    msg = stream.next() => match msg {
                        // 读取与执行解耦：订阅循环只负责投递，处理在独立
                        // 任务里跑。串行 await 执行会让一条长任务（timeout
                        // 最长 30 分钟）期间完全不发 XREADGROUP，积压消息
                        // 全部队头阻塞（实测重启后 lag 被占住 ~30 分钟）。
                        // 许可在读取侧获取：在途执行打满时订阅自然背压，
                        // 不会无限派发。
                        Some(Ok((msg_id, bytes))) => {
                            let permit = tokio::select! {
                                _ = shutdown.wait() => break 'subscribe,
                                acquired = claim_slots.clone().acquire_owned() => match acquired {
                                    Ok(permit) => permit,
                                    Err(_) => break 'subscribe,
                                },
                            };
                            self.spawn_ready_processing(
                                pipe.clone(),
                                msg_id,
                                bytes,
                                permit,
                            );
                        }
                        Some(Err(e)) => {
                            tracing::warn!("task stream error, resubscribing: {e}");
                            break;
                        }
                        None => {
                            tracing::warn!("ready stream ended, resubscribing");
                            break;
                        }
                    }
                }
            }

            tokio::select! {
                _ = shutdown.wait() => break 'subscribe,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }
        }

        Ok(())
    }

    /// 把一条已拿到执行许可的 ready 消息派发到独立任务。主订阅循环与
    /// pending 清扫器共用：调用方负责先取得许可（执行打满时在读取侧背
    /// 压），处理任务持有许可到执行结束。panic 只杀单条处理任务并记录，
    /// 消息不 ack 留在 PEL，超过 idle 阈值后由清扫器重新认领。
    fn spawn_ready_processing(
        &self,
        pipe: Arc<ReadyPipeline>,
        msg_id: String,
        bytes: Vec<u8>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let guarded = AssertUnwindSafe(this.process_ready_message(&pipe, msg_id, &bytes));
            if let Err(payload) = guarded.catch_unwind().await {
                tracing::warn!(
                    "ready message processing panicked: {}",
                    panic_message(&payload)
                );
            }
        });
    }

    /// 执行一条 ready 消息的全管线：反序列化 → start_task → execute →
    /// 发布结果 → ack。主订阅循环与 pending 恢复清扫器共用。
    async fn process_ready_message(&self, pipe: &ReadyPipeline, msg_id: String, bytes: &[u8]) {
        let task: Task = match serde_json::from_slice(bytes) {
            Ok(t) => t,
            Err(e) => {
                // 毒消息永远不会反序列化成功，不 ack 会被清扫器每 10 分钟
                // 重投一次、空转到流被删为止；ack 丢弃让队列过得去。
                tracing::warn!(msg_id = %msg_id, "Failed to deserialize task from ready stream: {e}; dropping");
                if let Err(e) = pipe
                    .task_backend
                    .ack(
                        &pipe.ready_stream,
                        &pipe.group,
                        std::slice::from_ref(&msg_id),
                    )
                    .await
                {
                    tracing::warn!(msg_id = %msg_id, "Failed to ack poison ready message: {e}");
                }
                return;
            }
        };

        // Notify orchestrator that task is now running.
        if let Some(ref orch) = self.orchestrator {
            if let Err(e) = orch.start_task(&task.id).await {
                // 领域拒绝（TaskFailed：任务不存在/已在跑/已终态）说明这条
                // ready 消息是历史残留或重复投递——执行无意义，结果也无法
                // 落库（complete 要求 Running），ack 后直接丢弃，避免白跑
                // LLM 并被清扫器反复认领。非领域错误（基础设施抖动）保留
                // 原行为：继续执行。
                if matches!(e, cog_core::SFError::TaskFailed { .. }) {
                    tracing::warn!(task_id = %task.id, msg_id = %msg_id, "ready message rejected by DAG ({e}); dropping");
                    if let Err(e) = pipe
                        .task_backend
                        .ack(
                            &pipe.ready_stream,
                            &pipe.group,
                            std::slice::from_ref(&msg_id),
                        )
                        .await
                    {
                        tracing::warn!(task_id = %task.id, msg_id = %msg_id, "Failed to ack stale ready message: {e}");
                    }
                    return;
                }
                tracing::warn!(task_id = %task.id, "Failed to start task via orchestrator: {e}");
            }
        }

        // timeout_seconds 是任务自带的执行契约。编排侧的超时检查器只会把
        // Running 任务标记失败/重试，并不取消这里的执行 future；不在此兑
        // 现，失控的 agent 任务会无限期占住串行消费位置（实测 90+ 分钟不
        // 返回，后续 ready 消息全部队头阻塞）。超时按执行失败处理，结果
        // 走既有发布路径，重试/DLQ 由 DAG 侧决定。
        let timeout = Duration::from_secs(task.timeout_seconds.max(1));
        let result = match tokio::time::timeout(timeout, self.execute(&task)).await {
            Ok(result) => result,
            Err(_) => Err(SFError::Agent(format!(
                "Task execution timed out after {} seconds",
                task.timeout_seconds
            ))),
        };

        let payload = match result {
            Ok(r) => {
                let msg = DagMessage::TaskComplete {
                    message_id: format!("res-{}", task.id),
                    timestamp: chrono::Utc::now(),
                    task_id: task.id.clone(),
                    result: r.output.clone(),
                    sender: "executor-loop".into(),
                    recipient: "dag-executor".into(),
                };
                match serde_json::to_vec(&msg) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(task_id = %task.id, "Failed to serialize task result: {e}");
                        return;
                    }
                }
            }
            Err(e) => {
                let msg = DagMessage::TaskFailed {
                    message_id: format!("res-{}", task.id),
                    timestamp: chrono::Utc::now(),
                    task_id: task.id.clone(),
                    // 文本给人读，类型给判定读。`e` 是 SFError，类型在这里
                    // 就地取出，不让下游回头去解析那句 Display。
                    error: e.to_string(),
                    error_cause: e.upstream_failure(),
                    // 上游明说的等待同样要过桥：算出重试时刻的是消费这条
                    // 消息的进程，明说的时长必须在消息里跟着走。
                    retry_after_secs: e.retry_after_secs(),
                    sender: "executor-loop".into(),
                    recipient: "dag-executor".into(),
                };
                match serde_json::to_vec(&msg) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(task_id = %task.id, "Failed to serialize task failure: {e}");
                        return;
                    }
                }
            }
        };

        let result_stream = format!("orchestrator:results:{}", pipe.workspace_id);
        if let Err(e) = pipe.result_backend.publish(&result_stream, &payload).await {
            tracing::warn!(task_id = %task.id, "Failed to publish result: {e}");
        } else {
            tracing::info!(task_id = %task.id, "Published task result to {result_stream}");
        }
        if let Err(e) = pipe
            .task_backend
            .ack(
                &pipe.ready_stream,
                &pipe.group,
                std::slice::from_ref(&msg_id),
            )
            .await
        {
            tracing::warn!(task_id = %task.id, msg_id = %msg_id, "Failed to ack ready message: {e}");
        }
    }
}

/// process_ready_message 的共享上下文，主订阅循环与 pending 清扫器共用，
/// 持有所有权以便清扫器把处理派发到独立任务。
struct ReadyPipeline {
    task_backend: Arc<dyn MessageBackend>,
    result_backend: Arc<dyn MessageBackend>,
    ready_stream: String,
    group: String,
    workspace_id: String,
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[async_trait]
impl TaskExecutor for TaskExecutorRouter {
    fn supports(&self, task_type: &TaskType) -> bool {
        // supports is a sync trait method; use try_read to avoid blocking.
        match self.executors.try_read() {
            Ok(guard) => guard.iter().any(|e| e.supports(task_type)),
            Err(_) => false,
        }
    }

    async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
        TaskExecutorRouter::execute(self, task).await
    }
}

impl Default for TaskExecutorRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod ready_pipeline_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{MessageStream, TaskResultMetadata, UpstreamFailure};

    #[derive(Default)]
    struct ScriptedBackend {
        published: tokio::sync::Mutex<Vec<(String, Vec<u8>)>>,
        acks: tokio::sync::Mutex<Vec<(String, String, Vec<String>)>>,
    }

    #[async_trait]
    impl MessageBackend for ScriptedBackend {
        async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
            self.published
                .lock()
                .await
                .push((subject.to_string(), payload.to_vec()));
            Ok(())
        }
        async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
            self.acks
                .lock()
                .await
                .push((stream.to_string(), group.to_string(), ids.to_vec()));
            Ok(())
        }
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Vec<(String, Vec<u8>)>> {
            Ok(Vec::new())
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        async fn dlq(&self, _stream: &str, _msg_id: &str, _reason: &str) -> SFResult<()> {
            Ok(())
        }
    }

    /// 上游拒绝并明说等待时长的执行器，用来验证类型与时长都过了总线。
    struct RefusingExecutor;

    #[async_trait]
    impl TaskExecutor for RefusingExecutor {
        fn supports(&self, _task_type: &TaskType) -> bool {
            true
        }
        async fn execute(&self, _task: &Task) -> SFResult<TaskResult> {
            Err(SFError::upstream_refused_after(
                UpstreamFailure::RateLimited,
                "API error (HTTP 429): slow down",
                Some(90),
            ))
        }
    }

    struct SlowExecutor {
        sleep_secs: u64,
        panic_instead: bool,
    }

    #[async_trait]
    impl TaskExecutor for SlowExecutor {
        fn supports(&self, _task_type: &TaskType) -> bool {
            true
        }
        async fn execute(&self, _task: &Task) -> SFResult<TaskResult> {
            if self.panic_instead {
                panic!("executor boom");
            }
            tokio::time::sleep(Duration::from_secs(self.sleep_secs)).await;
            Ok(TaskResult {
                success: true,
                output: serde_json::json!({"done": true}),
                metadata: TaskResultMetadata::new("slow"),
            })
        }
    }

    fn test_task(timeout_seconds: u64) -> Task {
        let now = chrono::Utc::now();
        // task_type custom 变体即可，SlowExecutor supports 全部类型。
        let task: Task = serde_json::from_value(serde_json::json!({
            "id": "t-timeout",
            "task_type": {"custom": "slow"},
            "status": "pending",
            "input": {},
            "blocked_by": [],
            "blocks": [],
            "priority": 1,
            "created_at": now,
            "updated_at": now,
            "retry_count": 0,
            "max_retries": 3,
            "timeout_seconds": timeout_seconds,
            "is_executable": true
        }))
        .unwrap();
        task
    }

    fn test_pipe(backend: Arc<ScriptedBackend>) -> Arc<ReadyPipeline> {
        Arc::new(ReadyPipeline {
            task_backend: backend.clone(),
            result_backend: backend,
            ready_stream: "ready".into(),
            group: "grp".into(),
            workspace_id: "ws".into(),
        })
    }

    #[tokio::test]
    async fn execution_is_cancelled_at_task_timeout() {
        // 回归：execute future 不受 timeout_seconds 约束，失控 agent 任务能
        // 占住串行消费位置 90+ 分钟，后续消息全部队头阻塞。
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(SlowExecutor {
                sleep_secs: 10,
                panic_instead: false,
            }))
            .await;

        let started = std::time::Instant::now();
        router
            .process_ready_message(
                &pipe,
                "m1".into(),
                &serde_json::to_vec(&test_task(1)).unwrap(),
            )
            .await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "execution was not cancelled at the declared timeout: {:?}",
            started.elapsed()
        );

        let published = backend.published.lock().await;
        assert_eq!(published.len(), 1, "timeout must still publish a result");
        let msg: DagMessage = serde_json::from_slice(&published[0].1).unwrap();
        match msg {
            DagMessage::TaskFailed { error, .. } => {
                assert!(error.contains("timed out after 1 seconds"), "{error}");
            }
            other => panic!("expected TaskFailed on timeout, got {other:?}"),
        }
        let acks = backend.acks.lock().await;
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].2, vec!["m1".to_string()]);
    }

    /// 上游的失败类型与它明说的等待都要过总线：算出重试时刻的是消费这条消息
    /// 的进程，两个信号留在产生它们的进程里，消费侧就只能拿文本去猜。
    #[tokio::test]
    async fn a_refusal_publishes_its_cause_and_stated_wait() {
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(RefusingExecutor))
            .await;

        router
            .process_ready_message(
                &pipe,
                "m1".into(),
                &serde_json::to_vec(&test_task(30)).unwrap(),
            )
            .await;

        let published = backend.published.lock().await;
        assert_eq!(published.len(), 1);
        match serde_json::from_slice::<DagMessage>(&published[0].1).unwrap() {
            DagMessage::TaskFailed {
                error_cause,
                retry_after_secs,
                ..
            } => {
                assert_eq!(error_cause, Some(UpstreamFailure::RateLimited));
                assert_eq!(retry_after_secs, Some(90));
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn poison_ready_message_is_acked() {
        // 回归：反序列化失败直接 return 不 ack，毒消息被清扫器每 10 分钟
        // 重投一次，永远过不去。
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(SlowExecutor {
                sleep_secs: 0,
                panic_instead: false,
            }))
            .await;

        router
            .process_ready_message(&pipe, "m-poison".into(), b"not-json")
            .await;

        assert!(backend.published.lock().await.is_empty());
        let acks = backend.acks.lock().await;
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].2, vec!["m-poison".to_string()]);
    }

    #[tokio::test]
    async fn successful_ready_message_publishes_and_acks() {
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(SlowExecutor {
                sleep_secs: 0,
                panic_instead: false,
            }))
            .await;

        router
            .process_ready_message(
                &pipe,
                "m-ok".into(),
                &serde_json::to_vec(&test_task(60)).unwrap(),
            )
            .await;

        let published = backend.published.lock().await;
        assert_eq!(published.len(), 1);
        let msg: DagMessage = serde_json::from_slice(&published[0].1).unwrap();
        assert!(matches!(msg, DagMessage::TaskComplete { .. }));
        let acks = backend.acks.lock().await;
        assert_eq!(acks[0].2, vec!["m-ok".to_string()]);
    }

    struct QueueBackend {
        published: tokio::sync::Mutex<Vec<(String, Vec<u8>)>>,
        acks: tokio::sync::Mutex<Vec<(String, String, Vec<String>)>>,
        queue: tokio::sync::Mutex<std::collections::VecDeque<(String, Vec<u8>)>>,
    }

    impl QueueBackend {
        fn new(queue: Vec<(String, Vec<u8>)>) -> Self {
            Self {
                published: tokio::sync::Mutex::new(Vec::new()),
                acks: tokio::sync::Mutex::new(Vec::new()),
                queue: tokio::sync::Mutex::new(queue.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl MessageBackend for QueueBackend {
        async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
            self.published
                .lock()
                .await
                .push((subject.to_string(), payload.to_vec()));
            Ok(())
        }
        async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
            // 吐出预置消息后永久 pending，模拟空流长轮询。
            let items: Vec<(String, Vec<u8>)> = self.queue.lock().await.drain(..).collect();
            Ok(Box::pin(
                futures::stream::iter(items.into_iter().map(Ok)).chain(futures::stream::pending()),
            ))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
            self.acks
                .lock()
                .await
                .push((stream.to_string(), group.to_string(), ids.to_vec()));
            Ok(())
        }
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Vec<(String, Vec<u8>)>> {
            Ok(Vec::new())
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        async fn dlq(&self, _stream: &str, _msg_id: &str, _reason: &str) -> SFResult<()> {
            Ok(())
        }
    }

    struct ActiveGuard {
        active: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    struct ConcurrencyExecutor {
        sleep_secs: u64,
        active: Arc<std::sync::atomic::AtomicUsize>,
        max_active: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl TaskExecutor for ConcurrencyExecutor {
        fn supports(&self, _task_type: &TaskType) -> bool {
            true
        }
        async fn execute(&self, _task: &Task) -> SFResult<TaskResult> {
            use std::sync::atomic::Ordering;
            let _guard = ActiveGuard {
                active: self.active.clone(),
            };
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(self.sleep_secs)).await;
            Ok(TaskResult {
                success: true,
                output: serde_json::json!({"done": true}),
                metadata: TaskResultMetadata::new("concurrency"),
            })
        }
    }

    #[tokio::test]
    async fn slow_task_does_not_block_ready_stream() {
        // 回归：主订阅循环串行 await 处理，一条长任务执行期间不发
        // XREADGROUP，后续消息全部队头阻塞（实测 timeout 1800s 的任务把
        // 读取位占住约 30 分钟，lag 无法消化）。派发改为 spawn 后，第二条
        // 消息必须与第一条并发执行，两者都在各自 1s timeout 后完成。
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mk_task = |id: &str| {
            let mut task = test_task(1);
            task.id = id.into();
            serde_json::to_vec(&task).unwrap()
        };
        let backend = Arc::new(QueueBackend::new(vec![
            ("m1".into(), mk_task("t-a")),
            ("m2".into(), mk_task("t-b")),
        ]));
        let router = Arc::new(
            TaskExecutorRouter::new()
                .with_executor(Arc::new(ConcurrencyExecutor {
                    sleep_secs: 10,
                    active: active.clone(),
                    max_active: max_active.clone(),
                }))
                .await,
        );

        let shutdown = ShutdownSignal::new();
        let runner = {
            let router = router.clone();
            let backend = backend.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                router
                    .run_consumer(backend.clone(), backend, "ws", false, shutdown)
                    .await
            })
        };

        let started = std::time::Instant::now();
        loop {
            if backend.acks.lock().await.len() == 2 {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "both messages were not processed"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "stream stayed blocked during the slow task: {:?}",
            started.elapsed()
        );
        assert!(
            max_active.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "messages were not processed concurrently"
        );

        shutdown.trigger();
        runner.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn claimed_processing_panic_is_contained_and_not_acked() {
        // 回归：清扫 tick 内直接 await 处理，一条消息 panic 会永久杀死
        // spawn 的清扫任务；现在处理被 catch_unwind 兜住，消息不 ack、留
        // 在 PEL 等下一轮重新认领。
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(SlowExecutor {
                sleep_secs: 0,
                panic_instead: true,
            }))
            .await;

        let outcome = AssertUnwindSafe(router.process_ready_message(
            &pipe,
            "m-boom".into(),
            &serde_json::to_vec(&test_task(5)).unwrap(),
        ))
        .catch_unwind()
        .await;
        assert!(outcome.is_err(), "panic must be caught by catch_unwind");
        assert!(
            backend.acks.lock().await.is_empty(),
            "panicked message must stay unacked for redelivery"
        );
    }
}

/// 队列归属：变更生成的产物只有产出它的进程读得到，所以承载这类任务的
/// ready 队列只能由执行器角色的部署来读。非属主读到了就是白跑一轮——
/// 变更写进一个没有任何读者存在的目录，且账单照付。
#[cfg(test)]
mod ready_queue_placement_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{MessageStream, TaskResultMetadata};
    use std::collections::{HashMap, VecDeque};

    /// 一条队列上的预置消息：(msg_id, payload)。
    type Messages = VecDeque<(String, Vec<u8>)>;

    /// 按 subject 分队列投递的后端，并记录"谁订阅了哪条队列"。
    #[derive(Default)]
    struct PerStreamBackend {
        queues: tokio::sync::Mutex<HashMap<String, Messages>>,
        subscribed: tokio::sync::Mutex<Vec<String>>,
        acks: tokio::sync::Mutex<Vec<String>>,
    }

    impl PerStreamBackend {
        async fn queue(&self, stream: &str, id: &str, payload: Vec<u8>) {
            self.queues
                .lock()
                .await
                .entry(stream.to_string())
                .or_default()
                .push_back((id.to_string(), payload));
        }
    }

    #[async_trait]
    impl MessageBackend for PerStreamBackend {
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn subscribe(&self, subject: &str, _group: &str) -> SFResult<MessageStream> {
            self.subscribed.lock().await.push(subject.to_string());
            let items: Vec<(String, Vec<u8>)> = self
                .queues
                .lock()
                .await
                .entry(subject.to_string())
                .or_default()
                .drain(..)
                .collect();
            Ok(Box::pin(
                futures::stream::iter(items.into_iter().map(Ok)).chain(futures::stream::pending()),
            ))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, _stream: &str, _group: &str, ids: &[String]) -> SFResult<()> {
            self.acks.lock().await.extend(ids.iter().cloned());
            Ok(())
        }
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Vec<(String, Vec<u8>)>> {
            Ok(Vec::new())
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        async fn dlq(&self, _stream: &str, _msg_id: &str, _reason: &str) -> SFResult<()> {
            Ok(())
        }
    }

    struct AcceptEverything;

    #[async_trait]
    impl TaskExecutor for AcceptEverything {
        fn supports(&self, _task_type: &TaskType) -> bool {
            true
        }
        async fn execute(&self, _task: &Task) -> SFResult<TaskResult> {
            Ok(TaskResult {
                success: true,
                output: serde_json::json!({"done": true}),
                metadata: TaskResultMetadata::new("accept"),
            })
        }
    }

    fn ordinary_task() -> Vec<u8> {
        serde_json::to_vec(&Task::new(
            "t-ordinary",
            TaskType::Custom("unit_of_work".into()),
            serde_json::json!({"goal": "ordinary"}),
        ))
        .unwrap()
    }

    fn generation_task() -> Vec<u8> {
        serde_json::to_vec(&Task::new(
            "t-generate",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it", "evolution_mode": "generate_change"}),
        ))
        .unwrap()
    }

    /// 跑到两条消息都处理完，或超时。
    async fn settle(backend: &PerStreamBackend, want_acks: usize) {
        let started = std::time::Instant::now();
        while backend.acks.lock().await.len() < want_acks {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "consumer did not process the expected messages: {:?}",
                backend.acks.lock().await
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn run_as(owns_executor_role: bool) -> (Arc<PerStreamBackend>, ShutdownSignal) {
        let shared = crate::ready_queue::ready_stream("ws");
        let local = crate::ready_queue::process_local_ready_stream("ws");
        let backend = Arc::new(PerStreamBackend::default());
        backend.queue(&shared, "m-ordinary", ordinary_task()).await;
        backend.queue(&local, "m-generate", generation_task()).await;

        let router = Arc::new(
            TaskExecutorRouter::new()
                .with_executor(Arc::new(AcceptEverything))
                .await,
        );
        let shutdown = ShutdownSignal::new();
        let runner = {
            let router = router.clone();
            let backend = backend.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                router
                    .run_consumer(backend.clone(), backend, "ws", owns_executor_role, shutdown)
                    .await
            })
        };
        tokio::pin!(runner);
        // 非属主只会收到一条消息；属主会收到两条。由调用方用 settle 等待，
        // 这里只确认订阅已建立。
        let started = std::time::Instant::now();
        while backend.subscribed.lock().await.is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "no subscription"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (backend, shutdown)
    }

    #[tokio::test]
    async fn a_deployment_without_the_executor_role_leaves_change_generation_alone() {
        let (backend, shutdown) = run_as(false).await;
        settle(&backend, 1).await;

        let acks = backend.acks.lock().await.clone();
        assert_eq!(
            acks,
            vec!["m-ordinary".to_string()],
            "a worker without the executor role must run only the shared queue"
        );
        let subscribed = backend.subscribed.lock().await.clone();
        assert!(
            !subscribed.contains(&crate::ready_queue::process_local_ready_stream("ws")),
            "a worker without the executor role must not subscribe to the \
             process-local queue at all: reading it would execute a change \
             generation where nothing scans the change dir, and the redelivery \
             sweeper would keep re-reading it. subscribed: {subscribed:?}"
        );
        drop(acks);
        shutdown.trigger();
    }

    #[tokio::test]
    async fn the_executor_role_runs_both_queues() {
        let (backend, shutdown) = run_as(true).await;
        settle(&backend, 2).await;

        let mut acks = backend.acks.lock().await.clone();
        acks.sort();
        assert_eq!(
            acks,
            vec!["m-generate".to_string(), "m-ordinary".to_string()],
            "the executor role owns change generation and must read its queue"
        );
        let subscribed = backend.subscribed.lock().await.clone();
        assert!(
            subscribed.contains(&crate::ready_queue::process_local_ready_stream("ws")),
            "subscribed: {subscribed:?}"
        );
        shutdown.trigger();
    }
}
