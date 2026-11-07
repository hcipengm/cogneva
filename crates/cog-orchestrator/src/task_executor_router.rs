use async_trait::async_trait;
use cog_core::{
    DagMessage, MessageBackend, OrchestratorControl, SFError, SFResult, ShutdownSignal, Task,
    TaskExecutor, TaskResult, TaskStatus, TaskType,
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
// The threshold is shared with the publisher's stall threshold, which reads the
// same window from the other side: a task still `Scheduled` after it is one
// whose message this sweeper is no longer going to deliver. Defined once in
// cog-core so the two cannot drift into a pair that publishes a duplicate while
// the first copy is still on its way.
const PENDING_IDLE_MS: u64 = cog_core::config::DEFAULT_READY_CLAIM_IDLE_SECS * 1000;
// cap: bound — sweeper poll period; PENDING_IDLE_MS is the claim threshold, this only sets how late a stuck message is seen
const CLAIM_INTERVAL: Duration = Duration::from_secs(60);
/// 一次认领要多少条：够把下一次醒来之前的工作铺在 executor 前面，也小到一次认领
/// 本身跨不过两轮之间的间隔。
// cap: bound — 够把下一次醒来之前的工作铺在 executor 前面，又小到一次认领跨不过两轮之间的间隔
const CLAIM_BATCH: usize = 16;
/// 给在途执行上限的那个环境变量名。
///
/// 这个数是**同时花额度的任务条数**，不是这台机器能吃多少：token 窗口按
/// 「一道活要多少」切分（`jobs_the_window_can_pay_for`），而在途的每一道都在
/// 同一份窗口里花。它此前是一个写死的 `4`，于是「一次跑几道」这件事没有运维
/// 入口——实测 2026-11-01：两个进程各 4 条在途，看板上恰好 8 条 `running`，
/// k3 的 5 小时窗口（1,670,569 token）在开窗 10 分钟里被花掉 1,105,275，
/// 八道变更各走一半、一道也没走完。这种数只能改代码才改得动，而它要改的时机
/// 恰好是额度读数变了的时候。
pub const CLAIM_CONCURRENCY_ENV: &str = "COGNEVA_TASK_CLAIM_CONCURRENCY";

/// 没有配置时的在途执行上限。
///
/// 与 [`cog_core::BoardWipLimits`] 同一个口径：**不配就按默认值跑，不配不等于
/// 零**。一个凭空造出来的 0 会让所有执行停摆，而它与「没配」在读数上是同一个
/// 空白。
// cap: bound — 进程级许可：本进程读的每条就绪队列共用同一份，多读一条队列不该把在途执行翻倍
pub const DEFAULT_CLAIM_CONCURRENCY: usize = 4;

/// How many stop-the-line tasks one process runs at once.
///
/// Not [`claim_concurrency()`]: that number sizes how many ordinary changes may
/// spend the token window together, and a red main is not ordinary work -- it
/// is the reason nothing newer can be deployed at all. It gets a budget of one
/// so that it can run while every ordinary permit is held, and stays at one so
/// that the cost of the exception is bounded by a single extra task in flight.
/// One is enough because a red main is one finding: the deployer files one row
/// per revision per day and the row names the whole failing set.
// cap: bound — 这条例外的价钱就定在这一格：单开一份许可，同时最多多一条在跑的活；不是量出来的，是「例外不该翻倍」定的
pub const STOP_THE_LINE_CLAIM_CONCURRENCY: usize = 1;

/// 进程本地车道的在途执行上限，从环境读。
///
/// 它是这一条车道自己的预算，与 [`claim_concurrency()`] 分开：只有带 executor
/// 角色的部署读这条队列（消息里的产物写进**本进程**的变更目录，别的部署读了
/// 也交不出去），所以「几条一起跑」只能由这个值定，别的部署配多少与它无关。
pub const LOCAL_CLAIM_CONCURRENCY_ENV: &str = "COGNEVA_LOCAL_CLAIM_CONCURRENCY";

/// How many process-local tasks one process runs at once when nothing is configured.
///
/// Same argument as [`STOP_THE_LINE_CLAIM_CONCURRENCY`], for the other lane that
/// only the executor deployment reads: its messages carry an artifact written
/// into this process's change directory, so a deployment that does not own the
/// executor role cannot run them at all and there is no second reader to pick
/// up the slack. Sharing the ordinary allowance made this lane stop reading
/// whenever ordinary work held every permit -- measured 2026-11-06 20:5x at
/// 1,771 s of waiting against 2 permits, with `entries-read` frozen a day
/// behind and 1,374 entries queued, while the publish side refused to add more
/// to a queue nobody was reading.
///
/// The default stays at one, and it is a knob rather than a second measurement
/// for a reason worth writing down before anyone turns it up. This lane is the
/// one every change-generation task goes to, so the number here is how many
/// changes can be worked on at once; measured on 2026-11-07 05:2x it was one,
/// with 1,904 tasks queued behind `claim_in_flight = 1`, `claim_wait_seconds`
/// pinned at 1,793 s, and 43 finished tasks a day -- 44 days to drain.
///
/// Turning it up is worth doing only if the step after this lane has room, and
/// that step was measured rather than inferred. On the same host on 2026-11-07
/// 06:0x the build gate's bound in force was 1 (`cogneva-build-gate.sh --who`,
/// dimension `io`). Over the six hours before that reading it made 19
/// acquisitions and held a slot for 244 of those 360 minutes -- 68% -- while
/// callers waited 31 minutes in total, a mean wait of 1.65 minutes against a
/// mean service of 12.8 minutes. One verification per slot is therefore about
/// 112 a day at full occupancy against the ~76 already running, so the gate has
/// room for roughly half again before its own queue is the one that grows. The
/// lane, by contrast, read `claim_in_flight = 1` for 100% of that same window
/// on every pod that carried it, and `claim_wait_seconds` stood at 836 s with
/// 1,954 rows still owed: the saturated step is this one, not the build.
///
/// That reading replaced an earlier one in this comment which claimed the gate
/// could pass about 53 changes a day whatever this lane admitted. It cannot be
/// settled by reading either bound -- both are 1, so both look like the wall --
/// and the number that separates them is how much of the last six hours each
/// was actually holding, which is 68% against 100%.
///
/// The wall behind that reading is memory, not the gate, and it was measured
/// the same hour the second permit was written into the chart and taken back
/// out of it. Measured on this host 2026-11-07 over the same six hours, the
/// evolution pod's `max_over_time(container_memory_working_set_bytes)` hit a
/// peak of 15.96 GiB (on this host) against its 16 GiB ceiling, with a mean of
/// 8.27 GiB (on this host): one change task already fills the ceiling, so a
/// second permit would not queue behind the build, it would be killed with the
/// first one still in flight. The namespace quota cannot hand this pod more --
/// measured the same day, 78Gi of `limits.memory` against 59.6Gi in use, with
/// 12Gi of replacement overlap required for a roll (on this host).
///
/// So the precondition for raising this number is a reading, not a judgement:
/// the pod's 24-hour peak has to come down to 12 GiB (on this host) and stay
/// there before a second permit goes in. The knob exists so that whoever does
/// that can widen the lane without a rebuild -- not so that raising it alone
/// can be mistaken for raising output.
///
/// 它现在是两件事的共同起点：导出模式的起始值与下限（还没量到一条任务的价格
/// 时用它），以及这个导出的界在任何一次读失败之后退回的那一格。
// cap: bound — 这条例外的价钱就定在这一格：单开一份许可，同时最多多一条在跑的活
pub const DEFAULT_LOCAL_CLAIM_CONCURRENCY: usize = 1;

/// 读一条在途执行上限。
///
/// `0` 被拒而不是被当成「不限」：两者在配置里是同一个字符，而误读成停摆
/// （或误读成不限）都会把额度花光的形状藏起来。拒绝时用默认值，并把这一行
/// 报告出去——悄悄换成默认值等于运维以为自己改的那个数生效了。
///
/// 本地车道那一格不到这里：它在调用之前就把 `0` 读成「按读数导出」
/// （见 [`local_claim_concurrency`]），所以同一条命令行在两条车道上含义不同，
/// 而这是有意的——那条车道的界真的可以量出来。
pub fn parse_claim_concurrency(raw: &str) -> Result<usize, String> {
    match raw.trim().parse::<usize>() {
        Ok(0) => Err(
            "0 would stop every execution; leave the variable unset to run at the default".into(),
        ),
        Ok(n) => Ok(n),
        Err(_) => Err(format!("`{}` is not a count of tasks", raw.trim())),
    }
}

/// 本进程的在途执行上限，从环境读。
///
/// 进程级上限：本进程读的每条 ready 队列共用同一份许可，多读一条队列不该把
/// 在途执行翻倍。它同时是 `cogneva_task_router_claim_slots` 报出去的那个数，
/// 所以配了多少、生效了多少在读数面是同一个值。
pub fn claim_concurrency() -> usize {
    match std::env::var(CLAIM_CONCURRENCY_ENV) {
        Err(_) => DEFAULT_CLAIM_CONCURRENCY,
        Ok(raw) => match parse_claim_concurrency(&raw) {
            Ok(n) => n,
            Err(problem) => {
                tracing::warn!(
                    variable = CLAIM_CONCURRENCY_ENV,
                    problem = %problem,
                    using = DEFAULT_CLAIM_CONCURRENCY,
                    "task claim concurrency entry ignored"
                );
                DEFAULT_CLAIM_CONCURRENCY
            }
        },
    }
}

/// 本进程本地车道的在途执行上限，从环境读；`None` = 由读数导出。
///
/// 与 [`claim_concurrency()`] 差一处，而这一处是故意的：不配**不是**取默认值，
/// 是取导出。这条车道的界取决于容器那一刻能拿多少内存和一条任务实际吃多少
/// （见 [`crate::lane_memory`]），两者都不在配置里，所以「没配」读成「去量」，
/// 而不是「按写下这一行的人那天看到的数跑」。
///
/// `0` 在这里不是被拒的输入，与平台其余几个界同一个口径
/// （`COGNEVA_BUILD_GATE_MAX_CONCURRENT: "0"`、目标容量 `"auto"`）：0 = 按读数导出，
/// 正数 = 钉死它的覆盖位。
pub fn local_claim_concurrency() -> Option<usize> {
    match std::env::var(LOCAL_CLAIM_CONCURRENCY_ENV) {
        Err(_) => None,
        Ok(raw) if raw.trim() == "0" || raw.trim().is_empty() => None,
        Ok(raw) => match parse_claim_concurrency(&raw) {
            Ok(n) => Some(n),
            Err(problem) => {
                tracing::warn!(
                    variable = LOCAL_CLAIM_CONCURRENCY_ENV,
                    problem = %problem,
                    using = "the derived bound",
                    "local task claim concurrency entry ignored"
                );
                None
            }
        },
    }
}

/// 本进程的在途执行预算：闸门本身，加上配出去的那个数。
///
/// 两者成对传，是因为它们必须成对读：`slots` 是真正拦在路上的许可，`limit` 是
/// 报给读数面的预算。分成两个参数时，「许可几条」与「读数说几条」是两个可以各自
/// 漂走的数——写死常量的时候正是这样，谁也看不见它。
#[derive(Clone)]
struct ClaimBudget {
    slots: Arc<tokio::sync::Semaphore>,
    limit: usize,
    /// 这个界是量出来的还是钉死的，随预算一起走：读数面要照着它打
    /// `source` 标签（见 [`crate::observable::CLAIM_SLOTS_SOURCE_LABEL`]）。
    source: &'static str,
}

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
        // `claim_concurrency()` 条在途执行。读一次，两条腿用的是同一个数——
        // 一处读一次会让「报出去的预算」和「真正的许可数」各走各的。
        let claim_concurrency = claim_concurrency();
        let claim_slots = Arc::new(tokio::sync::Semaphore::new(claim_concurrency));

        let claim = ClaimBudget {
            slots: claim_slots,
            limit: claim_concurrency,
            // 普通活那一格是部署钉的：它的界在 token 预算维度上，进程里没有
            // 那个读数可导（见 `deploy/helm/cogneva/tunables.yaml` 的同名行）。
            source: crate::observable::SOURCE_OVERRIDE,
        };
        // 停线那条通道的许可不来自上面那一份：它的整条理由就是「别的活占满
        // 所有许可时它仍然要跑」。共用一份时，优先级最高的那条变更也只是排在
        // 普通变更后面等前一条跑完。它自己的一份是这条例外的代价上界：同时
        // 最多一条修复在跑。
        let stop_the_line = crate::ready_queue::stop_the_line_ready_stream(workspace_id);
        let preempting = ClaimBudget {
            slots: Arc::new(tokio::sync::Semaphore::new(STOP_THE_LINE_CLAIM_CONCURRENCY)),
            limit: STOP_THE_LINE_CLAIM_CONCURRENCY,
            source: crate::observable::SOURCE_OVERRIDE,
        };
        // 进程本地那条通道同理，而且这一条是被实测逼出来的：它和普通通道共用一份
        // 许可时，普通活只要占满那一份，这条通道的读循环就停在 `acquire_owned` 上，
        // 一条消息都不再读。2026-11-06 20:5x 实测：许可证 2 个，`default` 与
        // `default:local` 各等了 1,674 与 1,771 秒（同一次抓取），`local` 的
        // `entries-read` 停在一天前、`lag` 涨到 1,374；发布侧按「这条队列还有没被读走
        // 的条目」把新任务挡在外面，于是自演化那一路在队列两端一起停住——读的一侧
        // 等许可，写的一侧等读。代号里的优先级（停线 > 自演化 > 普通活）此前只写在
        // 发布序上，认领这一侧没有它，所以那条优先序在满负荷时是一句话。
        //
        // 它自己的一份是这条例外的代价上界。这个数是整个平台的变更吞吐（每一条
        // 变更生成任务都只走这条道），而它的另一半代价在 pod 的内存上限上。
        //
        // 所以这个数不写死：配了就是那个数（覆盖位），没配或配成 0 则由容器内存与
        // 本进程的实测足迹导出，并每窗重算一次——界是量出来的，写死只能反映写下
        // 它那天的容器。上限那一半仍然只能由部署来给（namespace 配额），导出只
        // 负责把已有的上限用满。
        let local_bound = local_claim_concurrency();
        let local_slots = Arc::new(tokio::sync::Semaphore::new(
            local_bound.unwrap_or(DEFAULT_LOCAL_CLAIM_CONCURRENCY),
        ));
        let local = crate::ready_queue::process_local_ready_stream(workspace_id);
        let own = ClaimBudget {
            slots: local_slots.clone(),
            limit: local_bound.unwrap_or(DEFAULT_LOCAL_CLAIM_CONCURRENCY),
            source: if local_bound.is_none() {
                crate::observable::SOURCE_DERIVED
            } else {
                crate::observable::SOURCE_OVERRIDE
            },
        };
        // 只有真读这条车道的部署才起导出循环：控制面那份 Pod 不读 `:local`，
        // 在那里每 30 秒量一次内存再改一个没人用的信号量，只是一条空转的循环。
        if local_bound.is_none() && owns_executor_role {
            crate::lane_memory::spawn_lane_memory_bound(
                local.clone(),
                local_slots,
                DEFAULT_LOCAL_CLAIM_CONCURRENCY,
                shutdown.clone(),
                std::path::PathBuf::from(crate::lane_memory::CGROUP_ROOT),
            );
        }

        let loops = crate::ready_queue::consumed_ready_streams(workspace_id, owns_executor_role)
            .into_iter()
            .map(|ready_stream| {
                let budget = if ready_stream == stop_the_line {
                    preempting.clone()
                } else if ready_stream == local {
                    own.clone()
                } else {
                    claim.clone()
                };
                self.run_one_ready_stream(
                    task_backend.clone(),
                    result_backend.clone(),
                    ready_stream,
                    workspace_id.to_string(),
                    budget,
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
        claim: ClaimBudget,
        shutdown: ShutdownSignal,
    ) -> SFResult<()> {
        // 闸门与它报出去的预算一起进来、一起用：分开传时，「真正许可几条」与
        // 「读数面说几条」是两个各走各的数，而它们不一致正是这个数被写死时
        // 没人看得见的原因。
        let claim_slots = claim.slots;
        let claim_concurrency = claim.limit;
        let claim_source = claim.source;
        // Use a stable group name per workspace so that pod restacks do not
        // leak an unbounded number of consumer groups and so messages are not
        // lost when a new pod takes over. The group name intentionally omits
        // a random UUID; Redis Streams consumer groups are persistent and a
        // restarted consumer will resume from the last acknowledged ID.
        let group = crate::ready_queue::ready_group(&workspace_id);
        // One loop instance per consumed ready stream, so the liveness name has to
        // carry the stream: a shared name would let a dead stream's consumer hide
        // behind the beats of the stream that is still being served.
        let consumer_loop_name = format!("orchestrator_ready_consumer[{ready_stream}]");
        let reclaim_loop_name = format!("orchestrator_ready_reclaim[{ready_stream}]");

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

        // 读取侧背压的读数：许可打满时订阅循环停在这里、不再发 XREADGROUP，
        // 而「读循环安静」这件事本身分不清是总线不回还是本进程没有位置。
        // 许可的预算与等待时长从这一处报出去，和流读取的安静时长并排读。
        crate::observable::claim_slots_observable().register(
            &ready_stream,
            claim_slots.clone(),
            claim_concurrency,
            claim_source,
        );

        // 观测面单独成任务，不搭清扫器的节拍：清扫卡在认领上时指标序列会跟着
        // 静止，而抓取面上一条静止的序列和一条干净流量的序列是同一个样子——
        // 刚好把最该被看见的停滞藏了起来。
        crate::observable::spawn_pending_observer(
            crate::observable::READY_STREAM_PENDING_LOOP,
            task_backend.clone(),
            ready_stream.clone(),
            group.clone(),
            PENDING_IDLE_MS,
            CLAIM_INTERVAL,
            shutdown.clone(),
        );

        {
            let sweeper = self.clone();
            let sweep_shutdown = shutdown.clone();
            let sweep_pipe = pipe.clone();
            let sweep_slots = claim_slots.clone();
            // Detached: this sweeper's fate is not the caller's. The consumer
            // loop below is awaited in place, and the two end together on the
            // same shutdown signal.
            drop(cog_core::loop_health::spawn(
                reclaim_loop_name,
                cog_core::loop_health::Cadence::Periodic(CLAIM_INTERVAL),
                sweep_shutdown.clone(),
                // Rebuilt per attempt, so everything the body consumes is cloned here.
                move |beat| {
                    let sweeper = sweeper.clone();
                    let sweep_shutdown = sweep_shutdown.clone();
                    let sweep_pipe = Arc::clone(&sweep_pipe);
                    let sweep_slots = Arc::clone(&sweep_slots);
                    async move {
                        let mut ticker = tokio::time::interval(CLAIM_INTERVAL);
                        loop {
                            beat.beat();
                            tokio::select! {
                                biased;
                                _ = sweep_shutdown.wait() => break,
                                _ = ticker.tick() => {
                                    sweeper
                                        .sweep_stalled_ready(
                                            &sweep_pipe,
                                            &sweep_slots,
                                            &sweep_shutdown,
                                            CLAIM_BATCH,
                                        )
                                        .await;
                                }
                            }
                        }
                    }
                },
            ));
        }

        // Resubscribe on stream failure/end instead of exiting the spawned
        // task: a single transient read error historically ended the loop and
        // froze the ready group for days until the next pod restart. The
        // resubscription lives inside the body, so a restart resumes reading
        // the same group.
        //
        // Awaited in place rather than detached: the caller of this function
        // awaits until the loop stops, and its return is what says the stream
        // is no longer served.
        let this = self.clone();
        let _ = cog_core::loop_health::spawn(
            consumer_loop_name,
            cog_core::loop_health::Cadence::EventDriven,
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = this.clone();
                let task_backend = Arc::clone(&task_backend);
                let ready_stream = ready_stream.clone();
                let group = group.clone();
                let shutdown = shutdown.clone();
                let claim_slots = Arc::clone(&claim_slots);
                let pipe = Arc::clone(&pipe);
                async move {
                    'subscribe: loop {
                        beat.beat();
                        let mut stream = match task_backend.subscribe(&ready_stream, &group).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!("ready stream subscribe failed, retrying: {e}");
                                tokio::select! {
                                    _ = shutdown.wait() => break 'subscribe,
                                    _ = tokio::time::sleep(crate::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF) => continue 'subscribe,
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
                                        let claims = crate::observable::claim_slots_observable();
                                        // 有空位就直接拿：排队不深的干净路径上不为这一格多读一次库。
                                        let permit = match claim_slots.clone().try_acquire_owned() {
                                            Ok(permit) => permit,
                                            // 没有空位时才问一句「这条还跑不跑」。许可是在途
                                            // 执行的界；一条任务已经终态、或者行已经不在时，
                                            // 这条消息永远不会执行，它只需要被 ack 掉。让它排在
                                            // 许可队列里，就是拿唯一一个执行位置去处理一件不执行
                                            // 的事——积压越深，这类残留越多（实测队头 4000 条里
                                            // 58% 的任务已终态），真正能跑的活被它们挡在后面。
                                            // 停在积压后面等，等来的不是队头松动，而是队列自己。
                                            Err(_) => {
                                                if this
                                                    .drop_if_its_task_cannot_run(
                                                        &pipe, &msg_id, &bytes,
                                                    )
                                                    .await
                                                {
                                                    continue;
                                                }
                                                claims.wait_started(&ready_stream);
                                                let acquired = tokio::select! {
                                                    _ = shutdown.wait() => {
                                                        claims.wait_ended(&ready_stream);
                                                        break 'subscribe;
                                                    }
                                                    acquired = claim_slots.clone().acquire_owned() => acquired,
                                                };
                                                claims.wait_ended(&ready_stream);
                                                match acquired {
                                                    Ok(permit) => permit,
                                                    Err(_) => break 'subscribe,
                                                }
                                            }
                                        };
                                        let hold = claims.permit_taken(&ready_stream);
                                        this.spawn_ready_processing(
                                            pipe.clone(),
                                            msg_id,
                                            bytes,
                                            permit,
                                            hold,
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
                            _ = tokio::time::sleep(crate::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF) => {}
                        }
                    }
                }
            },
        )
        .await;

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
        hold: crate::observable::ClaimHold,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // Kept for the life of the task so the permit accounting is released
            // with the permit, including when the processing future panics.
            let hold = hold;
            let guarded =
                AssertUnwindSafe(this.process_ready_message(&pipe, msg_id, &bytes, &hold));
            if let Err(payload) = guarded.catch_unwind().await {
                tracing::warn!(
                    "ready message processing panicked: {}",
                    panic_message(&payload)
                );
            }
        });
    }

    /// 一条还没拿到执行许可的 ready 消息，如果它的任务已经不可能再跑，
    /// 就地 ack 掉并返回 true；调用方据此跳过这一条、不占许可。
    ///
    /// 「不可能再跑」只认两种看得见的结局：行已经不在，或者行是终态。
    /// 其余一律返回 false 交给正常路径——包括读不成 `Task` 的载荷（那一条
    /// 要走死信队列，判据与记账都在正常路径上）和没有编排侧的部署（说不
    /// 出结局就不替它下结论）。行还在、状态不是终态时也可能跑不成（例如
    /// 另一份副本正把它跑着），那种情形留给 `start_task` 的领域拒绝去判，
    /// 这里不猜。
    async fn drop_if_its_task_cannot_run(
        &self,
        pipe: &ReadyPipeline,
        msg_id: &str,
        bytes: &[u8],
    ) -> bool {
        let Some(ref orch) = self.orchestrator else {
            return false;
        };
        let Ok(task) = serde_json::from_slice::<Task>(bytes) else {
            return false;
        };
        let cannot_run = match orch.get_task(&task.id).await {
            None => true,
            Some(row) => matches!(
                row.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            ),
        };
        if !cannot_run {
            return false;
        }
        let ids = [msg_id.to_string()];
        if let Err(e) = pipe
            .task_backend
            .ack(&pipe.ready_stream, &pipe.group, &ids)
            .await
        {
            // ack 没落地就不算处理掉：这一条还在 PEL 里，下一轮清扫会再投一次，
            // 那时许可可能已经空出来，正常路径会走到同一个结论。
            tracing::warn!(
                task_id = %task.id,
                msg_id = %msg_id,
                "failed to ack a ready message whose task can no longer run: {e}"
            );
            return false;
        }
        tracing::debug!(
            task_id = %task.id,
            msg_id = %msg_id,
            "dropped a ready message whose task can no longer run, without taking an execution permit"
        );
        true
    }

    /// 清扫的一拍：重新认领停滞的 pending 消息并投递。
    ///
    /// 认领到的每一条都先问一句「这条还跑不跑」，跑不成的就地 ack、**不排进
    /// 执行许可的队列**。这条判据主订阅循环早就有（见
    /// [`Self::drop_if_its_task_cannot_run`]），这里却是另一条路：清扫器是唯一
    /// 一条成批认领、并且只在认领**之后**才去拿许可的路，于是 16 条里只要有 1
    /// 个空位，剩下 15 条就一个字都不做地排在许可后面——而它们绝大多数正是上
    /// 一次跑完没 ack 留下的残留（集群实测 2026-11-07：`orchestrator:ready:default`
    /// 上 lag=0、pending=245，也就是全部投递过、245 条没 ack）。执行许可是「同时
    /// 在跑的活有几条」的界，不是「一条消息要走多快」的界；拿它去排队处理一件
    /// 不执行的事，付的是所有真活多等一轮。
    async fn sweep_stalled_ready(
        &self,
        pipe: &Arc<ReadyPipeline>,
        slots: &Arc<tokio::sync::Semaphore>,
        shutdown: &ShutdownSignal,
        batch: usize,
    ) {
        let claimed = match pipe
            .task_backend
            .claim_pending(&pipe.ready_stream, &pipe.group, PENDING_IDLE_MS, batch)
            .await
        {
            Ok(Some(claimed)) if !claimed.is_empty() => claimed,
            Ok(_) => return,
            Err(e) => {
                tracing::warn!(
                    stream = %pipe.ready_stream,
                    "Pending claim sweep failed: {e}"
                );
                return;
            }
        };
        tracing::warn!(
            stream = %pipe.ready_stream,
            count = claimed.len(),
            "Claimed idle pending ready messages for re-execution"
        );
        for (msg_id, bytes) in claimed {
            if self
                .drop_if_its_task_cannot_run(pipe, &msg_id, &bytes)
                .await
            {
                continue;
            }
            // 先拿许可再派发：限制重执行并发，shutdown 时也不再起新执行；许可在
            // 处理任务内持有到执行结束。
            let permit = tokio::select! {
                _ = shutdown.wait() => break,
                acquired = slots.clone().acquire_owned() => match acquired {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };
            // 清扫器与订阅循环共用同一份许可，所以它拿到的每一条都计入在途数与
            // 持有上界：只报订阅循环那几条，会让「在途打满」恰好在许可靠清扫器
            // 维持时读不出来。
            let hold = crate::observable::claim_slots_observable().permit_taken(&pipe.ready_stream);
            self.spawn_ready_processing(pipe.clone(), msg_id, bytes, permit, hold);
        }
    }

    /// 执行一条 ready 消息的全管线：反序列化 → start_task → execute →
    /// 发布结果 → ack。主订阅循环与 pending 恢复清扫器共用。
    async fn process_ready_message(
        &self,
        pipe: &ReadyPipeline,
        msg_id: String,
        bytes: &[u8],
        hold: &crate::observable::ClaimHold,
    ) {
        let mut task: Task = match serde_json::from_slice(bytes) {
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
                // ack 之后任务还停在 Scheduled：清扫器看不见 ack，只能看见
                // 年纪，而它现在一律重投、不再扣分。所以「这条消息死了」这句
                // 话只能由这里说——**这里是唯一看得见 ack 的读者**。说得出
                // 名字就说：载荷是 publisher 从这一笔任务自己的行序列化出来
                // 的，整份读不出来就是这一笔的载荷不可编码，按普通失败路径记
                // 一笔、退避后再序列化一次，并由它自己的预算封顶。整份载荷
                // 连 JSON 都不是时才说不出名字，那种情形只记日志。
                let charged = self
                    .report_poison_ready_message(task_id_of_poison_payload(bytes), &e, &msg_id)
                    .await;
                // 上面那条路要能记账，前提是载荷里读得出一个 task_id，并且有 orchestrator
                // 接得住。两条都不成立时这条消息就谁都不欠——那正是死信队列的位置：它是
                // 唯一一个不需要领域对象就能收下这份字节的地方。能记账时不写，是因为那时
                // 损失已经落在任务行上，再写一份就是把同一件事记在两处。
                if !charged {
                    if let Err(dlq_err) = pipe
                        .task_backend
                        .dlq(
                            &pipe.ready_stream,
                            &msg_id,
                            bytes,
                            &format!("unparsable ready message: {e}"),
                        )
                        .await
                    {
                        tracing::error!(
                            msg_id = %msg_id,
                            "dead-letter write failed; this message's bytes are now lost: {dlq_err}"
                        );
                    }
                }
                return;
            }
        };

        // The publisher does not offer an owned task to a competing-consumer
        // stream, but a message put there by another deployment still reaches
        // this consumer. Running it would be the exact thing ownership exists
        // to prevent -- the owner would find the task started by whoever read
        // the queue first, and its lease would name the wrong holder. So the
        // message is refused and acked: the task row is untouched and stays
        // Pending, where the owner's own claim finds it. Leaving it unacked
        // instead would make the sweeper redeliver it every claim interval for
        // as long as the task exists.
        //
        // The stop-the-line lane is not the ordinary queue and is asked the
        // same question through [`crate::ready_queue::carries_an_owner`]: a
        // row filed there is meant for whoever is free, so a payload that
        // still names a face's unit is run rather than dropped.
        if let Some(owner) = task
            .owner_unit
            .as_deref()
            .filter(|_| crate::ready_queue::carries_an_owner(&task))
        {
            // Named per message rather than counted into one number: the
            // question this answers is "which task went where", and a bare
            // count cannot say it. Each occurrence is a publisher that has not
            // been updated, so every one of them is worth a line.
            tracing::warn!(
                task_id = %task.id,
                owner,
                msg_id = %msg_id,
                "ready message names an owner and was found on the shared stream; refusing to run it for anyone but that owner"
            );
            if let Err(e) = pipe
                .task_backend
                .ack(
                    &pipe.ready_stream,
                    &pipe.group,
                    std::slice::from_ref(&msg_id),
                )
                .await
            {
                tracing::warn!(task_id = %task.id, msg_id = %msg_id, "Failed to ack an owned ready message: {e}");
            }
            return;
        }

        // Notify orchestrator that task is now running.
        if let Some(ref orch) = self.orchestrator {
            if let Err(e) = orch.admit_and_start(&task.id).await {
                // 准入在执行它的这一处再判一次（`admit_and_start`：看门狗退回
                // Pending 的行重排一次再起，终态仍拒）——就绪消息的许可是发布
                // 时给的，而读它的人要等到队列排空才轮到这一条。
                //
                // 这一步是 Scheduled→Running 的唯一转移：它失败了而消
                // 息继续被执行并 ack，任务就永久留在 Scheduled——消息已被
                // 消费、执行也跑了，complete 却因状态非 Running 落不了库，
                // 队列读数干净，publisher 只扫 Pending、超时检查器只收
                // Running，谁也看不见它，直到 stalled-Scheduled 清扫为它再
                // 付一次重试预算。这就是 dag_task_stalled_after_ready 的根。
                //
                // 领域拒绝（任务不存在/已在跑/已终态）说明这条 ready 消息是
                // 历史残留或重复投递——执行无意义，结果也无法落库
                // （complete 要求 Running），ack 后直接丢弃，避免白跑 LLM
                // 并被清扫器反复认领。其余成因（基础设施抖动）走下面那条
                // 分支：不 ack，留给总线自己重投。
                if start_failure_is_stale(&e) {
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
                // 基础设施抖动：不执行、不 ack。消息留在 PEL，超过 idle 阈
                // 值后由清扫器重新认领并完整重走管线——重投时要么 start_task
                // 成功，要么任务已被 stalled-Scheduled 清扫收回而落入上面的
                // 领域拒绝分支。传输层的重投窗口先于调度层的 reclaim 窗口，
                // 抖动在烧重试预算之前就被总线自己补上了。
                tracing::warn!(task_id = %task.id, msg_id = %msg_id, "start_task failed transiently ({e}); leaving the ready message unacked for transport redelivery");
                return;
            }
        }

        // 这一轮要用的时限以行上那一格为准：开始之前它可能刚被抬过——上一次
        // 被自己的时限截断，行上的预算因此翻倍（判据与抬法在编排侧那一处）。
        // 手里这份是从就绪消息里反序列化来的，落在抬高之前；照它跑就是在旧的
        // 时限上再截断一次，而回改那一轮预算的读者是行——两边必须读同一个数。
        if let Some(ref orch) = self.orchestrator {
            if let Some(fresh) = orch.get_task(&task.id).await {
                task.timeout_seconds = fresh.timeout_seconds;
            }
        }

        // timeout_seconds 是任务自带的执行契约。编排侧的超时检查器只会把
        // Running 任务标记失败/重试，并不取消这里的执行 future；不在此兑
        // 现，失控的 agent 任务会无限期占住串行消费位置（实测 90+ 分钟不
        // 返回，后续 ready 消息全部队头阻塞）。超时按执行失败处理，结果
        // 走既有发布路径，重试/DLQ 由 DAG 侧决定。
        let timeout = Duration::from_secs(task.timeout_seconds.max(1));
        // 许可的持有上界随之公布：等待许可的读循环只有超过在途任务自己声明的
        // 执行时长，才说明「没有人能把它放出来」；没有这一条，安静读循环只能
        // 靠猜。
        hold.note_timeout(task.timeout_seconds.max(1));
        let result = match tokio::time::timeout(timeout, self.execute(&task)).await {
            Ok(result) => result,
            Err(_) => Err(SFError::Agent(format!(
                "{}{} seconds",
                crate::dag_executor::orchestrator::DEADLINE_CUT_PREFIX,
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

        let result_stream = cog_core::task_result_stream(&pipe.workspace_id);
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

    /// 把一条已经 ack 掉的毒载荷记到它自己那一笔任务上。
    ///
    /// 这里是「这条消息死了」的**第一读者**，也是唯一一个:调度面的清扫器只
    /// 看得见任务行有多老，看不见 ack，两条路在它眼里同形，所以它已经不再
    /// 扣分、只把任务放回队列。要有人为「消息死了」说话，只能在看得见 ack
    /// 的地方说，也就是这里。
    ///
    /// 名字取不到就不上报：没名字的追偿是又一次误伤，宁可让那一笔在调度面
    /// 多转一轮（它会到窗口后被重投，并可能自愈）。
    async fn report_poison_ready_message(
        &self,
        task_id: Option<String>,
        parse_error: &serde_json::Error,
        msg_id: &str,
    ) -> bool {
        let Some(task_id) = task_id else {
            tracing::warn!(
                msg_id = %msg_id,
                "a ready message could not be deserialized and its payload does not name a \
                 task; dropped with no one to charge"
            );
            return false;
        };
        let Some(ref orch) = self.orchestrator else {
            tracing::warn!(
                task_id = %task_id, msg_id = %msg_id,
                "no orchestrator to record the undecodable ready message against; the task \
                 will be re-armed when it stalls"
            );
            return false;
        };
        let error = format!(
            "undecodable ready message: the message carrying this task was acknowledged and \
             dropped instead of being run, because its payload could not be deserialized \
             ({parse_error})"
        );
        match orch.fail_task(&task_id, error, None).await {
            Ok((retried, cancelled, _)) => {
                tracing::warn!(
                    task_id = %task_id, msg_id = %msg_id, retried, cancelled = cancelled.len(),
                    "recorded an undecodable ready message against the task whose payload it was"
                );
                true
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id, msg_id = %msg_id,
                    "cannot record the undecodable ready message against its task: {e}"
                );
                false
            }
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

/// start_task 失败的处置边界：领域拒绝（任务不存在/已在跑/已终态）说明这
/// 条 ready 消息是残留，ack 丢弃；其余都是基础设施抖动，消息必须留在
/// PEL 交给清扫器重投，绝不能在 DAG 不知情的情况下继续执行。
fn start_failure_is_stale(e: &SFError) -> bool {
    matches!(e, cog_core::SFError::TaskFailed { .. })
}

/// 从一份反序列化失败的就绪载荷里尽量取出任务 id。
///
/// 读不成 `Task` 不等于这份载荷不是 JSON：字段增减、两个版本错配都会让它走不
/// 进类型，而外层的对象和 `id` 键还在。所以退一步按 `Value` 读一次，取不到就
/// 承认取不到——不猜、不哈希、不拿别的东西顶替名字。
fn task_id_of_poison_payload(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value.get("id")?.as_str().map(str::to_string)
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
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(None)
        }

        async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
            self.published
                .lock()
                .await
                .push((subject.to_string(), payload.to_vec()));
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {
            // 这个夹具没有长轮询：它的流要么立刻结束，要么永久 pending，
            // 没有一条已发出的读需要等它返回，所以没有可停的东西。
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
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            Ok(Some(Vec::new()))
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        /// These doubles carry the messages the router's own code hands over
        /// and keep no per-group position, so they cannot say what a group has
        /// not been handed.
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
        async fn dlq(
            &self,
            _stream: &str,
            _msg_id: &str,
            _payload: &[u8],
            _reason: &str,
        ) -> SFResult<()> {
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

    /// The permit accounting a processing task carries. The tests call the
    /// pipeline directly rather than through a read loop, so the guard is one
    /// nobody registered a stream for; it still has to be passed, because the
    /// timeout it carries is what the claim-stall rule compares a wait against.
    fn test_hold() -> crate::observable::ClaimHold {
        let observer = Arc::new(crate::observable::ClaimSlotsObservable::new());
        observer.permit_taken("test-ready")
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
                &test_hold(),
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
                &test_hold(),
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
            .process_ready_message(&pipe, "m-poison".into(), b"not-json", &test_hold())
            .await;

        assert!(backend.published.lock().await.is_empty());
        let acks = backend.acks.lock().await;
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].2, vec!["m-poison".to_string()]);
    }

    /// 整份载荷读不进 `Task` 不等于它没有名字：字段类型随版本变、字段增减，
    /// 外层对象与 `id` 都还在。名字取得回来，这次投递的死才有主体可追；取不
    /// 回来就不追——没名字的追偿是又一次误伤。
    #[test]
    fn an_undecodable_payload_still_names_its_task() {
        let task = test_task(60);
        let mut drifted = serde_json::to_value(&task).unwrap();
        // 同一个字段换了类型：这一份一定读不进 `Task`，而 `id` 毫发无伤。
        drifted["timeout_seconds"] = serde_json::json!("sixty");
        let bytes = serde_json::to_vec(&drifted).unwrap();
        assert!(
            serde_json::from_slice::<Task>(&bytes).is_err(),
            "样例必须读不进 `Task`，否则这条测试什么都没测"
        );
        assert_eq!(
            task_id_of_poison_payload(&bytes).as_deref(),
            Some(task.id.as_str())
        );

        // 名字真的取不到时才承认取不到：不猜、不哈希、不拿别的东西顶替。
        assert_eq!(task_id_of_poison_payload(b"not-json"), None);
        assert_eq!(task_id_of_poison_payload(b"{\"other\":1}"), None);
    }

    #[tokio::test]
    async fn an_owned_ready_message_is_refused_and_acked() {
        // 发布侧不再把带归属的任务放进共享流，但另一代部署仍在跑旧代码时
        // 这种消息还是会到消费者手里。执行它等于让任意读者替所有者开工，
        // 所以这里拒执行；不 ack 会让清扫器按认领间隔一直重投同一条。
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(SlowExecutor {
                sleep_secs: 0,
                panic_instead: false,
            }))
            .await;

        let mut owned = test_task(60);
        owned.owner_unit = Some("8b".into());
        router
            .process_ready_message(
                &pipe,
                "m-owned".into(),
                &serde_json::to_vec(&owned).unwrap(),
                &test_hold(),
            )
            .await;

        assert!(
            backend.published.lock().await.is_empty(),
            "an owned task must not run here, so nothing may be published for it"
        );
        let acks = backend.acks.lock().await;
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].2, vec!["m-owned".to_string()]);
        drop(acks);

        // 同一份载荷去掉归属就必须照常执行：没有这一半，一个什么都不做的
        // 路由也能让上面的断言变绿。
        router
            .process_ready_message(
                &pipe,
                "m-free".into(),
                &serde_json::to_vec(&test_task(60)).unwrap(),
                &test_hold(),
            )
            .await;
        assert_eq!(backend.published.lock().await.len(), 1);
    }

    /// 停线通道上的归属不是拒绝执行的理由。
    ///
    /// 停线单是给「谁空谁跑」的，派工那一侧（[`crate::ready_queue::carries_an_owner`]）
    /// 已经不拿归属拦它；消费者如果仍按共享队列的规矩把这条 ack 掉，投递会被丢在
    /// 最后一格，读起来和执行失败是一回事。
    #[tokio::test]
    async fn an_owned_stop_the_line_message_runs() {
        let backend = Arc::new(ScriptedBackend::default());
        let pipe = test_pipe(backend.clone());
        let router = TaskExecutorRouter::new()
            .with_executor(Arc::new(SlowExecutor {
                sleep_secs: 0,
                panic_instead: false,
            }))
            .await;

        let mut lane = test_task(60);
        lane.owner_unit = Some("f5".into());
        lane.input[cog_core::INPUT_READY_LANE] =
            serde_json::Value::String(cog_core::READY_LANE_STOP_THE_LINE.into());
        router
            .process_ready_message(
                &pipe,
                "m-lane".into(),
                &serde_json::to_vec(&lane).unwrap(),
                &test_hold(),
            )
            .await;

        assert_eq!(
            backend.published.lock().await.len(),
            1,
            "the lane's row has to run wherever it lands: nobody else will retry it \
             once this message is acked away"
        );
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
                &test_hold(),
            )
            .await;

        let published = backend.published.lock().await;
        assert_eq!(published.len(), 1);
        let msg: DagMessage = serde_json::from_slice(&published[0].1).unwrap();
        assert!(matches!(msg, DagMessage::TaskComplete { .. }));
        let acks = backend.acks.lock().await;
        assert_eq!(acks[0].2, vec!["m-ok".to_string()]);
    }

    #[test]
    fn only_a_dag_rejection_is_stale() {
        // 回归：start_task 的非领域失败曾被「继续执行」处理——消息随后被
        // ack，任务留在 Scheduled，直到 stalled-Scheduled 清扫为它再付一次
        // 重试预算（dag_task_stalled_after_ready）。只有领域拒绝才算残留可
        // 丢弃；基础设施抖动必须留给传输层重投。
        //
        // 两条断言互为对照：只断言「非领域不是残留」时，一个恒返回 false 的
        // 实现也能过，而那样上面那条 ack 丢弃的路径永远不可达。
        assert!(start_failure_is_stale(&SFError::TaskFailed {
            task_id: "t-1".into(),
            reason: "already running".into(),
        }));
        assert!(!start_failure_is_stale(&SFError::Agent(
            "state backend blip".into()
        )));
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
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(None)
        }

        async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
            self.published
                .lock()
                .await
                .push((subject.to_string(), payload.to_vec()));
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {
            // 这个夹具没有长轮询：它的流要么立刻结束，要么永久 pending，
            // 没有一条已发出的读需要等它返回，所以没有可停的东西。
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
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            Ok(Some(Vec::new()))
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        /// These doubles carry the messages the router's own code hands over
        /// and keep no per-group position, so they cannot say what a group has
        /// not been handed.
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
        async fn dlq(
            &self,
            _stream: &str,
            _msg_id: &str,
            _payload: &[u8],
            _reason: &str,
        ) -> SFResult<()> {
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
            &test_hold(),
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

/// 执行许可是「同时在跑的活有几条」的界，不是「一条消息要走多快」的界：一条
/// 任务已经终态时它永远不会被执行，那条消息只需要被 ack 掉。让这种消息排在
/// 许可队列里，唯一的作用是让真正能跑的活多等一轮——积压越深，死消息占比越
/// 高，等得越久。
#[cfg(test)]
mod ready_permit_fast_lane_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::MessageStream;
    use std::time::Instant;

    /// 一条就绪流上放着预置的几条消息，读完之后保持打开。记下被 ack 的 id。
    ///
    /// `stalled` 是清扫器那条路（`claim_pending`）看到的停滞消息：它一次只发
    /// 一次，发完就空——重复发会让「谁把它处理掉了」读不出来。
    struct OneShotBackend {
        messages: tokio::sync::Mutex<Vec<(String, Vec<u8>)>>,
        stalled: tokio::sync::Mutex<Vec<(String, Vec<u8>)>>,
        acks: tokio::sync::Mutex<Vec<String>>,
    }

    impl OneShotBackend {
        fn new(messages: Vec<(String, Vec<u8>)>) -> Self {
            Self {
                messages: tokio::sync::Mutex::new(messages),
                stalled: tokio::sync::Mutex::new(Vec::new()),
                acks: tokio::sync::Mutex::new(Vec::new()),
            }
        }

        /// 让清扫器在下一拍认领到这几条停滞消息。
        fn with_stalled(mut self, stalled: Vec<(String, Vec<u8>)>) -> Self {
            self.stalled = tokio::sync::Mutex::new(stalled);
            self
        }
    }

    #[async_trait]
    impl MessageBackend for OneShotBackend {
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(None)
        }
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {}
        async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
            let items: Vec<(String, Vec<u8>)> = self.messages.lock().await.drain(..).collect();
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
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            let stalled: Vec<(String, Vec<u8>)> = self.stalled.lock().await.drain(..).collect();
            Ok(Some(stalled))
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
        async fn dlq(
            &self,
            _stream: &str,
            _msg_id: &str,
            _payload: &[u8],
            _reason: &str,
        ) -> SFResult<()> {
            Ok(())
        }
    }

    /// 一张只有这一行的图：`get_task` 永远回答它。
    struct OneRowGraph {
        task: Task,
    }

    #[async_trait]
    impl OrchestratorControl for OneRowGraph {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(&self, _goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
            Ok(tasks.iter().map(|task| task.id.clone()).collect())
        }
        async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn add_task(&self, _task: Task) -> SFResult<()> {
            Ok(())
        }
        async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            Vec::new()
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            Vec::new()
        }
        async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
            Ok(false)
        }
        async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn dlq_len(&self) -> SFResult<usize> {
            Ok(0)
        }
        async fn start_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn complete_task(
            &self,
            _task_id: &str,
            _result: serde_json::Value,
        ) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn fail_task(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::contract::llm::UpstreamFailure>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            Ok((false, Vec::new(), false))
        }
        async fn fail_task_after(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::contract::llm::UpstreamFailure>,
            _retry_after_secs: Option<u64>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            Ok((false, Vec::new(), false))
        }
        async fn cancel_task(&self, _task_id: &str) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn get_task(&self, _task_id: &str) -> Option<Task> {
            Some(self.task.clone())
        }
        async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            Vec::new()
        }
        async fn get_dependents(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (Vec::new(), Vec::new())
        }
        async fn delete_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn all_completed(&self) -> bool {
            true
        }
        async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
            Ok(false)
        }
    }

    fn task_in(status: &str) -> Task {
        let now = chrono::Utc::now();
        serde_json::from_value(serde_json::json!({
            "id": "t-fast-lane",
            "task_type": {"custom": "slow"},
            "status": status,
            "input": {},
            "blocked_by": [],
            "blocks": [],
            "priority": 1,
            "created_at": now,
            "updated_at": now,
            "retry_count": 0,
            "max_retries": 3,
            "timeout_seconds": 5,
            "is_executable": true
        }))
        .unwrap()
    }

    /// 把一条消息喂给读循环，许可为零（也就是「有一个位置，但它已经名花有主」）。
    fn run_with_no_permit(
        router: TaskExecutorRouter,
        backend: Arc<OneShotBackend>,
        shutdown: ShutdownSignal,
    ) -> tokio::task::JoinHandle<SFResult<()>> {
        tokio::spawn(async move {
            let claim = ClaimBudget {
                slots: Arc::new(tokio::sync::Semaphore::new(0)),
                limit: 0,
                source: crate::observable::SOURCE_OVERRIDE,
            };
            router
                .run_one_ready_stream(
                    backend.clone() as Arc<dyn MessageBackend>,
                    backend as Arc<dyn MessageBackend>,
                    "ready".to_string(),
                    "ws".to_string(),
                    claim,
                    shutdown,
                )
                .await
        })
    }

    #[tokio::test]
    async fn a_message_whose_task_is_already_terminal_leaves_without_a_permit() {
        let payload = serde_json::to_vec(&task_in("cancelled")).unwrap();
        let backend = Arc::new(OneShotBackend::new(vec![("m-dead".into(), payload)]));
        let router = TaskExecutorRouter::new().with_orchestrator(Arc::new(OneRowGraph {
            task: task_in("cancelled"),
        }));

        let shutdown = ShutdownSignal::new();
        let runner = run_with_no_permit(router, backend.clone(), shutdown.clone());

        let started = Instant::now();
        loop {
            if !backend.acks.lock().await.is_empty() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "a message for a cancelled task waited on the execution permit instead of leaving"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(backend.acks.lock().await.as_slice(), ["m-dead".to_string()]);

        shutdown.trigger();
        runner.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_message_whose_task_can_still_run_waits_for_the_permit() {
        // 反面对照：这条判据只管「跑不成」的消息。任务还在排队时，它就该排在
        // 许可后面等——先 ack 再等等于把活丢了。
        let payload = serde_json::to_vec(&task_in("scheduled")).unwrap();
        let backend = Arc::new(OneShotBackend::new(vec![("m-live".into(), payload)]));
        let router = TaskExecutorRouter::new().with_orchestrator(Arc::new(OneRowGraph {
            task: task_in("scheduled"),
        }));

        let shutdown = ShutdownSignal::new();
        let runner = run_with_no_permit(router, backend.clone(), shutdown.clone());

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            backend.acks.lock().await.is_empty(),
            "a runnable task's message was discarded while the permit was merely busy"
        );

        shutdown.trigger();
        runner.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_stalled_message_whose_task_is_already_terminal_leaves_without_a_permit() {
        // 清扫器是唯一一条成批认领、并且只在认领之后才去拿许可的路。停滞的
        // 消息里多数是上一次跑完没 ack 的残留，它们的任务往往早就终态了；让
        // 这些消息排在执行许可后面，付的是所有真活多等一轮。许可为零（一个
        // 空位都没有）时它们仍然必须被 ack 掉，这就是判据。
        let payload = serde_json::to_vec(&task_in("cancelled")).unwrap();
        let backend = Arc::new(
            OneShotBackend::new(Vec::new()).with_stalled(vec![("m-stalled-dead".into(), payload)]),
        );
        let router = TaskExecutorRouter::new().with_orchestrator(Arc::new(OneRowGraph {
            task: task_in("cancelled"),
        }));

        let shutdown = ShutdownSignal::new();
        let runner = run_with_no_permit(router, backend.clone(), shutdown.clone());

        let started = Instant::now();
        loop {
            if backend
                .acks
                .lock()
                .await
                .iter()
                .any(|id| id == "m-stalled-dead")
            {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "清扫器把一条已终态任务的消息排在执行许可后面，而它永远不会被执行"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        shutdown.trigger();
        runner.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_stalled_message_whose_task_can_still_run_waits_for_the_permit() {
        // 反面对照：这条判据只管「跑不成」的消息。任务还排得上时，清扫器不
        // 许先 ack 再等——那等于把活丢了。
        let payload = serde_json::to_vec(&task_in("scheduled")).unwrap();
        let backend = Arc::new(
            OneShotBackend::new(Vec::new()).with_stalled(vec![("m-stalled-live".into(), payload)]),
        );
        let router = TaskExecutorRouter::new().with_orchestrator(Arc::new(OneRowGraph {
            task: task_in("scheduled"),
        }));

        let shutdown = ShutdownSignal::new();
        let runner = run_with_no_permit(router, backend.clone(), shutdown.clone());

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            backend.acks.lock().await.is_empty(),
            "清扫器把一条还跑得成的任务的消息在许可忙的时候丢掉了"
        );

        shutdown.trigger();
        runner.await.unwrap().unwrap();
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
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(None)
        }

        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {
            // 这个夹具没有长轮询：它的流要么立刻结束，要么永久 pending，
            // 没有一条已发出的读需要等它返回，所以没有可停的东西。
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
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            Ok(Some(Vec::new()))
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }
        /// These doubles carry the messages the router's own code hands over
        /// and keep no per-group position, so they cannot say what a group has
        /// not been handed.
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
        async fn dlq(
            &self,
            _stream: &str,
            _msg_id: &str,
            _payload: &[u8],
            _reason: &str,
        ) -> SFResult<()> {
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

    /// 普通活占满许可时，进程本地那条通道照样要读。
    ///
    /// 两条通道共用一份许可，而许可只在任务跑完时归还；于是普通活只要同时占满
    /// 那一份，本地通道的订阅循环就停在 `acquire_owned` 上，一条消息都不再读。
    /// 2026-11-06 20:5x 线上就是这个样子：许可证 2 个，`default` 与
    /// `default:local` 各等了 1,674 与 1,771 秒，`local` 的 `entries-read` 停在
    /// 一天前、`lag` 涨到 1,374，发布侧按「这条队列还有没被读走的条目」把新任务
    /// 挡在外面——自演化那一路在队列两端一起停住。
    ///
    /// 这条用例把普通通道塞满（每一项都不返回），再看本地那一条有没有把消息读完。
    /// 本地通道没有自己的一份许可时，这里永远等不到 ack。
    struct Blocker;

    #[async_trait]
    impl TaskExecutor for Blocker {
        fn supports(&self, _task_type: &TaskType) -> bool {
            true
        }
        async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
            // 普通活永不返回：占住许可，模拟一条跑了很久的活。自演化那一条照常
            // 答完——这一条用例判的是「本地通道还能不能读到消息」，不是它跑多久。
            if task.task_type.kind_key() == "unit_of_work" {
                std::future::pending::<()>().await;
            }
            Ok(TaskResult {
                success: true,
                output: serde_json::json!({"done": true}),
                metadata: TaskResultMetadata::new("accept"),
            })
        }
    }

    #[tokio::test]
    async fn a_full_ordinary_pool_still_lets_the_process_local_lane_read() {
        let shared = crate::ready_queue::ready_stream("ws");
        let local = crate::ready_queue::process_local_ready_stream("ws");
        let backend = Arc::new(PerStreamBackend::default());
        // 比共享许可多塞几条，保证那一份被占满、订阅循环停在许可上。
        for i in 0..(DEFAULT_CLAIM_CONCURRENCY + 2) {
            backend
                .queue(&shared, &format!("m-ordinary-{i}"), ordinary_task())
                .await;
        }
        backend.queue(&local, "m-generate", generation_task()).await;

        let router = Arc::new(
            TaskExecutorRouter::new()
                .with_executor(Arc::new(Blocker))
                .await,
        );
        let shutdown = ShutdownSignal::new();
        let runner = {
            let router = router.clone();
            let backend = backend.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                router
                    .run_consumer(backend.clone(), backend, "ws", true, shutdown)
                    .await
            })
        };

        settle(&backend, 1).await;
        assert_eq!(
            backend.acks.lock().await.clone(),
            vec!["m-generate".to_string()],
            "普通许可占满时本地通道没能读到自己的消息——它和普通活共用一份许可，\
             而那一份永远不会在普通活跑完之前归还"
        );
        drop(runner);
        shutdown.trigger();
    }
}

#[cfg(test)]
mod claim_concurrency_tests {
    use super::*;

    /// 配了多少就是多少：这个数是「同时花额度的任务条数」，运维写下的那个数
    /// 必须原样生效，多一条少一条都会改变窗口被切碎的方式。
    #[test]
    fn a_configured_limit_is_the_number_of_tasks_in_flight() {
        assert_eq!(parse_claim_concurrency("1").unwrap(), 1);
        assert_eq!(parse_claim_concurrency(" 12 ").unwrap(), 12);
    }

    /// `0` 被拒：它读作「不跑任何执行」，而写它的人最可能想说的是「不限」。
    /// 两种误读都会把额度花光的形状藏起来，所以它既不生效也不静默换成默认值。
    #[test]
    fn zero_is_refused_because_it_would_stop_every_execution() {
        let problem = parse_claim_concurrency("0").expect_err("0 must not be a limit");
        assert!(
            problem.contains("stop every execution"),
            "the refusal must name what 0 does: {problem}"
        );
    }

    /// 不是数的东西同样被拒，而且判词带上原文——运维要能在日志里认出是哪一行
    /// 配置没生效。
    #[test]
    fn a_word_is_not_a_count_of_tasks() {
        let problem = parse_claim_concurrency("four").expect_err("a word is not a count");
        assert!(problem.contains("four"), "{problem}");
    }

    /// 默认值本身必须是一个能用的数：`0` 作为兜底会把「没配」变成「全停」。
    ///
    /// 两条都是常量之间的关系，所以在编译期判：写错的那一天编译不过，而不是等到
    /// 有人把测试跑起来。
    #[test]
    fn the_default_is_a_usable_limit() {
        const {
            assert!(DEFAULT_CLAIM_CONCURRENCY > 0);
            assert!(DEFAULT_CLAIM_CONCURRENCY < CLAIM_BATCH);
            assert!(DEFAULT_LOCAL_CLAIM_CONCURRENCY > 0);
            assert!(DEFAULT_LOCAL_CLAIM_CONCURRENCY < CLAIM_BATCH);
        }
    }

    /// 本地车道的预算来自它自己的变量，不是普通活那一格。
    ///
    /// 两条车道必须是两个数：本地车道是每一条变更生成任务的必经之路，它的许可数
    /// 就是整个平台的变更吞吐；普通活那一格调大时如果连带把本地车道也调大，运维
    /// 以为自己在调「同时跑几件普通活」，实际动了吞吐与 pod 内存两件事。2026-11-07
    /// 05:2x 实测本地车道 1 个许可后面压着 1,904 条任务、`claim_wait_seconds` 卡在
    /// 1,793 秒，就是这个数在起作用。
    #[test]
    fn the_local_lane_budget_is_its_own_number() {
        // This variable is read by `local_claim_concurrency()` alone and no other
        // test in this binary touches either variable, so the process-wide
        // environment is not shared state here.
        std::env::set_var(LOCAL_CLAIM_CONCURRENCY_ENV, "3");
        std::env::set_var(CLAIM_CONCURRENCY_ENV, "7");
        assert_eq!(
            local_claim_concurrency(),
            Some(3),
            "the local lane reads its own variable, not the ordinary one"
        );
        assert_eq!(
            claim_concurrency(),
            7,
            "and the ordinary lane is not moved by the local one"
        );

        std::env::remove_var(LOCAL_CLAIM_CONCURRENCY_ENV);
        assert_eq!(
            local_claim_concurrency(),
            None,
            "unset means derive it from the readings, not freeze the number seen on the day it was written"
        );

        std::env::remove_var(CLAIM_CONCURRENCY_ENV);
    }

    /// `0` 不是「一条也不跑」，与平台其余几个界同一个口径：0 = 按读数导出。
    ///
    /// 这条判据改自 2026-11-07：那一天之前 0 被当成非法值退回默认，理由是
    /// 「它会把吞吐写成零」。现在 0 有第三个含义，而它仍然是「不停摆」——
    /// 导出量不出价格时给的是 1（见 `lane_memory::derive_ceiling`），不是 0。
    #[test]
    fn a_zero_local_budget_asks_for_the_derived_bound_rather_than_stopping() {
        std::env::set_var(LOCAL_CLAIM_CONCURRENCY_ENV, "0");
        assert_eq!(
            local_claim_concurrency(),
            None,
            "0 asks for the derived bound; a stop would be the one reading it must never have"
        );
        std::env::remove_var(LOCAL_CLAIM_CONCURRENCY_ENV);
    }
}
