use crate::contract::llm::UpstreamFailure;
use crate::{SFError, SFResult, Task, TaskPlan, TaskStatus};
use async_trait::async_trait;

/// Abstract interface for orchestrating tasks without hard-coupling to
/// `sf-orchestrator::DagExecutor`.
/// Intended for use by `cog-supervisor` and any other crate that needs to
/// read or mutate the task graph but must not depend on `sf-orchestrator`.
/// All methods take `&self` so that callers can hold a plain
/// `Arc<dyn OrchestratorControl>`; the implementation is responsible for
/// internal synchronisation.
#[async_trait]
pub trait OrchestratorControl: Send + Sync {
    /// Submit a new goal with explicit pre-defined tasks.
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()>;

    /// Submit a goal with optional tasks. If `tasks` is empty, the orchestrator
    /// will automatically decompose the goal into atomic tasks via ActionPlanner.
    ///
    /// 空 `tasks` 是一条合法的提交形态（接口层的裸 goal、聊天入口的可执行意图都这样
    /// 提交）：分解侧会为这道目标补出一条承载它的行（`Task::goal_row`），子任务挂在
    /// 它下面、唯一收口任务的声明与判退都落在它身上。调用方自己带了行时用它自己的
    /// 那一条，id 就是客户端拿到的 id。
    ///
    /// Returns the ids this call actually added to the graph. A submission
    /// whose id the graph already holds is an idempotent no-op — the intent is
    /// in hand — so it is not among them. Callers that need to distinguish
    /// "queued just now" from "already held" must read this back rather than
    /// assume their input ids were accepted: a held id may name a finished
    /// task, in which case nothing is running and the returned set is empty.
    async fn submit_goal_auto(&self, goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>>;

    /// Assign a pending/scheduled task to a specific agent.
    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()>;

    /// Add a single task to the existing graph.
    async fn add_task(&self, task: Task) -> SFResult<()>;

    /// 用一条改过的行替换图里已有的那一条。
    ///
    /// 范围结构（父节点）、验收标准、归属都是行的字段，改它们不是状态迁移，
    /// 走不了 [`Self::schedule_task`] 那一族。删了重加也不行：删会把依赖边
    /// 一起剪掉，重加拿不到原来那条的边与状态。
    ///
    /// 默认实现拒绝：没有一份可以就地改的图时，说出来比静默丢掉改动安全。
    async fn replace_task(&self, _task: Task) -> SFResult<()> {
        Err(SFError::NotImplemented(
            "replace_task: this orchestrator has no writable graph, so an existing task \
             cannot be changed in place"
                .into(),
        ))
    }

    /// 写一条计划基线。
    ///
    /// 与 [`Self::task_plans`] 成对：读面回答「当初打算怎么做」，这一条把打算
    /// 写进去。没有写面时基线只能由分解侧自己产出，人改不动计划——甘特图就成了
    /// 一张事后记录，而不是一张可以拿来对齐的工具。
    async fn save_task_plan(&self, _plan: &TaskPlan) -> SFResult<()> {
        Err(SFError::NotImplemented(
            "save_task_plan: this orchestrator holds no plan store, so a baseline cannot \
             be written here"
                .into(),
        ))
    }

    /// Returns true if any task in the set is still retryable.
    async fn crew_can_retry(&self, task_ids: &[String]) -> bool;

    /// Retry all failed tasks in the given set. Returns the number retried.
    async fn crew_retry_all(&self, task_ids: &[String]) -> usize;

    /// Return all tasks whose dependencies are satisfied and are ready to run.
    async fn get_ready_tasks(&self) -> Vec<Task>;

    /// Return every task currently in the graph.
    async fn get_all_tasks(&self) -> Vec<Task>;

    /// Return every task currently in the graph, and report a read that failed.
    ///
    /// [`Self::get_all_tasks`] answers an empty list when the graph cannot be
    /// read, so a reading fed by it prints "0 tasks" for a store that is down.
    /// That is the one number a reader cannot question: zero work and an
    /// unreadable graph are different situations, and the surface built on
    /// this one shows the second as `unread`.
    ///
    /// The default folds [`Self::get_all_tasks`], which is honest for an
    /// orchestrator holding its tasks in memory -- it has no store to fail at.
    /// An orchestrator backed by one has to override this; until it does, the
    /// fold is the same silent zero as before.
    async fn get_all_tasks_checked(&self) -> SFResult<Vec<Task>> {
        Ok(self.get_all_tasks().await)
    }

    /// Return the tasks that carry a venue label, and only those.
    ///
    /// The venue board answers three numbers per venue by folding
    /// [`Task::venue`], and every row without a label is read only to be
    /// dropped. A workspace that has been running for a while is almost
    /// entirely such rows -- 实测 2026-11-06: 9,987 of 9,991 rows carried no
    /// label, and the board that reads them shows four tasks. The default
    /// filters [`Self::get_all_tasks`],
    /// which answers correctly and reads everything; an orchestrator backed by
    /// a store that can exclude by label should override this.
    ///
    /// Terminal rows are part of the answer: the board's "latest verdict" and
    /// "written back" columns are read from tasks that are already done, so a
    /// reader that reused [`crate::TaskStatus::affects_dispatch`] here would
    /// empty both columns and still look plausible.
    async fn get_venue_tasks(&self) -> Vec<Task> {
        self.get_all_tasks()
            .await
            .into_iter()
            .filter(|task| task.venue_label().is_some())
            .collect()
    }

    /// 这个 workspace 里每条任务的计划基线。
    ///
    /// 与 [`Self::get_all_tasks`] 成对存在：任务图回答「现在是什么样」，基线回答
    /// 「当初打算怎么做」，读面要把两者并排才画得出甘特。返回 `Err` 与返回空集合
    /// 不是一回事——前者说这个部署读不到基线，后者说这些任务都没排过期，而一个
    /// 把前者读成后者的读面会把整屏画成「未排期」。
    async fn task_plans(&self) -> SFResult<Vec<TaskPlan>> {
        Err(SFError::NotImplemented("task_plans".into()))
    }

    /// Push a failed task to the dead-letter queue.
    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool>;

    /// Retry a single failed task.
    async fn retry_task(&self, task_id: &str) -> SFResult<()>;

    /// Return the number of items in the dead-letter queue.
    async fn dlq_len(&self) -> SFResult<usize>;

    /// Mark a scheduled task as running.
    async fn start_task(&self, task_id: &str) -> SFResult<()>;

    /// Start the task a ready message named, re-admitting it if it has been
    /// handed back to `Pending` since that message was published.
    ///
    /// A ready message authorizes execution when it is *published*, and the
    /// consumer runs the task *after* reading it -- at the queue depths this
    /// deployment admits, long after. The stall sweep returns a row that sat in
    /// `Scheduled` past its window to `Pending`, so a message older than that
    /// window arrives naming a `Pending` row, and [`Self::start_task`] — which
    /// only accepts `Scheduled` — refuses it. The reader then treats the
    /// refusal as a stale copy and acks the message away.
    ///
    /// Handing the row back was right (it had waited too long); reading it as
    /// "this message is rubbish" is not: the task is still ready, its turn has
    /// come, and the message is the evidence. Measured on 2026-11-06: 144 of
    /// the 145 ready messages read in ten minutes were refused this way and
    /// dropped, while three claim slots sat idle and 291 ready tasks waited in
    /// that same stream to be re-queued and dropped in turn.
    ///
    /// So admission is decided once more where it is used: a `Pending` row is
    /// scheduled again -- the sweep's own step in reverse, atomic against a
    /// concurrent claim -- and then started. Every other state still refuses
    /// through [`Self::start_task`], which is what keeps a stale copy of a
    /// finished task from being run a second time.
    async fn admit_and_start(&self, task_id: &str) -> SFResult<()> {
        if self
            .get_task(task_id)
            .await
            .is_some_and(|t| t.status == TaskStatus::Pending)
        {
            // Failing to schedule is not the error to report here: another
            // reader may have claimed the row a moment ago, and the start below
            // then names the real reason (already running, already terminal).
            let _ = self.schedule_task(task_id).await;
        }
        self.start_task(task_id).await
    }

    /// Mark a running task as completed.
    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>>;

    /// Mark a running task as failed and drive retry / cancellation logic.
    /// Returns `(retried, cancelled_ids, dlq_pushed)`.
    ///
    /// `error` 是给人读的文本，`cause` 是它的类型（传输层给过信号时才有）。
    /// 两个一起传：判定读类型，日志读文本，不让下游回头去解析措辞。
    async fn fail_task(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)>;

    /// [`Self::fail_task`] for a failure whose upstream named the wait it wants
    /// before the next attempt.
    ///
    /// 单独一个名字而不是多一个 `None` 参数：能说出这个时长的调用点很少，把
    /// 它写在名字上，"这次重试有上游给的时刻"就在调用处看得见，而不是淹没在
    /// 一串同样传 `None` 的调用里。
    async fn fail_task_after(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)>;

    /// Cancel a task and return cascaded cancellations.
    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>>;

    /// [`Self::cancel_task`] with a reason that says why, not just that.
    ///
    /// A cancelled row keeps no other trace: it never ran, so there is no
    /// result to read and no failure to explain it, and "someone cancelled it"
    /// is what a reader of the graph is left with. A sweep that knows more --
    /// the alert this work was filed for is no longer firing, the predecessor
    /// it waited on can never settle -- has to be able to say so on the row,
    /// because the row is the only place the answer survives the round.
    ///
    /// The default drops the reason rather than refusing the call: an
    /// orchestrator that cannot record one still has to be able to cancel, and
    /// a test double that ignores it is not a caller that has to be fixed.
    async fn cancel_task_with_reason(
        &self,
        task_id: &str,
        _reason: String,
    ) -> SFResult<Vec<String>> {
        self.cancel_task(task_id).await
    }

    /// Look up a single task by id.
    async fn get_task(&self, task_id: &str) -> Option<Task>;

    /// Schedule a pending task (move to scheduled state).
    async fn schedule_task(&self, task_id: &str) -> SFResult<()>;

    /// 接活：把一条待接的行交给 `claimant`，写负责人与租约，原子地。
    ///
    /// 与 [`Self::schedule_task`] 答的不是一件事：那个把一条行挪进「已派发」，
    /// 不记谁接了；这个记下责任人，并且只让一个人接走。派工侧在跨过队列边界时
    /// 用它，员工侧（带外 worker、以后回合里的举手）用同一个。
    ///
    /// 默认实现是「做不到」，不是「先排上」：一个接不了活的后端必须让调用方看见
    /// 它没接上，否则调用方会以为责任已经有人担了。
    async fn claim_task(&self, task_id: &str, _claimant: &str) -> SFResult<Task> {
        let _ = task_id;
        Err(SFError::NotImplemented("claim_task".into()))
    }

    /// Check for timed-out tasks and return their ids + side-effects.
    /// Tuple is `(task_id, retried, cancelled_ids, dlq_pushed)`.
    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)>;

    /// Return direct dependents of a task.
    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Return direct dependencies of a task.
    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Return the full task graph as (nodes, edges).
    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>);

    /// Delete a task from the graph.
    async fn delete_task(&self, task_id: &str) -> SFResult<()>;

    /// Return true when every task in the graph is completed.
    async fn all_completed(&self) -> bool;

    /// Replay a task from the DLQ if present. Returns true when replayed.
    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool>;

    /// Every task id a change could have been worked under, or `None` when this
    /// control plane does not dispatch changes at all.
    ///
    /// Here rather than in the crate that owns the rule because a reader reaches
    /// every other crate through this one: a read face that recomputed the id
    /// from the identity would be a second copy of a hash that decides which
    /// rows belong to which change, and the two copies would keep agreeing
    /// right up until the day one of them was edited.
    ///
    /// The default is `None`, not an empty vec: an empty set is the answer "this
    /// change has no handovers", which a cost read turns into a confident zero.
    /// `None` says the question cannot be answered by this implementation, and a
    /// caller is expected to report the figure as unmeasurable instead.
    fn change_dispatch_ids(&self, _identity: &crate::ChangeIdentity) -> Option<Vec<String>> {
        None
    }
}

/// Core DAG execution engine interface.
/// Decouples the orchestrator control layer from concrete DAG implementations
/// so that alternative execution backends (distributed, external workflow
/// engines, etc.) can be plugged in without changing control-plane code.
#[async_trait]
pub trait DagExecutor: Send + Sync {
    /// Submit a goal by injecting tasks into the DAG.
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()>;

    /// Batch add tasks. Returns the IDs of tasks that were actually inserted
    /// (duplicates are idempotently skipped).
    async fn add_tasks_batch(&self, tasks: Vec<Task>) -> SFResult<Vec<String>>;

    /// Add a single task. Fails if the task already exists or introduces a cycle.
    async fn add_task(&self, task: Task) -> SFResult<()>;

    /// Replace a task already in the graph with a changed copy of it.
    ///
    /// Separate from the state transitions below because it changes what the
    /// row *is* (its place in the scope tree, its acceptance criteria) rather
    /// than where it is in the run. The default refuses: an implementation
    /// whose graph is a read-only projection has nowhere to put the change,
    /// and dropping it silently would report success for a write that did not
    /// happen.
    async fn replace_task(&self, _task: Task) -> SFResult<()> {
        Err(SFError::NotImplemented(
            "replace_task: this DAG implementation has no writable graph".into(),
        ))
    }

    // ─── State transitions ───────────────────────────────────────────────

    /// Schedule a pending task.
    async fn schedule_task(&self, task_id: &str) -> SFResult<()>;

    /// 接活：把一条待接的行交给 `claimant`，写负责人与租约，原子地；返回认领
    /// 之后的行。
    ///
    /// 与 [`Self::schedule_task`] 分开：那个只把行挪进「已派发」，不记谁接了。
    /// 接活必须记下责任人，并且只让一个人接走——派工侧的读循环跨过队列边界时
    /// 用它，员工侧（带外 worker、以后回合里的举手）用同一个。
    ///
    /// 默认是「做不到」，不是「先排上」：接不了活的后端必须让调用方看见它没接
    /// 上，否则调用方会以为责任已经有人担了。
    async fn claim_task(&self, task_id: &str, _claimant: &str) -> SFResult<Task> {
        let _ = task_id;
        Err(SFError::NotImplemented(
            "claim_task: this DAG implementation cannot record who took a row".into(),
        ))
    }

    /// Assign a task to a specific agent.
    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()>;

    /// Mark a scheduled task as running.
    async fn start_task(&self, task_id: &str) -> SFResult<()>;

    /// Mark a running task as completed. Returns IDs of newly-scheduled dependents.
    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>>;

    /// Mark a running task as failed. Returns `(retried, cancelled_ids, dlq_pushed)`.
    /// `cause` 是 `error` 的类型，见 [`OrchestratorControl::fail_task`]。
    async fn fail_task(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)>;

    /// [`Self::fail_task`] for a failure whose upstream named the wait it wants
    /// before the next attempt.
    async fn fail_task_after(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)>;

    /// Cancel a task and cascade-cancel all downstream dependents.
    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>>;

    /// [`Self::cancel_task`] with the reason the row keeps; see
    /// [`OrchestratorControl::cancel_task_with_reason`].
    async fn cancel_task_with_reason(
        &self,
        task_id: &str,
        _reason: String,
    ) -> SFResult<Vec<String>> {
        self.cancel_task(task_id).await
    }

    /// Retry a failed task (reset to Pending).
    async fn retry_task(&self, task_id: &str) -> SFResult<()>;

    // ─── DLQ operations ──────────────────────────────────────────────────

    /// Push a failed task to the dead-letter queue.
    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool>;

    /// Return the number of items in the DLQ.
    async fn dlq_len(&self) -> SFResult<usize>;

    /// Replay a task from the DLQ back into the DAG.
    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool>;

    // ─── Queries ─────────────────────────────────────────────────────────

    /// Tasks whose dependencies are satisfied and are Pending.
    async fn find_ready_tasks(&self) -> Vec<Task>;

    /// Tasks that are Pending or Scheduled with all deps completed.
    async fn get_ready_tasks(&self) -> Vec<Task>;

    /// All tasks currently in the graph.
    async fn get_all_tasks(&self) -> Vec<Task>;

    /// All tasks currently in the graph, with a read that failed reported as
    /// such. The reason is [`OrchestratorControl::get_all_tasks_checked`]'s:
    /// [`Self::get_all_tasks`] answers an empty graph when the store is down,
    /// and a reading built on it prints "0 tasks" -- the one number a reader
    /// cannot question. The default folds `get_all_tasks`, which is honest for
    /// an executor holding its graph in memory; one backed by a store has to
    /// override this, and until it does the fold is the same silent zero.
    async fn all_tasks_checked(&self) -> SFResult<Vec<Task>> {
        Ok(self.get_all_tasks().await)
    }

    /// The tasks that carry a venue label, and only those.
    ///
    /// Same answer as filtering [`Self::get_all_tasks`] by
    /// [`Task::venue_label`], which is what the default does. An executor whose
    /// store can exclude unlabeled rows should override it: the venue board
    /// reads this on every poll and the graph is mostly rows the board drops.
    /// Terminal rows are part of the answer -- the board's latest-verdict and
    /// written-back columns come from tasks that are already done.
    async fn get_venue_tasks(&self) -> Vec<Task> {
        self.get_all_tasks()
            .await
            .into_iter()
            .filter(|task| task.venue_label().is_some())
            .collect()
    }

    /// Look up a single task by id.
    async fn get_task(&self, task_id: &str) -> Option<Task>;

    /// Direct dependents of a task.
    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Direct dependencies of a task.
    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Return the full task graph as (nodes, edges).
    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>);

    // ─── Bulk / utility ──────────────────────────────────────────────────

    /// Check for timed-out running tasks and drive retry/cancel logic.
    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)>;

    /// Remove a task from the graph.
    async fn delete_task(&self, task_id: &str) -> SFResult<()>;

    /// True when every task in the graph is completed.
    async fn all_completed(&self) -> bool;

    /// True if any task in the set is still retryable.
    async fn crew_can_retry(&self, task_ids: &[String]) -> bool;

    /// Retry all failed tasks in the set. Returns number retried.
    async fn crew_retry_all(&self, task_ids: &[String]) -> usize;
}
