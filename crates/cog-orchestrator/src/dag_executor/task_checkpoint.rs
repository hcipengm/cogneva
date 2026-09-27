//! 续跑链的产出侧：把在跑任务的进度落成检查点。
//!
//! 为什么只能在运行期写：检查点必须在任务成功**之前**存在，而任务被杀或随着
//! 版本滚动被换掉时，最后一刻的写入不会发生——所以产出点是持有任务的进程的
//! 周期动作，不是停机钩子。
//!
//! 为什么续跑指针落在任务的板上而不是流里：任务归属的事实已经在 DAG 状态上
//! （租约持有者），板是同一把键（task_id）下的字符串 KV，加入一个指针不需要
//! 新契约；而流是通道，不是存储，无人读的流只会无界增长。
//!
//! 恢复端按 agent 自己的 id 反查它那一份指针（`{task_id}-{role}` 的 role 段），
//! 所以这里写下去的值必须与恢复端读的字段名同源——两处都用
//! [`checkpoint_field`] 生成，不手写字面量。

use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{AgentManager, CheckpointStore, SFResult, StateBackend};

// 板上的续跑指针字段名与 agent 的命名，词汇定义在 `cog-core`：产出侧（本模块）
// 与恢复侧（派发时建 agent 的那一端）各写一份字面量不会报错，只会让两边认领的
// 不是同一批 agent，而整条链看起来就像「从来没有可续跑的进度」。
use cog_core::{agent_id_for, checkpoint_field};

/// `agent_id` 是不是 `task_id` 这个任务的 agent。
///
/// 任务与 agent 的唯一关联是命名约定（[`agent_id_for`]）——没有映射表，注册表
/// 里那份 `task_ids` 在生产里也没有产出方。角色不从这个 id 里解析，而是由
/// agent 自己声明（`WorkerInfo.role`）后拼回去精确比对：解析式（取任务 id 之后
/// 的全部）在任务 id 互为前缀时会互相认领，`t-1` 会把 `t-11-planner` 算成自己
/// 的 `1-planner`，于是写下去的指针挂到了别人的板上。
pub fn belongs_to_task(agent_id: &str, role: &str, task_id: &str) -> bool {
    !role.is_empty() && agent_id == agent_id_for(task_id, role)
}

/// 一轮产出侧动作的读数。
///
/// 五个量分开记而不是折成一个成败：`saved` 是「有了续跑点」，`unpersisted` 是
/// 「快照拿到了但存储里没有」——那是 agent 没配存储，一份指针写下去就指向空气；
/// `failed` 是快照调用本身失败。前者是配置病、后者是运行病，共用一个计数器就
/// 分不出来了。`superseded` 是本轮替下去的旧续跑点，`unsuperseded` 是替不下去
/// 的那一份——它不影响续跑（新的已经在了），只影响存储的占用，所以既不与
/// `failed` 合并（那是「这次没续跑成」），也不与 `saved` 合并（那是「有得续」）。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointRound {
    /// 这个任务名下的 agent 数。
    pub agents: usize,
    /// 快照已落存储、指针已上板。
    pub saved: usize,
    /// 快照拿到了，但存储里查不到它（没有可恢复的续跑点，故不写指针）。
    pub unpersisted: usize,
    /// agent 取不到，或快照调用失败。
    pub failed: usize,
    /// 上一份续跑点已被删除：留存的只有指针指着的这一份。
    pub superseded: usize,
    /// 上一份续跑点删不掉（存储报错），它会一直留在库里。
    pub unsuperseded: usize,
}

impl CheckpointRound {
    /// 这一轮没有任何 agent 属于这个任务。
    ///
    /// 它不是「没什么可做」的同义词：持有者在跑，而链里没有它的 agent——要么
    /// 这条执行路径的 agent 不按 [`agent_id_for`] 命名（那它就没有续跑点），
    /// 要么名册已经读不到它了。前者是**已知的覆盖边界**，后者是缺陷，两者在这里
    /// 同形，所以这一格只作为「有任务在跑却无人可查」的计数发布，不挂规则。
    pub fn is_quiet(&self) -> bool {
        self.agents == 0
    }
}

/// 产出侧需要看到的两个动作，把「谁的 agent 在跑」与「快照存下来了没有」
/// 收成一对窄接口：真正的实现是 [`cog_core::AgentManager`] 与
/// [`cog_core::CheckpointStore`]（见 [`LiveCheckpointAgents`]），而判断逻辑
/// 只依赖这两个动作，可以用替身把四种结局都跑出来。
#[async_trait::async_trait]
pub trait CheckpointAgents: Send + Sync {
    /// 这个任务名下的 `(agent_id, role)`。
    async fn agents_of(&self, task_id: &str) -> SFResult<Vec<(String, String)>>;

    /// 让某个 agent 对当前状态拍一张快照，返回快照 id。
    async fn snapshot(&self, task_id: &str, agent_id: &str) -> SFResult<String>;

    /// 删掉一份已经被替下去的续跑点。
    ///
    /// 留存的份数是这套产出的容量上限：不删的话，每一次快照都在库里加一份
    /// 实物——而续跑只需要最新那一份，旧的那些没有任何读者，只会在卷上
    /// 无限累积。
    async fn forget(&self, checkpoint_id: &str) -> SFResult<()>;

    /// 这张快照是否真的躺在存储里。
    ///
    /// 必须探存储本身，不能拿快照调用的成功当依据：agent 侧在没有配置检查点
    /// 存储时**照样返回快照**，只是不落盘——那种情况下写指针等于写下一条指向
    /// 不存在的续跑点的路标，而恢复端只会看到「找不到」。
    async fn persisted(&self, checkpoint_id: &str) -> SFResult<bool>;
}

/// [`CheckpointAgents`] 的生产实现：agent 名册来自 agent 管理器，落盘判据来自
/// 检查点存储。
pub struct LiveCheckpointAgents {
    manager: Arc<dyn AgentManager>,
    store: Arc<dyn CheckpointStore>,
}

impl LiveCheckpointAgents {
    pub fn new(manager: Arc<dyn AgentManager>, store: Arc<dyn CheckpointStore>) -> Self {
        Self { manager, store }
    }
}

#[async_trait::async_trait]
impl CheckpointAgents for LiveCheckpointAgents {
    async fn agents_of(&self, task_id: &str) -> SFResult<Vec<(String, String)>> {
        let workers = self.manager.list_workers().await?;
        Ok(workers
            .into_iter()
            .filter(|w| belongs_to_task(&w.agent_id, &w.role, task_id))
            .map(|w| (w.agent_id, w.role))
            .collect())
    }

    async fn snapshot(&self, task_id: &str, agent_id: &str) -> SFResult<String> {
        let agent = self
            .manager
            .get_agent(agent_id)
            .await?
            .ok_or_else(|| cog_core::SFError::Agent(format!("agent not found: {agent_id}")))?;
        let checkpoint = agent.snapshot(task_id.to_string()).await?;
        Ok(checkpoint.checkpoint_id)
    }

    async fn persisted(&self, checkpoint_id: &str) -> SFResult<bool> {
        Ok(self.store.load(checkpoint_id).await?.is_some())
    }

    async fn forget(&self, checkpoint_id: &str) -> SFResult<()> {
        self.store.delete(checkpoint_id).await
    }
}

/// 为一个任务名下的每个 agent 落检查点，并把可恢复的那些指针挂到任务的板上。
pub async fn checkpoint_task(
    task_id: &str,
    agents: &dyn CheckpointAgents,
    state: &Arc<dyn StateBackend>,
) -> CheckpointRound {
    let mut round = CheckpointRound::default();
    let roster = match agents.agents_of(task_id).await {
        Ok(roster) => roster,
        Err(e) => {
            tracing::warn!(task = task_id, error = %e, "cannot enumerate the agents of a running task");
            return round;
        }
    };

    // 板读一次给整轮用：上一份指针在写新的之前读，才知道该替掉哪一份。读不到
    // 只影响「旧的那份能不能回收」，不影响本轮该写的指针照写——续跑是主功能，
    // 容量是附带条件，不能因为后者读不到就把前者停掉。
    let previous = match state.get_board(task_id).await {
        Ok(Some(board)) => board.fields,
        Ok(None) => Default::default(),
        Err(e) => {
            tracing::warn!(task = task_id, error = %e, "cannot read the task board before checkpointing; superseded checkpoints are kept this round");
            Default::default()
        }
    };

    for (agent_id, role) in roster {
        round.agents += 1;
        let checkpoint_id = match agents.snapshot(task_id, &agent_id).await {
            Ok(id) => id,
            Err(e) => {
                round.failed += 1;
                tracing::warn!(
                    task = task_id,
                    agent = %agent_id,
                    error = %e,
                    "task checkpoint snapshot failed"
                );
                continue;
            }
        };
        match agents.persisted(&checkpoint_id).await {
            // 指针只在续跑点真的存在时写：写下一条指不到东西的路标，恢复端
            // 只会把它读成「没有续跑点」，而产出侧的读数会说是「写了」。
            Ok(true) => {
                match state
                    .set_board_field(task_id, &checkpoint_field(&role), &checkpoint_id)
                    .await
                {
                    Ok(()) => {
                        round.saved += 1;
                        let old = previous
                            .get(&checkpoint_field(&role))
                            .filter(|old| **old != checkpoint_id);
                        if let Some(old) = old {
                            // 顺序不能反：先写指针再删旧的。反过来，删成功而写
                            // 失败就成了「续跑点没了」；这个顺序最坏也只是多留
                            // 一份没人指的实物。
                            match agents.forget(old).await {
                                Ok(()) => round.superseded += 1,
                                Err(e) => {
                                    round.unsuperseded += 1;
                                    tracing::warn!(
                                        task = task_id,
                                        checkpoint = %old,
                                        error = %e,
                                        "cannot delete the checkpoint this one replaces; it stays in the store"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        round.failed += 1;
                        tracing::warn!(
                            task = task_id,
                            agent = %agent_id,
                            error = %e,
                            "cannot record the resume pointer on the task board"
                        );
                    }
                }
            }
            Ok(false) => {
                round.unpersisted += 1;
                tracing::warn!(
                    task = task_id,
                    agent = %agent_id,
                    checkpoint = %checkpoint_id,
                    "snapshot was taken but is not in the checkpoint store: the agent has no store configured, so there is nothing to resume from"
                );
            }
            Err(e) => {
                round.failed += 1;
                tracing::warn!(
                    task = task_id,
                    checkpoint = %checkpoint_id,
                    error = %e,
                    "cannot read back a checkpoint to confirm it was persisted"
                );
            }
        }
    }

    round
}

/// 账上每个量对应的读数标签，产出侧按它记录计数器。
pub fn outcome_counts(round: &CheckpointRound) -> HashMap<&'static str, f64> {
    HashMap::from([
        ("saved", round.saved as f64),
        ("unpersisted", round.unpersisted as f64),
        ("failed", round.failed as f64),
        ("superseded", round.superseded as f64),
        ("unsuperseded", round.unsuperseded as f64),
        // 一格计数而不是一个事实：它数的是「有任务在跑、链里却查不到它的 agent」
        // 的轮数，也就是这个任务的进度没有产出方的时间。已知的覆盖边界（非
        // squad 命名的执行路径）与真正的失配在这里同形，所以它只作为读数存在，
        // 不挂规则——挂上去就会对边界常态常年报警。
        ("no_agents", if round.is_quiet() { 1.0 } else { 0.0 }),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{ContextBoard, Event, TaskCheckpoint};
    use std::sync::Mutex;

    /// 只记板字段的最小 StateBackend。
    #[derive(Default)]
    struct BoardRecorder {
        fields: Mutex<HashMap<(String, String), String>>,
    }

    #[async_trait::async_trait]
    impl StateBackend for BoardRecorder {
        async fn get_agent_state(&self, _agent_id: &str) -> SFResult<Option<cog_core::AgentState>> {
            Ok(None)
        }
        async fn set_agent_state(
            &self,
            _agent_id: &str,
            _state: &cog_core::AgentState,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn cas_agent_state(
            &self,
            _agent_id: &str,
            _expected: &cog_core::AgentState,
            _new: &cog_core::AgentState,
        ) -> SFResult<bool> {
            Ok(true)
        }
        async fn get_checkpoint(&self, _task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
            Ok(None)
        }
        async fn save_checkpoint(&self, _checkpoint: &TaskCheckpoint) -> SFResult<()> {
            Ok(())
        }
        async fn append_event(&self, _task_id: &str, _event: &Event) -> SFResult<u64> {
            Ok(0)
        }
        async fn get_events(
            &self,
            _task_id: &str,
            _offset: u64,
            _limit: usize,
        ) -> SFResult<Vec<Event>> {
            Ok(Vec::new())
        }
        async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
            let fields: HashMap<String, String> = self
                .fields
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .filter(|((task, _), _)| task == task_id)
                .map(|((_, field), value)| (field.clone(), value.clone()))
                .collect();
            if fields.is_empty() {
                return Ok(None);
            }
            Ok(Some(ContextBoard {
                task_id: task_id.to_string(),
                fields,
                updated_at: chrono::Utc::now(),
            }))
        }
        async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
            self.fields
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert((task_id.to_string(), field.to_string()), value.to_string());
            Ok(())
        }
        async fn delete_checkpoint(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn delete_board(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn remove_board_field(&self, _task_id: &str, _field: &str) -> SFResult<()> {
            Ok(())
        }
    }

    /// 名册、快照、落盘三个动作都可编排的替身。
    #[derive(Default)]
    struct FakeAgents {
        roster: Vec<(String, String)>,
        /// agent_id → 快照 id；缺席表示快照调用失败。
        snapshots: HashMap<String, String>,
        /// 存储里真的有的快照 id。
        persisted: Vec<String>,
        /// 被删掉的快照 id，按删除顺序。
        forgotten: Mutex<Vec<String>>,
        /// 让删除失败，用来验证「旧的那份回收不掉」这条路。
        forget_fails: bool,
    }

    #[async_trait::async_trait]
    impl CheckpointAgents for FakeAgents {
        async fn agents_of(&self, _task_id: &str) -> SFResult<Vec<(String, String)>> {
            Ok(self.roster.clone())
        }
        async fn snapshot(&self, _task_id: &str, agent_id: &str) -> SFResult<String> {
            self.snapshots
                .get(agent_id)
                .cloned()
                .ok_or_else(|| cog_core::SFError::Agent(format!("snapshot failed: {agent_id}")))
        }
        async fn persisted(&self, checkpoint_id: &str) -> SFResult<bool> {
            Ok(self.persisted.iter().any(|id| id == checkpoint_id))
        }
        async fn forget(&self, checkpoint_id: &str) -> SFResult<()> {
            if self.forget_fails {
                return Err(cog_core::SFError::Database("delete failed".into()));
            }
            self.forgotten
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(checkpoint_id.to_string());
            Ok(())
        }
    }

    /// 板的两半：留着具体类型读断言，交给被测函数的是 trait 对象。
    fn board() -> (Arc<BoardRecorder>, Arc<dyn StateBackend>) {
        let recorder = Arc::new(BoardRecorder::default());
        let state: Arc<dyn StateBackend> = recorder.clone();
        (recorder, state)
    }

    fn fake(pairs: &[(&str, &str)], persisted: &[&str]) -> FakeAgents {
        FakeAgents {
            roster: pairs
                .iter()
                .map(|(id, role)| (id.to_string(), role.to_string()))
                .collect(),
            snapshots: pairs
                .iter()
                .map(|(id, _)| (id.to_string(), format!("cp-{id}")))
                .collect(),
            persisted: persisted.iter().map(|s| s.to_string()).collect(),
            forgotten: Mutex::new(Vec::new()),
            forget_fails: false,
        }
    }

    /// 上一轮已经在板上留了 `pointer` 这个续跑点。
    fn with_previous_pointer(
        recorder: &Arc<BoardRecorder>,
        task_id: &str,
        role: &str,
        pointer: &str,
    ) {
        recorder
            .fields
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                (task_id.to_string(), checkpoint_field(role)),
                pointer.to_string(),
            );
    }

    #[test]
    fn an_agent_belongs_to_the_task_its_id_names() {
        assert!(belongs_to_task("squad:t-1-planner", "planner", "t-1"));
        assert!(belongs_to_task("squad:t-1-evaluator", "evaluator", "t-1"));
        // 任务 id 互为前缀时不许互相认领：这条错了，写下去的就是别人的续跑点。
        assert!(!belongs_to_task("squad:t-11-planner", "planner", "t-1"));
        assert!(!belongs_to_task("squad:t-1-planner", "planner", "t-11"));
        assert!(!belongs_to_task("squad:t-2-planner", "planner", "t-1"));
        // 少了 squad 前缀的裸任务名不是这个词汇里的一个 id。
        assert!(!belongs_to_task("t-1", "planner", "t-1"));
        assert!(!belongs_to_task("t-1-planner", "planner", "t-1"));
        // 角色必须与 id 的尾段一致：声明是 planner 的 agent 不该被按 generator 认领。
        assert!(!belongs_to_task("squad:t-1-planner", "generator", "t-1"));
        assert!(!belongs_to_task("squad:t-1-planner", "", "t-1"));
        // 并行分支的 agent 有自己的一段后缀，不属于任务级的这个角色。
        assert!(!belongs_to_task("t-1-branch-0-planner", "planner", "t-1"));
    }

    #[test]
    fn the_pointer_field_is_derived_not_spelled() {
        assert_eq!(checkpoint_field("planner"), "checkpoint:planner");
    }

    #[test]
    fn the_agent_id_is_derived_not_spelled() {
        assert_eq!(agent_id_for("t-1", "planner"), "squad:t-1-planner");
    }

    #[tokio::test]
    async fn every_agent_that_reached_the_store_gets_a_pointer() {
        let (board, state) = board();
        let agents = fake(
            &[
                ("squad:t-1-planner", "planner"),
                ("squad:t-1-generator", "generator"),
            ],
            &["cp-squad:t-1-planner", "cp-squad:t-1-generator"],
        );

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.agents, 2);
        assert_eq!(round.saved, 2);
        assert_eq!(round.unpersisted, 0);
        assert_eq!(round.failed, 0);
        let fields = board.fields.lock().unwrap();
        assert_eq!(
            fields
                .get(&("t-1".into(), "checkpoint:planner".into()))
                .map(String::as_str),
            Some("cp-squad:t-1-planner")
        );
        assert_eq!(
            fields
                .get(&("t-1".into(), "checkpoint:generator".into()))
                .map(String::as_str),
            Some("cp-squad:t-1-generator")
        );
    }

    #[tokio::test]
    async fn a_snapshot_that_never_reached_the_store_gets_no_pointer() {
        let (board, state) = board();
        let agents = fake(&[("squad:t-1-planner", "planner")], &[]);

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.agents, 1);
        assert_eq!(round.saved, 0);
        assert_eq!(round.unpersisted, 1);
        assert_eq!(round.failed, 0);
        assert!(
            board.fields.lock().unwrap().is_empty(),
            "a pointer to a checkpoint that is not in the store would read as resumable"
        );
    }

    #[tokio::test]
    async fn a_failed_snapshot_is_counted_apart_from_an_unpersisted_one() {
        let (board, state) = board();
        let mut agents = fake(
            &[("squad:t-1-planner", "planner")],
            &["cp-squad:t-1-planner"],
        );
        agents.snapshots.clear();

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.agents, 1);
        assert_eq!(round.saved, 0);
        assert_eq!(round.unpersisted, 0);
        assert_eq!(round.failed, 1);
        assert!(board.fields.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_task_whose_agents_are_not_here_is_quiet() {
        let (board, state) = board();
        let agents = fake(&[], &[]);

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert!(round.is_quiet());
        assert_eq!(round.saved, 0);
        assert!(board.fields.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_checkpoint_a_new_one_replaces_is_deleted() {
        let (board, state) = board();
        with_previous_pointer(&board, "t-1", "planner", "cp-old");
        let mut agents = fake(&[("squad:t-1-planner", "planner")], &["cp-old"]);
        agents.persisted = vec!["cp-squad:t-1-planner".into()];

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.saved, 1);
        assert_eq!(round.superseded, 1);
        assert_eq!(round.unsuperseded, 0);
        assert_eq!(
            *agents.forgotten.lock().unwrap(),
            vec!["cp-old".to_string()],
            "the superseded checkpoint is what keeps the store bounded by the live set"
        );
    }

    #[tokio::test]
    async fn a_checkpoint_that_replaces_nothing_is_not_deleted() {
        let (board, state) = board();
        with_previous_pointer(&board, "t-1", "planner", "cp-old");
        // 只有 generator 在这一轮里有 agent：planner 的旧指针不属于本轮任何人。
        let agents = fake(
            &[("squad:t-1-generator", "generator")],
            &["cp-squad:t-1-generator"],
        );

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.saved, 1);
        assert_eq!(round.superseded, 0);
        assert!(agents.forgotten.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_checkpoint_that_cannot_delete_is_counted_apart() {
        let (board, state) = board();
        with_previous_pointer(&board, "t-1", "planner", "cp-old");
        let mut agents = fake(&[("squad:t-1-planner", "planner")], &["cp-old"]);
        agents.persisted = vec!["cp-squad:t-1-planner".into()];
        agents.forget_fails = true;

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(
            round.saved, 1,
            "the new resume point is in place regardless"
        );
        assert_eq!(round.superseded, 0);
        assert_eq!(round.unsuperseded, 1);
        assert_eq!(round.failed, 0, "nothing failed to be checkpointed");
        assert_eq!(
            board
                .fields
                .lock()
                .unwrap()
                .get(&("t-1".into(), "checkpoint:planner".into()))
                .map(String::as_str),
            Some("cp-squad:t-1-planner")
        );
    }

    #[tokio::test]
    async fn an_unreadable_board_does_not_stop_the_pointer_from_being_written() {
        let (board, _) = board();
        let mut agents = fake(
            &[("squad:t-1-planner", "planner")],
            &["cp-squad:t-1-planner"],
        );
        agents.persisted = vec!["cp-squad:t-1-planner".into()];
        // 板读不出来只是「旧的那份回收不掉」，本轮的续跑点照写。
        let state: Arc<dyn StateBackend> = Arc::new(UnreadableBoard {
            inner: board.clone(),
        });

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.saved, 1);
        assert_eq!(round.superseded, 0);
        assert!(agents.forgotten.lock().unwrap().is_empty());
    }

    #[test]
    fn a_round_with_no_agents_is_counted_rather_than_absent() {
        let quiet = CheckpointRound {
            agents: 0,
            ..Default::default()
        };
        let counts = outcome_counts(&quiet);
        assert_eq!(counts.get("no_agents"), Some(&1.0));
        for outcome in [
            "saved",
            "unpersisted",
            "failed",
            "superseded",
            "unsuperseded",
        ] {
            assert_eq!(counts.get(outcome), Some(&0.0), "{outcome}");
        }

        // 有 agent 的那一轮不记这一格：它数的是「查不到 agent 的轮数」。
        let busy = CheckpointRound {
            agents: 2,
            saved: 2,
            ..Default::default()
        };
        let counts = outcome_counts(&busy);
        assert_eq!(counts.get("no_agents"), Some(&0.0));
        assert_eq!(counts.get("saved"), Some(&2.0));
    }

    #[tokio::test]
    async fn one_agent_failing_does_not_stop_the_others() {
        let (board, state) = board();
        let mut agents = fake(
            &[
                ("squad:t-1-planner", "planner"),
                ("squad:t-1-evaluator", "evaluator"),
            ],
            &["cp-squad:t-1-planner", "cp-squad:t-1-evaluator"],
        );
        agents.snapshots.remove("squad:t-1-planner");

        let round = checkpoint_task("t-1", &agents, &state).await;

        assert_eq!(round.agents, 2);
        assert_eq!(round.saved, 1);
        assert_eq!(round.failed, 1);
        let fields = board.fields.lock().unwrap();
        assert!(!fields.contains_key(&("t-1".into(), "checkpoint:planner".into())));
        assert!(fields.contains_key(&("t-1".into(), "checkpoint:evaluator".into())));
    }

    /// 写得进、读不出的板：只用来把「板读失败」这条路走通。
    struct UnreadableBoard {
        inner: Arc<BoardRecorder>,
    }

    #[async_trait::async_trait]
    impl StateBackend for UnreadableBoard {
        async fn get_board(&self, _task_id: &str) -> SFResult<Option<ContextBoard>> {
            Err(cog_core::SFError::Database("board down".into()))
        }
        async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
            self.inner.set_board_field(task_id, field, value).await
        }
        async fn get_agent_state(&self, _agent_id: &str) -> SFResult<Option<cog_core::AgentState>> {
            Ok(None)
        }
        async fn set_agent_state(
            &self,
            _agent_id: &str,
            _state: &cog_core::AgentState,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn cas_agent_state(
            &self,
            _agent_id: &str,
            _expected: &cog_core::AgentState,
            _new: &cog_core::AgentState,
        ) -> SFResult<bool> {
            Ok(true)
        }
        async fn get_checkpoint(&self, _task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
            Ok(None)
        }
        async fn save_checkpoint(&self, _checkpoint: &TaskCheckpoint) -> SFResult<()> {
            Ok(())
        }
        async fn append_event(&self, _task_id: &str, _event: &Event) -> SFResult<u64> {
            Ok(0)
        }
        async fn get_events(
            &self,
            _task_id: &str,
            _offset: u64,
            _limit: usize,
        ) -> SFResult<Vec<Event>> {
            Ok(Vec::new())
        }
        async fn delete_checkpoint(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn delete_board(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn remove_board_field(&self, _task_id: &str, _field: &str) -> SFResult<()> {
            Ok(())
        }
    }
}
