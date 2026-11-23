//! 再平衡读图的三条路：读不到，就不出计划。
//!
//! `TaskRebalancer::plan()` 与 `recovery_plan()` 各读一次图、读一次每个 agent 的状态。
//! 这几处原本都把「读失败」折叠成一个具体的值，于是三种不同的失联在同一条读数上：
//!
//! - 待派队列读失败 ⇒ 空表，计划照常返回，「没有可派的任务」；
//! - 任务表读失败 ⇒ 每个 agent 数出 0 条在跑，看起来个个空闲，谁也不让谁；
//! - 恢复时读失败 ⇒ 每个检查点都读成「任务已不在图里」，恢复计划悄悄空掉。
//!
//! 三种都是**成功返回的空计划**——调用方看不出与「集群真的平衡」的区别，于是失联
//! 期间调度照转、恢复不跑，而每一条读数都是绿的。
//!
//! 另一面是单个 agent 的状态读失败：那不是整趟算不成，是这一格此刻量不出来。这一趟
//! 不排它、其余照排；把它读成「没有状态」＝「空闲」，失联时正是最不该派活的时刻。
//!
//! 判据因此四半：队列读失败 ⇒ `Err`；任务表读失败 ⇒ `Err`；恢复时读失败 ⇒ `Err`；
//! 单个 agent 状态读失败 ⇒ 计划照出，但那一个不进可用名单。前三半在旧写法下都是
//! `Ok`，后一半在旧写法下会把失联的那个算进可用名单。

use crate::common;

use chrono::Utc;
use std::sync::Arc;

use cog_core::{
    AgentState, OrchestratorControl, SFError, SFResult, StateBackend, Task, TaskCheckpoint,
    TaskType,
};
use cog_supervisor::lifecycle_coordinator::RecoveredCheckpoint;
use cog_supervisor::registry::{AgentInfo, AgentRegistry};
use cog_supervisor::task_rebalancer::{TaskRebalancer, TaskRebalancerConfig};
use cog_test_support::{MemoryStateBackend, TaskGraphStub};

const UNANSWERED: &str = "the state store did not answer";

fn make_agent(id: &str) -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        agent_id: id.into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: Vec::new(),
        last_heartbeat: now,
        state_since: now,
        registered_at: now,
    }
}

fn rebalancer_with(
    reg: Arc<AgentRegistry>,
    backend: Arc<dyn StateBackend>,
    orch: Arc<dyn OrchestratorControl>,
) -> TaskRebalancer {
    TaskRebalancer::new(reg, backend, orch, TaskRebalancerConfig::default())
}

fn checkpoint(task_id: &str) -> RecoveredCheckpoint {
    RecoveredCheckpoint {
        agent_id: "a-dead".into(),
        task_id: task_id.into(),
        checkpoint: TaskCheckpoint {
            task_id: task_id.into(),
            snapshot_id: format!("snap-{task_id}"),
            event_offset: 0,
            timestamp: Utc::now(),
        },
    }
}

/// 只坏一面的状态后端：`get_agent_state` 对**一个** agent 答错，其余照常。
///
/// 只坏一个而不是全坏，是因为这一面的判据正是「一格的读数读不到，不影响别的格」：
/// 全坏的话，`continue` 与「整趟返回 `Err`」两种写法都会让计划只剩健康的那些，
/// 用例就分不开它们。别的方法在这条路径上不会被碰，写 `unreachable!`——日后若被
/// 碰到，当场炸出是哪一个，而不是让一个静默的空实现替它回答。
struct UnreadableFor {
    inner: MemoryStateBackend,
    agent_id: &'static str,
}

#[async_trait::async_trait]
impl StateBackend for UnreadableFor {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        if agent_id == self.agent_id {
            return Err(SFError::Database(UNANSWERED.into()));
        }
        self.inner.get_agent_state(agent_id).await
    }

    async fn set_agent_state(&self, _agent_id: &str, _state: &AgentState) -> SFResult<()> {
        unreachable!("the rebalancer does not write agent states")
    }

    async fn cas_agent_state(
        &self,
        _agent_id: &str,
        _expected: &AgentState,
        _new: &AgentState,
    ) -> SFResult<bool> {
        unreachable!("the rebalancer does not swap agent states")
    }

    async fn get_checkpoint(&self, _task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        unreachable!("the rebalancer does not read checkpoints from the backend")
    }

    async fn save_checkpoint(&self, _checkpoint: &TaskCheckpoint) -> SFResult<()> {
        unreachable!("the rebalancer does not save checkpoints")
    }

    async fn append_event(&self, _task_id: &str, _event: &cog_core::Event) -> SFResult<u64> {
        unreachable!("the rebalancer does not append task events")
    }

    async fn get_events(
        &self,
        _task_id: &str,
        _offset: u64,
        _limit: usize,
    ) -> SFResult<Vec<cog_core::Event>> {
        unreachable!("the rebalancer does not read task events")
    }

    async fn get_board(&self, _task_id: &str) -> SFResult<Option<cog_core::ContextBoard>> {
        unreachable!("the rebalancer does not read context boards")
    }

    async fn set_board_field(&self, _task_id: &str, _field: &str, _value: &str) -> SFResult<()> {
        unreachable!("the rebalancer does not write context boards")
    }

    async fn delete_checkpoint(&self, _task_id: &str) -> SFResult<()> {
        unreachable!("the rebalancer does not delete checkpoints")
    }

    async fn delete_board(&self, _task_id: &str) -> SFResult<()> {
        unreachable!("the rebalancer does not delete context boards")
    }

    async fn remove_board_field(&self, _task_id: &str, _field: &str) -> SFResult<()> {
        unreachable!("the rebalancer does not remove context board fields")
    }
}

/// 待派队列读不到：计划算不成，不是一个「没有可派任务」的空计划。
#[tokio::test]
async fn a_ready_queue_that_cannot_be_read_fails_the_pass() {
    let reg = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let orch: Arc<dyn OrchestratorControl> = Arc::new(TaskGraphStub::whose_ready_read_fails(
        "ready queue unreachable",
    ));

    let err = rebalancer_with(reg, backend, orch)
        .plan()
        .await
        .expect_err("读不到待派队列时不能给出一个空计划");
    assert!(
        format!("{err}").contains("ready queue unreachable"),
        "判词要点名是那条读答不上来：{err}"
    );
}

/// 任务表读不到：数不出在跑几件，就不能说这个 agent 空闲。
#[tokio::test]
async fn a_task_table_that_cannot_be_read_fails_the_pass() {
    let reg = Arc::new(AgentRegistry::new());
    reg.register_agent(make_agent("agent-1"));

    let backend = MemoryStateBackend::new();
    backend
        .set_agent_state("agent-1", &AgentState::Idle)
        .await
        .unwrap();
    let backend: Arc<dyn StateBackend> = Arc::new(backend);

    let orch: Arc<dyn OrchestratorControl> = Arc::new(TaskGraphStub::whose_task_read_fails(
        "task table unreachable",
    ));

    let err = rebalancer_with(reg, backend, orch)
        .plan()
        .await
        .expect_err("数不出在跑几件时，不能把它读成 0 件");
    assert!(
        format!("{err}").contains("task table unreachable"),
        "判词要点名是那条读答不上来：{err}"
    );
}

/// 恢复时读不到图：整份恢复计划算不成；读得到时，仍在图里的那条照旧恢复。
#[tokio::test]
async fn a_graph_that_cannot_be_read_fails_the_recovery_plan() {
    let reg = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());

    // 读得到的那一半：检查点对应的任务还在图里，它就照旧被恢复。
    let readable: Arc<dyn OrchestratorControl> = Arc::new(TaskGraphStub::holding(vec![Task::new(
        "t-1",
        TaskType::Planner,
        serde_json::json!({}),
    )]));
    let plan = rebalancer_with(reg.clone(), backend.clone(), readable)
        .recovery_plan(&[checkpoint("t-1")])
        .await
        .expect("读得到图时恢复计划照常给出");
    assert_eq!(plan.checkpoint_recoveries.len(), 1);

    // 读不到的那一半：不是「这条任务已经不在图里了」，是这一份计划算不成。
    let unreadable: Arc<dyn OrchestratorControl> = Arc::new(TaskGraphStub::whose_task_read_fails(
        "task table unreachable",
    ));
    let err = rebalancer_with(reg, backend, unreadable)
        .recovery_plan(&[checkpoint("t-1")])
        .await
        .expect_err("读不到图时不能把每个检查点都读成「任务已不在图里」");
    assert!(
        format!("{err}").contains("task table unreachable"),
        "判词要点名是那条读答不上来：{err}"
    );
}

/// 一个 agent 的状态读不到：这一趟不排它，别的照排，计划本身照出。
///
/// 旧写法把这条错误折叠成 `None`，而 `None` 在这里的意思是「后端里没有这格记录，
/// 那就是空闲」——于是后端失联的那一刻，恰恰是失联的这个 agent 被派活的那一刻。
#[tokio::test]
async fn an_agent_whose_state_cannot_be_read_is_left_out_of_the_pass() {
    let reg = Arc::new(AgentRegistry::new());
    reg.register_agent(make_agent("agent-1"));
    reg.register_agent(make_agent("agent-2"));

    let backend: Arc<dyn StateBackend> = Arc::new(UnreadableFor {
        inner: MemoryStateBackend::new(),
        agent_id: "agent-2",
    });

    let orch = common::orchestrator::control("ws");
    orch.submit_goal(
        "g",
        vec![
            Task::new("t-1", TaskType::Planner, serde_json::json!({})),
            Task::new("t-2", TaskType::Planner, serde_json::json!({})),
        ],
    )
    .await
    .unwrap();

    let plan = rebalancer_with(reg, backend, orch)
        .plan()
        .await
        .expect("一个 agent 的状态读不到，不叫整趟算不成");

    assert_eq!(
        plan.available_agents, 1,
        "读不到状态的那一个不是「空闲」，不能进可用名单"
    );
    assert_eq!(plan.assignments.len(), 2, "活照排给读得到的那个");
    for (agent_id, task_id) in &plan.assignments {
        assert_eq!(agent_id, "agent-1", "{task_id} 派给了状态读不到的 agent-2");
    }
}
