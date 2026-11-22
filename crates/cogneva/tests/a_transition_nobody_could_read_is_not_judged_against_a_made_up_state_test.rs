//! 读不到状态时，转换不判、框架不编。
//!
//! 生命周期协调器为每一次健康报告读一次当前状态，再按状态机判这次转换、用
//! CAS 落库，最后把「从 A 到 B」发到智能体总线上——那是名册状态格的唯一来源。
//!
//! 两处都把「读失败」折叠成了一个具体的状态：判断那一路折叠成 `Init`，于是
//! 一次存储不答被报成「invalid transition Init -> Suspect for agent a-1」，读
//! 的人去查状态机，而真正没答的是存储；记录那一路折叠成 `Active`，于是总线
//! 上一帧写着「从 active 到 suspect」，而没有任何人读到过 active。
//!
//! 判据三半：读不到时不判（错误里点名存储回了什么、也绝不拿编出来的当前值去
//! CAS）；转换落地了但「从」读不到时，记录与框架说 `None` / "unknown"；存储
//! 正常时这两处照旧说出真的状态——否则「不改也绿」。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::broadcast;

use cog_core::{AgentEvent, AgentState, HealthIssue, SFError, SFResult, StateBackend};
use cog_storage::MemoryStateBackend;
use cog_supervisor::health_checker::HealthReport;
use cog_supervisor::lifecycle_coordinator::LifecycleCoordinator;
use cog_supervisor::registry::{AgentInfo, AgentRegistry};

const UNANSWERED: &str = "the state store did not answer";

/// 包着内存后端的替身：前 `fail_reads` 次读状态回错（`usize::MAX` 表示一直回
/// 错），其余方法原样转给真身。`swaps` 数的是 CAS 被调了几次——没读到状态的
/// 那一路连 CAS 都不该碰。
struct UnreadableState {
    inner: MemoryStateBackend,
    fail_reads: usize,
    reads: AtomicUsize,
    swaps: AtomicUsize,
}

impl UnreadableState {
    fn failing(fail_reads: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStateBackend::new(),
            fail_reads,
            reads: AtomicUsize::new(0),
            swaps: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl StateBackend for UnreadableState {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst);
        if read < self.fail_reads {
            return Err(SFError::Database(UNANSWERED.into()));
        }
        self.inner.get_agent_state(agent_id).await
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        self.inner.set_agent_state(agent_id, state).await
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        self.swaps.fetch_add(1, Ordering::SeqCst);
        self.inner.cas_agent_state(agent_id, expected, new).await
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<cog_core::TaskCheckpoint>> {
        self.inner.get_checkpoint(task_id).await
    }

    // 协调器在这条路径上不碰这几个面；日后若开始碰，这里当场炸出是哪一个，
    // 而不是让一个静默的空实现替它回答。
    async fn save_checkpoint(&self, _checkpoint: &cog_core::TaskCheckpoint) -> SFResult<()> {
        unreachable!("the coordinator does not save checkpoints")
    }

    async fn append_event(&self, _task_id: &str, _event: &cog_core::Event) -> SFResult<u64> {
        unreachable!("the coordinator does not append task events")
    }

    async fn get_events(
        &self,
        _task_id: &str,
        _offset: u64,
        _limit: usize,
    ) -> SFResult<Vec<cog_core::Event>> {
        unreachable!("the coordinator does not read task events")
    }

    async fn get_board(&self, _task_id: &str) -> SFResult<Option<cog_core::ContextBoard>> {
        unreachable!("the coordinator does not read context boards")
    }

    async fn set_board_field(&self, _task_id: &str, _field: &str, _value: &str) -> SFResult<()> {
        unreachable!("the coordinator does not write context boards")
    }

    async fn delete_checkpoint(&self, _task_id: &str) -> SFResult<()> {
        unreachable!("the coordinator does not delete checkpoints")
    }

    async fn delete_board(&self, _task_id: &str) -> SFResult<()> {
        unreachable!("the coordinator does not delete context boards")
    }

    async fn remove_board_field(&self, _task_id: &str, _field: &str) -> SFResult<()> {
        unreachable!("the coordinator does not remove context board fields")
    }
}

fn registry_with(agent_id: &str) -> Arc<AgentRegistry> {
    let registry = Arc::new(AgentRegistry::new());
    registry.register_agent(AgentInfo {
        agent_id: agent_id.into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });
    registry
}

fn suspect_report(agent_id: &str) -> HealthReport {
    HealthReport {
        suspect: vec![(agent_id.into(), HealthIssue::Suspect { missed_beats: 2 })],
        ..Default::default()
    }
}

/// 状态一直读不到：拒绝转换，理由点名存储，绝不拿 `Init` 顶替去判、去 CAS。
#[tokio::test]
async fn a_transition_whose_state_could_not_be_read_is_refused_with_the_read_error() {
    let backend = UnreadableState::failing(usize::MAX);
    backend
        .inner
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .expect("the in-memory store answers");
    let (tx, _rx) = broadcast::channel(64);
    let coord = LifecycleCoordinator::new(registry_with("a-1"), backend.clone(), tx);

    let result = coord
        .handle_health_report(&suspect_report("a-1"))
        .await
        .expect("the health report is handled");

    assert!(
        result.transitioned.is_empty(),
        "状态没读到，转换顺序却记下了一条：{:?}",
        result.transitioned
    );
    let (agent, reason) = result
        .failed
        .first()
        .unwrap_or_else(|| panic!("读失败既没拦住也没写进失败清单：{result:?}"));
    assert_eq!(agent, "a-1");
    assert!(
        reason.contains(UNANSWERED),
        "错误里没有存储回的那句话，读的人只能去查状态机：{reason}"
    );
    assert!(
        !reason.contains("invalid transition"),
        "把读不出来的状态当成 Init 去判，报出来的是一个没人观测过的状态：{reason}"
    );
    assert_eq!(
        backend.swaps.load(Ordering::SeqCst),
        0,
        "没读到当前状态却去 CAS 了：拿一个编出来的「当前」去换，换成了就是在这里写的假账"
    );
}

/// 转换落地了、「从」读不到：记录说 `None`，框架说 "unknown"，不编 active。
#[tokio::test]
async fn a_transition_from_an_unreadable_state_does_not_claim_a_from_state() {
    // 第一次读（记录用）失败，第二次读（判断用）成功。
    let backend = UnreadableState::failing(1);
    backend
        .inner
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .expect("the in-memory store answers");
    let (agent_tx, mut agent_rx) = broadcast::channel(64);
    let (supervisor_tx, _supervisor_rx) = broadcast::channel(64);
    let coord = LifecycleCoordinator::new(registry_with("a-1"), backend.clone(), supervisor_tx)
        .with_agent_events(agent_tx);

    let result = coord
        .handle_health_report(&suspect_report("a-1"))
        .await
        .expect("the health report is handled");

    assert_eq!(
        result.transitioned.len(),
        1,
        "判断那一次读是成功的，这条转换应当落地：{result:?}"
    );
    assert_eq!(
        result.transitioned[0].1, None,
        "「从」读不到，记录却给出了一个状态"
    );

    match agent_rx
        .try_recv()
        .expect("the landed transition is announced")
    {
        AgentEvent::StateChange { from, to, .. } => {
            assert_eq!(to, "suspect");
            assert_eq!(
                from, "unknown",
                "总线那头写着从 {from} 出发，而没有任何人读到过它"
            );
        }
        other => panic!("expected a StateChange frame, got {other:?}"),
    }
}

/// 反面对照：存储正常时，记录与框架都说得出现在真的「从」是哪个。
#[tokio::test]
async fn a_readable_state_still_names_where_the_transition_started() {
    let backend = UnreadableState::failing(0);
    backend
        .inner
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .expect("the in-memory store answers");
    let (agent_tx, mut agent_rx) = broadcast::channel(64);
    let (supervisor_tx, _supervisor_rx) = broadcast::channel(64);
    let coord = LifecycleCoordinator::new(registry_with("a-1"), backend.clone(), supervisor_tx)
        .with_agent_events(agent_tx);

    let result = coord
        .handle_health_report(&suspect_report("a-1"))
        .await
        .expect("the health report is handled");

    assert_eq!(result.transitioned[0].1, Some(AgentState::Active));
    match agent_rx
        .try_recv()
        .expect("the landed transition is announced")
    {
        AgentEvent::StateChange { from, to, .. } => {
            assert_eq!(from, "active", "读得到的状态被报成读不到：{from}");
            assert_eq!(to, "suspect");
        }
        other => panic!("expected a StateChange frame, got {other:?}"),
    }
}
