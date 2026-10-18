//! 任务走到终态时把它名下的 agent 放掉。
//!
//! 创建幂等（见 `agent_name_is_one_object_test.rs`）只挡住了「同一件任务重跑
//! 时不新建对象」这一半；另一半是「任务结束之后对象还在不在」。名字是按
//! (task, role) 推出来的，所以上一件任务的 agent 停在那里不会有人再用它，而
//! 管理器对每一个都持有强引用——不断开，它就永久占着一条到总线的连接、一条
//! 订阅和一个注册表条目。这里的四条用例量的都是「放掉」：消费者真的结束了、
//! 名册里不再出现、名字可以重新给一个新对象、退役对象不会被当成活人交回去。

mod common;

use async_trait::async_trait;
use cog_agent::GlobalAgentManager;
use cog_core::{AgentManager, MessageBackend, MessageStream, PendingStats, SFResult, StateBackend};
use cog_storage::{MemoryAgentRegistry, MemoryStateBackend};
use cog_stream::MemoryMessageBackend;
use common::mocks::MockLLMProvider;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A backend that records what was asked of it, so "the consumer was stopped"
/// is a reading rather than a claim about the code path.
///
/// It also records subscribes: the consumer loop subscribes inside the task
/// that `create_agent` spawns, so a stop issued immediately after create can
/// arrive before that task has registered anything to stop. Waiting for the
/// subscribe is what makes the test measure the stop instead of the schedule.
struct RecordingBackend {
    inner: Arc<MemoryMessageBackend>,
    subscribes: Mutex<Vec<(String, String)>>,
    stops: Mutex<Vec<(String, String)>>,
    /// When present, `stop_stream` waits here before it stops anything, so a
    /// test can hold a retirement halfway through and do something else at
    /// exactly that instant.
    stop_gate: Option<Arc<tokio::sync::Semaphore>>,
    /// When present, a stop does not return until `parties` stops are waiting
    /// at the same time. A retirement loop that stops its agents one after
    /// another never gets past the first, which is how "they were retired
    /// together" becomes a reading instead of a claim about the code.
    stop_barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl RecordingBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryMessageBackend::new()),
            subscribes: Mutex::new(Vec::new()),
            stops: Mutex::new(Vec::new()),
            stop_gate: None,
            stop_barrier: None,
        })
    }

    /// A backend whose stops all have to be waiting at once: the first one
    /// does not return until the `parties`-th arrives.
    fn barriered(parties: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryMessageBackend::new()),
            subscribes: Mutex::new(Vec::new()),
            stops: Mutex::new(Vec::new()),
            stop_gate: None,
            stop_barrier: Some(Arc::new(tokio::sync::Barrier::new(parties))),
        })
    }

    /// A backend whose stops block until `release_stop`: the retirement is
    /// asked for and recorded, but has not finished.
    fn gated() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryMessageBackend::new()),
            subscribes: Mutex::new(Vec::new()),
            stops: Mutex::new(Vec::new()),
            stop_gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
            stop_barrier: None,
        })
    }

    fn release_stop(&self) {
        if let Some(gate) = &self.stop_gate {
            gate.add_permits(1);
        }
    }

    fn stops(&self) -> Vec<(String, String)> {
        self.stops.lock().unwrap().clone()
    }

    async fn wait_for_stop(&self) {
        for _ in 0..2000 {
            if !self.stops.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the retirement never asked to stop anything");
    }

    /// Wait until the consumer loop has subscribed to `subject`, or give up
    /// after a bound that is long enough to be a real wait and short enough to
    /// fail the run rather than hang it.
    async fn wait_for_subscribe(&self, subject: &str) {
        for _ in 0..2000 {
            if self
                .subscribes
                .lock()
                .unwrap()
                .iter()
                .any(|(s, _)| s == subject)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the consumer never subscribed to {subject}");
    }
}

#[async_trait]
impl MessageBackend for RecordingBackend {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        self.inner.publish(subject, payload).await
    }

    async fn payload_limit(&self, subject: &str) -> SFResult<Option<usize>> {
        self.inner.payload_limit(subject).await
    }

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        self.subscribes
            .lock()
            .unwrap()
            .push((subject.to_string(), group.to_string()));
        self.inner.subscribe(subject, group).await
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        self.inner.subscribe_from(subject, group, start_id).await
    }

    async fn stop_stream(&self, subject: &str, group: &str) {
        self.stops
            .lock()
            .unwrap()
            .push((subject.to_string(), group.to_string()));
        if let Some(gate) = &self.stop_gate {
            gate.acquire().await.expect("stop gate").forget();
        }
        if let Some(barrier) = &self.stop_barrier {
            barrier.wait().await;
        }
        self.inner.stop_stream(subject, group).await;
    }

    async fn create_consumer_group(&self, stream: &str, group: &str) -> SFResult<()> {
        self.inner.create_consumer_group(stream, group).await
    }

    async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
        self.inner.ack(stream, group, ids).await
    }

    async fn claim_pending(
        &self,
        stream: &str,
        group: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        self.inner
            .claim_pending(stream, group, min_idle_ms, count)
            .await
    }

    async fn pending_stats(
        &self,
        stream: &str,
        group: &str,
        idle_threshold_ms: u64,
    ) -> SFResult<Option<PendingStats>> {
        self.inner
            .pending_stats(stream, group, idle_threshold_ms)
            .await
    }

    async fn stream_lag(&self, stream: &str, group: &str) -> SFResult<Option<u64>> {
        self.inner.stream_lag(stream, group).await
    }
}

fn manager(backend: Arc<RecordingBackend>) -> GlobalAgentManager {
    let registry = Arc::new(MemoryAgentRegistry::new());
    let messages: Arc<dyn MessageBackend> = backend;
    let state: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    GlobalAgentManager::new(registry, messages, state)
}

fn llm() -> Arc<dyn cog_core::LlmClient> {
    Arc::new(MockLLMProvider::new())
}

/// 退役要做的第一件事是把消费者停下来，而且停的是这个 agent 那一条订阅。
#[tokio::test]
async fn retiring_a_task_agent_ends_its_consumer() {
    let backend = RecordingBackend::new();
    let manager = manager(Arc::clone(&backend));
    let name = cog_core::agent_id_for("task-1", "planner");
    manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("create");
    let inbox = format!("orchestrator:agent:{name}:inbox");
    backend.wait_for_subscribe(&inbox).await;

    let retired = manager.retire_task_agents("task-1").await;

    assert_eq!(retired, 1, "the task owned exactly one agent");
    assert_eq!(
        backend.stops(),
        vec![(inbox.clone(), format!("agent-{name}"))],
        "the stop must name this agent's own inbox and group"
    );
    assert!(
        manager.list_workers().await.is_empty(),
        "a retired agent is not a worker"
    );
}

/// 名册里不再有它，按名字也取不回来——退役对象不是「还能用的活人」。
#[tokio::test]
async fn a_retired_agent_is_not_listed_and_cannot_be_looked_up() {
    let backend = RecordingBackend::new();
    let manager = manager(Arc::clone(&backend));
    let name = cog_core::agent_id_for("task-1", "planner");
    manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("create");
    backend
        .wait_for_subscribe(&format!("orchestrator:agent:{name}:inbox"))
        .await;

    // 直接退这一个对象，名册的清理留给循环——验的是名册的读者看不到它，
    // 而不是它已经被摘掉。
    let agent = manager.list_workers().await.remove(0).agent;
    let report = agent.retire().await;

    assert!(agent.is_retired());
    assert!(
        report.performed,
        "the first retire is the one that removes it"
    );
    assert!(
        report.consumer_ended,
        "the consumer must be joined out, not merely asked to stop: a flag \
         read between reads leaves at most one block period to wait"
    );
    assert!(!report.was_busy, "an idle agent has nothing in flight");
    assert!(
        manager.list_workers().await.is_empty(),
        "list_workers is read by the resume path; a retired id resolving to \
         nothing must not be handed back as a worker"
    );
    assert!(
        manager.get_agent(&name).await.expect("lookup").is_none(),
        "a retired agent must not answer a lookup by name"
    );
}

/// 退役之后再建同名对象，拿到的是新的那一个：旧的对象已经答不出话了。
#[tokio::test]
async fn creating_after_retirement_yields_a_new_identity() {
    let backend = RecordingBackend::new();
    let manager = manager(Arc::clone(&backend));
    let name = cog_core::agent_id_for("task-1", "planner");
    let first = manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("first create");
    backend
        .wait_for_subscribe(&format!("orchestrator:agent:{name}:inbox"))
        .await;
    let retired = manager.retire_task_agents("task-1").await;
    assert_eq!(retired, 1);

    let second = manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("second create");

    assert!(
        !Arc::ptr_eq(&first, &second),
        "the retired object must not be handed back as the live one"
    );
    let live = manager.list_workers().await;
    assert_eq!(live.len(), 1, "one name is still one worker");
    assert!(!live[0].agent.is_retired());
}

/// 名册里还留着退役对象时（清理尚未发生），同名创建也必须换新的，而不是把
/// 空壳交出去。这正是「上一次生命留下的对象」与「这个名字还活着」的区别。
#[tokio::test]
async fn a_create_finds_the_new_object_when_the_roster_still_holds_the_retired_one() {
    let backend = RecordingBackend::new();
    let manager = manager(Arc::clone(&backend));
    let name = cog_core::agent_id_for("task-1", "planner");
    let first = manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("first create");
    backend
        .wait_for_subscribe(&format!("orchestrator:agent:{name}:inbox"))
        .await;
    manager.list_workers().await.remove(0).agent.retire().await;

    let second = manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("second create");

    assert!(
        !Arc::ptr_eq(&first, &second),
        "the shell of the previous life is not the agent under this name"
    );
    let live = manager.list_workers().await;
    assert_eq!(live.len(), 1);
    assert!(
        live[0].agent_id == name && !live[0].agent.is_retired(),
        "the roster must hold the new object, not the retired one"
    );
}

/// 退役的收尾发生在「新对象已经挂到同一个名字下」之后：收尾只能放掉自己退掉的
/// 那一个对象，新来的那一个要留在名册里。
///
/// 这是「按身份摘」与「按名字摘」唯一可分辨的交错。实测：把收尾换成按名字摘
/// （名字从待退目标上带下来，实现者手上本来就有它），名册被清空，这一条红；
/// 前四条仍然全绿。反过来，若「按名字摘」是收尾时才去名册里查名字，它查不到
/// 已经退场的空壳、也就不摘任何东西——那种写法是失效而不是等价，测不出差别，
/// 这正说明判据要对着「名字从哪儿来」而不是对着「名字用没用」。
#[tokio::test]
async fn a_late_retirement_cleanup_does_not_take_the_new_object_with_it() {
    let backend = RecordingBackend::gated();
    let manager = Arc::new(manager(Arc::clone(&backend)));
    let name = cog_core::agent_id_for("task-1", "planner");
    let first = manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("first create");
    backend
        .wait_for_subscribe(&format!("orchestrator:agent:{name}:inbox"))
        .await;

    // 退役停在半路：对象已标记退役、停的动作已被记录，但管理器还没把它摘掉。
    let retirer = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move { manager.retire_task_agents("task-1").await })
    };
    backend.wait_for_stop().await;

    // 此刻另一次重跑给同一个名字建了新的对象。
    let second = manager
        .create_agent(&name, "planner", llm(), Some("task-1"))
        .await
        .expect("second create");
    assert!(!Arc::ptr_eq(&first, &second), "a new life, a new object");

    backend.release_stop();
    assert_eq!(retirer.await.expect("join"), 1, "one object was retired");

    let live = manager.list_workers().await;
    assert_eq!(
        live.len(),
        1,
        "the cleanup took the new object with it: it removed by name, not by \
         the object it had retired"
    );
    let kept: Arc<dyn cog_core::Agent> = live[0].agent.clone();
    assert!(
        Arc::ptr_eq(&kept, &second),
        "the roster must keep the object created while the retirement was \
         still finishing"
    );
}

/// 一件任务名下的多个对象是一起退的：三个消费者都停在闸门上，收尾才走得下去。
///
/// 这条判据量的不是快慢，是「它们同时在下」。串行退的实现永远等不到第三个到达——
/// 第一个就卡在闸门上——所以把 `join_all` 改回 `for` 循环，它会在这里超时失败。
#[tokio::test]
async fn a_tasks_agents_stop_together_rather_than_one_after_another() {
    let roles = ["planner", "coder", "reviewer"];
    let backend = RecordingBackend::barriered(roles.len());
    let manager = manager(Arc::clone(&backend));
    for role in roles {
        let name = cog_core::agent_id_for("task-1", role);
        manager
            .create_agent(&name, role, llm(), Some("task-1"))
            .await
            .expect("create");
        backend
            .wait_for_subscribe(&format!("orchestrator:agent:{name}:inbox"))
            .await;
    }

    let retired =
        tokio::time::timeout(Duration::from_secs(2), manager.retire_task_agents("task-1"))
            .await
            .expect(
                "the remaining stops never arrived: the agents were retired one after \
             another, so the first one held the barrier alone",
            );

    assert_eq!(retired, roles.len(), "every object was retired");
    assert_eq!(
        backend.stops().len(),
        roles.len(),
        "each agent's own inbox was stopped"
    );
    assert!(manager.list_workers().await.is_empty());
}
