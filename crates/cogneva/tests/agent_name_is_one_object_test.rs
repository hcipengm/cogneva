//! 同一个 agent 名只对应一个活的对象。
//!
//! 名字是按 (task, role) 推出来的（`cog_core::agent_id_for`），而重跑在这套系统里
//! 是常态：清扫器把到期的 `Scheduled` 放回队列、任务失败后回投，走的都是同一条路。
//! 每次重跑都新建一个对象，就会在同一个名字下挂出若干个互不认识的消费者——它们
//! 订阅同一条 inbox、各自占一条到总线的连接、注册表里后一个覆盖前一个，而管理器
//! 对每一个都持有强引用，所以一个也不会被回收。2026-10-18 现网稳态约 8800 条连接、
//! 10.8 GiB 常驻，来源就是这条路径。
//!
//! 这条用例量的是「创建幂等」这一半：同名的第二次创建拿回的是**同一个对象**，
//! 名册长度不增。

use crate::common;

use cog_agent::GlobalAgentManager;
use cog_core::{AgentManager, MessageBackend, StateBackend};
use cog_storage::{MemoryAgentRegistry, MemoryStateBackend};
use cog_stream::MemoryMessageBackend;
use common::mocks::MockLLMProvider;
use std::sync::Arc;

fn manager() -> GlobalAgentManager {
    let registry = Arc::new(MemoryAgentRegistry::new());
    let messages: Arc<dyn MessageBackend> = Arc::new(MemoryMessageBackend::new());
    let state: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    GlobalAgentManager::new(registry, messages, state)
}

#[tokio::test]
async fn creating_the_same_name_twice_returns_the_same_agent() {
    let manager = manager();
    let llm: Arc<dyn cog_core::LlmClient> = Arc::new(MockLLMProvider::new());
    let name = cog_core::agent_id_for("task-1", "planner");

    let first = manager
        .create_agent(&name, "planner", llm.clone(), Some("task-1"))
        .await
        .expect("first create");
    let second = manager
        .create_agent(&name, "planner", llm.clone(), Some("task-1"))
        .await
        .expect("second create");

    assert!(
        Arc::ptr_eq(&first, &second),
        "the second create must hand back the live agent, not a second one"
    );
    assert_eq!(
        manager.list_workers().await.len(),
        1,
        "one name must be one worker in the roster"
    );
}

#[tokio::test]
async fn a_different_task_gets_its_own_agent() {
    let manager = manager();
    let llm: Arc<dyn cog_core::LlmClient> = Arc::new(MockLLMProvider::new());

    let a = manager
        .create_agent(
            &cog_core::agent_id_for("task-a", "planner"),
            "planner",
            llm.clone(),
            Some("task-a"),
        )
        .await
        .expect("create a");
    let b = manager
        .create_agent(
            &cog_core::agent_id_for("task-b", "planner"),
            "planner",
            llm.clone(),
            Some("task-b"),
        )
        .await
        .expect("create b");

    assert!(
        !Arc::ptr_eq(&a, &b),
        "two tasks must not share one agent object"
    );
    assert_eq!(manager.list_workers().await.len(), 2);
}
