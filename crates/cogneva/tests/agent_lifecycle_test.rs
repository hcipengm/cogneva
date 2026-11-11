use cog_agent::lifecycle::{LifecycleManager, StateTransitionHook};
use cog_core::{AgentState, StateBackend};
use cog_storage::MemoryStateBackend;
use std::sync::Arc;

fn mk_backend() -> Arc<dyn StateBackend> {
    Arc::new(MemoryStateBackend::new())
}

#[tokio::test]
async fn test_register_and_get_state() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Init));
}

#[tokio::test]
async fn test_duplicate_register_is_idempotent() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    assert!(mgr.register("a-1").await.is_ok());
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Init));
}

#[tokio::test]
async fn test_valid_transitions() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();

    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
    mgr.transition("a-1", AgentState::Completing).await.unwrap();
    mgr.transition("a-1", AgentState::Inactive).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
}

#[tokio::test]
async fn test_invalid_transition_rejected() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();

    // Dead is terminal
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
    assert!(mgr.transition("a-1", AgentState::Active).await.is_err());
}

#[tokio::test]
async fn test_transition_hook_fired() {
    let backend = mk_backend();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls2 = calls.clone();

    let hook: StateTransitionHook = Arc::new(move |id, from, to| {
        let mut v = calls2.lock().unwrap();
        v.push((id.to_string(), from, to));
    });

    let mgr = LifecycleManager::new(backend).with_transition_hook(hook);
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    let v = calls.lock().unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(
        v[0],
        ("a-1".to_string(), AgentState::Init, AgentState::Registered)
    );
}

/// 一次失败的尝试不把员工从名册上判死。
///
/// 失败的是这一条活，不是这个人：循环还在、命令通道还开着，下一次派发照样能接。
/// 把这一行写成 `Dead` 的话，监管者把它报成死的、再平衡把它排除在外，一个活着
/// 的员工从此接不到活——而每一次失败都会再判一次，谁也不会因此收到一条错误。
#[tokio::test]
async fn a_failed_attempt_does_not_take_the_employee_off_the_board() {
    struct FailingLlm;

    #[async_trait::async_trait]
    impl cog_core::LlmClient for FailingLlm {
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::LLM("upstream is down".into()))
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::LLM("upstream is down".into()))
        }

        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            Err(cog_core::SFError::LLM("upstream is down".into()))
        }

        async fn health_check(&self) -> bool {
            false
        }
    }

    let backend = mk_backend();
    let agent = cog_agent::Agent::new(
        cog_core::RuntimeConfig {
            agent_id: "a-1".into(),
            max_iterations: 1,
            ..Default::default()
        },
        Arc::new(FailingLlm),
    )
    .with_lifecycle(Arc::new(LifecycleManager::new(backend.clone())));
    agent.start().await;
    assert_eq!(
        backend.get_agent_state("a-1").await.unwrap(),
        Some(AgentState::Idle)
    );

    let outcome = agent
        .prompt(serde_json::json!({"task": "the attempt fails"}))
        .await;
    assert!(outcome.is_err(), "这一次尝试本来就该失败");
    assert_eq!(
        backend.get_agent_state("a-1").await.unwrap(),
        Some(AgentState::Idle),
        "一次失败的尝试把员工判成了死的：它还在，下一次派发还要接活"
    );

    agent.retire().await;
}

/// 死是终点，回得来只有一条路：同一个工号重新登记。
///
/// 两条判据各自对付一边。`transition` 出不去，是为了让活着的对象没法把一行
/// `Dead` 改回自己能干活的状态——那样「谁被判死了」就只是最后一次写入者的
/// 说法。`register` 回得来，是因为同一个名字重新上岗是常态：旧对象被回收、
/// 进程重启后同名重建、任务重跑再起一个。不回来的话，死过一次的员工从此读成
/// `Dead`，监管者把它报成死的、再平衡把它排除在外——一个正在干活的员工接不到活。
#[tokio::test]
async fn test_dead_comes_back_only_through_registration() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();

    assert!(
        mgr.transition("a-1", AgentState::Active).await.is_err(),
        "活着的对象不能把一行 Dead 改成自己能干活的状态"
    );
    assert_eq!(mgr.get_state("a-1").await.unwrap(), Some(AgentState::Dead));

    mgr.register("a-1").await.unwrap();
    assert_eq!(
        mgr.get_state("a-1").await.unwrap(),
        Some(AgentState::Registered),
        "同一个工号重新登记，这一行要回到「已登记」，而不是停在上一次的死上"
    );
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
}

#[tokio::test]
async fn test_idempotent_same_state_transition() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Init).await.unwrap();
    mgr.transition("a-1", AgentState::Init).await.unwrap();
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Init));
}

#[tokio::test]
async fn test_cas_prevents_concurrent_overwrite() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend.clone());
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    // Directly mutate the backend to simulate a concurrent change
    backend
        .set_agent_state("a-1", &AgentState::Dead)
        .await
        .unwrap();

    // Transition should fail because the backend state no longer matches
    let result = mgr.transition("a-1", AgentState::Active).await;
    assert!(
        result.is_err(),
        "Expected transition to fail after concurrent state change"
    );

    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Dead));
}

#[tokio::test]
async fn test_cas_retry_succeeds_on_stale_read() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend.clone());
    mgr.register("a-1").await.unwrap();

    // Transition to Registered
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    // Manually set state to Idle (simulating another process moving it forward)
    backend
        .set_agent_state("a-1", &AgentState::Idle)
        .await
        .unwrap();

    // Transition Registered -> Active should retry, see Idle, and fail
    // because Idle -> Active IS valid, but our first read was Registered.
    // Wait, with CAS: read=Registered, CAS fails because current=Idle,
    // re-read=Idle, check Idle->Active is valid, CAS succeeds.
    let result = mgr.transition("a-1", AgentState::Active).await;
    assert!(
        result.is_ok(),
        "Retry should succeed when re-read state allows transition"
    );
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Active));
}

#[tokio::test]
async fn test_all_valid_transitions() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);

    // Init -> Registered -> Active -> Idle -> Completing -> Inactive -> Active -> Idle -> Suspect -> Active -> Dead
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
    mgr.transition("a-1", AgentState::Completing).await.unwrap();
    mgr.transition("a-1", AgentState::Inactive).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
    mgr.transition("a-1", AgentState::Suspect).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
}

#[tokio::test]
async fn test_invalid_transitions_rejected() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();

    // Init -> Active (must go through Registered)
    assert!(mgr.transition("a-1", AgentState::Active).await.is_err());

    // Init -> Idle
    assert!(mgr.transition("a-1", AgentState::Idle).await.is_err());

    // Registered -> Suspect
    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    assert!(mgr.transition("a-1", AgentState::Suspect).await.is_err());

    // Dead -> anything
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
    assert!(mgr.transition("a-1", AgentState::Init).await.is_err());
    assert!(mgr.transition("a-1", AgentState::Registered).await.is_err());
    assert!(mgr.transition("a-1", AgentState::Active).await.is_err());
}

#[tokio::test]
async fn test_concurrent_transition_race() {
    let backend = mk_backend();
    let mgr = Arc::new(LifecycleManager::new(backend));

    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    // Spawn two concurrent transitions from Registered -> Active
    let mgr1 = mgr.clone();
    let mgr2 = mgr.clone();

    let t1 = tokio::spawn(async move { mgr1.transition("a-1", AgentState::Active).await });
    let t2 = tokio::spawn(async move { mgr2.transition("a-1", AgentState::Active).await });

    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();

    // At least one must succeed; the other may succeed (idempotent same-state)
    // or fail due to CAS race.
    assert!(
        r1.is_ok() || r2.is_ok(),
        "At least one concurrent transition should succeed"
    );

    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Active));
}

/// 同一个员工身上连着到的两条 Prompt，是两件活，不是一段对话。
///
/// 收件箱的 `Prompt` 说的是「开始一次新的运行」；「接着说」是 `Continue`，另一条。
/// 员工无状态：上一轮留在窗口里的对话不属于这一次，这一次从账（尝试日志）与这条
/// 消息重建。不重置的话，第二件活的首个请求里带着第一件活的请求与它工具的结果，
/// 模型把那些当成这一件的前提——它不报错，它只是照着别人的活说。
#[tokio::test]
async fn two_inbox_prompts_on_one_employee_do_not_share_a_window() {
    #[derive(Default)]
    struct RecordingLlm {
        calls: std::sync::Mutex<Vec<Vec<cog_core::Message>>>,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for RecordingLlm {
        async fn chat_stream(
            &self,
            messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.calls.lock().unwrap().push(messages.to_vec());
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            let text = r#"{"content":"ok","artifacts":[]}"#.to_string();
            tokio::spawn(async move {
                let mut producer = producer;
                let _ = producer
                    .push(cog_core::AssistantMessageEvent::TextDelta {
                        content_index: 0,
                        delta: text.clone(),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(cog_core::ChatResponse {
                    content: vec![cog_core::ContentBlock::text(text)],
                    api: "mock".into(),
                    provider: "mock".into(),
                    model: "mock".into(),
                    response_id: None,
                    usage: cog_core::Usage::default(),
                    stop_reason: cog_core::StopReason::Stop,
                    error_message: None,
                    upstream_failure: None,
                    retry_after_secs: None,
                    timestamp: chrono::Utc::now(),
                });
            });
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("the kernel loop never asks for a single-shot completion")
        }

        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            unimplemented!("the kernel loop streams")
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    let llm = Arc::new(RecordingLlm::default());
    let agent = cog_agent::Agent::new(
        cog_core::RuntimeConfig {
            agent_id: "a-inbox".into(),
            max_iterations: 1,
            ..Default::default()
        },
        llm.clone(),
    );
    agent.start().await;
    for marker in ["inbox-first", "inbox-second"] {
        agent
            .handle_inbox(cog_core::InboxMessage::Prompt {
                goal: serde_json::json!({ "task": format!("这一件活 {marker}") }),
                reply_stream: None,
                from_agent: None,
            })
            .await
            .expect("投递的这一条要真的跑起来");
    }

    // 守卫只活过这一次读：读到的请求拷一份出来，锁在 `retire().await` 之前就放掉。
    let second = {
        let calls = llm.calls.lock().unwrap();
        calls.last().expect("第二件活发出了它的请求").clone()
    };
    assert!(
        !second.iter().any(|m| m.content().contains("inbox-first")),
        "第二件活的首个请求里带着第一件活的消息：员工把上一次运行留在了进程里"
    );
    assert!(
        second.iter().any(|m| m.content().contains("inbox-second")),
        "第二件活的请求没有进来：{second:?}"
    );
    agent.retire().await;
}
