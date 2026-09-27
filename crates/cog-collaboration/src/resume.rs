//! 派发时接回上一任持有的进度。
//!
//! 任务的进度在它自己的进程里，进程一死就只剩已经落盘的那一份：持有任务的
//! 进程按自己的节拍把检查点写下来（产出侧，见 `cog-orchestrator` 的
//! `task_checkpoint`），并把可恢复的那一份的 id 挂在任务的板上；派发端在这里
//! 把它读回来。
//!
//! 两件事必须同源：字段名（[`cog_core::checkpoint_field`]）与 agent 的名字
//! （[`cog_core::agent_id_for`]）。任何一处手写字面量，产出与恢复就会各自指向
//! 不同的东西，而这条链失败的样子是「任务从头重跑」——与第一次执行同形。

use std::sync::Arc;

use cog_core::{Agent, AgentManager, LlmClient, SFResult, StateBackend};

/// 恢复端可能的结局，闭集。
///
/// 两个都发布（零也发布）：`restored` 恒为 0 就是「续跑一次都没发生过」——
/// 那与「没有任务需要续跑」是两件事，只看成功计数器分不出来。
pub const RESUME_OUTCOMES: [&str; 2] = ["restored", "failed"];

/// 建一个角色 agent，并把这个 (task, role) 的续跑点接回来（有的话）。
///
/// 恢复必须发生在 agent 开跑之前：`restore` 会 abort 在跑的任务并重置命令
/// 通道，那是「把状态换成快照里的状态」的实现方式，对已经开工的 agent 调用
/// 等于把当前这一步丢掉。
pub(crate) async fn create_role_agent(
    manager: &Arc<dyn AgentManager>,
    llm: &Arc<dyn LlmClient>,
    state: Option<&Arc<dyn StateBackend>>,
    task_id: &str,
    role: &str,
) -> SFResult<Arc<dyn Agent>> {
    let agent = manager
        .create_agent(&cog_core::agent_id_for(task_id, role), role, llm.clone())
        .await?;
    resume_if_checkpointed(state, &agent, task_id, role).await;
    Ok(agent)
}

/// 板上有这个角色的续跑指针就恢复，返回是否真的恢复了。
///
/// 没有指针是常态（第一次执行就是），不是错误路径；有指针却恢复失败要看得见：
/// 那意味着一条指向不存在快照的路标，或者一份读不出来的快照——两者都会让任务
/// 从零重跑，而重跑与「本来就没有续跑点」在日志之外的面上完全同形。
pub(crate) async fn resume_if_checkpointed(
    state: Option<&Arc<dyn StateBackend>>,
    agent: &Arc<dyn Agent>,
    task_id: &str,
    role: &str,
) -> bool {
    let Some(state) = state else {
        return false;
    };
    let field = cog_core::checkpoint_field(role);
    let pointer = match state.get_board(task_id).await {
        Ok(Some(board)) => board.fields.get(&field).cloned(),
        Ok(None) => None,
        Err(e) => {
            tracing::warn!(
                task = task_id,
                role,
                error = %e,
                "cannot read the task board: whether this role has a resume point is unknown"
            );
            crate::observable::global_observable().record_resume("failed");
            return false;
        }
    };

    let Some(checkpoint_id) = pointer else {
        return false;
    };

    match agent.restore_from_id(&checkpoint_id).await {
        Ok(()) => {
            tracing::info!(
                task = task_id,
                role,
                checkpoint = %checkpoint_id,
                "resumed a task from the checkpoint its previous holder left"
            );
            crate::observable::global_observable().record_resume("restored");
            true
        }
        Err(e) => {
            tracing::warn!(
                task = task_id,
                role,
                checkpoint = %checkpoint_id,
                error = %e,
                "the resume point on the task board could not be restored; this run starts from the beginning"
            );
            crate::observable::global_observable().record_resume("failed");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{AgentCheckpoint, ContextBoard, Event, TaskCheckpoint};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 只答板字段的 StateBackend。
    #[derive(Default)]
    struct BoardBackend {
        fields: Mutex<HashMap<String, String>>,
        read_fails: bool,
    }

    #[async_trait::async_trait]
    impl StateBackend for BoardBackend {
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
            if self.read_fails {
                return Err(cog_core::SFError::Database("board unavailable".into()));
            }
            let fields = self
                .fields
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if fields.is_empty() {
                return Ok(None);
            }
            Ok(Some(ContextBoard {
                task_id: task_id.to_string(),
                fields,
                updated_at: chrono::Utc::now(),
            }))
        }
        async fn set_board_field(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> SFResult<()> {
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

    /// 记下被恢复的那个 checkpoint id 的 agent 替身。
    ///
    /// 记录表放在替身**外面**：断言经 `dyn Agent` 读的是同一份，不需要为了
    /// 取回具体类型再走一次向下转型。
    #[derive(Default)]
    struct RestoreRecorder {
        restored: Arc<Mutex<Vec<String>>>,
        /// 让恢复失败，用来验证「有指针但恢复不了」这条路。
        restore_fails: bool,
    }

    impl RestoreRecorder {
        /// 返回替身与它的记录表（分开持有，测试里两半都看得到）。
        fn recording(restore_fails: bool) -> (Arc<dyn Agent>, Arc<Mutex<Vec<String>>>) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let agent = RestoreRecorder {
                restored: seen.clone(),
                restore_fails,
            };
            (Arc::new(agent), seen)
        }
    }

    #[async_trait::async_trait]
    impl Agent for RestoreRecorder {
        async fn prompt(&self, _input: serde_json::Value) -> SFResult<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
        async fn start(&self) {}
        async fn snapshot(&self, _task_id: String) -> SFResult<AgentCheckpoint> {
            Err(cog_core::SFError::Agent("not used".into()))
        }
        async fn restore(&self, _snapshot: &AgentCheckpoint) -> SFResult<()> {
            Ok(())
        }
        async fn restore_from_id(&self, checkpoint_id: &str) -> SFResult<()> {
            if self.restore_fails {
                return Err(cog_core::SFError::Agent("checkpoint not found".into()));
            }
            self.restored
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(checkpoint_id.to_string());
            Ok(())
        }
        // 这条链只用得到 prompt/snapshot/restore/restore_from_id；其余方法在
        // 替身上没有意义，用显式的「没实现」而不是空实现——空实现会让「测试
        // 走过一条真实现才有的路」看不出来。
        async fn continue_(&self, _input: serde_json::Value) -> SFResult<serde_json::Value> {
            Err(cog_core::SFError::NotImplemented("continue_".into()))
        }
        async fn steer(&self, _instruction: String) -> SFResult<()> {
            Err(cog_core::SFError::NotImplemented("steer".into()))
        }
        async fn abort(&self) -> SFResult<()> {
            Ok(())
        }
        async fn reset(&self) -> SFResult<()> {
            Ok(())
        }
        async fn state(&self) -> SFResult<cog_core::AgentState> {
            Err(cog_core::SFError::NotImplemented("state".into()))
        }
        async fn wait_for_idle(&self) -> SFResult<()> {
            Ok(())
        }
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::NotImplemented("chat_stream".into()))
        }
        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            Err(cog_core::SFError::NotImplemented("complete_stream".into()))
        }
        async fn read_board(&self, _task_id: &str, _field: &str) -> SFResult<Option<String>> {
            Ok(None)
        }
        async fn write_board(&self, _task_id: &str, _field: &str, _value: &str) -> SFResult<()> {
            Ok(())
        }
        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> SFResult<()> {
            Ok(())
        }
    }

    fn board_with(role: &str, checkpoint_id: &str) -> Arc<BoardBackend> {
        let backend = BoardBackend::default();
        backend
            .fields
            .lock()
            .unwrap()
            .insert(cog_core::checkpoint_field(role), checkpoint_id.to_string());
        Arc::new(backend)
    }

    fn as_state(backend: &Arc<BoardBackend>) -> Arc<dyn StateBackend> {
        backend.clone()
    }

    #[tokio::test]
    async fn a_pointer_on_the_board_is_restored_before_the_agent_runs() {
        let backend = board_with("planner", "cp-42");
        let state = as_state(&backend);
        let (agent, seen) = RestoreRecorder::recording(false);

        let resumed = resume_if_checkpointed(Some(&state), &agent, "t-1", "planner").await;

        assert!(resumed);
        assert_eq!(*seen.lock().unwrap(), vec!["cp-42".to_string()]);
    }

    #[tokio::test]
    async fn a_role_without_a_pointer_is_left_alone() {
        let backend = board_with("planner", "cp-42");
        let state = as_state(&backend);
        let (agent, seen) = RestoreRecorder::recording(false);

        // 同一个任务、另一个角色：它自己没有续跑点，不该被别人的指针恢复。
        let resumed = resume_if_checkpointed(Some(&state), &agent, "t-1", "generator").await;

        assert!(!resumed);
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_empty_board_is_not_an_error() {
        let backend = Arc::new(BoardBackend::default());
        let state = as_state(&backend);
        let (agent, seen) = RestoreRecorder::recording(false);

        assert!(!resume_if_checkpointed(Some(&state), &agent, "t-1", "planner").await);
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_state_backend_means_no_resume_not_a_panic() {
        let (agent, seen) = RestoreRecorder::recording(false);
        assert!(!resume_if_checkpointed(None, &agent, "t-1", "planner").await);
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_pointer_that_cannot_be_restored_is_reported_as_a_failure() {
        let backend = board_with("planner", "cp-gone");
        let state = as_state(&backend);
        let (agent, seen) = RestoreRecorder::recording(true);

        let resumed = resume_if_checkpointed(Some(&state), &agent, "t-1", "planner").await;

        assert!(!resumed, "a failed restore is not a resumed task");
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unreadable_board_is_not_the_same_as_no_pointer() {
        let backend = Arc::new(BoardBackend {
            read_fails: true,
            ..Default::default()
        });
        let state = as_state(&backend);
        let (agent, seen) = RestoreRecorder::recording(false);

        assert!(!resume_if_checkpointed(Some(&state), &agent, "t-1", "planner").await);
        assert!(seen.lock().unwrap().is_empty());
    }
}
