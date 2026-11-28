use cog_core::{AgentState, SFError, SFResult, StateBackend};
use std::sync::Arc;

/// Hook called on every valid state transition.
/// The hook receives `(agent_id, old_state, new_state)`.
/// Errors are logged but do not block the transition.
use AgentState::*;

pub type StateTransitionHook = Arc<dyn Fn(&str, AgentState, AgentState) + Send + Sync>;

/// 员工生命周期这一行状态的那台机器。
///
/// 它只做一件事：把「这一位现在处于哪一格」按一张固定的转换表校验、用比较交换
/// 落到持久面上，再把这次转变告诉钩子。员工对象自己的相位（`AgentInner::state`）
/// 不进这里——那一格是「这个对象这一刻在干什么」，进程重启就没了；这里的一行
/// 是「这一位在系统里处于什么状态」，读它的是监管者、调度与界面。
///
/// 谁写、谁判死活，分开：
///
/// - 写入只有两条路，都经过这台机器的转换表与 CAS：员工对象在开跑、结束、让位、
///   离职时报告自己的相位；监管者按健康报告判出 `Suspect` / `Dead`（见
///   `cog-supervisor` 的 `lifecycle_coordinator`）。谁都不直接 `SET` 这一行。
/// - 「还活着吗」的判据不在这里。曾经有一个自带倒计时的心跳：它数的是自己发的
///   tick，从不看对方有没有回应，于是接上它的每个员工都会在几十秒后被自己判死——
///   系统宣布一个正在干活的员工死了，而没有任何一方报错。存活的证据是名册上的
///   心跳与监管者的健康报告；这里只负责把判决写成一行合法的状态。
pub struct LifecycleManager {
    backend: Arc<dyn StateBackend>,
    transition_hook: Option<StateTransitionHook>,
}

impl LifecycleManager {
    pub fn new(backend: Arc<dyn StateBackend>) -> Self {
        Self {
            backend,
            transition_hook: None,
        }
    }

    pub fn with_transition_hook(mut self, hook: StateTransitionHook) -> Self {
        self.transition_hook = Some(hook);
        self
    }

    /// 登记一个员工，已有的行保持不动。
    ///
    /// 没有行就等于 `Init`。已经有一行时是幂等的——除了那一行是 `Dead`：同一个
    /// 工号重新登记，说明这一位又上岗了（旧对象已经离职、被回收，或者进程重启
    /// 后同一个名字重新建了一个对象）。这不是临终那一刻的延续，而是新的一次
    /// 上岗，所以直接把它摆回 `Registered`。名册那边是同一个道理：离职的墓碑在
    /// 重新登记时抹掉，同一个人不会因为离开过一次就永远回不来。
    ///
    /// `transition` 仍然把 `Dead` 当终点：活着的对象不能从死人那一格爬回来，
    /// 能回来的只有**重新登记**的这一个——走过的是一次带新对象的入口，不是一次
    /// 状态转换。不这么做的话，死过一次的员工重新上岗后仍然读成 `Dead`：监管者
    /// 把它报成死的、再平衡把它排除在外，一个正在干活的员工从此接不到活。
    pub async fn register(&self, agent_id: &str) -> SFResult<()> {
        match self.backend.get_agent_state(agent_id).await? {
            None => {
                self.backend
                    .set_agent_state(agent_id, &AgentState::Init)
                    .await?;
            }
            Some(AgentState::Dead) => {
                let mut swapped = self
                    .backend
                    .cas_agent_state(agent_id, &AgentState::Dead, &AgentState::Registered)
                    .await?;
                if !swapped {
                    // 并发里有人先动过这一行：已经不是 Dead 就说明它在前一次转换
                    // 里走到了别的格子上，登记这件事已经成立；仍然是 Dead 就重试
                    // 一次，两次都输只可能是持久面在反复改写同一行。
                    let now = self.backend.get_agent_state(agent_id).await?;
                    if matches!(now, Some(AgentState::Dead)) {
                        swapped = self
                            .backend
                            .cas_agent_state(agent_id, &AgentState::Dead, &AgentState::Registered)
                            .await?;
                    } else {
                        swapped = true;
                    }
                }
                if !swapped {
                    return Err(SFError::Agent(format!(
                        "agent {agent_id} is Dead and the row kept changing under re-registration; \
                         it stays off the board until the next register"
                    )));
                }
                if let Some(ref hook) = self.transition_hook {
                    hook(agent_id, AgentState::Dead, AgentState::Registered);
                }
            }
            Some(_) => {}
        }
        Ok(())
    }

    /// Transition an agent to a new state, validating the transition first.
    /// Uses compare-and-swap (CAS) to prevent concurrent overwrites.
    /// If the backend state has changed since we read it, the operation
    /// is retried once with the fresh current state.
    pub async fn transition(&self, agent_id: &str, to: AgentState) -> SFResult<()> {
        let mut from = self
            .backend
            .get_agent_state(agent_id)
            .await?
            .unwrap_or(AgentState::Init);

        if !Self::is_valid_transition(from, to) {
            return Err(SFError::Agent(format!(
                "invalid transition: {:?} -> {:?} for agent {}",
                from, to, agent_id
            )));
        }

        // Attempt CAS; if it fails, re-read and retry once.
        let mut swapped = self.backend.cas_agent_state(agent_id, &from, &to).await?;
        if !swapped {
            from = self
                .backend
                .get_agent_state(agent_id)
                .await?
                .unwrap_or(AgentState::Init);
            if !Self::is_valid_transition(from, to) {
                return Err(SFError::Agent(format!(
                    "invalid transition: {:?} -> {:?} for agent {}",
                    from, to, agent_id
                )));
            }
            swapped = self.backend.cas_agent_state(agent_id, &from, &to).await?;
        }

        if !swapped {
            return Err(SFError::Agent(format!(
                "CAS failed for agent {}: concurrent state change detected",
                agent_id
            )));
        }

        if let Some(ref hook) = self.transition_hook {
            hook(agent_id, from, to);
        }

        Ok(())
    }

    /// Get the current lifecycle state of an agent.
    pub async fn get_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        self.backend.get_agent_state(agent_id).await
    }

    fn is_valid_transition(from: AgentState, to: AgentState) -> bool {
        match (from, to) {
            // Init can go to Registered or Dead
            (Init, Registered) | (Init, Dead) => true,
            // Registered can go to Active, Idle, Inactive, or Dead
            (Registered, Active)
            | (Registered, Idle)
            | (Registered, Inactive)
            | (Registered, Dead) => true,
            // Active can go to Idle, Completing, Suspect, or Dead
            (Active, Idle) | (Active, Completing) | (Active, Suspect) | (Active, Dead) => true,
            // Idle can go to Active, Completing, Suspect, or Dead
            (Idle, Active) | (Idle, Completing) | (Idle, Suspect) | (Idle, Dead) => true,
            // Completing can go to Idle, Inactive, or Dead
            (Completing, Idle) | (Completing, Inactive) | (Completing, Dead) => true,
            // Inactive can go to Registered, Active, or Dead
            (Inactive, Registered) | (Inactive, Active) | (Inactive, Dead) => true,
            // Suspect can recover to Active or go to Dead
            (Suspect, Active) | (Suspect, Dead) => true,
            // 死是终点：一次转换出不去。回得来只有一条路，`register` 那次
            // 「同一个工号重新上岗」——那不是这一格上的转换，是一个新对象的入口。
            (Dead, _) => false,
            // Same-state transitions are idempotent
            (a, b) if a == b => true,
            _ => false,
        }
    }
}
