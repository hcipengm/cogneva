//! HTTP handlers for the Agent registration / heartbeat endpoints.
//! Wires the [`cog_core::AgentRegistry`] trait to the gateway, exposing:
//! - `POST /api/v1/agents/register` — register a new Agent
//! - `POST /api/v1/agents/{id}/heartbeat` — renew TTL
//! - `DELETE /api/v1/agents/{id}` — graceful deregister
//! - `GET /api/v1/agents/{id}` — fetch a registration
//! - `GET /api/v1/agents` — list live Agents

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;
use cog_core::{AgentRegistration, Claims, ResourceInfo};
use cog_core::{NewAgentMessage, SendOutcome};

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub hostname: String,
    pub pod_ip: String,
    pub role: String,
    pub workspace_id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub resources: ResourceInfo,
    /// Optional client-supplied uuid for deterministic agent_id.  When `None`
    /// we generate a fresh `uuid_v4` so multiple instances of the same pod
    /// don't collide on `blake3(hostname|ip|role)`.
    #[serde(default)]
    pub uuid: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub agent_id: String,
    pub registration: AgentRegistration,
}

pub async fn register_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<RegisterRequest>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };

    let uuid = req.uuid.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let agent_id = cog_core::generate_agent_id(&req.hostname, &req.pod_ip, &req.role, &uuid);
    let now = chrono::Utc::now();
    let registration = AgentRegistration {
        agent_id: agent_id.clone(),
        role: req.role,
        workspace_id: req.workspace_id,
        capabilities: req.capabilities,
        resources: req.resources,
        hostname: req.hostname,
        pod_ip: req.pod_ip,
        registered_at: now,
        last_heartbeat: now,
    };

    match registry.register(&registration).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(RegisterResponse {
                agent_id,
                registration,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// 属主在工作台上招一个员工。
///
/// 这一步之前，名册上的人只有两种来路：进程启动时自报的常驻工人，和一件任务派
/// 生出来的小队成员。两者都不是「人可以在工作台上点出来」的那种——属主想多一个
/// 能说话的员工，没有任何入口。
///
/// 建出来的就是一个进程生命周期内的通用员工：它自己订阅收件箱，谁都能对它说话，
/// 它也能接平台上的活。它不带角色、不带提示词——角色是注册表上的一列标签，不是
/// 一段写死的性格；能力来自它和别的员工同一套工具与同一条基础指令。
///
/// 名字来自名册行：注册那一刻发一个工号，姓名由工号算出来（`employee_label`），
/// 于是「工号 姓名」永不重复，重读也不会改名。
pub async fn create_handler(
    State(state): State<Arc<GatewayState>>,
    caller: Option<Extension<Claims>>,
) -> Response {
    let Some(pool) = state.agent_pool.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "this deployment runs no agent pool in this process, so an employee hired here would have nothing to run it",
            })),
        )
            .into_response();
    };
    let llm = state
        .llm_client
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let Some(llm) = llm else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "no model is configured yet: an employee needs one before it can be hired",
            })),
        )
            .into_response();
    };

    let agent_id = format!("employee:{}", new_employee_suffix());
    let words = caller
        .as_ref()
        .map(|Extension(claims)| claims.preferred_username.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| WEBUI_SENDER.to_string());
    match pool.create_agent(&agent_id, EMPLOYEE_ROLE, llm, None).await {
        Ok(_) => {
            // 写名册写在建人成功之后：反过来的话，一次建失败会在名册上留下
            // 一个从没存在过的人，维护环每次对账都要替它失败一遍。
            let persistence: Option<String> = match state.key_value.as_ref() {
                Some(kv) => match enroll(kv, &agent_id, EMPLOYEE_ROLE, &words).await {
                    Ok(()) => None,
                    Err(e) => {
                        tracing::warn!(
                            agent_id = %agent_id,
                            error = %e,
                            "the employee exists in this process but could not be written to the roster"
                        );
                        Some(e)
                    }
                },
                None => Some(
                    "this deployment keeps no key-value store, so the roster cannot outlive this process"
                        .to_string(),
                ),
            };
            tracing::info!(
                agent_id,
                hired_by = %words,
                persisted = persistence.is_none(),
                "an employee joined the roster"
            );
            // 展示名取自名册那一行：工号是这个人唯一的编号来源。读不到就印
            // 「待编」——不退回机器码，那串十六进制看着像名字，于是没人去补号。
            let label = match state.agent_registry.as_ref() {
                Some(registry) => registry
                    .employee(&agent_id)
                    .await
                    .ok()
                    .flatten()
                    .map(|row| row.label()),
                None => None,
            }
            .unwrap_or_else(|| cog_core::unregistered_label(&agent_id));
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "display_name": label,
                    "hired_by": words,
                    "persisted": persistence.is_none(),
                    "persistence_error": persistence,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// 员工 id 的后半截。
///
/// 取随机而不按名字派生：同一个属主可以把两个员工都叫「阿蓝」，名字不是身份，
/// id 才是。短前缀让手写与日志里都认得出来这是一个员工，`employee:` 也是读面
/// 分辨它与小队成员（`squad:`）的凭据。
fn new_employee_suffix() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(8)
        .collect()
}

/// 名册在持久存储里的那一份。
///
/// 进程里的员工池是内存的：Pod 重启，池里一个人都不剩。名册不能跟着池一起
/// 归零——「一直在岗」说的正是重启之后这些人还在。所以每位员工入职时写一行到
/// 共享存储，维护环按这一份把人重新建回来。
///
/// 用一张 hash 而不是每人一个键：恢复时要一次读出全部，而键空间没有遍历。
pub const ROSTER_KEY: &str = "cogneva:roster";

/// 名册维护环的名字。意外退出会按这个名字记一次重启，读面上认得出来。
pub const ROSTER_KEEPER_LOOP: &str = "gateway_roster_keeper";

/// 名册维护的节拍。
///
/// 一分钟一次：要盯的事只有「池里少了名册上的人」，那只有进程重启或员工被收走
/// 时才会发生；比这更密只是对同一条 hash 做无谓的读。
// cap: bound — 名册维护的节拍；它盯的变化只在进程重启或员工被收走时发生，不是量出来的
pub const ROSTER_KEEPER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// 「说过话的人」名册：名下没有常驻进程、只在说话那一小段时间里活着的员工。
///
/// 界面上的每一段对话就是这样一个人：名字由会话推出来，说话的那几分钟它挂在
/// 注册表上，说完就注销。注册表回答的是「此刻叫得到谁」，而人要看的是「我跟
/// 谁说过话、以后还找不找得到他」——这两个问题不一样，注册表上的 TTL 一过，
/// 后者就读成「这个人从来没存在过」。
///
/// 所以另记一份不过期的名册。用一张 hash：读面一次读出全部，键空间没有遍历。
pub const PEOPLE_KEY: &str = "cogneva:people";

/// 说过话的一位员工。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersonRow {
    /// 这段对话的会话 id。名字由它算出来，接着聊也要按它投回同一个人；
    /// 丢了这一格，名单上这个人就只剩一个念不回去的名字。
    pub session_id: String,
    /// 这段会话第一次说出自己是谁时算出的身份种子（`Kai#a9ab11fc`）。它是
    /// **种子，不是展示名**：展示名只有一处派生，来自 [`cog_core::EmployeeRecord`]
    /// 的工号；读面拿这一格只用来在绑定行缺失时把同一个人认回来（`persona#short`）。
    /// 把它当名字印出去，同一个人就会在名册上以 `E0412 林清和`、在对话里以
    /// `Kai#a9ab11fc` 各出现一次。
    pub display_name: String,
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
    /// 这位是从哪儿来的：界面发 `web`，别的客户端自己声明。
    ///
    /// 名字由会话哈希出来，读的人从名字上分不出「有人坐在这儿聊」和「一个探针
    /// 开了一个会话、说了一句、再也没回来」——2026-11-06 名册上那 29 行全是后者，
    /// `first_seen` 与 `last_seen` 落在同一秒里。来源写在行上，名单才有得筛。
    ///
    /// 没有这位的旧行按 `None` 读：那时候还没人记来源，编一个等于假装量过。
    #[serde(default)]
    pub platform: Option<String>,
    /// 同一台设备的同一个客户端。重开一次会话还是这一格，换台机器就不是。
    #[serde(default)]
    pub device_id: Option<String>,
}

/// 一个会话落在哪位员工身上：一张不过期的表，键是会话 id。
///
/// 名字本来是「实例指纹 ＋ 会话」算出来的，于是滚一次 Pod 换一个指纹，同一个
/// 会话就变成一个从没见过的员工——界面上两处读的是同一份名册，看到的却是两个
/// 人。会话绑一次人，之后无论哪个实例接手都按这一行认人。
pub const SESSION_EMPLOYEE_KEY: &str = "cogneva:session_employee";

/// 绑在会话上的那一位。
///
/// 与 [`PersonRow`] 分开记：那一行是「说过话的人」，这一行是「这段对话是谁」。
/// 一个人可以在好几段对话里说话，那时名册上有好几行，绑定的方向是会话 → 人。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEmployee {
    pub agent_id: String,
    pub persona: String,
    pub short: String,
    pub handle: String,
    /// 这一位跨重启还是不是同一个人。绑定的意义就是把它变成「是」，所以读面
    /// 可以拿这一位判断这行是不是绑出来的。
    #[serde(default)]
    pub stable: bool,
}

/// 读一段会话绑定的人。没有绑定、或那一行读不懂时回 `None`，让调用方去分配。
pub async fn read_bound_session(
    kv: &dyn cog_core::KeyValueStore,
    session_id: &str,
) -> Option<SessionEmployee> {
    let text = match kv.hget(SESSION_EMPLOYEE_KEY, session_id).await {
        Ok(text) => text?,
        Err(e) => {
            tracing::warn!(error = %e, session_id, "a session binding could not be read");
            return None;
        }
    };
    match serde_json::from_str::<SessionEmployee>(&text) {
        Ok(row) => Some(row),
        Err(e) => {
            tracing::warn!(
                error = %e,
                session_id,
                "a session binding does not parse; the next turn re-mints it"
            );
            None
        }
    }
}

/// 把一段会话绑到一位员工身上。后写覆盖先写是有意的：调用方只在表里没有这一
/// 行时才写，两个实例同时首轮撞上时谁先写谁定，之后读到的都是同一行。
pub async fn write_bound_session(
    kv: &dyn cog_core::KeyValueStore,
    session_id: &str,
    row: &SessionEmployee,
) -> cog_core::SFResult<()> {
    let text = serde_json::to_string(row)?;
    kv.hset(SESSION_EMPLOYEE_KEY, session_id, &text).await
}

/// 这段会话在「说过话的人」名册上的那一行。
///
/// 绑定的表是后加的：在这之前说过话的会话没有绑定，而它们的名册行就是它们的
/// 人。新表的第一轮先认这一行，认不出来才新发一个名字——不然同一个会话会在名
/// 单上变成两个人，而那正是要修的那个病。
pub async fn person_for_session(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    session_id: &str,
) -> Option<(String, PersonRow)> {
    read_people(kv)
        .await
        .into_iter()
        .find(|(_, row)| row.session_id == session_id)
}

/// 一次读「说过话的人」的结果。解析不了的行跳过并计数，不按位置猜它是谁。
pub async fn read_people(kv: &Arc<dyn cog_core::KeyValueStore>) -> Vec<(String, PersonRow)> {
    let pairs = match kv.hgetall(PEOPLE_KEY).await {
        Ok(pairs) => pairs,
        Err(e) => {
            tracing::warn!(error = %e, "the people roster could not be read");
            return Vec::new();
        }
    };
    let mut rows = Vec::new();
    for (agent_id, text) in pairs {
        match serde_json::from_str::<PersonRow>(&text) {
            Ok(row) => rows.push((agent_id, row)),
            Err(e) => tracing::warn!(
                agent_id = %agent_id,
                error = %e,
                "a people row does not parse; skipping it rather than guessing who it was"
            ),
        }
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.1.last_seen));
    rows
}

/// 名册上的一行。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RosterRow {
    /// 入职时登记的用途标签。它不限制这个人能做什么——能力来自同一套工具与同
    /// 一条基础指令——这一列只是读面上与小队成员分开的那一格。
    pub role: String,
    /// 谁招的：认证主体上的名字，不是请求里自报的那一个。
    #[serde(default)]
    pub hired_by: String,
    pub hired_at: chrono::DateTime<chrono::Utc>,
}

/// 把一个人写进名册。
pub async fn enroll(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    agent_id: &str,
    role: &str,
    hired_by: &str,
) -> Result<(), String> {
    let row = RosterRow {
        role: role.to_string(),
        hired_by: hired_by.to_string(),
        hired_at: chrono::Utc::now(),
    };
    let text = serde_json::to_string(&row).map_err(|e| e.to_string())?;
    kv.hset(ROSTER_KEY, agent_id, &text)
        .await
        .map_err(|e| e.to_string())
}

/// 一次读名册的结果。
///
/// 读不出来与读出来是空的必须分得开：两者都返回一份空名单，维护环就会把
/// 「存里没有人」和「存读不到、其实可能有人」当成同一件事，而后者正是人丢掉的
/// 那一刻。所以失败条数与错误原因都带出来，由调用方写进日志。
#[derive(Debug, Default)]
pub struct RosterRead {
    pub rows: Vec<(String, RosterRow)>,
    /// 有字段、但那一行解析不出来的条数。
    pub unreadable: usize,
    /// 整张名册读失败的原因。
    pub error: Option<String>,
}

pub async fn read_roster(kv: &Arc<dyn cog_core::KeyValueStore>) -> RosterRead {
    let pairs = match kv.hgetall(ROSTER_KEY).await {
        Ok(pairs) => pairs,
        Err(e) => {
            return RosterRead {
                rows: Vec::new(),
                unreadable: 0,
                error: Some(e.to_string()),
            }
        }
    };
    let mut read = RosterRead::default();
    for (agent_id, text) in pairs {
        match serde_json::from_str::<RosterRow>(&text) {
            Ok(row) => read.rows.push((agent_id, row)),
            Err(e) => {
                read.unreadable += 1;
                tracing::warn!(
                    agent_id = %agent_id,
                    error = %e,
                    "a roster row does not parse; skipping it rather than guessing who it was"
                );
            }
        }
    }
    read
}

/// 对账里「把一个人弄回池子」这一个动作，由调用方提供。
///
/// 收成一个闭包而不是直接吃一个 `AgentManager`：对账逻辑本身只做「按名册逐人
/// 问一句」，把它和池子的建对象路径分开，才能在测试里不造一个完整的 Agent。
pub enum Summon {
    /// 池里本来就有人，什么都没建。
    Held,
    /// 池里没有，这一次建回来了。
    Built,
    /// 另有一个活着的进程在服务这个人，这一趟什么都不该建。
    Elsewhere,
}

pub type SummonFn = Box<
    dyn FnMut(String, String) -> futures::future::BoxFuture<'static, Result<Summon, String>> + Send,
>;

/// 一次名册对账的结果。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RosterPass {
    /// 名册上读得出、要照顾的有几个人。
    pub named: usize,
    /// 其中池里本来就有的。
    pub present: usize,
    /// 这一次被重新建回来的。
    pub restored: usize,
    /// 名册读不出、行解析不了或人建不回来的总数。
    pub failed: usize,
    /// 由另一个活着的进程服务、这一趟交给对方的人。
    pub elsewhere: usize,
}

/// 按名册逐人对账一次。
///
/// 名册是持久的、池是内存的，两边不一致只说明「这个进程是新的」。对账不删除、
/// 不改名：名册上的一行就是一个人，池里没有就把人建回来，池里有的不动。
pub async fn restore_roster_from(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    summon: &mut SummonFn,
) -> RosterPass {
    let read = read_roster(kv).await;
    let mut pass = RosterPass {
        named: read.rows.len(),
        failed: read.unreadable + usize::from(read.error.is_some()),
        ..RosterPass::default()
    };
    if let Some(error) = read.error.as_deref() {
        tracing::warn!(
            error,
            "the roster could not be read; nobody is restored this pass"
        );
    }
    for (agent_id, row) in read.rows {
        match summon(agent_id.clone(), row.role.clone()).await {
            Ok(Summon::Held) => pass.present += 1,
            Ok(Summon::Built) => {
                pass.restored += 1;
                // 这里不印名字。名字只有一处派生（名册行的 `label`），而这一趟
                // 只读了雇佣名册、没读员工名册：按 id 现算一个名字，同一个人就
                // 会在构建日志里与员工页上顶着两个名字。
                tracing::info!(
                    agent_id = %agent_id,
                    hired_by = %row.hired_by,
                    "an employee named by the roster is back on the pool"
                );
            }
            // 不是「池里有」，也不是「建回来了」：这个进程没有这个人，也不该有。
            // 计成 present 会让「在岗人数」把别人的进程算成自己的。
            Ok(Summon::Elsewhere) => pass.elsewhere += 1,
            Err(e) => {
                pass.failed += 1;
                tracing::warn!(
                    agent_id = %agent_id,
                    error = %e,
                    "an employee named by the roster could not be put back on the pool"
                );
            }
        }
    }
    pass
}

/// 这个名字该不该由本进程服务。
///
/// 名册是每个进程都会读的，所以「把名册上的人建回来」这句话在每个进程里都成立
/// ——同一个员工于是被两个进程各建了一份，订阅同一个 inbox、注册同一个键、
/// 心跳互相覆盖。后果不是多花一点内存：一条消息只会投给其中一个进程，而
/// 挂起列表上记的是同一个消费者名，谁也说不清那一条归谁；一个进程重启，另一个
/// 进程手里的活也没有人能接。
///
/// 判据只能读注册表：注册记录里写着「这个进程」。两个进程同时认领时，必须有一
/// 方让出去，而让位规则得是两边读数一样、结果也一样的全序——进程名是唯一同时
/// 满足这两条的值。名字小的那个留下。
pub fn this_process_hosts(record: Option<&cog_core::AgentRegistration>, mine: &str) -> bool {
    match record {
        // 没有任何活着的进程认领这个名字：谁先看到谁建，是今天的行为。
        None => true,
        Some(record) if record.hostname == mine => true,
        Some(record) => mine < record.hostname.as_str(),
    }
}

/// 生产环境的 summon：池里有人就交回，没有就建一个进程生命周期的人。
///
/// 建之前先问注册表：这个名字已经被另一个活着的进程服务时，本进程不但不建，
/// 手里原有的那一份也要交出去——否则两边会一直各留一份，让位规则永远没有终点。
pub fn pool_summoner(
    pool: Arc<dyn cog_core::AgentManager>,
    llm: Arc<dyn cog_core::LlmClient>,
    registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    mine: String,
) -> SummonFn {
    Box::new(move |agent_id, role| {
        let pool = pool.clone();
        let llm = llm.clone();
        let registry = registry.clone();
        let mine = mine.clone();
        Box::pin(async move {
            if let Some(registry) = registry.as_ref() {
                let record = registry.get(&agent_id).await.map_err(|e| e.to_string())?;
                if !this_process_hosts(record.as_ref(), &mine) {
                    // 交出去之后不再建：这一趟的名册行到此为止。注册记录还在对方
                    // 手里，下一趟读到的仍是对方，所以不会来回抢。
                    if pool
                        .get_agent(&agent_id)
                        .await
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        pool.retire_agent(&agent_id)
                            .await
                            .map_err(|e| e.to_string())?;
                        tracing::info!(
                            agent_id = %agent_id,
                            host = %record.map(|r| r.hostname).unwrap_or_default(),
                            "another live process serves this employee; this one let go of it"
                        );
                    }
                    return Ok(Summon::Elsewhere);
                }
            }
            match pool.get_agent(&agent_id).await {
                Ok(Some(_)) => Ok(Summon::Held),
                Ok(None) => pool
                    .create_agent(&agent_id, &role, llm, None)
                    .await
                    .map(|_| Summon::Built)
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            }
        })
    })
}

/// 这个进程能做的事少了一样，名册对账就没有可判的对象：没有存，名册本就不存在；
/// 没有池或没有模型，读出来的名字也建不成人。按「这一趟没有人要照顾」返回。
pub async fn restore_roster(state: &GatewayState) -> RosterPass {
    let (Some(kv), Some(pool)) = (state.key_value.as_ref(), state.agent_pool.as_ref()) else {
        return RosterPass::default();
    };
    let llm = state
        .llm_client
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let Some(llm) = llm else {
        return RosterPass::default();
    };
    let mut summon = pool_summoner(
        pool.clone(),
        llm,
        state.agent_registry.clone(),
        cog_core::env::process_identity(|name| std::env::var(name).ok()),
    );
    restore_roster_from(kv, &mut summon).await
}

/// 名册维护环：池是内存的，名册不是，这条环让两者在重启后重新对得上。
pub fn spawn_roster_keeper(
    state: Arc<GatewayState>,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    cog_core::loop_health::spawn(
        ROSTER_KEEPER_LOOP,
        cog_core::loop_health::Cadence::Periodic(ROSTER_KEEPER_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let state = state.clone();
            let stop = shutdown.clone();
            async move {
                let mut tick = tokio::time::interval(ROSTER_KEEPER_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => return,
                        _ = tick.tick() => {}
                    }
                    let pass = restore_roster(&state).await;
                    if pass.named > 0 || pass.failed > 0 {
                        tracing::info!(
                            named = pass.named,
                            present = pass.present,
                            restored = pass.restored,
                            elsewhere = pass.elsewhere,
                            failed = pass.failed,
                            "roster pass"
                        );
                    }
                }
            }
        },
    )
}

pub async fn heartbeat_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.heartbeat(&agent_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn deregister_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.deregister(&agent_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn get_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.get(&agent_id).await {
        Ok(Some(reg)) => (StatusCode::OK, Json(reg)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "agent not registered"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Put one command on a named agent's inbox.
///
/// The inbox subscription and the command semantics existed, but nothing in
/// the platform ever produced a message for them, so an agent could be
/// addressed only from inside its own process. This is the producer that
/// closes the path: the body is the wire form of [`cog_core::InboxMessage`].
///
/// A `reply_stream` named by the caller is refused here. The platform ends a
/// reply stream once the ask it belongs to is over -- that ending is the only
/// thing that keeps one from outliving its question -- and a name supplied
/// from outside belongs to a reader this process cannot see, so nothing here
/// may delete it. The stream is also not something an outside caller can read:
/// the store is not reachable from the client side. Accepting the field would
/// therefore answer 202 for a key that can stay in the store for good, with no
/// way to drain it. An agent asking another agent gets an answer through the
/// `send_to_agent` tool with `expect_reply`, whose stream the platform opens
/// and ends by construction.
///
/// Two delivery paths, tried in that order:
///
/// - an agent running in *this* process, addressed directly, so the command
///   reaches its execution path without waiting for a poll;
/// - a name held by a process elsewhere, which this one cannot see and cannot
///   call into. The registry is the only thing that knows it exists, and it
///   answers with evidence of life rather than with a row that merely exists,
///   so a name it returns belongs to a running consumer group. The command
///   goes onto that agent's inbox stream, which is the same stream the
///   consumer reads by construction -- the name comes from
///   [`cog_core::agent_inbox_stream`], not from a second copy of the format.
///
/// Accepted, not processed: the agent's consumer owns execution, and the
/// answer goes to `reply_stream` rather than to this response. A name that
/// resolves to no live agent anywhere is a 404, because queueing a command for
/// a process that will never read it is indistinguishable from losing it.
pub async fn send_message_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<MessageQuery>,
    caller: Option<Extension<Claims>>,
    Json(mut msg): Json<cog_core::InboxMessage>,
) -> Response {
    // Who said it, taken from the request rather than invented. The
    // authenticated principal wins over anything the caller says about itself:
    // a caller that could name itself could put its words in someone else's
    // mouth, and the record of who spoke is the whole point of this line. A
    // request that arrives without that layer falls back to the name it
    // supplies, and failing that to the channel it spoke through.
    let from_unit = caller
        .as_ref()
        .map(|Extension(claims)| claims.preferred_username.trim().to_string())
        .filter(|name| !name.is_empty())
        .or_else(|| {
            query
                .from
                .as_deref()
                .map(str::trim)
                .filter(|from| !from.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| WEBUI_SENDER.to_string());
    // Read before delivery: opening the ask rewrites the reply stream on the
    // message, and the transcript is of what was said, not of what was
    // rewritten on the way out.
    let kind = command_name(&msg);
    let body = command_body(&msg);
    let sent = Sent {
        from: &from_unit,
        to: &agent_id,
        kind,
        body: &body,
    };

    // Checked before either delivery path: both of them hand the command to a
    // handler that would publish the answer into that stream and leave it
    // there, because ending it is the asker's job and this route has no asker
    // it knows.
    if let Some(stream) = caller_named_reply_stream(&msg) {
        record_send(
            &state,
            &sent,
            SendOutcome::Refused,
            None,
            Some("the request named its own reply_stream, which this route refuses"),
        )
        .await;
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "this route does not take a caller-named reply_stream: \
                          the platform cannot read or reclaim a stream whose owner \
                          it does not know. Pass `?wait_secs=N` instead and this \
                          route will open the stream itself, read the answer and \
                          end it before it answers; an agent asking another agent \
                          gets the same thing through the send_to_agent tool with \
                          expect_reply",
                "reply_stream": stream,
                "agent_id": agent_id,
            })),
        )
            .into_response();
    }

    // The wait this route can hold is not the caller's to set alone: the whole
    // route sits under the deployment's per-request budget, and a wait that
    // outlives it is cut off by that budget with a bare 408 — the caller's
    // words delivered, the record never written, and nothing said about why.
    // The budget is read here, per request, because it is reloadable.
    let budget_secs = state
        .request_timeout_secs
        .load(std::sync::atomic::Ordering::Relaxed);
    let wait = match ask_window(&msg, query.wait_secs, budget_secs) {
        Ok(wait) => wait,
        Err(e) => {
            record_send(&state, &sent, SendOutcome::Refused, None, Some(&e)).await;
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e, "agent_id": agent_id})),
            )
                .into_response();
        }
    };

    // Opened before the command goes out, whichever path delivers it: a
    // consumer group starts at the stream's current end, so an answer
    // published between the publish and the subscribe would sit behind the
    // reader and the wait would run to its deadline with the answer already
    // on the wire.
    //
    // Every prompt gets a stream, not only the ones this request waits for.
    // The answer is what the agent produced, and an agent that answers after
    // the request returned has still answered this question: without a stream
    // the sentence has nowhere to land and is dropped on the floor. Commands
    // that carry no answer (abort, reset, steering, continue) open nothing.
    let ask = if matches!(msg, cog_core::InboxMessage::Prompt { .. }) {
        match state.message_backend.clone() {
            Some(backend) => match Ask::open(backend, &mut msg, wait).await {
                Ok(ask) => Some(ask),
                Err(e) => {
                    record_send(&state, &sent, SendOutcome::Refused, None, Some(&e)).await;
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e, "agent_id": agent_id})),
                    )
                        .into_response();
                }
            },
            // The answer is read off the queue, so a deployment with no queue
            // cannot serve a waiting request even when the agent is in this
            // process. A request that asked for no answer still goes out: the
            // in-process path can deliver it, only the answer has nowhere to
            // land, and that is the shape this deployment already had.
            None if wait.is_some() => {
                record_send(
                    &state,
                    &sent,
                    SendOutcome::Refused,
                    None,
                    Some("message_backend not configured: an answer has nowhere to land"),
                )
                .await;
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": "message_backend not configured: an answer has \
                                  nowhere to land without one",
                        "agent_id": agent_id,
                    })),
                )
                    .into_response();
            }
            None => None,
        }
    } else {
        None
    };

    // A dropped `ask` on any of the returns below ends its stream, so a
    // refused delivery does not leave one behind.
    let delivered = match deliver(&state, &agent_id, msg).await {
        Ok(delivered) => delivered,
        Err(response) => {
            let detail = format!(
                "delivery was refused before the command reached the agent: HTTP {}",
                response.status().as_u16()
            );
            record_send(&state, &sent, SendOutcome::Refused, None, Some(&detail)).await;
            return response;
        }
    };

    match ask {
        None => {
            record_send(&state, &sent, SendOutcome::Queued, None, None).await;
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "queued": true,
                    "agent_id": agent_id,
                    "via": delivered.label(),
                })),
            )
                .into_response()
        }
        // A request that asked for no answer is answered now and its answer is
        // collected after it: that is what "queued" means, and the record is
        // the row a late answer settles.
        Some(ask) if ask.window().is_none() => {
            let row = record_send(&state, &sent, SendOutcome::Queued, None, None).await;
            spawn_late_collector(state.clone(), ask, row, agent_id.clone(), delivered);
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "queued": true,
                    "agent_id": agent_id,
                    "via": delivered.label(),
                })),
            )
                .into_response()
        }
        Some(mut ask) => {
            let window = ask.window().unwrap_or_default();
            let started = std::time::Instant::now();
            let outcome = ask.next_answer(window).await;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            // One line per ask, beside the queue: the queue keeps no record of
            // who asked, so this is the only place the two ends meet.
            tracing::info!(
                to = %agent_id,
                via = delivered.label(),
                outcome = outcome.label(),
                elapsed_ms,
                "asked an agent and waited for its answer"
            );
            let waited_out = matches!(outcome, AskOutcome::TimedOut { .. });
            let answer = answer_for(agent_id.clone(), delivered, outcome);
            let row = record_send(
                &state,
                &sent,
                answer.outcome,
                answer.reply.as_deref(),
                answer.detail.as_deref(),
            )
            .await;
            if waited_out {
                // The request is over; the agent is not. The stream stays open
                // under a collector so a later answer lands on this same row
                // instead of being dropped because the caller stopped waiting.
                spawn_late_collector(state.clone(), ask, row, agent_id.clone(), delivered);
            } else {
                ask.close().await;
            }
            answer.response
        }
    }
}

/// The name the transcript uses for a command sent through this route.
///
/// The route is how a person reaches an agent; it deliberately does not invent
/// a unit name for them, because a transcript that attributes a person's words
/// to an agent they may not hold would read as something an agent said.
const WEBUI_SENDER: &str = "webui";

/// 招一个员工时写进注册表的那一列角色。
///
/// 它不是一段人设，也不是一条提示词：名册与界面读它只是为了把「平台上的员工」
/// 与「一件任务派生出来的小队成员」分开。员工能做什么由工具与基础指令决定，与
/// 这一列无关，所以这里只有一个值，不按场地分叉——分叉就会长出「memory 员工」。
pub const EMPLOYEE_ROLE: &str = "employee";

/// Read the conversation with one agent, both directions, oldest first.
///
/// The counterpart of the send route: what a person said to an agent and what
/// it answered, readable after the response has scrolled away. A deployment
/// with no database says so rather than answering an empty conversation, which
/// a reader would take as "nothing was ever said".
pub async fn conversation_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<ConversationQuery>,
) -> Response {
    let Some(store) = state.agent_messages.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "no database is configured for agent conversations: this \
                          deployment keeps no transcript",
                "agent_id": agent_id,
            })),
        )
            .into_response();
    };
    let limit = query
        .limit
        .unwrap_or(CONVERSATION_DEFAULT_LIMIT)
        .min(cog_core::ROWS_KEPT_PER_PEER as usize);
    match store.conversation(&agent_id, limit).await {
        Ok(messages) => {
            let lines: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({ "agent_id": agent_id, "messages": lines })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string(), "agent_id": agent_id})),
        )
            .into_response(),
    }
}

/// What a reader may ask a conversation for.
#[derive(Debug, Deserialize, Default)]
pub struct ConversationQuery {
    /// How many lines, newest window first, returned oldest first. Absent means
    /// the default; above the store's bound is clamped to the bound, because
    /// asking for more than is kept is not an error -- it is the whole window.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// One line of a conversation as the reader gets it.
#[derive(Debug, Serialize)]
pub struct MessageView {
    pub id: i64,
    pub from_unit: String,
    pub to_unit: String,
    pub kind: String,
    pub body: String,
    pub outcome: String,
    pub reply: Option<String>,
    pub outcome_detail: Option<String>,
    /// RFC 3339, so a reader in another timezone can tell when it was said
    /// without parsing the server's formatting choice.
    pub created_at: String,
}

impl From<&cog_core::AgentMessage> for MessageView {
    fn from(message: &cog_core::AgentMessage) -> Self {
        Self {
            id: message.id,
            from_unit: message.from_unit.clone(),
            to_unit: message.to_unit.clone(),
            kind: message.kind.clone(),
            body: message.body.clone(),
            outcome: message.outcome.as_str().to_string(),
            reply: message.reply.clone(),
            outcome_detail: message.outcome_detail.clone(),
            created_at: message.created_at.to_rfc3339(),
        }
    }
}

/// How long the route will hold a request open waiting for an answer.
///
/// A route that waits is a connection held by the caller, and every proxy in
/// front of it has its own idle bound; a request told to wait longer than this
/// is refused rather than silently cut down to it, because a caller that asked
/// for five minutes and was given two would read the timeout as the agent's
/// silence rather than as this bound.
// cap: bound — 一次等待的上限；等到这个数仍没结果就按这个界报出去，不沉默
pub const MAX_WAIT_SECS: u64 = 120;

/// 请求自己的预算里，留给「等完之后还要做的事」的那一截。
///
/// 等待结束之后这条请求还要写一行会话、把这句回答序列化回去；这些都在同一个
/// 预算之下。等满 `budget - margin` 就把剩下的时间交给它们，否则回话写不进
/// 去，调用方看到的会是一条空体 408——话送到了、记录没写、也没说为什么。
// cap: bound — 预算里留给收尾的那一截
pub const ASK_BUDGET_MARGIN_SECS: u64 = 5;

/// 请求已经答完之后，那句迟到的回答还值得等多久。
///
/// 会话不是一次请求的附属品：员工跑一个回合要几十秒到几分钟，属主关掉页面
/// 也不该让这句话消失。这个界只是让每条开着的回话流有一个尽头，不是让等不到
/// 的回答变成失败——到点仍然沉默的行保持「没等到」不变。
// cap: bound — 迟到的回答最多再收这么久；到点这条流就结束
pub const LATE_ANSWER_WINDOW: std::time::Duration = std::time::Duration::from_secs(600);

/// The consumer group the route reads its answers with.
///
/// One name for every ask because each ask owns its own stream: the group is
/// the reader, and a stream with exactly one reader does not need a second
/// identity.
const ASK_GROUP: &str = "asker";

/// What the request asked for beyond delivery.
#[derive(Debug, Deserialize, Default)]
pub struct MessageQuery {
    /// Seconds to wait for the agent's answer. Absent means do not wait: the
    /// reply is queued and this route answers `202`, which is what it has
    /// always done.
    #[serde(default)]
    pub wait_secs: Option<u64>,
    /// Who is speaking, for the transcript. Absent means a person through the
    /// WebUI; an agent that sends through this route names itself.
    #[serde(default)]
    pub from: Option<String>,
}

/// How many lines of a conversation a read returns when the caller does not say.
// cap: bound — 读对话时调用方没说要多少行时的默认值
const CONVERSATION_DEFAULT_LIMIT: usize = 100;

/// Whether this request wants an answer, and why not when it cannot have one.
///
/// Split out from the handler so the decision is a function of the request
/// alone: which commands have an answer at all is a property of the command,
/// not of the deployment it is sent to.
fn ask_window(
    msg: &cog_core::InboxMessage,
    wait_secs: Option<u64>,
    budget_secs: u64,
) -> Result<Option<std::time::Duration>, String> {
    let Some(secs) = wait_secs else {
        return Ok(None);
    };
    if !matches!(msg, cog_core::InboxMessage::Prompt { .. }) {
        return Err(format!(
            "wait_secs applies to `prompt` only: the other commands carry no answer \
             (an abort, a reset and a steering change what the agent is doing, and \
             a continue resumes a run that answers on its own stream); got {}",
            command_name(msg)
        ));
    }
    if secs == 0 {
        return Err("wait_secs must be at least 1".into());
    }
    if secs > MAX_WAIT_SECS {
        return Err(format!(
            "wait_secs above {MAX_WAIT_SECS} is refused rather than clamped: a caller \
             that asked for {secs}s and was given less would read the timeout as the \
             agent's silence"
        ));
    }
    // 这条路由活在整个部署的每请求预算之下。等得比预算还久，切掉它的是那个预算，
    // 而且切得最难读：话已经送到，记录还没写，回给调用方的是一条空体 408。所以
    // 超预算的等待在这里就被拒掉，拒绝里点名这个部署能等多久——不是悄悄折短。
    let holdable = budget_secs.saturating_sub(ASK_BUDGET_MARGIN_SECS);
    if secs > holdable {
        return Err(format!(
            "wait_secs above {holdable} is refused on this deployment: a request is \
             answered within {budget_secs}s (gateway.request_timeout_secs), and a wait \
             that outlives it is cut off before the conversation line is written. Ask \
             for no wait at all to get 202 now -- the answer is written to the \
             conversation when it arrives, whichever way this request ends"
        ));
    }
    Ok(Some(std::time::Duration::from_secs(secs)))
}

/// The command's own name, for error text.
fn command_name(msg: &cog_core::InboxMessage) -> &'static str {
    match msg {
        cog_core::InboxMessage::Prompt { .. } => "prompt",
        cog_core::InboxMessage::Steering { .. } => "steering",
        cog_core::InboxMessage::Continue { .. } => "continue",
        cog_core::InboxMessage::Abort => "abort",
        cog_core::InboxMessage::Reset => "reset",
    }
}

/// The words a command carries, for the transcript.
///
/// A prompt's payload is a JSON goal, which is the whole of what was said; the
/// two commands that carry no text answer with their own name so a reader sees
/// that something happened rather than an empty line.
fn command_body(msg: &cog_core::InboxMessage) -> String {
    match msg {
        cog_core::InboxMessage::Prompt { goal, .. } => match goal {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        },
        cog_core::InboxMessage::Continue { instruction } => instruction.clone(),
        cog_core::InboxMessage::Steering { instruction } => instruction.clone(),
        cog_core::InboxMessage::Abort => "(abort)".into(),
        cog_core::InboxMessage::Reset => "(reset)".into(),
    }
}

/// What was said, before the outcome is known: the four fields every written
/// line carries whichever way the send ends.
struct Sent<'a> {
    from: &'a str,
    to: &'a str,
    kind: &'a str,
    body: &'a str,
}

/// Write one settled send down, if this deployment has somewhere to write it.
///
/// A store that fails does not change the answer the caller is about to get:
/// the command was delivered or refused already, and a lost transcript line is
/// not a reason to report the delivery as failed.
///
/// The row comes back because the outcome can still move after this call: an
/// agent that answers later answers this same send, and settling that row is
/// how the answer reaches the transcript instead of being dropped.
async fn record_send(
    state: &GatewayState,
    sent: &Sent<'_>,
    outcome: SendOutcome,
    reply: Option<&str>,
    detail: Option<&str>,
) -> Option<i64> {
    let store = state.agent_messages.as_ref()?;
    let message = NewAgentMessage {
        from_unit: sent.from,
        to_unit: sent.to,
        kind: sent.kind,
        body: sent.body,
        outcome,
        reply,
        outcome_detail: detail,
    };
    match store.record(&message).await {
        Ok(row) => Some(row),
        Err(e) => {
            tracing::warn!(
                to = %sent.to,
                outcome = outcome.as_str(),
                error = %e,
                "could not record a message sent to an agent"
            );
            None
        }
    }
}

/// Keep reading one ask's reply stream after the request that asked has been
/// answered, and settle the conversation line with whatever arrives.
///
/// This is what makes the conversation independent of the request that carried
/// the question: an agent's turn outlives the caller's patience, the caller's
/// connection, and the deployment's per-request budget. Without this, "did it
/// answer" has exactly one moment it can be true.
///
/// The frame at the end is how a reader learns about it: the workbench holds no
/// timer for this and cannot poll a stream it does not own, so the write itself
/// says the roster's conversations changed.
fn spawn_late_collector(
    state: Arc<GatewayState>,
    ask: Ask,
    row: Option<i64>,
    agent_id: String,
    delivered: Delivered,
) {
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        let outcome = ask.await_late().await;
        tracing::info!(
            to = %agent_id,
            via = delivered.label(),
            outcome = outcome.label(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "an answer to an already-answered request finished arriving"
        );
        let text = match outcome {
            // A string answer is what the agent said; any other shape is kept
            // as its JSON text so the transcript never drops an answer it does
            // not recognise.
            AskOutcome::Replied(reply) => match &reply {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            },
            // Silence of every other kind has an outcome too, and the row has
            // to carry it: the window this collector just waited out is also
            // the life of the stream the answer would have arrived on, so from
            // here on no answer can reach this row.
            other => return settle_no_answer(&state, row, &agent_id, other).await,
        };
        settle_late_answer(&state, row, &agent_id, &text).await;
    });
}

/// Put a late answer on the row its send wrote, and say the conversation moved.
async fn settle_late_answer(state: &GatewayState, row: Option<i64>, agent_id: &str, text: &str) {
    let (Some(store), Some(id)) = (state.agent_messages.as_ref(), row) else {
        return;
    };
    match store.answer(id, text).await {
        Ok(true) => say_the_conversation_moved(state),
        Ok(false) => tracing::warn!(
            row = id,
            to = %agent_id,
            "a late answer found no unanswered row to settle: the conversation had \
             already moved on or trimmed it"
        ),
        Err(e) => tracing::warn!(
            row = id,
            to = %agent_id,
            error = %e,
            "could not write a late answer into the transcript"
        ),
    }
}

/// Put down that no answer came, on the row its send wrote.
///
/// The row was written `queued`: delivered, no answer waited for. A reader sees
/// that as "it will answer on its own", and for as long as the row stands it
/// keeps saying so -- even after the stream the answer would have arrived on is
/// gone. What this closes is that gap. A person talking to an employee that is
/// running another task has their sentence queued behind the whole run; if the
/// run outlasts the wait, the sentence is never answered, and until now nothing
/// said so: the delivery was recorded as a promise nobody would keep.
///
/// Which outcome it settles as is the reason the answer is not here. Waiting
/// the window out is `timed_out` -- nobody answered. A reply stream that broke,
/// or an answer that could not be read, is `failed` -- those two are different
/// things to a reader, and folding them together would send someone looking for
/// a busy employee when the connection is what went away.
async fn settle_no_answer(
    state: &GatewayState,
    row: Option<i64>,
    agent_id: &str,
    outcome: AskOutcome,
) {
    let (Some(store), Some(id)) = (state.agent_messages.as_ref(), row) else {
        return;
    };
    let (settled, detail) = match outcome {
        AskOutcome::TimedOut { waited_secs } => (
            SendOutcome::TimedOut,
            format!(
                "delivered, but no answer arrived within {waited_secs}s: the employee may \
                 be running another task, so this sentence is queued behind it, and the \
                 reply stream closed when the wait ran out"
            ),
        ),
        AskOutcome::StreamError { error } => (
            SendOutcome::Failed,
            format!("delivered, but the reply stream broke before an answer: {error}"),
        ),
        AskOutcome::Unreadable { error } => (
            SendOutcome::Failed,
            format!("delivered, but the answer could not be read: {error}"),
        ),
        // The one outcome that has an answer is `settle_late_answer`'s.
        AskOutcome::Replied(_) => return,
    };
    match store.no_answer(id, settled, &detail).await {
        Ok(true) => {
            tracing::info!(
                to = %agent_id,
                row = id,
                outcome = settled.as_str(),
                "a send nobody answered was settled as such"
            );
            say_the_conversation_moved(state);
        }
        Ok(false) => tracing::warn!(
            row = id,
            to = %agent_id,
            "a send settled by silence found no waiting row: the conversation had \
             already moved on, was trimmed, or was settled by its answer"
        ),
        Err(e) => tracing::warn!(
            row = id,
            to = %agent_id,
            error = %e,
            "could not write a send that never got its answer into the transcript"
        ),
    }
}

/// Say the roster's conversations moved.
///
/// The frame is how a reader learns about a settled row: the workbench holds no
/// timer for this and cannot poll a stream it does not own, so the write itself
/// says the conversations changed.
fn say_the_conversation_moved(state: &GatewayState) {
    // 没有接收者不是错误：这一帧的读者是打开着的那一面，一个都没有的时候它
    // 本来就无处可去。写下去的那一行仍然在库里。
    let _ = state.event_tx.send(cog_core::AgentEvent::ResourceChanged {
        domain: "agents".to_string(),
        timestamp: chrono::Utc::now(),
    });
}

/// Which of the two delivery paths took the command.
#[derive(Clone, Copy)]
enum Delivered {
    /// An agent in this process, handed the command directly.
    Pool,
    /// A name held elsewhere, reached through the shared inbox stream.
    Inbox,
}

impl Delivered {
    fn label(self) -> &'static str {
        match self {
            Self::Pool => "agent_pool",
            Self::Inbox => "inbox",
        }
    }
}

/// Put the command on the named agent's queue, or say why it could not go.
// The refusal is an HTTP response the route hands straight back to axum, so
// the large variant is the type the caller actually wants; boxing it would
// only move the same bytes onto the heap for the length of one return.
#[allow(clippy::result_large_err)]
async fn deliver(
    state: &GatewayState,
    agent_id: &str,
    msg: cog_core::InboxMessage,
) -> Result<Delivered, Response> {
    // Local first. A miss is not an answer -- the same name may be held by
    // another process, which is the whole point of the registry.
    if let Some(pool) = state.agent_pool.as_ref() {
        match pool.get_agent(agent_id).await {
            Ok(Some(agent)) => {
                return match agent.receive_message(msg).await {
                    Ok(()) => Ok(Delivered::Pool),
                    Err(e) => Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response()),
                }
            }
            Ok(None) => {}
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response())
            }
        }
    }

    let Some(registry) = state.agent_registry.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response());
    };
    match registry.get(agent_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "no live agent under this name"})),
            )
                .into_response())
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response())
        }
    }

    // A name with a live holder but no queue to put the command on is a
    // deployment fault, not a delivery: say so instead of answering 202 for a
    // message that was never written anywhere.
    let Some(backend) = state.message_backend.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "message_backend not configured"})),
        )
            .into_response());
    };
    let payload = match serde_json::to_vec(&msg) {
        Ok(payload) => payload,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response())
        }
    };
    let stream = cog_core::agent_inbox_stream(agent_id);
    match backend.publish(&stream, &payload).await {
        Ok(()) => Ok(Delivered::Inbox),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string(), "stream": stream})),
        )
            .into_response()),
    }
}

/// One asked exchange: the response to return, and what to write down.
struct Answer {
    response: Response,
    outcome: SendOutcome,
    reply: Option<String>,
    detail: Option<String>,
}

/// Turn one ask's outcome into the response to return and the row to write.
fn answer_for(agent_id: String, delivered: Delivered, outcome: AskOutcome) -> Answer {
    match outcome {
        AskOutcome::Replied(reply) => {
            // The answer arrives as parsed JSON; a string answer is what the
            // agent said, any other shape is kept as its JSON text so the
            // transcript never drops an answer it does not recognise.
            let text = match &reply {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            Answer {
                response: (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "replied": true,
                        "agent_id": agent_id,
                        "via": delivered.label(),
                        "reply": reply,
                    })),
                )
                    .into_response(),
                outcome: SendOutcome::Replied,
                reply: Some(text),
                detail: None,
            }
        }
        AskOutcome::TimedOut { waited_secs } => Answer {
            response: (
                StatusCode::GATEWAY_TIMEOUT,
                Json(serde_json::json!({
                    "replied": false,
                    "agent_id": agent_id,
                    "via": delivered.label(),
                    "waited_secs": waited_secs,
                    "error": "the agent did not answer within the wait this request \
                              asked for; the run itself was not cancelled, and an \
                              answer that arrives later is written to this \
                              conversation",
                })),
            )
                .into_response(),
            outcome: SendOutcome::TimedOut,
            reply: None,
            detail: Some(format!(
                "no answer arrived within {waited_secs}s; an answer that arrives later \
                 is written to this conversation"
            )),
        },
        AskOutcome::Unreadable { error } => Answer {
            response: (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "replied": false,
                    "agent_id": agent_id,
                    "error": format!("the agent answered with bytes that are not JSON: {error}"),
                })),
            )
                .into_response(),
            outcome: SendOutcome::Failed,
            reply: None,
            detail: Some(format!(
                "the agent answered with bytes that are not JSON: {error}"
            )),
        },
        AskOutcome::StreamError { error } => Answer {
            response: (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "replied": false,
                    "agent_id": agent_id,
                    "error": format!("the reply stream failed before an answer arrived: {error}"),
                })),
            )
                .into_response(),
            outcome: SendOutcome::Failed,
            reply: None,
            detail: Some(format!(
                "the reply stream failed before an answer arrived: {error}"
            )),
        },
    }
}

/// One ask's reply stream, from the moment the key exists until it is ended.
///
/// The stream is opened here rather than named by the caller for the reason
/// the refusal above gives: ending it is the asker's job, and this is the
/// asker. It is ended on every path the wait reaches, and the `Drop` below is
/// what covers the paths that reach none of them -- a cancelled request, or a
/// `?` return between the group's creation and the wait.
struct Ask {
    backend: Arc<dyn cog_core::MessageBackend>,
    stream: String,
    reader: cog_core::MessageStream,
    /// How long this request will hold the connection for. `None` means the
    /// caller asked for no answer: the stream is still opened, and the answer
    /// is collected after the request has been answered.
    window: Option<std::time::Duration>,
    armed: bool,
}

impl Ask {
    /// Open a reply stream and put its name on `msg`.
    ///
    /// The group is created before the subscribe because that is what brings
    /// the key into existence, and the subscriber starts from the stream's
    /// current end; a group created afterwards would start behind the answer
    /// this call is about to ask for.
    async fn open(
        backend: Arc<dyn cog_core::MessageBackend>,
        msg: &mut cog_core::InboxMessage,
        window: Option<std::time::Duration>,
    ) -> Result<Self, String> {
        let stream = ask_stream();
        if let cog_core::InboxMessage::Prompt { reply_stream, .. } = msg {
            *reply_stream = Some(stream.clone());
        } else {
            return Err("only a prompt has an answer to wait for".into());
        }
        backend
            .create_consumer_group(&stream, ASK_GROUP)
            .await
            .map_err(|e| format!("cannot open a reply stream: {e}"))?;
        let reader = match backend.subscribe(&stream, ASK_GROUP).await {
            Ok(reader) => reader,
            // The key exists from the group's creation, and no guard owns it
            // yet, so this path ends it here.
            Err(e) => {
                backend.stop_stream(&stream, ASK_GROUP).await;
                if let Err(del) = backend.delete_stream(&stream).await {
                    tracing::warn!(
                        stream = %stream,
                        error = %del,
                        "a reply stream opened for an ask that could not read it was left behind"
                    );
                }
                return Err(format!("cannot read the reply stream: {e}"));
            }
        };
        Ok(Self {
            backend,
            stream,
            reader,
            window,
            armed: true,
        })
    }

    /// End the stream and stop counting this ask as outstanding.
    async fn close(&mut self) {
        self.backend.stop_stream(&self.stream, ASK_GROUP).await;
        if let Err(e) = self.backend.delete_stream(&self.stream).await {
            tracing::warn!(
                stream = %self.stream,
                error = %e,
                "the ask's reply stream outlived its answer: this backend cannot delete streams"
            );
        }
        self.armed = false;
    }

    /// The window this request holds the connection for, if it holds one.
    fn window(&self) -> Option<std::time::Duration> {
        self.window
    }

    /// Read at most one answer, waiting at most `window`.
    ///
    /// The stream is left open: a timeout is the caller's cue to hand the ask
    /// to the collector rather than a reason to end the stream, and the answer
    /// may still be on its way.
    async fn next_answer(&mut self, window: std::time::Duration) -> AskOutcome {
        let waited = tokio::time::timeout(window, futures::StreamExt::next(&mut self.reader)).await;
        match waited {
            Ok(Some(Ok((id, bytes)))) => {
                if let Err(e) = self
                    .backend
                    .ack(&self.stream, ASK_GROUP, std::slice::from_ref(&id))
                    .await
                {
                    tracing::warn!(stream = %self.stream, error = %e, "cannot ack an answer");
                }
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map(AskOutcome::Replied)
                    .unwrap_or_else(|e| AskOutcome::Unreadable {
                        error: e.to_string(),
                    })
            }
            Ok(Some(Err(e))) => AskOutcome::StreamError {
                error: e.to_string(),
            },
            Ok(None) => AskOutcome::StreamError {
                error: "the reply stream ended without a message".into(),
            },
            Err(_) => AskOutcome::TimedOut {
                waited_secs: window.as_secs(),
            },
        }
    }

    /// Keep reading for an answer after the request that asked has been
    /// answered, then end the stream whichever way this ends.
    async fn await_late(mut self) -> AskOutcome {
        let outcome = self.next_answer(LATE_ANSWER_WINDOW).await;
        self.close().await;
        outcome
    }
}

impl Drop for Ask {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let backend = self.backend.clone();
        let stream = self.stream.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    backend.stop_stream(&stream, ASK_GROUP).await;
                    if let Err(e) = backend.delete_stream(&stream).await {
                        tracing::warn!(
                            stream = %stream,
                            error = %e,
                            "an abandoned ask left its reply stream behind"
                        );
                    }
                });
            }
            Err(_) => tracing::warn!(
                stream = %self.stream,
                "an abandoned ask could not be reaped: no runtime to end its stream"
            ),
        }
    }
}

/// How one ask ended.
enum AskOutcome {
    Replied(serde_json::Value),
    TimedOut {
        waited_secs: u64,
    },
    /// The agent published something that is not JSON.
    Unreadable {
        error: String,
    },
    /// The queue failed, or the stream ended without a message.
    StreamError {
        error: String,
    },
}

impl AskOutcome {
    fn label(&self) -> &'static str {
        match self {
            Self::Replied(_) => "replied",
            Self::TimedOut { .. } => "timeout",
            Self::Unreadable { .. } => "unreadable",
            Self::StreamError { .. } => "stream_error",
        }
    }
}

/// The stream one ask waits on, named by the ask itself.
///
/// The name is the correlation: a reply carries nothing saying which question
/// it answers, so two questions sharing a stream would leave the reader unable
/// to tell the answers apart. Giving each ask its own stream makes the
/// correlation a property of the address.
///
/// Process id, wall clock and a counter together, because no one of them is
/// unique alone: the counter restarts at zero with the process, the clock
/// collides inside a tick, and the process id is reused.
fn ask_stream() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "gateway:ask:{}:{:x}:{}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The reply stream the caller put in the body, if any.
///
/// Split out so the refusal can be judged without a gateway state: what is
/// being decided is which bodies this route will take, and that depends on the
/// body alone.
fn caller_named_reply_stream(msg: &cog_core::InboxMessage) -> Option<&str> {
    match msg {
        cog_core::InboxMessage::Prompt {
            reply_stream: Some(stream),
            ..
        } => Some(stream),
        _ => None,
    }
}

/// One registry record as the roster prints it.
///
/// 这里只搬注册表那一行，**不印名字**：名字来自名册行（工号＋姓名），由
/// [`identity_of`] 在后面合并进来。两个地方各印一次时，同一张名单上会出现
/// 两种拼法，而读的人只会以为是两个人。
fn agent_view(reg: &cog_core::AgentRegistration) -> serde_json::Value {
    serde_json::to_value(reg).unwrap_or(serde_json::Value::Null)
}

/// Split the roster into the two populations it actually holds.
///
/// A pool worker is capacity: it exists whether or not there is work. A squad
/// member is demand: one squad per task in flight, created when the task is
/// dispatched and retired when it ends. Summed into one number they answer no
/// question — "few agents" then reads alike whether the pool is idle or the
/// work has stopped coming.
fn roster_split(regs: &[cog_core::AgentRegistration]) -> (usize, usize) {
    let squads = regs
        .iter()
        .filter(|r| r.agent_id.starts_with(cog_core::SQUAD_ID_PREFIX))
        .count();
    (regs.len() - squads, squads)
}

/// 把名册那一行摊成身份字段。
///
/// 展示名只有这一个来源：有工号的人印 `E0412 林清和`，没工号的人印「待编
/// 某某」。读面不再各自拼一遍——三处各拼一次时，同一个人在员工页与对话页
/// 会顶着两个名字，而读的人只会以为是两个人。
pub fn identity_of(
    agent_id: &str,
    roster: &std::collections::HashMap<String, cog_core::EmployeeRecord>,
) -> serde_json::Value {
    match roster.get(agent_id) {
        Some(row) => serde_json::json!({
            "display_name": row.label(),
            "number": row
                .number
                .map(|n| format!("{}{n:04}", cog_core::EMPLOYEE_NUMBER_PREFIX)),
            "person_name": row.number.map(cog_core::employee_name),
            "retired": !row.is_active(),
            "retired_at": row.retired_at,
        }),
        None => serde_json::json!({
            "display_name": cog_core::unregistered_label(agent_id),
            "number": serde_json::Value::Null,
            "person_name": serde_json::Value::Null,
            "retired": false,
        }),
    }
}

fn merge_fields(view: &mut serde_json::Value, extra: serde_json::Value) {
    if let (Some(obj), Some(extra)) = (view.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            obj.insert(key.clone(), value.clone());
        }
    }
}

/// 名单：心跳上此刻活着的人，加上说过话但已经下线的人。
///
/// 同一个人两处都有时只印一行——注册表那一行回答「此刻在不在」，名册那一行
/// 回答「这个人存不存在」。印成两行会让读的人以为撞了名，而其中一个点进去
/// 什么也没有。
///
/// `live` 是这次合并唯一的判断位：有它是此刻叫得到，没有是这个人说过话、现在
/// 不在岗。界面据此决定这一格是「在岗」还是「休息中」，以及能不能用对话那条路
/// 接着聊（只有说过话的人带着 `session_id`）。
///
/// 第三份来源是雇佣名册（[`ROSTER_KEY`]）。池是内存的，读面又只读注册表时，
/// 招进来的人在维护环把他建回来之前会整个消失——名单上少一个人，读的人分不出
/// 「没这个人」和「这个人这一刻不在池里」。所以名册上的人即使池里没有，也印一
/// 行（`live: false`）：在不在岗是在岗位那一格的问题，存不存在是名册的问题。
///
/// 第四份来源是持久名册（`cog_storage::EMPLOYEES_KEY` 那张 hash）：注册表会
/// 过期，people 与 roster 只覆盖走过对话与招人这两条路的人，按任务现起的成员
/// （探针、小队角色）三条路都不经过。名册行还回答「在职还是离职」。
fn roster_with_people(
    regs: &[cog_core::AgentRegistration],
    people: Vec<(String, PersonRow)>,
    hired: Vec<(String, RosterRow)>,
    employees: Vec<cog_core::EmployeeRecord>,
) -> Vec<serde_json::Value> {
    let roster: std::collections::HashMap<String, cog_core::EmployeeRecord> = employees
        .iter()
        .map(|row| (row.agent_id.clone(), row.clone()))
        .collect();
    let mut views: Vec<serde_json::Value> = regs
        .iter()
        .map(|reg| {
            let mut view = agent_view(reg);
            if let Some(obj) = view.as_object_mut() {
                obj.insert("live".to_string(), serde_json::json!(true));
            }
            merge_fields(&mut view, identity_of(&reg.agent_id, &roster));
            view
        })
        .collect();
    let by_id: std::collections::HashMap<String, PersonRow> = people.into_iter().collect();
    // 在岗的人也要带上他的会话 id：此刻他正说着话，界面要给的是「接着这一段
    // 聊」，而不是等他一闭嘴才给。反过来，注册表里的名字更接近此刻，保留它。
    for view in views.iter_mut() {
        let Some(agent_id) = view
            .get("agent_id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        let Some(row) = by_id.get(&agent_id) else {
            continue;
        };
        if let Some(obj) = view.as_object_mut() {
            obj.insert("session_id".to_string(), serde_json::json!(row.session_id));
            // 来源跟着人走，不跟着这次心跳走：此刻在不在岗由注册表回答，这位
            // 从哪儿来由名册回答，两处读到的是同一行。
            if let Some(platform) = row.platform.as_deref() {
                obj.insert("platform".to_string(), serde_json::json!(platform));
            }
            if let Some(device_id) = row.device_id.as_deref() {
                obj.insert("device_id".to_string(), serde_json::json!(device_id));
            }
        }
    }
    let live_ids: std::collections::HashSet<&str> =
        regs.iter().map(|reg| reg.agent_id.as_str()).collect();
    // 最近说过话的排在前面：名单是一次扫读，先出现的人该是此刻最可能要找的。
    let mut idle: Vec<(String, PersonRow)> = by_id
        .into_iter()
        .filter(|(agent_id, _)| !live_ids.contains(agent_id.as_str()))
        .collect();
    idle.sort_by_key(|row| std::cmp::Reverse(row.1.last_seen));
    for (agent_id, row) in idle {
        // 这里印的不是这一行里存的种子，而是名册上那一位的名字：名字只有一处
        // 派生（`EmployeeRecord::label`），存下来的那份只是身份的种子。没有
        // 名册行的人才印「待编」——那是「这个人还没被编号」，不是「这个人叫
        // Kai#a9ab11fc」。两处各印一个名字，读的人会以为看见了两个人。
        let mut view = serde_json::json!({
            "agent_id": agent_id.clone(),
            "display_name": cog_core::unregistered_label(&agent_id),
            "role": "chat",
            "workspace_id": "default",
            "capabilities": Vec::<String>::new(),
            "hostname": String::new(),
            "pod_ip": String::new(),
            "registered_at": row.first_seen,
            "last_heartbeat": row.last_seen,
            "live": false,
            "session_id": row.session_id,
            "platform": row.platform,
            "device_id": row.device_id,
        });
        if roster.contains_key(&agent_id) {
            merge_fields(&mut view, identity_of(&agent_id, &roster));
        }
        views.push(view);
    }
    // 名册上有、另外两份都没有的人：招进来过，这一刻池里没有他。雇佣行不存
    // 名字，所以显示名取自持久名册那一行；连名册都还没有的人印「待编」，而不是
    // 把 id 本身当名字——那串十六进制看着像名字，于是没人去补这一次编号。
    let printed: std::collections::HashSet<String> = views
        .iter()
        .filter_map(|view| view.get("agent_id").and_then(|v| v.as_str()))
        .map(str::to_string)
        .collect();
    for (agent_id, row) in hired {
        if printed.contains(agent_id.as_str()) {
            continue;
        }
        let mut view = serde_json::json!({
            "agent_id": agent_id.clone(),
            "display_name": cog_core::unregistered_label(&agent_id),
            "role": row.role,
            "workspace_id": "default",
            "capabilities": Vec::<String>::new(),
            "hostname": String::new(),
            "pod_ip": String::new(),
            "registered_at": row.hired_at,
            "last_heartbeat": row.hired_at,
            "live": false,
            "hired_by": row.hired_by,
        });
        merge_fields(&mut view, identity_of(&agent_id, &roster));
        views.push(view);
    }
    // 第四份来源：持久名册。前三份都是「此刻还在某处留下过痕迹的人」——注册表
    // 会过期、people 与 roster 只覆盖走过对话与招人这两条路的人。按任务现起的
    // 成员（探针、小队的那几个角色）三条路都不经过，于是名册上根本没有它们；
    // 反过来，退役的人注册表一删键就无处可查。名册行两件事都记得，所以这里按
    // 它补齐，并且把「在职/离职」作为一个显式字段印出来，而不是让读的人从
    // 「在不在注册表里」去猜。
    let printed: std::collections::HashSet<String> = views
        .iter()
        .filter_map(|view| view.get("agent_id").and_then(|v| v.as_str()))
        .map(str::to_string)
        .collect();
    for row in employees {
        if printed.contains(row.agent_id.as_str()) {
            continue;
        }
        let retired = !row.is_active();
        views.push(serde_json::json!({
            "agent_id": row.agent_id.clone(),
            "display_name": row.label(),
            "number": row
                .number
                .map(|n| format!("{}{n:04}", cog_core::EMPLOYEE_NUMBER_PREFIX)),
            "person_name": row.number.map(cog_core::employee_name),
            "role": row.role,
            "workspace_id": "default",
            "capabilities": Vec::<String>::new(),
            "hostname": String::new(),
            "pod_ip": String::new(),
            "registered_at": row.first_seen,
            "last_heartbeat": row.last_seen,
            "live": false,
            "retired": retired,
            "retired_at": row.retired_at,
        }));
    }
    views
}

pub async fn list_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.list().await {
        Ok(regs) => {
            let people = match state.key_value.as_ref() {
                Some(kv) => read_people(kv).await,
                None => Vec::new(),
            };
            // 雇佣名册读不出来时按空处理：这一份缺了，名单漏掉的是「今天招过谁」，
            // 不是「谁在说话」，不能因此让整条读面变成 503。
            let hired = match state.key_value.as_ref() {
                Some(kv) => read_roster(kv).await.rows,
                None => Vec::new(),
            };
            let people_count = people
                .iter()
                .filter(|(id, _)| !regs.iter().any(|reg| &reg.agent_id == id))
                .count();
            // 名册读不出来按空处理，理由与雇佣名册那一份相同：它缺了，漏掉的是
            // 「存在过谁」，而注册表那份仍然会把此刻在岗的人一个不少地交出来。
            // 整条读面因此不该变成 503——但也不能装作读过：日志要说得出来这次
            // 名册是空的还是没读到。
            let employees = match registry.employees().await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "the employee roster could not be read; this pass lists only what the live \
                         registry and the two key-value rosters know about"
                    );
                    Vec::new()
                }
            };
            let agents = roster_with_people(&regs, people, hired, employees);
            let count = agents.len();
            let (workers, squads) = roster_split(&regs);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "agents": agents,
                    "count": count,
                    "worker_agents": workers,
                    "squad_agents": squads,
                    "people_agents": people_count,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Agent Lifecycle Control Commands ───
// These POST endpoints dispatch lifecycle commands through the Supervisor
// trait, which emits events on the Supervisor broadcast channel for
// downstream consumption (gRPC server, hook engine, audit log, etc.).

#[derive(Debug, Deserialize)]
pub struct CheckpointQuery {
    /// Optional task ID.  When omitted the caller should supply it in the
    /// request body of a future revision; for now we require it.
    pub task_id: String,
}

pub async fn kill_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.kill_agent(&agent_id, "user-requested").await {
            Ok(true) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "dispatched",
                })),
            )
                .into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "agent not found",
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "kill",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

pub async fn restart_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.restart_agent(&agent_id, false).await {
            Ok(true) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "dispatched",
                })),
            )
                .into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "agent not found",
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "restart",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

pub async fn checkpoint_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<CheckpointQuery>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.checkpoint_agent(&agent_id, &query.task_id).await {
            Ok(checkpoint_id) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "checkpoint",
                    "status": "dispatched",
                    "checkpoint_id": checkpoint_id,
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "checkpoint",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "checkpoint",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        agent_view, caller_named_reply_stream, enroll, read_people, read_roster,
        restore_roster_from, roster_split, roster_with_people, PersonRow, RosterRow, Summon,
        SummonFn, PEOPLE_KEY, ROSTER_KEY,
    };
    use cog_core::InboxMessage;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// 名册只用得到 hash：其余方法不是这条路径的一部分，落到它们身上说明
    /// 被测代码问了不该问的东西。
    #[derive(Default)]
    struct FakeRosterStore {
        hashes: Mutex<HashMap<String, HashMap<String, String>>>,
        /// 让整张名册读失败：验证「读不出来」与「读出来是空的」不是同一个答案。
        broken: bool,
    }

    #[async_trait::async_trait]
    impl cog_core::KeyValueStore for FakeRosterStore {
        async fn get(&self, _key: &str) -> cog_core::SFResult<Option<String>> {
            unreachable!("the roster is a hash")
        }

        async fn set(&self, _key: &str, _value: &str) -> cog_core::SFResult<()> {
            unreachable!("the roster is a hash")
        }

        async fn set_ex(&self, _key: &str, _value: &str, _ttl_secs: u64) -> cog_core::SFResult<()> {
            unreachable!("the roster is a hash")
        }

        async fn del(&self, _key: &str) -> cog_core::SFResult<()> {
            unreachable!("the roster is a hash")
        }

        async fn expire(&self, _key: &str, _ttl_secs: i64) -> cog_core::SFResult<()> {
            unreachable!("the roster is a hash")
        }

        async fn ttl(&self, _key: &str) -> cog_core::SFResult<i64> {
            unreachable!("the roster is a hash")
        }

        async fn incr(&self, _key: &str, _by: i64) -> cog_core::SFResult<i64> {
            unreachable!("the roster is a hash")
        }

        async fn decr(&self, _key: &str, _by: i64) -> cog_core::SFResult<i64> {
            unreachable!("the roster is a hash")
        }

        async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> cog_core::SFResult<i64> {
            unreachable!("the roster is a hash")
        }

        async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
            unreachable!("the roster is a hash")
        }

        async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
            unreachable!("the roster is a hash")
        }

        async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
            unreachable!("the roster is a hash")
        }

        async fn hset(&self, key: &str, field: &str, value: &str) -> cog_core::SFResult<()> {
            self.hashes
                .lock()
                .unwrap()
                .entry(key.to_string())
                .or_default()
                .insert(field.to_string(), value.to_string());
            Ok(())
        }

        async fn hget(&self, key: &str, field: &str) -> cog_core::SFResult<Option<String>> {
            Ok(self
                .hashes
                .lock()
                .unwrap()
                .get(key)
                .and_then(|hash| hash.get(field).cloned()))
        }

        async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
            unreachable!("the roster is only ever added to here")
        }

        async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
            unreachable!("the roster is read by field and value together")
        }

        async fn hgetall(&self, key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
            if self.broken {
                return Err(cog_core::SFError::Internal(
                    "the roster store is unreachable".into(),
                ));
            }
            Ok(self
                .hashes
                .lock()
                .unwrap()
                .get(key)
                .map(|hash| hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default())
        }

        async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
            unreachable!("the roster is read whole")
        }
    }

    /// 记下每一次「把人弄回池子」的请求，按脚本回答。
    fn recording_summoner(
        seen: Arc<Mutex<Vec<(String, String)>>>,
        answer: impl Fn(&str) -> Result<Summon, String> + Send + 'static,
    ) -> SummonFn {
        Box::new(move |agent_id, role| {
            let seen = Arc::clone(&seen);
            let answer = answer(agent_id.as_str());
            Box::pin(async move {
                seen.lock().unwrap().push((agent_id, role));
                answer
            })
        })
    }

    /// 入职写在名册上，重启后读得到同一个人。这是「一直在岗」的全部内容：
    /// 进程换了，id、角色与招他的人一个字节都不变。
    #[tokio::test]
    async fn a_roster_row_survives_the_process_that_hired_it() {
        let store: Arc<dyn cog_core::KeyValueStore> = Arc::new(FakeRosterStore::default());
        enroll(&store, "employee:abc12345", "employee", "owner")
            .await
            .unwrap();

        let read = read_roster(&store).await;
        assert!(read.error.is_none());
        assert_eq!(read.rows.len(), 1);
        let (agent_id, row) = &read.rows[0];
        assert_eq!(agent_id, "employee:abc12345");
        assert_eq!(row.role, "employee");
        assert_eq!(row.hired_by, "owner");

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut summon = recording_summoner(Arc::clone(&seen), |_| Ok(Summon::Built));
        let pass = restore_roster_from(&store, &mut summon).await;

        assert_eq!(pass.named, 1);
        assert_eq!(pass.restored, 1);
        assert_eq!(pass.present, 0);
        assert_eq!(pass.failed, 0);
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[("employee:abc12345".to_string(), "employee".to_string())]
        );
    }

    /// 池里已经有的人不重建：对账是「补缺」不是「重来」。第二趟必须报
    /// present 而不是再算一次 restored，否则每分钟的 restore 计数会把一个
    /// 常驻的人数成新来的。
    #[tokio::test]
    async fn a_person_already_on_the_pool_is_not_built_again() {
        let store: Arc<dyn cog_core::KeyValueStore> = Arc::new(FakeRosterStore::default());
        enroll(&store, "employee:abc12345", "employee", "owner")
            .await
            .unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut summon = recording_summoner(Arc::clone(&seen), |_| Ok(Summon::Held));
        let pass = restore_roster_from(&store, &mut summon).await;

        assert_eq!(pass.named, 1);
        assert_eq!(pass.present, 1);
        assert_eq!(pass.restored, 0);
    }

    /// 名册读不出来时不能报成「名册是空的」：后者会让维护环安静地什么都不做，
    /// 而人就在存储里。失败必须计数。
    #[tokio::test]
    async fn a_roster_that_cannot_be_read_is_a_failure_not_an_empty_roster() {
        let store: Arc<dyn cog_core::KeyValueStore> = Arc::new(FakeRosterStore {
            broken: true,
            ..FakeRosterStore::default()
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut summon = recording_summoner(Arc::clone(&seen), |_| Ok(Summon::Built));
        let pass = restore_roster_from(&store, &mut summon).await;

        assert_eq!(pass.named, 0);
        assert_eq!(pass.failed, 1);
        assert!(seen.lock().unwrap().is_empty());
    }

    /// 解析不了的那一行跳过并计数，不按位置猜它是谁：一次瞎猜会把一个陌生人
    /// 建进池子，而那个名字已经有人在用。
    #[tokio::test]
    async fn a_row_that_does_not_parse_is_counted_not_guessed() {
        let store = Arc::new(FakeRosterStore::default());
        store
            .hashes
            .lock()
            .unwrap()
            .entry(ROSTER_KEY.to_string())
            .or_default()
            .insert("employee:broken1".to_string(), "{not json".to_string());
        let store: Arc<dyn cog_core::KeyValueStore> = store;

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut summon = recording_summoner(Arc::clone(&seen), |_| Ok(Summon::Built));
        let pass = restore_roster_from(&store, &mut summon).await;

        assert_eq!(pass.named, 0);
        assert_eq!(pass.failed, 1);
        assert!(seen.lock().unwrap().is_empty());
    }

    /// 建不回来的人计一次失败，不挡住同一次对账里的下一个人。
    #[tokio::test]
    async fn one_employee_that_cannot_be_summoned_does_not_cost_the_next_its_turn() {
        let store: Arc<dyn cog_core::KeyValueStore> = Arc::new(FakeRosterStore::default());
        enroll(&store, "employee:first000", "employee", "owner")
            .await
            .unwrap();
        enroll(&store, "employee:second00", "employee", "owner")
            .await
            .unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut summon = recording_summoner(Arc::clone(&seen), |agent_id| {
            if agent_id == "employee:first000" {
                Err("the pool refused".to_string())
            } else {
                Ok(Summon::Built)
            }
        });
        let pass = restore_roster_from(&store, &mut summon).await;

        assert_eq!(pass.named, 2);
        assert_eq!(pass.restored, 1);
        assert_eq!(pass.failed, 1);
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn a_roster_row_reads_back_with_the_role_it_was_hired_for() {
        let row = RosterRow {
            role: "employee".into(),
            hired_by: "owner".into(),
            hired_at: chrono::Utc::now(),
        };
        let text = serde_json::to_string(&row).unwrap();
        let back: RosterRow = serde_json::from_str(&text).unwrap();
        assert_eq!(back.role, row.role);
        assert_eq!(back.hired_by, row.hired_by);
        assert_eq!(back.hired_at, row.hired_at);
    }

    /// 名册上每一行都要读得成一个名字，而路由用的 id 一个字节都不能变：
    /// 有读 id 的调用方，动了它就断；名字只是给读的人看的那一列。
    /// 池子是按产能起的那一批，小队是按任务起的那一批。合成一个数就答不了
    /// 任何问题：「人少」既可能是池子闲着，也可能是活停了。
    #[test]
    fn the_roster_counts_capacity_apart_from_demand() {
        let pool = cog_core::AgentRegistration::new(
            "luna-1822ab0e",
            "cog-agent",
            "127.0.0.1",
            "planner",
            "default",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        let squad = cog_core::AgentRegistration::new(
            cog_core::agent_id_for("task-1", "generator"),
            "cog-agent",
            "127.0.0.1",
            "generator",
            "default",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        assert_eq!(roster_split(&[pool, squad]), (1, 1));
    }

    #[test]
    fn the_roster_prints_a_name_next_to_the_id_it_keeps() {
        let reg = cog_core::AgentRegistration::new(
            cog_core::agent_id_for("task-1", "planner"),
            "cog-agent",
            "127.0.0.1",
            "planner",
            "default",
            vec!["planner".into()],
            cog_core::ResourceInfo::default(),
        );
        let view = agent_view(&reg);
        assert_eq!(view["agent_id"], reg.agent_id);
        // 名字不再由 id 算出来：它来自名册行（工号＋姓名），由 `identity_of`
        // 合并。这一行要是自己又印一个名字，两种拼法就会同时出现在名单上。
        assert!(
            view.get("display_name").is_none(),
            "agent_view must not print a name of its own"
        );
    }

    /// 名册上的一个人在名单上印成「工号 姓名」，而不是一串机器码。
    #[test]
    fn a_roster_row_prints_its_employee_number_and_name() {
        let reg = cog_core::AgentRegistration::new(
            cog_core::agent_id_for("task-1", "planner"),
            "cog-agent",
            "127.0.0.1",
            "planner",
            "default",
            vec!["planner".into()],
            cog_core::ResourceInfo::default(),
        );
        let now = chrono::Utc::now();
        let row = cog_core::EmployeeRecord {
            agent_id: reg.agent_id.clone(),
            number: Some(7),
            role: "planner".into(),
            first_seen: now,
            last_seen: now,
            retired_at: None,
        };
        let views = roster_with_people(
            std::slice::from_ref(&reg),
            Vec::new(),
            Vec::new(),
            vec![row],
        );
        assert_eq!(views.len(), 1, "一个人只印一行");
        let name = views[0]["display_name"].as_str().expect("display_name");
        assert_eq!(name, cog_core::employee_label(7));
        assert!(name.starts_with("E0007 "), "name {name} carries no 工号");
        assert!(
            !name.contains('#'),
            "name {name} still reads as a machine code"
        );
        assert_eq!(views[0]["number"], "E0007");
    }

    /// 退役的人在名单上留一行，标成离职；工号不因退役而变。
    #[test]
    fn a_retired_employee_keeps_its_row_and_its_number() {
        let now = chrono::Utc::now();
        let row = cog_core::EmployeeRecord {
            agent_id: "squad:gone-1-2026-11-07-planner".into(),
            number: Some(41),
            role: "planner".into(),
            first_seen: now,
            last_seen: now,
            retired_at: Some(now),
        };
        let views = roster_with_people(&[], Vec::new(), Vec::new(), vec![row]);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["retired"], true);
        assert_eq!(views[0]["display_name"], cog_core::employee_label(41));
    }

    /// 说过一句话的人，闭嘴之后还在名单上。
    ///
    /// 注册表那一行是心跳，说完几十秒就被摘掉；名单要回答的是「我跟谁说过话、
    /// 以后还找不找得到他」。所以这位员工下线之后仍然印出来，只是不再是
    /// `live`，并且带着他那段会话的 id——没有这一格，名单上这个人就只剩一个
    /// 点进去什么也没有的名字。
    #[tokio::test]
    async fn a_person_who_only_speaks_is_still_on_the_roster() {
        let store = Arc::new(FakeRosterStore::default());
        let at = chrono::Utc::now();
        let row = PersonRow {
            session_id: "conv-1".into(),
            display_name: "Kai#a9ab11fc".into(),
            first_seen: at,
            last_seen: at,
            platform: None,
            device_id: None,
        };
        store
            .hashes
            .lock()
            .unwrap()
            .entry(PEOPLE_KEY.to_string())
            .or_default()
            .insert(
                "kai-a9ab11fc".to_string(),
                serde_json::to_string(&row).unwrap(),
            );
        let store: Arc<dyn cog_core::KeyValueStore> = store;

        let views = roster_with_people(&[], read_people(&store).await, Vec::new(), Vec::new());

        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["agent_id"], "kai-a9ab11fc");
        // 没被编号的人印「待编」，不印那一串身份种子：种子看着像名字，于是
        // 没人去补这一次编号。
        assert_eq!(
            views[0]["display_name"],
            cog_core::unregistered_label("kai-a9ab11fc")
        );
        assert_eq!(views[0]["live"], false);
        assert_eq!(views[0]["session_id"], "conv-1");
    }

    /// 记来源之前写下的行仍然读得回来。
    ///
    /// 生产上那 29 行是升级前写的，没有 `platform` / `device_id`。这两个新列若
    /// 是必填，整张名册会在升级的那一刻被读成空的——名单上的人一个都不剩，而
    /// 读面只会显示「没有员工」。
    #[tokio::test]
    async fn a_people_row_written_before_provenance_still_parses() {
        let store = Arc::new(FakeRosterStore::default());
        store
            .hashes
            .lock()
            .unwrap()
            .entry(PEOPLE_KEY.to_string())
            .or_default()
            .insert(
                "kai-a9ab11fc".to_string(),
                r#"{"session_id":"conv-1","display_name":"Kai#a9ab11fc",
                    "first_seen":"2026-11-06T09:00:00Z","last_seen":"2026-11-06T09:00:01Z"}"#
                    .to_string(),
            );
        let store: Arc<dyn cog_core::KeyValueStore> = store;

        let people = read_people(&store).await;

        assert_eq!(people.len(), 1, "旧行读不回来，整张名册就空了");
        assert_eq!(people[0].1.platform, None);
        assert_eq!(people[0].1.device_id, None);
    }

    /// 同一个人两处都有时只印一行，并且那一行是「在岗」的。
    ///
    /// 印成两行，读的人会以为撞了名；只印名册那一行，一个正在说话的人会被读成
    /// 休息中。会话 id 从他那一行并到在岗的这条上：正在说话的人也要能接着聊。
    #[tokio::test]
    async fn a_person_on_both_rosters_is_printed_once_as_live() {
        let at = chrono::Utc::now();
        let reg = cog_core::AgentRegistration::new(
            "kai-a9ab11fc",
            "cog-agent",
            "127.0.0.1",
            "kai-a9ab11fc",
            "default",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        let row = PersonRow {
            session_id: "conv-1".into(),
            display_name: "Kai#a9ab11fc".into(),
            first_seen: at,
            last_seen: at,
            platform: None,
            device_id: None,
        };

        let views = roster_with_people(
            &[reg],
            vec![("kai-a9ab11fc".to_string(), row)],
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["live"], true);
        assert_eq!(views[0]["session_id"], "conv-1");
    }

    /// 招进来的人即使池里这一刻没有他，也还在名单上；池里有时只印一行。
    ///
    /// 池是内存的。Pod 重启到维护环把人建回来之间，读面若只读注册表，这位就
    /// 从名单上消失了——读的人分不出「没这个人」和「这个人这一刻不在池里」，
    /// 而后者正是重启后的常态。反过来，两处都有时印两行，读的人会以为撞了名。
    #[tokio::test]
    async fn a_hired_employee_outlives_the_pool() {
        let at = chrono::Utc::now();
        let row = RosterRow {
            role: "evaluator".into(),
            hired_by: "owner".into(),
            hired_at: at,
        };
        let hired = vec![("employee:abc123".to_string(), row.clone())];

        let views = roster_with_people(&[], Vec::new(), hired.clone(), Vec::new());
        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["agent_id"], "employee:abc123");
        assert_eq!(views[0]["role"], "evaluator");
        assert_eq!(views[0]["live"], false);
        assert_eq!(views[0]["hired_by"], "owner");

        let reg = cog_core::AgentRegistration::new(
            "employee:abc123",
            "cog-agent",
            "127.0.0.1",
            "employee:abc123",
            "default",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        let views = roster_with_people(&[reg], Vec::new(), hired, Vec::new());
        assert_eq!(views.len(), 1, "同一个人印两行，读的人会以为撞了名");
        assert_eq!(views[0]["live"], true);
    }

    /// 解析不了的那一行跳过：按位置猜它是谁，会把一个陌生人印成员工。
    #[tokio::test]
    async fn a_people_row_that_does_not_parse_is_skipped() {
        let store = Arc::new(FakeRosterStore::default());
        store
            .hashes
            .lock()
            .unwrap()
            .entry(PEOPLE_KEY.to_string())
            .or_default()
            .insert("broken".to_string(), "{not json".to_string());
        let store: Arc<dyn cog_core::KeyValueStore> = store;

        assert!(read_people(&store).await.is_empty());
    }

    /// A reply stream the caller names is taken out of the body rather than
    /// honoured, and the other two shapes -- a prompt with no reply stream and
    /// a command that is not a prompt at all -- go through. Reading only the
    /// refusal would not say the route still accepts anything.
    #[test]
    fn only_a_caller_named_reply_stream_is_refused() {
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Prompt {
                goal: serde_json::json!({"goal": "run the tests"}),
                reply_stream: Some("caller:reply:1".to_string()),
            }),
            Some("caller:reply:1")
        );
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Prompt {
                goal: serde_json::json!({"goal": "run the tests"}),
                reply_stream: None,
            }),
            None
        );
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Abort),
            None,
            "only a prompt can name a reply stream"
        );
    }
}
