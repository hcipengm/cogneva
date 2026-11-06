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
/// 名字由名册按 id 算出来（`display_name_for`），与每一个别的人同一个规则：一个
/// id 永远对应同一个名字，重读不会改名。
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
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "display_name": cog_core::display_name_for(&agent_id),
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
    /// 名册给这个人算出的名字（`Kai#a9ab11fc`）。写入时算一次：读面不该再
    /// 持有一份名字算法，两份算法迟早会有一个字母不一样。
    pub display_name: String,
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
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
    rows.sort_by(|a, b| b.1.last_seen.cmp(&a.1.last_seen));
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
                tracing::info!(
                    agent_id = %agent_id,
                    name = %cog_core::display_name_for(&agent_id),
                    hired_by = %row.hired_by,
                    "an employee named by the roster is back on the pool"
                );
            }
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

/// 生产环境的 summon：池里有人就交回，没有就建一个进程生命周期的人。
pub fn pool_summoner(
    pool: Arc<dyn cog_core::AgentManager>,
    llm: Arc<dyn cog_core::LlmClient>,
) -> SummonFn {
    Box::new(move |agent_id, role| {
        let pool = pool.clone();
        let llm = llm.clone();
        Box::pin(async move {
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
    let mut summon = pool_summoner(pool.clone(), llm);
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
        let AskOutcome::Replied(reply) = outcome else {
            return;
        };
        // A string answer is what the agent said; any other shape is kept as
        // its JSON text so the transcript never drops an answer it does not
        // recognise.
        let text = match &reply {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
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
        Ok(true) => {
            if state
                .event_tx
                .send(cog_core::AgentEvent::ResourceChanged {
                    domain: "agents".to_string(),
                    timestamp: chrono::Utc::now(),
                })
                .is_err()
            {
                // 没有接收者不是错误：这一帧的读者是打开着的那一面，一个都没有
                // 的时候它本来就无处可去。写下去的那一行仍然在库里。
            }
        }
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
/// The routing id stays exactly as registered — it is derived from (task, role)
/// at both ends and cannot carry a name — and the readable name is derived from
/// that id here, at the one surface a person reads. Storing the name instead
/// would let the two drift: a record whose id was rewritten would keep a name
/// that no longer resolves to it.
fn agent_view(reg: &cog_core::AgentRegistration) -> serde_json::Value {
    let mut view = serde_json::to_value(reg).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = view.as_object_mut() {
        obj.insert(
            "display_name".to_string(),
            serde_json::json!(cog_core::display_name_for(&reg.agent_id)),
        );
    }
    view
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

/// 名单：心跳上此刻活着的人，加上说过话但已经下线的人。
///
/// 同一个人两处都有时只印一行——注册表那一行回答「此刻在不在」，名册那一行
/// 回答「这个人存不存在」。印成两行会让读的人以为撞了名，而其中一个点进去
/// 什么也没有。
///
/// `live` 是这次合并唯一的判断位：有它是此刻叫得到，没有是这个人说过话、现在
/// 不在岗。界面据此决定这一格是「在岗」还是「休息中」，以及能不能用对话那条路
/// 接着聊（只有说过话的人带着 `session_id`）。
fn roster_with_people(
    regs: &[cog_core::AgentRegistration],
    people: Vec<(String, PersonRow)>,
) -> Vec<serde_json::Value> {
    let mut views: Vec<serde_json::Value> = regs
        .iter()
        .map(|reg| {
            let mut view = agent_view(reg);
            if let Some(obj) = view.as_object_mut() {
                obj.insert("live".to_string(), serde_json::json!(true));
            }
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
        }
    }
    let live_ids: std::collections::HashSet<&str> =
        regs.iter().map(|reg| reg.agent_id.as_str()).collect();
    // 最近说过话的排在前面：名单是一次扫读，先出现的人该是此刻最可能要找的。
    let mut idle: Vec<(String, PersonRow)> = by_id
        .into_iter()
        .filter(|(agent_id, _)| !live_ids.contains(agent_id.as_str()))
        .collect();
    idle.sort_by(|a, b| b.1.last_seen.cmp(&a.1.last_seen));
    for (agent_id, row) in idle {
        views.push(serde_json::json!({
            "agent_id": agent_id,
            "display_name": row.display_name,
            "role": "chat",
            "workspace_id": "default",
            "capabilities": Vec::<String>::new(),
            "hostname": String::new(),
            "pod_ip": String::new(),
            "registered_at": row.first_seen,
            "last_heartbeat": row.last_seen,
            "live": false,
            "session_id": row.session_id,
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
            let people_count = people
                .iter()
                .filter(|(id, _)| !regs.iter().any(|reg| &reg.agent_id == id))
                .count();
            let agents = roster_with_people(&regs, people);
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
        let name = view["display_name"].as_str().expect("display_name present");
        assert!(name.contains('#'), "name {name} is not a handle");
        assert_eq!(name, cog_core::display_name_for(&reg.agent_id));
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

        let views = roster_with_people(&[], read_people(&store).await);

        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["agent_id"], "kai-a9ab11fc");
        assert_eq!(views[0]["display_name"], "Kai#a9ab11fc");
        assert_eq!(views[0]["live"], false);
        assert_eq!(views[0]["session_id"], "conv-1");
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
        };

        let views = roster_with_people(&[reg], vec![("kai-a9ab11fc".to_string(), row)]);

        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["live"], true);
        assert_eq!(views[0]["session_id"], "conv-1");
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
