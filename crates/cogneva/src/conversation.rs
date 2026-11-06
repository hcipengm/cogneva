//! 界面上一段对话的一轮，跑在系统唯一的那条智能体循环上。
//!
//! 这里只做接线：把编排器的控制面做成一组工具（读台账、派活、催跑、撤回），把
//! 系统自己那三本账（变更、原则、告警）做成另一组只读工具，装进智能体内核的
//! 工具表，再用调用方给好的历史跑一轮。循环、守卫、预算、token 账与总线事件都在
//! 内核里发生——与一件任务的一次尝试是同一条，不是并排的两条。
//!
//! 三本账让「这件事到哪一步了」由员工自己读出来：账上写着什么它就看见什么，不
//! 必等谁去翻一下再回来告诉它。进化闭环的反馈就落在这三本账里——哪条需求还没落
//! 地、哪条原则还没有门、哪里正亮着，读到的是读数而不是印象。
//!
//! 工具集就是**这个进程装配好的那一份**：它有什么工具由身体决定，不由"它是个什么
//! 角色"决定。界面上的员工与 worker 池、小队是同一种数字员工，只是生命周期挂在会话
//! 上——会话开时它在，会话关时它走，实例可以换，名字与 id 不换。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

// 工具表的写入方法在 `cog_core` 的契约 trait 上，实现是内核的注册表：两个同名，
// 这里要的是前者的方法，实现按后者取。
use cog_core::contract::tool::{Tool, ToolHandler, ToolImplementation, ToolRegistry as _};
use cog_core::{
    AgentEvent, AuditKind, AuditStream, HierarchyManager, OrchestratorControl, QuotaContext,
    RuntimeConfig, SFError, SFResult,
};
use cog_gateway::chat::{ConversationRequest, ConversationTurn};

/// 这一段对话在名册里属于哪个分组。
///
/// 分组只参与名字的哈希材料，把界面上这些员工与 worker 池、进化池的同号槽位
/// 分开。它是清单里写得出来的字面量，所以同一个会话滚一次 Pod 还是同一个名字。
const CHAT_GROUP: &str = "web";

/// 名册上那一条多久续一次。
///
/// 名册的过期是 30 秒：一轮对话动不动就超过它，不续的话它会在自己还在说话的
/// 时候从名册上消失——同事这时候发给它的消息就投进一个没人认领的收件箱。
// cap: bound — 名册过期 30 秒，续约间隔取它的三分之一
const REGISTRATION_HEARTBEAT_SECS: u64 = 10;

/// 一次开跑最多读几条积压消息。
///
/// 上下文的容量是有限的，收件箱不是：一晚上攒了五十条，全塞进这一轮会把这一轮
/// 自己的问题挤出去。读不完的留在流里，下一轮接着读。
// cap: bound — 一轮对话的上下文里，收件箱这一块最多占几条
const INBOX_DRAIN_MAX: usize = 16;

/// 读收件箱时，等一条新消息等多久算「这一轮没有更多了」。
///
/// 这是人正等着的一段对话，不是后台消费者：静默窗口按人的耐心取，而不是按
/// 总线的延迟上限取。
// cap: bound — 收件箱排空的静默窗口，按一段对话能接受的额外等待取
const INBOX_QUIET_MS: u64 = 200;

/// 一次开跑花在读收件箱上的总预算。
///
/// 消息接连不断地来也不无限读：这条界与条数界互为兜底，谁先到谁停。
// cap: bound — 开跑前读收件箱的总时间预算
const INBOX_DRAIN_BUDGET_MS: u64 = 1_500;

/// 领回「上一轮读到一半就断了」的消息，等它闲置多久。
///
/// 与后台消费者同一个取法：同一段会话的两轮不会同时在跑，所以这个窗口只要
/// 大过一轮正常对话的读耗时，就不会把另一个人正在读的那条抢过来。
// cap: bound — 回收待处理消息的最小闲置时长
const INBOX_TAKEOVER_IDLE_MS: u64 = 5_000;

/// 一次回收调用要多少条。
// cap: bound — 一次回收调用要多少条待处理消息
const INBOX_TAKEOVER_BATCH: usize = 16;

/// 一次 `list_tasks` 最多回多少行。
// cap: bound — 一次读台账回多少行；整张图会把上下文和这次回答一起淹掉
const LIST_TASKS_LIMIT: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: bound — 默认页只有上限的一半：上限是显式要多的人能拿到的，默认值给的是「一眼扫完、还能接着问」的量
const LIST_TASKS_DEFAULT: usize = 20;

/// 一次读变更账向账本要多少行，也是调用方能要到的上限。
///
/// 未了结的那本账有几千行，全取回来会把这一轮的上下文和这次回答一起淹掉。取回来
/// 再按调用方的 `match`/`state` 筛，所以窗口要够筛而不是刚好够看。
// cap: bound — 一次读变更账向账本要多少行
const LIST_CHANGES_WINDOW: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: bound — 默认页是窗口的一半：够扫一眼，也留得住接着追问的余地
const LIST_CHANGES_DEFAULT: usize = 20;
/// 一条变更的历史最多回几步。
// cap: bound — 一次 `get_change` 回多少条状态迁移；更早的那几步读不出「现在在哪」以外的信息
const CHANGE_HISTORY_LIMIT: usize = 20;
/// 一次读原则账向账本要多少行，也是调用方能要到的上限。
// cap: bound — 一次读原则账向账本要多少行
const LIST_PRINCIPLES_WINDOW: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: bound — 默认页是窗口的一半，理由同变更账那一对
const LIST_PRINCIPLES_DEFAULT: usize = 20;
/// 一次读告警最多回几条。
// cap: bound — 一次读告警回几条；亮着的告警多到看不完时，本身就是要先说出来的事
const ACTIVE_ALERTS_LIMIT: usize = 20;

/// 这一段对话在名册里的身份：名字与 id。
///
/// 生命周期就是会话——同一个 `session_id` 跨轮、跨重启都是同一个员工，换一次
/// 实例不换名字。名字用的与本进程 worker 池同一个纯函数（人名 ＋ 指纹短码），
/// 所以账上、名册上、消息上读到的都是同一个人；而"每轮一个 `web-chat-<uuid>`"
/// 读起来像每轮换一个从没见过的员工，按 id 记下的归属也就跟着换人。
///
/// 指纹缺失时退回进程自身的装配标识：名字仍然唯一，但它只稳定到下一次滚动，
/// 于是 `stable` 如实置 false——这是"滚一次会不会换名"的那一位，不能糊。
pub fn employee_for(session_id: &str) -> cog_agent::identity::AgentName {
    let fingerprint = std::env::var(cog_core::env::INSTANCE_FINGERPRINT)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    employee_named(fingerprint.as_deref(), session_id)
}

/// 名字的算法本体：种子是「实例指纹 ＋ 会话」，两样都变才换人。
///
/// 单独拆出来是为了让判据问得到它——环境变量在同一个测试进程里是全局的，
/// 平行跑的两条测试互相改它，读到的就不是自己设的那个值。
fn employee_named(fingerprint: Option<&str>, session_id: &str) -> cog_agent::identity::AgentName {
    match fingerprint {
        Some(seed) => cog_agent::identity::name_for(&format!("{seed}:{session_id}"), CHAT_GROUP, 0),
        // 指纹缺失时用进程自身的装配标识顶上：名字仍然唯一，但滚一次 Pod 就换一个
        // 人，于是 `stable` 如实置 false——这一位说的是「跨重启还是不是同一个人」。
        None => {
            let mut name = cog_agent::identity::name_for(
                &format!("{}:{}", cog_agent::identity::agent_instance(), session_id),
                CHAT_GROUP,
                0,
            );
            name.stable = false;
            name
        }
    }
}

/// 装配层交给网关的那条循环：一轮对话就是内核的一次运行。
pub struct PlatformConversation {
    orchestrator: Arc<dyn OrchestratorControl>,
    /// 派活之前要过的那道准入。与 HTTP 提交面读的是同一个管理器、同一份估值：
    /// 少这一道，一个人今天用光的额度只挡得住 HTTP，挡不住界面上的一句话，
    /// 而两者要的是同一份钱。
    admission: Option<Arc<dyn HierarchyManager>>,
    /// 拒绝也要落一条。提交面拒单时写 `AuditKind::QuotaEnforcement`；界面上
    /// 同一句话被同一条规则挡住却不写，账上就只剩「有人今天没派活」。
    audit: Option<Arc<dyn AuditStream>>,
    /// 这个进程装配好的那份工具表——与 worker 池、小队用的是同一份。
    ///
    /// 这里不另列一份工具清单：手上有什么工具由**身体所在的进程**决定（有没有
    /// 消息队列、有没有沙盒、有没有出门的客户端），不是由"它是个什么角色"决定。
    /// 另写一份短表就等于在没有沙盒的进程里假装没有执行能力，在有沙盒的进程里
    /// 又把能力藏起来——两种都是把一个数字员工做成一个按钮。
    ///
    /// 取的是内核那份具体注册表而不是 `dyn` 契约：契约只有 `register` 与
    /// `names`，能把一只工具装进去、能报出名字，却读不回工具本身，也就复制不了。
    tools: Option<Arc<cog_agent::ToolRegistry>>,
    /// 名册。有它，这个员工在说话的那段时间里对同事是可见的——`list_agents`
    /// 列得出它的名字，`send_to_agent` 找得到这个地址。
    roster: Option<Arc<dyn cog_core::AgentRegistry>>,
    /// 收件箱。与名册是两半：只有名册没有收件箱，消息投进一个没人读的流；只有
    /// 收件箱没有名册，同事根本发现不了这个地址。缺哪一半都由进程如实缺席。
    inbox: Option<Arc<dyn cog_core::MessageBackend>>,
    /// 「说过话的人」名册。注册表在他闭嘴几十秒后就把他摘掉，而人要找的是
    /// 「那个跟我聊过的人还在不在」——这条记录不过期，所以它和心跳不是一件事。
    people: Option<Arc<dyn cog_core::KeyValueStore>>,
    /// 三本只读账。读它们回答的是「这件事现在到哪一步了」——答案来自账，不是
    /// 来自印象，也不是从某面墙上抄下来的一段话。
    ledgers: Ledgers,
}

impl PlatformConversation {
    // 八个入参各自是一件可选能力，没有哪几件天然该合成一个名词：会话要的是
    // 「这个部署里装了哪些」，把其中几个打包只会让调用方多记一个类型名，
    // 而少掉的参数个数并不能让这行更好读。构造完就各归各位，没有第二处
    // 地方再按这个分组使用它们。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        orchestrator: Arc<dyn OrchestratorControl>,
        admission: Option<Arc<dyn HierarchyManager>>,
        audit: Option<Arc<dyn AuditStream>>,
        tools: Option<Arc<cog_agent::ToolRegistry>>,
        roster: Option<Arc<dyn cog_core::AgentRegistry>>,
        inbox: Option<Arc<dyn cog_core::MessageBackend>>,
        people: Option<Arc<dyn cog_core::KeyValueStore>>,
        ledgers: Ledgers,
    ) -> Self {
        Self {
            orchestrator,
            admission,
            audit,
            tools,
            roster,
            inbox,
            people,
            ledgers,
        }
    }
}

/// 把这一位说过话的员工记进名册。
///
/// 第一次写下 `first_seen`，之后每一轮只推进 `last_seen`：把第一次见过的时刻
/// 每轮重写成现在，「这个人来了多久」就永远读成「刚刚」。先读再写是有意的——
/// 这一格只在人第一次说话时写一次，丢一次并发只会让两个时间都落在同一轮附近，
/// 而读失败时退回「现在」不会让谁少一行。
pub(crate) async fn note_person(
    kv: &dyn cog_core::KeyValueStore,
    employee: &cog_agent::identity::AgentName,
    session_id: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> cog_core::SFResult<()> {
    let first_seen = match kv
        .hget(cog_gateway::agents::PEOPLE_KEY, &employee.id)
        .await?
    {
        Some(text) => serde_json::from_str::<cog_gateway::agents::PersonRow>(&text)
            .map(|row| row.first_seen)
            .unwrap_or(now),
        None => now,
    };
    let row = cog_gateway::agents::PersonRow {
        session_id: session_id.to_string(),
        display_name: employee.handle.clone(),
        first_seen,
        last_seen: now,
    };
    let text = serde_json::to_string(&row)?;
    kv.hset(cog_gateway::agents::PEOPLE_KEY, &employee.id, &text)
        .await
}

impl ConversationTurn for PlatformConversation {
    fn turn<'a>(
        &'a self,
        request: ConversationRequest,
        events: tokio::sync::mpsc::Sender<AgentEvent>,
    ) -> Pin<Box<dyn Future<Output = SFResult<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            // 这一段对话是谁：名字由会话推出，跨轮不变。派活与记账都按这个名字。
            let employee = employee_for(&request.session_id);
            // 先把他记进「说过话的人」名册：注册表上的心跳在他闭嘴几十秒后就
            // 过期，而人要找的是「那个跟我聊过的人」。写不进去不是这一轮的
            // 失败——他照样能说话，只是名单上从此少一个人；这一句是那个差别的
            // 唯一读数。
            if let Some(kv) = self.people.as_ref() {
                if let Err(e) =
                    note_person(&**kv, &employee, &request.session_id, chrono::Utc::now()).await
                {
                    tracing::warn!(
                        agent_id = %employee.id,
                        error = %e,
                        "这位员工没有记进名册：他在名单上不会出现"
                    );
                }
            }
            let config = RuntimeConfig {
                agent_id: employee.id.clone(),
                // 没有角色：这一段对话就是这位员工本人。role 取它的 id，内核因此
                // 按 `agent:<id>` 落账、按 id 取预算与停滞窗口——账上、名册上、
                // 日志里读到的是同一个名字，而不是「界面上那一类」。
                role: employee.id.clone(),
                // 没有轮数上限：这一轮什么时候结束，由**模型自己不再要工具**来定，
                // 就像人说话——问一句「你查完了没有」不该由一份常量代答。这里曾经
                // 写着一个比平台默认还小的数，界面上这位于是比任何同事都早被要求
                // 「别查了，先答」，而那正是它答不出「这件事落地了没有」的一半原因。
                //
                // 止损不靠轮数：卡住由内核的单步停滞超时兜底，上下文有自己的窗口，
                // 花销记在使用账上。轮数是这三样缺位时的替代品，这里不再拿它当尺。
                max_iterations: u32::MAX,
                ..Default::default()
            };
            // 工具面 = 这个进程装配好的那一份 ＋ 控制面五只。
            //
            // 复制的是**整张注册表**（`fork`），不是工具清单：哪些工具存在只是
            // 一半，另一半是它们怎么跑——沙箱后端、护栏、插件源、两个超时，
            // 全都挂在注册表上。把 `list()` 装进一个空的注册表，搬过去的是一张
            // 看得见却跑不动的名单：`run_command` 每次调用都回
            // `SandboxBackend not configured for shell tool`，屏幕上就是「这位员工
            // 从不调用工具」。实测 2026-11-06 界面对话里的员工四次调用全落在这句上。
            //
            // 表本身仍是这一轮私有的：控制面五只只属于这一段对话，注册进共用表里
            // 会变成每个 agent 都能派活。
            let tools = match self.tools.as_ref() {
                Some(shared) => shared.fork(),
                None => cog_agent::ToolRegistry::new(),
            };
            // 三本只读账与控制面五只同一处装：它们读的也是这个进程里已发布的
            // 服务，缺的那本由进程如实缺席，而工具会把它说出来。
            for tool in ledger_tools(&self.ledgers) {
                tools.register(tool);
            }
            // 改自己：一只，只申请不执行。它与上面那四只同一处装、同一本能读也能
            // 写的账——读账是「现在到哪一步」，申请是「把它往前推一步」，两件事
            // 用的是同一条记录，所以问「我提的那条呢」时答案就在它刚读过的地方。
            tools.register(crate::self_change::tool(
                self.ledgers.changes.clone(),
                employee.id.clone(),
            ));
            for tool in control_plane_tools(
                self.orchestrator.clone(),
                self.admission.clone(),
                self.audit.clone(),
                request.owner.clone(),
                employee.id.clone(),
            ) {
                tools.register(tool);
            }
            // 名册上「它能做什么」就是这一轮手上真有的工具，不是一段标签。
            // 这里曾经填的是它自己的 id：能力字段里装着一个名字，读名册的人
            // 看到的是「mira 的能力是 mira」，按能力找人的那一步什么也筛不出来。
            // 员工是通用智能体：它没有预设，有的只是这个进程给它的工具；具体
            // 做哪件活，由拿到的那句话决定。
            let capabilities = tools.names();
            let mut runtime = cog_agent::AgentRuntime::new(config, events)
                .with_tools(tools)
                // 没有身份的连接不声明人：编一个名字会读成真有人花过这笔钱。
                .with_call_identity(
                    request.owner.clone(),
                    cog_core::LlmOrigin::Interactive.as_str(),
                );

            // 开跑前先读收件箱：同事在它不在的时候发来的话，这一轮要看得见。
            // 读完就确认，下一轮只读新的；回信地址跟着消息一起带进来，等这一轮
            // 有了答案再投回去。
            let mut askers: Vec<String> = Vec::new();
            let mut history: Vec<cog_core::Message> = Vec::new();
            if let Some(backend) = self.inbox.as_ref() {
                for msg in drain_inbox(backend, &employee.id).await {
                    if let cog_core::InboxMessage::Prompt {
                        reply_stream: Some(stream),
                        ..
                    } = &msg
                    {
                        askers.push(stream.clone());
                    }
                    if let Some(text) = inbox_as_message(&msg) {
                        history.push(cog_core::Message::user(format!("[同事发来的消息]\n{text}")));
                    }
                }
            }
            history.extend(request.messages);

            // 说话的这一段时间它在名册上活着：续约间隔比名册的过期短，否则一轮
            // 长对话会在自己还在说的时候从名册上消失。
            let lease = match self.roster.as_ref() {
                Some(roster) => {
                    Some(EmployeeLease::take(roster.clone(), &employee, capabilities).await)
                }
                None => None,
            };

            let answer = runtime
                .run_conversation(history, request.llm.as_ref())
                .await;

            // 说完就下线：名下没有常驻进程，名册上留着一条过期的记录只会让同事
            // 以为它还醒着。
            if let Some(lease) = lease {
                lease.release().await;
            }

            // 问它的人要的是答案。等这一轮跑完，把结果投给每一个带回来的回信地址；
            // 投不出去要说出来——发送方那边正在等，而这边已经没什么可重试的了。
            if let (Some(backend), Ok(value)) = (self.inbox.as_ref(), answer.as_ref()) {
                for stream in &askers {
                    match serde_json::to_vec(value) {
                        Ok(payload) => {
                            if let Err(e) = backend.publish(stream, &payload).await {
                                tracing::warn!(stream = %stream, error = %e, "回信没有投出去");
                            }
                        }
                        Err(e) => tracing::warn!(stream = %stream, error = %e, "回信序列化失败"),
                    }
                }
            }
            answer
        })
    }
}

/// 这个员工在名册上活着的那一段时间：一轮对话。
///
/// 它名下没有常驻进程，所以「活着」是一次登记加一条续约；说完话就注销。崩掉
/// 的那一轮由名册自己的过期兜底——最多多留一个过期周期，不会永远挂在那儿。
struct EmployeeLease {
    roster: Arc<dyn cog_core::AgentRegistry>,
    agent_id: String,
    beat: tokio::task::JoinHandle<()>,
}

impl EmployeeLease {
    async fn take(
        roster: Arc<dyn cog_core::AgentRegistry>,
        employee: &cog_agent::identity::AgentName,
        capabilities: Vec<String>,
    ) -> Self {
        let registration = cog_core::AgentRegistration::new(
            employee.id.clone(),
            cog_agent::identity::agent_instance(),
            "127.0.0.1",
            employee.id.clone(),
            "default",
            capabilities,
            cog_core::ResourceInfo::default(),
        );
        if let Err(e) = roster.register(&registration).await {
            // 登记不上不是这一轮的失败：它照样能说话、能派活，只是同事在名册上
            // 看不到它。这一句就是那个差别的唯一读数。
            tracing::warn!(
                agent_id = %employee.id,
                error = %e,
                "名册登记失败：这一轮同事看不到它"
            );
        }
        let beat = tokio::spawn(heartbeat_loop(roster.clone(), registration));
        Self {
            roster,
            agent_id: employee.id.clone(),
            beat,
        }
    }

    async fn release(self) {
        self.beat.abort();
        if let Err(e) = self.roster.deregister(&self.agent_id).await {
            tracing::warn!(agent_id = %self.agent_id, error = %e, "名册注销失败");
        }
    }
}

/// 续约到这一轮结束为止。
///
/// 心跳失败时重新登记：名册里的键有有效期，丢了的那一条不该让这个员工在剩下的
/// 对话里一直隐身，而日志里除了一行警告什么都看不出来。
async fn heartbeat_loop(
    roster: Arc<dyn cog_core::AgentRegistry>,
    registration: cog_core::AgentRegistration,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(REGISTRATION_HEARTBEAT_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if let Err(e) = roster.heartbeat(&registration.agent_id).await {
            if let Err(re) = roster.register(&registration).await {
                tracing::warn!(
                    agent_id = %registration.agent_id,
                    error = %e,
                    reregister = %re,
                    "名册续约失败"
                );
            }
        }
    }
}

/// 把收件箱里积压的消息读出来，读了就确认。
///
/// 位置记在组上，不记在这个进程里：所以它不在的那段时间收到的消息，下一轮开跑
/// 时照样读得到——员工本身无状态，队列就是它的状态。
async fn drain_inbox(
    backend: &Arc<dyn cog_core::MessageBackend>,
    agent_id: &str,
) -> Vec<cog_core::InboxMessage> {
    let stream = cog_core::agent_inbox_stream(agent_id);
    // 与后台消费者同一个组名：一个员工只有一条收件箱，谁读都从同一个位置接着读。
    let group = format!("agent-{agent_id}");
    if let Err(e) = backend.create_consumer_group(&stream, &group).await {
        tracing::warn!(agent_id, error = %e, "收件箱的组建不出来：这一轮读不到消息");
        return Vec::new();
    }

    let mut out = Vec::new();

    // 上一轮读到一半就断了（Pod 被滚掉、连接断）的条目还在待处理队列里，组的
    // 位置没有越过它们。先领回来，否则它们要等下一次有人认领才动。
    loop {
        match backend
            .claim_pending(
                &stream,
                &group,
                INBOX_TAKEOVER_IDLE_MS,
                INBOX_TAKEOVER_BATCH,
            )
            .await
        {
            Ok(Some(batch)) if !batch.is_empty() => {
                let full = batch.len() == INBOX_TAKEOVER_BATCH;
                let ids: Vec<String> = batch.iter().map(|(id, _)| id.clone()).collect();
                for (_, bytes) in &batch {
                    push_or_log(&mut out, bytes, agent_id);
                }
                if let Err(e) = backend.ack(&stream, &group, &ids).await {
                    tracing::warn!(agent_id, error = %e, "领回来的消息确认不了，下一轮会再读到");
                }
                if !full || out.len() >= INBOX_DRAIN_MAX {
                    return out;
                }
            }
            Ok(_) => break,
            Err(e) => {
                tracing::warn!(agent_id, error = %e, "回收待处理消息失败");
                break;
            }
        }
    }

    let mut reader = match backend.subscribe(&stream, &group).await {
        Ok(reader) => reader,
        Err(e) => {
            tracing::warn!(agent_id, error = %e, "订阅收件箱失败");
            return out;
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(INBOX_DRAIN_BUDGET_MS);
    while out.len() < INBOX_DRAIN_MAX {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        let window = left.min(Duration::from_millis(INBOX_QUIET_MS));
        match tokio::time::timeout(window, reader.next()).await {
            Ok(Some(Ok((id, bytes)))) => {
                push_or_log(&mut out, &bytes, agent_id);
                if let Err(e) = backend.ack(&stream, &group, &[id]).await {
                    tracing::warn!(agent_id, error = %e, "消息确认不了，下一轮会再读到");
                }
            }
            Ok(Some(Err(e))) => {
                tracing::warn!(agent_id, error = %e, "收件箱读到一半出错");
                break;
            }
            Ok(None) => break,
            // 静默窗口里没有新消息：这一轮就这些。读被打断的那一条若服务端已经
            // 发出来了，它留在组里的待处理队列上，下一轮按闲置时长领回来。
            Err(_) => break,
        }
    }
    out
}

/// 一条读得懂就收下，读不懂就跳过。
///
/// 认不出来的字节重试也不会认出来，留在流里等于把这个收件箱卡死。跳过是有意的：
/// 它响应的是「这一条读不懂」，不是「这一条以后能读懂」。
fn push_or_log(out: &mut Vec<cog_core::InboxMessage>, bytes: &[u8], agent_id: &str) {
    match serde_json::from_slice(bytes) {
        Ok(msg) => out.push(msg),
        Err(e) => tracing::warn!(agent_id, error = %e, "收件箱里有一条读不懂的消息，已跳过"),
    }
}

/// 收件箱里的一条怎么进这一轮的历史。
///
/// 线上没有发信人：命令的线上形态只带指令，不带谁写的（发信人那半在看到回执的
/// 发送方手里）。所以这里只说「同事发来的」，不编一个名字。
fn inbox_as_message(msg: &cog_core::InboxMessage) -> Option<String> {
    use cog_core::InboxMessage;
    match msg {
        InboxMessage::Prompt { goal, .. } => Some(match goal {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        }),
        InboxMessage::Continue { instruction } | InboxMessage::Steering { instruction } => {
            Some(instruction.clone())
        }
        // 撤停与重置是给正在跑的那一轮的指令，不是一条要说的话。
        InboxMessage::Abort | InboxMessage::Reset => None,
    }
}

/// 这段对话能用的操作：**只读台账、只派活、不执行**。
///
/// 闭集写成枚举而不是一张名字表：每一项在 [`ControlPlaneOp::tool`] 里必须配一个
/// handler（match 是穷尽的），所以「提供给模型的工具」与「真的会执行的分支」不会
/// 分叉——只提供不执行在模型眼里等于没有这个工具，而它会一直试下去。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ControlPlaneOp {
    DispatchGoal,
    ListTasks,
    GetTask,
    RetryTask,
    CancelTask,
}

/// 这一段对话交给模型的全部工具，按给模型看的顺序。
const CONTROL_PLANE: &[ControlPlaneOp] = &[
    ControlPlaneOp::DispatchGoal,
    ControlPlaneOp::ListTasks,
    ControlPlaneOp::GetTask,
    ControlPlaneOp::RetryTask,
    ControlPlaneOp::CancelTask,
];

impl ControlPlaneOp {
    fn name(self) -> &'static str {
        match self {
            ControlPlaneOp::DispatchGoal => "dispatch_goal",
            ControlPlaneOp::ListTasks => "list_tasks",
            ControlPlaneOp::GetTask => "get_task",
            ControlPlaneOp::RetryTask => "retry_task",
            ControlPlaneOp::CancelTask => "cancel_task",
        }
    }

    fn description(self) -> &'static str {
        match self {
            ControlPlaneOp::DispatchGoal => {
                "把一件要做的事交给编排器：它会分解成任务、派给执行者。返回目标号——\
                 回复里引用它时必须照抄工具返回的那个值，不许自己编；分解是异步的，\
                 不要说它已经跑完。"
            }
            ControlPlaneOp::ListTasks => {
                "读任务台账：按状态或目标号筛出若干条任务，新的在前。回答「进展、谁在做、\
                 卡在哪」之前先读它，不要凭印象回答。"
            }
            ControlPlaneOp::GetTask => "读一条任务的全文，含它的输入与结果载荷。",
            ControlPlaneOp::RetryTask => "重新跑一条失败或卡住的任务。",
            ControlPlaneOp::CancelTask => {
                "撤回一条任务，并说明理由。理由会落在这条行上——没有理由的取消在读面上\
                 与「不知道谁为什么把它停了」没有分别。"
            }
        }
    }

    fn parameters(self) -> serde_json::Value {
        match self {
            ControlPlaneOp::DispatchGoal => serde_json::json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "要做的事，一句话说清做什么与怎样算做完"
                    }
                },
                "required": ["goal"]
            }),
            ControlPlaneOp::ListTasks => serde_json::json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "description": "只看这一种状态（pending / scheduled / running / completed / failed / cancelled）"
                    },
                    "goal_id": {
                        "type": "string",
                        "description": "只看这一道目标下的任务"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "最多回几行，默认 20"
                    }
                }
            }),
            ControlPlaneOp::GetTask | ControlPlaneOp::RetryTask => serde_json::json!({
                "type": "object",
                "properties": {
                    "task_id": { "type": "string", "description": "任务号" }
                },
                "required": ["task_id"]
            }),
            ControlPlaneOp::CancelTask => serde_json::json!({
                "type": "object",
                "properties": {
                    "task_id": { "type": "string", "description": "任务号" },
                    "reason": { "type": "string", "description": "为什么撤回" }
                },
                "required": ["task_id", "reason"]
            }),
        }
    }

    /// 这一项装成内核认的工具：名字、说明、参数结构，以及那个一定存在的执行分支。
    fn tool(
        self,
        orchestrator: Arc<dyn OrchestratorControl>,
        admission: Option<Arc<dyn HierarchyManager>>,
        audit: Option<Arc<dyn AuditStream>>,
        owner: String,
        actor: String,
    ) -> Tool {
        let handler = match self {
            ControlPlaneOp::DispatchGoal => {
                dispatch_handler(orchestrator, admission, audit, owner, actor)
            }
            ControlPlaneOp::ListTasks => list_handler(orchestrator),
            ControlPlaneOp::GetTask => get_handler(orchestrator),
            ControlPlaneOp::RetryTask => retry_handler(orchestrator),
            ControlPlaneOp::CancelTask => cancel_handler(orchestrator),
        };
        Tool {
            name: self.name().to_string(),
            description: self.description().to_string(),
            parameters: self.parameters(),
            implementation: ToolImplementation::Native(handler),
        }
    }
}

/// 这段对话的工具集。
fn control_plane_tools(
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
    actor: String,
) -> Vec<Tool> {
    CONTROL_PLANE
        .iter()
        .map(|op| {
            op.tool(
                orchestrator.clone(),
                admission.clone(),
                audit.clone(),
                owner.clone(),
                actor.clone(),
            )
        })
        .collect()
}

/// 三本只读账：变更、原则、告警。
///
/// 它们在本进程里就是已发布的服务——与网关上那些面板读的是同一批行，没有第二份
/// 拷贝、没有第二个口径。员工因此不必等谁去翻一下再回来告诉它：一件事现在到哪
/// 一步、有没有门管着、哪里正亮着，它自己读得到，读到的就是账上写着的。
///
/// 只读是这一层的边界，不是员工能力的边界：要改账就得走它自己的那条门（派活、
/// 提交、拒绝），而「判定这件事做完了没有」永远由看树的那一侧写，不由声称做完
/// 的那一侧写。
#[derive(Clone, Default)]
pub struct Ledgers {
    /// 变更账：一条需求现在在哪一步，最后一次量是什么时候、读的哪个版本。
    pub changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>,
    /// 原则账：一条原则的正文、载体、它在哪会红、现在有没有门。
    pub principles: Option<Arc<dyn cog_core::PrincipleLedgerStore>>,
    /// 告警：现在亮着的那几条。
    pub alerts: Option<Arc<dyn cog_core::AlertStore>>,
}

/// 三本账装成四只工具：两只看变更（列表与单条），一只看原则，一只看告警。
fn ledger_tools(ledgers: &Ledgers) -> Vec<Tool> {
    vec![
        list_changes_tool(ledgers.changes.clone()),
        get_change_tool(ledgers.changes.clone()),
        list_principles_tool(ledgers.principles.clone()),
        active_alerts_tool(ledgers.alerts.clone()),
    ]
}

/// 一本账在这个进程里缺席时回的那一条。
///
/// 不能回一个空列表：「这个进程没接到账」与「账上一条都不欠」是两个相反的读数，
/// 而把它们读成同一个的正是正在追这件事的人——一条真欠着的活就此消失，谁也不会
/// 再去问它。
fn ledger_absent(what: &str) -> serde_json::Value {
    serde_json::json!({
        "available": false,
        "note": format!(
            "这个进程没有接到{what}：本部署里它没有被发布，所以这里读不到。\
             这是「读不到」，不是「{what}上没有东西」——别把这一句转述成「没有」。"
        ),
    })
}

/// 账上的时刻按 RFC 3339 回：模型引用它时要照抄，而抄一个半句话的调试格式会在
/// 下一次引用时对不上。
fn stamp(at: Option<chrono::DateTime<chrono::Utc>>) -> Option<String> {
    at.map(|t| t.to_rfc3339())
}

fn limit_of(args: &serde_json::Value, ceiling: usize, default: usize) -> usize {
    args.get("limit")
        .and_then(|v| v.as_u64())
        .map(|n| (n as usize).clamp(1, ceiling))
        .unwrap_or(default)
}

/// 一个词形的参数：去空白、转小写，空串当没给。
fn word_arg(args: &serde_json::Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
}

fn make_tool(
    name: &str,
    description: &str,
    parameters: serde_json::Value,
    handler: ToolHandler,
) -> Tool {
    Tool {
        name: name.to_string(),
        description: description.to_string(),
        parameters,
        implementation: ToolImplementation::Native(handler),
    }
}

/// 一条变更压成模型读得动的一小段。
///
/// 「落地了没有」的答案由四列一起给：`state` 说最后一次看树看到了什么，
/// `delivered_rev` 说这条改动进过哪个提交，`landed_at` 说哪一次量它满足了，
/// `measured_rev` 说那次量读的是哪个树。缺了后两列，「landed」会被读成「刚才量的」，
/// 而它可能是一个月前量的。
fn change_summary(row: &cog_core::ChangeLedgerRow) -> serde_json::Value {
    serde_json::json!({
        "identity": row.identity.as_key(),
        "kind": row.identity.kind,
        "scope": row.identity.scope,
        "literal": row.identity.literal,
        "state": row.state.as_str(),
        "priority": row.priority().as_str(),
        "block": row.block().as_str(),
        "title": row.title,
        "assertion": row.assertion,
        "measured_at": stamp(row.measured_at),
        "measured_rev": row.measured_rev,
        "delivered_rev": row.delivered_rev,
        "landed_at": stamp(row.landed_at),
        "unsettled_reason": row.unsettled_reason,
        "holds_the_line": row.holds_the_line(),
        "sources": row
            .sources
            .iter()
            .map(|source| {
                serde_json::json!({
                    "doc": source.doc,
                    "verdict": source.verdict,
                    "priority": source.priority.as_str(),
                    "block": source.block.as_str(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn event_summary(event: &cog_core::ChangeLedgerEvent) -> serde_json::Value {
    serde_json::json!({
        "observed_at": event.observed_at.to_rfc3339(),
        "from": event.from_state.map(|state| state.as_str()),
        "to": event.to_state.as_str(),
        "measured_rev": event.measured_rev,
        "retire_reason": event.retire_reason.map(|reason| reason.as_str()),
        "unsettled_reason": event.unsettled_reason,
    })
}

/// 这一行有没有在说这几个字：身份、标题、正文三处都算。
fn change_says(row: &cog_core::ChangeLedgerRow, needle: &str) -> bool {
    let haystack = format!(
        "{} {} {}",
        row.identity.as_key(),
        row.title.as_deref().unwrap_or_default(),
        row.assertion.as_deref().unwrap_or_default()
    );
    haystack.to_lowercase().contains(needle)
}

fn list_changes_tool(changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>) -> Tool {
    make_tool(
        "list_changes",
        "读变更账：哪些需求还没落地、各自停在哪一步、上一次量到的是什么。\
         回答「这件事做完了没有、卡在哪」之前先读它，按 `match` 找到那几条再看 state、\
         delivered_rev、landed_at。这是只读：它改不了账，也不会把哪一条标成做完——\
         做完只有看树的那一侧量得出来。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "match": {
                    "type": "string",
                    "description": "只看身份、标题或正文里含这几个字的行"
                },
                "state": {
                    "type": "string",
                    "description": "只看这一种状态：conflict / pending / partial / undecided——这四种是还没落地的"
                },
                "limit": {
                    "type": "integer",
                    "description": "最多回几行，默认 20"
                }
            }
        }),
        native(move |args| {
            let changes = changes.clone();
            async move {
                let Some(store) = changes else {
                    return Ok(ledger_absent("变更账"));
                };
                let limit = limit_of(&args, LIST_CHANGES_WINDOW, LIST_CHANGES_DEFAULT);
                let wanted = word_arg(&args, "state");
                let needle = word_arg(&args, "match");

                let census = store.census().await?;
                let mut rows = store.outstanding(LIST_CHANGES_WINDOW).await?;
                if let Some(wanted) = &wanted {
                    rows.retain(|row| row.state.as_str() == wanted);
                }
                if let Some(needle) = &needle {
                    rows.retain(|row| change_says(row, needle));
                }
                rows.truncate(limit);

                // 欠账总额从普查来，不从这一页来：这一页是筛过的、截过的，
                // 拿它当总数会把「还有多少没做完」读成一句谎话。
                let owed: u64 = census
                    .iter()
                    .filter(|(state, _)| state.is_outstanding())
                    .map(|(_, rows)| rows)
                    .sum();
                let terminal_asked = wanted
                    .as_deref()
                    .and_then(|word| word.parse::<cog_core::ChangeState>().ok())
                    .is_some_and(|state| !state.is_outstanding());

                Ok(serde_json::json!({
                    "available": true,
                    "owed_total": owed,
                    "census": census
                        .iter()
                        .map(|(state, rows)| serde_json::json!({
                            "state": state.as_str(),
                            "rows": rows,
                        }))
                        .collect::<Vec<_>>(),
                    "returned": rows.len(),
                    "changes": rows.iter().map(change_summary).collect::<Vec<_>>(),
                    "note": terminal_asked.then_some(
                        "landed 与 retired 是两种了结的结局，不进未了结的那本账，所以这里只\
                         回得出它们的条数（在上面的 census 里）。问某一条就用 get_change。"
                    ),
                }))
            }
        }),
    )
}

fn get_change_tool(changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>) -> Tool {
    make_tool(
        "get_change",
        "读变更账里的一条：它现在在哪一步、什么时候量过、是不是某次提交交付的，\
         以及它是怎么一步步走到这里的（history）。身份是三段，照抄 list_changes 回给你的\
         那三段——不要自己把 `kind:scope:literal` 这串拆开，正文里常带冒号，拆错了问的\
         就是另一条变更。这是只读：它不会把这一条标成做完。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "kind": { "type": "string", "description": "检查的种类，如 grep / absent / exists" },
                "scope": { "type": "string", "description": "检查看哪里：一个路径或一个目录" },
                "literal": { "type": "string", "description": "逐字匹配的那段正文" }
            },
            "required": ["kind", "scope", "literal"]
        }),
        native(move |args| {
            let changes = changes.clone();
            async move {
                let Some(store) = changes else {
                    return Ok(ledger_absent("变更账"));
                };
                let identity = cog_core::ChangeIdentity::new(
                    require_str(&args, "kind")?,
                    require_str(&args, "scope")?,
                    require_str(&args, "literal")?,
                );
                let Some(row) = store.get(&identity).await? else {
                    return Ok(serde_json::json!({
                        "available": true,
                        "found": false,
                        "note": "账上没有这条身份。三段里任何一段差一个字都是一条不同的变更；\
                                 用 list_changes 按字面找一遍。",
                    }));
                };
                let events = store.events(&identity, CHANGE_HISTORY_LIMIT).await?;
                Ok(serde_json::json!({
                    "available": true,
                    "found": true,
                    "change": change_summary(&row),
                    "history": events.iter().map(event_summary).collect::<Vec<_>>(),
                }))
            }
        }),
    )
}

fn principle_summary(row: &cog_core::PrincipleRow) -> serde_json::Value {
    serde_json::json!({
        "slug": row.slug,
        "lead": row.lead,
        "carrier_path": row.carrier_path,
        "host_gate": row.host_gate,
        "where_red": row.where_red,
        "platform_gate": row.platform_gate,
        "check_kind": row.check_kind.map(|kind| kind.as_str()),
        "state": row.state.map(|state| state.as_str()),
        "priority": row.priority.as_str(),
        "refusals": row.refusals,
        "measured_at": stamp(row.measured_at),
        "measured_rev": row.measured_rev,
    })
}

fn list_principles_tool(principles: Option<Arc<dyn cog_core::PrincipleLedgerStore>>) -> Tool {
    make_tool(
        "list_principles",
        "读原则账。给了 `paths` 就回答「我要改这几个文件，哪几条原则管着它、在哪会红」——\
         动手前先读它；不给 `paths` 就回答「哪些原则还没有门」。每一行带触发条件与判据：\
         `where_red` 是它在哪会红，`check_kind` 是它按哪种方式判（shape 是动手之前判、\
         tree 是事后走一遍树、live 是读运行中的系统、judgement 归人）。这是只读。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "paths": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "要改的路径；给了就只回门立在这些路径上的原则"
                },
                "limit": {
                    "type": "integer",
                    "description": "最多回几行，默认 20"
                }
            }
        }),
        native(move |args| {
            let principles = principles.clone();
            async move {
                let Some(store) = principles else {
                    return Ok(ledger_absent("原则账"));
                };
                let limit = limit_of(&args, LIST_PRINCIPLES_WINDOW, LIST_PRINCIPLES_DEFAULT);
                let paths: Vec<String> = args
                    .get("paths")
                    .and_then(|v| v.as_array())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_str())
                            .map(str::trim)
                            .filter(|path| !path.is_empty())
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();

                let census = store.census().await?;
                let kinds = store.check_kind_census().await?;
                let (mode, mut rows) = if paths.is_empty() {
                    (
                        "outstanding",
                        store.outstanding(LIST_PRINCIPLES_WINDOW).await?,
                    )
                } else {
                    (
                        "gating",
                        store.gating_paths(&paths, LIST_PRINCIPLES_WINDOW).await?,
                    )
                };
                rows.truncate(limit);

                Ok(serde_json::json!({
                    "available": true,
                    "mode": mode,
                    "paths": paths,
                    "census": {
                        "enforced": census.enforced,
                        "unenforced": census.unenforced,
                        "unmeasured": census.unmeasured,
                        "retired": census.retired,
                        "rows": census.rows(),
                        "coverage": census.coverage(),
                    },
                    "check_kind_census": {
                        "shape": kinds.shape,
                        "tree": kinds.tree,
                        "live": kinds.live,
                        "judgement": kinds.judgement,
                        "undeclared": kinds.undeclared,
                    },
                    "returned": rows.len(),
                    "principles": rows.iter().map(principle_summary).collect::<Vec<_>>(),
                }))
            }
        }),
    )
}

fn active_alerts_tool(alerts: Option<Arc<dyn cog_core::AlertStore>>) -> Tool {
    make_tool(
        "active_alerts",
        "读现在亮着的告警，新的在前。回答「系统现在有没有事」之前先读它。\
         这是只读：它不消警、不确认，也不替谁判断这条告警该不该处理。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "最多回几条，默认 20"
                }
            }
        }),
        native(move |args| {
            let alerts = alerts.clone();
            async move {
                let Some(store) = alerts else {
                    return Ok(ledger_absent("告警账"));
                };
                let limit = limit_of(&args, ACTIVE_ALERTS_LIMIT, ACTIVE_ALERTS_LIMIT);
                let rows = store.list_active(limit);
                Ok(serde_json::json!({
                    "available": true,
                    "returned": rows.len(),
                    "alerts": rows
                        .iter()
                        .map(|alert| serde_json::json!({
                            "id": alert.id,
                            "severity": alert.severity.as_str(),
                            "event_type": alert.event_type,
                            "message": alert.message,
                            "agent_id": alert.agent_id,
                            "task_id": alert.task_id,
                            "timestamp": alert.timestamp.to_rfc3339(),
                        }))
                        .collect::<Vec<_>>(),
                }))
            }
        }),
    )
}

/// 把一次工具调用包成内核认的 handler：一个值进、一个值出。
fn native<F, Fut>(f: F) -> ToolHandler
where
    F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = SFResult<serde_json::Value>> + Send + 'static,
{
    Arc::new(move |args| Box::pin(f(args)))
}

fn require_str<'a>(args: &'a serde_json::Value, key: &str) -> SFResult<&'a str> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SFError::Validation(format!("{key} 不能为空")))
}

/// 派活之前的那道准入，与 HTTP 提交面同一条：同一个 5 级配额管理器、同一份
/// 估值口径（[`cog_gateway::tasks::estimated_goal_tokens`]）。
///
/// 拦住时回一句给人看的话，不回错误：拒绝是这一轮的结果，模型该把它说出来，
/// 而不是把同一次派发重试一遍。没有配额管理器时放行——与提交面一致，「没人配过
/// 上限」不等于「谁都不许提交」。
async fn admission_refusal(
    admission: Option<&Arc<dyn HierarchyManager>>,
    owner: &str,
    goal: &str,
) -> Option<QuotaRefusal> {
    let hierarchy = admission?;
    let context = QuotaContext {
        // 没有身份的连接不编一个人：编出来的名字会把这次提交记到别人头上。
        user_id: (!owner.is_empty()).then(|| owner.to_string()),
        workspace_id: None,
        team_id: None,
        organization_id: None,
        global_id: None,
    };
    let estimated = cog_gateway::tasks::estimated_goal_tokens(goal, 0);
    let decision = hierarchy.check(&context, estimated).await;
    if decision.allowed {
        return None;
    }
    Some(QuotaRefusal {
        estimated,
        blocked_by: decision.blocked_by.len(),
    })
}

/// 一次被准入挡下的派活。它带着读数，因为写入账面的那条要落同样的两个数。
struct QuotaRefusal {
    estimated: u64,
    blocked_by: usize,
}

impl QuotaRefusal {
    fn sentence(&self) -> String {
        format!(
            "今天的额度不够这一次提交：估计要 {} token，被 {} 层上限挡住。\
             等额度恢复再提交，或请管理员调整上限。",
            self.estimated, self.blocked_by
        )
    }
}

/// 被挡下的这一次写进审计链：与提交面同一个 kind、同一个动作名，目标号那一栏
/// 是 `-`——这一次根本没有目标，编一个 id 会让账上的拒绝看起来像一次派发。
async fn record_refusal(
    audit: Option<&Arc<dyn AuditStream>>,
    actor: &str,
    owner: &str,
    refusal: &QuotaRefusal,
) {
    let Some(stream) = audit else { return };
    let _ = stream
        .append(
            AuditKind::QuotaEnforcement,
            actor,
            "-",
            "quota.dispatch_rejected",
            serde_json::json!({
                "surface": "conversation",
                // 人和员工是两个名字：这一次是谁（员工）替谁（owner）派的。
                // 只记 owner 时，同一个人开两个会话的两笔拒绝在账上分不开。
                "owner": owner,
                "estimated_tokens": refusal.estimated,
                "blocked_by": refusal.blocked_by,
            }),
        )
        .await;
}

fn dispatch_handler(
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
    actor: String,
) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        let admission = admission.clone();
        let audit = audit.clone();
        let owner = owner.clone();
        let actor = actor.clone();
        async move {
            let goal = require_str(&args, "goal")?.to_string();
            if let Some(refusal) = admission_refusal(admission.as_ref(), &owner, &goal).await {
                // 与提交面同一格：拒绝写 `QuotaEnforcement`，目标号那一栏是
                // `-`——这一次根本没有目标，编一个 id 会让账上的拒绝看起来像
                // 一次派发。
                record_refusal(audit.as_ref(), &actor, &owner, &refusal).await;
                return Ok(serde_json::json!({
                    "status": "refused",
                    "reason": refusal.sentence(),
                    "note": "这一次没有派出去，也没有目标号。把原因说给用户听，不要重试同一次提交"
                }));
            }
            let goal_id = uuid::Uuid::new_v4().to_string();
            let row = cog_core::Task::goal_row(goal_id.clone(), &goal);
            // 分解是异步的（规划要几十秒），所以这一条不在工具调用里等：把目标号交
            // 回去，分解在后台跑，进展由台账回答。判退也是 Ok——成败读目标行本身。
            let id_for_submit = goal_id.clone();
            let submitter = orchestrator.clone();
            tokio::spawn(async move {
                if let Err(e) = submitter.submit_goal_auto(&goal, vec![row]).await {
                    tracing::warn!(goal = %id_for_submit, error = %e, "目标没能交给编排器");
                }
            });
            Ok(serde_json::json!({
                "goal_id": goal_id,
                "status": "submitted",
                "note": "已交给编排器分解。要查进展用 list_tasks，goal_id 就用这个值；\
                         这个 id 是唯一权威，回复里引用时照抄它"
            }))
        }
    })
}

fn list_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let status = args
                .get("status")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_ascii_lowercase());
            let goal = args
                .get("goal_id")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| (n as usize).clamp(1, LIST_TASKS_LIMIT))
                .unwrap_or(LIST_TASKS_DEFAULT);

            let mut rows = orchestrator.get_all_tasks().await;
            if let Some(status) = status {
                rows.retain(|task| task.status.as_str() == status);
            }
            if let Some(goal) = goal {
                rows.retain(|task| task.goal_id.as_deref() == Some(goal.as_str()));
            }
            // 新的在前：问「进展」时关心的是正在动的那几行。
            rows.sort_by_key(|task| std::cmp::Reverse(task.updated_at));
            let total = rows.len();
            rows.truncate(limit);

            Ok(serde_json::json!({
                "total_matching": total,
                "returned": rows.len(),
                "tasks": rows.iter().map(task_summary).collect::<Vec<_>>(),
            }))
        }
    })
}

fn get_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let id = require_str(&args, "task_id")?.to_string();
            match orchestrator.get_task(&id).await {
                Some(task) => Ok(serde_json::to_value(&task)?),
                None => Ok(serde_json::json!({
                    "found": false,
                    "note": format!("图里没有 {id} 这条任务")
                })),
            }
        }
    })
}

fn retry_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let id = require_str(&args, "task_id")?.to_string();
            orchestrator
                .retry_task(&id)
                .await
                .map_err(|e| SFError::Agent(format!("{id} 没能重试：{e}")))?;
            Ok(serde_json::json!({ "task_id": id, "status": "retried" }))
        }
    })
}

fn cancel_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let id = require_str(&args, "task_id")?.to_string();
            let reason = require_str(&args, "reason")?.to_string();
            let cascaded = orchestrator
                .cancel_task_with_reason(&id, reason)
                .await
                .map_err(|e| SFError::Agent(format!("{id} 没能取消：{e}")))?;
            Ok(serde_json::json!({
                "task_id": id,
                "status": "cancelled",
                "also_cancelled": cascaded,
            }))
        }
    })
}

/// 台账里一行压成模型读得动的一小段。长字段（`result`、`input`）只在 `get_task`
/// 里给，否则一次列 20 行就把上下文占满了。
fn task_summary(task: &cog_core::Task) -> serde_json::Value {
    serde_json::json!({
        "id": task.id,
        "kind": task.task_type,
        "status": task.status,
        "goal_id": task.goal_id,
        "parent_task_id": task.parent_task_id,
        "agent_id": task.agent_id,
        "retry_count": task.retry_count,
        "updated_at": task.updated_at,
        "error": task.error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 控制面这一组是闭集，而且里面没有执行工具。
    ///
    /// 执行能力本身不在这里长：它随进程的注册表来（`run_command` 之类的在内核
    /// 装好的那一份里）。这一组只负责读台账与派活——在这里再出现一个执行工具，
    /// 界面上就多出一条编排器看不见的活。加一项要在这里说清它是读还是派活。
    #[test]
    fn the_control_plane_group_reads_and_dispatches_but_never_executes() {
        let names: Vec<&str> = CONTROL_PLANE.iter().map(|op| op.name()).collect();
        assert_eq!(
            names,
            vec![
                "dispatch_goal",
                "list_tasks",
                "get_task",
                "retry_task",
                "cancel_task"
            ],
            "工具集是闭集：加一项就要在这里说清楚它是读还是派活"
        );
        for name in &names {
            for banned in ["run", "exec", "shell", "bash", "write", "command", "eval"] {
                assert!(
                    !name.contains(banned),
                    "{name} 看起来是执行工具：这段对话不执行工作，活派给编排器"
                );
            }
        }
        // 每一项都要有说明与参数结构：没有说明的工具模型不会用，没有结构的参数
        // 它只能瞎填。handler 那侧由 `tool` 里的穷尽 match 保证，不在这里重判。
        for op in CONTROL_PLANE {
            assert!(!op.description().trim().is_empty(), "{}", op.name());
            assert_eq!(op.parameters()["type"], "object", "{}", op.name());
        }
    }

    /// 界面上那笔账的 actor 是这位员工自己，且必须在网关声明的闭集里。
    #[test]
    fn the_chat_is_recorded_under_its_own_name() {
        let employee = employee_for("session-abc");
        let actor = format!("agent:{}", employee.id);
        assert!(cog_gateway::actor_labels::declared(&actor), "{actor}");
    }

    /// 会话是生命周期：同一段对话跨轮、跨重启都是同一个员工。
    ///
    /// 换成每轮一个新 id（`web-chat-<uuid>` 那种），账上读到的就是每轮换一个从没
    /// 见过的员工，按 id 记的归属跟着换人，而屏幕上是同一段对话。
    #[test]
    fn a_session_is_one_employee_across_calls_and_restarts() {
        let first = employee_named(Some("inst-7f3a"), "session-abc");
        let second = employee_named(Some("inst-7f3a"), "session-abc");
        assert_eq!(first.id, second.id);
        assert_eq!(first.handle, second.handle);
        assert!(first.stable, "有指纹时名字跨重启稳定");
        assert!(!first.persona.is_empty());
    }

    /// 换一段会话就是换一个员工：账上要能分开两个人。
    #[test]
    fn two_sessions_are_two_employees() {
        let a = employee_named(Some("inst-7f3a"), "session-a");
        let b = employee_named(Some("inst-7f3a"), "session-b");
        assert_ne!(a.id, b.id);
    }

    /// 没有指纹时名字仍然唯一，但它只稳定到下一次滚动——这一位必须如实是 false。
    #[test]
    fn an_employee_without_a_fingerprint_says_it_is_not_stable() {
        let name = employee_named(None, "session-a");
        assert!(!name.stable, "没有指纹却说稳定：滚一次 Pod 就换人");
        assert!(!name.id.is_empty());
        assert!(employee_named(Some("inst-7f3a"), "session-a").stable);
    }

    /// id 直接当 Redis 的 key 与消费者组名用：只含 `[a-z0-9-]`，不为空，也不叫 unknown。
    ///
    /// 名字里带上会话本身（中文、空格、斜杠、超长）时，这些字不能漏进 id——
    /// 漏进去的那一段会让收件箱的 key 建不出来，消息读成「投出去了」而没人收到。
    #[test]
    fn an_employee_id_is_a_key_nobody_has_to_guess() {
        let long = "x".repeat(4096);
        let sessions: [&str; 4] = ["", "会话", "a b/c", &long];
        for seed in [Some("inst-7f3a"), None] {
            for session in sessions {
                let name = employee_named(seed, session);
                assert!(!name.id.is_empty(), "员工没有 id：名册上只能记成 unknown");
                assert!(!name.id.contains("unknown"), "{}", name.id);
                assert!(
                    name.id
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                    "{} 不是能直接当 stream key 的 id",
                    name.id
                );
                assert!(!name.persona.is_empty(), "员工没有名字");
            }
        }
    }

    /// 三本账装成四只只读工具，每一只都说得出它读的是哪一本。
    ///
    /// 「只读」是这一层的边界：判一条需求做完了没有只能由看树的那一侧写，读账的
    /// 人也去写就成了自己给自己发完成证明。加一只工具要在这里说清它读什么，而名字
    /// 里出现派活、撤回、重跑这类动作的，就不是读账。
    #[test]
    fn the_ledger_tools_read_the_three_accounts_and_change_nothing() {
        let tools = ledger_tools(&Ledgers::default());
        let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "list_changes",
                "get_change",
                "list_principles",
                "active_alerts"
            ],
            "账工具是闭集：加一只就要在这里说清它读哪本账"
        );
        for tool in &tools {
            assert!(!tool.description.trim().is_empty(), "{}", tool.name);
            assert_eq!(tool.parameters["type"], "object", "{}", tool.name);
            assert!(
                tool.description.contains("只读"),
                "{} 没有说自己是只读的：模型会以为它能替谁把账改了",
                tool.name
            );
            for banned in ["dispatch", "cancel", "retry", "write", "run", "exec"] {
                assert!(!tool.name.contains(banned), "{} 不是读账", tool.name);
            }
        }
    }

    /// 账缺席时说的必须是「读不到」，不能是「没有」。
    ///
    /// 回一个空列表，读者会把「这个进程没接到账」读成「账上一条都不欠」——两个
    /// 相反的读数，而正要追这件事的人看到的正好是后者，于是这条活谁也不会再去问。
    #[tokio::test]
    async fn an_absent_ledger_says_it_cannot_read_rather_than_that_nothing_is_owed() {
        for tool in ledger_tools(&Ledgers::default()) {
            let ToolImplementation::Native(handler) = tool.implementation else {
                panic!("{} 不是原生工具", tool.name);
            };
            let answer = handler(serde_json::json!({}))
                .await
                .unwrap_or_else(|e| panic!("{} 连一句「读不到」都没给出来：{e}", tool.name));
            assert_eq!(answer["available"], false, "{}", tool.name);
            assert!(
                answer["note"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("读不到"),
                "{} 把缺席说成了别的什么：{}",
                tool.name,
                answer["note"]
            );
            for list in ["changes", "principles", "alerts"] {
                assert!(
                    answer.get(list).is_none(),
                    "{} 在账缺席时回了一份 {list}：空的清单读起来就是「一条都不欠」",
                    tool.name
                );
            }
        }
    }

    /// 一条变更的那几列要一起给。
    ///
    /// 只给 state 是不够的：「landed」只说某一次量看到了，不说量的是哪个树、是不
    /// 是某个提交交付的。缺了后两列，一个月前量的 landed 会被读成刚才量的。
    #[test]
    fn a_change_row_carries_which_tree_it_measured_and_which_commit_delivered_it() {
        let identity = cog_core::ChangeIdentity::new("grep", "crates/x/src", "a:b");
        let row = cog_core::ChangeLedgerRow {
            identity: identity.clone(),
            state: cog_core::ChangeState::Pending,
            title: Some("把 a:b 写进去".into()),
            assertion: Some("这一行必须出现".into()),
            sources: vec![cog_core::ChangeSource {
                doc: "internal-docs/2026-11-04_x.md".into(),
                verdict: Some("pending".into()),
                priority: cog_core::ChangePriority::P1,
                block: cog_core::ChangeBlock::NONE,
            }],
            first_seen_at: chrono::Utc::now(),
            measured_at: Some(chrono::Utc::now()),
            measured_rev: Some("def456".into()),
            delivered_rev: Some("abc123".into()),
            landed_at: None,
            unsettled_reason: None,
            retire_reason: None,
            retired_at: None,
        };
        let summary = change_summary(&row);
        // 三段分开给：正文里带冒号时，`kind:scope:literal` 这串拆不回来，而
        // `get_change` 要的正是三段。
        assert_eq!(summary["kind"], "grep");
        assert_eq!(summary["scope"], "crates/x/src");
        assert_eq!(summary["literal"], "a:b");
        assert_eq!(summary["state"], "pending");
        assert_eq!(summary["priority"], "p1");
        assert_eq!(summary["measured_rev"], "def456");
        assert_eq!(summary["delivered_rev"], "abc123");
        assert_eq!(
            summary["holds_the_line"], true,
            "交付了却还欠着＝这条改动正压在线上，必须读得出来"
        );
        assert_eq!(
            summary["sources"][0]["doc"],
            "internal-docs/2026-11-04_x.md"
        );
    }

    /// 同事发来的话怎么进这一轮的历史。
    ///
    /// 一条 prompt 就是一句话；continue 与 steering 是同一件事的两种口吻，都
    /// 照原文进。撤停与重置不是话，是给正在跑的那一轮的指令——把「abort」当成
    /// 一句用户消息读进去，模型会回答它，而发信人等的是「它停了」。
    #[test]
    fn an_inbox_command_becomes_a_sentence_or_nothing() {
        use cog_core::InboxMessage;
        assert_eq!(
            inbox_as_message(&InboxMessage::Prompt {
                goal: serde_json::json!("把首页配色改一下"),
                reply_stream: None,
            })
            .as_deref(),
            Some("把首页配色改一下")
        );
        assert_eq!(
            inbox_as_message(&InboxMessage::Prompt {
                goal: serde_json::json!({"step": 2}),
                reply_stream: None,
            })
            .as_deref(),
            Some("{\"step\":2}")
        );
        assert_eq!(
            inbox_as_message(&InboxMessage::Continue {
                instruction: "接着做".into()
            })
            .as_deref(),
            Some("接着做")
        );
        assert_eq!(
            inbox_as_message(&InboxMessage::Steering {
                instruction: "换个方向".into()
            })
            .as_deref(),
            Some("换个方向")
        );
        assert_eq!(inbox_as_message(&InboxMessage::Abort), None);
        assert_eq!(inbox_as_message(&InboxMessage::Reset), None);
    }

    /// 同事发来的消息，开跑前被读出来；读完就确认，不会在下一轮再读一遍。
    #[tokio::test]
    async fn what_arrived_while_it_was_away_is_read_before_the_run() {
        let backend: Arc<dyn cog_core::MessageBackend> =
            Arc::new(cog_test_support::MemoryMessageBackend::new());
        let agent_id = "luna-test-0001";
        let msg = cog_core::InboxMessage::Prompt {
            goal: serde_json::json!("帮我看一下昨天那次发布为什么红了"),
            reply_stream: None,
        };
        backend
            .publish(
                &cog_core::agent_inbox_stream(agent_id),
                &serde_json::to_vec(&msg).unwrap(),
            )
            .await
            .unwrap();

        let drained = drain_inbox(&backend, agent_id).await;
        assert_eq!(drained.len(), 1, "收件箱里有一条，读出来了一条");
        assert_eq!(
            inbox_as_message(&drained[0]).as_deref(),
            Some("帮我看一下昨天那次发布为什么红了")
        );
    }

    /// 一个只剩硬上限的管理器：每一级都已经用完。
    struct Exhausted;

    #[async_trait::async_trait]
    impl HierarchyManager for Exhausted {
        async fn check(&self, _ctx: &QuotaContext, _tokens: u64) -> cog_core::HierarchyDecision {
            let blocked = cog_core::ScopeStatus {
                scope: cog_core::QuotaScope::User,
                target_id: "someone".into(),
                remaining: 0,
                used_today: 1_000_000,
                limits: cog_core::QuotaLimits::new(1_000_000, 1_000_000),
                // 这一格是声明的：只有声明过的上限才拦人。
                enforced: true,
                blocking: true,
                warning: false,
            };
            cog_core::HierarchyDecision {
                allowed: false,
                warnings: Vec::new(),
                blocked_by: vec![blocked.clone()],
                scopes: vec![blocked],
            }
        }

        async fn set_used(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
            _tokens: u64,
        ) -> SFResult<()> {
            Ok(())
        }

        async fn history(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
            _days: u32,
        ) -> SFResult<Vec<cog_core::UsageHistoryEntry>> {
            Ok(Vec::new())
        }

        async fn limits(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
        ) -> SFResult<Option<cog_core::QuotaLimits>> {
            Ok(None)
        }

        async fn set_limits(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
            _limits: cog_core::QuotaLimits,
        ) -> SFResult<()> {
            Ok(())
        }

        async fn clear_limits(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
        ) -> SFResult<()> {
            Ok(())
        }
    }

    /// 额度用尽时，界面上的一句话也派不出去——与 HTTP 提交面同一个答案。
    ///
    /// 不加这一道，同一个人今天在 HTTP 上已经提交不了，在聊天框里却还能派；
    /// 而两条路要的是同一份钱，账上看得出来，准入上却看不见。
    #[tokio::test]
    async fn a_dispatch_over_the_day_s_quota_is_refused_before_it_reaches_the_orchestrator() {
        let admission: Arc<dyn HierarchyManager> = Arc::new(Exhausted);
        let refusal = admission_refusal(Some(&admission), "someone", "把首页的颜色改一下")
            .await
            .expect("额度用尽时必须拒绝");
        let sentence = refusal.sentence();
        assert!(sentence.contains("额度"), "{sentence}");
    }

    /// 一条只记不读的审计流，用来数拒绝有没有落账。
    #[derive(Default)]
    struct RecordingAudit(std::sync::Mutex<Vec<(AuditKind, String, String, String)>>);

    #[async_trait::async_trait]
    impl AuditStream for RecordingAudit {
        async fn append(
            &self,
            kind: AuditKind,
            actor: &str,
            target: &str,
            action: &str,
            _detail: serde_json::Value,
        ) -> SFResult<cog_core::AuditEvent> {
            self.0.lock().unwrap().push((
                kind,
                actor.to_string(),
                target.to_string(),
                action.to_string(),
            ));
            Err(SFError::Internal("只记不读".into()))
        }

        async fn read_all(&self) -> SFResult<Vec<cog_core::AuditEvent>> {
            Ok(Vec::new())
        }
    }

    /// 被挡下的派活要在审计链上留一条：提交面拒单写的那一格，界面同样写。
    ///
    /// 不写的话，同一条规则在两个入口上一个留痕一个不留——账上只看得见「今天
    /// 有人没派活」，看不出是被额度挡下的。
    #[tokio::test]
    async fn a_refused_dispatch_leaves_the_same_audit_row_the_submit_face_writes() {
        let audit = Arc::new(RecordingAudit::default());
        let refusal = QuotaRefusal {
            estimated: 128,
            blocked_by: 1,
        };
        let stream: Arc<dyn AuditStream> = audit.clone();
        record_refusal(Some(&stream), "luna-1822ab0e", "someone", &refusal).await;

        let rows = audit.0.lock().unwrap();
        assert_eq!(rows.len(), 1, "拒绝没有落账");
        let (kind, actor, target, action) = &rows[0];
        assert_eq!(*kind, AuditKind::QuotaEnforcement);
        // 落账的是那个员工，不是「谁在用这个界面」：同一个人开两个会话，
        // 两笔拒绝是两个人做的。
        assert_eq!(actor, "luna-1822ab0e");
        assert_eq!(target, "-", "这一次没有目标，账上不该编一个 id");
        assert_eq!(action, "quota.dispatch_rejected");
    }

    /// 没人配过上限时不拦：与提交面一致，「没人管」不等于「谁都不许提交」。
    #[tokio::test]
    async fn a_dispatch_without_a_quota_manager_is_not_refused() {
        assert!(admission_refusal(None, "someone", "把首页的颜色改一下")
            .await
            .is_none());
    }

    /// 只用到 hash 的键值面：这条路径问别的，就落到 `unreachable!` 上。
    #[derive(Default)]
    struct FakePeople {
        fields: std::sync::Mutex<std::collections::HashMap<(String, String), String>>,
    }

    impl FakePeople {
        fn stored(&self, field: &str) -> String {
            self.fields
                .lock()
                .unwrap()
                .get(&(
                    cog_gateway::agents::PEOPLE_KEY.to_string(),
                    field.to_string(),
                ))
                .cloned()
                .expect("这一位没有写进名册")
        }
    }

    #[async_trait::async_trait]
    impl cog_core::KeyValueStore for FakePeople {
        async fn get(&self, _k: &str) -> SFResult<Option<String>> {
            unreachable!("名册是一张 hash")
        }
        async fn set(&self, _k: &str, _v: &str) -> SFResult<()> {
            unreachable!("名册是一张 hash")
        }
        async fn set_ex(&self, _k: &str, _v: &str, _t: u64) -> SFResult<()> {
            unreachable!("名册不过期")
        }
        async fn del(&self, _k: &str) -> SFResult<()> {
            unreachable!("这条路径不删名册")
        }
        async fn expire(&self, _k: &str, _t: i64) -> SFResult<()> {
            unreachable!("名册不过期")
        }
        async fn ttl(&self, _k: &str) -> SFResult<i64> {
            unreachable!("名册不过期")
        }
        async fn incr(&self, _k: &str, _b: i64) -> SFResult<i64> {
            unreachable!("名册不计数")
        }
        async fn decr(&self, _k: &str, _b: i64) -> SFResult<i64> {
            unreachable!("名册不计数")
        }
        async fn incr_ex(&self, _k: &str, _b: i64, _t: u64) -> SFResult<i64> {
            unreachable!("名册不计数")
        }
        async fn sadd(&self, _k: &str, _m: &str) -> SFResult<()> {
            unreachable!("名册是一张 hash")
        }
        async fn srem(&self, _k: &str, _m: &str) -> SFResult<()> {
            unreachable!("名册是一张 hash")
        }
        async fn smembers(&self, _k: &str) -> SFResult<Vec<String>> {
            unreachable!("名册是一张 hash")
        }
        async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()> {
            self.fields
                .lock()
                .unwrap()
                .insert((key.to_string(), field.to_string()), value.to_string());
            Ok(())
        }
        async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>> {
            Ok(self
                .fields
                .lock()
                .unwrap()
                .get(&(key.to_string(), field.to_string()))
                .cloned())
        }
        async fn hdel(&self, _k: &str, _f: &str) -> SFResult<()> {
            unreachable!("这条路径不删名册")
        }
        async fn hvals(&self, _k: &str) -> SFResult<Vec<String>> {
            unreachable!("名册按字段读")
        }
        async fn hgetall(&self, _k: &str) -> SFResult<Vec<(String, String)>> {
            unreachable!("名册按字段读")
        }
        async fn hlen(&self, _k: &str) -> SFResult<u64> {
            unreachable!("名册按字段读")
        }
    }

    /// 说过话的人要留在名册上，而且「第一次见」不能被后面的每一轮改写成现在。
    ///
    /// 每一轮都写 `first_seen = now`，这个名字下的「他来了多久」就永远读成刚刚；
    /// 只写第一次而不推进 `last_seen`，名单上的人就永远排在最后。
    #[tokio::test]
    async fn a_turn_records_the_person_without_moving_his_first_sighting() {
        let kv = FakePeople::default();
        let employee = employee_named(Some("inst-7f3a"), "conv-1");
        let first = chrono::Utc::now() - chrono::TimeDelta::hours(2);

        note_person(&kv, &employee, "conv-1", first).await.unwrap();
        let later = first + chrono::TimeDelta::minutes(5);
        note_person(&kv, &employee, "conv-1", later).await.unwrap();

        let row: cog_gateway::agents::PersonRow =
            serde_json::from_str(&kv.stored(&employee.id)).expect("名册那一行要读得回来");
        assert_eq!(row.first_seen, first);
        assert_eq!(row.last_seen, later);
        assert_eq!(row.session_id, "conv-1");
        assert_eq!(row.display_name, employee.handle);
    }
}
