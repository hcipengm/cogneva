//! Web UI chat turn — bridges the WebSocket `chat_message` client message to
//! the LLM and streams the reply back to the requesting connection as
//! `agent_event` envelopes (message.start / message.text_delta / message.end),
//! matching what the SPA's `useEventStream` store renders.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};

use crate::websocket_protocol::ServerMessage;
use crate::GatewayState;

/// 一次请求里最多带多少条历史（用户与助手合计）。
///
/// 这是「装得进一次请求」的界，不是「存得下多少」的界：记录本身留在库里，按会话
/// 自己的上界裁剪，见存储面。
// cap: bound — 一次会话带多少轮历史进上下文，界的是请求体积
const SESSION_HISTORY_CAP: usize = 40;

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// 装配层在启动时用这同一个句柄填进来的执行器。
///
/// 先发布空句柄、等插件都起来之后再填：这段对话要的编排器由某个插件在 `init`
/// 里发布，而网关拿到这个字段时它还没发布。空着的那段时间界面收到的是「还没接上」，
/// 不是一句凭印象的回答。
pub type ConversationHandle = Arc<ConversationSlot>;

/// 槽本身的类型：发布与取用都按它对齐（`PluginContext` 按具体类型检索，写错一层
/// `Arc` 就取不到，界面于是永远停在「还没接上」而没有任何报错）。
pub type ConversationSlot = std::sync::OnceLock<Arc<dyn ConversationTurn>>;

/// 一段对话的一轮：跑在系统唯一的那条智能体循环上。
///
/// 网关自己不跑循环。它把这一轮要看到的历史交出去，把循环发出来的事件读回来——
/// 工具调用、守卫、预算、token 记账、总线事件都只在循环里发生一次。给界面单独写一
/// 条循环（哪怕它只读台账、只派活），系统里就有第二条执行路径：它做的事不进任务图、
/// 不进账、不经过任何门，而屏幕上看起来与编排器派出去的活一模一样。
///
/// 实现方是装配层：智能体内核与编排器都在它手里，网关两个都摸不到。
pub trait ConversationTurn: Send + Sync {
    /// 跑这一轮，事件写进 `events`。
    ///
    /// 返回的是这一轮的结局：`reply` 里有话就是一个答出来的结尾，预算用尽与上游
    /// 失败各自带自己的状态。
    fn turn<'a>(
        &'a self,
        request: ConversationRequest,
        events: mpsc::Sender<cog_core::AgentEvent>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = cog_core::SFResult<serde_json::Value>> + Send + 'a>,
    >;

    /// 这个人手上还有哪些「总是允许 / 总是拒绝」的规则。
    ///
    /// 默认是一张空名单：装配层没装规则存储时，界面上就该什么都列不出来，而不是
    /// 列一份猜的。列出来的是这个人自己的规则加上这一段对话的规则——他能在哪一段
    /// 对话里被这条规则管住，就该在哪儿看得见它。
    fn permission_rules<'a>(
        &'a self,
        _owner: &'a str,
        _session_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<PermissionRuleView>> + Send + 'a>>
    {
        Box::pin(async { Vec::new() })
    }

    /// 撤掉一条规则，返回撤完之后的名单。
    ///
    /// 返回整份名单而不是一个布尔，是因为界面上唯一要画的就是这份名单：撤掉了哪一条
    /// 由「新的名单里没有它」表达，多一个回执只是在描述一件已经看得见的事。
    fn revoke_permission_rule<'a>(
        &'a self,
        _owner: &'a str,
        _session_id: &'a str,
        _rule_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<PermissionRuleView>> + Send + 'a>>
    {
        Box::pin(async { Vec::new() })
    }
}

/// 一条留下来的规则在界面上长什么样。
///
/// 五格都给人看：哪个工具、碰什么、放还是拦、管这一段还是管这个人、撤的时候按哪个号。
/// 不把规则本身（内核的类型）递出来——网关与界面都没有理由认识内核的枚举。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PermissionRuleView {
    pub id: String,
    pub tool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub allow: bool,
    /// `session` 或 `owner`。
    pub scope: String,
}

/// 一次执行前的询问，从装配层递到界面上。
///
/// 网关不认识「权限模式」这个类型，只认识它的名字：模式是内核的闭集，网关只负责
/// 让这个名字原样出现在问句里，并在答案回来时原样送回。名字不认识的由装配层拒绝，
/// 网关不猜一个默认值。
#[derive(Debug, Clone)]
pub struct PermissionAskView {
    pub request_id: String,
    pub session_id: String,
    pub tool: String,
    /// 人要看的那一行：这个工具要拿什么参数。只报工具名的话，屏幕上会出现一百次
    /// 一模一样的「write_file」。
    pub summary: String,
    pub mode: String,
}

/// 人给回来的答案。拒绝要带理由，理由要交回模型。
///
/// `always` 是另一半答案：人除了回答这一次，还可以要求「这个工具碰这种东西就别再问了」。
/// 两个字段上的 `always` 是同一件事的两面——记住的是结论（放行或拒绝），规则由装配层
/// 写进它那一侧的存储；网关只把人的原话搬过去，不解释「同类」是什么意思。
#[derive(Debug, Clone)]
pub enum PermissionAnswer {
    Allow { always: bool },
    Deny { reason: String, always: bool },
}

impl PermissionAnswer {
    /// 只放行这一次。
    pub fn allow() -> Self {
        Self::Allow { always: false }
    }

    /// 只拒绝这一次。
    pub fn deny(reason: impl Into<String>) -> Self {
        Self::Deny {
            reason: reason.into(),
            always: false,
        }
    }

    /// 人是否要求把这一次的结论记成一条规则。
    pub fn remembered(&self) -> bool {
        match self {
            Self::Allow { always } | Self::Deny { always, .. } => *always,
        }
    }
}

/// 谁把那一问送到人面前。
///
/// 实现在连接那一侧：它知道这条 socket 在哪里，也知道答案从哪一条上行消息回来。
/// 网关的聊天函数只拿着这个 trait——它不执行工具，也就不该知道工具长什么样。
#[async_trait::async_trait]
pub trait PermissionBridge: Send + Sync {
    async fn ask(&self, view: PermissionAskView) -> PermissionAnswer;
}

/// 一条连接上的权限通道：等着人答的问题，和每一段对话用的模式。
///
/// 两样放在一起，是因为它们都只活在这一条连接上：问题按请求号索引（同一段对话里
/// 可以有两问同时在等），模式按会话号索引（同一段对话跨轮次保持同一格）。
pub struct PermissionChannel {
    pending: Mutex<HashMap<String, tokio::sync::oneshot::Sender<PermissionAnswer>>>,
    modes: Mutex<HashMap<String, String>>,
}

impl Default for PermissionChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl PermissionChannel {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            modes: Mutex::new(HashMap::new()),
        }
    }

    /// 这一段对话现在用哪一格模式。没声明过就是 `default`——比「不问」严，
    /// 比「只读」松，是人在场时的常态。
    pub fn mode(&self, session_id: &str) -> String {
        self.modes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_id)
            .cloned()
            .unwrap_or_else(|| "default".to_string())
    }

    /// 换这一段对话的模式。名字原样存下，不认识的名字由装配层在用到时拒绝——
    /// 网关不认识闭集，也就不该在这里替它猜一格。
    pub fn set_mode(&self, session_id: &str, mode: &str) {
        self.modes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id.to_string(), mode.to_string());
    }

    /// 把一条上行答案交给等着的那一问。没人等（重复的答案、已经断开的问）就是
    /// 什么都没有发生——不是错误，也不该造一条回执。
    pub fn answer(&self, request_id: &str, answer: PermissionAnswer) -> bool {
        let sender = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request_id);
        match sender {
            Some(tx) => tx.send(answer).is_ok(),
            None => false,
        }
    }

    /// 这一问的那一头没了：从表上摘掉，不再等答案。
    ///
    /// 不摘的话这张表跟着「连上来又断掉、每次都问过一句」的连接数长，而每一行都是一个
    /// 等不到答案的发送端——它自己不会消失。
    fn forget(&self, request_id: &str) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request_id);
    }

    fn register(&self, request_id: &str) -> tokio::sync::oneshot::Receiver<PermissionAnswer> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(request_id.to_string(), tx);
        rx
    }
}

/// 走这条 socket 的问人通道。
///
/// 问句从这条连接自己的出站管道发出去，与聊天帧走同一条路；答案从另一条上行消息
/// 回来。上行消息在套接字循环里处理，这一问在聊天任务里等——两件事不在同一个任务
/// 上，所以答案表是共享的，按请求号找。
pub struct SocketPermissionBridge {
    session_id: String,
    out: mpsc::Sender<String>,
    channel: Arc<PermissionChannel>,
}

impl SocketPermissionBridge {
    pub fn new(
        session_id: String,
        out: mpsc::Sender<String>,
        channel: Arc<PermissionChannel>,
    ) -> Self {
        Self {
            session_id,
            out,
            channel,
        }
    }
}

#[async_trait::async_trait]
impl PermissionBridge for SocketPermissionBridge {
    async fn ask(&self, view: PermissionAskView) -> PermissionAnswer {
        let rx = self.channel.register(&view.request_id);
        let Ok(frame) = serde_json::to_string(
            &crate::websocket_protocol::ServerMessage::PermissionRequest {
                request_id: view.request_id.clone(),
                session_id: self.session_id.clone(),
                tool: view.tool.clone(),
                summary: view.summary.clone(),
                mode: view.mode.clone(),
            },
        ) else {
            self.channel.forget(&view.request_id);
            return PermissionAnswer::deny("这一问没能发出去，所以没有执行。");
        };
        if self.out.send(frame).await.is_err() {
            self.channel.forget(&view.request_id);
            return PermissionAnswer::deny("这条连接已经关闭，没有人回答，所以没有执行。");
        }
        // 没有人给这一问上时限：人在读它。但「没人会再读到它」必须立刻结束，否则这一轮
        // 会一直挂在「等我点头」上，而读它的那个人已经不在了——答案永远不会回来，`rx`
        // 也就永远不报错（发答案的那一头还活在这条通道的表里）。出站管道那一头没了就是
        // 连接断了，按拒绝收场，一个悬着的问不会变成一次默认放行，也不会变成一次泄漏。
        let answer = tokio::select! {
            got = rx => got.ok(),
            _ = self.out.closed() => None,
        };
        match answer {
            Some(answer) => answer,
            None => {
                self.channel.forget(&view.request_id);
                PermissionAnswer::deny("这条连接在回答之前关闭了，所以没有执行。")
            }
        }
    }
}

/// 交给那一轮的全部输入。历史由网关装配——它读的是这段对话在库里的记录。
pub struct ConversationRequest {
    pub session_id: String,
    /// 这一轮记在谁头上。空串是「这条连接没有身份」，与「声明了某个人」分开记。
    pub owner: String,
    pub messages: Vec<cog_core::Message>,
    pub llm: Arc<dyn cog_core::LlmClient>,
    /// 这条连接握手时自报的平台（界面发 `web`），没有就是没声明。
    ///
    /// 它不是身份：身份是 `owner`。它回答的是名单上另一件事——这一行是谁开出
    /// 来的会话。名字由会话哈希而来，人跟探针在名字上看不出区别，来源在行上。
    pub platform: Option<String>,
    /// 同一台设备的同一个客户端，同样来自握手。
    pub device_id: Option<String>,
    /// 这一轮处在哪种权限模式下，取的是闭集里的名字。不认识的由装配层按更严的
    /// 那一格处理——一次拼错的模式名不该让这一轮问得更少。
    pub permission_mode: String,
    /// 这条连接上「问人」的那条路。没有它时，需要确认的那一步拒绝执行并说明
    /// 为什么——不是一个没人做过的决定被记成做过的。
    pub permissions: Option<Arc<dyn PermissionBridge>>,
}

/// 握手时客户端自报的「我在哪儿」，借用版。
///
/// [`ConversationRequest`] 里那两格是它穿过总线时的样子；这一份只在一跳里活着，
/// 所以借而不拷。两格回答的是同一个问题，所以一起走：分行传只是把参数个数加一。
#[derive(Debug, Clone, Copy, Default)]
pub struct ClientOrigin<'a> {
    pub platform: Option<&'a str>,
    pub device_id: Option<&'a str>,
}

/// 循环往网关送事件的管道有多深。
///
/// 一个模型的回合可以连着吐几百个小增量，而界面那头的写是有背压的（慢客户端会
/// 堵住 WebSocket）。管道浅了生产端就跟着堵，把一次推理拖成一次写网络；深到能装下
/// 一整个回合的增量，生产端就不会因为读者慢而停下。
// cap: bound — 事件管道深度，界的是「读者慢多久会堵住循环」
const EVENT_CHANNEL_CAP: usize = 512;

/// 被叫停之后，最多等多久把已经说出口的那半句读回来。
///
/// 正常情况下这个等待立刻结束：那一轮被丢下时，它手里的事件发送端一起没了，读
/// 回那一头马上看到管道关闭。这个数只兜「有人另留了一份发送端」的底——那时界面
/// 上的停止按钮会卡在这个等待里，而这正是它最不该表现出犹豫的地方。
// cap: bound — 停止之后等读回管道关闭的上限，界的是停止按钮的响应时间
const STOPPED_DRAIN: std::time::Duration = std::time::Duration::from_secs(2);

/// 这一轮从循环里读回来的东西：回复正文，它派出去的目标号，它翻开过的变更。
#[derive(Default)]
struct TurnJournal {
    reply: String,
    goals: Vec<String>,
    changes: Vec<String>,
}

/// 这一条连接上正在跑的轮，按会话号索引。
///
/// 客户端只拿着会话号：`chat_cancel` 寻址的是「这一段对话」，而一轮的身份是服务端
/// 生成的。同一段对话里可以同时有好几轮（用户连发两句，前一句还在答），所以一格里
/// 是若干条停止信号，一次停止把它们全发了。
///
/// 存的是 `watch` 的发送端而不是 `AbortHandle`：停的方式必须是「让那一轮自己放下
/// 手里的 future」，而不是在任务边界上砍一刀——被砍掉的任务不会走它自己的收尾，
/// 名册、记录、界面都会停在一个中间态上。这里只负责把信号递到那一轮手里。
#[derive(Clone, Default)]
pub struct RunningTurns {
    turns: Arc<Mutex<HashMap<String, Vec<RunningTurn>>>>,
}

struct RunningTurn {
    message_id: String,
    stop: watch::Sender<bool>,
}

impl RunningTurns {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记一轮，拿回它的停止信号。
    fn register(&self, session_id: &str, message_id: &str) -> watch::Receiver<bool> {
        let (stop, rx) = watch::channel(false);
        let mut turns = self.turns.lock().unwrap_or_else(|e| e.into_inner());
        turns
            .entry(session_id.to_string())
            .or_default()
            .push(RunningTurn {
                message_id: message_id.to_string(),
                stop,
            });
        rx
    }

    /// 这一轮结束了（答完、失败或被停），把它从名册上摘掉。
    ///
    /// 摘不摘都要能再停一次而不出错，所以这里只按 message_id 精确地删：同一段对话
    /// 里别的那几轮不能被顺手清掉。
    fn finish(&self, session_id: &str, message_id: &str) {
        let mut turns = self.turns.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(list) = turns.get_mut(session_id) {
            list.retain(|turn| turn.message_id != message_id);
            // 空格子不留在表里：一段开过很多轮的会话关掉以后，这里不该留着一串
            // 空 vec 让下一个人以为还有东西在跑。
            if list.is_empty() {
                turns.remove(session_id);
            }
        }
    }

    /// 停掉这一段上正在跑的每一轮，返回停掉了几轮。
    pub fn cancel(&self, session_id: &str) -> usize {
        let list = {
            let mut turns = self.turns.lock().unwrap_or_else(|e| e.into_inner());
            turns.remove(session_id).unwrap_or_default()
        };
        let stopped = list.len();
        for turn in list {
            // 接收方已经走了（那一轮刚好在这之前结束）时 send 会失败，这不是错。
            let _ = turn.stop.send(true);
        }
        stopped
    }
}

/// 这一轮在名册上的那一格，随这个 guard 一起消失。
///
/// 不用「在每条 return 之前调一次 finish」：这条路有五六处出口（没有执行器、没有
/// 模型、正常答完、被叫停），漏一处，那一格就永远留在表里，下一次停止会发给一个
/// 不存在的轮，而真正该停的那一轮收不到。
struct RunningGuard<'a> {
    running: &'a RunningTurns,
    session_id: &'a str,
    message_id: &'a str,
}

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.running.finish(self.session_id, self.message_id);
    }
}

/// 等到有人说停为止。
///
/// 通道关掉而没有人说过停时**永远不返回**：那一轮的任务没了收件人，不等于用户
/// 按了停止。把「信号消失」读成「停止」，会在连接断开、名册被清掉这类与用户
/// 意图无关的时刻，把一轮好好的回答砍掉。
async fn wait_cancelled(mut stop: watch::Receiver<bool>) {
    loop {
        if stop.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        if *stop.borrow_and_update() {
            return;
        }
    }
}

/// 一轮的两种结局：跑完了，或者被叫停。
enum TurnOutcome<T> {
    Finished(T),
    Stopped,
}

/// 跑这一轮，直到它自己结束或者有人叫停。
///
/// 叫停的做法是**放下**这个 future：循环、模型调用、工具调用都在它里面，丢下它，
/// 这一轮就真的不往下走了。只把结果扔掉的写法（让它继续跑完、不采纳结果）在屏幕上
/// 看起来一模一样，而模型那边还在生成、花销还在记——停止按钮不能只是遮住这一轮。
async fn run_until_stopped<F, T>(work: F, stop: watch::Receiver<bool>) -> TurnOutcome<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::select! {
        // 用户按下停止的那一刻优先：两边同时就绪时，留下的那一半应该真的是他想停的。
        biased;
        _ = wait_cancelled(stop) => TurnOutcome::Stopped,
        value = work => TurnOutcome::Finished(value),
    }
}

/// 把循环的事件翻成界面认的帧，顺便把这一轮说过的话记下来。
///
/// 读的是同一条循环真正做过的事：模型说了什么（`MessageUpdate`），派了哪个目标
/// （`dispatch_goal` 的工具结局）。两样都不是网关自己推断出来的——它没有第二条
/// 路可以得到它们。
async fn pump_events(
    mut events: mpsc::Receiver<cog_core::AgentEvent>,
    out: mpsc::Sender<String>,
    // 同一帧也落在这条总线上。界面（员工那一面、活动流）读的是总线上的
    // `agent:<id>`，不是这一条连接：只在连接里发，页面上什么都不会动。
    bus: tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
    session_id: String,
    message_id: String,
) -> TurnJournal {
    use cog_core::{AgentEvent, AssistantMessageEvent};
    let mut journal = TurnJournal::default();
    let mut spoke = false;
    // 结局那一帧只有调用号，没有工具名：名字在开始那一帧上。对照表按调用号把它们
    // 接起来——猜「是不是 dispatch_goal」只能看返回里有没有 goal_id，而别的工具也
    // 可以回一个目标号，那时界面会多出一条它并不负责的目标帧。
    let mut in_flight: HashMap<String, String> = HashMap::new();
    // 这一轮是谁在说，说过没有。名字从事件自己身上取：网关不重算一遍，两份
    // 名字算法迟早会有一个字母不一样，而那时界面按名字认人，认不出来也不报错。
    let mut told: Option<String> = None;
    while let Some(event) = events.recv().await {
        // 没有订阅者时 send 会失败，这不是错：连接先走了，帧没有人要。
        let _ = bus.send(event.clone());
        // 界面靠这一帧把执行者的工具调用归到这段对话下面：工具帧只带 `agent_id`，
        // 没有这一帧，这段对话自己的执行过程一条也接不回来，屏幕上就只剩一问一答。
        let speaker = match &event {
            AgentEvent::MessageUpdate { agent_id, .. }
            | AgentEvent::ToolExecutionStart { agent_id, .. }
            | AgentEvent::ToolExecutionEnd { agent_id, .. } => Some(agent_id.as_str()),
            _ => None,
        };
        if let Some(agent_id) = speaker {
            if told.as_deref() != Some(agent_id) {
                told = Some(agent_id.to_string());
                emit(
                    &out,
                    &session_id,
                    "agent.identity",
                    serde_json::json!({ "message_id": message_id, "agent_id": agent_id }),
                )
                .await;
            }
        }
        match event {
            AgentEvent::MessageUpdate {
                assistant_event, ..
            } => match assistant_event {
                // 每一轮思考都是同一段对话里的新一段话。不隔开，两段会连成一句，
                // 读起来像模型话没说清，而不是像它中间去查了一次台账。
                AssistantMessageEvent::Start { .. } if spoke => {
                    emit_delta(&out, &session_id, &message_id, "\n\n").await;
                    journal.reply.push_str("\n\n");
                }
                AssistantMessageEvent::TextDelta { delta, .. } => {
                    spoke = true;
                    emit_delta(&out, &session_id, &message_id, &delta).await;
                    journal.reply.push_str(&delta);
                }
                // 推理模型在第一个字之前可以想几十秒。把这段也发出去，界面才是在
                // 「它正在想」而不是一个空泡；不发这一族帧，同一段等待在屏幕上与
                // 卡死同形。
                AssistantMessageEvent::ThinkingStart { .. } => {
                    emit_thinking_start(&out, &session_id, &message_id).await;
                }
                AssistantMessageEvent::ThinkingDelta { delta, .. } => {
                    emit_thinking_delta(&out, &session_id, &message_id, &delta).await;
                }
                AssistantMessageEvent::ThinkingEnd { .. } => {
                    emit_thinking_end(&out, &session_id, &message_id).await;
                }
                _ => {}
            },
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                ..
            } => {
                in_flight.insert(tool_call_id, tool_name);
            }
            // 目标身份单独发一帧：界面要拿它去接「这个目标下的执行者在做什么」，
            // 而不是去解析回复正文里的一个 id。值与工具回给模型的同一份。
            //
            // 变更同理：它翻开的是账上哪一条，只有工具返回里那一串三段身份答得准。
            // 不单独发这一帧，界面上「这条变更」就只是回复正文里的几个字，点不进
            // 变更账——而它刚刚读的那一行就在账上，离一次点击那么近。
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                result,
                ..
            } => match in_flight.remove(&tool_call_id).as_deref() {
                Some("dispatch_goal") => {
                    if let Some(goal_id) = result.get("goal_id").and_then(|v| v.as_str()) {
                        if !journal.goals.iter().any(|g| g == goal_id) {
                            journal.goals.push(goal_id.to_string());
                            emit(
                                &out,
                                &session_id,
                                "message.goal",
                                serde_json::json!({ "message_id": message_id, "goal_id": goal_id }),
                            )
                            .await;
                        }
                    }
                }
                Some("get_change") => {
                    let change_id = result
                        .get("change")
                        .and_then(|change| change.get("identity"))
                        .and_then(|v| v.as_str());
                    if let Some(change_id) = change_id {
                        if !journal.changes.iter().any(|c| c == change_id) {
                            journal.changes.push(change_id.to_string());
                            emit(
                                &out,
                                &session_id,
                                "message.change",
                                serde_json::json!({
                                    "message_id": message_id,
                                    "change_id": change_id,
                                }),
                            )
                            .await;
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    journal
}
fn envelope(session_id: &str, event_type: &str, inner: serde_json::Value) -> String {
    let msg = ServerMessage::AgentEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        session_id: Some(session_id.to_string()),
        task_id: None,
        payload: serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": event_type,
            "timestamp": now_iso(),
            "payload": inner,
        }),
        server_time: now_iso(),
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

async fn emit(
    out: &mpsc::Sender<String>,
    session_id: &str,
    event_type: &str,
    inner: serde_json::Value,
) {
    // Receiver only disappears when the socket loop ended — nothing to do then.
    let _ = out.send(envelope(session_id, event_type, inner)).await;
}

async fn emit_message_start(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    role: &str,
) {
    emit(
        out,
        session_id,
        "message.start",
        serde_json::json!({ "message_id": message_id, "role": role }),
    )
    .await;
}

async fn emit_delta(out: &mpsc::Sender<String>, session_id: &str, message_id: &str, delta: &str) {
    emit(
        out,
        session_id,
        "message.text_delta",
        serde_json::json!({ "message_id": message_id, "delta": delta }),
    )
    .await;
}

async fn emit_thinking_start(out: &mpsc::Sender<String>, session_id: &str, message_id: &str) {
    emit(
        out,
        session_id,
        "message.thinking_start",
        serde_json::json!({ "message_id": message_id }),
    )
    .await;
}

async fn emit_thinking_delta(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    delta: &str,
) {
    emit(
        out,
        session_id,
        "message.thinking_delta",
        serde_json::json!({ "message_id": message_id, "delta": delta }),
    )
    .await;
}

async fn emit_thinking_end(out: &mpsc::Sender<String>, session_id: &str, message_id: &str) {
    emit(
        out,
        session_id,
        "message.thinking_end",
        serde_json::json!({ "message_id": message_id }),
    )
    .await;
}

async fn emit_message_end(out: &mpsc::Sender<String>, session_id: &str, message_id: &str) {
    emit(
        out,
        session_id,
        "message.end",
        serde_json::json!({ "message_id": message_id }),
    )
    .await;
}

/// 这一轮花了多少：token、轮次、时长、上下文占用、哪一路模型答的、算了多少钱。
///
/// 数字全部来自循环挂在交付物上的那一格，这里一个也不算——token 与轮次只有
/// 跑循环的那边数得准，网关重算一遍就有了两套账。模型名与费用缺省时发 `null`：
/// 上游没报名字、没声明价目，都是「没读到」，界面上那一格因此空着而不是写零。
///
/// 只在循环真的交了用量时发这一帧。失败与被叫停的那一轮没有交付物，屏幕上就
/// 不出现这一行——发一帧全零的用量，读起来是「这一轮没花钱」，而事实是这一轮
/// 的账没有读回来。
async fn emit_turn_usage(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    usage: Option<&serde_json::Value>,
) {
    let Some(usage) = usage else { return };
    let count = |key: &str| usage.get(key).and_then(serde_json::Value::as_u64);
    emit(
        out,
        session_id,
        "message.usage",
        serde_json::json!({
            "message_id": message_id,
            "input_tokens": count("input_tokens"),
            "output_tokens": count("output_tokens"),
            "total_tokens": count("total_tokens"),
            "rounds": count("rounds"),
            "context_tokens": count("context_tokens"),
            "duration_ms": count("duration_ms"),
            "model": usage.get("model").and_then(serde_json::Value::as_str),
            "cost_usd": usage.get("cost_usd").and_then(serde_json::Value::as_f64),
        }),
    )
    .await;
}

/// 这一轮给模型看的历史：最近 [`SESSION_HISTORY_CAP`] 条，旧的最先。
///
/// 读不到就按「没有历史」回答，不按「历史是空的」记一笔：这两种在读数上要分得开，
/// 而这一趟能做的只有回答——拿一个读不到的库把聊天停掉，是把「不知道」当成「没有」。
async fn load_history(store: &crate::ChatTurnHandle, session_id: &str) -> Vec<cog_core::Message> {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮没有历史上下文"
        );
        return Vec::new();
    };
    match store.history(session_id, SESSION_HISTORY_CAP).await {
        Ok(turns) => turns_to_messages(turns),
        Err(e) => {
            tracing::warn!(
                error = %e,
                session = %session_id,
                "聊天：历史读不出来，这一轮按没有上下文回答"
            );
            Vec::new()
        }
    }
}

/// 记录里的一行变成模型看得懂的一条。角色的闭集在存储面，这里只做搬运。
fn turns_to_messages(turns: Vec<cog_core::ChatTurn>) -> Vec<cog_core::Message> {
    turns
        .into_iter()
        .map(|turn| match turn.role {
            cog_core::ChatTurnRole::User => cog_core::Message::user(turn.content),
            cog_core::ChatTurnRole::Assistant => cog_core::Message::assistant_text(turn.content),
        })
        .collect()
}

/// 把这一轮写进记录。写不进去只留一条 warn：用户已经在等一个回答，拿「记不下来」
/// 把回答吞掉，比丢一行历史更糟。
async fn record_turn(store: &crate::ChatTurnHandle, session_id: &str, user: &str, assistant: &str) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store.record_turn(session_id, user, assistant).await {
        tracing::warn!(
            error = %e,
            session = %session_id,
            "聊天：这一轮没写进记录"
        );
    }
}

/// 记下这一轮，连同它交出去的那道目标。
///
/// 目标身份原本只活在两个瞬间的地方：回复正文里的一句话，和实时流上的一帧。
/// 关掉页面再打开，这段对话在记录里就成了一段纯问答——「这条能不能关掉」问的
/// 正是它的反面，而刷新恰好是这个问题最需要答对的时候。帧是「此刻正在发生」的
/// 视图，记录才是关掉之后仍然成立的那一份。
async fn record_dispatched_turn(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    user: &str,
    assistant: &str,
    goal_id: &str,
) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store
        .record_dispatched_turn(session_id, user, assistant, goal_id)
        .await
    {
        tracing::warn!(
            error = %e,
            session = %session_id,
            "聊天：这一轮没写进记录"
        );
    }
}

/// 记下这一轮没答出来的那句话，以及没答出来的原因。
///
/// 答不出来的那一轮同样是用户对这个平台说过话的一轮：什么都不写，用户说的那句
/// 就只活在他刚收到的那个响应里，下一次提问的上下文里也没有它——一段对话在最
/// 需要留下证据的时候（上游坏了）反而是空的。原因写在同一行上，读者才知道这一
/// 轮为什么有问无答；将来构建提示词时它不会当成模型说过的话被回放。
async fn record_failed_turn(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    user: &str,
    failure: &str,
) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store.record_failed_turn(session_id, user, failure).await {
        tracing::warn!(
            error = %e,
            session = %session_id,
            "聊天：这一轮没写进记录"
        );
    }
}

/// Run one chat turn: echo the user message, hand the turn to the platform's one
/// agent loop, stream what that loop produces back to the connection, and
/// record the turn in the session history.
///
/// 这一屏不执行任何工作。它能不能读台账、能不能派活，取决于装配层给那条循环装了
/// 哪些工具；它自己只有一条把历史交出去、把事件读回来的路。
///
/// 参数多是这一屏本来的形状：一轮对话的六样输入，加上问不问人的两样配置。打包成
/// 结构体只是把同一组参数挪进另一个括号，而按签名读正文的那些普查（回声的正文来自
/// 哪个参数、记录的写入在哪一步）会因此失去抓手。
#[allow(clippy::too_many_arguments)]
pub async fn run_chat_turn(
    state: Arc<GatewayState>,
    out: mpsc::Sender<String>,
    session_id: String,
    content: String,
    // 这次对话属于谁。空串是「这条连接没有身份」——那时调用方不声明人，账落在
    // 没有人的那一格，与「声明了某个人」分开。
    owner: String,
    // 这条连接握手时自报的平台与设备，原样带进这一轮。见 [`ConversationRequest`]。
    // 两格一起走：它们回答同一个问题，分开传只是把参数个数往上加一个。
    origin: ClientOrigin<'_>,
    // 这条连接上正在跑的轮。这一轮登记进去，客户端的停止才找得到它。
    running: RunningTurns,
    // 这一轮处在哪种权限模式下（闭集里的名字），以及问人的那条路。两格一起走：
    // 模式决定问不问，通道决定问得到谁。
    permission_mode: String,
    permissions: Option<Arc<dyn PermissionBridge>>,
) {
    // 1. Echo the user message so the UI renders it (the real transport has
    //    no local echo, unlike the mock client).
    let user_mid = format!("user-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &user_mid, "user").await;
    emit_delta(&out, &session_id, &user_mid, &content).await;
    emit_message_end(&out, &session_id, &user_mid).await;

    let assistant_mid = format!("assistant-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &assistant_mid, "assistant").await;
    // 登记得早，摘得晚：这中间只要漏一条路，停止按钮就会在这段对话上找不到这一
    // 轮，而用户已经按下去了。所以摘不靠某一条 return，靠 guard——它的寿命就是
    // 这一轮在这个函数里的寿命。
    let stop = running.register(&session_id, &assistant_mid);
    let _registered = RunningGuard {
        running: &running,
        session_id: &session_id,
        message_id: &assistant_mid,
    };

    let Some(conversation) = state.conversation.get().cloned() else {
        let reason =
            "这段对话还没有接到系统的那条循环上（装配层没有登记会话执行器），无法处理聊天消息。";
        emit_delta(&out, &session_id, &assistant_mid, reason).await;
        // 没答出来也要落库，且落在收尾帧之前：用户看到这一轮结束就立刻追问时，
        // 这一句若还没进记录，下一轮的上下文里就没有它。
        record_failed_turn(&state.chat_turns, &session_id, &content, reason).await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        return;
    };

    let llm = state
        .llm_client
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();

    let Some(llm) = llm else {
        let reason = "LLM 未配置或未就绪，无法处理聊天消息。请先在右上角完成 LLM 设置。";
        emit_delta(&out, &session_id, &assistant_mid, reason).await;
        record_failed_turn(&state.chat_turns, &session_id, &content, reason).await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        return;
    };

    // 2. Build the transcript: what this session already said, plus this
    //    sentence. 循环读到的是这段记录，不是网关的记忆。员工的指示不在这里：
    //    它是那位员工自己的事，由会话执行器按声明取，网关只搬对话。
    let mut messages = load_history(&state.chat_turns, &session_id).await;
    messages.push(cog_core::Message::user(content.clone()));

    // 3. Hand it over. 事件由另一个任务读：循环要能一直往管道里写，不因为这一头
    //    在等它返回而互相卡住。
    let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAP);
    let mut pump = tokio::spawn(pump_events(
        events_rx,
        out.clone(),
        state.event_tx.clone(),
        session_id.clone(),
        assistant_mid.clone(),
    ));
    let outcome = run_until_stopped(
        conversation.turn(
            ConversationRequest {
                session_id: session_id.clone(),
                owner,
                messages,
                llm,
                platform: origin.platform.map(str::to_string),
                device_id: origin.device_id.map(str::to_string),
                permission_mode,
                permissions,
            },
            events_tx,
        ),
        stop,
    )
    .await;

    // 被叫停时，那一轮的 future 刚被丢下：它手里的事件发送端跟着没了，读回这一头
    // 应当立刻看到管道关闭。等一会儿只是兜底——真有谁克隆了发送端、把管道撑住，
    // 也不能让停止按钮卡在这里。已经流到屏幕上的字不算白说：那一半记在这一轮的
    // 记录里，用户看到的就是结束在这儿。
    let journal = if matches!(&outcome, TurnOutcome::Stopped) {
        match tokio::time::timeout(STOPPED_DRAIN, &mut pump).await {
            Ok(journal) => journal.unwrap_or_default(),
            Err(_) => {
                pump.abort();
                TurnJournal::default()
            }
        }
    } else {
        pump.await.unwrap_or_default()
    };

    // 4a. 被停掉的那一轮：先说「停了」，再说「这一轮结束了」。
    //
    // 叫停的那一刻，这一轮的结果已经被丢下了；一个「丢了还能读回来」的结果只可能
    // 是假的，所以这里不再看它。
    if matches!(&outcome, TurnOutcome::Stopped) {
        let reason = "已中止";
        emit(
            &out,
            &session_id,
            "message.cancelled",
            serde_json::json!({ "message_id": assistant_mid }),
        )
        .await;
        // 记录先落，再发收尾帧：界面上收到上面那一帧会立刻允许追问，而下一轮读
        // 历史就在这之后——不先落库，被停掉的那一句在下一轮的上下文里不存在。
        record_failed_turn(&state.chat_turns, &session_id, &content, reason).await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        return;
    }

    // 4. 收尾：说过的字已经在屏幕上，这里只补这一轮没话可说的那两种情况。
    let outcome = match outcome {
        TurnOutcome::Finished(outcome) => outcome,
        // 上面已经 return。
        TurnOutcome::Stopped => unreachable!("a stopped turn returned above"),
    };
    // 这一轮花了什么：循环把用量挂在这一轮的交付物上（`usage` 那一个键），
    // 这里只搬，不重算——token 与轮次只有循环那边数得准。失败的那一轮没有
    // 交付物，也就没有这一格；那是「没读到」，不是「花了零」。
    let turn_usage = outcome
        .as_ref()
        .ok()
        .and_then(|value| value.get("usage"))
        .cloned();
    let mut reply = journal.reply;
    let mut failure: Option<String> = None;
    match outcome {
        Err(err) => {
            let text = err.to_string();
            emit_delta(
                &out,
                &session_id,
                &assistant_mid,
                &format!("LLM 调用失败：{text}"),
            )
            .await;
            failure = Some(text);
        }
        // 没有流式增量不等于没有回答：上游可能一次把整段话给回来。这时答案在
        // 循环的交付物里，不把它接上就会把一轮有答案的对话记成「模型没有返回任何
        // 内容」——一次答得好好的对话在历史里变成一次失败。
        Ok(result) if reply.trim().is_empty() => {
            if let Some(text) = result
                .get("reply")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                emit_delta(&out, &session_id, &assistant_mid, text).await;
                reply = text.to_string();
            }
        }
        Ok(_) => {}
    }

    // 派了活但一个字都没说：气泡不能空着（界面把空回复画成失败），而且用户得知道
    // 目标号——那是他后面查进展时唯一要用的值。
    if failure.is_none() && reply.trim().is_empty() {
        if let Some(goal_id) = journal.goals.first() {
            let line = format!(
                "已作为目标 {goal_id} 交给编排器分解执行；分解完成或失败都会回报，\
                 进度在这段对话标题那一行的「常驻 · 守护」徽章上。"
            );
            emit_delta(&out, &session_id, &assistant_mid, &line).await;
            reply = line;
        }
    }
    // 5. Record the turn. 失败的那一轮也写：用户说的这句话是对这个平台说过的，
    //    模型没答出来是这一轮的一个事实，不是把它忘掉的许可。空回复按失败写，
    //    因为它对用户而言和一个错误没有分别，而「什么都不写」是最坏的那种差别。
    //    派了活的那一轮把目标记在记录里：帧只活在连接里，刷新之后历史要能读出
    //    「这一轮交出去过一道目标」，否则追问「刚才那个目标怎么样了」时模型手里
    //    什么都没有。
    match (&failure, reply.is_empty()) {
        (Some(err), _) => {
            record_failed_turn(&state.chat_turns, &session_id, &content, err).await;
        }
        (None, true) => {
            record_failed_turn(
                &state.chat_turns,
                &session_id,
                &content,
                "模型没有返回任何内容",
            )
            .await;
        }
        (None, false) => match journal.goals.first() {
            Some(goal_id) => {
                record_dispatched_turn(&state.chat_turns, &session_id, &content, &reply, goal_id)
                    .await
            }
            None => record_turn(&state.chat_turns, &session_id, &content, &reply).await,
        },
    }

    // 6. 这一轮的花销：排在收尾帧之前，因为界面把它画在这一条消息下面——收尾帧
    //    一到，这一条就定稿了。
    emit_turn_usage(&out, &session_id, &assistant_mid, turn_usage.as_ref()).await;

    // 7. 收尾帧放最后：`message.end` 是「这一轮结束了」的信号，界面收到它就把下
    //    一句发上来，而 `run_chat_turn` 每一轮是各自 spawn 的。记录写在这一帧之后，
    //    用户立刻追问时下一轮读历史的那一次查询就跑在上一轮提交之前——上一轮在
    //    上下文里不存在，模型只能说「我看不到刚才那轮」。实测（2026-11-05）：连续
    //    追问每一轮都丢上一轮，隔几秒再问就记得。先把记录落下来，再说这一轮结束。
    emit_message_end(&out, &session_id, &assistant_mid).await;
}
#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::ChatTurnStore;

    /// 一次询问要走到人面前，答案要回到正好问出它的那一问上。
    ///
    /// 三条一起成立才算通：问句真的出了这条连接（屏幕上没有它，那一轮就一直等
    /// 到断线）；请求号原样带出去（答案按它回来）；答案落到等待的那一问上，
    /// 而不是落到「这一段对话上正在等的那一个」——同一段对话可以有两问同时在等。
    #[tokio::test]
    async fn an_answer_reaches_the_question_that_asked_for_it() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let channel = Arc::new(PermissionChannel::new());
        let bridge = SocketPermissionBridge::new("s1".into(), out_tx, Arc::clone(&channel));
        let ask = PermissionAskView {
            request_id: "perm-1".into(),
            session_id: "s1".into(),
            tool: "write_file".into(),
            summary: "path=a.txt".into(),
            mode: "default".into(),
        };
        let waiter = tokio::spawn(async move { bridge.ask(ask).await });

        let frame = out_rx.recv().await.expect("问句必须出门");
        let parsed: serde_json::Value = serde_json::from_str(&frame).expect("问句是 JSON");
        assert_eq!(parsed["type"], "permission_request");
        assert_eq!(parsed["request_id"], "perm-1");
        assert_eq!(parsed["tool"], "write_file");

        assert!(
            channel.answer("perm-1", PermissionAnswer::allow()),
            "这一问正在等，答案必须找得到它"
        );
        assert!(matches!(
            waiter.await.unwrap(),
            PermissionAnswer::Allow { always: false }
        ));
        assert!(
            !channel.answer("perm-1", PermissionAnswer::allow()),
            "同一个请求号答第二次不该再落进任何一问"
        );
    }

    /// 没人回答的那一问不是默认放行：连接那一头消失时，等的人拿到的是拒绝。
    #[tokio::test]
    async fn a_question_whose_connection_disappears_is_refused() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let channel = Arc::new(PermissionChannel::new());
        let bridge = SocketPermissionBridge::new("s1".into(), out_tx, Arc::clone(&channel));
        let ask = PermissionAskView {
            request_id: "perm-2".into(),
            session_id: "s1".into(),
            tool: "run_command".into(),
            summary: "command=cargo test".into(),
            mode: "default".into(),
        };
        let waiter = tokio::spawn(async move { bridge.ask(ask).await });
        let _ = out_rx.recv().await.expect("问句必须出门");
        // 出站那一头没了（连接断掉），等的那一头必须立刻得到拒绝。
        drop(out_rx);
        match waiter.await.unwrap() {
            PermissionAnswer::Deny { reason, always } => {
                assert!(reason.contains("没有执行"), "{reason}");
                assert!(!always, "没人回答的那一问不该被记成一条规则");
            }
            PermissionAnswer::Allow { .. } => panic!("没人回答时不许放行"),
        }
    }

    /// 目标身份必须走自己的一帧。
    ///
    /// 它现在只写在回复正文里也能被读到——但那是让人去解析一句话，而这句话的措辞
    /// 随提示词改。界面靠名字认这一帧：名字改了，界面不会报错，只是从此再也接不上
    /// 「这道目标下的执行者在做什么」。所以名字与两个字段一起钉在这里。
    #[test]
    fn a_goal_submission_carries_its_identity_in_its_own_frame() {
        let raw = envelope(
            "s1",
            "message.goal",
            serde_json::json!({ "message_id": "m-1", "goal_id": "goal-9" }),
        );
        let value: serde_json::Value = serde_json::from_str(&raw).expect("frame is JSON");
        let event = &value["payload"];
        assert_eq!(event["type"], "message.goal");
        assert_eq!(event["payload"]["goal_id"], "goal-9");
        assert_eq!(event["payload"]["message_id"], "m-1");
        // 会话身份在信封上，不在帧体里：界面按它把这一帧落回哪一段对话。
        assert_eq!(value["session_id"], "s1");
    }

    /// 事件流里说的话变成界面上的字，一轮与一轮之间隔开。
    ///
    /// 网关不再自己调模型，它读的是循环发出来的 `MessageUpdate`。这里钉的是那层
    /// 翻译：增量要逐字发出去，新的一段话要隔一行（否则两轮句子连成一句），而且
    /// 同一个气泡里说的所有字都要留给记录——帧只活在连接里。
    #[tokio::test]
    async fn the_loop_s_events_become_the_frames_the_interface_draws() {
        use cog_core::{AgentEvent, AssistantMessageEvent};
        let (events_tx, events_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(32);
        let (bus, _bus_rx) = tokio::sync::broadcast::channel(8);
        let pump = tokio::spawn(pump_events(
            events_rx,
            out_tx,
            bus,
            "s1".into(),
            "m-1".into(),
        ));

        let update = |event| AgentEvent::MessageUpdate {
            agent_id: "a".into(),
            assistant_event: event,
            timestamp: chrono::Utc::now(),
        };
        for piece in ["先说一句", "再说一句"] {
            events_tx
                .send(update(AssistantMessageEvent::Start {
                    timestamp: chrono::Utc::now(),
                }))
                .await
                .unwrap();
            events_tx
                .send(update(AssistantMessageEvent::TextDelta {
                    content_index: 0,
                    delta: piece.into(),
                    timestamp: chrono::Utc::now(),
                }))
                .await
                .unwrap();
        }
        drop(events_tx);

        let journal = pump.await.expect("pump finished");
        assert_eq!(journal.reply, "先说一句\n\n再说一句");
        assert!(journal.goals.is_empty());

        let mut lines = Vec::new();
        while let Some(line) = out_rx.recv().await {
            lines.push(line);
        }
        let joined = lines.join("\n");
        // 三段文字是三帧，前面还有一帧说明这一轮是谁在说。
        assert_eq!(lines.len(), 4, "一段话三帧，加上身份那一帧：{joined}");
        assert!(lines[0].contains("agent.identity"), "{joined}");
        assert!(lines[1].contains("先说一句"), "{joined}");
        assert!(lines[2].contains("\\n\\n"), "两轮之间没有隔行：{joined}");
        assert!(lines[3].contains("再说一句"), "{joined}");
    }

    /// 这一轮是谁，要单独走一帧，而且一轮只说一次。
    ///
    /// 工具帧只带 `agent_id`，界面拿它去归到哪段对话下面，靠的就是这一帧。少了
    /// 它，这段对话自己的执行过程一条也接不回来，屏幕上只剩一问一答——2026-11-06
    /// 属主对着员工界面提的就是这件事。名字从事件身上取，网关不另算一遍。
    #[tokio::test]
    async fn a_turn_says_who_is_speaking_in_one_frame_of_its_own() {
        use cog_core::{AgentEvent, AssistantMessageEvent};
        let (events_tx, events_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(32);
        let (bus, _bus_rx) = tokio::sync::broadcast::channel(8);
        let pump = tokio::spawn(pump_events(
            events_rx,
            out_tx,
            bus,
            "s1".into(),
            "m-1".into(),
        ));

        for _ in 0..2 {
            events_tx
                .send(AgentEvent::MessageUpdate {
                    agent_id: "kai-a9ab11fc".into(),
                    assistant_event: AssistantMessageEvent::Start {
                        timestamp: chrono::Utc::now(),
                    },
                    timestamp: chrono::Utc::now(),
                })
                .await
                .unwrap();
        }
        drop(events_tx);
        let _ = pump.await;

        let mut identity = Vec::new();
        while let Some(line) = out_rx.recv().await {
            let value: serde_json::Value = serde_json::from_str(&line).expect("frame is JSON");
            if value["payload"]["type"] == "agent.identity" {
                identity.push(value);
            }
        }
        assert_eq!(
            identity.len(),
            1,
            "同一个人一轮里只报一次，重复报就是重复的条目"
        );
        assert_eq!(
            identity[0]["payload"]["payload"]["agent_id"],
            "kai-a9ab11fc"
        );
        assert_eq!(identity[0]["payload"]["payload"]["message_id"], "m-1");
        assert_eq!(identity[0]["session_id"], "s1");
    }

    /// 同一帧也落在界面的总线上。
    ///
    /// 员工那一面与活动流读的是网关的 `agent:<id>` 广播，不是这一条聊天连接：
    /// 只在连接里发帧，屏幕上一次工具调用也看不见——2026-11-06 实测，对话里
    /// 跑完六次工具调用，员工那一面一行都没有。这条判据钉的是同一帧两处都在。
    #[tokio::test]
    async fn the_same_frames_land_on_the_bus_the_interface_reads() {
        use cog_core::AgentEvent;
        let (events_tx, events_rx) = mpsc::channel(8);
        let (out_tx, _out_rx) = mpsc::channel(32);
        let (bus, mut bus_rx) = tokio::sync::broadcast::channel(8);
        let pump = tokio::spawn(pump_events(
            events_rx,
            out_tx,
            bus,
            "s1".into(),
            "m-1".into(),
        ));

        events_tx
            .send(AgentEvent::ToolExecutionStart {
                agent_id: "mira-5ff6bdd2".into(),
                tool_call_id: "call-1".into(),
                tool_name: "run_command".into(),
                args: serde_json::json!({ "command": "ls" }),
                timestamp: chrono::Utc::now(),
            })
            .await
            .unwrap();
        drop(events_tx);
        let _ = pump.await;

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), bus_rx.recv())
            .await
            .expect("这一帧没有落到总线：界面上看不到它")
            .expect("总线这一头应该收到一帧");
        match event {
            AgentEvent::ToolExecutionStart {
                agent_id,
                tool_name,
                ..
            } => {
                assert_eq!(agent_id, "mira-5ff6bdd2");
                assert_eq!(tool_name, "run_command");
            }
            other => panic!("总线收到的不是工具帧：{other:?}"),
        }
    }

    /// 派出去的每个目标都要有自己的那一帧，重复的不发第二遍。
    ///
    /// 界面靠这一帧把目标号接到执行者视图上；同一个目标发两帧，界面那边就会看到
    /// 两个同名条目。
    #[tokio::test]
    async fn every_dispatched_goal_gets_one_frame() {
        use cog_core::AgentEvent;
        let (events_tx, events_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(32);
        let (bus, _bus_rx) = tokio::sync::broadcast::channel(8);
        let pump = tokio::spawn(pump_events(
            events_rx,
            out_tx,
            bus,
            "s1".into(),
            "m-1".into(),
        ));

        // 真事件流是「开始 → 结局」成对的：结局只带调用号，名字在开始那一帧上。
        let started = |id: &str, tool: &str| AgentEvent::ToolExecutionStart {
            agent_id: "a".into(),
            tool_call_id: id.into(),
            tool_name: tool.into(),
            args: serde_json::json!({}),
            timestamp: chrono::Utc::now(),
        };
        let ended = |id: &str, result| AgentEvent::ToolExecutionEnd {
            agent_id: "a".into(),
            tool_call_id: id.into(),
            result,
            is_error: false,
            timestamp: chrono::Utc::now(),
        };
        for (i, goal) in ["goal-1", "goal-1", "goal-2"].into_iter().enumerate() {
            let id = format!("call-{i}");
            events_tx.send(started(&id, "dispatch_goal")).await.unwrap();
            let result = serde_json::json!({ "goal_id": goal });
            events_tx.send(ended(&id, result)).await.unwrap();
        }
        // 别的工具不产生目标帧：这条流上只认派活那一条——哪怕它的返回里也有一个
        // 目标号（列表会带 goal_id），也不算它派的。
        events_tx
            .send(started("call-other", "list_tasks"))
            .await
            .unwrap();
        let other = ended(
            "call-other",
            serde_json::json!({ "goal_id": "goal-9", "total_matching": 1 }),
        );
        events_tx.send(other).await.unwrap();
        drop(events_tx);

        let journal = pump.await.expect("pump finished");
        assert_eq!(
            journal.goals,
            vec!["goal-1".to_string(), "goal-2".to_string()]
        );

        let mut frames = Vec::new();
        while let Some(line) = out_rx.recv().await {
            frames.push(line);
        }
        // 身份那一帧 + 两个不同的目标各一帧（goal-1 重复，不报第二遍）。
        assert_eq!(frames.len(), 3, "{frames:?}");
        assert!(frames[0].contains("agent.identity"), "{frames:?}");
        assert!(frames[1].contains("message.goal") && frames[1].contains("goal-1"));
        assert!(frames[2].contains("goal-2"));
    }

    /// 这一轮的花销原样交给界面：网关不重算，只搬循环量出来的那一份。
    #[tokio::test]
    async fn the_turn_s_usage_reaches_the_interface_as_one_frame() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let counted = serde_json::json!({
            "input_tokens": 1234,
            "output_tokens": 56,
            "total_tokens": 1290,
            "rounds": 3,
            "context_tokens": 1234,
            "duration_ms": 4200,
            "model": "some-model",
            "cost_usd": 0.0123,
        });

        emit_turn_usage(&out_tx, "s1", "m-1", Some(&counted)).await;

        let frame = out_rx.recv().await.expect("这一帧要发出去");
        assert!(frame.contains("\"message.usage\""), "{frame}");
        assert!(frame.contains("\"rounds\":3"), "{frame}");
        assert!(frame.contains("\"input_tokens\":1234"), "{frame}");
        assert!(frame.contains("\"model\":\"some-model\""), "{frame}");
        assert!(frame.contains("\"cost_usd\":0.0123"), "{frame}");
    }

    /// 没读到就不发：一帧全零的花销，读起来是「这一轮不要钱、没花时间」。
    #[tokio::test]
    async fn a_turn_without_a_reading_does_not_get_a_frame_of_zeroes() {
        let (out_tx, mut out_rx) = mpsc::channel(4);

        emit_turn_usage(&out_tx, "s1", "m-1", None).await;
        drop(out_tx);

        assert!(
            out_rx.recv().await.is_none(),
            "循环没交出用量时，屏幕上不该出现一行零"
        );
    }

    /// 历史从记录里来，不从这个进程的内存里来。
    ///
    /// 这一格替换掉的是那个 `HashMap`：它在同一个进程里读自己刚写的东西，所以
    /// 「历史还在」这件事在重启之前永远为真——而重启正是要证明的那一件事。
    #[tokio::test]
    async fn the_history_the_model_sees_comes_from_the_store() {
        let store = std::sync::Arc::new(FakeTurns::default());
        store
            .record_turn("s1", "上一问", "上一答")
            .await
            .expect("seed the fake transcript");
        let handle: crate::ChatTurnHandle = Some(store.clone());

        let messages = load_history(&handle, "s1").await;
        assert_eq!(
            messages.iter().map(|m| m.role()).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        assert_eq!(messages[0].content(), "上一问");
        assert_eq!(messages[1].content(), "上一答");
        // 上下文窗口的界要有人传给存储面，否则「最近 40 条」只是这里的一句注释。
        assert_eq!(*store.last_limit.lock().await, Some(SESSION_HISTORY_CAP));
    }

    /// 回答出来的这一轮必须写进记录，且是作为一问一答一起写。
    #[tokio::test]
    async fn an_answered_turn_is_written_down_as_a_pair() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_turn(&handle, "s1", "问", "答").await;
        let held = store.turns.lock().await.clone();
        assert_eq!(
            held,
            vec![
                cog_core::ChatTurn {
                    role: cog_core::ChatTurnRole::User,
                    content: "问".into()
                },
                cog_core::ChatTurn {
                    role: cog_core::ChatTurnRole::Assistant,
                    content: "答".into()
                },
            ]
        );
    }

    /// 交出去的那一轮必须把目标记进记录，而不是只发一帧。
    ///
    /// 这一格盯的是刷新之后的那一半：帧只活在连接里，历史从记录里来。记录里没有
    /// 目标，重新打开这段对话就退化成一段纯问答——界面把「常驻·守护」显示成
    /// 「可下线」，执行者在做什么也无从接起。所以这里读的不是那一帧，是记录。
    #[tokio::test]
    async fn a_dispatched_turn_keeps_its_goal_where_the_history_can_read_it() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_dispatched_turn(&handle, "s1", "把这件事做了", "已受理", "goal-7").await;

        // 问句与答复照样成对进上下文：交出去不等于这一轮就不用记。
        let held = store.turns.lock().await.clone();
        assert_eq!(held.len(), 2, "这一轮要成对落盘：{held:?}");
        assert_eq!(held[1].content, "已受理");

        let dispatched = store.dispatched.lock().await.clone();
        assert_eq!(
            dispatched,
            vec![("把这件事做了".to_string(), "goal-7".to_string())],
            "目标身份必须落在记录里，不能只活在回复正文与那一帧上"
        );
    }

    /// 没答出来的那一轮同样是用户说过的一句话，必须留下。
    ///
    /// 上游不可用的那段时间，这个平台收到的每一句话原本都会消失：记录只在
    /// 回答送达后才写。这里要的正是反面——问句和原因都进记录，而构建上下文时
    /// 它不冒充模型说过的话。
    #[tokio::test]
    async fn an_unanswered_turn_still_keeps_what_the_user_said() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_failed_turn(&handle, "s1", "问", "upstream 503").await;

        let held = store.unanswered.lock().await.clone();
        assert_eq!(
            held,
            vec![("问".to_string(), "upstream 503".to_string())],
            "问句与原因都要落在记录里"
        );
        // 它不进上下文：失败通知不是模型说过的话。
        assert!(store.turns.lock().await.is_empty());
    }

    /// 没有记录库的部署：照样回答，不假装有历史，也不悄悄记在别处。
    #[tokio::test]
    async fn a_deployment_without_a_store_answers_without_history() {
        let handle: crate::ChatTurnHandle = None;
        assert!(load_history(&handle, "s1").await.is_empty());
        // 不发散：这里只要求「不写进任何东西」——没有第二个家可写。
        record_turn(&handle, "s1", "问", "答").await;
        record_failed_turn(&handle, "s1", "问", "答不出来").await;
    }

    /// 读记录失败按「没有历史」回答，而不是按「历史是空的」记一笔。
    #[tokio::test]
    async fn a_transcript_that_cannot_be_read_answers_without_context() {
        let store = std::sync::Arc::new(FakeTurns::default());
        store
            .record_turn("s1", "问", "答")
            .await
            .expect("seed the fake transcript");
        store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let handle: crate::ChatTurnHandle = Some(store.clone());
        assert!(load_history(&handle, "s1").await.is_empty());
        // 写也失败时不许把这一轮的回答吞掉：record_turn 只留 warn。
        record_turn(&handle, "s1", "问", "答").await;
    }

    /// 一份内存里的记录，只作测试替身。生产里没有它的对应物——这次改动拆掉的
    /// 正是那样一张表。
    #[derive(Default)]
    struct FakeTurns {
        turns: tokio::sync::Mutex<Vec<cog_core::ChatTurn>>,
        /// 有问无答的那几轮：留在记录里，但不进上下文（`history` 不返回它们）。
        unanswered: tokio::sync::Mutex<Vec<(String, String)>>,
        /// 交出去的那几轮与它们的目标：(问句, goal_id)。
        dispatched: tokio::sync::Mutex<Vec<(String, String)>>,
        last_limit: tokio::sync::Mutex<Option<usize>>,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl cog_core::ChatTurnStore for FakeTurns {
        async fn record_turn(
            &self,
            session_id: &str,
            user: &str,
            assistant: &str,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            let mut held = self.turns.lock().await;
            if session_id != "s1" {
                return Ok(());
            }
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::User,
                content: user.to_string(),
            });
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::Assistant,
                content: assistant.to_string(),
            });
            Ok(())
        }

        async fn record_dispatched_turn(
            &self,
            session_id: &str,
            user: &str,
            assistant: &str,
            goal_id: &str,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            if session_id != "s1" {
                return Ok(());
            }
            let mut held = self.turns.lock().await;
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::User,
                content: user.to_string(),
            });
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::Assistant,
                content: assistant.to_string(),
            });
            self.dispatched
                .lock()
                .await
                .push((user.to_string(), goal_id.to_string()));
            Ok(())
        }

        async fn record_failed_turn(
            &self,
            session_id: &str,
            user: &str,
            failure: &str,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            if session_id != "s1" {
                return Ok(());
            }
            self.unanswered
                .lock()
                .await
                .push((user.to_string(), failure.to_string()));
            Ok(())
        }

        async fn history(
            &self,
            _session_id: &str,
            limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::ChatTurn>> {
            *self.last_limit.lock().await = Some(limit);
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            Ok(self.turns.lock().await.clone())
        }

        /// 读回整段会话（含失败轮），按时间正序截取最后 `limit` 条。
        ///
        /// 与 `history` 的差别只在「失败轮算不算」：上下文里不放它们，屏幕上要放。
        async fn transcript(
            &self,
            _session_id: &str,
            limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::ChatTurnRecord>> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            if limit == 0 {
                return Ok(Vec::new());
            }
            let turns = self.turns.lock().await;
            let start = turns.len().saturating_sub(limit);
            Ok(turns[start..]
                .iter()
                .enumerate()
                .map(|(offset, turn)| cog_core::ChatTurnRecord {
                    id: (start + offset) as i64 + 1,
                    role: turn.role,
                    content: turn.content.clone(),
                    failure: None,
                    goal: None,
                    created_at: chrono::Utc::now(),
                })
                .collect())
        }
    }

    #[test]
    fn envelope_matches_frontend_contract() {
        let raw = envelope(
            "s1",
            "message.start",
            serde_json::json!({"message_id": "m1", "role": "assistant"}),
        );
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["type"], "agent_event");
        assert_eq!(v["session_id"], "s1");
        assert_eq!(v["payload"]["type"], "message.start");
        assert_eq!(v["payload"]["payload"]["message_id"], "m1");
        assert_eq!(v["payload"]["payload"]["role"], "assistant");
    }

    /// 停止是一族有名字的上行消息，名字就是 `chat_cancel`。
    ///
    /// 界面按这个名字发，网关按这个名字认。名字错了不会有报错：那条消息落进
    /// 「不是这一族的上行消息」，用户按下的停止在服务端根本没有发生过。
    #[test]
    fn a_stop_is_a_message_of_its_own_named_chat_cancel() {
        let raw = serde_json::json!({
            "type": "chat_cancel",
            "session_id": "s-1",
            "timestamp": "2026-11-11T00:00:00Z",
        });
        match serde_json::from_value::<crate::websocket_protocol::ClientMessage>(raw) {
            Ok(crate::websocket_protocol::ClientMessage::ChatCancel { session_id, .. }) => {
                assert_eq!(session_id, "s-1");
            }
            other => panic!("`chat_cancel` 没有解析成停止：{other:?}"),
        }
        // 不带时间戳也要认得出：时间戳只是旁注，不是这条消息的身份。
        let bare = serde_json::json!({ "type": "chat_cancel", "session_id": "s-1" });
        assert!(matches!(
            serde_json::from_value::<crate::websocket_protocol::ClientMessage>(bare),
            Ok(crate::websocket_protocol::ClientMessage::ChatCancel { .. })
        ));
        // 反过来：一句聊天不是一个停止。两条消息共用一个名字时，用户每说一句话
        // 就会把自己上一轮停掉。
        let chat = serde_json::json!({ "type": "chat_message", "content": "你好" });
        assert!(matches!(
            serde_json::from_value::<crate::websocket_protocol::ClientMessage>(chat),
            Ok(crate::websocket_protocol::ClientMessage::ChatMessage { .. })
        ));
    }

    /// 一次停止停的是这一段上**全部**在跑的轮，别的那一段一轮都不动。
    ///
    /// 同一段对话里可以同时有好几轮（用户连发两句），而客户端只拿着会话号；
    /// 把「发一句就少停一轮」写进这里，第二句的回答会在用户停手之后继续往下说。
    #[tokio::test]
    async fn a_stop_reaches_every_turn_in_that_session_and_no_other() {
        let running = RunningTurns::new();
        let first = running.register("s-1", "m-1");
        let second = running.register("s-1", "m-2");
        let elsewhere = running.register("s-2", "m-3");

        // 没有停止之前，一段上那两个接收端一个都不该动。
        let mut first = first.clone();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), first.changed())
                .await
                .is_err()
        );

        assert_eq!(running.cancel("s-1"), 2);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), wait_cancelled(first))
                .await
                .is_ok(),
            "这一段上第一轮没有收到停止"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), wait_cancelled(second))
                .await
                .is_ok(),
            "这一段上第二轮没有收到停止"
        );
        // 另一段对话上那一轮的信号没有被动过：它还在等。
        let mut elsewhere = elsewhere;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), elsewhere.changed())
                .await
                .is_err()
        );
    }

    /// 结束掉的那一轮再也不会被停：停止按钮不该停到上一轮的尸体上。
    ///
    /// 不摘干净的话，名册会一直长：一段开过几十轮的对话关掉以后，这里留着一串
    /// 没人接的信号，下一次停止发给它们，而真正在跑的那一轮收不到。
    #[tokio::test]
    async fn a_finished_turn_is_not_stopped_again() {
        let running = RunningTurns::new();
        let stop = running.register("s-1", "m-1");
        running.finish("s-1", "m-1");
        assert_eq!(running.cancel("s-1"), 0);
        assert!(!*stop.borrow());
    }

    /// 停止信号消失不等于用户按了停止。
    ///
    /// 通道的发送端在「这一轮结束」「连接被清掉」时都会消失；把这件事读成停止，
    /// 会在与用户意图无关的时刻把一轮好好的回答砍掉——而那正是最难复现的一类。
    #[tokio::test]
    async fn a_signal_that_disappears_without_a_stop_does_not_stop_anything() {
        let (stop, rx) = watch::channel(false);
        drop(stop);
        let work = async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            7
        };
        match run_until_stopped(work, rx).await {
            TurnOutcome::Finished(value) => assert_eq!(value, 7),
            TurnOutcome::Stopped => panic!("发送端关掉被读成了「用户按了停止」"),
        }
    }

    /// 停下来的那一轮是真的被放下了，不是「结果没人要」。
    ///
    /// 循环、模型调用、工具调用都在那个 future 里。只丢结果不丢 future 的写法在
    /// 屏幕上看起来一模一样，而模型那边还在生成、花销还在记——停止按钮就成了一个
    /// 遮住屏幕的动作。
    #[tokio::test]
    async fn a_stop_drops_the_turn_instead_of_letting_it_finish() {
        struct Dropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Dropped(dropped.clone());
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            run_until_stopped(
                async move {
                    let _flag = flag;
                    std::future::pending::<()>().await;
                },
                rx,
            )
            .await
        });

        // 让那一轮真的开始等，再按停止：不然这里测的是「还没开始就被丢掉」。
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        stop.send(true).expect("the turn is still registered");

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("停止之后那一轮没有结束")
            .expect("the turn task did not panic");
        assert!(matches!(outcome, TurnOutcome::Stopped));
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "那一轮的结果被丢掉了，但它自己还在跑：停止没有真的停下来"
        );
    }

    /// 一轮先跑完、停止后到：结局是跑完，不是被停。
    #[tokio::test]
    async fn a_turn_that_ends_before_the_stop_is_not_called_stopped() {
        let (stop, rx) = watch::channel(false);
        let outcome = run_until_stopped(async { 7 }, rx).await;
        let _ = stop;
        match outcome {
            TurnOutcome::Finished(value) => assert_eq!(value, 7),
            TurnOutcome::Stopped => panic!("已经答完的一轮被读成了被停"),
        }
    }
}
