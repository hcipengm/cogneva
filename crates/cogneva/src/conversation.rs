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
use cog_gateway::chat::{
    CheckpointFile, CheckpointList, CheckpointView, ConversationRequest, ConversationTurn,
    MemoryEntryView, RewindReport,
};

/// 这一屏交给模型的那一句：属主要在对话里办的事，和只是说话，怎么分。
///
/// 不另起一次分类调用：这一轮本来就要回答，模型在回答里选择调不调
/// `start_task` 就是分类。工具调用不是额外模型调用，只多几十个 token。
const CONVERSATION_INTENT_INSTRUCTION: &str = "这一屏是属主在和你说话。属主这一句如果要求办一件事——改代码、修东西、做出来或去查——就在这一轮调用 start_task，把 goal、acceptance、terminal、now、gap 一起给出：terminal 是做完之后世界的样子，now 是此刻树上读到的样子，gap 是还缺什么；然后回答属主。如果只是问题、闲聊或追问，就不要调 start_task，直接回答。不要把一句普通的话派成任务。";

/// `/task` 与 `/chat`：属主自己说的判词，不由模型再猜一遍。
///
/// 两个词只在句首生效（前面的空白不算），后面跟什么都行。它们不改原话——
/// 意图行里存的仍是属主打的整句——只改这一轮交给模型的那一段说明。别的写法
/// （句子中间出现 `/task`、`/tasking`）不是开关：把一句提到开关词的话读成开关，
/// 是替属主做了他没做的决定。
fn intent_instruction(turn: &str) -> String {
    // 开关是句首那**一个词**，不是句首的几个字母：`/tasking` 是一个普通词，
    // 拿前缀去匹配会把它读成 `/task`。
    let word = turn
        .trim_start()
        .split(char::is_whitespace)
        .next()
        .unwrap_or("");
    let directive = match word {
        "/task" => {
            "属主这一句打了 `/task`：这是一件要办的事，这一轮必须调用 start_task 把它交出来。"
        }
        "/chat" => "属主这一句打了 `/chat`：这一句只是说话，不要调用 start_task。",
        _ => return CONVERSATION_INTENT_INSTRUCTION.to_string(),
    };
    format!("{CONVERSATION_INTENT_INSTRUCTION}\n\n{directive}")
}

/// 这一句里有没有「可能是一件活」的动词。**只做前置，不主判。**
///
/// 回答那一轮自己会判；它答了「不是活」之后，这一句默认被标成已接手，下一批
/// 分诊不会再捡。这条预筛守的是另一半：一句带着「改 / 修 / 查 / 部署」的话被
/// 判成了闲聊时，不标接手，留给批量分诊带着原文重判一次——那一次是兜底，不是
/// 主判。没有动词的句子（问候、感想、纯问题）照常标掉，闲聊因此仍然零额外调用。
///
/// 判据故意只是「出现过这些词」：它不判断语气、不判断谁在说、不判断上下文。
/// 一个确定的字面量至少是确定的；让这条前置去猜意图，就等于把主判搬到一个更
/// 便宜也更瞎的调用上。
fn may_be_work(text: &str) -> bool {
    const WORK_VERBS: &[&str] = &[
        "改",
        "修",
        "做",
        "建",
        "删",
        "加",
        "查",
        "写",
        "跑",
        "部署",
        "提交",
        "实现",
        "补",
        "清理",
        "迁移",
        "上线",
        "重命名",
        "替换",
        "fix",
        "change",
        "add",
        "build",
        "deploy",
        "implement",
        "remove",
        "update",
        "write",
        "run",
        "ship",
        "migrate",
        "refactor",
    ];
    let lowered = text.to_lowercase();
    WORK_VERBS.iter().any(|verb| lowered.contains(verb))
}

/// 这一段对话在名册里属于哪个分组。
///
/// 分组只参与名字的哈希材料，把界面上这些员工与 worker 池、进化池的同号槽位
/// 分开。它是清单里写得出来的字面量，所以同一个会话滚一次 Pod 还是同一个名字。
const CHAT_GROUP: &str = "web";

/// 名册上那一条多久续一次。
///
/// 名册的过期是 30 秒：一轮对话动不动就超过它，不续的话它会在自己还在说话的
/// 时候从名册上消失——同事这时候发给它的消息就投进一个没人认领的收件箱。
/// 取的是 agent 续约的同一个拍子（`AGENT_REGISTRATION_TTL_SECS` ÷ 拍数）：会话员工
/// 与常驻 worker 在同一本名册上，续约节奏不同就会一边活一边被判死。
const REGISTRATION_HEARTBEAT_SECS: u64 = cog_core::config::AGENT_HEARTBEAT_INTERVAL_SECS;

/// 一次开跑最多读几条积压消息。
///
/// 上下文的容量是有限的，收件箱不是：一晚上攒了五十条，全塞进这一轮会把这一轮
/// 自己的问题挤出去。读不完的留在流里，下一轮接着读。
// cap: fixed — 一轮对话的上下文里，收件箱这一块最多占几条
// keep: 删掉后收件箱这一块可以任意长，把一段对话的上下文淹掉；由「一轮上下文里最多占几条」兜住
const INBOX_DRAIN_MAX: usize = 16;

/// 读收件箱时，等一条新消息等多久算「这一轮没有更多了」。
///
/// 这是人正等着的一段对话，不是后台消费者：静默窗口按人的耐心取，而不是按
/// 总线的延迟上限取。
// cap: fixed — 收件箱排空的静默窗口，按一段对话能接受的额外等待取
// keep: 删掉后排空没有静默窗口，一段对话的额外等待无界；由「按对话能接受的额外等待取」兜住
const INBOX_QUIET_MS: u64 = 200;

/// 一次开跑花在读收件箱上的总预算。
///
/// 消息接连不断地来也不无限读：这条界与条数界互为兜底，谁先到谁停。
// cap: fixed — 开跑前读收件箱的总时间预算
// keep: 删掉后开跑前读收件箱没有总预算，一条挂住的队列拖着整段对话；由「开跑前读收件箱的总时间预算」兜住
const INBOX_DRAIN_BUDGET_MS: u64 = 1_500;

/// 领回「上一轮读到一半就断了」的消息，等它闲置多久。
///
/// 与后台消费者同一个取法：同一段会话的两轮不会同时在跑，所以这个窗口只要
/// 大过一轮正常对话的读耗时，就不会把另一个人正在读的那条抢过来。
// cap: fixed — 回收待处理消息的最小闲置时长
// keep: 删掉后把还在排空的消息抢回来，同一件活做两遍；由「最小闲置时长」兜住
const INBOX_TAKEOVER_IDLE_MS: u64 = 5_000;

/// 一次回收调用要多少条。
// cap: fixed — 一次回收调用要多少条待处理消息
// keep: 删掉后一次回收把所有待处理消息拉进内存；由「一次回收要多少条」兜住
const INBOX_TAKEOVER_BATCH: usize = 16;

/// 一次 `list_tasks` 最多回多少行。
// cap: fixed — 一次读台账回多少行；整张图会把上下文和这次回答一起淹掉
// keep: 删掉后整张图把上下文和这次回答一起淹掉；由「一次读台账回多少行」兜住
const LIST_TASKS_LIMIT: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: fixed — 默认页只有上限的一半：上限是显式要多的人能拿到的，默认值给的是「一眼扫完、还能接着问」的量
// keep: 删掉后默认页等于上限，一眼扫不完也不留追问余地；由「默认页是上限的一半」兜住
const LIST_TASKS_DEFAULT: usize = 20;

/// 一次读变更账向账本要多少行，也是调用方能要到的上限。
///
/// 未了结的那本账有几千行，全取回来会把这一轮的上下文和这次回答一起淹掉。取回来
/// 再按调用方的 `match`/`state` 筛，所以窗口要够筛而不是刚好够看。
// cap: fixed — 一次读变更账向账本要多少行
// keep: 删掉后一次读变更账要的行数无界；由「一次读变更账要多少行」兜住
const LIST_CHANGES_WINDOW: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: fixed — 默认页是窗口的一半：够扫一眼，也留得住接着追问的余地
// keep: 删掉后默认页等于窗口，够不着一眼扫完又留得住追问的余地；由「默认页是窗口的一半」兜住
const LIST_CHANGES_DEFAULT: usize = 20;
/// 一条变更的历史最多回几步。
// cap: fixed — 一次 `get_change` 回多少条状态迁移；更早的那几步读不出「现在在哪」以外的信息
// keep: 删掉后回到「一次 `get_change` 回多少条状态迁移；更早的那几步读不出「现在在哪」以外的信息」所描述的情形；由这条约定兜住
const CHANGE_HISTORY_LIMIT: usize = 20;
/// 一次读原则账向账本要多少行，也是调用方能要到的上限。
// cap: fixed — 一次读原则账向账本要多少行
// keep: 删掉后一次读原则账要的行数无界；由「一次读原则账要多少行」兜住
const LIST_PRINCIPLES_WINDOW: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: fixed — 默认页是窗口的一半，理由同变更账那一对
// keep: 删掉后默认页等于窗口；由「默认页是窗口的一半」兜住
const LIST_PRINCIPLES_DEFAULT: usize = 20;
/// 一次读告警最多回几条。
// cap: fixed — 一次读告警回几条；亮着的告警多到看不完时，本身就是要先说出来的事
// keep: 删掉后亮着的告警一次全回来，说不完的告警把回答淹掉；由「告警多到看不完本身就是要先说的事」兜住
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
    // 员工页点出来的会话 id 自己就写着「跟谁说话」：这一条是纯函数，不依赖身份
    // 绑定表在不在。没有这一支时，进程没装身份存储的部署会把同一段会话派给一个
    // 由指纹算出来的陌生人——员工页点 E0412，答话的却是别人。
    if let Some(target) = session_target(session_id) {
        return employee_for_target(target);
    }
    employee_named(fingerprint().as_deref(), session_id)
}

/// 这个进程的实例指纹。缺了就是缺了——调用方据此把名字标成不跨重启。
fn fingerprint() -> Option<String> {
    std::env::var(cog_core::env::INSTANCE_FINGERPRINT)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 这一段对话的人：先认登录身份，再认会话，都没有才分配一个并写回去。
///
/// 认人的顺序就是「这个人是谁」离得由近到远：
///
/// 1. 登录身份（`owner`）。同一个属主开第二段对话，读到的是**同一位员工**——
///    名册上不多一行、工号不重发、账上不换人。人不是会话的一部分。
/// 2. 会话那一行。没有登录身份的连接（探针、升级前的老客户端）只有这一条线，
///    它把「第一次见面时的那个人」钉在会话上，滚一次 Pod 也不换人。
/// 3. 「说过话的人」名册上属于这段会话的那一行。绑定表是后加的，更老的会话
///    没有它；不认这一步，升级之后同一个会话会在名单上变成两个人。
/// 4. 都没有才按「实例指纹 ＋ 会话」新发一个——名字是算出来的，不是查出来的。
///
/// 写回时两边都写：会话那一行让同一段对话接着是这个人，身份那一行让他的下一段
/// 对话也是这个人。没有第二句，「同一个人开两段对话就是两个人」这个病会从另一
/// 个门原样回来。
/// 会话 id 的派生前缀：员工页点谁，就用谁的 id 算出这一段会话。
///
/// WebUI 那一侧用同一个前缀算 id；后端只认这一个前缀，不另收一个「我在跟谁
/// 说话」的客户端字段。同一个人从主对话入口和员工页入口进来，落到同一个
/// `agent:<员工 id>` 上，历史因此是同一段。
const SESSION_FOR_AGENT_PREFIX: &str = "agent:";

pub fn session_for_employee(agent_id: &str) -> String {
    format!("{SESSION_FOR_AGENT_PREFIX}{agent_id}")
}

fn session_target(session_id: &str) -> Option<&str> {
    session_id
        .strip_prefix(SESSION_FOR_AGENT_PREFIX)
        .map(str::trim)
        .filter(|target| !target.is_empty())
}

/// 被点名的那个员工：id 就是名册上的 id，显示名先用 id，名册行会覆盖它。
fn employee_for_target(agent_id: &str) -> cog_agent::identity::AgentName {
    cog_agent::identity::AgentName {
        persona: agent_id.to_string(),
        short: String::new(),
        handle: agent_id.to_string(),
        id: agent_id.to_string(),
        stable: true,
    }
}

async fn bound_employee(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    fingerprint: Option<&str>,
    owner: Option<&str>,
    session_id: &str,
) -> cog_agent::identity::AgentName {
    let owner = owner.map(str::trim).filter(|owner| !owner.is_empty());
    let as_name = |row: cog_gateway::agents::SessionEmployee| cog_agent::identity::AgentName {
        persona: row.persona,
        short: row.short,
        handle: row.handle,
        id: row.agent_id,
        stable: row.stable,
    };

    // 这一段会话已经钉了人：会话绑定先于属主，员工页点谁就以谁为准。
    if let Some(row) = cog_gateway::agents::read_bound_session(&**kv, session_id).await {
        let employee = as_name(row);
        // 这个属主从前没有绑定：把他记下来，下一段普通会话才认得回同一个人。
        bind_user(kv, owner, &employee).await;
        return employee;
    }
    // 员工页点出来的会话：id 本身就是「跟谁说话」。没有绑定就按 id 钉住，
    // 第一轮之后上面那条会话绑定就会接住它。
    if let Some(target) = session_target(session_id) {
        let employee = employee_for_target(target);
        bind_session(kv, session_id, &employee).await;
        bind_user(kv, owner, &employee).await;
        return employee;
    }
    if let Some(owner) = owner {
        if let Some(row) = cog_gateway::agents::read_bound_user(&**kv, owner).await {
            let employee = as_name(row);
            // 这一段会话也记到他名下：会话表是给「没有登录身份的连接」与老会话
            // 用的，有登录身份的这一段同样要能按会话认回来。
            bind_session(kv, session_id, &employee).await;
            return employee;
        }
    }
    // 老会话：名册行上的 id 与名字就是它的人，按那一行认，不再重算。
    let existing = cog_gateway::agents::person_for_session(kv, session_id).await;
    let employee = match existing.as_ref().and_then(|(id, row)| {
        let (persona, short) = row.display_name.split_once('#')?;
        (id == &format!("{}-{}", persona.to_lowercase(), short)).then(|| {
            cog_agent::identity::AgentName {
                persona: persona.to_string(),
                short: short.to_string(),
                handle: row.display_name.clone(),
                id: id.clone(),
                stable: true,
            }
        })
    }) {
        Some(employee) => employee,
        None => employee_named(fingerprint, session_id),
    };
    bind_session(kv, session_id, &employee).await;
    bind_user(kv, owner, &employee).await;
    employee
}

fn binding_of(employee: &cog_agent::identity::AgentName) -> cog_gateway::agents::SessionEmployee {
    cog_gateway::agents::SessionEmployee {
        agent_id: employee.id.clone(),
        persona: employee.persona.clone(),
        short: employee.short.clone(),
        handle: employee.handle.clone(),
        stable: true,
    }
}

async fn bind_session(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    session_id: &str,
    employee: &cog_agent::identity::AgentName,
) {
    if let Err(e) =
        cog_gateway::agents::write_bound_session(&**kv, session_id, &binding_of(employee)).await
    {
        tracing::warn!(
            agent_id = %employee.id,
            session_id,
            error = %e,
            "会话没有绑定到人：下一位接手的实例会重算这个名字"
        );
    }
}

async fn bind_user(
    kv: &Arc<dyn cog_core::KeyValueStore>,
    owner: Option<&str>,
    employee: &cog_agent::identity::AgentName,
) {
    let Some(owner) = owner else {
        return;
    };
    if let Err(e) = cog_gateway::agents::write_bound_user(&**kv, owner, &binding_of(employee)).await
    {
        tracing::warn!(
            agent_id = %employee.id,
            owner,
            error = %e,
            "登录身份没有绑定到人：这个属主的下一段对话会换成另一个人"
        );
    }
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
    /// 运行时原则账。与上面三本不同的地方只有一处：这段对话里的人能往里写——
    /// 员工提案，属主拍板。批了之后下一轮开头它就随基础指令交给每个人，所以
    /// 「从对话里改规则」不用重发版，也不用重启谁。
    principles: Option<Arc<dyn cog_core::RuntimePrincipleStore>>,
    /// 进化控制面：放行一条等审批的变更、把正在跑的那一版退回去。它由反射插件
    /// 发布；没装的进程照实缺席，而不是把「这里没有能拍板的东西」说成「没有
    /// 待批的变更」——那两个读数要做的事相反。
    evolution: Option<Arc<dyn cog_core::EvolutionAdmin>>,
    /// 外部 MCP 服务：它的工具还没进过任何一张工具表，这一轮把它们装进来。
    /// 没有端点的部署就是 `None`，界面据此如实说「没挂外部服务」。
    mcp: Option<Arc<dyn cog_core::McpClient>>,
    /// 属主配的钩子。同一张配置表里两类动作、两个执行者：判定（阻断 / 改写）
    /// 在工具动手之前由这一侧回答，通知（webhook / redis / log / notify）交给
    /// 发布引擎。没装引擎的进程照实缺席，界面上没有这一行。
    hooks: Option<Arc<dyn cog_core::HookEngine>>,
    /// 外部价值判定的落盘面：属主就「什么算好」说的一句话，原样记进账。
    ///
    /// 与上面那几件同一类可选能力——没有装的进程照实缺席，工具会说「记不下来」，
    /// 而不是回一句「已记录」把一句没落盘的话说成落盘了。
    taste: Option<Arc<dyn cog_core::TasteIntentSink>>,
    /// 执行器那一侧。检查点拍的是**那棵树**，所以这件事只能问它；没有接执行器
    /// 的进程照实缺席，界面读到的是「这个部署做不了检查点」，而不是一张空列表。
    sandbox: Option<Arc<dyn cog_core::SandboxBackend>>,
    /// 意图落点：与网关的 `GatewayState.intent_events` 是同一份。这里只借来写
    /// 属主在对话里说的那一句；缺席时 `record(None, ...)` 会计进 `unwired`，
    /// 不会让这一轮安静地不写。
    intent_events: Option<Arc<dyn cog_core::EventStore>>,
}

impl PlatformConversation {
    // 这些入参各自是一件可选能力，没有哪几件天然该合成一个名词：会话要的是
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
        principles: Option<Arc<dyn cog_core::RuntimePrincipleStore>>,
        evolution: Option<Arc<dyn cog_core::EvolutionAdmin>>,
        taste: Option<Arc<dyn cog_core::TasteIntentSink>>,
        mcp: Option<Arc<dyn cog_core::McpClient>>,
        hooks: Option<Arc<dyn cog_core::HookEngine>>,
        sandbox: Option<Arc<dyn cog_core::SandboxBackend>>,
        intent_events: Option<Arc<dyn cog_core::EventStore>>,
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
            principles,
            evolution,
            taste,
            mcp,
            hooks,
            sandbox,
            intent_events,
        }
    }

    /// 这一段对话是谁在说话：绑在登录身份上的那个人，与 `turn()` 用的是同一个。
    ///
    /// 抽出来是因为检查点要按**同一棵树的同一个车道**去拍：车道由这里的 `id`
    /// 决定，两处各算一次名字，快照就可能落在另一棵树上——而读数是「拍到了」。
    async fn employee_here(
        &self,
        owner: Option<&str>,
        session_id: &str,
    ) -> cog_agent::identity::AgentName {
        match self.people.as_ref() {
            Some(kv) => bound_employee(kv, fingerprint().as_deref(), owner, session_id).await,
            None => employee_for(session_id),
        }
    }
}

/// 会话开头的那一份快照，只在这个会话还没有任何一份的时候拍。
///
/// 拍不下来不算这一轮的失败：检查点是附加能力，树不是检出（或者执行器这一跳
/// 没通）时那一轮照样要能说话。但「为什么拍不下来」要落下来——停在一张空列表
/// 上分不出「还没跑过」与「这个部署做不了」。
async fn open_checkpoints(
    sandbox: Arc<dyn cog_core::SandboxBackend>,
    kv: Arc<dyn cog_core::KeyValueStore>,
    owner: String,
    session_id: String,
    agent_id: String,
) {
    let existing = match crate::checkpoints::load(&kv, &owner, &session_id).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::debug!(session_id, error = %e, "这一段会话的检查点读不到，这一轮不拍");
            return;
        }
    };
    if existing.rows.iter().any(|row| row.turn == 0) {
        return;
    }
    let key = crate::checkpoints::key(&session_id, 0);
    match crate::checkpoints::capture(&sandbox, &agent_id, &session_id, &key).await {
        Ok(_) => {
            let row = crate::checkpoints::Row {
                key,
                turn: 0,
                at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(e) = crate::checkpoints::remember(&kv, &owner, &session_id, row).await {
                tracing::warn!(session_id, error = %e, "会话开头的检查点没记下来");
            }
        }
        Err(reason) => {
            tracing::debug!(session_id, %reason, "会话开头的检查点拍不下来");
            cog_core::ignore_lost!(
                crate::checkpoints::mark_unavailable(&kv, &owner, &session_id, reason).await
            );
        }
    }
}

/// 这一轮结束时的快照。
///
/// 记在跑完之后派出去：快照要走一趟执行器读一遍树，放在开跑之前会变成「说完话
/// 以后它先愣一下」，放在交付之后同步等，人看到最后一行字要多等这一趟。代价是
/// 名字上的差一轮——退回到第 N 轮之前要的是第 N−1 轮的快照，这条规则在
/// [`crate::checkpoints::rewind_target`] 一处写着。
async fn close_checkpoints(
    sandbox: Arc<dyn cog_core::SandboxBackend>,
    kv: Arc<dyn cog_core::KeyValueStore>,
    owner: String,
    session_id: String,
    agent_id: String,
) {
    let existing = match crate::checkpoints::load(&kv, &owner, &session_id).await {
        Ok(rows) => rows,
        Err(_) => return,
    };
    let turn = existing.rows.iter().map(|row| row.turn).max().unwrap_or(0) + 1;
    let key = crate::checkpoints::key(&session_id, turn);
    match crate::checkpoints::capture(&sandbox, &agent_id, &session_id, &key).await {
        Ok(_) => {
            let row = crate::checkpoints::Row {
                key,
                turn,
                at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(e) = crate::checkpoints::remember(&kv, &owner, &session_id, row).await {
                tracing::warn!(session_id, turn, error = %e, "这一轮的检查点没记下来");
            }
        }
        Err(reason) => {
            tracing::debug!(session_id, turn, %reason, "这一轮的检查点拍不下来");
        }
    }
}

/// 会话开头那张快照与这一轮的第一道命令之间的闸。
///
/// 拍快照那一趟在开跑时就被派出去（见 `turn` 里那一段），这一层包在工具面的
/// 执行器外面：不碰树的调用原样透传，第一次真要动树时才等那一趟走完。
/// 不变量因此没变——命令仍然排在那张快照之后；变的是**回答不排在它后面**。
struct OpeningSnapshot {
    inner: Arc<dyn cog_core::SandboxBackend>,
    /// 那一趟走完了没有。拍不下来也算走完：它的出路是记下「这个会话没有
    /// 开头快照」，不是把这一轮的命令永远扣住。
    landed: tokio::sync::watch::Receiver<bool>,
}

impl OpeningSnapshot {
    /// 等那一趟走完。已经有结论的闸不再等：第一次读到的就是 `true`。
    async fn settled(&self) {
        let mut landed = self.landed.clone();
        // 发送端随那一趟一起丢掉（任务 panic 也一样）：那时闸跟着消失，命令
        // 照过——一张拍不下来的快照不该变成一张永远不落地的门。
        while !*landed.borrow() && landed.changed().await.is_ok() {}
    }
}

#[async_trait::async_trait]
impl cog_core::SandboxBackend for OpeningSnapshot {
    async fn execute(&self, req: &cog_core::SandboxRequest) -> SFResult<cog_core::SandboxResult> {
        self.settled().await;
        self.inner.execute(req).await
    }

    async fn execute_stream(
        &self,
        req: &cog_core::SandboxRequest,
    ) -> SFResult<cog_core::CommandEventStream> {
        self.settled().await;
        self.inner.execute_stream(req).await
    }

    // 下面三问不碰树：队列、边界、预编译照当时的样子答，不排在快照后面。
    async fn boundary(&self) -> SFResult<cog_core::SandboxBoundary> {
        self.inner.boundary().await
    }

    async fn lane(&self) -> SFResult<cog_core::BuildLaneReading> {
        self.inner.lane().await
    }

    async fn precompile(&self, bytes: &[u8]) -> SFResult<String> {
        self.inner.precompile(bytes).await
    }
}

/// 这一轮那句话是从哪儿来的。
///
/// 名字是哈希出来的，读的人从名字上分不出人跟探针。来源是在握手时客户端自报的，
/// 它不是身份——真正的身份是认证主体；它回答的是「这一行是谁开出来的会话」。
#[derive(Debug, Clone, Default)]
pub(crate) struct PersonOrigin {
    pub platform: Option<String>,
    pub device_id: Option<String>,
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
    origin: &PersonOrigin,
    now: chrono::DateTime<chrono::Utc>,
) -> cog_core::SFResult<()> {
    let previous = match kv
        .hget(cog_gateway::agents::PEOPLE_KEY, &employee.id)
        .await?
    {
        Some(text) => serde_json::from_str::<cog_gateway::agents::PersonRow>(&text).ok(),
        None => None,
    };
    let first_seen = previous.as_ref().map(|row| row.first_seen).unwrap_or(now);
    let row = cog_gateway::agents::PersonRow {
        session_id: session_id.to_string(),
        display_name: employee.handle.clone(),
        first_seen,
        last_seen: now,
        // 来源只在新的一轮说得出口时覆盖：这一轮的客户端声明了它是谁，就用它；
        // 没声明就留着上一轮记下的那一格。没有这一层，一个老客户端接上来的
        // 第一句话就会把「web」擦成「不知道」。
        platform: origin
            .platform
            .clone()
            .or_else(|| previous.as_ref().and_then(|row| row.platform.clone())),
        device_id: origin
            .device_id
            .clone()
            .or_else(|| previous.as_ref().and_then(|row| row.device_id.clone())),
    };
    let text = serde_json::to_string(&row)?;
    kv.hset(cog_gateway::agents::PEOPLE_KEY, &employee.id, &text)
        .await
}

/// 屏幕上那一问里「它要拿什么动手」这一行最多多长。
///
/// 界的是这一行的体积，不是参数能有多大：真正拦住大参数的是工具自己的上限，
/// 这里只保证一条询问不会把整份文件正文塞进对话框。
// cap: fixed — 一行摘要的长度，界的是问句在屏幕上的体积
// keep: 删掉后一行摘要跟着问句长度走，屏幕上放不下；由「界的是问句在屏幕上的体积」兜住

/// 把一次调用的参数压成一行：`command` / `path` 这类最要紧的那一格先说，
/// 那样屏幕上第一眼看到的就是它要动什么。
fn permission_summary(arguments: &serde_json::Value) -> String {
    let text = match arguments {
        serde_json::Value::Object(map) => {
            const LEAD: [&str; 4] = ["command", "path", "file_path", "pattern"];
            let mut parts: Vec<String> = Vec::new();
            for key in LEAD {
                if let Some(value) = map.get(key).and_then(|v| v.as_str()) {
                    parts.push(format!("{key}={value}"));
                }
            }
            if parts.is_empty() {
                serde_json::Value::Object(map.clone()).to_string()
            } else {
                parts.join("  ")
            }
        }
        other => other.to_string(),
    };
    // 这一行是**批准前的摘要**：属主按它点头。截断等于让人批准自己没看到的
    // 那部分，按「有损默认无损」整段交出去；要短的是渲染，不是它。
    text
}

/// 把内核的「问一句」接到这条连接上。
///
/// 内核只知道有人会被问；这一层知道问的是这条 socket，答的是坐在它后面的人。
/// 问之前先看这个人留下来的规则——他上一次已经答过的那一类，不该再问第二遍；
/// 而这一次答完，若他要求记住，规则就写进存储，下一段对话照样算数。
///
/// 顺序是：模式先定（勾了 bypass 就是不要问了，这是比一条旧规则更晚、更明确的表态），
/// 规则其次，都没有才问人。规则与模式的结论都原样返回，内核据此决定执行还是把理由
/// 交回模型。
struct ConversationPermissions {
    bridge: Arc<dyn cog_gateway::chat::PermissionBridge>,
    session_id: String,
    /// 这一段对话属于谁。没有登录身份时是 `None`：那时「总是」只能记成本会话的。
    owner: Option<String>,
    /// 规则住在哪儿。没装 KV 时规则只在这一轮里生效，读不回来也撤不掉——界面上那份
    /// 名单会是空的，而「空名单」正是「没地方存」在这个平台上的样子。
    store: Option<Arc<dyn cog_core::KeyValueStore>>,
    /// 这一段对话现在生效的规则：这一轮的，加上这个人的。进来时读一遍，之后在内存里
    /// 跟着「总是」长，同一个回合里的下一次同类调用就不再问了。
    rules: std::sync::Mutex<cog_agent::permission::PermissionRuleSet>,
}

impl ConversationPermissions {
    /// 记下这次答案。先落内存（同一个回合立即生效），再落存储（跨回合、跨重启）。
    async fn remember(&self, ask: &cog_agent::permission::PermissionAsk, allow: bool) {
        let scope = match &self.owner {
            Some(_) => cog_agent::permission::PermissionScope::Owner,
            None => cog_agent::permission::PermissionScope::Session {
                session_id: self.session_id.clone(),
            },
        };
        let rule = cog_agent::permission::PermissionRule {
            tool: ask.tool.clone(),
            // 记的是这一次要碰的那个东西：下次同一个工具碰同一个东西不再问，
            // 碰别的东西照问。「记住这个工具的一切」是另一条规则（不带对象），
            // 界面不提供它——一次点头不该扩成一张网。
            subject: cog_agent::permission::ask_subject(&ask.arguments).map(str::to_string),
            allow,
            scope,
        };
        self.rules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(rule.clone());
        if let Some(kv) = self.store.as_ref() {
            if let Err(error) =
                crate::permission_rules::remember(kv, self.owner.as_deref(), rule).await
            {
                tracing::warn!(%error, tool = %ask.tool, "这条「总是」没写进存储，重启后不再生效");
            }
        }
    }
}

#[async_trait::async_trait]
impl cog_agent::permission::PermissionDecider for ConversationPermissions {
    async fn decide(
        &self,
        ask: &cog_agent::permission::PermissionAsk,
    ) -> cog_agent::permission::PermissionVerdict {
        let ruled = self
            .rules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .evaluate(ask, &self.session_id);
        if let Some(verdict) = ruled {
            return verdict;
        }
        let view = cog_gateway::chat::PermissionAskView {
            request_id: format!("perm-{}", uuid::Uuid::new_v4()),
            session_id: self.session_id.clone(),
            tool: ask.tool.clone(),
            summary: permission_summary(&ask.arguments),
            mode: ask.mode.as_str().to_string(),
            // 计划正文原样递出去：这是人要在点「批准」之前读完的那一份，不是摘要。
            plan: (ask.tool == cog_agent::permission::EXIT_PLAN_TOOL)
                .then(|| {
                    ask.arguments
                        .get("plan")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .flatten(),
            // 改动类工具把那一次改动一起递出去：人要答的是「这份 diff 落不落盘」。
            // 折它的是内核那一层（它认识工具的入参），这一层只转手；认不出改动
            // 形状时是 `None`，界面上退回一行摘要而不是一张空白的 diff。
            diff: cog_agent::permission::edit_preview(&ask.tool, &ask.arguments),
        };
        match self.bridge.ask(view).await {
            cog_gateway::chat::PermissionAnswer::Allow { always } => {
                if always {
                    self.remember(ask, true).await;
                }
                cog_agent::permission::PermissionVerdict::Allow
            }
            cog_gateway::chat::PermissionAnswer::Deny { reason, always } => {
                if always {
                    self.remember(ask, false).await;
                }
                cog_agent::permission::PermissionVerdict::Deny { reason }
            }
        }
    }
}

/// 这一段对话跑在多大的窗口里。
///
/// 内核的构造只写了几格，其余取 [`cog_core::RuntimeConfig`] 的默认，窗口正是其中
/// 之一。网关判「这条消息是不是太大了」读的就是这个数（见
/// [`cog_gateway::chat::oversized_threshold_tokens`]），所以两处必须同源：在这里
/// 另写一个数，界面上落成文件的那条线就会与循环真正被撑破的那条线错开。
/// 执行器答回来的那份清单，读成界面上要说的话。
///
/// 读不懂就是失败，不是「没有文件要改」：把一份残缺的答复画成「什么都不用改」，
/// 人会点下那一下确认，而树其实会被改。`applied` 报的是**这次调用要不要改树**，
/// 不是「改成功了没有」——没改成功的那一次会走 `Err`，界面不会看到这一份。
fn rewind_report(
    turn: usize,
    target: usize,
    apply: bool,
    value: &serde_json::Value,
) -> Result<RewindReport, String> {
    let malformed = || format!("执行器答回来的退回清单读不懂：{value}");
    let mut files = Vec::new();
    for file in value
        .get("files")
        .and_then(|files| files.as_array())
        .ok_or_else(malformed)?
    {
        let path = file
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(malformed)?;
        let change = file
            .get("change")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(malformed)?;
        files.push(CheckpointFile {
            path: path.to_string(),
            change: change.to_string(),
        });
    }
    let truncated = value
        .get("truncated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Ok(RewindReport {
        turn,
        target,
        applied: apply,
        files,
        truncated,
    })
}

/// 这一段对话真跑在多大的窗口里；`None` = 还没有任何读数（不是零个 token）。
///
/// 读的是进程里那份实测/规格读数——与循环自己裁剪用的是同一个（`AgentRuntime`
/// 也挂在同一个全局观测面上）。没有部署声明的窗口这一路：那个数只能来自型号
/// 公布的规格和实测，写进部署文档就是替上游拍一个它没说过的数。
fn conversation_window_tokens() -> Option<usize> {
    let (window, _) = cog_agent::observable::global_observable().context_window_decision_for(None);
    window
}

/// 窗口那条界：`usize::MAX` 在 `oversized_threshold_tokens` 里等于「这条线不设」。
///
/// 没人说过窗口时只能这样答——这时唯一真实的界是一帧能带多少字节，拿一个替上游
/// 拍出来的窗口去判「这句话要不要落成文件」，判的是没人量过的数。
fn conversation_context_window_tokens() -> usize {
    conversation_window_tokens().unwrap_or(usize::MAX)
}

impl ConversationTurn for PlatformConversation {
    fn context_window_tokens(&self) -> usize {
        conversation_context_window_tokens()
    }

    /// 这一段选过哪只模型。没接键值存储时读作「没选过」——那个部署上确实没有
    /// 选择可以留下来，答一个默认名字就是在替人做决定。键值存储接了但读失败是
    /// `Err`：那是另一件事，界面要说得出来。
    fn chosen_model<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<String>, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Ok(None);
            };
            crate::session_model::load(kv, owner, session_id).await
        })
    }

    fn choose_model<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        model: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<String>, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Err(
                    "这个部署没有接键值存储，选不了模型：选择没地方记下来，下一轮就会读不到它。"
                        .into(),
                );
            };
            crate::session_model::save(kv, owner, session_id, model).await
        })
    }

    fn checkpoints<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<CheckpointList, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Err("这个部署没有接检查点存储，列不出检查点。".into());
            };
            let rows = crate::checkpoints::load(kv, owner, session_id).await?;
            if rows.rows.is_empty() {
                // 一份都没有：要么还没跑完一轮，要么当时的树拍不下来。后者的
                // 理由存在 `unavailable` 里——两句话分开说，人才知道自己该
                // 再跑一轮还是换一个部署。
                if let Some(reason) = rows.unavailable {
                    return Err(reason);
                }
                return Ok(CheckpointList::default());
            }
            let hidden = rows
                .rows
                .len()
                .saturating_sub(crate::checkpoints::MAX_LISTED_CHECKPOINTS);
            let rows = rows
                .rows
                .into_iter()
                .skip(hidden)
                .map(|row| CheckpointView {
                    key: row.key,
                    turn: row.turn,
                    at: row.at,
                })
                .collect();
            Ok(CheckpointList { rows, hidden })
        })
    }

    fn rewind<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        turn: usize,
        apply: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RewindReport, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let Some(sandbox) = self.sandbox.as_ref() else {
                return Err("这个部署没有接执行器，读不到这棵树，退不回去。".into());
            };
            let Some(kv) = self.people.as_ref() else {
                return Err("这个部署没有接检查点存储，退不回去。".into());
            };
            let Some(target) = crate::checkpoints::rewind_target(turn) else {
                return Err("会话开头之前没有可退的地方。".into());
            };
            let rows = crate::checkpoints::load(kv, owner, session_id).await?;
            if !rows.rows.iter().any(|row| row.turn == target) {
                return Err(format!(
                    "这一段对话没有第 {target} 轮的检查点（退回到第 {turn} 轮之前要的就是它），退不回去。"
                ));
            }
            let key = crate::checkpoints::key(session_id, target);
            let employee = self.employee_here(Some(owner), session_id).await;
            let value = if apply {
                crate::checkpoints::restore(sandbox, &employee.id, session_id, &key).await?
            } else {
                crate::checkpoints::plan(sandbox, &employee.id, session_id, &key).await?
            };
            let report = rewind_report(turn, target, apply, &value)?;
            if apply {
                // 退回去之后，后面那几轮的快照不再对应这棵树：它们的改动已经被
                // 丢掉了。留着名字，下一次退回时会有人挑到一份「退到这里也一样」，
                // 而它其实指向一段已经不存在的历史。
                crate::checkpoints::forget_after(kv, owner, session_id, target).await?;
            }
            Ok(report)
        })
    }

    fn sandbox_boundary<'a>(
        &'a self,
        _owner: &'a str,
        _session_id: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<cog_core::SandboxBoundary, String>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            // 边界由跑命令的那一层自报：独立执行器就答执行器的，进程内就答进程内
            // 的。装配层不替任何一层拼一份「安全」描述——那会是一句由没跑过这条
            // 命令的人写下的话，改部署时也不会跟着变。
            let Some(sandbox) = self.sandbox.as_ref() else {
                return Err("这个部署没有接执行器，说不清命令跑在哪里。".into());
            };
            sandbox
                .boundary()
                .await
                .map_err(|error| format!("执行器没有答出自己的边界：{error}"))
        })
    }

    fn turn<'a>(
        &'a self,
        request: ConversationRequest,
        events: tokio::sync::mpsc::Sender<AgentEvent>,
    ) -> Pin<Box<dyn Future<Output = SFResult<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            // 这一段对话是谁：绑在登录身份上的人，跨会话、跨轮、跨实例不变。
            // 派活与记账都按这个名字。没有绑定表（进程没装 KV）时退回按会话
            // 算出来的那个名字。
            let owner = Some(request.owner.as_str()).filter(|owner| !owner.trim().is_empty());
            let employee = self.employee_here(owner, &request.session_id).await;
            let mut intent_identity = None;
            if let Some(model_text) = last_user_text(&request.messages) {
                // 原话优先取请求带进来的那一格（未展开）；没有就退回模型读到的那段
                // ——旧调用方没有这一格，那时两者本来就是同一段。
                let original = request
                    .original_text
                    .as_deref()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| model_text.clone());
                let expansion = (original != model_text).then_some(model_text.as_str());
                let arrival = request
                    .arrival_id
                    .clone()
                    .unwrap_or_else(|| model_text.clone());
                let row = chat_intent_row(
                    &request.session_id,
                    request.owner.as_str(),
                    &employee.id,
                    &arrival,
                    &original,
                    expansion,
                );
                intent_identity = Some(row.identity.clone());
                cog_gateway::intent::record(self.intent_events.as_ref(), row).await;
            }
            // 先把他记进「说过话的人」名册：注册表上的心跳在他闭嘴几十秒后就
            // 过期，而人要找的是「那个跟我聊过的人」。写不进去不是这一轮的
            // 失败——他照样能说话，只是名单上从此少一个人；这一句是那个差别的
            // 唯一读数。
            //
            // 没有登录身份的连接不写这一行：名册要回答「这个人是谁」，而
            // 一条没有身份的连接答不出这一问。给它记一行，名单上就多一个
            // 没有属主的员工，读的人分不出那是「有人坐在这儿」还是「一个探针
            // 开了一段会话、说了一句、再也没回来」——2026-11-06 名册上那 29 行
            // 全是后者。
            if let Some(kv) = self.people.as_ref() {
                if owner.is_none() {
                    tracing::debug!(
                        session_id = %request.session_id,
                        "a connection without a login identity said nothing to the roster: \
                         the row would name an employee nobody can ask about"
                    );
                } else {
                    let origin = PersonOrigin {
                        platform: request.platform.clone(),
                        device_id: request.device_id.clone(),
                    };
                    if let Err(e) = note_person(
                        &**kv,
                        &employee,
                        &request.session_id,
                        &origin,
                        chrono::Utc::now(),
                    )
                    .await
                    {
                        tracing::warn!(
                            agent_id = %employee.id,
                            error = %e,
                            "这位员工没有记进名册：他在名单上不会出现"
                        );
                    }
                }
            }
            // 窗口的起点取自 LLM 配置里那只型号公布的规格，而配置可以在两轮之间
            // 被改：开场先问一次池状态，`cog_core::model_windows` 由那条读数刷新，
            // 下面那句读数才是这一轮真正跑的窗口。问不到不是失败——沿用上一份，
            // 第一次超窗时由实测补上。
            if let Err(e) = cog_gateway::llm_admin::refresh_model_windows().await {
                tracing::debug!(
                    error = %e,
                    "这一轮没读到 LLM 配置里的型号窗口，沿用上一份读数"
                );
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
                // 窗口由上面那个读数给出，不在这里另写一次：网关拿它判「这条消息
                // 是不是太大了」，判的必须是这一轮真正跑在的那个窗口。没人说过时
                // 就是 `None`，不写一个假的数——`usize::MAX` 会在账上、日志里读成
                // 一个 1.8e19 的窗口，0 会被读成「零个 token 的窗口」。
                context_window_size: conversation_window_tokens(),
                ..Default::default()
            };
            // 会话开头的那棵树：只在第一次进到这里时真的拍（之后读一行 KV 就
            // 知道已经有了）。
            //
            // 这一趟派出去跑，不在这里等。它是附加能力，拍不下来时这一趟自己
            // 会把「这个会话没有开头快照」记进账（`mark_unavailable`）；把它放在
            // 回答的必经之路上，等到的只是执行器换版的那整个重试窗口——2026-11-24
            // 实测一轮因此停在 `message.start` 上 3 分 26 秒，最后落的还是一句
            // 与快照毫无关系的上游拒绝。
            //
            // 但它仍要落在这一轮动手之前：放到开跑之后，拍到的就是这一轮改了
            // 一半的树，读数上却还叫「会话开头」。所以闸装在**命令**那一侧
            // （见 `OpeningSnapshot`）：不碰树的回答不等它，要动树的时候才等。
            let opening = match (owner, self.sandbox.as_ref(), self.people.as_ref()) {
                (Some(owner_name), Some(sandbox), Some(kv)) => {
                    let (landed_tx, landed) = tokio::sync::watch::channel(false);
                    let who = owner_name.to_string();
                    let session = request.session_id.clone();
                    let agent = employee.id.clone();
                    let capturing = Arc::clone(sandbox);
                    let kv = Arc::clone(kv);
                    tokio::spawn(async move {
                        open_checkpoints(capturing, kv, who, session, agent).await;
                        // 拍到了、拍不下来、这个会话本来就有开头快照，都是这一趟
                        // 走完了：闸放下，命令照常过。
                        cog_core::ignore_lost!(landed_tx.send(true));
                    });
                    Some((landed, Arc::clone(sandbox)))
                }
                _ => None,
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
            let mut tools = match self.tools.as_ref() {
                Some(shared) => shared.fork(),
                None => cog_agent::ToolRegistry::new(),
            };
            // 命令那一侧的闸：第一次真要动树的时候，先等会话开头那张快照。
            // 子任务复制的是这张已经上过闸的表（`tools.fork()`），所以子任务
            // 的第一道命令排的也是同一张快照之后。
            if let Some((landed, sandbox)) = opening {
                tools.set_sandbox_backend(Arc::new(OpeningSnapshot {
                    inner: sandbox,
                    landed,
                }));
            }
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
            // 规则账：员工提案，属主拍板。拍板的两只只在**这一轮有登录身份**时
            // 注册——没有人的连接批准不了任何东西，否则账上会多出一行「由一位
            // 员工替属主做的决定」。提案那只对每个员工都在，缺账时它如实说
            // 「读不到」，而不是假装提案成功。
            for tool in principle_tools(
                self.principles.clone(),
                employee.id.clone(),
                owner.is_some(),
            ) {
                tools.register(tool);
            }
            // 方向盘：定档、把账上一行交出去、放行与回滚、说一句「什么算好」。
            // 五只与上面那几组同一处装。定档、拍板与说判据只在有登录身份时注册
            // （写进去的是「谁说的」）；交出去与派活同级，任何一段对话都能做，
            // 判据仍然取账上那一行。
            for tool in change_direction_tools(
                self.ledgers.changes.clone(),
                self.orchestrator.clone(),
                self.evolution.clone(),
                self.admission.clone(),
                self.audit.clone(),
                self.taste.clone(),
                owner.map(str::to_string),
                employee.id.clone(),
            ) {
                tools.register(tool);
            }
            // 外部 MCP 服务：这一轮问一次它有哪些工具，逐只装进来。每轮问一次
            // 而不是启动时缓存：远端那张表是会变的，缓存一份等于把「今天有
            // 什么」钉在进程启动的那一刻。问不到时就一只也没有——这一轮如实
            // 少掉它们，而不是拿昨天的表冒充今天的。
            let mcp_report = match self.mcp.as_ref() {
                Some(client) => {
                    let listed = crate::mcp_tools::tools(Arc::clone(client)).await;
                    for tool in &listed.tools {
                        tools.register(tool.clone());
                    }
                    Some(listed.report())
                }
                None => None,
            };
            // 名册上「它能做什么」就是这一轮手上真有的工具，不是一段标签。
            // 这里曾经填的是它自己的 id：能力字段里装着一个名字，读名册的人
            // 看到的是「mira 的能力是 mira」，按能力找人的那一步什么也筛不出来。
            // 员工是通用智能体：它没有预设，有的只是这个进程给它的工具；具体
            // 做哪件活，由拿到的那句话决定。
            // 动手之前先问人。通道来自这条连接（网页），模式来自这一段对话当前
            // 那一格；名字不认识时落到 `default`——比「不问」严、比「只读」松，
            // 一次手误不该让这一轮问得更少。没有通道的连接不是「不问」，而是
            // 需要问的那一步拒绝执行（见 `permission::decide`）。
            let permission_mode =
                cog_agent::permission::PermissionMode::parse(&request.permission_mode)
                    .unwrap_or(Default::default());
            // 判定者先于工具表装好：子任务要用同一个判定者，而工具得在
            // `with_tools` 之前进表。晚一步建出来，子任务就只能自己造一个
            // 「不问」的判定者——那等于用一次分解绕开了人的决定。
            let permission_session = request.session_id.clone();
            let decider: Option<Arc<dyn cog_agent::permission::PermissionDecider>> =
                match request.permissions {
                    Some(bridge) => {
                        let owner_name = owner.map(str::to_string);
                        let rules = match self.people.as_ref() {
                            Some(kv) => {
                                crate::permission_rules::load(
                                    kv,
                                    owner_name.as_deref(),
                                    &permission_session,
                                )
                                .await
                            }
                            None => cog_agent::permission::PermissionRuleSet::default(),
                        };
                        Some(Arc::new(ConversationPermissions {
                            bridge,
                            session_id: permission_session,
                            owner: owner_name,
                            store: self.people.clone(),
                            rules: std::sync::Mutex::new(rules),
                        })
                            as Arc<dyn cog_agent::permission::PermissionDecider>)
                    }
                    None => None,
                };
            // plan 模式下多一只「把计划递出去请人批准」：它是这个模式的唯一出口，
            // 而别的模式里没有待批的计划，挂上去只会让模型多试一次拿不到结果的调用。
            if permission_mode == cog_agent::permission::PermissionMode::Plan {
                tools.register(cog_agent::tools::builtins::exit_plan_mode());
            }
            // 子智能体：这一次调用内部分解出去的一条独立上下文。工具表在**这一刻**
            // 复制一份给子任务，然后才把这只会派子任务的工具装进父表——子任务手上
            // 没有它自己，所以父子深度就是一。配置与权限都跟这一轮走：同一个窗口、
            // 同一个判定者、同一个模式。
            let child_tools = tools.fork();
            tools.register(cog_agent::tools::builtins::spawn_subagent(
                cog_agent::tools::builtins::SubagentWiring {
                    llm: Some(Arc::clone(&request.llm)),
                    template: config.clone(),
                    tools: Some(Arc::new(child_tools)),
                    decider: decider.clone(),
                    mode: permission_mode,
                    // 子任务不是新的人：它读到的是父这一行，花销记在同一段会话名下。
                    roster: self.roster.clone(),
                    owner: request.owner.clone(),
                    session: request.session_id.clone(),
                    files_seen: None,
                },
            ));
            // 判据审是另一次调用，但它读的是同一份工具面：fork 一份，读树
            // 用 read_file / command，判者只回答「这段终态在树上立不立得住」。
            // 判据审只拿读的几只：它要读树，不该派活或改树。
            let eval_tools = tools.read_only();
            let mut runtime = cog_agent::AgentRuntime::new(config, events).with_tools(tools);
            // 属主写下的约定：只在这条连接既是某个登录身份、又接上了键值存储时
            // 才挂读面。没有身份就没有「属主」，读不到任何一个人的话——不挂，
            // 而不是挂一个空桶：后者会让界面上出现一行「加载了 0 条」，读起来
            // 像「这个人还没写过」，而事实是这条连接问不出「这个人」是谁。
            if let (Some(kv), Some(owner)) = (self.people.as_ref(), owner) {
                runtime = runtime.with_memory_source(Arc::new(
                    crate::owner_memory::OwnerMemoryBucket::new(Arc::clone(kv), owner),
                ));
            }
            // 属主配的钩子。同一张配置表分两路：判定型（阻断 / 改写）挂在工具
            // 动手之前由这一侧回答，通知型交给发布引擎。发布引擎自己会跳过判定
            // 型（`HookAction::is_guard`）——一个动作只有一个执行者，否则一次
            // 调用会被拒两次，读数里出现两条只实际发生了一条的记录。
            let hook_defs: Vec<cog_core::HookDef> = match self.hooks.as_ref() {
                Some(engine) => engine.list_hooks().await,
                None => Vec::new(),
            };
            let hook_runs = Arc::new(cog_agent::hooks::guard::Runs::default());
            // 这一轮真的发给了哪些同事、每一条的结局。工具路径（`send_to_agent`）
            // 发出去的话在发起方这一侧只剩一行日志，人翻不到；收在这里随交付物
            // 回去，界面上才有「谁发给谁、说了什么、结局如何」这一行。
            //
            // 挂在 `after_tool_call` 而不是工具自己身上：工具拿不到「这一段对话」
            // 这个概念——它只知道收件人地址；而这一轮把哪只工具跑成什么样，循环
            // 已经在这里交出来了，包括被拒的那一次（`Err`）。
            let colleague_messages: Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            // 这一轮挂到共享任务图上的活。任务图是账本，对话是一种读面：没有这一格，
            // 人就只能离开这段对话去工作台找「我刚让它办的那件事」，而两处读的本来
            // 就是同一张表（`get_all_tasks`），没有第二个出处可言。
            let filed_tasks: Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            {
                let rules = cog_agent::hooks::guard::rules(&hook_defs);
                let guard = if rules.is_empty() {
                    None
                } else {
                    Some(cog_agent::hooks::guard::before_tool_call(
                        rules,
                        Arc::clone(&hook_runs),
                    ))
                };
                let before_engine = self.hooks.clone();
                let after_engine = self.hooks.clone();
                let after_log = Arc::clone(&colleague_messages);
                let filed_log = Arc::clone(&filed_tasks);
                let intent_for_hook = intent_identity.clone();
                runtime = runtime.with_hooks(cog_agent::AgentHooks {
                    before_tool_call: Some(Arc::new(
                        move |tool: &str, arguments: &mut serde_json::Value| {
                            // 这条活是哪一句意图交出来的：工具自己不知道，
                            // 这一轮知道。挂在参数上，任务行与意图因此由同一个
                            // 身份连起来，不靠事后按时间猜。
                            if tool == "start_task" {
                                if let (Some(intent), Some(object)) =
                                    (intent_for_hook.as_deref(), arguments.as_object_mut())
                                {
                                    object
                                        .insert("intent_id".to_string(), serde_json::json!(intent));
                                }
                            }
                            // 观测型钩子（log / notify / webhook）也要看见这次
                            // 调用：事件先发出去，判定再回答。两件事的顺序不
                            // 影响结果，但先发出去意味着被拒的调用也在账上。
                            if let Some(engine) = before_engine.as_ref() {
                                engine.emit_detached(
                                    cog_core::HookEvent::new(cog_core::HookTrigger::OnToolBefore)
                                        .with_payload(serde_json::json!({
                                            "tool": tool,
                                            "arguments": arguments.clone(),
                                        })),
                                );
                            }
                            match guard.as_ref() {
                                Some(guard) => guard(tool, arguments),
                                None => Ok(()),
                            }
                        },
                    )),
                    after_tool_call: Some(Arc::new(
                        move |tool: &str,
                              arguments: &serde_json::Value,
                              result: &SFResult<serde_json::Value>| {
                            if let Some(engine) = after_engine.as_ref() {
                                engine.emit_detached(
                                    cog_core::HookEvent::new(cog_core::HookTrigger::OnToolAfter)
                                        .with_payload(serde_json::json!({
                                            "tool": tool,
                                            "arguments": arguments,
                                            "ok": result.is_ok(),
                                        })),
                                );
                            }
                            if tool == "send_to_agent" {
                                let record = colleague_message(arguments, result);
                                match after_log.lock() {
                                    Ok(mut log) => log.push(record),
                                    Err(poisoned) => poisoned.into_inner().push(record),
                                }
                            }
                            if tool == "start_task" {
                                if let Some(row) = task_list(arguments, result) {
                                    match filed_log.lock() {
                                        Ok(mut log) => log.push(row),
                                        Err(poisoned) => poisoned.into_inner().push(row),
                                    }
                                }
                            }
                        },
                    )),
                    transform_context: None,
                });
            }
            runtime = runtime.with_permissions(decider, permission_mode);
            // 名册交给运行时：它在每轮开头读到自己那一行的工号与名字，写进系统
            // 前缀。这里不做这件事——构造点不止这一处，漏一处那一种执行体就
            // 不知道自己的名字，而漏掉不会报错，只会回答「我是 unknown」。
            if let Some(roster) = self.roster.as_ref() {
                runtime = runtime.with_roster(roster.clone());
            }
            // 没有身份的连接不声明人：编一个名字会读成真有人花过这笔钱。
            let runtime = runtime.with_call_identity(
                request.owner.clone(),
                cog_core::LlmOrigin::Interactive.as_str(),
            );
            // 这一轮的每一发都记在这段对话名下：界面上的「这段会话花了多少」是账本
            // 按这一列折叠出来的，不挂这一格，那几发的 token 只数得进按人 / 按
            // 环境的总数，屏幕上一段会话的合计就会比它下面每一轮加起来少。
            let runtime = runtime.with_call_session(request.session_id.clone());
            // 这一段选的模型：只挂在这一轮的每一发上。没选过时是 `None`，走上游
            // 配置的那一只——不是在这里另写一个默认名。
            let mut runtime = runtime.with_model(request.model.clone());
            if let Some(causation) = intent_identity.as_deref().map(cog_core::intent_event_id) {
                runtime = runtime.with_call_causation(causation);
            }

            // 开跑前先读收件箱：同事在它不在的时候发来的话，这一轮要看得见。
            // 读完就确认，下一轮只读新的；回信地址跟着消息一起带进来，等这一轮
            // 有了答案再投回去。
            let mut askers: Vec<String> = Vec::new();
            let mut history: Vec<cog_core::Message> = Vec::new();
            // 同事发来的话也是意图：谁说的、说给谁、原话。它们走收件箱进来，
            // 不经过 WebUI 那条入口，但进的是同一本意图账——否则「所有输入先
            // 成为一条意图」只对属主成立。这一轮把它们一并接手，下一批分诊
            // 因此不会把已经答过的话再捡一遍。
            let mut inbox_intents: Vec<String> = Vec::new();
            if let Some(backend) = self.inbox.as_ref() {
                for msg in drain_inbox(backend, &employee.id).await {
                    if let cog_core::InboxMessage::Prompt {
                        reply_stream: Some(stream),
                        ..
                    } = &msg
                    {
                        askers.push(stream.clone());
                    }
                    if let Some((said_by, text)) = inbox_intent(&msg) {
                        let row = cog_core::IntentRow::new(
                            "internal",
                            cog_core::intent_identity(
                                "internal",
                                &format!("{said_by}:{}:{text}", employee.id),
                            ),
                            said_by,
                            employee.id.clone(),
                            text,
                            None,
                        );
                        inbox_intents.push(row.identity.clone());
                        cog_gateway::intent::record(self.intent_events.as_ref(), row).await;
                    }
                    if let Some(text) = inbox_as_message(&msg) {
                        history.push(cog_core::Message::user(text));
                    }
                }
            }
            // 这一轮属主说的那句话：判据审拿它当「是不是意图要的那件事」的尺子，
            // 也是 `/task`、`/chat` 这两个显式开关读的那一格。历史随后就被搬进
            // 内核，这里先把这一句留下。
            let turn_text = last_user_text(&request.messages).unwrap_or(Default::default());
            // 这一句放在历史最前面：模型在回答这一轮时，手里同时有「这是谁」
            // 与「什么算活、活要带哪三格」。不调一个分类器，选择调不调
            // `start_task` 就是分类；属主写了 `/task` 或 `/chat` 时，那句话
            // 已经替模型判过，不再让它猜。
            history.push(cog_core::Message::system(intent_instruction(&turn_text)));
            history.extend(request.messages);

            // 说话的这一段时间它在名册上活着：续约间隔比名册的过期短，否则一轮
            // 长对话会在自己还在说的时候从名册上消失。
            let lease = match self.roster.as_ref() {
                Some(roster) => Some(EmployeeLease::take(roster.clone(), &employee).await),
                None => None,
            };

            let mut answer = runtime
                .run_conversation(history, request.llm.as_ref())
                .await;
            // 判据审：另一次调用，读这一轮的工作树，判三格立不立得住。不成立
            // 的任务当场撤回；判词随这一轮的交付物回给属主，不把「写歪了」
            // 留给执行者去撞。
            let review_items: Vec<(
                String,
                cog_core::contract::ledger_item::LedgerItem,
                Option<String>,
            )> = filed_tasks
                .lock()
                .ok()
                .map(|log| {
                    log.iter()
                        .filter_map(|row| {
                            let task_id = row.get("task_id")?.as_str()?.to_string();
                            let item = row.get("ledger_item")?.clone();
                            let definition = row
                                .get("definition")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string);
                            serde_json::from_value(item)
                                .ok()
                                .map(|item| (task_id, item, definition))
                        })
                        .collect()
                })
                .unwrap_or(Default::default());
            if !review_items.is_empty() {
                let items: Vec<cog_core::contract::ledger_item::LedgerItem> = review_items
                    .iter()
                    .map(|(_, item, _)| item.clone())
                    .collect();
                // 这一轮定下的「这一类是什么、怎样算做完」：判据审按定义判断。
                let defined = review_items
                    .iter()
                    .find_map(|(_, _, definition)| definition.clone());
                let definition = defined.as_deref();
                let eval_config = RuntimeConfig {
                    agent_id: format!("criterion-review:{}", request.session_id),
                    role: cog_core::roles::EVALUATOR.to_string(),
                    ..Default::default()
                };
                let eval_agent: Arc<dyn cog_core::Agent> = Arc::new(
                    cog_agent::Agent::new(eval_config, Arc::clone(&request.llm))
                        .with_tools(eval_tools),
                );
                let evaluator = cog_collaboration::actors::EvaluatorActor::new(eval_agent);
                let verdicts = evaluator
                    .review_criteria(&turn_text, &items, definition)
                    .await;
                let mut attached = Vec::new();
                for ((task_id, _, _), verdict) in review_items.iter().zip(verdicts.iter()) {
                    if !verdict.ok {
                        let _ = self
                            .orchestrator
                            .cancel_task_with_reason(task_id, verdict.reason.clone())
                            .await;
                    }
                    attached.push(verdict.clone());
                }
                if let Ok(value) = answer.as_mut() {
                    if let Some(object) = value.as_object_mut() {
                        object.insert(
                            "intent_review".to_string(),
                            serde_json::to_value(&attached).unwrap_or_default(),
                        );
                    }
                }
            }

            // 这一句意图的下场：这一轮如果真把活交给了任务图，意图挂到那条
            // 任务上；没有交，就是「这一句只是话」。两条都写一条接手事件，
            // 否则它会被下一批分诊重新捡起来，而那一次分诊没有这一轮的对话。
            //
            // 空 task_id 是「没有任务」的读数，不是一条编出来的任务号：读面按
            // 事件的存在判「有人接手过」，不按 task_id 去反查一条不存在的行。
            if let Some(sink) = self.intent_events.as_ref() {
                let owner_identity = intent_identity.clone();
                let mut handled = inbox_intents;
                if let Some(identity) = intent_identity {
                    handled.push(identity);
                }
                if !handled.is_empty() {
                    let filed = filed_tasks.lock().ok().and_then(|log| {
                        log.first().and_then(|row| {
                            row.get("task_id")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string)
                        })
                    });
                    let (task_id, actor) = match filed {
                        Some(task_id) => (task_id, "conversation"),
                        None => (String::new(), "conversation:no-work"),
                    };
                    for identity in handled {
                        // 漏判兜底：这一轮没派活，而属主那句带着「可能是一件活」的
                        // 动词——不标接手，留给下一批分诊带着原文重判一次。预筛只
                        // 决定「要不要留给人再看」，不决定它是不是活。
                        if task_id.is_empty()
                            && owner_identity.as_deref() == Some(identity.as_str())
                            && may_be_work(&turn_text)
                        {
                            continue;
                        }
                        let events = cog_core::intent_triaged_events(
                            &[identity.clone()],
                            &task_id,
                            actor,
                            chrono::Utc::now(),
                        );
                        let mut failure = None;
                        for event in events {
                            if let Err(e) = sink.append(event).await {
                                failure = Some(e);
                                break;
                            }
                        }
                        if let Some(e) = failure {
                            tracing::warn!(
                                identity = %identity,
                                error = %e,
                                "this turn's intent was not marked; the next batch may read it again"
                            );
                        }
                    }
                }
            }
            // 挂没挂外部服务、这一次连上没有、问到了几只工具：随这一轮的交付物
            // 一起回去。没有端点的部署没有这一格，界面上也就没有这一行——「没挂」
            // 与「挂了但连不上」是两句不同的话。
            if let (Some(report), Ok(value)) = (mcp_report, answer.as_mut()) {
                if let Some(object) = value.as_object_mut() {
                    object.insert("mcp".to_string(), report);
                }
            }
            // 钩子的读数：配了哪些、这一轮哪几条真的跑过。一轮结束也是「停止」
            // 那个触发点——这一轮不会再有下一次工具调用，也不会有下一次模型
            // 调用，此刻发出去的通知就是这一轮的收尾。
            if let (Some(engine), Ok(value)) = (self.hooks.as_ref(), answer.as_mut()) {
                engine.emit_detached(
                    cog_core::HookEvent::new(cog_core::HookTrigger::OnAgentEnd)
                        .with_agent_id(employee.id.clone())
                        .with_payload(serde_json::json!({ "session_id": request.session_id })),
                );
                if let Some(object) = value.as_object_mut() {
                    object.insert(
                        "hooks".to_string(),
                        cog_agent::hooks::guard::report(&hook_defs, &hook_runs),
                    );
                }
            }
            // 这一轮发给同事的每一条。只在真发过时挂这一格：没有它，界面上就是
            // 「这一轮没跟同事说过话」，而不是「这个平台不做这件事」。
            if let Ok(value) = answer.as_mut() {
                if let Some(object) = value.as_object_mut() {
                    let sent = match colleague_messages.lock() {
                        Ok(mut log) => std::mem::take(&mut *log),
                        Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
                    };
                    if !sent.is_empty() {
                        object.insert("colleagues".to_string(), serde_json::Value::Array(sent));
                    }
                }
            }
            // 这一轮挂上任务图的活。与同事消息同一个道理：只在真挂过时出现，
            // 「这一轮没派活」与「这个平台不在对话里显示任务」因此分得开。
            if let Ok(value) = answer.as_mut() {
                if let Some(object) = value.as_object_mut() {
                    let filed = match filed_tasks.lock() {
                        Ok(mut log) => std::mem::take(&mut *log),
                        Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
                    };
                    if !filed.is_empty() {
                        object.insert("tasks".to_string(), serde_json::Value::Array(filed));
                    }
                }
            }

            // 说完就下线：名下没有常驻进程，名册上留着一条过期的记录只会让同事
            // 以为它还醒着。
            if let Some(lease) = lease {
                lease.release().await;
            }

            // 这一轮的树：派出去拍，不在这里等。它是附加的一趟远程读盘，等在
            // 交付之前，人看到最后一行字要多等这一趟；而快照宁可晚几秒到，
            // 也不该让回答慢下来。失败已经在这一侧记过日志，这里不再重试。
            if let Some(owner_name) = owner {
                if let (Some(sandbox), Some(kv)) = (self.sandbox.clone(), self.people.clone()) {
                    let session = request.session_id.clone();
                    let agent = employee.id.clone();
                    let who = owner_name.to_string();
                    tokio::spawn(async move {
                        close_checkpoints(sandbox, kv, who, session, agent).await;
                    });
                }
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

    /// 这个人手上还有哪些留下来的规则，这一段的加上他自己的。
    ///
    /// 读不到存储时给一张空名单，与「他名下真的没有规则」在界面上读起来一样——
    /// 这是这个平台的现状：一条规则的可见性取决于存储接没接上，而不取决于它存不存在。
    /// 要分清两者得给这一帧加一个「读没读到」的标记，而眼下唯一会读到的是人自己
    /// 点开的那一眼，多一个标记只是多一格没人会看的字。
    fn permission_rules<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Vec<cog_gateway::chat::PermissionRuleView>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Vec::new();
            };
            let owner = Some(owner).filter(|owner| !owner.trim().is_empty());
            crate::permission_rules::load(kv, owner, session_id)
                .await
                .rules()
                .iter()
                .map(permission_rule_view)
                .collect()
        })
    }

    fn revoke_permission_rule<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        rule_id: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Vec<cog_gateway::chat::PermissionRuleView>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Vec::new();
            };
            let owner = Some(owner).filter(|owner| !owner.trim().is_empty());
            crate::permission_rules::revoke(kv, owner, session_id, rule_id)
                .await
                .rules()
                .iter()
                .map(permission_rule_view)
                .collect()
        })
    }

    /// 这个人写下的约定：下一轮开头会自动进提示词的那几句。
    ///
    /// 读不到存储时给一张空名单，与「这个人还没写过」在界面上读起来一样——
    /// 与规则那一面同一个现状，也同一个取舍：眼下唯一的读者是人自己点开的那
    /// 一眼，多一格「读没读到」只是多一格没人看的字。
    fn owner_memory<'a>(
        &'a self,
        owner: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<MemoryEntryView>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Vec::new();
            };
            let owner = Some(owner).filter(|owner| !owner.trim().is_empty());
            owner_memory_view(crate::owner_memory::list(kv, owner).await)
        })
    }

    fn remember_owner_memory<'a>(
        &'a self,
        owner: &'a str,
        text: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<MemoryEntryView>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Vec::new();
            };
            let owner = Some(owner).filter(|owner| !owner.trim().is_empty());
            match crate::owner_memory::remember(
                kv,
                owner,
                text,
                chrono::Utc::now(),
                conversation_window_tokens(),
            )
            .await
            {
                Ok(entries) => owner_memory_view(entries),
                Err(error) => {
                    // 写不进去要把理由说给写的人：空名单读起来会像「写完就没了」，
                    // 而实际是这一句没有被记下。
                    tracing::warn!(%error, "属主写的这一句约定没有记下来");
                    Vec::new()
                }
            }
        })
    }

    fn forget_owner_memory<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<MemoryEntryView>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(kv) = self.people.as_ref() else {
                return Vec::new();
            };
            let owner = Some(owner).filter(|owner| !owner.trim().is_empty());
            match crate::owner_memory::forget(kv, owner, id).await {
                Ok(entries) => owner_memory_view(entries),
                Err(error) => {
                    tracing::warn!(%error, id, "撤掉这一句约定没写回去");
                    owner_memory_view(crate::owner_memory::list(kv, owner).await)
                }
            }
        })
    }
}

/// 一个人写下的约定，按写下的顺序。
fn owner_memory_view(entries: Vec<cog_agent::memory::MemoryEntry>) -> Vec<MemoryEntryView> {
    entries
        .into_iter()
        .map(|entry| MemoryEntryView {
            id: entry.id,
            text: entry.text,
        })
        .collect()
}

/// 一条规则递到界面上时的样子：人看得懂的四个字，加一个撤销用的号。
fn permission_rule_view(
    rule: &cog_agent::permission::PermissionRule,
) -> cog_gateway::chat::PermissionRuleView {
    cog_gateway::chat::PermissionRuleView {
        id: rule.id(),
        tool: rule.tool.clone(),
        subject: rule.subject.clone(),
        allow: rule.allow,
        scope: match &rule.scope {
            cog_agent::permission::PermissionScope::Owner => "owner",
            cog_agent::permission::PermissionScope::Session { .. } => "session",
        }
        .to_string(),
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
    /// 已经在 `release` 里下过线了。`Drop` 看这一位决定还要不要再下一次。
    released: bool,
}

impl EmployeeLease {
    async fn take(
        roster: Arc<dyn cog_core::AgentRegistry>,
        employee: &cog_agent::identity::AgentName,
    ) -> Self {
        // 第四个参数是名册上的角色格。对话员工是个人，登记的就该是人这个角色：
        // 曾经写 `chat`（「这一轮在说」当作角色），于是名册上同一个人上线一次是
        // `chat`、别处读到的又是别的词，两次读数对不上，而且「名册上的角色」这
        // 一格从此有两种量——人的身份与这一轮在干什么。现在角色只有一个来源：
        // 这一段活里的职务分工写在任职与账行上（`task_attempt.role`），人自己的
        // 名册行只答「是谁」，`EMPLOYEE_ROLE` 就是「一个员工」。
        let registration = cog_core::AgentRegistration::new(
            employee.id.clone(),
            cog_agent::identity::agent_instance(),
            "127.0.0.1",
            cog_gateway::agents::EMPLOYEE_ROLE,
            "default",
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
            released: false,
        }
    }

    async fn release(mut self) {
        self.released = true;
        self.beat.abort();
        if let Err(e) = self.roster.deregister(&self.agent_id).await {
            tracing::warn!(agent_id = %self.agent_id, error = %e, "名册注销失败");
        }
    }
}

/// 没走 `release` 就消失的那一轮，也要下线。
///
/// 这一轮是一个 future：界面按下停止、连接被丢掉、或者它在上游出错的地方被放下时，
/// 这个 lease 随它一起被丢下，`release` 那一行根本走不到。而心跳是一条独立的
/// 任务，没有这一处，它会一直替一个已经没人回答的员工续约——名册上这个人永远
/// 「在岗」，同事发给他的话投进一个没人读的收件箱，读名单的人以为他还醒着。
///
/// 所以下线跟着 lease 自己的寿命走，不跟着某一行代码有没有被执行到。
impl Drop for EmployeeLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.beat.abort();
        // 注销是异步的，而 Drop 里不能 await：交给运行时的另一条任务。运行时不在了
        // （进程正在收尾）就没有可交的地方——那时名册本身也在消失。
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let roster = self.roster.clone();
        let agent_id = self.agent_id.clone();
        handle.spawn(async move {
            if let Err(e) = roster.deregister(&agent_id).await {
                tracing::warn!(agent_id = %agent_id, error = %e, "名册注销失败");
            }
        });
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
        // wall: 等=收件箱的下一条 / 等不到=按静默窗结束这一轮读，已读的照常带走 / 续跑=能，下一轮再读
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

/// 收件箱里的一条怎么进这一轮的历史：一行抬头说明出处，下面是它说的话。
///
/// 措辞不在这里定：常驻员工那条路读的是同一份消息，两条路必须说成同一句话
/// （见 `cog_agent::inbox`）。这里只负责「这一段对话的收件箱消息要不要进历史」。
fn inbox_as_message(msg: &cog_core::InboxMessage) -> Option<String> {
    cog_agent::inbox::as_turn_text(msg)
}

/// 收件箱里的一条同事消息：谁说的、原话。撤停与重置是平台对正在跑的那一轮
/// 说话，不是某个人交办的一件事，因此不落意图行。
///
/// 原文不借用 [`inbox_as_message`] 的转述：那一份是给模型读的回合文本，加了
/// 「谁发来的」这类前缀；意图行存的是那个人说的原话，一个字节都不改写。
fn inbox_intent(msg: &cog_core::InboxMessage) -> Option<(String, String)> {
    use cog_core::InboxMessage;
    let (said_by, text) = match msg {
        InboxMessage::Prompt {
            goal, from_agent, ..
        } => (
            from_agent.clone().unwrap_or_default(),
            goal.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| goal.to_string()),
        ),
        InboxMessage::Continue {
            instruction,
            from_agent,
        }
        | InboxMessage::Steering {
            instruction,
            from_agent,
        } => (from_agent.clone().unwrap_or_default(), instruction.clone()),
        InboxMessage::Abort | InboxMessage::Reset => return None,
    };
    (!text.trim().is_empty()).then_some((said_by, text))
}

/// 工具路径（`send_to_agent`）发出去的一条，写成界面上那一行要的全部事实：
/// 发给谁、用什么方式、说了什么、结局如何。
///
/// 结局取自回执而不是工具名字：`delivered` 是「已经发布进收件人的收件箱」，
/// `replied` 是「等到了回信」，`reason` 是没等到时的那一句。等不到回信不等于
/// 对方没做完——`timeout` 只说「没在时限内答」，这两件事在界面上必须是两句不
/// 同的话，所以回执里已有的 `hint` 也一起带上，不在网关或界面另编一句。
///
/// 被拒的那一次没有回执，只有错误：收件人不存在、名册读不出来，这些话要说给
/// 发信的人，而不是记成一条「发过了」。
fn colleague_message(
    arguments: &serde_json::Value,
    result: &SFResult<serde_json::Value>,
) -> serde_json::Value {
    let to = arguments
        .get("agent_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let kind = arguments
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("prompt");
    let content = arguments
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    match result {
        Ok(receipt) => serde_json::json!({
            "to": to,
            "kind": kind,
            "content": content,
            "delivered": receipt
                .get("delivered")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            "replied": receipt.get("replied").cloned(),
            "reply": receipt.get("reply").cloned(),
            "reason": receipt.get("reason").cloned(),
            "hint": receipt.get("hint").cloned(),
        }),
        Err(e) => serde_json::json!({
            "to": to,
            "kind": kind,
            "content": content,
            "delivered": false,
            "error": e.to_string(),
        }),
    }
}

/// 工具路径（`start_task`）挂上任务图的一行，写成对话里那份列表要的事实：哪一行、
/// 是什么活、派给谁、属于哪个 goal。
///
/// 不在这里写状态：这一刻只有「已经进队」，而「谁在做、做到哪」是任务图随时间的
/// 读数，得从同一张表里读回来（`GET /api/v1/tasks/list`）。把入队那一刻的状态抄进
/// 对话，等于给任务图另开一份副本——一份只在派活那一瞬正确的副本。
///
/// 被拒的那一次（没写验收标准、指派的人不存在）不产出一行：任务图上根本没有这条
/// 记录，列表里出现一条查不到的活比少一条更糟。拒绝的原因在工具回执里，模型看得见。
fn task_list(
    arguments: &serde_json::Value,
    result: &SFResult<serde_json::Value>,
) -> Option<serde_json::Value> {
    let receipt = result.as_ref().ok()?;
    let task_id = receipt.get("task_id")?.as_str()?;
    // 第一行当标题：派活的人写 goal 时第一行就是一句话的活名，整段是给接手人看的
    // 说明。列表里一行放不下整段，而截断整段的第一个 60 字会把条件截掉一半。
    let title = arguments
        .get("goal")
        .and_then(serde_json::Value::as_str)
        .and_then(|goal| goal.lines().next())
        .unwrap_or("")
        .trim();
    let ledger_item = match (
        arguments
            .get("terminal")
            .and_then(serde_json::Value::as_str),
        arguments.get("now").and_then(serde_json::Value::as_str),
        arguments.get("gap").and_then(serde_json::Value::as_str),
    ) {
        (Some(terminal), Some(now), Some(gap)) => Some(serde_json::json!({
            "id": task_id,
            "terminal": terminal,
            "now": now,
            "gap": gap,
            "state": "未落",
        })),
        _ => None,
    };
    Some(serde_json::json!({
        "task_id": task_id,
        "goal": title,
        "goal_id": receipt.get("goal_id").cloned(),
        "venue": receipt.get("venue").cloned(),
        "assigned_to": receipt.get("assigned_to").cloned(),
        // 这一行是哪一句意图交出来的。挂上它，属主在屏幕上能顺着看
        // 「我说的那句话 → 意图 → 任务 → 终态」；没有它，任务与那句话之间的
        // 关系只存在于这一轮的上下文里，页面一关就断了。
        "intent_id": arguments.get("intent_id").cloned(),
        "ledger_item": ledger_item,
        // 这一类是什么、怎样算做完：随派活的那一次交给判据审，而不是让它只就事论事。
        "definition": arguments.get("definition").cloned(),
    }))
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
                    },
                    "definition": {
                        "type": "string",
                        "description": "这一类是什么、这一类里怎样算做完——判据审按它判断，不要写成步骤清单"
                    },
                    "goal_id": {
                        "type": "string",
                        "description": "续在已有的目标下：交同一个 id，条目就挂到那一道目标上；不给就新开一个"
                    },
                    "items": {
                        "type": "array",
                        "description": "已经知道的拆法：给出时跳过分解，按每条直接建任务。每条是终态／现在／差距三格；缺 `id` 时按序号生成",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "terminal": {"type": "string", "description": "做完之后世界的自然语言描述"},
                                "now": {"type": "string", "description": "树上此刻的样子"},
                                "gap": {"type": "string", "description": "终态减现在"}
                            },
                            "required": ["terminal", "now", "gap"]
                        }
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
    #[allow(clippy::too_many_arguments)]
    fn tool(
        self,
        orchestrator: Arc<dyn OrchestratorControl>,
        admission: Option<Arc<dyn HierarchyManager>>,
        audit: Option<Arc<dyn AuditStream>>,
        owner: String,
        actor: String,
        door: Option<Door>,
    ) -> Tool {
        let handler = match self {
            ControlPlaneOp::DispatchGoal => {
                dispatch_handler(orchestrator, admission, audit, owner, actor, door)
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
/// 交活的那个门：本进程的 HTTP 客户端与服务基址。没有它时对话侧直接调编排器
/// （进程内装配、测试），有它时打的是与 `start_task` 同一个已服务路径。
type Door = (Arc<dyn cog_core::HttpClient>, String);

fn control_plane_tools(
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
    actor: String,
) -> Vec<Tool> {
    let door = door_client();
    CONTROL_PLANE
        .iter()
        .map(|op| {
            op.tool(
                orchestrator.clone(),
                admission.clone(),
                audit.clone(),
                owner.clone(),
                actor.clone(),
                door.clone(),
            )
        })
        .collect()
}

/// 本进程能打到的门：基址取自 Agent 循环配置里的 memory API 地址（同一个服务），
/// 客户端取自网络配置；任何一半缺了就不走门，退回进程内调用。
fn door_client() -> Option<Door> {
    let base = cog_agent::AgentLoopConfig::load()
        .ok()
        .and_then(|config| config.memory_api_base.clone())
        .filter(|base| !base.trim().is_empty())?;
    let config = cog_net::HttpClientConfig::load().ok()?;
    let client = cog_net::ReqwestHttpClient::from_config(&config).ok()?;
    Some((Arc::new(client), base))
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

/// 属主给一条变更定档时，账上那个「文档」的名字前缀。
///
/// 按人分开：账的每一条引用记着「这是谁说的」，两个属主对同一条变更说了不同的
/// 档，读的人要分得出是谁的那一句。同一个属主再说一次，是他自己那一格被改写，
/// 不是多出一条别人说的。
const OWNER_BAND_DOC_PREFIX: &str = "owner:";

/// 三段身份：三格分开给，不从 `kind:scope:literal` 那串里拆。
///
/// 正文里常带冒号，拆错了问的就是另一条变更，而账上会诚实地回「没有这一条」——
/// 读的人于是以为这条变更不存在。
/// 三段身份在写账之前先过判据面：判不了的形状当场拒，并说清是哪一条线。
///
/// 校验放在写点，不放在判据处：判据处已经答得了「这一次量不了」，但那一答发生在
/// 行**落进账之后**——读者看到的是「还欠着」，而没有任何东西能把它还清。写点是从
/// 模型手里接话的那一处，模型还在，还能改一句再写。
///
/// 拒法回 JSON 而不是 `Err`：这是这一轮的结果，模型该把它说出来，而不是把同一次
/// 调用重试一遍——与这张表上其他拒法（`unknown_band`、`unknown_change`）同一档。
fn identity_arg(args: &serde_json::Value) -> Result<cog_core::ChangeIdentity, serde_json::Value> {
    let read = |key: &str| {
        args.get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or_default()
    };
    let (kind, scope, literal) = (read("kind"), read("scope"), read("literal"));
    for (field, value) in [("kind", kind), ("scope", scope), ("literal", literal)] {
        if value.is_empty() {
            return Err(serde_json::json!({
                "available": true,
                "refused": "blank",
                "field": field,
                "note": format!(
                    "{field} 是空的。一条变更的身份是 kind、scope、literal 三段，\
                     缺一段就查不了任何东西——先 list_changes 找一条真实的身份，照抄那三段。"
                ),
            }));
        }
    }
    cog_orchestrator::change_measure::measurable_identity(kind, scope, literal).map_err(|refused| {
        serde_json::json!({
            "available": true,
            "refused": refused.code(),
            "note": refused.reason(),
        })
    })
}

/// 外循环的方向盘：属主的一句话落到派工顺序与判据上。
///
/// 与那四只只读账工具分开，因为它们写：定档改的是「多急」，交出去改的是「现在
/// 谁做」，放行与回滚改的是「正在跑的是哪一版」，说一句「什么算好」改的是这件事
/// 该往哪儿算。判「做完没有」仍然不在这里——那只由看树的那一侧写，
/// `dispatch_change` 交出去的活也还是按账上那一行判。
///
/// 定档、拍板与说判据只在**这一段连接带登录身份**时注册：账上的一行引用要写
/// 「是谁说的」，一条没有人的连接答不出这一问；给它记一行，账上就多一条没有人的
/// 决定。
///
/// 交出去与 `dispatch_goal` 一样要过准入：它是一次派活，钱从同一个额度里出。拒绝
/// 写进的也是同一格审计，拒的 actor 是这位员工而不是「用这个界面的人」。
#[allow(clippy::too_many_arguments)]
fn change_direction_tools(
    changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>,
    orchestrator: Arc<dyn OrchestratorControl>,
    evolution: Option<Arc<dyn cog_core::EvolutionAdmin>>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    taste: Option<Arc<dyn cog_core::TasteIntentSink>>,
    owner: Option<String>,
    actor: String,
) -> Vec<Tool> {
    let mut tools = vec![dispatch_change_tool(
        changes.clone(),
        orchestrator,
        admission,
        audit.clone(),
        // 没有登录身份时是空串：准入照提交面同一句「没人配过上限不等于谁都不许
        // 提交」放行，而账上记的 owner 是空的，不编一个人。
        owner.clone().unwrap_or_default(),
        actor,
    )];
    if let Some(owner) = owner {
        tools.push(set_change_priority_tool(changes, owner.clone()));
        tools.push(approve_change_tool(evolution.clone()));
        tools.push(rollback_change_tool(evolution));
        // 说一句「什么算好」也只在有人的连接上：这一行署的是提交者，一句没有人
        // 署名的判据日后会被读成一条系统自己的标准。审计流与 HTTP 面那一格同一条：
        // 两个入口进来的是同一种提交，链上不该读成两件事。
        tools.push(submit_taste_intent_tool(taste, audit, owner));
    }
    tools
}

fn set_change_priority_tool(
    changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>,
    owner: String,
) -> Tool {
    // 这个属主在账上的名字。写进去的是引用，不是行上的一个档位列：一条变更的
    // 档位是它所有引用里最高的那一条，改写自己那一格不会动别人的说法。
    let doc = format!("{OWNER_BAND_DOC_PREFIX}{owner}");
    make_tool(
        "set_change_priority",
        "给变更账上的一条定档：p0 最急，p3 最缓，unstated 表示不再要任何档。\
         档决定派工顺序——同一面里 p0 的行先被量、先被交出去。写的是属主自己那一格\
         引用，同一个属主再说一次会改写前一句，不会留下两句互相矛盾的话。\
         一条变更的档是它所有引用里最高的那一条：你把它降下来时，若还有别的文档\
         要求得更急，返回里的 effective 会比你要的高，照实说给属主，别宣布降成了。\
         只对账上已有的行生效：三段身份照抄 list_changes，别在这里新造一条变更。\
         这是写账，但它改的是「多急」，不是「做完没有」——后者只能由看树的那一侧量。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "kind": { "type": "string", "description": "检查的种类：grep（scope 下要有这段原文）、exists（scope 这个路径要在树上）、gone（这个路径不在树上）" },
                "scope": { "type": "string", "description": "检查看哪里：一个路径或一个目录" },
                "literal": { "type": "string", "description": "逐字匹配的那段正文" },
                "band": {
                    "type": "string",
                    "enum": ["p0", "p1", "p2", "p3", "unstated"],
                    "description": "要的档位；unstated 是撤回属主要求的档"
                }
            },
            "required": ["kind", "scope", "literal", "band"]
        }),
        native(move |args| {
            let changes = changes.clone();
            let doc = doc.clone();
            async move {
                let Some(store) = changes else {
                    return Ok(ledger_absent("变更账"));
                };
                let identity = match identity_arg(&args) {
                    Ok(identity) => identity,
                    Err(refusal) => return Ok(refusal),
                };
                let asked = require_str(&args, "band")?;
                let priority = match asked.parse::<cog_core::ChangePriority>() {
                    Ok(priority) => priority,
                    Err(_) => {
                        return Ok(serde_json::json!({
                            "available": true,
                            "refused": "unknown_band",
                            "note": format!(
                                "账上没有 {asked:?} 这一档：只有 p0、p1、p2、p3、unstated，\
                                 照抄其中一个。"
                            ),
                        }));
                    }
                };
                if store.get(&identity).await?.is_none() {
                    return Ok(serde_json::json!({
                        "available": true,
                        "refused": "unknown_change",
                        "note": "账上没有这条身份，也没有替它新开一行：三段里差一个字就是\
                                 另一条变更。先用 list_changes 按字面找一遍。",
                    }));
                }
                let reading = cog_core::ChangeReading {
                    identity: identity.clone(),
                    // 只改档：标题与正文留空，账上已有的那两格不被这一次改写。
                    title: None,
                    assertion: None,
                    source: cog_core::ChangeSource {
                        doc,
                        verdict: None,
                        priority,
                        block: cog_core::ChangeBlock::NONE,
                    },
                };
                store
                    .record_readings(std::slice::from_ref(&reading), None)
                    .await?;
                let effective = store
                    .get(&identity)
                    .await?
                    .map(|row| row.priority())
                    .unwrap_or(priority);
                Ok(serde_json::json!({
                    "available": true,
                    "change": identity.as_key(),
                    "asked": priority.as_str(),
                    "effective": effective.as_str(),
                    "note": if effective == priority {
                        format!("这一条现在按 {} 排队。", priority.as_str())
                    } else {
                        format!(
                            "你要的是 {}，账算出来是 {}：这一条的档是所有引用里最高的那一条，\
                             还有别的文档要求得更急。属主要的是「不再这么急」，得让那条文档改口，\
                             这一句不要替他宣布。",
                            priority.as_str(),
                            effective.as_str()
                        )
                    },
                }))
            }
        }),
    )
}

/// 属主说一句「什么算好」，记成一条外部判定。
///
/// 这是味道（taste）这条输入在对话里的入口。别的进化输入都是缺陷语义——失败的活、
/// 还在响的告警——它们说哪里疼，说不出往哪走；这条说的是后者。它照原话落盘、署属主
/// 的名，随后和网关那个提交面一样，被读成一件活；而那句话随任务走，判这件活的每个
/// 座位的判据后面都会逐字带上它。
fn submit_taste_intent_tool(
    tastes: Option<Arc<dyn cog_core::TasteIntentSink>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
) -> Tool {
    make_tool(
        "submit_taste_intent",
        "把属主说的一句「什么算好」记成一条外部判定，原话照录、署属主的名字。三种：\
         direction_preference 说两个方向里偏好哪一个（preferred 相比 alternative，都要说出来，\
         只说一个方向是建议去做一件事，不是两个里选一个）；rubric_override 说评判标准本身\
         该改读成什么（criterion 是现在的判据名，new_wording 是它该说的话）；\
         evaluator_verdict 判某份材料够不够格（insufficient 不能支持结论、contradicted 指向\
         反面、confirmed 确实支持）——这一种说的是这一次的结果，不是往后的方向，它不会进\
         判据。三种都要 rationale：一张没有理由的判据，日后没人能说清它为什么该在。\
         subject 是这条判定针对的东西，同一个 subject 加同一种判定在冷却期内只落成一件活。\
         evidence_refs 指这次判断依据的材料（source id 或绝对 URI），没有就不填——\
         别把 subject 换成一段描述，那不是键，日后查不回来。\
         记之前先跟属主对一遍原话：这是署他名字的判定，不是你的总结。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "subject": { "type": "string", "description": "这条判定针对的是什么，用属主的说法；重复提交的抑制按它加判定种类做键" },
                "kind": {
                    "type": "string",
                    "enum": ["direction_preference", "rubric_override", "evaluator_verdict"],
                    "description": "判定的种类"
                },
                "preferred": { "type": "string", "description": "direction_preference：属主偏好的那个方向" },
                "alternative": { "type": "string", "description": "direction_preference：它比下去的那个方向" },
                "criterion": { "type": "string", "description": "rubric_override：要改的那条标准，按系统里的名字说" },
                "new_wording": { "type": "string", "description": "rubric_override：它该读成什么" },
                "verdict": {
                    "type": "string",
                    "enum": ["insufficient", "contradicted", "confirmed"],
                    "description": "evaluator_verdict：材料对结论值多少"
                },
                "rationale": { "type": "string", "description": "为什么，用属主的原话" },
                "evidence_refs": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "这次判断依据的材料，按引用给（source id 或绝对 URI）"
                }
            },
            "required": ["subject", "kind", "rationale"]
        }),
        native(move |args| {
            let tastes = tastes.clone();
            let audit = audit.clone();
            let owner = owner.clone();
            async move {
                let Some(sink) = tastes else {
                    return Ok(ledger_absent("判定账"));
                };
                let subject = match taste_text(&args, "subject") {
                    Some(subject) => subject,
                    None => {
                        return Ok(taste_refused(
                            "unnamed_subject",
                            "subject 必填：这条判定针对的是什么，用属主的说法说出来。\
                             没有它的判定日后查不回来，也抑制不了重复提交。",
                        ));
                    }
                };
                let Some(kind) = taste_text(&args, "kind") else {
                    return Ok(taste_refused(
                        "unknown_kind",
                        "kind 必填：direction_preference、rubric_override、evaluator_verdict 里选一个。",
                    ));
                };
                let payload = match taste_payload(&args, &kind.to_ascii_lowercase()) {
                    Ok(payload) => payload,
                    Err(refusal) => return Ok(refusal),
                };
                let evidence_refs = match evidence_refs_arg(&args) {
                    Ok(refs) => refs,
                    Err(refusal) => return Ok(refusal),
                };
                let intent = cog_core::TasteIntent {
                    id: uuid::Uuid::new_v4(),
                    subject,
                    // 署名的是坐在连接这一端的人，不是替他记下这句的员工：账上读
                    // 「谁说的」，不是「谁打字快」。
                    submitted_by: owner.clone(),
                    submitted_at: chrono::Utc::now(),
                    payload,
                    evidence_refs,
                };
                if let Err(error) = intent.validate() {
                    return Ok(taste_refused("invalid_submission", &error));
                }
                let kind = intent.payload.kind();
                let summary = intent.payload.summary();
                sink.submit(&intent).await?;
                // 落盘之后的审计那一格，与 HTTP 提交面同一条 kind、同一个动作名：
                // 两个入口进来的是同一种提交，链上不该读成两件事。写不进去只告警、
                // 不退回：提交已经落盘，此时回错会让属主重说一遍，而重说说不动任何
                // 已经写下的事实。
                if let Some(audit) = audit.as_ref() {
                    let detail = serde_json::json!({
                        "kind": kind,
                        "subject": intent.subject,
                        "payload": intent.payload,
                        "evidence_refs": intent.evidence_refs,
                    });
                    if let Err(error) = audit
                        .append(
                            AuditKind::Custom(cog_gateway::taste::TASTE_AUDIT_KIND.to_string()),
                            &intent.submitted_by,
                            &intent.subject,
                            cog_gateway::taste::TASTE_AUDIT_ACTION,
                            detail,
                        )
                        .await
                    {
                        tracing::warn!(
                            id = %intent.id,
                            error = %error,
                            "taste intent stored but not written to the audit chain"
                        );
                    }
                }
                Ok(serde_json::json!({
                    "available": true,
                    "id": intent.id.to_string(),
                    "kind": kind,
                    "subject": intent.subject,
                    "submitted_by": intent.submitted_by,
                    "summary": summary,
                    "note": "记下了，署的是属主的名字。它随后经与其它信号同一条冷却/幂等框架\
                             变成一件活；那句话会随这件活走，判它的每个座位都在判据后面读到原话。\
                             已经被同一 subject 同一种判定接住时不会再来一件——那是抑制，不是丢失。",
                }))
            }
        }),
    )
}

/// 一个给人看的拒绝：记下来这件事没成，以及为什么。
fn taste_refused(kind: &str, note: &str) -> serde_json::Value {
    serde_json::json!({
        "available": true,
        "refused": kind,
        "note": note,
    })
}

/// 提交体里的一段正文：去空白，空串当没给。
fn taste_text(args: &serde_json::Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// 按种类把参数拼成判定体；缺哪一半就在拼的时候说清楚，而不是等落盘时才发现。
///
/// 返回 `Err` 是给人看的拒绝（这一轮该把它说出来），不是异常：模型重试同一句不该
/// 把一次缺字段的调用变成一次失败。
fn taste_payload(
    args: &serde_json::Value,
    kind: &str,
) -> Result<cog_core::TasteIntentPayload, serde_json::Value> {
    let rationale = taste_text(args, "rationale");
    match kind {
        "direction_preference" => {
            let preferred = taste_text(args, "preferred");
            let alternative = taste_text(args, "alternative");
            let (Some(preferred), Some(alternative), Some(rationale)) =
                (preferred, alternative, rationale)
            else {
                return Err(taste_refused(
                    "incomplete_judgement",
                    "direction_preference 要把两边都说出来：preferred（属主偏好的方向）、\
                     alternative（它比下去的方向）、rationale（为什么）三个都必填。\
                     只说一个方向是「去做这件事」的建议，不是两个方向里选一个——\
                     落成的活是两回事，替属主补出另一边就是替他做了选择。",
                ));
            };
            Ok(cog_core::TasteIntentPayload::DirectionPreference(
                cog_core::DirectionPreferenceIntent {
                    preferred,
                    alternative,
                    rationale,
                },
            ))
        }
        "rubric_override" => {
            let criterion = taste_text(args, "criterion");
            let new_wording = taste_text(args, "new_wording");
            let (Some(criterion), Some(new_wording), Some(rationale)) =
                (criterion, new_wording, rationale)
            else {
                return Err(taste_refused(
                    "incomplete_judgement",
                    "rubric_override 要说清改哪一条、改成什么：criterion（要改的那条标准，\
                     按系统里的名字说）、new_wording（它该读成什么）、rationale（为什么）\
                     三个都必填。缺了 criterion 的那一端，接手的人不知道改哪儿，\
                     会先来问你一次——这一问本来在这里就能省掉。",
                ));
            };
            Ok(cog_core::TasteIntentPayload::RubricOverride(
                cog_core::RubricOverrideIntent {
                    criterion,
                    new_wording,
                    rationale,
                },
            ))
        }
        "evaluator_verdict" => {
            let Some(verdict) = taste_text(args, "verdict") else {
                return Err(taste_refused(
                    "unknown_verdict",
                    "evaluator_verdict 要说材料对结论值多少：insufficient（不能支持）、\
                     contradicted（指向反面）、confirmed（确实支持）三个词里选一个。",
                ));
            };
            let verdict = match verdict.to_ascii_lowercase().as_str() {
                "insufficient" => cog_core::EvaluatorVerdict::Insufficient,
                "contradicted" => cog_core::EvaluatorVerdict::Contradicted,
                "confirmed" => cog_core::EvaluatorVerdict::Confirmed,
                other => {
                    return Err(taste_refused(
                        "unknown_verdict",
                        &format!(
                            "{other:?} 不在闭集里：只有 insufficient、contradicted、confirmed。\
                             照抄其中一个，别换一个近义词——这三个词决定了接着做什么。"
                        ),
                    ));
                }
            };
            let Some(rationale) = rationale else {
                return Err(taste_refused(
                    "incomplete_judgement",
                    "evaluator_verdict 要有 rationale：结论单独拿出来，接手的人无从下手。",
                ));
            };
            Ok(cog_core::TasteIntentPayload::EvaluatorVerdict(
                cog_core::EvaluatorVerdictIntent { verdict, rationale },
            ))
        }
        other => Err(taste_refused(
            "unknown_kind",
            &format!(
                "{other:?} 不是一种判定：只有 direction_preference、rubric_override、\
                 evaluator_verdict。这三种覆盖不开的东西，说明它其实是一件活或一条缺陷，\
                 走那条路。"
            ),
        )),
    }
}

/// 材料引用：每一项都得是一个字符串。
///
/// 形状与上限由契约层判（`TasteIntent::validate`），这里只保证拼得进那份结构：一个
/// 数字或一个对象拼进列表，落盘时会被读成「没有引用材料」，而这正是这一格要防的事。
fn evidence_refs_arg(args: &serde_json::Value) -> Result<Vec<String>, serde_json::Value> {
    match args.get("evidence_refs") {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(items)) => {
            let mut refs = Vec::with_capacity(items.len());
            for item in items {
                let Some(text) = item.as_str() else {
                    return Err(taste_refused(
                        "bad_evidence",
                        "evidence_refs 的每一项都是一条引用：材料的 source id，或者一个绝对 URI。\
                         列表里有一项不是字符串。",
                    ));
                };
                refs.push(text.to_string());
            }
            Ok(refs)
        }
        Some(_) => Err(taste_refused(
            "bad_evidence",
            "evidence_refs 是一个列表，每一项是一条引用（source id 或绝对 URI）。",
        )),
    }
}

// prompt: consumed by crates/cog-core/src/contract/change_ledger.rs::ChangeState
fn dispatch_change_tool(
    changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>,
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
    actor: String,
) -> Tool {
    make_tool(
        "dispatch_change",
        "把变更账上的这一行现在就交给执行者：它按这条变更的正文做出一笔改动，判据就是账上\
         这一行——同一身份落到 landed 或 retired 才算完，不是它自己说做完了就算完。\
         交之前先 get_change 看一眼：已经了结的行不派。同一身份重复派不会多出一行任务，\
         返回里的 status 会说是新交的还是本来就在。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "kind": { "type": "string", "description": "检查的种类：grep（scope 下要有这段原文）、exists（scope 这个路径要在树上）、gone（这个路径不在树上）" },
                "scope": { "type": "string", "description": "检查看哪里：一个路径或一个目录" },
                "literal": { "type": "string", "description": "逐字匹配的那段正文" }
            },
            "required": ["kind", "scope", "literal"]
        }),
        native(move |args| {
            let changes = changes.clone();
            let orchestrator = orchestrator.clone();
            let admission = admission.clone();
            let audit = audit.clone();
            let owner = owner.clone();
            let actor = actor.clone();
            async move {
                let Some(store) = changes else {
                    return Ok(ledger_absent("变更账"));
                };
                let identity = match identity_arg(&args) {
                    Ok(identity) => identity,
                    Err(refusal) => return Ok(refusal),
                };
                let Some(row) = store.get(&identity).await? else {
                    return Ok(serde_json::json!({
                        "available": true,
                        "refused": "unknown_change",
                        "note": "账上没有这条身份：三段里差一个字就是另一条变更，\
                                 先用 list_changes 按字面找一遍。",
                    }));
                };
                // 只有 landed 是「了结」。被判出账的行仍然是 pending：它的身份还
                // 欠着一句话，派出去也不该当成没人管的活。
                if row.state == cog_core::ChangeState::Landed {
                    return Ok(serde_json::json!({
                        "available": true,
                        "refused": "already_settled",
                        "state": row.state.as_str(),
                        "note": "这一条已经了结：派出去也不会被当成一件还欠着的活。",
                    }));
                }
                // 这个控制面认不认「按变更派工」，要它自己说：一个不跑派工环的
                // 实现回 `None`，它给的 id 集合会是空的，编一个 id 就等于派给
                // 一个不存在的数。这里照它的缺席说「这里不按变更派工」。
                let Some(ids) = orchestrator.change_dispatch_ids(&identity) else {
                    return Ok(serde_json::json!({
                        "available": true,
                        "refused": "not_dispatched_here",
                        "note": "这个进程的控制面不按变更派工：这里没有能把这一行交出去的派工环。",
                    }));
                };
                // 同一个 id 在图里已经有一行时，再交一次是幂等的空操作。先看一遍
                // 图：已经有了就说「本来就在」，并且点名那一行。这里**不**换一个
                // 世代再交一次——开新的一代是派工环按「这次失败是环境的，不是这条
                // 变更的」做出的判断；一段对话看得到的是「这条还没落地」，看不到
                // 那个分类。按一下开一代，就会按一下多烧一份同一条变更的活，而读
                // 面只会看到「交出去了」。
                let held: std::collections::HashSet<String> = orchestrator
                    .get_all_tasks()
                    .await
                    .into_iter()
                    .map(|task| task.id)
                    .collect();
                if let Some(generation) = ids.iter().position(|id| held.contains(id)) {
                    return Ok(serde_json::json!({
                        "available": true,
                        "status": "already_held",
                        "change": identity.as_key(),
                        "task_id": ids[generation],
                        "generation": generation,
                        "acceptance": "同一身份的账行落到 landed 或 retired，判据取账上那一行",
                        "note": "这一条在图上已经有一行了，再交一次不会多出一笔。那一行\
                                 还在不在跑、为什么没跑完，读任务台账；要重跑那一次就用 \
                                 retry_task。重新开一代是派工环在环境失败时做的判断，\
                                 不是这里按一下——多按几次就是同一条变更烧好几份钱。",
                    }));
                }
                // 交在第 0 代上：那是身份本身，也是以前每一次派工用的那个 id。
                let (goal, rows) = cog_orchestrator::change_ledger::handover_on_request(&row, 0);
                // 与 `dispatch_goal` 同一道门、同一份估值：交一条变更也是一次派活，
                // 钱从同一个额度里出。挡下时回的是这一轮的结果，不是错误——模型要
                // 把原因说出来，而不是把同一次重试一遍。
                if let Some(refusal) = admission_refusal(admission.as_ref(), &owner, &goal).await {
                    record_refusal(audit.as_ref(), &actor, &owner, &refusal).await;
                    return Ok(serde_json::json!({
                        "available": true,
                        "status": "refused",
                        "reason": refusal.sentence(),
                        "note": "这一次没有交出去，账上那一行还是欠着的。把原因说给属主，\
                                 不要重试同一次提交。",
                    }));
                }
                let task_id = ids[0].clone();
                let added = orchestrator.submit_goal_auto(&goal, rows).await?;
                let fresh = added.iter().any(|id| id == &task_id);
                Ok(serde_json::json!({
                    "available": true,
                    "status": if fresh { "submitted" } else { "already_held" },
                    "change": identity.as_key(),
                    "task_id": task_id,
                    "generation": 0,
                    "acceptance": "同一身份的账行落到 landed 或 retired，判据取账上那一行",
                    "note": "进展用 list_tasks（goal_id 就是这条变更所属的那一组）看；\
                             这一行什么时候算完，读变更账，不读这条任务的回话。",
                }))
            }
        }),
    )
}

fn approve_change_tool(evolution: Option<Arc<dyn cog_core::EvolutionAdmin>>) -> Tool {
    make_tool(
        "approve_change",
        "放行一条等人工审批的变更：它已经过了门、停在「等属主拍板」，这一句把它交上去——\
         提交、构建、切换。只对等审批的行有效，别的状态会回一句「没动」。它是写：会换掉\
         正在跑的那一版。`change_id` 照抄 list 出来的那个，不要自己编。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "change_id": {
                    "type": "string",
                    "description": "等审批的那条变更的 id，照抄后台列出来的值"
                }
            },
            "required": ["change_id"]
        }),
        native(move |args| {
            let evolution = evolution.clone();
            async move {
                let Some(admin) = evolution else {
                    return Ok(admin_absent());
                };
                let change_id = require_str(&args, "change_id")?.to_string();
                match admin.approve_change(&change_id).await {
                    Ok(done) => Ok(serde_json::json!({
                        "available": true,
                        "approved": true,
                        "change_id": done.change_id,
                        "commit": done.commit_hash,
                        "switched": done.switched,
                        "note": "已经交上去了。切换完的那一版要等它起来，别在这一句之后\
                                 就宣布线已经是新的。",
                    })),
                    Err(e) => Ok(serde_json::json!({
                        "available": true,
                        "approved": false,
                        "note": format!(
                            "没有放行：{e}。这不等于它已经在跑，也不等于被否决——\
                             照实说给属主，不要重试同一次。"
                        ),
                    })),
                }
            }
        }),
    )
}

fn rollback_change_tool(evolution: Option<Arc<dyn cog_core::EvolutionAdmin>>) -> Tool {
    make_tool(
        "rollback_change",
        "把正在跑的那一版退回上一次部署，并重启。这是**部署级**回滚：退的是整条线上\
         的那一版，不是把账上某一行改回未落——账上的行只有 landed 与 retired 两种了结，\
         退回来之后下一次量会照实把它判回未落。属主说「把刚才那次回滚掉」时用它；\
         说「这一条别做了」那是定档与账的事，不是回滚。`why` 会被记下来。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "why": {
                    "type": "string",
                    "description": "为什么要退：写清是哪一次、因为什么"
                }
            }
        }),
        native(move |args| {
            let evolution = evolution.clone();
            async move {
                let Some(admin) = evolution else {
                    return Ok(admin_absent());
                };
                // 理由不送给 admin：它那次调用不带理由，但这条调用自己要留下
                // 一笔——读日志的人至少看得出是谁、为什么按了它。
                let why = args
                    .get("why")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .unwrap_or(Default::default());
                tracing::info!(why, "rollback requested from a conversation");
                match admin.rollback().await {
                    Ok(done) => Ok(serde_json::json!({
                        "available": true,
                        "rolled_back": done.rolled_back,
                        "message": done.message,
                        "note": "退回去的那一版要等它起来，别在这一句之后就宣布线已经回去了。",
                    })),
                    Err(e) => Ok(serde_json::json!({
                        "available": true,
                        "rolled_back": false,
                        "note": format!(
                            "没有回滚：{e}。照实说给属主；不要重试同一次，也不要宣布退回去了。"
                        ),
                    })),
                }
            }
        }),
    )
}

/// 这个进程没有装配进化控制面时回的那一条。
///
/// 与账缺席同一个道理：「这里没有能放行/回滚的东西」不是「没有值得放行的变更」。
fn admin_absent() -> serde_json::Value {
    serde_json::json!({
        "available": false,
        "note": "这个进程没有装配进化控制面：这里没有能放行或回滚的东西。\
                 这是「做不了」，不是「没有该做的」——别把这一句转述成「没有待批的变更」。",
    })
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
                "kind": { "type": "string", "description": "检查的种类：grep（scope 下要有这段原文）、exists（scope 这个路径要在树上）、gone（这个路径不在树上）" },
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
                    .unwrap_or(Default::default());

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

/// 员工在对话里提案一条运行时原则。
///
/// 这是员工能做的那半件「改规则」：它写得下一条规则（什么时候、看什么、做什么），
/// 但写不了「它生效了」——生效只由属主拍板。提案落进与常驻循环同一本账，所以批了
/// 之后下一轮开头它就在每个人的系统前缀里，不需要重发版、也不需要重启谁。
pub const PROPOSE_PRINCIPLE: &str = "propose_principle";

/// 属主批准一条提案。
pub const APPROVE_PRINCIPLE: &str = "approve_principle";

/// 属主否决一条提案。
pub const REJECT_PRINCIPLE: &str = "reject_principle";

/// 读账上此刻生效的规则与还等着拍的提案。
pub const LIST_RUNTIME_PRINCIPLES: &str = "list_runtime_principles";

/// 规则账这一组：提案（每个人）+ 拍板（只在有人的对话里）。
///
/// 拍板的两只只在**这一轮有登录身份**时注册：没有人的连接批准不了任何东西——
/// 那份「谁说的」在账上会读成一位员工替属主做的决定，而这里是它唯一不许自己
/// 拍板的地方。规则与角色无关：这一组对每个员工都一样，谁提的都进同一本账。
fn principle_tools(
    account: Option<Arc<dyn cog_core::RuntimePrincipleStore>>,
    employee: String,
    owner_present: bool,
) -> Vec<Tool> {
    let mut tools = vec![
        list_runtime_principles_tool(account.clone()),
        propose_principle_tool(account.clone(), employee),
    ];
    if owner_present {
        tools.push(decide_principle_tool(
            account.clone(),
            cog_core::RuntimePrincipleState::Approved,
        ));
        tools.push(decide_principle_tool(
            account,
            cog_core::RuntimePrincipleState::Rejected,
        ));
    }
    tools
}

fn list_runtime_principles_tool(account: Option<Arc<dyn cog_core::RuntimePrincipleStore>>) -> Tool {
    make_tool(
        LIST_RUNTIME_PRINCIPLES,
        "读运行时原则账：此刻生效的规则（每轮开头交给每个人）与还等着属主拍的提案。\
         属主问「有什么新提案」「现在按哪些规则做事」时先读它；批准、否决时把返回里的\
         slug 照抄过去，不要自己编一个。`state` 可以只看一态。它是只读的——改不了账，\
         也不会把提案变成生效；生效只有属主的批准。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "enum": ["proposed", "approved", "rejected", "retired"],
                    "description": "只看这一态；不给就看全部"
                }
            }
        }),
        native(move |args| {
            let account = account.clone();
            async move {
                let Some(store) = account else {
                    return Ok(ledger_absent("运行时原则账"));
                };
                let state = match word_arg(&args, "state") {
                    Some(word) => match cog_core::RuntimePrincipleState::parse(&word) {
                        Some(state) => Some(state),
                        None => {
                            return Ok(serde_json::json!({
                                "available": true,
                                "refused": "unknown_state",
                                "note": format!(
                                    "账上没有 {word:?} 这一态：只有 proposed、approved、\
                                     rejected、retired 四种，照抄其中一个。"
                                ),
                            }))
                        }
                    },
                    None => None,
                };
                let rows = store.list(state).await?;
                let rules: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|row| {
                        serde_json::json!({
                            "slug": row.slug,
                            "state": row.state.as_str(),
                            "in_force": row.in_force(),
                            "trigger": row.trigger,
                            "criterion": row.criterion,
                            "action": row.action,
                            "proposed_by": row.proposed_by,
                            "proposed_at": stamp(Some(row.proposed_at)),
                            "decided_at": stamp(row.decided_at),
                            "used": row.used,
                        })
                    })
                    .collect();
                let pending = rows
                    .iter()
                    .filter(|row| row.state == cog_core::RuntimePrincipleState::Proposed)
                    .count();
                let in_force = rows.iter().filter(|row| row.in_force()).count();
                Ok(serde_json::json!({
                    "available": true,
                    "rules": rules,
                    "in_force": in_force,
                    "pending": pending,
                    "note": if rows.is_empty() {
                        "账上此刻一行都没有。"
                    } else if pending > 0 {
                        "有几条提案还等着属主拍板：把它们的 slug 照抄给属主，\
                         不要替属主决定，也不要把提案当成已经在执行的规则。"
                    } else {
                        "没有等着拍的提案。"
                    },
                }))
            }
        }),
    )
}

fn propose_principle_tool(
    account: Option<Arc<dyn cog_core::RuntimePrincipleStore>>,
    employee: String,
) -> Tool {
    make_tool(
        PROPOSE_PRINCIPLE,
        "把一条想让平台照着做的规则提上账：`trigger` 是它什么时候适用，`criterion` 是看什么，\
         `action` 是做什么。提了**不生效**：属主批准之后，下一轮运行开头它才随基础指令交到\
         每个人手上。规则不是对人的设定——写「什么时候、看什么、做什么」，不写「你是谁」；\
         同一本账对每个员工都一样，不按人、不按角色分。",
        serde_json::json!({
            "type": "object",
            "properties": {
                "slug": {
                    "type": "string",
                    "description": "这条规则的名字，全账唯一，回复里引用它时照抄"
                },
                "trigger": {
                    "type": "string",
                    "description": "什么情况下适用"
                },
                "criterion": {
                    "type": "string",
                    "description": "看什么，看到什么算符合"
                },
                "action": {
                    "type": "string",
                    "description": "符合与不符合各自做什么"
                }
            },
            "required": ["slug", "trigger", "criterion", "action"]
        }),
        native(move |args| {
            let account = account.clone();
            let employee = employee.clone();
            async move {
                let Some(store) = account else {
                    return Ok(ledger_absent("运行时原则账"));
                };
                let text = |key: &str| {
                    args.get(key)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string()
                };
                let row = match cog_core::RuntimePrinciple::proposed(
                    text("slug"),
                    text("trigger"),
                    text("criterion"),
                    text("action"),
                    employee,
                    chrono::Utc::now(),
                ) {
                    Ok(row) => row,
                    // 三段缺任何一段都不是一条能被判的规则：回给模型看清是哪一段，
                    // 而不是把它存成一句「这条今天没有内容」。
                    Err(e) => {
                        return Ok(serde_json::json!({
                            "proposed": false,
                            "refused": "incomplete",
                            "note": format!("没有进账：{e}"),
                        }))
                    }
                };
                match store.propose(&row).await {
                    Ok(()) => Ok(serde_json::json!({
                        "proposed": true,
                        "slug": row.slug,
                        "in_force": false,
                        "trigger": row.trigger,
                        "criterion": row.criterion,
                        "action": row.action,
                        "note": "提案已经进账，现在还不生效：要属主批准。批准之后，下一轮\
                                 运行开头它会随基础指令交给每个人；在那之前不要按它做主，\
                                 回复里说清它还等着批。"
                    })),
                    Err(e) => Ok(serde_json::json!({
                        "proposed": false,
                        "refused": "not_filed",
                        "slug": row.slug,
                        "note": format!("没有进账：{e}"),
                    })),
                }
            }
        }),
    )
}

fn decide_principle_tool(
    account: Option<Arc<dyn cog_core::RuntimePrincipleStore>>,
    decision: cog_core::RuntimePrincipleState,
) -> Tool {
    let (name, verb, meaning) = match decision {
        cog_core::RuntimePrincipleState::Approved => (
            APPROVE_PRINCIPLE,
            "批准",
            "批准之后它会立刻成为规则：下一轮运行开头随基础指令交给每个人。",
        ),
        _ => (
            REJECT_PRINCIPLE,
            "否决",
            "否决的行留在账上，读得到「这条提过、当时没同意」；同一个名字可以按新文本重新提案。",
        ),
    };
    make_tool(
        name,
        &format!(
            "属主{verb}一条提案。只在属主在这段对话里明确说了{verb}时调用——这是属主\
             的决定，不是你的判断。{meaning}"
        ),
        serde_json::json!({
            "type": "object",
            "properties": {
                "slug": {
                    "type": "string",
                    "description": "要拍的那条提案的名字"
                }
            },
            "required": ["slug"]
        }),
        native(move |args| {
            let account = account.clone();
            async move {
                let Some(store) = account else {
                    return Ok(ledger_absent("运行时原则账"));
                };
                let slug = args
                    .get("slug")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                if slug.is_empty() {
                    return Ok(serde_json::json!({
                        "moved": false,
                        "note": "没有动：要给一个 slug——账上按名字认那条提案，空名字拍不到任何一条。",
                    }));
                }
                let moved = store.decide(&slug, decision, chrono::Utc::now()).await?;
                Ok(serde_json::json!({
                    "slug": slug,
                    "moved": moved,
                    "state": decision.as_str(),
                    "in_force": moved && decision.in_force(),
                    "note": if moved {
                        "账上这一行动了。"
                    } else {
                        "没有动：账上没有这条等着拍的提案——它可能已经批过、被否决了，\
                         或者名字对不上。先说清这一点，不要宣布它生效了。"
                    }
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

#[allow(clippy::too_many_arguments)]
fn dispatch_handler(
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
    actor: String,
    door: Option<Door>,
) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        let admission = admission.clone();
        let audit = audit.clone();
        let owner = owner.clone();
        let actor = actor.clone();
        let door = door.clone();
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
            // 续在已有目标下还是新开一道：与 `start_task` 同一个语义，同一个参数名。
            let goal_id = args
                .get("goal_id")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            // 调用方已经知道拆法时直接交 items：跳过分解，按每条建一件活。
            // 三格与定义都随任务走，判据审读得到；id 缺了就按序号生成——回执里给的是
            // 真正入图的那几个 id。
            let items = args
                .get("items")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or(Default::default());
            if !items.is_empty() {
                let definition = args
                    .get("definition")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let mut tasks = Vec::new();
                let mut ids = Vec::new();
                for (index, item) in items.iter().enumerate() {
                    let id = item
                        .get("id")
                        .and_then(|value| value.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("{goal_id}-{}", index + 1));
                    let text = |key: &str| {
                        item.get(key)
                            .and_then(|value| value.as_str())
                            .unwrap_or("")
                            .to_string()
                    };
                    let terminal = text("terminal");
                    let now = text("now");
                    let gap = text("gap");
                    let mut input = serde_json::json!({ "query": goal });
                    if let Some(definition) = &definition {
                        input["definition"] = serde_json::json!(definition);
                    }
                    // 五格的中心是 `Task`：三格直接挂在任务上，不经值对象再从 `input` 拷。
                    let task = cog_core::Task::new(
                        id.clone(),
                        cog_core::TaskType::Custom(
                            cog_core::contract::work_item::PLATFORM_AGENT_TASK.into(),
                        ),
                        input,
                    )
                    .with_cells(terminal, now, gap);
                    tasks.push(task);
                    ids.push(id);
                }
                // 有门就走门：与 `start_task` 同一条已服务路径，同一个请求体形状。
                // 门的回执是「已排队」，与进程内那条一样，结局都在任务行上读。
                if let Some((client, base)) = door.as_ref() {
                    let rows: Vec<serde_json::Value> = tasks
                        .iter()
                        .map(|task| {
                            let cells = task.cells();
                            let acceptance = cells
                                .map(|(terminal, _, _)| vec![terminal.to_string()])
                                .unwrap_or_default();
                            let mut input = task.input.clone();
                            input["acceptance"] = serde_json::json!(acceptance);
                            // 时长没由这一条给就按任务行的默认值声明：门把 0 读成
                            // 「这活不花时间」并拒收，缺省才是「照常估」。
                            let declared = input
                                .get("declared_duration_seconds")
                                .and_then(|value| value.as_u64())
                                .filter(|value| *value > 0)
                                .unwrap_or(cog_core::types::task::DEFAULT_DECLARED_DURATION_SECS);
                            cog_core::contract::task_door::task_door_row(
                                &task.id,
                                cog_core::contract::work_item::PLATFORM_AGENT_TASK,
                                input,
                                cells,
                                declared,
                                true,
                            )
                        })
                        .collect();
                    let workspace = std::env::var(cog_core::env::WORKSPACE_ID)
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or_else(|| "default".to_string());
                    let body = cog_core::contract::task_door::task_door_body(
                        &goal_id, &goal, &workspace, rows,
                    );
                    let url = format!("{}/api/v1/tasks", base.trim_end_matches('/'));
                    let request = cog_core::HttpRequest::post(url)
                        .json(&body)
                        .map_err(|e| cog_core::SFError::Validation(e.to_string()))?;
                    let response = client.execute(request).await?;
                    if !response.is_success() {
                        let text = String::from_utf8_lossy(&response.body).to_string();
                        return Err(cog_core::SFError::Validation(format!(
                            "the task door refused this batch (HTTP {}): {}",
                            response.status,
                            text.trim()
                        )));
                    }
                    return Ok(serde_json::json!({
                        "goal_id": goal_id,
                        "task_ids": ids,
                        "status": "submitted",
                        "note": "已按给定的拆法经任务门直接建任务，没有走分解。要查进展用 list_tasks；goal_id 与 task_ids 照抄"
                    }));
                }
                let submitter = orchestrator.clone();
                let goal_for_submit = goal.clone();
                let goal_for_warn = goal_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = submitter.submit_goal_auto(&goal_for_submit, tasks).await {
                        tracing::warn!(goal = %goal_for_warn, error = %e, "这批 items 没能交给编排器");
                    }
                });
                return Ok(serde_json::json!({
                    "goal_id": goal_id,
                    "task_ids": ids,
                    "status": "submitted",
                    "note": "已按给定的拆法直接建任务，没有走分解。要查进展用 list_tasks；goal_id 与 task_ids 照抄"
                }));
            }
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
    // 待办的状态由任务行的结局现算（[`LedgerItem::at_outcome`]），不抄进任务行：
    // 同一个结局只在 `status` 一处写，任务重试一次，读出来的状态跟着变，没有
    // 第二份要同步。差距那一格原样带回——它写的是「世界上还缺什么」。
    let ledger_item = task
        .ledger_item()
        .and_then(|item| serde_json::to_value(item).ok());
    let mut summary = serde_json::json!({
        "id": task.id,
        "kind": task.task_type,
        "status": task.status,
        "goal_id": task.goal_id,
        "parent_task_id": task.parent_task_id,
        "agent_id": task.agent_id,
        "retry_count": task.retry_count,
        "updated_at": task.updated_at,
        "error": task.error,
        // 谁在跑、租约何时过期：状态是 Running 时这两格是唯一能读出「孤儿」的
        // 证据。结果与 input 不在这里给——一次列 20 行会把上下文占满，
        // 要读那两格走 `get_task`。
        "lease_owner": task.lease_owner,
        "lease_expires_at": task.lease_expires_at,
    });
    if let (Some(item), Some(object)) = (ledger_item, summary.as_object_mut()) {
        object.insert("ledger_item".to_string(), item);
    }
    summary
}

/// 方向盘那四只的门。单独一个文件，因为它要造的假账里有几个账契约的写入口，
/// 而「读账的这一层不许出现写入口」那道门按文本读这个文件。
#[cfg(test)]
mod direction_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::RuntimePrincipleStore as _;

    /// 定义从派活的那一次一路走到判据审：工具参数里有它，任务行带着它，判据审读它。
    ///
    /// 没有这次改动这条会红：派活只收 `goal`，判据审拿到的定义恒为 `None`，
    /// 「按定义判断」只剩就事论事。
    #[test]
    fn the_definition_travels_from_the_dispatch_to_the_judge() {
        let source = include_str!("conversation.rs");
        assert!(
            source.contains("判据审按它判断"),
            "派活工具的参数里没有定义这一格"
        );
        assert!(
            source.contains("\"definition\": arguments.get(\"definition\").cloned()"),
            "任务行没有把定义带下去"
        );
        assert!(
            source.contains("review_criteria(&turn_text, &items, definition)"),
            "判据审没有读这一轮的定义"
        );
    }

    /// 调用方知道拆法时直接交 items：跳过分解，按每条建一件活，三格与定义随任务走。
    ///
    /// 没有这次改动这条会红：派活只认 `goal`，一条已经拆好的活还要再被 planner 拆一次，
    /// 三格与定义在进图那一刻就丢了。
    #[tokio::test]
    async fn known_items_skip_decomposition_and_become_tasks() {
        use cog_core::OrchestratorControl as _;
        let graph = std::sync::Arc::new(cog_test_support::TaskGraphStub::empty());
        let tool = ControlPlaneOp::DispatchGoal.tool(
            graph.clone(),
            None,
            None,
            "owner".into(),
            "actor".into(),
            None,
        );
        let ToolImplementation::Native(handler) = tool.implementation else {
            panic!("dispatch_goal 不是原生工具");
        };
        let answer = handler(serde_json::json!({
            "goal": "把三处收进一处",
            "goal_id": "goal-known",
            "definition": "这一类：把三处收进一处；做完＝只剩一处。",
            "items": [
                {"terminal": "a 收好了", "now": "还没", "gap": "缺 a"},
                {"id": "mine", "terminal": "b 收好了", "now": "还没", "gap": "缺 b"}
            ]
        }))
        .await
        .expect("这批 items 应当被受理");
        assert_eq!(answer["status"], "submitted", "{answer}");
        assert_eq!(
            answer["goal_id"], "goal-known",
            "给了 goal_id 就要续在那一道目标上"
        );
        let ids: Vec<&str> = answer["task_ids"]
            .as_array()
            .expect("回执里要有 task_ids")
            .iter()
            .filter_map(|id| id.as_str())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"mine"), "给定的 id 要照用：{ids:?}");

        let mut rows = Vec::new();
        for _ in 0..200 {
            rows = graph.get_all_tasks().await;
            if rows.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(rows.len(), 2, "两条 items 应当在图上各成一件活");
        let mine = rows.iter().find(|t| t.id == "mine").expect("mine");
        assert_eq!(mine.terminal, "b 收好了", "三格在任务的一等字段上");
        // 两个入口都走同一张门行（`task_door_row`）：三格随行交，
        // 谁也不再往 `input` 里塞一份。
        let tools_source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("cog-agent/src/tools.rs"),
        )
        .expect("tools.rs");
        assert!(
            tools_source.contains("task_door_row("),
            "智能体侧的 start_task 没有走门行那条路"
        );
        assert!(
            !tools_source.contains("ITEM_INPUT_KEY"),
            "智能体侧还在把三格塞进 input：系统流程必须用任务字段"
        );
        assert_eq!(
            mine.input["definition"],
            "这一类：把三处收进一处；做完＝只剩一处。"
        );
    }

    /// 门在时，已经拆好的 items 走的是与 `start_task` 同一条已服务路径：
    /// 一次 POST `/api/v1/tasks`，请求体里带着定义、验收与「已经想过」的标记，
    /// 进程内那张图上一件也不多出来。
    ///
    /// 没有这次改动这条会红：控制面的派活只往进程内的编排器交，
    /// 服务进程与对话进程不共享同一张图，交出去的活在对面的任务列表里读不到。
    #[tokio::test]
    async fn known_items_go_through_the_served_door_when_there_is_one() {
        #[derive(Debug, Default)]
        struct Captured(std::sync::Mutex<Vec<cog_core::HttpRequest>>);

        #[async_trait::async_trait]
        impl cog_core::HttpClient for Captured {
            async fn execute(
                &self,
                request: cog_core::HttpRequest,
            ) -> cog_core::SFResult<cog_core::HttpResponse> {
                self.0.lock().unwrap().push(request);
                Ok(cog_core::HttpResponse {
                    status: 201,
                    headers: Default::default(),
                    body: br#"{"task_ids":["FROM-DOOR"]}"#.to_vec(),
                })
            }
        }

        use cog_core::OrchestratorControl as _;
        let graph = std::sync::Arc::new(cog_test_support::TaskGraphStub::empty());
        let capture = std::sync::Arc::new(Captured::default());
        let tool = ControlPlaneOp::DispatchGoal.tool(
            graph.clone(),
            None,
            None,
            "owner".into(),
            "actor".into(),
            Some((capture.clone(), "http://door.example/".into())),
        );
        let ToolImplementation::Native(handler) = tool.implementation else {
            panic!("dispatch_goal 不是原生工具");
        };
        let answer = handler(serde_json::json!({
            "goal": "把三处收进一处",
            "goal_id": "goal-known",
            "definition": "这一类：把三处收进一处；做完＝只剩一处。",
            "items": [
                {"terminal": "a 收好了", "now": "还没", "gap": "缺 a"},
                {"id": "mine", "terminal": "b 收好了", "now": "还没", "gap": "缺 b"}
            ]
        }))
        .await
        .expect("这批 items 应当被门受理");
        assert_eq!(answer["status"], "submitted", "{answer}");
        assert_eq!(
            answer["task_ids"]
                .as_array()
                .expect("回执里要有 task_ids")
                .iter()
                .filter_map(|id| id.as_str())
                .count(),
            2
        );

        let posts = capture.0.lock().unwrap();
        assert_eq!(posts.len(), 1, "一次派活只该发一次请求");
        assert_eq!(
            posts[0].url, "http://door.example/api/v1/tasks",
            "门地址后面不能多出一个斜杠"
        );
        assert_eq!(posts[0].method, "POST");
        let body: serde_json::Value =
            serde_json::from_slice(posts[0].body.as_ref().expect("请求体")).expect("请求体是 JSON");
        assert_eq!(body["goal_id"], "goal-known");
        assert_eq!(body["intent_kind"], "task");
        let tasks = body["tasks"].as_array().expect("请求体里要有任务行");
        assert_eq!(tasks.len(), 2);
        let mine = tasks.iter().find(|t| t["id"] == "mine").expect("mine");
        assert_eq!(
            mine["input"]["definition"],
            "这一类：把三处收进一处；做完＝只剩一处。"
        );
        assert_eq!(mine["terminal"], "b 收好了", "三格随行交，不塞进 input");
        assert_eq!(
            mine["input"]["acceptance"][0], "b 收好了",
            "验收就是这一条的终态：没有它门会以 acceptance_required 拒收"
        );
        assert_eq!(
            mine[cog_core::roles::PLANNER]["verified"],
            true,
            "拆法来自调用方，门不该再为这一行花一次分解"
        );
        assert!(
            mine["declared_duration_seconds"].as_u64().unwrap_or(0) > 0,
            "门把 0 秒读成「这活不花时间」并拒收，没给时长就按行默认值声明"
        );
        drop(posts);
        assert!(
            graph.get_all_tasks().await.is_empty(),
            "走了门就不该在进程内再建一遍"
        );
    }

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

    /// 员工页读的是任务图那一行：只有「完成且差距已空」读作「已落」；完成却
    /// 还写着差距、失败、撤回、还在跑，都是「未落」。差距那一格原样带回——
    /// 一次运行报告成功不等于树上的那件事成立了，把差距抹掉等于让「跑完了」
    /// 冒充「世界变了」。
    #[test]
    fn the_owner_sees_the_ledger_state_of_what_the_turn_filed() {
        use cog_core::contract::ledger_item::{LedgerItem, LedgerState, ITEM_INPUT_KEY};
        let item = LedgerItem {
            id: "work-1".into(),
            terminal: "树上是这个样子".into(),
            now: "现在还不是".into(),
            gap: "还差那一处".into(),
            state: LedgerState::Open,
        };
        let base = || {
            let now = chrono::Utc::now();
            cog_core::Task {
                submitted_by: None,
                id: "work-1".into(),
                task_type: cog_core::TaskType::Custom(
                    cog_core::contract::work_item::PLATFORM_AGENT_TASK.into(),
                ),
                status: cog_core::TaskStatus::Pending,
                input: serde_json::json!({
                    "task": "把那一处改掉",
                    ITEM_INPUT_KEY: serde_json::to_value(&item).expect("待办该序列化得出来"),
                }),
                terminal: item.terminal.clone(),
                now: item.now.clone(),
                gap: item.gap.clone(),
                result: None,
                error: None,
                error_cause: None,
                failure_payer: None,
                blocked_by: vec![],
                blocks: vec![],
                priority: 1,
                created_at: now,
                updated_at: now,
                agent_id: None,
                workspace_id: None,
                retry_count: 0,
                max_retries: 1,
                retry_not_before: None,
                started_at: None,
                declared_duration_seconds: 30,
                lease_owner: None,
                lease_expires_at: None,
                action_planner_meta: None,
                goal_id: Some("goal-1".into()),
                parent_task_id: None,
                sink_task_id: None,
                is_executable: true,
                face_id: None,
                venue: None,
                owner_unit: None,
                bound_by_handover_id: None,
                acceptance: vec!["那一处不再出现".into()],
            }
        };
        // 差距还写着：完成也仍是未落——跑完了不等于世界变了。
        for (status, state) in [
            (cog_core::TaskStatus::Pending, "未落"),
            (cog_core::TaskStatus::Running, "未落"),
            (cog_core::TaskStatus::Completed, "未落"),
            (cog_core::TaskStatus::Failed, "未落"),
            (cog_core::TaskStatus::Cancelled, "未落"),
        ] {
            let mut task = base();
            task.status = status;
            let summary = task_summary(&task);
            assert_eq!(
                summary["ledger_item"]["state"], state,
                "{status:?} 在员工页上读成了 {state}"
            );
            assert_eq!(summary["status"], serde_json::json!(status));
            assert_eq!(
                summary["ledger_item"]["gap"], "还差那一处",
                "差距是世界上的缺口，不是这一次运行的结局"
            );
            assert_eq!(summary["ledger_item"]["terminal"], "树上是这个样子");
        }
        // 完成且差距被读树的人清掉：这时才读作已落。
        let mut reached = base();
        let mut verified = item.clone();
        verified.gap = String::new();
        reached.status = cog_core::TaskStatus::Completed;
        reached.input = serde_json::json!({
            "task": "把那一处改掉",
            ITEM_INPUT_KEY: serde_json::to_value(&verified).expect("待办该序列化得出来"),
        });
        assert_eq!(task_summary(&reached)["ledger_item"]["state"], "已落");

        // 没有三格的行不凭空长出一条待办：读面缺这一格，与「这一条没有差距」分得开。
        let mut bare = base();
        bare.input = serde_json::json!({ "task": "把那一处改掉" });
        assert!(task_summary(&bare).get("ledger_item").is_none());
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

    /// 发给同事的一条话，写成界面上那一行要的全部事实。
    ///
    /// 三种结局必须分得开：等到了回信（`replied` 真、`reply` 有）、发出去了
    /// 但没等/没等到（`reason` 说明为什么不等于做完了）、根本没发出去
    /// （`delivered` 假、`error` 有原因）。把缺席读成假值，界面上就会把
    /// 「没问」画成「问了没答」。
    #[test]
    fn a_message_sent_to_a_colleague_carries_its_outcome() {
        let args = serde_json::json!({
            "agent_id": "mira",
            "kind": "prompt",
            "message": "查一下昨晚那次发布为什么红了",
        });

        let replied = colleague_message(
            &args,
            &Ok(serde_json::json!({
                "delivered": true,
                "replied": true,
                "reply": "是配置少了一行",
            })),
        );
        assert_eq!(replied["to"], "mira");
        assert_eq!(replied["kind"], "prompt");
        assert_eq!(replied["content"], "查一下昨晚那次发布为什么红了");
        assert_eq!(replied["delivered"], true);
        assert_eq!(replied["replied"], true);
        assert_eq!(replied["reply"], "是配置少了一行");

        let timed_out = colleague_message(
            &args,
            &Ok(serde_json::json!({
                "delivered": true,
                "replied": false,
                "reason": "timeout",
                "hint": "它可能还在跑",
            })),
        );
        assert_eq!(timed_out["replied"], false);
        assert_eq!(timed_out["reason"], "timeout");
        assert_eq!(timed_out["hint"], "它可能还在跑");

        let refused = colleague_message(
            &args,
            &Err(cog_core::SFError::Agent(
                "no live agent named \"mira\"".to_string(),
            )),
        );
        assert_eq!(refused["delivered"], false);
        assert!(
            refused["error"]
                .as_str()
                .is_some_and(|text| text.contains("no live agent")),
            "没发出去的那一条要有原因：{refused}"
        );
    }

    /// 挂上任务图的一行，只写那一刻就已确定的事实：哪一行、什么活（第一行）、
    /// 派给谁、属于哪个 goal。状态不在这里——它是任务图随时间的读数。
    #[test]
    fn a_filed_row_carries_the_title_and_no_state() {
        let args = serde_json::json!({
            "goal": "把上周的记忆写入读一遍\n验收：每一条都有一到十的评分",
            "acceptance": ["每一条都有一到十的评分"],
        });
        let row = task_list(
            &args,
            &Ok(serde_json::json!({
                "filed": true,
                "task_id": "task-1",
                "goal_id": "goal-9",
                "venue": "memory",
                "assigned_to": "mira",
            })),
        )
        .expect("挂上去的一行要进列表");
        assert_eq!(row["task_id"], "task-1");
        assert_eq!(
            row["goal"], "把上周的记忆写入读一遍",
            "活名取的是 goal 的第一行：整段说明挤不进列表的一行"
        );
        assert_eq!(row["goal_id"], "goal-9");
        assert_eq!(row["venue"], "memory");
        assert_eq!(row["assigned_to"], "mira");
        assert!(
            row.get("status").is_none(),
            "状态被抄进了对话：任务图从此有一份停在入队的副本"
        );

        // 被拒的那一次任务图上没有这一行：列表里出现一条查不到的活比少一条更糟。
        let refused = task_list(
            &args,
            &Err(cog_core::SFError::Validation(
                "a filing without a criterion is refused".to_string(),
            )),
        );
        assert!(refused.is_none(), "被拒的派活不该在列表里占一行");
        let no_id = task_list(&args, &Ok(serde_json::json!({ "filed": true })));
        assert!(no_id.is_none(), "没有 task_id 就指不到任务图上任何一行");
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
                from_agent: None,
                intent: None,
            })
            .as_deref(),
            Some("[没留名的同事发来的消息]\n把首页配色改一下")
        );
        assert_eq!(
            inbox_as_message(&InboxMessage::Prompt {
                goal: serde_json::json!({"step": 2}),
                reply_stream: None,
                from_agent: None,
                intent: None,
            })
            .as_deref(),
            Some("[没留名的同事发来的消息]\n{\"step\":2}")
        );
        assert_eq!(
            inbox_as_message(&InboxMessage::Continue {
                instruction: "接着做".into(),
                from_agent: None
            })
            .as_deref(),
            Some("[没留名的同事发来的消息]\n接着做")
        );
        assert_eq!(
            inbox_as_message(&InboxMessage::Steering {
                instruction: "换个方向".into(),
                from_agent: None
            })
            .as_deref(),
            Some("[没留名的同事发来的消息]\n换个方向")
        );
        assert_eq!(inbox_as_message(&InboxMessage::Abort), None);
        assert_eq!(inbox_as_message(&InboxMessage::Reset), None);
    }

    /// 消息带发信人时，历史里写的是那个人，不是「某个同事」。
    ///
    /// 收件人手里只有这一条消息：说完这句话它可能要回信、要往下追、要把话转给
    /// 第三个人，每一步都得知道话是谁说的。老消息没有这一列，照旧读成无名同事
    /// ——线上已经进流的条目不能因为加了发信人就变成一条读不懂的消息。
    #[test]
    fn a_message_that_names_its_sender_is_read_as_coming_from_that_employee() {
        use cog_core::InboxMessage;
        let named = InboxMessage::Steering {
            instruction: "等下再合，构建还红着".into(),
            from_agent: Some("squad:task-1-reviewer".into()),
        };
        assert_eq!(
            inbox_as_message(&named).as_deref(),
            Some("[同事 squad:task-1-reviewer 发来的消息]\n等下再合，构建还红着")
        );

        let older: InboxMessage = serde_json::from_value(
            serde_json::json!({"type": "steering", "instruction": "等下再合，构建还红着"}),
        )
        .expect("a message written before the sender column existed still parses");
        assert_eq!(
            inbox_as_message(&older).as_deref(),
            Some("[没留名的同事发来的消息]\n等下再合，构建还红着"),
            "没留名就说没留名，不编名字"
        );
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
            from_agent: Some("E0412".into()),
            intent: None,
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
            Some("[同事 E0412 发来的消息]\n帮我看一下昨天那次发布为什么红了")
        );
    }

    /// 一个只剩硬上限的管理器：每一级都已经用完。
    pub(super) struct Exhausted;

    #[async_trait::async_trait]
    impl HierarchyManager for Exhausted {
        fn default_limits(&self) -> cog_core::QuotaLimits {
            cog_core::QuotaLimits::new(1_000_000, 1_000_000)
        }

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
    pub(super) struct RecordingAudit(std::sync::Mutex<Vec<(AuditKind, String, String, String)>>);

    impl RecordingAudit {
        pub(super) fn actions(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|row| row.3.clone())
                .collect()
        }

        pub(super) fn actors(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|row| row.1.clone())
                .collect()
        }
    }

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
        async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>> {
            // 绑定的第一步要按会话反查名册那一行，所以这张 hash 得读得回来。
            Ok(self
                .fields
                .lock()
                .unwrap()
                .iter()
                .filter(|((k, _), _)| k == key)
                .map(|((_, f), v)| (f.clone(), v.clone()))
                .collect())
        }
        async fn hlen(&self, key: &str) -> SFResult<u64> {
            Ok(self
                .fields
                .lock()
                .unwrap()
                .keys()
                .filter(|(k, _)| k == key)
                .count() as u64)
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

        let origin = PersonOrigin::default();
        note_person(&kv, &employee, "conv-1", &origin, first)
            .await
            .unwrap();
        let later = first + chrono::TimeDelta::minutes(5);
        note_person(&kv, &employee, "conv-1", &origin, later)
            .await
            .unwrap();

        let row: cog_gateway::agents::PersonRow =
            serde_json::from_str(&kv.stored(&employee.id)).expect("名册那一行要读得回来");
        assert_eq!(row.first_seen, first);
        assert_eq!(row.last_seen, later);
        assert_eq!(row.session_id, "conv-1");
        assert_eq!(row.display_name, employee.handle);
    }

    /// 滚一次 Pod 不换人。
    ///
    /// 名字是「实例指纹 ＋ 会话」算出来的，指纹随每次滚动变化，于是同一个会话
    /// 换一个实例接手就变成一个从没见过的员工——2026-11-07 界面上「工作台」与
    /// 「员工」两处读同一份名册却看到不同的人，一半原因在这里。绑定把第一次见面
    /// 的那个人钉在会话上。
    #[tokio::test]
    async fn a_session_keeps_its_person_after_the_instance_fingerprint_changes() {
        let store = Arc::new(FakePeople::default());
        let kv: Arc<dyn cog_core::KeyValueStore> = store;

        let first = bound_employee(&kv, Some("inst-7f3a"), None, "conv-1").await;
        // 同一个会话，另一个实例接手：指纹不同，按会话重算会算出另一个人。
        let unbound_after_roll = employee_named(Some("inst-9c02"), "conv-1");
        assert_ne!(
            unbound_after_roll.id, first.id,
            "这条判据的前提是重算会换名；指纹已经不参与名字了，得换个前提"
        );

        let after_roll = bound_employee(&kv, Some("inst-9c02"), None, "conv-1").await;

        assert_eq!(after_roll.id, first.id);
        assert_eq!(after_roll.handle, first.handle);
    }

    /// 升级之前说过话的会话，第一轮认它原来的那一行，不新发一个人。
    ///
    /// 绑定表是后加的：老会话没有这一行，而它的名册行就是它的人。认不出这一步，
    /// 升级之后同一个会话会在名单上变成两个名字。
    #[tokio::test]
    async fn a_session_that_spoke_before_binding_existed_keeps_its_roster_person() {
        let store = Arc::new(FakePeople::default());
        let kv: Arc<dyn cog_core::KeyValueStore> = store;
        let older = employee_named(Some("inst-aaaa"), "conv-1");
        note_person(
            &*kv,
            &older,
            "conv-1",
            &PersonOrigin::default(),
            chrono::Utc::now(),
        )
        .await
        .unwrap();

        // 升级之后接手的是另一个实例：按会话重算会得到另一个人。
        let employee = bound_employee(&kv, Some("inst-bbbb"), None, "conv-1").await;

        assert_eq!(employee.id, older.id);
        assert_eq!(employee.handle, older.handle);
    }

    /// 同一个属主开两段对话，是同一个人；人不是会话的一部分。
    ///
    /// 按会话绑人时，同一个人每开一个新窗口就多一位员工：名册上行数随窗口数涨，
    /// 工号一个个发出去，账上他今天花的钱被拆成好几个人。这条病读起来完全正常
    /// ——每一行都有号、都有名字、都不重复。
    ///
    /// 判据说的是「员工行数不变」，所以除了两个会话读到同一个 id，还要数一遍：
    /// 绑定表里这个属主只有一行，名册上没有多出行来。
    #[tokio::test]
    async fn one_owner_is_one_employee_across_sessions() {
        let store = Arc::new(FakePeople::default());
        let kv: Arc<dyn cog_core::KeyValueStore> = store;

        let first = bound_employee(&kv, Some("inst-7f3a"), Some("user-1"), "conv-1").await;
        // 第二段对话换了一个实例指纹：没有登录身份这条线，重算会算出另一个人。
        let unbound_second = employee_named(Some("inst-9c02"), "conv-2");
        assert_ne!(
            unbound_second.id, first.id,
            "这条判据的前提是换了会话就会换人；前提没了，得换个问法"
        );

        let second = bound_employee(&kv, Some("inst-9c02"), Some("user-1"), "conv-2").await;

        assert_eq!(second.id, first.id);
        assert_eq!(second.handle, first.handle);
        assert_eq!(
            cog_core::KeyValueStore::hlen(&*kv, cog_gateway::agents::USER_EMPLOYEE_KEY)
                .await
                .unwrap(),
            1,
            "同一个属主绑出了不止一行：他的名字下已经是一个部门，而不是一个人"
        );
        assert_eq!(
            cog_core::KeyValueStore::hlen(&*kv, cog_gateway::agents::PEOPLE_KEY)
                .await
                .unwrap(),
            0,
            "绑人不该顺手往「说过话的人」名册里写行——那是说过话才写的地方"
        );
    }

    /// 没有登录身份的连接不往属主绑定表里写东西。
    ///
    /// 探针、升级前的老客户端、以及任何没带登录身份的连接，`owner` 都是空的。
    /// 空串当成一个属主写下去，名册上就有了一行「无名氏」，之后每一台没登录的
    /// 机器都会绑到它身上——一个谁都对得上的身份。
    #[tokio::test]
    async fn a_connection_without_a_login_identity_binds_nobody() {
        let store = Arc::new(FakePeople::default());
        let kv: Arc<dyn cog_core::KeyValueStore> = store;

        let employee = bound_employee(&kv, Some("inst-7f3a"), None, "conv-1").await;
        bound_employee(&kv, Some("inst-7f3a"), Some("   "), "conv-1").await;

        assert_eq!(
            cog_core::KeyValueStore::hlen(&*kv, cog_gateway::agents::USER_EMPLOYEE_KEY)
                .await
                .unwrap(),
            0,
            "没有登录身份的连接在属主绑定表里留下了一行：之后任何一个空属主都会绑到它身上"
        );
        // 会话这一行照写：没有登录身份也还是这一段对话的人。
        let bound = cog_gateway::agents::read_bound_session(&*kv, "conv-1")
            .await
            .expect("会话照样绑人");
        assert_eq!(bound.agent_id, employee.id);
    }

    /// 名册那一行记着这个人从哪儿来，且不会被一轮没声明来源的话擦掉。
    ///
    /// 名字是哈希出来的，读的人从名字上分不出「有人坐在这儿聊」和「一个探针开了
    /// 一个会话、说了一句、再也没回来」。来源只在新的一轮说得出口时覆盖，否则
    /// 一个老客户端接上来的第一句话就把「web」擦成「不知道」。
    #[tokio::test]
    async fn a_people_row_keeps_the_origin_it_was_first_seen_from() {
        let kv = FakePeople::default();
        let employee = employee_named(Some("inst-7f3a"), "conv-1");
        let at = chrono::Utc::now();
        let web = PersonOrigin {
            platform: Some("web".into()),
            device_id: Some("device-1".into()),
        };

        note_person(&kv, &employee, "conv-1", &web, at)
            .await
            .unwrap();
        note_person(
            &kv,
            &employee,
            "conv-1",
            &PersonOrigin::default(),
            at + chrono::TimeDelta::minutes(5),
        )
        .await
        .unwrap();

        let row: cog_gateway::agents::PersonRow =
            serde_json::from_str(&kv.stored(&employee.id)).expect("名册那一行要读得回来");
        assert_eq!(row.platform.as_deref(), Some("web"));
        assert_eq!(row.device_id.as_deref(), Some("device-1"));
    }

    // ── 规则账：员工提案，属主在同一段对话里拍板 ──
    //
    // 这一段判的是外循环那半句话：「员工能从对话里提案新原则，属主在同一段对话里
    // 看到、批准或否决，批准后下一轮即生效」。下一轮生效不在这一段里重造一遍：
    // 交付是每轮现读（`cog_agent::principles`），所以「下一轮读得到」在这里就是
    // 「账上这一行成了生效态、`in_force` 收得下它」。

    /// 账的内存版：只实现这张对话用得到的几条，护栏与 PostgreSQL 那份一致。
    #[derive(Default)]
    struct FakePrinciples {
        rows: std::sync::Mutex<Vec<cog_core::RuntimePrinciple>>,
    }

    impl FakePrinciples {
        fn state_of(&self, slug: &str) -> Option<cog_core::RuntimePrincipleState> {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.slug == slug)
                .map(|row| row.state)
        }
    }

    #[async_trait::async_trait]
    impl cog_core::RuntimePrincipleStore for FakePrinciples {
        async fn propose(&self, principle: &cog_core::RuntimePrinciple) -> cog_core::SFResult<()> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.slug == principle.slug) {
                Some(row) if row.state == cog_core::RuntimePrincipleState::Rejected => {
                    *row = principle.clone();
                    Ok(())
                }
                Some(row) => Err(cog_core::SFError::Conflict(format!(
                    "{} 已经是 {:?}",
                    principle.slug, row.state
                ))),
                None => {
                    rows.push(principle.clone());
                    Ok(())
                }
            }
        }

        async fn decide(
            &self,
            slug: &str,
            decision: cog_core::RuntimePrincipleState,
            at: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<bool> {
            use cog_core::RuntimePrincipleState::{Approved, Proposed, Rejected, Retired};
            let from = match decision {
                Approved | Rejected => Proposed,
                Retired => Approved,
                Proposed => {
                    return Err(cog_core::SFError::Validation(
                        "拍板只有批准、否决、撤回".into(),
                    ))
                }
            };
            let mut rows = self.rows.lock().unwrap();
            match rows
                .iter_mut()
                .find(|row| row.slug == slug && row.state == from)
            {
                Some(row) => {
                    row.state = decision;
                    row.decided_at = Some(at);
                    Ok(true)
                }
                None => Ok(false),
            }
        }

        async fn in_force(&self) -> cog_core::SFResult<Vec<cog_core::RuntimePrinciple>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|row| row.in_force())
                .cloned()
                .collect())
        }

        async fn note_used(&self, slugs: &[String]) -> cog_core::SFResult<()> {
            let mut rows = self.rows.lock().unwrap();
            for row in rows.iter_mut().filter(|row| slugs.contains(&row.slug)) {
                row.used += 1;
            }
            Ok(())
        }

        async fn list(
            &self,
            state: Option<cog_core::RuntimePrincipleState>,
        ) -> cog_core::SFResult<Vec<cog_core::RuntimePrinciple>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|row| state.is_none_or(|state| state == row.state))
                .cloned()
                .collect())
        }
    }

    pub(super) async fn call(
        tools: &[Tool],
        name: &str,
        args: serde_json::Value,
    ) -> serde_json::Value {
        let tool = tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| {
                panic!(
                    "没有 {name}：{}",
                    tools
                        .iter()
                        .map(|t| &t.name)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            });
        let ToolImplementation::Native(handler) = &tool.implementation else {
            panic!("{name} 不是原生工具");
        };
        handler(args)
            .await
            .unwrap_or_else(|e| panic!("{name} 调用失败：{e}"))
    }

    fn proposal(slug: &str) -> serde_json::Value {
        serde_json::json!({
            "slug": slug,
            "trigger": "在合并之前",
            "criterion": "构建是不是绿的",
            "action": "红了就停下来修"
        })
    }

    /// 提案进账、落在提它的那个员工名下，而且当场说清它还不生效。
    ///
    /// 这是员工能做的半件事。回一句「已记录」而不说「还等着批」，模型就会把一条
    /// 没人批准过的句子当成规则照着做——账上它还是提案态，读面与行为从此对不上。
    #[tokio::test]
    async fn a_proposal_from_the_chat_lands_under_the_employee_and_is_not_in_force() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), false);

        let answer = call(&tools, PROPOSE_PRINCIPLE, proposal("build-before-merge")).await;

        assert_eq!(answer["proposed"], true, "{answer}");
        assert_eq!(
            answer["in_force"], false,
            "提案当场生效就不是提案了：{answer}"
        );
        assert!(
            answer["note"].as_str().unwrap_or_default().contains("批准"),
            "回复没有说它等着属主批准：{}",
            answer["note"]
        );
        assert_eq!(
            account.state_of("build-before-merge"),
            Some(cog_core::RuntimePrincipleState::Proposed),
            "账上没有这一行，或者它不是提案态"
        );
        let rows = account.rows.lock().unwrap();
        assert_eq!(rows[0].proposed_by, "E0412", "提案人不是这位员工");
    }

    /// 三段缺一段就没有进账，而且回给模型的是缺了哪一段。
    ///
    /// 少一段的规则读起来像「这一条今天没有内容」——存进去之后，每一轮都会把它
    /// 当成一条没有约束的规则交出去，而账上看着一切正常。
    #[tokio::test]
    async fn an_incomplete_proposal_is_refused_instead_of_filed() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), false);

        let answer = call(
            &tools,
            PROPOSE_PRINCIPLE,
            serde_json::json!({
                "slug": "half-a-rule",
                "trigger": "在合并之前",
                "criterion": "  ",
                "action": "红了就停下来修"
            }),
        )
        .await;

        assert_eq!(answer["proposed"], false, "{answer}");
        assert_eq!(answer["refused"], "incomplete", "{answer}");
        assert!(
            answer["note"]
                .as_str()
                .unwrap_or_default()
                .contains("criterion"),
            "没有说清缺的是哪一段：{}",
            answer["note"]
        );
        assert!(
            account.rows.lock().unwrap().is_empty(),
            "缺一段的规则进了账"
        );
    }

    /// 属主在同一段对话里批准，下一轮读到它就是规则。
    ///
    /// 这就是 A15 那一句：提案与拍板在同一条对话里完成，生效不是重发版换来的，
    /// 而是账上这一行成了生效态——下一轮开头现读（`in_force`）所以立刻读得到。
    #[tokio::test]
    async fn the_owner_approves_in_the_same_conversation_and_the_next_run_reads_it() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), true);

        call(&tools, PROPOSE_PRINCIPLE, proposal("build-before-merge")).await;
        let answer = call(
            &tools,
            APPROVE_PRINCIPLE,
            serde_json::json!({ "slug": "build-before-merge" }),
        )
        .await;

        assert_eq!(answer["moved"], true, "{answer}");
        assert_eq!(answer["state"], "approved", "{answer}");
        assert_eq!(answer["in_force"], true, "{answer}");
        assert_eq!(
            account.state_of("build-before-merge"),
            Some(cog_core::RuntimePrincipleState::Approved)
        );
        // 下一轮开头读的那一集里已经有它：交付是每轮现读，所以这就是「下一轮生效」。
        let live = account.in_force().await.unwrap();
        assert_eq!(
            live.iter().map(|row| row.slug.as_str()).collect::<Vec<_>>(),
            vec!["build-before-merge"]
        );
    }

    /// 属主在对话里看得见「等着拍的提案」，拍完看得见它进了生效集。
    ///
    /// 没有这一只，「批准」就成了一个要凭记忆写 slug 的动作：提案是别的员工在
    /// 别的会话里提的，属主这一段对话里没有任何地方读得到它——「同一段对话里
    /// 看到」就只剩下提案人自己在场的那一次。
    #[tokio::test]
    async fn the_owner_sees_what_is_waiting_and_then_what_is_in_force() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), true);

        let empty = call(&tools, LIST_RUNTIME_PRINCIPLES, serde_json::json!({})).await;
        assert_eq!(empty["pending"], 0, "{empty}");
        assert_eq!(empty["in_force"], 0, "{empty}");

        call(&tools, PROPOSE_PRINCIPLE, proposal("build-before-merge")).await;
        let waiting = call(
            &tools,
            LIST_RUNTIME_PRINCIPLES,
            serde_json::json!({ "state": "proposed" }),
        )
        .await;
        assert_eq!(waiting["pending"], 1, "{waiting}");
        assert_eq!(
            waiting["rules"][0]["slug"], "build-before-merge",
            "{waiting}"
        );
        assert_eq!(
            waiting["rules"][0]["in_force"], false,
            "提案被读成了生效：{waiting}"
        );
        assert!(
            waiting["note"]
                .as_str()
                .unwrap_or_default()
                .contains("slug"),
            "没有告诉它拍板要用哪个名字：{}",
            waiting["note"]
        );

        call(
            &tools,
            APPROVE_PRINCIPLE,
            serde_json::json!({ "slug": "build-before-merge" }),
        )
        .await;
        let live = call(&tools, LIST_RUNTIME_PRINCIPLES, serde_json::json!({})).await;
        assert_eq!(live["pending"], 0, "{live}");
        assert_eq!(live["in_force"], 1, "{live}");
        assert_eq!(live["rules"][0]["in_force"], true, "{live}");
    }

    /// 读面只认账上的四种拼法：别的词回一句「没有这一态」，不猜成某一种。
    #[tokio::test]
    async fn an_unknown_state_word_is_refused_instead_of_guessed() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account), "E0412".into(), false);

        let answer = call(
            &tools,
            LIST_RUNTIME_PRINCIPLES,
            serde_json::json!({ "state": "draft" }),
        )
        .await;

        assert_eq!(answer["refused"], "unknown_state", "{answer}");
        assert!(
            answer.get("rules").is_none(),
            "猜了一态照样读了账：{answer}"
        );
    }

    /// 否决留在账上，而且同一个名字可以按新文本重新提案。
    ///
    /// 「当时没同意」本身是一条记录，不是待办；但把它做成一次性用掉的编号，下一版
    /// 同一个名字就永远提不上来——被否决的规则往往正是最该改的那几条。
    #[tokio::test]
    async fn a_rejected_proposal_stays_on_the_account_and_may_come_back() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), true);

        call(&tools, PROPOSE_PRINCIPLE, proposal("build-before-merge")).await;
        let rejected = call(
            &tools,
            REJECT_PRINCIPLE,
            serde_json::json!({ "slug": "build-before-merge" }),
        )
        .await;
        assert_eq!(rejected["moved"], true, "{rejected}");
        assert_eq!(
            rejected["in_force"], false,
            "否决的东西不能报成生效：{rejected}"
        );
        assert!(account.in_force().await.unwrap().is_empty());

        let again = call(&tools, PROPOSE_PRINCIPLE, proposal("build-before-merge")).await;
        assert_eq!(
            again["proposed"], true,
            "被否决的名字提不上来第二次：{again}"
        );
        assert_eq!(
            account.state_of("build-before-merge"),
            Some(cog_core::RuntimePrincipleState::Proposed)
        );
    }

    /// 生效中的规则不能被同一行静默改写：提不进去，而且说的是为什么。
    ///
    /// 改一条原则是「先提新的、批了再撤旧的」。允许第二次提案覆盖，就等于允许任何人
    /// 不经拍板改掉一条属主批过的规则，而账上看不出它被改过。
    #[tokio::test]
    async fn a_live_rule_is_not_overwritten_by_proposing_the_same_slug_again() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), true);

        call(&tools, PROPOSE_PRINCIPLE, proposal("build-before-merge")).await;
        call(
            &tools,
            APPROVE_PRINCIPLE,
            serde_json::json!({ "slug": "build-before-merge" }),
        )
        .await;
        let answer = call(
            &tools,
            PROPOSE_PRINCIPLE,
            serde_json::json!({
                "slug": "build-before-merge",
                "trigger": "任何时候",
                "criterion": "我说了算",
                "action": "照新的做"
            }),
        )
        .await;

        assert_eq!(answer["proposed"], false, "{answer}");
        assert_eq!(answer["refused"], "not_filed", "{answer}");
        assert!(
            answer["note"]
                .as_str()
                .unwrap_or_default()
                .contains("Conflict")
                || answer["note"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("已经是"),
            "没有说出账拒绝它的理由：{}",
            answer["note"]
        );
        let live = account.in_force().await.unwrap();
        assert_eq!(live[0].criterion, "构建是不是绿的", "生效的那一版被改了");
    }

    /// 没有登录身份的连接批准不了任何东西：那两只工具根本不注册。
    ///
    /// 注册了也挡不住模型去调；账上会多出一行「由一位员工替属主做的决定」，而
    /// 读账的人分不出它和真的拍板。
    #[test]
    fn a_connection_without_a_login_identity_cannot_decide() {
        let account = Arc::new(FakePrinciples::default());
        for owner_present in [false, true] {
            let names: Vec<String> =
                principle_tools(Some(account.clone()), "E0412".into(), owner_present)
                    .into_iter()
                    .map(|tool| tool.name)
                    .collect();
            if owner_present {
                assert_eq!(
                    names,
                    vec![
                        LIST_RUNTIME_PRINCIPLES.to_string(),
                        PROPOSE_PRINCIPLE.to_string(),
                        APPROVE_PRINCIPLE.to_string(),
                        REJECT_PRINCIPLE.to_string()
                    ],
                    "有人在的时候四只都在"
                );
            } else {
                assert_eq!(
                    names,
                    vec![
                        LIST_RUNTIME_PRINCIPLES.to_string(),
                        PROPOSE_PRINCIPLE.to_string()
                    ],
                    "没有人的连接拿到了拍板工具：{names:?}"
                );
            }
        }
    }

    /// 拍一条不在账上等着拍的提案：动没动要分开说，不能报成「批了」。
    ///
    /// 重复调用、名字写错、已经批过——三种情况账上都回「没动」。把它们回成成功，
    /// 模型就会对属主说「已经批准了」，而生效集里没有这一条。
    #[tokio::test]
    async fn deciding_a_slug_that_is_not_waiting_does_not_claim_it_moved() {
        let account = Arc::new(FakePrinciples::default());
        let tools = principle_tools(Some(account.clone()), "E0412".into(), true);

        let answer = call(
            &tools,
            APPROVE_PRINCIPLE,
            serde_json::json!({ "slug": "never-proposed" }),
        )
        .await;

        assert_eq!(answer["moved"], false, "{answer}");
        assert_eq!(answer["in_force"], false, "{answer}");
        assert!(
            answer["note"]
                .as_str()
                .unwrap_or_default()
                .contains("不要宣布"),
            "没有提醒它别宣布生效：{}",
            answer["note"]
        );
    }

    /// 一轮被放下时，名册上那个人也要下线。
    ///
    /// 一轮对话是一个 future。界面按下停止、连接被丢掉，这个 future 在中间就被放下
    /// 了，`release` 那一行走不到——而心跳是一条独立的任务，不停它，名册上这个人
    /// 会一直「在岗」并每十秒续一次约。名单上多一个永远醒着、却再也不回答的人，
    /// 比少一行更坏：同事发给他的话投进一个没人读的收件箱。
    #[tokio::test]
    async fn a_turn_that_is_dropped_also_takes_its_name_off_the_roster() {
        #[derive(Default)]
        struct CountingRoster {
            heartbeats: AtomicUsize,
            deregistrations: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl cog_core::AgentRegistry for CountingRoster {
            async fn register(&self, _r: &cog_core::AgentRegistration) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn heartbeat(&self, _agent_id: &str) -> cog_core::SFResult<()> {
                self.heartbeats.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn deregister(&self, _agent_id: &str) -> cog_core::SFResult<()> {
                self.deregistrations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn get(
                &self,
                _agent_id: &str,
            ) -> cog_core::SFResult<Option<cog_core::AgentRegistration>> {
                Ok(None)
            }
            async fn list(&self) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
            async fn list_by_role(
                &self,
                _role: &str,
            ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
        }

        use std::sync::atomic::{AtomicUsize, Ordering};

        let roster = Arc::new(CountingRoster::default());
        let employee = employee_named(Some("inst-7f3a"), "cancelled-turn");
        let registry: Arc<dyn cog_core::AgentRegistry> = roster.clone();
        let lease = EmployeeLease::take(registry, &employee).await;
        // 心跳的第一拍是立刻到的：先让它真的开始续约，再把它放下。
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            roster.heartbeats.load(Ordering::SeqCst) >= 1,
            "心跳根本没跑起来"
        );

        drop(lease);
        for _ in 0..100 {
            if roster.deregistrations.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            roster.deregistrations.load(Ordering::SeqCst),
            1,
            "那一轮被放下了，名册上这个人还在：下一次打开名单会读到他「在岗」"
        );

        let beats = roster.heartbeats.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            roster.heartbeats.load(Ordering::SeqCst),
            beats,
            "心跳还在替一个已经不在的人续约"
        );
    }

    /// 对话员工在名册上登记成「一个人」，不是「这一轮在说」。
    ///
    /// 角色格曾经写 `chat`——那是「这一轮正在说话」这个状态，被读成名册上的角色。
    /// 于是同一个工号上线一次是 `chat`、别处读到的是另一个词，两次读数对不上；而且
    /// 名册这一格从此同时装两种量：人的身份与这一轮在干什么。定稿模型里角色是这一段
    /// 活里的职务分工，写在任职与账行（`task_attempt.role`）上；人自己的名册行只答
    /// 「是谁」。所以对话员工登记成 `EMPLOYEE_ROLE`，而且名册角色的闭集里不再有
    /// `chat` 与 `probe`——前者是状态，后者只在测试夹具里出现过。
    #[tokio::test]
    async fn a_conversation_registers_as_a_person_not_as_a_purpose_label() {
        #[derive(Default)]
        struct RecordingRoster {
            roles: std::sync::Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl cog_core::AgentRegistry for RecordingRoster {
            async fn register(&self, r: &cog_core::AgentRegistration) -> cog_core::SFResult<()> {
                self.roles
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(r.role.clone());
                Ok(())
            }
            async fn heartbeat(&self, _agent_id: &str) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn deregister(&self, _agent_id: &str) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn get(
                &self,
                _agent_id: &str,
            ) -> cog_core::SFResult<Option<cog_core::AgentRegistration>> {
                Ok(None)
            }
            async fn list(&self) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
            async fn list_by_role(
                &self,
                _role: &str,
            ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
        }

        let roster = Arc::new(RecordingRoster::default());
        let employee = employee_named(Some("inst-9c21"), "role-turn");
        let registry: Arc<dyn cog_core::AgentRegistry> = roster.clone();
        let lease = EmployeeLease::take(registry, &employee).await;
        drop(lease);

        let roles = roster
            .roles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(
            roles,
            vec![cog_gateway::agents::EMPLOYEE_ROLE.to_string()],
            "对话员工登记的角色应是人这个角色"
        );
        for role in &roles {
            assert!(
                role == cog_gateway::agents::EMPLOYEE_ROLE
                    || cog_core::roles::ALL.contains(&role.as_str()),
                "名册角色是闭集：人的身份词或小队／带外单元的角色名，出现「{role}」就是又一种角色"
            );
            assert!(
                role != "chat" && role != "probe",
                "「{role}」不是角色：前者是这一轮在说的状态，后者只在测试夹具里出现"
            );
        }
    }

    /// 账缺席时两只工具都如实说「读不到」，不假装提案成功或拍板成功。
    ///
    /// 与三本只读账同一句口径：把缺席说成「没有」会让这条活谁也不会再去问。
    #[tokio::test]
    async fn an_absent_account_says_it_cannot_read_rather_than_filing_anything() {
        let tools = principle_tools(None, "E0412".into(), true);
        for (name, args) in [
            (LIST_RUNTIME_PRINCIPLES, serde_json::json!({})),
            (PROPOSE_PRINCIPLE, proposal("build-before-merge")),
            (
                APPROVE_PRINCIPLE,
                serde_json::json!({ "slug": "build-before-merge" }),
            ),
            (
                REJECT_PRINCIPLE,
                serde_json::json!({ "slug": "build-before-merge" }),
            ),
        ] {
            let answer = call(&tools, name, args).await;
            assert_eq!(answer["available"], false, "{name}: {answer}");
            assert!(
                answer["note"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("读不到"),
                "{name} 把账缺席说成了别的什么：{}",
                answer["note"]
            );
        }
    }

    /// 把一条连接上的问答记下来，好在问完之后看问的是什么。
    struct ARecordingBridge {
        /// 人这一次怎么答。放在可变格里，是因为同一条连接上先后几问可以有不同答案
        /// （先「总是允许」、后「总是拒绝」），而那正是规则该被证明的路径。
        answer: std::sync::Mutex<cog_gateway::chat::PermissionAnswer>,
        seen: std::sync::Mutex<Vec<cog_gateway::chat::PermissionAskView>>,
    }

    impl ARecordingBridge {
        fn answering(answer: cog_gateway::chat::PermissionAnswer) -> Arc<Self> {
            Arc::new(Self {
                answer: std::sync::Mutex::new(answer),
                seen: std::sync::Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl cog_gateway::chat::PermissionBridge for ARecordingBridge {
        async fn ask(
            &self,
            view: cog_gateway::chat::PermissionAskView,
        ) -> cog_gateway::chat::PermissionAnswer {
            self.seen.lock().unwrap().push(view);
            self.answer.lock().unwrap().clone()
        }
    }

    /// 屏幕上那一行先说它要动什么。
    ///
    /// 只报工具名的话，连续十次 `write_file` 在对话框里是十行一样的字，人看不出
    /// 这一问与上一问的区别，于是「确认」退化成闭眼点允许。界还必须有：一条询问
    /// 不能把整份文件正文顶进对话框。
    #[test]
    fn a_question_leads_with_what_it_is_about_to_touch() {
        let summary = permission_summary(&serde_json::json!({
            "command": "cargo test -p cog-agent",
            "cwd": "/opt/cogneva/sandbox",
        }));
        assert!(
            summary.starts_with("command=cargo test"),
            "最要紧的那一格没有排在前面：{summary}"
        );

        // 批准前的摘要必须整段：属主按它点头，截掉的那部分就是「他没看到却同意」。
        let huge = "x".repeat(2000);
        let whole = permission_summary(&serde_json::json!({ "path": huge }));
        assert!(
            whole.chars().count() > 2000 && !whole.ends_with('…'),
            "摘要被截断了：{} 字符，末尾 {}",
            whole.chars().count(),
            whole.chars().last().unwrap_or(' ')
        );
    }

    /// 人答的那一句原样变成内核的判词：允许就执行，拒绝连理由一起交回模型。
    ///
    /// 这一层是内核与那条 socket 之间唯一的翻译。它把拒绝翻成允许（或者把理由
    /// 吃掉）时，屏幕上点头的那个人说的话就不作数了，而内核那边每一步都还在按
    /// 「有人说可以」往下跑。
    #[tokio::test]
    async fn the_answer_becomes_the_verdict_the_kernel_acts_on() {
        let allow = ARecordingBridge::answering(cog_gateway::chat::PermissionAnswer::allow());
        let decider = ConversationPermissions {
            bridge: allow.clone(),
            session_id: "session-42".into(),
            owner: Some("alice".into()),
            store: None,
            rules: Default::default(),
        };
        let ask = cog_agent::permission::PermissionAsk {
            call_id: "call-1".into(),
            agent_id: "agent-7".into(),
            tool: "write_file".into(),
            arguments: serde_json::json!({ "path": "crates/x.rs" }),
            mode: cog_agent::permission::PermissionMode::Default,
        };
        let verdict =
            <ConversationPermissions as cog_agent::permission::PermissionDecider>::decide(
                &decider, &ask,
            )
            .await;
        assert!(
            matches!(verdict, cog_agent::permission::PermissionVerdict::Allow),
            "人答了允许，内核却拿到拒绝"
        );
        {
            let seen = allow.seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].session_id, "session-42", "问句要落到这条会话上");
            assert_eq!(seen[0].tool, "write_file");
            assert_eq!(seen[0].mode, "default");
            assert!(
                seen[0].request_id.starts_with("perm-"),
                "请求号要能带回来寻址：{}",
                seen[0].request_id
            );
        }

        let deny = ARecordingBridge::answering(cog_gateway::chat::PermissionAnswer::Deny {
            always: false,
            reason: "这一步先别动这个文件".into(),
        });
        let decider = ConversationPermissions {
            bridge: deny,
            session_id: "session-42".into(),
            owner: Some("alice".into()),
            store: None,
            rules: Default::default(),
        };
        match <ConversationPermissions as cog_agent::permission::PermissionDecider>::decide(
            &decider, &ask,
        )
        .await
        {
            cog_agent::permission::PermissionVerdict::Deny { reason } => {
                assert_eq!(
                    reason, "这一步先别动这个文件",
                    "拒绝的理由被吃掉了：模型收到的会是一句没有来由的失败"
                );
            }
            other => panic!("人答了拒绝，内核却拿到 {other:?}"),
        }
    }

    fn a_call(tool: &str, arguments: serde_json::Value) -> cog_agent::permission::PermissionAsk {
        cog_agent::permission::PermissionAsk {
            call_id: "call-1".into(),
            agent_id: "agent-7".into(),
            tool: tool.into(),
            arguments,
            mode: cog_agent::permission::PermissionMode::Default,
        }
    }

    /// 这条连接的某一轮：拿着 store 里这一格的规则，问人在不在。
    async fn decider_over(
        bridge: Arc<ARecordingBridge>,
        store: Option<Arc<dyn cog_core::KeyValueStore>>,
        owner: Option<&str>,
        session: &str,
    ) -> ConversationPermissions {
        let rules = match store.as_ref() {
            Some(kv) => crate::permission_rules::load(kv, owner, session).await,
            None => Default::default(),
        };
        ConversationPermissions {
            bridge,
            session_id: session.into(),
            owner: owner.map(str::to_string),
            store,
            rules: std::sync::Mutex::new(rules),
        }
    }

    async fn verdict_of(
        decider: &ConversationPermissions,
        ask: &cog_agent::permission::PermissionAsk,
    ) -> cog_agent::permission::PermissionVerdict {
        <ConversationPermissions as cog_agent::permission::PermissionDecider>::decide(decider, ask)
            .await
    }

    /// 人答过一次「总是允许」之后，同一个工具碰同一个东西不再问；碰别的东西照问。
    ///
    /// 这条路径没有规则就整条不存在：每次调用都会重新弹一次窗，人点到第十次时
    /// 那句「允许」已经不是回答，只是肌肉记忆。
    #[tokio::test]
    async fn an_answer_to_always_allow_stops_the_next_lookalike_from_being_asked() {
        let bridge = ARecordingBridge::answering(cog_gateway::chat::PermissionAnswer::Allow {
            always: true,
        });
        let decider = decider_over(bridge.clone(), None, Some("alice"), "session-42").await;

        let command = serde_json::json!({ "command": "cargo test -p cog-agent" });
        assert!(matches!(
            verdict_of(&decider, &a_call("run_command", command.clone())).await,
            cog_agent::permission::PermissionVerdict::Allow
        ));
        assert!(matches!(
            verdict_of(&decider, &a_call("run_command", command.clone())).await,
            cog_agent::permission::PermissionVerdict::Allow
        ));
        assert_eq!(
            bridge.seen.lock().unwrap().len(),
            1,
            "第二次同样的调用不该再问人"
        );

        // 同一个工具、另一个命令：还是得问。
        assert!(matches!(
            verdict_of(
                &decider,
                &a_call(
                    "run_command",
                    serde_json::json!({ "command": "cargo publish" })
                )
            )
            .await,
            cog_agent::permission::PermissionVerdict::Allow
        ));
        assert_eq!(
            bridge.seen.lock().unwrap().len(),
            2,
            "记住的是这一次碰的那个东西，不是这个工具的全部"
        );
    }

    /// 「总是拒绝」写下的是一条拒绝规则，下一次同类调用连问都不问就拒绝——而它的理由
    /// 说明是被哪条规则拦下的，不是那个「没人回答」的兜底句。
    #[tokio::test]
    async fn a_remembered_denial_refuses_without_asking_again() {
        let bridge = ARecordingBridge::answering(cog_gateway::chat::PermissionAnswer::Deny {
            reason: "别跑这个".into(),
            always: true,
        });
        let store: Arc<dyn cog_core::KeyValueStore> =
            Arc::new(crate::permission_rules::MemoryKv::default());
        let decider = decider_over(bridge.clone(), Some(store.clone()), Some("alice"), "s1").await;

        let ask = a_call(
            "run_command",
            serde_json::json!({ "command": "rm -rf build" }),
        );
        assert!(matches!(
            verdict_of(&decider, &ask).await,
            cog_agent::permission::PermissionVerdict::Deny { .. }
        ));
        // 下一条连接、另一段对话：规则从存储里读回来，仍然算数。
        let later = decider_over(
            ARecordingBridge::answering(cog_gateway::chat::PermissionAnswer::allow()),
            Some(store),
            Some("alice"),
            "s2",
        )
        .await;
        match verdict_of(&later, &ask).await {
            cog_agent::permission::PermissionVerdict::Deny { reason } => {
                assert!(
                    reason.contains("规则"),
                    "拒绝的理由要指向那条规则：{reason}"
                )
            }
            other => panic!("留下来的拒绝规则没有生效：{other:?}"),
        }
        assert_eq!(bridge.seen.lock().unwrap().len(), 1, "只问过一次");
    }

    /// 两条规则同时命中时拒绝说了算，而且撤销之后重新开始问——这是这一整套的门。
    #[tokio::test]
    async fn a_deny_rule_wins_over_an_allow_rule_and_revoking_brings_the_question_back() {
        use cog_agent::permission::{PermissionRule, PermissionScope};

        let store: Arc<dyn cog_core::KeyValueStore> =
            Arc::new(crate::permission_rules::MemoryKv::default());
        let rule = |allow: bool| PermissionRule {
            tool: "run_command".into(),
            subject: Some("cargo test".into()),
            allow,
            scope: PermissionScope::Owner,
        };
        crate::permission_rules::remember(&store, Some("alice"), rule(true))
            .await
            .unwrap();
        crate::permission_rules::remember(&store, Some("alice"), rule(false))
            .await
            .unwrap();

        let bridge = ARecordingBridge::answering(cog_gateway::chat::PermissionAnswer::allow());
        let decider = decider_over(bridge.clone(), Some(store.clone()), Some("alice"), "s1").await;
        let ask = a_call(
            "run_command",
            serde_json::json!({ "command": "cargo test -p cogneva" }),
        );
        assert!(
            matches!(
                verdict_of(&decider, &ask).await,
                cog_agent::permission::PermissionVerdict::Deny { .. }
            ),
            "两条规则都命中时必须是拒绝"
        );
        assert!(bridge.seen.lock().unwrap().is_empty(), "有规则就不该再问");

        // 撤掉那条拒绝的，问句回来；剩下那条放行规则照旧生效。
        crate::permission_rules::revoke(&store, Some("alice"), "s1", &rule(false).id()).await;
        let after = decider_over(bridge.clone(), Some(store.clone()), Some("alice"), "s1").await;
        assert!(matches!(
            verdict_of(&after, &ask).await,
            cog_agent::permission::PermissionVerdict::Allow
        ));
        // 两条都撤掉，问句就真的回来了。
        crate::permission_rules::revoke(&store, Some("alice"), "s1", &rule(true).id()).await;
        let bare = decider_over(bridge.clone(), Some(store), Some("alice"), "s1").await;
        assert!(matches!(
            verdict_of(&bare, &ask).await,
            cog_agent::permission::PermissionVerdict::Allow
        ));
        assert_eq!(
            bridge.seen.lock().unwrap().len(),
            1,
            "撤掉全部规则之后，同类调用要重新问人"
        );
    }
}

/// 这一轮里属主说的那一句（最后一条用户消息里的文字块）。没有文字（只带了附件）
/// 就不算「说了一句话」——意图行要的是原文。
fn last_user_text(messages: &[cog_core::Message]) -> Option<String> {
    messages.iter().rev().find_map(|message| match message {
        cog_core::Message::User { content, .. } => {
            let text: String = content
                .iter()
                .filter_map(|block| match block {
                    cog_core::ContentBlock::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            (!text.trim().is_empty()).then_some(text)
        }
        _ => None,
    })
}

/// 这一段对话说出口的那一句，落到哪一行意图上。
///
/// `addressed_to` 是**正在和属主对话的那个智能体**，不是某个员工的泛指，也不是
/// 客户端自报的字段；身份只由会话和原文决定，同一句重投是同一行。
fn chat_intent_row(
    session_id: &str,
    owner: &str,
    employee_id: &str,
    arrival: &str,
    text: &str,
    model_text: Option<&str>,
) -> cog_core::IntentRow {
    let mut row = cog_core::IntentRow::new(
        "chat",
        cog_core::chat_intent_identity(session_id, arrival),
        owner,
        employee_id,
        text,
        None,
    );
    // 这一句属于哪一段会话：会话是投影，这一格是它的归属。别的入口（外部情报、
    // A2A）没有会话，那一格空着，不编一个。
    row.session = Some(session_id.to_string());
    // 命令展开之后交给模型的那一段：与原话不同才写。会话表坏掉之后，「模型当时
    // 看到的是什么」只有这一格答得出来。
    row.model_text = model_text.map(str::to_string);
    row
}

#[cfg(test)]
mod chat_intent_row_tests {
    use super::chat_intent_row;

    /// 收件人是正在和属主对话的那个智能体，不是空的，也不是客户端自报的字段。
    #[test]
    fn the_row_names_the_agent_in_the_conversation() {
        let row = chat_intent_row("s1", "owner", "luna-1", "a-1", "把这件事办了", None);
        assert_eq!(row.source, "chat");
        assert_eq!(row.said_by, "owner");
        assert_eq!(row.addressed_to, "luna-1");
        assert_eq!(row.text, "把这件事办了");
        assert!(row.identity.starts_with("chat:s1:"));
    }

    /// 同一段会话里的同一句是同一行；换会话或换原文才是另一行。
    #[test]
    fn the_same_sentence_in_the_same_session_is_one_row() {
        let a = chat_intent_row("s1", "owner", "luna-1", "m-1", "改回来", None);
        let b = chat_intent_row("s1", "owner", "luna-1", "m-1", "改回来", None);
        let c = chat_intent_row("s2", "owner", "luna-1", "m-1", "改回来", None);
        let d = chat_intent_row("s1", "owner", "luna-1", "m-2", "改回去", None);
        assert_eq!(a.identity, b.identity);
        assert_ne!(a.identity, c.identity);
        assert_ne!(a.identity, d.identity);
    }
}

#[cfg(test)]
mod session_for_employee_tests {
    use super::{employee_for_target, session_for_employee, session_target};

    /// 员工页的会话 id 由员工 id 派生：同一员工永远同一段，换员工才换会话。
    #[test]
    fn the_session_id_is_derived_from_the_employee_id() {
        let session = session_for_employee("employee:1");
        assert_eq!(session, "agent:employee:1");
        assert_eq!(session_target(&session), Some("employee:1"));
        assert_eq!(session_for_employee("employee:1"), session);
        assert_ne!(
            session_for_employee("employee:1"),
            session_for_employee("employee:2")
        );
        assert_eq!(session_target("随机会话"), None);
    }

    /// 没有身份绑定表时，会话前缀仍然决定是谁：员工页点谁就是谁。
    ///
    /// 这一支走的是纯函数 `employee_for`（进程没装 KV 时装配层用的就是它）：
    /// 少了它，同一段 `agent:E0412` 会落到一个由实例指纹算出来的陌生人头上，
    /// 而员工页点的那一行与答话的人从此不是同一个。
    #[test]
    fn the_prefix_names_the_employee_even_without_an_identity_store() {
        let employee = super::employee_for(&session_for_employee("E0412"));
        assert_eq!(employee.id, "E0412");
        assert_eq!(employee.handle, "E0412");
    }

    /// 目标会话钉住的就是被点名的那个员工，不是属主的默认员工。
    #[test]
    fn a_target_employee_keeps_the_id_it_was_addressed_by() {
        let employee = employee_for_target("employee:1");
        assert_eq!(employee.id, "employee:1");
        assert_eq!(employee.handle, "employee:1");
    }
}

#[cfg(test)]
mod inbox_intent_tests {
    use super::inbox_intent;
    use cog_core::InboxMessage;

    /// 同事发来的一句要办的话是意图：原话不改写，发信人随行；没留名就如实空着。
    /// 撤停与重置不是某人交办的一件事，不落意图行。
    #[test]
    fn a_colleagues_message_is_an_intent_with_its_sender() {
        let prompt = InboxMessage::Prompt {
            goal: serde_json::json!("把那一处改掉"),
            reply_stream: None,
            from_agent: Some("E0412".into()),
            intent: None,
        };
        assert_eq!(
            inbox_intent(&prompt),
            Some(("E0412".to_string(), "把那一处改掉".to_string()))
        );
        let unnamed = InboxMessage::Steering {
            instruction: "先别动那一处".into(),
            from_agent: None,
        };
        assert_eq!(
            inbox_intent(&unnamed),
            Some((String::new(), "先别动那一处".to_string())),
            "没留名不是编一个名字，也不是把这句话丢掉"
        );
        assert_eq!(inbox_intent(&InboxMessage::Abort), None);
        assert_eq!(inbox_intent(&InboxMessage::Reset), None);
    }
}

#[cfg(test)]
mod intent_instruction_tests {
    use super::{intent_instruction, CONVERSATION_INTENT_INSTRUCTION};

    /// 句首的 `/task`、`/chat` 是属主的显式判词：说了算，不再让模型猜。
    #[test]
    fn a_leading_switch_says_what_the_turn_is() {
        let task = intent_instruction("/task 把那一处改掉");
        assert!(task.contains("必须调用 start_task"), "{task}");
        let chat = intent_instruction("  /chat 今天怎么样");
        assert!(chat.contains("不要调用 start_task"), "{chat}");
    }

    /// 没打开关时逐字是原来那一句：开关不是新的默认。
    #[test]
    fn a_turn_without_a_switch_keeps_the_ordinary_instruction() {
        assert_eq!(
            intent_instruction("把那一处改掉"),
            CONVERSATION_INTENT_INSTRUCTION
        );
    }

    /// 句子中间提到开关词不算开关：替属主做一个他没做的决定，比让他多打一次
    /// `/task` 更贵。
    #[test]
    fn a_switch_word_inside_a_sentence_is_not_a_switch() {
        for turn in ["请解释 /task 是什么意思", "/tasking 不是这个开关"] {
            assert_eq!(
                intent_instruction(turn),
                CONVERSATION_INTENT_INSTRUCTION,
                "{turn:?} 被读成了开关"
            );
        }
    }
}

#[cfg(test)]
mod may_be_work_tests {
    use super::may_be_work;

    /// 带「可能是一件活」动词的一句：不标接手，留给下一批分诊重判。
    #[test]
    fn a_sentence_with_a_work_verb_is_left_for_the_batch_judge() {
        for text in [
            "把这处改掉",
            "帮我部署一下",
            "fix the failing test",
            "查一下日志",
        ] {
            assert!(may_be_work(text), "{text:?} 被读成了纯闲聊");
        }
    }

    /// 没有动词的问候与感想：照常标掉，闲聊仍然零额外调用。
    #[test]
    fn a_greeting_is_not_left_for_the_batch_judge() {
        for text in ["今天怎么样", "你好", "谢谢你", "what is this"] {
            assert!(!may_be_work(text), "{text:?} 被读成了可能是一件活");
        }
    }
}
