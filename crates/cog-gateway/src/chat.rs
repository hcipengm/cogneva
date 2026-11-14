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

/// 一条长消息落成的文件放在哪。这个路径由**执行器**解释：写入与读回都走同一个
/// 沙箱后端（模型用 `read_file` 读绝对路径，执行器原样认它），所以这里写死一个
/// 两边都在的位置，而不是这个进程自己的临时目录——网关与执行器未必在同一台机器上。
pub const ATTACHMENT_ROOT: &str = "/tmp/cogneva-chat-attachments";

/// 一帧能带多大。传输库自己用的就是这个数（axum/tungstenite 的 64 MiB 默认），
/// 这里把它写出来只为一件事：阈值要读它。不写出来的话「帧上限」在阈值那一段
/// 是一个没人写过的数，两个数之间也就没有关系可言。
// cap: fixed — 传输层的协议默认值，不是谁挑的拨盘；钉出来让阈值读得到它
pub const MAX_INBOUND_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// UTF-8 一个字最多几个字节。把字节上限折成字符时按最坏的算，读出来的字数就
/// 不会比真到手的多。
// cap: fixed — UTF-8 编码本身给的上界
const MAX_UTF8_BYTES_PER_CHAR: usize = 4;

/// 这一轮窗口里，一句用户话最多占到几分之一。
///
/// 窗口不只装这一句话：系统前缀、工具表、前面的历史与这一轮的回答都住在里面。
/// 一句话可以长，但不能长到把回答挤出去——超过这个份额的，原文落成文件，循环
/// 拿到的是引用与开头。
// cap: bound — 窗口分给一句提问的份额，剩下来的要装前缀、工具表、历史与回答
const INPUT_SHARE_OF_CONTEXT: usize = 8;

/// 引用里留多长的开头：阈值的几分之一。够看出这条消息在讲什么，又不至于让
/// 「引用」自己再吃一大口窗口。
// cap: bound — 开头摘录占阈值的份额，与阈值同向变化
const HEAD_SHARE_OF_THRESHOLD: usize = 4;

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// 一条消息多大就不再整条进上下文，而是落成文件。
///
/// 两条读数取最紧的一条：
///
/// - **模型窗口**：这一段对话真正跑在多大的窗口里（由执行体报，见
///   [`ConversationTurn::context_window_tokens`]），留一份给一句提问。窗口大的
///   部署能把更长的消息直接读进来；窗口小的部署更早落文件。
/// - **帧上限**：一帧带得进来的字符数。超过它，消息连送都送不进来，落文件也就
///   无从谈起——所以它也是这条阈值的一个上界，哪怕今天它比窗口松。
///
/// 两条都是入参：换窗口、换帧上限，返回值跟着变（有一条测试就量这个）。
pub fn oversized_threshold_tokens(context_window_tokens: usize, frame_limit_bytes: usize) -> usize {
    // 字节 → 字符按最坏编码算，再按工作区唯一的口径（`cog_core::token_estimate`）
    // 折成 token。两步都用别的模块已经钉死的数，这里不自造一个换算。
    let frame_chars = frame_limit_bytes / MAX_UTF8_BYTES_PER_CHAR;
    let frame_tokens = frame_chars / cog_core::token_estimate::CHARS_PER_TOKEN;
    let context_tokens = context_window_tokens / INPUT_SHARE_OF_CONTEXT;
    frame_tokens.min(context_tokens).max(1)
}

/// 一段话落成的文件路径。
///
/// 按内容取哈希：同一段原文永远落在同一个路径上。这不是省事——记录里存的是原文
/// 本身，下一轮读历史时要把同一句话再折成引用，而那条路上没有「这条消息的文件
/// 在哪」的列可查；路径能由内容算回来，读回全文这件事就不依赖一张额外的表。
pub fn attachment_path(session_id: &str, content: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(content.as_bytes()));
    format!(
        "{ATTACHMENT_ROOT}/{}/{}",
        session_dir_name(session_id),
        &digest[..16]
    )
}

/// 会话号进目录名。会话号是客户端给的，直接拼进路径就是一条可以走出目录的边
/// （`../` 之类的字符在文件系统里是有含义的），所以只留名字里的字，其余落成 `-`。
fn session_dir_name(session_id: &str) -> String {
    let name: String = session_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if name.trim_matches('-').is_empty() {
        "unattributed".to_string()
    } else {
        name
    }
}

/// 一条落成文件的消息，循环看到的那一句。
///
/// 它不是截断后的正文：开头之后没有半句话，参考文自己说清「完整原文在哪、它有多
/// 长、这一段只是开头」，读它的模型因此知道自己少看了什么，以及去哪儿补。
pub fn oversized_reference(content: &str, path: &str, threshold_tokens: usize) -> String {
    let head_chars = (threshold_tokens / HEAD_SHARE_OF_THRESHOLD)
        .saturating_mul(cog_core::token_estimate::CHARS_PER_TOKEN)
        .max(1);
    let total = content.chars().count();
    let head: String = content.chars().take(head_chars).collect();
    format!(
        "[这条消息有 {total} 个字符，比这一轮能直接带进对话的量（约 {threshold_tokens} token）\
         大；原文已逐字存成文件 {path}，要看全文就用 read_file 读这个路径。下面是开头的一部分，\
         不是全文。]\n\n{head}"
    )
}

/// 一条已经落成文件的消息：界面认它、循环读它，用的都是这两格。
#[derive(Debug, Clone)]
pub struct OversizedAttachment {
    pub path: String,
    pub chars: usize,
    /// 循环看到的那一句（引用 + 开头 + 读回全文的路径）。
    pub reference: String,
}

/// 把原文逐字写进执行器的文件系统，并给出循环该看到的那一句。
///
/// 走的是模型自己读文件用的那个后端：写在这个进程的磁盘上，执行器那边的
/// `read_file` 读不到；写在同一份后端上，「落成文件」与「能读回」才是同一件事。
pub async fn archive_oversized(
    backend: Option<&Arc<dyn cog_core::SandboxBackend>>,
    session_id: &str,
    content: &str,
    threshold_tokens: usize,
) -> Result<OversizedAttachment, String> {
    let Some(backend) = backend else {
        return Err("这个部署没有装配沙箱后端，原文没有地方可写".into());
    };
    let path = attachment_path(session_id, content);
    let request = cog_core::SandboxRequest {
        task_id: format!("chat-attachment-{session_id}"),
        agent_id: String::new(),
        payload: cog_core::SandboxPayload::WriteFile {
            path: path.clone(),
            content: content.to_string(),
        },
        input: serde_json::Value::Null,
        limits: cog_core::ResourceLimits::default(),
    };
    match backend.execute(&request).await {
        Ok(result) if result.exit_code == 0 => Ok(OversizedAttachment {
            reference: oversized_reference(content, &path, threshold_tokens),
            path,
            chars: content.chars().count(),
        }),
        Ok(result) => Err(format!(
            "写入执行器失败（exit {}）：{}",
            result.exit_code,
            result.stderr.trim()
        )),
        Err(e) => Err(e.to_string()),
    }
}

/// 超过阈值就落文件，否则原样放过。
///
/// 阈值要问执行体自己的窗口；循环没接上（槽是空的）时这一轮本来就跑不了，也就不
/// 值得为它写文件——返回 `None`，让后面那条「还没接上」的话照原样说出去。
async fn archive_if_oversized(
    state: &GatewayState,
    session_id: &str,
    content: &str,
) -> Result<Option<OversizedAttachment>, String> {
    let Some(window) = state
        .conversation
        .get()
        .map(|turn| turn.context_window_tokens())
    else {
        return Ok(None);
    };
    let threshold = oversized_threshold_tokens(window, MAX_INBOUND_FRAME_BYTES);
    if cog_core::estimate_tokens(content) <= threshold {
        return Ok(None);
    }
    archive_oversized(
        state.sandbox_backend.as_ref(),
        session_id,
        content,
        threshold,
    )
    .await
    .map(Some)
}

/// 这一句交给循环的消息：正文（或长消息的引用）在前，附件块在后。
///
/// 顺序有含义：先说字，再看图。图排在前面时，模型会先描述图、再回答那句话，
/// 而人是拿图补充自己的话的。
///
/// 媒体块原样带上去，判「这个模型收不收」是协议边缘的事（`cog_llm::provider::media`
/// 按模型能力降级并留下说明）——网关在这里筛一遍，等于把「模型看不见这张图」变成
/// 一个静默的丢弃。
pub fn user_message(text: &str, attachments: &[cog_core::ChatAttachment]) -> cog_core::Message {
    user_message_with_mentions(text, &[], attachments)
}

/// 一句话，加上它点名的那几份文件，再加上随它上来的媒体。
///
/// 顺序与理由同 [`user_message`]：先是人说的字，然后是他点名的文件的内容，最后
/// 是图。文件排在媒体前面，因为一句话里「@ 的那份代码」与「这张截图」是两件事，
/// 前者要能顺着句子读下去。
pub fn user_message_with_mentions(
    text: &str,
    mentions: &[FileMention],
    attachments: &[cog_core::ChatAttachment],
) -> cog_core::Message {
    let mut blocks: Vec<cog_core::ContentBlock> =
        Vec::with_capacity(attachments.len() + mentions.len() + 1);
    if !text.is_empty() {
        blocks.push(cog_core::ContentBlock::text(text));
    }
    blocks.extend(
        mentions
            .iter()
            .map(|mention| cog_core::ContentBlock::text(&mention.block)),
    );
    blocks.extend(
        attachments
            .iter()
            .map(|attachment| cog_core::ContentBlock::Media {
                data: attachment.data.clone(),
                mime_type: attachment.mime_type.clone(),
            }),
    );
    // 全空在这里没有意义（调用方已经拦掉），但留一条比返回一条没有内容的消息好。
    if blocks.is_empty() {
        return cog_core::Message::user("");
    }
    cog_core::Message::user_blocks(blocks)
}

/// 一句话里点名了的文件：`@` 后面跟一串像路径的字。
///
/// 只认「词的起头」，且这一串里只有路径会有的字（字母数字、`._/-\`）——邮箱里
/// 那个 `@` 不该被当成点名。称呼与文件名在这里分不开（`@张三` 也可能真是一份
/// 中文名文件），所以它照样被当成一个名字去读：读不到就不铺开，模型手上仍是
/// 那句话本身。末尾的标点不属于路径：「看看 @src/main.rs。」里的句号是这句话的。
///
/// 重复的点名只读一次，顺序按第一次出现。
pub fn mentioned_paths(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (at, ch) in text.char_indices() {
        if ch != '@' {
            continue;
        }
        // 只认词的起头：邮箱里那个 `@` 前面紧贴着一个字，不算点名。
        if text[..at]
            .chars()
            .next_back()
            .is_some_and(|before| !before.is_whitespace())
        {
            continue;
        }
        let path: String = text[at + 1..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '\\'))
            .collect();
        // 句末的句号属于这句话，不属于路径。
        let path = path.trim_end_matches('.');
        if path.is_empty() {
            continue;
        }
        if out.iter().any(|seen| seen == path) {
            continue;
        }
        out.push(path.to_string());
    }
    out
}

/// 一份被 `@` 点名的文件，已经读成要交给模型的那一段字。
#[derive(Debug)]
pub struct FileMention {
    pub path: String,
    /// 正文本身，或一句「太长，去 read_file 它」。
    pub block: String,
}

/// 把点名的文件读上来。
///
/// 走的是员工自己 `read_file` 用的那个后端：读不到的路（文件不存在、二进制、
/// 后端没装）就跳过——这一句里 `@` 那条路径还在，模型仍然可以自己读它，跳过
/// 只是少了一次顺手的铺开，不是把话改掉。
///
/// 铺开的正文合计超过这一轮能直接带进来的量就不再铺开，改成一句「去读它」：
/// 一条消息点名五十个文件时，全铺进去的结果是整轮答不了，而铺开本来只省一次
/// `read_file`。
async fn read_mentions(
    backend: Option<&Arc<dyn cog_core::SandboxBackend>>,
    paths: &[String],
    threshold_tokens: usize,
) -> Vec<FileMention> {
    let Some(backend) = backend else {
        return Vec::new();
    };
    let mut mentions: Vec<FileMention> = Vec::new();
    let mut inlined = 0usize;
    for path in paths {
        let request = cog_core::SandboxRequest {
            task_id: format!("chat-mention-{path}"),
            agent_id: String::new(),
            payload: cog_core::SandboxPayload::ReadFile { path: path.clone() },
            input: serde_json::Value::Null,
            limits: cog_core::ResourceLimits::default(),
        };
        let Ok(result) = backend.execute(&request).await else {
            continue;
        };
        if result.exit_code != 0 {
            continue;
        }
        let tokens = cog_core::estimate_tokens(&result.stdout);
        if inlined.saturating_add(tokens) > threshold_tokens {
            mentions.push(FileMention {
                path: path.clone(),
                block: format!(
                    "[这些文件加起来超过这一轮能直接带进来的量（约 {threshold_tokens} token）；\
                     「{path}」的全文没有铺在这里，要看就用 read_file 读它。]"
                ),
            });
            continue;
        }
        inlined += tokens;
        mentions.push(FileMention {
            path: path.clone(),
            block: format!(
                "[用户在这句话里点名了这个文件：{path}，下面是它的全文]\n\n{}",
                result.stdout
            ),
        });
    }
    mentions
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

    /// 这一段对话真正跑在多大的窗口里（token）。
    ///
    /// 不是旁注：一句多长的用户话要落成文件，判据就是它。窗口只有执行体知道——
    /// 循环的构造在它手里——所以这条数值由它报，网关算阈值时读的就是这一个数。
    /// 报一个比真窗口大的数，会有消息没落文件却把窗口撑破；报一个比真窗口小的，
    /// 本来读得下的话被提前推到文件里。
    fn context_window_tokens(&self) -> usize;

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

    /// 这个人在过去的对话里写下的约定：下一轮开头自动进提示词的那几句。
    ///
    /// 名单属于人，不属于某一段对话：同一个人换一段对话读到的还是这几句。
    /// 默认是一张空名单——装配层没接存储时，界面上就该列不出来，而不是列一份猜的。
    fn owner_memory<'a>(
        &'a self,
        _owner: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<MemoryEntryView>> + Send + 'a>>
    {
        Box::pin(async { Vec::new() })
    }

    /// 记下一句约定，返回记完之后的整份名单。
    ///
    /// 返回名单而不是新那条：界面上唯一要画的就是这份名单，写没写进去由「新的
    /// 名单里多了它」表达。
    fn remember_owner_memory<'a>(
        &'a self,
        _owner: &'a str,
        _text: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<MemoryEntryView>> + Send + 'a>>
    {
        Box::pin(async { Vec::new() })
    }

    /// 撤掉一句约定，返回撤完之后的整份名单。
    fn forget_owner_memory<'a>(
        &'a self,
        _owner: &'a str,
        _id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<MemoryEntryView>> + Send + 'a>>
    {
        Box::pin(async { Vec::new() })
    }
}

/// 一句约定递到界面上时的样子。
///
/// 只有号与原文：它写的是什么，就是模型读到的那一句，这里不改写、不摘要——
/// 界面上显示的和提示词里载入的必须是同一句话，否则人没法核对「它到底读到了什么」。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemoryEntryView {
    pub id: String,
    pub text: String,
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
    /// 待批准的计划正文。只有「把计划递出去」那一问带它，别的询问是 `None`。
    ///
    /// 不并进 `summary`：摘要有自己的界（一行字、读一眼就够），而计划是要人读完
    /// 再点头的东西——截到一百来个字符，批准就变成了闭眼点。
    pub plan: Option<String>,
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
                plan: view.plan.clone(),
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

/// 这一轮从循环里读回来的东西：回复正文，它派出去的目标号，它翻开过的变更，
/// 以及它调过的工具。
///
/// 工具调用记在这里而不是只转给界面：帧活在连接里，页面一关就断了，而重开时要
/// 重画的那张待办面板只有这些调用能画出来。记的是**调用的结果**，不是「调用发生
/// 过」——面板要的是最后一次写进去的清单。
#[derive(Default)]
struct TurnJournal {
    reply: String,
    goals: Vec<String>,
    changes: Vec<String>,
    tool_calls: Vec<cog_core::ChatToolCall>,
    /// 这一轮是谁在说。实时那句身份由 `agent.identity` 帧带给界面；页面关掉之后
    /// 没有帧可读，记录是唯一还写着答案作者的地方。
    agent: Option<String>,
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
    // 调用号 → 这一笔在 `journal.tool_calls` 里的位置。结束那一帧只带调用号，
    // 结果要补回到开始那一帧记下的那一笔上，而不是另起一笔——另起一笔会让面板
    // 看到两次调用，其中一次永远没有结果。
    let mut call_at: HashMap<String, usize> = HashMap::new();
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
            // 第一个开口的就是这一轮的回答者：之后要是有别的执行者在同一程度上
            // 出声（派出去的活），那几句说的是那道目标的进展，不是这一问的回答。
            if journal.agent.is_none() {
                journal.agent = Some(agent_id.to_string());
            }
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
                args,
                timestamp,
                ..
            } => {
                in_flight.insert(tool_call_id.clone(), tool_name.clone());
                call_at.insert(tool_call_id.clone(), journal.tool_calls.len());
                journal.tool_calls.push(cog_core::ChatToolCall {
                    tool_call_id,
                    tool_name,
                    args: json_text(&args),
                    result: None,
                    status: cog_core::ChatToolCallStatus::Running,
                    started_at: timestamp,
                    finished_at: None,
                });
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
                is_error,
                timestamp,
                ..
            } => {
                // 先把这一笔收尾，再读它的名字去派发目标／变更那两帧：两件事用
                // 同一个调用号，但一件是「记下来给重开页面看」，另一件是「此刻
                // 发给界面的一帧」。
                if let Some(at) = call_at.remove(&tool_call_id) {
                    if let Some(call) = journal.tool_calls.get_mut(at) {
                        call.result = Some(json_text(&result));
                        call.status = if is_error {
                            cog_core::ChatToolCallStatus::Error
                        } else {
                            cog_core::ChatToolCallStatus::Ok
                        };
                        call.finished_at = Some(timestamp);
                    }
                }
                match in_flight.remove(&tool_call_id).as_deref() {
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
                }
            }
            _ => {}
        }
    }
    journal
}

/// 一帧上的 JSON 值折成记录里的那一列。
///
/// 存文本而不是值：页面拿记录与它实时画过的那一笔对照，重新编码一个值会在空格
/// 与键序上产生差别，而两笔看上去会是「同一次调用」。
fn json_text(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
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
    // 这条消息的原文落在哪个文件、有多长。不是每一条都有：只有超过窗口的消息才
    // 落文件，其余的是 `null`——界面上「附件」与「正文」因此能分开画。
    attachment: Option<(&str, usize)>,
) {
    emit(
        out,
        session_id,
        "message.start",
        serde_json::json!({
            "message_id": message_id,
            "role": role,
            "attachment": attachment.map(|(path, chars)| {
                serde_json::json!({ "path": path, "chars": chars })
            }),
        }),
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
    ledger: Option<&dyn cog_core::LlmUsageLedger>,
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
            // 这一段会话到目前为止的合计。单看这一轮的数是看不出一段对话花了多少
            // 的——每一轮只报自己，而读者要问的往往是「这段聊下来多少钱」。
            "session": session_spend_frame(ledger, session_id).await,
        }),
    )
    .await;
}

/// 这一轮读进了哪几句属主约定。
///
/// 与用量同一处分：数字来自循环的交付物，网关一个也不编。没有读面的部署根本
/// 不会有 `memory` 这一格，于是也不会有这一帧——「这一轮没有约定」与「这个平台
/// 不读约定」在屏幕上必须分得开：后者是配置还没接上，前者是这个人还没写。
async fn emit_turn_memory(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    memory: Option<&serde_json::Value>,
) {
    let Some(memory) = memory else { return };
    let loaded: Vec<serde_json::Value> = memory
        .get("loaded")
        .and_then(serde_json::Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let id = row.get("id").and_then(serde_json::Value::as_str)?;
                    let text = row.get("text").and_then(serde_json::Value::as_str)?;
                    Some(serde_json::json!({ "id": id, "text": text }))
                })
                .collect()
        })
        .unwrap_or_default();
    emit(
        out,
        session_id,
        "message.memory",
        serde_json::json!({
            "message_id": message_id,
            "loaded": loaded,
        }),
    )
    .await;
}

/// 这一段会话到目前为止花了多少：账本按会话那一列折叠出来的那一份。
///
/// 三种答案在屏幕上必须分得开：账本没接（`readable: false`）、这一段还没有行
/// （读到了，`calls: 0`）、有行。把前两个折成同一个 0，「这一段还没花」与「没
/// 读账本」就成了同一句话，而后者下一次会静默地一直显示「没花」。
///
/// 钱按币种分开列：价目本可能换过，把两种币折成一个和就是编一个汇率。
async fn session_spend_frame(
    ledger: Option<&dyn cog_core::LlmUsageLedger>,
    session: &str,
) -> serde_json::Value {
    let Some(ledger) = ledger else {
        return serde_json::json!({ "readable": false, "reason": "no_ledger" });
    };
    match ledger.spend_for_session(session).await {
        Ok(spend) => {
            let spend = spend.unwrap_or_default();
            serde_json::json!({
                "readable": true,
                "calls": spend.calls,
                "input_tokens": spend.tokens_input,
                "output_tokens": spend.tokens_output,
                "cached_tokens": spend.tokens_cached,
                "money": spend.money,
                "unpriced_calls": spend.unpriced_calls,
            })
        }
        Err(e) => {
            // 读失败不是一个读数：说「没读到」和说「花了零」是两句话，而这一段
            // 从下一次起会一直显示「花了零」。
            serde_json::json!({ "readable": false, "reason": e })
        }
    }
}

/// 额度拒绝的正文 → 一句话。
///
/// 上游代理把「哪一级、谁、已经花了多少、上限多少、什么时候恢复」写进了拒绝正文
/// （`reason` / `scope` / `target` / `spent_today` / `hard_limit` /
/// `retry_after_seconds`）。在这一条路径上它只会被当成一段错误文本糊进气泡，而
/// 读者要的正是那几个数：已经花了多少、上限是多少、等多久。
///
/// 认不出（不是额度拒绝）时返回 `None`，调用方照原样说「调用失败」——把一次网络
/// 故障硬翻译成「额度用完了」是最坏的那种误读。
///
/// 这句话里**不点时区**：日界是这台进程所在时钟的零点（`cog_core::day` 的约定，
/// 部署给整个集群设一个时钟），不是 UTC。写成「UTC 零点」在时钟不是 UTC 的部署里
/// 就是一句错的话，而读者唯一需要的时刻是 `retry_after_seconds` 给的那个等待。
fn owner_budget_refusal_sentence(error_text: &str) -> Option<String> {
    let start = error_text.find('{')?;
    // 拒绝正文可能被别的东西包着（前缀、尾随换行），只认第一段 JSON。
    let mut stream =
        serde_json::Deserializer::from_str(&error_text[start..]).into_iter::<serde_json::Value>();
    let body = stream.next()?.ok()?;
    if body.get("reason").and_then(serde_json::Value::as_str)
        != Some(cog_core::REFUSED_BY_OWNER_BUDGET)
    {
        return None;
    }
    let scope = body.get("scope").and_then(serde_json::Value::as_str)?;
    let target = body
        .get("target")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let spent = body.get("spent_today").and_then(serde_json::Value::as_u64);
    let limit = body.get("hard_limit").and_then(serde_json::Value::as_u64);
    let wait = body
        .get("retry_after_seconds")
        .and_then(serde_json::Value::as_u64);

    // 级别认的是执法面写进正文的那几个词（`QuotaScope::as_str`：user / ws /
    // team / org / global），不是英文全名：自己再拼一套名字，工作区那一级就会
    // 落进兜底、被说成「整个平台」，读者于是去找属主，而该找的是租户管理员。
    //
    // 认不出来的时候也不许说「整个平台」：那是替一个陌生的级别编一个主体。
    let who = match cog_core::QuotaScope::parse(scope) {
        Some(cog_core::QuotaScope::User) => format!("本人 {target}"),
        Some(cog_core::QuotaScope::Workspace) => format!("工作区 {target}"),
        Some(cog_core::QuotaScope::Team) => format!("团队 {target}"),
        Some(cog_core::QuotaScope::Organization) => format!("组织 {target}"),
        Some(cog_core::QuotaScope::Global) => "整个平台".to_string(),
        None => format!("{scope} 这一级"),
    };
    let spent_part = match (spent, limit) {
        (Some(spent), Some(limit)) => format!("今天已用 {spent} / 上限 {limit} tokens"),
        (Some(spent), None) => format!("今天已用 {spent} tokens"),
        _ => "今天的额度已经用完".to_string(),
    };
    let recovery = match wait {
        Some(secs) if secs >= 120 => format!("约 {} 分钟后恢复", secs.div_ceil(60)),
        Some(secs) => format!("约 {secs} 秒后恢复"),
        None => "日界之后恢复".to_string(),
    };
    Some(format!(
        "这一轮没有发出去：{who}的当日额度已用完（{spent_part}）。额度按日归零，\
         {recovery}；要提前恢复只能由属主调高这一级的上限。"
    ))
}

/// 一轮失败时屏幕上要说的那句话。
///
/// 只对额度拒绝做翻译，其余原样照抄并加上「调用失败」：一次网络故障被硬翻成
/// 「额度用完了」，读者会去找额度，而真正要修的是别处。
fn failure_text(error_text: &str) -> String {
    owner_budget_refusal_sentence(error_text)
        .unwrap_or_else(|| format!("LLM 调用失败：{error_text}"))
}

/// 这一段对话的配额余量：与管理面执法时读的是同一份判定。
///
/// 三种答案必须分得开（这正是这一格存在的理由）：
/// - 这个进程没接配额层级 → `503` + `readable: false`：读不到，不是「无限」；
/// - 接了、没人给这个人配过上限 → `200` + `enforced: false`：**未配置**，
///   `used_today` 照报（花销是账本的投影，与有没有上限无关）；
/// - 配过 → `200` + `enforced: true` 带 `remaining`。
///
/// 把「未配置」画成「剩余无限」是最坏的一种写法：它把「谁都没给这一格设过界」
/// 说成「这一格不需要界」，而执法面恰恰是「没配过就不拒」——两句话意思相反。
pub async fn quota_handler(
    axum::extract::State(state): axum::extract::State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let user_id = request
        .extensions()
        .get::<String>()
        .cloned()
        .unwrap_or_else(|| "anonymous".to_string());
    let Some(hierarchy) = state.hierarchy_manager.as_ref() else {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "readable": false,
                "reason": "no_hierarchy_manager",
                "note": "这个进程没有接配额层级，读不到任何一格上限",
            })),
        )
            .into_response();
    };
    // 与代理路径同一个读法：这个人能看到的两级是「本人」与「平台」。工作区那一
    // 级取决于这一发落在哪个租户，对话开始时还不知道——拒绝正文会说出它。
    let ctx = cog_core::QuotaContext {
        user_id: Some(user_id.clone()),
        global_id: Some(crate::GLOBAL_QUOTA_TARGET.to_string()),
        ..cog_core::QuotaContext::default()
    };
    let decision = hierarchy.check(&ctx, 0).await;
    let now_unix = chrono::Utc::now().timestamp();
    // 哪一格是「这一轮会撞上的那一格」：已经越线的格子要排在前面，哪怕它比
    // 另一个还没满的格子更宽。只报「最窄的已声明格」会让头行写着「剩 80k」、
    // 下一轮照样被平台级拒掉——两个都是执法面自己算的，读的人却看到两个答案。
    // 与代理路径拒人时读的是同一个 `blocked_by.first()`（由窄到宽）。
    let effective_cell = decision
        .blocked_by
        .first()
        .or_else(|| decision.scopes.iter().find(|scope| scope.enforced));
    let effective = effective_cell.map(|scope| {
        serde_json::json!({
            "scope": scope.scope.as_str(),
            "target": scope.target_id,
            "soft_limit": scope.limits.soft_limit,
            "hard_limit": scope.limits.hard_limit,
            "used_today": scope.used_today,
            "remaining": scope.remaining,
            "blocking": scope.blocking,
            "warning": scope.warning,
        })
    });
    let used_today: u64 = decision
        .scopes
        .iter()
        .find(|scope| scope.scope == cog_core::QuotaScope::User)
        .map(|scope| scope.used_today)
        .unwrap_or(0);
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({
            "readable": true,
            "user_id": user_id,
            // `null` = 没人为这个人（也没有人给平台）配过上限。这是「未配置」，
            // 不是「无限」：执法面在没配过的格子上不拒，但那是「没设界」。
            "effective": effective,
            "used_today": used_today,
            "resets_at_unix": now_unix + cog_core::day::seconds_to_next_day(now_unix) as i64,
            "scopes": decision.scopes,
        })),
    )
        .into_response()
}

/// 上下文被挤掉了一段：模型下一轮不再看得见它，屏幕上得说一声。
///
/// 只在循环真的报了裁剪时发。没有裁剪也发一帧「0 条」是最省事的写法，但它把
/// 一次真事件稀释成每轮都有的噪声，读者从此不再看它——「没发生」与「没量到」
/// 在这一格上本来就该沉默。
///
/// 正文由界面拼：这里给的是计数与两段开头（还在的尾巴从哪起、被挤掉那段的
/// 开头是什么），不在这里写一句人话——同一句话在界面与日志里各写一遍，改一处
/// 漏一处时两边的说法就开始不一样。
async fn emit_turn_compaction(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    compaction: Option<&serde_json::Value>,
) {
    let Some(compaction) = compaction else { return };
    let count = |key: &str| compaction.get(key).and_then(serde_json::Value::as_u64);
    emit(
        out,
        session_id,
        "message.compaction",
        serde_json::json!({
            "message_id": message_id,
            "dropped_messages": count("dropped_messages"),
            "freed_tokens": count("freed_tokens"),
            "kept_messages": count("kept_messages"),
            "kept_from": compaction.get("kept_from").and_then(serde_json::Value::as_str),
            "dropped_head": compaction.get("dropped_head").and_then(serde_json::Value::as_str),
            "budget_tokens": count("budget_tokens"),
            "target_tokens": count("target_tokens"),
        }),
    )
    .await;
}

/// 这一轮给模型看的历史：最近 [`SESSION_HISTORY_CAP`] 条，旧的最先。
///
/// 读不到就按「没有历史」回答，不按「历史是空的」记一笔：这两种在读数上要分得开，
/// 而这一趟能做的只有回答——拿一个读不到的库把聊天停掉，是把「不知道」当成「没有」。
async fn load_history(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    threshold_tokens: usize,
    backend: Option<&Arc<dyn cog_core::SandboxBackend>>,
) -> Vec<cog_core::Message> {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮没有历史上下文"
        );
        return Vec::new();
    };
    let turns = match store.history(session_id, SESSION_HISTORY_CAP).await {
        Ok(turns) => turns,
        Err(e) => {
            tracing::warn!(
                error = %e,
                session = %session_id,
                "聊天：历史读不出来，这一轮按没有上下文回答"
            );
            return Vec::new();
        }
    };
    let mut messages = turns_to_messages(turns);
    for message in messages.iter_mut() {
        if !matches!(message, cog_core::Message::User { .. }) {
            continue;
        }
        let text = message.content();
        if cog_core::estimate_tokens(&text) <= threshold_tokens {
            continue;
        }
        // 折叠的是「太长的那段字」，不是这一句话：随它上来的图与附件照样在这一条
        // 消息里。把它们连同正文一起换成一条引用，等于让后面几轮再也看不见那张图。
        let media: Vec<cog_core::ChatAttachment> = match message {
            cog_core::Message::User { content, .. } => content
                .iter()
                .filter_map(|block| match block {
                    cog_core::ContentBlock::Media { data, mime_type } => {
                        Some(cog_core::ChatAttachment {
                            name: None,
                            mime_type: mime_type.clone(),
                            data: data.clone(),
                        })
                    }
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        // 每个用户自己的话都先写一遍文件：执行器的 `/tmp` 会随它的重启消失，
        // 而记录里的原文还在。写一次就写一次——读到一条引用却打不开它，比读到
        // 一段被截断的正文更糟。
        match archive_oversized(backend, session_id, &text, threshold_tokens).await {
            Ok(archived) => *message = user_message(&archived.reference, &media),
            Err(reason) => tracing::warn!(
                session = %session_id,
                error = %reason,
                "聊天：上一轮的原文没有写回执行器，这一轮只能按记录里的原文带进去"
            ),
        }
    }
    messages
}

/// 记录里的一行变成模型看得懂的一条。角色的闭集在存储面，这里只做搬运。
fn turns_to_messages(turns: Vec<cog_core::ChatTurn>) -> Vec<cog_core::Message> {
    turns
        .into_iter()
        .map(|turn| match turn.role {
            // 附件随这一行一起回来：后面几轮问的还是同一张图，那张图必须还在
            // 模型看得见的地方，而不是只在发它的那一轮里出现过一次。
            cog_core::ChatTurnRole::User => user_message(&turn.content, &turn.attachments),
            cog_core::ChatTurnRole::Assistant => cog_core::Message::assistant_text(turn.content),
        })
        .collect()
}

/// 把这一轮写进记录。写不进去只留一条 warn：用户已经在等一个回答，拿「记不下来」
/// 把回答吞掉，比丢一行历史更糟。
#[allow(clippy::too_many_arguments)]
async fn record_turn(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    owner: &str,
    user: &str,
    model: Option<&str>,
    attachments: &[cog_core::ChatAttachment],
    assistant: &str,
    tool_calls: &[cog_core::ChatToolCall],
    agent: Option<&str>,
) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store
        .record_turn(
            session_id,
            owner,
            user,
            model,
            attachments,
            assistant,
            tool_calls,
            agent,
        )
        .await
    {
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
// 每一格都是这一行上独立的一样东西，与 `ChatTurnStore` 那条方法的参数一一对应：
// 打包成一个结构体只是把同一个形状换个地方写一遍，调用点上就少了一格看得见的东西。
#[allow(clippy::too_many_arguments)]
async fn record_dispatched_turn(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    owner: &str,
    user: &str,
    model: Option<&str>,
    attachments: &[cog_core::ChatAttachment],
    assistant: &str,
    goal_id: &str,
    tool_calls: &[cog_core::ChatToolCall],
    agent: Option<&str>,
) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store
        .record_dispatched_turn(
            session_id,
            owner,
            user,
            model,
            attachments,
            assistant,
            goal_id,
            tool_calls,
            agent,
        )
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
// 每一格都是这一行上独立的一样东西，与 `ChatTurnStore` 那条方法一一对应：打包成
// 一个结构体只是把同一个形状换个地方写一遍，调用点上就少了一格看得见的东西。
#[allow(clippy::too_many_arguments)]
async fn record_failed_turn(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    owner: &str,
    user: &str,
    attachments: &[cog_core::ChatAttachment],
    failure: &str,
    tool_calls: &[cog_core::ChatToolCall],
    agent: Option<&str>,
) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store
        .record_failed_turn(
            session_id,
            owner,
            user,
            attachments,
            failure,
            tool_calls,
            agent,
        )
        .await
    {
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
    // 这一句点的哪一条命令（`/名字`），`None` 是普通的一句话。命令不是发给模型的
    // 那句话：`content` 是人打的那一行，交给模型的是展开后的说明。
    command: Option<crate::commands::Invocation>,
    // 随这一句上来的图片 / PDF / 音频。数据保持 base64：交给循环的媒体块就是
    // 这个形状，网关不拆包也不重编。
    attachments: Vec<cog_core::ChatAttachment>,
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
    // 一条太长、整条进不了上下文的消息，先在执行器那边落成文件：原文一个字节都
    // 不丢，循环看到的只是引用与开头。这一步排在回声之前，因为回声那一帧就要把
    // 「这条消息是附件」带给界面。
    //
    // 写不进去就不发：这条消息发出去只能被窗口切掉一半，而「原文没被截断」是这个
    // 平台对每一句话的承诺，不是能拿「反正模型也读不完」换掉的东西。
    let archived = match archive_if_oversized(&state, &session_id, &content).await {
        Ok(archived) => archived,
        Err(reason) => {
            let user_mid = format!("user-{}", uuid::Uuid::new_v4());
            emit_message_start(&out, &session_id, &user_mid, "user", None).await;
            emit_delta(&out, &session_id, &user_mid, &content).await;
            let assistant_mid = format!("assistant-{}", uuid::Uuid::new_v4());
            emit_message_start(&out, &session_id, &assistant_mid, "assistant", None).await;
            let text = format!(
                "这条消息太长，要落成文件才能整条留下，而原文没有写进去（{reason}）。\
                 发出去只会被窗口截掉一半，所以这一轮没有发。"
            );
            emit_delta(&out, &session_id, &assistant_mid, &text).await;
            // 这一句还没交给循环，工具一个都没跑过：工具那两格空空如也。
            record_failed_turn(
                &state.chat_turns,
                &session_id,
                &owner,
                &content,
                &attachments,
                &text,
                &[],
                None,
            )
            .await;
            emit_message_end(&out, &session_id, &user_mid).await;
            emit_message_end(&out, &session_id, &assistant_mid).await;
            return;
        }
    };

    // 1. Echo the user message so the UI renders it (the real transport has
    //    no local echo, unlike the mock client).
    let user_mid = format!("user-{}", uuid::Uuid::new_v4());
    emit_message_start(
        &out,
        &session_id,
        &user_mid,
        "user",
        archived.as_ref().map(|a| (a.path.as_str(), a.chars)),
    )
    .await;
    emit_delta(&out, &session_id, &user_mid, &content).await;
    emit_message_end(&out, &session_id, &user_mid).await;

    let assistant_mid = format!("assistant-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &assistant_mid, "assistant", None).await;
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
        // 这一段还没接到循环上，工具一个都没跑过。
        record_failed_turn(
            &state.chat_turns,
            &session_id,
            &owner,
            &content,
            &attachments,
            reason,
            &[],
            None,
        )
        .await;
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
        // 循环没起来，工具一个都没跑过。
        record_failed_turn(
            &state.chat_turns,
            &session_id,
            &owner,
            &content,
            &attachments,
            reason,
            &[],
            None,
        )
        .await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        return;
    };

    // 这一轮的窗口尺：长消息的折叠与技能说明的上限都用它。窗口从会话执行器读，
    // 不在网关另写一个数——两处不同源，界面上落成文件的那条线就会与循环真正被
    // 撑破的那条线错开。
    let threshold = oversized_threshold_tokens(
        conversation.context_window_tokens(),
        MAX_INBOUND_FRAME_BYTES,
    );

    // 1b. 一条命令：认名字、展开说明。认不出来就在这儿说，不交给模型——屏幕上
    //     要是让模型去答「/websearch 是什么」，它只能用猜的，而真正发生的是平台
    //     不认识那个名字。展开后的那一段只活在这一轮：记录里存的是人打的那一行
    //     （给读记录的人看），交给模型的那一份另存一列（给后面几轮看），两列都
    //     不是「这一句话」的替身。
    let expansion = match command.as_ref() {
        None => None,
        Some(invocation) => {
            match crate::commands::expand(
                state.external_skill_registry.as_ref(),
                invocation,
                threshold,
            )
            .await
            {
                Ok(text) => Some(text),
                Err(reason) => {
                    emit_delta(&out, &session_id, &assistant_mid, &reason).await;
                    // 记成没答的那一轮：这句话确实对这个平台说过，认不出命令也是
                    // 这一轮的事实。没答的那一轮不进模型的历史（见 `history`），
                    // 所以模型不会把一句平台提示当成自己说过的话。
                    // 命令没认出来，这一轮还没交给循环，工具一个都没跑过。
                    record_failed_turn(
                        &state.chat_turns,
                        &session_id,
                        &owner,
                        &content,
                        &attachments,
                        &reason,
                        &[],
                        None,
                    )
                    .await;
                    emit_message_end(&out, &session_id, &assistant_mid).await;
                    return;
                }
            }
        }
    };

    // 2. Build the transcript: what this session already said, plus this
    //    sentence. 循环读到的是这段记录，不是网关的记忆。员工的指示不在这里：
    //    它是那位员工自己的事，由会话执行器按声明取，网关只搬对话。
    //
    //    读历史时同一条规则再生效一次：记录里存的是原文（界面上要能读回它），
    //    而循环读到的是引用加开头（整句再进一次上下文，窗口撑破的就是它）。
    let mut messages = load_history(
        &state.chat_turns,
        &session_id,
        threshold,
        state.sandbox_backend.as_ref(),
    )
    .await;
    let text = match (&archived, &expansion) {
        (Some(archived), _) => archived.reference.clone(),
        (None, Some(expansion)) => expansion.clone(),
        (None, None) => content.clone(),
    };
    // `@` 点名的文件在这一轮就铺进上下文：人是拿它补充这句话的，等模型自己
    // 想起来再读一遍，是把一句已经给了材料的话当成一个调研任务。
    let mentions = read_mentions(
        state.sandbox_backend.as_ref(),
        &mentioned_paths(&content),
        threshold,
    )
    .await;
    messages.push(user_message_with_mentions(&text, &mentions, &attachments));

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
                // 记录在下面几步还要用这个身份：这一轮属于谁，写的是同一件事。
                owner: owner.clone(),
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
        // 被叫停之前跑过的工具仍跟着这一轮记下来：它们确实跑过，而「停的时候
        // 它已经在做什么」正是重开这段对话时最该看得见的那一半。
        record_failed_turn(
            &state.chat_turns,
            &session_id,
            &owner,
            &content,
            &attachments,
            reason,
            &journal.tool_calls,
            journal.agent.as_deref(),
        )
        .await;
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
    // 这一轮读进了哪几句属主约定：只有这一轮挂了读面才有这一格。
    let turn_memory = outcome
        .as_ref()
        .ok()
        .and_then(|value| value.get("memory"))
        .cloned();
    // 这一轮挤掉了哪一段上下文：循环只在真裁过时才挂这一格，没有就是 `None`。
    let turn_compaction = outcome
        .as_ref()
        .ok()
        .and_then(|value| value.get("compaction"))
        .cloned();
    let mut reply = journal.reply;
    let mut failure: Option<String> = None;
    match outcome {
        Err(err) => {
            let text = err.to_string();
            // 额度拒绝的正文里写着「哪一级、花了多少、上限多少、什么时候恢复」，
            // 不译成一句话它就是一串 JSON 糊在气泡里；而这句话正是要让人知道
            // 该找谁、等多久的那一句。
            let rendered = failure_text(&text);
            emit_delta(&out, &session_id, &assistant_mid, &rendered).await;
            // 记下来的也是这一句：刷新之后历史里读到的必须是同一句话，而不是
            // 刷新前一句人话、刷新后一段 JSON。
            failure = Some(rendered);
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
            record_failed_turn(
                &state.chat_turns,
                &session_id,
                &owner,
                &content,
                &attachments,
                err,
                &journal.tool_calls,
                journal.agent.as_deref(),
            )
            .await;
        }
        (None, true) => {
            record_failed_turn(
                &state.chat_turns,
                &session_id,
                &owner,
                &content,
                &attachments,
                "模型没有返回任何内容",
                &journal.tool_calls,
                journal.agent.as_deref(),
            )
            .await;
        }
        (None, false) => match journal.goals.first() {
            Some(goal_id) => {
                record_dispatched_turn(
                    &state.chat_turns,
                    &session_id,
                    &owner,
                    &content,
                    expansion.as_deref(),
                    &attachments,
                    &reply,
                    goal_id,
                    &journal.tool_calls,
                    journal.agent.as_deref(),
                )
                .await
            }
            None => {
                record_turn(
                    &state.chat_turns,
                    &session_id,
                    &owner,
                    &content,
                    expansion.as_deref(),
                    &attachments,
                    &reply,
                    &journal.tool_calls,
                    journal.agent.as_deref(),
                )
                .await
            }
        },
    }

    // 6. 这一轮的现场：上下文挤掉了什么、花了多少。都排在收尾帧之前，因为界面
    //    把它们画在这一条消息下面——收尾帧一到，这一条就定稿了。压缩在前：
    //    它说的是这一轮模型手上少看到了什么，读的人先知道这件事再读花销。
    emit_turn_memory(&out, &session_id, &assistant_mid, turn_memory.as_ref()).await;
    emit_turn_compaction(&out, &session_id, &assistant_mid, turn_compaction.as_ref()).await;
    emit_turn_usage(
        state.llm_usage.as_deref(),
        &out,
        &session_id,
        &assistant_mid,
        turn_usage.as_ref(),
    )
    .await;

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

    /// 一个把每一句都放过去的阈值：下面这几条量的是历史本身，不是「多长算长」。
    const NEVER_OVERSIZED: usize = usize::MAX / 2;

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
            plan: None,
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
            plan: None,
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

    /// 这一轮调过的工具要留在记录那一侧，而不是只发了帧。
    ///
    /// 帧活在连接里：页面一关，这段对话的待办面板就只剩一问一答。这里读的是
    /// 那一轮结束之后还留着的东西——每一笔调用的名字、入参、结果、结局与两头的
    /// 时刻，以及回答者是谁。少任何一格，重开时面板要么空着，要么把「跑到一半」
    /// 画成「已经返回」。
    #[tokio::test]
    async fn the_tools_a_turn_ran_are_kept_where_a_reopened_page_reads_them() {
        use cog_core::AgentEvent;
        let (events_tx, events_rx) = mpsc::channel(8);
        let (out_tx, _out_rx) = mpsc::channel(32);
        let (bus, _bus_rx) = tokio::sync::broadcast::channel(8);
        let pump = tokio::spawn(pump_events(
            events_rx,
            out_tx,
            bus,
            "s1".into(),
            "m-1".into(),
        ));

        let started_at = chrono::Utc::now();
        events_tx
            .send(AgentEvent::ToolExecutionStart {
                agent_id: "kai-a9ab11fc".into(),
                tool_call_id: "call-write".into(),
                tool_name: "write_file".into(),
                args: serde_json::json!({ "path": "src/main.rs" }),
                timestamp: started_at,
            })
            .await
            .unwrap();
        // 中途那一帧只带调用号与增量：它既不该另起一笔，也不该把这一笔收尾。
        events_tx
            .send(AgentEvent::ToolExecutionUpdate {
                agent_id: "kai-a9ab11fc".into(),
                tool_call_id: "call-write".into(),
                partial_result: serde_json::json!("写了一半"),
                timestamp: chrono::Utc::now(),
            })
            .await
            .unwrap();
        // 没跑完的那一笔：被停掉的一轮里它是真的发生过，但没有结果。
        events_tx
            .send(AgentEvent::ToolExecutionStart {
                agent_id: "kai-a9ab11fc".into(),
                tool_call_id: "call-hung".into(),
                tool_name: "run_command".into(),
                args: serde_json::json!({ "command": "sleep 600" }),
                timestamp: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let finished_at = chrono::Utc::now();
        events_tx
            .send(AgentEvent::ToolExecutionEnd {
                agent_id: "kai-a9ab11fc".into(),
                tool_call_id: "call-write".into(),
                result: serde_json::json!({ "ok": true }),
                is_error: false,
                timestamp: finished_at,
            })
            .await
            .unwrap();
        drop(events_tx);

        let journal = pump.await.expect("pump finished");
        assert_eq!(
            journal.agent.as_deref(),
            Some("kai-a9ab11fc"),
            "回答者没留在记录里：重开之后这一段是谁答的、这些调用归给谁都不知道"
        );
        assert_eq!(journal.tool_calls.len(), 2, "{:?}", journal.tool_calls);

        let write = &journal.tool_calls[0];
        assert_eq!(write.tool_call_id, "call-write");
        assert_eq!(write.tool_name, "write_file");
        assert_eq!(write.args, "{\"path\":\"src/main.rs\"}");
        assert_eq!(write.result.as_deref(), Some("{\"ok\":true}"));
        assert_eq!(write.status, cog_core::ChatToolCallStatus::Ok);
        assert_eq!(write.started_at, started_at);
        assert_eq!(write.finished_at, Some(finished_at));

        let hung = &journal.tool_calls[1];
        assert_eq!(hung.tool_call_id, "call-hung");
        assert_eq!(
            hung.status,
            cog_core::ChatToolCallStatus::Running,
            "没收尾的一笔被记成了一个有结局的调用：重开之后它看起来像是跑完了"
        );
        assert!(hung.result.is_none() && hung.finished_at.is_none());
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

        emit_turn_usage(None, &out_tx, "s1", "m-1", Some(&counted)).await;

        let frame = out_rx.recv().await.expect("这一帧要发出去");
        assert!(frame.contains("\"message.usage\""), "{frame}");
        assert!(frame.contains("\"rounds\":3"), "{frame}");
        assert!(frame.contains("\"input_tokens\":1234"), "{frame}");
        assert!(frame.contains("\"model\":\"some-model\""), "{frame}");
        assert!(frame.contains("\"cost_usd\":0.0123"), "{frame}");
    }

    /// 只答「这一段会话花了多少」的账本替身。
    struct SessionLedger;

    #[async_trait::async_trait]
    impl cog_core::LlmUsageLedger for SessionLedger {
        async fn record(&self, _: &cog_core::LlmUsageRecord) -> Result<(), String> {
            unimplemented!("这一格只读不写")
        }
        async fn record_exchange(&self, _: &cog_core::LlmExchangeRecord) -> Result<(), String> {
            unimplemented!("这一格只读不写")
        }
        async fn upstream_window(
            &self,
            _: &str,
            _: u64,
        ) -> Result<cog_core::UpstreamWindowReading, String> {
            unimplemented!("这一格只读不写")
        }
        async fn burned_between(&self, _: &str, _: i64, _: i64) -> Result<u64, String> {
            unimplemented!("这一格只读不写")
        }
        async fn record_window(&self, _: &cog_core::StoredWindow) -> Result<(), String> {
            unimplemented!("这一格只读不写")
        }
        async fn stored_window(
            &self,
            _: &str,
            _: u64,
        ) -> Result<Option<cog_core::StoredWindow>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn stored_windows(&self, _: &str) -> Result<Vec<cog_core::StoredWindow>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spent_since(&self, _: &str, _: i64) -> Result<u64, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_actor_since(
            &self,
            _: i64,
        ) -> Result<(u64, std::collections::BTreeMap<String, u64>), String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_upstream_and_actor(
            &self,
        ) -> Result<Vec<cog_core::UpstreamActorSpend>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn task_cost_quantile(
            &self,
            _: i64,
            _: f64,
        ) -> Result<Option<(u64, u64, i64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn task_has_started(&self, _: &str) -> Result<bool, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_for_session(
            &self,
            session: &str,
        ) -> Result<Option<cog_core::contract::usage_ledger::SessionSpend>, String> {
            if session != "s1" {
                // 另一段会话在账上没有行：读到了「零」，不是「没读到」。
                return Ok(None);
            }
            Ok(Some(cog_core::contract::usage_ledger::SessionSpend {
                calls: 3,
                tokens_input: 4_000,
                tokens_cached: 1_000,
                tokens_output: 500,
                money: vec![cog_core::contract::usage_ledger::SessionMoney {
                    currency: "USD".into(),
                    cost_micros: 12_300,
                    priced_calls: 2,
                }],
                unpriced_calls: 1,
            }))
        }
        async fn spend_by_task(&self, _: &[String]) -> Result<Vec<(String, u64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn priced_spend_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<cog_core::PricedSpend, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_person_since(&self, _: i64) -> Result<Vec<(String, u64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_workspace_since(&self, _: i64) -> Result<Vec<(String, u64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_task_since(
            &self,
            _: &[String],
            _: i64,
        ) -> Result<Vec<(String, u64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn task_attribution(
            &self,
            _: &str,
        ) -> Result<Option<cog_core::TaskAttribution>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn unattributed_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<u64, String> {
            unimplemented!("这一格只读不写")
        }
        async fn outcomes_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::contract::usage_ledger::OutcomeCount>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn metering_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::contract::usage_ledger::MeteringCount>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn skips_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::contract::usage_ledger::SkippedWorkLine>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn verdicts_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::contract::usage_ledger::SpendVerdictLine>, String> {
            unimplemented!("这一格只读不写")
        }
    }

    /// 一帧花销带的是「这一轮」加「这一段会话到目前为止」：只看这一轮，
    /// 「这段聊下来多少钱」就永远问不出来。
    #[tokio::test]
    async fn the_usage_frame_carries_the_conversation_s_total_so_far() {
        let ledger = SessionLedger;
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let counted = serde_json::json!({ "input_tokens": 100, "output_tokens": 10 });

        emit_turn_usage(Some(&ledger), &out_tx, "s1", "m-1", Some(&counted)).await;

        let frame = out_rx.recv().await.expect("这一帧要发出去");
        let frame: serde_json::Value = serde_json::from_str(&frame).expect("一帧 JSON");
        let session = &frame["payload"]["payload"]["session"];
        assert_eq!(session["readable"], true, "{session}");
        assert_eq!(session["calls"], 3, "{session}");
        assert_eq!(session["input_tokens"], 4_000, "{session}");
        assert_eq!(session["output_tokens"], 500, "{session}");
        assert_eq!(session["cached_tokens"], 1_000, "{session}");
        assert_eq!(session["unpriced_calls"], 1, "{session}");
        // 钱按币种分开列，不折成一个和。
        assert_eq!(session["money"][0]["currency"], "USD", "{session}");
        assert_eq!(session["money"][0]["cost_micros"], 12_300, "{session}");
        assert_eq!(session["money"][0]["priced_calls"], 2, "{session}");
    }

    /// 账本没接时，「读不到」不能读成「花了零」。
    #[tokio::test]
    async fn a_missing_ledger_reads_as_unread_not_as_zero() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let counted = serde_json::json!({ "input_tokens": 100, "output_tokens": 10 });

        emit_turn_usage(None, &out_tx, "s1", "m-1", Some(&counted)).await;

        let frame: serde_json::Value =
            serde_json::from_str(&out_rx.recv().await.expect("这一帧要发出去")).unwrap();
        let session = &frame["payload"]["payload"]["session"];
        assert_eq!(session["readable"], false, "{session}");
        assert_eq!(session["reason"], "no_ledger", "{session}");
        assert!(
            session.get("calls").is_none(),
            "读不到的时候不许给出一个「0 次调用」：{session}"
        );
    }

    /// 额度拒绝的正文要变成一句带数的话：哪一级、花了多少、上限多少、等多久。
    #[tokio::test]
    async fn an_owner_budget_refusal_becomes_a_sentence_with_the_numbers() {
        let raw = r#"API error (HTTP 402 PAYMENT_REQUIRED): {"error":"这个人今天的额度已经用完","reason":"owner_budget_spent","scope":"user","target":"u-1","owner":"u-1","spent_today":12340,"hard_limit":12000,"retry_after_seconds":7200}"#;

        let sentence = owner_budget_refusal_sentence(raw).expect("认得出这是额度拒绝");
        assert!(sentence.contains("u-1"), "{sentence}");
        assert!(
            sentence.contains("12340"),
            "已经花了多少要在句子里：{sentence}"
        );
        assert!(sentence.contains("12000"), "上限要在句子里：{sentence}");
        assert!(
            sentence.contains("120 分钟"),
            "等多久要在句子里：{sentence}"
        );
        assert!(
            !sentence.contains('{'),
            "原样的 JSON 不该留在人话里：{sentence}"
        );
    }

    /// 拒绝正文里的级别词逐级译成人话，一个都不许落进兜底。
    ///
    /// 判据是执法面**自己写出去的那几个词**（`QuotaScope::as_str`），不是英文
    /// 全名：自己再拼一套名字时，`ws` 会落进兜底被说成「整个平台」，读者据此
    /// 去找属主，而该找的是租户管理员——正文里那个 `target`（工作区 id）也一起
    /// 丢了。所以每一级都要看它译出来的主体，不是只看「有没有译出什么」。
    #[test]
    fn every_scope_the_proxy_names_reads_back_as_that_scope() {
        let sentence = |scope: &str, target: &str| {
            let raw = format!(
                r#"API error (HTTP 402 PAYMENT_REQUIRED): {{"reason":"owner_budget_spent","scope":"{scope}","target":"{target}","spent_today":10,"hard_limit":5,"retry_after_seconds":60}}"#
            );
            owner_budget_refusal_sentence(&raw).expect("五级里任何一级都认得出")
        };
        for (scope, target, want) in [
            ("user", "u-1", "本人 u-1"),
            ("ws", "ws-9", "工作区 ws-9"),
            ("team", "t-3", "团队 t-3"),
            ("org", "o-7", "组织 o-7"),
            ("global", "platform", "整个平台"),
        ] {
            let said = sentence(scope, target);
            assert!(
                said.contains(want),
                "{scope} 这一级要译成「{want}」：{said}"
            );
        }

        // 认不出来的级别不许替它编一个主体：说成「整个平台」等于让人去找错的人。
        let unknown = sentence("galaxy", "x-1");
        assert!(
            unknown.contains("galaxy 这一级"),
            "陌生的级别要照原词说出来：{unknown}"
        );
        assert!(
            !unknown.contains("整个平台"),
            "认不出来的级别不许被说成平台级：{unknown}"
        );
    }

    /// 一轮失败时说哪句话：额度拒绝译成人话，其余照抄。
    ///
    /// 这一条钉的是**接线**：把翻译函数写好了却没接进失败那一支，等于没做——
    /// 屏幕上仍然是一段 JSON。
    #[test]
    fn a_failed_turn_says_the_quota_sentence_when_the_proxy_refused_on_the_budget() {
        let refused = r#"API error (HTTP 402 PAYMENT_REQUIRED): {"error":"这个人今天的额度已经用完","reason":"owner_budget_spent","scope":"user","target":"u-1","spent_today":12340,"hard_limit":12000,"retry_after_seconds":60}"#;
        let said = failure_text(refused);
        assert!(said.contains("u-1"), "{said}");
        assert!(said.contains("12340"), "{said}");
        assert!(
            !said.starts_with("LLM 调用失败"),
            "额度拒绝要说出原因，不是一句「调用失败」：{said}"
        );

        let broke = "connection reset by peer";
        assert_eq!(
            failure_text(broke),
            "LLM 调用失败：connection reset by peer"
        );
    }

    /// 不是额度拒绝的错误不许被翻译成额度拒绝。
    #[tokio::test]
    async fn a_network_failure_is_not_translated_into_a_quota_refusal() {
        assert!(owner_budget_refusal_sentence("connection reset by peer").is_none());
        assert!(
            owner_budget_refusal_sentence("API error (HTTP 429): {\"reason\":\"refused_by_pace\"}")
                .is_none(),
            "配速拒绝是「等一等」，不是「额度用完了」"
        );
    }

    /// 上下文被挤掉一段时，界面收到一帧带着计数与两段开头的现场。
    #[tokio::test]
    async fn a_turn_that_squeezed_the_context_says_so_in_one_frame() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let squeezed = serde_json::json!({
            "dropped_messages": 12,
            "freed_tokens": 3456,
            "kept_messages": 9,
            "kept_from": "user: 任务输入",
            "dropped_head": "assistant: 先解释一段…",
            "budget_tokens": 64000,
            "target_tokens": 32000,
        });

        emit_turn_compaction(&out_tx, "s1", "m-1", Some(&squeezed)).await;

        let frame = out_rx.recv().await.expect("这一帧要发出去");
        assert!(frame.contains("\"message.compaction\""), "{frame}");
        assert!(frame.contains("\"dropped_messages\":12"), "{frame}");
        assert!(frame.contains("\"freed_tokens\":3456"), "{frame}");
        assert!(
            frame.contains("\"kept_from\":\"user: 任务输入\""),
            "{frame}"
        );
        assert!(
            frame.contains("\"dropped_head\":\"assistant: 先解释一段…\""),
            "{frame}"
        );
        assert!(frame.contains("\"budget_tokens\":64000"), "{frame}");
    }

    /// 没裁过就不发：每轮一帧「挤掉 0 条」，读者很快就不再读它了。
    #[tokio::test]
    async fn a_turn_that_did_not_squeeze_anything_says_nothing() {
        let (out_tx, mut out_rx) = mpsc::channel(4);

        emit_turn_compaction(&out_tx, "s1", "m-1", None).await;
        drop(out_tx);

        assert!(
            out_rx.recv().await.is_none(),
            "循环没报裁剪时，屏幕上不该出现「挤掉 0 条」"
        );
    }

    /// 没读到就不发：一帧全零的花销，读起来是「这一轮不要钱、没花时间」。
    #[tokio::test]
    async fn a_turn_without_a_reading_does_not_get_a_frame_of_zeroes() {
        let (out_tx, mut out_rx) = mpsc::channel(4);

        emit_turn_usage(None, &out_tx, "s1", "m-1", None).await;
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
            .record_turn("s1", "u-1", "上一问", None, &[], "上一答", &[], None)
            .await
            .expect("seed the fake transcript");
        let handle: crate::ChatTurnHandle = Some(store.clone());

        let messages = load_history(&handle, "s1", NEVER_OVERSIZED, None).await;
        assert_eq!(
            messages.iter().map(|m| m.role()).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        assert_eq!(messages[0].content(), "上一问");
        assert_eq!(messages[1].content(), "上一答");
        // 上下文窗口的界要有人传给存储面，否则「最近 40 条」只是这里的一句注释。
        assert_eq!(*store.last_limit.lock().await, Some(SESSION_HISTORY_CAP));
    }

    /// 名单是「一个人的会话，最近说话的在前，搜索只在他自己的行里找」。
    ///
    /// 真库上那条查询另有一条用例（`cog-storage/tests/chat_turn_store.rs`）；这一条
    /// 钉的是替身与它同一套判据——替身偏离了，用它的那几条测试就不再说明生产里的
    /// 行为，而它们看起来仍然全绿。
    #[tokio::test]
    async fn the_listing_is_one_persons_conversations_newest_first() {
        let store = FakeTurns::default();
        for (session, owner, line) in [
            ("s-old", "u-1", "旧的一问"),
            ("s-theirs", "u-2", "别人的一问"),
            ("s-new", "u-1", "新的一问"),
        ] {
            store
                .record_turn(session, owner, line, None, &[], "答", &[], None)
                .await
                .expect("seed the fake transcript");
        }

        let mine = store.sessions("u-1", 10, None).await.expect("list");
        let ids: Vec<&str> = mine.iter().map(|s| s.session_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["s-new", "s-old"],
            "名单没有把最近说话的排在前面，或者把别人的会话也列了出来"
        );
        assert_eq!(mine[1].title, "旧的一问", "标题不是这一段开口的第一句");

        let searched = store.sessions("u-1", 10, Some("旧")).await.expect("search");
        assert_eq!(searched.len(), 1, "搜索没有在自己的一段话里找到这几个字");
        assert!(
            store
                .sessions("u-1", 10, Some("别人"))
                .await
                .expect("search")
                .is_empty(),
            "搜索翻到了别人的会话里"
        );
        assert!(
            store
                .sessions("", 10, None)
                .await
                .expect("list with an empty identity")
                .is_empty(),
            "空身份拿到了名单"
        );
    }

    /// `@` 后面那串是路径，不是称呼。
    ///
    /// 只认词的起头，且这一串只能是路径会有的字：一条「早上好 @张三」或一句
    /// 「写邮件到 a@b.com」都不该让网关去读一个文件，而「看看 @src/main.rs。」
    /// 里的句号属于这句话，不属于路径。
    #[test]
    fn an_at_sign_names_a_path_not_a_greeting() {
        assert_eq!(
            mentioned_paths("看看 @src/main.rs，还有 @crates/cog-core/src/lib.rs。"),
            vec!["src/main.rs", "crates/cog-core/src/lib.rs"]
        );
        assert!(
            mentioned_paths("写邮件到 a@b.com").is_empty(),
            "邮箱里那个 @ 不是点名"
        );
        assert_eq!(
            mentioned_paths("看看 @docs/README.md。"),
            vec!["docs/README.md"],
            "句号被当成了路径的一部分"
        );
        assert_eq!(
            mentioned_paths("@src/main.rs 和再提一次 @src/main.rs"),
            vec!["src/main.rs"],
            "同一份文件读一次就够"
        );
    }

    /// 点名的文件铺在正文之后、媒体之前：先读字，再读那份代码，最后看图。
    #[tokio::test]
    async fn a_mentioned_file_arrives_before_the_picture() {
        let backend: std::sync::Arc<dyn cog_core::SandboxBackend> =
            std::sync::Arc::new(FileBackend::with("src/main.rs", "fn main() {}\n"));
        let mentions = read_mentions(
            Some(&backend),
            &mentioned_paths("这里为什么编译不过 @src/main.rs"),
            NEVER_OVERSIZED,
        )
        .await;
        assert_eq!(mentions.len(), 1, "点名的那一份没有读上来");
        let message = user_message_with_mentions(
            "这里为什么编译不过 @src/main.rs",
            &mentions,
            &[cog_core::ChatAttachment {
                name: Some("err.png".into()),
                mime_type: "image/png".into(),
                data: "QUJD".into(),
            }],
        );
        let cog_core::Message::User { content, .. } = message else {
            panic!("交给循环的不是一条用户消息");
        };
        assert_eq!(content.len(), 3, "三样没有各占一格：{content:?}");
        assert!(
            content[1]
                .as_text()
                .is_some_and(|text| text.contains("fn main() {}\n")),
            "第二格不是那份文件的正文：{content:?}"
        );
        assert!(
            matches!(&content[2], cog_core::ContentBlock::Media { .. }),
            "图没有排在最后：{content:?}"
        );
    }

    /// 点名的文件太多时，铺开的只是「去哪儿读」，不是把这一轮撑破。
    #[tokio::test]
    async fn too_much_mentioned_text_becomes_a_note_to_read_it_yourself() {
        let long = "x".repeat(4_000);
        let backend: std::sync::Arc<dyn cog_core::SandboxBackend> =
            std::sync::Arc::new(FileBackend::with("big.txt", &long));
        let mentions = read_mentions(Some(&backend), &["big.txt".to_string()], 10).await;
        assert_eq!(mentions.len(), 1);
        assert!(
            mentions[0].block.contains("read_file"),
            "太长的那份没有说「去读它」：{}",
            mentions[0].block
        );
        assert!(
            !mentions[0].block.contains(&long),
            "太长的那份仍然整份铺了进来"
        );
    }

    /// 读不到的点名不冒充读过：没有这一份，就没有这一格。
    #[tokio::test]
    async fn a_mention_that_cannot_be_read_adds_nothing() {
        let backend: std::sync::Arc<dyn cog_core::SandboxBackend> =
            std::sync::Arc::new(FileBackend::default());
        let mentions = read_mentions(Some(&backend), &["missing.rs".to_string()], 10).await;
        assert!(
            mentions.is_empty(),
            "读不到的文件点出了一格内容：{mentions:?}"
        );
    }

    /// 一份只认得几个路径的沙箱：这一条量的是「点名读上来了没有」。
    #[derive(Default)]
    struct FileBackend {
        files: std::collections::HashMap<String, String>,
    }

    impl FileBackend {
        fn with(path: &str, content: &str) -> Self {
            let mut files = std::collections::HashMap::new();
            files.insert(path.to_string(), content.to_string());
            Self { files }
        }
    }

    #[async_trait::async_trait]
    impl cog_core::SandboxBackend for FileBackend {
        async fn execute(
            &self,
            req: &cog_core::SandboxRequest,
        ) -> cog_core::SFResult<cog_core::SandboxResult> {
            let cog_core::SandboxPayload::ReadFile { path } = &req.payload else {
                return Ok(cog_core::SandboxResult {
                    stdout: String::new(),
                    stderr: String::new(),
                    exit_code: 1,
                    output: None,
                    duration_ms: 0,
                    resource_usage: cog_core::ResourceUsage::default(),
                    workdir: None,
                });
            };
            let found = self.files.get(path);
            Ok(cog_core::SandboxResult {
                stdout: found.cloned().unwrap_or_default(),
                stderr: if found.is_some() {
                    String::new()
                } else {
                    format!("no such file: {path}")
                },
                exit_code: if found.is_some() { 0 } else { 1 },
                output: None,
                duration_ms: 0,
                resource_usage: cog_core::ResourceUsage::default(),
                workdir: None,
            })
        }

        async fn precompile(&self, _bytes: &[u8]) -> cog_core::SFResult<String> {
            Ok(String::new())
        }
    }

    /// 记录里带着的图，装回提示词时仍然是媒体块。
    ///
    /// 「发出去时带了图」是另一条门的事；这一条量的是下一轮：用户接着问「那这里
    /// 呢」时，模型手上要有上一句说的那张图。图只在发它的那一轮出现过一次，等于
    /// 后面几轮都在对着一张看不见的画说话。
    #[tokio::test]
    async fn the_history_carries_the_picture_that_came_with_the_question() {
        let store = std::sync::Arc::new(FakeTurns::default());
        store
            .record_turn(
                "s1",
                "u-1",
                "这里错了",
                None,
                &[cog_core::ChatAttachment {
                    name: Some("shot.png".into()),
                    mime_type: "image/png".into(),
                    data: "QUJD".into(),
                }],
                "看到了",
                &[],
                None,
            )
            .await
            .expect("seed the fake transcript");
        let handle: crate::ChatTurnHandle = Some(store.clone());

        let messages = load_history(&handle, "s1", NEVER_OVERSIZED, None).await;
        let cog_core::Message::User { content, .. } = &messages[0] else {
            panic!("第一条不是用户那句：{messages:?}");
        };
        assert_eq!(content.len(), 2, "正文与图没有各占一格：{content:?}");
        assert_eq!(content[0].as_text(), Some("这里错了"));
        assert!(
            matches!(
                &content[1],
                cog_core::ContentBlock::Media { data, mime_type }
                    if data == "QUJD" && mime_type == "image/png"
            ),
            "记录里的图没有装回提示词：{content:?}"
        );
        // 回答那一半不替模型带图：它说的话里没有附件。
        assert!(matches!(&messages[1], cog_core::Message::Assistant { .. }));
    }

    /// 长消息折成引用时，折掉的是字，不是随它上来的图。
    ///
    /// 折叠把整条消息换成一句「原文在文件里」——顺手把媒体块也换掉，是在这一条
    /// 最容易写错的地方让图静默消失：屏幕上有引用、模型看不见图，而两边都不报错。
    #[tokio::test]
    async fn folding_a_long_message_keeps_the_picture_that_came_with_it() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let long = "很长的一段话".repeat(200);
        store
            .record_turn(
                "s1",
                "u-1",
                &long,
                None,
                &[cog_core::ChatAttachment {
                    name: Some("shot.png".into()),
                    mime_type: "image/png".into(),
                    data: "QUJD".into(),
                }],
                "读完了",
                &[],
                None,
            )
            .await
            .expect("seed the fake transcript");
        let handle: crate::ChatTurnHandle = Some(store.clone());
        let backend: std::sync::Arc<dyn cog_core::SandboxBackend> =
            std::sync::Arc::new(AcceptingBackend);

        let messages = load_history(&handle, "s1", 10, Some(&backend)).await;
        let cog_core::Message::User { content, .. } = &messages[0] else {
            panic!("第一条不是用户那句：{messages:?}");
        };
        assert_eq!(
            content.len(),
            2,
            "折叠之后只剩一段话，图不见了：{content:?}"
        );
        let folded = content[0].as_text().expect("第一格要是那句引用");
        assert!(
            folded.contains("read_file"),
            "第一格不是「原文在哪」的引用：{folded}"
        );
        assert!(
            !folded.contains(&long),
            "第一格还是原文本身：这一轮等于没有折叠"
        );
        assert!(
            matches!(
                &content[1],
                cog_core::ContentBlock::Media { data, mime_type }
                    if data == "QUJD" && mime_type == "image/png"
            ),
            "折叠把随这一句上来的图一起换掉了：{content:?}"
        );
    }

    /// 一条命令：记录里存的是人打的那一行，后面几轮读到的却是展开后的说明。
    ///
    /// 两格要是读成同一份，两个方向都是错的：重开页面时人看到一整份 SKILL.md 画成
    /// 「我说的话」；第二句追问时模型手上没有那份规程，只能按自己的猜法继续——一段
    /// 用技能开的对话，从第二轮起就不再是那个技能在做了。
    #[tokio::test]
    async fn a_command_leaves_what_the_person_typed_and_what_the_model_read() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_turn(
            &handle,
            "s1",
            "u-1",
            "/web-research 查一下 1.90",
            Some("先搜，再读。"),
            &[],
            "查到了",
            &[],
            None,
        )
        .await;

        // 模型那一侧（下一轮读历史）拿到的是展开后的说明。
        let messages = load_history(&handle, "s1", NEVER_OVERSIZED, None).await;
        assert_eq!(
            messages[0].content(),
            "先搜，再读。",
            "后面几轮读到的不是展开后的说明"
        );
        // 读者的那一侧是原来的人话（替身把两者分开存着，见 `FakeTurns`）。
        assert_eq!(
            store.typed.lock().await.clone(),
            vec!["/web-research 查一下 1.90".to_string()],
            "人打的那一行没有被记下来"
        );
    }

    /// 一个只回答「写成功了」的沙箱：这一条量的是折叠，不是文件怎么写。
    struct AcceptingBackend;

    #[async_trait::async_trait]
    impl cog_core::SandboxBackend for AcceptingBackend {
        async fn execute(
            &self,
            _req: &cog_core::SandboxRequest,
        ) -> cog_core::SFResult<cog_core::SandboxResult> {
            Ok(cog_core::SandboxResult {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
                output: None,
                duration_ms: 0,
                resource_usage: cog_core::ResourceUsage::default(),
                workdir: None,
            })
        }

        async fn precompile(&self, _bytes: &[u8]) -> cog_core::SFResult<String> {
            Ok(String::new())
        }
    }

    /// 回答出来的这一轮必须写进记录，且是作为一问一答一起写。
    #[tokio::test]
    async fn an_answered_turn_is_written_down_as_a_pair() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_turn(&handle, "s1", "u-1", "问", None, &[], "答", &[], None).await;
        let held = store.turns.lock().await.clone();
        assert_eq!(
            held,
            vec![
                cog_core::ChatTurn {
                    role: cog_core::ChatTurnRole::User,
                    content: "问".into(),
                    attachments: Vec::new(),
                },
                cog_core::ChatTurn {
                    role: cog_core::ChatTurnRole::Assistant,
                    content: "答".into(),
                    attachments: Vec::new(),
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
        record_dispatched_turn(
            &handle,
            "s1",
            "u-1",
            "把这件事做了",
            None,
            &[],
            "已受理",
            "goal-7",
            &[],
            None,
        )
        .await;

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
        record_failed_turn(&handle, "s1", "u-1", "问", &[], "upstream 503", &[], None).await;

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
        assert!(load_history(&handle, "s1", NEVER_OVERSIZED, None)
            .await
            .is_empty());
        // 不发散：这里只要求「不写进任何东西」——没有第二个家可写。
        record_turn(&handle, "s1", "u-1", "问", None, &[], "答", &[], None).await;
        record_failed_turn(&handle, "s1", "u-1", "问", &[], "答不出来", &[], None).await;
    }

    /// 读记录失败按「没有历史」回答，而不是按「历史是空的」记一笔。
    #[tokio::test]
    async fn a_transcript_that_cannot_be_read_answers_without_context() {
        let store = std::sync::Arc::new(FakeTurns::default());
        store
            .record_turn("s1", "u-1", "问", None, &[], "答", &[], None)
            .await
            .expect("seed the fake transcript");
        store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let handle: crate::ChatTurnHandle = Some(store.clone());
        assert!(load_history(&handle, "s1", NEVER_OVERSIZED, None)
            .await
            .is_empty());
        // 写也失败时不许把这一轮的回答吞掉：record_turn 只留 warn。
        record_turn(&handle, "s1", "u-1", "问", None, &[], "答", &[], None).await;
    }

    /// 一份内存里的记录，只作测试替身。生产里没有它的对应物——这次改动拆掉的
    /// 正是那样一张表。
    #[derive(Default)]
    struct FakeTurns {
        turns: tokio::sync::Mutex<Vec<cog_core::ChatTurn>>,
        /// 每条助手行带回来的工具调用与回答者，按写入顺序与 `turns` 里的助手行
        /// 一一对应。真实实现把这两样写在助手那一行上，替身分开存，`transcript`
        /// 读的时候按同一个序号接回去。
        calls: tokio::sync::Mutex<Vec<(Vec<cog_core::ChatToolCall>, Option<String>)>>,
        /// 有问无答的那几轮：留在记录里，但不进上下文（`history` 不返回它们）。
        unanswered: tokio::sync::Mutex<Vec<(String, String)>>,
        /// 交出去的那几轮与它们的目标：(问句, goal_id)。
        dispatched: tokio::sync::Mutex<Vec<(String, String)>>,
        /// 人打过的每一行原话。真实实现把它写进 `content`、把展开写进 `model_content`，
        /// 这里分成两格，测的才是「两者不是同一份」。
        typed: tokio::sync::Mutex<Vec<String>>,
        last_limit: tokio::sync::Mutex<Option<usize>>,
        /// 每一次写入：(会话, 属于谁, 人打的那一行)。会话名单就是从这几条读出来的。
        lines: tokio::sync::Mutex<Vec<(String, String, String)>>,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl cog_core::ChatTurnStore for FakeTurns {
        async fn record_turn(
            &self,
            session_id: &str,
            owner: &str,
            user: &str,
            model: Option<&str>,
            attachments: &[cog_core::ChatAttachment],
            assistant: &str,
            tool_calls: &[cog_core::ChatToolCall],
            agent: Option<&str>,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            self.lines.lock().await.push((
                session_id.to_string(),
                owner.to_string(),
                user.to_string(),
            ));
            let mut held = self.turns.lock().await;
            if session_id != "s1" {
                return Ok(());
            }
            self.typed.lock().await.push(user.to_string());
            // 与 SQL 里那条 `COALESCE(model_content, content)` 同一个读法：这一格
            // 存的是交给模型的那一份，不是人打的那一行。
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::User,
                content: model.unwrap_or(user).to_string(),
                attachments: attachments.to_vec(),
            });
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::Assistant,
                content: assistant.to_string(),
                attachments: Vec::new(),
            });
            self.calls
                .lock()
                .await
                .push((tool_calls.to_vec(), agent.map(str::to_string)));
            Ok(())
        }

        async fn record_dispatched_turn(
            &self,
            session_id: &str,
            owner: &str,
            user: &str,
            model: Option<&str>,
            attachments: &[cog_core::ChatAttachment],
            assistant: &str,
            goal_id: &str,
            tool_calls: &[cog_core::ChatToolCall],
            agent: Option<&str>,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            self.lines.lock().await.push((
                session_id.to_string(),
                owner.to_string(),
                user.to_string(),
            ));
            if session_id != "s1" {
                return Ok(());
            }
            let mut held = self.turns.lock().await;
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::User,
                content: model.unwrap_or(user).to_string(),
                attachments: attachments.to_vec(),
            });
            held.push(cog_core::ChatTurn {
                role: cog_core::ChatTurnRole::Assistant,
                content: assistant.to_string(),
                attachments: Vec::new(),
            });
            self.calls
                .lock()
                .await
                .push((tool_calls.to_vec(), agent.map(str::to_string)));
            self.dispatched
                .lock()
                .await
                .push((user.to_string(), goal_id.to_string()));
            Ok(())
        }

        async fn record_failed_turn(
            &self,
            session_id: &str,
            owner: &str,
            user: &str,
            _attachments: &[cog_core::ChatAttachment],
            failure: &str,
            _tool_calls: &[cog_core::ChatToolCall],
            _agent: Option<&str>,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            self.lines.lock().await.push((
                session_id.to_string(),
                owner.to_string(),
                user.to_string(),
            ));
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
            let calls = self.calls.lock().await;
            let start = turns.len().saturating_sub(limit);
            // 助手行与 `calls` 一一对应：问句那几行没有工具调用，也不占一格。
            let mut assistant_at = 0usize;
            Ok(turns[start..]
                .iter()
                .enumerate()
                .map(|(offset, turn)| {
                    let (tool_calls, agent) = match turn.role {
                        cog_core::ChatTurnRole::Assistant => {
                            let held = calls.get(assistant_at).cloned().unwrap_or_default();
                            assistant_at += 1;
                            held
                        }
                        cog_core::ChatTurnRole::User => (Vec::new(), None),
                    };
                    cog_core::ChatTurnRecord {
                        id: (start + offset) as i64 + 1,
                        role: turn.role,
                        content: turn.content.clone(),
                        failure: None,
                        goal: None,
                        // 记录里只留认人的那两格，与真正的存储面一致：重开页面要认
                        // 得出这一条带过一张图，不需要把那幅图再下载一遍。
                        attachments: turn
                            .attachments
                            .iter()
                            .map(|attachment| cog_core::ChatAttachmentInfo {
                                name: attachment.name.clone(),
                                mime_type: attachment.mime_type.clone(),
                            })
                            .collect(),
                        tool_calls,
                        agent,
                        created_at: chrono::Utc::now(),
                    }
                })
                .collect())
        }

        /// 名单按人过滤、最近说话的在前。与真实实现同一套判据：别人的行不出现，
        /// 搜索只在同一个人的行里找，空身份一条都不给。
        async fn sessions(
            &self,
            owner: &str,
            limit: usize,
            search: Option<&str>,
        ) -> cog_core::SFResult<Vec<cog_core::ChatSessionSummary>> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            if owner.trim().is_empty() || limit == 0 {
                return Ok(Vec::new());
            }
            let needle = search.unwrap_or("").trim().to_lowercase();
            let lines = self.lines.lock().await;
            let mut found: Vec<cog_core::ChatSessionSummary> = Vec::new();
            for (session_id, line_owner, user) in lines.iter() {
                if line_owner != owner {
                    continue;
                }
                match found.iter_mut().find(|s| &s.session_id == session_id) {
                    Some(entry) => entry.turns += 1,
                    None => found.push(cog_core::ChatSessionSummary {
                        session_id: session_id.clone(),
                        title: user.clone(),
                        last_activity_at: chrono::Utc::now(),
                        turns: 1,
                    }),
                }
            }
            // 插入顺序是写入顺序：翻过来就是最近说话的在前。
            found.reverse();
            Ok(found
                .into_iter()
                .filter(|summary| {
                    needle.is_empty()
                        || lines.iter().any(|(session_id, line_owner, user)| {
                            session_id == &summary.session_id
                                && line_owner == owner
                                && user.to_lowercase().contains(&needle)
                        })
                })
                .take(limit)
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
