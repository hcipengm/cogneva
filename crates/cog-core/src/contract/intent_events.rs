//! 属主说的话是这条流水线的**一种输入**，与外部情报、外部仓库、系统自发现同级。
//!
//! 在此之前它只有一处落点：对话记录。记录是「这里有过一段对话」，不是「这里有一件
//! 要办的事」——两者在库里长得一样，于是属主说过的每一句需要有人去做的话，都停在
//! 记录里，等属主自己再想起一次。这一条把它变成一行：谁在哪说的、说给谁、原话、
//! 从哪一条派生。
//!
//! 两个面分开：写面只回答「把这一句记下来」，读面只回答「哪些还没有人接手」。写面
//! 给对话那一屏，读面给把它变成待办的那条循环——谁都不该拿到对方的那一半。
//!
//! 「接手」记成**第二条事件**（`intent_triaged`），不是改写第一条：原话与到达时刻是
//! 证据，证据不改；派生出什么、派给谁，是后来发生的事。于是「哪条处理过」是一次
//! 事件流上的存在性判断，而不是一个会被覆盖的状态列。

use chrono::{DateTime, Utc};

use crate::SFResult;

/// 任务行上带着「它是从哪条意图派生的」的那个键。
///
/// 写它的人（planner 派生的待办）与读它的人（收件台判「处理过没有」）各拼一次字面量
/// 就会漂，所以名字只在这里出现一次。
pub const INTENT_ID_KEY: &str = "intent_id";

/// 一条意图：谁、在哪、说给谁、原话、从哪一条派生。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntentRow {
    /// 这一句话的身份。会话＋这句话的稳定后缀：同一条连接里同一句重投是同一行，
    /// 换一条会话说同一句是两行——两次到达是两件事，即使字一样。
    pub identity: String,
    /// 从哪儿进来的（`chat`、`external_intel`、……）。同一个身份经两个入口到达是
    /// 两条意图：入口决定了这一句被谁听见。
    pub source: String,
    /// 说这句话的人；空串是「没有身份的那条连接」。
    pub said_by: String,
    /// 说给谁听（员工身份）。空串是「没有指名某个人」——今天的对话就是一整段，
    /// 没有人名可选，而空串把这件事写下来，而不是替它编一个名字。
    pub addressed_to: String,
    /// 原话，一个字节都不改写。转述过的意图是另一个人的话。
    pub text: String,
    /// 由哪一条派生；`None` 是一线入口。
    pub parent: Option<String>,
    /// 模型实际读到的那一段（斜杠命令展开之后的文本）；`None` 表示与 `text` 相同。
    ///
    /// `text` 永远是原话，不改写；展开是派生出来的另一种写法，它也要留一份——
    /// 会话表坏掉之后，「模型当时看到的是什么」只有这一格答得出来。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_text: Option<String>,
    /// 这一句属于哪一段会话；`None` 是「这条入口没有会话」（外部情报、A2A 的
    /// 任务提交、系统自发现）。会话是投影，这一格是它的归属。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// 到达时刻，由写入口取自己的钟填进来。
    pub at: DateTime<Utc>,
}

impl IntentRow {
    /// 一个入口交上来的那一行：身份、谁说的、说给谁、原话、父意图。
    ///
    /// `at` 在这里取钟，`identity` 就是幂等键：入口不再自己补两格，否则每多一个
    /// 入口就多一份「时间怎么取、键怎么拼」的写法。
    pub fn new(
        source: impl Into<String>,
        identity: impl Into<String>,
        said_by: impl Into<String>,
        addressed_to: impl Into<String>,
        text: impl Into<String>,
        parent: Option<String>,
    ) -> Self {
        Self {
            identity: identity.into(),
            source: source.into(),
            said_by: said_by.into(),
            addressed_to: addressed_to.into(),
            text: text.into(),
            parent,
            session: None,
            model_text: None,
            at: Utc::now(),
        }
    }

    /// 幂等键：这一句话的身份。重投拿到同一个键，事件流不写第二行。
    pub fn idempotency_key(&self) -> String {
        self.identity.clone()
    }

    /// 载荷。读面按同一组键还原，键名只此一份。
    pub fn attributes(&self) -> serde_json::Value {
        serde_json::json!({
            "source": self.source,
            "said_by": self.said_by,
            "addressed_to": self.addressed_to,
            "text": self.text,
            "parent": self.parent,
            "session": self.session,
            "model_text": self.model_text,
        })
    }

    /// 从载荷与身份还原一行。读面用这一个，不再自己拼键名。
    pub fn from_attributes(
        identity: String,
        attributes: &serde_json::Value,
        at: DateTime<Utc>,
    ) -> Option<Self> {
        let text = attributes.get("text")?.as_str()?.to_string();
        let read = |key: &str| {
            attributes
                .get(key)
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        Some(Self {
            identity,
            source: read("source"),
            said_by: read("said_by"),
            addressed_to: read("addressed_to"),
            text,
            parent: attributes
                .get("parent")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            session: attributes
                .get("session")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            model_text: attributes
                .get("model_text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            at,
        })
    }
}

/// 一条意图事件的 id：由身份派生，26 位。
///
/// 派生而不是现发：同一轮里别的事实（附件、工具调用、回答）要在意图事件**落下
/// 之前**就把因果指过来；身份两边算得出同一个值，id 也必须算得出同一个值。
pub fn intent_event_id(identity: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("intent:{identity}").as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02X}")).collect();
    hex.chars().take(26).collect()
}

/// 一条意图的身份：来源加这个来源自己的外部 id。
///
/// 对话这一路的「外部 id」是**到达 id**（一次投递的身份），不是原文：同一段会话里
/// 说两遍「继续」是两轮，拿原文做键会把它们并成一轮；而同一句重投（同一个到达 id）
/// 仍是同一行。原文只进正文，不进身份。
///
/// 来源决定「这句话被谁听见」，外部 id 决定「在这个来源里是哪一句」。两个入口
/// 用同一个拼法，读面才不必按入口分叉；A2A 的 `goal_id`、对话的
/// `会话＋原文摘要` 都是这一格。
pub fn intent_identity(source: &str, external_id: &str) -> String {
    format!("{source}:{external_id}")
}

/// 对话那一侧的外部 id：会话加原文的稳定摘要。
///
/// 后缀取自原文而不是序号或时刻：同一句重投要落在同一行，否则重连一次、重试一次
/// 就在事件流里多出一条「属主又说了一遍」。不取时刻的代价是：同一段会话里隔很久
/// 一字不差地说两遍，只会有一行——那一行仍然在，仍然会被接手，只是第二遍不另算。
pub fn chat_intent_identity(session_id: &str, text: &str) -> String {
    intent_identity("chat", &format!("{session_id}:{:016x}", fnv1a(text)))
}

/// 一位同事对另一位同事说的那句话的身份：发信人、收件人、说话的方式与原话共同决定。
///
/// 与对话那一路同一个取向：同一句话重投是同一行（重投是传输在重试，不是又说了一遍），
/// 换一个收件人、换一种方式（prompt／steering／continue）就是另一行。发信人为空串时
/// 照写——「没有身份的那条连接」在这里与别处同形，不替它编一个名字。
pub fn colleague_intent_identity(from: &str, to: &str, kind: &str, text: &str) -> String {
    intent_identity("agent", &format!("{from}>{to}:{kind}:{:016x}", fnv1a(text)))
}

/// FNV-1a：只要求稳定与散得开，不要求抗碰撞——它是行身份的后缀，不是凭证。
fn fnv1a(bytes: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// 一条「意图到达」的事实：id 由身份派生，时刻取行的时刻。
pub fn intent_received_event(row: &IntentRow) -> crate::FactEvent {
    let mut event = crate::FactEvent::new(crate::EventPayload::IntentRecorded(row.clone()));
    event.event_id = Some(intent_event_id(&row.identity));
    event.at = Some(row.at);
    event
}

/// 一批「已接手」的事实：一条一条写，流上每行自带身份。
pub fn intent_triaged_events(
    identities: &[String],
    task_id: &str,
    actor: &str,
    at: DateTime<Utc>,
) -> Vec<crate::FactEvent> {
    identities
        .iter()
        .map(|identity| {
            let mut event = crate::FactEvent::new(crate::EventPayload::IntentTriaged {
                identity: identity.clone(),
                task_id: task_id.to_string(),
                actor: actor.to_string(),
            });
            event.at = Some(at);
            event
        })
        .collect()
}

/// 读「还没有人接手的意图」。
#[async_trait::async_trait]
pub trait IntentReader: Send + Sync {
    /// 按到达次序取最多 `limit` 条还没有接手的意图。
    ///
    /// 读不出来是 `Err`，不是空清单：把一次读失败读成「没有人说过话」，会让这一段
    /// 里的每一句都在下一拍被当成新到达，或者在下一拍之前一直没有人看见。
    async fn undispatched(&self, limit: i64) -> SFResult<Vec<IntentRow>>;

    /// 按身份取回那一行。派生的任务要知道「这句话是谁说的、说给谁」——它手上
    /// 只有身份（待办的 id 就是身份），而这张脸给的就是那一条。
    ///
    /// 读不出来是 `Err`，找不到是 `Ok(None)`：两者分开，「库没答」不该读成
    /// 「这条意图不存在」。
    async fn by_identity(&self, identity: &str) -> SFResult<Option<IntentRow>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> IntentRow {
        IntentRow {
            identity: "chat:s1:9f2a".into(),
            source: "chat".into(),
            said_by: "owner".into(),
            addressed_to: String::new(),
            text: "把这批门禁的归属改回它判的那个对象".into(),
            parent: None,
            session: None,
            model_text: None,
            at: Utc::now(),
        }
    }

    /// 载荷与还原是同一个形状：读面不再自己拼键名，两边写着同一组键。
    #[test]
    fn a_row_survives_its_own_attributes() {
        let original = row();
        let back = IntentRow::from_attributes(
            original.identity.clone(),
            &original.attributes(),
            original.at,
        )
        .expect("a recorded row is readable");
        assert_eq!(back, original);
    }

    /// 没有正文的载荷不是一条意图：读回来 `None` 而不是一条空话。
    #[test]
    fn a_row_without_its_own_words_is_not_an_intent() {
        assert!(IntentRow::from_attributes(
            "chat:s1:0000".into(),
            &serde_json::json!({"source": "chat"}),
            Utc::now(),
        )
        .is_none());
    }

    /// 幂等键就是身份：同一句重投是同一行。
    #[test]
    fn the_idempotency_key_is_the_identity() {
        assert_eq!(row().idempotency_key(), "chat:s1:9f2a");
    }

    /// 身份是「来源＋外部 id」：换来源或换外部 id 都是另一条。
    #[test]
    fn identity_is_source_plus_external_id() {
        assert_eq!(intent_identity("a2a", "goal-7"), "a2a:goal-7");
        assert_eq!(intent_identity("chat", "s1:9f2a"), "chat:s1:9f2a");
        assert_ne!(intent_identity("a2a", "g"), intent_identity("agent", "g"));
    }

    /// 对话的外部 id 只由会话和原文决定；空白不是到达。
    #[test]
    fn the_chat_identity_is_stable_and_text_shaped() {
        let a = chat_intent_identity("s1", "改回来");
        assert_eq!(a, chat_intent_identity("s1", "改回来"));
        assert_ne!(a, chat_intent_identity("s2", "改回来"));
        assert_ne!(a, chat_intent_identity("s1", "改回去"));
        assert!(a.starts_with("chat:s1:"));
    }

    /// 构造器把身份与时刻一起补上，入口不再各拼一份。
    #[test]
    fn the_constructor_fills_the_identity_time_and_key() {
        let made = IntentRow::new(
            "a2a",
            intent_identity("a2a", "g1"),
            "u1",
            "",
            "写回来",
            None,
        );
        assert_eq!(made.source, "a2a");
        assert_eq!(made.identity, "a2a:g1");
        assert_eq!(made.said_by, "u1");
        assert_eq!(made.addressed_to, "");
        assert_eq!(made.text, "写回来");
        assert_eq!(made.idempotency_key(), "a2a:g1");
    }

    /// 同事之间的那句话：同一个发信人、收件人、方式与原话重投是同一行；
    /// 换收件人或换方式就是另一行。
    #[test]
    fn a_colleague_message_identity_names_both_ends() {
        let a = colleague_intent_identity("staff-1", "staff-2", "prompt", "把门禁修了");
        assert_eq!(
            a,
            colleague_intent_identity("staff-1", "staff-2", "prompt", "把门禁修了")
        );
        assert!(a.starts_with("agent:staff-1>staff-2:prompt:"));
        assert_ne!(
            a,
            colleague_intent_identity("staff-1", "staff-3", "prompt", "把门禁修了"),
            "换一个收件人是另一条意图"
        );
        assert_ne!(
            a,
            colleague_intent_identity("staff-1", "staff-2", "steering", "把门禁修了"),
            "换一种说话方式是另一条意图"
        );
        assert_ne!(
            a,
            colleague_intent_identity("staff-1", "staff-2", "prompt", "把门禁修好"),
            "换一句话是另一条意图"
        );
    }
}
