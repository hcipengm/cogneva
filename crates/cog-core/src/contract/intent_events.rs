//! 一条意图进门时的行，与写它的那一个接口。
//!
//! 二十六处入口（对话、外部仓库、外部世界、系统自发现、账本自己的结构来源）交上来
//! 的东西，先落成**同一种行**：来源、外部身份、原文、因果父行。判官随后按机械判据把
//! 它分成回答／任务／变更／原则——分类不在入口做，入口只翻译。
//!
//! 这里只有行与接口：写它的实现在存储那一侧（`cog-storage` 的 `intent_events`），
//! 网关按服务取用它，于是「谁把意图写进流」与「谁读它」不必互相认识。

use async_trait::async_trait;

use crate::SFResult;

/// 意图这一族的 kind。闭集里的一个词。
pub const INTENT_RECEIVED: &str = "intent_received";

/// 一条进门时的意图行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentRow {
    /// 谁把它带进来的：`webui`、`cli`、`github`、`outward`、`self_signal`、`ledger`…
    pub source: String,
    /// 来源那边的身份（会话 id、issue 号、URL、任务 id）。同一个来源里唯一。
    pub external_id: String,
    /// 原文。**不摘要、不改写**：判官要判的就是这段话本身。
    pub text: String,
    /// 因果父行：这一条是被哪一条引出来的（上一轮、上一条事件）。空表示没有父。
    pub parent: String,
}

impl IntentRow {
    /// 这条意图的行身份：来源＋外部身份。幂等键由它派生，重投不会写第二行。
    pub fn identity(&self) -> String {
        format!("{}:{}", self.source, self.external_id)
    }
}

/// 把一条意图写进流。
#[async_trait]
pub trait IntentEvents: Send + Sync {
    /// 记下一条意图。重投同一个 (writer, identity) 不写第二行——「收到过」由库回答。
    async fn record(&self, row: &IntentRow, actor: &str) -> SFResult<()>;
}

/// 读那些还没有被派生成任务的意图。写者与读者是两个面：入口只写，planner 只读。
#[async_trait]
pub trait IntentReader: Send + Sync {
    /// 还没派生的意图，按到达先后。读不到返回 `Err`，不返回空表。
    async fn undispatched(&self, limit: i64) -> SFResult<Vec<IntentRow>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 行身份只取决于来源与外部身份：原文改了不换行。
    #[test]
    fn the_row_identity_is_source_and_external_id() {
        let a = IntentRow {
            source: "webui".into(),
            external_id: "session-1".into(),
            text: "先看日志".into(),
            parent: String::new(),
        };
        let mut b = a.clone();
        b.text = "换成看指标".into();
        assert_eq!(a.identity(), b.identity());
        assert_ne!(a.identity(), "webui:session-2");
    }
}
