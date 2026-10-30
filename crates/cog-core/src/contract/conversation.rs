//! 两段对话的契约：跟模型的聊天记录，与跟同侪的消息往来。
//!
//! 两者形状相近但问的不同：聊天记录是**一次请求要带上的上下文**加**读者重开页面
//! 要看的那一段**，消息往来是**我对某个 peer 说过什么、它答了什么**。放在一起是
//! 因为它们同属"说过什么"这一族，且同一个进程（安全网关）同时写这两张表、读这两
//! 段话——它只该知道这两段话的形状，不该知道它们落在哪张表里。
//!
//! 类型与 trait 在这里，实现留在拥有数据库的那个 crate：句子是契约，SQL 不是。

use async_trait::async_trait;

use crate::SFResult;

/// Rows kept per session: two per turn, so a hundred user/assistant pairs.
///
/// Larger than the forty messages the model is shown as context, because the
/// two bounds answer different questions: the context window is what fits in a
/// request, and this is how far back a reader can still see what was said.
// cap: bound
pub const ROWS_KEPT_PER_SESSION: i64 = 200;

/// Who said one line of the transcript.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatTurnRole {
    User,
    Assistant,
}

impl ChatTurnRole {
    /// The spelling the column's check constraint holds.
    pub fn as_str(self) -> &'static str {
        match self {
            ChatTurnRole::User => "user",
            ChatTurnRole::Assistant => "assistant",
        }
    }

    /// Read one back. A value outside the set is not a role this build can name;
    /// the reader decides what to do with a row written by a version that had
    /// more of them rather than folding it into one of these.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "user" => Some(ChatTurnRole::User),
            "assistant" => Some(ChatTurnRole::Assistant),
            _ => None,
        }
    }
}

/// One line of the transcript, oldest first when returned by
/// [`ChatTurnStore::history`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatTurn {
    pub role: ChatTurnRole,
    pub content: String,
}

/// One line of the transcript as a reader shows it: with the row's identity, its
/// time, and the reason it has no answer when it has none.
///
/// [`ChatTurn`] is what a prompt is built from and therefore deliberately
/// forgets all three: the model is shown who said what, not when it was said.
/// A reader reopening a conversation needs the parts the prompt drops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatTurnRecord {
    pub id: i64,
    pub role: ChatTurnRole,
    pub content: String,
    /// Why this line never got an answer. Both halves of an unanswered exchange
    /// carry the same reason, so a reader that finds either end can say why.
    pub failure: Option<String>,
    /// The goal this line handed work to, when it handed work to one.
    ///
    /// A conversation that dispatched is not the same thing as one that
    /// answered: the first keeps running on the server after the page closes,
    /// the second exists only while the socket is open. A reader that can only
    /// see the goal on the live frame loses that distinction exactly when it is
    /// being relied on -- on reopen.
    pub goal: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// The transcript's operations.
///
/// A trait rather than the concrete store because the gateway must keep
/// answering when the database is not reachable: the turn still streams, the
/// history is simply absent, and a test can hold a transcript across two states
/// without a server.
#[async_trait]
pub trait ChatTurnStore: Send + Sync {
    /// Record one completed exchange: the user's message and the reply that was
    /// actually delivered.
    ///
    /// Written as one turn rather than two appends so a reader never sees a
    /// question whose answer is still in flight -- half a turn reads as an
    /// answer that never came.
    async fn record_turn(&self, session_id: &str, user: &str, assistant: &str) -> SFResult<()>;

    /// Record one exchange whose reply handed the work to the orchestrator, and
    /// the goal it created.
    ///
    /// A separate method rather than a longer `record_turn`: the two write
    /// different shapes, and an implementation that answered this one by
    /// forwarding to `record_turn` would keep the words and drop the goal --
    /// which reads, on reopen, as a conversation that never dispatched anything.
    async fn record_dispatched_turn(
        &self,
        session_id: &str,
        user: &str,
        assistant: &str,
        goal_id: &str,
    ) -> SFResult<()>;

    /// Record one exchange whose reply never arrived, and why.
    ///
    /// A failed model call used to leave nothing behind: the words the user
    /// said to this platform existed only in the response they had just
    /// received, and the next attempt started from a history that denied the
    /// earlier one had happened. The answer's absence is a fact about the turn,
    /// not a reason to forget the turn -- so the question is kept, and the row
    /// carries the reason instead of an answer.
    async fn record_failed_turn(&self, session_id: &str, user: &str, failure: &str)
        -> SFResult<()>;

    /// The last `limit` lines of a session, oldest first.
    ///
    /// Empty for a session that was never recorded, which is what a reader
    /// starting a fresh conversation wants; a limit of zero asks for nothing and
    /// answers nothing rather than the whole transcript.
    ///
    /// Unanswered exchanges are left out: this is the view a prompt is built
    /// from, and a failure notice replayed as an assistant turn would put words
    /// in the model's mouth that the user never received as an answer. They
    /// stay readable in the table itself.
    async fn history(&self, session_id: &str, limit: usize) -> SFResult<Vec<ChatTurn>>;

    /// The last `limit` lines of a session as a reader shows them, oldest first,
    /// unanswered exchanges included.
    ///
    /// The difference from [`Self::history`] is the whole point: a conversation
    /// that is only visible while the socket is open is not a record, it is a
    /// live view. Reload the page and the transcript must still be there,
    /// including the turns whose answer never arrived -- those are the ones a
    /// person most needs to find again.
    async fn transcript(&self, session_id: &str, limit: usize) -> SFResult<Vec<ChatTurnRecord>>;
}

/// Rows kept per receiving peer, so a chatty sender cannot grow the table
/// without bound. Larger than what a panel shows at once, because how far back
/// a reader can scroll and what fits on one screen answer different questions.
// cap: bound
pub const ROWS_KEPT_PER_PEER: i64 = 500;

/// The settled shape of one send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// Delivered; no answer was asked for.
    Queued,
    /// Delivered, and the agent answered within the wait.
    Replied,
    /// Delivered, and no answer arrived within the wait the caller asked for.
    TimedOut,
    /// Refused before delivery: the name holds nothing live, the body was
    /// malformed, or the deployment could not deliver it.
    Refused,
    /// Delivery or the reply stream broke after the command was accepted.
    Failed,
}

impl SendOutcome {
    /// Every outcome this vocabulary has, in the order a reader should see
    /// them. The column's check constraint, [`Self::parse`] and the panel's
    /// label map are all derived from this rather than spelling the words
    /// again — a variant added to `as_str` alone would be a row this build
    /// writes and cannot read back.
    pub const ALL: [SendOutcome; 5] = [
        SendOutcome::Queued,
        SendOutcome::Replied,
        SendOutcome::TimedOut,
        SendOutcome::Refused,
        SendOutcome::Failed,
    ];

    /// The spelling the column's check constraint holds.
    pub fn as_str(self) -> &'static str {
        match self {
            SendOutcome::Queued => "queued",
            SendOutcome::Replied => "replied",
            SendOutcome::TimedOut => "timed_out",
            SendOutcome::Refused => "refused",
            SendOutcome::Failed => "failed",
        }
    }

    /// Read one back. A value outside the set is not an outcome this build can
    /// name; the reader reports it rather than folding it into one of these.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|outcome| outcome.as_str() == value)
    }
}

/// One line of a conversation, as stored. Oldest first when returned by
/// [`AgentMessageStore::conversation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMessage {
    pub id: i64,
    pub from_unit: String,
    pub to_unit: String,
    pub kind: String,
    pub body: String,
    pub outcome: SendOutcome,
    /// What the agent answered, for a `replied` row and only that one.
    pub reply: Option<String>,
    /// Why it did not answer, for `timed_out` / `refused` / `failed`. Kept
    /// apart from `reply` so a reader can never mistake an error for something
    /// the agent said.
    pub outcome_detail: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// What a caller hands the store to record one send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentMessage<'a> {
    /// The name that sent it. The WebUI speaks as itself rather than inventing
    /// a unit name: an owner reading the transcript must be able to tell their
    /// own words from an agent's.
    pub from_unit: &'a str,
    pub to_unit: &'a str,
    pub kind: &'a str,
    pub body: &'a str,
    pub outcome: SendOutcome,
    pub reply: Option<&'a str>,
    pub outcome_detail: Option<&'a str>,
}

/// The conversation's two operations.
///
/// A trait rather than the concrete store because the gateway must keep
/// delivering when no database is reachable: the send goes through, the
/// transcript is simply not written, and a test can hold a conversation without
/// a server.
#[async_trait]
pub trait AgentMessageStore: Send + Sync {
    /// Record one send whose outcome is already settled.
    async fn record(&self, message: &NewAgentMessage<'_>) -> SFResult<()>;

    /// The last `limit` lines of a peer's conversation, oldest first.
    ///
    /// Both directions: an exchange belongs to the history of both parties.
    /// Empty for a peer nobody has addressed, and empty for a limit of zero
    /// rather than the whole table.
    async fn conversation(&self, peer: &str, limit: usize) -> SFResult<Vec<AgentMessage>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 角色与结局的写回读是同一个闭集：一个 `as_str` 里有、`parse` 里没有的
    /// 拼写，就是本版本写得进、读不回的格子。
    #[test]
    fn every_spelling_reads_back_as_what_wrote_it() {
        for role in [ChatTurnRole::User, ChatTurnRole::Assistant] {
            assert_eq!(ChatTurnRole::parse(role.as_str()), Some(role));
        }
        for outcome in SendOutcome::ALL {
            assert_eq!(SendOutcome::parse(outcome.as_str()), Some(outcome));
        }
    }

    /// 集合外的拼写不被折进任何一个既有格：一条由别的版本写的行，读成
    /// "某个我认得的结局"就是替它撒谎。
    #[test]
    fn a_spelling_outside_the_set_is_not_folded_into_one() {
        assert_eq!(ChatTurnRole::parse("system"), None);
        assert_eq!(SendOutcome::parse("dropped"), None);
    }
}
