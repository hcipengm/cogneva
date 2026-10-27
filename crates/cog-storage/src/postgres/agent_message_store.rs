//! Who said what to which agent, and what came back.
//!
//! A command sent to an agent had two ends and no record of either meeting: the
//! inbox stream carried the bytes, the ask route opened a reply stream, and once
//! both had been read and acknowledged neither side could answer "what did I
//! send this agent, and did it answer". The only copy was the HTTP response of
//! the call itself, which a deployment reading it once in a terminal kept
//! nowhere.
//!
//! What is kept is a conversation, not an audit trail of deliveries: the route
//! writes one row per send once its outcome is settled, so a reader never sees
//! a command that is still in flight listed as though it had failed. The
//! conversation is read from either side -- an exchange between two agents
//! belongs to both of their histories, and a reader looking at one of them
//! would otherwise see half a dialogue.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{SFError, SFResult};

/// Rows kept per receiving peer, so a chatty sender cannot grow the table
/// without bound. Larger than what a panel shows at once, because how far back
/// a reader can scroll and what fits on one screen answer different questions.
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
    pub created_at: DateTime<Utc>,
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

/// PostgreSQL conversation store.
pub struct PostgresAgentMessageStore {
    pool: PgPool,
}

impl PostgresAgentMessageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const INSERT_SQL: &str = "\
INSERT INTO agent_messages \
    (from_unit, to_unit, kind, body, outcome, reply, outcome_detail) \
VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// Drop what fell out of this peer's window. Bounded per receiver rather than
/// globally: one chatty sender must not push another conversation out.
const TRIM_SQL: &str = "\
DELETE FROM agent_messages \
WHERE to_unit = $1 \
  AND id NOT IN ( \
      SELECT id FROM agent_messages WHERE to_unit = $1 ORDER BY id DESC LIMIT $2 \
  )";

/// The newest rows in either direction, then put back into the order a
/// conversation is read in. Reading oldest-first directly would need a count of
/// the conversation, which is a second query that can disagree with the first
/// under a concurrent write.
const CONVERSATION_SQL: &str = "\
SELECT id, from_unit, to_unit, kind, body, outcome, reply, outcome_detail, created_at \
FROM ( \
    SELECT id, from_unit, to_unit, kind, body, outcome, reply, outcome_detail, created_at \
    FROM agent_messages \
    WHERE to_unit = $1 OR from_unit = $1 \
    ORDER BY id DESC LIMIT $2 \
) newest \
ORDER BY id";

#[async_trait]
impl AgentMessageStore for PostgresAgentMessageStore {
    async fn record(&self, message: &NewAgentMessage<'_>) -> SFResult<()> {
        let from_unit = message.from_unit.trim();
        let to_unit = message.to_unit.trim();
        if from_unit.is_empty() || to_unit.is_empty() {
            return Err(SFError::Validation(
                "an agent message needs both ends: a row with no sender or no addressee \
                 cannot be read back as a conversation with anybody"
                    .into(),
            ));
        }
        // Exactly one shape per row, the same rule the chat transcript holds:
        // an answer is what the agent said, a detail is why it did not, and a
        // row claiming both would have two truths.
        match (message.outcome, message.reply, message.outcome_detail) {
            (SendOutcome::Replied, None, _) => {
                return Err(SFError::Validation(
                    "a replied message must carry the reply: an outcome that says the \
                     agent answered with nothing reads as an answer that was given"
                        .into(),
                ))
            }
            (SendOutcome::Replied, Some(_), Some(_)) => {
                return Err(SFError::Validation(
                    "a replied message carries the reply, not a failure detail: the two \
                     would disagree about what the agent said"
                        .into(),
                ))
            }
            (SendOutcome::Queued, Some(_), _) => {
                return Err(SFError::Validation(
                    "a queued message has no answer yet: nothing was waited for".into(),
                ))
            }
            _ => {}
        }
        let mut tx = self.pool.begin().await.map_err(|e| {
            SFError::Database(format!("start the agent message's transaction: {e}"))
        })?;
        sqlx::query(INSERT_SQL)
            .bind(from_unit)
            .bind(to_unit)
            .bind(message.kind)
            .bind(message.body)
            .bind(message.outcome.as_str())
            .bind(message.reply)
            .bind(message.outcome_detail)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("record an agent message: {e}")))?;
        // Same transaction as the insert: a trim on its own could delete a row
        // another writer had just appended, and a crash between the two would
        // leave the bound unenforced.
        sqlx::query(TRIM_SQL)
            .bind(to_unit)
            .bind(ROWS_KEPT_PER_PEER)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("trim the agent conversation: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("commit the agent message: {e}")))?;
        Ok(())
    }

    async fn conversation(&self, peer: &str, limit: usize) -> SFResult<Vec<AgentMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(CONVERSATION_SQL)
            .bind(peer)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read an agent conversation: {e}")))?;
        rows.into_iter()
            .map(|row| {
                let outcome: String = row.try_get("outcome").map_err(db_err)?;
                let outcome = SendOutcome::parse(&outcome).ok_or_else(|| {
                    SFError::Database(format!(
                        "agent message holds an outcome this build cannot name: {outcome:?}"
                    ))
                })?;
                Ok(AgentMessage {
                    id: row.try_get("id").map_err(db_err)?,
                    from_unit: row.try_get("from_unit").map_err(db_err)?,
                    to_unit: row.try_get("to_unit").map_err(db_err)?,
                    kind: row.try_get("kind").map_err(db_err)?,
                    body: row.try_get("body").map_err(db_err)?,
                    outcome,
                    reply: row.try_get("reply").map_err(db_err)?,
                    outcome_detail: row.try_get("outcome_detail").map_err(db_err)?,
                    created_at: row.try_get("created_at").map_err(db_err)?,
                })
            })
            .collect()
    }
}

fn db_err(e: sqlx::Error) -> SFError {
    SFError::Database(format!("decode an agent message: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store whose pool was never connected: every case below is decided
    /// before the first statement, so no server is needed to see it.
    fn unconnected() -> PostgresAgentMessageStore {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .expect("build a lazy pool");
        PostgresAgentMessageStore::new(pool)
    }

    fn message<'a>(outcome: SendOutcome, reply: Option<&'a str>) -> NewAgentMessage<'a> {
        NewAgentMessage {
            from_unit: "webui",
            to_unit: "cogneva-worker-0",
            kind: "prompt",
            body: "run the tests",
            outcome,
            reply,
            outcome_detail: None,
        }
    }

    /// The transcript's two columns must not disagree: `reply` is what the agent
    /// said, `outcome` says whether it said anything. A row that claims an
    /// answer and carries none reads, in the panel, as an agent that answered
    /// with silence -- and it is written as fact.
    #[tokio::test]
    async fn a_reply_outcome_without_a_reply_is_refused() {
        let store = unconnected();
        let e = store
            .record(&message(SendOutcome::Replied, None))
            .await
            .expect_err("a replied row with no reply is not a recordable exchange");
        assert!(matches!(e, SFError::Validation(_)), "got {e:?}");
    }

    /// The other half of the same invariant: a command nobody waited for has no
    /// answer, so an answer in that row would be an answer to a question the
    /// record says was never asked.
    #[tokio::test]
    async fn a_queued_send_cannot_carry_an_answer() {
        let store = unconnected();
        let e = store
            .record(&message(SendOutcome::Queued, Some("done")))
            .await
            .expect_err("a queued row cannot claim an answer");
        assert!(matches!(e, SFError::Validation(_)), "got {e:?}");
    }

    /// A reply and a failure detail are two different accounts of the same
    /// moment; a row holding both has no single truth to read back.
    #[tokio::test]
    async fn an_answer_and_a_failure_detail_cannot_share_a_row() {
        let store = unconnected();
        let mut m = message(SendOutcome::Replied, Some("done"));
        m.outcome_detail = Some("the stream broke");
        let e = store.record(&m).await.expect_err("one row, one account");
        assert!(matches!(e, SFError::Validation(_)), "got {e:?}");
    }

    /// A conversation with nobody is not a conversation: the row could not be
    /// asked for by either party and would sit in the table unread.
    #[tokio::test]
    async fn a_message_with_no_sender_or_no_addressee_is_refused() {
        let store = unconnected();
        let mut m = message(SendOutcome::Queued, None);
        m.from_unit = "   ";
        assert!(matches!(
            store.record(&m).await,
            Err(SFError::Validation(_))
        ));
        let mut m = message(SendOutcome::Queued, None);
        m.to_unit = "";
        assert!(matches!(
            store.record(&m).await,
            Err(SFError::Validation(_))
        ));
    }

    /// The stored spelling is what the column's check constraint admits, and
    /// the reader must accept exactly what the writer writes -- a name that
    /// parsed nowhere would make every row of that outcome unreadable.
    /// Walking the declaration rather than a list written here: a variant that
    /// stays out of `ALL` is one `parse` cannot read back, and a list repeated
    /// in this test would not notice.
    #[test]
    fn every_outcome_round_trips_through_its_stored_spelling() {
        let mut words = std::collections::BTreeSet::new();
        for outcome in SendOutcome::ALL {
            assert!(
                words.insert(outcome.as_str()),
                "two outcomes share the spelling {:?}; the column would hold a word that parses back as the other",
                outcome.as_str()
            );
            assert_eq!(SendOutcome::parse(outcome.as_str()), Some(outcome));
        }
        assert_eq!(words.len(), SendOutcome::ALL.len());
        assert_eq!(SendOutcome::parse("answered"), None);
        // 大小写不同就不是这一列的拼写：check 约束只认这一个词。
        assert_eq!(SendOutcome::parse("Queued"), None);
    }
}
