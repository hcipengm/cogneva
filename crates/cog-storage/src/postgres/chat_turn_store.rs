//! PostgreSQL-backed chat transcript.
//!
//! A chat turn used to be recorded in the gateway process's own `HashMap`, and
//! the schema said otherwise: `chat_sessions` and `messages` were created by the
//! migrations with a `user_id` foreign key, and no code ever wrote a row. The
//! two homes disagreed in the direction that matters, because the one that was
//! written to died with the process -- a restart, a rollout, an OOM kill, and
//! every conversation lost its history while the tables still promised it was
//! kept.
//!
//! The transcript lives here now, keyed by the session id the client sends. Not
//! by a user: the chat socket is not authenticated, and a `user_id NOT NULL`
//! column would have forced the writer to invent an owner for every conversation
//! -- a made-up fact recorded as if it were measured. The tables shaped around
//! that invented owner are gone; when an authenticated chat arrives, the owner
//! becomes a column on this table rather than a second table beside it.
//!
//! What is kept is a transcript, not an audit log: the oldest rows of a session
//! fall off once it holds [`ROWS_KEPT_PER_SESSION`]. Unbounded growth is the
//! other way a store like this dies, and a chat panel does not scroll back to
//! last year.

use async_trait::async_trait;
use sqlx::PgPool;

use cog_core::{SFError, SFResult};

/// Rows kept per session: two per turn, so a hundred user/assistant pairs.
///
/// Larger than the forty messages the model is shown as context, because the
/// two bounds answer different questions: the context window is what fits in a
/// request, and this is how far back a reader can still see what was said.
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

/// The transcript's two operations.
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

    /// The last `limit` lines of a session, oldest first.
    ///
    /// Empty for a session that was never recorded, which is what a reader
    /// starting a fresh conversation wants; a limit of zero asks for nothing and
    /// answers nothing rather than the whole transcript.
    async fn history(&self, session_id: &str, limit: usize) -> SFResult<Vec<ChatTurn>>;
}

/// PostgreSQL chat transcript.
pub struct PostgresChatTurnStore {
    pool: PgPool,
}

impl PostgresChatTurnStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// One statement for the pair, so the transcript never holds a question whose
/// answer is missing.
const INSERT_TURN_SQL: &str = "\
INSERT INTO chat_turns (session_id, role, content) \
VALUES ($1, 'user', $2), ($1, 'assistant', $3)";

/// Drop what fell out of the window. `NOT IN` over the newest rows keeps the
/// bound per session rather than global: one busy session must not push another
/// one's history out.
const TRIM_TURNS_SQL: &str = "\
DELETE FROM chat_turns \
WHERE session_id = $1 \
  AND id NOT IN ( \
      SELECT id FROM chat_turns WHERE session_id = $1 ORDER BY id DESC LIMIT $2 \
  )";

/// The newest `limit` rows, then put back into the order a prompt is built in.
/// Reading oldest-first directly would need a count of the session, which is a
/// second query that can disagree with the first under a concurrent write.
const HISTORY_SQL: &str = "\
SELECT role, content FROM ( \
    SELECT id, role, content FROM chat_turns WHERE session_id = $1 ORDER BY id DESC LIMIT $2 \
) newest \
ORDER BY id";

#[async_trait]
impl ChatTurnStore for PostgresChatTurnStore {
    async fn record_turn(&self, session_id: &str, user: &str, assistant: &str) -> SFResult<()> {
        if session_id.trim().is_empty() {
            return Err(SFError::Validation(
                "a chat turn cannot be recorded without a session: it would land in a transcript \
                 nobody can ask for"
                    .into(),
            ));
        }
        let mut tx =
            self.pool.begin().await.map_err(|e| {
                SFError::Database(format!("start the chat turn's transaction: {e}"))
            })?;
        sqlx::query(INSERT_TURN_SQL)
            .bind(session_id)
            .bind(user)
            .bind(assistant)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("record a chat turn: {e}")))?;
        // Same transaction as the insert: a trim that ran on its own could
        // delete a row another writer had just appended to a different session,
        // and a crash between the two would leave the bound unenforced.
        sqlx::query(TRIM_TURNS_SQL)
            .bind(session_id)
            .bind(ROWS_KEPT_PER_SESSION)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("trim the chat transcript: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("commit the chat turn: {e}")))?;
        Ok(())
    }

    async fn history(&self, session_id: &str, limit: usize) -> SFResult<Vec<ChatTurn>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, String)> = sqlx::query_as(HISTORY_SQL)
            .bind(session_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the chat transcript: {e}")))?;
        rows.into_iter()
            .map(|(role, content)| {
                let role = ChatTurnRole::parse(&role).ok_or_else(|| {
                    SFError::Database(format!(
                        "chat transcript holds a role this build cannot name: {role:?}"
                    ))
                })?;
                Ok(ChatTurn { role, content })
            })
            .collect()
    }
}
