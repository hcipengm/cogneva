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

pub use cog_core::contract::conversation::{
    ChatTurn, ChatTurnRecord, ChatTurnRole, ChatTurnStore, ROWS_KEPT_PER_SESSION,
};

/// PostgreSQL chat transcript.
pub struct PostgresChatTurnStore {
    pool: PgPool,
}

impl PostgresChatTurnStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Write one exchange and keep the session's window bounded, in one
    /// transaction.
    ///
    /// `assistant` and `failure` are the two shapes a turn has: an answer that
    /// was delivered, or the reason none was. Exactly one of them is given, so a
    /// row can never claim both.
    async fn write(
        &self,
        session_id: &str,
        user: &str,
        assistant: Option<&str>,
        failure: Option<&str>,
        goal: Option<&str>,
    ) -> SFResult<()> {
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
        let mut query = sqlx::query(match (assistant, goal) {
            (Some(_), Some(_)) => INSERT_DISPATCHED_TURN_SQL,
            (Some(_), None) => INSERT_TURN_SQL,
            (None, _) => INSERT_FAILED_TURN_SQL,
        })
        .bind(session_id)
        .bind(user);
        query = match (assistant, goal) {
            (Some(reply), Some(goal_id)) => query.bind(reply).bind(goal_id),
            (Some(reply), None) => query.bind(reply),
            (None, _) => query.bind(failure.unwrap_or("")),
        };
        query
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
}

/// One statement for the pair, so the transcript never holds a question whose
/// answer is missing.
const INSERT_TURN_SQL: &str = "\
INSERT INTO chat_turns (session_id, role, content) \
VALUES ($1, 'user', $2), ($1, 'assistant', $3)";

/// The same pair for a turn that handed its work to the orchestrator. The goal
/// goes on the assistant row only: the reply is where the platform said what it
/// did with the message, and the question is not what carries a goal.
const INSERT_DISPATCHED_TURN_SQL: &str = "\
INSERT INTO chat_turns (session_id, role, content, goal_id) \
VALUES ($1, 'user', $2, NULL), ($1, 'assistant', $3, $4)";

/// The same pair for an exchange that never got an answer: the reply's content
/// is empty because nothing was said, and `failure` carries why. Both rows get
/// the reason so a reader that finds either half knows what happened to the
/// turn.
const INSERT_FAILED_TURN_SQL: &str = "\
INSERT INTO chat_turns (session_id, role, content, failure) \
VALUES ($1, 'user', $2, $3), ($1, 'assistant', '', $3)";

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
    SELECT id, role, content FROM chat_turns \
    WHERE session_id = $1 AND failure IS NULL \
    ORDER BY id DESC LIMIT $2 \
) newest \
ORDER BY id";

/// The same window as [`HISTORY_SQL`], with the columns a reader needs and none
/// of the `failure IS NULL` filter: the unanswered half is exactly what a
/// reopened conversation has to show.
const TRANSCRIPT_SQL: &str = "\
SELECT id, role, content, failure, goal_id, created_at FROM ( \
    SELECT id, role, content, failure, goal_id, created_at FROM chat_turns \
    WHERE session_id = $1 \
    ORDER BY id DESC LIMIT $2 \
) newest \
ORDER BY id";

#[async_trait]
impl ChatTurnStore for PostgresChatTurnStore {
    async fn record_turn(&self, session_id: &str, user: &str, assistant: &str) -> SFResult<()> {
        self.write(session_id, user, Some(assistant), None, None)
            .await
    }

    async fn record_dispatched_turn(
        &self,
        session_id: &str,
        user: &str,
        assistant: &str,
        goal_id: &str,
    ) -> SFResult<()> {
        if goal_id.trim().is_empty() {
            return Err(SFError::Validation(
                "a dispatched turn cannot be recorded without its goal: the row would say work \
                 was handed off without saying to what"
                    .into(),
            ));
        }
        self.write(session_id, user, Some(assistant), None, Some(goal_id))
            .await
    }

    async fn record_failed_turn(
        &self,
        session_id: &str,
        user: &str,
        failure: &str,
    ) -> SFResult<()> {
        self.write(session_id, user, None, Some(failure), None)
            .await
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

    async fn transcript(&self, session_id: &str, limit: usize) -> SFResult<Vec<ChatTurnRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<(
            i64,
            String,
            String,
            Option<String>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(TRANSCRIPT_SQL)
            .bind(session_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the chat transcript: {e}")))?;
        rows.into_iter()
            .map(|(id, role, content, failure, goal, created_at)| {
                let role = ChatTurnRole::parse(&role).ok_or_else(|| {
                    SFError::Database(format!(
                        "chat transcript holds a role this build cannot name: {role:?}"
                    ))
                })?;
                Ok(ChatTurnRecord {
                    id,
                    role,
                    content,
                    failure,
                    goal,
                    created_at,
                })
            })
            .collect()
    }
}
