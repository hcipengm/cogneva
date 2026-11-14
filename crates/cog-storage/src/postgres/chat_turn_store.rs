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
    ChatAttachment, ChatAttachmentInfo, ChatSessionSummary, ChatTurn, ChatTurnRecord, ChatTurnRole,
    ChatTurnStore, ROWS_KEPT_PER_SESSION,
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
    ///
    /// `model` 只在「人打的那一行」与「模型读到的那一段」不是同一段字时给出
    /// （一条 `/名字` 命令）：它写在用户那一行上，助手那一行永远是空的——回答
    /// 展开成什么，交给模型的和界面上看到的是同一段。
    // 每一格都是这一行上独立的一列（人打的那句、模型读到的那段、附件、回答、
    // 失败原因、目标），打包成一个结构体只是把同一个形状换个地方写一遍。加列时
    // 这里跟着加一个参数，读的人一眼看得出这一行现在由哪几样组成。
    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        session_id: &str,
        owner: &str,
        user: &str,
        model: Option<&str>,
        attachments: &[ChatAttachment],
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
        // 附件与这一句同写一行，存成 JSON：它是一串形状固定的值，不是要按列查
        // 的东西——拆成表只会让「这一行带了几张图」变成一次 join。
        let attachments = serde_json::to_string(attachments)
            .map_err(|e| SFError::Validation(format!("attachments cannot be serialised: {e}")))?;
        let mut query = sqlx::query(match (assistant, goal) {
            (Some(_), Some(_)) => INSERT_DISPATCHED_TURN_SQL,
            (Some(_), None) => INSERT_TURN_SQL,
            (None, _) => INSERT_FAILED_TURN_SQL,
        })
        .bind(session_id)
        .bind(owner)
        .bind(user);
        query = match (assistant, goal) {
            (Some(reply), Some(goal_id)) => query
                .bind(attachments)
                .bind(model)
                .bind(reply)
                .bind(goal_id),
            (Some(reply), None) => query.bind(attachments).bind(model).bind(reply),
            (None, _) => query.bind(failure.unwrap_or("")).bind(attachments),
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
INSERT INTO chat_turns (session_id, owner, role, content, attachments, model_content) \
VALUES ($1, $2, 'user', $3, $4, $5), ($1, $2, 'assistant', $6, NULL, NULL)";

/// The same pair for a turn that handed its work to the orchestrator. The goal
/// goes on the assistant row only: the reply is where the platform said what it
/// did with the message, and the question is not what carries a goal.
const INSERT_DISPATCHED_TURN_SQL: &str = "\
INSERT INTO chat_turns (session_id, owner, role, content, attachments, model_content, goal_id) \
VALUES ($1, $2, 'user', $3, $4, $5), ($1, $2, 'assistant', $6, NULL, NULL, $7)";

/// The same pair for an exchange that never got an answer: the reply's content
/// is empty because nothing was said, and `failure` carries why. Both rows get
/// the reason so a reader that finds either half knows what happened to the
/// turn.
const INSERT_FAILED_TURN_SQL: &str = "\
INSERT INTO chat_turns (session_id, owner, role, content, failure, attachments) \
VALUES ($1, $2, 'user', $3, $4, $5), ($1, $2, 'assistant', '', $4, NULL)";

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
/// `model_content` 优先：一行命令在记录里存的是人打的那一行，交给模型的却是展开
/// 后的说明，而这一条查询回答的正是「这一段对话交给过模型什么」。
const HISTORY_SQL: &str = "\
SELECT role, COALESCE(model_content, content) AS content, attachments FROM ( \
    SELECT id, role, content, model_content, attachments FROM chat_turns \
    WHERE session_id = $1 AND failure IS NULL \
    ORDER BY id DESC LIMIT $2 \
) newest \
ORDER BY id";

/// The same window as [`HISTORY_SQL`], with the columns a reader needs and none
/// of the `failure IS NULL` filter: the unanswered half is exactly what a
/// reopened conversation has to show.
const TRANSCRIPT_SQL: &str = "\
SELECT id, role, content, failure, goal_id, attachments, created_at FROM ( \
    SELECT id, role, content, failure, goal_id, attachments, created_at FROM chat_turns \
    WHERE session_id = $1 \
    ORDER BY id DESC LIMIT $2 \
) newest \
ORDER BY id";

/// One person's sessions, newest activity first, with the line that opened each.
///
/// `strpos(lower(..))` rather than `ILIKE '%' || $2 || '%'`: the search text
/// comes from a person, and `%` or `_` typed into a search box is a character
/// being looked for, not a wildcard. Grouped by session with `MAX(created_at)`
/// as the order key, because "most recently spoken in" is what a reader means
/// when they expect the conversation they just left at the top.
///
/// `turns` counts the question rows, not the table's rows: a turn is written as
/// a pair, so counting rows would report two per exchange and the same
/// conversation would show twice as long a list of turns as it had turns.
const SESSIONS_SQL: &str = "\
SELECT s.session_id, \
       (SELECT t.content FROM chat_turns t \
         WHERE t.session_id = s.session_id AND t.role = 'user' \
         ORDER BY t.id LIMIT 1) AS first_line, \
       MAX(s.created_at) AS last_activity_at, \
       COUNT(*) FILTER (WHERE s.role = 'user') AS turns \
FROM chat_turns s \
WHERE s.owner = $1 \
GROUP BY s.session_id \
HAVING $2 = '' OR EXISTS ( \
    SELECT 1 FROM chat_turns m \
     WHERE m.session_id = s.session_id \
       AND strpos(lower(m.content), lower($2)) > 0 \
) \
ORDER BY MAX(s.created_at) DESC \
LIMIT $3";

/// 一行的附件列。`NULL` 是「这一行没有附件」——老行与助手那一行都是它，
/// 空数组与它在这里不区分：两者对读者、对提示词都是同一个意思。
fn parse_attachments(raw: Option<&str>) -> SFResult<Vec<ChatAttachment>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(raw).map_err(|e| {
        SFError::Database(format!(
            "chat transcript holds attachments this build cannot read: {e}"
        ))
    })
}

#[async_trait]
impl ChatTurnStore for PostgresChatTurnStore {
    async fn record_turn(
        &self,
        session_id: &str,
        owner: &str,
        user: &str,
        model_content: Option<&str>,
        attachments: &[ChatAttachment],
        assistant: &str,
    ) -> SFResult<()> {
        self.write(
            session_id,
            owner,
            user,
            model_content,
            attachments,
            Some(assistant),
            None,
            None,
        )
        .await
    }

    async fn record_dispatched_turn(
        &self,
        session_id: &str,
        owner: &str,
        user: &str,
        model_content: Option<&str>,
        attachments: &[ChatAttachment],
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
        self.write(
            session_id,
            owner,
            user,
            model_content,
            attachments,
            Some(assistant),
            None,
            Some(goal_id),
        )
        .await
    }

    async fn record_failed_turn(
        &self,
        session_id: &str,
        owner: &str,
        user: &str,
        attachments: &[ChatAttachment],
        failure: &str,
    ) -> SFResult<()> {
        self.write(
            session_id,
            owner,
            user,
            None,
            attachments,
            None,
            Some(failure),
            None,
        )
        .await
    }

    async fn history(&self, session_id: &str, limit: usize) -> SFResult<Vec<ChatTurn>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(HISTORY_SQL)
            .bind(session_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the chat transcript: {e}")))?;
        rows.into_iter()
            .map(|(role, content, attachments)| {
                let role = ChatTurnRole::parse(&role).ok_or_else(|| {
                    SFError::Database(format!(
                        "chat transcript holds a role this build cannot name: {role:?}"
                    ))
                })?;
                Ok(ChatTurn {
                    role,
                    content,
                    attachments: parse_attachments(attachments.as_deref())?,
                })
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
            Option<String>,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(TRANSCRIPT_SQL)
            .bind(session_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the chat transcript: {e}")))?;
        rows.into_iter()
            .map(
                |(id, role, content, failure, goal, attachments, created_at)| {
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
                        // 读者认得出这份附件是什么，但拿不到它的字节：字节留在记录里
                        // 给提示词用，翻一页历史不该把每张图再下载一遍。
                        attachments: parse_attachments(attachments.as_deref())?
                            .into_iter()
                            .map(|attachment| ChatAttachmentInfo {
                                name: attachment.name,
                                mime_type: attachment.mime_type,
                            })
                            .collect(),
                        created_at,
                    })
                },
            )
            .collect()
    }

    async fn sessions(
        &self,
        owner: &str,
        limit: usize,
        search: Option<&str>,
    ) -> SFResult<Vec<ChatSessionSummary>> {
        // Nobody's conversations are not listed for anyone: `""` is the owner of
        // rows written before the column existed and of connections that never
        // named a person, and answering that with every unowned conversation
        // would put them all in front of whoever asked with an empty identity.
        if owner.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let probe = search.unwrap_or("").trim().to_string();
        let rows: Vec<(String, String, chrono::DateTime<chrono::Utc>, i64)> =
            sqlx::query_as(SESSIONS_SQL)
                .bind(owner)
                .bind(&probe)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("list the chat sessions: {e}")))?;
        Ok(rows
            .into_iter()
            .map(
                |(session_id, first_line, last_activity_at, turns)| ChatSessionSummary {
                    session_id,
                    title: title_of(&first_line),
                    last_activity_at,
                    turns,
                },
            )
            .collect())
    }
}

/// The first thing said in a session, cut to something a list row can hold.
///
/// A title is a pointer, not the message: the whole first line lives in the
/// transcript and is one click away. Cutting at a character boundary matters
/// here because the first line is often Chinese, where a byte cut would split a
/// character and produce text that no longer says what it said.
fn title_of(first_line: &str) -> String {
    // cap: fixed — a list row's width, not a resource bound: the whole first line
    // stays in the transcript and is one click away.
    const MAX_CHARS: usize = 60;
    let mut title: String = first_line
        .lines()
        .next()
        .unwrap_or("")
        .chars()
        .take(MAX_CHARS)
        .collect();
    if first_line.chars().count() > MAX_CHARS {
        title.push('…');
    }
    title.trim().to_string()
}
