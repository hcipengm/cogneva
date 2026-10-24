//! The chat transcript's storage, against a live PostgreSQL.
//!
//! Four facts here are the database's rather than the caller's, and none of them
//! can be checked without a server: that a turn is written as a pair so a reader
//! never sees a question whose answer is still in flight, that a turn whose
//! reply never came is kept but kept out of the prompt's history, that the bound
//! applies per session rather than to the table as a whole, and that the
//! transcript survives a new store handle -- which is the whole point, since the
//! store this replaced lived in one process and died with it.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test chat_turn_store -- --ignored
//! ```
//!
//! Every row carries a per-run session id and is deleted at the end.

use sqlx::PgPool;

use cog_storage::{
    ChatTurn, ChatTurnRole, ChatTurnStore, PostgresChatTurnStore, ROWS_KEPT_PER_SESSION,
};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

struct Probe {
    store: PostgresChatTurnStore,
    pool: PgPool,
    /// A session id no other run and no other test shares.
    session: String,
}

async fn probe() -> Probe {
    let url = database_url();
    migrate_once(&url).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the throwaway database");
    let session = format!("probe-chat-turns-{}", uuid::Uuid::new_v4());
    Probe {
        store: PostgresChatTurnStore::new(pool.clone()),
        pool,
        session,
    }
}

/// Apply the migrations under a session advisory lock, so concurrent test
/// binaries do not race on the same DDL.
async fn migrate_once(url: &str) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to take the lock");
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(97_003_i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    cog_storage::migrate::Migrator::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .run(url)
        .await
        .expect("apply the migrations to the throwaway database");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(97_003_i64)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

async fn cleanup(probe: &Probe) {
    sqlx::query("DELETE FROM chat_turns WHERE session_id LIKE $1")
        .bind(format!("{}-%", probe.session))
        .execute(&probe.pool)
        .await
        .expect("delete the rows this test wrote");
}

fn turn(role: ChatTurnRole, content: &str) -> ChatTurn {
    ChatTurn {
        role,
        content: content.to_string(),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_turn_reads_back_in_the_order_it_was_said_and_from_another_handle() {
    let probe = probe().await;
    probe
        .store
        .record_turn(&probe.session, "第一问", "第一答")
        .await
        .expect("record the first turn");
    probe
        .store
        .record_turn(&probe.session, "第二问", "第二答")
        .await
        .expect("record the second turn");

    // 换一个句柄读：这正是这次改动的要点——记录不属于哪个进程，重启之后新实例
    // 读到的必须是同一段对话。
    let other = PostgresChatTurnStore::new(probe.pool.clone());
    let history = other
        .history(&probe.session, 40)
        .await
        .expect("read the transcript back");
    assert_eq!(
        history,
        vec![
            turn(ChatTurnRole::User, "第一问"),
            turn(ChatTurnRole::Assistant, "第一答"),
            turn(ChatTurnRole::User, "第二问"),
            turn(ChatTurnRole::Assistant, "第二答"),
        ],
        "读回来的顺序或角色不对：{history:?}"
    );

    // 只要最近几条时给的是最近那几条，且仍然按时间正序——模型拿到的历史必须从旧到新。
    let tail = probe
        .store
        .history(&probe.session, 2)
        .await
        .expect("read the tail");
    assert_eq!(
        tail,
        vec![
            turn(ChatTurnRole::User, "第二问"),
            turn(ChatTurnRole::Assistant, "第二答"),
        ],
        "最近两条读错了：{tail:?}"
    );

    cleanup(&probe).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_session_that_was_never_recorded_reads_empty_and_a_limit_of_zero_asks_for_nothing() {
    let probe = probe().await;
    let history = probe
        .store
        .history(&format!("{}-never", probe.session), 40)
        .await
        .expect("a session nobody wrote reads empty, not an error");
    assert!(history.is_empty(), "{history:?}");

    probe
        .store
        .record_turn(&probe.session, "问", "答")
        .await
        .expect("record a turn");
    // limit 0 是「一条也不要」，不是「不限」——读成后者会把整段对话塞进一次请求。
    let none = probe
        .store
        .history(&probe.session, 0)
        .await
        .expect("read with a zero limit");
    assert!(none.is_empty(), "{none:?}");

    cleanup(&probe).await;
}

/// The bound is per session, and the transcript of another conversation is not
/// what pays for it.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_bound_trims_one_session_and_leaves_another_alone() {
    let probe = probe().await;
    let busy = format!("{}-busy", probe.session);
    let quiet = format!("{}-quiet", probe.session);
    probe
        .store
        .record_turn(&quiet, "安静的一问", "安静的一答")
        .await
        .expect("record the quiet session");

    // 写到刚好越过上界：每轮两行，所以多写一轮就一定会裁。
    let over = (ROWS_KEPT_PER_SESSION / 2) + 2;
    for i in 0..over {
        probe
            .store
            .record_turn(&busy, &format!("问{i}"), &format!("答{i}"))
            .await
            .expect("record a turn of the busy session");
    }

    let kept = probe
        .store
        .history(&busy, ROWS_KEPT_PER_SESSION as usize * 4)
        .await
        .expect("read the busy transcript");
    assert!(
        kept.len() <= ROWS_KEPT_PER_SESSION as usize,
        "上界没生效：留着 {} 行，界是 {ROWS_KEPT_PER_SESSION}",
        kept.len()
    );
    // 留下的必须是最新的尾段：裁掉的新内容比留住的老内容的代价大得多。
    let last = kept.last().expect("the newest turn is always kept");
    assert_eq!(
        last,
        &turn(ChatTurnRole::Assistant, &format!("答{}", over - 1)),
        "最新一轮被裁掉了：{last:?}"
    );

    let quiet_history = probe
        .store
        .history(&quiet, 10)
        .await
        .expect("read the quiet transcript");
    assert_eq!(
        quiet_history,
        vec![
            turn(ChatTurnRole::User, "安静的一问"),
            turn(ChatTurnRole::Assistant, "安静的一答"),
        ],
        "另一段对话被这一段的裁剪带走了：{quiet_history:?}"
    );

    sqlx::query("DELETE FROM chat_turns WHERE session_id LIKE $1")
        .bind(format!("{}-%", probe.session))
        .execute(&probe.pool)
        .await
        .expect("delete the rows this test wrote");
}

/// An exchange whose reply never came is kept, and it is kept out of the
/// history a prompt is built from.
///
/// This is the database's half of the guarantee: what the user said survives an
/// upstream outage, and the reason it went unanswered is on the same row. The
/// row count is read directly because `history` is the view that deliberately
/// hides it -- a test that only asked `history` would pass on a store that
/// dropped the row entirely.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_unanswered_turn_is_kept_out_of_the_history_but_not_out_of_the_table() {
    let probe = probe().await;
    probe
        .store
        .record_failed_turn(&probe.session, "上游坏了的时候问的话", "API error 503")
        .await
        .expect("record an unanswered turn");

    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT role, content, failure FROM chat_turns WHERE session_id = $1 ORDER BY id",
    )
    .bind(&probe.session)
    .fetch_all(&probe.pool)
    .await
    .expect("read the rows back");
    assert_eq!(
        rows,
        vec![
            (
                "user".to_string(),
                "上游坏了的时候问的话".to_string(),
                Some("API error 503".to_string())
            ),
            (
                "assistant".to_string(),
                String::new(),
                Some("API error 503".to_string())
            ),
        ],
        "没答出来的那一轮没有连原因一起落库：{rows:?}"
    );

    let history = probe
        .store
        .history(&probe.session, 40)
        .await
        .expect("read the history");
    assert!(
        history.is_empty(),
        "失败通知被当成模型说过的话喂回上下文：{history:?}"
    );

    // 之后正常的一轮不能被它挡住，也不该把它顶出上下文窗口。
    probe
        .store
        .record_turn(&probe.session, "恢复之后问的话", "答")
        .await
        .expect("record an answered turn");
    let history = probe
        .store
        .history(&probe.session, 40)
        .await
        .expect("read the history again");
    assert_eq!(
        history,
        vec![
            turn(ChatTurnRole::User, "恢复之后问的话"),
            turn(ChatTurnRole::Assistant, "答"),
        ],
        "恢复之后的那一轮读错了：{history:?}"
    );

    cleanup(&probe).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_turn_without_a_session_is_refused_rather_than_filed_under_nothing() {
    let probe = probe().await;
    let refused = probe.store.record_turn("   ", "问", "答").await;
    assert!(
        refused.is_err(),
        "没有会话的一轮必须被拒：它落进一段谁也问不到的记录里"
    );
    cleanup(&probe).await;
}
