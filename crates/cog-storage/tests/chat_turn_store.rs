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

mod common;

use sqlx::PgPool;

use cog_storage::{
    ChatTurn, ChatTurnRole, ChatTurnStore, PostgresChatTurnStore, ROWS_KEPT_PER_SESSION,
};

use cog_test_support::database_url;

struct Probe {
    store: PostgresChatTurnStore,
    pool: PgPool,
    /// A session id no other run and no other test shares.
    session: String,
    /// 这段对话属于谁。每次跑一个独占的值，名单那一格才不会读到别人写下的行。
    owner: String,
}

async fn probe() -> Probe {
    let url = database_url();
    common::migrate_once(&url, 97_003).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the throwaway database");
    let session = format!("probe-chat-turns-{}", uuid::Uuid::new_v4());
    let owner = format!("probe-owner-{}", uuid::Uuid::new_v4());
    Probe {
        store: PostgresChatTurnStore::new(pool.clone()),
        pool,
        session,
        owner,
    }
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
        attachments: Vec::new(),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_turn_reads_back_in_the_order_it_was_said_and_from_another_handle() {
    let probe = probe().await;
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "第一问",
            None,
            &[],
            "第一答",
            &[],
            None,
        )
        .await
        .expect("record the first turn");
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "第二问",
            None,
            &[],
            "第二答",
            &[],
            None,
        )
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
        .record_turn(
            &probe.session,
            &probe.owner,
            "问",
            None,
            &[],
            "答",
            &[],
            None,
        )
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
        .record_turn(
            &quiet,
            &probe.owner,
            "安静的一问",
            None,
            &[],
            "安静的一答",
            &[],
            None,
        )
        .await
        .expect("record the quiet session");

    // 写到刚好越过上界：每轮两行，所以多写一轮就一定会裁。
    let over = (ROWS_KEPT_PER_SESSION / 2) + 2;
    for i in 0..over {
        probe
            .store
            .record_turn(
                &busy,
                &probe.owner,
                &format!("问{i}"),
                None,
                &[],
                &format!("答{i}"),
                &[],
                None,
            )
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
        .record_failed_turn(
            &probe.session,
            &probe.owner,
            "上游坏了的时候问的话",
            &[],
            "API error 503",
            &[],
            None,
        )
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
        .record_turn(
            &probe.session,
            &probe.owner,
            "恢复之后问的话",
            None,
            &[],
            "答",
            &[],
            None,
        )
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

/// A dispatched exchange keeps the goal it created, where a reopened page can
/// read it.
///
/// The goal used to exist only on the live frame and in the reply's wording, so
/// reloading the page turned a conversation that was still running on the server
/// into an ordinary Q&A -- the one reading that is wrong in the direction that
/// matters, because the user closes the tab believing there was nothing left to
/// wait for. The column is read directly as well as through `transcript`: the
/// read side could fill the field from anywhere, and what has to be true is that
/// the row holds it.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_dispatched_turn_keeps_its_goal_on_the_row_the_history_reads() {
    let probe = probe().await;
    probe
        .store
        .record_dispatched_turn(
            &probe.session,
            &probe.owner,
            "把这件事做了",
            None,
            &[],
            "已受理",
            "goal-7788",
            &[],
            None,
        )
        .await
        .expect("record a dispatched turn");
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "那这个呢",
            None,
            &[],
            "这个直接答",
            &[],
            None,
        )
        .await
        .expect("record an answered turn");

    let rows: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT role, goal_id FROM chat_turns WHERE session_id = $1 ORDER BY id")
            .bind(&probe.session)
            .fetch_all(&probe.pool)
            .await
            .expect("read the rows back");
    assert_eq!(
        rows,
        vec![
            ("user".to_string(), None),
            ("assistant".to_string(), Some("goal-7788".to_string())),
            ("user".to_string(), None),
            ("assistant".to_string(), None),
        ],
        "交出去的那一轮没把目标落在它的答复行上：{rows:?}"
    );

    let transcript = probe
        .store
        .transcript(&probe.session, 40)
        .await
        .expect("read the transcript");
    let goals: Vec<Option<&str>> = transcript.iter().map(|line| line.goal.as_deref()).collect();
    assert_eq!(
        goals,
        vec![None, Some("goal-7788"), None, None],
        "读回来的每一行都要带着它自己的目标与否：{goals:?}"
    );

    // 上下文窗口不受影响：目标身份是给读者的，不是给模型的。
    let history = probe
        .store
        .history(&probe.session, 40)
        .await
        .expect("read the history");
    assert_eq!(history.len(), 4, "交出去的那一轮也该进上下文：{history:?}");

    cleanup(&probe).await;
}

/// 这一轮调过的工具与回答者，随助手那一行一起回来。
///
/// 重开页面时那张待办面板只从记录里画得出来：实时帧在页面关掉那一刻就断了。
/// 所以这里读的不是「写进去了没有」，而是「读回来还是不是那一笔」——名字、入参、
/// 结果、结局、两头的时刻，以及谁答的。少任何一格，面板要么空着，要么把跑到
/// 一半的调用画成已经返回。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_tools_a_turn_ran_and_its_answerer_read_back_with_the_row() {
    let probe = probe().await;
    let started = chrono::Utc::now();
    let finished = started + chrono::Duration::seconds(3);
    let calls = vec![
        cog_storage::ChatToolCall {
            tool_call_id: "call-write".into(),
            tool_name: "write_file".into(),
            args: "{\"path\":\"src/main.rs\"}".into(),
            result: Some("{\"ok\":true}".into()),
            status: cog_storage::ChatToolCallStatus::Ok,
            started_at: started,
            finished_at: Some(finished),
        },
        cog_storage::ChatToolCall {
            tool_call_id: "call-hung".into(),
            tool_name: "run_command".into(),
            args: "{\"command\":\"sleep 600\"}".into(),
            result: None,
            status: cog_storage::ChatToolCallStatus::Running,
            started_at: started,
            finished_at: None,
        },
    ];
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "把这件事做了",
            None,
            &[],
            "做完了",
            &calls,
            Some("kai-a9ab11fc"),
        )
        .await
        .expect("record a turn that ran tools");

    let transcript = probe
        .store
        .transcript(&probe.session, 40)
        .await
        .expect("read the transcript");
    assert_eq!(transcript.len(), 2, "{transcript:?}");
    // 调用是助手那一行做的事，不是问句的：问的人一个工具也没跑。
    assert!(
        transcript[0].tool_calls.is_empty() && transcript[0].agent.is_none(),
        "问句那一行被写上了别人的工具调用：{transcript:?}"
    );
    assert_eq!(
        transcript[1].tool_calls, calls,
        "读回来的调用与写进去的不是同一笔"
    );
    assert_eq!(
        transcript[1].agent.as_deref(),
        Some("kai-a9ab11fc"),
        "回答者没跟着回来：重开之后这一段是谁答的认不出来"
    );

    // 没调工具的轮不因此带上一个空数组之外的东西，也没被弄成失败。
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "再问一句",
            None,
            &[],
            "答",
            &[],
            None,
        )
        .await
        .expect("record a plain turn");
    let transcript = probe
        .store
        .transcript(&probe.session, 40)
        .await
        .expect("read the transcript again");
    assert!(
        transcript[3].tool_calls.is_empty() && transcript[3].agent.is_none(),
        "没调工具的一轮被写上了上一轮的调用：{transcript:?}"
    );

    cleanup(&probe).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_turn_without_a_session_is_refused_rather_than_filed_under_nothing() {
    let probe = probe().await;
    let refused = probe
        .store
        .record_turn("   ", &probe.owner, "问", None, &[], "答", &[], None)
        .await;
    assert!(
        refused.is_err(),
        "没有会话的一轮必须被拒：它落进一段谁也问不到的记录里"
    );
    cleanup(&probe).await;
}

/// 随一句话上来的图，随那一行一起回来。
///
/// 这一格量的是列的存在与它装得下的东西：历史要读到同一个 base64（下一轮把它
/// 装回媒体块，模型才看得见那张图），读者的记录只拿名字与类型（重开页面认得出
/// 「说过一张图」，不必把图本身再下载一遍）。两边的差别正是这两条读法的差别。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_attachment_rides_with_its_row_and_keeps_its_bytes() {
    let probe = probe().await;
    let shot = cog_storage::ChatAttachment {
        name: Some("shot.png".into()),
        mime_type: "image/png".into(),
        data: "QUJD".into(),
    };
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "这里错了",
            None,
            std::slice::from_ref(&shot),
            "看到了",
            &[],
            None,
        )
        .await
        .expect("record a turn that carried a picture");

    let history = probe
        .store
        .history(&probe.session, 40)
        .await
        .expect("read the history");
    assert_eq!(
        history[0].attachments,
        vec![shot.clone()],
        "字节在历史里丢了"
    );
    assert!(
        history[1].attachments.is_empty(),
        "回答那一半不该带附件：{:?}",
        history[1].attachments
    );

    let transcript = probe
        .store
        .transcript(&probe.session, 40)
        .await
        .expect("read the transcript");
    assert_eq!(
        transcript[0].attachments,
        vec![cog_storage::ChatAttachmentInfo {
            name: Some("shot.png".into()),
            mime_type: "image/png".into(),
        }],
        "读者的记录里少了这条附件：{:?}",
        transcript[0].attachments
    );

    cleanup(&probe).await;
}

/// 一条命令展开之后：模型那一侧读回展开的说明，读记录的人读回人打的那一行。
///
/// 两列分开不是因为「存两份更保险」，而是因为它们回答的不是同一个问题：后面几轮
/// 要照着那份说明继续做，而重开页面的人要看的是自己打的那一句——把整份 SKILL.md
/// 画成「我刚说的话」，是另一种谎。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_command_keeps_both_the_line_typed_and_the_instructions_the_model_read() {
    let probe = probe().await;
    let instructions = "[用户在这一轮点了技能「web-research」]\n\n先搜，再读。";
    probe
        .store
        .record_turn(
            &probe.session,
            &probe.owner,
            "/web-research 查一下 1.90",
            Some(instructions),
            &[],
            "查到了",
            &[],
            None,
        )
        .await
        .expect("record a command turn");

    let history = probe
        .store
        .history(&probe.session, 40)
        .await
        .expect("read the history");
    assert_eq!(
        history[0].content, instructions,
        "历史交给模型的是人打的那一行，不是展开后的说明：{}",
        history[0].content
    );

    let transcript = probe
        .store
        .transcript(&probe.session, 40)
        .await
        .expect("read the transcript");
    assert_eq!(
        transcript[0].content, "/web-research 查一下 1.90",
        "读记录的人看到的是展开后的说明，不是自己打的那一行：{}",
        transcript[0].content
    );

    cleanup(&probe).await;
}

/// 名单是「一个人的会话，最近说话的在前，搜索只在他自己的会话里找」。
///
/// 这三件事都是那条 SQL 的后果，替身按同一个意图又写了一遍：两边一起错的
/// 时候只有真库上这一条会红。所以这里要的是数据库自己的答案，不是调用方的。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_listing_is_one_persons_conversations_newest_first_and_search_is_literal() {
    let probe = probe().await;
    let mine_first = format!("{}-first", probe.session);
    let mine_second = format!("{}-second", probe.session);
    let theirs = format!("{}-theirs", probe.session);
    let stranger = format!("stranger-{}", probe.owner);

    probe
        .store
        .record_turn(
            &mine_first,
            &probe.owner,
            "第一段 百分之百",
            None,
            &[],
            "第一答",
            &[],
            None,
        )
        .await
        .expect("record the first conversation");
    probe
        .store
        .record_turn(
            &theirs,
            &stranger,
            "别人的会话",
            None,
            &[],
            "别人的答",
            &[],
            None,
        )
        .await
        .expect("record someone else's conversation");
    probe
        .store
        .record_turn(
            &mine_second,
            &probe.owner,
            "第二段",
            None,
            &[],
            "第二答",
            &[],
            None,
        )
        .await
        .expect("record the second conversation");

    let listed = probe
        .store
        .sessions(&probe.owner, 50, None)
        .await
        .expect("list the sessions");
    let ids: Vec<&str> = listed.iter().map(|s| s.session_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![mine_second.as_str(), mine_first.as_str()],
        "名单没有把最近说话的排在前面，或者把别人的会话也列了出来"
    );
    assert_eq!(
        listed[1].title, "第一段 百分之百",
        "标题不是这一段开口的第一句：{}",
        listed[1].title
    );
    assert_eq!(
        listed[1].turns, 1,
        "一段一问答被数成了两轮：turns 数的是问句，不是这一对的两行"
    );

    // 搜索是字面的：`%` 是人在找这个字，不是「什么都算」。
    let literal = probe
        .store
        .sessions(&probe.owner, 50, Some("百"))
        .await
        .expect("search a character that is there");
    assert_eq!(
        literal
            .iter()
            .map(|s| s.session_id.as_str())
            .collect::<Vec<_>>(),
        vec![mine_first.as_str()],
        "搜索没有在自己的一段话里找到这几个字"
    );
    let wildcard = probe
        .store
        .sessions(&probe.owner, 50, Some("%"))
        .await
        .expect("search a percent sign");
    assert!(
        wildcard.is_empty(),
        "`%` 被当成了通配符：{} 段会话被它全捞了出来",
        wildcard.len()
    );
    let other_persons_word = probe
        .store
        .sessions(&probe.owner, 50, Some("别人的会话"))
        .await
        .expect("search a line someone else wrote");
    assert!(
        other_persons_word.is_empty(),
        "搜索翻到了别人的会话里：名单的边界只挡了不过滤的那一次"
    );

    // 空身份一条都不给：那不是一个「没有会话的人」，是「不知道是谁」。
    let unowned = probe
        .store
        .sessions("", 50, None)
        .await
        .expect("list with an empty identity");
    assert!(unowned.is_empty(), "空身份拿到了会话名单");

    cleanup(&probe).await;
}
