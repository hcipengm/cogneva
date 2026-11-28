//! 重开一段对话，它当时做过的事还在。
//!
//! 一段对话的实时帧只活在连接里：页面一关，工具调用那一串帧就断了。而这一段
//! 手边那张待办面板，正是从这串调用里折出来的——不落库，重开之后它只剩一问
//! 一答，读者看到的是「它什么都没做」。
//!
//! 要落的是四样东西，缺一样就有一半回不来：
//!
//! 1. **调用本身**（名字、入参、结果、结局、两头的时刻）：面板要画的是「做了
//!    什么、做成了没有」，不是「调用发生过」；
//! 2. **回答者**（`agent_id`）：工具调用按它在对话里认领，而这个名字里带进程
//!    指纹，重开页面时前端自己算不出来；
//! 3. **回程**：`list_messages` 把这两格交回去，页面在 `hydrate` 时重放；
//! 4. **归属**：重放回来的调用要真的被这一段收下——要么按回答者，要么按会话号。
//!
//! 数据库那一半（真的写进去、真的读回来）在 `cog-storage/tests/chat_turn_store.rs`
//! 的真库用例里跑；网关那一半（帧变成日记里的一笔）在 `cog-gateway/src/chat.rs`
//! 的单测里跑。这里钉的是接线与形状：列在不在、语句写不写、帧读不读、页面接不接。

use crate::common::routes::read_repo;

const CORE: &str = "crates/cog-core/src/contract/conversation.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const SESSIONS: &str = "crates/cog-gateway/src/sessions.rs";
const STORE: &str = "crates/cog-storage/src/postgres/chat_turn_store.rs";
const PG_UP: &str = "crates/cog-storage/migrations/postgres/104_chat_turn_tool_calls.up.sql";
const PG_DOWN: &str = "crates/cog-storage/migrations/postgres/104_chat_turn_tool_calls.down.sql";
const MYSQL_UP: &str = "crates/cog-storage/migrations/mysql/097_chat_turn_tool_calls.up.sql";
const CLIENT: &str = "web/src/api/client.ts";
const CHAT_STORE_TS: &str = "web/src/store/chatStore.ts";
const STREAM_STORE_TS: &str = "web/src/store/streamStore.ts";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 一条 `const NAME: &str = "…";` 的正文。判据只读 SQL 本身，不读旁边解释它的注释。
fn the_sql(source: &str, name: &str, file: &str) -> String {
    let opener = format!("const {name}: &str = \"");
    let Some((_, after)) = source.split_once(&opener) else {
        panic!("{file} 里找不到 {name}：这条判据读的那条语句改了名字或搬了家");
    };
    let body = after.split_once("\";").map(|(sql, _)| sql).unwrap_or(after);
    body.to_string()
}

/// 契约里有一个「一笔调用」的形状，它带着收没收尾。
///
/// 只记「调用发生过」是不够的：重开之后一笔没有结果的调用，与一笔跑完了的调用
/// 在面板上是两件事——一件还在跑，一件做完了。
#[test]
fn a_recorded_call_carries_what_it_was_given_and_what_it_gave_back() {
    let core = read_repo(CORE);
    for field in [
        "pub tool_call_id: String",
        "pub tool_name: String",
        "pub args: String",
        "pub result: Option<String>",
        "pub status: ChatToolCallStatus",
        "pub started_at: chrono::DateTime<chrono::Utc>",
        "pub finished_at: Option<chrono::DateTime<chrono::Utc>>",
    ] {
        assert!(
            core.contains(field),
            "{CORE} 的 ChatToolCall 缺了 `{field}`：记录里的一笔调用少了这一格，重开时的面板就画不全"
        );
    }
    assert!(
        core.contains("pub enum ChatToolCallStatus") && core.contains("Running"),
        "{CORE} 里没有「还在跑」这一种结局：没有结果的调用会被读成已经返回"
    );
    assert!(
        core.contains("pub tool_calls: Vec<ChatToolCall>"),
        "{CORE} 的 ChatTurnRecord 不带这一轮的调用：记录读回来时无处安放"
    );
    assert!(
        core.contains("pub agent: Option<String>"),
        "{CORE} 的 ChatTurnRecord 不带回答者：重开之后是谁答的、这些调用归给谁都不知道"
    );
}

/// 两列都落在记录表上，两种方言各一份，不另开表。
#[test]
fn the_calls_and_the_answerer_are_two_columns_on_the_transcript_table() {
    let pg = read_repo(PG_UP);
    for column in ["tool_calls", "agent_id"] {
        assert!(
            pg.contains(&format!(
                "ALTER TABLE chat_turns ADD COLUMN IF NOT EXISTS {column}"
            )),
            "{PG_UP} 没在记录表上加 {column}：这一格没有落处"
        );
    }
    assert!(
        !pg.contains("CREATE TABLE"),
        "{PG_UP} 另开了一张表：调用与它所属的那一轮会像以前那样各说各话"
    );

    let down = read_repo(PG_DOWN);
    for column in ["tool_calls", "agent_id"] {
        assert!(
            down.contains(&format!("DROP COLUMN IF EXISTS {column}")),
            "{PG_DOWN} 不回滚 {column}：迁移回退之后留下一个它不认识的列"
        );
    }

    let mysql = read_repo(MYSQL_UP);
    assert!(
        mysql.contains("ADD COLUMN tool_calls LONGTEXT")
            && mysql.contains("ADD COLUMN agent_id VARCHAR(191)"),
        "{MYSQL_UP} 没给 MySQL 部署加上这两列：同一段对话在另一个部署上重开是空的"
    );
    assert!(
        !mysql.contains("tool_calls TEXT"),
        "{MYSQL_UP} 用了 TEXT：它的 64 KiB 会把一份长调用清单静默截断"
    );
}

/// 写进去、读回来：三条 INSERT 都带这两格，TRANSCRIPT_SQL 也把它们选出来。
#[test]
fn every_insert_writes_the_calls_and_the_reader_selects_them_back() {
    let store = read_repo(STORE);
    for name in [
        "INSERT_TURN_SQL",
        "INSERT_DISPATCHED_TURN_SQL",
        "INSERT_FAILED_TURN_SQL",
    ] {
        let sql = the_sql(&store, name, STORE);
        assert!(
            sql.contains("tool_calls, agent_id"),
            "{STORE} 的 {name} 不写这两格：这一轮调过的工具与回答者到不了库里\n{sql}"
        );
    }
    let transcript = the_sql(&store, "TRANSCRIPT_SQL", STORE);
    assert!(
        transcript.contains("tool_calls") && transcript.contains("agent_id"),
        "{STORE} 的 TRANSCRIPT_SQL 不选这两格：库里有，读的人拿不到\n{transcript}"
    );

    assert!(
        store.contains("fn parse_tool_calls(") && store.contains("tool_calls: parse_tool_calls("),
        "{STORE} 把工具调用那一列原样交了出去：它是一段 JSON 文本，读者要的是调用本身"
    );
}

/// 帧到日记：开始那一帧记一笔，结束那一帧把结果补回同一笔上。
#[test]
fn the_frames_become_one_entry_each_in_the_journal() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("tool_calls: Vec<cog_core::ChatToolCall>"),
        "{CHAT} 的日记里没有这一条：帧读完之后调用就只剩发出去的那一眼"
    );
    assert!(
        chat.contains("journal.tool_calls.push(cog_core::ChatToolCall"),
        "{CHAT} 不在开始那一帧上记一笔：这一轮结束之后什么也没留下"
    );
    assert!(
        chat.contains("call_at.remove(&tool_call_id)"),
        "{CHAT} 结束那一帧没有接回开始那一笔：重开之后看到的是一笔没有结果的调用加一笔没有开始的返回"
    );
    assert!(
        chat.contains("journal.agent = Some(agent_id.to_string())"),
        "{CHAT} 不记回答者：记录里的调用没有可以归给的人"
    );
    // 收尾那一刻也要落下，不然重开之后每一笔的时长都画成 0。
    assert!(
        chat.contains("call.finished_at = Some(timestamp)"),
        "{CHAT} 不记调用的收尾时刻：重开之后面板上每一笔都像是刚开始就结束了"
    );
}

/// 回程：这段对话的记录里带着这两格交给页面。
#[test]
fn the_history_endpoint_hands_the_calls_and_the_answerer_back() {
    let sessions = read_repo(SESSIONS);
    let Some((_, after)) = sessions.split_once("pub async fn list_messages_handler(") else {
        panic!("{SESSIONS} 里找不到 list_messages_handler：这条判据读的函数改了名字或搬了家");
    };
    let body = after
        .split("pub async fn send_message_handler(")
        .next()
        .unwrap_or(after);
    assert!(
        body.contains("\"tool_calls\": turn.tool_calls"),
        "{SESSIONS} 不把这一轮的调用交回去：页面上那张待办面板重开后画不出来"
    );
    assert!(
        body.contains("\"agent_id\": turn.agent"),
        "{SESSIONS} 不把回答者交回去：重放回来的调用认不出该归给谁"
    );
}

/// 页面这一侧：认得出这一格，重放回同一本账，并且归到这一段下面。
#[test]
fn the_page_replays_the_calls_into_the_conversation_it_belongs_to() {
    let client = read_repo(CLIENT);
    assert!(
        client.contains("tool_calls?: ChatToolCallLine[] | null"),
        "{CLIENT} 的记录行不认 tool_calls：网关交回去的这一格被类型挡在外面"
    );
    assert!(
        client.contains("agent_id?: string | null"),
        "{CLIENT} 的记录行不认 agent_id：回答者到不了页面这一侧"
    );

    let stream = read_repo(STREAM_STORE_TS);
    assert!(
        stream.contains("session?: string | null"),
        "{STREAM_STORE_TS} 的调用格没有会话归属：重放回来的调用只能靠回答者认领，而回答者可能没被记下"
    );
    assert!(
        stream.contains("startedAt: record.startedAt ?? record.at"),
        "{STREAM_STORE_TS} 不用记录里的起点：重开之后每一笔的时长都画成 0"
    );

    let store = read_repo(CHAT_STORE_TS);
    assert!(
        store.contains("const replay = useStreamStore.getState().noteToolCall;"),
        "{CHAT_STORE_TS} 的 hydrate 不把读回来的调用放进同一本账：待办面板重开是空的"
    );
    assert!(
        store.contains("session: sessionId,"),
        "{CHAT_STORE_TS} 重放时不带会话号：回答者空着的老轮会被别段对话认领或者没人认领"
    );
    assert!(
        store.contains("const agentId = session.agentId ?? rememberedAgent;")
            && store.contains("agentId,"),
        "{CHAT_STORE_TS} 不把记录里的回答者补回这一段：`agent.identity` 帧重开时不会重发"
    );

    let pane = read_repo(PANE);
    assert!(
        pane.contains("call.session && call.session === sessionId"),
        "{PANE} 只按回答者认领调用：重开之后靠会话号放回来的那些一条也进不了这一段"
    );
}
