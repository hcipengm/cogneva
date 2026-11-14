//! 过去的会话：找得回来、只找自己的、重开时接上原来那一段。
//!
//! 一场对话的记录在服务端留着，而页面上那份「开着的那几段」记在浏览器里。换一台
//! 机器、清掉浏览器存储、同事拿了另一台——记录还在，路没了。要补的正是那条路：
//!
//! 1. **名单**是这个人自己的会话，最近说话的在前，第一句当作标题；
//! 2. **搜索**在这几个字上做字面匹配：搜索框里打的 `%` 是人在找 `%`，不是「什么都
//!    算」；而且只在自己那几段里找；
//! 3. **没有身份**要拒绝，不是给一张空名单——「你没有会话」与「我们不知道你是谁」
//!    是两个答案，只有一个是实话；
//! 4. **点开**用的是原来那个会话号：接上的是原来那一段，不是新开一段同名的。
//!
//! 名单查询的答案在数据库里，那一半在 `cog-storage/tests/chat_turn_store.rs` 的
//! 真库用例里跑；这里钉的是接线：路由挂在哪里、handler 读不读存储、记录里带不带
//! 属于谁、页面有没有这条路。

use crate::common::routes::read_repo;

const ROUTES: &str = "crates/cog-gateway/src/lib.rs";
const SESSIONS: &str = "crates/cog-gateway/src/sessions.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";
const STORE: &str = "crates/cog-storage/src/postgres/chat_turn_store.rs";
const PG_MIGRATION: &str = "crates/cog-storage/migrations/postgres/103_chat_turn_owner.up.sql";
const MYSQL_MIGRATION: &str = "crates/cog-storage/migrations/mysql/096_chat_turn_owner.up.sql";
const CLIENT: &str = "web/src/api/client.ts";
const STORE_TS: &str = "web/src/store/chatStore.ts";
const PANEL: &str = "web/src/conversation/SessionHistory.tsx";
const DOCK: &str = "web/src/conversation/SessionDock.tsx";

/// `list_sessions_handler` 的函数体：签名之后到下一个 handler 之前。
fn the_handler_body(source: &str) -> &str {
    let Some((_, after)) = source.split_once("pub async fn list_sessions_handler(") else {
        panic!("{SESSIONS} 里找不到 list_sessions_handler：这条判据读的函数改了名字或搬了家");
    };
    let after = after
        .split("pub async fn create_session_handler(")
        .next()
        .unwrap_or(after);
    after
}

/// 名单这条路在登录之后，而且真的读记录。
#[test]
fn the_listing_is_served_behind_the_login_and_reads_the_transcript() {
    let routes = read_repo(ROUTES);
    assert!(
        routes.contains(".route(\"/api/v1/sessions\", get(sessions::list_sessions_handler))"),
        "{ROUTES} 没有登记名单的入口：页面没有地方问「我有哪些会话」"
    );

    // 这条路由必须落在挂了鉴权的那个分块里。挪到公开路由上，名单就变成了一份
    // 谁都能读的名单——而它是按人过滤的，过滤所依据的身份根本不会到达 handler。
    let block_at = routes
        .find("let viewer_routes")
        .unwrap_or_else(|| panic!("{ROUTES} 里找不到 viewer_routes：这一块改了名字"));
    let block = &routes[block_at..];
    let guard_at = block
        .find("require_role(")
        .unwrap_or_else(|| panic!("{ROUTES} 的 viewer_routes 不再挂 require_role：名单没有门槛"));
    let listing_at = block
        .find("sessions::list_sessions_handler")
        .expect("名单的路由不在 viewer_routes 里");
    assert!(
        listing_at < guard_at,
        "{ROUTES} 把名单挂到了鉴权之外：handler 读不到 Claims，名单只能退回「谁都不是」"
    );
    assert!(
        block[guard_at..].contains(".layer(auth_layer.clone())"),
        "{ROUTES} 的 viewer_routes 不再挂 auth_layer：Claims 不会被注入，名单会一律 401"
    );

    let sessions = read_repo(SESSIONS);
    let body = the_handler_body(&sessions);
    assert!(
        body.contains("state.chat_turns.as_ref()"),
        "{SESSIONS} 的名单不读聊天记录库：它只会答出一张空名单，而记录一直留着"
    );
    assert!(
        body.contains("store.sessions(owner, limit, q.search.as_deref())"),
        "{SESSIONS} 不把这个人、上限与搜索词交给存储面：名单、排序、搜索三者只剩一处是空的"
    );
    // 空名单那一句只能出现在「这套部署没有记录库」的分支里。放在读存储之前，
    // 就等于不管库里有什么都答「你没有会话」。
    if let (Some(empty), Some(store)) = (body.find("\"items\": []"), body.find("state.chat_turns"))
    {
        assert!(
            store < empty,
            "{SESSIONS} 还没读记录就先答了一张空名单：记录里有的会话永远列不出来"
        );
    }
}

/// 名单按人过滤；没有身份就拒绝，不给一张空名单。
#[test]
fn only_my_own_conversations_are_listed_and_no_identity_is_refused() {
    let sessions = read_repo(SESSIONS);
    let body = the_handler_body(&sessions);
    assert!(
        body.contains("c.sub.trim()"),
        "{SESSIONS} 不从 Claims 的 sub 取人：名单没有可以过滤的身份"
    );
    assert!(
        body.contains("StatusCode::UNAUTHORIZED"),
        "{SESSIONS} 没有身份时答的不是拒绝：一张空名单在说「你没有会话」，而实话是「不知道你是谁」"
    );
    assert!(
        body.contains("simple_code::UNAUTHORIZED"),
        "{SESSIONS} 的拒绝没有带上那个码：客户端只能按 401 去猜是哪一种没身份"
    );

    // 记录那一侧：写入时带上「属于谁」，名单才分得开谁是谁的。
    let socket = read_repo(SOCKET);
    assert!(
        socket.contains("let owner = conn.user_id.to_string()"),
        "{SOCKET} 不把握手认定的人带进这一轮：记录里没有可以写下的身份"
    );
    let chat = read_repo(CHAT);
    assert!(
        chat.contains(".record_turn(session_id, owner, user, model, attachments, assistant)"),
        "{CHAT} 写记录时不带身份：会话名下的每一行都成了「不属于任何人」"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("owner, role, content"),
        "{STORE} 的 INSERT 没有 owner 这一列：身份传到了却留在参数里"
    );
}

/// 搜索是字面匹配，不是通配符；名单只在自己的行里找。
#[test]
fn the_search_looks_for_the_characters_typed_and_only_in_ones_own_lines() {
    let store = read_repo(STORE);
    assert!(
        store.contains("WHERE s.owner = $1"),
        "{STORE} 的名单查询不按人过滤：一个人能列出所有人的会话"
    );
    assert!(
        store.contains("strpos(lower(m.content), lower($2)) > 0"),
        "{STORE} 的搜索没有按字面找：换一种写法（`ILIKE`）会把搜索框里打的 `%` 当成通配符"
    );
    // 负面判据只读那条 SQL 本身，不读文件：注释里正解释着为什么不用 `ILIKE`。
    let sql = store
        .split_once("const SESSIONS_SQL: &str = \"")
        .and_then(|(_, after)| after.split_once("\";"))
        .map(|(sql, _)| sql)
        .unwrap_or_else(|| panic!("{STORE} 里找不到 SESSIONS_SQL：名单查询改了名字或搬了家"));
    assert!(
        !sql.contains("ILIKE"),
        "{STORE} 的名单用上了 ILIKE：搜索框里的 `%` 与 `_` 会变成通配符，找的东西和打的字不是一回事"
    );
    assert!(
        store.contains("ORDER BY MAX(s.created_at) DESC"),
        "{STORE} 的名单不按最近说话排序：刚从哪一段出来，它就不在哪一段上"
    );

    // 空身份一条都不给：那一格装的是老行与没有身份的连接，不是「所有人」。
    assert!(
        store.contains("owner.trim().is_empty()"),
        "{STORE} 不挡空身份：老行（owner 是空串）会被当成一个叫「」的人的会话列出来"
    );
}

/// 身份写在这一张表上，不是旁边的第二张表——081 的顺序在这里也要成立。
#[test]
fn the_owner_is_a_column_on_the_transcript_in_both_dialects() {
    let pg = read_repo(PG_MIGRATION);
    assert!(
        pg.contains("ALTER TABLE chat_turns ADD COLUMN IF NOT EXISTS owner"),
        "{PG_MIGRATION} 没在记录表上加 owner：身份没有落处"
    );
    assert!(
        !pg.contains("CREATE TABLE"),
        "{PG_MIGRATION} 另开了一张表：会话的归属与它的行会像以前那样各说各话"
    );
    assert!(
        pg.contains("idx_chat_turns_owner"),
        "{PG_MIGRATION} 没有为名单建索引：按人读会话会退化成全表扫描"
    );

    let mysql = read_repo(MYSQL_MIGRATION);
    assert!(
        mysql.contains("ALTER TABLE chat_turns ADD COLUMN owner"),
        "{MYSQL_MIGRATION} 没在记录表上加 owner：MySQL 部署上没有名单可读"
    );
    assert!(
        !mysql.contains("CREATE TABLE"),
        "{MYSQL_MIGRATION} 另开了一张表：同一个形状在两种方言下分成了两个"
    );
}

/// 页面这一侧：一个名单面板、一个搜索框、点开接上原来那一段。
#[test]
fn the_page_lists_the_conversations_and_reopens_the_one_that_was_picked() {
    let client = read_repo(CLIENT);
    assert!(
        client.contains("listChatSessions"),
        "{CLIENT} 没有读名单的办法"
    );
    assert!(
        client.contains("'/api/v1/sessions'") && client.contains("/api/v1/sessions?search="),
        "{CLIENT} 名单请求的路径与网关的登记对不上"
    );

    let store = read_repo(STORE_TS);
    assert!(
        store.contains("listSessions:"),
        "{STORE_TS} 不把名单放进状态：面板没有东西可画"
    );
    assert!(
        store.contains("historyError"),
        "{STORE_TS} 把读失败画成空名单：人会把「读不到」当成「我没有别的会话」"
    );

    let panel = read_repo(PANEL);
    assert!(
        panel.contains("void listSessions(query)"),
        "{PANEL} 的搜索框不发出去：按回车与点「找」都只改本地状态"
    );
    assert!(
        panel.contains("openSession(id)") && panel.contains("hydrate(id)"),
        "{PANEL} 点开时不用名单给的那个会话号：新开的是一段同名的空对话"
    );
    let dock = read_repo(DOCK);
    assert!(
        dock.contains("<SessionHistory"),
        "{DOCK} 没有挂上名单面板：这条路在页面上不存在"
    );
}
