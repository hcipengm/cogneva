//! 一条「总是允许 / 总是拒绝」要走通的那一整条链：从人点的那一下，到下一次同类调用
//! 不再问，再到面板上把它撤掉、问句回来。
//!
//! 五个环节分开看都能编译、都能单独跑通，合起来却可能什么都不发生：
//!
//! 1. 人答的答案带着「要记住」——不带这一格，界面上的按钮点了与「允许一次」没有区别；
//! 2. 装配层在问人之前先查规则——不查的话规则存在那里也永远不会被读到；
//! 3. 记下来的规则写进存储——只放内存的话，下一段对话、下一次重启就忘了；
//! 4. 协议与界面把「总是」与「撤销」送出去、把名单收回来；
//! 5. 两个方向的结论都落地：放行等于不问，拒绝等于直接拒绝且优先于放行。
//!
//! 判据读的是两侧的源文件（前端没有测试运行器），行为本身由
//! `cog_agent::permission` 与 `cogneva::conversation` 的单元测试钉住。

use crate::common::routes::read_repo;

const PERMISSION: &str = "crates/cog-agent/src/permission.rs";
const CONVERSATION: &str = "crates/cogneva/src/conversation.rs";
const RULES: &str = "crates/cogneva/src/permission_rules.rs";
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const TRANSPORT: &str = "web/src/transport/websocket.ts";
const STORE: &str = "web/src/store/permissionStore.ts";

/// 规则先于问句：装配层问人之前必须已经把规则查过一遍，否则一条留下来的规则
/// 永远轮不到它说话——人还是每次都被问。
#[test]
fn the_rules_are_consulted_before_the_person_is_asked() {
    let conversation = read_repo(CONVERSATION);
    let evaluate = conversation
        .find(".evaluate(ask, &self.session_id)")
        .unwrap_or_else(|| panic!("{CONVERSATION} 不问规则就直接问人"));
    let ask = conversation
        .find("self.bridge.ask(view)")
        .unwrap_or_else(|| panic!("{CONVERSATION} 里找不到问人的那一步"));
    assert!(
        evaluate < ask,
        "{CONVERSATION} 先问人再查规则：留下来的规则不会省掉任何一次询问"
    );
    assert!(
        conversation.contains("if always {"),
        "{CONVERSATION} 不读答案里的「要记住」：界面上那个按钮点与不点一样"
    );
}

/// 拒绝优先：命中的规则里只要有一条拒绝，结论就是拒绝，与规则写下来的先后无关。
#[test]
fn a_deny_rule_is_reached_before_an_allow_rule() {
    let permission = read_repo(PERMISSION);
    let deny = permission
        .find("find(|rule| !rule.allow)")
        .unwrap_or_else(|| panic!("{PERMISSION} 里没有「先找拒绝」的那一步"));
    let allow = permission
        .find("any(|rule| rule.allow)")
        .unwrap_or_else(|| panic!("{PERMISSION} 里没有「再找放行」的那一步"));
    assert!(
        deny < allow,
        "{PERMISSION} 先看放行：一条后来加的拒绝会永远轮不到它说话"
    );
}

/// 规则要落在存储里，不是只活在这一次询问的内存里。
#[test]
fn a_rule_is_written_where_the_next_conversation_will_read_it() {
    let rules = read_repo(RULES);
    assert!(
        rules.contains("kv.set(") && rules.contains("async fn load("),
        "{RULES} 没有写或没有读：规则只活在内存里，下一段对话照样问"
    );
    assert!(
        rules.contains("RULE_KEY_PREFIX"),
        "{RULES} 没有统一的键前缀：规则散在几个拼法不同的键上，撤的时候找不全"
    );
}

/// 上限也在这一边：人答「总是」的时候记的是这一次碰的那个东西，不是这个工具的一切。
#[test]
fn a_remembered_answer_covers_what_was_asked_not_the_whole_tool() {
    let conversation = read_repo(CONVERSATION);
    assert!(
        conversation.contains("ask_subject(&ask.arguments)"),
        "{CONVERSATION} 记规则时不带对象：一次点头会扩成「这个工具往后全都放行」"
    );
}

/// 协议两个方向都要有：「要记住」跟着答案回去，名单跟着请求回来。
#[test]
fn the_protocol_carries_the_remembered_answer_and_the_list() {
    let protocol = read_repo(PROTOCOL);
    assert!(
        protocol.contains("remember: bool"),
        "{PROTOCOL} 的答案里没有「要记住」这一格：界面上的按钮无处出口"
    );
    assert!(
        protocol.contains("#[serde(rename = \"permission_rules\")]"),
        "{PROTOCOL} 里没有 `permission_rules`：名单出不去"
    );
    assert!(
        protocol.contains("rules: Vec<crate::chat::PermissionRuleView>"),
        "{PROTOCOL} 回执里不带规则本体：界面上画不出可撤的那一行"
    );

    let socket = read_repo(SOCKET);
    assert!(
        socket.contains("revoke_permission_rule"),
        "{SOCKET} 不把撤销递给装配层：面板上的撤销按钮按下去什么都不发生"
    );
}

/// 界面两头都得有：把「总是」送出去，把名单收回来并逐条可撤。
#[test]
fn the_page_offers_both_answers_and_a_list_that_can_be_revoked() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("answerPermission(ask, true, true)")
            && pane.contains("answerPermission(ask, false, true)"),
        "{PANE} 没有把「总是允许 / 总是拒绝」作为答案发出去"
    );
    assert!(
        pane.contains("permission_rule_revoke") && pane.contains("撤销"),
        "{PANE} 列了规则却撤不掉：规则一旦留下就再也回不到「每次都问」"
    );

    let transport = read_repo(TRANSPORT);
    assert!(
        transport.contains("'permission_rules'"),
        "{TRANSPORT} 认不出 `permission_rules` 帧：名单到了这一层就被丢掉"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("rules: PermissionRule[] | null"),
        "{STORE} 分不出「还没读到」与「一条也没有」：一次失败的读取会被读成干净的家底"
    );
}
