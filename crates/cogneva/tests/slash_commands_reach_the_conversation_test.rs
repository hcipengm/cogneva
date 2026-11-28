//! 一行 `/名字` 点的是一份说明，不是一句问话：它进这一轮，模型照着它做。
//!
//! 这条链有五段，断在任何一段上，屏幕上都只是「模型答得奇怪」：
//!
//! 1. **界面**在整句以 `/` 开头时列出服务端那份名单，键盘挑得到；
//! 2. **上行**把它当成一条命令发（不是一句以斜杠开头的文字），名字与要求分开；
//! 3. **网关**认得这个形状：名字后面紧跟空白或行尾才算命令，`/usr/local/bin`
//!    这类报错照旧是一句话；
//! 4. **展开**：技能目录里那一份 `SKILL.md` 原样铺进这一轮（不截断），要求排在
//!    它后面；名字打不出来、说明读不出来、说明超过窗口，都在这里当场说；
//! 5. **记录**：人打的那一行与交给模型的那一份分开存——重开页面读回前者，第二
//!    轮读回后者，否则一段用技能开的对话从第二轮起就不再照那份规程做了。
//!
//! 纯行为的那一半在 `cog_gateway::commands::tests` 与 `cog_gateway::chat::tests`
//! 里跑；这一面钉的是两截接在一起（同一个名字、同一条帧、同一个展开点）。

use crate::common::routes::read_repo;

const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const CLIENT: &str = "web/src/api/client.ts";
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const COMMANDS: &str = "crates/cog-gateway/src/commands.rs";
const ROUTES: &str = "crates/cog-gateway/src/lib.rs";

/// 名单读得到：服务端从技能注册表列出，界面打 `/` 时向它要。
#[test]
fn the_page_lists_what_the_deployment_can_be_asked_to_do() {
    let routes = read_repo(ROUTES);
    assert!(
        routes.contains("commands::catalogue_handler"),
        "{ROUTES} 没有登记命令名单的入口：界面上那份列表没有来源"
    );
    assert!(
        routes.contains("/api/v1/chat/commands"),
        "{ROUTES} 换了路径：页面还在问老地址"
    );
    let commands = read_repo(COMMANDS);
    assert!(
        commands.contains("registry.list().await"),
        "{COMMANDS} 不向技能注册表要名单：列出来的是写死的那几条"
    );
    let client = read_repo(CLIENT);
    assert!(
        client.contains("getChatCommands"),
        "{CLIENT} 没有读名单的办法"
    );
    let pane = read_repo(PANE);
    assert!(
        pane.contains("getChatCommands()"),
        "{PANE} 打 `/` 时不去问服务端：列表中看不出来这个部署装了什么"
    );
    assert!(
        pane.contains("slashCandidates.map"),
        "{PANE} 不把候选画出来：键盘挑不到看不见的名字"
    );
}

/// 键盘挑名字：上下键走动，回车/Tab 把名字补进来，不把半截名字发出去。
#[test]
fn the_keyboard_picks_a_name_instead_of_sending_the_half_word() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("slashQuery("),
        "{PANE} 不认整句开头那个 `/`：名单没有打开的时候"
    );
    assert!(
        pane.contains("acceptSlash("),
        "{PANE} 的回车不挑名字：`/webres` 会被当成一句话发出去"
    );
    assert!(
        pane.contains("const pick = slashCandidates[slash.index]"),
        "{PANE} 的回车不落在挑中的那一行上"
    );
    // 名单开着时 Esc 收掉：否则一份挡住输入框的列表没有退路。
    assert!(pane.contains("setSlash(null)"), "{PANE} 收不掉这份名单");
}

/// 上行分成两条帧：一条命令与一句以斜杠开头的文字不是同一个意思。
#[test]
fn a_command_travels_as_a_command_not_as_a_sentence_with_a_slash() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("commandLine(text)"),
        "{PANE} 发之前不分命令与普通的一句话"
    );
    assert!(
        pane.contains("type: 'command'"),
        "{PANE} 把命令当普通文字发：服务端只能按开头的斜杠去猜"
    );

    let protocol = read_repo(PROTOCOL);
    assert!(
        protocol.contains("#[serde(rename = \"command\")]"),
        "{PROTOCOL} 没有 `command` 那一帧：界面发了也解析不了"
    );
    assert!(
        protocol.contains("name: String") && protocol.contains("arguments: String"),
        "{PROTOCOL} 的命令帧没有把名字与要求分开"
    );

    let socket = read_repo(SOCKET);
    assert!(
        socket.contains("ClientMessage::Command {"),
        "{SOCKET} 不认识这条帧：命令在套接字上被丢掉"
    );
    assert!(
        socket.contains("Some(invocation)"),
        "{SOCKET} 不把「点了哪一条」交给这一轮：网关只能看到一串文字"
    );
}

/// 网关这一句真的展开了，而且不截断。
#[test]
fn the_gateway_expands_the_named_skill_into_this_turn() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("crate::commands::expand("),
        "{CHAT} 不展开点名的技能：模型看到的是 `/web-research 查一下` 这几个字"
    );
    assert!(
        chat.contains("state.external_skill_registry.as_ref()"),
        "{CHAT} 展开时不看这个部署的技能注册表：拿的是别处的名单"
    );
    // 展开之后要真的把那一份交给循环：展开出来又丢掉，屏幕上就只有 `/web-research`
    // 这半句话被模型当成一个人问的问题。
    assert!(
        chat.contains("(None, Some(expansion)) => expansion.clone()"),
        "{CHAT} 展开了却没交给循环：模型读到的是人打的那一行"
    );

    let commands = read_repo(COMMANDS);
    assert!(
        commands.contains("definition.skill_md"),
        "{COMMANDS} 不把 SKILL.md 正文铺进去：展开的只是一句话"
    );
    assert!(
        !commands.contains("chars().take(") && !commands.contains("truncate"),
        "{COMMANDS} 截了说明：半份规程读起来和「模型没照做」一模一样"
    );
    assert!(
        commands.contains("estimate_tokens(&text)"),
        "{COMMANDS} 不量说明有多长：一份装不进窗口的说明会被默默送进去"
    );
}

/// 认不出、读不出、太长：都在这里说，不交给模型去猜。
#[test]
fn a_command_that_cannot_run_is_answered_by_the_platform() {
    let chat = read_repo(CHAT);
    let at = chat
        .find("crate::commands::expand(")
        .expect("这一轮不再展开命令");
    // 按字符截：这一段里有中文，按字节切会落在字的中间。
    let block: String = chat[at..].chars().take(1400).collect();
    assert!(
        block.contains("Err(reason) =>"),
        "{CHAT} 展开失败时没有分支：失败会被当成一句空话发出去"
    );
    assert!(
        block.contains("record_failed_turn("),
        "{CHAT} 认不出的那一轮没有记下来：重开页面它就消失了"
    );
    // 失败分支在把这一句交给循环之前返回：不 return 就会带着一份空展开继续跑。
    let err_at = block.find("Err(reason) =>").unwrap();
    assert!(
        block[err_at..].contains("return;"),
        "{CHAT} 展开失败之后没有停下：模型仍然会收到这一句"
    );

    let commands = read_repo(COMMANDS);
    assert!(
        commands.contains("没有叫「"),
        "{COMMANDS} 认不出名字时不给一句人话"
    );
    assert!(
        commands.contains("这一段对话现在能用的是"),
        "{COMMANDS} 不列出能用的名字：人只能靠猜"
    );
}

/// 记录里两列：人打的那一行（读记录的人看）与交给模型的那一份（后面几轮看）。
#[test]
fn what_the_person_typed_and_what_the_model_read_are_stored_apart() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("expansion.as_deref()"),
        "{CHAT} 不把展开后的那一份交给记录：第二轮起模型手上只剩 `/web-research`"
    );
    assert!(
        chat.contains("record_turn(\n                    &state.chat_turns,\n                    &session_id,\n                    &owner,\n                    &content,"),
        "{CHAT} 记录里没把「人打的那一行」与「模型读到的那一份」分开传"
    );

    let store = read_repo("crates/cog-storage/src/postgres/chat_turn_store.rs");
    assert!(
        store.contains("COALESCE(model_content, content)"),
        "历史读的那一条查询没有优先读展开后的那一份：后面几轮看不到技能说明"
    );
    assert!(
        store.contains("model_content"),
        "存储里没有这一列：展开只在发它的那一轮存在"
    );
}
