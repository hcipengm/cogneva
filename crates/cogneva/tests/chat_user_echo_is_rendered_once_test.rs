//! 属主自己说的那句话，在对话面上只许出现一次。
//!
//! 一段对话的正文有两个来源：界面在按下回车时先把自己那一句记进账（网络慢一秒
//! 也不能看起来像「我说了话，但这里什么都没发生」），服务端随后把它原样回声回来
//! （`message.start` 一条角色为 `user` 的帧）。这两个来源**只有一个是这句话的
//! 字节**，而没有任何东西保证它们不叠加：
//!
//! - 服务端把这句话当**增量**发（`start` → `text_delta(content)` → `end`）；
//! - 界面收到增量时是**追加**（`text: message.text + delta`）。
//!
//! 于是只要本地那份抄本还留着，回声一来就是同一句连着写两遍——没有分隔、没有气泡
//! 边界，读起来像属主把话打重了。实测（2026-11-04，属主在 WebUI 的 Live 面板里
//! 为此提了两次）：「我给你的话你都两次显示出来了」。
//!
//! 判据只有一条，但它是一个**等式**，因为修法有两个方向、哪个方向单独改都会坏：
//!
//! - 服务端用增量送这句话 ⇔ 界面在回声的 `start` 上把本地抄本清空。
//!
//! 两边同时成立，这句话由服务端唯一提供；两边同时不成立，它由界面唯一提供。
//! 只成立一边就是这里要拦的坏画面：只清空而服务端不再发增量，属主那一句变成空白
//! 气泡；只发增量而不清空，就是上面那个重复两遍。
//!
//! 判不了的：这两个文件里没有一条断言跑得起来界面。它判的是两侧对「谁供字节」
//! 的约定一致，不是屏幕上真的只有一个气泡。

use crate::common::routes::read_repo;

const CLIENT: &str = "web/src/store/chatStore.ts";
const SERVER: &str = "crates/cog-gateway/src/chat.rs";

/// 服务端那一半：回声了一条 `user` 的 `start` 之后，把同一句当成增量再发一遍。
///
/// 返回它用的消息 id（形如 `&user_mid`）。没有这段回声就不是这一侧的供源。
fn server_sends_the_echo_as_a_delta(source: &str) -> Option<String> {
    // 这条帧的身份是写死的字面量 `"user"`：靠角色的位置认，不靠变量名。
    let start_line = source
        .lines()
        .position(|line| line.contains("emit_message_start(") && line.contains("\"user\""))?;
    let start = source.lines().nth(start_line)?;

    // 第三个实参是这条消息的 id：`emit_message_start(&out, &session_id, &user_mid, "user")`。
    let args: Vec<&str> = start
        .split_once("emit_message_start(")?
        .1
        .split(')')
        .next()?
        .split(',')
        .map(str::trim)
        .collect();
    let id = args.get(2)?.to_string();

    // 紧随其后的几行里，同一 id 的增量帧带的是入参 `content`——属主自己那一句。
    let tail: Vec<&str> = source.lines().skip(start_line + 1).take(6).collect();
    let delta = tail
        .iter()
        .find(|line| line.contains("emit_delta(") && line.contains(&id))?;
    let delta_args: Vec<&str> = delta
        .split_once("emit_delta(")?
        .1
        .split(')')
        .next()?
        .split(',')
        .map(str::trim)
        .collect();

    // 第四个实参就是这句话的正文。
    let payload = delta_args.get(3)?;
    payload.contains("content").then(|| id)
}

/// 这个函数收的那一句，确实是调用方传进来的那句话。
fn the_parameter_is_the_callers_sentence(source: &str) -> bool {
    let Some((_, after)) = source.split_once("pub async fn run_chat_turn(") else {
        return false;
    };
    let signature = after.split(") {").next().unwrap_or(after);
    signature
        .lines()
        .any(|line| line.trim() == "content: String,")
}

/// 界面那一半：增量是**追加**进正文的。
///
/// 这条不是判据本身，是那个等式的前提。增量若改成整体替换，本地留没留抄本就都
/// 看不到重复，等式也就不再是这里的规矩。
fn the_client_appends_deltas(source: &str) -> bool {
    source.contains("text: message.text + delta")
}

/// 界面在回声的 `start` 上把本地那一句清空。
fn the_client_clears_its_copy_on_the_echo(source: &str) -> bool {
    // 认的是「把等回声的那一条换成服务端的身份」这处更新，不认注释。
    let Some((_, after)) = source.split_once("withMessage(session, awaited,") else {
        return false;
    };
    let update = after.split("}));").next().unwrap_or(after);
    update
        .lines()
        .filter_map(|line| line.trim().strip_prefix("text:"))
        .any(|value| {
            let value = value.trim().trim_end_matches(',');
            value == "''" || value == "\"\"" || value == "``"
        })
}

#[test]
fn the_users_own_sentence_has_exactly_one_source_of_bytes() {
    let client = read_repo(CLIENT);
    let server = read_repo(SERVER);

    assert!(
        the_client_appends_deltas(&client),
        "界面不再把增量追加进正文（`text: message.text + delta` 找不到了）：\
         下面那个等式是按「增量累加」推出来的，先确认现在的写法是什么，再决定\
         本地抄本该不该清"
    );
    assert!(
        the_parameter_is_the_callers_sentence(&server),
        "{SERVER} 里的 run_chat_turn 不再收 `content: String`：\
         回声的内容来源变了，这条判据读的已经不是属主那句话"
    );

    // 认不出服务端的回声帧时不要静默判成「另一侧」：那会让这条门在服务端换了
    // 写法之后继续绿着，而重复两遍依然在屏幕上。
    let server_sends = server_sends_the_echo_as_a_delta(&server).unwrap_or_else(|| {
        panic!(
            "{SERVER} 里找不到「`user` 的 start 之后用同一个 id 再发一条 `content` 增量」\
             这段回声：要么回声改成了别的形状（那条线的 id 与正文怎么走要在这里重新认），\
             要么它被删了——删了的话，界面必须自己留着那一句"
        )
    });
    let client_clears = the_client_clears_its_copy_on_the_echo(&client);

    let server_sends_a_delta = !server_sends.is_empty();
    assert_eq!(
        server_sends_a_delta,
        client_clears,
        "属主那一句话的两个来源对不上：服务端{}把它当增量再发一遍，\
         而界面在回声的 `start` 上{}清空本地那一份。\n\
         两个都成立 ⇒ 这句话由服务端唯一提供；两个都不成立 ⇒ 由界面唯一提供。\n\
         只成立一边的时候：服务端发增量而界面不清 —— 屏幕上同一句连着写两遍，\
         没有分隔也没有气泡边界（实测 2026-11-04，属主为此提了两次）；\
         界面清空而服务端不发增量 —— 属主那一句变成一个空白气泡。",
        if server_sends_a_delta {
            format!("（消息 id {server_sends}）")
        } else {
            "不".to_string()
        },
        if client_clears { "仍然" } else { "没有" }
    );
}
