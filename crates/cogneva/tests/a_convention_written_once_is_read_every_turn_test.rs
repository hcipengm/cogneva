//! 属主写一句约定，之后每一轮开头都读到它；没写的时候提示词里没有它。
//!
//! 这条链有五段，缺任何一段，界面上看起来都像「写了没用」：
//!
//! 1. **存在哪**：按人分桶的一份 JSON（键值存储没有扫描，一条一桶就列不回自己
//!    写过哪几句）；
//! 2. **怎么进提示词**：装配层把这一位属主的读面挂到运行时上，运行时在每轮开头
//!    把这一段落放进系统前缀，变了就换掉旧的；
//! 3. **看得见**：这一轮真读进提示词的那几条随交付物回到网关，网关发一帧；
//! 4. **写与撤**：套接字上的三条帧，与规则面板同一套回执（整份名单）；
//! 5. **页面上**：一处写、一处撤、一行「这一轮读进了什么」。
//!
//! 行为那一半在 `cog_agent::agent_kernel::runtime::tests`
//! （`a_convention_written_by_the_owner_reaches_the_prompt_and_is_named`、
//! `a_convention_rewritten_between_turns_replaces_the_old_one`）与
//! `cogneva::owner_memory::tests`（读写、同句只存一条、越界拒绝、桶满拒绝）；
//! 这里钉的是接线：哪一处挂上去、哪一处读回来。

use crate::common::routes::read_repo;

const STORE: &str = "crates/cogneva/src/owner_memory.rs";
const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const RUNTIME: &str = "crates/cog-agent/src/agent_kernel/runtime.rs";
const MEMORY: &str = "crates/cog-agent/src/memory.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";
const WEB_STORE: &str = "web/src/store/memoryStore.ts";
const WEB_CHAT: &str = "web/src/store/chatStore.ts";
const WEB_EVENTS: &str = "web/src/types/events.ts";
const WEB_PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 一段源码里某个函数（或块）从开头到下一次同缩进的 `}`：判据只读这一块，免得
/// 文件别处的同名短语把结论带偏。
fn block<'a>(source: &'a str, open: &str) -> &'a str {
    let Some((_, after)) = source.split_once(open) else {
        panic!("找不到 {open:?}：这一处改了名字或搬了家，判据要跟着改");
    };
    after
}

/// 存在哪：一个人一个桶，桶里一整份名单；容量有界并标了来源。
#[test]
fn the_conventions_live_in_one_bucket_per_person_with_a_stated_ceiling() {
    let store = read_repo(STORE);
    assert!(
        store.contains("owner_memory:owner:")
            || store.contains("format!(\"{MEMORY_KEY_PREFIX}:owner:{owner}\")"),
        "{STORE} 的桶键不是按人分的：一个人写的约定会被另一个人读到"
    );
    for f in [
        "pub async fn list(",
        "pub async fn remember(",
        "pub async fn forget(",
    ] {
        assert!(store.contains(f), "{STORE} 缺 {f}：这条链少了读写撤的一环");
    }
    // 容量两条必须写明来源：这是一个界，读的人要能分清它是量出来的还是钉死的。
    assert!(
        store.contains("MEMORY_ENTRY_LIMIT_CHARS") && store.contains("override"),
        "{STORE} 的容量界没有标来源：读的人分不清它是不是一条实测"
    );
    assert!(
        store.contains("MEMORY_BUCKET_LIMIT_CHARS"),
        "{STORE} 没有整份名单的上限：这段约定每一轮都要进系统前缀，没有界的时候越写越长"
    );
    // 桶满了要拒绝，不能悄悄挤掉最早的一条。
    let remember = block(&store, "pub async fn remember(");
    assert!(
        remember.contains("超过上限") || remember.contains("MEMORY_BUCKET_LIMIT_CHARS"),
        "{STORE} 的 remember 不判上限：写下去的那句会把别的挤掉而写的人不知道"
    );
}

/// 怎么进提示词：装配层挂读面，运行时在每轮开头并进去、变了换掉旧的。
#[test]
fn the_assembly_hands_the_owner_s_own_memory_to_the_loop() {
    let assembly = read_repo(ASSEMBLY);
    assert!(
        assembly.contains(".with_memory_source("),
        "{ASSEMBLY} 没有把属主约定的读面挂到运行时上：内核永远读不到任何一句"
    );
    assert!(
        assembly.contains("owner_memory::OwnerMemoryBucket::new"),
        "{ASSEMBLY} 挂的不是按人读的桶：换一个人读到的是同一个人的约定"
    );

    let runtime = read_repo(RUNTIME);
    let absorb = block(&runtime, "async fn absorb_base_instruction(&mut self) {");
    // 只看这一块的开头：下一个函数之前。
    let absorb = absorb
        .split("async fn absorb_principles")
        .next()
        .unwrap_or(absorb);
    assert!(
        absorb.contains("absorb_owner_memory"),
        "{RUNTIME} 的每轮开头不读属主约定：写了也进不了提示词"
    );
    let apply = block(
        &runtime,
        "fn apply_memory_section(&mut self, section: Option<String>) {",
    );
    let apply = apply.split("\n    fn ").next().unwrap_or(apply);
    assert!(
        apply.contains("drop_from_head_system(&old)"),
        "{RUNTIME} 换约定时不先拿掉旧的：撤掉的与新的会并排留在同一段前缀里"
    );
    assert!(
        apply.contains("merge_head_system(&section)"),
        "{RUNTIME} 不把这一段并进系统前缀：核心里读了，模型读不到"
    );

    let memory = read_repo(MEMORY);
    assert!(
        memory.contains("属主在之前的对话里写下的约定"),
        "{MEMORY} 的那一段没有说清它是谁写的、对谁生效：模型会把它当成平台规则或建议"
    );
}

/// 看得见：这一轮真读进提示词的条目随交付物回网关，网关发一帧。
#[test]
fn the_turn_reports_what_it_loaded_and_the_gateway_says_so() {
    let runtime = read_repo(RUNTIME);
    assert!(
        runtime.contains("\"memory\".to_string()"),
        "{RUNTIME} 的交付物里没有这一格：界面上只能知道「配了」，不能知道「读进了什么」"
    );
    assert!(
        runtime.contains("self.memory_source.is_some()"),
        "{RUNTIME} 不区分「没有读面」与「读了 0 条」：两者在读数面上会折成同一句话"
    );

    let chat = read_repo(CHAT);
    assert!(
        chat.contains("async fn emit_turn_memory("),
        "{CHAT} 没有发这一帧：清单读回来了却没有落到对话上"
    );
    assert!(
        chat.contains("\"message.memory\""),
        "{CHAT} 的帧名与页面认的名字对不上：页面永远收不到这一行"
    );
    let run = block(&chat, "async fn run_chat_turn");
    assert!(
        run.contains("emit_turn_memory("),
        "{CHAT} 定义了发帧的函数却没有调用：这一帧从写下那天起不会出现"
    );
}

/// 写与撤：三条上行帧，回执都是整份名单。
#[test]
fn writing_and_forgetting_travel_on_their_own_frames() {
    let protocol = read_repo(PROTOCOL);
    for name in [
        "\"owner_memory\"",
        "\"owner_memory_write\"",
        "\"owner_memory_forget\"",
    ] {
        assert!(
            protocol.contains(&format!("#[serde(rename = {name})]")),
            "{PROTOCOL} 缺 {name} 这一帧：页面上按了没有人接"
        );
    }
    let socket = read_repo(SOCKET);
    for call in [
        "owner_memory(conn.user_id)",
        "remember_owner_memory(conn.user_id, &text)",
        "forget_owner_memory(conn.user_id, &id)",
    ] {
        assert!(
            socket.contains(call),
            "{SOCKET} 没有处理 {call}：帧到了就掉在地上"
        );
    }
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("fn remember_owner_memory<"),
        "{CHAT} 的装配面没有写入口：套接字那条路接不上去"
    );
}

/// 页面上：一处写、一处撤、一行「这一轮读进了什么」。
#[test]
fn the_page_lets_a_person_write_one_and_shows_what_was_loaded() {
    let store = read_repo(WEB_STORE);
    assert!(
        store.contains("entries: MemoryEntry[] | null"),
        "{WEB_STORE} 不把名单放进状态：面板没有东西可画"
    );

    for (file, needle, why) in [
        (
            WEB_EVENTS,
            "'message.memory': 'memory'",
            "聊天帧表里没有这一帧，到了也落不进消息",
        ),
        (
            WEB_CHAT,
            "case 'memory':",
            "消息上不记这一格，那一行永远画不出来",
        ),
        (WEB_PANE, "type: 'owner_memory_write'", "页面上没有写的地方"),
        (
            WEB_PANE,
            "type: 'owner_memory_forget'",
            "页面上撤不掉一句写错的约定",
        ),
        (WEB_PANE, "message.memory", "这一轮读进了什么没有画出来"),
    ] {
        assert!(
            read_repo(file).contains(needle),
            "{file} 缺 {needle:?}：{why}"
        );
    }
}
