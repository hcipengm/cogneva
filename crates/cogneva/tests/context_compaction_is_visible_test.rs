//! 上下文被挤掉一段，这件事要在屏幕上说得出口。
//!
//! 窗口装不下整段对话时，循环从提示词的头部删掉几条：模型这一轮就看不见它们了。
//! 这是长对话的正常行为，但它此前是**静默**的——屏幕上一个字也没有，读者只能在
//! 模型答得奇怪时去猜「它是不是忘了」。这条链有四段，断在任何一段上，结果都是
//! 「压缩发生了，但没人知道」：
//!
//! 1. **记录**：`context.rs` 的裁剪留下现场（挤掉几条、多少 token、还在的尾巴从
//!    哪一条开始、被挤掉那一段的开头）；没有裁剪就没有记录。
//! 2. **交付**：循环把这份现场挂在这一轮的交付物上（`compaction` 那一格），只在
//!    真裁过时挂——每轮一帧「0 条」是噪声。
//! 3. **上行**：网关原样搬成一帧 `message.compaction`，同样只在有时发。
//! 4. **界面**：帧被认出来、记在说这句话的那一条上、画成一行（挤掉多少、留下
//!    多少、从哪一句起）。
//!
//! 行为面在 `cog_agent::context::tests`（`a_trim_records_what_fell_out_and_where_the_tail_begins`、
//! `an_untouched_window_reports_no_compaction_and_taking_clears_it`）、
//! `cog_agent::agent_kernel::runtime::tests::a_turn_that_overflows_its_window_says_what_fell_out`
//! 与 `cog_gateway::chat::tests::{a_turn_that_squeezed_the_context_says_so_in_one_frame,
//! a_turn_that_did_not_squeeze_anything_says_nothing}` 里跑；这一面钉的是四段接在
//! 一起（同一个字段名、同一条帧、同一个渲染入口）。

use crate::common::routes::read_repo;

const CONTEXT: &str = "crates/cog-agent/src/context.rs";
const RUNTIME: &str = "crates/cog-agent/src/agent_kernel/runtime.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const EVENTS: &str = "web/src/types/events.ts";
const STORE: &str = "web/src/store/chatStore.ts";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 第一段：裁剪留下现场，而且这份现场能被取走。
#[test]
fn a_trim_leaves_a_record_of_what_fell_out() {
    let context = read_repo(CONTEXT);
    assert!(
        context.contains("pub struct Compaction"),
        "{CONTEXT} 没有这一份现场：挤掉了什么没人记，屏幕上也就没得说"
    );
    assert!(
        context.contains("take_compaction"),
        "{CONTEXT} 记录了却取不走：循环拿不到它"
    );
    // 记录里必须有「尾巴从哪一条开始」这一格——只报条数，读者不知道现在还能
    // 往前看到哪里。
    assert!(
        context.contains("kept_from"),
        "{CONTEXT} 不记保留边界：读者只知道挤掉了多少，不知道留下来的从哪一句起"
    );
    // 没裁过就不许有现场：一个恒存在的空记录会让界面每一轮都多一行。
    assert!(
        context.contains("compaction: None"),
        "{CONTEXT} 的窗口初始化时带着一份现场：没裁过也会报"
    );
}

/// 第二段：循环把现场挂上交付物，只在真裁过时挂。
#[test]
fn the_turn_carries_the_record_only_when_something_was_squeezed() {
    let runtime = read_repo(RUNTIME);
    assert!(
        runtime.contains("take_compaction()"),
        "{RUNTIME} 不向窗口要这份现场：记录留在循环里，谁也读不到"
    );
    assert!(
        runtime.contains("\"compaction\".to_string()"),
        "{RUNTIME} 不把现场挂在交付物上：网关收不到它"
    );
    let at = runtime
        .find("take_compaction()")
        .expect("这一轮不再取裁剪现场");
    // 条件句而不是「取现场之后若干字符内」：交付物上还会增加别的格（这一轮读进
    // 的约定、跑过的钩子），它们都排在压缩这一格前面，一个按距离量出来的窗口会
    // 被无关的加法挤破——那正是这个判据红过的原因。真正要钉的是两件事：取现场
    // 在先，挂点只有一个且包在 `Some` 里。
    let conditional = runtime
        .find("if let Some(compaction) = compaction")
        .expect("runtime.rs 无条件挂这一格：没裁过的轮次也会报一份空的，界面每轮多一行");
    assert!(
        at < conditional,
        "{RUNTIME} 先挂后取：挂上去的那份现场还是上一次的"
    );
    let arm: String = runtime[conditional..].chars().take(400).collect();
    assert!(
        arm.contains("\"compaction\".to_string()"),
        "{RUNTIME} 取到现场却没在 `Some` 里挂上：有裁剪的轮次反而看不到它"
    );
    assert_eq!(
        runtime.matches("\"compaction\".to_string()").count(),
        1,
        "{RUNTIME} 不止一处挂压缩：条件以外的那一处每轮都会报一份空的"
    );
}

/// 第三段：网关搬成一帧，且只在有时发。
#[test]
fn the_gateway_sends_the_record_as_its_own_frame() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("\"message.compaction\""),
        "{CHAT} 没有这一帧：交付物上有现场，界面上没有"
    );
    assert!(
        chat.contains("let Some(compaction) = compaction else { return }"),
        "{CHAT} 没裁过也会发一帧：每轮一次「挤掉 0 条」，读者从此不再读它"
    );
    // 帧必须在收尾帧之前发：界面把它画在这一条消息下面，`message.end` 一到这一条
    // 就定稿了，晚到的帧没有落点。
    let compaction_at = chat
        .find("emit_turn_compaction(&out,")
        .expect("不再在一轮里发送压缩帧");
    // 下一个收尾帧就是这一条的收尾：两者之间还能夹几条别的读数（同事消息、
    // 任务列表），只要排在那之前就行。
    let tail = &chat[compaction_at..];
    assert!(
        tail.find("emit_message_end(").is_some(),
        "{CHAT} 这一帧排在收尾帧之后：界面收到时这一条已经定稿，画不上去了"
    );
}

/// 第四段：界面认得这个帧、记在这条消息上、画成一行。
#[test]
fn the_pane_draws_what_was_squeezed_as_one_line() {
    let events = read_repo(EVENTS);
    assert!(
        events.contains("'message.compaction'"),
        "{EVENTS} 不认这一帧：整帧被丢掉，压缩仍然静默"
    );
    assert!(
        events.contains("ContextCompaction"),
        "{EVENTS} 没有这一帧的形状：界面只能拿到一串没名字的数"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("case 'compaction'"),
        "{STORE} 不把这一帧记到消息上：它到不了渲染那一层"
    );

    let pane = read_repo(PANE);
    assert!(
        pane.contains("compactionLine(message.compaction)"),
        "{PANE} 不画这一行：帧到了屏幕上仍然什么都没有"
    );
    assert!(
        pane.contains("上下文压缩"),
        "{PANE} 这一行不说人话：读者看不出这一条消息下面多的是什么"
    );
    // 尾巴从哪一句起必须画出来——只说「压缩了」等于让读者自己去猜模型还记得什么。
    assert!(
        pane.contains("keptFrom"),
        "{PANE} 不写保留边界：读者知道挤掉了，却不知道现在还能往前看到哪一句"
    );
}
