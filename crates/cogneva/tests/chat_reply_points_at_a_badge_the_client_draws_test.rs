//! 回执里点名的那个位置，界面上真的画着那几个字。
//!
//! 用户把一句话交给编排器之后，短时间内唯一会读到的东西就是回执那一句。它说
//! 「进度在 X」，用户就去看 X；若 X 不存在，屏幕上「进度不更新」与「这道目标
//! 根本没跑起来」长得一模一样——而正确的那一半（它在跑）没有任何地方说出来。
//!
//! 实测（2026-11-04，属主在 WebUI 的 Live 面板里）：回执写着「进度会实时出现在
//! 右侧目标流水线」，而这个工作台里没有叫这个名字的窗格。进度实际落在这一段对话
//! 标题行的「常驻 · 守护」徽章上（`ConversationPane` 的 `KindBadge`）。人去右侧找
//! 一个不存在的东西，找完之后唯一能得出的结论是「它不行」。
//!
//! 判据：`chat.rs` 的回执里，凡是以「」括起来、被当作界面位置告诉用户的那几个字，
//! 都必须在 `web/src` 里逐字出现。下面点名认的那一个就是徽章名。
//!
//! 判不了的：它判的是「这几个字在客户端的某处出现过」，不是「它就是那一处」。

use crate::common::{fsio, root};

const SERVER: &str = "crates/cog-gateway/src/chat.rs";
const CLIENT: &str = "web/src/conversation/ConversationPane.tsx";

/// 回执里告诉用户「看这里」的那个徽章名，带中文引号，形状与源码里一致。
const NAMED_LOCATION: &str = "「常驻 · 守护」";

/// 客户端画这个徽章时用的写法：`<span>常驻 · 守护</span>`。
fn client_draws(label: &str) -> bool {
    let bare = label.trim_matches(|c| c == '「' || c == '」');
    let client = fsio::read(&root::repo_root().join(CLIENT));
    client.contains(&format!(">{bare}<"))
}

#[test]
fn the_reply_tells_the_owner_to_look_where_the_badge_is_drawn() {
    let server = fsio::read(&root::repo_root().join(SERVER));

    assert!(
        server.contains(NAMED_LOCATION),
        "{SERVER} 的回执不再点名 {NAMED_LOCATION} 这个位置。\
         如果改成了别的位置，把这条判据换成那个字面量，并确认 {CLIENT} 真的画它；\
         如果回执从此不点名任何位置，那用户提交之后就没有任何一句告诉他去哪看进度"
    );
    assert!(
        client_draws(NAMED_LOCATION),
        "{CLIENT} 里找不到 {NAMED_LOCATION} 这几个字：回执把用户送到一个不存在的位置，\
         而他在那里看到的空与「这道目标根本没跑起来」一模一样"
    );
}
