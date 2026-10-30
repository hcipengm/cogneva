//! principle: feedback_no_hardcoding_config_surface
//!
//! 授权码回跳不是一次重定向，是一次**三个窗口之间的握手**：WebUI 弹一个窗口，
//! 平台把浏览器送回一张公开落地页，落地页把 `code` 通过 `window.opener` 的
//! `postMessage` 递回打开它的那一页。这条链上任何一环少掉，用户看到的都不是
//! 报错——落地页照常显示一串授权码，让人自己复制粘贴。于是「自动完成」退化成
//! 「多一步手工」，而两步在日志上没有区别：没人报错，也没人知道本该自动。
//!
//! 这一族已经踩过两次，形态相同：
//!
//! - 弹窗带 `noopener`：`window.opener` 恒为 null，落地页连递送的入口都没有；
//! - 弹窗保留了 opener，但接收方没有 `message` 监听：消息发出去落地无声，
//!   弹窗自己关掉，向导那一头停在「用户还没授权」。
//!
//! 本门把这一对当判据：**凡以 `authorize_url` 打开窗口的地方，不许带 `noopener`，
//! 且那个文件必须挂 `cogneva-gitee-oauth` 的接收端**。两条都要，因为任何一条单独
//! 成立都不够——带 opener 没监听与不带 opener 一样只能手工粘贴。
//!
//! 判不了什么：不判监听器里的 `state` 校验写没写、也不判收到之后换码换得对不对
//! （那是运行时）；不判落地页那一侧发什么（那一侧由
//! `popup_handshake_words_are_the_words_the_other_page_sends_test` 与发送端互校）；
//! 不判 GitHub 设备码那条腿（它不打开窗口递送结果）。
//!
//! 实测（2026-10-30）：`web/src` 下 2 处 `window.open(…authorize_url…)`——登录入口
//! （`AuthOverlay`）带 opener 且挂了监听，设置里的代码平台入口（`ContributionOverlay`）
//! 两样都缺，用户点完授权只能复制粘贴。消融：把 `ContributionOverlay` 那一行改回
//! `'_blank', 'noopener'`，本门红；删掉它的 `message` 监听同样红。

use std::path::PathBuf;

use crate::common::routes::{read, repo_root, strip_comments, walk};
use crate::common::rust_production::relative_name;

const WEB_SRC: &str = "web/src";
/// 落地页发回来的那条消息。两端各拼一遍，这里只认这一个词。
const CODE_WORD: &str = "cogneva-gitee-oauth";

/// 一处「打开授权窗」的调用：文件、行号、那一行（去注释后）。
struct Popup {
    file: String,
    line: usize,
    text: String,
}

fn popups() -> Vec<Popup> {
    let root = repo_root();
    let mut files: Vec<PathBuf> = Vec::new();
    walk(&root.join(WEB_SRC), &["ts", "tsx"], &mut files);
    assert!(
        !files.is_empty(),
        "{} 下一份源文件都没读到：这条门读错了目录，读不到与「没有违规」同形",
        WEB_SRC
    );
    let mut out = Vec::new();
    for path in files {
        let text = read(&path);
        let code = strip_comments(&text);
        let rel = relative_name(&root, &path);
        for (index, line) in code.lines().enumerate() {
            let trimmed = line.trim();
            if !trimmed.contains("window.open(") || !trimmed.contains("authorize_url") {
                continue;
            }
            out.push(Popup {
                file: rel.clone(),
                line: index + 1,
                text: trimmed.to_string(),
            });
        }
    }
    out
}

#[test]
fn every_authorization_popup_keeps_its_opener_and_listens_for_the_code() {
    let found = popups();
    assert!(
        !found.is_empty(),
        "一处打开授权窗的调用都没找到：判据空转，等于没判"
    );

    let mut failures = Vec::new();
    let mut sources: Vec<(String, String)> = Vec::new();

    for popup in &found {
        if popup.text.contains("noopener") {
            failures.push(format!(
                "{}:{} 打开授权窗时带了 noopener——落地页拿不到 opener，只能把授权码显示出来让人复制：\n    {}",
                popup.file, popup.line, popup.text
            ));
        }
        if !sources.iter().any(|(name, _)| name == &popup.file) {
            let text = read(&repo_root().join(&popup.file));
            sources.push((popup.file.clone(), strip_comments(&text)));
        }
    }

    for (file, code) in &sources {
        if !code.contains(CODE_WORD) {
            failures.push(format!(
                "{file} 打开了授权窗，却没有挂 `{CODE_WORD}` 的接收端——消息发出去没人收，弹窗自己关掉，向导停在「还没授权」"
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
