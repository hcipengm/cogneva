//! Issue conversation — multi-round clarification state machine.
//!
//! When triage decides an issue lacks information, the bot posts a
//! clarification question (subject to `conversation.auto_reply`), waits for
//! the reporter's reply, re-evaluates, and gives up after
//! `max_clarification_rounds` or `awaiting_reply_timeout_hours`.

use chrono::{DateTime, Utc};

use crate::config::{ConversationConfig, GitHubIntegrationConfig};

use crate::error::Result;
use crate::provider::{CodePlatformProvider, PlatformComment};

/// Who produced a conversation turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationRole {
    /// The issue reporter or another human.
    User,
    /// The Cogneva bot.
    Bot,
}

/// A single conversation turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ConversationTurn {
    /// Who wrote this turn.
    pub role: ConversationRole,
    /// Turn body (markdown).
    pub body: String,
    /// When the turn was created.
    pub created_at: DateTime<Utc>,
}

/// Conversation lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationState {
    /// No clarification in progress.
    Idle,
    /// Waiting for the reporter to reply.
    AwaitingClarification,
    /// Reporter has sent a new turn since the bot's last question. The turn may
    /// be text, a screenshot, or a link — whether it makes the issue actionable
    /// is triage's call, not decided here.
    UserReplied,
    /// Timed out waiting for a reply.
    Stale,
    /// Escalated to a human.
    Escalated,
}

/// Multi-round clarification conversation for one issue.
#[derive(Debug, Clone)]
pub struct IssueConversation {
    /// Issue number on the platform.
    pub issue_number: u64,
    /// Conversation turns in chronological order.
    pub turns: Vec<ConversationTurn>,
    /// Current state.
    pub state: ConversationState,
    /// Number of clarification rounds used.
    pub rounds: u32,
}

impl IssueConversation {
    /// Create a fresh conversation for an issue.
    pub fn new(issue_number: u64) -> Self {
        Self {
            issue_number,
            turns: Vec::new(),
            state: ConversationState::Idle,
            rounds: 0,
        }
    }

    /// Rebuild the conversation from platform comments, classifying authors
    /// as bot or user via `bot_username`.
    pub fn from_comments(
        issue_number: u64,
        comments: &[PlatformComment],
        bot_username: &str,
        config: &ConversationConfig,
    ) -> Self {
        let mut convo = Self::new(issue_number);
        for comment in comments {
            // 机器人评论的两种身份：作者名匹配，或正文带机器人签名后缀。
            // 后者覆盖"平台 token 属于人类账号"的场景——评论作者是人类
            // 用户名，但内容是机器人发的；漏判会把机器人追问当用户回复，
            // 每轮发现都重复追问（已在生产实证）。
            let signed_by_bot = !config.bot_signature.is_empty()
                && comment
                    .body
                    .trim_end()
                    .ends_with(config.bot_signature.as_str());
            let role = if comment.author == bot_username || signed_by_bot {
                ConversationRole::Bot
            } else {
                ConversationRole::User
            };
            convo.turns.push(ConversationTurn {
                role,
                body: comment.body.clone(),
                created_at: comment.created_at,
            });
        }
        convo.rounds = convo
            .turns
            .iter()
            .filter(|t| t.role == ConversationRole::Bot)
            .count() as u32;

        // Derive state from whether the reporter came back with a new turn
        // after our last question. We do NOT judge actionability here — that is
        // triage's job, and it now reads the full thread (including image/link
        // attachments, which can carry a valid answer). A non-empty user turn
        // (text, screenshot, or link) therefore marks `UserReplied` and triggers
        // a triage re-evaluation; only an empty/whitespace comment leaves us
        // waiting. This prevents re-asking when there is no new turn while still
        // letting a screenshot-only reply be acted on when triage can use it.
        let last_bot = convo
            .turns
            .iter()
            .rposition(|t| t.role == ConversationRole::Bot);
        if let Some(idx) = last_bot {
            let user_responded = convo.turns[idx + 1..]
                .iter()
                .any(|t| t.role == ConversationRole::User && !t.body.trim().is_empty());
            convo.state = if user_responded {
                ConversationState::UserReplied
            } else if convo.timed_out(config) {
                ConversationState::Stale
            } else {
                ConversationState::AwaitingClarification
            };
        }
        convo
    }

    /// Record a bot question. Returns the comment body to post (question +
    /// signature), or `None` when the round budget is exhausted.
    pub fn ask(&mut self, question: &str, config: &ConversationConfig) -> Option<String> {
        if self.rounds >= config.max_clarification_rounds {
            self.state = ConversationState::Stale;
            return None;
        }
        let body = if config.bot_signature.is_empty() {
            question.to_string()
        } else {
            format!("{}\n\n{}", question, config.bot_signature)
        };
        self.turns.push(ConversationTurn {
            role: ConversationRole::Bot,
            body: body.clone(),
            created_at: Utc::now(),
        });
        self.rounds += 1;
        self.state = ConversationState::AwaitingClarification;
        Some(body)
    }

    /// Post a bot reply to the issue when `auto_reply` is enabled.
    /// Returns `true` when the comment was actually posted.
    pub async fn post_reply(
        &self,
        provider: &dyn CodePlatformProvider,
        body: &str,
        config: &ConversationConfig,
    ) -> Result<bool> {
        if !config.auto_reply {
            tracing::info!(
                issue = self.issue_number,
                "auto_reply disabled; clarification question not posted"
            );
            return Ok(false);
        }
        provider
            .comment_on_issue(self.issue_number, body.to_string())
            .await?;
        Ok(true)
    }

    /// True when the wait for a user reply exceeded the configured timeout.
    pub fn timed_out(&self, config: &ConversationConfig) -> bool {
        let Some(last_bot) = self
            .turns
            .iter()
            .rev()
            .find(|t| t.role == ConversationRole::Bot)
        else {
            return false;
        };
        let elapsed = Utc::now() - last_bot.created_at;
        elapsed.num_hours() > config.awaiting_reply_timeout_hours as i64
    }

    /// Advance the state machine after a scan. Returns the timeout decision
    /// when the conversation went stale.
    pub fn check_timeout(&mut self, config: &GitHubIntegrationConfig) -> Option<&'static str> {
        if self.state == ConversationState::AwaitingClarification
            && self.timed_out(&config.conversation)
        {
            self.state = ConversationState::Stale;
            return Some("clarification timed out");
        }
        None
    }

    /// Media attachment URLs found in the reporter's turns (chronological,
    /// de-duplicated). Screenshots/recordings are valid answers — they are
    /// fetched and fed to the multimodal actionability judge.
    pub fn media_urls(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for turn in &self.turns {
            if turn.role != ConversationRole::User {
                continue;
            }
            for url in extract_media_urls(&turn.body) {
                if !out.contains(&url) {
                    out.push(url);
                }
            }
        }
        cap_recent(out, MAX_MEDIA_PER_INTENT)
    }

    /// Render the comment thread as triage context. Triage must judge
    /// actionability from the whole conversation — issue body plus the
    /// reporter's follow-up replies — not just the (often terse) original body.
    /// Image/link attachments are kept verbatim so a capable judge can use
    /// them; a judge that cannot read images is told to ask for the text.
    pub fn triage_context(&self) -> String {
        if self.turns.is_empty() {
            return String::new();
        }
        let mut out = String::new();
        for turn in &self.turns {
            let who = match turn.role {
                ConversationRole::User => "Reporter",
                ConversationRole::Bot => "Cogneva bot (previous question)",
            };
            out.push_str(&format!("{}: {}\n", who, turn.body.trim()));
        }
        out.trim_end().to_string()
    }
}

/// At most this many attachments are fetched and sent to the judge per
/// intent (the most recent ones).
pub const MAX_MEDIA_PER_INTENT: usize = 6;

/// Keep at most `cap` items, preferring the most recent (tail) — comments
/// grow over time and the latest attachments are the most relevant.
fn cap_recent(mut urls: Vec<String>, cap: usize) -> Vec<String> {
    if urls.len() > cap {
        urls.drain(0..urls.len() - cap);
    }
    urls
}

/// Extract media attachment URLs from a markdown/HTML comment body.
///
/// Covers markdown image embeds (`![alt](url)`), HTML media tags
/// (`<img>`/`<video>`/`<audio>`/`<source src=...>`), markdown links and bare
/// URLs that point at a media file or a platform user-attachment (GitHub's
/// screenshot URLs have no file extension). Order preserved, de-duplicated.
pub fn extract_media_urls(body: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |url: &str| {
        let url = url.trim();
        if url.is_empty() {
            return;
        }
        if !out.iter().any(|u| u == url) {
            out.push(url.to_string());
        }
    };

    // Markdown image embeds: ![alt](url) — always treated as media.
    let bytes = body.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'!' && bytes[i + 1] == b'[' {
            if let Some((url, next)) = markdown_link_target(body, i + 1) {
                push(&url);
                i = next;
                continue;
            }
        }
        i += 1;
    }

    // HTML media tags: src="..." / src='...' on img/video/audio/source/picture.
    for (tag, rest) in split_tags(body) {
        if matches!(
            tag.as_str(),
            "img" | "video" | "audio" | "source" | "picture"
        ) {
            if let Some(u) = attr_value(&rest, "src") {
                push(&u);
            }
            if let Some(u) = attr_value(&rest, "poster") {
                push(&u);
            }
        }
    }

    // Markdown links [text](url) and bare http(s) tokens: media only when the
    // URL looks like a media file or a platform user-attachment. Walk byte
    // offsets but only slice at char boundaries (a multibyte UTF-8 char such as
    // CJK would otherwise make `body[j..]` panic mid-character).
    let mut j = 0;
    while j < body.len() {
        if body.is_char_boundary(j) && body[j..].starts_with('[') {
            if let Some((url, next)) = markdown_link_target(body, j) {
                if looks_like_media_url(&url) {
                    push(&url);
                }
                j = next;
                continue;
            }
        }
        j += 1;
    }
    for token in body.split_whitespace() {
        let t = token.trim_matches(|c: char| matches!(c, '(' | ')' | '<' | '>' | ',' | '.'));
        if (t.starts_with("http://") || t.starts_with("https://")) && looks_like_media_url(t) {
            push(t);
        }
    }

    cap_recent(out, MAX_MEDIA_PER_INTENT)
}

/// Given a markdown construct starting at `pos` (the `[` of either `![..]` or
/// `[..]`), return the link target URL and the index just past the closing
/// `)`. Returns `None` when this is not a complete markdown link.
fn markdown_link_target(body: &str, pos: usize) -> Option<(String, usize)> {
    let bytes = body.as_bytes();
    if bytes.get(pos) != Some(&b'[') {
        return None;
    }
    let close_bracket = body[pos..].find(']')? + pos;
    let after = close_bracket + 1;
    if bytes.get(after) != Some(&b'(') {
        return None;
    }
    let open_paren = after;
    let close_paren = body[open_paren + 1..].find(')')? + open_paren + 1;
    let raw = body[open_paren + 1..close_paren].trim();
    // A markdown link can be [text](url "title"); strip an optional title.
    let url = raw
        .split_whitespace()
        .next()
        .unwrap_or(raw)
        .trim_matches('<')
        .trim_end_matches('>');
    if url.is_empty() {
        return None;
    }
    Some((url.to_string(), close_paren + 1))
}

/// Split an HTML-ish body into (tag name, attributes) pairs for tags that
/// carry a `src` attribute. Best-effort parser for comment bodies, not a full
/// HTML parser.
fn split_tags(body: &str) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find('<') {
        let after = &rest[start + 1..];
        let Some(end_rel) = after.find('>') else {
            break;
        };
        let inner = &after[..end_rel];
        let inner = inner.trim_start_matches('/').trim();
        if let Some(space) = inner.find(|c: char| c.is_whitespace()) {
            let tag = inner[..space].to_ascii_lowercase();
            let attrs = inner[space..].to_string();
            tags.push((tag, attrs));
        }
        rest = &after[end_rel + 1..];
    }
    tags
}

/// Read an attribute value from an HTML tag's attribute string.
fn attr_value(attrs: &str, name: &str) -> Option<String> {
    let needle = name;
    let idx = attrs.to_ascii_lowercase().find(needle)?;
    let after = &attrs[idx + needle.len()..];
    let after = after.trim_start();
    if !after.starts_with('=') {
        return None;
    }
    let after = after[1..].trim_start();
    let quote = after.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let val = &after[1..];
    let end = val.find(quote)?;
    Some(val[..end].to_string())
}

/// Whether a URL points at media we can fetch: a media file extension or a
/// platform user-attachment path (GitHub screenshot URLs have no extension).
fn looks_like_media_url(url: &str) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let lower = path.to_ascii_lowercase();
    let ext_ok = [
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg", ".mp4", ".webm", ".mov", ".mkv",
        ".mp3", ".wav", ".ogg", ".m4a", ".pdf",
    ]
    .iter()
    .any(|e| lower.ends_with(e));
    ext_ok || lower.contains("/user-attachments/") || lower.contains("githubusercontent.com/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comment(author: &str, body: &str, secs: i64) -> PlatformComment {
        PlatformComment {
            author: author.into(),
            body: body.into(),
            created_at: DateTime::from_timestamp(secs, 0).unwrap(),
        }
    }

    fn now_ts() -> i64 {
        Utc::now().timestamp()
    }

    #[test]
    fn awaiting_state_when_bot_asked_and_no_reply() {
        let comments = vec![comment("cogneva-bot", "please clarify", now_ts())];
        let convo = IssueConversation::from_comments(
            7,
            &comments,
            "cogneva-bot",
            &ConversationConfig::default(),
        );
        assert_eq!(convo.state, ConversationState::AwaitingClarification);
        assert_eq!(convo.rounds, 1);
    }

    #[test]
    fn bot_comment_recognized_by_signature_when_author_is_human() {
        // 平台 token 属于人类账号时，机器人评论的作者是人类用户名；
        // 签名后缀必须把它判为 Bot，否则每轮发现都会重复追问。
        let cfg = ConversationConfig::default();
        let comments = vec![comment(
            "hcipengm",
            "Could you describe the problem?\n\n— Cogneva Bot",
            now_ts(),
        )];
        let convo = IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg);
        assert_eq!(convo.state, ConversationState::AwaitingClarification);
        assert_eq!(convo.rounds, 1);
    }

    #[test]
    fn user_replied_after_bot_sets_user_replied() {
        let comments = vec![
            comment("cogneva-bot", "please clarify", now_ts() - 100),
            comment("alice", "here are the steps", now_ts()),
        ];
        let convo = IssueConversation::from_comments(
            7,
            &comments,
            "cogneva-bot",
            &ConversationConfig::default(),
        );
        assert_eq!(convo.state, ConversationState::UserReplied);
    }

    #[test]
    fn stale_after_timeout() {
        let cfg = ConversationConfig {
            awaiting_reply_timeout_hours: 1,
            ..Default::default()
        };
        let old = now_ts() - 3 * 3600;
        let comments = vec![comment("cogneva-bot", "please clarify", old)];
        let convo = IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg);
        assert_eq!(convo.state, ConversationState::Stale);
    }

    #[test]
    fn ask_respects_round_budget() {
        let cfg = ConversationConfig {
            max_clarification_rounds: 1,
            ..Default::default()
        };
        let mut convo = IssueConversation::new(7);
        assert!(convo.ask("q1", &cfg).is_some());
        assert!(convo.ask("q2", &cfg).is_none());
        assert_eq!(convo.state, ConversationState::Stale);
    }

    #[test]
    fn ask_appends_signature() {
        let cfg = ConversationConfig::default();
        let mut convo = IssueConversation::new(7);
        let body = convo.ask("what version?", &cfg).unwrap();
        assert!(body.contains("what version?"));
        assert!(body.contains(&cfg.bot_signature));
    }

    #[test]
    fn image_only_reply_is_a_user_turn() {
        // 截图-only 评论也是报告者的一次发言：图片可能就装着答案。是否可行动
        // 交给 triage（它现在能读到整段对话）判，状态机不再用"有没有文字"替它
        // 决定，所以这里应翻成 UserReplied 触发一次 triage，而不是永远等待。
        let cfg = ConversationConfig::default();
        let comments = vec![
            comment(
                "hcipengm",
                "Could you describe the problem?\n\n— Cogneva Bot",
                now_ts() - 200,
            ),
            comment(
                "hcipengm",
                "![image](https://github.com/user-attachments/assets/abc)",
                now_ts(),
            ),
        ];
        let convo = IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg);
        assert_eq!(convo.state, ConversationState::UserReplied);
        assert_eq!(convo.rounds, 1);
    }

    #[test]
    fn empty_comment_keeps_waiting_but_link_counts() {
        // 纯空白评论不是一次发言（继续等待）；而贴一个链接是非空发言——它可能
        // 指向复现仓库/gist/截图，可行动性同样交给 triage，不在此丢弃。
        let cfg = ConversationConfig::default();
        let mk = |body: &str| {
            let comments = vec![
                comment("cogneva-bot", "please clarify", now_ts() - 200),
                comment("alice", body, now_ts()),
            ];
            IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg).state
        };
        assert_eq!(mk("   "), ConversationState::AwaitingClarification);
        assert_eq!(
            mk("https://example.com/thing"),
            ConversationState::UserReplied
        );
    }

    #[test]
    fn text_reply_after_question_is_user_replied() {
        let cfg = ConversationConfig::default();
        let comments = vec![
            comment("cogneva-bot", "please clarify", now_ts() - 200),
            comment(
                "alice",
                "it crashes on startup, see trace: https://x/y",
                now_ts(),
            ),
        ];
        let convo = IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg);
        assert_eq!(convo.state, ConversationState::UserReplied);
    }

    #[test]
    fn extract_media_urls_finds_images_html_and_bare_links() {
        let body = "看这个截图 ![](https://github.com/u/r/assets/abc) 和
            <video controls src=\"https://x.com/demo.mp4\"></video>
            裸链 https://cdn.example.com/shot.png 以及普通链接 https://github.com/repo/issues/3";
        let urls = extract_media_urls(body);
        assert!(urls.iter().any(|u| u.contains("assets/abc")), "{urls:?}");
        assert!(urls.iter().any(|u| u.ends_with("demo.mp4")), "{urls:?}");
        assert!(urls.iter().any(|u| u.ends_with("shot.png")), "{urls:?}");
        // A plain issue link is not media.
        assert!(!urls.iter().any(|u| u.ends_with("issues/3")), "{urls:?}");
    }

    #[test]
    fn media_urls_collects_only_user_turns_deduped() {
        let cfg = ConversationConfig::default();
        let comments = vec![
            comment(
                "hcipengm",
                "Could you clarify?\n\n— Cogneva Bot",
                now_ts() - 200,
            ),
            comment(
                "alice",
                "截图：![a](https://github.com/u/x/assets/1) 还有 https://x/a.png",
                now_ts(),
            ),
        ];
        let convo = IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg);
        let urls = convo.media_urls();
        assert_eq!(urls.len(), 2, "{urls:?}");
        assert!(urls[0].contains("assets/1"));
    }

    #[test]
    fn triage_context_includes_full_thread() {
        let cfg = ConversationConfig::default();
        let comments = vec![
            comment("cogneva-bot", "please clarify", now_ts() - 200),
            comment("alice", "复现：启动即崩溃 v0.2.0", now_ts()),
        ];
        let convo = IssueConversation::from_comments(7, &comments, "cogneva-bot", &cfg);
        let ctx = convo.triage_context();
        assert!(ctx.contains("please clarify"));
        assert!(ctx.contains("复现：启动即崩溃 v0.2.0"));
        assert!(ctx.contains("Reporter"));
        assert!(IssueConversation::new(3).triage_context().is_empty());
    }
}
