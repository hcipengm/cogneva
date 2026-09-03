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
    /// Clarified — ready to enter the fix pipeline.
    Clarified,
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

        // Derive state: a bot question with no later substantive user reply
        // means we are still waiting (or stale); a later user reply that
        // actually carries text means clarified. A picture-only / empty /
        // link-only comment is NOT an answer — treating it as one made the bot
        // re-ask on every such comment (observed in production).
        let last_bot = convo
            .turns
            .iter()
            .rposition(|t| t.role == ConversationRole::Bot);
        if let Some(idx) = last_bot {
            let user_replied = convo.turns[idx + 1..]
                .iter()
                .any(|t| t.role == ConversationRole::User && is_substantive_reply(&t.body));
            convo.state = if user_replied {
                ConversationState::Clarified
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
}

/// A user comment counts as an answer only if it carries readable text.
///
/// A screenshot pasted with no description, an empty comment, or a bare link
/// gives the triage model nothing to work with, so the bot must keep waiting
/// rather than treat it as "replied" and ask again. Markdown image embeds
/// (`![alt](url)`) and bare URLs are removed before counting; a comment is
/// substantive when at least one letter/digit/CJK character remains.
pub(crate) fn is_substantive_reply(body: &str) -> bool {
    let mut s = body.to_string();
    // Remove markdown image embeds ![alt](url) (alt text like "image" carries
    // no answer content).
    while let Some(start) = s.find("![") {
        let after = &s[start + 2..];
        let Some(rel_open) = after.find("](") else {
            break;
        };
        let open_abs = start + 2 + rel_open + 2;
        let Some(rel_close) = s[open_abs..].find(')') else {
            break;
        };
        let close_abs = open_abs + rel_close + 1;
        s.replace_range(start..close_abs, " ");
    }
    // Drop bare URL tokens, then count any real characters (CJK counts as
    // alphanumeric via Unicode Letter category).
    s.split_whitespace()
        .filter(|w| !w.starts_with("http://") && !w.starts_with("https://"))
        .flat_map(|w| w.chars())
        .any(|c| c.is_alphanumeric())
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
    fn clarified_when_user_replied_after_bot() {
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
        assert_eq!(convo.state, ConversationState::Clarified);
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
    fn image_only_reply_keeps_waiting() {
        // 图片-only 评论（截图无文字说明）不是有效回答：状态应保持
        // AwaitingClarification，而不是翻成 Clarified 触发再追问。
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
        assert_eq!(convo.state, ConversationState::AwaitingClarification);
        assert_eq!(convo.rounds, 1);
    }

    #[test]
    fn empty_and_link_only_replies_keep_waiting() {
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
            ConversationState::AwaitingClarification
        );
    }

    #[test]
    fn text_reply_after_question_is_clarified() {
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
        assert_eq!(convo.state, ConversationState::Clarified);
    }

    #[test]
    fn substantive_reply_detection() {
        assert!(!is_substantive_reply(
            "![image](https://github.com/x/y.png)"
        ));
        assert!(!is_substantive_reply(""));
        assert!(!is_substantive_reply("   \n\t "));
        assert!(!is_substantive_reply("https://example.com"));
        assert!(is_substantive_reply(
            "see this screenshot ![](https://x/y.png)"
        ));
        assert!(is_substantive_reply("复现：启动即崩溃"));
        assert!(is_substantive_reply("v0.2.0"));
    }
}
