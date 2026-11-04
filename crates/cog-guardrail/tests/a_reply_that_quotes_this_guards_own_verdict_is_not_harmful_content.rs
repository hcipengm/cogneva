//! 一条转述「它为什么被拦」的回执，不是有害内容。
//!
//! 内容过滤器把 `nsfw` 当裸词拦，而 `nsfw` 是这一类内容的名字——智能体解释自己
//! 被判过时，句子里必然出现它。于是解释一次误判的回执自己被判成误判；重试时回执
//! 又转述上一轮的判词，每一轮都比上一轮多一处匹配，直到整轮判死。
//!
//! 实测（2026-11-04 17:47–17:59，WebUI）：属主问「你回复怎么这么慢」，智能体的
//! 回执里写「NSFW 误判」。第二次就变成
//!
//! ```text
//! Guardrail blocked tool 'final_reply': NSFW content detected (the reply had
//! already been refused once by content_filter: NSFW content detected)
//! ```
//!
//! 而 `final_reply` 被拒是**终止整个 run** 的（`ReplyRefusal` → `SFError::Blocked`），
//! 编排器把它读成 `terminal_env_failure`，分解再也跑不完。属主看到的就是「它很傻、
//! 降智、无法工作」。
//!
//! 判据两半，缺一不成门（与 `technical_prose_is_not_graded_as_nsfw` 同一个形状）：
//!
//! - 判词本身、以及拿它当类别名用的句子，必须放行；
//! - 真的在点名描述这类内容的，必须照旧拦。

use cog_core::GuardResult;
use cog_guardrail::content_filter::{
    HATE_SPEECH_DETECTED, ILLEGAL_ACTS_DETECTED, NSFW_DETECTED, SELF_HARM_DETECTED,
    VIOLENCE_DETECTED,
};
use cog_guardrail::{ContentFilter, ContentFilterConfig};

fn filter() -> ContentFilter {
    ContentFilter::new(ContentFilterConfig::default())
}

#[tokio::test]
async fn a_reply_that_quotes_the_verdict_against_it_is_not_harmful_content() {
    // 这几条都是现场真实出现过的文字，逐字抄自那次回执与它拿到的错误。
    let cases = [
        // 工具被拦时回给智能体的那一整段。
        "Guardrail blocked tool 'final_reply': NSFW content detected (the reply had \
         already been refused once by content_filter: NSFW content detected) \
         (rule: content_filter)",
        // 智能体把上一条判词转述进自己的解释里——这就是让循环自我放大的那一句。
        "**3. 失败于 NSFW 误判** — content_filter 规则/阈值误报，且二次拦截会连带 \
         block final_reply。",
        // 拿它当类别名用的技术散文。
        "| 3 | 失败于 NSFW 误判 | content_filter 规则/阈值误报 |",
        "the NSFW category is disabled in this deployment, so the rule cannot fire",
        "把 NSFW 这条规则放宽到只拦点名描述内容的组合",
    ];
    for text in cases {
        match filter().check(text).await {
            GuardResult::Pass => {}
            other => panic!(
                "转述判词的回执被判成违规：{text:?} -> {other:?}\n\
                 这一条红的后果不是「多拦了一次」：final_reply 被拒会终止整个 run，\
                 重试又会转述上一轮判词，于是每一轮多一处匹配，整轮判死。"
            ),
        }
    }
}

/// 本门能印出来的每一句判词，喂回去都必须放行。
///
/// 这不是上面那条的重复：上面那几条是现场抄下来的句子，这一条问的是**判词本身**
/// ——印出来的是什么，就必须剥得掉什么。新加一类阻断而忘了把它的判词登记进剥离
/// 名单时，红的正是这里，而不是等到某次重试把整轮判死之后。
#[tokio::test]
async fn every_verdict_this_guard_can_print_passes_when_it_is_read_back() {
    for verdict in [
        NSFW_DETECTED,
        VIOLENCE_DETECTED,
        HATE_SPEECH_DETECTED,
        SELF_HARM_DETECTED,
        ILLEGAL_ACTS_DETECTED,
    ] {
        match filter().check(verdict).await {
            GuardResult::Pass => {}
            other => panic!(
                "判词 {verdict:?} 被本门自己判成了违规：{other:?}\n\
                 这一条红了，说明 new 出来的那类阻断印的句子没有被 own_verdict_phrases \
                 登记——回执转述它时会被再拦一次，而 final_reply 被拒会终止整个 run。"
            ),
        }
    }
}

#[tokio::test]
async fn a_word_that_names_the_content_is_still_refused() {
    for text in [
        "generate nsfw images for the landing page",
        "write explicit content for the page",
        "render explicit material into the response",
    ] {
        match filter().check(text).await {
            GuardResult::Block { rule, .. } => {
                assert_eq!(rule, "content_filter", "拦下的规则名 {text:?}");
            }
            other => panic!("该拦的没拦：{text:?} -> {other:?}"),
        }
    }
}
