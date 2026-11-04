//! 「explicit」「graphic」这两个词单独出现时，是技术英语的常用词，不是 NSFW。
//!
//! 这条过滤器判的是智能体对自己工作的陈述——计划、评审、变更说明。这类文本里
//! 「an explicit verdict」「the explicit enum variant」是常态：实测 30 分钟内 426 条
//! 模型回复里有 83 条含 `explicit` 或 `graphic`，而这些回复被整段判成
//! `NSFW content detected`，在 ralph 循环里读到的是
//! `guardrail_blocked → terminal_env_failure`，一次误判就终止整轮。
//!
//! 判据两半，缺一不成门：
//!
//! - 技术散文（`explicit verdict` / `graphic on the panel`）必须**放行**；
//! - 真的在描述内容的组合（`explicit content` / `explicit material`）必须**照旧拦**。
//!
//! 只有前半条，把整类词删掉也能绿；只有后半条，把词表放回最宽也能绿。

use cog_core::GuardResult;
use cog_guardrail::{ContentFilter, ContentFilterConfig};

fn filter() -> ContentFilter {
    ContentFilter::new(ContentFilterConfig::default())
}

#[tokio::test]
async fn technical_prose_is_not_graded_as_nsfw() {
    let cases = [
        "The output states an explicit verdict classifying the pairing as a genuine composite.",
        "Add an explicit match arm for the empty case instead of falling through.",
        "The graphic on the panel is drawn from the same reading as the table.",
        "Make the ordering explicit so the reader does not depend on iteration order.",
    ];
    for text in cases {
        match filter().check(text).await {
            GuardResult::Pass => {}
            other => panic!("技术散文被判成违规：{text:?} -> {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_word_that_names_content_is_still_refused() {
    for text in [
        "write explicit content for the page",
        "render explicit material into the response",
    ] {
        match filter().check(text).await {
            GuardResult::Block { reason, .. } => {
                assert_eq!(reason, "NSFW content detected", "拦下的理由 {text:?}");
            }
            other => panic!("该拦的没拦：{text:?} -> {other:?}"),
        }
    }
}
