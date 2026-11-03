//! 同一份请求，在它被拒时上游给出的等待期内，只发一次。
//!
//! 重放不是重试：重试是换一个时刻再问同一个问题，重放是在上游已经说过「这个
//! 窗口不行」之后，把逐字节相同的请求再买一遍——答案只能是同一句拒绝，而整份
//! prompt 已经付过费了。这两件事在这套系统里长得一模一样，因为发起重跑的那些层
//! （pipeline 的下一次 attempt、ralph 的下一轮、任务失败后的回投）拿不到上一次
//! 真正发出去的字节，也拿不到它是被怎么拒的。实测 2026-10-26：三分钟内 26 次
//! 逐字节相同的生成请求，每一次紧接着一条自报「一分钟后再来」的熔断拒绝。
//!
//! 所以这道闸放在**全进程**，不放在某个 agent 上：拒绝是上游对这份请求下的判词，
//! 与是哪一个 agent 对象发的无关，而重跑往往换一个新的 agent 实例（名字按
//! task+role 推，ralph 每轮铸一个新任务 id）。放在实例上，它就只在同一实例被复用
//! 时才看得见，而那正是最少见的那种重放。
//!
//! 只有**带等待时长**的拒绝才记。超时、卡顿、没有类型依据的失败都说不出「同一份
//! 请求何时值得再问」，把它们记下来会把一次抖动变成一段沉默——闸会挡住一个上游
//! 明明愿意回答的请求。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, Utc};

use cog_core::contract::llm::UpstreamFailure;

/// 闸里最多同时记多少份被拒的请求。
///
/// 上限而不是无限：这份记忆只在等待窗口内有意义，而等待窗口是分钟到天级；
/// 超出上限的旧条目先删，删掉只是少省一次调用，不会挡住本该发出去的东西。
const MAX_REMEMBERED: usize = 256;

/// 一份被拒的请求：上游说什么时候值得再问，以及它是怎么拒的。
#[derive(Debug, Clone)]
struct Refusal {
    until: DateTime<Utc>,
    cause: UpstreamFailure,
    reason: String,
}

fn guard() -> &'static Mutex<HashMap<String, Refusal>> {
    static GUARD: OnceLock<Mutex<HashMap<String, Refusal>>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 这份请求是否还在它被拒时给出的等待期内。
///
/// 返回拒绝的成因与原文：被拦下的那一轮要把**同一份失败**交回给上层，而不是另
/// 造一个「没发出去」的错误——上层拿成因与等待做退避决定，换了措辞就换了判据。
pub fn suppressed(request: &str, now: DateTime<Utc>) -> Option<(UpstreamFailure, String, i64)> {
    let mut held = guard().lock().unwrap_or_else(|e| e.into_inner());
    match held.get(request) {
        Some(refusal) if now < refusal.until => Some((
            refusal.cause,
            refusal.reason.clone(),
            (refusal.until - now).num_seconds().max(1),
        )),
        Some(_) => {
            // 窗口已过：这条记忆不再有意义，连同过期的一并清掉。
            held.retain(|_, refusal| now < refusal.until);
            None
        }
        None => None,
    }
}

/// 记下一次带等待时长的拒绝。
pub fn remember(
    request: &str,
    cause: UpstreamFailure,
    wait_secs: u64,
    reason: String,
    now: DateTime<Utc>,
) {
    let mut held = guard().lock().unwrap_or_else(|e| e.into_inner());
    if held.len() >= MAX_REMEMBERED {
        held.retain(|_, refusal| now < refusal.until);
        if held.len() >= MAX_REMEMBERED {
            // 全都还在窗口内：丢掉一个，由等待时间最短的那个先走——它最早过期，
            // 也最不值钱。
            if let Some(oldest) = held
                .iter()
                .min_by_key(|(_, refusal)| refusal.until)
                .map(|(key, _)| key.clone())
            {
                held.remove(&oldest);
            }
        }
    }
    held.insert(
        request.to_string(),
        Refusal {
            until: now + chrono::Duration::seconds(wait_secs as i64),
            cause,
            reason,
        },
    );
}

/// 这份请求拿到了答案，闸里那条记忆就该退场。
pub fn forget(request: &str) {
    let mut held = guard().lock().unwrap_or_else(|e| e.into_inner());
    held.remove(request);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn a_refused_request_is_held_only_for_the_wait_it_named() {
        let request = "guard-test/held-only-for-the-wait";
        assert!(
            suppressed(request, now()).is_none(),
            "nothing was refused yet"
        );

        remember(
            request,
            UpstreamFailure::RateLimited,
            60,
            "refused: come back later".into(),
            now(),
        );
        let (cause, reason, remaining) =
            suppressed(request, now()).expect("inside the wait this is a replay");
        assert_eq!(cause, UpstreamFailure::RateLimited);
        assert!(reason.contains("come back later"), "{reason}");
        assert!(remaining > 0 && remaining <= 60, "{remaining}");

        // 换一份请求就是换了一个问题，不受这份记忆约束。
        assert!(suppressed("guard-test/a-different-request", now()).is_none());

        // 窗口过了就不再拦：那时它是一次合法的重试。
        let after = now() + chrono::Duration::seconds(61);
        assert!(suppressed(request, after).is_none());
    }

    #[test]
    fn an_answer_retires_the_memory() {
        let request = "guard-test/answered";
        remember(
            request,
            UpstreamFailure::RateLimited,
            600,
            "refused".into(),
            now(),
        );
        assert!(suppressed(request, now()).is_some());
        forget(request);
        assert!(
            suppressed(request, now()).is_none(),
            "a request that was answered is not a replay risk"
        );
    }
}
