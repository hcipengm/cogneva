//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//! 把一次调用写成事件流里的一行。这是那一族的**写入口**，读侧还没切过来。
//!
//! 三条形状上的讲究，都与 `cogneva_event` 的信封有关：
//!
//! - 信封的每一列都点名写，不靠默认值：默认值只盖住「服务端时刻」与「第一次尝试」，
//!   其余每一列缺了都少一个能力（迁移 117 的逐列注释写了每一个）。
//! - `on conflict (writer, idempotency_key) do nothing`：同一次调用重放写的是同一条。
//! - `attributes` 是 text 列，存的是载荷本身的 JSON，不是它的再次编码——一条事件读回来
//!   时那一格应当是能直接解析的对象，而不是一个含 JSON 的字符串。这一点由这里的
//!   `to_string()` 与读侧的 `from_str` 一对保证，写错的方向是「看起来存进去了」。

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{EventPayload, EventStore, FactEvent, LlmCall, SFError, SFResult};

/// 事件流的写入器，只认这一种事实。落库走那条流**唯一的写口**。
pub struct LlmCallEventStore {
    pool: PgPool,
    store: std::sync::Arc<dyn EventStore>,
}

impl LlmCallEventStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        let store: std::sync::Arc<dyn EventStore> = std::sync::Arc::new(
            crate::postgres::event_log::EventLogStore::new(pool.clone(), writer),
        );
        Self { pool, store }
    }
}

impl LlmCallEventStore {
    /// 写一条调用事实：与其他族同一条流、同一个写口。
    pub async fn append(&self, call: &LlmCall) -> SFResult<()> {
        let mut event = FactEvent::new(EventPayload::LlmCall(call.clone()));
        event.event_id = Some(ulid_like_id(&call.call_id));
        event.at = Some(call.at);
        self.store.append(event).await
    }
}

/// 一次「没命中」按**为什么没命中**分类后的一格：类名与条数。
///
/// 命中率低是一个数，而两种低法要做的活完全不同：前缀接不上（工具定义变了、system
/// 里注入了每轮都变的东西、历史被改写）与接得上却没有命中（上一轮的字节这次没被
/// 复用）。合成一个数，谁也看不出该修哪一层。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheMissClass {
    /// `model`／`system`／`tools`／`messages`／`none`（接得上却没命中）／`unmeasured`。
    pub class: String,
    pub misses: i64,
}

/// 窗口里每一次「没命中」按上面的分类计数。
///
/// 只看同会话里**前一条留下清单**的那些调用：没有清单的那一次无从判断接没接上，
/// 记成 `unmeasured` 而不是猜一个类。缓存命中数取自那一行自己的 `cached_tokens`。
pub async fn cache_miss_breaks(
    pool: &PgPool,
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
) -> SFResult<Vec<CacheMissClass>> {
    use cog_core::contract::prompt_prefix::{classify_manifest_break, parse_manifest};
    use std::collections::BTreeMap;
    let rows: Vec<(String, Option<String>, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT idempotency_key, attributes::jsonb ->> 'session', cached_tokens, \
                attributes::jsonb ->> 'prompt_manifest' \
           FROM cogneva_event \
          WHERE kind = 'llm_call' AND at >= $1 AND at < $2 \
          ORDER BY at, event_id",
    )
    .bind(since)
    .bind(until)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read calls for cache misses: {e}")))?;
    let mut previous: BTreeMap<String, cog_core::contract::prompt_prefix::PrefixManifest> =
        BTreeMap::new();
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    for (_call_id, session, cached, manifest) in rows {
        let Some(session) = session else { continue };
        let manifest = manifest.as_deref().and_then(parse_manifest);
        let hit = cached.unwrap_or(0) > 0;
        if !hit {
            let class = match (&previous.get(&session), &manifest) {
                (Some(before), Some(now)) => classify_manifest_break(before, now).as_str(),
                // 没有前一条或没有这一次的清单：判断不了，记成没量到。
                _ => "unmeasured",
            };
            *counts.entry(class.to_string()).or_default() += 1;
        }
        match manifest {
            Some(manifest) => {
                previous.insert(session, manifest);
            }
            // 没有清单的那一次不在链上：前后比较不能跨过它。
            None => {
                previous.remove(&session);
            }
        }
    }
    Ok(counts
        .into_iter()
        .map(|(class, misses)| CacheMissClass { class, misses })
        .collect())
}

/// 同会话相邻两次调用之间，前缀断在哪一格：前一条的身份、后一条的身份、断法。
///
/// 读的是两次调用**自己记下的清单**，不重算请求体：请求体可能已经随留底过期，
/// 而清单是调用那一刻算的。没有清单的那一次调用不能被跳过——跨过它去比前后两条，
/// 报出来的「接得上」是关于两条彼此没见过的请求说的。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixObservation {
    pub previous: String,
    pub next: String,
    pub break_: cog_core::contract::prompt_prefix::PrefixBreak,
}

impl LlmCallEventStore {
    /// 一个会话里每一次前缀断裂，按发生顺序。
    pub async fn prefix_breaks_in_session(
        &self,
        session: &str,
    ) -> SFResult<Vec<PrefixObservation>> {
        use cog_core::contract::prompt_prefix::{classify_manifest_break, parse_manifest};
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT idempotency_key, attributes::jsonb ->> 'prompt_manifest' \
               FROM cogneva_event \
              WHERE kind = 'llm_call' AND attributes::jsonb ->> 'session' = $1 \
              ORDER BY at, event_id",
        )
        .bind(session)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("read calls of session {session}: {e}")))?;
        let mut out = Vec::new();
        let mut previous: Option<(String, cog_core::contract::prompt_prefix::PrefixManifest)> =
            None;
        for (call_id, manifest) in rows {
            let Some(manifest) = manifest.as_deref().and_then(parse_manifest) else {
                // 没有清单：这一条不在链上，也不假装它没出现过——前后的比较不能跨过它。
                previous = None;
                continue;
            };
            if let Some((previous_id, previous_manifest)) = &previous {
                out.push(PrefixObservation {
                    previous: previous_id.clone(),
                    next: call_id.clone(),
                    break_: classify_manifest_break(previous_manifest, &manifest),
                });
            }
            previous = Some((call_id, manifest));
        }
        Ok(out)
    }
}

impl LlmCallEventStore {
    /// 按批号取这一批发起过的调用，按发生时刻排。
    ///
    /// 批号是那次调用自己带进信封的那一列；这里只读列，不在载荷里再取一次
    /// ——「写的人放载荷、读的人按列」正是这一族此前答「这一批没有记录」的形状。
    ///
    /// 读不回来的那一行**点名报错**，不跳过：少一行会让「这一批花了多少」算少，
    /// 而算少的数看起来与真的少花了一样。
    pub async fn calls_in_batch(&self, batch_id: &str) -> SFResult<Vec<cog_core::LlmCall>> {
        const SQL: &str = "\
SELECT idempotency_key, actor, attributes, at FROM cogneva_event \
 WHERE kind = 'llm_call' AND batch_id = $1 \
 ORDER BY at, event_id";
        let rows = sqlx::query(SQL)
            .bind(batch_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read llm calls of batch {batch_id}: {e}")))?;
        let mut calls = Vec::with_capacity(rows.len());
        for row in rows {
            let call_id: String = row
                .try_get("idempotency_key")
                .map_err(|e| SFError::Database(format!("read a call identity: {e}")))?;
            let actor: String = row.try_get("actor").unwrap_or(Default::default());
            let text: String = row
                .try_get("attributes")
                .map_err(|e| SFError::Database(format!("read a call payload: {e}")))?;
            let at: chrono::DateTime<chrono::Utc> = row
                .try_get("at")
                .map_err(|e| SFError::Database(format!("read a call time: {e}")))?;
            let attributes: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                SFError::Database(format!(
                    "call {call_id} carries a payload that is not JSON: {e}"
                ))
            })?;
            let call = cog_core::LlmCall::from_attributes(call_id.clone(), actor, &attributes, at)
                .ok_or_else(|| {
                    SFError::Database(format!(
                        "call {call_id} is missing a cell its own writer always writes"
                    ))
                })?;
            calls.push(call);
        }
        Ok(calls)
    }
}

/// 事件 id 由调用身份派生，同一身份重放得到同一个 id。
///
/// 不做成随机 ULID：那样重放会造出第二个 id，而 id 是主键——两个 id 指着同一件事，
/// 读到的人要自己去猜哪条是真的。派生之后，`(writer, idempotency_key)` 与主键说的是
/// 同一件事，两条保护不会互相打架。
fn ulid_like_id(call_id: &str) -> String {
    // 26 个字符的 Crockford base32 形状，取自调用身份的前 26 个合法字符，不足则补 0。
    // 这不是 ULID 的时间序，只是同一个 26 字符外形——信封要的是「够长且稳定」，而
    // 「按 id 排序即按时间排序」这条约定属于**入账事件**，不是这一族。
    let mut out = String::with_capacity(26);
    for ch in call_id.chars().filter(|c| c.is_ascii_alphanumeric()) {
        if out.len() == 26 {
            break;
        }
        out.push(ch.to_ascii_uppercase());
    }
    while out.len() < 26 {
        out.push('0');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 同一个调用身份派生出同一个 id：重放不会造出第二个主键。
    #[test]
    fn the_event_id_is_a_function_of_the_call_identity() {
        assert_eq!(ulid_like_id("call-abc"), ulid_like_id("call-abc"));
        assert_ne!(ulid_like_id("call-abc"), ulid_like_id("call-abd"));
    }

    /// id 恒为 26 个字符：信封那一列是定长的，短的会被拒或者被补齐到别人的形状。
    #[test]
    fn the_event_id_is_always_the_envelope_width() {
        for call_id in ["", "x", &"y".repeat(40)] {
            assert_eq!(ulid_like_id(call_id).len(), 26, "{call_id:?}");
        }
        assert!(
            ulid_like_id("").chars().all(|c| c == '0'),
            "an empty identity has to pad rather than fall short"
        );
    }
}
