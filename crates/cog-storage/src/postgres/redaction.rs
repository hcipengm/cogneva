//! principle: feedback_a_bound_whose_only_raise_me_signal_is_pass_failure_never_asks_to_be_raised
//!
//! 擦除：**销毁正文，追加一条 `redacted` 事件，永不改写历史**。
//!
//! 「擦掉一段正文」与「改一条历史」在读面上很容易被做成同一件事，而它们的区别正是
//! 这一族的全部：历史事件是「发生过什么」的记录，改它等于让回放读到一件从没发生过的
//! 事；正文是「那段字节本身」，销毁它不影响任何一条事实的因果链——被擦那条事件还在，
//! 它的哈希还在，只是那个哈希从此指不到可读的字节。
//!
//! 顺序是**先销毁、后追加**，理由是可重试：删一段已经不存在的正文是成功（见
//! [`large_object::unlink`]），所以「销毁成功、追加失败」之后再跑一次是安全的，第二次
//! 只补上那条事件。反过来（先追加）会让一条「已擦除」在没有真正擦掉的库里立着——那是
//! 这个仓库最忌的形状：一条判词说做完了，而事实没做。
//!
//! 进程如果死在两者之间，正文已经没了而事件还没写：那一格由 `redacted` 的**缺失**
//! 读得出来（`body_oid` 在库里指向一段不存在的字节，且没有事件记过它）。

use sqlx::PgPool;

use cog_core::{check as check_envelope, EnvelopeFields, SFError, SFResult};

use super::large_object;

/// 擦除事件的 kind。与 [`cog_core::KINDS`] 里的那一条是同一个词。
pub const KIND: &str = "redacted";

/// 一次擦除的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redaction {
    /// 留下的那条 `redacted` 事件的 id。
    pub event_id: String,
    /// 被销毁的那一段正文的 oid。
    pub oid: u32,
}

/// 事件 id 由「这一段正文 + 它的哈希」派生：同一次擦除重放，写回同一条事件。
fn event_id_for(oid: u32, sha256: &str) -> String {
    let mut out = String::with_capacity(26);
    for ch in format!("R{oid}{sha256}").chars() {
        if out.len() == 26 {
            break;
        }
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
        }
    }
    while out.len() < 26 {
        out.push('0');
    }
    out
}

/// 擦掉一段正文，并在事件流里记下这件事。
///
/// `sha256` 与 `bytes` 是封口时算好的那一对读数（[`large_object::seal`]）：擦除要把
/// 「擦掉的是哪一段」写清楚，而 oid 会随着字节一起消失，哈希是它唯一还认得出来的身份。
///
/// `actor` 是**要求擦除的人**，与 `writer`（哪个进程写的这条事件）分开：归因问的是
/// 前者，读数是后者。
pub async fn redact_body(
    pool: &PgPool,
    writer: &str,
    actor: &str,
    oid: u32,
    sha256: &str,
    bytes: u64,
    reason: &str,
) -> SFResult<Redaction> {
    let reason = reason.trim();
    if reason.is_empty() {
        return Err(SFError::Validation(
            "擦除要说得出为什么：一条没有理由的 `redacted` 与一次意外丢字节在账面上同形".into(),
        ));
    }

    let event_id = event_id_for(oid, sha256);
    let subject = format!("body:{oid}");
    let attributes = serde_json::json!({
        "body_oid": oid,
        "sha256": sha256,
        "bytes": bytes,
        "reason": reason,
    })
    .to_string();
    check_envelope(&EnvelopeFields {
        event_id: event_id.clone(),
        kind: KIND.into(),
        subject_kind: "body".into(),
        subject_id: subject.clone(),
        unit_of_work: subject,
        step: "redact".into(),
        outcome: "ok".into(),
        idempotency_key: format!("redact:{oid}:{sha256}"),
        actor: actor.into(),
        writer: writer.into(),
        scope: "body".into(),
        attributes: attributes.clone(),
    })
    .map_err(|defect| {
        SFError::Database(format!(
            "redacted refused before it was written: {}",
            defect.as_str()
        ))
    })?;

    // **先判信封、再销毁**。反过来的顺序里，一次被拒的信封（空 actor、非对象载荷）
    // 会让正文已经被 `lo_unlink` 掉，而库里没有一条 `redacted` 事件说明它去了哪——
    // 「被擦掉」与「坏了」从此分不开。销毁这一步可重复（`unlink` 只在它还在时删），
    // 所以顺序换过来不损失重放安全。
    large_object::unlink(pool, oid).await?;

    sqlx::query(
        "INSERT INTO cogneva_event (\
            event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
            idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, \
            attributes, at) \
         VALUES ($1, 'redacted', 'body', $2, $2, 'redact', 'ok', 1, $3, NULL, NULL, $4, $5, \
                 'body', 1, $6, NOW()) \
         ON CONFLICT (writer, idempotency_key) DO NOTHING",
    )
    .bind(&event_id)
    .bind(format!("body:{oid}"))
    .bind(format!("redact:{oid}:{sha256}"))
    .bind(actor)
    .bind(writer)
    .bind(attributes)
    .execute(pool)
    .await
    .map_err(|e| SFError::Database(format!("append redacted: {e}")))?;

    Ok(Redaction { event_id, oid })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_event_id_is_a_function_of_the_body_and_its_hash() {
        assert_eq!(event_id_for(7, "abc"), event_id_for(7, "abc"));
        assert_ne!(event_id_for(7, "abc"), event_id_for(8, "abc"));
        assert_ne!(event_id_for(7, "abc"), event_id_for(7, "abd"));
        assert_eq!(event_id_for(7, "abc").len(), 26);
    }

    /// 空的理由不是理由：那条事件说得出的只有「有人擦了一段」，说不出「为什么」。
    #[tokio::test]
    async fn a_redaction_without_a_reason_is_refused() {
        // 连库之前就拒：池子指向一个不存在的地址也无所谓，这条用例要的是那句拒词。
        let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .expect("a lazy pool needs no server");
        let err = redact_body(&pool, "writer", "actor", 1, "sha", 10, "   ")
            .await
            .expect_err("a blank reason is refused");
        assert!(matches!(err, SFError::Validation(_)), "{err:?}");
    }
}
