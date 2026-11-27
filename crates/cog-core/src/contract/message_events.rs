//! 一条消息被发出：谁发的、发给谁、正文、渠道。会话是它的投影。
//!
//! 队列与走线不是事实：投递失败、重投、没人读，都是投递面的事，不改写这一条。
//! 与意图、待办、员工同形：一个 trait、一条 record，信封在这一处拼一次。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{EnvelopeFields, SFResult};

/// 这条事实说的是什么：一条消息。
pub const MESSAGE_SUBJECT_KIND: &str = "message";
/// 一条消息被发出。
pub const MESSAGE_SENT: &str = "message_sent";

/// 一条消息的身份：两端＋渠道＋正文＋时刻的内容指纹。
///
/// 不带时刻的话，同一对人之间两句一模一样的话会被并成一条——那是两次说话，
/// 不是一次重投。反过来，整条重投（同一时刻、同样的字节）拿到同一个键。
pub fn message_idempotency(
    from: &str,
    to: &str,
    kind: &str,
    text: &str,
    session: Option<&str>,
    causation: Option<&str>,
    at: DateTime<Utc>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for part in [
        from,
        to,
        kind,
        text,
        session.unwrap_or_default(),
        causation.unwrap_or_default(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    hasher.update(at.timestamp_millis().to_string().as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!("message:{hex}")
}

/// 一条「消息已发出」的事实：调用点只交出两侧与正文，信封由这一处拼。
pub fn message_event(
    from: &str,
    to: &str,
    kind: &str,
    text: &str,
    session: Option<&str>,
    causation: Option<&str>,
    artifact: Option<&str>,
    at: DateTime<Utc>,
) -> crate::FactEvent {
    let mut event = crate::FactEvent::new(crate::EventPayload::MessageSent {
        from: from.to_string(),
        to: to.to_string(),
        kind: kind.to_string(),
        text: text.to_string(),
        session: session.map(str::to_string),
        causation: causation.map(str::to_string),
        artifact: artifact.map(str::to_string),
    });
    event.at = Some(at);
    event
}

/// 这条消息的信封。
pub fn message_envelope_of(
    from: &str,
    to: &str,
    kind: &str,
    text: &str,
    session: Option<&str>,
    causation: Option<&str>,
    at: DateTime<Utc>,
    writer: &str,
    event_id: &str,
) -> EnvelopeFields {
    let attributes = serde_json::json!({
        "from": from,
        "to": to,
        "kind": kind,
        "text": text,
        "session": session,
        "at": at.to_rfc3339(),
    })
    .to_string();
    EnvelopeFields {
        event_id: event_id.to_string(),
        kind: MESSAGE_SENT.to_string(),
        subject_kind: MESSAGE_SUBJECT_KIND.to_string(),
        subject_id: if from.is_empty() { to } else { from }.to_string(),
        unit_of_work: to.to_string(),
        step: "sent".into(),
        outcome: "ok".into(),
        idempotency_key: message_idempotency(from, to, kind, text, session, causation, at),
        // 信封的 actor 不许空：空串在这里读作「没留名」，不是编一个人。
        actor: if from.is_empty() {
            "unknown".to_string()
        } else {
            from.to_string()
        },
        writer: writer.to_string(),
        scope: "message".into(),
        attributes,
        causation_id: causation.map(str::to_string),
        batch_id: None,
        session_id: session.map(str::to_string),
        attempt: 1,
        payload_version: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// kind 在闭集里，信封过得了检查，正文原样在载荷里。
    #[test]
    fn a_sent_message_is_a_known_fact_with_its_text_intact() {
        assert!(crate::is_known_kind(MESSAGE_SENT));
        let at = DateTime::from_timestamp(100, 0).unwrap();
        let fields = message_envelope_of(
            "employee:1",
            "employee:2",
            "prompt",
            "把那一处改掉",
            Some("s-1"),
            Some("intent-event-1"),
            at,
            "d",
            "e-1",
        );
        assert!(crate::contract::event_envelope::check(&fields).is_ok());
        let attributes: serde_json::Value = serde_json::from_str(&fields.attributes).unwrap();
        assert_eq!(attributes["text"], "把那一处改掉");
        assert_eq!(attributes["from"], "employee:1");
    }

    /// 同一句话在同一时刻重投是同一行；同一句话晚一刻说，是第二条事实。
    #[test]
    fn the_same_bytes_at_the_same_moment_are_one_row() {
        let a = DateTime::from_timestamp(100, 0).unwrap();
        let b = DateTime::from_timestamp(101, 0).unwrap();
        assert_eq!(
            message_idempotency("a", "b", "prompt", "x", None, None, a),
            message_idempotency("a", "b", "prompt", "x", None, None, a)
        );
        assert_ne!(
            message_idempotency("a", "b", "prompt", "x", None, None, a),
            message_idempotency("a", "b", "prompt", "x", None, None, b)
        );
    }
}
