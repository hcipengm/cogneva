//! 消息事实的写者：`cogneva_event` 上的 `message_sent`。会话是它的投影。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[cfg(test)]
mod tests {
    use super::*;

    /// 一条消息的信封拼得出来、过得了检查。
    #[test]
    fn a_message_has_a_complete_envelope() {
        let payload = cog_core::EventPayload::MessageSent {
            artifact: None,
            from: "a".into(),
            to: "b".into(),
            kind: "prompt".into(),
            text: "x".into(),
            session: None,
            causation: None,
        };
        let fields = payload.envelope("e-1", "probe");
        assert!(cog_core::contract::event_envelope::check(&fields).is_ok());
        assert_eq!(fields.kind, "message_sent");
    }
}
