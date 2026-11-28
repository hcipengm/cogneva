//! 擦除外置正文：销毁 raw 里那一份，追加一条 `redacted`（带哈希与长度）。
//!
//! 外置到 raw 存储的正文没有大对象 oid，身份就是它的存放处（`artifact://…`）与哈希。
//! 顺序与 S31 一致：**先销毁、后追加**——重放一次是安全的，反过来会留下一条「已擦除」
//! 而没有真正擦掉的库。

use sha2::{Digest, Sha256};

use cog_core::{FactEvent, MemoryBackend, SFError, SFResult};

/// 擦掉一份外置正文；它不在时答 `false`（幂等，不是错误）。
pub async fn redact_artifact(
    backend: &dyn MemoryBackend,
    events: &dyn cog_core::EventStore,
    namespace: &str,
    id: &str,
    uri: &str,
    actor: &str,
    reason: &str,
) -> SFResult<bool> {
    let Some(raw) = backend.get_raw(namespace, id).await? else {
        return Ok(false);
    };
    let bytes = raw.payload.len() as u64;
    let sha256 = {
        let digest = Sha256::digest(&raw.payload);
        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            hex.push_str(&format!("{byte:02x}"));
        }
        hex
    };
    backend.delete_raw(namespace, id).await?;
    let mut event = FactEvent::new(cog_core::EventPayload::Redacted {
        oid: None,
        artifact: Some(uri.to_string()),
        sha256,
        bytes,
        reason: reason.to_string(),
        actor: actor.to_string(),
    });
    event.event_id = Some(events.next_event_id());
    events.append(event).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingEvents {
        seen: Mutex<Vec<cog_core::EventPayload>>,
    }

    #[async_trait::async_trait]
    impl cog_core::EventStore for RecordingEvents {
        fn writer(&self) -> &str {
            "redact-artifact-test"
        }
        fn next_event_id(&self) -> String {
            format!("e-{}", self.seen.lock().unwrap().len() + 1)
        }
        async fn append(&self, event: FactEvent) -> SFResult<()> {
            self.seen.lock().unwrap().push(event.payload);
            Ok(())
        }
    }

    /// 擦掉一份外置正文：raw 没了、事实还在，理由带着哈希与长度；再擦一次是空操作。
    #[tokio::test]
    async fn erasing_an_artifact_keeps_the_fact_and_drops_the_bytes() {
        use cog_core::MemoryBackend as _;
        let backend = crate::MemoryMemoryBackend::new();
        let raw = cog_core::RawSource {
            id: "m-1".into(),
            namespace: "default".into(),
            content_type: "text/plain".into(),
            payload: b"secret".to_vec(),
            tags: Vec::new(),
            created_at: chrono::Utc::now(),
            archived_at: chrono::Utc::now(),
        };
        backend.archive_raw(&raw).await.expect("archived");
        let events = RecordingEvents {
            seen: Mutex::new(Vec::new()),
        };
        let erased = redact_artifact(
            &backend,
            &events,
            "default",
            "m-1",
            "artifact://default/m-1",
            "owner",
            "asked",
        )
        .await
        .expect("erased");
        assert!(erased);
        assert!(backend.get_raw("default", "m-1").await.unwrap().is_none());
        let seen = events.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        match &seen[0] {
            cog_core::EventPayload::Redacted {
                oid,
                artifact,
                sha256,
                bytes,
                reason,
                ..
            } => {
                assert_eq!(*oid, None);
                assert_eq!(artifact.as_deref(), Some("artifact://default/m-1"));
                assert_eq!(*bytes, 6);
                assert_eq!(sha256.len(), 64, "哈希要逐字留下");
                assert_eq!(reason, "asked");
            }
            other => panic!("不是 redacted：{other:?}"),
        }
        drop(seen);
        let again = redact_artifact(
            &backend,
            &events,
            "default",
            "m-1",
            "artifact://default/m-1",
            "owner",
            "asked",
        )
        .await
        .expect("idempotent");
        assert!(!again, "再擦一次是空操作");
        assert_eq!(events.seen.lock().unwrap().len(), 1);
    }
}
