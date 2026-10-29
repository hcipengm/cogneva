//! Archiving a full issue title as an agent id, on the raw layer the
//! deployment actually wires.
//!
//! The claim is about a pairing, not about either side alone: the ingestor
//! turns an agent end into a raw record and derives a bounded object key from
//! the agent id, and the object store it writes to is the filesystem one whose
//! paths have a length limit. With the in-memory store this passes whatever the
//! key looks like; with the filesystem store a key built from the verbatim id
//! fails with `ENAMETOOLONG` at the moment a real deployment archives an issue
//! titled like one. So the test lives here, where both crates meet, rather than
//! in either crate's own tests -- a copy of the store kept for the purpose
//! would drop exactly the constraint being measured.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::broadcast;

use cog_core::{AgentEvent, MemoryBackend, Message};

/// A real issue title, long enough that a key built from it is over the limit.
const LONG_AGENT_ID: &str = "squad:decompose-Fix gitee issue #34295240: 刷新接口未区分访问令牌和刷新令牌，访问令牌可换取新令牌";

fn agent_end(agent_id: &str) -> AgentEvent {
    AgentEvent::AgentEnd {
        agent_id: agent_id.into(),
        messages: vec![Message::user(
            "the deploy key lives in the security gateway",
        )],
        crew_id: None,
        squad_id: None,
        timestamp: Utc::now(),
    }
}

#[tokio::test]
async fn a_long_agent_id_archives_and_keeps_its_verbatim_tag() {
    let dir = tempfile::tempdir().expect("a temp dir for the raw layer");
    let composite = cog_memory::CompositeMemoryBackend::new(
        Arc::new(cog_storage::FileObjectBackend::new(dir.path())),
        Arc::new(cog_memory::NoopVectorBackend::new()),
        8,
    );
    let backend: Arc<dyn MemoryBackend> = Arc::new(composite);
    let ingestor = cog_memory::MemoryIngestor::new(
        backend.clone(),
        Arc::new(cog_memory::RuleBasedExtractor::new()),
    );
    let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
    let _handle = ingestor.spawn(event_tx.subscribe());

    event_tx
        .send(agent_end(LONG_AGENT_ID))
        .expect("the ingestor is subscribed");

    let mut archived = None;
    for _ in 0..100 {
        if let Ok(list) = backend.list_raw("default", None).await {
            if let Some(id) = list.into_iter().next() {
                archived = Some(id);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let id = archived.expect("a long agent id must still archive on the filesystem backend");
    assert!(id.len() < 128, "the archived id is not bounded: {id}");

    let raw = backend
        .get_raw("default", &id)
        .await
        .expect("the raw record is readable")
        .expect("the raw record is there");
    assert!(
        raw.tags
            .iter()
            .any(|tag| tag == &format!("agent_id:{LONG_AGENT_ID}")),
        "the verbatim agent id has to survive in the tags, or the archive cannot be \
         traced back to the agent it came from; got {:?}",
        raw.tags
    );
}
