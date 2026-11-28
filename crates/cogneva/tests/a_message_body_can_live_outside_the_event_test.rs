//! 消息正文可以外置：事件只留存放处的引用，字节在 `artifact://` 那一份里。
//!
//! 三段各判一次——外置时不带正文的事件**必须**靠引用区分（否则两条不同的外置
//! 消息撞在空串上）、写者存不下时退回内联、投影按引用回读。缺任何一段，外置就是
//! 一条会把正文读成空串的改法。
//!
//! 没有这次改动这条会红：`MessageSent` 没有 `artifact`，写者与投影都不认这条通道。

use crate::common::routes::repo_root;

fn message(artifact: Option<&str>) -> cog_core::EventPayload {
    cog_core::EventPayload::MessageSent {
        from: "employee:1".into(),
        to: "employee:2".into(),
        kind: "answer".into(),
        text: String::new(),
        session: None,
        causation: None,
        artifact: artifact.map(str::to_string),
    }
}

#[test]
fn two_externalised_messages_do_not_share_an_idempotency_key() {
    let a = message(Some("artifact://default/a")).envelope("e-1", "writer");
    let b = message(Some("artifact://default/b")).envelope("e-2", "writer");
    assert_eq!(a.attributes.contains("artifact://default/a"), true);
    assert!(
        a.idempotency_key != b.idempotency_key,
        "正文外置时幂等键仍拿空串算：两条不同的消息会撞成一条"
    );
    let inline = cog_core::EventPayload::MessageSent {
        from: "employee:1".into(),
        to: "employee:2".into(),
        kind: "answer".into(),
        text: "改好了".into(),
        session: None,
        causation: None,
        artifact: None,
    }
    .envelope("e-3", "writer");
    assert!(inline.idempotency_key != a.idempotency_key);
}

#[test]
fn the_writer_archives_first_and_falls_back_inline() {
    let source = std::fs::read_to_string(repo_root().join("crates/cog-agent/src/tools.rs"))
        .expect("tools.rs");
    assert!(
        source.contains("crate::archive::post_raw(")
            && source.contains("message body could not be archived"),
        "写者没有先外置、存不下时退回内联"
    );
}

#[test]
fn the_projection_reads_the_body_back_from_its_artifact() {
    let source = std::fs::read_to_string(
        repo_root().join("crates/cog-storage/src/postgres/chat_turn_rebuild.rs"),
    )
    .expect("chat_turn_rebuild.rs");
    assert!(
        source.contains("attributes.get(\"artifact\")") && source.contains("artifacts.read(uri)"),
        "投影没有按引用回读：外置的消息在会话里会读成空"
    );
}
