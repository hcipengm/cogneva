//! 一条变更要说得清「哪件活要的它」，否则落地率只能给分段总数。
//!
//! 三处必须同时成立，缺一处这条链就断在中间：
//! - 候选自己的载荷里带 `unit_of_work`（写者与目录逐字一致）；
//! - 写候选事件的那一步把演化结果上的这一格带下来；
//! - 有一条按它读的查询（`attributes->>'unit_of_work'`），而不是按进程级的 `scope`
//!   或信封里那条变更自己的 id。
//!
//! 没有这次改动这条会红：`ChangeCandidate` 没有 `unit_of_work`，也没有按它读的口。

use cog_core::contract::reflection::{candidate_attributes, ChangeCandidate};

fn candidate(unit: Option<&str>) -> ChangeCandidate {
    ChangeCandidate {
        artifact_id: "change-1".into(),
        kind: "code_change".into(),
        description: "d".into(),
        content: "x".into(),
        created_at: chrono::Utc::now(),
        eval_summary: None,
        tiering: None,
        unit_of_work: unit.map(str::to_string),
        recorded_status: "awaiting_review".into(),
        status: "candidate".into(),
        decision_reason: None,
        event_id: String::new(),
    }
}

#[test]
fn the_candidate_payload_carries_the_goal_and_the_catalog_names_it() {
    let with_goal = candidate_attributes(&candidate(Some("goal-9")));
    assert_eq!(
        with_goal["unit_of_work"], "goal-9",
        "候选载荷里没有它要回答的那件事"
    );
    let without = candidate_attributes(&candidate(None));
    assert!(
        without["unit_of_work"].is_null(),
        "没有目标来源时必须是 null，而不是一个空串凑出来的身份"
    );

    let fields =
        cog_core::contract::event_payload::payload_fields_for("change_candidate", "generated", 1)
            .expect("目录里没有 change_candidate");
    assert!(
        fields.iter().any(|(name, _)| *name == "unit_of_work"),
        "目录没有点名这一格：写者写出去而目录不认，读的人按名字取不到"
    );
}

#[test]
fn the_writer_and_the_reader_use_the_same_identity() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let pipeline =
        std::fs::read_to_string(root.join("crates/cog-reflection/src/change_pipeline.rs"))
            .expect("change_pipeline.rs");
    assert!(
        pipeline.contains("unit_of_work: result.unit_of_work.clone()"),
        "写候选事件的那一步没有把演化结果上的目标带下来"
    );
    let store = std::fs::read_to_string(
        root.join("crates/cog-storage/src/postgres/change_candidate_store.rs"),
    )
    .expect("change_candidate_store.rs");
    assert!(
        store.contains("attributes->>'unit_of_work' = $3"),
        "没有一条按目标读的查询：落地率只能给分段总数"
    );
    assert!(
        store.contains("pub async fn unit_candidates"),
        "按目标读的口没有导出成函数"
    );
    // 「目标 → 变更 → 落地没有」要有一条把两半拼起来的读法，而不是让调用方
    // 各自去拼：拼法一多，对落地账的第二次折法就会与第一次漂开。
    assert!(
        store.contains("pub async fn unit_landing_chain")
            && store.contains("landing_states::landing_states(pool)")
            && store.contains("unit_candidates(pool, kinds, unit_of_work)"),
        "没有把候选与落地账拼成一条链的读法"
    );
}
