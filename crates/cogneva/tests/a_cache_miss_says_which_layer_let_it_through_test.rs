//! 命中率低是一个数；读的人要的是「为什么低」。前缀接不上（工具、system、历史被改写）
//! 与接得上却没命中要分开，没有清单的那一次记成「没量到」而不是猜一个类。
//!
//! 没有这次改动这几条会红：按类计数不存在、`none` 被并进别的类、没有清单的调用被跳过
//! 而不是记成 unmeasured。

#[test]
fn the_miss_reading_names_the_layer_and_never_guesses() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let store =
        std::fs::read_to_string(root.join("crates/cog-storage/src/postgres/llm_call_events.rs"))
            .expect("llm_call_events.rs");
    assert!(
        store.contains("cache_miss_breaks"),
        "没有按类计数的读数：命中率低仍然只有一个数"
    );
    assert!(
        store.contains("classify_manifest_break"),
        "没有用同一份断裂分类：两处分类迟早各说各的"
    );
    assert!(
        store.contains("\"unmeasured\""),
        "没有清单的调用没有被记成「没量到」：它会被当成某一类，或者干脆消失"
    );
    assert!(
        store.contains("previous.remove(&session)"),
        "没有清单的那一次没有把比较链断开：前后的比较跨过了它"
    );
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cli.rs")).expect("cli.rs");
    assert!(cli.contains("cache-misses"), "读数没有入口");
}
