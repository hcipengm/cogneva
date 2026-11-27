//! 沙盒只跑输入变了的格：宿主脚本在 `cargo test --workspace` 上按 cell plan 收窄，
//! 而「哪些格」由 `cogneva cell-plan` 算（依赖方向也要算进去：改一个被依赖的包，
//! 依赖它的包同样要重跑）。
//!
//! 没有这次改动这几条会红：`cell-plan` 不存在、垫片不看它、读不出来时不是回落全量。

use cog_reflection::cell_reuse::plan;

#[test]
fn the_cell_plan_reuses_unchanged_inputs_and_reruns_the_rest() {
    let cells = vec![
        "cog-core:suite".to_string(),
        "cog-storage:suite".to_string(),
        "cogneva:suite".to_string(),
    ];
    let decided = plan(&cells, &["cog-core".to_string()]);
    assert_eq!(decided.rerun, vec!["cog-core:suite"]);
    assert_eq!(decided.reused.len(), 2);
}

#[test]
fn the_shim_asks_the_plan_and_falls_back_to_the_full_run() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let shim = std::fs::read_to_string(root.join("scripts/hooks/cargo")).expect("cargo shim");
    assert!(
        shim.contains("cell-plan --changed-paths-from -"),
        "垫片没有问这一批要跑哪些格"
    );
    assert!(
        shim.contains("COGNEVA_CELL_PLAN"),
        "收窄没有开关：一处算错就没有退回去的路"
    );
    assert!(
        shim.trim_end().ends_with("exec \"$real_cargo\" \"$@\""),
        "垫片的最后一条不是原样执行：读不出来时回不到全量"
    );
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cell_plan.rs"))
        .expect("cell_plan.rs");
    assert!(
        cli.contains("dependents_closure"),
        "只按自己的目录判：改一个被依赖的包会跳过依赖它的那些格"
    );
    assert!(
        cli.contains("workspace_wide"),
        "认不出归属的路径没有按「全都变了」兜底：那一类改动会跳过全部格"
    );
}
