//! 示例 4：SRE 自愈 — 沙盒边界探测 + patch 安全校验。
//! 运行：`cargo run -p cogneva --example sre_self_heal`

use cog_reflection::sandbox::{detect_sandbox, enforce_sandbox_boundary, SandboxSignals};
use cog_reflection::PatchPipeline;

fn main() {
    // 1. 探测当前隔离环境
    let signals = SandboxSignals::from_environment();
    match detect_sandbox(&signals) {
        Some(kind) => println!("sandbox detected: {kind}"),
        None => println!("no sandbox detected"),
    }

    // 2. 边界强制：未隔离 + 无显式声明时自动降级 dry-run
    let config = cog_core::SelfEvolutionConfig {
        enabled: true,
        auto_apply: true,
        auto_deploy: true,
        ..Default::default()
    };
    let (effective, decision) = enforce_sandbox_boundary(&config, &signals);
    println!("boundary decision: {decision:?}");
    println!(
        "effective auto_apply={} auto_deploy={}",
        effective.auto_apply, effective.auto_deploy
    );

    // 3. 自愈 patch 的安全校验：路径必须在 workspace 内且不触碰受保护文件
    let patch = "diff --git a/src/lib.rs b/src/lib.rs\n--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n";
    let files = PatchPipeline::parse_patch(patch).expect("valid unified diff");
    println!("patch affects: {files:?}");

    let evil = "diff --git a/../../etc/passwd b/../../etc/passwd\n--- a/../../etc/passwd\n+++ b/../../etc/passwd\n@@ -1 +1 @@\n-a\n+b\n";
    match PatchPipeline::parse_patch(evil) {
        Ok(files) => {
            let root = std::env::temp_dir();
            match PatchPipeline::validate_patch_files(&files, &root) {
                Ok(()) => println!("unexpected: escape patch accepted"),
                Err(e) => println!("escape patch rejected: {e}"),
            }
        }
        Err(e) => println!("malformed patch rejected: {e}"),
    }
}
