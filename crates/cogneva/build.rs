//! build.rs — auto-generate plugin registry from Cargo.toml dependencies.
//! Scans `[dependencies]` for all `cog-*` crates (excluding `cog-core`)
//! and emits `plugin_registry_generated.rs` into `$OUT_DIR`.
//! Descriptors are topologically sorted at runtime by
//! [`cog_core::PluginRunner::from_descriptors`]; the order in the generated
//! array does not affect init order.
//! When a new first-party crate is added:
//! 1. Add its `Cargo.toml` entry.
//! 2. Ensure its `plugin.rs` exposes `pub const DESCRIPTOR`.

use std::fs;

fn main() {
    let cargo_toml = fs::read_to_string("Cargo.toml").expect("read Cargo.toml");

    let mut cog_deps = Vec::new();
    let mut in_deps = false;

    for line in cargo_toml.lines() {
        let trimmed = line.trim();
        if trimmed == "[dependencies]" {
            in_deps = true;
            continue;
        }
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_deps = false;
            continue;
        }
        if in_deps && trimmed.starts_with("cog-") {
            let crate_name = trimmed.split('=').next().unwrap().trim();
            if crate_name != "cog-core" {
                cog_deps.push(crate_name.to_string());
            }
        }
    }

    cog_deps.sort();

    let mut lines = vec![
        "/// Every first-party plugin descriptor this binary registers, derived from".to_string(),
        "/// `[dependencies]` so that adding a crate cannot leave this list stale.".to_string(),
        "pub fn all_descriptors() -> &'static [cog_core::PluginDescriptor] {".to_string(),
        "    &[".to_string(),
    ];

    // cog-* plugins (alphabetical, including cog-eval)
    for dep in &cog_deps {
        let mod_name = dep.replace('-', "_");
        lines.push(format!("        {}::plugin::DESCRIPTOR,", mod_name));
    }

    lines.push("    ]".to_string());
    lines.push("}".to_string());
    lines.push(String::new());
    lines.push(
        "/// Build a [`cog_core::PluginRunner`] from the static descriptor list.".to_string(),
    );
    lines.push("///".to_string());
    lines.push(
        "/// Descriptors are topologically sorted by [`cog_core::PluginRunner::from_descriptors`]"
            .to_string(),
    );
    lines.push("/// before the runner is returned.".to_string());
    lines.push("pub fn register_all() -> cog_core::SFResult<cog_core::PluginRunner> {".to_string());
    lines.push("    cog_core::PluginRunner::from_descriptors(all_descriptors())".to_string());
    lines.push("}".to_string());

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = std::path::Path::new(&out_dir).join("plugin_registry_generated.rs");
    fs::write(&out_path, lines.join("\n")).expect("write generated registry");

    println!("cargo:rerun-if-changed=Cargo.toml");

    // 嵌入构建来源：容器全量构建时源码树无 .git，由 Dockerfile ARG
    // GIT_REVISION 经 COGNEVA_GIT_REVISION 环境变量注入；本机构建直接查 git。
    // 线上镜像必须能回答"我跑的是哪个 commit"，浮动 tag :local 不携带该信息。
    let revision = std::env::var("COGNEVA_GIT_REVISION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(git_revision)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=COGNEVA_GIT_REVISION={revision}");
    println!("cargo:rerun-if-env-changed=COGNEVA_GIT_REVISION");
    println!("cargo:rerun-if-changed=.git/HEAD");
    // HEAD 只在切分支时动，提交动的是 refs/heads 下那个文件；少了这一行，提交
    // 之后重编译出来的印章还指着上一个 commit（实测：HEAD 已到 9a10a72，缓存
    // 的 build script 输出仍写 890e4c0）。
    println!("cargo:rerun-if-changed=.git/refs/heads");

    // Name the code, not just the release it belongs to. The declared version
    // alone is shared by every commit since the last release — measured once at
    // 93 consecutive commits — so a report of "0.5.8" cannot be told apart from
    // the released 0.5.8. git names the difference and computes it from the
    // history alone, which is why every producer reaches the same string
    // without agreeing on anything: the label is derived, not maintained.
    // Injected by whoever builds into a tree without .git, exactly as the
    // revision is; the fallback says the distance could not be read rather than
    // claiming zero.
    let version_id = std::env::var("COGNEVA_VERSION_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(git_version_id)
        .unwrap_or_else(|| format!("v{}-unknown", env!("CARGO_PKG_VERSION")));
    println!("cargo:rustc-env=COGNEVA_VERSION_ID={version_id}");
    println!("cargo:rerun-if-env-changed=COGNEVA_VERSION_ID");
    println!("cargo:rerun-if-changed=.git/refs/tags");
    println!("cargo:rerun-if-changed=.git/packed-refs");
}

/// `git describe` output for HEAD, or `None` when git or the tags are missing.
///
/// `--long` keeps the distance in the output even at the release commit, so the
/// shape never varies and a caller never has to guess whether a missing
/// distance means "zero" or "unknown". `--match` keeps `promote/*` and `gen-*`
/// tags, which the cluster writes, from ever being taken for releases.
fn git_version_id() -> Option<String> {
    let output = std::process::Command::new("git")
        .args([
            "describe", "--tags", "--long", "--dirty", "--match", "v[0-9]*",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

fn git_revision() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let mut rev = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .map(|o| o.status.success() && !o.stdout.is_empty())
        .unwrap_or(false);
    if dirty {
        rev.push_str("-dirty");
    }
    Some(rev)
}
