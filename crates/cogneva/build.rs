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
        "/// Build a [`cog_core::PluginRunner`] from the static descriptor list.".to_string(),
        "///".to_string(),
        "/// Descriptors are topologically sorted by [`cog_core::PluginRunner::from_descriptors`]"
            .to_string(),
        "/// before the runner is returned.".to_string(),
        "pub fn register_all() -> cog_core::SFResult<cog_core::PluginRunner> {".to_string(),
        "    let descriptors: &[cog_core::PluginDescriptor] = &[".to_string(),
    ];

    // cog-* plugins (alphabetical, including cog-eval)
    for dep in &cog_deps {
        let mod_name = dep.replace('-', "_");
        lines.push(format!("        {}::plugin::DESCRIPTOR,", mod_name));
    }

    lines.push("    ];".to_string());
    lines.push("    cog_core::PluginRunner::from_descriptors(descriptors)".to_string());
    lines.push("}".to_string());

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = std::path::Path::new(&out_dir).join("plugin_registry_generated.rs");
    fs::write(&out_path, lines.join("\n")).expect("write generated registry");

    println!("cargo:rerun-if-changed=Cargo.toml");
}
