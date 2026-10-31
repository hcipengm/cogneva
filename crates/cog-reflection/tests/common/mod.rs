// 每个集成测试文件各自编一次这份夹具，只用得到其中几个的文件不该为此报警。
#![allow(dead_code)]

//! Running git against a scratch tree, and failing the case when git does.
//!
//! The two suites here both judge a git state, so both had to run git the same
//! way. A helper that swallowed a non-zero exit would have let one of them read
//! a tree it never reached.

use std::path::{Path, PathBuf};
use std::process::Command;

pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

// The repo root and the production census are the workspace copies, mounted by
// path the same way the gates that use them mounted them; what is shared here
// is the cut that turns the census into this crate's shipping sources. Two
// gates make that cut -- the one that judges the order of a cheap check and an
// expensive gate, and the one that judges the order of a write and its
// recurrence bump -- and a second copy of it stops matching the day the census
// or the prefix changes, which reads exactly like a tree where nothing is wrong.
#[path = "../../../cogneva/tests/common/root.rs"]
mod root;

#[path = "../../../cogneva/tests/common/rust_production.rs"]
pub mod rust_production;

use root::repo_root;
use rust_production::{production_sources, relative_name};

/// This crate's shipping sources: the shared census, cut down to the crate.
pub fn source_files() -> Vec<PathBuf> {
    let root = repo_root();
    let prefix = format!(
        "{}/",
        relative_name(&root, &root.join("crates/cog-reflection/src"))
    );
    production_sources(&root)
        .into_iter()
        .filter(|path| relative_name(&root, path).starts_with(&prefix))
        .collect()
}
