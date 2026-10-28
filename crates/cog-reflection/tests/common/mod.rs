// 每个集成测试文件各自编一次这份夹具，只用得到其中几个的文件不该为此报警。
#![allow(dead_code)]

//! Running git against a scratch tree, and failing the case when git does.
//!
//! The two suites here both judge a git state, so both had to run git the same
//! way. A helper that swallowed a non-zero exit would have let one of them read
//! a tree it never reached.

use std::path::Path;
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
