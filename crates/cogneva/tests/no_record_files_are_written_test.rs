//! 记录只以结构化形式存在于库与流里：磁盘上不许再出现「拼出来的记录文件」。
//!
//! 判据是形状：生产代码里 `fs::write`／`File::create` 的目标带 `.json`／`.md` 的，
//! 一律红；安装器（`crates/bootstrap`）写的是机器的配置文件，不是平台的记录，单列。

use crate::common::routes::repo_root;
use crate::common::rust_production::{production_text, production_texts};

/// 这些文件写的不是平台记录：
/// - `crates/bootstrap/`：机器的配置文件（k3s、apt、inventory），安装器的产物；
/// - `crates/cog-memory/src/schema_backend.rs`：记忆子系统自己的数据文件（`schema.json`），
///   是那个子系统的存储，不是「用标题/摘要/状态拼出来的记录」。
const NOT_PLATFORM_RECORDS: &[&str] = &[
    "crates/bootstrap/src/",
    "crates/cog-memory/src/schema_backend.rs",
];

#[test]
fn no_production_path_writes_a_record_file() {
    let root = repo_root();
    let mut offenders = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if NOT_PLATFORM_RECORDS
            .iter()
            .any(|allowed| relative.starts_with(allowed))
        {
            continue;
        }
        let production = production_text(&text);
        let lines: Vec<&str> = production.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            let writes = line.contains("fs::write") || line.contains("File::create");
            if !writes {
                continue;
            }
            let window = lines[index..(index + 3).min(lines.len())].join(" ");
            if window.contains(".json") || window.contains(".md") {
                offenders.push(format!("{relative}:{}: {}", index + 1, line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "这些生产路径在写记录文件——记录要进库或进流，磁盘上的那一份既不能查也不跟着读数走：\n{}",
        offenders.join("\n")
    );
}
