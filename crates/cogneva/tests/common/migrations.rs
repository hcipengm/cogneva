//! `crates/*/migrations/**/*.up.sql`：重放时会建表的那些。
//!
//! 两条普查读同一批文件（「库只存列」与「投影表自证有读者」），而它们先前各自抄了
//! 一份同样的走树代码。抄件之间不会打架，只会分开长：一处开始收 `mysql/`、另一处
//! 只收 `postgres/`，两份判词就关于两棵不同的树，而两份都绿。
//!
//! 判据在 `the_fixture_reading_steps_are_written_once_test`。

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use super::routes::walk;

/// 树里每一份迁移的 `.up.sql`。
///
/// 只收 `.up.sql`：`.down.sql` 描述的是回滚到旧形状，不是这棵树想去的终态。
pub fn up_migrations(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let crates = root.join("crates");
    let Ok(entries) = std::fs::read_dir(&crates) else {
        return out;
    };
    for entry in entries.flatten() {
        let migrations = entry.path().join("migrations");
        if !migrations.is_dir() {
            continue;
        }
        // 递归只该有一份（`common::fsio` 里那一份）。`walk` 按 `Path::extension` 收，
        // 而这里要的是 `*.up.sql`——扩展名是 `sql`，`.up` 是名字的一部分，所以先按
        // `sql` 收齐再按后缀过滤，而不是把 `"up.sql"` 当扩展名传进去（那样一份也收
        // 不到，空着与「全清完了」同形）。
        let mut sql = Vec::new();
        walk(&migrations, &["sql"], &mut sql);
        out.extend(sql.into_iter().filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".up.sql"))
        }));
    }
    out
}
