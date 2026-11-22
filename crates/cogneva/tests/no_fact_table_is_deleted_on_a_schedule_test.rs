//! principle: feedback_a_bound_whose_only_raise_me_signal_is_pass_failure_never_asks_to_be_raised
//!
//! 事实面不许有「到点删」。
//!
//! 一条事实（一条事件、一句对话、一条通知、一次尝试的日志）在库里只有一份；按时间
//! 或按行窗删掉它，那一刻之后没有任何东西能把它拿回来，而删表这件事从外面看与「表
//! 一直很小」完全同形——没有任何读者会因此报警。所以这条判据不判「该不该删」，只判
//! **删之前有没有一份读得回来的副本**：
//!
//! - 名单是 [`cog_storage::table_shapes::FACT_TABLES`]：一行就是那段事实的唯一一份。
//! - 写 `DELETE FROM <那张表>` 的那个函数，必须在同一个函数体里做完整的三步：`put`
//!   （写副本）、`get`（读回来）、逐字节比对（`!= bytes` 那一句）。少任何一步，这一批
//!   就不许删，而这个测试红。
//!
//! 这条判据不认注释、不认提交信息：它读的是同一个函数里有没有那三个动作的顺序。
//! 一条注释可以撒谎，一段代码要做完三步才不红。

use std::path::PathBuf;

use crate::common::rust_production::rust_files_under;

/// 从 `delete_at` 往前找它所在的函数，返回那个函数的函数体（大括号配对到结尾）。
fn enclosing_body(text: &str, delete_at: usize) -> Option<&str> {
    let before = &text[..delete_at];
    let fn_at = before.rfind("fn ")?;
    let open = fn_at + text[fn_at..].find('{')?;
    let mut depth = 0usize;
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[open..open + offset + 1]);
                }
            }
            _ => {}
        }
    }
    None
}

/// 一个函数体是否做完了「写副本 → 读回来 → 逐字节比」这三步。
fn archives_and_verifies(body: &str) -> bool {
    body.contains(".put(")
        && body.contains(".get(")
        && (body.contains("!= bytes") || body.contains("!= &bytes"))
}

#[test]
fn every_fact_delete_sits_beside_the_archive_that_verifies_it() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/cogneva is two levels under the workspace root")
        .to_path_buf();
    let src = crate_dir.join("crates/cog-storage/src");

    let mut offenders = Vec::new();
    for file in rust_files_under(&src) {
        let text = std::fs::read_to_string(&file).unwrap_or_default();
        // `#[cfg(test)]` 之后的部分不算：那里的 DELETE 是夹具，删的是它自己刚写的行。
        // 产出与夹具的切法只有一份（`common::rust_production`）：在第一个
        // `#[cfg(test)]` 上切，会在文件中间带着 test-only 项时把后面的产出整段丢掉。
        let production = crate::common::rust_production::production_text(&text);
        let production = production.as_str();
        for table in cog_storage::table_shapes::FACT_TABLES {
            let needle = format!("DELETE FROM {table}");
            let mut search = 0;
            while let Some(rel) = production[search..].find(&needle) {
                let at = search + rel;
                search = at + needle.len();
                let Some(body) = enclosing_body(production, at) else {
                    offenders.push(format!(
                        "{}：`DELETE FROM {table}` 不在任何函数体里，判不出它有没有先归档",
                        file.display()
                    ));
                    continue;
                };
                if !archives_and_verifies(body) {
                    offenders.push(format!(
                        "{}：`DELETE FROM {table}` 所在的函数没有「写副本 → 读回来 → 逐字节比」",
                        file.display()
                    ));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "事实面有到点删（删之前没有一份读得回来的副本）：\n  {}",
        offenders.join("\n  ")
    );
}

/// 名单本身也得像个名单：空名单让上面那条判据永远绿，而那正是它要防的形状。
#[test]
fn the_fact_table_list_is_not_empty() {
    assert!(
        cog_storage::table_shapes::FACT_TABLES.len() >= 5,
        "事实表名单短得可疑：{:?}",
        cog_storage::table_shapes::FACT_TABLES
    );
}
