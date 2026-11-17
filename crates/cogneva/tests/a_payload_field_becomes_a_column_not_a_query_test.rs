//! 事件载荷存 text；要按里面的某个字段**筛行**，就把它升成一列，不在查询里 cast。
//!
//! 为什么要这条门：载荷存 text 是为了逐字节保留（内容寻址与「擦除后仍能证明原字节」
//! 都靠这份字节），代价是「按载荷里的键筛」写起来是 `attributes::jsonb ->> 'x'`——
//! 那条路能跑，但没有索引，且它一出现就说明「这个字段已经变成查询面了」。查询面一旦
//! 出现，正确的动作是在迁移里加一个**生成列**（`GENERATED ALWAYS AS (...) STORED`，
//! 自带索引、不会与载荷漂移），而不是让查询去扫全表解析。
//!
//! 所以生产 Rust 代码里不许出现 `attributes::jsonb`：要筛，就加列。迁移文件不受这条
//! 约束——生成列的定义本来在那里。

use std::path::Path;

use crate::common;

use common::rust_production::production_sources;

#[test]
fn no_production_query_reads_into_the_payload() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the workspace root")
        .to_path_buf();
    let mut offenders = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (number, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if line.contains("attributes::jsonb") {
                offenders.push(format!("{}:{}", path.display(), number + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these read into the event payload with a cast: {offenders:#?}\n\
         A field that is filtered on is a query surface: promote it with a generated \
         column in a migration (`GENERATED ALWAYS AS (attributes::jsonb ->> 'x') STORED`), \
         so the filter is indexed and cannot drift from the payload."
    );
}
