//! 经 A2A 提上来的活要读得到「谁提的」：两条受理路径建 goal 行时都把归属写进行里。
//!
//! 归属取的是认证中间件已经验过的那一份 (`Claims`)，不是在这里再读一次请求头。
//! 这条门判的是**接线**：把 `input["origin"]` 那一行删掉、或者让处理器不再取
//! `Claims`，单元用例照样绿——它们测的是拼装函数，不测谁调它。

#[test]
fn both_a2a_goal_rows_carry_who_asked() {
    let root = crate::common::routes::repo_root();
    let path = root.join("crates/cog-gateway/src/a2a.rs");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读不到 {}：{e}", path.display()));
    // 只看生产代码：测试里也建 goal 行，它们不该被这条门要求带归属。
    let production = text.split("mod tests {").next().unwrap_or(&text);

    let mut sites = 0;
    for (idx, _) in production.match_indices("Task::goal_row(") {
        sites += 1;
        let window = &production[idx..(idx + 700).min(production.len())];
        assert!(
            window.contains("input[\"origin\"]"),
            "这一处 goal 行没有写归属：{}",
            window.lines().next().unwrap_or("")
        );
        assert!(
            window.contains("record(state.intent_events.as_ref("),
            "这一处 goal 行没有先落一条意图行（统一写入口 `intent::record`）：三种来源要进同一本账"
        );
    }
    assert_eq!(sites, 2, "A2A 的生产路径应当有两处 goal 行构造（流与 send），实际 {sites}");
    assert_eq!(
        production.matches("Option<Extension<Claims>>").count(),
        4,
        "两个处理器与它们各自转交的那一层都要取认证主体（`Option<Extension<Claims>>`）"
    );
}
