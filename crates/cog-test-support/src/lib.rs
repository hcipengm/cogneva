//! 跨 crate 的夹具约定：同一个外部服务、同一条读取规则，不要在每个 crate 里各写一遍。
//!
//! 这里的每一条都曾经是散在各 crate 的 `tests/` 里的抄件。它们不是「各自的测试数据」——
//! 它们说的是同一件事：这台机器上用哪个 PostgreSQL、哪个 Redis，以及没有的时候是停还是
//! 跳过。抄件之间没有东西比较，一边改了措辞或改了「没有就停 / 没有就跳过」这条规矩，
//! 另一边不会红：两边都还是绿的，只是绿的来源不同。
//!
//! 判据在 `crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs`：
//! 这份文件里定义过的函数体，不许在任何一份夹具里再出现一遍。

/// 这些用例要用的那个 PostgreSQL：一次性的库，由 `COGNEVA_TEST_DATABASE_URL` 指定。
///
/// 没设就停，不回落默认地址——这些用例会建表、写行、删数据，落到一台恰好跑着
/// PostgreSQL 的机器上，比干脆不跑更糟。
pub fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}
