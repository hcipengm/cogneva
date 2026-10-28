//! 跨 crate 的夹具约定：同一个外部服务、同一条读取规则，不要在每个 crate 里各写一遍。
//!
//! 这里的每一条都曾经是散在各 crate 的 `tests/` 里的抄件。它们不是「各自的测试数据」——
//! 它们说的是同一件事：这台机器上用哪个 PostgreSQL、哪个 Redis，以及没有的时候是停还是
//! 跳过。抄件之间没有东西比较，一边改了措辞或改了「没有就停 / 没有就跳过」这条规矩，
//! 另一边不会红：两边都还是绿的，只是绿的来源不同。
//!
//! 判据在 `crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs`：
//! 这份文件里定义过的函数体，不许在任何一份夹具里再出现一遍。

/// 下面那几个入口读的名字。一条名字只在这里拼写一次。
///
/// 拼第二遍的地方不会因为拼错而红：`env::var` 读一个没人设置的名字，与「本来就没配」
/// 在调用点上完全同形，旋钮静默失效。所以名字是常量，函数只读常量。
const DATABASE_URL_ENV: &str = "COGNEVA_TEST_DATABASE_URL";
const REDIS_URL_ENV: &str = "COGNEVA_TEST_REDIS_URL";

/// 这些用例要用的那个 PostgreSQL：一次性的库，由 `COGNEVA_TEST_DATABASE_URL` 指定。
///
/// 没设就停，不回落默认地址——这些用例会建表、写行、删数据，落到一台恰好跑着
/// PostgreSQL 的机器上，比干脆不跑更糟。
pub fn database_url() -> String {
    std::env::var(DATABASE_URL_ENV).expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// 这些用例要用的那个 Redis，没设就用开发机上的默认端口。
///
/// 回落在这里是对的而落库地址那里是错的，因为两者的失败方式不同：这些用例只读写自己
/// 那一轮生成的 session id，连到一台开发机自带的 Redis 上最多是读不到别人的数据；
/// 而建表、删行的用例落到一台不是为此准备的库上，删掉的是真东西。
pub fn redis_url() -> String {
    std::env::var(REDIS_URL_ENV).unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

/// 同上，但没设就停：这些用例会删键，必须落在调用方点名的那个实例上，不能靠猜。
pub fn redis_url_required() -> String {
    std::env::var(REDIS_URL_ENV)
        .expect("set COGNEVA_TEST_REDIS_URL to a throwaway Redis before running this test")
}
