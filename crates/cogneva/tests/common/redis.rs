//! 测试怎么找到 Redis，以及连不上时读成「不适用」而不是失败。
//!
//! 地址本身（读哪个环境变量、没设时回落到哪个端口）不在这里：那是跨 crate 的夹具
//! 约定，在 `cog_test_support` 里只有一份。
//!
//! 两套集成测试各抄了一遍这段：环境变量拿地址、回落默认端口、连不上就 `eprintln!`
//! 跳过。抄件的分歧不会红——一套跳过、另一套往下跑并失败，而失败的那一条说的是
//! 「这台机器上没起 Redis」，不是「这次改动有问题」。所以「跳过」这件事本身也放在
//! 这里，各文件只留一个入口去调它。
//!
//! 地址是参数的那一份单独留着：跳过规则要能在不改环境变量的前提下被检查，而并行的
//! 其他用例读的是同一份环境。

// 这一份按「每个测试二进制各判一次」编译：只有会连 Redis 的那几个用它，其余的把
// 它编进来却一条都不调。去掉这个 allow 等于要求每份夹具都写一遍自己的 redis 读取。
#![allow(dead_code)]

/// 连上配额管理器要用的那个 store，或者在没有可达实例时返回 `None` 让用例跳过。
pub async fn open() -> Option<redis::aio::ConnectionManager> {
    open_at(&cog_test_support::redis_url()).await
}

/// 同上，但地址由调用方给。
///
/// 地址不可解析与连不上都读成跳过：这两种在「这台机器上没有可用的 Redis」上是同一
/// 件事，而用例要判的是被测代码，不是环境。
pub async fn open_at(redis_url: &str) -> Option<redis::aio::ConnectionManager> {
    let client = match redis::Client::open(redis_url) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("SKIP: unusable Redis url ({e})");
            return None;
        }
    };
    match cog_storage::redis::connect(&client).await {
        Ok(conn) => Some(conn),
        Err(e) => {
            eprintln!("SKIP: no Redis reachable ({e})");
            None
        }
    }
}

/// The sandbox has no Redis. A connection failure there has to read as "not
/// applicable" rather than as a broken quota store, or the verdict says which
/// services happened to be up instead of whether the change is sound.
pub async fn assert_unreachable_skips() {
    let conn = open_at("redis://127.0.0.1:1").await;
    assert!(
        conn.is_none(),
        "an unreachable Redis must skip, not connect"
    );
}
