//! 界面刷新一轮的周期，后端与界面是同一个数。
//!
//! 写入延迟的判词拿 p99 与它的一半比：这个数由界面自己定（轮询间隔），后端读不到
//! 浏览器里的值，只能按协议常量收在 `cog_core::contract::conversation::UI_REFRESH_MS`。
//! 两边各写一个数时，界面改了周期而判词的线不动——读数仍然「有人看」，只是它在拿
//! 一个旧周期判今天的延迟。
//!
//! 没有这条门：后端常量与界面源码里的默认值可以各自漂移而没有任何东西红。

use crate::common::{fsio, root};

#[test]
fn the_backend_constant_and_the_web_default_are_the_same_number() {
    let hook = fsio::read(&root::repo_root().join("web/src/hooks/useResource.ts"));
    assert!(
        hook.contains("pollMs = 30_000"),
        "界面资源轮询的默认周期变了：先确认判词那条线该跟着改，再回来改这里的断言"
    );
    assert_eq!(
        cog_core::contract::conversation::UI_REFRESH_MS,
        30_000,
        "后端判词用的界面周期与界面源码里的默认值不是同一个数"
    );
}
