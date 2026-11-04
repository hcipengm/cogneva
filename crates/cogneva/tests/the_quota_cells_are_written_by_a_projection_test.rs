//! 额度格子的唯一写者是投影循环，而那条循环必须在进程启动时真的被拉起来。
//!
//! 两件事分开都无声，合起来才是这条额度面的全部：
//!
//! **一、格子里那个数只能从账本重算。** 请求路径上原先有两处自增
//! （`charge_owner_budget` 记人、`charge_task_budget` 记任务）。它们与账本各记一份，
//! Redis 丢一个键、或某条账本行写失败之后就开始分叉，而分叉之后没有任何一条读数
//! 比得出这个差——上限拦的是格子里的数，账上记的是真花销，两者谁对没人知道。
//! 两处自增已删，格子改由 `project_quota_usage` 按拍覆盖。这一条判的就是它们没有
//! 回来：名字再出现，就是第二条写路径又接上了。
//!
//! **二、投影循环必须被启动。** 删掉 `run()` 里那一行，编译照样通过，函数还定义在
//! 那里（任何「这个函数存在吗」的检查都绿），而格子从此再不更新：Redis 重启之后
//! 所有人的额度一直停在 0，谁都不会被拦。这是这条面上唯一一个「删一行、全绿、且
//! 后果只在生产里」的位置，所以它单独由一条门盯着。
//!
//! 判不了什么：这条门按形判，不按语义判。有人换一个名字、另写一条自增路径，它拦
//! 不住——那一半由契约挡：`HierarchyManager` 与 `TaskBudgetGate` 上已经没有增量写
//! 的入口，想加一条必须先改 trait，而改 trait 要人自己写下来。

use crate::common;

use common::routes::read_repo;
use common::rust_production::production_text;

/// 起投影循环的那个文件。
const GATEWAY: &str = "crates/cog-gateway/src/security_gateway.rs";

/// 请求路径上曾经的两条自增入口。
///
/// 拼出来而不是写成字面量：本文件也在生产代码扫描的范围里，写死会让它自己成为
/// 第二处出现。
const CHARGES: [&str; 2] = [
    concat!("charge_owner", "_budget"),
    concat!("charge_task", "_budget"),
];

#[test]
fn the_quota_cells_are_written_by_the_projection_loop_and_that_loop_is_started() {
    let source = read_repo(GATEWAY);
    let source = production_text(&source);

    assert!(
        source.len() > 10_000,
        "只读到 {} 字节的 {}：没读到文件与「里面没有第二条写路径」是两件事，前者不该读成后者",
        source.len(),
        GATEWAY
    );

    for charge in CHARGES {
        assert!(
            !source.contains(charge),
            "{} 又出现了：请求路径上再记一笔，就是与账本并行的第二本账。格子的值是账本\
             的投影，由 project_quota_usage 覆盖写，请求路径只落账本那一行",
            charge
        );
    }

    // 定义本身也含这个名字，所以判的是带 `drop(` 的那一行——起一个不回收句柄的
    // 后台循环，与这一面其他几条同一形状。
    assert!(
        source.contains(concat!("drop(spawn_quota", "_projection(")),
        "{} 里找不到启动投影循环的那一行：函数还在、编译还过，可格子从此不再更新，\
         而这件事只会在生产里、在 Redis 丢键之后显形",
        GATEWAY
    );
}
