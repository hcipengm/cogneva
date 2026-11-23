//! `cogneva batch-plan`：把账上那一批汇成一张计划的那一步，落到命令行上。
//!
//! 为什么要有这个入口：`LedgerBatchPlanner` 一直只有 `pub use`，`crates/` 下没有一处
//! 调用它——平台上写着「批怎么成一张计划」，真正在算批次的却是带外的 `round.py`。
//! 两份实现各自演进，而读的人分不出哪一份说了算。
//!
//! 输入是标准输入上的一行 JSON 数组，每一项是一条待办（`id`／`title`／`touches`／
//! `stage`）；输出是计划本身（`Plan` 的 JSON）。计划里的验收形状由 planner 固定：
//! 工作树与闸门，量法分别是 `family.py --all-todos` 的退出码与 `check-before-push.sh`
//! 跑绿——这两条与带外那道门量的是同一件事。
//!
//! 读不到输入、或计划本身不成立，就带着判词退非零：一个「什么也没说」的退出码会让
//! 调用方把「没算」读成「算过、这一批成立」。

use std::io::Read;

use cog_core::BatchPlanner;

/// 从标准输入读一批待办，打印计划。
pub async fn run_from_stdin() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let items: Vec<cog_core::BatchItem> = serde_json::from_str(&input)
        .map_err(|e| format!("这一批读不出来（要 JSON 数组，每项 id/title/touches）：{e}"))?;
    let plan = crate::action_planner::LedgerBatchPlanner::new()
        .plan_batch(&items)
        .await?;
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}
