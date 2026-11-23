//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//!
//! 把账上那一批未完成的活写成一张计划：这一批的「这一类」是什么、必须成立什么、怎么量。
//!
//! 与从目标往下拆的分工：拆解是人类的管理动作——执行者上下文有限、必须交接；智能体装
//! 得下整批，成本在**验证**（一次编译、一次闸门），所以拆得越碎，同一份验证付的次数
//! 越多。这一支入口因此不做分解：它把已经存在的活汇成一批，并写下这一批共同的定义。
//!
//! 计划不由这一支宣布合格：`Plan::validate` 是固定程序，判据是「性质非空、至少两种
//! 不同来源形状的验收（或写明为什么只有一种）、每条验收都带一条可执行的量法」。写不
//! 出两种形状，说明这一批实际上只有一种证据在管它——那是一条要报出来的缺口，不是一
//! 个可以糊过去的格式问题。
//!
//! **验收恰好两种形状，而且都不是「人看一眼」**：
//!
//! - **工作树**：这一批每一条的触点在树上都改到了。量法是 `family.py --all-todos`，
//!   它退出码为零就是「缺口清空」。
//! - **闸门**：这一批整体过一次 `check-before-push.sh`。量法是那条命令跑绿并记账。

use async_trait::async_trait;
use cog_core::types::action_plan::{Plan, PlanAcceptance};
use cog_core::{BatchItem, BatchPlanner, SFResult};

/// 账上那一批的写法。没有状态、没有时钟：它的全部输入就是这一批本身。
#[derive(Debug, Default, Clone, Copy)]
pub struct LedgerBatchPlanner;

impl LedgerBatchPlanner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl BatchPlanner for LedgerBatchPlanner {
    async fn plan_batch(&self, items: &[BatchItem]) -> SFResult<Plan> {
        // 一条也没有时不编一份空计划：空计划能通过 `validate`（只要填上两种形状的
        // 验收），而它说的「这一类」是空的——那正是「没有计划的实现」换了个样子。
        if items.is_empty() {
            return Ok(Plan::default());
        }
        let mut invariants: Vec<String> = items
            .iter()
            .map(|item| {
                format!(
                    "{}：{}（触点 {} 处，这一批里都要动到）",
                    item.id,
                    item.title,
                    item.touches.len()
                )
            })
            .collect();
        // 这一批的共同性质写在最后：上面每一条说的是「它自己成立」，这一条说的是
        // 「它们在同一份验证下一起成立」——拆开做与一起做唯一的区别就在这一句。
        invariants.push(format!(
            "这一批的 {} 条在同一份验证下一起成立：一次编辑、一次编译、一次闸门",
            items.len()
        ));
        // 终态是**三格齐**，不是「文件改过」。计划里写出来，是因为这是这一批做到什么
        // 程度才算完的唯一一句话：缺验证格的那几条到了推上去、跑起来为止都还不是
        // 「成了」，而这句话不说，一份全部「改过」的批次就会读作全部做完。
        let terminal = items.iter().filter(|item| item.is_terminal()).count();
        // 「写了已落、差距却还在」：账上最坏的一种形状——读作做完了，实际没到。
        let unverified = items
            .iter()
            .filter(|item| {
                item.item.state == cog_core::contract::ledger_item::LedgerState::Reached
                    && !item.item.gap_is_empty()
            })
            .count();
        invariants.push(format!(
            "这一批里到终态（差距清空）{terminal} 条，没到的 {} 条；其中写着已落却还留着差距的 {unverified} 条——\
             它们读作做完了、实际没到，不是终态",
            items.len() - terminal
        ));

        Ok(Plan {
            input_type: "ledger-batch".to_string(),
            invariants,
            acceptance: vec![
                PlanAcceptance {
                    shape: "工作树".to_string(),
                    statement: "这一批每一条的触点在树上都改到了，缺口为空".to_string(),
                    check: "python3 scripts/hooks/family.py --root . --all-todos".to_string(),
                },
                PlanAcceptance {
                    shape: "闸门".to_string(),
                    statement: "这一批整体过一次推送前的门，它跑绿并留下记账".to_string(),
                    check: "bash deploy/scripts/check-before-push.sh".to_string(),
                },
                PlanAcceptance {
                    shape: "终态".to_string(),
                    statement: "账上再没有一行写着 `已落` 却缺三格，而且这一轮的那几条三格齐"
                        .to_string(),
                    check: "python3 scripts/hooks/family.py --root . --terminal".to_string(),
                },
            ],
            counterexamples: vec![
                "只改了其中几条就编译：缺口还在，`family.py` 退出码非零".to_string(),
                "逐条各编译一次：同一份验证付了多次，而批次的意义正是只付一次".to_string(),
                "把没做的几条从台账上改成已落以消掉缺口".to_string(),
            ],
            one_shape_because: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, title: &str) -> BatchItem {
        BatchItem {
            id: id.to_string(),
            title: title.to_string(),
            touches: vec![format!("crates/{id}/src/lib.rs")],
            item: Default::default(),
        }
    }

    /// 写出来的计划必须过固定程序的判：这一支不自己宣布合格。
    #[tokio::test]
    async fn the_batch_plan_passes_the_fixed_validator() {
        let plan = LedgerBatchPlanner::new()
            .plan_batch(&[item("a", "把 A 做完"), item("b", "再把 B 做完")])
            .await
            .expect("planning a batch is not fallible");
        assert_eq!(plan.validate(), Ok(()), "计划过不了 validate：{plan:?}");
    }

    /// 每一条活都要在性质里点到名：漏一条，这份计划说的就不是「这一批」。
    #[tokio::test]
    async fn every_item_is_named_in_the_invariants() {
        let plan = LedgerBatchPlanner::new()
            .plan_batch(&[item("S50", "流式写入口"), item("S67", "批量入口")])
            .await
            .expect("planning");
        for id in ["S50", "S67"] {
            assert!(
                plan.invariants.iter().any(|line| line.starts_with(id)),
                "计划里没点到 {id}：{:?}",
                plan.invariants
            );
        }
    }

    /// 空批次不编一份空计划：能过 `validate` 的空计划说的是一个空的「这一类」。
    #[tokio::test]
    async fn an_empty_batch_produces_no_plan() {
        let plan = LedgerBatchPlanner::new()
            .plan_batch(&[])
            .await
            .expect("planning an empty batch is not an error");
        assert!(plan.invariants.is_empty());
        assert!(plan.acceptance.is_empty());
    }
}
