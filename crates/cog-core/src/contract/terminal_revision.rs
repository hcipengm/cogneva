//! 判据（终态）的修订：一条带理由的事实，以及它读回来的样子。
//!
//! 一轮的判据在下一次计划时定下，此后每一圈都对着**同一段话**说——这是「不会每圈
//! 换一个目标」的全部内容。要换那一段话，只有一条路：写一条带理由的修订。空理由不是
//! 一次修订，它只是把目标悄悄换掉；等于把「为什么换」这件事从历史里抹掉。
//!
//! 修订只改它点名的那一条待办：同批里其余的判据一个字都不动——一次修订顺手改掉
//! 别人的目标，读的人会以为那些目标本来就在那儿。

use chrono::{DateTime, Utc};

/// 一次修订：哪一条、从哪段话换到哪段话、为什么、谁换的。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRevision {
    /// 待办的标记（台账里那一条的 id）。
    pub item_id: String,
    /// 换之前那段终态。
    pub previous: String,
    /// 换之后的终态。
    pub terminal: String,
    /// 为什么换。空理由不算一次修订。
    pub reason: String,
    /// 谁换的。
    pub actor: String,
    /// 换的时刻；由落库那一刻填。
    pub at: DateTime<Utc>,
}

/// 一次修订能不能成立。
///
/// 三条都是「这算不算一次修订」而不是格式讲究：没有新终态就没有可对的描述；新终态
/// 与旧的一字不差，写下去只会在读数上多一条「换过」；没有理由，后面每一圈对着新
/// 那段话说的人就无从知道它为什么不是原来那段。
pub fn check_revision(revision: &TerminalRevision) -> Result<(), String> {
    if revision.terminal.trim().is_empty() {
        return Err("a revision needs the terminal it changes to".into());
    }
    if revision.terminal == revision.previous {
        return Err("a revision whose terminal is unchanged changes nothing".into());
    }
    if revision.reason.trim().is_empty() {
        return Err(
            "a revision needs a reason; without one the change is invisible in history".into(),
        );
    }
    Ok(())
}

/// 把修订套到这一批待办上：只改被点名的那几条，返回改了几条。
///
/// 同批里其余的条目一个字都不动——一次修订顺手改掉别人的目标，读的人会以为那些
/// 目标本来就在那儿。标记对不上的修订也不改任何东西：它点的是别的批次。
pub fn apply_revisions(
    items: &mut [crate::contract::ledger_item::LedgerItem],
    revisions: &[TerminalRevision],
) -> usize {
    let mut changed = 0;
    for item in items.iter_mut() {
        if let Some(revision) = revisions
            .iter()
            .filter(|revision| revision.item_id == item.id)
            .next_back()
        {
            if item.terminal != revision.terminal {
                item.terminal = revision.terminal.clone();
                changed += 1;
            }
        }
    }
    changed
}

/// 修订的写入口与读面。写在一个地方，读的人从这里拿，不各自去查事件表。
#[async_trait::async_trait]
pub trait TerminalRevisions: Send + Sync {
    /// 落一条修订。理由为空时拒绝，且什么都不写。
    async fn record(&self, revision: &TerminalRevision) -> crate::SFResult<()>;
    /// 这几条待办各自最近一次的修订；没有修订的条目不出现。
    async fn latest_for(&self, item_ids: &[String]) -> crate::SFResult<Vec<TerminalRevision>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn revision(previous: &str, terminal: &str, reason: &str) -> TerminalRevision {
        TerminalRevision {
            item_id: "S19".into(),
            previous: previous.into(),
            terminal: terminal.into(),
            reason: reason.into(),
            actor: "owner".into(),
            at: Utc::now(),
        }
    }

    fn item(id: &str, terminal: &str) -> crate::contract::ledger_item::LedgerItem {
        crate::contract::ledger_item::LedgerItem {
            id: id.into(),
            terminal: terminal.into(),
            now: String::new(),
            gap: String::new(),
            state: crate::contract::ledger_item::LedgerState::Open,
        }
    }

    /// 修订只改它点名的那一条；同一批里别人的判据一个字都不动。
    #[test]
    fn a_revision_changes_only_the_item_it_names() {
        let mut items = vec![item("S19", "old prompt rule"), item("S20", "another goal")];
        let revisions = vec![revision(
            "old prompt rule",
            "new prompt rule",
            "wrong surface",
        )];
        assert_eq!(apply_revisions(&mut items, &revisions), 1);
        assert_eq!(items[0].terminal, "new prompt rule");
        assert_eq!(items[1].terminal, "another goal");
        // 同一条修订再套一次什么也不改：它不是「改过」的累计。
        assert_eq!(apply_revisions(&mut items, &revisions), 0);
        // 标记对不上：一条都不改。
        let mut other = vec![item("S99", "someone else's goal")];
        assert_eq!(apply_revisions(&mut other, &revisions), 0);
        assert_eq!(other[0].terminal, "someone else's goal");
    }

    #[test]
    fn a_revision_needs_a_terminal_a_difference_and_a_reason() {
        assert!(check_revision(&revision("old", "new", "wrong surface")).is_ok());
        assert!(check_revision(&revision("old", "", "wrong surface")).is_err());
        assert!(check_revision(&revision("same", "same", "wrong surface")).is_err());
        assert!(check_revision(&revision("old", "new", "   ")).is_err());
    }
}
