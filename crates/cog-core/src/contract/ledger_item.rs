//! 一条待办：序号、终态、现在、差距、状态。
//!
//! 这五格是台账行与事件流共用的形状——带外那一面写成 Markdown 的一行，带内这一面是
//! 同一个结构体。判据只读 `terminal`：它是讨论写下的那段终态描述，允许散文，读数只是
//! 它的一种佐证。`gap` 是终态减现在的差，**写成缺什么**；空着表示这一条已经到终态。
//!
//! 这里算的只有一件机器能算的事：这一条还在不在批次里。判断「终态到没到」是判者的事，
//! 它读同一段 `terminal` 原文去核树——不在这个类型里做，因为一段散文与一棵树的比较
//! 不是相等比较。

/// 一条待办的状态。闭集只有两个词，与台账那一面同字。
///
/// 序列化写的就是台账上的那两个词：planner 的产出直接是一行台账，读它与写它的
/// 人用同一套词，多一个 `Open`／`Reached` 的旁名就多一处要对照的翻译。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub enum LedgerState {
    /// 还差着什么。
    #[serde(rename = "未落")]
    Open,
    /// 终态到了：差距那一格空着才成立。
    #[serde(rename = "已落")]
    Reached,
}

impl LedgerState {
    /// 台账上的那个词。
    pub fn as_str(self) -> &'static str {
        match self {
            LedgerState::Open => "未落",
            LedgerState::Reached => "已落",
        }
    }

    /// 从台账上的词读回来；不是这两个词就没有状态（读不到不是「没落」）。
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "未落" => Some(LedgerState::Open),
            "已落" => Some(LedgerState::Reached),
            _ => None,
        }
    }
}

/// 差距那一格里表示「没有差距」的那几个写法。破折号是排版，不是缺什么。
const EMPTY_GAP: [&str; 4] = ["", "—", "-", "–"];

/// 一条待办。字段与台账的五格一一对应。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct LedgerItem {
    /// 序号：稳定、永不复用。
    pub id: String,
    /// 终态：讨论写下的那段描述，原样。判据只读这一格。
    pub terminal: String,
    /// 现在：此刻的样子，写明来源。可以是散文。
    pub now: String,
    /// 差距：终态减现在，写成缺什么。空着 = 到终态了。
    pub gap: String,
    /// 状态。
    pub state: LedgerState,
}

impl LedgerItem {
    /// 差距是不是空着。空 = 这一条已经到终态（不再进批次）。
    pub fn gap_is_empty(&self) -> bool {
        EMPTY_GAP.contains(&self.gap.trim())
    }

    /// 这一条该不该进这一批。
    ///
    /// 三条规矩，与台账那一面同一套：
    /// - `未落` 且差距非空：进批；
    /// - `已落` 且差距为空：终态到了，不进批；
    /// - 写着 `已落` 却还留着差距：**按未完成读**——跳过它等于承认那个假的终态，
    ///   而那个假的终态从此不会被任何门看第二眼。
    pub fn is_in_batch(&self) -> bool {
        !self.gap_is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(terminal: &str, now: &str, gap: &str, state: LedgerState) -> LedgerItem {
        LedgerItem {
            id: "N99".into(),
            terminal: terminal.into(),
            now: now.into(),
            gap: gap.into(),
            state,
        }
    }

    /// 只有散文终态、没有机器读数的一条，仍然算得出「差距非空」。
    ///
    /// 这是这条类型存在的理由：判据是那段描述，不是一条测试。若这里要求一个读数，
    /// 那么所有描述性的待办都会被读成「信息不足」，而它们其实什么都写清楚了。
    #[test]
    fn a_prose_terminal_with_no_reading_still_has_an_open_gap() {
        let it = item(
            "三本账都只从事件流重建，删掉表能重建",
            "任务账本自建表，重建不读流；变更账本自带第二条流；原则账本零接触",
            "三本都要改成投影",
            LedgerState::Open,
        );
        assert!(it.is_in_batch(), "散文终态的一条被读成了不在批次里");
        assert!(!it.gap_is_empty());
    }

    /// `已落` 只在差距为空时成立；带着差距的 `已落` 要按未完成读。
    #[test]
    fn a_reached_row_with_a_gap_left_is_still_open() {
        assert!(!item("到", "到", "", LedgerState::Reached).is_in_batch());
        assert!(item("到", "还差一步", "还差一步", LedgerState::Reached).is_in_batch());
        for dash in ["—", "-", "–", "  "] {
            assert!(
                item("到", "到", dash, LedgerState::Reached).gap_is_empty(),
                "破折号是排版，不是缺口：{dash:?}"
            );
        }
    }

    /// 带外台账写的那两个词，与这里的两个词逐字相同。
    ///
    /// 这一条钉的是「两面一套推进词」：`todo_source` 只认 `未落`/`已落`，
    /// `LedgerState::parse` 也只认这两个；任一边改了词而另一边没改，
    /// 两边就会把同一条读成两个状态，而两边都不会报错。
    #[test]
    fn the_state_words_are_the_ones_the_ledger_writes() {
        let ledger = ["未落", "已落"];
        for word in ledger {
            assert!(
                LedgerState::parse(word).is_some(),
                "台账写的 {word} 在这里读不出来"
            );
        }
        for state in [LedgerState::Open, LedgerState::Reached] {
            assert!(
                ledger.contains(&state.as_str()),
                "这里写的 {} 台账不认",
                state.as_str()
            );
        }
    }

    /// 状态词是闭集：读不出来的词没有状态，不是「未落」。
    #[test]
    fn the_state_words_are_a_closed_set() {
        assert_eq!(LedgerState::parse("未落"), Some(LedgerState::Open));
        assert_eq!(LedgerState::parse("已落"), Some(LedgerState::Reached));
        assert_eq!(LedgerState::parse("进行中"), None);
        assert_eq!(LedgerState::Open.as_str(), "未落");
    }

    /// 线上那两个词与台账上的那两个词是同一对：planner 的答案直接是一行台账，
    /// 序列化出一份 `Open`／`Reached` 就等于让读它的人再翻一次词典，而翻错不会报错。
    #[test]
    fn a_ledger_item_on_the_wire_speaks_the_ledger_words() {
        let item = LedgerItem {
            id: "N01".into(),
            terminal: "到终态的样子".into(),
            now: "此刻的样子".into(),
            gap: "缺的那一件".into(),
            state: LedgerState::Open,
        };
        let wire = serde_json::to_string(&item).expect("序列化不了");
        assert!(wire.contains("未落"), "线上写的不是台账的词：{wire}");
        assert!(!wire.contains("Open"), "线上出现了台账不认的旁名：{wire}");
        let back: LedgerItem = serde_json::from_str(&wire).expect("读不回来");
        assert_eq!(back, item);
    }
}
