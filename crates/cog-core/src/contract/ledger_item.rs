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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub enum LedgerState {
    /// 还差着什么。默认态：一条什么也没写的待办还没到任何地方。
    #[serde(rename = "未落")]
    #[default]
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

/// 任务行 `input` 上带着这条待办三格的那个键。
///
/// 意图从十六处入口进来时，交上来的东西里若带着这条待办，就用同一个名字放三格：
/// 终态／现在／差距。读它的人（协作层）与写它的人（入口）各自拼一次字面量，改一处
/// 另一处不会红——所以名字住在这里，两边读同一个。
pub const ITEM_INPUT_KEY: &str = "ledger_item";

/// 差距那一格里表示「没有差距」的那几个写法。破折号是排版，不是缺什么。
const EMPTY_GAP: [&str; 4] = ["", "—", "-", "–"];

/// 一条待办。字段与台账的五格一一对应。
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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
    /// - `未落`：进批（旧三格行没有「差距」这一格，空着也不等于做完了）；
    /// - `已落` 且差距为空：终态到了，不进批；
    /// - 写着 `已落` 却还留着差距：**按未完成读**——跳过它等于承认那个假的终态，
    ///   而那个假的终态从此不会被任何门看第二眼。
    pub fn is_in_batch(&self) -> bool {
        self.state == LedgerState::Open || !self.gap_is_empty()
    }
}

/// 台账里的一条行是不是这一族的：`| S12 |`、`| N01 |`、`| S20.7 |` 都算。
fn is_ledger_row(line: &str) -> bool {
    let mut rest = line.trim_start();
    let Some(after) = rest.strip_prefix('|') else {
        return false;
    };
    rest = after.trim_start();
    let mut chars = rest.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    let digits: String = rest
        .chars()
        .skip_while(|c| c.is_ascii_alphabetic())
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    if digits.is_empty() || digits.ends_with('.') {
        return false;
    }
    rest.trim_start_matches(|c: char| c.is_ascii_alphabetic())
        .trim_start_matches(|c: char| c.is_ascii_digit() || c == '.')
        .trim_start()
        .starts_with('|')
}

/// 一行台账的格子（去掉首尾的竖线后按 `|` 切）。`｜` 是全角的，不切。
fn cells_of(line: &str) -> Vec<String> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(|c| c.trim().to_string())
        .collect()
}

/// 「差距」那一格：把行尾的 `｜ touches: …` 切掉，剩下的才是差距。
fn gap_of(cell: &str) -> String {
    match cell.find('｜') {
        Some(at) if cell[at..].trim_start_matches('｜').trim_start().starts_with("touches:") => {
            cell[..at].trim().to_string()
        }
        _ => match cell.find("| touches:") {
            Some(at) => cell[..at].trim().to_string(),
            None => cell.trim().to_string(),
        },
    }
}

/// 读一整本台账。**两种行形状都读**，形状由格子里的内容判，不由序号前缀判：
///
/// - 五格 `| 序号 | 终态 | 现在 | 差距 | 状态 |`：最后一格是状态词；
/// - 旧三格 `| S… | 描述 | 状态 | …`：第三格是状态词（搬迁前的写法还在用）。
///
/// 读不出的行**点名拒掉**，不静默跳过：一行被丢掉之后，它既不在批次里、也不在任何
/// 读数里，而「它不在」与「它做完了」在读的人眼里是同一件事。
pub fn parse_md(text: &str) -> Result<Vec<LedgerItem>, String> {
    let mut out = Vec::new();
    let mut bad: Vec<String> = Vec::new();
    for line in text.lines() {
        if !is_ledger_row(line) {
            continue;
        }
        let cells = cells_of(line);
        if cells.len() >= 5 {
            if let Some(state) = LedgerState::parse(cells.last().map(String::as_str).unwrap_or("")) {
                // `touches: unknown — 理由`：这一条明确本轮不排。它不是批次的一部分，
                // 与「还没做」分开——把它数进未完成会让每一轮的批都虚高。
                if cells[3]
                    .split_once("touches:")
                    .map(|(_, rest)| rest.trim_start().starts_with("unknown"))
                    .unwrap_or(false)
                {
                    continue;
                }
                let gap = gap_of(&cells[3]);
                if state == LedgerState::Reached && EMPTY_GAP.contains(&gap.trim()) {
                    continue; // 终态真的到了：不进批次。
                }
                out.push(LedgerItem {
                    id: cells[0].clone(),
                    terminal: cells[1].clone(),
                    now: cells[2].clone(),
                    gap,
                    state,
                });
                continue;
            }
            if !cells[0].starts_with('S') {
                bad.push(format!("{}（状态：{}）", cells[0], cells.last().cloned().unwrap_or_default()));
                continue;
            }
        }
        if cells.len() >= 3 {
            if let Some(state) = LedgerState::parse(&cells[2]) {
                // 旧三格行写着 `已落`：三格齐才是终态。缺格的 `已落` 是这一族最坏的
                // 形状——它把「文件改了」读成「做完了」，而从那一刻起这一行不再被任何
                // 门看。缺格按**未完成**读（`state = Open`、差距留一句），不是跳过，
                // 也不是拒读：跳过等于承认那个假的终态，拒读会让一条写错的旧记录把
                // 别的活一起停掉。
                let filled = ["代码", "上线", "验证"]
                    .iter()
                    .filter(|tag| line.contains(*tag))
                    .count();
                let state = if state == LedgerState::Reached && filled < 3 {
                    LedgerState::Open
                } else {
                    state
                };
                let gap = if state == LedgerState::Open && cells[2].starts_with("已落") {
                    format!("旧三格行缺格（{} 格齐）", filled)
                } else {
                    String::new()
                };
                out.push(LedgerItem {
                    id: cells[0].clone(),
                    terminal: cells[1].clone(),
                    now: String::new(),
                    gap,
                    state,
                });
                continue;
            }
        }
        bad.push(cells.first().cloned().unwrap_or_else(|| line.to_string()));
    }
    if !bad.is_empty() {
        return Err(format!(
            "这些行的状态词不在闭集里（只有 `未落` / `已落`）：{}",
            bad.join("、")
        ));
    }
    Ok(out)
}

/// 台账上**写着到终态**的那几条的序号：五格行、末格是 `已落`、差距那格空着。
///
/// 与 [`parse_md`] 是同一把尺子的两面：那一面只交「还进批次的」，这一面只交「已经到
/// 了的」。两个函数读同一组格子规则，一处改了另一处也必须改——所以这里带一条用例，
/// 拿同一本台账比对两面的答案，免得「做完了」与「还在做」各读各的。
pub fn reached_ids(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        if !is_ledger_row(line) {
            continue;
        }
        let cells = cells_of(line);
        if cells.len() < 5 {
            continue;
        }
        if LedgerState::parse(cells.last().map(String::as_str).unwrap_or(""))
            != Some(LedgerState::Reached)
        {
            continue;
        }
        if EMPTY_GAP.contains(&gap_of(&cells[3]).trim()) {
            out.push(cells[0].clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧三格行写着 `已落` 却没有三格：按未完成读。跳过它会承认一个假的终态。
    #[test]
    fn an_old_row_that_says_reached_without_three_cells_is_not_reached() {
        let text = "| S9 | 旧格式 | 已落 | 代码: a |\n";
        let items = parse_md(text).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].state, LedgerState::Open);
        assert!(items[0].is_in_batch());
        let full = "| S9 | 旧格式 | 已落 | 代码: a | 上线: b | 验证: c |\n";
        let items = parse_md(full).unwrap();
        assert_eq!(items[0].state, LedgerState::Reached);
        assert!(!items[0].is_in_batch());
    }

    /// 旧三格行没有「差距」这一格：它写着 `未落` 就进批，空差距不等于做完了。
    /// 只按差距判的那一版会把整族旧行读成「不在批次里」，而读数照样绿。
    #[test]
    fn an_old_three_cell_row_that_is_open_is_in_the_batch() {
        let text = "| S9 | 旧格式 | 未落 | 判据 ｜touches: c.rs |\n";
        let items = parse_md(text).unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0].is_in_batch());
    }

    /// 同一本台账：`parse_md` 交的是「还在做」，`reached_ids` 交的是「到了」，
    /// 两边合起来正好是整本，且不重叠。
    #[test]
    fn the_two_readers_split_the_ledger_between_them() {
        let text = "\
| S1 | 终态一 | 现在一 | 差距一 ｜ touches: a.rs | 未落 |
| S2 | 终态二 | 现在二 | — ｜ touches: b.rs | 已落 |
| S3 | 终态三 | 现在三 | 还差一点 ｜ touches: c.rs | 已落 |
| S4 | 终态四 | 现在四 | — | 已落 |
";
        let open: Vec<String> = parse_md(text).unwrap().into_iter().map(|i| i.id).collect();
        let done = reached_ids(text);
        assert_eq!(open, vec!["S1", "S3"]);
        assert_eq!(done, vec!["S2", "S4"]);
    }

    fn item(terminal: &str, now: &str, gap: &str, state: LedgerState) -> LedgerItem {
        LedgerItem {
            id: "N99".into(),
            terminal: terminal.into(),
            now: now.into(),
            gap: gap.into(),
            state,
        }
    }

    /// 五格行、旧三格行、做到终态的行，三种形状各读一次。
    ///
    /// 判的是「这一行做没做完」读得对不对，不是它的排版：形状由格数判，状态由闭集判，
    /// 读不出的点名拒掉。
    #[test]
    fn the_ledger_reader_takes_both_shapes_and_skips_a_reached_row() {
        let text = "\
| S1 | 终态一 | 现在一 | 差距一 ｜ touches: a.rs | 未落 |
| S20.7 | 终态二 | 现在二 | — ｜ touches: b.rs | 已落 |
| N3 | 终态三 | 现在三 | 差距三 | 未落 |
| S9 | 旧格式 | 未落 | 判据 ｜touches: c.rs |
";
        let items = parse_md(text).expect("这本台账该读得出来");
        let ids: Vec<&str> = items.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, vec!["S1", "N3", "S9"], "进批的应当只有未落的那几条");
        assert_eq!(items[0].gap, "差距一");
        assert_eq!(items[2].terminal, "旧格式");
    }

    /// 写了 `touches: unknown` 的行本轮不排，不数进未完成。
    #[test]
    fn a_deferred_row_is_not_counted_as_open() {
        let text = "\
| S1 | 终态 | 现在 | 差距 ｜ touches: unknown — 与 N31 同一批 | 未落 |
| S2 | 终态 | 现在 | 差距 ｜ touches: a.rs | 未落 |
";
        let items = parse_md(text).expect("该读得出来");
        let ids: Vec<&str> = items.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, vec!["S2"], "不排的那条不该进批");
    }

    /// 状态写成散文的行：点名拒掉，不静默丢掉。
    #[test]
    fn a_row_whose_status_is_prose_is_named_not_dropped() {
        let err = parse_md("| N7 | 终态 | 现在 | 差距 | 树里：还没做 |\n").expect_err("该拒");
        assert!(err.contains("N7"), "拒的时候要点名：{err}");
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
