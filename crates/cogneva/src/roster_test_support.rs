//! 「一张名册渲染出来是稳定、可逆的」这一条规则的唯一一份判据。
//!
//! 平台上有两张名册：读面那张四列（谁产出哪条读数），驱动面那张五列（哪个单元
//! 驱动哪个面、停下来时看哪条读数）。它们的行类型不同、解析不同、渲染不同，但
//! 判的是同一件事——注释与空行不是行、渲染是原样的、顺序一变字节不变。这三条
//! 断言曾经在两边各抄了一份：抄的时候都对，改的时候没有任何东西比较它们，于是
//! 「一边把注释也渲染了」与「两边都改了」在树上长得一模一样。
//!
//! 类型不同，规则相同：两个模块各自把自己的 `parse_roster` 与 `render` 递进来。
//! 每个函数都收一个 `what`，因为失败时第一个要知道的是**哪一张**名册坏了。

/// 一张名册走一遍解析再渲染，字节不许动；再走一遍仍是同一串。
///
/// 不动点这一半是这张名册的全部价值：导入的那一行与渲染出来的那一行完全一样，
/// 下游按字节比较两次读，才分不出哪个是手写的、哪个是重新渲染的。
pub(crate) fn a_round_trip_moves_no_byte<T, P, R>(what: &str, roster: &str, parse: P, render: R)
where
    T: std::fmt::Debug,
    P: Fn(&str) -> Result<Vec<T>, String>,
    R: Fn(&[T]) -> String,
{
    let rows = parse(roster).unwrap_or_else(|why| panic!("{what}：样本解析不了：{why}"));
    let expected: String = roster
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| format!("{line}\n"))
        .collect();
    assert_eq!(render(&rows), expected, "{what}：渲染后与原文不一致");
    let again = parse(&render(&rows))
        .unwrap_or_else(|why| panic!("{what}：自己渲染出来的名册解析不了：{why}"));
    assert_eq!(
        render(&again),
        render(&rows),
        "{what}：第二遍渲染与第一遍不同，这不是不动点"
    );
}

/// 注释与空行不是行：它们被跳过，不进入名册，也不被渲染回来。
pub(crate) fn comments_and_blank_lines_are_not_rows<T, P>(what: &str, parse: P)
where
    P: Fn(&str) -> Result<Vec<T>, String>,
{
    let rows = parse("\n# only a comment\n\n")
        .unwrap_or_else(|why| panic!("{what}：只有注释的一份名册解析不了：{why}"));
    assert!(rows.is_empty(), "{what}：只有注释的名册里出现了行");
}

/// 顺序不影响字节：两份内容相同、顺序相反的输入渲染成一模一样的字符串。
///
/// 这条是「两次读可以按字节比较」的前提——否则同一份名册换个导入顺序，下游
/// 就会读成一次变更。
pub(crate) fn a_reordered_roster_renders_the_same_bytes<T, P, R>(
    what: &str,
    roster: &str,
    parse: P,
    render: R,
) where
    T: Clone,
    P: Fn(&str) -> Result<Vec<T>, String>,
    R: Fn(&[T]) -> String,
{
    let rows = parse(roster).unwrap_or_else(|why| panic!("{what}：样本解析不了：{why}"));
    let mut reversed = rows.clone();
    reversed.reverse();
    assert_eq!(
        render(&rows),
        render(&reversed),
        "{what}：行的顺序变了，渲染出来的字节也变了"
    );
}
