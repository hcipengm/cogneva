//! `cogneva ledger status`：读五格台账，说还剩几条、每条差什么。
//!
//! 它住在 evaluator 这一层，因为「这条还差什么」是判者的问题，不是编排的问题：
//! 编排把活推进，判者回答「到了没有」；同一本台账，读它的人与判它的人是同一个角色。
//!
//! 这是把「台账怎么读」从带外的 Python 读者搬进产品的第一段：读法是
//! `cog_core::contract::ledger_item::parse_md`，批次规则是
//! `LedgerItem::is_in_batch`，两处都与平台自己的类型是同一份，不另写一套。
//!
//! 台账地址的解析顺序与产品别处一致：`COGNEVA_TODO_SOURCE` → `COGNEVA_TODO_LEDGER`
//! → 本机声明 `~/.config/cogneva/todo-ledger` 的第一行。都没有就报「读不到」并退
//! 非零——不回落共享清单（两个工人读同一本，其中一边的活永远没人做）。

use std::path::PathBuf;

/// 这台机器上这一本台账在哪。
pub fn ledger_path() -> Result<PathBuf, String> {
    for key in ["COGNEVA_TODO_SOURCE", "COGNEVA_TODO_LEDGER"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                let path = value.strip_prefix("md:").unwrap_or(value);
                return Ok(PathBuf::from(path));
            }
        }
    }
    let host = std::env::var("HOME")
        .map(|h| PathBuf::from(h).join(".config/cogneva/todo-ledger"))
        .map_err(|_| "没有 HOME，也没有显式台账地址".to_string())?;
    let first = std::fs::read_to_string(&host)
        .map_err(|e| format!("台账地址读不到：{}（{e}）", host.display()))?;
    let line = first
        .lines()
        .next()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .ok_or_else(|| format!("{} 是空的：这一台机器没有声明台账", host.display()))?;
    Ok(PathBuf::from(line))
}

/// 读这一本台账并打印未完成的那几条。
///
/// 退 1 表示「还有没做完的」，退 0 表示整本都到了终态；读不到退 2——三种情况分开，
/// 因为「读不到」与「都做完了」在只看退出码的调用方眼里是同一件事。
pub fn status() -> i32 {
    let path = match ledger_path() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ledger: 台账读不到：{}（{e}）", path.display());
            return 2;
        }
    };
    let items = match cog_core::contract::ledger_item::parse_md(&text) {
        Ok(items) => items,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let open: Vec<_> = items.iter().filter(|i| i.is_in_batch()).collect();
    println!(
        "ledger: {}（{} 条未完成 / {} 条读到）",
        path.display(),
        open.len(),
        items.len()
    );
    for item in &open {
        println!("  未完成 {}  {}", item.id, item.gap);
    }
    if open.is_empty() {
        0
    } else {
        1
    }
}

/// `cogneva ledger <action>` 的分派。今天有 `status`、`next`、`open` 与 `close`；别的名字点名说没有，
/// 不静默成功——一个不认识的子命令退 0 会被读成「跑过了」。
pub fn run() -> i32 {
    match std::env::args().nth(2).as_deref() {
        Some("status") | None => status(),
        Some("next") => next(),
        Some("open") => open(),
        Some("close") => close(),
        Some("gate") => gate(),
        Some(other) => {
            eprintln!("ledger: 不认识的动作 `{other}`（今天有 `status`、`next`、`open`、`close` 与 `gate`）");
            2
        }
    }
}

/// `cogneva ledger gate`：推之前的那一问——这一轮开门时钉下的那一批，做完了没有。
///
/// 判据与带外那道门是同一句话（属主 2026-11-21）：「做完了」只认台账上这一行自己写的
/// 状态——`已落` 且差距为空；触点动过不算（碰巧落在同一条路径上的改动与做了一半的改动
/// 在触点判据下同形）。没做完的那些要么整批做完，要么在 `Split:` 里**逐条点名**，
/// 理由必须过四条判据里能机检的三条。
///
/// 返回码分开：0 允许、1 拒、2 读不到。「读不到」不能印成「允许」，也不能印成「拒」。
pub fn gate() -> i32 {
    let outcome = gate_verdict();
    match outcome {
        GateOutcome::Allow(summary) => {
            println!("{summary}");
            0
        }
        GateOutcome::Refuse(message) => {
            eprintln!("{message}");
            1
        }
        GateOutcome::Unreadable(message) => {
            eprintln!("ledger: {message}");
            2
        }
    }
}

/// `gate` 的三种结局。分三个而不是 `Result<(), String>`：读不到与拒是两件事。
pub enum GateOutcome {
    Allow(String),
    Refuse(String),
    Unreadable(String),
}

/// 这一批做完了没有。IO 与判定分开：判定是纯函数，能拿夹具逐条问。
fn gate_verdict() -> GateOutcome {
    let path = match ledger_path() {
        Ok(p) => p,
        Err(e) => return GateOutcome::Unreadable(e),
    };
    let ledger_dir = path.parent().map(|p| p.display().to_string()).unwrap_or_default();
    let (repo, ok) = git(&ledger_dir, &["rev-parse", "--show-toplevel"]);
    if !ok || repo.is_empty() {
        return GateOutcome::Unreadable(format!("台账不在一个 git 仓里（{ledger_dir}）"));
    }
    let (sha, ok) = git(&repo, &["rev-parse", "--verify", "--quiet", ROUND_REF]);
    if !ok || sha.is_empty() {
        return GateOutcome::Unreadable("事件流里没有开着的一轮（先 `cogneva ledger open`）".into());
    }
    let (body, ok) = git(&repo, &["cat-file", "-p", &sha]);
    let payload: serde_json::Value = match ok.then(|| serde_json::from_str(&body)).transpose() {
        Ok(Some(v)) => v,
        _ => return GateOutcome::Unreadable(format!("{ROUND_REF} 的载荷读不出来")),
    };
    let batch: Vec<String> = payload
        .get("batch")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    if batch.is_empty() {
        return GateOutcome::Refuse("这一轮的批次是空的：没有一条待办说过要做什么".into());
    }
    let code_rev = payload
        .get("code_rev")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let first_rev = payload
        .get("first_rev")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            return GateOutcome::Unreadable(format!("台账读不到：{}（{e}）", path.display()))
        }
    };
    let reached: std::collections::HashSet<String> =
        cog_core::contract::ledger_item::reached_ids(&text).into_iter().collect();
    let item_touches = touches_by_id(&text);

    let tree = std::env::current_dir()
        .map(|d| d.display().to_string())
        .unwrap_or_default();
    let mut touched: Vec<String> = match git(&tree, &["diff", "--name-only", &code_rev]) {
        (out, true) => out.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect(),
        _ => Vec::new(),
    };
    // 台账仓那一侧也在这一轮里被改：那一侧的改动面同样算「这一批动了什么」。
    if !first_rev.is_empty() {
        if let (out, true) = git(&repo, &["diff", "--name-only", &first_rev]) {
            touched.extend(
                out.lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty())
                    .collect::<Vec<_>>(),
            );
        }
    }
    let base = std::env::var("COGNEVA_GATE_BASE").unwrap_or_else(|_| "origin/main".to_string());
    let messages = match git(&tree, &["log", "--format=%B", &format!("{base}..HEAD")]) {
        (out, true) => out,
        _ => return GateOutcome::Unreadable(format!("提交信息读不到（{base}..HEAD）")),
    };
    let bound = closed_batch_bound();
    match judge_gate(&batch, &reached, &touched, &messages, &item_touches, bound) {
        Ok(done) => GateOutcome::Allow(format!(
            "ledger: 台账上 {} 条未完成待办，这一推 {} 条到了终态。",
            batch.len(),
            done
        )),
        Err(e) => GateOutcome::Refuse(e),
    }
}

/// 这一推的判词。纯函数：批、到了的那几条、动过的文件、提交信息、每条的触点、
/// 这个宿主收过的最大一批（`None` = 量不到）。
pub fn judge_gate(
    batch: &[String],
    reached: &std::collections::HashSet<String>,
    touched: &[String],
    messages: &str,
    item_touches: &std::collections::HashMap<String, Vec<String>>,
    closed_bound: Option<usize>,
) -> Result<usize, String> {
    let done = batch.iter().filter(|id| reached.contains(*id)).count();
    let untouched: Vec<&String> = batch.iter().filter(|id| !reached.contains(*id)).collect();
    if untouched.is_empty() {
        return Ok(done);
    }
    let Some(split) = split_of(messages) else {
        return Err(format!(
            "FAIL: 台账上还有 {} 条未完成待办没做完，提交信息里也没有 \
             `Split: <1-4> — <S 号…> — <理由>`。\n      {}",
            untouched.len(),
            untouched.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("、")
        ));
    };
    let unaccounted: Vec<&String> = untouched
        .iter()
        .copied()
        .filter(|id| !split.declared.contains(*id))
        .collect();
    if !unaccounted.is_empty() {
        return Err(format!(
            "FAIL: 这些未完成待办既没做完也没被 `Split:` 点名：{}",
            unaccounted.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("、")
        ));
    }
    judge_split(&split, touched, item_touches, closed_bound)?;
    Ok(done)
}

/// 提交信息里的那一行拆批声明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    pub kind: char,
    pub declared: Vec<String>,
    pub detail: String,
}

/// 从提交信息里读 `Split: <1-4> — <S 号…> — <理由>`；读不到就是没有声明。
pub fn split_of(messages: &str) -> Option<Split> {
    for line in messages.lines() {
        let rest = match line.trim().strip_prefix("Split:") {
            Some(r) => r.trim(),
            None => continue,
        };
        let mut parts = rest.splitn(3, '—');
        let kind = parts.next()?.trim().chars().next()?;
        let declared_raw = parts.next()?;
        let detail = parts.next().unwrap_or("").trim().to_string();
        let declared: Vec<String> = declared_raw
            .split(|c: char| c == ',' || c == '，' || c.is_whitespace())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        return Some(Split {
            kind,
            declared,
            detail,
        });
    }
    None
}

/// 这条路径动的是回不去的东西：回滚一次提交回不到原状。
pub fn irreversible(path: &str) -> bool {
    let p = path.trim_start_matches("./");
    if p.ends_with(".test.sh") || p.split('/').any(|seg| seg == "tests") {
        return false;
    }
    p.starts_with("deploy/")
        || p.starts_with("scripts/host/")
        || p.starts_with("migrations/")
        || p.contains("/migrations/")
}

/// 三条理由逐条判真假。第 2 条（失败不可归因）机器判不了真假，只要求点名测试目标。
fn judge_split(
    split: &Split,
    touched: &[String],
    item_touches: &std::collections::HashMap<String, Vec<String>>,
    closed_bound: Option<usize>,
) -> Result<(), String> {
    match split.kind {
        '3' => {
            let batch_irr: Vec<&String> =
                touched.iter().filter(|p| irreversible(p)).take(3).collect();
            let excl_irr: Vec<&String> = split
                .declared
                .iter()
                .filter_map(|id| item_touches.get(id))
                .flatten()
                .filter(|p| irreversible(p))
                .take(3)
                .collect();
            if !batch_irr.is_empty() && !excl_irr.is_empty() {
                return Err(format!(
                    "FAIL: Split: 3 说不可逆或不对称，但两侧都碰了不可回退的面：这一批动了 {}，\
                     被排除的那几条也要动 {}。两侧都回不去，拆开并没有把不可回退的那一侧单独拎出来",
                    batch_irr.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("、"),
                    excl_irr.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("、"),
                ));
            }
            if batch_irr.is_empty() && excl_irr.is_empty() {
                return Err(
                    "FAIL: Split: 3 说不可逆或不对称，但两侧都没碰 deploy/、migrations/、\
                     scripts/host/——没有「回滚一次提交回不到原状」的东西，这条理由不成立"
                        .into(),
                );
            }
            Ok(())
        }
        '4' => {
            let Some(bound) = closed_bound else {
                return Err(
                    "FAIL: Split: 4 说这些一次读不完，但这个宿主还没有收过任何一批——\
                     量不到，不许当成读得完"
                        .into(),
                );
            };
            if split.declared.len() <= bound {
                return Err(format!(
                    "FAIL: Split: 4 说这些读不完，但这个宿主收过 {bound} 条一批：这一侧只有 \
                     {} 条，不算读不完",
                    split.declared.len()
                ));
            }
            Ok(())
        }
        '2' => {
            let names_a_target =
                split.detail.contains(".rs") || split.detail.contains("tests/") || split.detail.contains("test_");
            if !names_a_target {
                return Err("FAIL: Split: 2 要点名会重叠的测试目标（文件或测试名）".into());
            }
            Ok(())
        }
        other => Err(format!(
            "FAIL: Split: {other} 不在三条里（只认 2 归因 / 3 不可逆 / 4 读不完）"
        )),
    }
}

/// 台账每一行写着它要动哪些文件（`｜ touches: a, b`）。判第 3 条理由要用。
fn touches_by_id(text: &str) -> std::collections::HashMap<String, Vec<String>> {
    let mut out = std::collections::HashMap::new();
    for line in text.lines() {
        let row = line.trim();
        if !row.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = row.trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() < 5 {
            continue;
        }
        let Some(at) = row.find("touches:") else {
            continue;
        };
        let raw = row[at + "touches:".len()..]
            .split('|')
            .next()
            .unwrap_or("")
            .trim();
        if raw.starts_with("unknown") {
            continue;
        }
        let paths: Vec<String> = raw
            .split(|c: char| c == ',' || c == '，')
            .map(|p| p.split(['（', '(']).next().unwrap_or("").trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        out.insert(cells[0].to_string(), paths);
    }
    out
}

/// 这个宿主上真的收过的最大一批是几条（`rounds-closed.tsv` 第 3 格）。
/// 读不到就是 `None`（量不到），不是 0。
fn closed_batch_bound() -> Option<usize> {
    let path = std::path::PathBuf::from(
        std::env::var("COGNEVA_CLOSED_ROUNDS").unwrap_or_else(|_| {
            format!("{}/.local/state/cogneva/rounds-closed.tsv", home())
        }),
    );
    let text = std::fs::read_to_string(path).ok()?;
    let mut best: Option<usize> = None;
    for line in text.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            continue;
        }
        if let Ok(size) = parts[2].parse::<usize>() {
            best = Some(best.map_or(size, |b: usize| b.max(size)));
        }
    }
    best
}

/// 「下一步是什么」，由三样读数算出来：这一本的未完成条数、工作树干不干净、
/// 编译账里这一棵树的最后一条。
///
/// 与台账读者放在一起，是因为它们读的是同一件事的四面：写了什么（台账）、动没动
/// （工作树）、编没编（编译账）。顺序就是流程的顺序——先改完，再编一次，最后才谈推。
pub fn decide(open_rows: usize, dirty: bool, compiled_head: Option<&str>, head: &str) -> String {
    if open_rows > 0 {
        return format!("继续编辑——还有 {open_rows} 条未完成");
    }
    if dirty {
        return "先提交（编译垫片在脏树上不编）".to_string();
    }
    match compiled_head {
        None => "编译一次（这个宿主还没有这棵树的编译账）".to_string(),
        Some(compiled) if compiled != head => format!(
            "编译一次（账上最后一次编的是 {}，此刻 HEAD 是 {}）",
            &compiled[..compiled.len().min(12)],
            &head[..head.len().min(12)]
        ),
        Some(_) => "推（先跑 `deploy/scripts/check-before-push.sh`；绿了才推）".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 关轮那一行读数：时刻、第一次开工的基准、这一批几条、都是谁——四格用制表符分开，
    /// 读的人按第 3 格取「这个宿主收过多大的一批」。
    #[test]
    fn a_closed_round_writes_its_size_on_one_line() {
        let line = closed_round_line(
            "2026-11-24T01:02:03Z",
            "abcdef1234567890",
            &["N01".to_string(), "N02".to_string()],
        );
        let cells: Vec<&str> = line.split('\t').collect();
        assert_eq!(cells.len(), 4);
        assert_eq!(cells[1], "abcdef123456");
        assert_eq!(cells[2], "2");
        assert_eq!(cells[3], "N01,N02");
    }

    /// 书的载荷带着这一批、这一批每条的缺口、两个坐标与第一次开工的基准。
    #[test]
    fn the_round_payload_carries_the_batch_and_every_gap() {
        let batch = vec!["N01".to_string(), "N02".to_string()];
        let mut gaps = serde_json::Map::new();
        gaps.insert("N01".into(), serde_json::Value::String("缺的是：接线".into()));
        gaps.insert("N02".into(), serde_json::Value::String("—".into()));
        let payload = round_payload(&batch, &gaps, "abcdef123456", "fedcba654321", "111111111111");
        assert_eq!(payload["batch"].as_array().map(|a| a.len()), Some(2));
        assert_eq!(payload["gaps"]["N01"], "缺的是：接线");
        assert_eq!(payload["ledger_rev"], "abcdef123456");
        assert_eq!(
            payload["first_rev"], "111111111111",
            "重钉不该清零「自开工以来」的基准"
        );
        assert!(payload["opened_at"].as_str().is_some_and(|s| s.contains('T')));
    }


    fn ids(words: &[&str]) -> Vec<String> {
        words.iter().map(|s| s.to_string()).collect()
    }

    fn set(words: &[&str]) -> std::collections::HashSet<String> {
        words.iter().map(|s| s.to_string()).collect()
    }

    fn touches(pairs: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(id, paths)| {
                (
                    id.to_string(),
                    paths.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                )
            })
            .collect()
    }

    /// 整批到终态：不问提交信息。
    #[test]
    fn a_batch_that_reached_its_terminal_passes_without_a_split() {
        let batch = ids(&["N01", "N02"]);
        let reached = set(&["N01", "N02"]);
        assert_eq!(judge_gate(&batch, &reached, &[], "", &Default::default(), None), Ok(2));
    }

    /// 没做完又没点名：拒，而且把没做完的逐条列出来。
    #[test]
    fn an_unfinished_row_that_is_not_named_is_refused_by_name() {
        let batch = ids(&["N01", "N02"]);
        let reached = set(&["N01"]);
        let verdict = judge_gate(&batch, &reached, &[], "", &Default::default(), None).unwrap_err();
        assert!(verdict.contains("N02"), "判词没点名：{verdict}");
        assert!(verdict.contains("Split:"), "判词没说修法：{verdict}");
    }

    /// 点名了、理由也成立（第 3 条：只有这一批碰了不可回退的面）。
    #[test]
    fn a_split_that_carries_the_irreversible_side_alone_holds() {
        let batch = ids(&["N01", "N02"]);
        let reached = set(&["N01"]);
        let touched = ids(&["deploy/helm/values.yaml"]);
        let messages = "Split: 3 — N02 — 迁移要单独落地\n";
        let verdict = judge_gate(&batch, &reached, &touched, messages, &Default::default(), None);
        assert_eq!(verdict, Ok(1));
    }

    /// 两侧都碰不可回退的面：拆开没有把那一侧拎出来，理由不成立。
    #[test]
    fn a_split_whose_both_sides_are_irreversible_is_refused() {
        let batch = ids(&["N01", "N02"]);
        let reached = set(&["N01"]);
        let touched = ids(&["deploy/helm/values.yaml"]);
        let item_touches = touches(&[("N02", &["migrations/postgres/127_x.up.sql"])]);
        let messages = "Split: 3 — N02 — 迁移要单独落地\n";
        let verdict = judge_gate(&batch, &reached, &touched, messages, &item_touches, None)
            .unwrap_err();
        assert!(verdict.contains("两侧都碰"), "{verdict}");
    }

    /// 两侧都不碰：没有回不去的东西，理由不成立。
    #[test]
    fn a_split_with_no_irreversible_face_at_all_is_refused() {
        let batch = ids(&["N01", "N02"]);
        let reached = set(&["N01"]);
        let touched = ids(&["crates/a/src/lib.rs"]);
        let messages = "Split: 3 — N02 — 迁移要单独落地\n";
        let verdict = judge_gate(&batch, &reached, &touched, messages, &Default::default(), None)
            .unwrap_err();
        assert!(verdict.contains("两侧都没碰"), "{verdict}");
    }

    /// 第 4 条：界是这个宿主真收过的最大一批；量不到不许当成读得完。
    #[test]
    fn a_split_of_four_is_sized_by_what_this_host_has_closed() {
        let batch = ids(&["N01", "N02", "N03"]);
        let reached = set(&["N01"]);
        let messages = "Split: 4 — N02, N03 — 一次读不完\n";
        // 收过 5 条一批：这一侧只有 2 条，不算读不完。
        assert!(judge_gate(&batch, &reached, &[], messages, &Default::default(), Some(5)).is_err());
        // 量不到：不许当成读得完。
        assert!(judge_gate(&batch, &reached, &[], messages, &Default::default(), None).is_err());
        // 收过 1 条一批：2 条确实比它大。
        assert_eq!(
            judge_gate(&batch, &reached, &[], messages, &Default::default(), Some(1)),
            Ok(1)
        );
    }

    /// 第 2 条要点名会重叠的测试目标；第 5 条不在三条里。
    #[test]
    fn a_split_reason_must_name_a_test_or_be_one_of_the_three() {
        let batch = ids(&["N01", "N02"]);
        let reached = set(&["N01"]);
        let vague = "Split: 2 — N02 — 失败了\n";
        assert!(judge_gate(&batch, &reached, &[], vague, &Default::default(), None).is_err());
        let named = "Split: 2 — N02 — 与 crates/a/tests/x.rs 重叠\n";
        assert_eq!(
            judge_gate(&batch, &reached, &[], named, &Default::default(), None),
            Ok(1)
        );
        let out_of_set = "Split: 5 — N02 — 随便\n";
        assert!(judge_gate(&batch, &reached, &[], out_of_set, &Default::default(), None).is_err());
    }

    /// 声明那一行的形状：编号、点名的条、理由三段，缺一段就是没有声明。
    #[test]
    fn a_split_line_is_read_as_three_parts() {
        let split = split_of("subject\n\nSplit: 3 — N02，S07 — 回不去的面要单独落地\n").unwrap();
        assert_eq!(split.kind, '3');
        assert_eq!(split.declared, ids(&["N02", "S07"]));
        assert!(split.detail.contains("回不去"));
        assert!(split_of("没有这一行").is_none());
    }

    /// 顺序就是流程：先改完，再提交，再编一次，最后才谈推。
    #[test]
    fn the_next_step_follows_the_order_of_the_flow() {
        assert!(decide(3, false, None, "abc").starts_with("继续编辑"));
        assert!(decide(0, true, None, "abc").starts_with("先提交"));
        assert!(decide(0, false, None, "abc").contains("还没有这棵树的编译账"));
        assert!(decide(0, false, Some("old"), "new").contains("编译一次"));
        assert!(decide(0, false, Some("same"), "same").starts_with("推"));
    }
}

/// 这一棵树在这本编译账里的最后一条（时间、树、参数、HEAD、指纹）。
fn last_compile_for(tree: &str) -> Option<String> {
    let path = std::path::PathBuf::from(
        std::env::var("COGNEVA_COMPILE_LEDGER")
            .unwrap_or_else(|_| format!("{}/.local/state/cogneva/compile-ledger.tsv", home())),
    );
    let text = std::fs::read_to_string(path).ok()?;
    let mut found = None;
    for line in text.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 4 && parts[1] == tree {
            found = Some(parts[3].to_string());
        }
    }
    found
}

fn home() -> String {
    std::env::var("HOME").unwrap_or_default()
}

/// 跑一次 `git -C <tree> <args>`，返回 stdout（失败给空串：读不到不是「没有」，
/// 调用方按「量不到」处理——这里只把空串与退出码交上去）。
fn git(tree: &str, args: &[&str]) -> (String, bool) {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(tree)
        .args(args)
        .output();
    match out {
        Ok(o) => (String::from_utf8_lossy(&o.stdout).trim().to_string(), o.status.success()),
        Err(_) => (String::new(), false),
    }
}

/// `cogneva ledger next`：把下一步打出来。退 0 表示有下一步（总有），退 2 读不到台账。
pub fn next() -> i32 {
    let path = match ledger_path() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ledger: 台账读不到：{}（{e}）", path.display());
            return 2;
        }
    };
    let open = match cog_core::contract::ledger_item::parse_md(&text) {
        Ok(items) => items.iter().filter(|i| i.is_in_batch()).count(),
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let tree = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let (status, ok) = git(&tree, &["status", "--porcelain"]);
    if !ok {
        eprintln!("ledger: 工作树读不到（{tree}）");
        return 2;
    }
    let (head, _) = git(&tree, &["rev-parse", "HEAD"]);
    let compiled = last_compile_for(&tree);
    println!(
        "ledger: {}",
        decide(open, !status.is_empty(), compiled.as_deref(), &head)
    );
    0
}

/// 书：这一轮钉在哪一版台账上。载荷是 JSON，住在台账那个仓的 `refs/cogneva/round`。
pub const ROUND_REF: &str = "refs/cogneva/round";

/// `cogneva ledger open`：把这一批钉进书里。
///
/// 一批是什么由台账现算（未完成、且没写 `touches: unknown` 的那些），三格里的「差距」
/// 随行走——接着做的人打开轮就读得到每条还差什么，不必把整本台账重判一遍。
/// 重钉继承 `first_rev`：那一格是「自开工以来」的基准，不能因为重钉被清零。
pub fn open() -> i32 {
    let path = match ledger_path() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ledger: 台账读不到：{}（{e}）", path.display());
            return 2;
        }
    };
    let items = match cog_core::contract::ledger_item::parse_md(&text) {
        Ok(items) => items,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let ledger_dir = path.parent().map(|p| p.display().to_string()).unwrap_or_default();
    let (repo, ok) = git(&ledger_dir, &["rev-parse", "--show-toplevel"]);
    if !ok || repo.is_empty() {
        eprintln!("ledger: 台账不在一个 git 仓里（{ledger_dir}）：这一轮钉不到任何一版上");
        return 2;
    }
    let (ledger_rev, ok) = git(&repo, &["rev-parse", "HEAD"]);
    if !ok {
        eprintln!("ledger: 台账仓读不到 HEAD（{repo}）");
        return 2;
    }
    let (code_rev, _) = git(
        &std::env::current_dir()
            .map(|d| d.display().to_string())
            .unwrap_or_default(),
        &["rev-parse", "HEAD"],
    );
    // 重钉继承第一次开工的基准。
    let first_rev = existing_first_rev(&repo).unwrap_or_else(|| ledger_rev.clone());
    let batch: Vec<String> = items.iter().map(|i| i.id.clone()).collect();
    let gaps: serde_json::Map<String, serde_json::Value> = items
        .iter()
        .map(|i| (i.id.clone(), serde_json::Value::String(i.gap.clone())))
        .collect();
    let payload = round_payload(&batch, &gaps, &ledger_rev, &code_rev, &first_rev);
    let sha = match write_book(&repo, &payload.to_string()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ledger: {e}（{repo}）");
            return 2;
        }
    };
    let (_, ok) = git(&repo, &["update-ref", "--create-reflog", ROUND_REF, &sha]);
    if !ok {
        eprintln!("ledger: {ROUND_REF} 没写成");
        return 2;
    }
    println!(
        "ledger: 轮开在 {}@{}，这一批 {} 条",
        ROUND_REF,
        &payload["ledger_rev"].as_str().unwrap_or("").chars().take(12).collect::<String>(),
        payload["batch"].as_array().map(|a| a.len()).unwrap_or(0)
    );
    0
}

/// 书的载荷。拆出来是因为它是这一轮「是什么」的全部可判内容：批、每条的缺口、两个
/// 坐标与第一次开工的基准。写进去的是什么，读的人就该读到什么。
fn round_payload(
    batch: &[String],
    gaps: &serde_json::Map<String, serde_json::Value>,
    ledger_rev: &str,
    code_rev: &str,
    first_rev: &str,
) -> serde_json::Value {
    serde_json::json!({
        "event_id": format!("round-{}", ledger_rev.chars().take(12).collect::<String>()),
        "ledger_rev": ledger_rev,
        "code_rev": code_rev,
        "batch": batch,
        "gaps": gaps,
        "first_rev": first_rev,
        "opened_at": chrono::Utc::now().to_rfc3339(),
    })
}

/// 书里已经记着的 `first_rev`（没有书或读不到：`None`）。
fn existing_first_rev(repo: &str) -> Option<String> {
    let (sha, ok) = git(repo, &["rev-parse", "--verify", "--quiet", ROUND_REF]);
    if !ok || sha.is_empty() {
        return None;
    }
    let (body, ok) = git(repo, &["cat-file", "-p", &sha]);
    if !ok {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()?
        .get("first_rev")?
        .as_str()
        .map(str::to_string)
}

/// `cogneva ledger close`：把这一轮合上，并把「这一批真有这么大」记成一行读数。
///
/// 关轮不改批：书里写着开门那一刻的清单，这里只加一个 `closed_at`。读数文件记的是
/// **书里那一批**的大小——它是 `Split: 4`（一次读不完）的界，按关轮那一刻现算的批
/// （已全部到终态，故为空）记，那条读数就永远是 0。
pub fn close() -> i32 {
    let path = match ledger_path() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    let ledger_dir = path.parent().map(|p| p.display().to_string()).unwrap_or_default();
    let (repo, ok) = git(&ledger_dir, &["rev-parse", "--show-toplevel"]);
    if !ok || repo.is_empty() {
        eprintln!("ledger: 台账不在一个 git 仓里（{ledger_dir}）");
        return 2;
    }
    let (sha, ok) = git(&repo, &["rev-parse", "--verify", "--quiet", ROUND_REF]);
    if !ok || sha.is_empty() {
        eprintln!("ledger: 没有开着的一轮");
        return 1;
    }
    let (body, ok) = git(&repo, &["cat-file", "-p", &sha]);
    let mut payload: serde_json::Value = match ok.then(|| serde_json::from_str(&body)).transpose() {
        Ok(Some(v)) => v,
        _ => {
            eprintln!("ledger: {ROUND_REF} 的载荷读不出来");
            return 2;
        }
    };
    if payload.get("closed_at").and_then(|v| v.as_str()).is_some() {
        eprintln!("ledger: 这一轮已经关了");
        return 1;
    }
    let now = chrono::Utc::now().to_rfc3339();
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("closed_at".into(), serde_json::Value::String(now.clone()));
    }
    let new_sha = match write_book(&repo, &payload.to_string()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ledger: {e}");
            return 2;
        }
    };
    if !git(&repo, &["update-ref", "--create-reflog", ROUND_REF, &new_sha]).1 {
        eprintln!("ledger: {ROUND_REF} 没写成");
        return 2;
    }
    let batch: Vec<String> = payload
        .get("batch")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    let first_rev = payload
        .get("first_rev")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let line = closed_round_line(&now, &first_rev, &batch);
    let ledger_path_file = std::path::PathBuf::from(
        std::env::var("COGNEVA_CLOSED_ROUNDS").unwrap_or_else(|_| {
            format!("{}/.local/state/cogneva/rounds-closed.tsv", home())
        }),
    );
    if let Some(dir) = ledger_path_file.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    use std::io::Write;
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ledger_path_file)
    {
        Ok(mut f) => {
            let _ = writeln!(f, "{line}");
        }
        Err(e) => eprintln!(
            "ledger: 读数没写进去：{}（{e}）——这一轮关了，但那条界没有新增样本",
            ledger_path_file.display()
        ),
    }
    println!("ledger: 这一轮关了，本批 {} 条", batch.len());
    0
}

/// 关轮那一刻写下的那一行读数：时刻、第一次开工的基准、这一批几条、都是谁。
fn closed_round_line(now: &str, first_rev: &str, batch: &[String]) -> String {
    format!(
        "{now}\t{}\t{}\t{}",
        first_rev.chars().take(12).collect::<String>(),
        batch.len(),
        batch.join(",")
    )
}

/// 把一份载荷写成 blob 并返回它的 sha。
fn write_book(repo: &str, body: &str) -> Result<String, String> {
    use std::io::Write;
    let mut child = std::process::Command::new("git")
        .args(["-C", repo, "hash-object", "-w", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("书写不进去：{e}"))?;
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(body.as_bytes())
        .map_err(|e| format!("书写不进去：{e}"))?;
    let out = child.wait_with_output().map_err(|e| format!("书写不进去：{e}"))?;
    if !out.status.success() {
        return Err("书写不进去".to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
