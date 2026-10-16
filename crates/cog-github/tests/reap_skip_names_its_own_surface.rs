//! Gate: a round that cannot reap assess verdicts must say *which* of the three
//! reasons stopped it, on a channel this platform collects.
//!
//! The failure this exists for was measured 2026-10-16 while auditing every
//! debug-level log in `crates/`: this branch collapsed "cannot list open
//! issues", "cannot list open pull requests" and "both listed fine but one hit
//! the 100-entry page cap" into one line, and that line was written at `debug`.
//! Over seven days this platform collected zero debug lines — against thousands
//! at info — so neither the event nor its reason was readable. What a reader
//! could see was only its consequence: the verdict store growing.
//!
//! Two independent things are pinned here, and the negative control is the
//! second: the three reasons are named one per surface (a merged sentence reads
//! as "the account only grows", never as "why"), and the reaping condition
//! itself is *not* loosened — a partial listing must still skip the reap, since
//! reaping against a partial view drops verdicts of live intents.

use std::path::{Path, PathBuf};

const SOURCE: &str = "crates/cog-github/src/discovery_loop.rs";

/// The reap block, bounded by the two comments that open and close it.
const OPEN: &str = "// Reap verdicts of intents";
const CLOSE: &str = "// A2A 交叉验证";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cog-github")
        .to_path_buf()
}

/// The reap block, cut at its two markers.
///
/// Not "everything before the first `#[cfg(test)]`", which is the idiom used
/// elsewhere for this: this file carries `#[cfg(test)]` items at lines 379 and
/// 421, well above the loop, so that cut would take the production code with
/// it. Each marker below occurs exactly once in the file.
fn reap_block() -> String {
    let path = workspace_root().join(SOURCE);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let start = text
        .find(OPEN)
        .unwrap_or_else(|| panic!("{OPEN} not found in {SOURCE}"));
    let rest = &text[start..];
    let end = rest
        .find(CLOSE)
        .unwrap_or_else(|| panic!("{CLOSE} not found after the reap block"));
    code_only(&rest[..end])
}

/// Drop whole-line `//` comments. The judgements below are about what the code
/// does; the arm's own comment quotes the line it replaced, and a substring
/// search over the raw text would read that quote as the wiring.
fn code_only(text: &str) -> String {
    text.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn each_reason_a_reap_was_skipped_is_named_on_its_own() {
    let block = reap_block();

    assert!(
        !block.contains("incomplete open-intent listing"),
        "那一句「三种原因合成一句」又回来了。它把「哪一面列不出来」和「列全了但可能被截断」\n\
         说成同一件事，而两者的处置不同：前者要修列取，后者只需知道今天开着的意图超过一页。"
    );

    // 两面各自点名，且各自带上错误值（错误值被丢掉的 warn 只能读成「失败了」）。
    for (surface, message) in [
        ("issues", "verdict reap skipped: cannot list open issues"),
        (
            "pull requests",
            "verdict reap skipped: cannot list open pull requests",
        ),
    ] {
        let at = block
            .find(message)
            .unwrap_or_else(|| panic!("找不到这一句：{message}"));
        let before = &block[..at];
        let macro_name = nearest_macro(before);
        assert_eq!(
            macro_name, "tracing::warn!(",
            "「{message}」印在 {macro_name} 上：列取失败是故障（同一个 provider 在上一处\n\
             同样的失败也是 warn），不是细节。"
        );
        assert!(
            before.contains(&format!("surface = \"{surface}\"")),
            "「{message}」没有带 `surface = \"{surface}\"`：两面各印一行才点得出是哪一面。"
        );
        assert!(
            before.contains("error = %e"),
            "「{message}」没有带上错误值：只读得出「失败了」，读不出为什么。"
        );
    }

    // 截断那一支不是故障（自释：开放意图回落到上限以下就不再有这一行），与同 crate
    // 的池闸跳过同级别。
    let truncated = block
        .find("verdict reap skipped: open-intent listing may be truncated")
        .expect("「列全了但可能被截断」这一支没有读数：这一支原来和上面两种原因共用一句");
    let before = &block[..truncated];
    assert_eq!(
        nearest_macro(before),
        "tracing::info!(",
        "截断那一支的级别与同族的跳过不一致（池闸跳过是 info!）。"
    );
    assert!(
        before.contains("page_cap = PROVIDER_LIST_PAGE_SIZE")
            && before.contains("open_pull_requests = prs.len()"),
        "截断那一支没印出撞的是哪个上限、以及撞的时候两边各有多少条：读者据此判断\n\
         「今天开着的意图超过了一页」还是「provider 少给了一页」。"
    );
}

#[test]
fn the_three_readings_are_on_a_channel_this_platform_collects() {
    let block = reap_block();
    assert!(
        !block.contains("debug!"),
        "reap 这一段里还有 debug!：这个平台的 Loki 不采集 debug（七天零行，同一窗口\n\
         info 几千行），写在 debug 上的跳过等于没有读数面。"
    );
    assert!(
        !block.contains("trace!"),
        "reap 这一段里还有 trace!：这个平台同样不采集。"
    );
}

#[test]
fn the_reap_still_requires_a_provably_complete_listing() {
    // 负对照：上面两条一被读成「那就别判完整性了」，这一格会红。用**不完整**的列表
    // 去 reap，会把还开着的意图的判词删掉——那时账变干净了，而干净是假的。
    let block = reap_block();
    assert!(
        block.contains("if listings_complete(open_issues.len(), open_prs.len())"),
        "reap 不再要求「两次列取都完整」这个前提。"
    );
    let guard = block
        .find("if listings_complete(")
        .expect("上一个断言已经确认它在");
    let reap_call = block
        .find("self.verdicts_reap_closed(&open)")
        .expect("reap 的调用点不在了");
    assert!(
        guard < reap_call,
        "完整性判据出现在 reap 调用之后——那一次 reap 已经在不完整的列表上跑过了。"
    );
}

/// The tracing macro nearest to (and preceding) the end of `text`, spelled the
/// way the block spells it.
fn nearest_macro(text: &str) -> &'static str {
    [
        "tracing::info!(",
        "tracing::warn!(",
        "tracing::debug!(",
        "tracing::trace!(",
    ]
    .into_iter()
    .filter_map(|m| text.rfind(m).map(|i| (i, m)))
    .max()
    .map(|(_, m)| m)
    .unwrap_or("（不在任何 tracing 宏里）")
}
