//! Bot contribution board: the aggregation and markdown behind the tracking
//! issue that says what the bot fleet has contributed.
//!
//! The board is three sections over one population — the open pull requests
//! carrying the `cogneva-bot` metadata block:
//!
//! * **Consensus recommendations** — the issues more than one instance solved,
//!   ranked by measured eval improvement and statistical significance before
//!   the cross-validation tally, so a measured gain outranks a tally of
//!   unmeasured PASSes;
//! * **Open bot pull requests** — every bot PR with the evidence it carries;
//! * **Bot leaderboard** — per-instance totals.
//!
//! Only the deterministic part lives here. Listing the PRs, reading their
//! comments and upserting the issue are API calls and stay outside this module;
//! what it takes is data and what it hands back is text.
//!
//! An empty population is not an error. The board then states that per section,
//! one placeholder line each, which is what the tracking issue shows until the
//! first bot PR is cross-validated.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::cross_validation::{EvalMetrics, EVAL_MARKER_PREFIX};

/// Placeholder the consensus section shows when no two bots solved one issue.
pub const NO_COMPETING_SOLUTIONS: &str = "_No competing solutions for the same issue right now._";

/// Placeholder the open-PR section shows when no bot PR is open.
pub const NO_OPEN_PRS: &str = "_No open bot PRs._";

/// Placeholder the leaderboard shows when no bot has contributed yet.
pub const NO_BOT_ACTIVITY: &str = "_No bot activity yet._";

/// One open bot pull request as the board reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotContribution {
    /// Pull request number.
    pub number: u64,
    /// Pull request title.
    pub title: String,
    /// Pull request URL, linked from every table cell that names the PR.
    pub url: String,
    /// Authoring instance, from its metadata block.
    pub bot: String,
    /// Environment the instance ran in, from its metadata block.
    pub env: String,
    /// Eval note the instance published in its metadata block.
    pub eval_note: String,
    /// Issue the body declares it closes; two PRs naming the same issue compete.
    pub fixes: Option<u64>,
}

/// Cross-validation tally for one PR.
///
/// Every verdict comment is independent evidence: an instance that re-validates
/// after a push leaves one per head, and all of them are counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VerdictRecord {
    /// PASS verdicts recorded on the PR.
    pub passes: u32,
    /// FAIL verdicts recorded on the PR.
    pub fails: u32,
}

/// One structured eval marker, attributed to the instance that posted it.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalMarker {
    /// Validating instance.
    pub bot: String,
    /// The A/B measurement the marker carries.
    pub metrics: EvalMetrics,
}

impl EvalMarker {
    /// Parse every eval marker `body` carries, attributed to `bot`.
    ///
    /// A marker whose payload is not applicable carries no measurement (a
    /// tests-only verdict) and is dropped: counting it would rank a candidate
    /// on evidence it never produced.
    pub fn parse_all(bot: &str, body: &str) -> Vec<Self> {
        let mut out = Vec::new();
        let mut rest = body;
        while let Some(start) = rest.find(EVAL_MARKER_PREFIX) {
            let after = &rest[start + EVAL_MARKER_PREFIX.len()..];
            let Some(end) = after.find("-->") else {
                break;
            };
            if let Ok(metrics) = serde_json::from_str::<EvalMetrics>(after[..end].trim()) {
                if metrics.applicable {
                    out.push(Self {
                        bot: bot.to_string(),
                        metrics,
                    });
                }
            }
            rest = &after[end + "-->".len()..];
        }
        out
    }
}

/// One open bot PR together with the evidence collected about it.
#[derive(Debug, Clone, PartialEq)]
pub struct BoardPr {
    /// The contribution itself.
    pub pr: BotContribution,
    /// Cross-validation tally.
    pub verdicts: VerdictRecord,
    /// Structured eval markers, one per validating instance.
    pub evals: Vec<EvalMarker>,
}

/// One bot's row in the board's leaderboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotStat {
    /// Authoring instance.
    pub bot: String,
    /// Open PRs it authored.
    pub open_prs: u32,
    /// PASS verdicts those PRs collected.
    pub validations_passed: u32,
    /// Eval markers recording a statistically significant improvement.
    pub significant_improvements: u32,
}

/// The issue a PR body declares it closes (`Fixes #56` / `Closes #56`).
///
/// Case-insensitively, first declaration wins. `None` means the PR takes part
/// in no consensus group: the board groups by the issue the body names, and a
/// PR naming none competes with nothing.
pub fn parse_fixed_issue(body: &str) -> Option<u64> {
    let lower = body.to_lowercase();
    for keyword in ["fixes #", "closes #"] {
        let Some(at) = lower.find(keyword) else {
            continue;
        };
        let digits: String = lower[at + keyword.len()..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if let Ok(number) = digits.parse() {
            return Some(number);
        }
    }
    None
}

/// Issues more than one open bot PR targets, each with its candidate PRs.
///
/// One candidate per issue is not a consensus: it needs nothing recommending,
/// so those groups are left out.
pub fn competing_groups(board: &[BoardPr]) -> BTreeMap<u64, Vec<BoardPr>> {
    let mut groups: BTreeMap<u64, Vec<BoardPr>> = BTreeMap::new();
    for entry in board {
        if let Some(issue) = entry.pr.fixes {
            groups.entry(issue).or_default().push(entry.clone());
        }
    }
    groups.retain(|_, prs| prs.len() > 1);
    groups
}

/// A candidate's ranking key: significant-tier flag, best significant gain,
/// mean gain, PASS count, and fewest FAILs (as a negated count).
type RankKey = (bool, f64, f64, u32, i64);

/// Order one group best-first.
///
/// A candidate with a significant measured improvement outranks everything
/// without one; inside a tier the largest significant gain wins, then the mean
/// gain across validators, then independent PASS count, then fewest FAILs. PR
/// number ascending settles the rest, so two runs over the same input produce
/// one order.
pub fn rank_group(group: &mut [BoardPr]) {
    group.sort_by(|a, b| {
        rank_cmp(&rank_key(a), &rank_key(b))
            .reverse()
            .then_with(|| a.pr.number.cmp(&b.pr.number))
    });
}

/// Per-bot totals, most significant improvements first, then passes, then open
/// PRs, then handle — the handle is what makes equal rows a fixed order rather
/// than whatever order the PRs arrived in.
pub fn leaderboard(board: &[BoardPr]) -> Vec<BotStat> {
    let mut stats: BTreeMap<String, BotStat> = BTreeMap::new();
    for entry in board {
        let row = stats
            .entry(entry.pr.bot.clone())
            .or_insert_with(|| BotStat {
                bot: entry.pr.bot.clone(),
                open_prs: 0,
                validations_passed: 0,
                significant_improvements: 0,
            });
        row.open_prs += 1;
        row.validations_passed += entry.verdicts.passes;
        row.significant_improvements += entry
            .evals
            .iter()
            .filter(|e| {
                e.metrics.significant && e.metrics.improvement_pp().is_some_and(|pp| pp > 0.0)
            })
            .count() as u32;
    }
    let mut rows: Vec<BotStat> = stats.into_values().collect();
    rows.sort_by(|a, b| {
        b.significant_improvements
            .cmp(&a.significant_improvements)
            .then_with(|| b.validations_passed.cmp(&a.validations_passed))
            .then_with(|| b.open_prs.cmp(&a.open_prs))
            .then_with(|| a.bot.cmp(&b.bot))
    });
    rows
}

/// Render the whole board, the body of the tracking issue.
///
/// `updated_at` is passed in rather than read from the clock: two runs over the
/// same input then render the same bytes, which is what lets the caller skip
/// the issue edit when nothing but the clock moved.
pub fn render(board: &[BoardPr], updated_at: &str) -> String {
    let mut lines: Vec<String> = vec![
        "# Cogneva Bot Contribution Board".to_string(),
        String::new(),
        format!(
            "_Auto-generated from bot PR metadata, cross-validation verdicts and \
             structured eval markers. Last update: {updated_at}._"
        ),
        String::new(),
    ];

    lines.push("## Consensus recommendations".to_string());
    lines.push(String::new());
    let groups = competing_groups(board);
    if groups.is_empty() {
        lines.push(NO_COMPETING_SOLUTIONS.to_string());
        lines.push(String::new());
    } else {
        lines.push(
            "Multiple bots solved the same issue; ranked by measured eval improvement and \
             statistical significance, then validations. The full leaderboard is posted on \
             each competing PR."
                .to_string(),
        );
        lines.push(String::new());
        for (issue, mut prs) in groups {
            rank_group(&mut prs);
            lines.push(format!("### Issue #{issue}"));
            lines.push(String::new());
            for (i, entry) in prs.iter().enumerate() {
                lines.push(consensus_line(entry, i == 0));
            }
            lines.push(String::new());
        }
    }

    lines.push("## Open bot pull requests".to_string());
    lines.push(String::new());
    if board.is_empty() {
        lines.push(NO_OPEN_PRS.to_string());
    } else {
        lines.push("| PR | Bot | Env | Eval evidence | Cross-validation |".to_string());
        lines.push("|----|-----|-----|---------------|------------------|".to_string());
        let mut open: Vec<&BoardPr> = board.iter().collect();
        open.sort_by(|a, b| b.pr.number.cmp(&a.pr.number));
        for entry in open {
            lines.push(format!(
                "| [#{}]({}) | `{}` | {} | {} | ✅ {} / ❌ {} |",
                entry.pr.number,
                entry.pr.url,
                short(&entry.pr.bot, 24),
                short(&entry.pr.env, 22),
                evidence_cell(entry),
                entry.verdicts.passes,
                entry.verdicts.fails,
            ));
        }
    }
    lines.push(String::new());

    lines.push("## Bot leaderboard".to_string());
    lines.push(String::new());
    let rows = leaderboard(board);
    if rows.is_empty() {
        lines.push(NO_BOT_ACTIVITY.to_string());
    } else {
        lines
            .push("| Bot | Open PRs | Validations passed | Significant improvements |".to_string());
        lines
            .push("|-----|----------|--------------------|--------------------------|".to_string());
        for row in &rows {
            lines.push(format!(
                "| `{}` | {} | {} | {} |",
                short(&row.bot, 28),
                row.open_prs,
                row.validations_passed,
                row.significant_improvements,
            ));
        }
    }
    lines.push(String::new());

    lines.join("\n")
}

/// One candidate line in a consensus group. Only the leader carries the crown,
/// and only when something backs it.
fn consensus_line(entry: &BoardPr, lead: bool) -> String {
    let recommended = lead && has_evidence(entry);
    let crown = if recommended { "🏆 " } else { "" };
    let tag = if recommended { " **recommended**" } else { "" };
    format!(
        "- {crown}[#{}]({}) `{}` by `{}` — {} — ✅ {} / ❌ {}{tag}",
        entry.pr.number,
        entry.pr.url,
        short(&entry.pr.title, 60),
        entry.pr.bot,
        fmt_eval(&entry.evals),
        entry.verdicts.passes,
        entry.verdicts.fails,
    )
}

/// A leader has to be backed by something: a significant measured gain, or at
/// least one PASS. A crown on no evidence recommends the winner of nothing.
fn has_evidence(entry: &BoardPr) -> bool {
    rank_key(entry).0 || entry.verdicts.passes > 0
}

fn rank_key(entry: &BoardPr) -> RankKey {
    let mut best_significant = f64::NEG_INFINITY;
    let mut sum = 0.0;
    let mut measured = 0u32;
    for e in &entry.evals {
        let Some(pp) = e.metrics.improvement_pp() else {
            continue;
        };
        sum += pp;
        measured += 1;
        if e.metrics.significant && pp > 0.0 {
            best_significant = best_significant.max(pp);
        }
    }
    let mean = if measured > 0 {
        sum / f64::from(measured)
    } else {
        f64::NEG_INFINITY
    };
    (
        best_significant.is_finite(),
        best_significant,
        mean,
        entry.verdicts.passes,
        -i64::from(entry.verdicts.fails),
    )
}

fn rank_cmp(a: &RankKey, b: &RankKey) -> Ordering {
    a.0.cmp(&b.0)
        .then_with(|| a.1.total_cmp(&b.1))
        .then_with(|| a.2.total_cmp(&b.2))
        .then_with(|| a.3.cmp(&b.3))
        .then_with(|| a.4.cmp(&b.4))
}

/// The most trustworthy measurement among `evals`: significant first, then the
/// largest combined sample size, first-listed on a tie.
fn representative_eval(evals: &[EvalMarker]) -> Option<&EvalMetrics> {
    let mut best: Option<&EvalMarker> = None;
    for e in evals {
        let wins = match best {
            None => true,
            Some(b) => weight(e) > weight(b),
        };
        if wins {
            best = Some(e);
        }
    }
    best.map(|e| &e.metrics)
}

fn weight(e: &EvalMarker) -> (bool, u64) {
    let n = e.metrics.n_before.unwrap_or(0) + e.metrics.n_after.unwrap_or(0);
    (e.metrics.significant, n)
}

/// Success-rate gain and its evidence in one line, `None` when the PR carries
/// no applicable measurement.
fn eval_summary(evals: &[EvalMarker]) -> Option<String> {
    let m = representative_eval(evals)?;
    let mut parts: Vec<String> = Vec::new();
    if let Some(pp) = m.improvement_pp() {
        let mut seg = format!("{pp:+.1}pp");
        if let Some(z) = m.z {
            seg.push_str(&format!(" (z={z:.2})"));
        }
        seg.push_str(if m.significant {
            ", significant"
        } else {
            ", n.s."
        });
        if let (Some(nb), Some(na)) = (m.n_before, m.n_after) {
            seg.push_str(&format!(", n={nb}/{na}"));
        }
        parts.push(seg);
    }
    if let (Some(lb), Some(la)) = (m.latency_before_ms, m.latency_after_ms) {
        parts.push(format!("latency {lb}→{la}ms"));
    }
    if parts.is_empty() {
        return None;
    }
    let mut out = parts.join("; ");
    if evals.len() > 1 {
        out.push_str(&format!(" ({} validators)", evals.len()));
    }
    Some(out)
}

fn fmt_eval(evals: &[EvalMarker]) -> String {
    eval_summary(evals).unwrap_or_else(|| "—".to_string())
}

/// The open-PR table's evidence cell: the measured summary when there is one,
/// otherwise the instance's own note, otherwise a dash.
fn evidence_cell(entry: &BoardPr) -> String {
    eval_summary(&entry.evals).unwrap_or_else(|| {
        if entry.pr.eval_note == "n/a" {
            "—".to_string()
        } else {
            short(&entry.pr.eval_note, 28)
        }
    })
}

/// Table-cell text: `|` escaped, newlines flattened, truncated with an ellipsis.
fn short(text: &str, width: usize) -> String {
    let flat = text.replace('|', "\\|").replace('\n', " ");
    let flat = flat.trim();
    if flat.chars().count() <= width {
        return flat.to_string();
    }
    let mut cut: String = flat.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_validation::eval_marker;

    fn metrics(before: f64, after: f64, significant: bool) -> EvalMetrics {
        EvalMetrics {
            applicable: true,
            rate_before: Some(before),
            rate_after: Some(after),
            n_before: Some(40),
            n_after: Some(40),
            z: Some(if significant { 2.5 } else { 0.4 }),
            significant,
            latency_before_ms: None,
            latency_after_ms: None,
        }
    }

    fn entry(
        number: u64,
        bot: &str,
        fixes: Option<u64>,
        evals: Vec<EvalMarker>,
        passes: u32,
    ) -> BoardPr {
        BoardPr {
            pr: BotContribution {
                number,
                title: format!("fix the thing ({number})"),
                url: format!("https://example.test/pull/{number}"),
                bot: bot.to_string(),
                env: "prod".to_string(),
                eval_note: "n/a".to_string(),
                fixes,
            },
            verdicts: VerdictRecord { passes, fails: 0 },
            evals,
        }
    }

    fn marker(bot: &str, m: EvalMetrics) -> EvalMarker {
        EvalMarker {
            bot: bot.to_string(),
            metrics: m,
        }
    }

    #[test]
    fn an_empty_board_renders_the_three_placeholders() {
        let rendered = render(&[], "2026-10-18 01:04 UTC");
        let expected = concat!(
            "# Cogneva Bot Contribution Board\n",
            "\n",
            "_Auto-generated from bot PR metadata, cross-validation verdicts and structured eval \
             markers. Last update: 2026-10-18 01:04 UTC._\n",
            "\n",
            "## Consensus recommendations\n",
            "\n",
            "_No competing solutions for the same issue right now._\n",
            "\n",
            "## Open bot pull requests\n",
            "\n",
            "_No open bot PRs._\n",
            "\n",
            "## Bot leaderboard\n",
            "\n",
            "_No bot activity yet._\n",
        );
        assert_eq!(rendered, expected);
    }

    #[test]
    fn competing_candidates_fill_all_three_sections() {
        let weak = entry(
            11,
            "Bob#b1",
            Some(7),
            vec![marker("Cara#c1", metrics(0.50, 0.52, false))],
            1,
        );
        let strong = entry(
            12,
            "Alice#a1",
            Some(7),
            vec![marker("Cara#c1", metrics(0.50, 0.80, true))],
            2,
        );
        let solo = entry(13, "Bob#b1", Some(9), Vec::new(), 0);

        let rendered = render(&[weak, strong, solo], "2026-10-18 01:04 UTC");

        assert!(rendered.contains("### Issue #7"));
        assert!(rendered.contains(
            "- 🏆 [#12](https://example.test/pull/12) `fix the thing (12)` by `Alice#a1` \
             — +30.0pp (z=2.50), significant, n=40/40 — ✅ 2 / ❌ 0 **recommended**"
        ));
        // The unmeasured candidate still gets a row, ranked below the measured one.
        assert!(rendered.contains(
            "- [#11](https://example.test/pull/11) `fix the thing (11)` by `Bob#b1` \
             — +2.0pp (z=0.40), n.s., n=40/40 — ✅ 1 / ❌ 0"
        ));
        assert!(rendered.contains(
            "| [#13](https://example.test/pull/13) | `Bob#b1` | prod | — | ✅ 0 / ❌ 0 |"
        ));
        assert!(rendered.contains("| `Alice#a1` | 1 | 2 | 1 |"));
        assert!(rendered.contains("| `Bob#b1` | 2 | 1 | 0 |"));
        assert!(!rendered.contains(NO_COMPETING_SOLUTIONS));
        assert!(!rendered.contains(NO_OPEN_PRS));
        assert!(!rendered.contains(NO_BOT_ACTIVITY));
    }

    #[test]
    fn the_leaderboard_orders_equal_rows_by_handle() {
        let zoe = entry(1, "Zoe#z1", None, Vec::new(), 1);
        let ada = entry(2, "Ada#a1", None, Vec::new(), 1);
        let order = |board: &[BoardPr]| {
            leaderboard(board)
                .into_iter()
                .map(|row| row.bot)
                .collect::<Vec<_>>()
        };
        assert_eq!(order(&[zoe.clone(), ada.clone()]), vec!["Ada#a1", "Zoe#z1"]);
        // The same rows in the other input order: the handle decides, not the input.
        assert_eq!(order(&[ada, zoe]), vec!["Ada#a1", "Zoe#z1"]);
    }

    #[test]
    fn a_crowned_leader_needs_evidence() {
        let passes_only = entry(21, "Alice#a1", Some(5), Vec::new(), 0);
        let passed = entry(22, "Bob#b1", Some(5), Vec::new(), 1);
        let rendered = render(&[passes_only, passed], "2026-10-18 01:04 UTC");
        // Both have no measurement, so the PASS tally leads; nothing at all
        // would recommend nobody.
        assert!(rendered.contains("- 🏆 [#22](https://example.test/pull/22)"));
        assert!(!rendered.contains("- 🏆 [#21](https://example.test/pull/21)"));
    }

    #[test]
    fn markers_and_closing_issues_are_read_from_the_bodies_that_carry_them() {
        assert_eq!(parse_fixed_issue("Fixes #56\n"), Some(56));
        assert_eq!(parse_fixed_issue("body\n\ncloses #7 and more"), Some(7));
        assert_eq!(parse_fixed_issue("no declaration here"), None);

        let applicable = eval_marker(&metrics(0.5, 0.6, true));
        let not_applicable = eval_marker(&EvalMetrics {
            applicable: false,
            ..metrics(0.5, 0.6, true)
        });
        let body = format!(
            "## Cogneva cross-validation\n\n**Verdict: PASS**\n\n{applicable}\n{not_applicable}\n"
        );
        let parsed = EvalMarker::parse_all("Cara#c1", &body);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].bot, "Cara#c1");
        assert_eq!(parsed[0].metrics.rate_after, Some(0.6));
    }
}
