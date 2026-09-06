#!/usr/bin/env python3
"""Aggregate Cogneva bot PRs into a consensus leaderboard and contribution board.

Scans open pull requests, parses the machine-readable metadata each bot PR
carries (the `<!-- cogneva-bot-meta -->` block in the body), the verdict
markers (`<!-- cogneva-cv: ... -->`) and the structured A/B eval markers
(`<!-- cogneva-eval: {json} -->`) in cross-validation comments, then:

  * ranks competing solutions for the same issue by measured improvement and
    statistical significance (significant success-rate gains first, then mean
    improvement, then validation tally);
  * posts the ranking as an upserted leaderboard comment on every competing
    PR, so the consensus is visible where reviewers look;
  * labels the leading PR of each group `cogneva-recommended`;
  * maintains a single tracking issue with the overall board (competing
    groups, all open bot PRs, per-bot leaderboard).

Stdlib only; talks to the GitHub API with the workflow's GITHUB_TOKEN.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

META_MARKER = "<!-- cogneva-bot-meta -->"
CV_MARKER_RE = re.compile(
    r"<!-- cogneva-cv:\s*pr=(\d+)\s+head=([0-9a-f]+)\s+bot=(\S+?)\s*-->"
)
EVAL_MARKER_RE = re.compile(r"<!--\s*cogneva-eval:\s*(\{.*?\})\s*-->")
VERDICT_RE = re.compile(r"\*\*Verdict:\s*([A-Z_]+)\*\*")
FIXES_RE = re.compile(r"(?:Fixes|Closes)\s+#(\d+)", re.IGNORECASE)
BOARD_TITLE = "[Cogneva] Bot contribution board"
RECOMMEND_LABEL = "cogneva-recommended"
LEADERBOARD_MARKER = "<!-- cogneva-board:issue={issue} -->"


def api(path: str, method: str = "GET", body: dict | None = None) -> object:
    url = f"{os.environ.get('GITHUB_API_URL', 'https://api.github.com')}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if e.code in (404, 422):
            return None
        raise


def repo_path() -> str:
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not repo:
        sys.exit("GITHUB_REPOSITORY is not set; run this inside GitHub Actions.")
    return repo


def ensure_label(name: str, color: str, description: str) -> None:
    """Create a label if the repo does not have it yet (idempotent).

    Attaching a missing label to an issue/PR returns 422 and silently fails
    (the api() helper swallows it), so both labels the board relies on must
    exist before they are used.
    """
    repo = repo_path()
    if api(f"/repos/{repo}/labels/{name}") is not None:
        return
    api(
        f"/repos/{repo}/labels",
        "POST",
        {"name": name, "color": color, "description": description},
    )


def paginated(path: str) -> list:
    """GET a list endpoint, following Link: rel=\"next\" pagination."""
    out: list = []
    url = path
    base = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    while url:
        full = url if url.startswith("http") else f"{base}{url}"
        req = urllib.request.Request(full)
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        with urllib.request.urlopen(req) as resp:
            out.extend(json.loads(resp.read()))
            link = resp.headers.get("Link", "")
        nxt = None
        for part in link.split(","):
            if 'rel="next"' in part:
                m = re.search(r"<([^>]+)>", part)
                if m:
                    nxt = m.group(1)
        url = nxt
    return out


def parse_meta(body: str) -> dict | None:
    """Parse the `key: value` lines following the bot-meta marker."""
    if not body or META_MARKER not in body:
        return None
    tail = body.split(META_MARKER, 1)[1]
    meta: dict[str, str] = {}
    for line in tail.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("<!--") and "-->" in line and "cogneva" not in line:
            break
        m = re.match(r"^([a-zA-Z_]+):\s*(.*)$", line)
        if m:
            meta[m.group(1)] = m.group(2).strip()
        elif meta:
            # First non key-value line after the block ends it.
            break
    return meta or None


def collect() -> tuple[list[dict], dict[int, list[dict]]]:
    repo = repo_path()
    prs = paginated(f"/repos/{repo}/pulls?state=open&per_page=100")
    bot_prs: list[dict] = []
    comments: dict[int, list[dict]] = {}
    for pr in prs:
        meta = parse_meta(pr.get("body") or "")
        if not meta:
            continue
        number = pr["number"]
        fixes = FIXES_RE.findall(pr.get("body") or "")
        bot_prs.append(
            {
                "number": number,
                "title": pr.get("title", ""),
                "url": pr.get("html_url", ""),
                "author": (pr.get("user") or {}).get("login", ""),
                "bot": meta.get("bot", "unknown"),
                "env": meta.get("env", "n/a"),
                "eval": meta.get("eval", "n/a"),
                "related": meta.get("related", "n/a"),
                "fixes": fixes[0] if fixes else None,
                "labels": [lb.get("name", "") for lb in pr.get("labels", [])],
            }
        )
        comments[number] = paginated(
            f"/repos/{repo}/issues/{number}/comments?per_page=100"
        )
    return bot_prs, comments


def tally_verdicts(comments: list[dict]) -> tuple[int, int, list[str]]:
    """Count PASS/FAIL cross-validation comments.

    Returns (passes, fails, passers). A bot re-validating after a new push
    leaves one marker per head; every verdict is independent evidence.
    """
    passes = fails = 0
    passers: list[str] = []
    for c in comments:
        body = c.get("body") or ""
        m = CV_MARKER_RE.search(body)
        if not m:
            continue
        bot = m.group(3)
        v = VERDICT_RE.search(body)
        verdict = v.group(1) if v else ""
        if verdict.startswith("PASS"):
            passes += 1
            passers.append(bot)
        elif verdict.startswith("FAIL"):
            fails += 1
    return passes, fails, passers


def collect_evals(comments: list[dict]) -> list[dict]:
    """Parse structured eval markers from cross-validation comments.

    Returns one entry per marker: {"bot": <validator>, "metrics": <dict>}.
    Malformed JSON or non-applicable payloads are skipped.
    """
    out: list[dict] = []
    for c in comments:
        body = c.get("body") or ""
        cv = CV_MARKER_RE.search(body)
        bot = cv.group(3) if cv else (c.get("user") or {}).get("login", "?")
        for m in EVAL_MARKER_RE.finditer(body):
            try:
                metrics = json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
            if not isinstance(metrics, dict) or not metrics.get("applicable"):
                continue
            out.append({"bot": bot, "metrics": metrics})
    return out


def improvement_pp(metrics: dict) -> float | None:
    """Success-rate improvement in percentage points, if both rates exist."""
    rb, ra = metrics.get("rb"), metrics.get("ra")
    if isinstance(rb, (int, float)) and isinstance(ra, (int, float)):
        return (ra - rb) * 100.0
    return None


def rank_key(evals: list[dict], verdict: tuple[int, int, list[str]]) -> tuple:
    """Consensus ranking key: significance first, then magnitude, then tally.

    A candidate with at least one statistically significant measured
    improvement outranks everything without one; within a tier the best
    significant improvement wins, then the mean measured improvement across
    validators, then independent PASS count, then fewest FAILs.
    """
    passes, fails, _ = verdict
    measured = [
        (e["metrics"], pp)
        for e in evals
        if (pp := improvement_pp(e["metrics"])) is not None
    ]
    sig = [pp for m, pp in measured if m.get("significant") and pp > 0]
    pps = [pp for _, pp in measured]
    return (
        1 if sig else 0,
        max(sig) if sig else float("-inf"),
        sum(pps) / len(pps) if pps else float("-inf"),
        passes,
        -fails,
    )


def has_evidence(evals: list[dict], verdict: tuple[int, int, list[str]]) -> bool:
    """A leader must be backed by something: a significant measurement or a PASS."""
    passes, _, _ = verdict
    return bool(rank_key(evals, verdict)[0]) or passes > 0


def representative_eval(evals: list[dict]) -> dict | None:
    """Pick the most trustworthy metrics entry: significant first, then the
    largest combined sample size."""
    if not evals:
        return None

    def weight(e: dict) -> tuple:
        m = e["metrics"]
        n = (m.get("nb") or 0) + (m.get("na") or 0)
        return (1 if m.get("significant") else 0, n if isinstance(n, int) else 0)

    return max(evals, key=weight)["metrics"]


def fmt_eval(evals: list[dict]) -> str:
    """One-line human summary of a PR's structured eval evidence."""
    m = representative_eval(evals)
    if m is None:
        return "—"
    parts = []
    pp = improvement_pp(m)
    if pp is not None:
        seg = f"{pp:+.1f}pp"
        if isinstance(m.get("z"), (int, float)):
            seg += f" (z={m['z']:.2f})"
        seg += ", significant" if m.get("significant") else ", n.s."
        n = (m.get("nb"), m.get("na"))
        if all(isinstance(x, int) for x in n):
            seg += f", n={n[0]}/{n[1]}"
        parts.append(seg)
    lb, la = m.get("lb"), m.get("la")
    if isinstance(lb, int) and isinstance(la, int):
        parts.append(f"latency {lb}→{la}ms")
    if not parts:
        return "—"
    out = "; ".join(parts)
    if len(evals) > 1:
        out += f" ({len(evals)} validators)"
    return out


def short(text: str, n: int) -> str:
    text = text.replace("|", "\\|").replace("\n", " ").strip()
    return text if len(text) <= n else text[: n - 1] + "…"


def competing_groups(bot_prs: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for pr in bot_prs:
        if pr["fixes"]:
            groups[pr["fixes"]].append(pr)
    return {k: v for k, v in groups.items() if len(v) > 1}


def rank_group(
    prs: list[dict],
    evals: dict[int, list[dict]],
    verdicts: dict[int, tuple[int, int, list[str]]],
) -> list[dict]:
    return sorted(
        prs,
        key=lambda p: rank_key(evals.get(p["number"], []), verdicts[p["number"]]),
        reverse=True,
    )


def render_leaderboard_comment(
    issue: str,
    ranked: list[dict],
    evals: dict[int, list[dict]],
    verdicts: dict[int, tuple[int, int, list[str]]],
) -> str:
    """Per-issue consensus ranking, posted to every competing PR's comments."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"## Cogneva consensus leaderboard — issue #{issue}",
        "",
        f"{len(ranked)} candidate solutions, ranked by measured eval improvement "
        "and statistical significance (two-proportion z-test, |z| > 1.96), "
        "then independent cross-validation tally.",
        "",
        "| Rank | PR | Bot | Eval evidence | Validations |",
        "|------|----|-----|---------------|-------------|",
    ]
    for i, pr in enumerate(ranked):
        p, f, _ = verdicts[pr["number"]]
        crown = "🏆 " if i == 0 and has_evidence(
            evals.get(pr["number"], []), verdicts[pr["number"]]
        ) else ""
        lines.append(
            f"| {i + 1} | {crown}[#{pr['number']}]({pr['url']}) "
            f"`{short(pr['title'], 50)}` | `{short(pr['bot'], 24)}` "
            f"| {fmt_eval(evals.get(pr['number'], []))} | ✅ {p} / ❌ {f} |"
        )
    lines += [
        "",
        f"_Updated {now} by the bot contribution board; the leader carries the "
        f"`{RECOMMEND_LABEL}` label._",
    ]
    return "\n".join(lines)


def upsert_leaderboard_comments(
    groups: dict[str, list[dict]],
    evals: dict[int, list[dict]],
    verdicts: dict[int, tuple[int, int, list[str]]],
    comments: dict[int, list[dict]],
) -> None:
    """Post (or update) the consensus leaderboard on each competing PR."""
    repo = repo_path()
    for issue, prs in groups.items():
        marker = LEADERBOARD_MARKER.format(issue=issue)
        body = marker + "\n" + render_leaderboard_comment(issue, rank_group(prs, evals, verdicts), evals, verdicts)
        for pr in prs:
            existing = next(
                (
                    c
                    for c in comments.get(pr["number"], [])
                    if marker in (c.get("body") or "")
                ),
                None,
            )
            if existing:
                api(
                    f"/repos/{repo}/issues/comments/{existing['id']}",
                    "PATCH",
                    {"body": body},
                )
            else:
                api(
                    f"/repos/{repo}/issues/{pr['number']}/comments",
                    "POST",
                    {"body": body},
                )


def render(
    bot_prs: list[dict],
    evals: dict[int, list[dict]],
    verdicts: dict[int, tuple[int, int, list[str]]],
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Cogneva Bot Contribution Board",
        "",
        f"_Auto-generated from bot PR metadata, cross-validation verdicts and "
        f"structured eval markers. Last update: {now}._",
        "",
    ]

    competing = competing_groups(bot_prs)

    lines.append("## Consensus recommendations")
    lines.append("")
    if competing:
        lines.append(
            "Multiple bots solved the same issue; ranked by measured eval "
            "improvement and statistical significance, then validations. The "
            "full leaderboard is posted on each competing PR."
        )
        lines.append("")
        for issue, prs in sorted(competing.items(), key=lambda kv: kv[0]):
            ranked = rank_group(prs, evals, verdicts)
            lines.append(f"### Issue #{issue}")
            lines.append("")
            for i, pr in enumerate(ranked):
                p, f, _ = verdicts[pr["number"]]
                ev = evals.get(pr["number"], [])
                crown = "🏆 " if i == 0 and has_evidence(ev, verdicts[pr["number"]]) else ""
                tag = " **recommended**" if crown else ""
                lines.append(
                    f"- {crown}[#{pr['number']}]({pr['url']}) `{short(pr['title'], 60)}` "
                    f"by `{pr['bot']}` — {fmt_eval(ev)} — ✅ {p} / ❌ {f}{tag}"
                )
            lines.append("")
    else:
        lines.append("_No competing solutions for the same issue right now._")
        lines.append("")

    lines.append("## Open bot pull requests")
    lines.append("")
    if bot_prs:
        lines.append("| PR | Bot | Env | Eval evidence | Cross-validation |")
        lines.append("|----|-----|-----|---------------|------------------|")
        for pr in sorted(bot_prs, key=lambda p: p["number"], reverse=True):
            p, f, _ = verdicts[pr["number"]]
            ev = fmt_eval(evals.get(pr["number"], []))
            if ev == "—" and pr["eval"] != "n/a":
                ev = short(pr["eval"], 28)
            lines.append(
                f"| [#{pr['number']}]({pr['url']}) | `{short(pr['bot'], 24)}` "
                f"| {short(pr['env'], 22)} | {ev} | ✅ {p} / ❌ {f} |"
            )
    else:
        lines.append("_No open bot PRs._")
    lines.append("")

    lines.append("## Bot leaderboard")
    lines.append("")
    stats: dict[str, list] = defaultdict(lambda: [0, 0, 0])  # bot -> [prs, passes, sig]
    for pr in bot_prs:
        stats[pr["bot"]][0] += 1
        stats[pr["bot"]][1] += verdicts[pr["number"]][0]
        stats[pr["bot"]][2] += sum(
            1
            for e in evals.get(pr["number"], [])
            if e["metrics"].get("significant")
            and (improvement_pp(e["metrics"]) or 0) > 0
        )
    if stats:
        lines.append("| Bot | Open PRs | Validations passed | Significant improvements |")
        lines.append("|-----|----------|--------------------|--------------------------|")
        for bot, (n_prs, n_pass, n_sig) in sorted(
            stats.items(), key=lambda kv: (kv[1][2], kv[1][1], kv[1][0]), reverse=True
        ):
            lines.append(f"| `{short(bot, 28)}` | {n_prs} | {n_pass} | {n_sig} |")
    else:
        lines.append("_No bot activity yet._")
    lines.append("")

    return "\n".join(lines)


def upsert_board_issue(body: str) -> str:
    repo = repo_path()
    existing = paginated(
        f"/repos/{repo}/issues?state=open&per_page=100&creator=github-actions[bot]"
    )
    for issue in existing:
        if issue.get("title") == BOARD_TITLE and "pull_request" not in issue:
            api(f"/repos/{repo}/issues/{issue['number']}", "PATCH", {"body": body})
            return issue["html_url"]
    created = api(
        f"/repos/{repo}/issues",
        "POST",
        {"title": BOARD_TITLE, "body": body, "labels": ["cogneva-bot"]},
    )
    return (created or {}).get("html_url", "(board issue created)")


def mark_recommended(
    bot_prs: list[dict],
    evals: dict[int, list[dict]],
    verdicts: dict[int, tuple[int, int, list[str]]],
) -> None:
    """Label the leading PR of each competing group; clear stale labels."""
    repo = repo_path()
    winners: set[int] = set()
    for prs in competing_groups(bot_prs).values():
        ranked = rank_group(prs, evals, verdicts)
        top = ranked[0]
        if has_evidence(evals.get(top["number"], []), verdicts[top["number"]]):
            winners.add(top["number"])

    for pr in bot_prs:
        has_label = any(lb.get("name") == RECOMMEND_LABEL for lb in pr.get("labels", []))
        if pr["number"] in winners and not has_label:
            api(
                f"/repos/{repo}/issues/{pr['number']}/labels",
                "POST",
                {"labels": [RECOMMEND_LABEL]},
            )
        elif pr["number"] not in winners and has_label:
            api(
                f"/repos/{repo}/issues/{pr['number']}/labels/{RECOMMEND_LABEL}",
                "DELETE",
            )


def main() -> None:
    ensure_label(
        "cogneva-bot", "6f42c1", "PRs opened by Cogneva self-evolution instances"
    )
    ensure_label(
        RECOMMEND_LABEL, "2ea44f", "Leading bot solution for the issue (bot board)"
    )
    bot_prs, comments = collect()
    verdicts = {pr["number"]: tally_verdicts(comments[pr["number"]]) for pr in bot_prs}
    evals = {pr["number"]: collect_evals(comments[pr["number"]]) for pr in bot_prs}
    upsert_leaderboard_comments(competing_groups(bot_prs), evals, verdicts, comments)
    board = render(bot_prs, evals, verdicts)
    url = upsert_board_issue(board)
    mark_recommended(bot_prs, evals, verdicts)
    print(f"Board updated: {url}")
    print(f"Bot PRs: {len(bot_prs)}")


if __name__ == "__main__":
    main()
