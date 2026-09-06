#!/usr/bin/env python3
"""Aggregate Cogneva bot PRs into a contribution board.

Scans open pull requests, parses the machine-readable metadata each bot PR
carries (the `<!-- cogneva-bot-meta -->` block in the body and the
`<!-- cogneva-cv: ... -->` marker in cross-validation comments), and renders a
Markdown board:

  * consensus recommendations — bot PRs fixing the same issue, ranked by how
    many independent instances validated them in their own sandboxes;
  * every open bot PR with its environment, eval note and validation tally;
  * a per-bot leaderboard.

The board is written to a single tracking issue (created once, then updated).
The leading PR of each competing group gets a `cogneva-recommended` label.

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
VERDICT_RE = re.compile(r"\*\*Verdict:\s*([A-Z_]+)\*\*")
FIXES_RE = re.compile(r"(?:Fixes|Closes)\s+#(\d+)", re.IGNORECASE)
BOARD_TITLE = "[Cogneva] Bot contribution board"
RECOMMEND_LABEL = "cogneva-recommended"


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


def tally_verdicts(pr_number: int, comments: list[dict]) -> tuple[int, int, list[str]]:
    """Count PASS/FAIL cross-validation comments for a PR.

    Returns (passes, fails, passers). A bot re-validating after a new push
    leaves one marker per (head); every verdict is independent evidence.
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


def short(text: str, n: int) -> str:
    text = text.replace("|", "\\|").replace("\n", " ").strip()
    return text if len(text) <= n else text[: n - 1] + "…"


def render(bot_prs: list[dict], verdicts: dict[int, tuple[int, int, list[str]]]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Cogneva Bot Contribution Board",
        "",
        f"_Auto-generated from bot PR metadata and cross-validation comments. Last update: {now}._",
        "",
    ]

    # Group competing solutions by the issue they fix.
    groups: dict[str, list[dict]] = defaultdict(list)
    standalone: list[dict] = []
    for pr in bot_prs:
        key = pr["fixes"]
        groups[key].append(pr) if key else standalone.append(pr)

    competing = {k: v for k, v in groups.items() if len(v) > 1}

    lines.append("## Consensus recommendations")
    lines.append("")
    if competing:
        lines.append(
            "Multiple bots solved the same issue; ranked by independent sandbox validations."
        )
        lines.append("")
        for issue, prs in sorted(
            competing.items(), key=lambda kv: kv[0]
        ):
            ranked = sorted(
                prs,
                key=lambda p: (verdicts[p["number"]][0], -verdicts[p["number"]][1]),
                reverse=True,
            )
            best = ranked[0]
            bp, bf, _ = verdicts[best["number"]]
            for i, pr in enumerate(ranked):
                p, f, _ = verdicts[pr["number"]]
                tag = " **recommended**" if i == 0 and (p > 0 or len(ranked) == 1) else ""
                crown = "🏆 " if i == 0 and p > 0 else ""
                lines.append(
                    f"- {crown}[#{pr['number']}]({pr['url']}) `{short(pr['title'], 60)}` "
                    f"by `{pr['bot']}` — ✅ {p} / ❌ {f}{tag} (issue #{issue})"
                )
            lines.append("")
    else:
        lines.append("_No competing solutions for the same issue right now._")
        lines.append("")

    lines.append("## Open bot pull requests")
    lines.append("")
    if bot_prs:
        lines.append("| PR | Bot | Env | Eval | Cross-validation |")
        lines.append("|----|-----|-----|------|------------------|")
        for pr in sorted(bot_prs, key=lambda p: p["number"], reverse=True):
            p, f, _ = verdicts[pr["number"]]
            lines.append(
                f"| [#{pr['number']}]({pr['url']}) | `{short(pr['bot'], 24)}` "
                f"| {short(pr['env'], 22)} | {short(pr['eval'], 28)} | ✅ {p} / ❌ {f} |"
            )
    else:
        lines.append("_No open bot PRs._")
    lines.append("")

    lines.append("## Bot leaderboard")
    lines.append("")
    stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # bot -> [prs, passes]
    for pr in bot_prs:
        stats[pr["bot"]][0] += 1
        stats[pr["bot"]][1] += verdicts[pr["number"]][0]
    if stats:
        lines.append("| Bot | Open PRs | Validations passed |")
        lines.append("|-----|----------|--------------------|")
        for bot, (n_prs, n_pass) in sorted(
            stats.items(), key=lambda kv: (kv[1][1], kv[1][0]), reverse=True
        ):
            lines.append(f"| `{short(bot, 28)}` | {n_prs} | {n_pass} |")
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


def mark_recommended(bot_prs: list[dict], verdicts: dict[int, tuple[int, int, list[str]]]) -> None:
    """Label the leading PR of each competing group; clear stale labels."""
    repo = repo_path()
    groups: dict[str, list[dict]] = defaultdict(list)
    for pr in bot_prs:
        if pr["fixes"]:
            groups[pr["fixes"]].append(pr)
    winners: set[int] = set()
    for prs in groups.values():
        if len(prs) < 2:
            continue
        ranked = sorted(
            prs,
            key=lambda p: (verdicts[p["number"]][0], -verdicts[p["number"]][1]),
            reverse=True,
        )
        best_p = verdicts[ranked[0]["number"]][0]
        if best_p > 0:
            winners.add(ranked[0]["number"])

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
    verdicts = {pr["number"]: tally_verdicts(pr["number"], comments[pr["number"]]) for pr in bot_prs}
    board = render(bot_prs, verdicts)
    url = upsert_board_issue(board)
    mark_recommended(bot_prs, verdicts)
    print(f"Board updated: {url}")
    print(f"Bot PRs: {len(bot_prs)}")


if __name__ == "__main__":
    main()
