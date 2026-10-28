#!/usr/bin/env python3
"""A commit that references a module only the working tree has cannot build.

Why this is a gate and not a review item: on 2026-10-29 a `git commit -- <paths>`
in the shared tree carried one session's `use crate::test_support::scrape;` while
the file that defines it was still untracked in another session's checkout. The
working tree compiled -- the file was on disk -- and the push went out with a
tree that did not: CI failed on `unresolved import crate::test_support` and the
deployment driver stopped rolling at the last green revision for about an hour.
Nothing local could see it: the pre-push suite does not compile (Check is a CI
job) and even a local `cargo check` reads the working tree, not the commit.

So this judges the one thing that a working tree cannot vouch for. It takes the
`crate::<name>` paths the push *adds* and asks the pushed tip's own tree to
answer each one:

  * a module file -- `src/<name>.rs`, `src/<name>/mod.rs`, and the same under
    `tests/`, since an integration test's `crate::` is the test binary's root;
  * a `mod <name>` declaration anywhere under `src/` or `tests/` (this is what
    covers `#[path = "..."] mod <name>;` and modules declared in a submodule);
  * a root item -- `struct`/`enum`/`trait`/`type`/`fn`/`const`/`static`/
    `union`/`macro_rules!` named `<name>`;
  * a root `use` whose path or alias ends in `<name>` -- how a re-exported
    module from another crate is reached.

Judged by shape, not by file: comments and string literals are dropped before
the tokens are taken, because tests quote `crate::x::y` in prose and in fixtures,
and a quote is not a reference. The population is the net diff `base..head`, so a
reference added and removed inside the same push is not reported twice, and a
reference added by an earlier commit of the same push is answered by a module a
later commit of that push adds -- which is what CI reads.

Census before it became a gate: over `origin/main~200..origin/main` (200 commits,
600-odd changed `.rs` files) it named exactly one unresolvable reference, and that
one is the 2026-10-29 break above. Two earlier drafts were rejected by that same
census -- one did not know about `tests/common` (4 hits, all legitimate
`mod common;` declarations), one read `crate::` inside doc strings (2 hits, both
prose about another crate's module). Both are now part of the resolver.

What it does not judge, and cannot: a module *deleted* while something still
declares or imports it -- those references are not in the added lines, and the
Check job reads them. Neither does it judge dependencies that are not `crate::`
paths (`include_str!`, a fixture filename, a chart value). It is the cheap local
half of Check's most common red in a shared tree, not a replacement for it.

Exit codes: 0 the push's added references all resolve; 1 at least one does not;
2 the question could not be asked (bad range, not a git tree) -- an unasked
question is never green.
"""

import argparse
import re
import subprocess
import sys

TOKEN = re.compile(r"crate::([a-z_][a-z0-9_]*)")
CRATE_PATH = re.compile(r"(crates/[^/]+)/")
ROOT_ITEM = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?"
    r"(?:struct|enum|trait|type|fn|const|static|union|mod|macro_rules!)\s+(\w+)",
    re.M,
)
ROOT_USE = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?use\s+([^;]+);", re.M)


def run(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=False)


def git(*args):
    return run(*args).stdout


def strip_non_code(text):
    """Drop comments and string/char literals, keeping line structure.

    A path written in a test's prose or in a fixture string is not a reference;
    without this the census named two legitimate commits (2026-10-29) whose only
    sin was quoting another crate's module in a doc comment.
    """
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            depth, i = 1, i + 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    out.append("\n" if text[i] == "\n" else " ")
                    i += 1
        elif c == "r" and re.match(r'r#*"', text[i:]):
            m = re.match(r'r(#*)"', text[i:])
            hashes, i = m.group(1), i + len(m.group(0))
            close = '"' + hashes
            j = text.find(close, i)
            i = n if j < 0 else j + len(close)
            out.append(" ")
        elif c == '"':
            i += 1
            while i < n:
                if text[i] == "\\":
                    i += 2
                elif text[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            out.append(" ")
        elif c == "'" and re.match(r"'(\\.|[^\\'])'", text[i:]):
            i += len(re.match(r"'(\\.|[^\\'])'", text[i:]).group(0))
            out.append(" ")
        else:
            out.append(c)
            i += 1
    return "".join(out)


def resolves(rev, crate, token):
    for candidate in (
        f"{crate}/src/{token}.rs",
        f"{crate}/src/{token}/mod.rs",
        f"{crate}/tests/{token}.rs",
        f"{crate}/tests/{token}/mod.rs",
    ):
        if run("cat-file", "-e", f"{rev}:{candidate}").returncode == 0:
            return True
    declared = git(
        "grep",
        "-E",
        rf"^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?mod[[:space:]]+{token}\b",
        rev,
        "--",
        f"{crate}/src",
        f"{crate}/tests",
    )
    if declared.strip():
        return True
    for root in ("lib.rs", "main.rs"):
        text = git("show", f"{rev}:{crate}/src/{root}")
        if not text:
            continue
        if token in {m.group(1) for m in ROOT_ITEM.finditer(text)}:
            return True
        for m in ROOT_USE.finditer(text):
            if token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", m.group(1)):
                return True
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", default="origin/main", help="the revision being pushed onto")
    ap.add_argument("--head", default="HEAD", help="the tip being pushed")
    args = ap.parse_args()

    if run("rev-parse", "--verify", "--quiet", f"{args.base}^{{commit}}").returncode != 0:
        print(f"UNJUDGED  base {args.base} 不是一个提交：这一跑没有判任何东西", file=sys.stderr)
        return 2
    if run("rev-parse", "--verify", "--quiet", f"{args.head}^{{commit}}").returncode != 0:
        print(f"UNJUDGED  head {args.head} 不是一个提交", file=sys.stderr)
        return 2

    tip = git("rev-parse", args.head).strip()
    diff = run("diff", "-U0", "--no-color", "--no-renames", args.base, args.head)
    if diff.returncode != 0:
        print(f"UNJUDGED  取不到 {args.base}..{args.head} 的差异：{diff.stderr.strip()}", file=sys.stderr)
        return 2

    added = {}
    current = None
    for line in diff.stdout.splitlines():
        if line.startswith("+++ b/"):
            current = line[6:]
        elif line.startswith("+") and current and current.endswith(".rs"):
            added.setdefault(current, []).append(line[1:])

    references = {}
    for path, lines in added.items():
        m = CRATE_PATH.match(path)
        if not m:
            continue
        for token in TOKEN.findall(strip_non_code("\n".join(lines))):
            references.setdefault((m.group(1), token), set()).add(path)

    unresolved = []
    for (crate, token), paths in sorted(references.items()):
        if not resolves(tip, crate, token):
            unresolved.append((crate, token, sorted(paths)))

    for crate, token, paths in unresolved:
        print(
            f"RED  {crate} 新增了 crate::{token} 的引用，但 {tip[:9]} 这棵树里回答不了它：",
            file=sys.stderr,
        )
        for path in paths:
            print(f"       {path}", file=sys.stderr)
        print(
            "     多半是有个模块文件还停在工作树里没进这一笔；把定义它的文件一起提交，"
            "或把这一行去掉。",
            file=sys.stderr,
        )

    print(
        f"{args.base}..{args.head}（{tip[:9]}）：判了 {len(added)} 个改动的 .rs 文件、"
        f"{len(references)} 个新增的 crate:: 引用，{len(unresolved)} 个这棵树回答不了。"
    )
    return 1 if unresolved else 0


sys.exit(main())
