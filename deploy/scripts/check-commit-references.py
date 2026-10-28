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

So this judges the one thing that a working tree cannot vouch for: what the push
*adds* has to be answerable by the pushed tip's own tree. Two kinds of added
bytes are obligations, and both are read after comments and string literals are
dropped, because tests quote `crate::x::y` in prose and in fixtures and a quote
is not a reference:

  * a `crate::<name>` path. The tree answers it with a module file -- `src/
    <name>.rs`, `src/<name>/mod.rs`, or the same under `tests/`, since an
    integration test's `crate::` is the test binary's root -- or a `mod <name>`
    declaration anywhere under `src/` or `tests/` (this covers `#[path = "..."]`
    and modules declared in a submodule), or a root item (`struct`/`enum`/
    `trait`/`type`/`fn`/`const`/`static`/`union`/`macro_rules!`) named `<name>`,
    or a root `use` whose path or alias ends in `<name>` -- how a re-exported
    module from another crate is reached.

  * a `mod <name>;` declaration, `pub` or not. A declaration is a promise that
    the file exists, and taking it as proof of itself is the hole the first
    draft had: a push that adds `mod orphan;` and not `orphan.rs` compiled
    nowhere, and so did a push that added `crate::orphan` together with the
    declaration that was supposed to answer it (found 2026-10-29 by the session
    watching the rollout, with fixtures). The file is looked for as
    `src/<name>.rs` or `src/<name>/mod.rs` anywhere under the crate -- not only
    beside the declaring file, because a `mod x;` inside `mod inline { ... }`
    resolves to `.../inline/x.rs` and a narrower rule would red legitimate code
    -- unless the declaration carries `#[path = "..."]`, which is resolved
    against the declaring file's own directory and may point outside the crate
    (`#[path = "../../cogneva/tests/common/x.rs"]`).

The population is the net diff `base..head`, so a reference added and removed
inside the same push is not reported, and a reference added by an earlier commit
of the push is answered by a module a later commit of the same push adds --
which is what CI reads.

Census before it became a gate: over `origin/main~200..origin/main` (200 commits,
600-odd changed `.rs` files) it named exactly one unresolvable reference, and
that one is the 2026-10-29 break above. Two earlier drafts were rejected by that
same census -- one did not know about `tests/common` (4 hits, all legitimate
`mod common;` declarations), one read `crate::` inside doc strings (2 hits, both
prose about another crate's module). Both are now part of the resolver. The
declaration half was added afterwards and is covered by fixtures rather than by
a second census: it can only fire on a push that adds a declaration, and the
history it would be censused over is the one above.

What it does not judge, and cannot: a module *deleted* while something still
declares or imports it -- neither the declaration nor the reference is in the
added lines -- and dependencies that are not `crate::` paths (`include_str!`, a
fixture filename, a chart value). Those stay the Check job's to catch. This is
the cheap local half of Check's most common red in a shared tree, not a
replacement for it.

Exit codes: 0 everything the push adds resolves; 1 something does not; 2 the
question could not be asked (bad range, not a git tree) -- an unasked question
is never green.
"""

import argparse
import posixpath
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
MOD_DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([a-z_][a-z0-9_]*)\s*;")


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


class Tip:
    """The pushed tree, read once per question it is asked."""

    def __init__(self, rev):
        self.rev = rev
        self._files = {}
        self._trees = {}

    def file(self, path):
        if path not in self._files:
            self._files[path] = git("show", f"{self.rev}:{path}")
        return self._files[path]

    def tree(self, crate):
        if crate not in self._trees:
            self._trees[crate] = [
                p for p in git("ls-tree", "-r", "--name-only", self.rev, "--", crate).splitlines() if p
            ]
        return self._trees[crate]

    def exists(self, path):
        return run("cat-file", "-e", f"{self.rev}:{path}").returncode == 0

    def answers(self, crate, token):
        for candidate in (
            f"{crate}/src/{token}.rs",
            f"{crate}/src/{token}/mod.rs",
            f"{crate}/tests/{token}.rs",
            f"{crate}/tests/{token}/mod.rs",
        ):
            if self.exists(candidate):
                return True
        declared = git(
            "grep",
            "-E",
            rf"^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?mod[[:space:]]+{token}\b",
            self.rev,
            "--",
            f"{crate}/src",
            f"{crate}/tests",
        )
        if declared.strip():
            return True
        for root in ("lib.rs", "main.rs"):
            text = self.file(f"{crate}/src/{root}")
            if not text:
                continue
            if token in {m.group(1) for m in ROOT_ITEM.finditer(text)}:
                return True
            for m in ROOT_USE.finditer(text):
                if token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", m.group(1)):
                    return True
        return False

    def declaration_target(self, path, name):
        """Where a `mod <name>;` in `path` says its file is, or None if anywhere.

        A `#[path = "..."]` is resolved against the declaring file's directory
        and may leave the crate. Without one, the answer is a file of that name
        anywhere in the crate: taking the declaration itself as proof is what
        let `mod orphan;` through, and requiring it to sit beside the declaring
        file would red the legitimate `mod x;` written inside an inline module.
        """
        text = self.file(path)
        if not text:
            return None
        pattern = re.compile(
            rf'#\[path\s*=\s*"([^"]+)"\][\s\S]{{0,200}}?(?:pub(?:\([^)]*\))?\s+)?mod\s+{name}\s*;'
        )
        m = pattern.search(text)
        if m:
            return posixpath.normpath(posixpath.join(posixpath.dirname(path), m.group(1)))
        return None

    def has_module_file(self, crate, path, name):
        target = self.declaration_target(path, name)
        if target is not None:
            return self.exists(target)
        return any(
            p.endswith(f"/{name}.rs") or p.endswith(f"/{name}/mod.rs") for p in self.tree(crate)
        )


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

    tip = Tip(git("rev-parse", args.head).strip())
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
    declarations = {}
    for path, lines in added.items():
        m = CRATE_PATH.match(path)
        if not m:
            continue
        crate = m.group(1)
        code = strip_non_code("\n".join(lines))
        for token in TOKEN.findall(code):
            references.setdefault((crate, token), set()).add(path)
        for name in {m.group(1) for m in map(MOD_DECL.match, code.splitlines()) if m}:
            declarations.setdefault((crate, name), set()).add(path)

    failures = 0
    for (crate, token), paths in sorted(references.items()):
        if tip.answers(crate, token):
            continue
        failures += 1
        print(
            f"RED  {crate} 新增了 crate::{token} 的引用，但 {tip.rev[:9]} 这棵树里回答不了它：",
            file=sys.stderr,
        )
        for path in sorted(paths):
            print(f"       {path}", file=sys.stderr)
    for (crate, name), paths in sorted(declarations.items()):
        if all(tip.has_module_file(crate, path, name) for path in paths):
            continue
        failures += 1
        print(
            f"RED  {crate} 新增了 `mod {name};`，但 {tip.rev[:9]} 这棵树里找不到 "
            f"{name}.rs / {name}/mod.rs：",
            file=sys.stderr,
        )
        for path in sorted(paths):
            print(f"       {path}", file=sys.stderr)
    if failures:
        print(
            "     多半是有个模块文件还停在工作树里没进这一笔；把定义它的文件一起提交，"
            "或把这一行去掉。",
            file=sys.stderr,
        )

    print(
        f"{args.base}..{args.head}（{tip.rev[:9]}）：判了 {len(added)} 个改动的 .rs 文件、"
        f"{len(references)} 个新增的 crate:: 引用、{len(declarations)} 个新增的 mod 声明，"
        f"{failures} 个这棵树回答不了。"
    )
    return 1 if failures else 0


sys.exit(main())
