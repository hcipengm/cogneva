#!/usr/bin/env python3
"""Comment cites a test file -- the citation must name one the tree carries.

A comment (or a test header) that names a sibling test is how a reader is told
"this is checked over there". Rename or retire the named file and the sentence
keeps pointing into empty space: the reader finds no such file, and nothing
notices, because the citation is prose. Measured 2026-11-20 across the tracked
tree: 98 citations, 97 resolve, 1 did not (a fixture path in a comment that was
itself about fixtures; it now names the gate the sentence is about).

What counts (closed on purpose): a `*_test.rs` / `*_contract_test.rs` name and
a `*.test.sh` name appearing on a comment line -- `//`, `/*`, `///`, `//!` or a
block-comment continuation `*` in a Rust source; a `#` line (not a shebang) in
a shell script. Code lines are not read, and the boundary is stated rather than
left to be discovered: a shell command that runs a test fails loudly the day
the test is renamed, so it needs no reader; a comment does not.

Resolution is deliberately generous: a citation may be spelled relative to the
repository root, relative to the file that cites it, or as a bare basename of
some tracked file. Any of the three is live; a token resolved by none is red.

Why the population stops at test files, measured rather than assumed: over the
same comment lines, 202 citations of a `scripts/*.sh` or `scripts/*.py` path
resolve 194 times and miss 8 -- three name `scripts/host/*` scripts that live in
the operations repository, one names a private gate as the rule read here, and
four are hypothetical paths in prose (`check-x.sh`, `check-something.sh`,
`check-via-script.sh`). A hypothetical path is a legal shape and carries no
same-line marker to declare itself, so that family cannot be judged line by
line. A test file name in a comment is always a claim about this tree.

A deliberately dead citation (a retirement note, "the old `x_test.rs`") cannot
be told from a stale one by shape. The fix is a marker on the same line --
已退休 / 已删 / 旧路径 / 待建 -- the convention the tree's other citation gates
use. The marker covers its own line only; there is no exemption table.

Two population guards, because two different instruments can go blind and both
read as a clean run. "Read zero comment lines" and "nothing cites a missing
test" hand back the same answer, so the comment-line count has a floor; and "the
token pattern stopped matching" and "no comment names a test" do too, so the
citation count has one of its own. A defaulted run below either floor exits 3
rather than 0.

Usage: check-comment-test-references.py [file ...]   (run from inside the tree)
"""
import os
import re
import subprocess
import sys

TOKEN = re.compile(r"[A-Za-z0-9_./-]+\.test\.sh|[A-Za-z0-9_./-]*_test\.rs")
MARKER = re.compile(r"已退休|已删|旧路径|待建|retired")
RS_COMMENT = re.compile(r"^[ \t]*(?://|/\*|\*)")
SH_COMMENT = re.compile(r"^[ \t]*#(?:[^!]|$)")
# 今天这棵树 189247 行注释；下限沿用同族 check-comment-doc-references.sh 的口径与
# 档位——它判的也是「读者有没有读到东西」，不是「这里该有多少注释」。取整数量级
# 1e4：真读到东西时差着一个数量级，读者坏了（匹配式改窄、人口选空）时落在它下面。
COMMENT_FLOOR = 10000
# 今天 98 处引用；下限取四分之一（24），与 check-comment-capacity-figures.sh 同
# 一口径——那条门的注释写明了为什么零处必须红：「每一处都标明了归属」与「一处都
# 没读到」印的是同一句话。匹配式被改窄时交回来的正是零处。
CITE_FLOOR = 24


def git(*args):
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=False
    )


def comment_line(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        for lineno, line in enumerate(fh, 1):
            if path.endswith(".rs"):
                hit = RS_COMMENT.match(line)
            else:
                hit = SH_COMMENT.match(line)
            if hit:
                yield lineno, line.rstrip("\n")


def main(argv):
    top = git("rev-parse", "--show-toplevel")
    if top.returncode != 0:
        print("FAIL: not inside a git work tree: 读不到树就判不了引用", file=sys.stderr)
        return 3
    root = top.stdout.strip()
    os.chdir(root)

    defaulted = not argv
    if argv:
        files = [os.path.abspath(f) for f in argv]
    else:
        listing = git("ls-files", "*.rs", "*.sh")
        if listing.returncode != 0 or not listing.stdout.strip():
            print("FAIL: 树里一个 Rust 源文件或 shell 脚本都没有——没有人口就没有判据",
                  file=sys.stderr)
            return 3
        files = [os.path.join(root, p) for p in listing.stdout.split()]

    tracked = git("ls-files")
    if tracked.returncode != 0:
        print("FAIL: git ls-files 读不出（退出 %d）" % tracked.returncode, file=sys.stderr)
        return 3
    basenames = {os.path.basename(p) for p in tracked.stdout.split()}

    citations = 0
    marked = 0
    comments = 0
    missing = []
    for path in files:
        try:
            for lineno, line in comment_line(path):
                comments += 1
                tokens = TOKEN.findall(line)
                citations += len(tokens)
                if MARKER.search(line):
                    marked += len(tokens)
                    continue
                for tok in tokens:
                    t = tok[2:] if tok.startswith("./") else tok
                    if os.path.exists(os.path.join(root, t)):
                        continue
                    if os.path.exists(os.path.join(os.path.dirname(path), t)):
                        continue
                    if os.path.basename(t) in basenames:
                        continue
                    missing.append(
                        (os.path.relpath(path, root), lineno, tok)
                    )
        except OSError as exc:
            print("FAIL: 读不出 %s：%s——没读到就判不了「引用指向哪儿」" % (path, exc),
                  file=sys.stderr)
            return 3

    if defaulted and comments < COMMENT_FLOOR:
        print(
            "FAIL: %d 个文件里只读到 %d 行注释（下限 %d）。零行注释与「整棵树没有一处"
            "引用测试」在输出上同形——这一跑什么都没判。" % (len(files), comments, COMMENT_FLOOR),
            file=sys.stderr,
        )
        return 3

    if defaulted and citations < CITE_FLOOR:
        print(
            "FAIL: %d 行注释里只读到 %d 处测试引用（下限 %d）。「没有一处引用测试」与"
            "「匹配式不再认得出引用」在输出上同形——这一跑什么都没判。"
            % (comments, citations, CITE_FLOOR),
            file=sys.stderr,
        )
        return 3

    if missing:
        print("注释点名的测试文件不在树上。读注释的人跟不过去——", file=sys.stderr)
        print("修法是把名字改成树里的那一份；确实故意点名一个已不在的测试，"
              "在同一行标「已退休」「已删」「旧路径」「待建」「retired」。命中：",
              file=sys.stderr)
        for path, lineno, tok in missing:
            print("%s:%d: %s" % (path, lineno, tok), file=sys.stderr)
        return 1

    judged = citations - marked
    print(
        "OK: %d 个文件的 %d 行注释里 %d 处测试引用（%d 处判了、%d 处标了原因不判），"
        "判的全部落在树上" % (len(files), comments, citations, judged, marked)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
