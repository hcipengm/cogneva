#!/usr/bin/env python3
"""Every gate in `deploy/scripts/` must have something that runs it.

The gates are a directory; the things that run them are a list in
`.github/workflows/ci.yml`, another list in the `install-*.sh` reconcilers, and
a third in the script bodies that call each other. Nothing compares the
directory against those lists. A gate added without a caller does not fail
loudly -- it never runs at all, and "never runs" looks exactly like "always
green". The gate that would have caught the next regression sits in the tree,
readable, cited in prose, and dead.

So this asks one question per file: name the thing that runs it. Four kinds of
carrier count, all derived from the tree rather than from a list kept here:

  ci       a workflow names it on a line that runs it
  runtime  an `install-*.sh` reconciler names it (in-cluster timer)
  script   another script under `deploy/scripts/` names it
  test     a `*.test.sh` names it -- CI globs those directories, so a test
           that names a gate is a call path into it

A gate with none of those may still be deliberate: an analysis people run by
hand (`check-before-push.sh`), a survey whose answer nobody gates on. Those say
so in a `# carrier:` line near the top, and the line is checked to exist -- a
comment is not evidence, but it is the one place the intent is recorded next to
the file, and it moves when the file is renamed.

The reverse direction is checked too: a workflow that names a gate file which
is not there. That fails at once in CI, but it fails as "command not found",
which reads like an environment problem rather than a stale registration.
"""

import pathlib
import re
import sys

DECLARED_RE = re.compile(r"^#\s*carrier:\s*(\S.*)$")
# A line "runs" a script when it also carries a shell or interpreter word.
RUNS_RE = re.compile(r"\b(?:bash|sh|python3?)\b")
# Tests hold the path in a variable instead of running it inline.
ASSIGN_RE = re.compile(r"^\s*gate=")
COMMENT_RE = re.compile(r"^\s*#")


def names_on(line, name):
    """Does this line name `name` in the form a caller writes it?"""
    return re.search(r"(?<![\w.-])" + re.escape(name) + r"(?![\w.-])", line) is not None


def carriers(text, name, inline_runs_only):
    """→ line numbers that name `name` in a way that counts as a call."""
    hits = []
    for number, line in enumerate(text.splitlines(), 1):
        if COMMENT_RE.match(line) or not names_on(line, name):
            continue
        if inline_runs_only:
            if RUNS_RE.search(line) or ASSIGN_RE.match(line):
                hits.append(number)
        else:
            hits.append(number)
    return hits


def declared(text):
    """`# carrier:` from the header comment block -- everything above the first
    line of code. The block is as long as the gate's own explanation needs to
    be, so no line count; a declaration in the body would not be a header."""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not stripped.startswith("#"):
            return None
        match = DECLARED_RE.match(stripped)
        if match and match.group(1).strip():
            return match.group(1).strip()
    return None


def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    scripts = root / "deploy" / "scripts"
    if not scripts.is_dir():
        print(f"判不了：没有 {scripts}", file=sys.stderr)
        return 2

    gates = sorted(
        p for p in scripts.iterdir()
        if p.is_file() and p.name.startswith("check-")
        and p.suffix in (".sh", ".py")
    )
    if not gates:
        print("判不了：deploy/scripts 下一个 check-* 都没有", file=sys.stderr)
        return 2

    workflows = sorted((root / ".github").rglob("*.yml"))
    installers = sorted(scripts.glob("install-*.sh"))
    others = sorted(
        p for p in scripts.iterdir()
        if p.is_file() and p.suffix in (".sh", ".py") and p not in gates
    )
    tests = sorted(p for d in ("deploy/scripts/tests", ".github/scripts/tests")
                   if (root / d).is_dir() for p in (root / d).glob("*.test.sh"))

    def read(paths):
        return [(p, p.read_text(errors="replace")) for p in paths]

    workflow_text, installer_text, other_text, test_text = (
        read(workflows), read(installers), read(others), read(tests)
    )

    missing = []
    unnamed = []
    for gate in gates:
        found = []
        for label, corpus, inline in (
            ("ci", workflow_text, True),
            ("runtime", installer_text, True),
            ("script", other_text, True),
            ("test", test_text, False),
        ):
            hit = [(p, carries) for p, text in corpus
                   if (carries := carriers(text, gate.name, inline))]
            if hit:
                found.append((label, hit[0][0], hit[0][1][0]))
        say = declared(gate.read_text(errors="replace")) if not found else None
        if found:
            print(f"ok    - {gate.name}：{', '.join(l for l, _, _ in found)}"
                  f"（{found[0][1].relative_to(root)}:{found[0][2]}）")
        elif say:
            print(f"ok    - {gate.name}：就地声明「{say}」")
        else:
            unnamed.append(gate.name)

    # 反向：CI 点名了树里没有的门禁脚本。
    for path, text in workflow_text:
        for number, line in enumerate(text.splitlines(), 1):
            for name in re.findall(r"deploy/scripts/(check-[\w.-]+\.(?:sh|py))", line):
                if not (scripts / name).is_file():
                    missing.append(f"{path.relative_to(root)}:{number} 点名 {name}，树里没有这个文件")

    if unnamed:
        print(f"\n{len(unnamed)} 个门禁没有任何东西跑它——它不会红，因为从不执行：",
              file=sys.stderr)
        for name in unnamed:
            print(f"  - {name}", file=sys.stderr)
        print("给它一个调用点（CI 作业、install-*.sh、被别的脚本或测试点名），"
              "或者在文件头写一行 `# carrier: <谁跑它>`。两类都不是的，说明它不该叫 check-。",
              file=sys.stderr)
    for line in missing:
        print(f"  陈旧的登记：{line}", file=sys.stderr)

    if unnamed or missing:
        return 1
    print(f"\n{len(gates)} 个门禁都有载体")
    return 0


if __name__ == "__main__":
    sys.exit(main())
