#!/usr/bin/env python3
"""Every check `main` requires must be one a workflow can actually report.

`main` carries a required-status-checks list. That list is stored in the
repository settings, where nothing in this tree can read it, and the tree's copy
of the list lives in `.github/required-checks.txt`. A required context that no
workflow ever publishes is never reported, so the requirement is unsatisfiable:
with `enforce_admins` off it does not block anybody, it only makes every push
echo "N of M required status checks are expected" and then land on a bypass --
red and green become indistinguishable at the entrance.

That is not hypothetical. Measured 2026-10-28 on this repository: the settings
required `Bootstrap Check (ubuntu-latest)` and its two siblings, while the job
in `.github/workflows/ci.yml` is named `Bootstrap Cross-Platform Check`. Three
of nine contexts named nothing. Three commits whose `Test` and `Code Coverage`
were red sat on `main` for an hour, and no gate objected at the entrance.

So this asks one question per declared name: name the workflow job that
publishes it. The producer side is derived from the workflow files, never from
a list kept here -- a list checked against itself is true by construction.

The reverse direction is reported and not enforced. A workflow publishes many
checks that `main` does not require (`Deploy Manifest Parity`, `Web Build`,
`build`, `deploy`); those are choices, not defects. Counting them makes the
number in the green line mean something, and it is the number a reader needs to
notice when a name moves from required to optional.

What this cannot do: read the settings. A name that is well-formed here and
absent there is enforced by nobody, and no gate in this tree can see it. That
half belongs to whatever pushes this file to GitHub.

Usage: python3 deploy/scripts/check-required-checks.py [repo root]
"""

import re
import sys
from pathlib import Path

DECLARATION = ".github/required-checks.txt"
WORKFLOW_DIR = ".github/workflows"

JOB_KEY = re.compile(r"^  ([A-Za-z0-9_.-]+):\s*$")
JOB_NAME = re.compile(r"^    name:\s*(.+?)\s*$")
STRATEGY = re.compile(r"^    strategy:\s*$")
MATRIX = re.compile(r"^      matrix:\s*$")
MATRIX_INLINE = re.compile(r"^        ([A-Za-z0-9_.-]+):\s*\[(.*)\]\s*$")
MATRIX_KEY = re.compile(r"^        ([A-Za-z0-9_.-]+):\s*$")
MATRIX_ITEM = re.compile(r"^          -\s*(.+?)\s*$")
MATRIX_REF = re.compile(r"\$\{\{\s*matrix\.([A-Za-z0-9_.-]+)\s*\}\}")
UNEXPANDABLE = re.compile(r"^        include:\s*$")


def unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def matrix_values(body: str, start: int) -> tuple[dict[str, list[str]], bool]:
    """Read the matrix block that begins at `start`.

    Returns the scalar matrix keys and their values, and whether the block held
    a form this reader cannot expand. `include:` entries are key/value maps
    whose published name is assembled from whichever keys the name references;
    rather than guess, the job is marked unexpandable and any declared name
    that lands on it is answered "cannot decide" instead of "produced".
    """
    keys: dict[str, list[str]] = {}
    unexpandable = False
    lines = body.splitlines()
    i = start
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if UNEXPANDABLE.match(line):
            unexpandable = True
            i += 1
            continue
        inline = MATRIX_INLINE.match(line)
        if inline:
            keys[inline.group(1)] = [unquote(v) for v in inline.group(2).split(",") if v.strip()]
            i += 1
            continue
        block = MATRIX_KEY.match(line)
        if block:
            values = []
            i += 1
            while i < len(lines) and MATRIX_ITEM.match(lines[i]):
                values.append(unquote(MATRIX_ITEM.match(lines[i]).group(1)))
                i += 1
            keys[block.group(1)] = values
            continue
        # Anything at job scope ends the matrix block.
        if not line.startswith("        "):
            break
        i += 1
    return keys, unexpandable


def workflow_jobs(path: Path) -> list[tuple[str, str | None, dict[str, list[str]], bool]]:
    """Every job in a workflow: its id, its declared name, its matrix.

    The published check-run name is the job's `name:` when it has one and the
    job id otherwise. A matrix job that does not reference its matrix in the
    name gets the values appended in parentheses, one entry per scalar key in
    declaration order -- that is how `Bootstrap Cross-Platform Check` becomes
    `Bootstrap Cross-Platform Check (ubuntu-latest)`.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    jobs = []
    in_jobs = False
    current = None
    for index, line in enumerate(lines):
        if line.startswith("jobs:"):
            in_jobs = True
            continue
        if in_jobs and line and not line.startswith(" "):
            in_jobs = False
        if not in_jobs:
            continue
        job = JOB_KEY.match(line)
        if job:
            current = {"id": job.group(1), "name": None, "matrix_at": None}
            jobs.append(current)
            continue
        if current is None:
            continue
        if current["name"] is None:
            named = JOB_NAME.match(line)
            if named:
                current["name"] = unquote(named.group(1))
        if MATRIX.match(line):
            current["matrix_at"] = index + 1

    produced = []
    for job in jobs:
        base = job["name"] or job["id"]
        if job["matrix_at"] is None:
            produced.append((base, path.name, False))
            continue
        keys, unexpandable = matrix_values(text, job["matrix_at"])
        referenced = MATRIX_REF.findall(base)
        if referenced:
            values_of = [keys.get(key, []) for key in referenced]
            if not values_of or any(not v for v in values_of):
                produced.append((base, path.name, True))
                continue
            combos = [""]
            for values in values_of:
                combos = [f"{c}|{v}" for c in combos for v in values]
            for combo in combos:
                filled = base
                for value in combo.split("|")[1:]:
                    filled = MATRIX_REF.sub(lambda _m, v=value: v, filled, count=1)
                produced.append((filled, path.name, False))
            continue
        scalar = [v for v in keys.values() if v]
        if unexpandable or not scalar:
            produced.append((base, path.name, True))
            continue
        combos = [""]
        for values in keys.values():
            combos = [f"{c}|{v}" for c in combos for v in values]
        for combo in combos:
            suffix = ", ".join(combo.split("|")[1:])
            produced.append((f"{base} ({suffix})", path.name, False))
    return [(name, origin, undecidable) for name, origin, undecidable in produced]


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parents[2]
    declaration = root / DECLARATION
    if not declaration.is_file():
        print(f"{DECLARATION} is missing: `main` requires checks and the tree does not say which")
        return 1

    declared = []
    for number, line in enumerate(declaration.read_text(encoding="utf-8").splitlines(), 1):
        text = line.split("#", 1)[0].strip()
        if text:
            declared.append((text, number))
    if not declared:
        print(f"{DECLARATION} declares nothing: an empty requirement list is satisfied by every commit, "
              "and reads the same as a repository with no required checks")
        return 1

    workflows = sorted((root / WORKFLOW_DIR).glob("*.yml"))
    if not workflows:
        print(f"no workflow under {WORKFLOW_DIR}: every declared check would be unproduced")
        return 1
    produced = []
    for workflow in workflows:
        produced.extend(workflow_jobs(workflow))
    exact = {name for name, _origin, undecidable in produced if not undecidable}
    undecidable = {name for name, _origin, undecidable in produced if undecidable}

    problems = []
    seen = {}
    for name, number in declared:
        if name in seen:
            problems.append(f'line {number}: "{name}" is already declared on line {seen[name]}')
            continue
        seen[name] = number
        if name in exact:
            continue
        # An undecidable job is recorded by its base name; the declared context
        # may be the base plus a platform suffix this reader cannot enumerate.
        if name in undecidable or any(
            name.startswith(f"{base} (") for base in undecidable
        ):
            problems.append(
                f'line {number}: "{name}" lands on a matrix this reader cannot expand '
                "(`include:` entries); the answer is undecided, not produced"
            )
            continue
        problems.append(
            f'line {number}: no workflow under {WORKFLOW_DIR} produces "{name}"'
            f"{suggestion(name, exact)}; workflows GitHub generates itself (Pages' "
            "build/report-build-status/deploy) have no file here and cannot be required this way"
        )

    if problems:
        print("required checks that nothing publishes -- a requirement nothing can satisfy:")
        for problem in problems:
            print(f"  {problem}")
        print(f"declared={len(declared)} produced={len(exact)}")
        return 1

    unrequired = sorted(exact - set(seen))
    print(f"declared={len(declared)} produced={len(exact)} not-required={len(unrequired)} "
          "-- every required check has a producer")
    return 0


def suggestion(name: str, produced: set[str]) -> str:
    import difflib

    close = difflib.get_close_matches(name, sorted(produced), n=2, cutoff=0.6)
    if not close:
        return ""
    return "; closest produced name: " + ", ".join(f'"{c}"' for c in close)


if __name__ == "__main__":
    sys.exit(main())
