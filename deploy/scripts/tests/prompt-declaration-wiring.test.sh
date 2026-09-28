#!/usr/bin/env bash
# Gate for the two halves of every prompt: the declaration and its reader.
#
# `prompts/*.yaml` ships as a ConfigMap that a running process reads, so an entry
# there is not documentation — it is the instruction the model receives. That
# cuts both ways, and each direction has its own failure:
#
#   * A key the code asks for but nobody declares is a silent no-op. The call
#     site has a fallback, so nothing breaks visibly; the operator who edits
#     `system_prompts.yaml` to tune that prompt tunes nothing at all.
#   * A key nobody asks for is worse than absent: it reads as live, it is
#     editable, and it drifts. Two entries in this file had already drifted away
#     from the instruction actually in force — the declaration asked for a
#     Markdown code block while the reader can only parse `diff --git`, so the
#     prompt that ran asked for a shape its own parser drops.
#
# Neither direction is visible in any other gate: the manifests stay
# self-consistent, parity stays aligned, and the tests stay green. This one
# reconciles the two sets in both directions and refuses to guess: a prompt call
# site whose key is not a literal is an error, not a line to skip, because a
# skipped line would quietly shrink the "requested" set until the reconciliation
# passes by having nothing to compare.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"

# The carriers are copies; the first thing to establish is that they are still
# copies of the sources, or the reconciliation below would be reading one text
# while the cluster runs another.
"${repo}/deploy/scripts/sync-prompts.sh" --check

python3 - "${repo}" <<'PY'
import pathlib
import re
import sys

repo = pathlib.Path(sys.argv[1])

# --- what the code asks for -------------------------------------------------
# Every prompt lookup in the workspace. The receivers are the ones that hold a
# PromptProvider (`pm`/`prompt_manager` inside the crates, the two free functions
# for the process-global manager). `crates/` including tests: one test asks for
# `agent:default` through the global manager, and a request from a test is still
# a key the file has to declare. The `(?<!fn )` keeps the free functions'
# definition out of the sweep — that line names the parameter, not a key.
CALL = re.compile(
    r"\b(?:pm|prompt_manager)\.(?:render|get)\(|(?<!fn )\bglobal_(?:prompt|render)\("
)
KEY = re.compile(r'\s*"([a-z][a-z0-9_]*:[a-z0-9_]+)"')

requested = {}
for path in sorted((repo / "crates").rglob("*.rs")):
    text = path.read_text()
    for m in CALL.finditer(text):
        tail = text[m.end() :]
        key = KEY.match(tail)
        if not key:
            line = text.count("\n", 0, m.start()) + 1
            sys.exit(
                f"prompt call site without a literal key at "
                f"{path.relative_to(repo)}:{line} — a prompt key that is not a "
                f"literal cannot be reconciled against the declaration"
            )
        requested.setdefault(key.group(1), []).append(
            f"{path.relative_to(repo)}:{text.count(chr(10), 0, m.start()) + 1}"
        )

# The extraction itself has to be shown to have found something: an empty or
# shrunken set would satisfy one direction by accident.
if len(requested) < 4:
    sys.exit(
        f"only {len(requested)} prompt keys found in crates/**/*.rs — the "
        f"extraction is broken, not the tree"
    )

# --- what the file declares -------------------------------------------------
declared = {}
for path in sorted((repo / "prompts").glob("*.yaml")):
    body = path.read_text().split("\n")
    in_prompts = False
    for line in body:
        if line.startswith("prompts:"):
            in_prompts = True
            continue
        if not in_prompts:
            continue
        m = re.match(r"^  ([a-z][a-z0-9_]*:[a-z0-9_]+):\s*$", line)
        if m:
            declared.setdefault(m.group(1), []).append(str(path.relative_to(repo)))

problems = []

for key in sorted(requested):
    if key not in declared:
        problems.append(
            f"{key} is asked for by "
            + ", ".join(requested[key])
            + " but declared in no prompts/*.yaml"
        )
for key in sorted(declared):
    if key not in requested:
        problems.append(
            f"{key} is declared in "
            + ", ".join(declared[key])
            + " but no code reads it — the entry is editable and inert"
        )
for key in sorted(declared):
    if len(declared[key]) > 1:
        problems.append(
            f"{key} is declared twice ({', '.join(declared[key])}) — "
            "the registry is a map, so one of the two is silently overwritten"
        )

if problems:
    print("prompt declarations and their readers disagree:", file=sys.stderr)
    for p in problems:
        print(f"  {p}", file=sys.stderr)
    sys.exit(1)

print(f"prompt wiring: {len(requested)} key(s) requested == declared")
PY
