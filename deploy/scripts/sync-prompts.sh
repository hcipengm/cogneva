#!/usr/bin/env bash
# Propagate `prompts/*.yaml` into every carrier that ships them.
#
# The same prompt text lives in four places once it is deployed: the repo-level
# source (`prompts/`), the Helm chart's `files/` copies, the k3s ConfigMap that
# embeds them, and the rendered manifests generated from those. Only the first is
# authored; the other three are copies, and a copy that stops being one is how
# the live instruction and the declaration drift apart — the k3s copy is what a
# running process reads, the Helm copy is what a chart install reads, and nothing
# in either file says which of the two is current.
#
# So this script owns the copies: `--check` fails when any carrier is not
# byte-identical to its source, and the gate runs it in that mode. The edit goes
# into `prompts/`, then `sync-prompts.sh` writes the carriers, then
# `render-deploy.sh` refreshes `deploy/rendered/` (those are build products of
# the k3s/Helm manifests, not of this script).
#
#   sync-prompts.sh            write the carriers from the sources
#   sync-prompts.sh --check    report drift, exit 1, write nothing
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"

mode="write"
case "${1:-}" in
  --check) mode="check" ;;
  "") ;;
  *)
    echo "usage: $(basename "$0") [--check]" >&2
    exit 2
    ;;
esac

python3 - "${repo}" "${mode}" <<'PY'
import pathlib
import re
import sys

repo = pathlib.Path(sys.argv[1])
check_only = sys.argv[2] == "check"

src_dir = repo / "prompts"
helm_dir = repo / "deploy/helm/cogneva/files"
helm_tpl = repo / "deploy/helm/cogneva/templates/app-config.yaml"
k3s_path = repo / "deploy/k3s/prompts-configmap.yaml"

sources = sorted(p for p in src_dir.glob("*.yaml") if p.is_file())
if not sources:
    sys.exit("sync-prompts: no prompt sources under prompts/*.yaml")

# Which Helm file carries which source is declared by the chart template itself:
# the ConfigMap's data key is the file name the process will read, and the
# `Files.Get` beside it is the copy that ships. Read the mapping from there so a
# new source cannot quietly go unshipped — an unmapped source is an error, not a
# file that renders while its prompt stays behind.
tpl = helm_tpl.read_text()
helm_copy = {}
pair = re.compile(r'^  (\S+\.ya?ml): \|-\n\{\{ \.Files\.Get "files/([^"]+)" \| indent 4 \}\}$', re.M)
for data_key, path in pair.findall(tpl):
    helm_copy[data_key] = helm_dir / pathlib.Path(path).name

missing = [p.name for p in sources if p.name not in helm_copy]
if missing:
    sys.exit(
        "sync-prompts: no Helm data entry for "
        + ", ".join(missing)
        + " in deploy/helm/cogneva/templates/app-config.yaml"
    )

drift = []


def blocked(text: str, key: str):
    """Return (start, end) line indices of the `key: |` block scalar's body."""
    lines = text.split("\n")
    head = re.compile(r"^  " + re.escape(key) + r": \|\s*$")
    for i, line in enumerate(lines):
        if not head.match(line):
            continue
        j = i + 1
        while j < len(lines) and (lines[j].startswith("    ") or lines[j].strip() == ""):
            # A blank line ends the block only if the next non-blank line is not
            # part of it; YAML block scalars keep blank lines, so look ahead.
            if lines[j].strip() == "":
                k = j
                while k < len(lines) and lines[k].strip() == "":
                    k += 1
                if k >= len(lines) or not lines[k].startswith("    "):
                    break
            j += 1
        # Trim trailing blank lines that belong to the file's layout, not the body.
        end = j
        while end > i + 1 and lines[end - 1].strip() == "":
            end -= 1
        return i, end
    return None


def indent(content: str, pad: str = "    ") -> str:
    body = content.split("\n")
    if body and body[-1] == "":
        body.pop()  # a `|` block scalar carries exactly one trailing newline
    return "\n".join((pad + line) if line else "" for line in body)


def apply(path: pathlib.Path, wanted: str) -> None:
    if path.read_text() == wanted:
        return
    if check_only:
        drift.append(path)
        return
    path.write_text(wanted)


# 1. The Helm chart reads its copies straight off disk.
for src in sources:
    apply(helm_copy[src.name], src.read_text())

# 2. The k3s ConfigMap embeds each file as a block scalar under `data:`.
k3s = k3s_path.read_text()
original = k3s
for src in sources:
    span = blocked(k3s, src.name)
    if span is None:
        sys.exit(f"sync-prompts: {k3s_path} has no `  {src.name}: |` block")
    start, end = span
    lines = k3s.split("\n")
    rest = lines[end:]
    while rest and rest[0].strip() == "":
        rest.pop(0)
    # One blank line between the block and whatever follows, always: the file is
    # rewritten from its own text, so any looseness here accumulates a blank line
    # per run instead of converging.
    body = indent(src.read_text()).split("\n")
    k3s = "\n".join(lines[: start + 1] + body + [""] + rest)
apply(k3s_path, k3s)

if drift:
    print("sync-prompts: carriers are not byte-identical to their sources:", file=sys.stderr)
    for path in drift:
        print(f"  {path.relative_to(repo)}", file=sys.stderr)
    print("run deploy/scripts/sync-prompts.sh and commit the result", file=sys.stderr)
    sys.exit(1)

print(f"sync-prompts: {len(sources)} source(s) in step with every carrier")
PY
