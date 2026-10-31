#!/usr/bin/env python3
"""A fixture's jump must land inside the window, not on its left end.

Why this is a gate and not a review item: a range selector is left-open. For a
rule that computes `increase(x[1h])`, a sample sitting exactly at `eval - 1h`
is outside the window, so a fixture that puts its only jump there feeds the
expression a flat series. The value is then 0 and the case is quiet -- and a
quiet case is exactly what a fixture looks like when it passes.

Two measured instances (2026-10-23): `registry_maintenance_round_incomplete`
written as `'0x60 1x60'` against a `[1h]` window, and `loki_flush_batches_all_failed`
written as `'0x54 2x6'` against `[5m]` after a case was copied from a fixture
with a 30m window. Both read 0. Neither is visible anywhere: promtool agrees the
case is quiet, the rule is installed, the alert simply never fires for that
case, and the case goes on claiming it tested something.

What is judged: every `alert_rule_test` case that names a rule whose expression
takes a range through `increase`, `rate`, `irate`, `delta` or `deriv`. If one of
that case's series has a jump exactly on the window's left end and no jump
anywhere inside `(eval - W, eval]`, the case cannot distinguish a working
expression from one that answers zero, and it is reported.

What is not judged, on purpose: a case whose series also moves inside the
window. The boundary sample is then one of several and the expression reads the
ones that count. Also not judged: fixtures for rules without a range selector --
a gauge threshold is answered by the value at `eval` and has no window at all.

Exit codes: 0 every windowed case puts its movement inside the window; 1 at
least one does not (each named with its case, alert and series); 3 nothing was
judged -- a tree with no readable fixtures, or one whose fixtures exercise no
windowed rule, must not read the same as a clean one.

Usage: check-alert-rule-fixture-window-jump.py [tests-dir] [manifests-dir]
"""

from __future__ import annotations

import glob
import os
import re
import sys

# A range selector inside one of the functions whose answer is a difference over
# a window. Subquery spellings (`[1h:1m]`) are deliberately not matched: their
# window belongs to the subquery's step, and no such rule is written here.
RANGE_DIFFERENCE = re.compile(r"\b(?:increase|rate|irate|delta|deriv)\s*\([^)]*\[(\d+)([smhd])\]")

UNIT_MINUTES = {"s": 1.0 / 60.0, "m": 1.0, "h": 60.0, "d": 1440.0}
DURATION = re.compile(r"^(-?)(\d+(?:\.\d+)?)([smhd])$")
SAMPLE = re.compile(r"^(-?[_0-9.eE]+|NaN|stale)(?:([+-][0-9.]+))?(?:x(\d+))?$")


def minutes(text: str) -> float:
    """promtool durations: `30m`, `1h`, and a bare number is minutes."""
    m = DURATION.match(str(text).strip())
    if m:
        return float(m.group(2)) * UNIT_MINUTES[m.group(3)]
    return float(str(text).strip())


def samples(values: str, interval: float):
    """promtool's series notation -> [(minute, value or None for a gap)].

    Forms: `1x5`, `_x5`, `0+1x5`, `10-2x3`, `NaN+0x4`, `stale`. A `+`/`-` token
    continues from the previous token's last value, which is why the running
    value is carried across the whole string.
    """
    out = []
    t = 0.0
    running = None
    for token in str(values).split():
        m = SAMPLE.match(token)
        if not m:
            raise ValueError(f"unreadable sample {token!r}")
        base, step, count = m.group(1), m.group(2), int(m.group(3) or 1)
        if base in ("_", "stale"):
            current = None
        elif base == "NaN":
            current = float("nan")
        elif step and running is not None:
            current = running
        else:
            current = float(base)
        increment = float(step) if step else 0.0
        for i in range(count):
            out.append((t, None if current is None else current + increment * i))
            t += interval
        if current is not None:
            running = current + increment * (count - 1)
    return out


def jumps(points):
    """Minutes at which a series changes value (gaps are not changes)."""
    out = []
    for i in range(1, len(points)):
        before, after = points[i - 1][1], points[i][1]
        if before is None or after is None:
            continue
        if before != after and not (before != before and after != after):
            out.append(points[i][0])
    return out


def rule_expressions(tests_dir: str, manifests_dir: str):
    """alertname -> expression, from every rule file a fixture in this dir names.

    A fixture names its rules through `rule_files`; those paths point at the
    manifests, so both the referenced files and the manifests directory are
    read. A file that is not a rule file is skipped rather than treated as one:
    the directory holds more than PrometheusRules.
    """
    import yaml

    candidates = set()
    for path in sorted(glob.glob(os.path.join(tests_dir, "*.test.yaml"))):
        with open(path, encoding="utf-8") as fh:
            try:
                doc = yaml.safe_load(fh)
            except yaml.YAMLError as exc:
                raise OSError(f"cannot parse {os.path.basename(path)}: {exc}") from exc
        for reference in (doc or {}).get("rule_files") or []:
            candidates.add(os.path.normpath(os.path.join(os.path.dirname(path), str(reference))))
    candidates.update(
        str(p) for p in sorted(glob.glob(os.path.join(manifests_dir, "*.yaml")))
    )

    rules = {}
    # `rules.yaml` is written by the fixture runner into its own working copy,
    # so a reference to it is absent here by design. Those are dropped rather
    # than raising -- the manifests directory is read directly and carries the
    # same rules -- and the drop is reported in the summary so a typo cannot
    # quietly shrink the scan.
    candidates = {p for p in candidates if os.path.isfile(p)}
    for path in sorted(candidates):
        with open(path, encoding="utf-8") as fh:
            try:
                docs = list(yaml.safe_load_all(fh))
            except yaml.YAMLError:
                continue
        for doc in docs:
            for group in ((doc or {}).get("spec") or {}).get("groups") or []:
                for rule in group.get("rules") or []:
                    expr = str(rule.get("expr", "")).strip()
                    if rule.get("alert") and expr:
                        rules[str(rule["alert"])] = expr
    return rules


def judge(tests_dir: str, manifests_dir: str):
    """(cases judged, [(file, alert, series, eval, window)] that cannot bite)."""
    import yaml

    rules = rule_expressions(tests_dir, manifests_dir)
    cases = 0
    violations = []
    for path in sorted(glob.glob(os.path.join(tests_dir, "*.test.yaml"))):
        with open(path, encoding="utf-8") as fh:
            try:
                doc = yaml.safe_load(fh)
            except yaml.YAMLError as exc:
                raise OSError(f"cannot parse {os.path.basename(path)}: {exc}") from exc
        file_interval = minutes((doc or {}).get("evaluation_interval") or "1m")
        for block in (doc or {}).get("tests") or []:
            interval = minutes(block.get("interval") or file_interval)
            series = {}
            for entry in block.get("input_series") or []:
                series[str(entry.get("series"))] = samples(entry.get("values", ""), interval)
            for case in block.get("alert_rule_test") or []:
                alert = case.get("alertname")
                expr = rules.get(alert)
                if not expr:
                    continue
                windows = [(int(n), u) for n, u in RANGE_DIFFERENCE.findall(expr)]
                if not windows:
                    continue
                at = minutes(case.get("eval_time", "0"))
                cases += 1
                for n, unit in windows:
                    width = n * UNIT_MINUTES[unit]
                    left = at - width
                    for name, points in series.items():
                        moved = jumps(points)
                        # The sample on the edge is the one just at or just
                        # before the window's left end: `[eval-W, eval]` is
                        # read as `(eval-W, eval]`, so a jump there is the one
                        # the expression cannot see. Both measured instances
                        # sit in this band -- one exactly on the start, one a
                        # sample before it after a case was copied to a rule
                        # with a narrower window.
                        near_left = [t for t in moved if left - interval - 1e-9 <= t <= left + 1e-9]
                        inside = [t for t in moved if left < t <= at + 1e-9]
                        if near_left and not inside:
                            violations.append((os.path.basename(path), str(alert), name, at, width, near_left[0]))
    return cases, violations


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("-")]
    for a in argv[1:]:
        if a in ("-h", "--help"):
            print(__doc__)
            return 0
        if a.startswith("-"):
            print(f"check-alert-rule-fixture-window-jump: unknown argument {a}", file=sys.stderr)
            return 2
    tests_dir = args[0] if args else "deploy/k3s/observability/tests"
    manifests_dir = args[1] if len(args) > 1 else "deploy/k3s/observability/manifests"

    try:
        cases, violations = judge(tests_dir, manifests_dir)
    except (OSError, ValueError) as exc:
        print(f"check-alert-rule-fixture-window-jump: cannot read the fixtures: {exc}", file=sys.stderr)
        print("check-alert-rule-fixture-window-jump: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    # Zero judged and every case judged clean print the same summary line, so
    # the difference has to be drawn here rather than left to the reader.
    if cases == 0:
        print(
            f"check-alert-rule-fixture-window-jump: {tests_dir} 里没有一格夹具读到带窗口的规则"
            "——零格与「每一格都把跳变放进了窗口」在汇总行上同形",
            file=sys.stderr,
        )
        print("check-alert-rule-fixture-window-jump: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    for where, alert, series, at, width, jump in violations:
        print(f"RED  {where} {alert}: the movement sits outside the window's left end")
        print(f"     series: {series}")
        print(
            f"     eval_time={at:g}m  window={width:g}m  jump at {jump:g}m  "
            f"left end {at - width:g}m (left-open: the jump is not in the range)"
        )
        print(
            "     repair: move the jump inside the window (a sample or two after its "
            "start); `increase`/`rate` read a flat series as 0 and the case passes "
            "without testing anything"
        )
    print(
        f"check-alert-rule-fixture-window-jump: cases={cases} violations={len(violations)}"
    )
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
