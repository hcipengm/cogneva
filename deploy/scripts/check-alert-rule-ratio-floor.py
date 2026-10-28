#!/usr/bin/env python3
"""A rule that compares against a multiple of another reading must also carry a floor.

Why this is a gate and not a review item: "twice its own average" has no fixed
meaning. On a reading that moves by hundredths, a doubling of that movement is
still hundredths -- so a rule written as `rate_now > 2 * rate_lifetime` fires on
noise, and it fires forever, because the noise never stops being twice itself.
The repair is one clause: the same quantity also has to clear an absolute floor,
which is the answer to "how fast is fast enough to be an event at all".

The failure this catches is silent in both directions. Leave the floor out and
the rule is permanently red on small readings, which everyone learns to ignore.
Type the floor into the wrong side -- compare the lifetime average to a constant
-- and it is permanently quiet on the fast disks. Neither shape fails any syntax,
subject, or join check, and neither shows up as unhealthy: the expression simply
answers a different question than the one it claims to.

What this judges, by shape rather than by name (a metric called `_rate_` is a
convention, not a syntax): every rule whose expression computes a rate
(`deriv`, `rate`, `irate`) and compares it against a *scaled series*, i.e.
`> (... * 3)` or `> other_series * 0.5`, must also compare some quantity
against a bare number. Which quantity is not judged -- that is a reading of the
rule's meaning, and it belongs to whoever writes it.

Not judged on purpose: an age compared against a multiple of its own period
(`time() - ts > 6 * period_seconds`). Both sides are durations in the same
units, the period is already the floor, and a second bound would duplicate it.

Reported, not judged: every rule outside that shape. It may legitimately be a
plain `x > 90`, so the census counts it and says so rather than folding it into
the pass.

Exit codes: 0 every ratio rule carries a floor; 1 at least one does not (each
named with its expression); 3 a library could not be read -- which is not 0,
because "nothing violated" and "nothing was judged" must not look the same.

Usage: check-alert-rule-ratio-floor.py [--report] [cogneva.json] [manifest dir]
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys

# A comparison where a number sits on one side of the operator and a series on
# the other -- the shape that means "a multiple of a reading".
SERIES_TIMES_NUMBER = re.compile(
    r"(?:>=|<=|==|!=|>|<)\s*\(?\s*(?:"
    r"[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?\s*\*\s*-?\d+(?:\.\d+)?"
    r"|-?\d+(?:\.\d+)?\s*\*\s*[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?"
    r")\s*\)?"
)
# A comparison whose right-hand side is a bare number: the floor.
CMP_LITERAL = re.compile(r"(?:>=|<=|==|!=|>|<)\s*-?\d+(?:\.\d+)?(?![0-9.])")
# The quantity has to be a computed rate: an age compared against a multiple of
# its own period is a staleness rule, where the period is already the floor and
# a second bound would only duplicate it.
RATE_SHAPE = re.compile(r"\b(?:deriv|rate|irate)\s*\(")


def config_rule_library(config_path: str):
    """Yield (where, name, expression) for every rule in the in-process library."""
    with open(config_path, encoding="utf-8") as fh:
        config = json.load(fh)

    def walk(node, path):
        if isinstance(node, dict):
            if isinstance(node.get("rules"), list) and node["rules"] and "promql" in node["rules"][0]:
                for i, rule in enumerate(node["rules"]):
                    yield (f"{path}.rules[{i}]", str(rule.get("name", "?")), str(rule.get("promql", "")))
                return
            for key, value in node.items():
                yield from walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for i, value in enumerate(node):
                yield from walk(value, f"{path}[{i}]")

    yield from walk(config, "cogneva.json")


def manifest_rule_library(manifests_dir: str):
    """Yield (where, name, expression) for every rule in the manifest library."""
    if not os.path.isdir(manifests_dir):
        raise OSError(f"manifest directory is not there: {manifests_dir}")
    import yaml

    for path in sorted(glob.glob(os.path.join(manifests_dir, "*.yaml"))):
        with open(path, encoding="utf-8") as fh:
            try:
                docs = list(yaml.safe_load_all(fh))
            except yaml.YAMLError as exc:
                raise OSError(f"cannot parse {os.path.basename(path)}: {exc}") from exc
        for doc in docs:
            for group in ((doc or {}).get("spec") or {}).get("groups") or []:
                for rule in group.get("rules") or []:
                    expr = str(rule.get("expr", "")).strip()
                    if not expr:
                        continue
                    yield (os.path.basename(path), str(rule.get("alert", "?")), expr)


def carries_floor(expr: str) -> bool:
    """A floor is a numeric comparison that is not the ratio comparison itself.

    The ratio comparison `> (x * 2)` also matches the literal pattern at its
    final digit, so each literal hit is removed from the text before asking
    whether anything numeric is left.
    """
    stripped = SERIES_TIMES_NUMBER.sub(" ", expr)
    return CMP_LITERAL.search(stripped) is not None


def main(argv):
    report_only = False
    args = [a for a in argv[1:] if not a.startswith("-")]
    for a in argv[1:]:
        if a == "--report":
            report_only = True
        elif a in ("-h", "--help"):
            print(__doc__)
            return 0
        elif a.startswith("-"):
            print(f"check-alert-rule-ratio-floor: unknown argument {a}", file=sys.stderr)
            return 2
    config = args[0] if args else "deploy/helm/cogneva/files/cogneva.json"
    manifests = args[1] if len(args) > 1 else "deploy/k3s/observability/manifests"

    try:
        config_rules = list(config_rule_library(config))
        manifest_rules = list(manifest_rule_library(manifests))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"check-alert-rule-ratio-floor: cannot read a rule library: {exc}", file=sys.stderr)
        print("check-alert-rule-ratio-floor: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    # 读到零条与「一条都不违规」在汇总行上同形（`rules=0 ... violations=0`）。两套库
    # 各自都要至少读到一条：文件在而结构变了、目录在而清单没了，都是扫描面缩小，不是干净。
    empty = [
        (where, why)
        for where, why, found in (
            (config, f"{config} 里", config_rules),
            (manifests, f"{manifests} 里", manifest_rules),
        )
        if not found
    ]
    if empty:
        for where, why in empty:
            print(
                f"check-alert-rule-ratio-floor: {why}一条规则都没有读到"
                f"（{where}）——零条与「全都合规」在汇总行上同形",
                file=sys.stderr,
            )
        print("check-alert-rule-ratio-floor: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    rules = config_rules + manifest_rules

    scanned = len(rules)
    ratio_rules = 0
    with_floor = 0
    unjudged = []
    violations = []
    for where, name, expr in rules:
        if not SERIES_TIMES_NUMBER.search(expr) or not RATE_SHAPE.search(expr):
            unjudged.append((where, name))
            continue
        ratio_rules += 1
        if carries_floor(expr):
            with_floor += 1
        else:
            violations.append((where, name, expr))

    for where, name, expr in violations:
        print(f"RED  {where} {name}: compares against a multiple of a reading with no floor")
        print(f"     expr: {expr}")
        print(
            "     repair: add the absolute floor the same quantity has to clear -- "
            "a ratio of two small numbers is still a small number, and a rule that "
            "fires on it never stops"
        )
    for where, name in unjudged:
        print(f"CENSUS unjudged {where} {name}: no comparison against a scaled series")
    print(
        f"check-alert-rule-ratio-floor: rules={scanned} ratio_rules={ratio_rules} "
        f"with_floor={with_floor} unjudged={len(unjudged)} violations={len(violations)}"
    )
    if violations and not report_only:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
