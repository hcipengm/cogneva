#!/usr/bin/env python3
"""A rule that reads a runway must compare a margin to zero, not a number to a constant.

Why this is a gate and not a review item: a threshold on a *rate* does not mean
one thing. "Disk is 80% used" and "disk fills in seven days" both sound like
bounds, and both say something different for a disk growing 0.4 GiB a day than
for one growing 40. The repair has the same lead time either way, so the bound
that matters is "does the runway still cover the lead" -- and that is a
difference, not a constant.

The way to keep that difference honest is to publish it: the producer emits a
margin series (days left minus the lead the host publishes) and the rule
compares that margin to zero. A negative margin is the breach, which is also the
polarity the NVMe wear headroom already uses, so a reader who learns one of
these has learned both.

What this judges: every rule in either library whose expression reads a runway
metric -- a name carrying `_days_to_`, `_headroom_`, or `_days_left`. Such a
rule may only compare against zero. A non-zero number there is a lead time typed
into a rule, which is exactly the shape that goes stale silently: the rule keeps
firing (or stops) and nothing shows which lead the number was meant to encode.

Reported, not judged: a runway rule with no numeric comparison at all. It may be
a guard clause reading a state series, so the census counts it and says so
rather than claiming it passed.

Exit codes: 0 every runway rule compares a margin to zero; 1 at least one does
not (each one named with its threshold and the metric it reads); 3 a library
could not be read -- which is not 0, because "nothing violated" and "nothing
was judged" must not look the same.

Usage: check-alert-rule-derived-bounds.py [--report] [cogneva.json] [manifest dir]
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys

RUNWAY = re.compile(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*(?:_days_to_[a-zA-Z0-9_]+|_headroom_[a-zA-Z0-9_]*|_days_left))\b")
CMP_LITERAL = re.compile(r"(>=|<=|==|!=|>|<)\s*(-?\d+(?:\.\d+)?)(?![0-9.])")
BOUND_KEYS = ("greater_than", "less_than", "greater_than_or_equal", "less_than_or_equal", "equal")


def config_rule_library(config_path: str):
    """Yield (where, name, expression, thresholds) for every rule in the in-process library."""
    with open(config_path, encoding="utf-8") as fh:
        config = json.load(fh)

    def walk(node, path):
        if isinstance(node, dict):
            if isinstance(node.get("rules"), list) and node["rules"] and "promql" in node["rules"][0]:
                for i, rule in enumerate(node["rules"]):
                    expr = str(rule.get("promql", ""))
                    thresholds = []
                    cond = rule.get("condition")
                    if isinstance(cond, dict):
                        for key, value in cond.items():
                            if key in BOUND_KEYS:
                                thresholds.append(float(value))
                    thresholds.extend(float(m.group(2)) for m in CMP_LITERAL.finditer(expr))
                    yield (f"{path}.rules[{i}]", str(rule.get("name", "?")), expr, thresholds)
                return
            for key, value in node.items():
                yield from walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for i, value in enumerate(node):
                yield from walk(value, f"{path}[{i}]")

    yield from walk(config, "cogneva.json")


def manifest_rule_library(manifests_dir: str):
    """Yield (where, name, expression, thresholds) for every rule in the manifest library."""
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
                    thresholds = [float(m.group(2)) for m in CMP_LITERAL.finditer(expr)]
                    yield (os.path.basename(path), str(rule.get("alert", "?")), expr, thresholds)


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
            print(f"check-alert-rule-derived-bounds: unknown argument {a}", file=sys.stderr)
            return 2
    config = args[0] if args else "deploy/helm/cogneva/files/cogneva.json"
    manifests = args[1] if len(args) > 1 else "deploy/k3s/observability/manifests"

    try:
        config_rules = list(config_rule_library(config))
        manifest_rules = list(manifest_rule_library(manifests))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"check-alert-rule-derived-bounds: cannot read a rule library: {exc}", file=sys.stderr)
        print("check-alert-rule-derived-bounds: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    # 读到了零条规则与「规则全都合规」在汇总行上同形：`rules=0 ... violations=0` 读起来
    # 就是一声通过的汇总。两套库各自都必须至少读到一条，少一条就是这套库的读取面已经
    # 指错了地方（文件在、结构变了；目录在、清单没了）。3 是「什么都没判」，不是 0。
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
                f"check-alert-rule-derived-bounds: {why}一条规则都没有读到"
                f"（{where}）——零条与「全都合规」在汇总行上同形",
                file=sys.stderr,
            )
        print("check-alert-rule-derived-bounds: judged nothing (not the same as clean)", file=sys.stderr)
        return 3

    rules = config_rules + manifest_rules

    scanned = len(rules)
    runway_rules = 0
    judged = 0
    unjudged = []
    violations = []
    for where, name, expr, thresholds in rules:
        metrics = sorted(set(m.group(1) for m in RUNWAY.finditer(expr)))
        if not metrics:
            continue
        runway_rules += 1
        if not thresholds:
            unjudged.append((where, name, metrics))
            continue
        judged += 1
        for threshold in thresholds:
            if threshold != 0.0:
                violations.append((where, name, metrics, threshold, expr))

    for where, name, metrics, threshold, expr in violations:
        print(f"RED  {where} {name}: reads {', '.join(metrics)} but compares against {threshold:g}")
        print(f"     expr: {expr}")
        print(
            "     repair: compare a margin series to 0 -- the producer publishes "
            "the lead time and the margin left against it, so the number belongs there"
        )
    for where, name, metrics in unjudged:
        print(f"CENSUS unjudged {where} {name}: reads {', '.join(metrics)} with no numeric bound")
    print(
        f"check-alert-rule-derived-bounds: rules={scanned} runway_rules={runway_rules} "
        f"judged={judged} unjudged={len(unjudged)} violations={len(violations)}"
    )
    if violations and not report_only:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
