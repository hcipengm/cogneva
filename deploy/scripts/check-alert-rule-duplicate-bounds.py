#!/usr/bin/env python3
"""Two rule libraries, one question: does the same bound have two messengers?

`deploy/k3s/observability/manifests/*.yaml` (PrometheusRule) and
`deploy/helm/cogneva/files/cogneva.json` (`observability.infra_watch.rules`,
evaluated inside the application process) are separate libraries maintained by
different rounds. Nothing has ever compared them. A bound that lives in both is
not automatically wrong -- copying a threshold is sometimes the right call --
but it is always *two* places to change, and changing one and not the other
leaves two self-consistent rules disagreeing about the same object. No gate can
see that afterwards: each rule on its own is well formed.

So this script only reports. Adding a rule for a bound that already has one
doubles the paging and keeps the repair identical; the decision belongs to the
person who knows why the second copy exists. What the script gives them is the
census they otherwise have to do by hand.

Two tiers, because "the same bound" has two honest readings:

  identical_bound            the same normalised expression, character for
                             character. A copy, whether or not anyone meant it as
                             one.
  same_subject_same_threshold
                             different expressions, one subject series each, the
                             same comparison and the same number. Usually one
                             bound asked two different questions -- and the pair
                             that drifts first, because the two texts look
                             unrelated. Some of these are legitimate (a guard
                             clause, or the two directions of a boolean); the
                             report says *which* two rules to look at, not that
                             they are wrong.

Exit codes: 0 report (always, unless --strict), 1 with --strict when anything is
paired, 3 when a source cannot be read (a silent zero would read as "nothing is
duplicated", which is the worst possible answer here).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

# The tree is overridable so the test can build one file at a time; the reading
# itself never needs it.
REPO = Path(
    os.environ.get("COGNEVA_DUPLICATE_BOUNDS_REPO")
    or Path(__file__).resolve().parents[2]
)
MANIFEST_DIR = REPO / "deploy/k3s/observability/manifests"
CHART_RULES = REPO / "deploy/helm/cogneva/files/cogneva.json"

SEL = r"[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?"
CMP = r"(>=|<=|==|!=|>|<)"
NUM = r"(-?[0-9]+(?:\.[0-9]+)?)"
# A comparison against a constant: `A > 0.9`, `A >= 1`, `A == 0`. The left side
# may be any expression, so the match starts at the operator; what is being
# compared is the text to its left, cut at the nearest top-level `and`/`or`.
COMPARISON = re.compile(rf"\s*{CMP}\s*{NUM}")
# Top-level binary operator keywords: a comparison's subject never crosses one.
BINARY = (" and ", " or ", " unless ")
NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
# Words that appear in promql but are not series.
KEYWORDS = {
    "sum", "min", "max", "avg", "count", "by", "without", "on", "ignoring",
    "group_left", "group_right", "offset", "bool", "and", "or", "unless",
    "topk", "bottomk", "quantile", "count_values", "stddev", "stdvar",
    "sort", "sort_desc", "limitk", "limit_ratio", "absent",
    "absent_over_time", "changes", "resets", "deriv", "predict_linear",
    "histogram_quantile", "clamp_min", "clamp_max", "label_replace",
    "label_join", "vector", "scalar", "time", "timestamp", "hour", "minute",
    "month", "year", "day_of_week", "days_in_month", "round", "ceil", "floor",
    "abs", "rate", "irate", "increase", "delta", "idelta", "avg_over_time",
    "max_over_time", "min_over_time", "sum_over_time", "count_over_time",
    "last_over_time", "present_over_time", "quantile_over_time", "stddev_over_time",
}
# `by (...)` / `without (...)` hold label names, not series.
MODIFIER = re.compile(r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^()]*\)")


def die(why: str) -> None:
    print(f"duplicate bounds: {why}", file=sys.stderr)
    sys.exit(3)


def norm(expr: str) -> str:
    """Whitespace-free, comment-free form: two expressions are the 'same' only if
    they would be evaluated identically, so nothing semantic is removed."""
    return re.sub(r"\s+", "", expr)


def selectors(expr: str) -> frozenset[str]:
    """The vector selectors an expression reads, as `name{labels}` with the label
    set sorted and whitespace removed.

    Labels belong in the identity, values included: `readings{outcome="failed"}`
    and `readings{outcome="succeeded"}` are different objects, and a check that
    strips labels pairs every rule on a labelled family with every other one --
    which is how a report of twelve real pairs becomes a report of several
    hundred pairs nobody reads.

    The expression is read **raw**: normalising the whitespace first glues
    `A and B` into a single token called `AandB`.
    """
    body = MODIFIER.sub("", expr)
    out = set()
    for m in re.finditer(
        r'([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(\{(?:[^}"]|"(?:[^"\\]|\\.)*")*\})?', body
    ):
        name, labels = m.group(1), m.group(2)
        if "_" not in name or name in KEYWORDS:
            continue
        if labels:
            inner = labels.strip("{}")
            # Split on commas outside quotes, then drop whitespace only.
            pairs = [
                re.sub(r"\s+", "", p)
                for p in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', inner)
                if p.strip()
            ]
            out.add(name + "{" + ",".join(sorted(pairs)) + "}")
        else:
            out.add(name)
    return frozenset(out)


def yaml_rules() -> list[dict]:
    if not MANIFEST_DIR.is_dir():
        die(f"manifest directory is not there: {MANIFEST_DIR}")
    try:
        import yaml
    except ImportError:
        die("PyYAML is not importable")
    out = []
    files = sorted(p for p in MANIFEST_DIR.glob("*.yaml"))
    if not files:
        die(f"no manifests under {MANIFEST_DIR}")
    for path in files:
        try:
            docs = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except Exception as e:  # noqa: BLE001 - any parse failure is "cannot read"
            die(f"cannot parse {path.name}: {e}")
        for doc in docs:
            if not isinstance(doc, dict) or doc.get("kind") != "PrometheusRule":
                continue
            for group in (doc.get("spec") or {}).get("groups") or []:
                for rule in group.get("rules") or []:
                    if rule.get("alert") and rule.get("expr"):
                        out.append(
                            {
                                "name": rule["alert"],
                                "expr": str(rule["expr"]),
                                "source": f"manifests/{path.name}",
                                "kind": "yaml",
                            }
                        )
    return out


def chart_rules() -> list[dict]:
    if not CHART_RULES.is_file():
        die(f"in-process rule file is not there: {CHART_RULES}")
    try:
        doc = json.loads(CHART_RULES.read_text(encoding="utf-8"))
        rules = doc["observability"]["infra_watch"]["rules"]
    except Exception as e:  # noqa: BLE001
        die(f"cannot read {CHART_RULES.name}: {e}")
    if not rules:
        die("the in-process rule list is empty")
    out = []
    for rule in rules:
        expr = rule.get("promql") or rule.get("query") or ""
        cond = rule.get("condition")
        bound = None
        if isinstance(cond, dict):
            for key, op in (
                ("greater_than", ">"),
                ("greater_than_or_equal", ">="),
                ("less_than", "<"),
                ("less_than_or_equal", "<="),
                ("equal_to", "=="),
            ):
                if key in cond:
                    bound = (op, float(cond[key]))
                    break
        out.append(
            {
                "name": rule.get("name", "<unnamed>"),
                "expr": str(expr),
                "bound": bound,
                "source": "chart/cogneva.json",
                "kind": "json",
            }
        )
    return out


def left_operand(expr: str, pos: int) -> str:
    """The expression being compared at `pos`: everything to its left, back to
    the nearest top-level `and` / `or` / `unless`. PromQL comparisons are not
    parenthesised, so the subject of `A and B > 0` is `B`, not `A and B`."""
    head = expr[:pos]
    cut = 0
    for kw in BINARY:
        i = head.rfind(kw)
        if i >= 0:
            cut = max(cut, i + len(kw))
    return head[cut:]


def bounds(rule: dict) -> list[tuple[str, str, float]]:
    """(subject text, op, value) triples this rule compares. A rule may compare
    more than one thing; the subject text is the comparison, not the rule."""
    found = []
    if rule["kind"] == "json":
        cond = rule.get("bound")
        if cond:
            found.append((rule["expr"], cond[0], float(cond[1])))
        return found
    for m in COMPARISON.finditer(rule["expr"]):
        found.append((left_operand(rule["expr"], m.start()), m.group(1), float(m.group(2))))
    return found


def whole_bound(rule: dict) -> tuple[str, str | None, float | None]:
    """The rule as one (subject, op, value) triple.

    The chart keeps the number outside the expression (`promql` + `condition`),
    the manifests keep it inside (`expr: x > 3`). To compare the two libraries
    at all, both sides have to be reduced to the same shape. A manifest rule
    whose expression is *exactly one* comparison reduces to that comparison;
    anything else (a guard clause, an `and`/`or` of two conditions) keeps its
    whole text and no number, so it can only ever pair with a rule carrying the
    very same text.
    """
    if rule["kind"] == "json":
        cond = rule.get("bound")
        if cond:
            return (norm(rule["expr"]), cond[0], float(cond[1]))
        return (norm(rule["expr"]), None, None)
    cmps = list(COMPARISON.finditer(rule["expr"]))
    if len(cmps) == 1:
        m = cmps[0]
        return (
            norm(left_operand(rule["expr"], m.start())),
            m.group(1),
            float(m.group(2)),
        )
    return (norm(rule["expr"]), None, None)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--strict", action="store_true",
                    help="exit 1 when any pair is reported (default: report only)")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    rules = yaml_rules() + chart_rules()
    if not rules:
        die("no alert rules found in either library")

    identical: dict[tuple[str, str, float], list[dict]] = {}
    by_subject: dict[tuple[str, str, float], list[dict]] = {}
    for rule in rules:
        # Tier 1 is about the expression as a whole. Comparing the *comparison*
        # instead would pair two rules that merely share a guard clause
        # (`... unless reported == 1`), which is not a duplicated bound.
        # The threshold is part of the bound: two tiers of the same expression
        # (warning at 80, critical at 90) are one expression on purpose.
        identical.setdefault(whole_bound(rule), []).append(rule)
        for raw, op, value in bounds(rule):
            subs = selectors(raw)
            # Only expressions about **one** object. With two or more selectors the
            # pair set stops naming an object and starts naming a combination, and
            # combinations collide without meaning anything.
            if len(subs) == 1:
                (sub,) = subs
                by_subject.setdefault((sub, op, value), []).append(
                    {**rule, "op": op, "value": value, "object": norm(raw)}
                )

    pairs = []
    for (obj, _op, _val), members in sorted(identical.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        if len(members) < 2:
            continue
        pairs.append({
            "tier": "identical_bound",
            "object": obj,
            "op": "",
            "value": None,
            "rules": [{"name": m["name"], "source": m["source"]} for m in members],
        })
    for (sub, op, value), members in sorted(by_subject.items(), key=lambda kv: (-len(kv[1]), str(kv[0]))):
        # Already reported as an identical copy: same subject, same number, same text.
        names = {(m["name"], m["source"]) for m in members}
        if len(names) < 2:
            continue
        objs = {m["object"] for m in members}
        if len(objs) < 2:
            continue
        pairs.append({
            "tier": "same_subject_same_threshold",
            "object": sub,
            "op": op,
            "value": value,
            "rules": [{"name": m["name"], "source": m["source"]} for m in members],
        })

    if args.json:
        print(json.dumps({"rules": len(rules), "pairs": pairs}, ensure_ascii=False, indent=2))
    else:
        print(f"rules={len(rules)} pairs={len(pairs)}")
        for p in pairs:
            where = ", ".join(f'{r["name"]}@{r["source"]}' for r in p["rules"])
            cmp_ = "" if p["value"] is None else f'{p["op"]} {p["value"]:g}'
            print(f'{p["tier"]} object={p["object"]} bound={cmp_} rules={where}')
        if not pairs:
            print("no bound is carried by two rules")

    if args.strict and pairs:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
