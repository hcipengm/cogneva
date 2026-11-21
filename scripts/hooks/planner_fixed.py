#!/usr/bin/env python3
"""planner 的**固定程序那一半**：不用模型就能定的，全在这里定。

对整批每一条给出三种结局之一：

    ok      <id>  已有读数且形状合格（1.5 直接采用）
    shape   <id>  有读数但形状不合格——**这是要模型改的**，附理由
    none    <id>  没有读数——要模型给

它不判语义、不判对错，只判「这一条现在归谁」。agent 那半只接手 shape 与 none。
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402

A = re.compile(r"(?:判据读数|代码)\s*:\s*([a-z0-9-]+)\s*(?:→|->)\s*(\d+)")
B = re.compile(r"(?:判据读数|代码)\s*:\s*(?:test\s*=\s*)?((?:brain:)?[^\s:]+)::([^\s:｜|]+)")


def classify(item) -> tuple[str, str]:
    text = item.text
    if A.search(text) or B.search(text):
        return "ok", ""
    if "判据读数" in text:
        return "shape", "有 `判据读数` 但不是 A/B 形状"
    return "none", "没有读数"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=None)
    args = parser.parse_args(argv[1:])
    try:
        items = todo_source.load(args.source or todo_source.default_spec())
    except todo_source.Unreadable as e:
        print(f"planner_fixed: 台账读不到：{e}", file=sys.stderr)
        return 2
    counts = {"ok": 0, "shape": 0, "none": 0}
    for item in items:
        kind, why = classify(item)
        counts[kind] += 1
        print(f"  {kind:<5} {item.id}" + (f"  {why}" if why else ""))
    print(
        f"planner_fixed: 已定 {counts['ok']} 条；要模型处理 "
        f"{counts['shape'] + counts['none']} 条（形状坏 {counts['shape']}、没有 {counts['none']}）"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
