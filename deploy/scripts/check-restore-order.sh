#!/usr/bin/env bash
# 恢复步骤的顺序门：步骤清单里每一条 `requires` 都必须由**更早**的一步
# `produces`，且 `needs_gateway=1` 的步骤之前必须已经产出了 `gateway`。
#
# 判的是声明，不是步骤体里到底调了什么。这个边界是故意的：能不能从一段 shell
# 里看出它偷偷等了网关，取决于写法，按裸子串判会把注释里一句「本路径不依赖网关」
# 也判红。步骤体是否真的不需要网关，靠空白机演练在**没有网关的机器上**跑通来证，
# 不靠这条 grep 去猜。
#
# 与脚本双向对齐：清单里每个 id 在脚本里都要有 `run_step <id>`，脚本里每个
# `run_step` 也都要在清单里有一行。只对一边，新增步骤时另一半会静默缺席——
# 那正是「清单说恢复分四步、脚本只跑三步」这种最难在事后发现的分叉。
#
# 用法：
#   bash deploy/scripts/check-restore-order.sh
#   bash deploy/scripts/check-restore-order.sh --steps <tsv> --script <sh>
#   bash deploy/scripts/check-restore-order.sh --json
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$@" <<'PYEOF'
import argparse
import json
import re
import sys

IMPLICIT = "host_shell"  # 空白机自带的唯一前置，不需要任何一步装出来


def read_steps(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) != 5:
                rows.append(
                    {
                        "lineno": lineno,
                        "error": f"第 {lineno} 行有 {len(cols)} 列，步骤表固定 5 列（制表符分隔）",
                    }
                )
                continue
            rows.append(
                {
                    "lineno": lineno,
                    "id": cols[0].strip(),
                    "requires": [x.strip() for x in cols[1].split(",") if x.strip()],
                    "produces": [x.strip() for x in cols[2].split(",") if x.strip()],
                    "needs_gateway": cols[3].strip() == "1",
                    "gateway_flag": cols[3].strip(),
                }
            )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", default="deploy/restore/blank-machine.steps.tsv")
    parser.add_argument("--script", default="deploy/scripts/restore-blank-machine.sh")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    violations = []
    rows = read_steps(args.steps)
    if not rows:
        print(f"红: {args.steps} 里没有任何步骤（空表不是绿）", file=sys.stderr)
        return 1

    seen_ids = {}
    produced = {IMPLICIT: "空白机自带"}
    gateway_ready = False
    for index, row in enumerate(rows):
        if "error" in row:
            violations.append(row["error"])
            continue
        step = row["id"]
        if not step:
            violations.append(f"第 {row['lineno']} 行没有 id")
            continue
        if step in seen_ids:
            violations.append(f"步骤 {step} 重复（第 {seen_ids[step]} 行与第 {row['lineno']} 行）")
            continue
        seen_ids[step] = row["lineno"]
        if row["gateway_flag"] not in ("0", "1"):
            violations.append(f"步骤 {step} 的 needs_gateway 是 {row['gateway_flag']!r}，只认 0/1")
        for need in row["requires"]:
            if need not in produced:
                violations.append(
                    f"步骤 {step} 需要 {need}，但更早的步骤里没有谁产出它"
                    f"（已知产出：{', '.join(sorted(produced))}）"
                )
        if row["needs_gateway"] and not gateway_ready:
            violations.append(f"步骤 {step} 要网关，但更早的步骤里没有谁产出 gateway")
        if not row["produces"]:
            violations.append(f"步骤 {step} 不产出任何东西，后面没有谁可以依赖它")
        for out in row["produces"]:
            if out in produced:
                violations.append(f"步骤 {step} 重复产出 {out}（已由 {produced[out]} 产出）")
            produced[out] = step
            if out == "gateway":
                gateway_ready = True

    # 与脚本双向对齐。脚本里每步的写法是行首（允许缩进）`run_step <id>`。
    try:
        with open(args.script, encoding="utf-8") as f:
            script = f.read()
    except OSError as e:
        print(f"红: 读不到恢复脚本 {args.script}: {e}", file=sys.stderr)
        return 1
    called = re.findall(r"^\s*run_step\s+([A-Za-z0-9_.-]+)", script, re.MULTILINE)
    for step in seen_ids:
        if step not in called:
            violations.append(f"清单里的步骤 {step} 在脚本里没有 run_step {step}")
    for step in called:
        if step not in seen_ids:
            violations.append(f"脚本里的 run_step {step} 在步骤清单里没有行")

    result = {
        "steps": len(seen_ids),
        "requires": sorted(produced),
        "gateway_steps": [r["id"] for r in rows if r.get("needs_gateway")],
        "violations": violations,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            f"steps={result['steps']} 产出={len(result['requires'])} "
            f"等网关的步骤={len(result['gateway_steps'])} 违例={len(violations)}"
        )
        for v in violations:
            print(f"  红: {v}")
        if not violations:
            print("  绿")
    return 1 if violations else 0


sys.exit(main())
PYEOF
