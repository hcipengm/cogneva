#!/usr/bin/env bash
# 恢复步骤的顺序门：每个 `run_step` 上方那行 `# step:` 声明里，`requires` 都必须
# 由**更早**的一步 `produces`，且 `needs_gateway=1` 的步骤之前必须已经产出了
# `gateway`。
#
# 为什么判在调用点而不是一张步骤表：表和脚本是同一个东西的两种写法，只要分成
# 两份，就永远有「表里有行、脚本里没这一步」和「脚本里多一步、表里没登记」两个
# 方向的分叉，而这两个方向事后最难看出来。声明挨着 `run_step` 之后，前一个方向
# 变成一条挂空的注释（红），后一个方向变成没有声明的调用（红）——分叉不再是可写
# 的形状，而不是被一门专门的比对盖住。
#
# 判的是声明，不是步骤体里到底调了什么。这个边界是故意的：能不能从一段 shell
# 里看出它偷偷等了网关，取决于写法，按裸子串判会把注释里一句「本路径不依赖网关」
# 也判红。步骤体是否真的不需要网关，靠空白机演练在**没有网关的机器上**跑通来证，
# 不靠这条 grep 去猜。
#
# 声明形状（这一行必须在它所描述的 `run_step` 正上方，中间不许夹别的行）：
#   # step: requires=<逗号分隔> produces=<逗号分隔> needs_gateway=<0|1> — <为什么这一步在这里>
#
# 用法：
#   bash deploy/scripts/check-restore-order.sh
#   bash deploy/scripts/check-restore-order.sh --script <sh>
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
FIELDS = ("requires", "produces", "needs_gateway")

DECL = re.compile(r"^\s*#\s*step:\s*(?P<body>\S.*)$")
CALL = re.compile(r"^\s*run_step\s+(?P<id>[A-Za-z0-9_.-]+)\s*$")
FIELD = re.compile(r"\b(?P<key>[a-z_]+)=(?P<val>[^\s]*)")


def parse_decl(lineno, body):
    """一行 `# step:` -> (fields, note, error)。"""
    note = ""
    if " — " in body:
        body, note = body.split(" — ", 1)
    fields, rest = {}, FIELD.sub("", body).strip()
    for m in FIELD.finditer(body):
        key, val = m.group("key"), m.group("val")
        if key not in FIELDS:
            return None, "", f"第 {lineno} 行的声明有不认识的字段 {key!r}（只认 {', '.join(FIELDS)}）"
        if key in fields:
            return None, "", f"第 {lineno} 行的声明把 {key} 写了两遍"
        fields[key] = val
    missing = [k for k in FIELDS if k not in fields]
    if missing:
        return None, "", f"第 {lineno} 行的声明缺 {', '.join(missing)}"
    if rest:
        return None, "", f"第 {lineno} 行的声明里有解析不掉的字节 {rest!r}"
    if not note.strip():
        return None, "", (
            f"第 {lineno} 行的声明没写为什么这一步在这里（` — ` 之后的那句）："
            "只写依赖的步骤等于把问题留给下一个读者"
        )
    if fields["needs_gateway"] not in ("0", "1"):
        return None, "", f"第 {lineno} 行的 needs_gateway 是 {fields['needs_gateway']!r}，只认 0/1"
    if not [x for x in fields["produces"].split(",") if x.strip()]:
        return None, "", f"第 {lineno} 行的声明不产出任何东西，后面没有谁可以依赖它"
    return fields, note.strip(), ""


def split(value):
    return [x.strip() for x in value.split(",") if x.strip()]


def read_declarations(path):
    """脚本里的 (id, requires, produces, needs_gateway) 行，按出现顺序。

    声明与调用必须挨着：`# step:` 的下一行就该是 `run_step <同一个 id>`，中间
    夹任何一行（包括空行与别的注释）都算这条声明挂空了。
    """
    rows, violations = [], []
    pending = None  # (lineno, fields, note)
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            body = line.rstrip("\n")
            decl = DECL.match(body)
            if decl:
                if pending:
                    violations.append(
                        f"第 {pending['lineno']} 行的声明后面没有紧跟 run_step（第 {lineno} 行又是一条声明）"
                    )
                fields, note, error = parse_decl(lineno, decl.group("body"))
                if error:
                    violations.append(error)
                    pending = None
                else:
                    pending = {"lineno": lineno, "fields": fields, "note": note}
                continue
            call = CALL.match(body)
            if call:
                step = call.group("id")
                if not pending:
                    violations.append(
                        f"脚本里的 run_step {step}（第 {lineno} 行）上方没有 `# step:` 声明："
                        "没声明要什么、装出什么，顺序就无从判"
                    )
                else:
                    rows.append(
                        {
                            "lineno": pending["lineno"],
                            "call_lineno": lineno,
                            "id": step,
                            "requires": split(pending["fields"]["requires"]),
                            "produces": split(pending["fields"]["produces"]),
                            "needs_gateway": pending["fields"]["needs_gateway"] == "1",
                            "gateway_flag": pending["fields"]["needs_gateway"],
                        }
                    )
                    pending = None
                continue
            if not body.strip():
                continue
            if pending:
                violations.append(
                    f"第 {pending['lineno']} 行的声明与它要描述的 run_step 之间夹了第 {lineno} 行的内容"
                )
                pending = None
    if pending:
        violations.append(
            f"第 {pending['lineno']} 行的声明在文件末尾挂空：下面没有 run_step 承接它"
        )
    return rows, violations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", default="deploy/scripts/restore-blank-machine.sh")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        rows, violations = read_declarations(args.script)
    except OSError as e:
        print(f"红: 读不到恢复脚本 {args.script}: {e}", file=sys.stderr)
        return 1
    if not rows and not violations:
        print(f"红: {args.script} 里没有任何步骤（没有步骤证明不了任何顺序）", file=sys.stderr)
        return 1

    seen_ids = {}
    produced = {IMPLICIT: "空白机自带"}
    gateway_ready = False
    for row in rows:
        step = row["id"]
        if step in seen_ids:
            violations.append(f"步骤 {step} 重复（第 {seen_ids[step]} 行与第 {row['lineno']} 行）")
            continue
        seen_ids[step] = row["lineno"]
        for need in row["requires"]:
            if need not in produced:
                violations.append(
                    f"步骤 {step} 需要 {need}，但更早的步骤里没有谁产出它"
                    f"（已知产出：{', '.join(sorted(produced))}）"
                )
        if row["needs_gateway"] and not gateway_ready:
            violations.append(f"步骤 {step} 要网关，但更早的步骤里没有谁产出 gateway")
        for out in row["produces"]:
            if out in produced:
                violations.append(f"步骤 {step} 重复产出 {out}（已由 {produced[out]} 产出）")
            produced[out] = step
            if out == "gateway":
                gateway_ready = True

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
