#!/usr/bin/env bash
# 一轮的批没编辑完，不许提交。
#
# 为什么在提交这一刀：planner（产出台账）、generator（整批一次改完）、evaluator
# （判完成）都是同一个模型在扮演，流程对它唯一的约束就在它能自己敲的那些命令上。
# 一个「改一条提交一条」的节奏，与「整批改完再编译一次」在**读数上**完全同形——
# 都是一串绿提交——区别只在后者要等到最后才编得动。这道门把那句话变成事实：
# 开着的一轮里，批内还有待办没被触到，任何提交都停下。
#
# 判词借用 `family.py --editing`：它问的正是「这一遍编辑走点到的地方都动过没有」，
# 与编译垫片问同一个问题、走同一段代码。两处各写一份判据，改一处另一处不会红。
#
# 没有开着的一轮、或台账读不到：**不阻**，印一行说明。那不是「编完了」，
# 是这道门判不了——判不了不该表现得像通过，但也不该把一个不做这件事的人拦下。
set -u
root="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
family="$root/scripts/hooks/family.py"
[ -f "$family" ] || exit 0

out="$(python3 "$family" --all-todos --editing 2>&1)"
rc=$?
case "$rc" in
  0) exit 0 ;;
  3)
    printf 'pre-commit（批次）：这一轮判不了，提交不拦——%s\n' "$(printf '%s' "$out" | tail -1)" >&2
    exit 0 ;;
esac
if printf '%s' "$out" | grep -q '^TODO '; then
    printf 'pre-commit（批次）：这一轮的批还没编辑完，先别提交。\n' >&2
    printf '%s\n' "$out" | grep '^TODO ' | sed 's/^/  /' >&2
    cat >&2 <<'MSG'
  流程是：planner 产出待办 → 立刻定批 → generator 把**整批**一次改完 → 编译一次 →
  测试一次 → 门一次 → 推。改一条提交一条，等于把同一份验证付了 N 次，而且
  「这一批真做完了没有」永远没人答。
  确实要分多次提交（例如中途存档）：`git commit --no-verify`，并在信息里写清为什么。
MSG
    exit 1
fi
exit 0
