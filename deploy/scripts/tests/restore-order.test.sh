#!/usr/bin/env bash
# 顺序门的测试。每一条都从一个**真会坏**的形状出发：
#   - 依赖指向没人产出的东西（步骤被删掉、名字被改掉）；
#   - 等网关的步骤排在没有网关之前（空白机上的死循环）；
#   - 声明与调用各缺一边（最难看出来的分叉）：`run_step` 上方没声明，声明下面
#     没有 `run_step`，声明写错了却顶着一个真步骤。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECK="$ROOT/deploy/scripts/check-restore-order.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# 一行声明的简写：decl <id> <requires> <produces> <needs_gateway>
decl() {
    printf '# step: requires=%s produces=%s needs_gateway=%s — %s 这一步的理由\n' "$2" "$3" "$4" "$1"
    printf 'run_step %s\n' "$1"
}

# 1. 真树是绿的。声明与调用是同一份文件里的两种写法，任何一边漂了这条先红。
bash "$CHECK" >/dev/null || fail "真树的步骤声明不闭合"

# 2. 没有步骤不是绿：空脚本证明不了任何顺序。
printf '#!/bin/sh\n' >"$TMP/empty.sh"
if bash "$CHECK" --script "$TMP/empty.sh" >/dev/null 2>&1; then
    fail "空脚本被判成绿"
fi
printf '# 只有注释\n' >"$TMP/comments.sh"
if bash "$CHECK" --script "$TMP/comments.sh" >/dev/null 2>&1; then
    fail "只有注释的脚本被判成绿"
fi

# 3. 依赖一个没人产出的东西。
{ decl a host_shell a_out 0; decl b a_out,b_out b_out 0; } >"$TMP/unknown.sh"
out="$(bash "$CHECK" --script "$TMP/unknown.sh" 2>&1 || true)"
case "$out" in
    *"b_out"*) ;;
    *) fail "没点出缺的是 b_out：$out" ;;
esac

# 4. 等网关的步骤排在没有网关之前；补上一个产出 gateway 的更早步骤后就该绿。
{ decl a host_shell a_out 0; decl b a_out b_out 1; } >"$TMP/gw.sh"
out="$(bash "$CHECK" --script "$TMP/gw.sh" 2>&1 || true)"
case "$out" in
    *"要网关"*) ;;
    *) fail "等网关的步骤没被判红：$out" ;;
esac
{ decl a host_shell a_out,gateway 0; decl b a_out,gateway b_out 1; } >"$TMP/gw_ok.sh"
bash "$CHECK" --script "$TMP/gw_ok.sh" >/dev/null || fail "网关在前时仍被判红"

# 5. `run_step` 上方没有声明：调用方多了一步，谁都没登记过它要什么。
printf 'run_step a\n' >"$TMP/naked.sh"
out="$(bash "$CHECK" --script "$TMP/naked.sh" 2>&1 || true)"
case "$out" in
    *"run_step a"*"没有 \`# step:\` 声明"*) ;;
    *) fail "没有声明的步骤没被判红：$out" ;;
esac

# 6. 声明下面没有 `run_step`：写着一步却没人跑它。两个方向都要红。
printf '# step: requires=host_shell produces=a_out needs_gateway=0 — a 的理由\n' >"$TMP/stranded.sh"
out="$(bash "$CHECK" --script "$TMP/stranded.sh" 2>&1 || true)"
case "$out" in
    *"末尾挂空"*|*"没有 run_step"*) ;;
    *) fail "挂空的声明没被判红：$out" ;;
esac

# 6b. 声明与调用之间夹了一行：这条声明描述的到底是谁，读的人分不出来。
{
    printf '# step: requires=host_shell produces=a_out needs_gateway=0 — a 的理由\n'
    printf 'echo 中间插了一行\n'
    printf 'run_step a\n'
} >"$TMP/interleaved.sh"
out="$(bash "$CHECK" --script "$TMP/interleaved.sh" 2>&1 || true)"
case "$out" in
    *"夹了"*) ;;
    *) fail "声明与调用之间夹了内容没被判红：$out" ;;
esac

# 6c. 声明缺字段：`produces` 没写，等于没说这一步装出了什么。
printf '# step: requires=host_shell needs_gateway=0 — a 的理由\nrun_step a\n' >"$TMP/missing_field.sh"
out="$(bash "$CHECK" --script "$TMP/missing_field.sh" 2>&1 || true)"
case "$out" in
    *"缺 produces"*) ;;
    *) fail "缺字段的声明没被判红：$out" ;;
esac

# 6d. 声明不写为什么这一步在这里：只剩依赖的步骤是给别人留的谜。
printf '# step: requires=host_shell produces=a_out needs_gateway=0\nrun_step a\n' >"$TMP/no_note.sh"
out="$(bash "$CHECK" --script "$TMP/no_note.sh" 2>&1 || true)"
case "$out" in
    *"为什么这一步在这里"*) ;;
    *) fail "没写理由的声明没被判红：$out" ;;
esac

# 6e. 字段名写错（`need_gateway`）：不认识的字段不许当成没写。
printf '# step: requires=host_shell produces=a_out need_gateway=0 — a 的理由\nrun_step a\n' >"$TMP/typo.sh"
out="$(bash "$CHECK" --script "$TMP/typo.sh" 2>&1 || true)"
case "$out" in
    *"不认识的字段"*) ;;
    *) fail "写错的字段名没被判红：$out" ;;
esac

# 6f. `needs_gateway` 写成 2：闭集之外的值不许当成 0。
printf '# step: requires=host_shell produces=a_out needs_gateway=2 — a 的理由\nrun_step a\n' >"$TMP/bad_flag.sh"
out="$(bash "$CHECK" --script "$TMP/bad_flag.sh" 2>&1 || true)"
case "$out" in
    *"只认 0/1"*) ;;
    *) fail "闭集之外的 needs_gateway 没被判红：$out" ;;
esac

# 7. 重复产出同名东西：两个步骤都自称装了 cluster，后面的依赖就无从判断该等谁。
{ decl a host_shell cluster 0; decl b host_shell cluster 0; } >"$TMP/dup.sh"
if bash "$CHECK" --script "$TMP/dup.sh" >/dev/null 2>&1; then
    fail "重复产出被判成绿"
fi

# 8. 同一个步骤写了两次：第二条声明会把第一条悄悄顶掉。
{ decl a host_shell a_out 0; decl a host_shell b_out 0; } >"$TMP/twice.sh"
if bash "$CHECK" --script "$TMP/twice.sh" >/dev/null 2>&1; then
    fail "同一步骤声明两次被判成绿"
fi

echo "restore order OK"
