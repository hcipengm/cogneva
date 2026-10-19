#!/usr/bin/env bash
# 顺序门的测试。每一条都从一个**真会坏**的形状出发：
#   - 依赖指向没人产出的东西（步骤被删掉、名字被改掉）；
#   - 等网关的步骤排在没有网关之前（空白机上的死循环）；
#   - 清单与脚本各缺一边（最难看出来的分叉）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECK="$ROOT/deploy/scripts/check-restore-order.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# 1. 真树是绿的。步骤表与脚本是同一棵树上的两份文件，任何一边漂了这条先红。
bash "$CHECK" >/dev/null || fail "真树的步骤表不闭合"

# 2. 空表不是绿：没有步骤的清单证明不了任何顺序。
printf '# 只有注释\n' >"$TMP/empty.tsv"
printf '#!/bin/sh\n' >"$TMP/empty.sh"
if bash "$CHECK" --steps "$TMP/empty.tsv" --script "$TMP/empty.sh" >/dev/null 2>&1; then
    fail "空步骤表被判成绿"
fi

# 3. 依赖一个没人产出的东西。
printf 'a\thost_shell\ta_out\t0\ta\nb\ta_out,b_out\tb_out\t0\tb\n' >"$TMP/unknown.tsv"
printf 'run_step a\nrun_step b\n' >"$TMP/unknown.sh"
out="$(bash "$CHECK" --steps "$TMP/unknown.tsv" --script "$TMP/unknown.sh" 2>&1 || true)"
case "$out" in
    *"b_out"*) ;;
    *) fail "没点出缺的是 b_out：$out" ;;
esac

# 4. 等网关的步骤排在没有网关之前；补上一个产出 gateway 的更早步骤后就该绿。
printf 'a\thost_shell\ta_out\t0\ta\nb\ta_out\tb_out\t1\tb\n' >"$TMP/gw.tsv"
printf 'run_step a\nrun_step b\n' >"$TMP/gw.sh"
out="$(bash "$CHECK" --steps "$TMP/gw.tsv" --script "$TMP/gw.sh" 2>&1 || true)"
case "$out" in
    *"要网关"*) ;;
    *) fail "等网关的步骤没被判红：$out" ;;
esac
printf 'a\thost_shell\ta_out,gateway\t0\ta\nb\ta_out,gateway\tb_out\t1\tb\n' >"$TMP/gw_ok.tsv"
printf 'run_step a\nrun_step b\n' >"$TMP/gw_ok.sh"
bash "$CHECK" --steps "$TMP/gw_ok.tsv" --script "$TMP/gw_ok.sh" >/dev/null ||
    fail "网关在前时仍被判红"

# 5. 清单有、脚本没有；脚本有、清单没有。两个方向都要红。
printf 'a\thost_shell\ta_out\t0\ta\nb\ta_out\tb_out\t0\tb\n' >"$TMP/two.tsv"
printf 'run_step a\n' >"$TMP/short.sh"
out="$(bash "$CHECK" --steps "$TMP/two.tsv" --script "$TMP/short.sh" 2>&1 || true)"
case "$out" in
    *"没有 run_step b"*) ;;
    *) fail "脚本少一步没被判红：$out" ;;
esac
printf 'run_step a\nrun_step b\nrun_step c\n' >"$TMP/long.sh"
out="$(bash "$CHECK" --steps "$TMP/two.tsv" --script "$TMP/long.sh" 2>&1 || true)"
case "$out" in
    *"run_step c 在步骤清单里没有行"*) ;;
    *) fail "脚本多一步没被判红：$out" ;;
esac

# 6. 重复产出同名东西：两个步骤都自称装了 cluster，后面的依赖就无从判断该等谁。
printf 'a\thost_shell\tcluster\t0\ta\nb\thost_shell\tcluster\t0\tb\n' >"$TMP/dup.tsv"
printf 'run_step a\nrun_step b\n' >"$TMP/dup.sh"
if bash "$CHECK" --steps "$TMP/dup.tsv" --script "$TMP/dup.sh" >/dev/null 2>&1; then
    fail "重复产出被判成绿"
fi

echo "restore order OK"
