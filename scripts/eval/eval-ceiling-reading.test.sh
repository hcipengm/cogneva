#!/usr/bin/env bash
# principle: feedback_a_sample_i_write_myself_only_tests_the_gate_against_itself
#
# `eval-ceiling-reading.sh` 的夹具：只喂合成报表，不连集群、不调模型。
#
# 五条方向，各对着读数会怎么错：
#   1 跑完的主臂（198 已判 + 42 跳过 = 240）⇒ 0，--target 198 --sky 240；
#   2 目标还写着 240（没缩到可测天花板）⇒ 1，且 target_gap=42；
#   3 格子 skipped 与点名数对不上 ⇒ 1（报表自己都说不清跳了几行）；
#   4 没有格子 ⇒ 3，不是 0；
#   5 报表不在 ⇒ 3。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-ceiling-reading.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

# 主臂跑完：6 个格子各判 33 例、各跳 7 例；跳过段点名 7 道题、每条 scaffolds=2 seeds=3。
{
  echo '## per seed'
  for s in gepa agentflow; do
    for seed in 0 1 2; do
      printf 'cell\thle\t%s\tseed=%s\tresolved=20\ttotal=33\tskipped=7\terrored=0\n' "$s" "$seed"
    done
  done
  echo
  echo '## skipped'
  for i in 1 2 3 4 5 6 7; do
    printf 'skipped\thle\thle-case-%s\tscaffolds=2\tseeds=3\tthe backbone cannot see images\n' "$i"
  done
} >"$TMP/complete.md"

out=$(bash "$SCRIPT" "$TMP/complete.md" --target 198 --sky 240 2>&1); rc=$?
[ "$rc" -eq 0 ] && ok "跑完的主臂（198＋42=240）判 holds" || bad "跑完的主臂：期望 rc=0，实得 $rc（$out）"
case "$out" in *"judged_rows=198"*) ok "无报错时分母即已判（6 格 ×33=198）" ;;
  *) bad "已判行数：期望 198，实得「$(printf '%s\n' "$out" | grep judged_rows)」" ;; esac
case "$out" in *"skipped_rows=42"*) ok "跳过行数按每行 scaffolds×seeds 相加（7×2×3=42）" ;;
  *) bad "跳过行数：期望 42，实得「$(printf '%s\n' "$out" | grep skipped_rows)」" ;; esac

# 目标没缩：值守还写 240，而这一臂只能判 198。
out=$(bash "$SCRIPT" "$TMP/complete.md" --target 240 --sky 240 2>&1); rc=$?
[ "$rc" -eq 1 ] && ok "目标没缩到可测天花板 ⇒ 1" || bad "目标没缩：期望 rc=1，实得 $rc"
case "$out" in *"target_gap=42"*) ok "点名缺口 42（正是跳过的那批）" ;;
  *) bad "缺口：期望 target_gap=42，实得「$(printf '%s\n' "$out" | grep target_gap)」" ;; esac

# 格子说跳 6、点名 7：报表自相矛盾。
sed 's/skipped=7/skipped=6/g' "$TMP/complete.md" >"$TMP/inconsistent.md"
out=$(bash "$SCRIPT" "$TMP/inconsistent.md" 2>&1); rc=$?
[ "$rc" -eq 1 ] && ok "格子跳过数与点名数不一致 ⇒ 1" || bad "不一致：期望 rc=1，实得 $rc"
case "$out" in *"inconsistent="*) ok "不一致时判词点名 inconsistent" ;; *) bad "不一致：判词没点名" ;; esac

# 全格报错：分母满、一行答案也没有。`total=` 是计分分母，不因跑不动而缩小——照分母记
# 「已判」，一张一例都没判的报表会读出满行（2026-10-19 在真报表上实测就是这样：
# 36 格各 total=40 errored=40，检查点判过 0 例，而读数印 judged_rows=1440）。
{
  echo '## per seed'
  for s in gepa agentflow; do
    for seed in 0 1 2; do
      printf 'cell\thle\t%s\tseed=%s\tresolved=0\ttotal=40\tskipped=0\terrored=40\tblocked=0\n' "$s" "$seed"
    done
  done
} >"$TMP/all-errored.md"
out=$(bash "$SCRIPT" "$TMP/all-errored.md" --target 198 --sky 240 2>&1); rc=$?
[ "$rc" -eq 1 ] && ok "全格报错 ⇒ 1（分母满不等于量到了）" || bad "全格报错：期望 rc=1，实得 $rc（$out）"
case "$out" in *"judged_rows=0"*) ok "全格报错 ⇒ 已判 0（分母 240 不记成已判）" ;;
  *) bad "全格报错：期望 judged_rows=0，实得「$(printf '%s\n' "$out" | grep judged_rows)」" ;; esac
case "$out" in *"no-answer=every-row-errored-or-blocked"*) ok "全格报错 ⇒ 判词点名 no-answer" ;;
  *) bad "全格报错：判词没点名 no-answer" ;; esac

# 没有格子：这一列没读数。
printf '## table\n## skipped\n\n' >"$TMP/nocells.md"
out=$(bash "$SCRIPT" "$TMP/nocells.md" 2>&1); rc=$?
[ "$rc" -eq 3 ] && ok "没有格子 ⇒ 3" || bad "没有格子：期望 rc=3，实得 $rc"

out=$(bash "$SCRIPT" "$TMP/nope.md" 2>&1); rc=$?
[ "$rc" -eq 3 ] && ok "报表不在 ⇒ 3" || bad "报表不在：期望 rc=3，实得 $rc"

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
