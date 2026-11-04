#!/usr/bin/env bash
# 读一份 eval-table 报表，判「这一臂的天花板与值守的目标是不是同一个数」。
#
# 为什么要有这个读数：目标写成「题数 × 种子 × 外壳」时，只要有一道题这一臂测不了，
# 天花板就低于目标数——判据永远不亮，而「一直在跑」与「快到了」在日志上同形。
# 报表侧（`## skipped` 逐题点名）与循环侧（值守的 TARGET）过去各说各的，没有一条门
# 把两半对起来量；这一份就是那条门，判四件事：
#
#   * 报表自己自洽：每个格子的 `skipped=` 等于 `## skipped` 里点名的题数；
#   * `跳过行数 + 分母行数` 等于这条臂的题集行数（--sky，题集 × 种子 × 外壳）；
#   * **给出答案**的行数等于值守的目标（--target）；
#   * 分母不是靠「全格报错」撑满的：一格都没给出答案的报表，一行也没量到。
#
# 分母（`total=`）与已判是两件事：rig 里写明 `total` 是「参与计分的题数，跑不动的题按
# 没过计、不从这里扣掉」，于是 503 把所有题都挡在门外时，分母照样是满的。2026-10-19 实测：
# 主报表 36 格各 `total=40 errored=40`，检查点里判过 **0** 例——照分母记成「已判」，就是
# 一张一例都没判的报表读出 1440 行。所以已判＝分母减掉 errored 与 blocked。
#
# 两个 `--` 各自可选：只给一个就只判那一条。不给就只印读数与自洽结论。
# 判据全部按报表自己声明的数字推，不把 198/240/42 写死——写死的那天换题集就红。
#
# 用法：eval-ceiling-reading.sh <report.md> [--target <已判目标>] [--sky <题集行数>]
# 退出码：0 读数成立；1 读数不成立；3 报表读不出来（不许当 0 读）。

set -uo pipefail

REPORT=${1:-}
TARGET=""
SKY=""
shift 2>/dev/null || true
while [ $# -gt 0 ]; do
  case "$1" in
    --target) TARGET=${2:-}; shift 2;;
    --sky) SKY=${2:-}; shift 2;;
    *) echo "unknown argument: $1" >&2; exit 3;;
  esac
done
[ -n "$REPORT" ] && [ -f "$REPORT" ] || { echo "cannot read the report: ${REPORT:-<none>}" >&2; exit 3; }

# 跳过总行数：每行自带「这题被几个 scaffold × 几个 seed 跳过」，逐行相加。
skipped_rows=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | awk -F'\t' '$1=="skipped"{
  s=1; e=1;
  if (match($0,/scaffolds=[0-9]+/)) s=substr($0,RSTART+10,RLENGTH-10);
  if (match($0,/seeds=[0-9]+/))     e=substr($0,RSTART+6,RLENGTH-6);
  n += s*e } END{print n+0}')
skipped_ids=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | awk -F'\t' '$1=="skipped"{print $3}' | sort -u | grep -c . || true)

# 两个数分开记：分母（每个格子的 total= 相加）与「没给出答案」的行数（errored= 与
# blocked= 相加，两者互斥）。已判＝分母减掉没给出答案的那部分——只看分母时，一整轮
# 全被 503 挡住的报表与真判满的那一份在数上同形。
read -r denominator_rows errored_rows < <(grep $'^cell\t' "$REPORT" | awk '{
  t = 0;
  if (match($0,/total=[0-9]+/))   t = substr($0,RSTART+6,RLENGTH-6);
  if (match($0,/errored=[0-9]+/)) e += substr($0,RSTART+8,RLENGTH-8);
  if (match($0,/blocked=[0-9]+/)) e += substr($0,RSTART+8,RLENGTH-8);
  n += t } END{print n+0, e+0}')
judged_rows=$(( denominator_rows - errored_rows ))
[ "$judged_rows" -lt 0 ] && judged_rows=0
cells=$(grep -c $'^cell\t' "$REPORT" || true)

# 自洽：每个格子的 skipped= 必须等于点名的题数（格子与格子之间也是同一个数）。
cell_skipped=$(grep $'^cell\t' "$REPORT" | grep -o 'skipped=[0-9]*' | cut -d= -f2 | sort -u | tr '\n' ',' | sed 's/,$//')

echo "report=$REPORT"
echo "skipped_ids=$skipped_ids"
echo "skipped_rows=$skipped_rows"
echo "per_seed_cells=$cells"
echo "cell_skipped=$(printf '%s' "$cell_skipped")"
echo "denominator_rows=$denominator_rows"
echo "errored_rows=$errored_rows"
echo "judged_rows=$judged_rows"
echo "sum_rows=$((skipped_rows + denominator_rows))"
[ -n "$TARGET" ] && echo "target=$TARGET"
[ -n "$SKY" ] && echo "sky=$SKY"

[ -n "$REPORT" ] || exit 3
if [ "$cells" -eq 0 ]; then
  echo "verdict=unmeasurable reason=no-per-seed-cells"
  exit 3
fi

verdict=0
# 自洽：格子里的 skipped 必须与点名数一致；不一致说明报表自己都说不清跳了几行。
if [ "$skipped_rows" -eq 0 ] && [ "$cell_skipped" != "0" ]; then
  echo "inconsistent=cell-skipped-without-a-named-case"
  verdict=1
elif [ "$skipped_rows" -gt 0 ] && [ "$cell_skipped" != "$skipped_ids" ]; then
  echo "inconsistent=cell-skipped=$cell_skipped-named=$skipped_ids"
  verdict=1
fi
# 题集侧：跳过 + 分母 必须正好等于这条臂的题集行数。这里用分母而不是已判——报错的行
# 照样占着题集里的一行，按已判算会把「这一臂有多少行」与「这一轮判出多少」混成一个数。
if [ -n "$SKY" ]; then
  if [ "$((skipped_rows + denominator_rows))" -ne "$SKY" ]; then
    echo "sky_gap=$((SKY - skipped_rows - denominator_rows))"
    verdict=1
  fi
fi
# 一行也没给出答案：分母满不等于量到了。全格 503 的报表在算术上可能与目标严丝合缝。
if [ "$denominator_rows" -gt 0 ] && [ "$judged_rows" -eq 0 ]; then
  echo "no-answer=every-row-errored-or-blocked"
  verdict=1
fi
# 循环侧：已判行数必须等于值守的目标。
if [ -n "$TARGET" ]; then
  if [ "$judged_rows" -ne "$TARGET" ]; then
    echo "target_gap=$((TARGET - judged_rows))"
    verdict=1
  fi
fi
echo "verdict=$([ "$verdict" -eq 0 ] && echo holds || echo does-not-hold)"
exit "$verdict"
