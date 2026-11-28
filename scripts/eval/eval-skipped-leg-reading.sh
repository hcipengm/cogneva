#!/usr/bin/env bash
# 读一份 eval-table 报表的 `## skipped` 段，判「跳过这一腿」是否真的成立。
#
# 为什么要有这个读数：跳过带图题的代码落了，但「报表里到底有没有那一段」只有在**跑完的产物**
# 上才读得到——源码里有字面量、分支没走到，测试照样绿。这一份把产物本身当输入：
#
#   * `## skipped` 段在不在；
#   * 每一行点名的题号是不是子集里那些**带图**的题（不是个数对就算）；
#   * 每行 `scaffolds=` `seeds=` 与报表自己的格子数是否一致；
#   * 行数 × scaffolds × seeds 是否等于跳过的总行数（7 × 2 × 3 = 42 这一条算术）。
#
# 判据写法：全部按「报表自己声明的格子」推，不把 7/2/3 写死——写死的那天换子集就红。
#
# 用法：eval-skipped-leg-reading.sh <report.md> [--subset <subset.tsv>]
#   给了 --subset 就要求跳过的题号 ⊆ 子集里**带图**的那些（子集不含图字段时只查「在子集里」）。
# 退出码：0 读数成立；1 读数不成立；3 报表读不出来（不许当 0 读）。
#
# 复跑产物（零模型调用，与池子开不开无关）：把这一跑里带图的那几道题写成一份两列
# `benchmark<TAB>case` 子集起一个 eval-table Pod，参数与登记跑法相同，只把 `--subset`
# 换成它、`--scaffolds gepa,agentflow --seeds 0,1,2 --vision none`，报表写到另一个
# `--out`；跑完再拿本脚本读它。
# 2026-10-19 实测产物：7 行 skipped、scaffolds=2、seeds=3、6 个格子、42 行、verdict=holds。
set -uo pipefail

REPORT=${1:-}
SUBSET=""
shift 2>/dev/null || true
while [ $# -gt 0 ]; do
  case "$1" in
    --subset) SUBSET=${2:-}; shift 2;;
    *) echo "unknown argument: $1" >&2; exit 3;;
  esac
done
[ -n "$REPORT" ] && [ -f "$REPORT" ] || { echo "cannot read the report: ${REPORT:-<none>}" >&2; exit 3; }

skipped=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | grep -c $'^skipped\t' || true)
scaffolds=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | grep -o 'scaffolds=[0-9]*' | cut -d= -f2 | sort -u)
seeds=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | grep -o 'seeds=[0-9]*' | cut -d= -f2 | sort -u)
cells=$(grep -c $'^cell\thle\t' "$REPORT" || true)

case_ids=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | awk -F'\t' '$1=="skipped"{print $3}' | sort -u)
# 每个题号只印一行，`scaffolds=`/`seeds=` 说它被跳过几次：跳过总行数 = 题号数 × 两者之积。
uniq_cases=$(printf '%s\n' "$case_ids" | grep -c . || true)
# 每一行自带「这题被几个 scaffold × 几个 seed 跳过」，逐行相加才是跳过总行数。
total_rows=$(sed -n '/^## skipped$/,/^$/p' "$REPORT" | awk -F'\t' '$1=="skipped"{
  s=1; e=1;
  if (match($0,/scaffolds=[0-9]+/)) s=substr($0,RSTART+10,RLENGTH-10);
  if (match($0,/seeds=[0-9]+/))     e=substr($0,RSTART+6,RLENGTH-6);
  n += s*e } END{print n+0}')

echo "report=$REPORT"
echo "skipped_lines=$skipped"
echo "skipped_case_ids=$uniq_cases"
echo "scaffolds=$(echo $scaffolds | tr ' ' ',')"
echo "seeds=$(echo $seeds | tr ' ' ',')"
echo "per_seed_cells=$cells"
echo "skipped_rows=$total_rows"

verdict=0
[ "$skipped" -gt 0 ] || { echo "verdict=no-section"; verdict=1; }
if [ -n "$SUBSET" ] && [ -f "$SUBSET" ]; then
  missing=0
  while read -r _ c; do
    [ -n "$c" ] || continue
    printf '%s\n' "$case_ids" | grep -qx -- "$c" || { echo "missing_from_report=$c"; missing=1; }
  done < <(grep -v '^#' "$SUBSET" | awk -F'\t' 'NF>=2{print $1"\t"$2}')
  extra=0
  while read -r c; do
    [ -n "$c" ] || continue
    awk -F'\t' -v c="$c" 'NF>=2 && $2==c{f=1} END{exit !f}' "$SUBSET" || { echo "not_in_subset=$c"; extra=1; }
  done <<<"$case_ids"
  [ "$missing" -eq 0 ] && [ "$extra" -eq 0 ] || verdict=1
elif [ -n "$SUBSET" ]; then
  echo "cannot read the subset: $SUBSET" >&2; exit 3
fi
# 行数算术：每个题号都被每一对 (scaffold, seed) 跳过一次。
[ "$verdict" -eq 0 ] && [ "$total_rows" -gt 0 ] || verdict=1
echo "verdict=$([ "$verdict" -eq 0 ] && echo holds || echo does-not-hold)"
exit "$verdict"
