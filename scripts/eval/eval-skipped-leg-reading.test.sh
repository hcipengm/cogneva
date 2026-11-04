#!/usr/bin/env bash
# principle: feedback_a_sample_i_write_myself_only_tests_the_gate_against_itself
#
# `eval-skipped-leg-reading.sh` 的夹具：只喂合成报表，不连集群、不调模型。
#
# 四条方向，各对着读数会怎么错：
#   1 真形状（7 题 × scaffolds=2 × seeds=3）⇒ 0，且 `skipped_rows=42`（算术不是抄来的）；
#   2 段落不在 ⇒ 1（README/表格照旧，但这一腿没成立）；
#   3 点名的题号不在子集里 ⇒ 1（只看个数会让「跳错了题」照样绿）；
#   4 报表读不出来 ⇒ 3，不是 0。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-skipped-leg-reading.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

printf '# subset\nhle\thle-a\nhle\thle-b\nhle\thle-c\n' >"$TMP/subset.tsv"

cat >"$TMP/good.md" <<'EOF'
## per seed
cell	hle	gepa	seed=0	resolved=0	total=0	skipped=3	errored=0
cell	hle	agentflow	seed=0	resolved=0	total=0	skipped=3	errored=0

## skipped
skipped	hle	hle-a	scaffolds=2	seeds=1	the backbone cannot see images
skipped	hle	hle-b	scaffolds=2	seeds=1	the backbone cannot see images
skipped	hle	hle-c	scaffolds=2	seeds=1	the backbone cannot see images
EOF
out=$(bash "$SCRIPT" "$TMP/good.md" --subset "$TMP/subset.tsv" 2>&1); rc=$?
[ "$rc" -eq 0 ] && ok "真形状判 holds" || bad "真形状：期望 rc=0，实得 $rc（$out）"
case "$out" in *"skipped_rows=6"*) ok "跳过的总行数按每行的 scaffolds×seeds 相加（3×2=6）" ;;
  *) bad "行数算术：期望 skipped_rows=6，实得「$(printf '%s\n' "$out" | grep skipped_rows)」" ;; esac

grep -v '^## skipped' "$TMP/good.md" | grep -v '^skipped' >"$TMP/nosection.md"
out=$(bash "$SCRIPT" "$TMP/nosection.md" 2>&1); rc=$?
[ "$rc" -eq 1 ] && ok "段不在 ⇒ 1" || bad "段不在：期望 rc=1，实得 $rc"
case "$out" in *verdict=no-section*) ok "段不在时判词点名 no-section" ;; *) bad "段不在：判词没点名" ;; esac

sed 's/hle-c/hle-z/' "$TMP/good.md" >"$TMP/extra.md"
out=$(bash "$SCRIPT" "$TMP/extra.md" --subset "$TMP/subset.tsv" 2>&1); rc=$?
[ "$rc" -eq 1 ] && ok "跳错题（不在子集里）⇒ 1" || bad "跳错题：期望 rc=1，实得 $rc"
case "$out" in *not_in_subset=hle-z*) ok "跳错题时点名那个题号" ;; *) bad "跳错题：没点名题号" ;; esac

out=$(bash "$SCRIPT" "$TMP/nope.md" 2>&1); rc=$?
[ "$rc" -eq 3 ] && ok "报表不在 ⇒ 3" || bad "报表不在：期望 rc=3，实得 $rc"

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
