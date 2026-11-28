#!/usr/bin/env bash
# 开轮之前，这一批要交给平台的批计划器判一次（流程第 4 步：定批是固定程序）。
#
# 判的是**线接上了**：同一本台账，开轮那一句里说得出来「这一批的计划成立、几条、两种
# 验收形状」。接线之前 `open` 直接写书，计划器一次都没被问过——而「问过、成立」与
# 「没问」在读数上同形。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR
root=$(git rev-parse --show-toplevel)
bin="${COGNEVA_BIN:-$root/target/debug/cogneva}"
[ -x "$bin" ] || { echo "FAIL: 平台二进制不在：$bin" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree"
ledger="$tree/ledger.md"
# 五格行：两条未落、一条已落。已落那条不进批次。
cat >"$ledger" <<'MD'
| N01 | 甲 | 现在 | 差距 ｜ touches: crates/a/src/x.rs | 未落 |
| N02 | 乙 | 现在 | 差距 ｜ touches: crates/a/src/y.rs | 未落 |
| N03 | 丙 | 现在 | — | 已落 |
MD
git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm base

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 工作树里也要有 HEAD：`open` 记的是代码树的 rev。
out="$(cd "$tree" && COGNEVA_TODO_LEDGER="$ledger" "$bin" ledger open 2>&1)"; rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "计划成立（2 条，两种验收形状）"; } \
  && ok "开轮前把这一批交给了计划器，并说得出几条、两种形状" \
  || bad "计划器没被问（rc=$rc）：$out"

# 书里钉的就是那两条未落。
body=$(git -C "$tree" cat-file -p refs/cogneva/round 2>/dev/null)
{ printf '%s' "$body" | grep -q '"N01"' && printf '%s' "$body" | grep -q '"N02"' \
  && ! printf '%s' "$body" | grep -q '"N03"'; } \
  && ok "钉住的批次就是未落的那两条" \
  || bad "钉住的批次不对：$body"

# 空台账：说得出来「没有未完成的待办」，仍然不编一份空计划。
: >"$ledger"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm empty
out="$(cd "$tree" && COGNEVA_TODO_LEDGER="$ledger" "$bin" ledger open 2>&1)"; rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "没有未完成的待办"; } \
  && ok "空台账：明说没有待办" \
  || bad "空台账的行为不对（rc=$rc）：$out"

printf '\nledger-open-asks-the-planner.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
