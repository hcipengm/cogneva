#!/usr/bin/env bash
# `check-compile-discipline.sh` 的离线测试：夹具是一本假账本 + 一棵小树。
#
# 判的是「一批编辑只付一次编译」这条纪律的两个方向：
#   1. 编辑在编译之后 ⇒ 红（改了没编译，就想推）。
#   2. 一批里编译多次是读数：红的改到绿，跑几次记几次，不判红。
#   3. 编辑在编译之前、一批只编译一次 ⇒ 绿。
#   4. 账本读不到 ⇒ 不判（量不到不是零次），也不假绿成「通过」的措辞。
set -uo pipefail
# 夹具仓通过 `git -C <dir>` 或子 shell 的 `cd` 写，两者都不改调用方的仓：可是外层
# 带进来的绝对 `GIT_DIR` / `GIT_WORK_TREE` / `GIT_INDEX_FILE` 会让每一次调用写进**调用方**
# 的仓。一次清干净，文件作用域，后面的用例都覆盖。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
gate="$root/deploy/scripts/check-compile-discipline.sh"
bin="${COGNEVA_BIN:-$root/target/debug/cogneva}"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }
[ -x "$bin" ] || { echo "FAIL: 平台二进制不在：$bin" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

tree="$tmp/tree"
mkdir -p "$tree/deploy/scripts" "$tree/scripts/hooks" "$tree/crates/demo/src"
cp "$gate" "$tree/deploy/scripts/check-compile-discipline.sh"
echo "fn main() {}" >"$tree/crates/demo/src/lib.rs"
cat >"$tree/Cargo.toml" <<'TOML'
[workspace]
members = ["crates/demo"]
TOML
git -C "$tree" init -q
git -C "$tree" config user.email t@t; git -C "$tree" config user.name t
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base
git -C "$tree" branch -f main HEAD

ledger="$tmp/ledger.tsv"
stamp() { date -u -d "$1" +%Y-%m-%dT%H:%M:%SZ; }
record() { printf '%s\t%s\t%s\t-\t-\n' "$(stamp "$1")" "$tree" "$2" >>"$ledger"; }

run() { COGNEVA_BIN="$bin" COGNEVA_COMPILE_LEDGER="$ledger" COGNEVA_PUSH_BASE=main bash "$tree/deploy/scripts/check-compile-discipline.sh" 2>&1; }

# 1. 没有账本：不判。
out="$(COGNEVA_BIN="$bin" COGNEVA_COMPILE_LEDGER="$tmp/missing.tsv" COGNEVA_PUSH_BASE=main bash "$tree/deploy/scripts/check-compile-discipline.sh" 2>&1)"; rc=$?
{ [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "没判"; } && ok "账本读不到：退 3（没判），不假绿" || bad "账本读不到时的行为不对（rc=$rc）：$out"

# 2. 编译之后又改了文件 ⇒ 红。
echo "// edit after compile" >>"$tree/crates/demo/src/lib.rs"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "a change"
record "2000-01-01T00:00:00Z" "check"
out="$(run)"; rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "最后一次编译之后"; } && ok "编辑在编译之后：红" || bad "编辑在编译之后没红（rc=$rc）：$out"

# 3. 先编译后编辑（顺序对），一批只编译一次 ⇒ 绿。
touch -d "2000-01-01T00:00:10Z" "$tree/crates/demo/src/lib.rs"
record "2030-01-01T00:00:00Z" "check"
git -C "$tree" commit -q --allow-empty -m "no-op to move base time"
out="$(run)"; rc=$?
[ "$rc" -eq 0 ] && ok "编辑在前、编译在后、只编译一次：绿" || bad "正确的批次被拒（rc=$rc）：$out"

# 4. 一批里编译多次是**读数**不是错：红了改到绿，跑几次记几次（属主 2026-11-21）。
record "2030-01-01T00:05:00Z" "test"
out=$(run); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "2 次编译"; } \
  && ok "多次编译：绿，并把次数印出来" || bad "多次编译被当成错（rc=$rc）：$out"
record "2030-01-01T00:06:00Z" "check"
out=$(run); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "3 次编译"; } \
  && ok "再跑一次：仍然绿，次数跟着涨" || bad "第三次编译被拒（rc=$rc）：$out"

# 5. 开着一轮时用**这一轮的 code_rev**，不用 `origin/main`：三道门量同一件事。
#    读数里要说得出来用的是哪个基准——两个人拿两个基准问同一批，量出来就是两个数。
round_rev="$(git -C "$tree" rev-parse main)"
# 轮书：git ref 上的一版载荷。`code_rev` 就是这一轮开工时的代码 rev——
# 与另外两道门量的是同一个数。
set_round() { # $1 = code_rev
  blob=$(printf '{"code_rev":"%s"}' "$1" | git -C "$tree" hash-object -w --stdin)
  git -C "$tree" update-ref refs/cogneva/round "$blob"
}
set_round "$round_rev"
touch -d "2000-01-01T00:00:10Z" "$tree/crates/demo/src/lib.rs"
record "2030-01-01T00:07:00Z" "check"
out="$(COGNEVA_BIN="$bin" COGNEVA_COMPILE_LEDGER="$ledger" "$bin" ledger compiled --root "$tree" --base main --ledger "$ledger" 2>&1)"; rc=$?
{ printf '%s' "$out" | grep -q "这一轮的 code_rev"; } \
  && ok "开着一轮：基准是这一轮的 code_rev，而且说出来了" \
  || bad "没按这一轮的 code_rev 判（rc=$rc）：$out"
# 5b. 基线的 rev 在树里不存在：报「没判」（退 3），不能读成「没有改动」。
set_round "000000000000"
out="$(COGNEVA_BIN="$bin" "$bin" ledger compiled --root "$tree" --base main --ledger "$ledger" 2>&1)"; rc=$?
{ [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "没判"; } \
  && ok "基线读不出来：退 3（没判），不读成「没有改动」" \
  || bad "基线读不出来时的行为不对（rc=$rc）：$out"
git -C "$tree" update-ref -d refs/cogneva/round

printf '\ncompile-discipline.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
