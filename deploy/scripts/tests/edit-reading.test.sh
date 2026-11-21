#!/usr/bin/env bash
# `scripts/hooks/edit-reading.py`：`代码` 读数由程序算，三个条件各一条红证。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
reader="$root/scripts/hooks/edit-reading.py"
[ -f "$reader" ] || { echo "FAIL: $reader 不在" >&2; exit 1; }

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"; mkdir -p "$tree/crates/demo/src" "$tree/crates/demo/tests" "$tree/scripts/hooks"
cp "$reader" "$tree/scripts/hooks/edit-reading.py"; cp "$root/scripts/hooks/todo_source.py" "$tree/scripts/hooks/"
git -C "$tree" init -q; git -C "$tree" config user.email t@t; git -C "$tree" config user.name t
echo "fn main() {}" >"$tree/crates/demo/src/lib.rs"
printf '#[test]\nfn proves_it() { assert!(true); }\n' >"$tree/crates/demo/tests/proof.rs"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base; base=$(git -C "$tree" rev-parse HEAD)

row() { printf '| S1 | x | 未落 | %s | touches: crates/demo/src/lib.rs |\n' "$1"; }
ledger="$tree/ledger.md"
run() { COGNEVA_TODO_SOURCE="md:$ledger" python3 "$tree/scripts/hooks/edit-reading.py" --root "$tree" --base "$base" "$@" 2>&1; }

pass=0; fail=0
ok(){ printf 'ok    - %s\n' "$1"; pass=$((pass+1)); }
bad(){ printf 'FAIL- %s\n' "$1" >&2; fail=$((fail+1)); }

# 1. 不是那个形状：读作未填。
row "代码: crates/demo/tests/proof.rs" >"$ledger"
out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "不是"; } && ok "散文写法读作未填" || bad "散文被当成填了（rc=$rc）：$out"

# 2. 形状对，但测试文件不在本批 diff 里。
row "代码: test = crates/demo/tests/proof.rs::proves_it" >"$ledger"
out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "diff 里没有改动"; } && ok "测试不在本批 diff：false" || bad "旧测试被认了（rc=$rc）：$out"

# 3. 文件在 diff 里、函数也在 ⇒ 真。
echo "// edit" >>"$tree/crates/demo/tests/proof.rs"; git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "edit the test" >/dev/null
out=$(run); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "代码=true  S1"; } && ok "改过且函数在：true" || bad "合格的读数被判假（rc=$rc）：$out"

# 4. 给了绿名单而名字不在里面 ⇒ false。
printf 'some_other_test\n' >"$tmp/passed.txt"
out=$(run --passed "$tmp/passed.txt"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "绿名单"; } && ok "没在绿名单里：false" || bad "没跑绿的被认了（rc=$rc）：$out"

# 5. 名字在绿名单里 ⇒ true。
printf 'proves_it\n' >"$tmp/passed.txt"
out=$(run --passed "$tmp/passed.txt"); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "绿名单里有它：true" || bad "跑绿的被判假（rc=$rc）：$out"

# 6. 终态档要求绿名单：不给就判不了，不是绿。
out=$(run --stage final); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "终态档要绿名单"; } \
  && ok "终态档没给绿名单：判不了（不是绿）" || bad "终态档缺绿名单却过了（rc=$rc）：$out"

# 7. 红→绿：在 code_rev 上就绿的测试，不算测到了这次改动。
printf 'proves_it\n' >"$tmp/passed.txt"; printf 'other\n' >"$tmp/red.txt"
out=$(run --stage final --passed "$tmp/passed.txt" --red-at-base "$tmp/red.txt"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "就是绿的"; } \
  && ok "基线就绿：不算终态" || bad "基线就绿的测试被认了（rc=$rc）：$out"

# 8. 基线红、HEAD 绿 ⇒ 终态真。
printf 'proves_it\n' >"$tmp/red.txt"
out=$(run --stage final --passed "$tmp/passed.txt" --red-at-base "$tmp/red.txt"); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "红→绿：终态真" || bad "红→绿被判假（rc=$rc）：$out"

# 9. 计数型：普查面还有剩就不算改完，归零才算。
cat >"$tmp/census.py" <<'CENSUS'
import sys
n = int(open(sys.argv[0] + ".count").read().strip())
print("surface\tpath")
for i in range(n):
    print(f"state-file\tfile{i}.rs")
print(f"  state-files: {n}")
CENSUS
printf '3' >"$tmp/census.py.count"
printf '| S1 | x | 未落 | 判据读数: state-files → 0 | touches: crates/demo/src/lib.rs |\n' >"$ledger"
out=$(run --census "$tmp/census.py"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "还剩 3 处"; } \
  && ok "计数型还有剩：false，并说出剩几处" || bad "计数没归零却过了（rc=$rc）：$out"

printf '0' >"$tmp/census.py.count"
out=$(run --census "$tmp/census.py"); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "计数归零：true" || bad "归零被判假（rc=$rc）：$out"

# 10. 计数型但没给普查器：判不了，不是绿。
out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "没给 --census"; } \
  && ok "计数型缺普查器：判不了" || bad "缺普查器被当成绿（rc=$rc）：$out"

printf '\nedit-reading.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
