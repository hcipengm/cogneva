#!/usr/bin/env bash
# 同一份字节只买一次编译：垫片按账本里已有的事实判，不给 `Split` 留口子。
#
# 判的是「改一段编一段」这条习惯在读数上长什么样：同一棵树、同一个 HEAD、同一种
# 编译命令出现第二次 ⇒ 拒，除非头提交写了 `Recompiled: <为什么上一次不够>`。
# `build` 与 `test` 是两种命令，各一次不算重复；`check` 不产出可推的字节，不受限。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
shim="$root/scripts/hooks/cargo"
[ -f "$shim" ] || { echo "FAIL: 垫片不在：$shim" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree"
git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
echo hi >"$tree/a.txt"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base
# 一次真编过的树是**有产物**的树：垫片只在产物还在时才把「编过了」当真。
# 产物不在时那一次编译的字节已经不在盘上，再编一次是重建，不是重复购买。
printf 'target/\n' >"$tree/.gitignore"
git -C "$tree" add .gitignore >/dev/null
git -C "$tree" commit -qm gitignore
mkdir -p "$tree/target/debug/deps"
echo artifact >"$tree/target/debug/deps/liba.rlib"

ledger="$tmp/ledger.tsv"
pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

run() { # $@ = cargo 参数
  ( cd "$tree" && REAL_CARGO=/bin/true COGNEVA_COMPILE_LEDGER="$ledger" bash "$shim" "$@" 2>&1 )
}

out=$(run test); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "第一次 test：放行" || bad "第一次 test 被拒（rc=$rc）：$out"

out=$(run test); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "不再跑第二次"; } \
  && ok "同一 HEAD 上第二次 test：拒，并说清修法" \
  || bad "同一 HEAD 上第二次 test 没被拒（rc=$rc）：$out"

out=$(run build); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "同一 HEAD 上换一种命令（build）：放行" || bad "build 被误拒（rc=$rc）：$out"

out=$(run test -p cog-core); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "同一 HEAD 上另一条命令（不同目标）：放行" || bad "不同目标的 test 被误拒（rc=$rc）：$out"

out=$(run check); rc=$?
out2=$(run check); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "check 不受限：编辑期随时可用" || bad "check 被拒（rc=$rc）：$out2"

# 产物被外部删掉（缓存回收、人手 rm）：同一 HEAD 同一条命令再跑一次是**重建**。
rm -rf "$tree/target/debug/deps"
out=$(run test); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "重建"; } \
  && ok "产物被删之后同一条命令放行，并说清这是重建" \
  || bad "产物被删之后仍被拒（rc=$rc）：$out"
mkdir -p "$tree/target/debug/deps"
echo artifact >"$tree/target/debug/deps/liba.rlib"

git -C "$tree" commit -q --allow-empty -m "another batch"
out=$(run test); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "新 HEAD（先提交）：test 再次放行" || bad "新 HEAD 上的 test 被拒（rc=$rc）：$out"

git -C "$tree" commit -q --allow-empty -m "a retry" -m "Recompiled: 上一次跑的是旧镜像，红的与本批无关"
out=$(run test); rc=$?
out2=$(run test); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "头提交写了 Recompiled：同一 HEAD 再跑一次放行" || bad "Recompiled 没有放行（rc=$rc）：$out2"

# 拆批声明要有理由码，也要点名被排除的行（干净树 + 台账说还有未完成行）。
cat >"$tmp/ledger" <<'LEDGER'
#!/usr/bin/env bash
[ "$1" = ledger ] && [ "$2" = editing ] && exit 1
exit 0
LEDGER
chmod +x "$tmp/ledger"
split_run() { # $1 = 声明
  ( cd "$tree" && REAL_CARGO=/bin/true COGNEVA_COMPILE_LEDGER="$ledger" \
      COGNEVA_BIN="$tmp/ledger" COGNEVA_BATCH_SPLIT="$1" bash "$shim" doc 2>&1 )
}

out=$(split_run "2 — 共用同一个测试目标")
rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "排除"; } \
  && ok "拆批只写理由、不点名排除行：拒，并说清修法" \
  || bad "缺排除清单没被拒（rc=$rc）：$out"

out=$(split_run "5 — 理由 — 排除: S1")
rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "2/3/4"; } \
  && ok "拆批理由码不在 2/3/4：拒" \
  || bad "错的理由码没被拒（rc=$rc）：$out"

out=$(split_run "2 — 共用同一个测试目标 — 排除: S1,S2")
rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "按声明拆分"; } \
  && ok "理由码与被排除的行都写了：放行" \
  || bad "完整的拆批声明被误拒（rc=$rc）：$out"

printf '\none-batch-one-compile.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
