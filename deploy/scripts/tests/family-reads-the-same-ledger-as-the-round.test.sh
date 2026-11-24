#!/usr/bin/env bash
# 编译前那道门（`scripts/hooks/family.py`）读的台账，必须与 `round.py` 读的是同一本。
#
# 解析顺序只有一份，在 `todo_source.default_spec()` 里：环境变量 → 本机仓配置的两格
# （`cogneva.privateRoot` ＋ `cogneva.workLedger`）→ 本机声明 → 一份不存在的路径。
# family.py 自己再解一遍时，配好的那一本它看不见：读不到被报成「这台 worker 本来就
# 不记待办」，批次只剩工作区改动，缺口整批看不见。
#
# 判两件事：
#   1. 哪里都没配台账：不崩，明说「没有配置 worker 台账」，按工作区改动算；
#   2. 配在本机仓的两格：读的就是那一本，不报「没有配置」。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
unset COGNEVA_TODO_SOURCE COGNEVA_TODO_LEDGER COGNEVA_WORKER_LEDGER
root=$(git rev-parse --show-toplevel)
family_src="$root/scripts/hooks/family.py"
[ -f "$family_src" ] || { echo "FAIL: family.py 不在：$family_src" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks" "$tree/crates/a/src" "$tree/specs"
cp "$family_src" "$tree/scripts/hooks/family.py"
cp "$root/scripts/hooks/todo_source.py" "$tree/scripts/hooks/todo_source.py"
cp "$root/scripts/hooks/round.py" "$tree/scripts/hooks/round.py"
echo a >"$tree/crates/a/src/x.rs"
git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm base
BASE="$(git -C "$tree" rev-parse HEAD)"
git -C "$tree" branch -f origin/main "$BASE"
# 本机声明那一格也指到不存在的路径：两处都没有，才轮到「没有台账」这一支。
export HOME="$tmp/home"
mkdir -p "$HOME"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
run() { ( cd "$tree" && python3 "$tree/scripts/hooks/family.py" --all-todos ) 2>&1; }

# 1. 哪里都没配：不崩，明说。
out=$(run)
if printf '%s' "$out" | grep -q "没有配置 worker 台账"; then
  if printf '%s' "$out" | grep -q "Traceback\|UnboundLocalError"; then
    bad "没有台账那一支崩了：$out"
  else
    ok "没有台账：不崩，明说是「没有台账」而不是「全做完了」"
  fi
else
  bad "没有台账时没说清楚（或崩在别处）：$out"
fi

# 2. 配在本机仓的两格：读的就是那一本。
printf '| CC01 | 一条待办 | 还没动 ｜ touches: crates/a/src/x.rs | 改三处 | 未落 |\n' \
  >"$tree/specs/ledger.md"
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm "the worker's ledger" >/dev/null
git -C "$tree" config cogneva.privateRoot "$tree"
git -C "$tree" config cogneva.workLedger specs/ledger.md
out=$(run)
if printf '%s' "$out" | grep -q "没有配置 worker 台账"; then
  bad "台账配在本机仓的两格里，这一道门却说没有：$out"
elif printf '%s' "$out" | grep -q "family: 台账 $tree/specs/ledger.md" \
  && printf '%s' "$out" | grep -q "CC01"; then
  ok "台账配在本机仓的两格：读的是同一本，条目也进来了"
else
  bad "读到的不是那一本，或者条目没进来：$out"
fi

# 3. 一条 `touches: unknown` 的待办：仍在本批，但没有触点可判，不挡编译。
#    属主 2026-11-24：触点不是判据，能不能写出终态才是。它说的是「这一遍该动哪儿
#    还说不准」，不是「这一轮不排」；把它算成缺口，一条长期条目就能让整棵树永久编不了。
printf '| CC01 | 长期条目 | 已收口两处 ｜ touches: unknown — 长期：按面开新行 | 余下逐面 | 未落 |\n' \
  >"$tree/specs/ledger.md"
out=$(run); rc=$?
if [ "$rc" -ne 0 ]; then
  bad "一条 touches: unknown 的长期条目把编译门挡住了（rc=$rc）：$out"
elif printf '%s' "$out" | grep -q "仍在本批" && printf '%s' "$out" | grep -q "CC01"; then
  ok "长期条目仍在本批、只缺触点读数，也不挡编译"
else
  bad "长期条目没被说明白：$out"
fi

printf '\nfamily-reads-the-same-ledger-as-the-round.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
