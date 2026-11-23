#!/usr/bin/env bash
# `default_spec()` 的第三档：本的仓配置里那两格。
#
# 带外 worker 的台账住在一个私有仓里，名字与位置都不该写进公开树。地址是本机的
# `cogneva.privateRoot`，文件名是 `cogneva.workLedger`；两条都是配置，不进树。
#
# 判四件事：
#   1. 两格都在、文件是相对路径 ⇒ 解析成「私有仓里的那一本」；
#   2. 只有地址、没有文件名 ⇒ 不认（不回落到仓里随便一本）；
#   3. 文件名是绝对路径 ⇒ 原样用；
#   4. 两格都没有 ⇒ 回到原来的那条路（不存在的那本），不凭空造一个。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
unset COGNEVA_TODO_SOURCE COGNEVA_WORKER_LEDGER COGNEVA_TODO_LEDGER
root=$(git rev-parse --show-toplevel)
loader="$root/scripts/hooks/todo_source.py"
[ -f "$loader" ] || { echo "FAIL: 加载器不在：$loader" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
work="$tmp/work"
mkdir -p "$work" "$tmp/home" "$tmp/private/specs"
printf '| S1 | 内容 | 未落 | 判据 | touches: crates/a.rs |\n' >"$tmp/private/specs/ledger.md"
git -C "$work" init -q
git -C "$work" config user.email t@t
git -C "$work" config user.name t

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# HOME 指到空的目录：不读这台机器上真实的 ~/.config/cogneva/todo-ledger。
ask() { (cd "$work" && HOME="$tmp/home" python3 -c "
import sys
sys.path.insert(0, '$root/scripts/hooks')
import todo_source
print(todo_source.default_spec())
" 2>&1); }

# 1. 相对文件名 ⇒ 接在地址后面。
git -C "$work" config cogneva.privateRoot "$tmp/private"
git -C "$work" config cogneva.workLedger specs/ledger.md
out=$(ask)
[ "$out" = "md:$tmp/private/specs/ledger.md" ] \
    && ok "地址＋相对文件名：解析成私有仓里的那一本" \
    || bad "相对文件名的解析不对：$out"

# 2. 只有地址：不认，回到默认那条「不存在的那本」。
git -C "$work" config --unset cogneva.workLedger
out=$(ask)
case "$out" in
  md:*/.local/state/cogneva/todo-ledger.md) ok "只有地址没有文件名：不拿仓里随便一本顶替" ;;
  *) bad "只有地址时的返回值不对：$out" ;;
esac

# 3. 绝对文件名：原样用，不需要地址。
git -C "$work" config --unset cogneva.privateRoot
git -C "$work" config cogneva.workLedger "$tmp/private/specs/ledger.md"
out=$(ask)
[ "$out" = "md:$tmp/private/specs/ledger.md" ] \
    && ok "绝对文件名：原样用" \
    || bad "绝对文件名的解析不对：$out"

# 4. 两格都没有：默认那条路，不造一个不存在的地址。
git -C "$work" config --unset cogneva.workLedger
out=$(ask)
case "$out" in
  md:*/.local/state/cogneva/todo-ledger.md) ok "两格都没有：回到默认那条路" ;;
  *) bad "没有配置时的返回值不对：$out" ;;
esac

printf 'todo-source-ledger-from-repo-config.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
