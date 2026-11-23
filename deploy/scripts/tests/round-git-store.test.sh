#!/usr/bin/env bash
# 带外 worker 的轮：书是**台账仓里的 `refs/cogneva/round`**，不是集群事件流。
#
# 台账是 `md:<路径>`，住在一个与其远端对齐的 git 仓里；它不需要
# 集群，也不该因为「事件流读不到」就不可用。这一面判五件事：
#   1. `open` 把这一轮的清单写进 ref，`status` 按它读出「还剩几条」；
#   2. 这条路径**一次也没碰事件流**（用一个会留痕的假 psql 证明）；
#   3. 三格齐就能关；关掉的轮读作「没有开着的一轮」，不是错误；
#   4. ref 在、载荷读不出 ⇒ 退 3（书坏了 ≠ 没有轮）；
#   5. 台账不在 git 仓里 ⇒ 退 3 并点名，**不悄悄改读事件流**（那正是这一族在修的缺陷）。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
round="$root/scripts/hooks/round.py"
todo="$root/scripts/hooks/todo_source.py"
[ -f "$round" ] || { echo "FAIL: round.py 不在：$round" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks"
cp "$round" "$tree/scripts/hooks/round.py"
cp "$todo" "$tree/scripts/hooks/todo_source.py"

git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
# 台账仓的**地址**：载荷里记它，不记这台机器上的检出路径（同一本书各机器路径不同）。
git -C "$tree" remote add origin git@ledger.invalid:owner/book.git
printf '| S1 | 内容 | 未落 | touches: crates/a/src/x.rs\n' >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm base

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 事件流的替身：被调用就留痕。走 git 书时这个文件必须不存在。
cat >"$tmp/tripwire.sh" <<TRIP
#!/bin/sh
: >"$tmp/psql-was-called"
exit 2
TRIP
chmod +x "$tmp/tripwire.sh"
export COGNEVA_TODO_SOURCE="md:$tree/ledger.md"
export COGNEVA_EVENT_PSQL="$tmp/tripwire.sh"

run() { python3 "$tree/scripts/hooks/round.py" "$@" 2>&1; }

# 1. 开一轮：清单写进 ref。
out=$(run open); rc=$?
if [ "$rc" -eq 0 ] && git -C "$tree" rev-parse --verify --quiet refs/cogneva/round >/dev/null \
   && git -C "$tree" cat-file -p refs/cogneva/round | grep -q '"batch": \["S1"\]'; then
    ok "open 把这一批写进了 refs/cogneva/round"
fi

# 1b. 载荷记的是台账仓的**地址**，不是本机的检出路径：换一台机器读同一本载荷，
#     路径一定不同，而地址不变。检出路径写进 ref 等于把一台机器的布局当成这本书的身份。
payload=$(git -C "$tree" cat-file -p refs/cogneva/round)
if printf '%s' "$payload" | grep -q '"ledger_address": "git@ledger.invalid:owner/book.git"' \
   && ! printf '%s' "$payload" | grep -qF "$tree"; then
    ok "载荷带地址、不带本机检出路径"
else
    bad "open 没把清单写进 ref（rc=$rc）：$out"
fi

# 2. status 从 ref 读；事件流一次都没被调用。
out=$(run status); rc=$?
if [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "还剩 1 条" \
   && printf '%s' "$out" | grep -q "书=git refs/cogneva/round" \
   && [ ! -e "$tmp/psql-was-called" ]; then
    ok "status 读 git 书，且没有碰事件流"
else
    bad "status 没走 git 书或碰了事件流（rc=$rc）：$out"
fi

# 3. 三格齐 ⇒ 状态 0 ⇒ 关；关掉的轮读作「没有开着的一轮」（不是错误）。
printf '| S1 | 内容 | 未落 | 代码: a | 上线: b | 验证: c | touches: crates/a/src/x.rs\n' >"$tree/ledger.md"
out=$(run status); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "三格齐 S1"; } \
    && ok "三格齐的一轮：status 退 0" \
    || bad "三格齐时 status 判词不对（rc=$rc）：$out"
out=$(run close); rc=$?
if [ "$rc" -eq 0 ]; then
    out=$(run status); rc=$?
    { [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "没有开着的一轮"; } \
        && ok "关掉之后：读作「没有开着的一轮」" \
        || bad "关掉之后的 status 判词不对（rc=$rc）：$out"
else
    bad "close 没关掉这一轮（rc=$rc）：$out"
fi

# 4. ref 在、载荷不是 JSON ⇒ 退 3。书坏了不是「没有轮」。
sha=$(printf 'not json' | git -C "$tree" hash-object -w --stdin)
git -C "$tree" update-ref refs/cogneva/round "$sha"
out=$(run status); rc=$?
{ [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "不是 JSON"; } \
    && ok "载荷读不出：退 3" \
    || bad "坏载荷的判词不对（rc=$rc）：$out"
git -C "$tree" update-ref -d refs/cogneva/round

# 5. 台账不在 git 仓里：退 3 并点名，不改读事件流。
mkdir -p "$tmp/loose"
printf '| S1 | 内容 | 未落 | touches: crates/a/src/x.rs\n' >"$tmp/loose/ledger.md"
out=$(COGNEVA_TODO_SOURCE="md:$tmp/loose/ledger.md" python3 "$tree/scripts/hooks/round.py" status 2>&1); rc=$?
{ [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "不在一个 git 仓里" \
   && [ ! -e "$tmp/psql-was-called" ]; } \
    && ok "台账不在 git 仓里：退 3，不改读事件流" \
    || bad "无仓台账的判词不对（rc=$rc）：$out"

printf 'round-git-store.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
