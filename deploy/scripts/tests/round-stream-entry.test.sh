#!/usr/bin/env bash
# `scripts/hooks/round.py` 的事件流入口：它从哪儿拿到那条命令，拿不到时说不说得清。
#
# 判三件事：
#   1. 没有 `COGNEVA_EVENT_PSQL` 时，用仓库自带的 `scripts/host/event-psql.sh`。
#      没有这一档，每个调用方都要自己把环境变量拼对；拼错与「流里没有开着的一轮」
#      在读数上是同一句话，而这两种情况的处置相反。
#   2. 两档都没有 ⇒ 退 3（读不到），不是「没有开着的一轮」。
#   3. 命令在、但它失败了 ⇒ 也退 3：命令失败不是「流里没有那一行」。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
round="$root/scripts/hooks/round.py"
todo="$root/scripts/hooks/todo_source.py"
[ -f "$round" ] || { echo "FAIL: round.py 不在：$round" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks" "$tree/scripts/host"
cp "$round" "$tree/scripts/hooks/round.py"
cp "$todo" "$tree/scripts/hooks/todo_source.py"

git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
printf '| S1 | 内容 | 未落 | 判据 | touches: crates/a/src/x.rs\n' >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm base
REV="$(git -C "$tree" rev-parse HEAD)"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 事件流的替身：round 住在一行 `round_opened` 里，载荷是台账 rev。
#
# 第四列是**这一轮的 id 清单**——批次的正本在事件里，不在某个 rev 的台账上。
# 少了它，「还剩几条」永远是 0，而 0 条正是「这一轮做完了」的读数。
cat >"$tree/scripts/host/event-psql.sh" <<STUB
#!/bin/sh
case "\$*" in
  *round_opened*) printf '%s\t%s\t%s\t%s\n' "round-test" "$REV" "" '["S1"]' ;;
  *) : ;;
esac
STUB
chmod +x "$tree/scripts/host/event-psql.sh"

# 1. 没设环境变量，但仓库自带入口：读得到那一轮。
out=$(COGNEVA_EVENT_PSQL= COGNEVA_TODO_LEDGER="$tree/ledger.md" \
      python3 "$tree/scripts/hooks/round.py" status 2>&1); rc=$?
if [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "还剩 1 条"; then
    ok "没设变量也能读：用仓库自带的事件流入口"
else
    bad "没设变量时读不到那一轮（rc=$rc）：$out"
fi

# 2. 两档都没有：退 3，并点名缺的是什么。
mv "$tree/scripts/host/event-psql.sh" "$tree/scripts/host/event-psql.sh.off"
out=$(COGNEVA_EVENT_PSQL= COGNEVA_TODO_LEDGER="$tree/ledger.md" \
      python3 "$tree/scripts/hooks/round.py" status 2>&1); rc=$?
{ [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "COGNEVA_EVENT_PSQL"; } \
    && ok "两档都没有：退 3，并点名环境变量" \
    || bad "两档都没有时的判词不对（rc=$rc）：$out"

# 3. 命令在、但它失败：退 3，不是「没有开着的一轮」。
cat >"$tmp/broken.sh" <<'BROKEN'
#!/bin/sh
echo "connection refused" >&2
exit 2
BROKEN
chmod +x "$tmp/broken.sh"
out=$(COGNEVA_EVENT_PSQL="$tmp/broken.sh" COGNEVA_TODO_LEDGER="$tree/ledger.md" \
      python3 "$tree/scripts/hooks/round.py" status 2>&1); rc=$?
{ [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "connection refused"; } \
    && ok "命令失败：退 3，并把 stderr 原样带出来" \
    || bad "命令失败时的判词不对（rc=$rc）：$out"

printf 'round-stream-entry.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
