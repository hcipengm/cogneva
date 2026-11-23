#!/usr/bin/env bash
# 收工钩子的那一问：**没有开着的一轮**不是「这一轮还没关」。
#
# 2026-11-23 现场：一轮做完、`round.py close` 成功，收工钩子照样退 2，抬头是
# 「这一轮还没关，别停」，而它下一行印的是「没有开着的一轮」。原因是钩子拿
# `round.py status` 的裸往返码分支，而那边「没有轮」与「开着但有缺口」都退 1，
# 两件事在读者眼里成了一件。放行标记本来是等外部事件用的，被这条误报逼成了
# 每次收工的常规动作——那会让「这一轮没关」这个告警失去它唯一的意思。
#
# 修法不是让钩子去认 `status` 的某一句话，而是向 `round.py` 要它真正要的那一问
# （`stop-gate`），三态各是一个往返码：0 可以停、2 有缺口、其它是「判不了」。
# 这一份判钩子端到端四件事：
#   1. 没开过一轮的树（刚改过文件）：钩子放行；
#   2. 开着的一轮还有缺口：钩子拦住，并把缺口（哪一格缺）打出来；
#   3. 开着的一轮已终态（还没 close）：钩子放行——钩子判的是「能不能停」，不是
#      「关没关」；
#   4. 书读不出来：钩子拦住，判词说的是「读不出」而不是「没关」（读不出来不是
#      「可以停」）。
#
# 跑法：bash deploy/scripts/tests/stop-gate-no-round-is-not-an-unfinished-round.test.sh
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
round="$root/scripts/hooks/round.py"
todo="$root/scripts/hooks/todo_source.py"
hook="$root/scripts/hooks/stop-until-batch-done.sh"
for f in "$round" "$todo" "$hook"; do
    [ -f "$f" ] || { echo "FAIL: 缺 $f" >&2; exit 1; }
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks"
cp "$round" "$todo" "$hook" "$tree/scripts/hooks/"
chmod +x "$tree/scripts/hooks/stop-until-batch-done.sh"

git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
printf '| S1 | 内容 | 未落 | touches: crates/a/src/x.rs\n' >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm base

export COGNEVA_TODO_SOURCE="md:$tree/ledger.md"

pass=0
fail=0
ok() { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

run() { python3 "$tree/scripts/hooks/round.py" "$@" 2>&1; }
run_hook() { (cd "$tree" && bash "$tree/scripts/hooks/stop-until-batch-done.sh" 2>&1); }

# 钩子只在「最近 15 分钟内改过这棵树」时才往下一道判；每问之前先碰一下。
touch "$tree/scripts/hooks/round.py"

# 1. 没开过一轮：放行。修之前这里退 2。
out=$(run_hook); rc=$?
{ [ "$rc" -eq 0 ]; } \
    && ok "没有开着的一轮：钩子放行" \
    || bad "没有轮时钩子仍拦人" "退 0" "退 $rc：$out"

# 2. 开着的一轮还有缺口：拦住，缺口点名。
out=$(run open); rc=$?
[ "$rc" -eq 0 ] || bad "open 没开起一轮" "退 0" "退 $rc：$out"
touch "$tree/scripts/hooks/round.py"
out=$(run_hook); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "这一轮还没关" \
   && printf '%s' "$out" | grep -q "未完成 S1" \
   && printf '%s' "$out" | grep -q "缺 代码"; } \
    && ok "有缺口的一轮：钩子拦住并打出缺哪一格" \
    || bad "有缺口时的判词不对" "退 2 且点名「未完成 S1 缺 代码」" "退 $rc：$out"

# 3. 这一轮已终态、但还没 close：钩子放行——它能停，只是还差一次 close。
printf '| S1 | 内容 | 未落 | 代码: a | 上线: b | 验证: c |\n' >"$tree/ledger.md"
touch "$tree/scripts/hooks/round.py"
out=$(run_hook); rc=$?
{ [ "$rc" -eq 0 ]; } \
    && ok "已终态的一轮：钩子放行" \
    || bad "终态的一轮被拦下" "退 0" "退 $rc：$out"

# 4. 书坏了：拦住，且判词说「读不出」。
out=$(run close); rc=$?
[ "$rc" -eq 0 ] || bad "close 没关掉终态的一轮" "退 0" "退 $rc：$out"
sha=$(printf 'not json' | git -C "$tree" hash-object -w --stdin)
git -C "$tree" update-ref refs/cogneva/round "$sha"
touch "$tree/scripts/hooks/round.py"
out=$(run_hook); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "读不出这一轮" \
   && ! printf '%s' "$out" | grep -q "这一轮还没关"; } \
    && ok "书读不出来：钩子拦住，判词不说「没关」" \
    || bad "书坏了时的判词不对" "退 2 且说「读不出」、不说「还没关」" "退 $rc：$out"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
