#!/usr/bin/env bash
# principle: feedback_a_guard_that_hands_its_lock_to_the_child_it_starts_excludes_itself
# principle: feedback_a_locks_body_is_its_last_holder_not_its_current_one
# principle: feedback_a_sample_i_write_myself_only_tests_the_gate_against_itself
#
# `eval-supervisor-ensure.sh` 的夹具：只判三件在盘上就能判的事——「已在跑」的模式认不认
# 真正的值守、会不会把一笔排队的提交当成值守、以及到目标之后还会不会每 10 分钟起一条。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-supervisor-ensure.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

# 模式从脚本里原样取：两处各写一份，改了一处另一处会静默失配。
pattern=$(sed -n "s/^pattern='\(.*\)'$/\1/p" "$SCRIPT")
[ -n "$pattern" ] || { echo "FAIL: 脚本里读不到 pattern=" >&2; exit 1; }

# 只看造出来的那条命令行，不看全局进程表：宿主上真有一条值守在跑时 `pgrep -f` 总会命中，
# 负方向就永远判不出来。
cmdline_of() {
  printf '%s' "$1" >"$TMP/argv"
  bash -c 'exec -a "$(cat "$1")" sleep 3' _ "$TMP/argv" &
  local pid=$!
  sleep 0.3
  tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | sed 's/ $//'
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
}
match() { local l=$1; [[ $l =~ $pattern ]]; }

line=$(cmdline_of 'bash scripts/eval/supervise-eval-loop.sh')
if match "$line"; then ok "模式匹配真正的值守（$line）"
else bad "模式匹配不到值守：$line"; fi

line=$(cmdline_of 'bash scripts/principles/principle-commit.sh /tmp/msg.txt scripts/eval/supervise-eval-loop.sh')
if match "$line"; then bad "模式把排队的提交当成值守在跑（$line）"
else ok "模式不匹配只把路径写在参数里的命令行"; fi

# 起不起由 pgrep 桩控制：PATH 里塞一个恒 0 / 恒 1 的 pgrep，不去动真进程表。
mkdir -p "$TMP/run" "$TMP/idle"
printf '#!/bin/sh\nexit 0\n' >"$TMP/run/pgrep"
printf '#!/bin/sh\nexit 1\n' >"$TMP/idle/pgrep"
chmod +x "$TMP/run/pgrep" "$TMP/idle/pgrep"

# 假值守：被 source 时提供 lines/reached，被执行时留一行标记（真跑起来才写）。
# 标记只在**执行**时写：被 source 时写就分不出「起起来了」与「只是读了一下判据」。
cat >"$TMP/fake-sup.sh" <<'EOF'
lines() { [ -r "$CKPT" ] && wc -l <"$CKPT" || echo 0; }
reached() { [ "$1" -ge 2 ]; }
if [ "${BASH_SOURCE[0]}" = "$0" ]; then echo ran >>"$MARK"; fi
EOF
printf '{"kind":"header"}\n' >"$TMP/short.jsonl"
printf '{"kind":"header"}\n{"case":"a"}\n' >"$TMP/done.jsonl"

run_ensure() { # $1 = pgrep 桩目录，$2 = CKPT；其余 = 环境
  local pdir=$1 ck=$2; shift 2
  env PATH="$pdir:$PATH" SUP="$TMP/fake-sup.sh" OUT="$TMP/sup.out" CKPT="$ck" MARK="$TMP/ran" \
      LOG="$TMP/ensure.log" "$@" bash "$SCRIPT" | sed -n 's/^.* ensure: //p' 
}

v=$(run_ensure "$TMP/run" "$TMP/short.jsonl")
[ "$v" = "verdict=running" ] && ok "已有一条在跑 ⇒ running，不再起第二条" || bad "已在跑：期望 verdict=running，实得 $v"

rm -f "$TMP/ran"
v=$(run_ensure "$TMP/idle" "$TMP/short.jsonl")
case "$v" in
  "verdict=started pid="*)
    # 等标记，不等一个固定时长：被拉起的那个进程什么时候被调度由一个忙着的宿主决定，
    # 0.3 秒在 load 30+ 时会读成空，然后它 0.1 秒后写出标记，把下一格（「到目标不许起」）
    # 也一起判红——一次竞态罚两格。
    for _ in $(seq 1 100); do [ -s "$TMP/ran" ] && break; sleep 0.1; done
    [ -s "$TMP/ran" ] && ok "没在跑且未到目标 ⇒ started，且真把值守起起来了" \
      || bad "started 但值守没跑起来（等了 10 秒标记文件仍为空）" ;;
  *) bad "未到目标：期望 verdict=started，实得 $v" ;;
esac

# 被拉起的值守**不能继承** ensure 的锁：flock 锁的是打开文件描述，子进程连描述一起继承，
# 于是活得越久的值守握得越久，后面的 ensure 全变成 `locked`——恰好在值守死了、最需要拉起的
# 时候把自己锁在门外。这里用一个「起来之后活一会儿」的假值守验：拉起来之后锁必须立刻是空的。
cat >"$TMP/slow-sup.sh" <<'EOF'
lines() { echo 0; }
reached() { return 1; }
if [ "${BASH_SOURCE[0]}" = "$0" ]; then sleep 8; fi
EOF
rm -f "$TMP/ran"
env PATH="$TMP/idle:$PATH" SUP="$TMP/slow-sup.sh" OUT="$TMP/sup.out" CKPT="$TMP/short.jsonl" \
    MARK="$TMP/ran" LOG="$TMP/ensure.log" bash "$SCRIPT" >/dev/null 2>&1
sleep 0.5
if flock -n "$TMP/ensure.log.lock" -c true 2>/dev/null; then
  ok "拉起的值守没继承锁（否则之后每一条 ensure 都会判 locked）"
else
  bad "拉起的值守继承了 ensure 的锁：后续 ensure 会一直 locked，值守死了也拉不起来"
fi
pkill -f "$TMP/slow-sup[.]sh" 2>/dev/null

v=$(run_ensure "$TMP/idle" "$TMP/done.jsonl")
sleep 0.3
if [ "$v" = "verdict=target-reached" ] && [ ! -s "$TMP/ran" ]; then
  ok "已到目标 ⇒ target-reached，不再起（否则一天多 144 行）"
else
  bad "到目标：期望 verdict=target-reached 且不起，实得 $v"
fi

# 两条 ensure 撞上时只有一条能起：另一条拿不到锁，判词 `locked`，不起第二条值守。
# `locked` 还要带持有者：同一个词盖着「另一条在跑」与「我上次拉起的值守还握着锁」两种
# 相反处境，没有 pid 就只能靠 `fuser` 手工查（2026-10-20 就是这么查的）。这里锁由夹具
# 自己持有，所以持有者里必须出现夹具的 pid。
exec 9>>"$TMP/ensure.log.lock"; flock -n 9
v=$(run_ensure "$TMP/idle" "$TMP/short.jsonl")
hold_v=$v

# 但持有者读不到时，判词不许写成 `none`。`fuser` 缺失（rc=127）、出错（这里桩成 rc=2）
# 与「确实没人持有」（rc=1、无输出）在 stdout 上完全同形，只有退出码分得开。把读不到写成
# none，等于在唯一要回答的那一行上撒谎：查的人照着 none 收工，遗留的持有者继续挡着。
mkdir -p "$TMP/fuser-err" "$TMP/fuser-none"
printf '#!/bin/sh\nexit 2\n' >"$TMP/fuser-err/fuser"
printf '#!/bin/sh\nexit 1\n' >"$TMP/fuser-none/fuser"
chmod +x "$TMP/fuser-err/fuser" "$TMP/fuser-none/fuser"
v=$(run_ensure "$TMP/fuser-err" "$TMP/short.jsonl")
[ "$v" = "verdict=locked holders=unreadable(rc=2)" ] \
  && ok "fuser 出错 ⇒ 判词写 unreadable(rc=2)，不写 none" \
  || bad "持有者读不到：期望 holders=unreadable(rc=2)，实得 $v"
v=$(run_ensure "$TMP/fuser-none" "$TMP/short.jsonl")
[ "$v" = "verdict=locked holders=none" ] \
  && ok "fuser 说没人持有（rc=1）⇒ 才写 none" \
  || bad "确实无人持有：期望 holders=none，实得 $v"
exec 9>&-
v=$hold_v
case "$v" in
  "verdict=locked holders="*)
    h=${v#verdict=locked holders=}
    case " $h " in
      *" $$ "*) ok "另一条 ensure 持有锁 ⇒ locked 且点名持有者，不起第二条" ;;
      *) bad "locked 的判词没点到夹具这个持有者（$$）：实得 $v" ;;
    esac ;;
  *) bad "锁冲突：期望 verdict=locked holders=…，实得 $v" ;;
esac

# 单元声明（`Type=oneshot` 与 `KillMode=process`）不在这里判：那份模板属于装它的那一侧，
# 与本脚本不同仓；在这里按路径去找它，等于让公开仓写一条指向私有仓的路径。

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
