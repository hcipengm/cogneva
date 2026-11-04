#!/usr/bin/env bash
# principle: feedback_a_store_guard_that_snapshots_is_not_a_lock
# principle: feedback_running_the_eval_arm_turns_the_commit_gate_red
#
# `eval-host-arm.sh` 的互斥测试：不起真评测、不连集群（kubectl 用桩，二进制用桩），
# 只判「第二条臂起不来」与「第一条臂退出后第三条能起来」。
#
# 为什么需要这一格：原来的守卫是 `pgrep` 的一瞬快照。两条臂同时起跑时，那一刻
# store 都是空的，两边都放行，跑到一半对撞，其中一条静默停了 27 分钟——CPU 只用
# 25 秒、checkpoint 一字节没动、进程不报错不超时。快照对「还没开始的写者」没有
# 约束力，只有持锁到退出才有。这一格钉的就是「快照换成锁」。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-host-arm.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# 桩 kubectl：只回答 ClusterIP，值从文件读，好让同一个测试里换 IP。
cat >"$TMP/kubectl" <<'SH'
#!/bin/bash
case "$*" in
  *"get svc"*) cat "$IP_FILE"; exit 0 ;;
esac
exit 0
SH
chmod +x "$TMP/kubectl"
printf '10.43.0.9' >"$TMP/ip"
export IP_FILE="$TMP/ip"

# 桩二进制：记下自己起来过，然后睡住，好让第二条臂撞上锁。
cat >"$TMP/bin" <<'SH'
#!/bin/bash
printf '%s\n' "$*" >>"$STUB_LOG"
sleep "${STUB_SLEEP:-3}"
SH
chmod +x "$TMP/bin"

mkdir -p "$TMP/runs" "$TMP/data"
printf 'hle\ttask-a\n' >"$TMP/runs/pins.tsv"
# 预先给一份**原样导出**的配置：base_url 里是服务名，每次由脚本改写成当前 ClusterIP。
mkdir -p "$TMP/runs/.host-arm"
printf '{"llm_routing":{"backends":[{"provider":"gateway","model":"via","base_url":"http://cogneva-security-gateway:8081/v1"}]}}\n' \
  >"$TMP/runs/.host-arm/cogneva.json.src.json"

run_arm() { # run_arm <日志文件> ；其余靠导出到环境的桩
  # `SKIP_BUILDAH_GUARD=1` 跳掉的是 pgrep 快照，锁不在它管辖范围（脚本里锁已挪到
  # `fi` 之外）。留在这里的理由是最初的：这台机器与别的会话共用，门判这一笔时宿主上
  # 可能正跑着别人的 buildah——快照会先拦下，第二条臂就以 2 退出、根本没撞锁，
  # 「以 2 退出」于是绿得毫无意义（2026-10-19 14:05 实测，同一次门里两条断言因此红）。
  BIN="$TMP/bin" DATA_ROOT="$TMP/data" RUNS="$TMP/runs" \
  CONFIG="$TMP/runs/.host-arm/cogneva.json" LOCK="$TMP/arm.lock" \
  KUBECTL_CMD="$TMP/kubectl" STUB_LOG="$1" STUB_SLEEP="${2:-3}" \
  SKIP_BUILDAH_GUARD=1 \
    bash "$SCRIPT" --benchmarks swe-bench-pro --out "$TMP/out.md"
}

# 1. 第一条臂：应当起来并跑完。
run_arm "$TMP/log1" 10 & first=$!
# How long the arm takes to reach its flock is a property of the host, not of
# the lock: standalone this loop closes in well under a second, but the commit
# gate runs two suites at once under load 20+, and a 5-second bound made this
# case red for the wrong reason (2026-10-19 12:58, standalone green).
#
# A wall-clock bound is the wrong shape for that, however generous: on
# 2026-10-30 the gate ran this suite at load 40 and the arm had not reached its
# flock after 120 seconds. Giving up there took out five assertions at once --
# the wait went red, the second arm then won the lock and ran the benchmark
# nobody told it to run, and the first arm arrived to find the lock taken and
# exited 2. The arm's own life is the bound instead: while that process is
# alive it is still on its way, and the case is only red if the arm ends
# without the lock ever having been seen held -- which is exactly the failure
# (a lock taken too late, or not at all) this suite exists to catch. The
# remaining cap is a safety net against a hung arm, not a budget.
#
# 探针不许再去拿那把锁。它原来是 `flock -n "$TMP/arm.lock" true`——**探针自己就是竞争者**：
# 它拿住锁的那几毫秒里，第一条臂唯一一次 `flock -n 9` 必然失手，臂以 2 退出，而这一格把
# 自己造成的红读成「锁没生效」。2026-11-01 17:16 那次真红就是这么一串：`waited=0`
#（臂早被挤死并被回收）、臂退出码 2、第二条臂 rc 0 并真把评测跑起来了。空转时探针每 100ms
# 只占几毫秒，撞上的概率小；load 40 时占住的窗口被 fork/exec 拉长，同一份字节就红。
# 换成单向信号：臂只有拿到锁才会去跑桩二进制，**桩日志出现 = 它已经持有锁并且在跑**。
# 这个信号只由被测脚本写，探针一个字节都不碰那把锁。
held=0; waited=0
while [ "$waited" -lt 18000 ]; do
  if [ -f "$TMP/log1" ]; then held=1; break; fi
  kill -0 "$first" 2>/dev/null || break
  sleep 0.1; waited=$((waited + 1))
done
[ "$held" = 1 ] && ok "第一条臂起来后锁被持有（它已跑到桩二进制那一步）" || \
  bad "第一条臂退出前没跑到桩二进制那一步（等了 $((waited / 10)) 秒）：锁没生效，或者臂在更早的地方就退出了"

# 2. 第二条臂：撞锁必须立刻红着退出，且不许真的把评测跑起来。
out2=$(run_arm "$TMP/log2" 0 2>&1); rc2=$?
[ "$rc2" = 2 ] && ok "第二条臂以 2 退出" || bad "第二条臂退出码是 $rc2，不是 2"
case "$out2" in
  *"另一条 eval-host-arm 正持有"*) ok "第二条臂说清了是谁占着" ;;
  *) bad "第二条臂的输出没有点名锁：$out2" ;;
esac
[ -f "$TMP/log2" ] && bad "第二条臂仍然把评测跑起来了（桩日志被写）" || ok "第二条臂没有把评测跑起来"

# 3. 第一条臂跑完，锁释放；第三条臂应当能起来。
wait "$first"; rc1=$?
[ "$rc1" = 0 ] && ok "第一条臂正常退出" || bad "第一条臂退出码 $rc1"
grep -q 'eval-table' "$TMP/log1" 2>/dev/null && ok "第一条臂确实调了 eval-table" || bad "第一条臂没有调 eval-table"
out3=$(run_arm "$TMP/log3" 0 2>&1); rc3=$?
[ "$rc3" = 0 ] && ok "第一条臂退出后第三条臂能起来" || bad "锁没有释放：第三条臂退出码 $rc3（$out3）"

# 4. 换 ClusterIP 之后再跑：配置文件必须跟着新 IP，不许留着上一次写进去的那个。
#    就地改写的老做法在这里会红：地基里只剩旧 IP，没有服务名可换，改写是空操作。
printf '10.43.0.7' >"$TMP/ip"
out4=$(run_arm "$TMP/log4" 0 2>&1); rc4=$?
# 先证这一跑真的走到了改写那一步：否则「配置没变」既可能是改写坏了，也可能是这一跑
# 在更早的地方就退出了（2026-10-19 14:05 就是这么红的，报的是配置旧 IP，真因是别处退的）。
[ "$rc4" = 0 ] && ok "换 ClusterIP 后的那一跑正常退出" || \
  bad "换 ClusterIP 后的那一跑以 $rc4 退出：$out4"
cfg="$TMP/runs/.host-arm/cogneva.json"
if grep -q '10.43.0.7' "$cfg" 2>/dev/null && ! grep -q '10.43.0.9' "$cfg" 2>/dev/null; then
  ok "ClusterIP 变了之后配置跟着改写（没有留旧 IP）"
else
  bad "ClusterIP 变成 10.43.0.7 后配置仍是：$(cat "$cfg" 2>/dev/null)"
fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
