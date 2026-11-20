#!/usr/bin/env bash
# principle: feedback_a_check_nobody_invokes_is_not_a_gate_the_installer_must_own_the_wiring
# 写者租约这一族判两件事：
#   * 租约本身是个 CAS：空闲/过期才拿得到，别人拿不走，只有持有者能续/放
#   * 门在**写入口**生效：租约有效时，没声明身份的会话与别人都被拒，持有者放行；
#     状态读不回来时退出码是 3，不是 0
#
# 全部在临时目录里跑（WRITER_LEASE_FILE），不碰宿主真正的租约状态。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LEASE="$HERE/writer-lease.sh"
# 门在 `scripts/git-hooks/gates/`，租约本体与这份测试在 `scripts/`：三者不同级，所以
# 这里必须点名那条路径——按「同级」找会让整族在任何人提交时都红（must-green），而红的
# 那句话说的是「门不在」，不是「门不灵」。
#
# 2026-11-20 搬到公开仓时，门与租约第一次住在同一棵树里：这条路径跟着搬了一次。
GATE="$HERE/git-hooks/gates/check-writer-lease.sh"

pass=0; fail=0
ok()  { printf 'ok    %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL  %s\n' "$1" >&2; fail=$((fail + 1)); }

[ -x "$LEASE" ] || { bad "writer-lease.sh 不在或不可执行"; exit 1; }
[ -x "$GATE" ]  || { bad "check-writer-lease.sh 不在或不可执行"; exit 1; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export WRITER_LEASE_FILE="$WORK/lease.json"
l() { bash "$LEASE" "$@"; }
g() { bash "$GATE"; }

# 1. 没有租约：门放行，check 报空闲
g >/dev/null 2>&1; [ $? -eq 0 ] && ok "1. 无租约时门放行" || bad "1. 无租约被拒"
l check anyone >/dev/null 2>&1; [ $? -eq 2 ] && ok "1. check 报空闲（2）" || bad "1. check 没报空闲"

# 2. 拿租约：别人拿不走
l claim in-band "pool recovered" --ttl 60 >/dev/null 2>&1 || bad "2. 空闲时拿不到租约"
l claim out-of-band "manual" >/dev/null 2>&1 && bad "2. 租约被别人抢走了" || ok "2. 别人拿不走有效租约"

# 3. 门：持有者放行，别人和没身份的拒绝
COGNEVA_WRITER=in-band g >/dev/null 2>&1
[ $? -eq 0 ] && ok "3. 持有者通过" || bad "3. 持有者被拒"
COGNEVA_WRITER=c2 g >/dev/null 2>&1
[ $? -eq 1 ] && ok "3. 其他写者被拒" || bad "3. 其他写者没被拒"
out="$(env -u COGNEVA_WRITER bash "$GATE" 2>&1)"; rc=$?
[ "$rc" -eq 1 ] && grep -q '没有声明写者身份' < <(printf '%s' "$out")\
  && ok "3. 没声明身份的会话被拒，且拒词说清缺什么" || bad "3. 没声明身份时放行了（rc=$rc）"

# 4. 非持有者不能续/放
l renew c2 >/dev/null 2>&1 && bad "4. 别人续到了租约" || ok "4. 别人续不到"
l release c2 >/dev/null 2>&1 && bad "4. 别人释放了租约" || ok "4. 别人释放不了"

# 5. 过期即放行
l release in-band >/dev/null 2>&1
l claim in-band "short" --ttl 1 >/dev/null 2>&1
sleep 2
g >/dev/null 2>&1
[ $? -eq 0 ] && ok "5. 过期租约不再挡人" || bad "5. 过期租约还在挡人"

# 6. 读不回来是 3，不是 0
printf 'not json' >"$WRITER_LEASE_FILE"
g >/dev/null 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "6. 状态损坏时退出码 3（不冒充放行）" || bad "6. 损坏状态 rc=$rc"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
