#!/usr/bin/env bash
# principle: feedback_a_comment_inside_an_unquoted_heredoc_is_still_executed
# install-container-headroom.sh 的定向测试：判它**写出来的两份单元文件**本身。
#
# systemd 对不认识的行走的是「告警然后忽略」——一份被污染到只剩 ExecStart 还认得的
# service 文件照样装载成功，is-active 照样绿。这里判的正是那种「文件还在、配置已经没了」
# 的形状：每一行都得带 '='，ExecStart 指向的脚本得真的在，序列读写得是同一份。
#
# 跑法：bash deploy/scripts/tests/install-container-headroom.test.sh
# CI 的 deploy/scripts/tests/*.test.sh 通配会带上它；不碰 systemd，不装任何东西。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
installer="${repo}/deploy/scripts/install-container-headroom.sh"
cross_installer="${repo}/deploy/scripts/install-cross-layer-reconcile.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${installer}" ] || fail "安装脚本不存在：${installer}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

XDG_CONFIG_HOME="${work}" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${installer}" >/dev/null
XDG_CONFIG_HOME="${work}/cross" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${cross_installer}" >/dev/null

unit_dir="${work}/systemd/user"
svc="${unit_dir}/cogneva-container-headroom.service"
tmr="${unit_dir}/cogneva-container-headroom.timer"
[ -f "${svc}" ] || fail "没有写出 service：${svc}"
[ -f "${tmr}" ] || fail "没有写出 timer：${tmr}"

# 1. systemd 只认 key=value（段头与注释除外）。
bad="$(grep -nvE '^[[:space:]]*(#|$|\[[^]]+\]$)' "${svc}" | grep -v '=' || true)"
[ -z "${bad}" ] || fail "service 里有没有 '=' 的行（systemd 会告警后忽略）：${bad}"

# 2. 命令替换留下的现场：kubectl 的用法文本被铺进来过。
if grep -q 'Use "kubectl <command> --help"' "${svc}"; then
    fail "service 里混进了 kubectl --help 的输出——heredoc 里的反引号被执行了"
fi

# 3. 「不能加 NoNewPrivileges」那句必须留在产物里：这个读数的集群读取也走 sudo -n。
grep -q 'NoNewPrivileges' "${svc}" || fail "service 丢了 NoNewPrivileges 的说明"

# 4. ExecStart 指向的脚本得真的在、真的可执行，并且带着 --record。
exec_line="$(grep -m1 '^ExecStart=' "${svc}")" || fail "service 没有 ExecStart"
read -r -a exec_words <<<"${exec_line#ExecStart=}"
[ -x "${exec_words[0]}" ] || fail "ExecStart 指向的脚本不可执行：${exec_words[0]}"
printf '%s\n' "${exec_line}" | grep -q -- ' --record' \
    || fail "ExecStart 没有 --record：序列永远是空的，检查器会把「没在记」读成「没在跑」"

# 5. timer 的拍子与启用面。
grep -q '^OnCalendar=' "${tmr}" || fail "timer 没有 OnCalendar"
grep -q '^WantedBy=timers.target' "${tmr}" || fail "timer 没有 WantedBy=timers.target"

# 6. 检查器单元与 timer：脚本可执行、读的是同一个序列、红不被包装成成功。
chk="${unit_dir}/cogneva-container-headroom-series-check.service"
chkt="${unit_dir}/cogneva-container-headroom-series-check.timer"
[ -f "${chk}" ] || fail "没有写出检查器 service：${chk}"
[ -f "${chkt}" ] || fail "没有写出检查器 timer：${chkt}"
chk_line="$(grep -m1 '^ExecStart=' "${chk}")" || fail "检查器 service 没有 ExecStart"
read -r -a chk_words <<<"${chk_line#ExecStart=}"
[ -x "${chk_words[0]}" ] || fail "检查器的脚本不可执行：${chk_words[0]}"
chk_file="$(printf '%s\n' "${chk_words[@]}" | awk 'p { print; exit } $0 == "--file" { p = 1 }')"
env_series="$(grep -m1 '^Environment=COGNEVA_HEADROOM_SERIES=' "${svc}")"
env_series="${env_series#Environment=COGNEVA_HEADROOM_SERIES=}"
[ -n "${chk_file}" ] || fail "检查器没有 --file 参数：${chk_line}"
[ "${chk_file}" = "${env_series}" ] \
    || fail "产出与检查读的不是同一个序列：${env_series} vs ${chk_file}"
grep -q '^SuccessExitStatus=' "${chk}" \
    && fail "检查器把退出码 1 包装成了成功——缺拍就再也不会出现在 systemctl --failed 上"
grep -q '^WantedBy=timers.target' "${chkt}" || fail "检查器 timer 没有 WantedBy=timers.target"

# 7. 产出与检查不能同刻，且不能与跨层对账的拍子撞车：三份读数共用一个 Prometheus，
#    同刻跑会让一次慢查询同时打在三份读数上。
prod_cal="$(grep -m1 '^OnCalendar=' "${tmr}")"
chk_cal="$(grep -m1 '^OnCalendar=' "${chkt}")"
[ "${prod_cal}" != "${chk_cal}" ] || fail "产出与检查同刻：${prod_cal}"
cross_dir="${work}/cross/systemd/user"
for f in cogneva-cross-layer-reconcile.timer cogneva-cross-layer-reconcile-series-check.timer; do
    other="$(grep -m1 '^OnCalendar=' "${cross_dir}/${f}")"
    [ "${prod_cal}" != "${other}" ] || fail "与 ${f} 同刻：${prod_cal}"
done

# 8. 序列检查器要的间隔与产出 timer 的周期一致（1800 秒）。
printf '%s\n' "${chk_line}" | grep -q -- '--interval 1800' \
    || fail "检查器的 --interval 不是 1800：${chk_line}"

echo "install-container-headroom: 8 组用例通过"
