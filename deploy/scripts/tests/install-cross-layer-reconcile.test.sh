#!/usr/bin/env bash
# principle: feedback_a_comment_inside_an_unquoted_heredoc_is_still_executed
# install-cross-layer-reconcile.sh 的定向测试：判它**写出来的两份单元文件**本身。
#
# 为什么这两个断言值得单独一条测试：systemd 对不认识的行走的是「告警然后忽略」——
# 一份被污染到只剩 ExecStart 还认得的 service 文件照样装载成功，`is-active` 照样绿。
# 2026-10-18 实测的污染源是 heredoc 里的反引号：注释里写 `sudo -n k3s kubectl`，
# 因为 heredoc 没有引号，那一对反引号被 shell 执行，把 kubectl 的整页用法输出铺进了
# 文件；从那一行起每一行都不带 '='，systemd 逐行告警、逐行丢掉，而 unit 仍是 active。
#
# 跑法：bash deploy/scripts/tests/install-cross-layer-reconcile.test.sh
# CI 的 deploy/scripts/tests/*.test.sh 通配会带上它；不碰 systemd，不装任何东西。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
installer="${repo}/deploy/scripts/install-cross-layer-reconcile.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${installer}" ] || fail "安装脚本不存在：${installer}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

XDG_CONFIG_HOME="${work}" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${installer}" >/dev/null

unit_dir="${work}/systemd/user"
svc="${unit_dir}/cogneva-cross-layer-reconcile.service"
tmr="${unit_dir}/cogneva-cross-layer-reconcile.timer"
[ -f "${svc}" ] || fail "没有写出 service：${svc}"
[ -f "${tmr}" ] || fail "没有写出 timer：${tmr}"

# 1. systemd 只认 key=value。渲染结果里任何一条非注释、非空的行都必须带 '='，
#    否则那一行被静默忽略——文件看着还在，配置已经没了。
# 段头（`[Unit]`／`[Service]`）是唯一另一种合法形状；除此之外每一行都得带 '='。
bad="$(grep -nvE '^[[:space:]]*(#|$|\[[^]]+\]$)' "${svc}" | grep -v '=' || true)"
[ -z "${bad}" ] || fail "service 里有没有 '=' 的行（systemd 会告警后忽略）：${bad}"

# 2. 命令替换留下的现场：kubectl 的用法文本被铺进来过。
if grep -q 'Use "kubectl <command> --help"' "${svc}"; then
    fail "service 里混进了 kubectl --help 的输出——heredoc 里的反引号被执行了"
fi

# 3. 「不能加 NoNewPrivileges」那句必须留在产物里：它是同一天踩过的另一个坑的现场，
#    删掉它，下一个人会把标志加回去，然后得到「每次退出码 2、is-active 仍绿」。
grep -q 'NoNewPrivileges' "${svc}" || fail "service 丢了 NoNewPrivileges 的说明"

# 4. ExecStart 指向的脚本得真的在、真的可执行（`--record` 是参数，不是路径的一部分）。
exec_line="$(grep -m1 '^ExecStart=' "${svc}")" || fail "service 没有 ExecStart"
read -r -a exec_words <<<"${exec_line#ExecStart=}"
target="${exec_words[0]}"
[ -x "${target}" ] || fail "ExecStart 指向的脚本不可执行：${target}"

# 5. timer 的拍子与启用面。
grep -q '^OnCalendar=' "${tmr}" || fail "timer 没有 OnCalendar"
grep -q '^WantedBy=timers.target' "${tmr}" || fail "timer 没有 WantedBy=timers.target"

# 6. 产出单元必须带 --record，否则序列永远是空的，而检查器会把「没在记」读成「没在跑」。
printf '%s\n' "${exec_line}" | grep -q -- ' --record' \
    || fail "service 的 ExecStart 没有 --record：${exec_line}"

# 6b. 还必须带 --converge：判据 4 的差只有这条拍子能填。少了它，live 与树内声明
#     不一致就只剩一条红，而红没有执行者——那正是这条读数被加进来时要消灭的形状。
printf '%s\n' "${exec_line}" | grep -q -- ' --converge' \
    || fail "service 的 ExecStart 没有 --converge：${exec_line}"

# 7. 检查器单元与 timer：脚本可执行、读的是同一个序列、红不被包装成成功。
chk="${unit_dir}/cogneva-cross-layer-reconcile-series-check.service"
chkt="${unit_dir}/cogneva-cross-layer-reconcile-series-check.timer"
[ -f "${chk}" ] || fail "没有写出检查器 service：${chk}"
[ -f "${chkt}" ] || fail "没有写出检查器 timer：${chkt}"
chk_line="$(grep -m1 '^ExecStart=' "${chk}")" || fail "检查器 service 没有 ExecStart"
read -r -a chk_words <<<"${chk_line#ExecStart=}"
[ -x "${chk_words[0]}" ] || fail "检查器的脚本不可执行：${chk_words[0]}"
chk_file="$(printf '%s\n' "${chk_words[@]}" | awk 'p { print; exit } $0 == "--file" { p = 1 }')"
env_series="$(grep -m1 '^Environment=COGNEVA_RECONCILE_SERIES=' "${svc}")"
env_series="${env_series#Environment=COGNEVA_RECONCILE_SERIES=}"
[ -n "${chk_file}" ] || fail "检查器没有 --file 参数：${chk_line}"
[ "${chk_file}" = "${env_series}" ] \
    || fail "产出与检查读的不是同一个序列：${env_series} vs ${chk_file}"
grep -q '^SuccessExitStatus=' "${chk}" \
    && fail "检查器把退出码 1 包装成了成功——缺拍就再也不会出现在 systemctl --failed 上"
grep -q '^WantedBy=timers.target' "${chkt}" || fail "检查器 timer 没有 WantedBy=timers.target"

# 8. 两个 timer 不能同刻：同刻跑的话，一次迟到会同时被算成「缺拍」与「正常」。
prod_cal="$(grep -m1 '^OnCalendar=' "${tmr}")"
chk_cal="$(grep -m1 '^OnCalendar=' "${chkt}")"
[ "${prod_cal}" != "${chk_cal}" ] || fail "两个 timer 的拍子相同：${prod_cal}"

# 9. 检查器的 --interval 必须等于产出 timer 的周期。两个数分开写时，把周期从 30 分钟
#    改到 10 分钟而漏改 interval，缺拍判据就静默地迟钝三倍：序列照常落行、检查照常
#    通过，只有真停了的那天才会发现它一直没在报。
cadence="$(awk -v value="${prod_cal#OnCalendar=}" 'BEGIN {
    sub(/^[^:]*\*:/, "", value)
    sub(/:.*$/, "", value)
    n = split(value, m, ",")
    if (n < 1) exit 1
    step = 60 - m[n] + m[1]
    for (i = 1; i < n; i++) if (m[i+1] - m[i] < step) step = m[i+1] - m[i]
    print step * 60
}')"
interval_arg="$(printf '%s\n' "${chk_line}" | sed -n 's/.*--interval \([0-9][0-9]*\).*/\1/p')"
[ -n "${interval_arg}" ] || fail "检查器没有 --interval：${chk_line}"
[ "${interval_arg}" = "${cadence}" ] \
    || fail "检查器的 --interval ${interval_arg} 与产出的周期 ${cadence} 秒不一致：换周期漏了换判据"

echo "install-cross-layer-reconcile: 10 passed"
