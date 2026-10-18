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

# 4. ExecStart 指向的脚本得真的在、真的可执行。
exec_line="$(grep -m1 '^ExecStart=' "${svc}")" || fail "service 没有 ExecStart"
target="${exec_line#ExecStart=}"
[ -x "${target}" ] || fail "ExecStart 指向的脚本不可执行：${target}"

# 5. timer 的拍子与启用面。
grep -q '^OnCalendar=' "${tmr}" || fail "timer 没有 OnCalendar"
grep -q '^WantedBy=timers.target' "${tmr}" || fail "timer 没有 WantedBy=timers.target"

echo "install-cross-layer-reconcile: 5 passed"
