#!/usr/bin/env bash
# 把跨层对账接进这个用户的 systemd 实例，每半小时跑一次。
#
# 为什么是定时而不是某条流程里的一步：这条读数判的是**两层的差**，而起因可能出现在
# 任何一层——有人手工 patch 了配额、节点换了、某个 DaemonSet 在一个节点上没起来。
# 没有任何一条「改了什么之后」的钩子能覆盖这几种。定时跑是唯一不需要事先猜到
# 起因的载体。
#
# 它不写任何东西：判红只进退出码与 journal。权限也只到读——sudo -n 已经配好
# NOPASSWD 的 kubectl，不需要常驻权限，也不需要集群内的 ServiceAccount。
#
# 用法：bash deploy/scripts/install-cross-layer-reconcile.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
reconcile="${repo}/deploy/scripts/check-cross-layer-reconcile.sh"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-cross-layer-reconcile"

die() { printf 'install-cross-layer-reconcile: %s\n' "$*" >&2; exit 1; }

[ -f "${reconcile}" ] || die "对账脚本不在这里：${reconcile}"
[ -x "${reconcile}" ] || die "${reconcile} 不可执行"

mkdir -p "${unit_dir}"

cat >"${unit_dir}/${unit_name}.service" <<EOF
[Unit]
Description=Reconcile the declared, enforced and physical resource layers

[Service]
Type=oneshot
ExecStart=${reconcile}
# 只读，但会去问集群与 Prometheus；让它排在任何构建后面。
Nice=10
# 不能加 NoNewPrivileges=yes：读集群走的是「sudo -n k3s kubectl」，而这个标志让
# sudo 直接拒绝提权——装好之后每一次运行都以退出码 2 结束，看起来像「集群读不到」。
# 这里不给的是常驻特权：脚本本身只读，sudo 仍然只到那一条 NOPASSWD 的 kubectl。
TimeoutStartSec=300
EOF

cat >"${unit_dir}/${unit_name}.timer" <<'EOF'
[Unit]
Description=Reconcile the declared, enforced and physical resource layers

[Timer]
# 错开整点与半点：这台机器上的其他定时任务都落在那些时刻。
OnCalendar=*-*-* *:07,37:00
RandomizedDelaySec=120
Persistent=true

[Install]
WantedBy=timers.target
EOF

if [ "${COGNEVA_INSTALL_RENDER_ONLY:-0}" = 1 ]; then
    # 只写文件，不碰这个用户的 systemd 实例。测试要判「渲染出来的两份文件对不对」，
    # 而它不该顺带在这台机器上装一个定时器。
    printf 'install-cross-layer-reconcile: 只渲染（COGNEVA_INSTALL_RENDER_ONLY=1）：%s\n' "${unit_dir}"
    exit 0
fi

systemctl --user daemon-reload
systemctl --user enable --now "${unit_name}.timer"

state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "timer 是 ${state}，不是 active"
printf 'install-cross-layer-reconcile: %s.timer active, script %s\n' \
    "${unit_name}" "${reconcile}"
systemctl --user list-timers "${unit_name}.timer" --no-legend
