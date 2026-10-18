#!/usr/bin/env bash
# 把跨层对账接进这个用户的 systemd 实例，每半小时跑一次。
#
# 为什么是定时而不是某条流程里的一步：这条读数判的是**两层的差**，而起因可能出现在
# 任何一层——有人手工 patch 了配额、节点换了、某个 DaemonSet 在一个节点上没起来。
# 没有任何一条「改了什么之后」的钩子能覆盖这几种。定时跑是唯一不需要事先猜到
# 起因的载体。
#
# 它只写本机状态目录里的一行流水（序列），判红仍只进退出码与 journal。权限也只到读——
# sudo -n 已经配好 NOPASSWD 的 kubectl，不需要常驻权限，也不需要集群内的 ServiceAccount。
#
# 第二个单元是序列检查器：产出停了、或者连着两拍没到，`systemctl --failed` 上要看得见。
# 产出单元自己的退出码回答不了「我在不在跑」——它不在的时候，没有退出码可以红。
#
# 用法：bash deploy/scripts/install-cross-layer-reconcile.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
reconcile="${repo}/deploy/scripts/check-cross-layer-reconcile.sh"
checker="${repo}/deploy/scripts/check-reconcile-series.sh"
series="${XDG_STATE_HOME:-${HOME:?}/.local/state}/cogneva/cross-layer-reconcile.tsv"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-cross-layer-reconcile"
check_name="cogneva-cross-layer-reconcile-series-check"
interval=1800

die() { printf 'install-cross-layer-reconcile: %s\n' "$*" >&2; exit 1; }

[ -f "${reconcile}" ] || die "对账脚本不在这里：${reconcile}"
[ -x "${reconcile}" ] || die "${reconcile} 不可执行"
[ -x "${checker}" ] || die "${checker} 不存在或不可执行"

mkdir -p "${unit_dir}"

cat >"${unit_dir}/${unit_name}.service" <<EOF
[Unit]
Description=Reconcile the declared, enforced and physical resource layers

[Service]
Type=oneshot
# --record：每一拍（绿的、红的、读不到的）都往序列里落一行。判红的那一拍也是拍子，
# 不落的话序列上的洞会同时代表「没跑」与「跑了但红了」。
ExecStart=${reconcile} --record
Environment=COGNEVA_RECONCILE_SERIES=${series}
# 只读，但会去问集群与 Prometheus；让它排在任何构建后面。
Nice=10
# 不能加 NoNewPrivileges=yes：读集群走的是「sudo -n k3s kubectl」，而这个标志让
# sudo 直接拒绝提权——装好之后每一次运行都以退出码 2 结束，看起来像「集群读不到」。
# 这里不给的是常驻特权：脚本本身只读，sudo 仍然只到那一条 NOPASSWD 的 kubectl。
TimeoutStartSec=300
EOF

cat >"${unit_dir}/${check_name}.service" <<EOF
[Unit]
Description=Check that the cross-layer reconcile series still has its beats

[Service]
Type=oneshot
ExecStart=${checker} --file ${series} --interval ${interval}
# 不写 SuccessExitStatus：退出码 1（缺拍）就是这个单元**失败**。把红包装成成功，
# 「序列停了」就只剩 journal 里的一行，而 systemctl --failed 什么也不说。
Nice=10
TimeoutStartSec=60
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

cat >"${unit_dir}/${check_name}.timer" <<'EOF'
[Unit]
Description=Clock for the cross-layer reconcile series check

[Timer]
# 与产出的 :07/:37 错开 10 分钟：检查要看的是「上一拍到了没有」，和产出同刻跑
# 会让一次迟到同时被算成缺拍与正常。
OnCalendar=*-*-* *:17,47:00
RandomizedDelaySec=60
Persistent=true

[Install]
WantedBy=timers.target
EOF

if [ "${COGNEVA_INSTALL_RENDER_ONLY:-0}" = 1 ]; then
    # 只写文件，不碰这个用户的 systemd 实例。测试要判「渲染出来的文件对不对」，
    # 而它不该顺带在这台机器上装一个定时器。
    printf 'install-cross-layer-reconcile: 只渲染（COGNEVA_INSTALL_RENDER_ONLY=1）：%s\n' "${unit_dir}"
    exit 0
fi

systemctl --user daemon-reload
systemctl --user enable --now "${unit_name}.timer"
systemctl --user enable --now "${check_name}.timer"

# 立刻跑一拍，把序列文件种出来。不种的话，检查器在第一次产出之前（最多 30 分钟）
# 读到的是「文件不存在」——那是红，但红的原因是我们刚装好，不是拍子停了。
# 这一拍失败（集群读不到、Prometheus 没起）不影响安装：它照样落一行 `unreadable`，
# 那正是「装好了但读不到」该有的读数，交给检查器与 journal 去说。
systemctl --user start "${unit_name}.service" || \
    printf 'install-cross-layer-reconcile: 首拍没有成功（它已落一行 unreadable），见 journalctl --user -u %s\n' \
        "${unit_name}.service" >&2

state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "timer 是 ${state}，不是 active"
state="$(systemctl --user is-active "${check_name}.timer")"
[ "${state}" = active ] || die "检查器 timer 是 ${state}，不是 active"
printf 'install-cross-layer-reconcile: %s.timer active, script %s\n' \
    "${unit_name}" "${reconcile}"
printf 'install-cross-layer-reconcile: %s.timer active, series %s\n' \
    "${check_name}" "${series}"
systemctl --user list-timers "${unit_name}.timer" "${check_name}.timer" --no-legend
