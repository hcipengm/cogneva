#!/usr/bin/env bash
# 把容器内存余量读数接进这个用户的 systemd 实例，每半小时跑一次。
#
# 为什么是定时而不是「部署之后跑一次」：越界的原因有两种，都发生在部署之后——
# 数据在长（redis 的 RDB 从 400MB 涨到 485MB），或者有人手工 patch 过上限。
# 部署时的那一拍只能证明「当时装得下」。定时跑是唯一不需要事先猜到起因的载体。
#
# 它只写本机状态目录里的序列，判红只进退出码与 journal。权限也只到读：sudo -n 的
# kubectl 只用来发现 Prometheus 的 Service。
#
# 第二个单元是序列检查器（与跨层对账共用同一份脚本）：产出停了、或者连着两拍没到，
# 在 systemctl --failed 上要看得见。产出单元自己的退出码回答不了「我在不在跑」。
#
# 用法：bash deploy/scripts/install-container-headroom.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
gate="${repo}/deploy/scripts/check-container-headroom.sh"
checker="${repo}/deploy/scripts/check-reconcile-series.sh"
series="${XDG_STATE_HOME:-${HOME:?}/.local/state}/cogneva/container-headroom.tsv"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-container-headroom"
check_name="cogneva-container-headroom-series-check"
interval=1800

die() { printf 'install-container-headroom: %s\n' "$*" >&2; exit 1; }

[ -f "${gate}" ] || die "读数脚本不在这里：${gate}"
[ -x "${gate}" ] || die "${gate} 不可执行"
[ -x "${checker}" ] || die "${checker} 不存在或不可执行"

mkdir -p "${unit_dir}"

cat >"${unit_dir}/${unit_name}.service" <<EOF
[Unit]
Description=Judge container memory headroom against declared limits

[Service]
Type=oneshot
# --record：每一拍（绿的、红的、读不到的）都往序列里落一行。判红的那一拍也是拍子，
# 不落的话序列上的洞会同时代表「没跑」与「跑了但红了」。
ExecStart=${gate} --record
Environment=COGNEVA_HEADROOM_SERIES=${series}
# 只读，但会去问集群与 Prometheus；让它排在任何构建后面。
Nice=10
# 不能加 NoNewPrivileges=yes：读集群走的是「sudo -n k3s kubectl」，而这个标志让
# sudo 直接拒绝提权——装好之后每一次运行都以退出码 2 结束，看起来像「集群读不到」。
TimeoutStartSec=300
EOF

cat >"${unit_dir}/${check_name}.service" <<EOF
[Unit]
Description=Check that the container headroom series still has its beats

[Service]
Type=oneshot
ExecStart=${checker} --file ${series} --interval ${interval}
# 不写 SuccessExitStatus：退出码 1（缺拍）就是这个单元失败。把红包装成成功，
# 「序列停了」就只剩 journal 里的一行，而 systemctl --failed 什么也不说。
Nice=10
TimeoutStartSec=60
EOF

cat >"${unit_dir}/${unit_name}.timer" <<'EOF'
[Unit]
Description=Measure container memory headroom every half hour

[Timer]
# 与跨层对账的 :07/:37 错开：两者都要问 Prometheus，同刻跑会让一次查询超时
# 同时打在两份读数上。
OnCalendar=*-*-* *:12,42:00
RandomizedDelaySec=120
Persistent=true

[Install]
WantedBy=timers.target
EOF

cat >"${unit_dir}/${check_name}.timer" <<'EOF'
[Unit]
Description=Clock for the container headroom series check

[Timer]
# 与产出的 :12/:42 错开 10 分钟，理由同跨层对账：和产出同刻跑会让一次迟到
# 同时被算成缺拍与正常。
OnCalendar=*-*-* *:22,52:00
RandomizedDelaySec=60
Persistent=true

[Install]
WantedBy=timers.target
EOF

if [ "${COGNEVA_INSTALL_RENDER_ONLY:-0}" = 1 ]; then
    # 只写文件，不碰这个用户的 systemd 实例。测试要判「渲染出来的文件对不对」，
    # 而它不该顺带在这台机器上装一个定时器。
    printf 'install-container-headroom: 只渲染（COGNEVA_INSTALL_RENDER_ONLY=1）：%s\n' "${unit_dir}"
    exit 0
fi

systemctl --user daemon-reload
systemctl --user enable --now "${unit_name}.timer"
systemctl --user enable --now "${check_name}.timer"

# 立刻跑一拍，把序列文件种出来。不种的话，检查器在第一次产出之前（最多 30 分钟）
# 读到的是「文件不存在」——那是红，但红的原因是我们刚装好，不是拍子停了。
systemctl --user start "${unit_name}.service" || \
    printf 'install-container-headroom: 首拍没有成功（它已落一行 unreadable），见 journalctl --user -u %s\n' \
        "${unit_name}.service" >&2

state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "timer 是 ${state}，不是 active"
state="$(systemctl --user is-active "${check_name}.timer")"
[ "${state}" = active ] || die "检查器 timer 是 ${state}，不是 active"
printf 'install-container-headroom: %s.timer active, script %s\n' "${unit_name}" "${gate}"
printf 'install-container-headroom: %s.timer active, series %s\n' "${check_name}" "${series}"
systemctl --user list-timers "${unit_name}.timer" "${check_name}.timer" --no-legend
