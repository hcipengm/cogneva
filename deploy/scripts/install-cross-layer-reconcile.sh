#!/usr/bin/env bash
# 把跨层对账接进这个用户的 systemd 实例，每半小时跑一次。
#
# 为什么是定时而不是某条流程里的一步：这条读数判的是**两层的差**，而起因可能出现在
# 任何一层——有人手工 patch 了配额、节点换了、某个 DaemonSet 在一个节点上没起来。
# 没有任何一条「改了什么之后」的钩子能覆盖这几种。定时跑是唯一不需要事先猜到
# 起因的载体。
#
# 它写两样东西：本机状态目录里的一行流水（序列），以及——带 --converge 时——把
# live 与树内声明不一致的治理对象按声明 apply 回去。后者带测量的界：apply 之后仍装得下
# 此刻的用量才写（细则在门脚本头部）。除此之外全程只读；sudo -n 已经配好 NOPASSWD 的
# kubectl，不需要常驻权限，也不需要集群内的 ServiceAccount。
#
# 第二个单元是序列检查器：产出停了、或者连着两拍没到，`systemctl --failed` 上要看得见。
# 产出单元自己的退出码回答不了「我在不在跑」——它不在的时候，没有退出码可以红。
#
# 用法：bash deploy/scripts/install-cross-layer-reconcile.sh
# 装这一份不需要 root、也不吃参数。install-host-units.sh 只认这一行来决定装哪些——
# 它的成员从树里枚举，所以新加一个安装脚本要自己声明这一行。
# host-install: user
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
reconcile="${repo}/deploy/scripts/check-cross-layer-reconcile.sh"
checker="${repo}/deploy/scripts/check-reconcile-series.sh"
series="${XDG_STATE_HOME:-${HOME:?}/.local/state}/cogneva/cross-layer-reconcile.tsv"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-cross-layer-reconcile"
check_name="cogneva-cross-layer-reconcile-series-check"

die() { printf 'install-cross-layer-reconcile: %s\n' "$*" >&2; exit 1; }

# 拍子的三个数写成一个等式，不各写一遍：周期（分钟）、产出落在哪一分钟、检查器比产出
# 晚多少分钟。`--interval` 是「两拍之间超过多久算缺拍」，它必须等于 timer 的周期——
# 两个数分开写时，把周期从 30 分钟改成 10 分钟而漏改 interval，缺拍判据就迟钝三倍，
# 而序列照常落行、检查照常通过，没有任何东西会红。相位仍是人挑的数（要和这台机器上
# 别的 timer 错开），那条约束由 tests/ 里「不能同刻」那几组守着。
cadence_minutes=30
phase_minute=7
check_gap_minutes=10

# 一小时里均匀铺满才叫「每 N 分钟一拍」；不整除时下面那个循环会漏拍，而 interval
# 判据仍然按 N 分钟算——静默地把缺拍读成正常。所以这里直接拒绝。
if [ $((60 % cadence_minutes)) -ne 0 ] || [ "$cadence_minutes" -ge 60 ]; then
    die "周期 ${cadence_minutes} 分钟不整除 60：一小时里铺不满，而 --interval 仍按它算"
fi

interval=$((cadence_minutes * 60))
calendar_of() { # <第一拍的分钟> -> OnCalendar 串（每 cadence_minutes 分钟一拍）
    local minute="$1" out="" m
    for ((m = minute; m < 60; m += cadence_minutes)); do
        out+=$(printf ',%02d' "$m")
    done
    printf '*-*-* *:%s:00' "${out#,}"
}
on_calendar="$(calendar_of "$phase_minute")"
check_calendar="$(calendar_of "$(((phase_minute + check_gap_minutes) % 60))")"

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
# --converge：判据 4 的差由这条拍子自己填——树内声明已经唯一决定了该 apply 什么，
# 动作不需要第二个决定者。写集群之前先按此刻的用量验算装不装得下。
ExecStart=${reconcile} --record --converge
Environment=COGNEVA_RECONCILE_SERIES=${series}
# 每半小时一拍、可能写治理对象，但仍不占多少 CPU；让它排在任何构建后面。
Nice=10
# 不能加 NoNewPrivileges=yes：读集群、写治理对象走的都是「sudo -n k3s kubectl」，
# 而这个标志让 sudo 直接拒绝提权——装好之后每一次运行都以退出码 2 结束，看起来像
# 「集群读不到」。这里不给的是常驻特权：sudo 仍然只到那一条 NOPASSWD 的 kubectl。
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

# 两个 timer 仍写成带引号的 heredoc，排期用占位符事后替换：无引号的 heredoc 会把注释里
# 的反引号与 `$(` 一起执行，而这两份文件的注释是讲为什么的散文，随时可能写进一个反引号。
cat >"${unit_dir}/${unit_name}.timer" <<'EOF'
[Unit]
Description=Reconcile the declared, enforced and physical resource layers

[Timer]
# 错开整点与半点：这台机器上的其他定时任务都落在那些时刻。相位是 phase_minute，
# 与别的 timer 不同刻那条约束由测试里「不能同刻」那组守着。
OnCalendar=@ON_CALENDAR@
RandomizedDelaySec=120
Persistent=true

[Install]
WantedBy=timers.target
EOF
sed -i "s|@ON_CALENDAR@|${on_calendar}|" "${unit_dir}/${unit_name}.timer"

cat >"${unit_dir}/${check_name}.timer" <<'EOF'
[Unit]
Description=Clock for the cross-layer reconcile series check

[Timer]
# 产出相位 + check_gap_minutes：和产出同刻跑会让一次迟到同时被算成缺拍与正常。
OnCalendar=@CHECK_CALENDAR@
RandomizedDelaySec=60
Persistent=true

[Install]
WantedBy=timers.target
EOF
sed -i "s|@CHECK_CALENDAR@|${check_calendar}|" "${unit_dir}/${check_name}.timer"

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
# 只在序列还不存在时种：这条命令会被 install-host-units.sh 的定时器每半小时跑一遍，
# 每次都补一拍就成了额外的高频拍子，而序列的节律恰恰是检查器判缺拍的依据。
if [ ! -f "${series}" ]; then
    systemctl --user start "${unit_name}.service" || \
        printf 'install-cross-layer-reconcile: 首拍没有成功（它已落一行 unreadable），见 journalctl --user -u %s\n' \
            "${unit_name}.service" >&2
fi

state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "timer 是 ${state}，不是 active"
state="$(systemctl --user is-active "${check_name}.timer")"
[ "${state}" = active ] || die "检查器 timer 是 ${state}，不是 active"
printf 'install-cross-layer-reconcile: %s.timer active, script %s\n' \
    "${unit_name}" "${reconcile}"
printf 'install-cross-layer-reconcile: %s.timer active, series %s\n' \
    "${check_name}" "${series}"
systemctl --user list-timers "${unit_name}.timer" "${check_name}.timer" --no-legend
