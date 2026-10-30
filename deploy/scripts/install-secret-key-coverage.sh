#!/usr/bin/env bash
# 把 Secret 键覆盖读数接进这个用户的 systemd 实例，每半小时跑一次。
#
# 为什么是定时而不是安装时跑一次：这条读数判的是**树与现网的差**，而起因有三种，
# 都发生在装好之后——树里新增了一个清单要读的键、有人重建过 Secret、有人把某个键
# 手工删掉。2026-10-30 那次就是第一种：清单开始读 `app-password`，而现网 Secret 是
# 37 天前建的，没有这个键；安全网关一个可用 Pod 都不剩，而树里的契约测试全绿。
# 没有任何一条「改了什么之后」的钩子能覆盖这三种，定时跑是唯一不需要事先猜到起因的载体。
#
# 第二个单元是序列检查器（与容器余量共用同一份脚本）：产出停了、或者连着两拍没到，
# 在 systemctl --failed 上要看得见。产出单元自己的退出码回答不了「我在不在跑」。
#
# 用法：bash deploy/scripts/install-secret-key-coverage.sh
# 装这一份不需要 root、也不吃参数。install-host-units.sh 只认这一行来决定装哪些——
# 它的成员从树里枚举，所以新加一个安装脚本要自己声明这一行。
# host-install: user
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
gate="${repo}/deploy/scripts/check-secret-key-coverage.sh"
checker="${repo}/deploy/scripts/check-reconcile-series.sh"
series="${XDG_STATE_HOME:-${HOME:?}/.local/state}/cogneva/secret-key-coverage.tsv"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-secret-key-coverage"
check_name="cogneva-secret-key-coverage-series-check"

die() { printf 'install-secret-key-coverage: %s\n' "$*" >&2; exit 1; }

# 拍子的三个数写成一个等式，不各写一遍：周期（分钟）、产出落在哪一分钟、检查器比产出
# 晚多少分钟。`--interval` 是「两拍之间超过多久算缺拍」，它必须等于 timer 的周期。
# 相位由「不能同刻」那条门守着：它与本机其他宿主 timer 不许落在同一分钟的同一周期里。
cadence_minutes=30
phase_minute=9
check_gap_minutes=10

if [ $((60 % cadence_minutes)) -ne 0 ] || [ "${cadence_minutes}" -ge 60 ]; then
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

[ -f "${gate}" ] || die "读数脚本不在这里：${gate}"
[ -x "${gate}" ] || die "${gate} 不可执行"
[ -x "${checker}" ] || die "${checker} 不存在或不可执行"

mkdir -p "${unit_dir}"

cat >"${unit_dir}/${unit_name}.service" <<EOF
[Unit]
Description=Judge that every Secret key the rendered manifests read exists on the cluster

[Service]
Type=oneshot
# --live：除了树里的两面，还要读现网 Secret 的键集合。只判树里的那一版看不见
# 「树里有、现网没有」，而那正是 2026-10-30 那次故障的形状。
# --record：每一拍（绿的、红的、读不到的）都往序列里落一行。判红的那一拍也是拍子，
# 不落的话序列上的洞会同时代表「没跑」与「跑了但红了」。
ExecStart=${gate} --live --record
Environment=COGNEVA_KUBECTL=sudo -n k3s kubectl
Environment=COGNEVA_SECRET_KEY_SERIES=${series}
# 只读，但会去问集群；让它排在任何构建后面。
Nice=10
# 不能加 NoNewPrivileges=yes：读集群走的是「sudo -n k3s kubectl」，而这个标志让
# sudo 直接拒绝提权——装好之后每一次运行都以退出码 2 结束，看起来像「集群读不到」。
TimeoutStartSec=120
EOF

cat >"${unit_dir}/${check_name}.service" <<EOF
[Unit]
Description=Check that the secret key coverage series still has its beats

[Service]
Type=oneshot
ExecStart=${checker} --file ${series} --interval ${interval}
# 不写 SuccessExitStatus：退出码 1（缺拍）就是这个单元失败。把红包装成成功，
# 「序列停了」就只剩 journal 里的一行，而 systemctl --failed 什么也不说。
Nice=10
TimeoutStartSec=60
EOF

# 两个 timer 仍写成带引号的 heredoc，排期用占位符事后替换：无引号的 heredoc 会把注释里
# 的反引号与 `$(` 一起执行，而这两份文件的注释是讲为什么的散文，随时可能写进一个反引号。
cat >"${unit_dir}/${unit_name}.timer" <<'EOF'
[Unit]
Description=Judge the Secret key coverage every half hour

[Timer]
# 相位是 phase_minute，与别的宿主 timer 不同刻那条约束由
# tests/host-timers-do-not-collide.test.sh 守着（它从树里枚举，不是手抄名单）。
OnCalendar=@ON_CALENDAR@
RandomizedDelaySec=120
Persistent=true

[Install]
WantedBy=timers.target
EOF
sed -i "s|@ON_CALENDAR@|${on_calendar}|" "${unit_dir}/${unit_name}.timer"

cat >"${unit_dir}/${check_name}.timer" <<'EOF'
[Unit]
Description=Clock for the secret key coverage series check

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
    printf 'install-secret-key-coverage: 只渲染（COGNEVA_INSTALL_RENDER_ONLY=1）：%s\n' "${unit_dir}"
    exit 0
fi

systemctl --user daemon-reload
systemctl --user enable --now "${unit_name}.timer"
systemctl --user enable --now "${check_name}.timer"

# 立刻跑一拍，把序列文件种出来。不种的话，检查器在第一次产出之前（最多 30 分钟）
# 读到的是「文件不存在」——那是红，但红的原因是我们刚装好，不是拍子停了。
# 只在序列还不存在时种：这条命令会被 install-host-units.sh 的定时器每半小时跑一遍，
# 每次都补一拍就成了额外的高频拍子，而序列的节律恰恰是检查器判缺拍的依据。
if [ ! -f "${series}" ]; then
    systemctl --user start "${unit_name}.service" || \
        printf 'install-secret-key-coverage: 首拍没有成功（它已落一行 unreadable），见 journalctl --user -u %s\n' \
            "${unit_name}.service" >&2
fi

state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "timer 是 ${state}，不是 active"
state="$(systemctl --user is-active "${check_name}.timer")"
[ "${state}" = active ] || die "检查器 timer 是 ${state}，不是 active"
printf 'install-secret-key-coverage: %s.timer active, script %s\n' "${unit_name}" "${gate}"
printf 'install-secret-key-coverage: %s.timer active, series %s\n' "${check_name}" "${series}"
systemctl --user list-timers "${unit_name}.timer" "${check_name}.timer" --no-legend
