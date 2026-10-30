#!/usr/bin/env bash
# Wire the host build-cache reclaimer into this user's systemd instance.
#
# No privilege is needed and none is asked for: the caches belong to this user,
# and linger keeps the timer alive between sessions. Nothing here writes a path
# by hand -- the work root is the directory the checkout lives in unless the
# caller names another one, and the unit is generated from the paths this script
# resolves, so a checkout that moves is re-wired by running this again.
#
# What it installs: a timer that runs the reclaimer every half hour, and a
# service to run. The reclaimer itself decides whether there is anything to do;
# below the trigger a run is a couple of readings and no work.
# 装这一份不需要 root、也不吃参数。install-host-units.sh 只认这一行来决定装哪些——
# 它的成员从树里枚举，所以新加一个安装脚本要自己声明这一行。
# host-install: user
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
reclaimer="${repo}/deploy/scripts/host-target-gc.sh"
work_root="${1:-$(dirname "${repo}")}"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-host-target-gc"

die() { printf 'install-host-target-gc: %s\n' "$*" >&2; exit 1; }

# 拍子写成两个数，OnCalendar 由它们算出来，不把 ":13,43" 直接抄进模板：抄进去以后
# 改周期就得同时改串和注释，漏一处不会有东西红。相位仍是要和这台机器上别的 timer
# 错开的那个数，那条约束由 tests/host-timers-do-not-collide.test.sh 守着。
cadence_minutes=30
phase_minute=13

# 一小时里铺满才叫「每 N 分钟一拍」；不整除时下面那个循环会漏拍，而排期看上去还在，
# 只是少了一拍。所以这里直接拒绝。
if [ $((60 % cadence_minutes)) -ne 0 ] || [ "${cadence_minutes}" -ge 60 ]; then
    die "cadence ${cadence_minutes} min does not divide 60: the layout would silently drop a beat"
fi

calendar_of() { # <first beat's minute> -> OnCalendar string, one beat every cadence_minutes
    local minute="$1" out="" m
    for ((m = minute; m < 60; m += cadence_minutes)); do
        out+=$(printf ',%02d' "$m")
    done
    printf '*-*-* *:%s:00' "${out#,}"
}
on_calendar="$(calendar_of "${phase_minute}")"

[ -f "${reclaimer}" ] || die "the reclaimer is not at ${reclaimer}"
[ -d "${work_root}" ] || die "the work root ${work_root} is not a directory; name one: $0 <work root>"
[ -x "${reclaimer}" ] || die "${reclaimer} is not executable"
case "${work_root}" in
/*) ;;
*) die "the work root has to be an absolute path, got ${work_root}" ;;
esac

mkdir -p "${unit_dir}"

cat >"${unit_dir}/${unit_name}.service" <<EOF
[Unit]
Description=Release the host build cache when the disk is above the trigger

[Service]
Type=oneshot
ExecStart=${reclaimer}
Environment=COGNEVA_HOST_WORK_ROOT=${work_root}
# It deletes files, so it must never compete with the build whose tree it is
# judging: idle IO and the lowest priority.
Nice=19
IOSchedulingClass=idle
NoNewPrivileges=yes
TimeoutStartSec=7200
EOF

# 带引号的 heredoc ＋ 占位符事后替换：无引号的 heredoc 会把注释里的反引号与 `$(` 一起
# 执行，而这批脚本的注释是讲为什么的散文，随时可能写进一个反引号。
cat >"${unit_dir}/${unit_name}.timer" <<'EOF'
[Unit]
Description=Release the host build cache when the disk is above the trigger

[Timer]
# 相位来自 phase_minute：同一周期的两个 timer 落在同一个分钟，每一拍都会撞在一起，
# 而这一拍删文件——它按 idle IO、Nice 19 跑，撞上谁都是把对方拖慢。
# Persistent catches up a run that was missed while the machine was off.
OnCalendar=@ON_CALENDAR@
RandomizedDelaySec=180
Persistent=true

[Install]
WantedBy=timers.target
EOF
sed -i "s|@ON_CALENDAR@|${on_calendar}|" "${unit_dir}/${unit_name}.timer"

if [ "${COGNEVA_INSTALL_RENDER_ONLY:-0}" = 1 ]; then
    # 只写文件，不碰这个用户的 systemd 实例。测试要判「渲染出来的排期对不对」，
    # 而它不该顺带在这台机器上装一个定时器。
    printf 'install-host-target-gc: 只渲染（COGNEVA_INSTALL_RENDER_ONLY=1）：%s\n' "${unit_dir}"
    exit 0
fi

systemctl --user daemon-reload
systemctl --user enable --now "${unit_name}.timer"

# Its own reading: "enabled" is not the same statement as "there is a next run".
state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "the timer is ${state}, not active"
printf 'install-host-target-gc: %s active, work root %s, script %s\n' \
    "${unit_name}.timer" "${work_root}" "${reclaimer}"
systemctl --user list-timers "${unit_name}.timer" --no-legend
