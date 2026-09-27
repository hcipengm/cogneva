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
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
reclaimer="${repo}/deploy/scripts/host-target-gc.sh"
work_root="${1:-$(dirname "${repo}")}"
unit_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
unit_name="cogneva-host-target-gc"

die() { printf 'install-host-target-gc: %s\n' "$*" >&2; exit 1; }

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

cat >"${unit_dir}/${unit_name}.timer" <<'EOF'
[Unit]
Description=Release the host build cache when the disk is above the trigger

[Timer]
# Off the hour and off the half hour: nothing else on this machine fires then.
# Persistent catches up a run that was missed while the machine was off.
OnCalendar=*-*-* *:13,43:00
RandomizedDelaySec=180
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now "${unit_name}.timer"

# Its own reading: "enabled" is not the same statement as "there is a next run".
state="$(systemctl --user is-active "${unit_name}.timer")"
[ "${state}" = active ] || die "the timer is ${state}, not active"
printf 'install-host-target-gc: %s active, work root %s, script %s\n' \
    "${unit_name}.timer" "${work_root}" "${reclaimer}"
systemctl --user list-timers "${unit_name}.timer" --no-legend
