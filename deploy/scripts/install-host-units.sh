#!/usr/bin/env bash
# 一次装完本仓的宿主单元。装哪些、按什么顺序，从树里枚举——不靠谁记得住那几个名字。
#
# 为什么要有它：这些单元是这台机器上读数的载体，而「装哪几个」以前只存在于人的记忆和
# 一份 README 的散文里。忘掉一个的症状是那一面永远没有读数、`systemctl --failed` 也
# 什么都不会说——它压根没被装上，没有失败可报。
#
# 成员从 `deploy/scripts/install-*.sh` 枚举，每个安装脚本在自己头部声明一行
# `# host-install: user|root`：
#   user —— 不要 root、不吃参数，这条命令直接跑；
#   root —— 要 root，还要只有属主能给的输入（异地仓库地址、明文凭证目录），所以这里
#           不代跑，只把带占位符的调用原样印出来。
# 缺这一行、写两行、写了不认识的值，都直接退出码 2 并点名那个文件——否则新加一个安装
# 脚本会被安静地跳过，而「没装」在 `systemctl --failed` 上与「装好了」同形。
#
# 它把自己也装成一个 timer（`cogneva-host-units.timer`，每半小时一拍），跑的就是本命令。
# 那一拍是让「宿主与树一致」不再需要人记的地方：树里加了读数、改了排期、换了路径，最迟
# 半小时后宿主自己跟上，不需要谁记得回来重跑。第一次跑它仍然是人的动作——任何系统都得
# 有一个起点——但从跑完那一刻起，再没有第二个需要记的步骤。
#
# COGNEVA_INSTALL_RENDER_ONLY=1 时每一个都在只渲染模式下跑（含 root 的），写进各自的
# 临时 XDG 目录：测试要判「全跑一遍会不会有哪个写不出来」，而不该在这台机器上装东西。
#
# 用法：
#   bash deploy/scripts/install-host-units.sh [--list]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scripts_dir="${COGNEVA_INSTALL_SCRIPTS_DIR:-$here}"
self="$(basename "${BASH_SOURCE[0]}")"

die() { printf 'install-host-units: %s\n' "$*" >&2; exit 2; }

# 它自己也是一个单元，而且是让这件事不再需要人记的那一个：树变了（新增一个读数、改了
# 排期、换了路径），宿主上的副本不会自己跟上，除非有东西定期把两边对一遍。这个 timer
# 就是那个东西——每半小时按树重跑一遍本命令，成员照旧从树里枚举，所以新加的安装脚本
# 在下一拍自动被装上，没有人需要记得回来跑一次。
# 相位仍写在明处，和别的宿主 timer 不能同刻那条约束由 tests/host-timers-do-not-collide 守着。
self_cadence_minutes=30
self_phase_minute=3
if [ $((60 % self_cadence_minutes)) -ne 0 ] || [ "${self_cadence_minutes}" -ge 60 ]; then
    die "self cadence ${self_cadence_minutes} min does not divide 60: the layout would silently drop a beat"
fi
calendar_of() { # <第一拍的分钟> -> OnCalendar 串
    local minute="$1" out="" m
    for ((m = minute; m < 60; m += self_cadence_minutes)); do
        out+=$(printf ',%02d' "$m")
    done
    printf '*-*-* *:%s:00' "${out#,}"
}
self_unit="cogneva-host-units"
self_path="${here}/$(basename "${BASH_SOURCE[0]}")"

render_self_units() { # <落点目录>
    local dir="$1"
    mkdir -p "${dir}"
    cat >"${dir}/${self_unit}.service" <<EOF
[Unit]
Description=Re-install the host units this tree declares, when the tree has moved on

[Service]
Type=oneshot
# 没有 --converge 这种开关：这条命令本身就是幂等的（每个成员都按树重写单元并
# enable --now），跑第二遍等于再对一次账。它不删任何东西，只写单元并启用。
ExecStart=${self_path}
Nice=10
TimeoutStartSec=600
EOF
    cat >"${dir}/${self_unit}.timer" <<EOF
[Unit]
Description=Re-install the host units this tree declares

[Timer]
OnCalendar=$(calendar_of "${self_phase_minute}")
RandomizedDelaySec=60
Persistent=true

[Install]
WantedBy=timers.target
EOF
}

list_only=0
for arg in "$@"; do
    case "$arg" in
    --list) list_only=1 ;;
    -h|--help)
        sed -n '2,/^set -euo pipefail/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    *) die "不认识的参数：${arg}" ;;
    esac
done

# 声明行：必须恰好一行，值必须是 user 或 root。
class_of() { # <安装脚本>
    local f="$1" n value
    n="$(grep -c '^# host-install:' "${f}" || true)"
    [ "${n}" = 1 ] || die "$(basename "${f}") 有 ${n} 行 \`# host-install:\`，必须恰好 1 行"
    value="$(sed -n 's/^# host-install:[[:space:]]*//p' "${f}" | head -1)"
    case "${value}" in
    user|root) printf '%s' "${value}" ;;
    *) die "$(basename "${f}") 的 host-install 值认不出来：${value}（只认 user|root）" ;;
    esac
}

count=0
user_units=""
root_units=""
for f in "${scripts_dir}"/install-*.sh; do
    [ -e "${f}" ] || continue
    [ "$(basename "${f}")" = "${self}" ] && continue
    cls="$(class_of "${f}")"
    count=$((count + 1))
    case "${cls}" in
    user) user_units+="${f}"$'\n' ;;
    root) root_units+="${f}"$'\n' ;;
    esac
done
# 一个都没枚举到＝枚举本身坏了（目录不对、或有人把 install- 前缀改了）。这条命令这时
# 什么都不做却以 0 退出，读起来就像「都装好了」。
[ "${count}" -gt 0 ] || die "${scripts_dir} 下没有一个 install-*.sh"

if [ "${list_only}" = 1 ]; then
    while read -r f; do [ -n "${f}" ] && printf 'install  %s\n' "$(basename "${f}")"; done <<<"${user_units}"
    while read -r f; do [ -n "${f}" ] && printf 'print    %s（要 root 与属主输入，本命令只印调用）\n' "$(basename "${f}")"; done <<<"${root_units}"
    printf 'install-host-units: %d 个安装脚本（从树里枚举）\n' "${count}"
    exit 0
fi

if [ "${COGNEVA_INSTALL_RENDER_ONLY:-0}" = 1 ]; then
    while read -r f; do
        [ -n "${f}" ] || continue
        COGNEVA_INSTALL_RENDER_ONLY=1 bash "${f}" >/dev/null \
            || die "$(basename "${f}") 只渲染也失败——它装出来的排期进不了任何比较"
        printf 'install-host-units: 已渲染 %s\n' "$(basename "${f}")"
    done <<<"${user_units}${root_units}"
    render_self_units "${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
    exit 0
fi

while read -r f; do
    [ -n "${f}" ] || continue
    bash "${f}" || die "$(basename "${f}") 装失败"
done <<<"${user_units}"

# 装自己：从下一拍起，宿主与树的差由它自己补，不需要谁记得回来。
self_dir="${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
render_self_units "${self_dir}"
systemctl --user daemon-reload
systemctl --user enable --now "${self_unit}.timer"

while read -r f; do
    [ -n "${f}" ] || continue
    printf 'install-host-units: %s 要 root 与属主输入，没有代跑。用它自己的用法跑一次：\n' "$(basename "${f}")"
    grep -m3 -E '^# +sudo |^#   sudo ' "${f}" | sed 's/^# \{0,2\}/install-host-units:   /'
done <<<"${root_units}"

printf 'install-host-units: %d 个安装脚本，user 类已装完，root 类的调用见上\n' "${count}"
