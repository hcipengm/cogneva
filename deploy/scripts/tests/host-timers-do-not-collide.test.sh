#!/usr/bin/env bash
# principle: feedback_a_gate_whose_members_are_named_by_hand_must_scan_for_unlisted_members
# principle: feedback_a_handwritten_census_list_must_be_checked_against_the_whole_tree
# 这台机器上的宿主 timer 不许撞在同一个分钟——判的是**树里现在的全部**，不是一份手抄的名单。
#
# 为什么要有这一条：每个 install-*.sh 各自挑一个相位，挑的时候都只说「和别的错开」，
# 而没有任何东西真的把「别的」数出来。2026-10-30 实测的形状就是这样：headroom 的测试
# 拿自己的相位和跨层对账比了，却从没看过 host-target-gc 的 ":13,43"——它的安装脚本
# 连只渲染都做不到，测试想读它的排期也读不到。第五个安装脚本出现时，同一件事会再发生
# 一次，而那时候没有人会记得去改某条测试里的 for 循环。
#
# 所以这里的枚举从树里来，分两路：deploy/systemd/*.timer 是签入的静态单元，直接读；
# deploy/scripts/install-*.sh 里凡是支持 COGNEVA_INSTALL_RENDER_ONLY 的，渲染到各自的
# 临时 XDG 目录里把它写出的 timer 收上来。不支持只渲染、却在脚本里自己写 timer 的安装
# 脚本直接判红——它写出来的排期进不了这张比较表，那正是这条门要防的洞。
#
# 判据是「同一周期的两个 timer 的**排期分钟集合**不相交」，不是全部 timer 两两不相交：
# 周期不同的两个撞一分钟，重叠被粗的那一拍封顶（现在唯一一处是每日 02:17 的异地备份与
# 每小时第 17 分的序列检查，各一次、都是几乎不占资源的活）；周期相同的两个撞一分钟则是
# **每一拍**都撞。
#
# 判的是排期时刻，不是 RandomizedDelaySec 铺开的窗口：窗口重叠由那个延迟本身封顶（这里
# 最大 300 秒），而这几个单元该退让的地方已经写死在单元里（回收器 Nice=19、idle IO）。
#
# 跑法：bash deploy/scripts/tests/host-timers-do-not-collide.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 一个 OnCalendar 值 -> "周期分钟<TAB>0,13,43,..."（一天里的绝对分钟，升序）。
# 只认这两种形状：`*-*-* *:MM,MM:00`（每小时若干拍）与 `*-*-* HH:MM:00`（每天一拍）。
# 认不出来的形状直接判红——静默跳过就是这条门上一次失效的方式。
parse_calendar() { # <OnCalendar 值>
    local value="$1" time hour mins
    time="${value##* }"
    hour="${time%%:*}"
    mins="${time#*:}"
    mins="${mins%%:*}"
    awk -v hour="${hour}" -v mins="${mins}" 'BEGIN {
        n = split(mins, m, ",")
        if (n < 1) exit 1
        for (i = 1; i <= n; i++) {
            if (m[i] !~ /^[0-9][0-9]?$/) exit 1
            mins_ok[i] = m[i] + 0
        }
        total = 0
        if (hour == "*") {
            for (h = 0; h < 24; h++)
                for (i = 1; i <= n; i++) abs[++total] = h * 60 + mins_ok[i]
        } else {
            if (hour !~ /^[0-9][0-9]?$/) exit 1
            for (i = 1; i <= n; i++) abs[++total] = (hour + 0) * 60 + mins_ok[i]
        }
        # 升序（输入本来就近升序，这里不赌它）
        for (i = 2; i <= total; i++) {
            v = abs[i]; j = i - 1
            while (j >= 1 && abs[j] > v) { abs[j+1] = abs[j]; j-- }
            abs[j+1] = v
        }
        if (total == 1) { cadence = 1440 } else {
            cadence = 1440 - abs[total] + abs[1]
            for (i = 1; i < total; i++) if (abs[i+1] - abs[i] < cadence) cadence = abs[i+1] - abs[i]
        }
        out = ""
        for (i = 1; i <= total; i++) out = out (i > 1 ? "," : "") abs[i]
        print cadence "\t" out
    }'
}

# 收上来的每一行：「周期<TAB>分钟集合<TAB>timer 名<TAB>来源」
rows=""
add_timer() { # <timer 文件> <来源：安装脚本名或 static>
    local file="$1" source="$2" cal name parsed value
    cal="$(grep -c '^OnCalendar=' "${file}" || true)"
    [ "${cal}" = 1 ] || fail "$(basename "${file}") 有 ${cal} 条 OnCalendar，这条门只判一条：${file}"
    value="$(grep -m1 '^OnCalendar=' "${file}")"
    value="${value#OnCalendar=}"
    parsed="$(parse_calendar "${value}")" \
        || fail "$(basename "${file}") 的排期认不出来：${value}（认不出来就跳过=这条门又失效一次）"
    name="$(basename "${file}" .timer)"
    rows+="${parsed}"$'\t'"${name}"$'\t'"${source}"$'\n'
}

# 一路：签入的静态单元。
static_count=0
for f in "${repo}"/deploy/systemd/*.timer; do
    [ -e "${f}" ] || continue
    add_timer "${f}" "deploy/systemd"
    static_count=$((static_count + 1))
done

# 二路：每个安装脚本自己写出来的单元。名字从树里来，新加一个安装脚本自动进来。
installers=0
for script in "${repo}"/deploy/scripts/install-*.sh; do
    [ -e "${script}" ] || continue
    installers=$((installers + 1))
    name="$(basename "${script}" .sh)"
    grep -q '\.timer' "${script}" || fail "${name} 装的东西里没有 timer，这条门把它当成了定时任务来源——先确认它是不是不该在这里"

    if grep -q 'COGNEVA_INSTALL_RENDER_ONLY' "${script}"; then
        out="${work}/${name}"
        XDG_CONFIG_HOME="${out}" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${script}" >/dev/null \
            || fail "${name} 只渲染也跑不通，它的排期进不了这张表"
        found=0
        for f in "${out}"/systemd/user/*.timer; do
            [ -e "${f}" ] || continue
            add_timer "${f}" "${name}"
            found=$((found + 1))
        done
        [ "${found}" -gt 0 ] || fail "${name} 支持只渲染，却没有写出任何 timer：递归走不到它的排期"
    else
        # 不支持只渲染时，它的 timer 必须是签入的静态文件（上面那一路已经读了）。
        # 判据是脚本里点到了 deploy/systemd；自己拼一份 timer 正文的，进不了比较表。
        grep -q 'deploy/systemd' "${script}" \
            || fail "${name} 自己写 timer 却既不支持 COGNEVA_INSTALL_RENDER_ONLY、也不从 deploy/systemd 取：它的排期进不了同刻比较"
    fi
done
[ "${installers}" -gt 0 ] || fail "一个 install-*.sh 都没找到：枚举本身坏了，这条门会一直绿"

# 覆盖面的自证：至少要把三份 30 分钟一拍的定时任务与静态单元都收上来。数字不钉死
# 「应当是几」，只判「比静态单元多出来的那些安装脚本确实被读到了」。
[ "$(printf '%s' "${rows}" | grep -c .)" -gt "${static_count}" ] \
    || fail "收上来的 timer 不比静态单元多：安装脚本写出的排期没有被读到"

# 判：同一周期的两个 timer，排期分钟集合不许相交。
# 两个集合的交集用 awk 算：按周期分组，组内两两比。
collisions="$(printf '%s' "${rows}" | awk -F'\t' '
    {
        cadence[NR] = $1; set[NR] = $2; name[NR] = $3; src[NR] = $4
        n = NR
    }
    END {
        for (i = 1; i <= n; i++) {
            split(set[i], a, ",")
            for (j = i + 1; j <= n; j++) {
                if (cadence[i] != cadence[j]) continue
                split(set[j], b, ",")
                for (x = 1; x <= length(a); x++) {
                    if (a[x] == "") continue
                    for (y = 1; y <= length(b); y++) {
                        if (b[y] == "") continue
                        if (a[x] != b[y]) continue
                        # 同一个周期里撞的那些分钟每小时重复一遍；归约到一个周期内再报，
                        # 一处撞车只出一行——四十八行的状纸读不出「撞在哪」。
                        m = cadence[i]
                        key = m "\t" (a[x] % m) "\t" i "\t" j
                        if (key in seen) continue
                        seen[key] = 1
                        printf "%s（%s）与 %s（%s）在每一拍的同一刻相撞（周期 %d 分钟，偏移 %d 分钟）\n", \
                            name[i], src[i], name[j], src[j], m, a[x] % m
                    }
                }
            }
        }
    }')"
[ -z "${collisions}" ] || fail "宿主 timer 撞在同一刻：${collisions}"

count="$(printf '%s' "${rows}" | grep -c .)"
echo "host-timers-do-not-collide: ${count} 个 timer（${installers} 个安装脚本）通过"
