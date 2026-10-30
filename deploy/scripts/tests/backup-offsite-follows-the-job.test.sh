#!/usr/bin/env bash
# principle: feedback_a_unit_that_fires_before_its_producer_produces_pushes_yesterday_s_artifact
# 异地推的时刻必须落在「备份最晚可能跑完」之后——判的是**算出来的那一刻**，不是某个字面值。
#
# 为什么要有这一条：2026-10-19 写下的 OnCalendar 是 02:17，注释说「比备份 CronJob 晚一小时」，
# 而那个 03:17 属于 Etc/UTC（宿主是 Asia/Shanghai，即本地 11:17）。于是每天的推都早于
# 当天那份包九个多小时，推的是昨天那份——而推成功是绿的，没有任何东西会红。换成别的
# 字面值也一样：只要它是照着排期手算的，时区一变就又漂一次（2026-09-26 已经漂过一次）。
#
# 所以这里不钉 12:47，判的是等式：把 values.yaml 的排期按时区换成绝对时刻，加上
# startingDeadlineSeconds 与 runDeadlineSeconds，再换成宿主本地时刻，必须等于渲染出来的
# OnCalendar。最后一条用旧的 02:17 做反例：它必须**不**满足这个等式，否则这条测试对
# 「推得早」这种错没有分辨力。
#
# 跑法：bash deploy/scripts/tests/backup-offsite-follows-the-job.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
installer="${repo}/deploy/scripts/install-backup-offsite.sh"
values="${repo}/deploy/helm/cogneva/values.yaml"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${installer}" ] || fail "安装脚本不存在：${installer}"
[ -f "${values}" ] || fail "读不到 ${values}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

XDG_CONFIG_HOME="${work}" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${installer}" >/dev/null \
    || fail "只渲染也跑不通"
tmr="${work}/systemd/user/cogneva-backup-offsite.timer"
svc="${work}/systemd/user/cogneva-backup-offsite.service"
[ -f "${tmr}" ] || fail "没有写出 timer：${tmr}"
[ -f "${svc}" ] || fail "没有写出 service：${svc}"

# 1. 排期必须被替换过。没有 OnCalendar 的 timer 在 systemd 里是「装载成功、永不触发」。
grep -q '@ON_CALENDAR@' "${tmr}" && fail "占位符没有替换：$(cat "${tmr}")"
n="$(grep -c '^OnCalendar=' "${tmr}" || true)"
[ "${n}" = 1 ] || fail "timer 有 ${n} 条 OnCalendar，应当是 1 条：$(cat "${tmr}")"
cal="$(grep -m1 '^OnCalendar=' "${tmr}")"
cal="${cal#OnCalendar=}"
case "${cal}" in
*-*-*\ [0-2][0-9]:[0-5][0-9]:00) ;;
*) fail "排期形状认不出来：${cal}" ;;
esac
push_hm="${cal#*-*-* }"; push_hm="${push_hm%:00}"

# 2. service 也照旧写出来，否则定时器起来没有东西可跑。
grep -q '^ExecStart=' "${svc}" || fail "service 没有 ExecStart"
grep -q '^WantedBy=timers.target' "${tmr}" || fail "timer 没有 WantedBy=timers.target"

# 3. 等式：排期（按时区解释）＋ 两个界 ＝ 渲染出来的本地时刻。
value_of() { # <叶子名>
    awk -v key="$1" '
        $0 ~ "^[[:space:]]*" key ":" {
            sub("^[[:space:]]*" key ":[[:space:]]*", "")
            sub(/[[:space:]]+#.*$/, "")
            gsub(/"/, "")
            sub(/[[:space:]]+$/, "")
            print; exit
        }' "${values}"
}
schedule="$(value_of schedule)"
tz="$(value_of timeZone)"
run_deadline="$(value_of runDeadlineSeconds)"
start_deadline="$(value_of startingDeadlineSeconds)"
read -r s_min s_hour _rest <<<"${schedule}"
[ -n "${s_min:-}" ] && [ -n "${s_hour:-}" ] || fail "排期读不出来：${schedule}"
latest_epoch="$(TZ="${tz}" date -d "today ${s_hour}:${s_min}" +%s)"
latest_epoch=$((latest_epoch + run_deadline + start_deadline))

# 渲染出来的那一刻（按宿主本地解释）与 latest_epoch 相差必须是 24 小时的整数倍：
# 每天一拍、相位对，就只差整天的整数倍。用模运算而不是直接等，相位跨午夜时也成立。
rendered_epoch="$(date -d "today ${push_hm}" +%s)"
delta=$(( (rendered_epoch - latest_epoch) % 86400 ))
[ "${delta}" -ge 0 ] || delta=$((delta + 86400))
[ "${delta}" = 0 ] \
    || fail "推的时刻 ${push_hm}（宿主本地）不等于备份最晚可能跑完的那一刻 $(date -d "@${latest_epoch}" '+%H:%M')：差 $((delta / 60)) 分钟"

# 4. 分辨力：旧字面值 02:17 必须判不过。它过得了，说明上面那条等式对「推得早」没有分辨力。
old_epoch="$(date -d 'today 02:17' +%s)"
old_delta=$(( (old_epoch - latest_epoch) % 86400 ))
[ "${old_delta}" -ge 0 ] || old_delta=$((old_delta + 86400))
[ "${old_delta}" != 0 ] \
    || fail "旧的 02:17 也满足等式——这条测试分辨不出「推得比备份还早」，等于没判"

echo "backup-offsite-follows-the-job: 4 组用例通过（推在 $(date -d "@${latest_epoch}" '+%H:%M') 本地时刻）"
