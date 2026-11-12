#!/usr/bin/env bash
# 宿主构建槽的客户端：判的是「门发起的编译真的从槽里拿了一个位子」，不是「脚本跑完
# 没报错」。两个方向的字节都判：拿得到槽时不许多占一个，拿不到槽时不许假装跑过。
#
# 还有三种结局必须分得开：进了槽、没槽可进所以退 75、以及命令自己红。把后两者混成
# 一个非零，就会把「这台机器正忙」写成「这一笔改动坏了」；把第一个写成 0 而其实没进槽，
# 就是宿主上又多了一条不受界约束的构建。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
lib="${repo}/deploy/scripts/lib/build-slot.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${lib}" ] || fail "客户端不存在：${lib}"
# shellcheck source=../lib/build-slot.sh
. "${lib}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

gate_dir="${work}/gate"
mkdir -p "${gate_dir}" "${work}/own"

# 每次调用都用自己的环境：库在 source 时就读了这些值，所以每一段都在一个子 shell 里跑。
run_in() { # run_in <自己的锁> <槽目录> <说明> <命令…>
    local own="$1" gate="$2" what="$3"; shift 3
    (
        COGNEVA_BUILD_GATE_LOCK_DIR="${gate}" \
            COGNEVA_BUILD_LOCK_FILE="${own}" \
            COGNEVA_BUILD_SLOT_WAIT_SECS="${SLOT_WAIT:-20}" \
            COGNEVA_BUILD_SLOT_POLL_SECS="${SLOT_POLL:-1}" \
            bash -c '. "$1"; shift; build_slot_run "$@"' _ "${lib}" "${what}" "$@"
    )
}

# 1. 没有槽目录：退到本机自己的锁，并且**说出来**——「只和自己串行」与「和集群串行」
#    在别处读起来一模一样。
out="$(run_in "${work}/own/lock" "${work}/nope" "fallback" true 2>&1)" ||
    fail "没有槽目录时应当照样跑完：${out}"
printf '%s\n' "${out}" | grep -q 'no gate at' ||
    fail "没有槽时要说出这一跑只和自己串行，实际：${out}"

# 2. 有槽：拿的是一个已经存在的槽文件，跑的时候那个文件里写着持有者。
#    三条文件都在，是因为门的三个文件各有各的用途：bound 说有几个位子，
#    slot-* 是位子本身，builds.claim 是「有构建在跑」与「有人独占施工」的区别。
printf '2\n' >"${gate_dir}/buildgate.bound"
: >"${gate_dir}/builds.claim"
: >"${gate_dir}/slot-0.lock"
: >"${gate_dir}/slot-1.lock"
cat >"${work}/peek.sh" <<'SH'
#!/usr/bin/env bash
cat "$1"/slot-*.lock
SH
chmod +x "${work}/peek.sh"
out="$(run_in "${work}/own/lock" "${gate_dir}" "持有者记录" bash "${work}/peek.sh" "${gate_dir}" 2>&1)" ||
    fail "有槽时应当跑完：${out}"
printf '%s\n' "${out}" | grep -q '持有者记录' ||
    fail "跑的时候槽文件里应当写着我这一格的持有者记录，实际：${out}"
printf '%s\n' "${out}" | grep -q 'held slot-' ||
    fail "退出时要报出拿了哪个槽、等了多久、跑了多久：${out}"

# 2b. 被服务过就留一行：门的另一半只从这一行知道 `gate: …` 这一问已经被服务过。
#     没有它就只剩「从没被服务」，于是对这份声明永远让——2026-11-10 主线推进
#     就这样被饿了三个小时。同一个名字再跑一次只换时间，不叠一行。
run_in "${work}/own/lock" "${gate_dir}" "持有者记录" true >/dev/null 2>&1 ||
    fail "第二次拿槽应当照样跑完"
lines="$(grep -c $'\tgate: 持有者记录$' "${gate_dir}/builds.served" 2>/dev/null || true)"
[ "${lines}" = "1" ] ||
    fail "每个名字在 builds.served 里只留一行，实际 ${lines} 行：$(cat "${gate_dir}/builds.served" 2>&1)"
grep -Eq $'^[0-9]+\tgate: 持有者记录$' "${gate_dir}/builds.served" 2>/dev/null ||
    fail "被服务过的工作应当在 builds.served 里留一行时间：$(cat "${gate_dir}/builds.served" 2>&1)"

# 3. 被别人占着（bound=1）：等，而不是并排跑。判据是时间顺序，不是字数。
printf '1\n' >"${gate_dir}/buildgate.bound"
: >"${gate_dir}/slot-0.lock"
(
    exec 7<"${gate_dir}/slot-0.lock"
    flock -x 7
    printf '%s\t%s\t%s\n' 999 "$(date +%s)" "别人" >"${gate_dir}/slot-0.lock"
    sleep 2
) &
holder=$!
sleep 1
out="$(SLOT_WAIT=20 SLOT_POLL=1 run_in "${work}/own/lock" "${gate_dir}" "等槽" true 2>&1)" ||
    fail "槽空出来后应当跑完：${out}"
wait "${holder}" 2>/dev/null || true
printf '%s\n' "${out}" | grep -Eq 'waited [1-9][0-9]*s|waiting [1-9][0-9]*s' ||
    fail "被占时应当报出真的等了，实际：${out}"

# 4. 别人刚放下的新鲜声明：先让它，不抢。等不到就退 75，它不会替我过期。
printf '%s\t%s' "别人" "$(date +%s)" >"${gate_dir}/builds.reserve"
: >"${gate_dir}/slot-0.lock"
start=$(date +%s)
set +e
out="$(SLOT_WAIT=2 SLOT_POLL=1 run_in "${work}/own/lock" "${gate_dir}" "让给等待者" true 2>&1)"
rc=$?
set -e
elapsed=$(( $(date +%s) - start ))
[ "${rc}" -eq 75 ] || fail "别人有新鲜声明时不该抢槽，应当退 75，实际退 ${rc}：${out}"
[ "${elapsed}" -ge 2 ] || fail "应当等到自己的等待预算走完，实际只等了 ${elapsed}s：${out}"

# 同一份声明，一旦过了新鲜窗就不再算数：迟到的等待者也不能永远占着场子。
printf '%s\t%s' "别人" "$(( $(date +%s) - 1000 ))" >"${gate_dir}/builds.reserve"
out="$(SLOT_WAIT=2 SLOT_POLL=1 run_in "${work}/own/lock" "${gate_dir}" "过期声明" true 2>&1)" ||
    fail "过期的声明不该继续挡住后来的构建：${out}"

# 5. 等不到槽：退 75，并且说的是「没进槽」，不是「红了」。
printf '1\n' >"${gate_dir}/buildgate.bound"
: >"${gate_dir}/slot-0.lock"
rm -f "${gate_dir}/builds.reserve"
(
    exec 6<"${gate_dir}/slot-0.lock"
    flock -x 6
    sleep 5
) &
holder=$!
sleep 0.5
set +e
out="$(SLOT_WAIT=2 SLOT_POLL=1 run_in "${work}/own/lock" "${gate_dir}" "等不到" true 2>&1)"
rc=$?
set -e
wait "${holder}" 2>/dev/null || true
[ "${rc}" -eq 75 ] || fail "等不到槽应当退 75（没判），实际退 ${rc}：${out}"
printf '%s\n' "${out}" | grep -q 'refusing to run' ||
    fail "等不到槽时说清的是拒跑而不是判红：${out}"

# 6. 命令自己的退出码原样传出去：进槽不加一层判词。
printf '2\n' >"${gate_dir}/buildgate.bound"
rm -f "${gate_dir}/builds.reserve"
set +e
run_in "${work}/own/lock" "${gate_dir}" "命令红" bash -c 'exit 7' >/dev/null 2>&1
rc=$?
set -e
[ "${rc}" -eq 7 ] || fail "命令自己的退出码应当原样传出去，实际退 ${rc}"

# 7. 等槽与干活分两个数报出来：问的人要的是「这一格自己花了多久」，而终端上那行
#    给人读的文字（`held … for Ns after waiting Ms`）不能拿去减。设了文件名就要有
#    一行 `<waited>\t<held>\t<name>`，没设就一个字都不多写——别的调用方不必跟着改。
printf '1\n' >"${gate_dir}/buildgate.bound"
: >"${gate_dir}/slot-0.lock"
rm -f "${gate_dir}/builds.reserve"
report="${work}/waits.tsv"
(
    exec 5<"${gate_dir}/slot-0.lock"
    flock -x 5
    sleep 2
) &
holder=$!
sleep 0.5
report_in() { # <报告文件> <说明> <命令…>
    local file="$1" what="$2"; shift 2
    (
        COGNEVA_BUILD_SLOT_REPORT="${file}" \
            COGNEVA_BUILD_GATE_LOCK_DIR="${gate_dir}" \
            COGNEVA_BUILD_LOCK_FILE="${work}/own/lock" \
            COGNEVA_BUILD_SLOT_WAIT_SECS=20 \
            COGNEVA_BUILD_SLOT_POLL_SECS=1 \
            bash -c '. "$1"; shift; build_slot_run "$@"' _ "${lib}" "${what}" "$@"
    )
}
export COGNEVA_BUILD_SLOT_REPORT="${report}"
lines_before="$(build_slot_reported_lines)"
report_in "${report}" "报等槽" true >/dev/null 2>&1 || fail "带报告文件时应当照样跑完"
wait "${holder}" 2>/dev/null || true
[ -s "${report}" ] || fail "设了 COGNEVA_BUILD_SLOT_REPORT 却没有留下任何一行"
line="$(tail -n 1 "${report}")"
case "${line}" in
*$'\t'gate:\ 报等槽) ;;
*) fail "报告行应当是 <等槽秒>\\t<干活秒>\\t<名字>，实际：${line}" ;;
esac
waited="$(printf '%s' "${line}" | cut -f1)"
held="$(printf '%s' "${line}" | cut -f2)"
[ "${waited}" -ge 1 ] || fail "被占着的时候等槽秒数应当大于 0，实际 ${waited}：${line}"
case "${held}" in '' | *[!0-9]*) fail "干活秒数应当是数字，实际 ${held}：${line}" ;; esac
[ "$(build_slot_reported_waits_since "${lines_before}")" = "${waited}" ] ||
    fail "从某个位置往后求和应当只数这一行：得 $(build_slot_reported_waits_since "${lines_before}")，行里是 ${waited}"
[ "$(build_slot_reported_waits_since 0)" -ge "${waited}" ] || fail "从头求和应当不少于这一行"

# 没设文件名：一个字都不多写，终端那行照旧。别的调用方（集群里的构建）不该因为门
# 要这个读数而多出一份文件，也不该因为它不设就少一行给人读的话。
unset COGNEVA_BUILD_SLOT_REPORT
[ "$(build_slot_reported_lines)" = "0" ] || fail "没设报告文件时行数应当是 0"
[ "$(build_slot_reported_waits_since 0)" = "0" ] || fail "没设报告文件时等槽秒数应当是 0"
out="$(
    COGNEVA_BUILD_GATE_LOCK_DIR="${gate_dir}" \
        COGNEVA_BUILD_LOCK_FILE="${work}/own/lock" \
        COGNEVA_BUILD_SLOT_WAIT_SECS=20 \
        bash -c '. "$1"; shift; build_slot_run "$@"' _ "${lib}" "不报" true 2>&1
)" || fail "没设报告文件时应当照样跑完：${out}"
printf '%s\n' "${out}" | grep -q 'held slot-' || fail "没设报告文件时终端那行照旧要有：${out}"

echo "ok - 没有槽时退到本机的锁并说明；有槽时写持有者记录、被占时等待、等不到退 75、命令的退出码原样传出；等槽与干活分两个数落一份记录"
