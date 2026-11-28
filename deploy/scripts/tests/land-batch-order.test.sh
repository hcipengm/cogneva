#!/usr/bin/env bash
# principle: feedback_a_verdict_that_writes_no_state_is_invisible_to_a_reader_that_selects_by_state
# 一批落地的顺序是一条跑法，不是一个约定：谁在前、停在哪、下一步是谁，都由它答。
#
# 判据三条：
#   1. 全绿时六步按序各跑一次，日志的顺序就是表里的顺序；
#   2. 某一步红时，它之前的那几步跑过、之后的没跑，退出码就是那一步的序号——
#      读退出码的人分得出停在批门（2）还是停在推前门（3）；
#   3. 停下的那句话点名下一步是哪一步，接手的人不必回头读脚本。
#
# 每一步跑什么由 `COGNEVA_LAND_BIN` 换成夹具里的可执行文件，所以这里判的是顺序
# 本身，不是六个真命令的接线（那六个各有自己的门）。
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
script="${repo}/scripts/hooks/land-batch.sh"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 夹具仓：脚本先判「工作树干净」与「有没有要推的东西」，所以夹具要一个提交过、
# 且领先 `origin/main` 的仓——那两项是这一批的前提，不是被测的顺序。
fixture="${work}/repo"
mkdir -p "${fixture}/scripts/hooks"
git -C "${work}" init -q repo
git -C "${fixture}" -c user.email=gate@example.invalid -c user.name=gate \
    commit -q --allow-empty -m 'base'
git -C "${fixture}" update-ref refs/remotes/origin/main HEAD
git -C "${fixture}" -c user.email=gate@example.invalid -c user.name=gate \
    commit -q --allow-empty -m 'this batch'
cp "${script}" "${fixture}/scripts/hooks/land-batch.sh"
chmod +x "${fixture}/scripts/hooks/land-batch.sh"
# The script stops on a dirty tree before its first step, so the fixture's own copy
# has to be committed: an untracked copy would make every case below stop at the
# precondition instead of measuring the order.
git -C "${fixture}" add scripts
git -C "${fixture}" -c user.email=gate@example.invalid -c user.name=gate \
    commit -q -m 'the runner under test'

# 六步的替身：各写一行到日志。杀哪一步由 $1 给（空 = 都绿）。
make_bin() { # <目录> <要红的步序号>
    local dir="$1" red="${2:-}"
    mkdir -p "${dir}"
    local pair
    for pair in 1-rebound 2-gate 3-prepush 4-push 5-ledger 6-close; do
        local n="${pair%%-*}"
        local body="printf '%s\\n' '${pair}' >>\"\${LAND_LOG:?}\""
        if [ "${n}" = "${red}" ]; then
            body="printf '%s\\n' '${pair}' >>\"\${LAND_LOG:?}\"; exit 1"
        fi
        printf '#!/usr/bin/env bash\n%s\n' "${body}" >"${dir}/${pair}"
        chmod +x "${dir}/${pair}"
    done
}

run_land() { # <bin 目录> <日志> [额外 env…]
    local bin="$1" log="$2"
    shift 2
    env "${@}" COGNEVA_LAND_BIN="${bin}" LAND_LOG="${log}" \
        bash "${fixture}/scripts/hooks/land-batch.sh" >"${log}.out" 2>&1
}

# --- 1) 全绿：六步按序 ---
all="${work}/bin-all"
all_log="${work}/all.log"
make_bin "${all}" ""
: >"${all_log}"
run_land "${all}" "${all_log}" || fail "全绿的一批被判成没落地：$(cat "${all_log}.out")"
want='1-rebound
2-gate
3-prepush
4-push
5-ledger
6-close'
got="$(cat "${all_log}")"
[ "${got}" = "${want}" ] || fail "六步的顺序不是表里的顺序：期望 [${want}]，实际 [${got}]"
grep -q '全部 6 步绿' "${all_log}.out" \
    || fail "全绿时没说清楚这一批推上去了、轮关了：$(cat "${all_log}.out")"

# --- 2) 批门红：停在 2，三段读数各管一件事 ---
red="${work}/bin-red"
red_log="${work}/red.log"
make_bin "${red}" 2
: >"${red_log}"
rc=0
run_land "${red}" "${red_log}" || rc=$?
[ "${rc}" = 2 ] || fail "停在批门时退出码是 ${rc}，不是 2：读码的人分不出停在哪一步"
got="$(cat "${red_log}")"
want='1-rebound
2-gate'
[ "${got}" = "${want}" ] || fail "红在批门之后还跑了后面的步骤：期望 [${want}]，实际 [${got}]"
grep -q '停在 2/6' "${red_log}.out" || fail "没说停在第几步：$(cat "${red_log}.out")"
grep -q '下一步是 3/6' "${red_log}.out" || fail "没说下一步是哪一步：$(cat "${red_log}.out")"

# --- 3) 最后一步红：不说「下一步」，说这一批没有收尾 ---
red6="${work}/bin-red6"
red6_log="${work}/red6.log"
make_bin "${red6}" 6
: >"${red6_log}"
rc=0
run_land "${red6}" "${red6_log}" || rc=$?
[ "${rc}" = 6 ] || fail "停在关轮时退出码是 ${rc}，不是 6"
grep -q '这一批没有收尾' "${red6_log}.out" \
    || fail "最后一步失败时没有把「这一批没有收尾」说出来：$(cat "${red6_log}.out")"
if grep -q '下一步是' "${red6_log}.out"; then
    fail "最后一步失败还印了「下一步」：后面没有一步"
fi

# --- 4) dry run 只印顺序，不跑任何一步 ---
dry_log="${work}/dry.log"
make_bin "${work}/bin-dry" ""
: >"${dry_log}"
env COGNEVA_LAND_DRY=1 COGNEVA_LAND_BIN="${work}/bin-dry" LAND_LOG="${dry_log}" \
    bash "${fixture}/scripts/hooks/land-batch.sh" >"${dry_log}.out" 2>&1 \
    || fail "dry run 退了非零：$(cat "${dry_log}.out")"
[ -s "${dry_log}" ] && fail "dry run 跑了步骤：$(cat "${dry_log}")"
for n in 1 2 3 4 5 6; do
    grep -q "land-batch:   ${n})" "${dry_log}.out" || fail "dry run 没印出第 ${n} 步：$(cat "${dry_log}.out")"
done

echo "PASS: 落地顺序（六步按序、停在哪一步、下一步是谁、dry run 只印顺序）"
