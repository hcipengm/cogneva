#!/usr/bin/env bash
# 一批落地的顺序，作为树上的一条跑法。
#
# 每一步早就有自己的载体：`round.py`（钉与关）、`batch-gate.py`（这一批做完了没有）、
# `deploy/scripts/check-before-push.sh`（推前门）。缺的从来不是某一步，是**顺序**——
# 哪一步在前、哪一步没绿就停在哪里、停在那一格时下一步是哪一步。顺序只活在跑它的人
# 的记性里时，换一个人接手就要凭「我做过」重来一遍（CC30）。
#
# 顺序（翻行与提交台账在此之前：那两步是编辑与提交，跑法不替人写台账）：
#
#   1 台账已提交、工作树干净      没有提交的改动不会被推上去，先拦
#   2 重钉                        批次由这一轮钉住的那版台账定，台账变了要重钉
#   3 批门                        这一批的每一行要么做完，要么按两条理由点名
#   4 推前门                      在**要推的那棵树上**判，绿了才允许推
#   5 双推                        每个远端推到同一个哈希，比对后印出来
#   6 补记双推                    把 5 的两个哈希写回台账，再推一次台账
#   7 关轮                        全部做完才能关
#
# 任何一步没绿就停在该步，并把「下一步是什么」印出来。退出码就是那一步的退出码，
# 所以 `land-batch.sh; echo $?` 读得出是停在 3 还是 5：一门一码比一句话更难被误读。
#
# 台账、远端、轮的坐标都从树里算，不在这里各抄一份：
#   - 台账路径 = `todo_source.default_spec()`；
#   - 台账那个仓 = 台账文件所在目录的 git 顶层；
#   - 远端 = `git remote` 自己列的，不点名。
#
# COGNEVA_LAND_DRY=1 只把顺序印出来，不跑任何一步——顺序本身要能读，才谈得上接手。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/../.." && pwd)"
cd "${root}" || exit 1

drv="${COGNEVA_LAND_DRY:-0}"
fail_rc=0

say() { printf 'land-batch: %s\n' "$*"; }

# 一步：名字在前，命令在后。跑了什么、停在哪一步、下一步是什么，都由这一处决定，
# 不在下面各段里各写一遍（第二个副本就是会和第一个分叉的那一份）。
#
# COGNEVA_LAND_BIN 是一条只给测试用的缝：设了它，每一步跑的是那目录下的
# `<序号>-<短名>`，而不是真命令。没有它，这条跑法只能在真机器上验一次顺序；
# 有了它，顺序、停在哪一步、下一步是谁，都能在夹具里判。它不是旋钮——生产路径
# 上不设它，也没有默认值。
step_no=0
step_name=()
step_cmd=()
add_step() { # <短名> <名字> <命令…>
    local slug="$1" name="$2"
    shift 2
    step_no=$((step_no + 1))
    step_name+=("${name}")
    if [ -n "${COGNEVA_LAND_BIN:-}" ]; then
        step_cmd+=("${COGNEVA_LAND_BIN}/${step_no}-${slug}")
    else
        step_cmd+=("$*")
    fi
}

ledger_spec="$(python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); import todo_source; print(todo_source.default_spec())' "${here}" 2>/dev/null || true)"
[ -n "${ledger_spec}" ] || ledger_spec="${COGNEVA_TODO_LEDGER:-}"
ledger_path="${ledger_spec#md:}"
ledger_repo=""
if [ -n "${ledger_path}" ] && [ -d "${ledger_path}" ]; then
    ledger_repo="$(git -C "${ledger_path}" rev-parse --show-toplevel 2>/dev/null || true)"
fi

add_step rebound "重钉（这一轮钉住现在这版台账）" \
    "python3 ${here}/round.py rebound"
add_step gate "批门（这一批做完了没有）" \
    "python3 ${here}/batch-gate.py --root ${root} --base origin/main"
add_step prepush "推前门（推之前判这一棵树）" \
    "bash ${root}/deploy/scripts/check-before-push.sh"
add_step push "双推（每个远端同一个哈希）" \
    "push_every_remote"
add_step ledger "补记双推（把两个哈希写回台账并推台账）" \
    "record_and_push_ledger"
add_step close "关轮（全部做完才关）" \
    "python3 ${here}/round.py close"

# 第 0 步不是步骤表里的一条：它不是「这一批做完了没有」的一部分，是后面每一步的
# 前提。放进表里会让「停在 0」与「停在 1」看起来是同一种失败。
dirty="$(git status --porcelain 2>/dev/null || true)"
if [ -n "${dirty}" ]; then
    say "先停：工作树不干净。没提交的改动不会被推上去，而推前门判的是要推的那棵树。"
    printf '%s\n' "${dirty}" | sed 's/^/land-batch:   /'
    exit 1
fi

# 推之前先脱开：`origin/main` 比本地新时，推前门判的那棵树不是要推的那一棵。
git fetch --quiet origin 2>/dev/null || true
ahead="$(git rev-list --count origin/main..HEAD 2>/dev/null || printf '0')"
behind="$(git rev-list --count HEAD..origin/main 2>/dev/null || printf '0')"
if [ "${behind}" -gt 0 ]; then
    say "先停：本地比 origin/main 落后 ${behind} 个提交。先把它接上，再跑这一批——"
    say "推前门判的是树，落后的树不是要推的那一棵。"
    exit 1
fi
if [ "${ahead}" -eq 0 ]; then
    say "先停：没有要推的提交（HEAD 与 origin/main 同哈希）。这一批在哪里，先确认。"
    exit 1
fi

if [ "${drv}" = 1 ]; then
    say "顺序（dry run，不跑任何一步）："
    i=0
    while [ "${i}" -lt "${step_no}" ]; do
        printf 'land-batch:   %d) %s\n' "$((i + 1))" "${step_name[${i}]}"
        i=$((i + 1))
    done
    exit 0
fi

# 双推：远端由 `git remote` 决定，不点名。两个远端推到同哈希才算双推——一个推过头、
# 另一个没推上去，读 `git rev-parse` 的人才看得出，读日志的人看不出来。
push_every_remote() {
    local head rc=0 r
    head="$(git rev-parse HEAD)"
    for r in $(git remote); do
        if ! git push "${r}" HEAD:main 2>&1 | sed 's/^/land-batch:   /'; then
            say "远端 ${r} 没推上去"
            rc=1
            continue
        fi
        local seen
        seen="$(git ls-remote "${r}" refs/heads/main 2>/dev/null | cut -f1 || true)"
        if [ "${seen}" != "${head}" ]; then
            say "远端 ${r} 的 main 是 ${seen:-读不到}，不是 ${head}"
            rc=1
        fi
    done
    return "${rc}"
}

# 补记双推：哈希由这次推的结果现取，不从别处抄。写进台账的「变更记录」，再推台账
# 那个仓的每个远端——台账自己也要到两个地方，才谈得上别人读得到。
record_and_push_ledger() {
    local head day
    head="$(git rev-parse --short=10 HEAD)"
    day="$(date +%F)"
    if [ -z "${ledger_path}" ] || [ ! -f "${ledger_path}" ]; then
        say "台账读不到（${ledger_spec:-未指定}）：双推这一笔没有落到任何地方，记在这里：${day} ${head}"
        return 0
    fi
    printf '%s\n' "- ${day}：双推 \`${head}\`（每个远端同哈希；本跑由 \`scripts/hooks/land-batch.sh\` 按序跑完）。" \
        >>"${ledger_path}"
    [ -n "${ledger_repo}" ] || return 0
    git -C "${ledger_repo}" add -- "${ledger_path}" || return 1
    git -C "${ledger_repo}" commit -q -m "Record the double push of ${head}" -- "${ledger_path}" || return 1
    local r
    for r in $(git -C "${ledger_repo}" remote); do
        git -C "${ledger_repo}" push "${r}" HEAD:main 2>&1 | sed 's/^/land-batch:   /' || return 1
    done
    return 0
}

i=0
while [ "${i}" -lt "${step_no}" ]; do
    say "$((i + 1))/${step_no} ${step_name[${i}]}"
    if ! eval "${step_cmd[${i}]}"; then
        fail_rc=$((i + 1))
        say "停在 ${fail_rc}/${step_no}：${step_name[${i}]}"
        if [ "$((i + 1))" -lt "${step_no}" ]; then
            say "下一步是 $((i + 2))/${step_no}：${step_name[$((i + 1))]}（修好这一步再跑本脚本，它会从头判，前几步已经是绿的）"
        else
            say "这是最后一步；它没绿 = 这一批没有收尾。"
        fi
        exit "${fail_rc}"
    fi
    i=$((i + 1))
done
say "全部 ${step_no} 步绿：这一批推上去了，轮关了。"
