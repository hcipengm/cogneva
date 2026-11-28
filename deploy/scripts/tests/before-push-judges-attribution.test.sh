#!/usr/bin/env bash
# principle: feedback_a_face_the_gate_cannot_replay_is_fixed_by_making_it_replayable
# 「推上去的提交不带 AI 署名」在推前门里是**判过的**，不是「本机没验」。
#
# 这条判据以前只有 CI 跑（`.github/workflows/ci.yml` 的 `Check commit messages carry
# no AI attribution`），而推前门的判词一直把这一步列在「另有 N 面本机没验」里。起因不是
# 判据跑不了，是那一步写成了多行脚本块——门只回放单行 `run: <解释器> <路径>`，跳过的
# 那一步又被诚实地印进「没验」名单。门里另写一条专用调用能补上判，但补不掉那句话：
# 上一行印着「没验」，下一行又判了一遍，判词自相矛盾。
#
# 所以判五件事，每一件都有一条会在空输入上通过的守卫：
#   1. CI 那一步是**单行**、解释器加路径——门回放得动它（多行块 ⇒ 这一面又回到「没验」）；
#   2. 门里没有一条点名「署名」的 skip——有它就意味着这一步又回放不动了；
#   3. 脚本按事件（CI）与本机两种口径各算各的范围，且判据在两种口径下都会红；
#   4. 本机「没有输入」的两支（没有 origin/main、范围为空）说出**没判**，而不是绿，
#      也不是把判据对空范围的拒判读成红；
#   5. 给了根的调用（全树普查）读不到范围时**退非零**——那一面的绿不许不依赖它读的东西。
set -uo pipefail

# 夹具要建临时仓。外面递进来的 `GIT_DIR` 之类的绝对路径会让下面每一条 git 命令写进
# **调用方的仓**（2026-11-07 实测：夹具的 `config` 落进了调用方），所以整份清掉；
# 身份也让夹具自己给，不去读机器上那位提交人的配置。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
unset GITHUB_ACTIONS BASE_SHA HEAD_SHA
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
export GIT_AUTHOR_NAME=fixture GIT_AUTHOR_EMAIL=fixture@example.invalid
export GIT_COMMITTER_NAME=fixture GIT_COMMITTER_EMAIL=fixture@example.invalid

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scripts="$(cd "${here}/.." && pwd)"
repo="$(cd "${scripts}/../.." && pwd)"
gate="${scripts}/check-before-push.sh"
entry="${scripts}/check-commit-attribution-ci.sh"
checker="${scripts}/check-commit-attribution.sh"
ci="${repo}/.github/workflows/ci.yml"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"
[ -x "${entry}" ] || fail "范围脚本不存在或不可执行：${entry}"
[ -x "${checker}" ] || fail "判据脚本不存在或不可执行：${checker}"
[ -f "${ci}" ] || fail "CI 定义不存在：${ci}"

STEP='Check commit messages carry no AI attribution'

# 1. CI 那一步单行、解释器加路径。取**这一步自己的块**（名字到下一个 `- name:`），
#    而不是全文找：别的步骤写成什么样与这条判据无关，全文找会让一处无关的多行块
#    替这一步背红。
block="$(awk -v name="${STEP}" '
    index($0, "- name: " name) { inside = 1; print; next }
    inside && /^[[:space:]]*- name:/ { exit }
    inside { print }
' "${ci}")"
[ -n "${block}" ] || fail "ci.yml 里找不到「${STEP}」这一步：名字改了，门的回放与这里的判据都会安静地落空"

if printf '%s\n' "${block}" | grep -qE '^\s*run: \|'; then
    fail "「${STEP}」写成了多行脚本块：门只回放单行，这一面又回到「本机没验」的名单里"
fi
if ! printf '%s\n' "${block}" | grep -qE '^\s*run: (bash|python3|sh) \S'; then
    fail "「${STEP}」的 run: 不是「解释器 加 路径」：门回放不了这种写法。实际：
${block}"
fi
if ! printf '%s\n' "${block}" | grep -qE '^\s*run: (bash|python3|sh) \S*check-commit-attribution-ci\.sh\s*$'; then
    fail "「${STEP}」没有调 check-commit-attribution-ci.sh：范围计算若留在 YAML 里，这一步就得是多行块"
fi

# 2. 门里没有点名「署名」的 skip。有它就是「这一步回放不了，门替它说没验」的老形状。
if awk '!/^[[:space:]]*#/' "${gate}" | sed '/^[[:space:]]*$/d' | grep -E 'skip_step' | grep -q '署名'; then
    fail "门里有一条点名「署名」的 skip：说明 CI 那一步又回放不动了，判词会与判过的结果自相矛盾"
fi

# 3/4. 脚本本身：两套口径、三种输入。每个用例都在临时仓里跑，脚本读的是当前目录的仓。
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
cd "${tmp}" || exit 1
git init -q -b main .
printf 'a\n' > a.txt
git add a.txt
git commit -qm 'baseline'
git tag base

# 不带署名的干净提交
printf 'b\n' > b.txt
git add b.txt
git commit -qm 'Land the clean thing'
clean="$(git rev-parse HEAD)"

# 带署名的提交
printf 'c\n' > c.txt
git add c.txt
git commit -qm "$(printf 'Land the thing\n\nCo-Authored-By: Claude <noreply@anthropic.com>')"
attributed="$(git rev-parse HEAD)"

run_entry() { # run_entry <期望退出码> <说明> [环境变量…]
    local want="$1" what="$2"; shift 2
    local out rc
    out="$(env "$@" bash "${entry}" 2>&1)"; rc=$?
    if [ "${rc}" -ne "${want}" ]; then
        printf 'FAIL: %s：期望退出 %s，实际 %s\n%s\n' "${what}" "${want}" "${rc}" "${out}" >&2
        exit 1
    fi
    printf '%s' "${out}"
}

# CI 口径：按事件给的两端判，带署名那一笔必须红，干净范围必须绿。
out="$(run_entry 1 'CI 口径（base..head 带署名）' GITHUB_ACTIONS=true BASE_SHA="${clean}" HEAD_SHA="${attributed}")"
grep -q 'judging' <<<"${out}" || fail "CI 口径没有印出它判的范围：读不出判了哪一段与没判同形"
run_entry 0 'CI 口径（干净范围）' GITHUB_ACTIONS=true BASE_SHA="$(git rev-parse base)" HEAD_SHA="${clean}" >/dev/null
# 新建分支：before 全零，判据拿到的是 HEAD 可达的全部，仍然红。
run_entry 1 'CI 口径（before 全零）' GITHUB_ACTIONS=true BASE_SHA='0000000000000000000000000000000000000000' HEAD_SHA="${attributed}" >/dev/null 2>&1
# CI 里范围算不出来（两个都空）：判据拒判（2），脚本不替它放行。
run_entry 2 'CI 口径（两个 sha 都空）' GITHUB_ACTIONS=true >/dev/null 2>&1

# 本机口径：退回 origin/main..HEAD。
git update-ref refs/remotes/origin/main "${clean}"
out="$(run_entry 1 '本机口径（origin/main..HEAD 带署名）')"
grep -q 'SKIP' <<<"${out}" && fail "本机口径在有提交要判时印了 SKIP：那一支不该走到"

# 干净但不空：判过且绿，而不是靠 SKIP 变绿——两种绿必须分得开。
git checkout -q -b localclean "${clean}"
printf 'd\n' > d.txt
git add d.txt
git commit -qm 'Land another clean thing'
run_entry 0 '本机口径（干净且非空）' >/dev/null
git checkout -q main

git update-ref refs/remotes/origin/main "${attributed}"
out="$(run_entry 0 '本机口径（范围为空）')"
grep -q 'SKIP' <<<"${out}" || fail "本机口径（范围为空）没有说出「没判」：空范围的绿与判过的绿同形。
${out}"
grep -q '没有提交' <<<"${out}" || fail "本机口径（范围为空）的 SKIP 没有说清是没有提交：
${out}"

# 没有 origin/main：同样说出没判。
git update-ref -d refs/remotes/origin/main
out="$(bash "${entry}" 2>&1)"; rc=$?
[ "${rc}" -eq 0 ] || fail "本机口径（没有 origin/main）期望退出 0，实际 ${rc}：
${out}"
grep -q 'origin/main' <<<"${out}" || fail "本机口径（没有 origin/main）的 SKIP 没有点名缺的是什么：
${out}"

# 给了根（全树普查的调用方式）：读不到就**不许绿**。这条是被
# `an_empty_tree_is_never_green.test.sh` 判出来之后补的——那一轮整门只有这一格红，
# 说的是同一句话：它的绿不依赖它自称读的那份东西。钉在这里，下一回不必等整门跑完。
empty="$(mktemp -d "${tmp}/empty.XXXXXX")"
out="$(bash "${entry}" "${empty}" 2>&1)"; rc=$?
[ "${rc}" -ne 0 ] || fail "一棵空树上退 0（绿不依赖它读的东西）：空范围 / 没有 origin/main 都不是绿"

# 给了根且读得到：走向与不带根相同，只是判的是那棵树。
git update-ref refs/remotes/origin/main "${clean}"
out="$(bash "${entry}" "${tmp}" 2>&1)"; rc=$?
[ "${rc}" -eq 1 ] || fail "给了根、origin/main..HEAD 带署名，期望退出 1，实际 ${rc}：
${out}"

git update-ref refs/remotes/origin/main "${attributed}"
out="$(bash "${entry}" "${tmp}" 2>&1)"; rc=$?
[ "${rc}" -ne 0 ] || fail "给了根、范围为空，期望非零（看不了 ≠ 绿），实际退 0：
${out}"

echo "OK：署名判据在 CI 与本机两种口径下都判，CI 那一步回放得动，没输入时说出没判，给了根就不许绿"
