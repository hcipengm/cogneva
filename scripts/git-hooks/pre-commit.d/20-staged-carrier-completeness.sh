#!/usr/bin/env bash
# Refuse a commit whose staged deployment carrier is incomplete.
#
# Why at the commit: `deploy/k3s/` is the chart, once rendered and then
# hand-synced, so a change to the chart has an answer in a dozen other files.
# `git commit -- <paths>` carries only the paths it is given, and every gate that
# checks the carrier reads the **working tree**: where the generated files were
# just regenerated and therefore always agree. The commit is the thing CI reads,
# and CI is the first place an omission shows. On 2026-10-23 a limit change went
# out with three rendered configmaps and three rendered deployments left behind,
# and five CI jobs went red on a tree that was correct on disk. This asks the
# same question at the one point where the answer is still free.
#
# `deploy/rendered/` is no longer part of the question: it is not carried at all,
# every reader renders it from the chart at the moment it reads (see
# deploy/scripts/lib/rendered-cache.sh), so there is no copy that can be left
# behind.
#
# Two questions, each one a way the carrier can disagree with its chart:
#   * check-config-checksum-wiring -- the fingerprints follow the config they hash
#   * check-deploy-parity.sh      -- the static k3s manifests match the k3s profile
# Both read the assembled tree, and both are the same ones the deploy-parity CI
# job runs, so a green here means that job's verdict would be green too -- not
# "the scripts somebody remembered".
#
# Judged as the commit will hold it: the tree is assembled from the index, not
# from the working tree, so an edit a peer has not staged is not judged and does
# not refuse a commit it is no part of. Everything outside the carrier
# directories comes from HEAD; those directories come from the index, which is
# what a commit from this index would carry for them -- including deletions.
#
# The three check scripts come from HEAD with everything else, and that is on
# purpose: a commit must not be able to rewrite the tool that judges it in the
# same commit that it wants judged. A change to deploy/scripts/ is judged by CI
# like any other code.
#
# Cheap when it does not apply: if nothing under those directories is staged, the
# check exits before it copies anything. A carrier change costs a tree copy
# (about 30 MB here) and one render.
#
# Escape hatch: "cannot judge" never blocks. No helm on this host, no python3, a
# render that fails for a reason other than staleness, an index that cannot be
# read -- each says so and exits 0, because a machine that cannot answer is not a
# finding about the commit, and CI still governs what was pushed.
#
# Exit 0 means the carrier the commit would carry agrees with the chart in the
# same commit. Exit 1 means it does not.
set -u

repo="$(env -u GIT_DIR -u GIT_WORK_TREE git rev-parse --show-toplevel 2>/dev/null)" || exit 0
[ -n "${repo:-}" ] || exit 0
cd "$repo" || exit 0

carrier_dirs="deploy/helm deploy/k3s"

staged="$(git diff --cached --name-only -- $carrier_dirs 2>/dev/null)" || exit 0
[ -n "$staged" ] || exit 0

not_judged() {
    printf 'pre-commit（生成物载体）: 这一条今天判不了，不拦 —— %s\n' "$1" >&2
    exit 0
}

command -v helm >/dev/null 2>&1 || not_judged "PATH 里没有 helm"
command -v python3 >/dev/null 2>&1 || not_judged "PATH 里没有 python3"
[ -f deploy/scripts/render-deploy.sh ] || not_judged "这棵树里没有 render-deploy.sh"

work="$(mktemp -d)" || not_judged "建不了临时目录"
trap 'rm -rf "$work"' EXIT
tree="$work/tree"
mkdir -p "$tree" || not_judged "建不了临时目录"

# HEAD 全树，然后用**索引**覆盖载体目录：索引就是这一笔会带走的字节。
git archive HEAD 2>/dev/null | tar -x -C "$tree" 2>/dev/null ||
    not_judged "取不出 HEAD 的树"
git ls-files -z -- $carrier_dirs 2>/dev/null | xargs -0 -r git checkout-index -f --prefix="$tree/" 2>/dev/null ||
    not_judged "取不出索引里的载体字节"
# 索引里删掉的、HEAD 里还在的文件：这一笔提交里它们不存在。
comm -23 \
    <(git ls-tree -r --name-only HEAD -- $carrier_dirs 2>/dev/null | sort) \
    <(git ls-files -- $carrier_dirs 2>/dev/null | sort) |
    while IFS= read -r gone; do
        [ -n "$gone" ] && rm -f "$tree/$gone"
    done

checksum_out="$(cd "$tree" && bash deploy/scripts/check-config-checksum-wiring.sh 2>&1)"
checksum_rc=$?

parity_out="$(cd "$tree" && bash deploy/scripts/check-deploy-parity.sh 2>&1)"
parity_rc=$?

[ "$checksum_rc" -eq 0 ] && [ "$parity_rc" -eq 0 ] && exit 0

# 判不了与判红是两件事。每道核对各印一句能认出来的判词，认不出就是跑不起来。
checksum_stale=0
parity_stale=0
if [ "$checksum_rc" -ne 0 ]; then
    case "$checksum_out" in
    *"CONFIG CHECKSUM WIRING FAILED"*) checksum_stale=1 ;;
    *) not_judged "check-config-checksum-wiring.sh 跑不起来：$(printf '%s' "$checksum_out" | tail -1)" ;;
    esac
fi
if [ "$parity_rc" -ne 0 ]; then
    case "$parity_out" in
    *"PARITY 校验失败"*) parity_stale=1 ;;
    *) not_judged "check-deploy-parity.sh 跑不起来：$(printf '%s' "$parity_out" | tail -1)" ;;
    esac
fi
[ "$checksum_stale" -eq 1 ] || [ "$parity_stale" -eq 1 ] ||
    not_judged "两道核对都没有给出判词"

{
    printf 'pre-commit（生成物载体）: 这一笔提交里的载体与它自己的 chart 对不上。\n'
    printf '  这些文件是脚本从 deploy/helm 生成的；本机的门读工作树，那里刚生成过所以一定\n'
    printf '  对得上，而 commit 只带走点名的路径——CI 读的是提交，那一头会红。\n'
    printf '  修法：chart 变了就重跑 `bash deploy/scripts/sync-static-manifests.sh`（落配置正文\n'
    printf '  与指纹），并把 deploy/k3s 那一侧按渲染结果同步（parity 比的是它），一起暂存。\n'
    if [ "$checksum_stale" -eq 1 ]; then
        printf '%s\n' "$checksum_out" | grep -E 'FAILED|^  - ' | head -8
    fi
    if [ "$parity_stale" -eq 1 ]; then
        printf '%s\n' "$parity_out" | sed -e "s|$tree/||g" |
            grep -E 'PARITY 校验失败|^  - |^  [^ ]' | head -10
    fi
    printf '  （绝对前缀已隐去：「这一笔」是这次提交里的）\n'
} >&2
exit 1
