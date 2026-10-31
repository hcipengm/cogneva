#!/usr/bin/env bash
# principle: feedback_a_branch_converges_into_the_trunk_only_after_its_content_is_proven_there
#
# 非 trunk 的每一条分支，它的内容必须已经在 trunk 上——否则「这条分支可以删了」是一句
# 没有依据的话，而删掉之后要翻 reflog 才补得回来。
#
# 为什么这不是一道 CI 门：CI 检出的是**一条**分支（GitHub Actions 给的是 detached HEAD），
# 那里根本没有别的分支可判，扫出来的「零条不合格」与「没有任何分支」同形。这条判据的对象
# 是本机 / 远端的工作副本——收敛分支的那一刻才有人跑它。所以它是一把量尺，而不是流水线上
# 的一步；量出来的形状由 `deploy/scripts/tests/branch-convergence.test.sh` 逐格钉住。
#
# 判据是两条腿，任一成立即「内容已在 trunk」：
#   1. `git rev-list --count <trunk>..<branch>` 为 0——分支上没有 trunk 没有的提交；
#   2. `git cherry <trunk> <branch>` 里没有 `+` 开头的补丁——有提交，但每一笔的补丁都已
#      在 trunk 上（2026-10-23 那 8 条里 3 条就是这一档：直接 merge 会在三处冲突，而冲突
#      是「同一件事的两种写法」）。
# 两条都不成立的分支，修法是先证内容、再 `git merge --no-edit -s ours <branch>` 记一笔合入、
# 最后 `git branch -d`——`-d` 自带零丢失判据，`-D` 没有。
#
# 远端分支（`--remote <name>`）没有本地对象可走上面两条腿，只能判「它的 tip 是不是 trunk
# 的祖先」。查不到对象时按「判不了」报，不按「合格」——读不到与合格是两件事。
#
# 用法：
#   check-branch-convergence.sh [仓库] [--trunk <名>] [--remote <远端>]
#
# 退出码：0 每一条非 trunk 分支的内容都已在 trunk；1 至少一条没有（逐条点名）；
#         3 判不了（不是 git 仓库 / trunk 不在 / 远端读不到）。

set -uo pipefail

repo=""
trunk="main"
remote=""
while [ "$#" -gt 0 ]; do
    case "$1" in
    --trunk) trunk="${2:-}"; shift 2 ;;
    --remote) remote="${2:-}"; shift 2 ;;
    -h | --help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "check-branch-convergence: 不认识的参数 $1" >&2; exit 2 ;;
    *) repo="$1"; shift ;;
    esac
done
repo="${repo:-.}"

if ! git -C "$repo" rev-parse --git-dir >/dev/null 2>&1; then
    echo "check-branch-convergence: 不是一个 git 仓库：$repo" >&2
    echo "check-branch-convergence: judged nothing (not the same as clean)" >&2
    exit 3
fi
if ! git -C "$repo" rev-parse --verify --quiet "refs/heads/$trunk" >/dev/null; then
    echo "check-branch-convergence: 分支 $trunk 不在，判不了哪一条是 trunk" >&2
    echo "check-branch-convergence: judged nothing (not the same as clean)" >&2
    exit 3
fi

judged=0
uncontained=0
unknown=0

unmerged_count() { # $1=分支名，印出未上游补丁的条数；读不动印 `?`
    local n
    # `grep -c` 在零匹配时既印 0 又退 1——零条「未上游」恰恰是合格，
    # 因此这里必须 `|| true`，不能让它把合格读成不合格。
    n="$(git -C "$repo" cherry "$trunk" "$1" 2>/dev/null | grep -c '^+' || true)"
    printf '%s' "${n:-?}"
}

contained() { # $1=分支名，成功即「内容已在 trunk」
    local ahead unmerged
    ahead="$(git -C "$repo" rev-list --count "$trunk..$1" 2>/dev/null)" || return 1
    [ "$ahead" = 0 ] && return 0
    unmerged="$(unmerged_count "$1")"
    [ "$unmerged" = 0 ]
}

while IFS= read -r branch; do
    [ -z "$branch" ] && continue
    [ "$branch" = "$trunk" ] && continue
    judged=$((judged + 1))
    if contained "$branch"; then
        ahead="$(git -C "$repo" rev-list --count "$trunk..$branch")"
        printf 'ok    - %s：ahead=%s 未上游补丁=0\n' "$branch" "$ahead"
    else
        ahead="$(git -C "$repo" rev-list --count "$trunk..$branch" 2>/dev/null || echo '?')"
        unmerged="$(unmerged_count "$branch")"
        uncontained=$((uncontained + 1))
        printf 'RED   - %s：ahead=%s 未上游补丁=%s\n' "$branch" "$ahead" "$unmerged"
        printf '        修法：先证内容已在 %s（git cherry / 标识符 grep 到具体行号），再\n' "$trunk"
        printf '              git merge --no-edit -s ours %s，最后 git branch -d %s（不用 -D）\n' \
            "$branch" "$branch"
    fi
done < <(git -C "$repo" for-each-ref refs/heads --format='%(refname:short)')

if [ -n "$remote" ]; then
    listing="$(git -C "$repo" ls-remote --heads "$remote" 2>/dev/null)" || {
        echo "check-branch-convergence: 读不到远端 $remote" >&2
        echo "check-branch-convergence: judged nothing (not the same as clean)" >&2
        exit 3
    }
    while IFS=$'\t' read -r sha ref; do
        [ -z "${ref:-}" ] && continue
        name="${ref#refs/heads/}"
        [ "$name" = "$trunk" ] && continue
        judged=$((judged + 1))
        if ! git -C "$repo" cat-file -e "$sha^{commit}" 2>/dev/null; then
            unknown=$((unknown + 1))
            printf 'UNKNOWN - %s@%s：本地没有这个对象，判不了它是不是 %s 的祖先\n' \
                "$name" "$sha" "$trunk"
            continue
        fi
        if git -C "$repo" merge-base --is-ancestor "$sha" "refs/heads/$trunk" 2>/dev/null; then
            printf 'ok    - %s@%s：是 %s 的祖先\n' "$name" "$sha" "$trunk"
        else
            uncontained=$((uncontained + 1))
            printf 'RED   - %s@%s：不是 %s 的祖先\n' "$name" "$sha" "$trunk"
        fi
    done <<<"$listing"
fi

printf 'check-branch-convergence: branches=%s uncontained=%s unknown=%s\n' \
    "$judged" "$uncontained" "$unknown"
if [ "$uncontained" -gt 0 ]; then exit 1; fi
if [ "$unknown" -gt 0 ]; then exit 3; fi
if [ "$judged" -eq 0 ]; then
    echo "check-branch-convergence: 一条非 $trunk 分支都没有读到——零条与「全都已收敛」在汇总行上同形" >&2
    echo "check-branch-convergence: judged nothing (not the same as clean)" >&2
    exit 3
fi
exit 0
