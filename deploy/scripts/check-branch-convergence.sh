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
# `--single`：另一种问法——**除 trunk 外一条分支头都不许有**，内容在不在 trunk 上不参与
# 判定。默认那条判据只回答「能不能删」，它默认分支可以存在；而分支存在的代价不由它承担：
# 2026-11-10 远端一度有 8 条头（`ahead8`/`ghost9`/`same`/`master`/`store-ledger` 之类），
# 除 `master` 外每条的内容都早已在 main 上——每一条都是先被推上去，然后要有人在 211 笔
# 之后回头用 cherry 逐条证一遍才敢删。一条分支从诞生到被删之间的每一刻，都得有人回答
# 「它能不能删」；不诞生就没人要回答。所以这一档量的是「头有几个」，与内容无关，绿的条件
# 是恰好一个：trunk。
#
# 用法：
#   check-branch-convergence.sh [仓库] [--trunk <名>] [--remote <远端>] [--single]
#
# 退出码：0 每一条非 trunk 分支的内容都已在 trunk（`--single` 时：一条非 trunk 头都没有）；
#         1 至少一条没有（逐条点名）；3 判不了（不是 git 仓库 / trunk 不在 / 远端读不到）。

set -uo pipefail

repo=""
trunk="main"
remote=""
single=0
while [ "$#" -gt 0 ]; do
    case "$1" in
    --trunk) trunk="${2:-}"; shift 2 ;;
    --remote) remote="${2:-}"; shift 2 ;;
    --single) single=1; shift ;;
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
    # 给了 `--remote` 时这一档问的是**远端**的头：本机的分支是各会话的工作状态（工作树
    # 就得站在某条分支上），把它算进「远端只许一个」里，这条判据在每台工作机上都是红的。
    if [ "$single" -eq 1 ] && [ -n "$remote" ]; then continue; fi
    judged=$((judged + 1))
    if [ "$single" -eq 1 ]; then
        # 内容在不在 trunk 上不读：这一档问的是「头有几个」，读内容会把「已合并且该删」
        # 读成合格，而它恰恰是这条规矩要拦的形状。
        uncontained=$((uncontained + 1))
        printf 'RED   - %s：非 %s 分支不许存在（--single）\n' "$branch" "$trunk"
        printf '        修法：先证内容已在 %s，再 git merge --no-edit -s ours %s 记一笔合入，\n' \
            "$trunk" "$branch"
        printf '              最后 git branch -d %s 并向远端推删除（不用 -D）。\n' "$branch"
        continue
    fi
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
        if [ "$single" -eq 1 ]; then
            uncontained=$((uncontained + 1))
            printf 'RED   - %s@%s：远端也不许有非 %s 的头（--single）\n' "$name" "$sha" "$trunk"
            continue
        fi
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
if [ "$single" -eq 1 ]; then
    # 这一档的合格形状就是「零条非 trunk 头」，所以零条在这里是**答案**，不是判不了：
    # 上面那条「零条与全都已收敛同形」说的是默认那一问（它要数的是「还剩几条待收敛」）。
    printf 'check-branch-convergence: single=%s heads=%s——除 %s 外没有别的分支头\n' \
        "$trunk" "$judged" "$trunk"
    exit 0
fi
if [ "$judged" -eq 0 ]; then
    echo "check-branch-convergence: 一条非 $trunk 分支都没有读到——零条与「全都已收敛」在汇总行上同形" >&2
    echo "check-branch-convergence: judged nothing (not the same as clean)" >&2
    exit 3
fi
exit 0
