#!/usr/bin/env bash
# 私有标识：公开树里不许出现私有仓的名字——把私有仓那道判据接到公开仓的推之前。
#
# 为什么要有这一格：判据（私有仓的 `scripts/gates/check-private-references.sh --tree`）只住在
# 私有仓，只在私有仓提交时跑。2026-11-20 实测：公开仓一行注释点了私有仓的名字与它的路径，
# `5a3137db3` 把它推到两个远端，而推之前公开仓这一侧没有任何一层在问这个名字——判据没错，
# 它在错误的路径上。这一格只是一个**位置**：判据仍只有一份，住它原来住的地方。
#
# 与 CI 那道的关系：`.github/scripts/check-private-identifiers.sh` 的所有者句柄那一层是结构
# 判据（推之前这一侧已经回放，见 pre-push 的 CI 步）；它的**裸标识**那一层名单从 CI secret
# 注入，本机没有 secret，那一行字面就印着「这一遍没跑」。这一格补的正是同一件事的本机那一半：
# 名单不靠 secret，从本机私有仓自己的名字推。
#
# 名单为什么不写进这里：私有仓的名字写进公开树，就等于把它要防的东西放进被它守的那棵树里。
# 位置来自机器本地的一份配置 `git config cogneva.privateRoot`，它不在树里，换机器改配置、
# 加一个私有仓也只是多配一条。
#
# 判的是 **HEAD 的树**，不是工作树：推上去的是提交。共享树里别人的在飞改动会让「判工作树」
# 那一版替它红一次，而那种红归不到这一推头上。快照用 `git worktree add --detach` 造，判完即删；
# 判据问被守树的 origin 来豁免它自己的名字，链接工作树答得出来。
#
# 用法：bash deploy/scripts/check-private-names.sh [被守的仓] [rev]
#
# 第一个参数是被守的仓，与这一族里其余的门同一个形状（`bash 门 <root>`）：空树普查
# （`an_empty_tree_is_never_green.test.sh`）会拿一棵什么都没有的树问每一条门，一条
# 不认参数的门的「绿」答的是它自己那棵树，不是被问的那棵。省略时取本脚本所在的那棵。
#
# 退出码：0 干净 / 1 命中（拒推）/ 75 没验。75 与 pre-push 里构建槽那一路是同一个意思：
#   「这一格没判」要记成 UNJUDGED，不能塌成绿——跳过与判过在判词上原本长得一模一样。
set -uo pipefail

# 钩子环境里的 `GIT_DIR` 会把每一条 `git -C <别的树>` 答成正在提交的那一棵（判据文件头记着
# 这条的实测后果）。这里也问别的树，所以先把自己摘出来。
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="${1:-$(git -C "${here}" rev-parse --show-toplevel 2>/dev/null || true)}"
rev="${2:-HEAD}"
if [ -z "${root}" ] || ! git -C "${root}" rev-parse --git-dir >/dev/null 2>&1; then
    printf '私有标识：没验 —— 被守的仓不是一个 git 工作树：%s\n' "${root:-<没有根>}" >&2
    exit 75
fi

# `COGNEVA_PRIVATE_ROOT` 只给夹具用（与私有仓那道判据自己的自检同一个变量）：生产路径从这台
# 机器的 git 配置里问。
private_root="${COGNEVA_PRIVATE_ROOT:-}"
if [ -z "${private_root}" ]; then
    private_root="$(git -C "${root}" config --get cogneva.privateRoot 2>/dev/null || true)"
fi
gate="${private_root:+${private_root%/}/scripts/gates/check-private-references.sh}"
if [ -z "${private_root}" ] || [ ! -f "${gate}" ]; then
    printf '私有标识：没验 —— 这台机器没有私有仓检出。\n' >&2
    printf '  设法：git config cogneva.privateRoot <私有仓的路径>；名单从那个仓自己的名字推，不写进这棵树。\n' >&2
    exit 75
fi

tmp="$(mktemp -d)" || {
    printf '私有标识：没验 —— 建不出临时目录。\n' >&2
    exit 75
}
snap="${tmp}/tree"
cleanup() {
    git -C "${root}" worktree remove --force "${snap}" >/dev/null 2>&1 || rm -rf "${snap}"
    rm -rf "${tmp}"
}
trap cleanup EXIT

if ! git -C "${root}" worktree add --detach --quiet "${snap}" "${rev}" >/dev/null 2>&1; then
    printf '私有标识：没验 —— 造不出 %s 的快照（git worktree add 失败）。\n' "${rev}" >&2
    exit 75
fi

bash "${gate}" --tree "${snap}"
rc=$?
# 判据说「判不了」（3）就是这一格没验；别的非零码照原样传出去——那是红，不该被塌成没验。
if [ "${rc}" -eq 3 ]; then
    exit 75
fi
exit "${rc}"
