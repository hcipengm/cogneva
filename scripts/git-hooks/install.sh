#!/usr/bin/env bash
# 把这棵树里的 git 钩子接到当前克隆上。
#
# 钩子的**正文**住在公开仓的 `scripts/git-hooks/`，因为它守的就是这棵树，没有一处
# 需要保密；而 `.git/hooks/` 不会被 clone 带走，所以每个克隆要接线一次，接的就是
# 这一行：
#
#   bash scripts/git-hooks/install.sh
#
# 早先这些脚本住在私有仓、靠手工 `git config core.hooksPath` 指过去。那条路绕，而且
# 让「谁在守这棵树」这件事跟着某台机器的配置走。搬进来之后：正文随 clone 走，接线
# 一条命令，指向的路径由这个仓自己决定。
set -eu

top="$(git rev-parse --show-toplevel)"
hooks="${top}/scripts/git-hooks"

[ -x "${hooks}/pre-commit" ] || chmod +x "${hooks}/pre-commit"
[ -x "${hooks}/pre-push" ] || chmod +x "${hooks}/pre-push"
chmod +x "${hooks}"/pre-commit.d/*.sh 2>/dev/null || true

git -C "${top}" config core.hooksPath "${hooks}"
printf 'git hooks wired: core.hooksPath=%s\n' "${hooks}"
