#!/usr/bin/env bash
# Deterministic gate for "a build artifact never enters the work-tree delta".
#
# The failure this pins is silent and expensive. The pre-push gate judges the
# work tree and stamps a patch key computed over `git add -A`; the push hook asks
# for the key of the committed bytes. Anything `git add -A` picks up that the
# commit will not carry makes those two keys different, so a green two-hour run
# cannot be claimed by the push that follows it -- and nothing anywhere says why.
# Measured 2026-11-10: `web/node_modules` is a symlink into the main checkout in a
# git worktree, and `.gitignore` said `node_modules/`. The trailing slash matches a
# directory only, so the symlink read as an untracked file: the run judged seven
# files plus that link, the commit carried the seven, and the recorded key could
# never be recomputed.
#
# The fixture reads the repository's own `.gitignore`, so a rule that regresses to
# the trailing-slash form fails here rather than after the next long run. Three
# shapes of the same artifact are measured against a tree with none of it:
#   - absent                 -- the baseline every other shape must equal;
#   - a real directory        -- the shape the slash did cover;
#   - a symlink to a directory outside the tree -- the shape it did not.
# One counterexample keeps the rule honest in the other direction: a source file
# whose name merely starts with the pattern (`web/node_modules.ts`) must still be
# tracked, so `node_modules*` would fail here too.
set -euo pipefail
# A fixture repository is written through `git -C <dir>`, which changes the
# starting worktree but not the repository: an absolute `GIT_DIR` /
# `GIT_WORK_TREE` / `GIT_INDEX_FILE` handed down by the commit lander, a commit
# hook or a snapshot makes every call below write into the caller's repository
# instead. Clear it once, at file scope.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${repo}/.gitignore" ] || fail "${repo}/.gitignore 不存在，这一格判不了"

work="$(mktemp -d "${TMPDIR:-/tmp}/cogneva-ignore.XXXXXX")"
trap 'rm -rf "${work}"' EXIT

scratch="${work}/tree"
mkdir -p "${scratch}/web"
git -C "${scratch}" init -q
git -C "${scratch}" config user.email fixture@example.invalid
git -C "${scratch}" config user.name fixture
git -C "${scratch}" config commit.gpgsign false
# The repository's own rules, byte for byte: what is under test is this file.
cp "${repo}/.gitignore" "${scratch}/.gitignore"
printf 'export const a = 1;\n' >"${scratch}/web/a.ts"
git -C "${scratch}" add -A
git -C "${scratch}" commit -qm base

# What `git add -A` would put in the delta, as a tree id. The index is reset from
# HEAD first so one shape's leftovers cannot make the next shape look clean.
tree_of() { ( cd "${scratch}" && git read-tree HEAD && git add -A && git write-tree ); }

# 1. absent
t_none="$(tree_of)"

# 2. a real directory holding something
mkdir -p "${scratch}/web/node_modules/react"
printf '{}\n' >"${scratch}/web/node_modules/react/package.json"
t_dir="$(tree_of)"
[ "${t_dir}" = "${t_none}" ] || fail "真目录形态的 node_modules 进了工作树差值（这棵树 ${t_none:0:12}，带上它 ${t_dir:0:12}）"

# 3. a symlink to a directory outside the tree -- the shape the trailing slash missed
rm -rf "${scratch}/web/node_modules"
mkdir -p "${work}/outside/react"
printf '{}\n' >"${work}/outside/react/package.json"
ln -s "${work}/outside" "${scratch}/web/node_modules"
[ -L "${scratch}/web/node_modules" ] || fail "夹具没造出符号链接，这一格判不了"
t_link="$(tree_of)"
[ "${t_link}" = "${t_none}" ] || fail "符号链接形态的 node_modules 进了工作树差值（这棵树 ${t_none:0:12}，带上它 ${t_link:0:12}）"
rm -f "${scratch}/web/node_modules"

# Counterexample: a name that merely starts with the pattern is source, not artifact.
printf 'export const b = 2;\n' >"${scratch}/web/node_modules.ts"
t_src="$(tree_of)"
[ "${t_src}" != "${t_none}" ] || fail "web/node_modules.ts 被一起忽略了：规则太宽，连正常的源文件都不认了"
git -C "${scratch}" ls-tree -r --name-only "${t_src}" | grep -qx 'web/node_modules.ts' \
    || fail "web/node_modules.ts 不在写出的树里，反例没成立"
rm -f "${scratch}/web/node_modules.ts"

echo "OK: 构建产物不论真目录还是符号链接都不进工作树差值，名前缀相同的源文件照样进"
