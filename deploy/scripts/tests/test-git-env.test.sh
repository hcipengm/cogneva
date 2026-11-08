#!/usr/bin/env bash
# A test that builds a scratch repository must not write into the caller's.
#
# Why this is a gate and not a convention: `git -C <dir>` changes the starting
# worktree, not the repository. The commit lander, a commit hook and the fast
# gate all hand their children an absolute `GIT_DIR` / `GIT_WORK_TREE` /
# `GIT_INDEX_FILE`, so a fixture's `git config user.name t` or `git commit`
# lands in whichever repository invoked the suite. Measured 2026-11-07 against
# this directory: running `branch-convergence.test.sh` with an absolute GIT_DIR
# exported wrote `user.name=t` and `core.hooksPath=/dev/null` into the caller's
# config and judged 16 cases against the wrong tree. That is the shape of the
# six `t <t@example.com>` commits of 2026-10-27 and of the shared repository
# that had every hook switched off, and it is silent: the fixture still reports
# its own green cases.
#
# What it judges:
#   * every `deploy/scripts/tests/*.test.sh` that creates a repository clears
#     the git environment once, at file scope (`unset ... GIT_DIR ...
#     GIT_WORK_TREE ... GIT_INDEX_FILE ...`). File scope, not per call: a later
#     case added to the same file is covered without anyone remembering.
#   * the guard is what protects, in both directions: a guarded fixture leaves
#     the caller's config alone under an exported GIT_DIR, and the same fixture
#     with its unset line removed writes `t` into that config.
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
tests_dir="${repo}/deploy/scripts/tests"

pass=0
fail=0
ok() { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 仓是这一族文件的标志：建仓的才会拿 `-C` 去写自己的夹具。
creates_repo() { grep -qE '(^|[[:space:]])git([[:space:]]+[^[:space:]]+)*[[:space:]]init([[:space:]]|$)' "$1"; }
guarded() { grep -qE '^unset .*GIT_DIR.*GIT_WORK_TREE.*GIT_INDEX_FILE' "$1"; }

unguarded() { # $1 = 目录：印出「建仓但没有文件级 unset」的文件，一个一行
    local f
    for f in "$1"/*.test.sh; do
        [ -f "$f" ] || continue
        creates_repo "$f" || continue
        guarded "$f" && continue
        printf '%s\n' "$f"
    done
}

# ---- 1. 真树上每一个建仓的测试都清过 git 环境 ----
offenders="$(unguarded "$tests_dir")"
if [ -z "$offenders" ]; then
    ok "本目录每个建仓的测试都在文件级清过 GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE"
else
    bad "这些测试建了夹具仓却没有清 git 环境（外层带 GIT_DIR 跑时会写进调用方的仓）："
    printf '       %s\n' $offenders >&2
fi

# ---- 2. 检查器本身的错法：没清 / 只清一半 ----
fx="$work/fixture"
mkdir -p "$fx"
cat >"$fx/guarded.test.sh" <<'EOF'
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR
git -C "$d" init -q
git -C "$d" config user.name t
EOF
cat >"$fx/plain.test.sh" <<'EOF'
set -uo pipefail
git -C "$d" init -q
git -C "$d" config user.name t
EOF
cat >"$fx/half.test.sh" <<'EOF'
set -uo pipefail
unset GIT_DIR
git -C "$d" init -q
EOF
cat >"$fx/norepo.test.sh" <<'EOF'
set -uo pipefail
printf 'nothing to build here\n'
EOF
got="$(unguarded "$fx" | xargs -r -n1 basename | sort | tr '\n' ' ')"
want="half.test.sh plain.test.sh "
if [ "$got" = "$want" ]; then
    ok "只点名没清与只清一半的；清全的与不建仓的都不点名"
else
    bad "点名不对（期望「$want」，实得「$got」）"
fi

# ---- 3. 守卫真的挡得住：同一份夹具带绝对 GIT_DIR 跑，调用方的 config 不动 ----
mk_fixture() { # $1 = 文件，$2 = 1 带 unset / 空 不带
    {
        echo '#!/usr/bin/env bash'
        echo 'set -uo pipefail'
        [ -n "${2:-}" ] && echo 'unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR'
        echo 'd="$(mktemp -d)"'
        echo 'trap '"'"'rm -rf "$d"'"'"' EXIT'
        echo 'git -C "$d" init -q'
        echo 'git -C "$d" config user.name t'
        echo 'git -C "$d" config core.hooksPath /dev/null'
        echo 'printf "ok\n"'
    } >"$1"
    chmod +x "$1"
}

caller="$work/caller"
git init -q -b main "$caller"
git -C "$caller" config user.name s
git -C "$caller" config user.email s@example.invalid
git -C "$caller" commit -q --allow-empty -m base

run_under() { # $1 = 夹具脚本：在它的子进程里带绝对 GIT_DIR 跑一遍
    ( cd "$repo" && GIT_DIR="$caller/.git" GIT_WORK_TREE="$caller" \
        GIT_INDEX_FILE="$caller/.git/index" bash "$1" >/dev/null 2>&1 )
}

mk_fixture "$work/with-unset.sh" 1
run_under "$work/with-unset.sh"
got_name="$(git -C "$caller" config --get user.name)"
got_hooks="$(git -C "$caller" config --get core.hooksPath 2>/dev/null || true)"
if [ "$got_name" = s ] && [ -z "$got_hooks" ]; then
    ok "带 unset 的夹具没有改写调用方的 config"
else
    bad "调用方被写了：user.name=${got_name:-（空）} hooksPath=${got_hooks:-（空）}"
fi

mk_fixture "$work/without-unset.sh" ""
run_under "$work/without-unset.sh"
got_name="$(git -C "$caller" config --get user.name)"
got_hooks="$(git -C "$caller" config --get core.hooksPath 2>/dev/null || true)"
if [ "$got_name" = t ] && [ "$got_hooks" = /dev/null ]; then
    ok "去掉 unset 就写进调用方（这一格就是判据存在的理由）"
else
    bad "反例没有复现：user.name=${got_name:-（空）} hooksPath=${got_hooks:-（空）}"
fi

printf '\ntest-git-env.test: %d passed / %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
