#!/usr/bin/env bash
# Refuse a commit whose staged prose states a capacity as though it were the
# software's, or points at a document the reader cannot open.
#
# Why at the commit: both questions are already gates, and CI's Repo
# Conventions job asks them -- after the push, where a red holds the roll for a
# whole CI cycle (the deployer does not roll a rev while any signal is a
# failure) and costs the author a second commit to say the same thing. Each
# gate reads comment lines and takes seconds, so the identical question is free
# here and expensive there. On 2026-11-01 a commit went out whose fixture
# comment said "declares 21Gi, granted 20Gi" and reddened that job; the fix was
# two words.
#
# Why the staged paths and not the tree: this clone is written by more than one
# session at a time. The gates take a file list, and a whole-tree run would
# judge a peer's unstaged edit and refuse a commit it is no part of. The list
# handed over is the staged one, so the judgement is of this commit's bytes.
# Deletions are not in it: a path a commit only removes carries no prose.
#
# The list is narrowed to the extensions the gates themselves read
# (`crates/**/*.rs`, `deploy/**/*.{yaml,yml,sh}`) instead of handed over whole:
# each gate greps every path it is given, and a path outside its scope would be
# read as prose it is not.
#
# Each gate then gets the part of that list it declares. Capacity reads
# manifests, deploy scripts and Rust; the citation gate reads Rust comment
# lines, and CI runs it with its default population -- `git ls-files '*.rs'`.
# Handing it a `.sh` widens the judgement past what CI asks: its own test file
# carries the three citation shapes as Rust fixtures inside shell strings, and
# read as shell prose they are indistinguishable from a citation, so a commit
# touching that file could never be made without `--no-verify`. Measured
# 2026-11-07 on the commit that clears the git environment in the test fixtures.
#
# Escape hatch: "cannot judge" never blocks. No git, no gate script in this
# checkout (an older clone), nothing staged, a gate that cannot run -- each
# says so and exits 0, because a machine that cannot answer is not a finding
# about the commit, and CI still governs what is pushed.
set -u

repo="$(env -u GIT_DIR -u GIT_WORK_TREE git rev-parse --show-toplevel 2>/dev/null)" || exit 0
[ -n "${repo:-}" ] || exit 0
cd "$repo" || exit 0

not_judged() {
    printf 'pre-commit（注释正文）: 这一条今天判不了，不拦 —— %s\n' "$1" >&2
    exit 0
}

staged="$(git diff --cached --name-only --diff-filter=ACMR 2>/dev/null)" || exit 0
[ -n "$staged" ] || exit 0

inside=()
rust=()
while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in
    crates/*.rs | crates/**/*.rs)
        inside+=("$f")
        rust+=("$f")
        ;;
    deploy/rendered/*) continue ;;
    deploy/*.yaml | deploy/*.yml | deploy/*.sh) inside+=("$f") ;;
    deploy/**/*.yaml | deploy/**/*.yml | deploy/**/*.sh) inside+=("$f") ;;
    esac
done <<<"$staged"

[ "${#inside[@]}" -gt 0 ] || exit 0

gate_dir="deploy/scripts"
capacity="${gate_dir}/check-comment-capacity-figures.sh"
doc_refs="${gate_dir}/check-comment-doc-references.sh"

failed=0
judged=0
run_gate() { # $1 = 脚本，$2 = 判词首行，其余 = 交给它的路径（空则不跑：无参会把判据放大到整棵树）
    script="$1"
    verdict="$2"
    [ -f "$script" ] || return 0
    command -v grep >/dev/null 2>&1 || return 0
    shift 2
    [ "$#" -gt 0 ] || return 0
    judged=$((judged + 1))
    out="$(bash "$script" "$@" 2>&1)"
    rc=$?
    [ "$rc" -eq 0 ] && return 0
    failed=$((failed + 1))
    printf '%s\n' "$verdict" >&2
    # 绝对前缀对提交者没有意义：换成「这一笔」。
    printf '%s\n' "$out" | sed -e "s|$repo/||g" | grep -E 'FAIL|^  - |^[^ ]' | head -12 >&2
    return 0
}

run_gate "$capacity" "pre-commit（注释正文）: 这一笔的注释里，有容量数字没说是谁的数字。" "${inside[@]}"
run_gate "$doc_refs" "pre-commit（注释正文）: 这一笔的注释里，有指向私有文档的引用。" "${rust[@]}"

[ "$judged" -gt 0 ] || not_judged "两份脚本都不在这棵树的 deploy/scripts 里"

if [ "$failed" -gt 0 ]; then
    printf '  CI 的 Repo Conventions 作业问的是同一句话，推上去再红要多花一个周期，\n' >&2
    printf '  而那一整个周期主线部署器不滚。修法是把注释改成机制或标上归属，\n' >&2
    printf '  要照常提交：`git commit --no-verify`，并在提交信息里写清为什么。\n' >&2
    exit 1
fi
exit 0
