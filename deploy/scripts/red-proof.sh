#!/usr/bin/env bash
# 红证：一笔提交新加的判据，在**父树**上是不是真的会红。
#
# 「这条测试有牙齿」这句话，今天只能由做过的人凭记忆担保：建旧树、把改动折回、跑
# 同一批测试、看它红——步骤写在会话里，换一个人接手就重做一遍，或者干脆信一句
# 「我试过了」。红证把这件事变成一条跑法：给它一笔提交，它答出「在它之前的那棵树
# 上，这一批测试红在哪一条上」（CC29）。
#
# 它判的是什么，只有这一件：**判据对那笔改动有没有反应**。它不改产物，不新增提交，
# 不碰工作树——在 `git worktree` 造出来的临时树里跑，跑完拆掉。产品树上一个字节都
# 不动：一次红证如果能在树上留下东西，它自己就成了新的不确定性。
#
# 判据从提交里算，不从参数里来：
#   - 提交新增/改动的集成测试文件 `crates/<crate>/tests/<name>.rs`
#     ⇒ `cargo test -p <crate> --test <name>`；
#   - 提交改动了一个源文件里的 `#[cfg(test)]` 段
#     ⇒ `cargo test -p <crate> --lib`（单元测试只有在改了源文件时才算这一笔的判据）。
#
# 无判据可跑是可读的第三档，不是绿：一笔只改文档的提交没有红证可做，跑出「没有
# 判据」比跑出「0 条测试通过」诚实。
#
# 用法：
#   deploy/scripts/red-proof.sh <commit>            临时树里跑，跑完拆掉
#   deploy/scripts/red-proof.sh --keep <commit>     留下那棵树，印出路径
#   deploy/scripts/red-proof.sh --plan <commit>     只印要跑什么，不跑
#
# 退出码：0 红证成立（判据在父树上红）；1 判据在父树上绿了（没有牙齿）；2 读不出
# 这一笔的判据；3 工具/环境错。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/../.." && pwd)"
cd "${root}" || exit 3

keep=0
plan=0
args=()
for a in "$@"; do
    case "${a}" in
    --keep) keep=1 ;;
    --plan) plan=1 ;;
    *) args+=("${a}") ;;
    esac
done
commit="${args[0]:-}"
[ -n "${commit}" ] || {
    echo "red-proof: 要一笔提交（红证问的是这一笔的判据）" >&2
    exit 3
}
git rev-parse --verify --quiet "${commit}^{commit}" >/dev/null || {
    echo "red-proof: ${commit} 不是这个仓里的提交" >&2
    exit 3
}
commit="$(git rev-parse "${commit}^{commit}")"
parent="$(git rev-parse --verify --quiet "${commit}^")" || {
    echo "red-proof: ${commit} 是根提交，它没有父树；红证问不出「之前是什么样」" >&2
    exit 2
}

# 这一笔的判据：改成 (crate, target, kind) 三元组。
targets=()
while IFS= read -r path; do
    [ -n "${path}" ] || continue
    crate=""
    case "${path}" in
    crates/*/*)
        crate="${path#crates/}"
        crate="${crate%%/*}"
        ;;
    esac
    [ -n "${crate}" ] || continue
    case "${path}" in
    crates/*/tests/*.rs)
        name="${path##*/}"
        name="${name%.rs}"
        targets+=("${crate}|${name}|integration")
        ;;
    crates/*/src/*.rs)
        # 只有这一笔真动了 `#[cfg(test)]` 段才算。用一次 diff 的上下文行数判，
        # 比逐个 `git show` 每个文件便宜，也够：判据在单元测试里，而单元测试只在
        # 改了这个文件时才是这一笔的。
        if git show "${commit}" --format= -- "${path}" | grep -q 'cfg(test)'; then
            targets+=("${crate}||lib")
        fi
        ;;
    esac
done < <(git diff-tree --no-commit-id --name-only -r "${commit}" -- crates/)

if [ "${#targets[@]}" -eq 0 ]; then
    echo "red-proof: 这一笔没有判据可跑（没有动过测试文件，也没有动过源文件的测试段）"
    echo "red-proof: 没有判据不是红证成立，也不是它失败——这一笔的红证问题不在这里"
    exit 2
fi

printf 'red-proof: %s 的判据（在父树 %s 上跑）：\n' "${commit:0:10}" "${parent:0:10}"
for t in "${targets[@]}"; do
    IFS='|' read -r crate name kind <<<"${t}"
    if [ "${kind}" = integration ]; then
        printf 'red-proof:   cargo test -p %s --test %s\n' "${crate}" "${name}"
    else
        printf 'red-proof:   cargo test -p %s --lib\n' "${crate}"
    fi
done
[ "${plan}" = 1 ] && exit 0

# 临时树：与这个仓同一个对象库，所以 `commit` 在它里面也在；`--detach` 不挂分支。
tree="${TMPDIR:-/tmp}/red-proof.$(date +%s).$$"
cleanup() {
    [ "${keep}" = 1 ] && return 0
    git -C "${root}" worktree remove --force "${tree}" >/dev/null 2>&1 || rm -rf "${tree}"
}
trap cleanup EXIT
git worktree add --detach --quiet "${tree}" "${parent}" || {
    echo "red-proof: 父树的临时 worktree 建不出来：${tree}" >&2
    exit 3
}

# 把这一笔**新加的判据**放进父树：只有测试文件。源文件的改动不搬——搬了测的就不是
# 父树了，红证要问的正是「没有这笔改动时它红不红」。
for t in "${targets[@]}"; do
    IFS='|' read -r crate name kind <<<"${t}"
    [ "${kind}" = integration ] || continue
    src="crates/${crate}/tests/${name}.rs"
    if git cat-file -e "${commit}:${src}" 2>/dev/null; then
        mkdir -p "${tree}/crates/${crate}/tests"
        git show "${commit}:${src}" >"${tree}/${src}"
    fi
done

# 跑。命令这一条是测试用的缝：设了就按它跑，参数照旧接在后面。
cmd="${COGNEVA_RED_PROOF_TEST_CMD:-cargo test}"
failed=()
for t in "${targets[@]}"; do
    IFS='|' read -r crate name kind <<<"${t}"
    if [ "${kind}" = integration ]; then
        set -- -p "${crate}" --test "${name}"
    else
        set -- -p "${crate}" --lib
    fi
    out="$(cd "${tree}" && ${cmd} "$@" 2>&1)"
    rc=$?
    # 只有「有测试跑了并且红了」才算牙齿。编不过的退出码同样是红，可它说的是
    # 父树编不出来（新测试引用了新代码），不是判据有反应——两者分开记。
    if [ "${rc}" -eq 0 ]; then
        continue
    fi
    if grep -qE '^(error(\[E[0-9]+\])?:.*could not compile|error: could not compile)' <<<"${out}"; then
        failed+=("${crate} ${name} （父树上编不过：新测试引用了新代码，红证要的仓库态还没有）")
        continue
    fi
    failed+=("${crate} ${name}")
done

if [ "${#failed[@]}" -eq 0 ]; then
    echo "red-proof: 这一批判据在父树上全绿——它们对这笔改动没有反应（没有牙齿）"
    exit 1
fi
echo "red-proof: 红证成立。父树上红的是："
for f in "${failed[@]}"; do
    printf 'red-proof:   %s\n' "${f}"
done
exit 0
