#!/usr/bin/env bash
# principle: feedback_a_branch_converges_into_the_trunk_only_after_its_content_is_proven_there
# check-branch-convergence.sh 的定向测试。
#
# 被测的是「一条非 trunk 分支在被删掉之前，它的内容已经在 trunk 上」。治的病是这一句
# 没有依据的话：`git branch -d` 自带「已合并」判据，可 `-D` 没有，而「已合并」在两种
# 情况下都为假——一种是分支上真有 trunk 没有的提交，另一种是提交不同但**补丁早就以另一种
# 写法落到 trunk 上**（2026-10-23 那 8 条里 3 条就是这一档：同一个修复被 rebase 或手抄
# 成另一笔 sha，直接 merge 会在三处冲突）。只判第一种，就会把第二种误报成「还不能删」，
# 于是这道门被人用 `-D` 绕过去——那正是它要防的动作。
#
# 格子：
#   1. 只有 trunk 的仓库 ⇒ 3：零条被判与「全部已收敛」在汇总行上同形；
#   2. 分支停在 trunk 的提交上（ahead=0）⇒ 0，汇总里 `branches=1`（防扫描面缩到零）；
#   3. 一条已收敛、一条有独立提交 ⇒ 1，只点名后者（逐条判，不是整体一个数）；
#   4. 有独立提交但补丁已在 trunk 上（cherry 全 `-`）⇒ 0：这是第二条腿，少了它会误报；
#   5. 不是 git 仓库 ⇒ 3，不是 0；
#   6. trunk 名字不存在 ⇒ 3，不是 0；
#   7. 远端：所有非 trunk 头都是 trunk 的祖先 ⇒ 0；
#   8. 远端：有一个头不是祖先、对象在本地 ⇒ 1；
#   9. 远端：头指向本地没有的对象 ⇒ 3（判不了 ≠ 合格）。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-branch-convergence.sh"
[ -f "$gate" ] || { echo "FAIL: 被测脚本不在：$gate" >&2; exit 1; }

pass=0
fail=0
ok() { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望退出码 $2，实得 $3）"; fi; }
ck_has() {
    if printf '%s\n' "$2" | grep -qF -- "$3"; then ok "$1"; else
        bad "$1（期望含 $3）"
        printf '%s\n' "$2" | sed 's/^/        实得: /' | head -12 >&2
    fi
}
ck_lacks() {
    if printf '%s\n' "$2" | grep -qF -- "$3"; then
        bad "$1（不该含 $3）"
        printf '%s\n' "$2" | sed 's/^/        实得: /' | head -12 >&2
    else ok "$1"; fi
}

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

newrepo() { # $1=目录：一个 main 上只有一笔 base 提交的仓库
    git init -q -b main "$1"
    git -C "$1" config user.name t
    git -C "$1" config user.email t@example.com
    git -C "$1" config commit.gpgsign false
    git -C "$1" config core.hooksPath /dev/null
    printf 'base\n' >"$1/base.txt"
    git -C "$1" add base.txt
    git -C "$1" commit -q -m base
}

add_commit() { # $1=目录 $2=文件名 $3=内容 $4=提交信息
    printf '%s\n' "$3" >"$1/$2"
    git -C "$1" add "$2"
    git -C "$1" commit -q -m "$4"
}

# ---- case 1: 只有 trunk，零条可判 ----
r="$work/only-trunk"; newrepo "$r"
out="$(bash "$gate" "$r" 2>&1)"; rc=$?
ck_rc "只有 trunk ⇒ 3，不是 0" 3 "$rc"
ck_has "零条与干净分开说" "$out" "judged nothing"

# ---- case 2: 分支停在 trunk 的提交上 ----
r="$work/behind"; newrepo "$r"
git -C "$r" branch old
out="$(bash "$gate" "$r" 2>&1)"; rc=$?
ck_rc "ahead=0 的分支算已收敛" 0 "$rc"
ck_has "汇总报出被判的条数" "$out" "branches=1 uncontained=0"
ck_has "逐条说它为什么合格" "$out" "ok    - old"

# ---- case 3: 一条已收敛、一条有独立提交 ----
r="$work/mixed"; newrepo "$r"
git -C "$r" branch old
git -C "$r" checkout -q -b feature
add_commit "$r" feature.txt 'feature' feature
git -C "$r" checkout -q main
out="$(bash "$gate" "$r" 2>&1)"; rc=$?
ck_rc "有独立提交的分支 ⇒ 红" 1 "$rc"
ck_has "判词点名那条分支" "$out" "RED   - feature"
ck_has "汇总数出错的那一条" "$out" "branches=2 uncontained=1"
ck_has "已收敛的那一条仍判合格" "$out" "ok    - old"
ck_has "修法点名 ours 合入" "$out" "-s ours feature"
ck_has "修法点名 -d 而不是 -D" "$out" "git branch -d feature"
ck_lacks "修法里不出现 -D（-D 没有零丢失判据）" "$out" "branch -D"

# ---- case 4: 有独立提交，但补丁已经在 trunk 上 ----
# trunk 必须先往前走一笔，否则 cherry-pick 打到原来的父提交上会重放出**同一个** sha，
# 分支就成了「ahead=0」，测不到第二条腿。
r="$work/rebased"; newrepo "$r"
git -C "$r" checkout -q -b feature
add_commit "$r" feature.txt 'feature' feature
git -C "$r" checkout -q main
add_commit "$r" other.txt 'other' other
git -C "$r" cherry-pick feature >/dev/null 2>&1
out="$(bash "$gate" "$r" 2>&1)"; rc=$?
ck_rc "补丁已在 trunk 上 ⇒ 合格（第二条腿）" 0 "$rc"
ck_has "它被判为合格而不是被判零条" "$out" "branches=1 uncontained=0"
ck_has "判词说出它比 trunk 多几笔却仍合格" "$out" "ahead=1 未上游补丁=0"

# ---- case 5: 不是 git 仓库 ----
mkdir -p "$work/plain"
out="$(bash "$gate" "$work/plain" 2>&1)"; rc=$?
ck_rc "不是 git 仓库 ⇒ 3，不是 0" 3 "$rc"
ck_has "读不动与干净分开说" "$out" "judged nothing"

# ---- case 6: trunk 名字不存在 ----
r="$work/no-trunk"; newrepo "$r"
git -C "$r" branch other
out="$(bash "$gate" "$r" --trunk nope 2>&1)"; rc=$?
ck_rc "trunk 不在 ⇒ 3，不是 0" 3 "$rc"
ck_has "说清是哪条分支不在" "$out" "nope"

# ---- case 7: 远端分支都是 trunk 的祖先 ----
git init -q --bare "$work/origin7.git"
r="$work/remote7"; newrepo "$r"
git -C "$r" remote add origin "$work/origin7.git"
git -C "$r" branch same
git -C "$r" push -q origin main same
out="$(bash "$gate" "$r" --remote origin 2>&1)"; rc=$?
ck_rc "远端的头都是祖先 ⇒ 合格" 0 "$rc"
ck_has "远端那一条也进了判据" "$out" "ok    - same@"
ck_has "汇总里本地与远端都数上了" "$out" "uncontained=0"

# ---- case 8: 远端有一个头不是祖先，对象在本地 ----
git init -q --bare -b main "$work/origin8.git"
seed="$work/seed8"; newrepo "$seed"
git -C "$seed" remote add origin "$work/origin8.git"
git -C "$seed" checkout -q -b ahead8
add_commit "$seed" ahead.txt 'ahead' ahead
git -C "$seed" checkout -q main
git -C "$seed" push -q origin main ahead8
r="$work/clone8"
git clone -q "$work/origin8.git" "$r"
out="$(bash "$gate" "$r" --remote origin 2>&1)"; rc=$?
ck_rc "远端的头不是祖先 ⇒ 红" 1 "$rc"
ck_has "判词点名远端分支" "$out" "RED   - ahead8@"

# ---- case 9: 远端有一个头指向本地没有的对象 ----
git init -q --bare -b main "$work/origin9.git"
seed="$work/seed9"; newrepo "$seed"
git -C "$seed" remote add origin "$work/origin9.git"
git -C "$seed" checkout -q -b ghost9
add_commit "$seed" ghost.txt 'ghost' ghost
git -C "$seed" checkout -q main
git -C "$seed" push -q origin main ghost9
r="$work/clone9"
# `--no-local`：本地路径克隆默认硬链整个对象库，连 ghost9 那些不可达对象也会跟过来，
# 那样这一格就退化成「对象在本地」而不是「判不了」。
git clone -q --no-local --single-branch --branch main "$work/origin9.git" "$r"
out="$(bash "$gate" "$r" --remote origin 2>&1)"; rc=$?
ck_rc "判不了的头 ⇒ 3，不是 0 也不是 1" 3 "$rc"
ck_has "判不了单独记一档" "$out" "UNKNOWN - ghost9@"
ck_has "汇总把判不了与不合格分开数" "$out" "unknown=1"

printf '\nbranch-convergence.test: %d passed / %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
