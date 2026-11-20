#!/usr/bin/env bash
# Refuse a commit that leaves a producer registry pointing at a file that moved,
# or at a file that is still there but no longer carries the series it is
# registered for.
#
# Why at the commit: the two observability contract tables name, for every
# series, the file that publishes it. The Rust test that checks those rows is
# real but only runs when someone runs it -- and the person who moves the file
# runs their own module's tests. The cost then lands on whoever runs the
# contract test next, and on this host a red at the head of the mainline holds
# every later revision, so the cost lands on strangers. Asking the same question
# here costs about a second and asks it of the person who can fix it.
#
# "Could not judge" does not block: no python3, no cogneva checkout with the
# contract tables, an unreadable index. A machine that cannot answer is not a
# finding about the commit, and CI plus the Rust test still govern what was
# pushed.
set -u

repo="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
[ -n "$repo" ] || exit 0

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gate="$here/../../../gates/check-registered-producer-paths.py"
[ -f "$gate" ] || exit 0
command -v python3 >/dev/null 2>&1 || exit 0

out="$(python3 "$gate" --repo "$repo" 2>&1)"
rc=$?
# 判的是这个仓，但要说提交者认得的路径：门被 `--repo` 指着的时候会在消息里印那棵树的
# 绝对路径，而夹具仓在临时目录下——原样印出去，提交者读到的是他的临时目录。同一份理由
# 写在格式那一份钩子里（它 sed 掉 rustfmt 消息里的临时前缀）。
out="$(printf '%s\n' "$out" | sed "s|${repo}/||g; s|${repo}||g")"
case "$rc" in
0) exit 0 ;;
3)
    printf 'pre-commit（名册路径）: 这一条今天判不了，不拦：\n%s\n' "$out" >&2
    exit 0
    ;;
esac

cat >&2 <<EOF
pre-commit（名册路径）: 这次提交把按路径登记产出点的名册指到了不存在的东西上。

$out

修法：把登记行改到新家（同类行一起改），别留一个指向旧地址的许可；转发壳同罪——
文件名还在不代表产出还在。要照常提交（例如这条判据本身在修）：\`git commit --no-verify\`，
并在信息里写清为什么。
EOF
exit 1
