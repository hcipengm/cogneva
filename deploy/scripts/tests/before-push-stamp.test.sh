#!/usr/bin/env bash
# `--status` 判的是「这一笔提交验过没有」，不是「现在替我验一遍」，两者必须分得开。
#
# 为什么要有这一层：钩子最后一次问这条门的时候，问的是「验过没有」（`--status`）。这一问
# 要是落空，它就会把整条判定（整棵 `cargo test` 加全部脚本测试，分钟级到小时级）在**每一次
# 推送**里跑一遍，而且是在推的人干等的这段时间里跑；远端在这段时间里往前走，一被拒这一段
# 全白等。所以这里把三种结局钉住：
#
#   1. 没验过的提交 ⇒ 退出 1，并且话里点明要怎么验；
#   2. 验过的那一笔 ⇒ 退出 0；
#   3. 又提交了一笔 ⇒ 回到退出 1（旧账不能替新提交说话）。
#
# 还有一条反向的，钉的是共享工作树上的那个形状：别人的在飞改动不随这一推走，**不该**让
# 一笔已经验过的提交失效——否则三四个会话共写一棵树时，戳永远命不中自己，等于没记。
#
# 夹具在一个临时树里跑这份门的副本（它按自己的位置算仓库根），所以不用动真仓库��也不跑
# 那整条分钟级的判定：记账的键由 `--key` 印出来，这里自己落一笔，判的是读账那一侧。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

tree="${work}/tree"
mkdir -p "${tree}/deploy/scripts/tests" "${tree}/.github/workflows"
cp "${gate}" "${tree}/deploy/scripts/check-before-push.sh"
cd "${tree}"

git init -q -b main .
git config user.email gate@example.invalid
git config user.name gate
printf 'one\n' > tracked.txt
git add tracked.txt
git commit -q -m "one"

run() { bash deploy/scripts/check-before-push.sh "$@" >"${work}/out" 2>&1; }

# 1) 没验过：退出 1，且话里给出验证命令（只印「没验过」而不说怎么办，等于把人挡在门外）。
if run --status; then
    fail "没验过的提交报了绿；实得：$(cat "${work}/out")"
fi
grep -q 'check-before-push.sh`' "${work}/out" ||
    fail "退出 1 的话里没有给出验证命令；实得：$(cat "${work}/out")"

# 2) 记账那一笔：绿的一跑按提交落账。这里用 --key 拿同一个键自己落，判的是读账。
key="$(bash deploy/scripts/check-before-push.sh --key)"
[ -n "${key}" ] || fail "--key 什么都没印"
mkdir -p .git/check-before-push
printf 'fixture\n' > ".git/check-before-push/${key}"
run --status || fail "记过账的提交仍报没验过；实得：$(cat "${work}/out")"

# 3) 别人在这棵树上留下的未提交改动（改了跟踪文件、多了未跟踪文件）：不随这一推走，
#    已经验过的这一笔仍然是验过的。
printf 'two\n' >> tracked.txt
printf 'new\n' > untracked.txt
run --status || fail "别人的未提交改动让已验过的提交失效了；实得：$(cat "${work}/out")"

# 4) 又提交了一笔：键跟着 HEAD 走，旧账不再覆盖这一份字节。
git add -A
git commit -q -m "two"
if run --status; then
    fail "新提交仍报验过（旧账替新提交说了话）"
fi

echo "ok - before-push 的戳：没验过 / 验过 / 新提交 三种结局分得开，未提交改动不进键"
