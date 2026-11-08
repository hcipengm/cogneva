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

# 5) 提交之前那一问（`--status-content`）：键是**工作树的字节**，脏树上也记账——判的就是
#    这份脏，提交之后字节不变，所以两头的指纹一样。上面第 2 格的推送键在这里命不中。
printf 'three\n' > third.txt
git add third.txt
ckey="$(bash deploy/scripts/check-before-push.sh --key-content)" || fail "--key-content 退非零"
[ -n "${ckey}" ] || fail "--key-content 什么都没印"
printf 'fixture\n' > ".git/check-before-push/content.${ckey}"
run --status-content || fail "判过的这份字节在提交前仍报没验过；实得：$(cat "${work}/out")"

# 6) 提交不改变这棵树的内容：提交之后的同一份字节仍然命中（这是提交前那一问存在的理由）。
git commit -q -m "three"
run --status-content || fail "提交之后同一份字节的账失效了；实得：$(cat "${work}/out")"

# 7) 再改一个字节：内容键跟着变，旧账不再覆盖这份新字节。
printf 'four\n' >> tracked.txt
if run --status-content; then
    fail "改过之后仍报验过（旧账替新字节说了话）"
fi
printf 'one\ntwo\n' > tracked.txt

# 8) 判据本身变了（门自己的字节进指纹）：上一份结论不再作数。
printf '\n# judged differently\n' >> deploy/scripts/check-before-push.sh
if run --status-content; then
    fail "门自己的字节改了，旧账仍报验过"
fi

# 9) 还没有 HEAD 的树：读不出指纹 ⇒ 退 2。问不了不是红，退 1 会把它说成没验过。
bare="${work}/bare"
mkdir -p "${bare}/deploy/scripts"
cp "${gate}" "${bare}/deploy/scripts/check-before-push.sh"
( cd "${bare}" && git init -q -b main . )
rc=0
( cd "${bare}" && bash deploy/scripts/check-before-push.sh --status-content ) >"${work}/out" 2>&1 || rc=$?
[ "${rc}" -eq 2 ] || fail "还没有 HEAD 的树应退 2（问不了），实得 ${rc}：$(cat "${work}/out")"

# 10) 从钩子里被拉起来时，git 会把 `GIT_DIR`/`GIT_WORK_TREE` 指向**调用方那棵树**：
#     指纹必须仍描述这道门所在的这棵树。`GIT_WORK_TREE` 在手上时 `add -A` 收的是另一棵树
#     的文件——指纹会指着一棵与这次提交无关的树，而两边的判词看上去完全一样。
caller="${work}/caller"
git init -q -b main "${caller}"
git -C "${caller}" config user.email gate@example.invalid
git -C "${caller}" config user.name gate
printf 'caller\n' >"${caller}/only-in-caller.txt"
git -C "${caller}" add only-in-caller.txt
git -C "${caller}" commit -q -m caller
here_key="$(bash deploy/scripts/check-before-push.sh --key-content)"
there_key="$(GIT_DIR="${caller}/.git" GIT_WORK_TREE="${caller}" \
    bash deploy/scripts/check-before-push.sh --key-content)"
[ "${here_key}" = "${there_key}" ] ||
    fail "继承来的 git 定位变量换了指纹（这棵树 ${here_key:0:12}，调用方那棵树 ${there_key:0:12}）"

echo "ok - before-push 的戳：没验过 / 验过 / 新提交 三种结局分得开，未提交改动不进键；提交前那一问按工作树字节记，改字节或改判据都失效"
