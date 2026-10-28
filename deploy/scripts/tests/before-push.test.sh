#!/usr/bin/env bash
# check-before-push.sh 的两处「零」：一步都没跑起来，与一个脚本都没读到。
#
# 这条门自称把 CI 里那些只靠 shell 就能跑作业原样跑一遍。它的两种结局是
# 「跑过、全过」与「有红的」，而这两种之外还有第三种它原来印成第一种的样子：
#
#   - 从 ci.yml 取到的每一步都是多行脚本块 / 装依赖 ⇒ 可跑的 0 步，循环一次不进，
#     然后印「上列的 shell 面判据全绿」；
#   - `git ls-files '*.sh'` 交回空表 ⇒ shellcheck 没有参数、读 stdin、退 0——它判的
#     是一份空输入，而那句话与「每个脚本都过了」同形。
#
# 夹具在一个临时树里跑这份门的副本（它按自己的位置算仓库根），所以 CI 的形状由这里
# 的假 ci.yml 决定，不需要真的改仓库。
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

# 两个作业块都只剩多行脚本块：认得出来，但一步都跑不了。
cat >"${tree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: A multiline block the local runner cannot replay
        run: |
          echo one
          echo two
  deploy-parity:
    steps:
      - name: Another one
        run: |
          echo three
YAML

rc=0
out="$(cd "${tree}" && HOME="${work}/nohome" bash deploy/scripts/check-before-push.sh 2>&1)" || rc=$?

[ "${rc}" -ne 0 ] || fail "可跑的 0 步时退了 0：全跳过的绿与跑过的绿在读数上一样 —— ${out}"
case "${out}" in
  *"一步都没跑起来"*) ;;
  *) fail "判词没说是这一步门自己什么都没跑 —— ${out}" ;;
esac
echo "OK: 取到的步骤全是跳过的，门是红的并说明它自己没跑"

# 跳过的那一步必须点得出名字。2026-10-28：CI 的 Test 作业连着红在十几格上，
# 五笔提交照样往上走，而这份门当时印的是两行一模一样的 `SKIP  （多行脚本块）`
# ——「跑整棵工作区测试的那一步被跳过了」这件事，在这份读数里没有名字。
case "${out}" in
  *"SKIP  A multiline block the local runner cannot replay（多行脚本块）"*) ;;
  *) fail "跳过的步没有点名是哪一步 —— ${out}" ;;
esac
case "${out}" in
  *"SKIP  （多行脚本块）"*) fail "跳过的步仍然印着一对空括号，读的人认不出它 —— ${out}" ;;
  *) ;;
esac
echo "OK: 跳过的步点名到具体那一步"

case "${out}" in
  *"一个脚本都没读到"*) ;;
  *) fail "空脚本表没有被点名（shellcheck 读 stdin 退 0）—— ${out}" ;;
esac
echo "OK: git ls-files 空表时点名，而不是让 shellcheck 判一份空输入"

# 反方向：有一步真的跑起来了、而且是绿的，这条门才算跑过。假 ci.yml 换成一条可跑的
# 解释器加路径，判词里那一步要出现，并且不再印「一步都没跑起来」。
cat >"${tree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: A step this gate can replay
        run: bash -n deploy/scripts/check-before-push.sh
  deploy-parity:
    steps:
      - run: |
          echo three
YAML
rc=0
out="$(cd "${tree}" && HOME="${work}/nohome" bash deploy/scripts/check-before-push.sh 2>&1)" || rc=$?
case "${out}" in
  *"一步都没跑起来"*) fail "一步跑起来了却仍报「没跑」—— ${out}" ;;
  *) ;;
esac
case "${out}" in
  *"== -n deploy/scripts/check-before-push.sh"*) ;;
  *) fail "跑起来的那一步没有印出来 —— ${out}" ;;
esac
# 这一步仍是红的（临时树不是 git 仓库，脚本表为空），所以只断言它没被当成「全绿」。
[ "${rc}" -ne 0 ] || fail "空脚本表与「不是 git 仓库」在这棵临时树上应当是红的 —— ${out}"
echo "OK: 有一步跑起来时不再报「没跑」，且空脚本表仍然红"

# 没有写 `- name:` 的步骤也要有一段能读的字，不能退回空括号——空括号读起来像是
# 「有一个跳过的步」而说不出是哪一个，与点名之前是同一种沉默。
case "${out}" in
  *"SKIP  这一步没有名字（多行脚本块）"*) ;;
  *) fail "无名的跳过没有兜底说法 —— ${out}" ;;
esac
echo "OK: 没有名字的步骤印出兜底说法，而不是空括号"

echo "before-push.test: all checks passed"
