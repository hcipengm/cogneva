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
# A fixture repository is written through `git -C <dir>` or a subshell `cd`,
# and neither changes the repository: an absolute `GIT_DIR` / `GIT_WORK_TREE` /
# `GIT_INDEX_FILE` handed down by the commit lander, a commit hook or a snapshot
# makes every call below write into the caller's repository instead. Measured
# 2026-11-07: with an absolute GIT_DIR exported, a fixture's `config user.name`
# and `config core.hooksPath` landed in the caller's config -- the same shape
# that renamed everyone's commits `t` and switched the shared hooks off. Clear
# it once, at file scope, so the next case is covered too.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
# shellcheck source=tests/fixtures/stage-gate.sh
. "${here}/fixtures/stage-gate.sh"
gate="${repo}/deploy/scripts/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 夹具不是这台机器上的构建方，不能去抢宿主真正的构建槽：门现在每一格编译命令都从槽里
# 拿，指到真槽上这一格就会站在别的会话的构建后面排队（实测 300 秒被杀），而它要判的是
# 「cargo 被这样调用了吗」。给一个不存在的槽目录，门退到自己的锁上，形状是真的，代价是零。
export COGNEVA_BUILD_GATE_LOCK_DIR="${work}/no-gate"
export COGNEVA_BUILD_LOCK_FILE="${work}/build.lock"

tree="${work}/tree"
mkdir -p "${tree}/deploy/scripts/tests" "${tree}/.github/workflows"
stage_gate "${repo}" "${tree}"

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

# Test 腿：改动落在哪个包，那个包自己的测试先在本机跑一次，并且**总是**带上装配件
# `cogneva`——读全树源码的普查门住在它的测试里，按改动包收窄正好把它们排除掉。
#
# 这一格挡的是 2026-11-06 的事故：一笔直接推到 main 的改动只碰了一个包（滚动构建器），
# 而同一文件里那道门当场就会红，跑起来是秒级；推送之前没人跑它，此后每一版 Test 与
# Code Coverage 都是红的，部署器停在 5dad1f95 上整整九版，前面落地的修复一条都没生效。
#
# 反证：去掉这一条腿，下面这一次调用里既没有 `-p fake` 也没有 `-p cogneva`，这一格
# 会红在「这次要推的提交动过 1 个 crate」那一句上。
leg="${work}/test-leg"
mkdir -p "${leg}/deploy/scripts" "${leg}/.github/workflows" "${leg}/crates/fake/src" "${leg}/bin"
stage_gate "${repo}" "${leg}"
cat >"${leg}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: nothing to replay here
        run: |
          echo one
  deploy-parity:
    steps:
      - name: nothing here either
        run: |
          echo two
YAML
printf '[package]\nname = "fake"\nversion = "0.0.0"\n' >"${leg}/crates/fake/Cargo.toml"
printf 'pub fn a() {}\n' >"${leg}/crates/fake/src/lib.rs"
# 假的 cargo 只做一件事：把每次调用原样记下来，然后退 0。这一格要看见的是「它被这样
# 调用了吗」，不是这次测试的结果——把结果也演一遍会让红的原因说不清是哪一半。
cat >"${leg}/bin/cargo" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CARGO_LOG}"
exit 0
SH
chmod +x "${leg}/bin/cargo"
(
    cd "${leg}" || exit 1
    git init -q .
    git -c user.email=t@t.invalid -c user.name=t add -A
    git -c user.email=t@t.invalid -c user.name=t commit -qm base
    git update-ref refs/remotes/origin/main HEAD
    printf 'pub fn b() {}\n' >>crates/fake/src/lib.rs
    git -c user.email=t@t.invalid -c user.name=t add -A
    git -c user.email=t@t.invalid -c user.name=t commit -qm change
) >/dev/null 2>&1 || fail "造不出这一格的 git 夹具（init / origin/main / 一次改动）"

cargo_log="${work}/cargo-test-leg.log"
: >"${cargo_log}"
rc=0
out="$(cd "${leg}" && CARGO_LOG="${cargo_log}" PATH="${leg}/bin:${PATH}" \
    bash deploy/scripts/check-before-push.sh 2>&1)" || rc=$?

case "${out}" in
  *"== Test：这次动过 1 个 crate 的源码"*) ;;
  *) fail "改动落在 crates/fake 上，Test 腿没有跑起来 —— ${out}" ;;
esac
grep -qE '^test .*-p fake.* -p cogneva|^test .*-p cogneva.* -p fake' "${cargo_log}" ||
    fail "Test 腿没有把改动包与装配件一起交给 cargo：$(cat "${cargo_log}")"
echo "OK: 改动落在哪个包，Test 腿就判那个包加装配件"
echo "OK: Test 腿真的调用了 cargo test -p fake -p cogneva（rc=${rc}）"

# ---- 脚本测试继承到 git 定位变量时，不许把调用方那棵树翻成裸库 ----
# 这条门由 pre-push 钩子拉起时，钩子的 git 会把 `GIT_DIR` / `GIT_WORK_TREE` 指向
# **调用方那棵树**。`deploy/scripts/tests/*.test.sh` 各自 `git init` 夹具仓库，定位
# 变量还在手上时，`git init --bare <夹具>` 落的就是调用方那棵树——它被重新初始化成
# 裸库，`core.bare` 翻成 true，之后那棵树里每条 git 命令都报 "must be run in a work
# tree"，钩子返回时提交写不进去。
#
# 2026-11-07 实测：一笔提交在钩子里连卡两次，第二次真凶暴露——`branch-convergence.test.sh`
# 的三处 `git init --bare`。所以判据落在那棵**调用方树**上，不落在门印了哪句话：给它
# 一个真的工作树、让夹具脚本照原样 `git init --bare`，跑完它还得是工作树。
# 少了这一格，这处修复在门自己的读数上不可见——它只会在下一个人的提交上炸。
loc="${work}/locators"
mkdir -p "${loc}/caller" "${loc}/tree/deploy/scripts/tests" "${loc}/tree/.github/workflows"
stage_gate "${repo}" "${loc}/tree"
git -C "${loc}/caller" init -q -b main ||
    fail "造不出这一格的调用方工作树"
cat >"${loc}/tree/deploy/scripts/tests/fixture.test.sh" <<'SH'
#!/usr/bin/env bash
# 夹具脚本要的是**自己的**仓库，所以先把手上有没有调用方的定位变量记下来。
set -euo pipefail
[ -z "${LOCATORS_MARKER:-}" ] ||
    printf 'GIT_DIR=%s\nGIT_WORK_TREE=%s\n' \
        "${GIT_DIR-<unset>}" "${GIT_WORK_TREE-<unset>}" >"${LOCATORS_MARKER}"
d="$(mktemp -d)"
git init -q --bare "${d}/origin.git"
rm -rf "${d}"
SH
cat >"${loc}/tree/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: A step this gate can replay
        run: bash -n deploy/scripts/check-before-push.sh
YAML
marker="${loc}/ran"
rc=0
(
    cd "${loc}/tree" || exit 1
    LOCATORS_MARKER="${marker}" GIT_DIR="${loc}/caller/.git" GIT_WORK_TREE="${loc}/caller" \
        HOME="${work}/nohome" bash deploy/scripts/check-before-push.sh
) >"${loc}/out" 2>&1 || rc=$?
# 先证明这一格真的跑到了：夹具脚本一次都没被执行时，下面那句「它手里没有定位变量」
# 是在证明一件没发生的事，与没写这一格同形。
[ -f "${marker}" ] ||
    fail "夹具脚本没被这条门拉到，这一格什么都没验证：$(tail -5 "${loc}/out")"
# 判据是夹具**手里拿到的东西**，不是门印了哪句话：定位变量在手上时，夹具的
# `git init --bare` 落的是调用方那棵树。
if grep -qv '=<unset>$' "${marker}"; then
    fail "脚本测试继承了调用方的 git 定位变量，夹具的 git init 会落在那一棵树上：$(cat "${marker}")"
fi
# 再加一道到底的判据：跑完之后调用方那棵树还得是工作树。这一句是那场事故的原话
# ——core.bare 翻成 true 之后，那棵树里每条 git 命令都报 "must be run in a work tree"。
bare="$(git -C "${loc}/caller" rev-parse --is-bare-repository 2>/dev/null || echo unknown)"
[ "${bare}" = "false" ] ||
    fail "调用方那棵树被跑成了裸库（is-bare=${bare}），之后它里面每条 git 命令都会失败：$(tail -5 "${loc}/out")"
echo "OK: 脚本测试手里的 git init --bare 落在自己的夹具上，调用方那棵树仍是工作树（rc=${rc}）"

echo "before-push.test: all checks passed"
