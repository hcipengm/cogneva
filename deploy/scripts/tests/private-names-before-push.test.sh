#!/usr/bin/env bash
# 私有标识那一格：判的是**要推的那份字节**（HEAD 的树），不是工作树；没有私有仓检出的机器上
# 它是「没验」，不是绿；判据说「判不了」也是没验，而别的非零码照原样传出去（那是红）。
#
# 为什么这一格要有自己的反例：这条门的三条出口（干净 / 命中 / 没验）里，后两条塌成前一条在
# 判词上一个字节都不显示——一台没配私有仓的机器会把「没人问过」印成一次干净的推送。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
wrapper="${repo}/deploy/scripts/check-private-names.sh"
[ -f "${wrapper}" ] || { echo "FAIL: 找不到 ${wrapper}"; exit 1; }
fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 私有仓夹具：只有一处判据文件。行为由 PRIVATE_STUB_RC 控制；它把**自己拿到的树**打出来，
# 这一格才能问「判的到底是哪一份字节」，而不是只看退出码。
pr="${work}/private"
mkdir -p "${pr}/scripts/gates"
cat >"${pr}/scripts/gates/check-private-references.sh" <<'STUB'
#!/usr/bin/env bash
mode="${1:-}"; tree="${2:-}"
printf 'STUB_MODE=%s\n' "${mode}"
printf 'STUB_TREE=%s\n' "$(cd "${tree}" && pwd)"
printf 'STUB_FILES=%s\n' "$(git -C "${tree}" ls-files | tr '\n' ' ')"
printf 'STUB_HEAD=%s\n' "$(git -C "${tree}" rev-parse HEAD)"
exit "${PRIVATE_STUB_RC:-0}"
STUB
chmod +x "${pr}/scripts/gates/check-private-references.sh"

pub="${work}/public"
mkdir -p "${pub}/deploy/scripts"
git init -q "${pub}"
git -C "${pub}" config user.email probe@example.com
git -C "${pub}" config user.name probe
printf 'one\n' >"${pub}/committed.txt"
git -C "${pub}" add committed.txt
git -C "${pub}" commit -qm base
# 判据的副本放进夹具：它从自己的位置推被守的树，拿真仓当被守树就判错了对象。
# 这个副本里没有名单，名单从 `git config cogneva.privateRoot` 指向的仓推。
cp "${wrapper}" "${pub}/deploy/scripts/check-private-names.sh"
git -C "${pub}" config cogneva.privateRoot "${pr}"

run() { # <期望退出码> <标签>；stdout+stderr 写进 $out，rc 写进 $rc
    local want="$1" label="$2"; shift 2
    set +e
    out="$(env -u COGNEVA_PRIVATE_ROOT "$@" 2>&1)"
    rc=$?
    set -e
    [ "${rc}" -eq "${want}" ] || fail "${label}：期望退出码 ${want}，得到 ${rc}；输出：${out}"
}

# 1. 干净 ⇒ 0，判据真的被叫到了，模式是 --tree，判的是 HEAD 的树。
#    工作树里放一个没提交的文件：它不该出现在判据看到的那份字节里。
printf 'dirty\n' >"${pub}/dirty.txt"
run 0 "干净" env PRIVATE_STUB_RC=0 bash "${pub}/deploy/scripts/check-private-names.sh"
grep -q 'STUB_MODE=--tree' <<<"${out}" || fail "干净：判据没按 --tree 跑：${out}"
if grep -qx "STUB_TREE=$(cd "${pub}" && pwd)" <<<"${out}"; then fail "干净：判据判的是主工作树本身（快照没造出来）：${out}"; fi
grep -q 'committed.txt' <<<"${out}" || fail "干净：判据没看到 HEAD 里的文件：${out}"
if grep -q 'dirty.txt' <<<"${out}"; then fail "干净：判据看到了工作树里没提交的字节（判错了对象）：${out}"; fi
grep -qx "STUB_HEAD=$(git -C "${pub}" rev-parse HEAD)" <<<"${out}" || fail "干净：快照不是 HEAD 那一笔：${out}"

# 2. 命中 ⇒ 1，判词原样透出来（谁红的、红在哪一行，都在判据自己那句话里）。
run 1 "命中" env PRIVATE_STUB_RC=1 bash "${pub}/deploy/scripts/check-private-names.sh"
grep -q 'STUB_MODE=--tree' <<<"${out}" || fail "命中：判据没被叫到：${out}"

# 3. 判据说判不了（3）⇒ 这一格是没验（75），不是绿也不是红。
run 75 "判不了" env PRIVATE_STUB_RC=3 bash "${pub}/deploy/scripts/check-private-names.sh"

# 4. 判据自己崩了（2）⇒ 照原样传出去。把它也折成 75，就是拿「没验」盖掉一次真异常。
run 2 "崩了" env PRIVATE_STUB_RC=2 bash "${pub}/deploy/scripts/check-private-names.sh"

# 5. 这台机器没配私有仓检出 ⇒ 75，并说清怎么配。
git -C "${pub}" config --unset cogneva.privateRoot
run 75 "没配" bash "${pub}/deploy/scripts/check-private-names.sh"
grep -q '没验' <<<"${out}" || fail "没配：没说这是没验：${out}"
grep -q 'cogneva.privateRoot' <<<"${out}" || fail "没配：没告诉人怎么配：${out}"

# 6. 配了但那份检出不在（仓搬走了）⇒ 75，不是把它当成干净。
git -C "${pub}" config cogneva.privateRoot "${work}/gone"
run 75 "配了但不在" bash "${pub}/deploy/scripts/check-private-names.sh"

# 7. 认第一个参数当被守的仓：这一族（空树普查）会拿一棵空树问每一条门，
#    一个不认参数的门在那里答的是它自己那棵树。被问的那棵不是 git 仓 ⇒ 没验。
mkdir -p "${work}/empty"
run 75 "空树" bash "${pub}/deploy/scripts/check-private-names.sh" "${work}/empty"

# 8. 快照用完就删：跑了三次以上之后，仓里仍然只有主工作树这一个。
listing="$(git -C "${pub}" worktree list | wc -l | tr -d ' ')"
[ "${listing}" = "1" ] || fail "快照没删干净：worktree list 还有 ${listing} 条"

echo "private-names-before-push: 8 格通过（判 HEAD 的树、三种出口分开、快照即删）"
