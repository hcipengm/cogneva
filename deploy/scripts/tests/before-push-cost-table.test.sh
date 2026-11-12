#!/usr/bin/env bash
# 一道两小时的门，说不清时间花在哪一步，就只能整体喊「太慢」。这条用例钉的是那张表本身：
# 每一格都在、按秒数从长到短、红的那一格也在表上。表缺了哪一格，压缩门的时候就会按错的
# 那一格动刀——砍掉零成本的一步，或者砍掉唯一会红的那一步。
#
# 等槽从这一版起有读数了：构建槽客户端每次退出槽都报一行 `<等槽秒>\t<干活秒>\t<名字>`，
# 门据此把一格拆成「等槽」与它自己两行。用例判三件事——两行都在表上、拆开不改变合计、
# 量不到时不拆（整段记在格上，不产出一个负数）。
#
# 一整跑要两小时，而这张表的判据不该跟着两小时一起付，所以门留了个只念段日志的入口；
# 这里喂一份合成的段日志，判的是读法，不是跑法。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-before-push.sh"
# shellcheck source=tests/fixtures/stage-gate.sh
. "${here}/fixtures/stage-gate.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
# 门在开头 source 构建槽客户端；夹具里少了它，判的是一份跑不起来的门。复制整份
# 依赖，让夹具与真树的形状一致——否则这道测试在脚本改过之后仍然绿，而真门一跑就红。
# 这份知识只有一处：门再 source 别的文件时，改的是那个夹具助手。
stage_gate "${repo}" "${work}"

seg="${work}/segments"
printf '900\tok\tTest（整棵工作区）\n' >"${seg}"
printf '61\tok\tClippy\n' >>"${seg}"
printf '4\tRED\tFormat\n' >>"${seg}"
printf '0\tok\tshellcheck -S warning（227 个脚本）\n' >>"${seg}"

out="$(bash "${work}/deploy/scripts/check-before-push.sh" --cost-table "${seg}" 2>&1)" ||
    fail "读段日志退非零：${out}"

# 1) 每一格都在：少一格的表会让「合计」说谎，也会让下一次按错的格子动刀。
for what in 'Test（整棵工作区）' Clippy Format 'shellcheck -S warning（227 个脚本）'; do
    printf '%s\n' "${out}" | grep -qF -- "${what}" || fail "表里少了这一格：${what}；实得：${out}"
done

# 2) 长的在前：这张表是给人排期用的，读的人第一眼该看见最贵的那一格。
first_row="$(printf '%s\n' "${out}" | grep -E '^ *[0-9]+ ' | head -n 1)"
case "${first_row}" in
*900*Test*) ;;
*) fail "最长的那一格没有排在第一个：${first_row}" ;;
esac

# 3) 红的也在表上：红的那一跑同样要知道钱花在哪，而且「哪一格红」必须与秒数同处一行。
printf '%s\n' "${out}" | grep -qE '^ *4 +RED +Format$' ||
    fail "红的那一格没带上记号和秒数；实得：${out}"

# 4) 合计是各格之和：合计算错，排期就按错的总额排。
printf '%s\n' "${out}" | grep -q '合计 965 秒' || fail "合计不对（应为 965）；实得：${out}"

# 5) 空段日志不印表：没有读数时印一张空表，与「零成本」同形。
: >"${seg}"
out="$(bash "${work}/deploy/scripts/check-before-push.sh" --cost-table "${seg}" 2>&1)"
[ -z "${out}" ] || fail "空段日志不该印出表；实得：${out}"

# 6) 等槽单独成行：门慢可以是判据贵，也可以是机器满，两个答案要求的动作相反。一行
#    里混着两种时间，读的人只会去优化编译——而那一半根本不在这一步里。两行都在表上，
#    合计仍是各行之数（拆开不改变总额，只改变看得见什么）。
printf '40\t等槽\tcargo test -p cogneva\n' >"${seg}"
printf '60\tok\tcargo test -p cogneva\n' >>"${seg}"
out="$(bash "${work}/deploy/scripts/check-before-push.sh" --cost-table "${seg}" 2>&1)"
printf '%s\n' "${out}" | grep -qE '^ *40 +等槽 +cargo test -p cogneva$' ||
    fail "等槽的那一段没有单独成行；实得：${out}"
printf '%s\n' "${out}" | grep -qE '^ *60 +ok +cargo test -p cogneva$' ||
    fail "去掉排队之后这一格自己的时长应当还在表上；实得：${out}"
printf '%s\n' "${out}" | grep -q '合计 100 秒' ||
    fail "拆开之后合计仍应当是 100；实得：${out}"

# 7) 行是怎么切出来的：切法只有一处（`lib/gate-run-record.sh` 的 `cell_rows`），判它自己。
#    量不到就不切——整段记在格上，也不产出一个负数或者一个凭空减掉的时间。
# shellcheck source=../lib/gate-run-record.sh
. "${repo}/deploy/scripts/lib/gate-run-record.sh"
rows_of() { cell_rows "$@" | cut -f1,2 | tr '\t' ' ' | tr '\n' ' '; }
[ "$(rows_of 100 0 ok X)" = "100 ok " ] || fail "没有等槽时不该拆：$(rows_of 100 0 ok X)"
[ "$(rows_of 100 40 ok X)" = "40 等槽 60 ok " ] ||
    fail "有等槽时要拆成两行且加起来仍是一整段：$(rows_of 100 40 ok X)"
[ "$(rows_of 100 200 ok X)" = "100 ok " ] ||
    fail "等得比这一格还久说明两个读数的口径对不上，按量不到处理：$(rows_of 100 200 ok X)"
[ "$(rows_of 100 '' ok X)" = "100 ok " ] || fail "空值按量不到处理：$(rows_of 100 '' ok X)"
[ "$(rows_of 100 abc ok X)" = "100 ok " ] || fail "非数字按量不到处理：$(rows_of 100 abc ok X)"

# 8) 门不许绕过它去记整段：`record_segment "$((SECONDS - t0))"` 再出现一次，就是「排队
#    又混进了格子」，而那正是这一条落地时要消失的形状。
if grep -Eq 'record_segment "\$\(\(SECONDS - t0\)\)"' "${gate}"; then
    fail "门里还有一处把整段记成格子，排队没被拆出去：$(grep -n 'record_segment "\$((SECONDS - t0))"' "${gate}")"
fi
grep -q 'record_cell' "${gate}" || fail "门没有用 record_cell 记格子"
grep -q 'COGNEVA_BUILD_SLOT_REPORT' "${gate}" ||
    fail "门没有让构建槽客户端报等槽秒数，那一列永远是空的"

echo "ok - before-push 的成本表：每格都在、长的在前、红的带记号、合计是各格之和，空日志不印表，等槽单独成行且格子不再含排队"
