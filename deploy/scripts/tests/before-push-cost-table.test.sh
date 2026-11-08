#!/usr/bin/env bash
# 一道两小时的门，说不清时间花在哪一步，就只能整体喊「太慢」。这条用例钉的是那张表本身：
# 每一格都在、按秒数从长到短、红的那一格也在表上。表缺了哪一格，压缩门的时候就会按错的
# 那一格动刀——砍掉零成本的一步，或者砍掉唯一会红的那一步。
#
# 「等了多久、等谁」这一面这里不判，因为它现在**没有读数**：格子直接调 cargo，排队在
# cargo 里面。表上的一格是「这一格花了多少秒」，不是「编译了多少秒」——用例把这一点也
# 钉住，免得下一次有人拿它当编译成本。
#
# 一整跑要两小时，而这张表的判据不该跟着两小时一起付，所以门留了个只念段日志的入口；
# 这里喂一份合成的段日志，判的是读法，不是跑法。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/deploy/scripts"
cp "${gate}" "${work}/deploy/scripts/check-before-push.sh"

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

echo "ok - before-push 的成本表：每格都在、长的在前、红的带记号、合计是各格之和，空日志不印表"
