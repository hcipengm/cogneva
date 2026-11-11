#!/usr/bin/env bash
# 一跑门的段记录写在哪儿、怎么收尾、怎么不越积越多。
#
# 这条用例钉的是那份记录本身，不是「跑一遍门看看」：一整跑两小时，而这里的每一条
# 判据都是毫秒级的。记录被页面读成「最近一跑」，所以它有三件事不能错：
#
#   1. 形状与成本表同源：三列（秒、结局、说明）。换一种格式，网关就得再认一种，
#      而两份格式迟早会对不上——正是「又造了一份台账」的形状。
#   2. 收尾与否要读得出来：没收尾的那一份留在 `.running` 上，别改名。改名是唯一的
#      收尾记号，不另写终止行。
#   3. 每种树只留最近一跑：读者只读最新的那一份，留更多只是让宿主上多出没人读的文件；
#      但别人那棵树的记录一格都不许动—— prune 的范围写错，砍掉的就是别人的读数。
#   4. 权限不跟着跑门的 shell 走：读的人不是写的人（Pod 里 uid 1001）。umask 收成 077
#      时若跟着 0700/0600，Pod 那边读到的是「记录在、内容读不动」，与「还没人写过」同形。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
lib="${repo}/deploy/scripts/lib/gate-run-record.sh"
gate="${repo}/deploy/scripts/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${lib}" ] || fail "记录库不存在：${lib}"
# shellcheck source=../lib/gate-run-record.sh
. "${lib}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 1) 名字认得出是哪棵树，路径分隔符不许留在文件名里。
tag="$(gate_run_tag "/home/cogneva/work/wt-emp2")"
[ "${tag}" = "home_cogneva_work_wt-emp2" ] || fail "标签不是路径本身：${tag}"

# 2) 开一跑：文件在，是 `.running`，空着。
file="$(gate_run_begin "${work}/gate" "/home/cogneva/work/wt-emp2" gate 1700000000 4242)"
[ -n "${file}" ] || fail "开不了一跑（目录建不出？）"
[ -f "${file}" ] || fail "记录文件不在：${file}"
case "${file}" in *.running) ;; *) fail "没收尾的那一份必须是 .running：${file}" ;; esac

# 3) 记三格：形状就是成本表那种三列，原样喂给 `--cost-table` 要能印出来。
gate_run_append "${file}" 900 ok "Test（整棵工作区）"
gate_run_append "${file}" 4 RED Format
gate_run_append "${file}" 61 ok Clippy
out="$(bash "${gate}" --cost-table "${file}" 2>&1)" || fail "成本表读不了这份记录：${out}"
printf '%s\n' "${out}" | grep -qE '^ *900 +ok +Test' || fail "记录与成本表的读法对不上：${out}"
printf '%s\n' "${out}" | grep -qE '^ *4 +RED +Format$' || fail "红的那一格没带上结局：${out}"

# 4) 收尾：改名，不写终止行。收尾之后还是那三列，读数不变。
gate_run_finish "${file}"
done_file="${file%.running}.segments"
[ -f "${done_file}" ] || fail "收尾之后不在 .segments 上：${done_file}"
[ ! -e "${file}" ] || fail "收尾之后还留着 .running：${file}"
[ "$(wc -l <"${done_file}")" -eq 3 ] || fail "收尾往记录里多写了一行：$(cat "${done_file}")"

# 5) 同一棵树再开一跑：开跑的那一刻，上一跑的结果还在（读者两边都读得到），
#    这一跑收尾之后，被它取代的那份才收掉——盘上永远只有一份收尾记录。
second="$(gate_run_begin "${work}/gate" "/home/cogneva/work/wt-emp2" gate 1700009999 5252)"
[ -n "${second}" ] || fail "第二跑开不了"
[ -f "${done_file}" ] || fail "第二跑一开就把上一跑的结果收掉了：${done_file}"
gate_run_append "${second}" 7 ok Format
gate_run_finish "${second}"
[ ! -e "${done_file}" ] || fail "第二跑收尾之后，被取代的那份还留着：${done_file}"

# 6) 别的树的记录一格都不许动——prune 的范围写错，砍掉的就是别人的读数。
#    第二跑那份留着，第三跑开跑时只该动本树的，不该动别的树。
other="$(gate_run_begin "${work}/gate" "/home/cogneva/work/wt-other" gate 1700001111 6363)"
gate_run_append "${other}" 5 ok Format
gate_run_finish "${other}"
third="$(gate_run_begin "${work}/gate" "/home/cogneva/work/wt-emp2" gate 1700019999 7373)"
[ -f "${other%.running}.segments" ] || fail "别的树的记录被 prune 动了：${other%.running}.segments"
[ -n "${third}" ] || fail "第三跑开不了"

# 7) 写不进去不拦这一跑：目录建不出来时打空，调用方照常判。
blocked="${work}/blocked"
: >"${blocked}"
empty="$(gate_run_begin "${blocked}" "/home/cogneva/work/wt-emp2" gate 1 1)"
[ -z "${empty}" ] || fail "目录不可用时还打了一个路径：${empty}"

# 8) 权限由记录自己钉死，不由跑门的 shell 决定：目录 0755、文件 0644。
#    不然一个 `umask 077` 的 shell 就让 Pod 读不到——它看到的是「文件在、读不动」。
tight="${work}/tight"
( umask 077; gate_run_begin "${tight}" "/home/cogneva/work/wt-emp2" gate 1700029999 8484 >/dev/null )
[ "$(stat -c %a "${tight}/runs")" = "755" ] \
    || fail "runs/ 的权限跟着 umask 走了：$(stat -c %a "${tight}/runs")"
tight_mode="$(stat -c %a "${tight}/runs/"*.running)"
[ "${tight_mode}" = "644" ] || fail "记录的权限跟着 umask 走了：${tight_mode}"

# 9) 阶段是身份的一部分：同一棵树的提交前钩子与整门是两段，互相 prune 不到。
pre="$(gate_run_begin "${work}/gate" "/home/cogneva/work/wt-emp2" precommit 1700050000 9595)"
gate_run_append "${pre}" 3 ok "10-registered-producer-paths"
gate_run_finish "${pre}"
[ -f "${pre%.running}.segments" ] || fail "提交前钩子那一段没落成 .segments：${pre}"
third_done="$(ls "${work}/gate/runs/home_cogneva_work_wt-emp2-gate-"*.segments | head -1)"
[ -n "${third_done}" ] || fail "整门那一段被提交前钩子的 prune 收掉了"
case "${pre}" in
*"-precommit-"*) ;;
*) fail "阶段没进文件名：${pre}" ;;
esac

# 10) 认不出的阶段不记：第四个词会被读的那一侧当成树的另一半，记到错名字下比没有更坏。
unknown="$(gate_run_begin "${work}/gate" "/home/cogneva/work/wt-emp2" precommt 1700060000 9696)"
[ -z "${unknown}" ] || fail "认不出的阶段也落了记录：${unknown}"

# 11) 被拦下的那一段也要收尾：`exit 1` 之前不留下 `.running`，读者就不会以为它还在跑。
#     放在子壳里跑：wrap 的 trap 装在那个壳上，壳退出时触发，不会顶掉本用例自己的清理。
wrapgate="${work}/wrapgate"
( gate_run_wrap "${wrapgate}" "/home/cogneva/work/wt-emp2" prepush >/dev/null
  exit 1 ) || true
ls "${wrapgate}/runs/"*.segments >/dev/null 2>&1 \
    || fail "被拦下的那一段没收尾：$(ls "${wrapgate}/runs/" 2>&1)"
ls "${wrapgate}/runs/"*.running >/dev/null 2>&1 \
    && fail "被拦下的那一段还挂在 .running 上：$(ls "${wrapgate}/runs/")"

echo "ok  gate-run-record"
