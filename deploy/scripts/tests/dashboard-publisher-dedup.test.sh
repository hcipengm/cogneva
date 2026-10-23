#!/usr/bin/env bash
# principle: feedback_a_snapshot_published_by_several_processes_is_deduped_by_publisher_not_by_label_cell
# 快照型指标的去重口径：按**发布者**取一份完整快照，不按**标签格**取最大再拼。
#
# 为什么这条要单独钉一次：一个指标由多个 Pod 各报一份完整快照时，两种写法在**发布者一致**
# 的每一拍上给出同一个答案，于是所有常规读数都绿着——差别只在发布者不一致的那一拍出现，
# 而那一拍恰恰是任务账在动的证据。按状态格取最大会把两个 Pod 各自的半条读数拼成一个
# **谁都没报过**的数：A 报 pending=2037/running=1、B 报 pending=2038/running=0，
# 逐格取最大得 2038 与 1，相加是 2039，而那本账里只有 2038 行。
#
# 判据两半：面板必须点明**画的是某一个发布者的整份快照**（`on (instance)`＋按发布者总量
# 选一个），且树里不许再有对这个指标的逐格取最大。第三段是反向控制：把面板改回逐格取
# 最大，同一组判据必须红——不然这份测试只是在读自己写下的那个字符串。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
dash="${repo}/deploy/k3s/observability/manifests/06-grafana-dashboard-configmap.yaml"
pass=0
fail=0
ok() { printf 'ok    %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL  %s\n' "$1"; fail=$((fail + 1)); }

[ -f "$dash" ] || { printf 'FAIL  面板文件不在：%s\n' "$dash"; exit 1; }

# 判一份面板文本：绿的条件有两条，缺一条就红。$1 = 路径，$2 = 这一份叫什么
judge() {
    local path="$1" what="$2"
    grep -qF 'cogneva_task_unowned_rows and on (instance) topk (1, sum by (instance) (cogneva_task_unowned_rows))' "$path" \
        && ok "$what：画的是某一个发布者的整份快照" \
        || bad "$what：没有点明画的是哪个发布者（缺 on (instance) 或按发布者总量选一个）"
    if grep -qE 'max by \(status\) \(cogneva_task_unowned_rows\)' "$path"; then
        bad "$what：仍在对这个指标逐格取最大（两发布者的分歧会被拼成第三个值）"
    else
        ok "$what：树里没有对这个指标的逐格取最大"
    fi
}

judge "$dash" "真面板"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
# 反向控制：把快照口径拆掉，换回那条逐格取最大的老写法。
sed 's/cogneva_task_unowned_rows and on (instance) topk (1, sum by (instance) (cogneva_task_unowned_rows))/max by (status) (cogneva_task_unowned_rows)/' \
    "$dash" >"${work}/reverted.yaml"
grep -qF 'max by (status) (cogneva_task_unowned_rows)' "${work}/reverted.yaml" \
    || bad "反向控制：夹具没改成功（sed 没命中），这一段什么都没测"
# 在子 shell 里判：它的 ok/bad 计数不该混进这一份测试自己的账，这里只关心那两条判据
# 在这一份上**同时**红——只要有一条还绿，说明判据读的不是这里。
control="$( judge "${work}/reverted.yaml" "改回逐格取最大的那份" )"
if [ "$(printf '%s\n' "$control" | grep -c '^FAIL')" -eq 2 ] \
    && ! printf '%s\n' "$control" | grep -q '^ok'; then
    ok "反向控制：改回去两条判据都红（这一组不是空转）"
else
    bad "反向控制：改回去没有两条全红，判据读的不是这里"
    printf '%s\n' "$control" | sed 's/^/      /' >&2
fi

printf '\n通过 %d，失败 %d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
