#!/usr/bin/env bash
# 任务管理面那三条告警的夹具**自己**能不能红。
#
# 为什么要有这一层：`cogneva-json-tasks-face.test.yaml` 判的是「这三条会不会响」，
# 而它自己也可能是一枚橡皮图章——把界写错、把选择器写丢、把规则删掉，只要夹具没跟着红，
# 那一格就什么都没测。这里把部署配置的副本改坏五次，每次都必须红在**该红的那一格**：
#   1. 第一条的界 3600 抬到 1e9：它那条 firing 断言必须红——否则一条永远不响的规则也绿；
#   2. 第一条丢掉 `{agent="unassigned"}`：第一格（有主的活等了两小时）必须红——那一格判的
#      是选择器；只判阈值的夹具留不住它；
#   3. 第二条的界 900 抬到 1e9：它那条 firing 断言必须红；
#   4. 第三条的条件抬到 5（缺席那一下只返回 1）：它那条 firing 断言必须红；
#   5. 把第三条整条删掉：跑者必须非零而不是「夹具点名的规则不在库里所以没判」。
#
# 每一处改坏前先断言副本真的变了：改不动的夹具是「什么都没测」的另一种写法。
#
# 跑法：bash deploy/scripts/tests/tasks-face-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
case_file="${repo}/deploy/k3s/observability/tests/cogneva-json-tasks-face.test.yaml"

pass=0
fail=0
ok() { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() {
    fail=$((fail + 1))
    printf 'FAIL- %s\n' "$1"
    [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"
    [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { RULES_MANIFEST="$1" RULES_CASE="${2:-$case_file}" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

# 把那一库的副本按一段 python 改坏。$1=输出文件 $2=规则名 $3=拿到 rule 后要执行的片段
mutate() {
    python3 - "$config" "$1" "$2" "$3" <<'PY'
import json
import sys

src, out, name, snippet = sys.argv[1:5]
config = json.load(open(src, encoding="utf-8"))
rules = config["observability"]["infra_watch"]["rules"]
rule = next(r for r in rules if r["name"] == name)
exec(snippet)
json.dump(config, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY
}

# 0) 真文件必须绿。
run "$config"
if [ "$rc" -eq 0 ]; then
    ok "真配置：三条断言全绿"
else
    bad "真配置：这一族规则此刻是红的" "rc=0" "rc=$rc"
    sed 's/^/        /' "${work}/out" | tail -20 >&2
fi

# 1) 界抬高：firing 那一格必须红。
mutate "${work}/threshold.json" unowned_task_untouched_past_pickup 'rule["condition"]={"greater_than": 1e9}'
if cmp -s "$config" "${work}/threshold.json"; then
    bad "界抬高：副本没变（改不动）" "与真文件不同" "逐字相同"
else
    run "${work}/threshold.json"
    [ "$rc" -ne 0 ] && ok "界抬高到十亿：firing 那一格红了" || bad "界抬高到十亿：仍然绿——这条断言不判界"
fi

# 2) 丢掉选择器：有主的那格必须红（它判的是桶，不是阈值）。
mutate "${work}/selector.json" unowned_task_untouched_past_pickup \
    'rule["promql"]=rule["promql"].replace("{agent=\"unassigned\"}", "")'
if cmp -s "$config" "${work}/selector.json"; then
    bad "丢选择器：副本没变（改不动）" "与真文件不同" "逐字相同"
else
    run "${work}/selector.json"
    [ "$rc" -ne 0 ] && ok "丢掉 unassigned 选择器：有主那格红了" || bad "丢掉选择器：仍然绿——夹具不判桶"
fi

# 3) 第二条的界抬高。
mutate "${work}/stale.json" task_graph_measure_stale 'rule["condition"]={"greater_than": 1e9}'
if cmp -s "$config" "${work}/stale.json"; then
    bad "读图停住那条：副本没变" "与真文件不同" "逐字相同"
else
    run "${work}/stale.json"
    [ "$rc" -ne 0 ] && ok "读图停住那条界抬高：firing 那格红了" || bad "读图停住那条界抬高：仍然绿"
fi

# 4) 第三条的条件抬到缺席返回不到的数。
mutate "${work}/absent.json" task_graph_measure_absent 'rule["condition"]={"greater_than": 5.0}'
if cmp -s "$config" "${work}/absent.json"; then
    bad "缺席那条：副本没变" "与真文件不同" "逐字相同"
else
    run "${work}/absent.json"
    [ "$rc" -ne 0 ] && ok "缺席那条条件抬高：firing 那格红了" || bad "缺席那条条件抬高：仍然绿"
fi

# 5) 删掉第三条：夹具点名了库里没有的规则，跑者必须非零。
mutate "${work}/dropped.json" task_graph_measure_absent 'rules.remove(rule)'
if cmp -s "$config" "${work}/dropped.json"; then
    bad "删规则：副本没变" "与真文件不同" "逐字相同"
else
    run "${work}/dropped.json"
    [ "$rc" -ne 0 ] && ok "夹具点名的规则被删：跑者非零" || bad "规则被删仍然零——夹具的死指针没被抓住"
fi

printf '\n通过 %d，失败 %d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
