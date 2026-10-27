#!/usr/bin/env bash
# `cluster_read_failed` 那份夹具**自己**能不能红。
#
# 为什么要有这一层：夹具判的是「集群读失败会不会变成一条告警」，而它自己也可能是
# 一枚橡皮图章——把界写成永远够不着的数、把窗口收成一次快照、把规则删掉，只要夹具
# 没跟着红，那一格就什么都没测。三处改坏，每处都必须红在**该红的那一格**：
#   1. 界抬到十亿：第 2 格（pods 连失败四十分钟）必须红——否则一条永远不响的规则也绿；
#   2. 去掉 `increase()` 只留原始计数：第 3 格（失败集中在 0–20 分钟、之后一直好）必须
#      红——原始计数在 60 分钟时还停在 20，只有窗口里的增量才会归零，这一格就是
#      「窗口真的在滚」的证据；
#   3. 把规则整条删掉：跑者必须非零，而不是「夹具点名的规则不在库里所以没判」。
#
# 每一处改坏前都先断言副本真的变了：改不动的夹具是「什么都没测」的另一种写法。
#
# 跑法：bash deploy/scripts/tests/cluster-read-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
fixture="${repo}/deploy/k3s/observability/tests/cogneva-json-cluster-read.test.yaml"

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

run() { RULES_MANIFEST="$1" RULES_CASE="$fixture" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

# 按一段 python 改坏那一库的副本。$1=输出文件 $2=要执行的片段
mutate() {
    python3 - "$config" "$1" "$2" <<'PY'
import json
import sys

src, out, snippet = sys.argv[1:4]
config = json.load(open(src, encoding="utf-8"))
rules = config["observability"]["infra_watch"]["rules"]
rule = next(r for r in rules if r["name"] == "cluster_read_failed")
exec(snippet)
json.dump(config, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY
}

# expect_red <用例名> <副本>：副本必须与真文件不同，且跑者必须非零。
expect_red() {
    local label="$1" copy="$2"
    if cmp -s "$config" "$copy"; then
        bad "${label}：副本没变（改不动）" "与真文件不同" "逐字相同"
        return
    fi
    run "$copy"
    if [ "$rc" -ne 0 ]; then
        ok "$label"
    else
        bad "${label}：仍然绿——夹具不判这一半" "rc!=0" "rc=0"
    fi
}

# 0) 真配置必须绿。
run "$config"
if [ "$rc" -eq 0 ]; then
    ok "真配置全绿"
else
    bad "这一条规则此刻是红的" "rc=0" "rc=$rc"
    sed 's/^/        /' "${work}/out" | tail -20 >&2
    exit 1
fi

# 1) 界抬到十亿：firing 那一格必须红。
copy="${work}/threshold.json"
mutate "$copy" 'rule["condition"]={"greater_than": 1e9}'
expect_red "界抬到十亿，firing 那格红了" "$copy"

# 2) 去掉 increase()：窗口不再滚动，第 3 格（期望静）必须红。
copy="${work}/no-window.json"
mutate "$copy" '
old = "increase(cogneva_cluster_read_total{outcome=\"failed\"}[15m])"
assert rule["promql"].count(old) == 1, rule["promql"]
rule["promql"] = rule["promql"].replace(old, "cogneva_cluster_read_total{outcome=\"failed\"}")
'
expect_red "去掉窗口聚合，滚动那格红了" "$copy"

# 3) 删掉整条规则：跑者必须非零，而不是「没取到规则所以没判」。
copy="${work}/dropped.json"
mutate "$copy" 'rules.remove(rule)'
expect_red "规则被删，跑者非零" "$copy"

printf '\n通过 %d，失败 %d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
