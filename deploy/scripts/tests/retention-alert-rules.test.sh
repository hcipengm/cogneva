#!/usr/bin/env bash
# 两张账的保留期规则（任务事件档案 / token 账）那几份夹具**自己**能不能红。
#
# 为什么要有这一层：`cogneva-json-task-event-retention.test.yaml` 与
# `cogneva-json-llm-usage-retention.test.yaml` 判的是「这些规则会不会响」，而它们自己也
# 可能是一枚橡皮图章——把界写错、把新鲜度腿删掉、把成对的判据砍成一半、把规则删掉，
# 只要夹具没跟着红，那一格就什么都没测。这里对两份夹具各做四件事，每次都必须红在
# **该红的那一格**：
#   1. 界抬到十亿：它那条 firing 断言必须红——否则一条永远不响的规则也绿；
#   2. 新鲜度腿放行到十亿秒：把读数时间戳钉在 0 的那一格（期望静）必须红——否则
#      「读数早就停了」会被当成「读数说没事」，而那正是这张表最容易被读反的一格；
#   3. token 账那份额外把「删不动的行有多老」那半砍掉（改成 `> 0`）：回填正在追的
#      那一格（期望静）必须红——这一格存在的理由就是「计数单独不足以判」，
#      砍掉年龄那半它若不红，那一格就是装饰；
#   4. 把一条规则整条删掉：跑者必须非零，而不是「夹具点名的规则不在库里所以没判」。
#
# 每一处改坏前都先断言副本真的变了：改不动的夹具是「什么都没测」的另一种写法。
# 前两件对两份夹具都成立，因为它们用的是同一种形状（读数对读数、新鲜度腿、滚动窗口
# 计数），这正是两份夹具可以共用一个跑者的原因。
#
# 跑法：bash deploy/scripts/tests/retention-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
cases="${repo}/deploy/k3s/observability/tests"

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

run() { RULES_MANIFEST="$1" RULES_CASE="$2" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

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

# expect_red <用例名> <副本> <夹具>：副本必须与真文件不同，且跑者必须非零。
expect_red() {
    local label="$1" copy="$2" case_file="$3"
    if cmp -s "$config" "$copy"; then
        bad "${label}：副本没变（改不动）" "与真文件不同" "逐字相同"
        return
    fi
    run "$copy" "$case_file"
    if [ "$rc" -ne 0 ]; then
        ok "$label"
    else
        bad "$label：仍然绿——夹具不判这一半" "rc!=0" "rc=0"
    fi
}

# check_fixture <夹具文件名> <保留期那条规则名> <新鲜度腿的读数名> <可选：第四条改坏>
check_fixture() {
    local fixture="$1" behind_rule="$2" age_series="$3"
    local case_file="${cases}/${fixture}"

    # 0) 真文件必须绿。
    run "$config" "$case_file"
    if [ "$rc" -eq 0 ]; then
        ok "${fixture}：真配置全绿"
    else
        bad "${fixture}：这一族规则此刻是红的" "rc=0" "rc=$rc"
        sed 's/^/        /' "${work}/out" | tail -20 >&2
        return
    fi

    # 1) 界抬到十亿：firing 那一格必须红。
    local copy="${work}/${behind_rule}-threshold.json"
    mutate "$copy" "$behind_rule" 'rule["condition"]={"greater_than": 1e9}'
    expect_red "${fixture}：界抬到十亿，firing 那格红了" "$copy" "$case_file"

    # 2) 新鲜度腿放行到十亿秒：冻结读数那一格（期望静）必须红。
    copy="${work}/${behind_rule}-stale.json"
    mutate "$copy" "$behind_rule" \
        'rule["promql"]=rule["promql"].replace("< 1800", "< 1000000000")'
    expect_red "${fixture}：新鲜度腿失效，冻结读数那格红了" "$copy" "$case_file"

    # 3) 删掉整条规则：跑者必须非零，而不是「没取到规则所以没判」。
    copy="${work}/${behind_rule}-dropped.json"
    mutate "$copy" "$behind_rule" 'rules.remove(rule)'
    expect_red "${fixture}：规则被删，跑者非零" "$copy" "$case_file"
}

check_fixture cogneva-json-task-event-retention.test.yaml \
    task_event_archive_behind cogneva_task_events_oldest_age_seconds
check_fixture cogneva-json-llm-usage-retention.test.yaml \
    llm_usage_retention_behind cogneva_llm_usage_oldest_age_seconds

# 4) token 账独有：把「删不动的行有多老」那半砍掉，回填正在追那一格（期望静）必须红。
copy="${work}/fold-age.json"
mutate "$copy" llm_usage_fold_behind '
old = "> max without (pod, container, instance) (cogneva_llm_usage_keep_seconds and ((time() - cogneva_llm_usage_keep_seconds_observed_timestamp_seconds) < 1800))"
assert rule["promql"].count(old) == 1, rule["promql"]
rule["promql"] = rule["promql"].replace(old, "> 0")
'
expect_red "token 账：fold_behind 砍掉年龄那半，回填追赶那格红了" "$copy" \
    "${cases}/cogneva-json-llm-usage-retention.test.yaml"

printf '\n通过 %d，失败 %d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
