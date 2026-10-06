#!/usr/bin/env bash
# check-alert-rule-subjects.sh 的定向测试：判词必须由**表达式结构**推出来，
# 而且三种失败方向各自都要点名到自己那一条。
#
# 为什么要有这份测试：这条门禁的读数规则是「最外层聚合的 by/without 决定结果标签集」
# 那一族的文本推理，而这类推理**改坏了不会报错**——它只会让整张表的判词系统性偏松或者
# 偏严，报告的条数照出。前身就栽过一次：切片多带一个右括号，凡被函数包起来的表达式
# 一律误报（45 条读成 36 条），而当时的两个自检恰好都是裸选择器，全绿。所以这里每条
# 方向都断言**具体那句判词**，不只断言退出码。
#
# 三个方向，每条都靠改动真规则文件的一份副本造出来（控制集与豁免表里的名字因此都还在，
# 每个用例只隔离一件事）：
#   1. 真文件 -> 绿；
#   2. 一条普通规则丢掉聚合 -> 红，并点名它；
#   3. 豁免表 / 控制集里点名了一条**不存在**的规则 -> 红（豁免与规则一起过期）；
#   4. 一条已收敛规则的两侧配不上 -> 红（C78：合法但产不出序列）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-subjects.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

python3 - "${config}" "${work}" <<'PYEOF'
import json, sys
src, out = sys.argv[1], sys.argv[2]
doc = json.load(open(src, encoding='utf-8'))
rules = doc['observability']['infra_watch']['rules']
by_name = {r['name']: r for r in rules}

# 判词里那句条数由这份配置自己算出来：钉死一个字面量的话，每加一条规则这份
# 测试就会红一次，而它红的是它自己那份抄本，不是门禁坏了。
open(f'{out}/rule_count', 'w', encoding='utf-8').write(str(len(rules)))

def dump(name, rules):
    copy = json.loads(json.dumps(doc))
    copy['observability']['infra_watch']['rules'] = list(rules)
    json.dump(copy, open(f'{out}/{name}.json', 'w', encoding='utf-8'))

# 2. 丢掉聚合：原式是 sum by (upstream) (...)，换成裸选择器
changed = json.loads(json.dumps(rules))
for r in changed:
    if r['name'] == 'llm_calls_all_failing':
        r['promql'] = 'llm_calls_total{result="error"}'
dump('unreduced', changed)

# 3a. 豁免表点名了一条不存在的规则
dump('stale_exemption', [r for r in rules if r['name'] != 'pod_oom_killed'])
# 3b. 控制集点名了一条不存在的规则
dump('stale_control', [r for r in rules if r['name'] != 'registry_store_walk_failing'])

# 4. 已收敛，但两侧配不上：左侧带标签的向量，右侧 12 * 裸聚合（没有标签）
paired = json.loads(json.dumps(rules))
for r in paired:
    if r['name'] == 'background_loop_restarted':
        r['promql'] = ('sum by (loop) (cogneva_loop_role_declared)'
                       ' > 12 * max(cogneva_loop_owner_held)')
dump('unpaired', paired)
PYEOF

run() { # run <config> -> 打印合并输出，回传退出码
  local out
  if out="$(bash "${gate}" "$1" 2>&1)"; then
    printf '%s\n' "${out}"; return 0
  else
    printf '%s\n' "${out}"; return 1
  fi
}

# 1. 真文件必须绿，并说出它读了多少条
out="$(run "${config}")" || fail "真规则文件被判红：${out}"
grep -q 'ALERT SUBJECT OK' <<<"${out}" || fail "绿的时候也要报判词：${out}"
grep -q "规则 $(cat "${work}/rule_count") 条" <<<"${out}" || fail "没有把读到的条数报出来：${out}"

# 2. 丢掉聚合 -> 红，且点名那条规则
if out="$(run "${work}/unreduced.json")"; then fail "丢掉聚合的规则被判绿：${out}"; fi
grep -q 'llm_calls_all_failing' <<<"${out}" \
  || fail "判红却没点名那条规则：${out}"
grep -q 'LEAF' <<<"${out}" \
  || fail "判词要给出没收敛的那段子表达式：${out}"

# 3a. 豁免表过期 -> 红（静默放行的正是修完又退回去的那一条）
if out="$(run "${work}/stale_exemption.json")"; then fail "豁免表点名不存在的规则却判绿：${out}"; fi
grep -q '豁免表里的 pod_oom_killed 不在这份规则里' <<<"${out}" \
  || fail "豁免过期的判词不对：${out}"

# 3b. 控制集过期 -> 红
if out="$(run "${work}/stale_control.json")"; then fail "控制集点名不存在的规则却判绿：${out}"; fi
grep -q '控制集里的 registry_store_walk_failing 不在这份规则里' <<<"${out}" \
  || fail "控制集过期的判词不对：${out}"

# 4. 两侧配不上 -> 红，且说清是哪一侧
if out="$(run "${work}/unpaired.json")"; then fail "两侧配不上的规则被判绿：${out}"; fi
grep -q 'background_loop_restarted' <<<"${out}" \
  || fail "配不上的判词没点名规则：${out}"
grep -q 'label-less right side against a labelled left side' <<<"${out}" \
  || fail "配不上的判词没说清哪一侧：${out}"

# 少了参数以外的路径不存在时，是依赖错误不是判词
if bash "${gate}" "${work}/does-not-exist.json" >/dev/null 2>&1; then
  fail "文件不存在却当成功返回"
fi

echo "ALERT SUBJECT GATE TESTS OK"
