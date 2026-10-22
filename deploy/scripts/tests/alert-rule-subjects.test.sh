#!/usr/bin/env bash
# principle: feedback_a_row_per_pod_life_is_not_an_incident
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
#   4. 一条已收敛规则的两侧配不上 -> 红（C78：合法但产不出序列）；
#   5. 两参形态（`quantile_over_time(0.99, v)` / `topk(5, v)`）按**第二个实参**判：
#      第二个实参收敛 -> 绿；第二个实参是裸选择器 -> 红（不能因为「函数有两个实参」
#      就一律算收敛——那样一条身份随 Pod 走的 topk 规则会静默通过）。
#   6. 缺席函数（`absent`/`absent_over_time`）的身份只来自实参选择器的**等值**匹配器：
#      选择器里点名 Pod -> 红；选择器只带常量标签、或实参是聚合 -> 绿。
#   7. 摘要里以**指路口吻**点名了停用名单里的名字 -> 红（读者被派去找一条不存在的
#      规则）；同一个名字以**裸名字**出现 -> 绿（判据取的是出现形式，不是裸子串：
#      停用名单那一行本身就是判据的依据，沿革注释与夹具另有正当的裸出现）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-subjects.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

manifests="${repo}/deploy/k3s/observability/manifests"
python3 - "${config}" "${work}" "${manifests}" <<'PYEOF'
import json, sys
src, out = sys.argv[1], sys.argv[2]
doc = json.load(open(src, encoding='utf-8'))
rules = doc['observability']['infra_watch']['rules']
by_name = {r['name']: r for r in rules}

# 判词里那句条数由这份配置自己算出来：钉死一个字面量的话，每加一条规则这份
# 测试就会红一次，而它红的是它自己那份抄本，不是门禁坏了。
open(f'{out}/rule_count', 'w', encoding='utf-8').write(str(len(rules)))

# manifest 侧的条数同理由树算出：写死会让「加一条 manifest 规则」红在这份抄本上
import glob, os, yaml
mcount = 0
for f in sorted(glob.glob(os.path.join(sys.argv[3], '*.yaml'))):
    # 循环变量不能叫 doc：外层那份 chart JSON 也叫 doc，被覆盖之后 dump() 读到的是
    # 最后一份 manifest 文档，报一个与判词无关的 KeyError。
    for mdoc in yaml.safe_load_all(open(f, encoding='utf-8')):
        if isinstance(mdoc, dict) and mdoc.get('kind') == 'PrometheusRule':
            for g in (mdoc.get('spec') or {}).get('groups') or []:
                for r in g.get('rules') or []:
                    if r.get('alert') or r.get('record'):
                        mcount += 1
open(f'{out}/manifest_rule_count', 'w', encoding='utf-8').write(str(mcount))

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

# 5. 两参形态按第二个实参判。同一个函数名，只换第二个实参：
#    收敛的第二个实参 -> 整条收敛；裸选择器 -> 未收敛。
twoparam_ok = json.loads(json.dumps(rules))
twoparam_bad = json.loads(json.dumps(rules))
for r in twoparam_ok:
    if r['name'] == 'background_loop_restarted':
        r['promql'] = ('quantile_over_time(0.99, (sum by (loop)'
                       ' (cogneva_loop_role_declared))[30m:1m])')
for r in twoparam_bad:
    if r['name'] == 'background_loop_restarted':
        r['promql'] = 'topk(5, cogneva_loop_role_declared)'
dump('twoparam_ok', twoparam_ok)
dump('twoparam_bad', twoparam_bad)

# 6. 缺席函数：身份只来自实参选择器的**等值**匹配器，不是从被选序列继承的。
#    三个用例各隔离一件事——点名了 Pod 的选择器（红）、没点名 Pod 的选择器（绿）、
#    实参是聚合（绿，实测结果为空标签，也正是 runtime_asset_check_never_ran 的形态）。
#    真文件里那条 `absent(...{readout="..."})` 由用例 1 钉住：把它再读回成区间函数，
#    用例 1 立刻红。
absent_pod = json.loads(json.dumps(rules))
absent_plain = json.loads(json.dumps(rules))
absent_agg = json.loads(json.dumps(rules))
for r in absent_pod:
    if r['name'] == 'background_loop_restarted':
        r['promql'] = 'absent(cogneva_loop_role_declared{pod="p1"})'
for r in absent_plain:
    if r['name'] == 'background_loop_restarted':
        r['promql'] = 'absent(cogneva_loop_role_declared{loop="owner"})'
for r in absent_agg:
    if r['name'] == 'background_loop_restarted':
        r['promql'] = 'absent(max without (pod, container) (cogneva_loop_role_declared))'
dump('absent_pod', absent_pod)
dump('absent_plain', absent_plain)
dump('absent_agg', absent_agg)

# 7. 摘要里点名了一个**已停用**的名字。判据取反引号形态（散文的写法），所以同一个
#    名字的裸出现必须仍然是绿的——停用名单那一行就是裸名字，判据要是取裸子串，
#    它会把判据自己的依据一起判红。
retired_name = doc['observability']['infra_watch'].get('retired_rules', [None])[0]
if not retired_name:
    raise SystemExit('配置里没有停用名单，这一族用例无从构造')

prose_quoted = json.loads(json.dumps(rules))
prose_bare = json.loads(json.dumps(rules))
for r in prose_quoted:
    if r['name'] == 'background_loop_restarted':
        r['summary'] = r['summary'] + f' 把它与 `{retired_name}` 并读。'
for r in prose_bare:
    if r['name'] == 'background_loop_restarted':
        r['summary'] = r['summary'] + f' 这一行讲的是 {retired_name} 的沿革，裸名字。'
dump('prose_quoted', prose_quoted)
dump('prose_bare', prose_bare)
PYEOF

run() { # run <config> [manifest 目录] -> 打印合并输出，回传退出码
  local out
  if out="$(bash "${gate}" "$1" "${2:-${work}/no-manifests}" 2>&1)"; then
    printf '%s\n' "${out}"; return 0
  else
    printf '%s\n' "${out}"; return 1
  fi
}

# 1. 真文件必须绿，并说出它读了多少条。manifest 真目录一起走——这份门禁的射程是
#    这个仓发出去的全部规则，只喂 chart 那一份就测不到后半段。
out="$(run "${config}" "${manifests}")" || fail "真规则文件被判红：${out}"
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

# 5a. 两参形态、第二个实参收敛 -> 绿（这条也钉住 `background_loop_restarted` 在控制集里）
if ! out="$(run "${work}/twoparam_ok.json")"; then
  fail "两参函数的第二个实参已收敛，却被判红：${out}"; fi
grep -q 'ALERT SUBJECT OK' <<<"${out}" || fail "绿的时候也要报判词：${out}"

# 5b. 两参形态、第二个实参是裸选择器 -> 红，且点名它
if out="$(run "${work}/twoparam_bad.json")"; then
  fail "两参函数的第二个实参没收敛，却被判绿：${out}"; fi
grep -q 'background_loop_restarted' <<<"${out}" || fail "判红却没点名那条规则：${out}"
grep -q 'LEAF' <<<"${out}" || fail "判词要给出没收敛的那段子表达式：${out}"

# 6a. 缺席函数的选择器里点名了 Pod -> 红（结果身份仍随 Pod 走）
if out="$(run "${work}/absent_pod.json")"; then
  fail "缺席函数的实参点名了 Pod，却被判绿：${out}"; fi
grep -q 'background_loop_restarted' <<<"${out}" || fail "判红却没点名那条规则：${out}"
grep -q 'LEAF' <<<"${out}" || fail "判词要给出没收敛的那段子表达式：${out}"

# 6b. 选择器只带常量标签 -> 绿（这正是 backup_success_not_recorded 的形态）
if ! out="$(run "${work}/absent_plain.json")"; then
  fail "缺席函数的实参没点名 Pod，却被判红：${out}"; fi
grep -q 'ALERT SUBJECT OK' <<<"${out}" || fail "绿的时候也要报判词：${out}"

# 6c. 实参是聚合 -> 绿（实测结果为空标签）
if ! out="$(run "${work}/absent_agg.json")"; then
  fail "缺席函数的实参是聚合，却被判红：${out}"; fi
grep -q 'ALERT SUBJECT OK' <<<"${out}" || fail "绿的时候也要报判词：${out}"

# 7a. 摘要里以反引号点名了停用名单里的名字 -> 红，且点名那条规则和那个名字
if out="$(run "${work}/prose_quoted.json")"; then
  fail "摘要点名了已停用的规则名，却被判绿：${out}"; fi
grep -q 'background_loop_restarted' <<<"${out}" || fail "判红却没点名那条规则：${out}"
grep -q '摘要里点名了已停用' <<<"${out}" || fail "判词没说清是摘要点名：${out}"

# 7b. 同一个名字的**裸**出现 -> 绿（判据取形式，不取裸子串）
if ! out="$(run "${work}/prose_bare.json")"; then
  fail "摘要里的裸名字是正当写法，却被判红：${out}"; fi
grep -q 'ALERT SUBJECT OK' <<<"${out}" || fail "绿的时候也要报判词：${out}"

# 1b. 真文件走默认参数时必须把 manifest 侧也读进来，并报出它读了多少条。
#     这一条钉的是「manifest 里的规则也在射程内」：只读 chart 的那一版会把
#     deploy/k3s/observability/manifests 里那 6 条告警全部静默放过。
out="$(bash "${gate}" "${config}")" || fail "真规则文件（含 manifest）被判红：${out}"
grep -q "manifest 规则 $(cat "${work}/manifest_rule_count") 条" <<<"${out}" \
  || fail "没有把 manifest 侧的条数报出来：${out}"

# 8. manifest 里一条没聚合的规则 -> 红，且点名文件与规则名。
#    身份随 Pod 走这件事与规则写在哪无关，所以 manifest 侧用同一套判词。
mkdir -p "${work}/m-bad"
cat >"${work}/m-bad/00-prometheusrule-test.yaml" <<'YAML'
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: test
spec:
  groups:
    - name: test
      rules:
        - alert: TestManifestUnreduced
          expr: cogneva_loop_role_held
YAML
if out="$(run "${config}" "${work}/m-bad")"; then
  fail "manifest 里丢掉聚合的规则被判绿：${out}"; fi
grep -q 'TestManifestUnreduced' <<<"${out}" || fail "判红却没点名那条规则：${out}"
grep -q 'LEAF' <<<"${out}" || fail "判词要给出没收敛的那段子表达式：${out}"

# 9. manifest 与 chart 同名 -> 红。同一个告警名两个来源，身份去重与冷却记账分不开它们。
mkdir -p "${work}/m-collide"
cat >"${work}/m-collide/00-prometheusrule-test.yaml" <<'YAML'
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: test
spec:
  groups:
    - name: test
      rules:
        - alert: pod_oom_killed
          expr: max(some_metric)
YAML
if out="$(run "${config}" "${work}/m-collide")"; then
  fail "manifest 与 chart 同名却判绿：${out}"; fi
grep -q '与 chart 里的规则同名' <<<"${out}" || fail "同名冲突的判词不对：${out}"

# 10. 无 pod 维那六条的放行前提被抽掉 -> 红。
#     这一条钉的是「豁免不是一次写死的放行」：抓取路径一旦从 Node 角色改成 Pod 发现，
#     那六条立刻回到未收敛一栏，而不是继续被静默放行。
cp -r "${manifests}" "${work}/m-premise"
sed -i 's/role: Node/role: Pod/' "${work}/m-premise/13-scrapeconfig-host-readings.yaml"
if out="$(run "${config}" "${work}/m-premise")"; then
  fail "Node 角色改成 Pod 之后，那六条仍被放行：${out}"; fi
grep -q '放行的前提没了' <<<"${out}" || fail "前提消失的判词不对：${out}"
grep -q 'CognevaHostReadingProducerDown' <<<"${out}" || fail "前提消失时没点名具体规则：${out}"

# 少了参数以外的路径不存在时，是依赖错误不是判词
if bash "${gate}" "${work}/does-not-exist.json" >/dev/null 2>&1; then
  fail "文件不存在却当成功返回"
fi

echo "ALERT SUBJECT GATE TESTS OK"
