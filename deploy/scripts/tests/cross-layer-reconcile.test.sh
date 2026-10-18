#!/usr/bin/env bash
# check-cross-layer-reconcile.sh 的定向测试。
#
# 这条读数判的是**两个层的差**，所以「它判得出来」这句话只能由反例证明：每个用例
# 都从一份与现网同量级的三层读数出发，只改一处，断言退出码与判词点名都落在那一处。
#
# 用例：
#   1. 现网量级不改 -> 0；
#   2. 集群记账超过 allocatable -> 1，点名判据 1 与资源名；
#   3. anon + 页缓存底线越过 MemTotal -> 1，点名判据 2 与节点；
#   4. 两个节点里只有一个越界 -> 1，红只点名那个节点（不是「有节点越界」）；
#   5. 某节点没有 anon 序列 -> 2（缺席不是 0，是判不了）；
#   6. 某节点的 7 天窗口里没有 file 样本 -> 2；
#   7. 判据 2 绿但 PSI full 在计秒 -> 0，并且把「不一致本身就是结论」印出来；
#  10. 准入余量装不下部署器滚动 Job -> 1，点名判据 3 与资源名（used 含运行期自建
#      Pod，声明和仍装得进配额）；
#  11. evolution ConfigMap 里没有滚动 Job 的上限 -> 2（缺席不是「装得下」）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-cross-layer-reconcile.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

[ -x "${gate}" ] || fail "门脚本不存在或不可执行：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ---- 假 kubectl：按参数从场景目录里取 JSON ----
cat >"${work}/fake-kubectl" <<'EOF'
#!/usr/bin/env bash
scen="${SCEN:?}"
case " $* " in
    *" get resourcequota "*) cat "${scen}/quota.json" ;;
    *" get nodes "*)         cat "${scen}/nodes.json" ;;
    *" get configmap "*)     cat "${scen}/evcfg.json" ;;
    *" get svc "*)           echo "10.0.0.1" ;;
    *) echo "fake-kubectl: 不认识的参数：$*" >&2; exit 1 ;;
esac
EOF
chmod +x "${work}/fake-kubectl"

# ---- 假 Prometheus 查询：按 PromQL 里的关键字取场景里的四条读数 ----
cat >"${work}/fake-prom" <<'EOF'
#!/usr/bin/env python3
import json, os, sys

scen = os.environ["SCEN"]
q = sys.argv[1]
with open(os.path.join(scen, "prom.json"), encoding="utf-8") as f:
    table = json.load(f)
for needle, key in (("min_over_time", "file_min"), ("anon", "anon"),
                    ("MemTotal", "memtotal"), ("stalled", "psi")):
    if needle in q:
        print(json.dumps({"status": "success",
                          "data": {"resultType": "vector", "result": table.get(key, [])}}))
        break
else:
    sys.stderr.write(f"fake-prom: 不认识的查询：{q}\n")
    sys.exit(1)
EOF
chmod +x "${work}/fake-prom"

# ---- 场景构造：现网量级，每处改动只动一个数 ----
make_scenario() { # make_scenario <名字> <python 改动片段>
  local name="$1" mutation="$2"
  local dir="${work}/${name}"
  mkdir -p "${dir}"
  SCEN_OUT="${dir}" python3 - "${mutation}" <<'PY'
import json, os, sys

out = os.environ["SCEN_OUT"]
mutation = sys.argv[1]

Gi = 2 ** 30
Mi = 2 ** 20

state = {
    "quota": {
        "metadata": {"name": "cogneva-quota", "namespace": "cogneva"},
        "spec": {"hard": {"requests.cpu": "6", "requests.memory": "12Gi",
                          "limits.cpu": "23", "limits.memory": "46Gi", "pods": "40"}},
        "status": {"used": {"requests.cpu": "2410m", "requests.memory": "6864Mi",
                            "limits.cpu": "18100m", "limits.memory": "41664Mi",
                            "pods": "13"}},
    },
    "nodes": {"items": [{
        "metadata": {"name": "cogneva"},
        "status": {"allocatable": {"cpu": "23860m", "memory": "55706256Ki"}},
    }]},
    # 部署器滚动 Job 的声明上限（判据 3 的右边）：与现网 evolution ConfigMap 同量级。
    "evcfg": {"data": {
        "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_REQUEST": "200m",
        "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_REQUEST": "64Mi",
        "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT": "2",
        "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_LIMIT": "512Mi",
    }},
    "prom": {
        "anon": [{"metric": {"node": "cogneva", "slice": "user.slice"},
                  "value": [0, str(int(4.31 * Gi))]},
                 {"metric": {"node": "cogneva", "slice": "system.slice"},
                  "value": [0, str(int(1.63 * Gi))]},
                 {"metric": {"node": "cogneva", "slice": "kubepods.slice"},
                  "value": [0, str(int(7.37 * Gi))]}],
        "file_min": [{"metric": {"node": "cogneva", "slice": "user.slice"},
                      "value": [0, str(int(8.84 * Gi))]}],
        "memtotal": [{"metric": {"nodename": "cogneva"},
                      "value": [0, str(int(62.70 * Gi))]}],
        "psi": [{"metric": {"nodename": "cogneva"}, "value": [0, "0"]}],
    },
}

exec(mutation, {"state": state, "Gi": Gi, "Mi": Mi})

for key in ("quota", "nodes", "prom", "evcfg"):
    with open(os.path.join(out, key + ".json"), "w", encoding="utf-8") as f:
        json.dump(state[key], f)
PY
  printf '%s' "${dir}"
}

run_gate() { # run_gate <场景目录>：输出进 LAST_OUT，退出码进 RC（命令替换会丢退出码）
  local scen="$1"
  set +e
  SCEN="${scen}" COGNEVA_KUBECTL="${work}/fake-kubectl" \
  COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" \
      bash "${gate}" >"${work}/out.txt" 2>&1
  RC=$?
  set -e
  LAST_OUT="$(cat "${work}/out.txt")"
}

expect() { # expect <名字> <期望退出码> <必须出现的片段> <改动>
  local name="$1" want_rc="$2" needle="$3" mutation="$4"
  local scen
  scen="$(make_scenario "${name}" "${mutation}")"
  run_gate "${scen}"
  [ "${RC}" -eq "${want_rc}" ] \
      || fail "${name}: 期望退出码 ${want_rc}，实得 ${RC}；输出：${LAST_OUT}"
  printf '%s' "${LAST_OUT}" | grep -Fq -- "${needle}" \
      || fail "${name}: 判词里没有 ${needle}；输出：${LAST_OUT}"
  echo "ok   ${name}：退出码 ${want_rc}，判词含「${needle}」"
}

# 1. 现网量级。
expect clean 0 "CROSS-LAYER OK" 'pass'
printf '%s' "${LAST_OUT}" | grep -Fq "判据2 node=cogneva" \
    || fail "clean: 判据 2 没有按节点印读数；输出：${LAST_OUT}"

# 2. 集群记账超过 allocatable。
expect used-over-allocatable 1 "判据1 requests.memory" '
state["quota"]["status"]["used"]["requests.memory"] = str(80 * Gi)
'

# 3. 不可回收 + 页缓存底线越过 MemTotal。
expect anon-over-total 1 "判据2 node=cogneva" '
state["prom"]["anon"] = [{"metric": {"node": "cogneva", "slice": "kubepods.slice"},
                          "value": [0, str(int(58 * Gi))]}]
'

# 4. 两个节点，只有一个越界：红只点名越界的那个。
expect one-node-red 1 "判据2 node=node-b" '
state["nodes"]["items"].append({
    "metadata": {"name": "node-b"},
    "status": {"allocatable": {"cpu": "8000m", "memory": "16777216Ki"}}})
state["prom"]["anon"].append({"metric": {"node": "node-b", "slice": "kubepods.slice"},
                              "value": [0, str(int(14 * Gi))]})
state["prom"]["file_min"].append({"metric": {"node": "node-b", "slice": "user.slice"},
                                  "value": [0, str(int(4 * Gi))]})
state["prom"]["memtotal"].append({"metric": {"nodename": "node-b"},
                                  "value": [0, str(int(16 * Gi))]})
state["prom"]["psi"].append({"metric": {"nodename": "node-b"}, "value": [0, "0"]})
'
# 绿的节点照样印读数（那是它的读数），但它的行不能带 ✗，也不能进红名单。
printf '%s' "${LAST_OUT}" | grep -Eq '判据2 node=cogneva.*✗' \
    && fail "one-node-red: 绿的节点被判红了；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -A20 'CROSS-LAYER 不成立' | grep -q 'node=cogneva' \
    && fail "one-node-red: 红名单里连坐了绿的节点；输出：${LAST_OUT}"
echo "ok   one-node-red：另一个节点仍是绿的，没有被连坐"

# 5. 某节点没有 anon 序列。
expect anon-missing 2 "没有 cogneva_host_slice_anon_bytes 序列" '
state["nodes"]["items"].append({
    "metadata": {"name": "node-b"},
    "status": {"allocatable": {"cpu": "8000m", "memory": "16777216Ki"}}})
state["prom"]["memtotal"].append({"metric": {"nodename": "node-b"},
                                  "value": [0, str(int(16 * Gi))]})
'

# 6. 某节点的 7 天窗口里没有 file 样本。
expect file-window-empty 2 "推不出页缓存底线" '
state["nodes"]["items"].append({
    "metadata": {"name": "node-b"},
    "status": {"allocatable": {"cpu": "8000m", "memory": "16777216Ki"}}})
state["prom"]["anon"].append({"metric": {"node": "node-b", "slice": "kubepods.slice"},
                              "value": [0, str(int(2 * Gi))]})
state["prom"]["memtotal"].append({"metric": {"nodename": "node-b"},
                                  "value": [0, str(int(16 * Gi))]})
'

# 7. 判据 2 绿，但 PSI full 在计秒：不判红，把不一致写成结论。
expect psi-disagrees 0 "不一致本身就是结论" '
state["prom"]["psi"] = [{"metric": {"nodename": "cogneva"}, "value": [0, "0.25"]}]
'

# 8. 有 host_slice 序列、但没有这个节点的 MemTotal：判据 2 的分母不在。
expect memtotal-missing-for-node 2 "判据 2 的分母不在" '
state["nodes"]["items"].append({
    "metadata": {"name": "node-b"},
    "status": {"allocatable": {"cpu": "8000m", "memory": "16777216Ki"}}})
state["prom"]["anon"].append({"metric": {"node": "node-b", "slice": "kubepods.slice"},
                              "value": [0, str(int(2 * Gi))]})
'

# 9. 配额里没有 requests.memory：判据 1 的左边读不出来。
expect quota-key-missing 2 "判据 1 读不出来" '
del state["quota"]["status"]["used"]["requests.memory"]
'

# 10. 声明和装得进配额，但准入余量装不下部署器滚动 Job：used 里含着运行期自建
#     的 Pod，渲染期门禁只加声明的工作负载，所以它绿着而准入已经拒人。
expect admission-headroom-short 1 "判据3 limits.memory" '
state["quota"]["status"]["used"]["limits.memory"] = str(int(46 * Gi) - 256 * Mi)
'

# 11. evolution ConfigMap 里没有滚动 Job 的内存上限：判据 3 的右边不在，判不了。
expect job-limit-missing 2 "没有 COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_LIMIT" '
del state["evcfg"]["data"]["COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_LIMIT"]
'

# ---- 序列模式：每一拍落一行，红了、读不到也各落一行 ----
#
# 判据是「拍子之间有没有洞」，所以**判红的那一拍也必须是拍子**：只在绿的时候写，
# 序列上的洞就同时代表「没跑」和「跑了但红了」，而这两件事要修的地方不同。

run_gate_record() { # run_gate_record <场景目录> <序列文件>：带 --record 跑一次
  local scen="$1" series="$2"
  set +e
  SCEN="${scen}" COGNEVA_KUBECTL="${work}/fake-kubectl" \
  COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" \
  COGNEVA_RECONCILE_SERIES="${series}" \
      bash "${gate}" --record >"${work}/out.txt" 2>&1
  RC=$?
  set -e
  LAST_OUT="$(cat "${work}/out.txt")"
}

field() { awk -F'\t' -v n="$2" 'NR == 1 { print $n }' "$1"; }

# 10. 绿的一拍照样落一行，数值是这一拍读到的（allocatable = 55706256Ki、used = 6864Mi）。
scen="$(make_scenario record-ok 'pass')"
rec="${work}/series-ok.tsv"
run_gate_record "${scen}" "${rec}"
[ "${RC}" -eq 0 ] || fail "record-ok: 期望退出码 0，实得 ${RC}；输出：${LAST_OUT}"
[ "$(wc -l <"${rec}")" -eq 1 ] || fail "record-ok: 序列不是一行：$(cat "${rec}")"
[ "$(awk -F'\t' 'NR == 1 { print NF }' "${rec}")" -eq 7 ] \
    || fail "record-ok: 序列不是 7 列：$(cat "${rec}")"
[ "$(field "${rec}" 2)" = "ok" ] || fail "record-ok: 结局不是 ok：$(cat "${rec}")"
[ "$(field "${rec}" 3)" = "7197425664" ] || fail "record-ok: used_memory 不对：$(cat "${rec}")"
[ "$(field "${rec}" 4)" = "57043206144" ] || fail "record-ok: allocatable 不对：$(cat "${rec}")"
echo "ok   record-ok：一行 7 列，结局 ok，allocatable 是这一拍读到的数"

# 11. 判红的一拍也落一行（结局 broken，数值仍在）。
scen="$(make_scenario record-broken '
state["quota"]["status"]["used"]["requests.memory"] = str(80 * Gi)
')"
rec="${work}/series-broken.tsv"
run_gate_record "${scen}" "${rec}"
[ "${RC}" -eq 1 ] || fail "record-broken: 期望退出码 1，实得 ${RC}；输出：${LAST_OUT}"
[ "$(field "${rec}" 2)" = "broken" ] || fail "record-broken: 结局不是 broken：$(cat "${rec}")"
[ "$(field "${rec}" 3)" = "85899345920" ] || fail "record-broken: used_memory 不对：$(cat "${rec}")"
echo "ok   record-broken：红了照样是一拍，数值留在序列里"

# 12. 读不到的一拍也落一行（结局 unreadable，数值记 '-'）。
mkdir -p "${work}/no-scenario"
rec="${work}/series-unreadable.tsv"
run_gate_record "${work}/no-scenario" "${rec}"
[ "${RC}" -eq 2 ] || fail "record-unreadable: 期望退出码 2，实得 ${RC}；输出：${LAST_OUT}"
[ "$(field "${rec}" 2)" = "unreadable" ] || fail "record-unreadable: 结局不是 unreadable：$(cat "${rec}")"
[ "$(field "${rec}" 3)" = "-" ] || fail "record-unreadable: 读不到的数没有记 '-'：$(cat "${rec}")"
echo "ok   record-unreadable：读不到也是一拍，不写成洞"

# 13. 不带 --record 不写任何东西：日跑与测试都靠这个默认。
scen="$(make_scenario record-off 'pass')"
rec="${work}/series-off.tsv"
SCEN="${scen}" COGNEVA_KUBECTL="${work}/fake-kubectl" \
COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" \
COGNEVA_RECONCILE_SERIES="${rec}" \
    bash "${gate}" >/dev/null 2>&1 || fail "record-off: 不带 --record 时退出码不是 0"
[ ! -e "${rec}" ] || fail "record-off: 没带 --record 却写了序列：$(cat "${rec}")"
echo "ok   record-off：不带 --record 时一个字节也不写"

echo "cross-layer-reconcile: 15 passed"
