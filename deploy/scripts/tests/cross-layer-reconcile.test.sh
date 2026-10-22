#!/usr/bin/env bash
# principle: feedback_an_automatic_correction_is_bounded_by_a_measurement_not_by_a_direction
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
#  11. evolution ConfigMap 里没有滚动 Job 的上限 -> 2（缺席不是「装得下」）；
#  12. live 的配额上限与树内声明不同（其余三层仍装得下）-> 1，点名判据 4 与对象；
#  13. live 少一个声明面注解 -> 1，点名那个注解（缺席读作「没 apply 过」）；
#  14. 树内声明目录不在 -> 2（判不了，不是绿）；
#  15. LimitRange 逐字段不同 -> 1，判词点到那个字段；
#  16. --converge 且 live 低于树、用量装得下 -> 0，apply 了、序列记 converged；
#  17. --converge 且 live 高于树 -> 0，同样收敛回声明；
#  18. --converge 但收窄后放不下滚动 Job -> 1，不写；
#  19. --converge 但树内声明低于此刻用量 -> 1，不写；
#  20. --converge 但 LimitRange 上限低于在跑 Pod 的 limit -> 1，不写；
#  21. --converge 但本拍有判据读不到 -> 2，不写；
#  22. 不带 --converge -> 1，只报差、不写（默认只读）。
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
    *" get resourcequota "*)
        # apply 过之后，live 就是树内那一份：--converge 的第二个读数拍要能看到它。
        if [ -e "${scen}/APPLIED" ] && [ -e "${scen}/declared-quota.json" ]; then
            cat "${scen}/declared-quota.json"
        else
            cat "${scen}/quota.json"
        fi ;;
    *" get limitrange "*)
        if [ -e "${scen}/APPLIED" ] && [ -e "${scen}/declared-limitranges.json" ]; then
            cat "${scen}/declared-limitranges.json"
        else
            cat "${scen}/limitranges.json"
        fi ;;
    *" get pods "*)          cat "${scen}/pods.json" ;;
    *" get nodes "*)         cat "${scen}/nodes.json" ;;
    *" get configmap "*)     cat "${scen}/evcfg.json" ;;
    *" get svc "*)           echo "10.0.0.1" ;;
    *" apply -f "*)
        for f in "$@"; do :; done
        cp "$f" "${scen}/applied-$(date +%s%N).yaml"
        : >"${scen}/APPLIED"
        echo "applied" ;;
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

# 判据 4 的左边与现网同量级：声明侧挂在 quota 上，live 侧是 quota.json。
hard = {"requests.cpu": "6", "requests.memory": "12Gi",
        "limits.cpu": "23", "limits.memory": "46Gi", "pods": "40"}
budget = {"cogneva.io/runtime-pods-limits-cpu": "3",
          "cogneva.io/runtime-pods-limits-memory": "3.75Gi"}
container = {"type": "Container",
             "default": {"cpu": "500m", "memory": "512Mi"},
             "defaultRequest": {"cpu": "50m", "memory": "64Mi"},
             "max": {"cpu": "8", "memory": "16Gi"},
             "min": {"cpu": "10m", "memory": "16Mi"}}
pvc = {"type": "PersistentVolumeClaim", "max": {"storage": "80Gi"},
       "min": {"storage": "1Gi"}}

state = {
    "quota": {
        "metadata": {"name": "cogneva-quota", "namespace": "cogneva",
                     "annotations": dict(budget)},
        "spec": {"hard": dict(hard)},
        "status": {"used": {"requests.cpu": "2410m", "requests.memory": "6864Mi",
                            "limits.cpu": "18100m", "limits.memory": "41664Mi",
                            "pods": "13"}},
    },
    # live 的 LimitRange：与声明逐字段相等（判据 4 的另一个对象）。
    # 深拷贝：用例改 live 一侧时，声明侧必须保持原值，否则「差」被自己抹平。
    "limitranges": {"items": [{
        "metadata": {"name": "cogneva-limits", "namespace": "cogneva"},
        "spec": {"limits": [json.loads(json.dumps(container)),
                            json.loads(json.dumps(pvc))]},
    }]},
    # 树内渲染出来的那一份（判据 4 的左边）。改动只落在 live 侧时，它就是对照。
    "declared": {
        "ResourceQuota": {"apiVersion": "v1", "kind": "ResourceQuota",
                          "metadata": {"name": "cogneva-quota", "namespace": "cogneva",
                                       "annotations": dict(budget)},
                          "spec": {"hard": dict(hard)}},
        "LimitRange": {"apiVersion": "v1", "kind": "LimitRange",
                       "metadata": {"name": "cogneva-limits", "namespace": "cogneva"},
                       "spec": {"limits": [dict(container), dict(pvc)]}},
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
    # --converge 的第二个测量输入。默认没有还在跑的 Pod：LimitRange 的最大/最小
    # 对空集总是成立，所以「装不下」的用例必须自己放一个 Pod 进来。
    "pods": {"items": []},
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

for key in ("quota", "nodes", "prom", "evcfg", "limitranges", "pods"):
    with open(os.path.join(out, key + ".json"), "w", encoding="utf-8") as f:
        json.dump(state[key], f)

# apply 之后 live 就变成树内那一份（假 kubectl 在 APPLIED 存在时改读这两份）：
# 收敛成功与否由「重读出来是否逐字段相等」判，所以这里要准备重读的答案。
with open(os.path.join(out, "declared-quota.json"), "w", encoding="utf-8") as f:
    json.dump(state["declared"]["ResourceQuota"], f)
with open(os.path.join(out, "declared-limitranges.json"), "w", encoding="utf-8") as f:
    json.dump({"items": [state["declared"]["LimitRange"]]}, f)

# 树内声明写成渲染出来的样子。JSON 是 YAML 的子集，所以直接落 JSON，多个对象用
# `---` 分开——测试里不必再依赖 PyYAML 的 dump。
declared = os.path.join(out, "declared")
os.makedirs(declared, exist_ok=True)
with open(os.path.join(declared, "01-governance.yaml"), "w", encoding="utf-8") as f:
    for i, obj in enumerate(state["declared"].values()):
        if i:
            f.write("---\n")
        json.dump(obj, f, ensure_ascii=False)
        f.write("\n")
PY
  printf '%s' "${dir}"
}

run_gate() { # run_gate <场景目录> [声明目录]：输出进 LAST_OUT，退出码进 RC（命令替换会丢退出码）
  local scen="$1"
  local declared="${2:-${scen}/declared}"
  set +e
  SCEN="${scen}" COGNEVA_KUBECTL="${work}/fake-kubectl" \
  COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" \
  COGNEVA_RECONCILE_DECLARED="${declared}" \
      bash "${gate}" >"${work}/out.txt" 2>&1
  RC=$?
  set -e
  LAST_OUT="$(cat "${work}/out.txt")"
}

field() { awk -F'\t' -v n="$2" 'NR == 1 { print $n }' "$1"; }

run_gate_converge() { # run_gate_converge <场景目录> [--record <序列文件>]
  local scen="$1"
  shift
  local series=""
  if [ "${1:-}" = "--record" ]; then series="$2"; fi
  local args=(--converge)
  [ -n "${series}" ] && args=(--record --converge)
  set +e
  SCEN="${scen}" COGNEVA_KUBECTL="${work}/fake-kubectl" \
  COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" \
  COGNEVA_RECONCILE_DECLARED="${scen}/declared" \
  COGNEVA_RECONCILE_SERIES="${series}" \
      bash "${gate}" "${args[@]}" >"${work}/out.txt" 2>&1
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

# ---- 判据 4：live 的治理对象 == 树内声明 ----
# 这四条改的都只是 live 一侧（或树的读入路径），三层「装得下」照样成立——判据 4 要
# 证明的正是「装得下」回答不了的那件事：live 上的对象是不是这棵树的那一份。

# 12. live 的配额被人手工抬高，树内声明没动：判据 1/2/3 全绿，判据 4 红。
expect live-hard-drift 1 "判据4 ResourceQuota/cogneva-quota" '
state["quota"]["spec"]["hard"]["limits.memory"] = "50Gi"
'

# 13. live 少了一个声明面注解（运行期预算）：缺席在这里读作「没 apply 过」。
expect live-annotation-missing 1 "注解 cogneva.io/runtime-pods-limits-memory" '
del state["quota"]["metadata"]["annotations"]["cogneva.io/runtime-pods-limits-memory"]
'

# 14. 树内没有声明可对照（目录不在）：不是绿，是判不了。
scen="$(make_scenario declared-dir-missing-2 'pass')"
run_gate "${scen}" "${scen}/no-such-dir"
[ "${RC}" -eq 2 ] \
    || fail "declared-dir-missing-2: 期望退出码 2，实得 ${RC}；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -Fq "树内声明目录里没有 ResourceQuota/LimitRange" \
    || fail "declared-dir-missing-2: 判词没有点名声明目录；输出：${LAST_OUT}"
echo "ok   declared-dir-missing-2：声明目录不在时判不了（退出码 2）"

# 15. LimitRange 的一个字段与 live 不同：判词点到那个字段，不是笼统说「不一致」。
expect limitrange-drift 1 "limits[Container].max.cpu" '
state["limitranges"]["items"][0]["spec"]["limits"][0]["max"]["cpu"] = "12"
'

# ---- --converge：按树内声明填差，界是测量的 ----
# 每条都同时断言「该 apply 时 apply 了」与「不该 apply 时一个字节都没写」，
# 因为写集群这条路径的失败方式不是判错，而是**判错了还写出去**。

# 16. live 低于树（声明抬了、没人 apply）：apply 之后用量装得下 -> 收敛。
scen="$(make_scenario converge-raise '
state["quota"]["spec"]["hard"]["limits.cpu"] = "23"
state["declared"]["ResourceQuota"]["spec"]["hard"]["limits.cpu"] = "30"
')"
rec="${work}/series-converged.tsv"
run_gate_converge "${scen}" --record "${rec}"
[ "${RC}" -eq 0 ] || fail "converge-raise: 期望退出码 0，实得 ${RC}；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -Fq "按树内声明收敛" \
    || fail "converge-raise: 判词没有说收敛了；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    || fail "converge-raise: 没有 apply 任何对象"
[ "$(field "${rec}" 2)" = "converged" ] \
    || fail "converge-raise: 序列结局不是 converged：$(cat "${rec}")"
echo "ok   converge-raise：live 低于树时按声明填上，序列记 converged"

# 17. live 高于树（有人手工抬过）：同样扶正——下一次有人改声明时，正本只有一份。
scen="$(make_scenario converge-lower '
state["quota"]["spec"]["hard"]["limits.cpu"] = "40"
')"
run_gate_converge "${scen}"
[ "${RC}" -eq 0 ] || fail "converge-lower: 期望退出码 0，实得 ${RC}；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    || fail "converge-lower: live 高于树时没有扶正"
echo "ok   converge-lower：live 高于树时也收敛回声明"

# 18. 收窄天花板会吃掉判据 3 的余量：used 还装得下，但加上滚动 Job 就装不下 -> 不动。
scen="$(make_scenario converge-held-by-job '
state["quota"]["spec"]["hard"]["limits.memory"] = "52Gi"
state["declared"]["ResourceQuota"]["spec"]["hard"]["limits.memory"] = "41Gi"
')"
run_gate_converge "${scen}"
[ "${RC}" -eq 1 ] || fail "converge-held-by-job: 期望退出码 1，实得 ${RC}；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -Fq "部署器滚动 Job" \
    || fail "converge-held-by-job: 判词没有点名滚动 Job；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    && fail "converge-held-by-job: 装不下却写了集群"
echo "ok   converge-held-by-job：apply 后滚动 Job 进不来时拒绝收敛"

# 19. 树内声明比此刻的用量还低：apply 出去新 Pod 就进不来 -> 不动。
scen="$(make_scenario converge-held-by-used '
state["quota"]["spec"]["hard"]["limits.memory"] = "52Gi"
state["declared"]["ResourceQuota"]["spec"]["hard"]["limits.memory"] = "40Gi"
')"
run_gate_converge "${scen}"
[ "${RC}" -eq 1 ] || fail "converge-held-by-used: 期望退出码 1，实得 ${RC}；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -Fq "大于树内声明" \
    || fail "converge-held-by-used: 判词没有点名 used 与声明的不等式；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    && fail "converge-held-by-used: 装不下却写了集群"
echo "ok   converge-held-by-used：声明低于用量时拒绝收敛"

# 20. LimitRange 上限调低会拒绝正在跑的 Pod 的下一次滚动 -> 不动。
scen="$(make_scenario converge-held-by-limitrange '
state["declared"]["LimitRange"]["spec"]["limits"][0]["max"]["cpu"] = "4"
state["pods"]["items"] = [{"metadata": {"name": "web-0"},
    "spec": {"containers": [{"name": "web",
        "resources": {"limits": {"cpu": "8"}, "requests": {"cpu": "50m"}}}]}}]
')"
run_gate_converge "${scen}"
[ "${RC}" -eq 1 ] || fail "converge-held-by-limitrange: 期望退出码 1，实得 ${RC}；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -Fq "超过它" \
    || fail "converge-held-by-limitrange: 判词没有点名越界的 Pod；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    && fail "converge-held-by-limitrange: 装不下却写了集群"
echo "ok   converge-held-by-limitrange：LimitRange 会拒绝在跑 Pod 时拒绝收敛"

# 21. 本拍有判据读不到（判据 3 的右边不在）：即使差值可以填，也不写集群。
scen="$(make_scenario converge-held-by-unreadable '
state["quota"]["spec"]["hard"]["limits.cpu"] = "23"
state["declared"]["ResourceQuota"]["spec"]["hard"]["limits.cpu"] = "30"
del state["evcfg"]["data"]["COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT"]
')"
run_gate_converge "${scen}"
[ "${RC}" -eq 2 ] || fail "converge-held-by-unreadable: 期望退出码 2，实得 ${RC}；输出：${LAST_OUT}"
printf '%s' "${LAST_OUT}" | grep -Fq "自动收敛未执行" \
    || fail "converge-held-by-unreadable: 判词没有说收敛没执行；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    && fail "converge-held-by-unreadable: 读不全却写了集群"
echo "ok   converge-held-by-unreadable：读不全的一拍不写集群"

# 22. 不带 --converge：判据 4 只报差，一个字节也不写（默认只读）。
scen="$(make_scenario converge-off '
state["quota"]["spec"]["hard"]["limits.memory"] = "50Gi"
')"
run_gate "${scen}"
[ "${RC}" -eq 1 ] || fail "converge-off: 期望退出码 1，实得 ${RC}；输出：${LAST_OUT}"
ls "${scen}"/applied-*.yaml >/dev/null 2>&1 \
    && fail "converge-off: 没带 --converge 却写了集群"
echo "ok   converge-off：默认只读，报差但不写"

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
  COGNEVA_RECONCILE_DECLARED="${scen}/declared" \
      bash "${gate}" --record >"${work}/out.txt" 2>&1
  RC=$?
  set -e
  LAST_OUT="$(cat "${work}/out.txt")"
}

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
COGNEVA_RECONCILE_DECLARED="${scen}/declared" \
    bash "${gate}" >/dev/null 2>&1 || fail "record-off: 不带 --record 时退出码不是 0"
[ ! -e "${rec}" ] || fail "record-off: 没带 --record 却写了序列：$(cat "${rec}")"
echo "ok   record-off：不带 --record 时一个字节也不写"

echo "cross-layer-reconcile: 27 passed"
