#!/usr/bin/env bash
# check-container-headroom.sh 的定向测试。
#
# 这条读数判的是**单个容器**上「实际上限用量」与「声明的上限」之间的差，所以每个
# 用例都从一份与现网同量级的读数出发，只改一处，断言退出码与判词点名都落在那一处。
#
# 用例：
#   1. 全部有余量 -> 0，印出判过的容器数与最高三笔；
#   2. 一个容器 0.90 -> 1，点名它并印出比值；
#   3. 恰好等于红线 0.85 -> 1（判的是 ≥，不是 >）；
#   4. 0.84 -> 0；
#   5. 窗口内 OOM（比值很低）-> 1，点名它与 OOMKilled；
#   6. OOM 发生在窗口之外 -> 0（陈旧记录是历史，不是当前状态）；
#   7. 记着 OOMKilled 但没有终止时刻 -> 1（读不齐不能当绿）；
#   8. 没有 RSS 序列 -> 2（缺席不是「都余量充足」）；
#   9. 没有上限序列 -> 2（KSM 没起与「都没写上限」在这里同形，先报判不了）；
#  10. Prometheus 返回 error -> 2；
#  11. 一个容器没有上限 -> 0，但把它列出来（它不会被自己的上限杀，会去争节点的）；
#  12. --record 在绿、红、读不到三种结局下都落一行；
#  13. 红线与窗口写错 -> 2；
#  14. 窗口里留着样本、此刻已不存在的 Pod 不参与判定；
#  15. 没有 kube_pod_info -> 2；
#  16. init 容器的上限在另一族：有上限的 init 容器要被判，不能被读成「无上限」；
#  17. 真没有上限的 init 容器仍列出来，不判红；
#  18. 看得到 init 容器、看不到它们的上限族 -> 2（与第 9 条同一条道理）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-container-headroom.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
[ -x "${gate}" ] || fail "门脚本不存在或不可执行：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ---- 假 kubectl：只用来发现 Prometheus 的 Service ----
cat >"${work}/fake-kubectl" <<'EOF'
#!/usr/bin/env bash
case " $* " in
    *" get svc "*) echo "10.0.0.1" ;;
    *) echo "fake-kubectl: 不认识的参数：$*" >&2; exit 1 ;;
esac
EOF
chmod +x "${work}/fake-kubectl"

# ---- 假 Prometheus：按 PromQL 里的关键字取场景里的四族读数 ----
cat >"${work}/fake-prom" <<'EOF'
#!/usr/bin/env python3
import json, os, sys

scen = os.environ["SCEN"]
q = sys.argv[1]
with open(os.path.join(scen, "prom.json"), encoding="utf-8") as f:
    table = json.load(f)
if table.get("__error__"):
    print(json.dumps({"status": "error", "error": table["__error__"]}))
    sys.exit(0)
for needle, key in (("max_over_time", "rss"),
                    ("kube_pod_init_container_resource_limits", "init_limit"),
                    ("kube_pod_init_container_info", "init_info"),
                    ("kube_pod_container_resource_limits", "limit"),
                    ("kube_pod_info", "pods"),
                    ("last_terminated_reason", "oom_reason"),
                    ("last_terminated_timestamp", "oom_age")):
    if needle in q:
        print(json.dumps({"status": "success",
                          "data": {"resultType": "vector", "result": table.get(key, [])}}))
        break
else:
    sys.stderr.write(f"fake-prom: 不认识的查询：{q}\n")
    sys.exit(1)
EOF
chmod +x "${work}/fake-prom"

Gi=1073741824

make_scenario() { # make_scenario <名字> <python 改动片段>
    local name="$1" mutation="$2"
    local dir="${work}/${name}"
    mkdir -p "${dir}"
    SCEN_OUT="${dir}" python3 - "${mutation}" "${Gi}" <<'PY'
import json, os, sys

out = os.environ["SCEN_OUT"]
mutation = sys.argv[1]
Gi = int(sys.argv[2])

def series(family, pod, container, value):
    return {"metric": {"__name__": family, "pod": pod, "container": container,
                       "namespace": "cogneva"}, "value": [0, str(value)]}

state = {
    # 两个容器：主应用 3.0Gi / 6Gi（0.50），redis 1.0Gi / 2Gi（0.50）。
    "rss": [series("container_memory_rss", "cogneva-abc", "cogneva", int(3.0 * Gi)),
            series("container_memory_rss", "redis-0", "redis", int(1.0 * Gi))],
    "limit": [series("kube_pod_container_resource_limits", "cogneva-abc", "cogneva", 6 * Gi),
              series("kube_pod_container_resource_limits", "redis-0", "redis", 2 * Gi)],
    "pods": [series("kube_pod_info", "cogneva-abc", "", 1),
             series("kube_pod_info", "redis-0", "", 1)],
    "oom_reason": [],
    "oom_age": [],
    # init 容器的上限是另一族；默认没有 init 容器时两族都为空。
    "init_limit": [],
    "init_info": [],
}
exec(mutation)

with open(os.path.join(out, "prom.json"), "w", encoding="utf-8") as f:
    json.dump(state, f)
PY
}

status_of() { # status_of <场景目录> [额外环境…]
    local scen="$1"; shift
    local out
    set +e
    # 额外的 `VAR=值` 必须经 env 落地：经 "$@" 展开出来的赋值词在命令位会被当成
    # 命令名，不是赋值。
    out="$(SCEN="${scen}" COGNEVA_KUBECTL="${work}/fake-kubectl" \
        COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" env "$@" bash "${gate}" 2>&1)"
    local rc=$?
    set -e
    printf '%s\n' "${out}"
    return "${rc}"
}

# 1. 全部有余量
make_scenario healthy ''
out="$(status_of "${work}/healthy")" || fail "1 健康场景应退出 0，实得 $?"
case "${out}" in *"绿"*) ;; *) fail "1 判词没有绿：${out}" ;; esac
case "${out}" in *"判过 2 个"*) ;; *) fail "1 没有印出判过的容器数：${out}" ;; esac
case "${out}" in *"最高三笔"*) ;; *) fail "1 绿的时候也要印最高几笔：${out}" ;; esac

# 2. 一个容器 0.90
make_scenario over 'state["rss"][0]["value"][1] = str(int(5.4 * Gi))'
out="$(status_of "${work}/over")" && fail "2 越界场景应退出 1"
case "${out}" in *"cogneva-abc/cogneva"*) ;; *) fail "2 没有点名越界的容器：${out}" ;; esac
case "${out}" in *"0.90"*) ;; *) fail "2 没有印出比值：${out}" ;; esac

# 3. 恰好等于红线。取 2 的幂比值（3Gi/4Gi＝0.75），让「等于」是真的等于，
#    不是浮点算出来差一个 ulp 的「差不多」。
make_scenario equal 'state["limit"][0]["value"][1] = 4 * Gi
state["rss"][0]["value"][1] = 3 * Gi'
out="$(status_of "${work}/equal" COGNEVA_HEADROOM_RATIO=0.75)" && fail "3 等于红线应退出 1（判的是 ≥）"

# 4. 略低于红线
make_scenario below 'state["limit"][0]["value"][1] = 4 * Gi
state["rss"][0]["value"][1] = str(int(0.74 * 4 * Gi))'
out="$(status_of "${work}/below" COGNEVA_HEADROOM_RATIO=0.75)" || fail "4 0.74 应退出 0：${out}"

# 5. 窗口内 OOM，比值很低
make_scenario oom 'state["oom_reason"] = [series("kube_pod_container_status_last_terminated_reason",
        "redis-0", "redis", 1)]
state["oom_age"] = [series("kube_pod_container_status_last_terminated_timestamp",
        "redis-0", "redis", 600)]'
out="$(status_of "${work}/oom")" && fail "5 窗口内 OOM 应退出 1"
case "${out}" in *"redis-0/redis"*OOMKilled*) ;; *) fail "5 判词没有点名 OOM 的容器：${out}" ;; esac

# 6. OOM 在窗口外
make_scenario oom_old 'state["oom_reason"] = [series("kube_pod_container_status_last_terminated_reason",
        "redis-0", "redis", 1)]
state["oom_age"] = [series("kube_pod_container_status_last_terminated_timestamp",
        "redis-0", "redis", 100000)]'
out="$(status_of "${work}/oom_old")" || fail "6 陈旧的 OOM 记录不该判红：${out}"

# 7. 有原因没时刻
make_scenario oom_no_age 'state["oom_reason"] = [series("kube_pod_container_status_last_terminated_reason",
        "redis-0", "redis", 1)]'
out="$(status_of "${work}/oom_no_age")" && fail "7 读不齐应退出 1"
case "${out}" in *"没有 last_terminated_timestamp"*) ;; *) fail "7 判词没有说清缺的那一半：${out}" ;; esac

# 8. 没有 RSS 序列
make_scenario no_rss 'state["rss"] = []'
out="$(status_of "${work}/no_rss")" && fail "8 缺席应退出 2"
case "${out}" in *"没有 container_memory_rss"*) ;; *) fail "8 判词没有点名缺席的指标族：${out}" ;; esac

# 9. 没有上限序列
make_scenario no_limit 'state["limit"] = []'
out="$(status_of "${work}/no_limit")" && fail "9 缺席应退出 2"
case "${out}" in *"没有 kube_pod_container_resource_limits"*) ;; *) fail "9 判词没有点名缺席的指标族：${out}" ;; esac

# 10. Prometheus 返回 error
make_scenario prom_error 'state["__error__"] = "query timeout"'
out="$(status_of "${work}/prom_error")" && fail "10 Prometheus error 应退出 2"

# 11. 一个容器没有上限
make_scenario unlimited 'state["limit"] = state["limit"][:1]'
out="$(status_of "${work}/unlimited")" || fail "11 无上限不是红：${out}"
case "${out}" in *"无内存上限的容器 1 个"*"redis-0/redis"*) ;; *) fail "11 没有列出无上限的容器：${out}" ;; esac

# 12. --record 在三种结局下都落一行
series_file="${work}/headroom.tsv"
: >"${series_file}"
status_of "${work}/healthy" COGNEVA_HEADROOM_SERIES="${series_file}" >/dev/null || fail "12 绿场景应退出 0"
status_of "${work}/over" COGNEVA_HEADROOM_SERIES="${series_file}" >/dev/null && fail "12 红场景应退出 1"
status_of "${work}/no_rss" COGNEVA_HEADROOM_SERIES="${series_file}" >/dev/null && fail "12 读不到应退出 2"
# 三种结局都要落：只写绿的那一拍，序列上的洞分不开「没跑」与「跑了但红了」。
lines="$(SCEN="${work}/healthy" COGNEVA_KUBECTL="${work}/fake-kubectl" \
    COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" COGNEVA_HEADROOM_SERIES="${series_file}" \
    bash "${gate}" --record >/dev/null 2>&1 || true
    SCEN="${work}/over" COGNEVA_KUBECTL="${work}/fake-kubectl" \
    COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" COGNEVA_HEADROOM_SERIES="${series_file}" \
    bash "${gate}" --record >/dev/null 2>&1 || true
    SCEN="${work}/no_rss" COGNEVA_KUBECTL="${work}/fake-kubectl" \
    COGNEVA_PROM_QUERY_CMD="${work}/fake-prom" COGNEVA_HEADROOM_SERIES="${series_file}" \
    bash "${gate}" --record >/dev/null 2>&1 || true
    cat "${series_file}")"
[ "$(printf '%s\n' "${lines}" | wc -l)" = 3 ] || fail "12 --record 应落三行，实得：${lines}"
case "${lines}" in *"ok"*) ;; *) fail "12 序列里没有 ok：${lines}" ;; esac
case "${lines}" in *"broken"*) ;; *) fail "12 序列里没有 broken：${lines}" ;; esac
case "${lines}" in *"unreadable"*) ;; *) fail "12 序列里没有 unreadable：${lines}" ;; esac

# 13. 红线与窗口写错
out="$(status_of "${work}/healthy" COGNEVA_HEADROOM_RATIO=abc)" && fail "13 坏红线应退出 2"
out="$(status_of "${work}/healthy" COGNEVA_HEADROOM_RATIO=1.5)" && fail "13 红线越界应退出 2"
out="$(status_of "${work}/healthy" COGNEVA_HEADROOM_WINDOW=24)" && fail "13 窗口缺单位应退出 2"
out="$(status_of "${work}/healthy" COGNEVA_HEADROOM_WINDOW=1w)" && fail "13 窗口单位不认识应退出 2"

# 14. 窗口里还留着样本、但此刻已不存在的 Pod 不算「无上限」，直接不参与判定
make_scenario stale 'state["rss"].append(series("container_memory_rss", "cogneva-dead", "cogneva", 5 * Gi))'
out="$(status_of "${work}/stale")" || fail "14 已删除的 Pod 不该判红：${out}"
case "${out}" in *"cogneva-dead"*) fail "14 已删除的 Pod 不该出现在任何名单里：${out}" ;; esac
case "${out}" in *"判过 2 个"*) ;; *) fail "14 存活容器的判定数不对：${out}" ;; esac

# 15. 没有 kube_pod_info（KSM 部分缺席）
make_scenario no_pods 'state["pods"] = []'
out="$(status_of "${work}/no_pods")" && fail "15 没有 Pod 序列应退出 2"
case "${out}" in *"没有 kube_pod_info"*) ;; *) fail "15 判词没有点名缺席的指标族：${out}" ;; esac

# 16. init 容器的上限在另一族：有上限就该被判，不该被读成「无内存上限」
make_scenario init_limited 'state["rss"].append(series("container_memory_rss", "sandbox-abc", "seed-sandbox", int(0.1 * Gi)))
state["pods"].append(series("kube_pod_info", "sandbox-abc", "", 1))
state["init_info"] = [series("kube_pod_init_container_info", "sandbox-abc", "seed-sandbox", 1)]
state["init_limit"] = [series("kube_pod_init_container_resource_limits", "sandbox-abc", "seed-sandbox", int(0.5 * Gi))]'
out="$(status_of "${work}/init_limited")" || fail "16 init 容器有余量应退出 0：${out}"
case "${out}" in *"判过 3 个"*) ;; *) fail "16 有上限的 init 容器没有被判：${out}" ;; esac
case "${out}" in *"无内存上限"*) fail "16 有上限的 init 容器被读成了无上限：${out}" ;; esac

# 17. init 容器真的没有上限（KSM 报得出它、上限族里没有它）-> 仍列出来，不判红
make_scenario init_unlimited 'state["rss"].append(series("container_memory_rss", "sandbox-abc", "seed-sandbox", int(0.1 * Gi)))
state["pods"].append(series("kube_pod_info", "sandbox-abc", "", 1))
state["init_info"] = [series("kube_pod_init_container_info", "sandbox-abc", "seed-sandbox", 1),
                      series("kube_pod_init_container_info", "sandbox-abc", "tighten", 1)]
state["init_limit"] = [series("kube_pod_init_container_resource_limits", "sandbox-abc", "tighten", int(0.5 * Gi))]'
out="$(status_of "${work}/init_unlimited")" || fail "17 无上限的 init 容器不是红：${out}"
case "${out}" in *"无内存上限的容器 1 个"*"seed-sandbox"*) ;; *) fail "17 没有点名无上限的 init 容器：${out}" ;; esac

# 18. 看得到 init 容器、却看不到它们的上限族 -> 判不了，不是「它们都没有上限」
make_scenario init_family_absent 'state["rss"].append(series("container_memory_rss", "sandbox-abc", "seed-sandbox", int(0.1 * Gi)))
state["pods"].append(series("kube_pod_info", "sandbox-abc", "", 1))
state["init_info"] = [series("kube_pod_init_container_info", "sandbox-abc", "seed-sandbox", 1)]'
out="$(status_of "${work}/init_family_absent")" && fail "18 上限族缺席应退出 2"
case "${out}" in *"kube_pod_init_container_resource_limits"*) ;; *) fail "18 判词没有点名缺席的指标族：${out}" ;; esac

echo "container-headroom: 18 组用例通过"
