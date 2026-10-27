#!/usr/bin/env bash
# 观测事件日志的容量与读窗口是两个数、一条承诺。
#
# `cog_observability_events` 只追加，而且在清理循环出现之前**没有任何东西删过它的行**：
# 它的大小等于平台在线时长乘以事件速率。2026-10-27 实测 2.92M 行 / 1.6 GB，常规日每天
# 约 30 万行，糟的那天 100 万行（10-18）。这张表的每一行都进夜间备份，而恢复演练要把
# 整份库装回去才叫证明——单趟从 553 s 涨到 2271 s 就是这个增长，所以这两个数不是内务，
# 是演练预算的输入。
#
# 这条门量四件事：
#
#   1. 渲染出来的主应用容器带着这两个 env。不带时进程拿代码默认值，部署清单声明的数与
#      进程实际执行的那个数成了两个数，账上读到的判词与清单对不上。
#   2. `deploy/k3s/deployment.yaml` 同值。它是 parity 基线兼集群内 GitOps 拉取端的消费物，
#      用 helm 装与用 apply 装的清理必须同一条界。
#   3. 代码默认值与清单同值，且都是正整数；容量 0 是「本部署不清理」的合法表达，但
#      读窗口 0 不是——窗口是给读者的承诺，任何配置都不许把它拿掉。
#   4. 历史事件查询用的是同一个常量。两个拼写就是两句话：窗口承诺最近一天、而清理按
#      另一个数裁，裁掉的那一段读起来与「那段时间什么都没发生」逐字同形。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
profile="${chart}/profiles/k3s-single.yaml"
static="${repo}/deploy/k3s/deployment.yaml"
gateway_code="${repo}/crates/cog-storage/src/postgres/observability_gateway.rs"
cap_code="${repo}/crates/cog-storage/src/observability_event_cap.rs"

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

rendered="${work}/app.yaml"
helm template cogneva "${chart}" \
  -f "${profile}" \
  --set resourceGovernance.enabled=false \
  --namespace cogneva > "${rendered}"

# 从一份清单里取主应用 Deployment 容器的两个 env。渲染件是多文档，静态清单是单文档，
# 同一个读法都能吃。输出两行：max_rows=… / read_window=…
cap_pair() {
  python3 - "$1" <<'PY'
import sys, yaml

want = ("COGNEVA_OBSERVABILITY_EVENT_MAX_ROWS",
        "COGNEVA_OBSERVABILITY_EVENT_READ_WINDOW_SECS")
keys = ("max_rows", "read_window")

with open(sys.argv[1]) as fh:
    for doc in yaml.safe_load_all(fh):
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "Deployment":
            continue
        if doc.get("metadata", {}).get("name") != "cogneva":
            continue
        env = {}
        for c in doc["spec"]["template"]["spec"]["containers"]:
            for e in c.get("env", []):
                env[e.get("name")] = e.get("value")
        for name, key in zip(want, keys):
            print(f"{key}={env.get(name, '')}")
        break
    else:
        sys.exit("清单里没有 Deployment/cogneva")
PY
}

check_pair() {
  local label="$1" path="$2"
  local out max_rows read_window
  out="$(cap_pair "${path}")" || fail "${label}：${out}"
  max_rows="$(sed -n 's/^max_rows=//p' <<<"${out}")"
  read_window="$(sed -n 's/^read_window=//p' <<<"${out}")"

  [[ -n "${max_rows}" ]] \
    || fail "${label}：主应用容器没有 COGNEVA_OBSERVABILITY_EVENT_MAX_ROWS。没有它时进程用代码默认值，清单声明的容量与进程执行的那个容量成了两个数"
  [[ -n "${read_window}" ]] \
    || fail "${label}：主应用容器没有 COGNEVA_OBSERVABILITY_EVENT_READ_WINDOW_SECS。读窗口是给读者的承诺，缺它时进程只能拿代码默认值，而部署清单看起来什么都没说"
  [[ "${max_rows}" =~ ^[0-9]+$ ]] \
    || fail "${label}：容量不是非负整数（max_rows=${max_rows}）"
  [[ "${read_window}" =~ ^[0-9]+$ && "${read_window}" -gt 0 ]] \
    || fail "${label}：读窗口必须是正整数（read_window=${read_window}）。窗口为 0 等于「写完即可删」，那正是这条承诺要挡住的事"

  echo "${max_rows} ${read_window}"
}

read -r chart_rows chart_window <<<"$(check_pair "chart 渲染件" "${rendered}")"
read -r static_rows static_window <<<"$(check_pair "deploy/k3s 静态清单" "${static}")"

[[ "${chart_rows}" == "${static_rows}" ]] \
  || fail "两份清单的容量不同（chart=${chart_rows} static=${static_rows}）：用 helm 装与用 apply 装的清理不是同一条界"
[[ "${chart_window}" == "${static_window}" ]] \
  || fail "两份清单的读窗口不同（chart=${chart_window} static=${static_window}）"

# 代码默认值必须与清单同值：容器没拿到 env 时（老清单、手工 create job）进程拿默认值跑，
# 那时它执行的界与部署清单声明的界就不是同一个数了。
code_rows="$(sed -n 's/^pub const DEFAULT_MAX_ROWS: u64 = \([0-9_]*\);$/\1/p' "${cap_code}" | tr -d '_')"
code_window="$(sed -n 's/^pub const DEFAULT_READ_WINDOW_SECS: u64 = \([0-9_]*\);$/\1/p' "${cap_code}" | tr -d '_')"
[[ -n "${code_rows}" ]] || fail "${cap_code} 里找不到 DEFAULT_MAX_ROWS"
[[ -n "${code_window}" ]] || fail "${cap_code} 里找不到 DEFAULT_READ_WINDOW_SECS"
[[ "${code_rows}" == "${chart_rows}" ]] \
  || fail "代码默认容量 ${code_rows} 与清单 ${chart_rows} 不同：没注入 env 的那个进程会按另一条界清理"
[[ "${code_window}" == "${chart_window}" ]] \
  || fail "代码默认读窗口 ${code_window}s 与清单 ${chart_window}s 不同：没注入 env 的那个进程会按另一个窗口裁"

# 历史事件查询必须用同一个常量。它原来在这里写死 `Duration::hours(24)`——那是**第二份**
# 拼写，两份迟早会分叉，而分叉的那一天，被裁掉的那一段与「那段时间什么都没发生」同形。
grep -q 'observability_event_cap::DEFAULT_READ_WINDOW_SECS' "${gateway_code}" \
  || fail "${gateway_code} 的历史事件查询没有用 observability_event_cap::DEFAULT_READ_WINDOW_SECS：读窗口成了两句话，清理会裁掉查询正要读的那一段"
if grep -q 'Duration::hours(24)' "${gateway_code}"; then
  fail "${gateway_code} 里又出现了写死的 24 小时窗口：默认窗口只能有一个说法"
fi

echo "OK：两份清单与代码同源（容量 ${chart_rows} 行，读窗口 ${chart_window}s），历史事件查询读同一个常量"
