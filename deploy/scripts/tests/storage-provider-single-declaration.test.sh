#!/usr/bin/env bash
# principle: feedback_a_copied_tool_brings_lists_the_test_does_not_own
# 存储的 provider 只能有一处声明：`deploy/helm/cogneva/files/cogneva.json` 的
# `providers.storage.provider`。其余各处都是它的投影，不是第二处声明。
#
# 为什么要这条门：`templates/configmap.yaml` 曾经写死 `COGNEVA_STORAGE_PROVIDER: "local-fs"`，
# 而那份 json 声明的是 `seaweedfs`、values 里 seaweedfs 段默认 `enabled: true`。env 层按
# json 自己的 env→配置路径映射盖住 json，于是集群里跑着 SeaweedFS、进程却把 Raw 层与
# 冷层写去本地卷——两份值相等的今天是同一个答案，不等的那天谁也不知道哪一处是死的。
#
# 判据四条（按**出现形式**判，不只比值）：
#   1. 模板那一行是投影（`$doc.providers.storage.provider`），不是裸字面量——
#      手抄一个同值的字面量也要红，因为它会在下一次改 json 时沉默地留旧值；
#   2. 渲染出来的 env 值等于 json 声明的值；
#   3. values 的 `seaweedfs.enabled` 与 provider 同向：选 seaweedfs 就开着，选别的就关着；
#   4. `deploy/k3s/configmap.yaml`（单机投影）与 json 同值。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
fail() { echo "FAIL: $*"; exit 1; }

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }
command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

provider="$(python3 -I -c '
import json,sys
d=json.load(open(sys.argv[1]))
print(d.get("providers",{}).get("storage",{}).get("provider",""))
' "${chart}/files/cogneva.json")"
[ -n "${provider}" ] || fail "cogneva.json 里没有 providers.storage.provider：那这份声明本身不在，下面四条都不成立"

# 1. 模板那一行是投影，不是字面量。
line="$(grep -n 'COGNEVA_STORAGE_PROVIDER:' "${chart}/templates/configmap.yaml" || true)"
[ -n "${line}" ] || fail "模板里没有 COGNEVA_STORAGE_PROVIDER：终态允许它不存在，但若存在就只能有一处声明"
case "${line}" in
*\$\$doc.providers.storage.provider* | *'$doc.providers.storage.provider'*) ;;
*)
    fail "模板那一行不是从 cogneva.json 取的投影，是第二处声明：${line}"
    ;;
esac

# 2. 渲染出来的 env 值 == json 声明的值。
bash "${repo}/deploy/scripts/render-chart.sh" k3s-single >"${work}/rendered.yaml" \
    || fail "chart 渲染失败，读不出交付形态里的 provider"
got="$(grep -E '^  COGNEVA_STORAGE_PROVIDER:' "${work}/rendered.yaml" | head -1 | sed -E 's/^[^:]*: *"?([^"]*)"?.*/\1/')"
[ -n "${got}" ] || fail "渲染出的 ConfigMap 里没有 COGNEVA_STORAGE_PROVIDER"
[ "${got}" = "${provider}" ] \
    || fail "渲染出 ${got}，cogneva.json 声明的是 ${provider}：同一件事的两份值已经不等了"

# 3. values 的 seaweedfs.enabled 与 provider 同向。
sw="$(python3 -I -c '
import re,sys
lines=open(sys.argv[1]).read().splitlines()
start=None; indent=0
for i,l in enumerate(lines):
    m=re.match(r"^([ \t]*)seaweedfs:[ \t]*$", l)
    if m:
        start=i; indent=len(m.group(1)); break
if start is not None:
    for l in lines[start+1:]:
        if not l.strip() or l.lstrip().startswith("#"):
            continue
        lead=len(l)-len(l.lstrip())
        if lead<=indent:
            break
        m=re.match(r"^[ \t]*enabled:[ \t]*(\S+)", l)
        if m:
            print(m.group(1)); break
' "${chart}/values.yaml")"
[ -n "${sw}" ] || fail "values.yaml 的 seaweedfs 段没有 enabled：与 provider 同向的那一格读不到"
if [ "${provider}" = "seaweedfs" ]; then
    [ "${sw}" = "true" ] || fail "provider 是 seaweedfs，而 values 的 seaweedfs.enabled 是 ${sw}：声明的 provider 与真正部署的后端不同"
else
    [ "${sw}" = "false" ] || fail "provider 是 ${provider}，而 values 的 seaweedfs.enabled 是 ${sw}：两处声明各说各的"
fi

# 4. 单机投影与 json 同值。
k3s_line="$(grep -E '^  COGNEVA_STORAGE_PROVIDER:' "${repo}/deploy/k3s/configmap.yaml" | head -1 || true)"
[ -n "${k3s_line}" ] || fail "deploy/k3s/configmap.yaml 里没有 COGNEVA_STORAGE_PROVIDER"
k3s="$(printf '%s\n' "${k3s_line}" | sed -E 's/^[^:]*: *"?([^"]*)"?.*/\1/')"
[ "${k3s}" = "${provider}" ] \
    || fail "deploy/k3s/configmap.yaml 写的是 ${k3s}，cogneva.json 声明的是 ${provider}"

echo "PASS: 存储 provider 只有一处声明（模板是投影、渲染同值、values 同向、单机投影同值；${provider}）"
