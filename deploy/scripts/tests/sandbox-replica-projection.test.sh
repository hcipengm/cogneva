#!/usr/bin/env bash
# 沙箱执行器的副本数是**部署里的一个数**，不是模板里的一处硬编码：加一副副本，
# 就多一块卷、多一条客户端能钉上去的地址。这两件事都由 chart 从
# `sandboxExecutor.replicas` 投影出来：
#
#   1. 卷：StatefulSet 的 `volumeClaimTemplates`——一副副本一块，卷名是
#      `<模板>-<StatefulSet>-<序号>`，容器按 `$(POD_NAME)` 拼出自己那块；
#   2. 地址：`cogneva.json` 的 `system.sandbox_executor_url`，一条副本一个无头
#      Service 下的稳定 DNS 名。客户端按 agent 把 lane 钉在一条地址上。
#
# 判据是「换输入 ⇒ 值跟着变」：把 replicas 从 1 改成 3 再渲染一遍，卷与地址两条
# 都必须变成 3 条。少了任何一条，故障读起来仍然是绿的——一个地址照样能跑，只是
# 所有 lane 挤在同一副副本上，而那块卷写在每一副 Pod 的同一个路径上。
#
# 需要的工具与 check-deploy-parity.sh 同一套：helm、python3-yaml。
# 用法：bash deploy/scripts/tests/sandbox-replica-projection.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }

for bin in helm python3; do
    command -v "${bin}" >/dev/null || { echo "缺少依赖：${bin}" >&2; exit 2; }
done
python3 -c 'import yaml' 2>/dev/null || { echo "缺少依赖：python3-yaml" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

render() { # render <输出文件> [--set …]
    local out="$1"; shift
    bash "${repo}/deploy/scripts/render-chart.sh" k3s-single -- "$@" >"${out}" \
        || fail "渲染失败（k3s-single $*）"
    [ -s "${out}" ] || fail "渲染出来是空的（k3s-single $*）"
}

render "${work}/one.yaml"
render "${work}/three.yaml" --set sandboxExecutor.replicas=3

python3 - "${work}/one.yaml" "${work}/three.yaml" <<'PY'
import json
import sys

import yaml


def objects(path):
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if isinstance(d, dict)]


def one(docs, kind, name, path):
    hits = [
        d
        for d in docs
        if d.get("kind") == kind and (d.get("metadata") or {}).get("name") == name
    ]
    assert len(hits) == 1, f"{path}: {kind}/{name} 渲染出 {len(hits)} 份"
    return hits[0]


def check(path, replicas):
    docs = objects(path)
    sts = one(docs, "StatefulSet", "cogneva-sandbox-executor", path)
    headless = one(docs, "Service", sts["spec"]["serviceName"], path)
    config = one(docs, "ConfigMap", "cogneva-json", path)
    body = json.loads(config["data"]["cogneva.json"])

    # 1. 副本数就是传进去的那一个数。
    assert sts["spec"]["replicas"] == replicas, (
        f"{path}: replicas={sts['spec']['replicas']}，期望 {replicas}"
    )
    # 2. 一副副本一块卷：卷模板必须存在，且模板名与容器挂载的那个卷名对得上。
    templates = (sts["spec"].get("volumeClaimTemplates") or [])
    assert templates, f"{path}: StatefulSet 没有 volumeClaimTemplates，卷不是每副副本自己的"
    names = []
    for t in templates:
        names.append(t["metadata"]["name"])
        assert not t.get("spec", {}).get("volumeName"), (
            f"{path}: 卷模板绑定了 volumeName，那就不是一副副本一块卷了"
        )
    # 3. Pod 模板里不能有同名的 volumes 条目：卷由控制器注入，写两处就是两份事实。
    pod = sts["spec"]["template"]["spec"]
    pod_volumes = [v.get("name") for v in (pod.get("volumes") or [])]
    for name in names:
        assert name not in pod_volumes, (
            f"{path}: 卷 {name} 既有 volumeClaimTemplates 又有 Pod 级 volumes 条目"
        )
    mounted = {
        m.get("name")
        for c in (pod.get("containers") or []) + (pod.get("initContainers") or [])
        for m in (c.get("volumeMounts") or [])
    }
    for name in names:
        assert name in mounted, f"{path}: 卷模板 {name} 没有被任何容器挂载"

    # 4. 地址表：一条副本一个 DNS 名，序号从 0 起，端口是无头 Service 的那个。
    port = headless["spec"]["ports"][0]["port"]
    assert headless["spec"].get("clusterIP") == "None", (
        f"{path}: {sts['spec']['serviceName']} 不是无头 Service，每副副本就没有稳定 DNS"
    )
    expected = [
        f"http://cogneva-sandbox-executor-{i}.{sts['spec']['serviceName']}:{port}"
        for i in range(replicas)
    ]
    got = [e.strip() for e in body["system"]["sandbox_executor_url"].split(",")]
    assert got == expected, f"{path}: 地址表是 {got}，期望 {expected}"


check(sys.argv[1], 1)
check(sys.argv[2], 3)
print("SANDBOX REPLICA PROJECTION OK：副本数 1→3，卷模板与地址表都跟着变")
PY
