#!/usr/bin/env bash
# 部署面读权限门禁：接口问了哪几种工作负载，RBAC 就得授哪几种。
#
# 这条门来自一次实测（2026-10-27）：`/api/v1/cluster/deployments` 改成读五种工作负载
# 之后，镜像发了、屏也换了，端点自己的回答是
#
#   读不到 StatefulSet：apiserver 返回 403 Forbidden；读不到 DaemonSet：…；
#   读不到 CronJob：…；读不到 Job：…
#
# 而工作负载那一栏仍然是一张看起来完整的表——11 行 Deployment。postgres、redis、三个
# DaemonSet、两个 CronJob 都不在，空白读起来像「本来就没有」。代码、测试、CI 全绿：
# 缺的是那条只授了 `deployments` 的 ClusterRole。
#
# 判据：`crates/cog-gateway/src/cluster.rs` 里每种工作负载问的那条 (group, resource)，
# 在两个投递路径的 ClusterRole `cogneva-topology-reader` 里都带 `list`：
#   1. `deploy/rendered/k3s-single/…-clusterrole-cogneva-topology-reader.yaml`（chart
#      渲染产物，helm 安装的那套集群读它）；
#   2. `deploy/k3s/gitops-puller-rbac.yaml`（静态清单，集群内 GitOps 拉取端 apply 它）。
#
# 只认 `list`：集合级读取要有 list，按名读的 get 规则替代不了它——这正是当年那四条
# 按名读的 deployments 规则没能救回来的原因。
#
# 用法：bash deploy/scripts/check-workload-reader-rbac.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

try:
    import yaml
except ImportError:
    sys.exit("缺少依赖：PyYAML（python3 -c 'import yaml' 要能过）")

ROOT = sys.argv[1]
SERVER = os.path.join(ROOT, "crates/cog-gateway/src/cluster.rs")
ROLE = "cogneva-topology-reader"
CARRIERS = [
    "deploy/rendered/k3s-single/16-clusterrole-cogneva-topology-reader.yaml",
    "deploy/k3s/gitops-puller-rbac.yaml",
]

if not os.path.isfile(SERVER):
    sys.exit(f"读不到 {os.path.relpath(SERVER, ROOT)}——这一面要问哪几种工作负载无从知道")
server = open(SERVER, encoding="utf-8").read()

# 每种工作负载问的那条路径：`/apis/<group>/v1/namespaces/{namespace}/<resource>`。
found = re.findall(
    r'format!\("/apis/([a-z.\-]+)/v1/namespaces/\{namespace\}/([a-z]+)"\)', server
)
wanted = sorted(set(found))
if not wanted:
    sys.exit(
        f"{os.path.relpath(SERVER, ROOT)}: 一处 `/apis/<group>/v1/namespaces/…` 都没读到——"
        "正则读错了形状，不是这一面真的不读工作负载"
    )

problems = []
for rel in CARRIERS:
    path = os.path.join(ROOT, rel)
    if not os.path.isfile(path):
        problems.append(f"读不到 {rel}——这条投递路径的读权限无从检查")
        continue
    granted = {}
    for doc in yaml.safe_load_all(open(path, encoding="utf-8")):
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "ClusterRole" or (doc.get("metadata") or {}).get("name") != ROLE:
            continue
        for rule in doc.get("rules") or []:
            verbs = rule.get("verbs") or []
            for group in rule.get("apiGroups") or []:
                for resource in rule.get("resources") or []:
                    granted.setdefault((group, resource), set()).update(verbs)
    if not granted:
        problems.append(f"{rel}: 没有 ClusterRole {ROLE}——这条投递路径上这份读权限不存在")
        continue
    for group, resource in wanted:
        verbs = granted.get((group, resource), set())
        if "list" not in verbs:
            problems.append(
                f"{rel}: {ROLE} 没有对 {group or 'core'}/{resource} 授 list"
                + (f"（只有 {sorted(verbs)}）" if verbs else "")
            )

if problems:
    sys.exit(
        "工作负载的读权限没跟上面向它的接口：\n  "
        + "\n  ".join(problems)
        + "\n代码读五种而 ClusterRole 只授一种时，屏幕上只会少几行——不报错，也不缺数据。"
    )

print(
    "ok："
    + "、".join(f"{group}/{resource}" for group, resource in wanted)
    + f" 在渲染产物与静态清单里都授了 list"
)
PYEOF
