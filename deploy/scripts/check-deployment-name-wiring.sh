#!/usr/bin/env bash
# 部署自名（COGNEVA_DEPLOYMENT_NAME）的接线门禁。
#
# 这条 env 是样本日志那几条 gauge 的**写者身份**：容量与容量判定是每个部署自己的
# 声明，却落在所有部署共用的那张表里，series 键就是标签集。名字写错或写得和大家
# 一样，后果不是「读数难看」而是**顶替**——置 0（不清理）的部署与拿 200000 的部署
# 落在同一条 series 上，地板行归最后写的那家，读到的是谁写的看不出来。
#
# 为什么值必须等于工作负载自己的 metadata.name，而不能取 Pod 标签：
# `app.kubernetes.io/name` 是**应用**名，同一个镜像变出来的每个工作负载都带着
# 同一个值（本集群实测七个：主应用、安全网关、cluster-registry、沙箱执行器、
# buildah、backup、mainline Job），于是「别家不许顶这一家的声明」那道闸对它们全失效。
# 这正是 git-identity 那条门禁的同一形状：渲染期没有症状，清单合法、parity 也绿。
#
# 判据（都从渲染产物推导，不重复 values 里的任何常量）：
#   1. 凡设了这条 env 的工作负载，值必须是**字面量**——不是 valueFrom（那正是
#      「取了个别人也有的标签」的写法），且必须等于它自己的 metadata.name。
#   2. 两条之间的值必须互不相同：同值即两条工作负载共用一个身份，与第 1 条同时
#      成立时不可能，单列出来是为了让「改名只改了一处」也在同一条判据下暴露。
#   3. 域不能空、也不能缩到一条：今天在说话的是主应用与 evolution 两条，下界取 2。
#      这条下界是**声明**而不是推导——谁在跑那条 cap 循环是代码里的事（建在
#      `if let Some(pool) = self.pg_pool` 那一支上），清单里读不出来。所以它不假装
#      是闭集：改名、删 env、或把两条并成一条都会撞它。
#
# 用法：bash deploy/scripts/check-deployment-name-wiring.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - deploy/rendered <<'PYEOF'
import glob, os, sys

import yaml

RENDERED = sys.argv[1]
ENV_NAME = "COGNEVA_DEPLOYMENT_NAME"
# 工作负载里能承载这条 env 的 kind；CronJob 的 Pod 模板在 jobTemplate 下面。
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "CronJob")
MIN_NAMED = 2

profiles = sorted(d for d in os.listdir(RENDERED)
                  if os.path.isdir(os.path.join(RENDERED, d)))
if not profiles:
    sys.exit(f"没有可校验的渲染产物：{RENDERED}")


def docs(profile):
    for path in sorted(glob.glob(os.path.join(RENDERED, profile, "*.yaml"))):
        with open(path) as fh:
            for doc in yaml.safe_load_all(fh):
                if isinstance(doc, dict) and doc.get("kind"):
                    yield path, doc


def pod_template(doc):
    spec = doc["spec"]
    if doc["kind"] == "CronJob":
        return spec["jobTemplate"]["spec"]["template"]
    return spec["template"]


def env_entries(tpl):
    """(container, env) 对，含 initContainers——两边都可能带这条 env。"""
    for c in (tpl["spec"].get("containers") or []) + (tpl["spec"].get("initContainers") or []):
        for e in c.get("env") or []:
            yield c.get("name"), e


failures = []
for profile in profiles:
    named = []          # (workload, value) —— 只见字面量的那些
    for path, doc in docs(profile):
        if doc["kind"] not in WORKLOAD_KINDS:
            continue
        workload = f'{doc["kind"]}/{doc["metadata"]["name"]}'
        for container, env in env_entries(pod_template(doc)):
            if env.get("name") != ENV_NAME:
                continue
            where = f"{profile}:{workload}:{container}:{os.path.basename(path)}"
            value = env.get("value")
            if env.get("valueFrom") or value is None:
                failures.append(
                    f"{where}：{ENV_NAME} 不是字面量。取 Pod 标签（或任何 valueFrom 来源）"
                    f"时值来自**别人也有的那个标签**，本集群实测 `app.kubernetes.io/name`"
                    f" 一个值被七个工作负载共用，这道身份闸对它们全失效")
                continue
            if value != doc["metadata"]["name"]:
                failures.append(
                    f"{where}：{ENV_NAME}={value!r} 不等于工作负载自己的名字 "
                    f"{doc['metadata']['name']!r}（写者身份必须是自己，不是应用名、不是别人）")
            named.append((workload, value))

    if len(named) < MIN_NAMED:
        failures.append(
            f"{profile}：只有 {len(named)} 条工作负载设了 {ENV_NAME}，下界是 {MIN_NAMED}"
            f"（样本日志的容量声明按部署记名，说话的那两个部署各是一条）")
    seen = {}
    for workload, value in named:
        if value in seen:
            failures.append(
                f"{profile}：{workload} 与 {seen[value]} 同用 {ENV_NAME}={value!r}，"
                f"两条工作负载共用一个身份 ⇒ 一家的声明会被另一家顶掉")
        seen[value] = workload
    if not failures:
        print(f"{profile}: " + ", ".join(f"{w}={v}" for w, v in sorted(named)), flush=True)

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    sys.exit(1)
print("DEPLOYMENT NAME WIRING OK：每条设了写者身份的工作负载都用字面量报自己的名字，两两不同")
PYEOF
