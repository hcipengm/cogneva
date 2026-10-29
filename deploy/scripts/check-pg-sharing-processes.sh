#!/usr/bin/env bash
# 每个连 Postgres 的常驻工作负载都在同一台服务器上各开一套连接池，而每套池的上限
# 是把服务器的连接上限按「有几个进程在分摊」算出来的。那个数写在 ConfigMap 里，
# 而真正在连的进程数是拓扑的函数——两者分开写就会漂移，且漂移只在滚动时显形：
# 平时两个进程各占一份、余量看着够；滚动的那几十秒里新 Pod 已经连上、旧 Pod 还没
# 退出，多出来的那一份正好吃掉余量。实测 2026-10-30 新 Pod 报
# `pool timed out while waiting for an open connection`，同一时刻
# `pg_stat_activity` 里还有空闲连接——它们属于别的池，池之间不能互借。
#
# 判据：渲染产物里 COGNEVA_PG_SHARING_PROCESSES ≥ 带 COGNEVA_DATABASE_URL 的常驻
# Deployment 的副本数之和 + 1。加的那个是滚动影子。用副本数而不是 Deployment 个数：
# 一个 Deployment 扩到两副本就多一个进程，数 Deployment 会把它当成没变。
#
# 读渲染产物而不读 chart 模板：模板里的表达式渲染之后才是真正下发的那个数，读模板
# 会连「表达式写对了但没生效」也放过。
#
# 用法：bash deploy/scripts/check-pg-sharing-processes.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - deploy/rendered <<'PY'
import glob
import os
import sys

import yaml

base = sys.argv[1]
if not os.path.isdir(base):
    sys.exit(f"找不到 {base}/：先跑 deploy/scripts/render-deploy.sh")

profiles = sorted(
    name for name in os.listdir(base) if os.path.isdir(os.path.join(base, name))
)
if not profiles:
    sys.exit(f"{base}/ 下没有 profile 目录，渲染没跑过")

failed = 0
for profile in profiles:
    directory = os.path.join(base, profile)
    declared = None
    processes = 0
    for path in sorted(glob.glob(os.path.join(directory, "*.yaml"))):
        with open(path, encoding="utf-8") as handle:
            for document in yaml.safe_load_all(handle):
                if not document:
                    continue
                kind = document.get("kind")
                if kind == "ConfigMap" and document.get("metadata", {}).get("name") == "cogneva-config":
                    declared = document.get("data", {}).get("COGNEVA_PG_SHARING_PROCESSES")
                elif kind == "Deployment":
                    pod = document.get("spec", {}).get("template", {}).get("spec", {})
                    names = {
                        entry.get("name")
                        for container in pod.get("containers") or []
                        for entry in container.get("env") or []
                    }
                    # envFrom 的那份在 ConfigMap 里，这里只数直接声明的那一族：
                    # 数据库地址从 Secret 注入，不经过共用 ConfigMap，所以它出现在
                    # 容器自己的 env 上，也就只有真连库的工作负载有。
                    if "COGNEVA_DATABASE_URL" in names:
                        processes += int(document.get("spec", {}).get("replicas", 1))

    if declared is None:
        print(f"FAIL {profile}: ConfigMap cogneva-config 没有 COGNEVA_PG_SHARING_PROCESSES")
        failed += 1
        continue
    try:
        share = int(str(declared).strip())
    except ValueError:
        print(f"FAIL {profile}: COGNEVA_PG_SHARING_PROCESSES={declared!r} 不是整数")
        failed += 1
        continue

    needed = processes + 1
    if processes == 0:
        print(f"FAIL {profile}: 没有任何带 COGNEVA_DATABASE_URL 的 Deployment；判据读不到消费者")
        failed += 1
    elif share < needed:
        print(
            f"FAIL {profile}: COGNEVA_PG_SHARING_PROCESSES={share}，"
            f"但连库的常驻进程有 {processes} 个，滚动时 {needed} 个同时在线 —— "
            "少算的那份会在滚动里被当成余量用掉"
        )
        failed += 1
    else:
        print(f"ok   {profile}: 分摊 {share} 份 ≥ 常驻 {processes} + 滚动影子 1")

sys.exit(1 if failed else 0)
PY
