#!/usr/bin/env bash
# 配额必须装得下「稳态占用 + 一次滚动的重叠」——这是 S75 的判据。
#
# 为什么不是「装下渲染集声明的量」：`workload_resources.rs` 会按实测把 Pod 的
# requests 抬高（线上是 5040m，渲染集写的是 500m），而配额是从**渲染集**投影出来的。
# 于是投影算的是 3 核、线上真实占用 8 核以上，任何一次重建都撞线——2026-11-21 宕过一次。
#
# 读的是现网：`ResourceQuota.hard` 与 `used`，以及各 Deployment 声明的 requests
# （滚动时新旧两份同时存在，重叠量就是被替换那个 Pod 的 requests）。
#
# 用例名（读数点它）：quota_covers_steady_state_plus_one_rollover
#
# 读不到就退 3（没判）：这台机器没配置集群时，「量不到」不是「够了」。
set -uo pipefail

command -v kubectl >/dev/null 2>&1 || { echo "quota: 没有 kubectl，这一条没判。" >&2; exit 3; }
ns="${COGNEVA_NAMESPACE:-cogneva}"

hard=$(kubectl -n "$ns" get quota cogneva-quota -o jsonpath='{.spec.hard.requests\.cpu}' 2>/dev/null) || exit 3
used=$(kubectl -n "$ns" get quota cogneva-quota -o jsonpath='{.status.used.requests\.cpu}' 2>/dev/null) || exit 3
[ -n "$hard" ] && [ -n "$used" ] || { echo "quota: 配额读不出来，这一条没判。" >&2; exit 3; }

# 一次滚动的重叠：取所有 Deployment 里最大的那份 requests（被替换的那一个）。
largest=$(kubectl -n "$ns" get deploy -o jsonpath='{range .items[*]}{.spec.template.spec.containers[*].resources.requests.cpu}{"\n"}{end}' 2>/dev/null | sort -h | tail -1)
[ -n "$largest" ] || { echo "quota: Deployment 的 requests 读不出来，这一条没判。" >&2; exit 3; }

to_milli() { case "$1" in *m) echo "${1%m}";; *\.[0-9]) echo "$1" | awk '{printf "%d", $1*1000}';; *) echo "$1" | awk '{printf "%d", $1*1000}';; esac; }
h=$(to_milli "$hard"); u=$(to_milli "$used"); l=$(to_milli "$largest")

need=$((u + l))
if [ "$h" -lt "$need" ]; then
  printf 'FAIL: 配额 %sm 装不下「稳态 %sm + 一次滚动 %sm = %sm」——下一次重建会撞线。\n' "$h" "$u" "$l" "$need" >&2
  exit 1
fi
printf 'ok    - 配额 %sm ≥ 稳态 %sm + 一次滚动 %sm，余量 %sm\n' "$h" "$u" "$l" "$((h - u))"
