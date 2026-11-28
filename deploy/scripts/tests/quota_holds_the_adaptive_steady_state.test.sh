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
# 连不上集群退 4：这一格只在平台宿主上判，开发机不是判它的地方。连上了却读不出来
# 退 3（没判）——「量不到」不是「够了」，「这里不是判它的地方」也不是「没判成」。
set -uo pipefail

ns="${COGNEVA_NAMESPACE:-cogneva}"
# 集群的连接凭据只有 root 读得到（`/etc/rancher/k3s/k3s.yaml`），宿主上的其余读数口
# 都走这一条（`deploy/scripts/read-resource-account.sh` 的 `COGNEVA_KUBECTL` 同一个
# 默认值：`sudo -n k3s kubectl`，k3s 自己带 kubeconfig）。按裸 `kubectl` 判，这一格在
# 宿主上永远是「读不到」——2026-11-23 实测：裸 `kubectl` 连的是 `127.0.0.1:8080`，
# 集群明明就在，而三档里的「读不到」与「不够」被压成同一个红，量不到的那一格看起来
# 像判过了。
kubectl_cmd="${COGNEVA_KUBECTL:-sudo -n k3s kubectl}"
command -v "${kubectl_cmd%% *}" >/dev/null 2>&1 ||
    { echo "quota: 没有 ${kubectl_cmd%% *}，这一格只在平台宿主上判。" >&2; exit 4; }
kube() { ${kubectl_cmd} "$@"; }

# 「连不上集群」与「连上了但配额读不出来」是两件事：前者是这台机器不是判这一格的地方
# （平台宿主判），后者是判了但没判成。前者退 4（这一格不在这里判），后者退 3（没判成）——
# 两者都答不出「够了没有」，但动作相反：前者换地方，后者要人去看。
kube version --request-timeout=5s >/dev/null 2>&1 ||
    { echo "quota: 本机连不上集群，这一格只在平台宿主上判。" >&2; exit 4; }

hard=$(kube -n "$ns" get quota cogneva-quota -o jsonpath='{.spec.hard.requests\.cpu}' 2>/dev/null) || exit 3
used=$(kube -n "$ns" get quota cogneva-quota -o jsonpath='{.status.used.requests\.cpu}' 2>/dev/null) || exit 3
[ -n "$hard" ] && [ -n "$used" ] || { echo "quota: 配额读不出来，这一条没判。" >&2; exit 3; }

# 一次滚动的重叠：取所有 Deployment 里最大的那份 requests（被替换的那一个）。
largest=$(kube -n "$ns" get deploy -o jsonpath='{range .items[*]}{.spec.template.spec.containers[*].resources.requests.cpu}{"\n"}{end}' 2>/dev/null | sort -h | tail -1)
[ -n "$largest" ] || { echo "quota: Deployment 的 requests 读不出来，这一条没判。" >&2; exit 3; }

to_milli() { case "$1" in *m) echo "${1%m}";; *\.[0-9]) echo "$1" | awk '{printf "%d", $1*1000}';; *) echo "$1" | awk '{printf "%d", $1*1000}';; esac; }
h=$(to_milli "$hard"); u=$(to_milli "$used"); l=$(to_milli "$largest")

need=$((u + l))
if [ "$h" -lt "$need" ]; then
  printf 'FAIL: 配额 %sm 装不下「稳态 %sm + 一次滚动 %sm = %sm」——下一次重建会撞线。\n' "$h" "$u" "$l" "$need" >&2
  exit 1
fi
printf 'ok    - 配额 %sm ≥ 稳态 %sm + 一次滚动 %sm，余量 %sm\n' "$h" "$u" "$l" "$((h - u))"
