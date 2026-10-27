#!/usr/bin/env bash
# Cogneva 多节点镜像增量升级：把新的镜像 tar.gz 分发到全部节点并滚动重启应用。
# 前提：bootstrap 已完成首次部署（分发器 DaemonSet 与镜像服务 Pod 常设保留）。
#
# 用法：
#   deploy/scripts/distribute-image.sh dist/cogneva-image-v0.1.21-linux-x86_64.tar.gz
#   SKIP_RESTART=1 deploy/scripts/distribute-image.sh <tar.gz>   # 只分发不重启应用
#   deploy/scripts/distribute-image.sh --list-workloads          # 只印会滚哪些负载
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 换镜像面由交付树回答（deploy/scripts/platform-image-workloads.sh）：镜像导入
# 到节点之后，跑这个镜像的不只是几个 Deployment——宿主账本 / 进程普查 / 声明仓
# 是 DaemonSet，注册仓的卷走查是边车。名单写在这里时，漏掉的那些不会红，它们
# 只是继续跑旧二进制，而「全部节点已导入新镜像」照样印得出来。
platform_image_workloads() {
    bash "${here}/platform-image-workloads.sh"
}

TAR="${1:?用法: distribute-image.sh <image.tar.gz> | --list-workloads}"
if [ "$TAR" = "--list-workloads" ]; then
    # 这条面不构建、不碰集群：集合能被单独读一次，也就能被单独测。
    platform_image_workloads
    exit $?
fi
[ -f "$TAR" ] || { echo "文件不存在: $TAR"; exit 1; }
NS=cogneva

echo "==> 注入新镜像包到分发服务 Pod"
# 镜像服务是 Deployment（裸 Pod 被驱逐后无控制器重建，2026-09-14 实证导致
# 分发器永久 CrashLoop）；按 label 解析当前 Pod 名再 cp
kubectl -n "$NS" get deployment cogneva-image-server >/dev/null 2>&1 || {
    echo "错误: 镜像服务 Deployment cogneva-image-server 不存在。"
    echo "恢复: 重跑 bootstrap，或把 deploy/k8s/image-distributor.yaml 里的"
    echo "      __BUSYBOX_IMAGE__ 替换为可用 busybox 镜像后 kubectl apply。"
    exit 1
}
kubectl -n "$NS" wait --for=condition=Available deployment/cogneva-image-server --timeout=600s
SERVER_POD=$(kubectl -n "$NS" get pod -l app=cogneva-image-server -o jsonpath='{.items[0].metadata.name}')
[ -n "$SERVER_POD" ] || { echo "错误: 找不到镜像服务 Pod"; exit 1; }
kubectl -n "$NS" cp "$TAR" "$SERVER_POD":/share/image.tar.gz

echo "==> 触发全节点重新导入"
kubectl -n "$NS" rollout restart daemonset/cogneva-image-distributor
kubectl -n "$NS" rollout status daemonset/cogneva-image-distributor --timeout=900s

if [ "${SKIP_RESTART:-0}" != "1" ]; then
    workloads="$(platform_image_workloads)" || {
        echo "换镜像面的工作负载读不出来（理由见上），拒绝滚动一批判不了的集合" >&2
        exit 1
    }
    echo "==> 滚动重启应用负载（$(printf '%s\n' "$workloads" | wc -l) 个容器）"
    # 一个工作负载只重启一次、只等一次：同一对象上两个容器时，第二条命令等的是
    # 同一个滚动，读起来却像是两个各自独立的证据。
    while IFS=$'\t' read -r kind name; do
        [ -n "$kind" ] || continue
        kubectl -n "$NS" rollout restart "$(printf '%s' "$kind" | tr 'A-Z' 'a-z')/${name}"
    done < <(printf '%s\n' "$workloads" | cut -f1,2 | sort -u)
    while IFS=$'\t' read -r kind name; do
        [ -n "$kind" ] || continue
        kubectl rollout status -n "$NS" "$(printf '%s' "$kind" | tr 'A-Z' 'a-z')/${name}" --timeout=300s
    done < <(printf '%s\n' "$workloads" | cut -f1,2 | sort -u)
fi

echo "==> 完成：全部节点已导入新镜像"
