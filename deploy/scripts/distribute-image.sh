#!/usr/bin/env bash
# Cogneva 多节点镜像增量升级：把新的镜像 tar.gz 分发到全部节点并滚动重启应用。
# 前提：bootstrap 已完成首次部署（分发器 DaemonSet 与镜像服务 Pod 常设保留）。
#
# 用法：
#   deploy/scripts/distribute-image.sh dist/cogneva-image-v0.1.21-linux-x86_64.tar.gz
#   SKIP_RESTART=1 deploy/scripts/distribute-image.sh <tar.gz>   # 只分发不重启应用
set -euo pipefail

TAR="${1:?用法: distribute-image.sh <image.tar.gz>}"
[ -f "$TAR" ] || { echo "文件不存在: $TAR"; exit 1; }
NS=cogneva

echo "==> 注入新镜像包到分发服务 Pod"
kubectl -n "$NS" wait --for=condition=Ready pod/cogneva-image-server --timeout=600s
kubectl -n "$NS" cp "$TAR" cogneva-image-server:/share/image.tar.gz

echo "==> 触发全节点重新导入"
kubectl -n "$NS" rollout restart daemonset/cogneva-image-distributor
kubectl -n "$NS" rollout status daemonset/cogneva-image-distributor --timeout=900s

if [ "${SKIP_RESTART:-0}" != "1" ]; then
    echo "==> 滚动重启应用负载"
    kubectl -n "$NS" rollout restart deployment/cogneva deployment/cogneva-security-gateway deployment/cogneva-evolution 2>/dev/null || \
    kubectl -n "$NS" rollout restart deployment
    kubectl -n "$NS" rollout status deployment/cogneva --timeout=300s
fi

echo "==> 完成：全部节点已导入新镜像"
