#!/usr/bin/env bash
# SF-Network 可观测性栈 — K3s 卸载脚本

set -euo pipefail

NAMESPACE="monitoring"

echo "警告：这将删除 monitoring namespace 中的所有资源（包括持久化数据）！"
read -r -p "确认卸载? [y/N] " response
if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "已取消"
    exit 0
fi

echo "卸载 Helm releases..."
helm uninstall kube-prometheus-stack -n "${NAMESPACE}" 2>/dev/null || true
helm uninstall loki -n "${NAMESPACE}" 2>/dev/null || true
helm uninstall jaeger -n "${NAMESPACE}" 2>/dev/null || true

echo "删除 Namespace (将级联删除所有资源)..."
kubectl delete namespace "${NAMESPACE}" --wait=false 2>/dev/null || true

echo "卸载完成。PVC 中的持久化数据如需彻底删除，请手动清理："
echo "  kubectl get pvc -n ${NAMESPACE}"
