#!/usr/bin/env bash
# 轮换 Grafana 管理员密码

set -euo pipefail

NAMESPACE="monitoring"
SECRET_NAME="grafana-admin-credentials"

NEW_PASSWORD=$(openssl rand -base64 32)

kubectl patch secret "${SECRET_NAME}" -n "${NAMESPACE}" \
  --type='json' \
  -p='[{"op": "replace", "path": "/data/admin-password", "value":"'"$(echo -n "$NEW_PASSWORD" | base64 -w 0)"'"}]'

# 触发 Grafana Pod 重启以加载新密码
kubectl rollout restart deployment/sf-observability-grafana -n "${NAMESPACE}"

echo "Grafana 密码已更新并滚动重启"
echo "新密码: ${NEW_PASSWORD}"
echo "请妥善保存"
