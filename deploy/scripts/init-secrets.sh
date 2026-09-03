#!/usr/bin/env bash
# cogneva 密钥初始化：幂等初始化 cogneva-secrets。
#
# 元启动（bootstrap）在 apply 预渲染清单前自动调用本脚本，无需手动运行。
# 内部实例密钥（数据库/缓存/内部签名）首次安装时自动生成强随机值，
# 已存在则一律跳过、绝不覆盖（保护带外写入的平台 token 与既有密码）。
# 平台 token、LLM 上游等带外凭证不在此生成，留空由 WebUI 向导或
# kubectl edit secret 写入。
#
# 手动部署时用法（在 kubectl apply 之前运行一次；重复运行安全）：
#   bash deploy/scripts/init-secrets.sh
set -euo pipefail

NS="${COGNEVA_NS:-cogneva}"
SECRET=cogneva-secrets

# 生成 48 位十六进制强随机串（纯字母数字，可安全进入连接串/URL）。
gen() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24
  else
    head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}

echo "==> 确保命名空间 ${NS} 存在"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> 确保 Secret ${SECRET} 存在"
if ! kubectl -n "$NS" get secret "$SECRET" >/dev/null 2>&1; then
  kubectl -n "$NS" create secret generic "$SECRET"
fi

# 内部密钥：缺失（或为空）才生成随机值；非空则保留。
ensure_random() {
  local key="$1"
  local cur
  cur="$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.${key}}" 2>/dev/null || true)"
  if [ -n "$cur" ]; then
    echo "  ${key}: 已存在，保留不动"
    return
  fi
  local val b64
  val="$(gen)"
  b64="$(printf '%s' "$val" | base64 | tr -d '\n')"
  kubectl -n "$NS" patch secret "$SECRET" --type=json \
    -p="[{\"op\":\"add\",\"path\":\"/data/${key}\",\"value\":\"${b64}\"}]" >/dev/null
  echo "  ${key}: 已生成随机强密钥"
}

# 带外凭证：仅确保键存在（空占位），真值由向导/运维写入，脚本不生成。
ensure_blank() {
  local key="$1"
  local cur
  cur="$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.${key}}" 2>/dev/null || true)"
  if [ -z "$cur" ]; then
    kubectl -n "$NS" patch secret "$SECRET" --type=json \
      -p="[{\"op\":\"add\",\"path\":\"/data/${key}\",\"value\":\"\"}]" >/dev/null 2>&1 || true
  fi
}

echo "==> 内部实例密钥（自动随机生成，缺失才创建）"
ensure_random pg-password
ensure_random redis-password
ensure_random webhook-internal

echo "==> 带外凭证占位（留空，由 WebUI 向导或 kubectl edit secret 写入）"
ensure_blank llm-upstreams
ensure_blank llm-api-key
ensure_blank github-token
ensure_blank gitee-token
ensure_blank github-webhook-secret
ensure_blank gitee-webhook-token

cat <<'EOF'
==> 完成。元启动（bootstrap）会在 apply 清单前自动调用本脚本，无需手动运行。
    手动部署时，密钥就绪后部署对应 profile 的预渲染清单：
    kubectl apply -f deploy/rendered/k3s-single/   # 或 k3s-multi / k8s-standard
    平台 token / LLM 上游：经 WebUI 配置向导写入，或
    kubectl -n cogneva edit secret cogneva-secrets 后滚动对应 Deployment。
EOF
