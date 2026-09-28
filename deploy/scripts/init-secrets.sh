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
  # 用 merge patch 而非 JSON patch：全新的 Secret 没有 data 字段，
  # RFC 6902 的 add 因父路径 /data 不存在而被 API server 拒绝
  # （The request is invalid）。merge patch 对 map 是"置键"，语义等价。
  kubectl -n "$NS" patch secret "$SECRET" --type=merge \
    -p="{\"data\":{\"${key}\":\"${b64}\"}}" >/dev/null
  echo "  ${key}: 已生成随机强密钥"
}

# 实例身份指纹：必须随 Secret 存活，是实例身份的规范来源。容器里采集不到
# machine-id，指纹素材只剩 Pod 主机名与 veth MAC（每轮重启都变），所以身份
# 不能靠容器内推导——这里生成一次并永久保留，重装/换机器带上同一个 Secret
# 就是同一个实例。64 位十六进制，与机器指纹同形。
#
# A copy is kept on the host as well. A reinstall takes the namespace and the
# Secret down together -- that is what happened on 2026-09-23 -- and the Secret
# is the identity's only normative source, so once it is gone nothing on the
# cluster can say what the last instance was called. The host copy does not go
# with it, so every install writes one and a missing Secret is restored from it
# first. Losing it is not losing a configuration value: the fingerprint picks
# the name, the name is the git author, so the next revision is committed under
# a name nobody has seen and everyone tracing self-authored changes by author
# finds none of them.
HOST_STATE_DIR="${COGNEVA_HOST_STATE_DIR:-$HOME/.cogneva-ops}"
HOST_FINGERPRINT_FILE="${COGNEVA_HOST_FINGERPRINT_FILE:-$HOST_STATE_DIR/instance-fingerprint}"

# The shape a fingerprint has. A copy only counts if it has that shape: content
# that is non-empty but not 64 hex digits is a damaged file or something else
# written there, and it can be neither used as the identity (that would restore
# an instance nothing has ever seen) nor treated as absent (that is the silent
# rename this exists to prevent).
FINGERPRINT_RE='^[0-9a-fA-F]{64}$'

# Write the (0600) host copy. A write failure only warns: the identity itself
# is still valid, there is just nothing to carry into the next reinstall.
keep_host_fingerprint() {
  local val="$1"
  if [ ! -d "$HOST_STATE_DIR" ]; then
    mkdir -p "$HOST_STATE_DIR" 2>/dev/null || true
    chmod 700 "$HOST_STATE_DIR" 2>/dev/null || true
  fi
  if ! (umask 077 && printf '%s\n' "$val" > "$HOST_FINGERPRINT_FILE"); then
    echo "  instance-fingerprint: 警告：宿主保留副本 ${HOST_FINGERPRINT_FILE} 写不进去；" >&2
    echo "        下次重装若 Secret 一并丢失，实例会换名。" >&2
    return
  fi
  chmod 600 "$HOST_FINGERPRINT_FILE" 2>/dev/null || true
}

ensure_fingerprint() {
  local key=instance-fingerprint cur val b64
  cur="$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.${key}}" 2>/dev/null || true)"
  if [ -n "$cur" ]; then
    echo "  ${key}: 已存在，保留不动"
    # Backfill the host copy: this Secret may predate the copy, in which case
    # its next reinstall would rename the instance as before. Writing it now
    # changes nothing about the identity in use, only about the next reinstall.
    val="$(printf '%s' "$cur" | base64 -d 2>/dev/null || true)"
    if [ -n "$val" ]; then
      keep_host_fingerprint "$val"
    fi
    return
  fi
  # Nothing on the cluster. The host copy decides: restoring it is the same
  # instance, so the author name does not change hands.
  if [ -f "$HOST_FINGERPRINT_FILE" ]; then
    val="$(tr -d '[:space:]' < "$HOST_FINGERPRINT_FILE")"
    if ! printf '%s' "$val" | grep -qE "$FINGERPRINT_RE"; then
      echo "错误：宿主保留副本 ${HOST_FINGERPRINT_FILE} 的内容不是一个指纹（64 位十六进制），" >&2
      echo "      本次拒绝新建身份。新建会静默换掉实例署名，而按旧署名在仓库里追踪自产" >&2
      echo "      变更的人会再也找不到它们。修好这个文件，或清空它并重跑，表示确认换新身份。" >&2
      exit 1
    fi
    b64="$(printf '%s' "$val" | base64 | tr -d '\n')"
    kubectl -n "$NS" patch secret "$SECRET" --type=merge \
      -p="{\"data\":{\"${key}\":\"${b64}\"}}" >/dev/null
    echo "  ${key}: 已从宿主保留副本恢复（与上次安装是同一个实例）"
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    val="$(openssl rand -hex 32)"
  else
    val="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
  b64="$(printf '%s' "$val" | base64 | tr -d '\n')"
  # 用 merge patch 而非 JSON patch：全新的 Secret 没有 data 字段，
  # RFC 6902 的 add 因父路径 /data 不存在而被 API server 拒绝
  # （The request is invalid）。merge patch 对 map 是"置键"，语义等价。
  kubectl -n "$NS" patch secret "$SECRET" --type=merge \
    -p="{\"data\":{\"${key}\":\"${b64}\"}}" >/dev/null
  keep_host_fingerprint "$val"
  echo "  ${key}: 生成了新实例指纹，并留了一份在 ${HOST_FINGERPRINT_FILE}"
  echo "        本机此前没有这个实例的保留副本：这是第一次安装，或宿主副本被清掉了。"
}

# 读 Secret 中某个键的明文值（空则输出空）。
secret_value() {
  local key="$1"
  kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.${key}}" 2>/dev/null \
    | base64 -d 2>/dev/null || true
}

# SeaweedFS S3 网关的身份文件：同时携带 accessKey 与 secretKey，是唯一能被
# 网关直接读入的形态。两个键先生成，再由它们拼出 JSON；JSON 已存在则保留，
# 避免与已生效的凭证错位。
ensure_s3_identity() {
  local key=seaweedfs-s3.json cur access secret json b64
  cur="$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.${key}}" 2>/dev/null || true)"
  if [ -n "$cur" ]; then
    echo "  ${key}: 已存在，保留不动"
    return
  fi
  ensure_random s3-access-key
  ensure_random s3-secret-key
  access="$(secret_value s3-access-key)"
  secret="$(secret_value s3-secret-key)"
  json="$(cat <<JSON
{
  "identities": [
    {
      "name": "cogneva",
      "credentials": [
        { "accessKey": "${access}", "secretKey": "${secret}" }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
JSON
)"
  b64="$(printf '%s' "$json" | base64 | tr -d '\n')"
  # 用 merge patch 而非 JSON patch：全新的 Secret 没有 data 字段，
  # RFC 6902 的 add 因父路径 /data 不存在而被 API server 拒绝
  # （The request is invalid）。merge patch 对 map 是"置键"，语义等价。
  kubectl -n "$NS" patch secret "$SECRET" --type=merge \
    -p="{\"data\":{\"${key}\":\"${b64}\"}}" >/dev/null
  echo "  ${key}: 已生成（凭证取自 s3-access-key / s3-secret-key）"
}

# 带外凭证：仅确保键存在（空占位），真值由向导/运维写入，脚本不生成。
ensure_blank() {
  local key="$1"
  local cur
  cur="$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath="{.data.${key}}" 2>/dev/null || true)"
  if [ -z "$cur" ]; then
    kubectl -n "$NS" patch secret "$SECRET" --type=merge \
      -p="{\"data\":{\"${key}\":\"\"}}" >/dev/null 2>&1 || true
  fi
}

echo "==> 内部实例密钥（自动随机生成，缺失才创建）"
ensure_random pg-password
ensure_random redis-password
ensure_random webhook-internal
ensure_random jwt-secret
# 进化 Pod 的签名密钥：与主应用刻意不同源（它持有部署器，不能并入用户会话
# 的鉴权域），但也必须跨重启稳定——它每推进一个 rev 就被重建一次，缺这条
# 就等于鉴权域每轮换签。
ensure_random evolution-jwt-secret
ensure_random meili-master-key

echo "==> 实例身份指纹（64 位十六进制，缺失才创建）"
ensure_fingerprint

echo "==> 对象存储 S3 凭证（缺失才创建）"
ensure_s3_identity

echo "==> 带外凭证占位（留空，由 WebUI 向导或 kubectl edit secret 写入）"
ensure_blank llm-upstreams
ensure_blank llm-api-key
ensure_blank github-token
ensure_blank gitee-token
ensure_blank github-webhook-secret
ensure_blank gitee-webhook-token
ensure_blank gitee-oauth-client-secret
ensure_blank github-oauth-client-secret
ensure_blank notification-dingtalk-secret
ensure_blank notification-feishu-secret

cat <<'EOF'
==> 完成。元启动（bootstrap）会在 apply 清单前自动调用本脚本，无需手动运行。
    手动部署时，密钥就绪后部署对应 profile 的预渲染清单：
    kubectl apply -f deploy/rendered/k3s-single/   # 或 k3s-multi / k8s-standard
    平台 token / LLM 上游：经 WebUI 配置向导写入，或
    kubectl -n cogneva edit secret cogneva-secrets 后滚动对应 Deployment。
EOF
