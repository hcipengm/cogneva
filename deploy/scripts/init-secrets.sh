#!/usr/bin/env bash
# cogneva 密钥初始化：幂等初始化两个 Secret 对象。
#
# 元启动（bootstrap）在 apply 预渲染清单前自动调用本脚本，无需手动运行。
# 内部实例密钥（数据库/缓存/内部签名）首次安装时自动生成强随机值，
# 已存在则一律跳过、绝不覆盖（保护带外写入的平台 token 与既有密码）。
# 平台 token、LLM 上游等带外凭证不在此生成，留空由 WebUI 向导或
# kubectl edit secret 写入。
#
# 两个对象是权限边界，不是分类：cogneva-secrets 装业务 Pod 要读的键（整对象
# get），cogneva-secrets-oob 装只该由安全网关读的键。本脚本负责建出后者、把老
# 安装里混在前者的带外键搬过去，并把留空占位写在对的那一片上——这三件事都得在
# 清单 apply 之前完成，否则网关 Pod 起不来或向导写进一个没人读的键。
#
# 手动部署时用法（在 kubectl apply 之前运行一次；重复运行安全）：
#   bash deploy/scripts/init-secrets.sh
set -euo pipefail

NS="${COGNEVA_NS:-cogneva}"
SECRET=cogneva-secrets
OOB_SECRET=cogneva-secrets-oob

# 带外半的键表。列出而不按"内部表之外的都搬"来推断：后者会把运维用 kubectl
# 刚写进内部对象的新键也搬走，而那种搬移不报错、只在读它的那个进程那里变成
# 空值。多列一个不存在的键是无害的空操作，少列一个才会留下残留副本。
OOB_KEYS=(
  llm-upstreams
  github-token
  gitee-token
  github-webhook-secret
  gitee-webhook-token
  gitee-oauth-client-secret
  github-oauth-client-secret
  notification-dingtalk-secret
  notification-feishu-secret
  github-app-id
  github-app-installation-id
  github-app-private-key
  git-ssh-private-key
  git-ssh-private-key-pending
  gitee-refresh-token
  clickhouse-password
)

# 生成 48 位十六进制强随机串（纯字母数字，可安全进入连接串/URL）。
gen() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24
  else
    head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}

# --- While the control plane has not settled yet ---------------------------
#
# The API server is allowed to be slow during an install: K3s has just come up,
# several hundred MB of image were just imported into containerd, and those
# pages are still being written back. A request can then come back
#   Error from server (Timeout): request did not complete within requested
#   timeout - context deadline exceeded
# which is the API server's own deadline expiring, not a rejected request.
# Measured 2026-09-30 on a blank machine: the fifth key's patch hit it, `set -e`
# ended the script with rc=1, and the entire install was reported as failed --
# while the same machine finished converging minutes later (12/12 workloads
# Running, /health/ready 200). "Not finished yet" was reported as "failed", and
# what the user sees is rc=1, so they run it again (idempotent, but a wasted
# wait) when the right move is to wait.
#
# So the two failure classes have to stay apart: timeout / cannot-connect means
# the control plane is slow, retry; a real refusal (invalid request, forbidden,
# NotFound) answers the same way every time, so return it on the first try
# rather than stretching the error out. Everything retried here is a read or a
# merge patch: a retry creates no second writer and cannot leave a half write.
KUBECTL_RETRY_ATTEMPTS="${COGNEVA_KUBECTL_RETRY_ATTEMPTS:-8}"
KUBECTL_RETRY_SLEEP="${COGNEVA_KUBECTL_RETRY_SLEEP:-5}"
KUBECTL_TMP="$(mktemp -d)"
trap 'rm -rf "${KUBECTL_TMP}"' EXIT

is_transient_api_error() {
  case "$1" in
    *"request did not complete"*) return 0 ;;
    *"context deadline exceeded"*) return 0 ;;
    *"Unable to connect to the server"*) return 0 ;;
    *"connection refused"*) return 0 ;;
    *"i/o timeout"* | *"TLS handshake timeout"*) return 0 ;;
    *"etcdserver:"*) return 0 ;;
    *"unable to return a response"*) return 0 ;;
    *"ServiceUnavailable"*) return 0 ;;
    *"error dialing backend"*) return 0 ;;
  esac
  return 1
}

# How long to wait after failure number `attempt`. Linear backoff: 5s, then 5s
# more each time, about 140s over eight attempts -- wider than the ~40s stall
# that was measured, and still not long enough to turn a real fault into minutes.
retry_sleep() {
  local attempt="$1"
  echo "  kubectl ${2:-}：控制面还没稳（第 ${attempt}/${KUBECTL_RETRY_ATTEMPTS} 次），$((attempt * KUBECTL_RETRY_SLEEP))s 后重试" >&2
  sleep $((attempt * KUBECTL_RETRY_SLEEP))
}

# Read: stdout is the result and goes through to the caller untouched; stderr
# lands in a file so the retry decision can be made from what was actually said.
kubectl_read() {
  local attempt=1 rc err
  while :; do
    if kubectl "$@" 2>"${KUBECTL_TMP}/err"; then
      return 0
    else
      rc=$?
    fi
    err="$(cat "${KUBECTL_TMP}/err")"
    printf '%s\n' "${err}" >&2
    [ "${attempt}" -ge "${KUBECTL_RETRY_ATTEMPTS}" ] && return "${rc}"
    is_transient_api_error "${err}" || return "${rc}"
    retry_sleep "${attempt}" "${1:-}"
    attempt=$((attempt + 1))
  done
}

# Write: the caller does not want this command's stdout (it discarded it before
# too), so keep it quiet and only surface what failed.
kubectl_write() {
  local attempt=1 rc err
  while :; do
    if kubectl "$@" >"${KUBECTL_TMP}/out" 2>"${KUBECTL_TMP}/err"; then
      return 0
    else
      rc=$?
    fi
    err="$(cat "${KUBECTL_TMP}/err")"
    printf '%s\n' "${err}" >&2
    [ "${attempt}" -ge "${KUBECTL_RETRY_ATTEMPTS}" ] && return "${rc}"
    is_transient_api_error "${err}" || return "${rc}"
    retry_sleep "${attempt}" "${1:-}"
    attempt=$((attempt + 1))
  done
}

# Read one key's base64 value. Three outcomes must stay apart: a value / rc=0
# with empty output (this key is confirmed absent) / rc!=0 (could not read).
# Reading the third as the second costs something: the next ensure_random mints
# a fresh random value for a key that a running workload may be using, and that
# write does not report itself -- it shows up at the next failed database
# connection or signature check instead. So an unreadable key stops the script.
secret_b64_of() {
  local object="$1" key="$2" out
  if ! out="$(kubectl_read -n "$NS" get secret "$object" -o "jsonpath={.data.${key}}")"; then
    echo "错误：读不到 ${NS}/${object} 的 ${key}（控制面没有回应）。" >&2
    echo "      「读不到」不等于「不存在」——当成不存在会给一个可能正在用的密钥换值。" >&2
    exit 1
  fi
  printf '%s' "${out}"
}

secret_b64() { secret_b64_of "$SECRET" "$1"; }
oob_secret_b64() { secret_b64_of "$OOB_SECRET" "$1"; }

# 对象里现有的键名，一行一个。用 go-template 而不是 jsonpath：键里有连字符和
# 点（`seaweedfs-s3.json`），jsonpath 的 `.data.<key>` 在带点的键上会静默取空，
# 而"取空"在这里的语义是"这个键不存在"——一次误读会让搬迁把一个真键留在原地。
secret_keys_of() {
  kubectl_read -n "$NS" get secret "$1" -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
}

ensure_object() {
  local object="$1"
  if ! kubectl_read -n "$NS" get secret "$object" >/dev/null 2>&1; then
    kubectl_write -n "$NS" create secret generic "$object"
  fi
}

# 幂等合并写一个键（merge patch：全新对象没有 data 字段，RFC 6902 的 add 会因
# 父路径不存在被拒）。
patch_key() {
  local object="$1" key="$2" b64="$3"
  kubectl_write -n "$NS" patch secret "$object" --type=merge \
    -p="{\"data\":{\"${key}\":\"${b64}\"}}"
}

echo "==> 确保命名空间 ${NS} 存在"
# The dry run is read into a variable first: inside `$( )` its exit code would
# be the exit code of `printf`, so a failure there would pass unnoticed.
ns_manifest="$(kubectl_read create namespace "$NS" --dry-run=client -o yaml)"
printf '%s' "${ns_manifest}" | kubectl_write apply -f -

echo "==> 确保 Secret ${SECRET} / ${OOB_SECRET} 存在"
ensure_object "$SECRET"
ensure_object "$OOB_SECRET"

# 搬迁：老安装把带外键与内部键混在 cogneva-secrets 里，这里把它们搬到带外对象，
# 再把副本从内部对象删掉。只搬不删等于没搬——内部对象是整对象 get，业务 Pod
# 照样读得到那份副本，而"读得到"正是这次分家要消掉的东西。
#
# 顺序是先搬后删，且只在带外那份为空时才写：两份都有值时以带外那份为准，它是
# 分家之后的唯一写点；内部那份此时是残留，删除它不会丢掉任何还在被读的值。
# 幂等：第二次跑时内部对象已没有这些键，整段是空操作。
echo "==> 搬迁带外键（${SECRET} → ${OOB_SECRET}，幂等）"
legacy_keys="$(secret_keys_of "$SECRET")"
for key in "${OOB_KEYS[@]}"; do
  printf '%s\n' "${legacy_keys}" | grep -qx -- "${key}" || continue
  if [ -z "$(oob_secret_b64 "$key")" ]; then
    patch_key "$OOB_SECRET" "$key" "$(secret_b64 "$key")"
    echo "  ${key}: 已搬到 ${OOB_SECRET}"
  else
    echo "  ${key}: ${OOB_SECRET} 已有值，内部那份按残留处理"
  fi
  # 单键删除用 JSON patch：merge patch 只能置键，删键要靠 null，而 null 在这里
  # 与"把值写成空的 base64"分不开。路径里的键名是本脚本的常量，不来自外部。
  kubectl_write -n "$NS" patch secret "$SECRET" --type=json \
    -p="[{\"op\":\"remove\",\"path\":\"/data/${key}\"}]" \
    || echo "  ${key}: 内部对象副本删除失败（下次运行会再试）" >&2
done

# 内部密钥：缺失（或为空）才生成随机值；非空则保留。
ensure_random() {
  local key="$1"
  local cur
  cur="$(secret_b64 "$key")"
  if [ -n "$cur" ]; then
    echo "  ${key}: 已存在，保留不动"
    return
  fi
  local val b64
  val="$(gen)"
  b64="$(printf '%s' "$val" | base64 | tr -d '\n')"
  patch_key "$SECRET" "$key" "$b64"
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
  cur="$(secret_b64 "$key")"
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
    patch_key "$SECRET" "$key" "$b64"
    echo "  ${key}: 已从宿主保留副本恢复（与上次安装是同一个实例）"
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    val="$(openssl rand -hex 32)"
  else
    val="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
  b64="$(printf '%s' "$val" | base64 | tr -d '\n')"
  patch_key "$SECRET" "$key" "$b64"
  keep_host_fingerprint "$val"
  echo "  ${key}: 生成了新实例指纹，并留了一份在 ${HOST_FINGERPRINT_FILE}"
  echo "        本机此前没有这个实例的保留副本：这是第一次安装，或宿主副本被清掉了。"
}

# Read one key's plaintext value (empty output when it is empty). An unreadable
# Secret stops the script, in secret_b64.
secret_value() {
  local b64
  b64="$(secret_b64 "$1")"
  printf '%s' "$b64" | base64 -d 2>/dev/null || true
}

# SeaweedFS S3 网关的身份文件：同时携带 accessKey 与 secretKey，是唯一能被
# 网关直接读入的形态。两个键先生成，再由它们拼出 JSON；JSON 已存在则保留，
# 避免与已生效的凭证错位。
ensure_s3_identity() {
  local key=seaweedfs-s3.json cur access secret json b64
  cur="$(secret_b64 "$key")"
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
  patch_key "$SECRET" "$key" "$b64"
  echo "  ${key}: 已生成（凭证取自 s3-access-key / s3-secret-key）"
}

# 带外凭证：仅确保键存在（空占位），真值由向导/网关写入，脚本不生成。
# 占位写在对的那一片上：写进内部对象等于给业务 Pod 留一个没人读、却又被整对象
# get 覆盖着的键，而真正读它的网关那边仍是缺键。
ensure_blank_oob() {
  local key="$1"
  local cur
  cur="$(oob_secret_b64 "$key")"
  if [ -z "$cur" ]; then
    # A failed placeholder is not fatal (the real value is written by the
    # wizard; this is the empty string), but it has to be said out loud: a
    # missing key keeps a workload that reads it by name from starting, and
    # staying quiet leaves that discovery for then. Delays from a slow control
    # plane are absorbed by kubectl_write.
    patch_key "$OOB_SECRET" "$key" "" \
      || echo "  ${key}: 占位未写入（真值仍可由向导写入）" >&2
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

echo "==> 带外凭证占位（留空，由 WebUI 向导或 kubectl edit secret ${OOB_SECRET} 写入）"
ensure_blank_oob llm-upstreams
ensure_blank_oob github-token
ensure_blank_oob gitee-token
ensure_blank_oob github-webhook-secret
ensure_blank_oob gitee-webhook-token
ensure_blank_oob gitee-oauth-client-secret
ensure_blank_oob github-oauth-client-secret
ensure_blank_oob notification-dingtalk-secret
ensure_blank_oob notification-feishu-secret

cat <<'EOF'
==> 完成。元启动（bootstrap）会在 apply 清单前自动调用本脚本，无需手动运行。
    手动部署时，密钥就绪后部署对应 profile 的预渲染清单：
    kubectl apply -f deploy/rendered/k3s-single/   # 或 k3s-multi / k8s-standard
    平台 token / LLM 上游：经 WebUI 配置向导写入，或
    kubectl -n cogneva edit secret cogneva-secrets-oob 后滚动对应 Deployment。
EOF
