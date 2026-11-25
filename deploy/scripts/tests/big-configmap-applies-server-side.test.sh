#!/usr/bin/env bash
# 判据：凡是 apply `deploy/k3s/cogneva-json-configmap.yaml` 的那一行，必须带 `--server-side`。
#
# 为什么这一条要单独判：client-side apply 会把整份对象抄进
# `kubectl.kubernetes.io/last-applied-configuration` 注解，而注解的默认上限是
# Kubernetes 规定的 256KiB（262144 字节）。实测 2026-11-24 换版停在这一步：
#   The ConfigMap "cogneva-json" is invalid: metadata.annotations: Too long: may not be more than 262144 bytes
# 实测 2026-11-24 这份清单已经 767KB，并且只会继续长（它内嵌整份应用配置、告警规则与提示词）。
# server-side apply 不写那条注解，对象本身仍受 etcd 默认的 1.5MiB 上限约束——两件事
# 不是一回事，只是都在「清单变大」时才撞上。
#
#   1. 真树里的换版脚本 ⇒ 绿（有这一行，且带 --server-side）。
#   2. 夹具里同样的 apply 不带 --server-side ⇒ 红，并点名行号。
#   3. 夹具里根本没有 apply 这一份 ⇒ 红（删掉那一步不能读成「判过了」）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
target="deploy/k3s/cogneva-json-configmap.yaml"

# 判一份脚本：打印违规行；退出码 0 = 绿。
judge() {
  local file="$1" hits=0 bad=0 line
  while IFS= read -r line; do
    case "$line" in
      *kubectl*apply*"$target"*|*apply*"$target"*)
        hits=$((hits + 1))
        case "$line" in
          *--server-side*) ;;
          *) printf 'JSON-CONFIGMAP-CLIENT-SIDE %s\n' "$line" >&2; bad=$((bad + 1)) ;;
        esac
        ;;
    esac
  done < "$file"
  if [ "$hits" -eq 0 ]; then
    printf 'JSON-CONFIGMAP-NOT-APPLIED %s\n' "$file" >&2
    return 1
  fi
  [ "$bad" -eq 0 ]
}

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 1. 真树
if out=$(judge "$root/deploy/k3s/swap-image.sh" 2>&1); then
  ok "真树：换版脚本对这份 ConfigMap 走 server-side apply"
else
  bad "真树：换版脚本仍在 client-side apply 这份 ConfigMap"
  printf '%s\n' "$out" | sed 's/^/        /' >&2
fi

tmp="$(mktemp -d)" || exit 1
trap 'rm -rf "$tmp"' EXIT

# 2. 反面夹具：同一句缺 --server-side ⇒ 必须红
cat > "$tmp/client-side.sh" <<'SH'
kubectl apply -f deploy/k3s/cogneva-json-configmap.yaml
SH
if judge "$tmp/client-side.sh" >/dev/null 2>&1; then
  bad "夹具：不带 --server-side 的 apply 被判成绿"
else
  ok "夹具：不带 --server-side 的 apply 被判成红"
fi

# 3. 夹具里整步消失 ⇒ 必须红，不能读成「没有违规」
cat > "$tmp/no-apply.sh" <<'SH'
kubectl apply -f deploy/k3s/deployment.yaml
SH
if judge "$tmp/no-apply.sh" >/dev/null 2>&1; then
  bad "夹具：整步不见了仍被判成绿"
else
  ok "夹具：整步不见了被判成红"
fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
