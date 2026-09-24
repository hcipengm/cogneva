#!/usr/bin/env bash
# Deterministic gate for out-of-band credential delivery.
#
# Two silent failures are being judged here:
#   1. a wizard field whose Secret key no manifest ever reads — it reports
#      "saved" and changes nothing;
#   2. a chart that rewrites a wizard-delivered value back to its empty default
#      on every `helm upgrade` — the feature goes fail-closed with no error.
#
# Neither shows up as a stack trace, so a convention ("remember to wire it")
# is not enough: the key is read from the Rust constant that writes it, and the
# preservation rule is asserted for every key in the out-of-band list.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

admin="${repo}/crates/cog-gateway/src/contribution_admin.rs"
[ -f "${admin}" ] || fail "找不到 ${admin}"

# Collects the Secret keys that a workload reads through `secretKeyRef` on
# `cogneva-secrets`: the `key:` line right below the ref's `name:` line.
wired_keys() {
  awk '
    /name: cogneva-secrets/ { hit = NR }
    hit && NR <= hit + 2 && /key:/ {
      sub(/.*key:[[:space:]]*/, "");
      gsub(/["'"'"' ]/, "");
      print
    }
  ' "$1"
}

# --- negative control -----------------------------------------------------
# The checker must be able to answer "not wired"; otherwise every assertion
# below would pass against an empty file too.
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
cat > "${work}/wired.yaml" <<'EOF'
          env:
            - name: A
              valueFrom:
                secretKeyRef:
                  name: cogneva-secrets
                  key: probe-key
                  optional: true
EOF
cp "${work}/wired.yaml" "${work}/unwired.yaml"
sed -i 's/probe-key/other-key/' "${work}/unwired.yaml"
wired_keys "${work}/wired.yaml" | grep -qx "probe-key" \
  || fail "自检失败：命中样本被判为未接线"
if wired_keys "${work}/unwired.yaml" | grep -qx "probe-key"; then
  fail "自检失败：缺失样本被判为已接线"
fi

# --- 1) the key the wizard writes is the key the workloads read -----------
# Read from the constants rather than repeating the names here: a hand-written
# list would keep passing after the constant moved.
mapfile -t oauth_keys < <(
  sed -n 's/.*const SECRET_[A-Z_]*OAUTH_CLIENT_SECRET: &str = "\([^"]*\)".*/\1/p' "${admin}"
)
[ "${#oauth_keys[@]}" -gt 0 ] \
  || fail "没能从 ${admin} 提取出 OAuth 密钥键名，断言会变成空转"

for key in "${oauth_keys[@]}"; do
  for manifest in \
    "${repo}/deploy/helm/cogneva/templates/security-gateway.yaml" \
    "${repo}/deploy/k3s/gateway-deployment.yaml"
  do
    [ -f "${manifest}" ] || fail "找不到 ${manifest}"
    wired_keys "${manifest}" | grep -qx "${key}" \
      || fail "${manifest} 没有从 cogneva-secrets 读 ${key}（向导写的值没人消费）"
  done

  # Rendered manifests are what the apply path actually deploys: a chart that is
  # wired while a rendered profile is not means the value lands in a Secret
  # nobody reads on that profile.
  shopt -s nullglob
  rendered=("${repo}"/deploy/rendered/*/41-deployment-cogneva-security-gateway.yaml)
  [ "${#rendered[@]}" -gt 0 ] || fail "找不到任何预渲染的安全网关清单"
  for manifest in "${rendered[@]}"; do
    wired_keys "${manifest}" | grep -qx "${key}" \
      || fail "${manifest} 没有读 ${key}"
  done

  # --- 2) the produce side exists (placeholder + chart declaration) -------
  grep -qE "^ensure_blank[[:space:]]+${key}$" "${repo}/deploy/scripts/init-secrets.sh" \
    || fail "init-secrets.sh 没有为 ${key} 建空占位，全新安装时向导无处可写"

  grep -qE "^[[:space:]]*${key}:" "${repo}/deploy/helm/cogneva/templates/secret.yaml" \
    || fail "chart 的 Secret 模板没有声明 ${key}"
done

# --- 3) out-of-band credentials survive a chart upgrade -------------------
# Every key the wizard (or the operator, out of band) delivers lives only in
# the Secret. The chart owns that object, so each of them must be read back
# from the live Secret when values provide nothing.
out_of_band=(
  llm-api-key
  llm-upstreams
  github-token
  gitee-token
  github-webhook-secret
  gitee-webhook-token
  gitee-oauth-client-secret
  github-oauth-client-secret
)
[ "${#out_of_band[@]}" -gt 0 ] || fail "带外凭证清单是空的，这一节会静默变成空转"

chart_secret="${repo}/deploy/helm/cogneva/templates/secret.yaml"
for k in "${out_of_band[@]}"; do
  grep -qF "index \$existing.data \"${k}\"" "${chart_secret}" \
    || fail "chart 升级会把 ${k} 覆盖成空值：secret.yaml 没有保留既有值"
done

# Every key this gate is about must be in that list, or the list drifted away
# from the keys that actually exist.
for key in "${oauth_keys[@]}"; do
  printf '%s\n' "${out_of_band[@]}" | grep -qx "${key}" \
    || fail "${key} 不在带外凭证清单里，升级保留规则没覆盖它"
done

echo "PASS: ${oauth_keys[*]} 都有消费面（chart/k3s/预渲染），带外凭证在升级时全部保留"
