#!/usr/bin/env bash
# Deterministic gate for out-of-band credential delivery.
#
# Three silent failures are being judged here:
#   1. a wizard field whose Secret key no manifest ever reads — it reports
#      "saved" and changes nothing;
#   2. a chart that rewrites a wizard-delivered value back to its empty default
#      on every `helm upgrade` — the feature goes fail-closed with no error;
#   3. a credential read from the wrong one of the two Secret objects — the
#      business half is read whole-object by every workload holding it, so a key
#      that belongs in the out-of-band half but sits in the business half is
#      handed to all of them, and nothing says so.
#
# None of these shows up as a stack trace, so a convention ("remember to wire
# it") is not enough: the key is read from the Rust constant that writes it, and
# the preservation rule is asserted for every key in the out-of-band list.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

admin="${repo}/crates/cog-gateway/src/contribution_admin.rs"
[ -f "${admin}" ] || fail "找不到 ${admin}"

# The two Secret objects. Which one a key lives in is decided by who reads it:
# only the security gateway → out-of-band; any business Pod → the business half.
OBJ_INTERNAL="cogneva-secrets"
OBJ_OOB="cogneva-secrets-oob"

# Collects the Secret keys a workload reads through `secretKeyRef` on the given
# object: the `key:` line right below the ref's `name:` line.
#
# The object name is matched to the end of the line, not as a prefix: one object
# name is a prefix of the other, so a prefix match would report a read of the
# out-of-band object as a read of the business one — the exact confusion this
# gate now exists to catch.
wired_keys() {
  awk -v want="$2" '
    $0 ~ ("name:[[:space:]]*" want "[[:space:]]*$") { hit = NR }
    hit && NR <= hit + 2 && /key:/ {
      sub(/.*key:[[:space:]]*/, "");
      gsub(/["'"'"' ]/, "");
      print
    }
  ' "$1"
}

# --- negative control -----------------------------------------------------
# The checker must be able to answer "not wired"; otherwise every assertion
# below would pass against an empty file too. Three samples: wired, unwired,
# and wired-but-to-the-other-object.
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
cat > "${work}/wired.yaml" <<'EOF'
          env:
            - name: A
              valueFrom:
                secretKeyRef:
                  name: cogneva-secrets-oob
                  key: probe-key
                  optional: true
EOF
cp "${work}/wired.yaml" "${work}/unwired.yaml"
sed -i 's/probe-key/other-key/' "${work}/unwired.yaml"
wired_keys "${work}/wired.yaml" "${OBJ_OOB}" | grep -qx "probe-key" \
  || fail "自检失败：命中样本被判为未接线"
if wired_keys "${work}/unwired.yaml" "${OBJ_OOB}" | grep -qx "probe-key"; then
  fail "自检失败：缺失样本被判为已接线"
fi
if wired_keys "${work}/wired.yaml" "${OBJ_INTERNAL}" | grep -qx "probe-key"; then
  fail "自检失败：读到带外对象的样本被判成读内部对象（对象名互为前缀）"
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
    wired_keys "${manifest}" "${OBJ_OOB}" | grep -qx "${key}" \
      || fail "${manifest} 没有从 ${OBJ_OOB} 读 ${key}（向导写的值没人消费）"
  done

  # Rendered manifests are what the apply path actually deploys: a chart that is
  # wired while a rendered profile is not means the value lands in a Secret
  # nobody reads on that profile.
  shopt -s nullglob
  rendered=("${repo}"/deploy/rendered/*/41-deployment-cogneva-security-gateway.yaml)
  [ "${#rendered[@]}" -gt 0 ] || fail "找不到任何预渲染的安全网关清单"
  for manifest in "${rendered[@]}"; do
    wired_keys "${manifest}" "${OBJ_OOB}" | grep -qx "${key}" \
      || fail "${manifest} 没有从 ${OBJ_OOB} 读 ${key}"
  done

  # --- 2) the produce side exists (placeholder + chart declaration) -------
  grep -qE "^ensure_blank_oob[[:space:]]+${key}$" "${repo}/deploy/scripts/init-secrets.sh" \
    || fail "init-secrets.sh 没有为 ${key} 在 ${OBJ_OOB} 建空占位，全新安装时向导无处可写"

  grep -qE "^[[:space:]]*${key}:" "${repo}/deploy/helm/cogneva/templates/secret-oob.yaml" \
    || fail "chart 的 ${OBJ_OOB} 模板没有声明 ${key}"
done

# Every key delivered out of band -- by the wizard, or by an operator editing
# the Secret -- lives only in the out-of-band Secret. The chart owns that
# object, so each of them must be read back from the live Secret when values
# provide nothing. Named here rather than derived: the point of the list is to
# be the second opinion to each producer's claim, and a list derived from those
# claims would agree with them by construction.
out_of_band=(
  llm-upstreams
  github-token
  gitee-token
  github-webhook-secret
  gitee-webhook-token
  gitee-oauth-client-secret
  github-oauth-client-secret
  notification-dingtalk-secret
  notification-feishu-secret
)
[ "${#out_of_band[@]}" -gt 0 ] || fail "带外凭证清单是空的，这一节会静默变成空转"

# --- 2b) the notification signing keys are wired the same way -------------
# These are the same class of key as the OAuth secrets above, with one more
# step in between: the business side never sees them, and the gateway signs on
# its behalf. So the failure modes are the same two plus a third of their own —
# a key no manifest reads, a key the chart blanks on upgrade, and a signing
# face wired to a key that was never given a place to be written.
#
# The env names come from the contract module both sides read rather than a
# hand-written list here: a list would keep passing after a constant moved.
platform_sign="${repo}/crates/cog-core/src/contract/platform_sign.rs"
[ -f "${platform_sign}" ] || fail "找不到 ${platform_sign}"

# The Secret key a workload reads for one env var: the `key:` line inside the
# same secretKeyRef block.
key_for_env() {
  awk -v want="$2" '
    $1 == "-" && $2 == "name:" && $3 == want { hit = NR }
    hit && NR <= hit + 4 && /key:/ {
      sub(/.*key:[[:space:]]*/, "");
      gsub(/["'"'"' ]/, "");
      print
      exit
    }
  ' "$1"
}

# The object that same block reads from — the second half of the answer, and the
# one a key-name-only checker cannot see.
object_for_env() {
  awk -v want="$2" '
    $1 == "-" && $2 == "name:" && $3 == want { hit = NR }
    hit && NR > hit && NR <= hit + 4 && /name:/ {
      sub(/.*name:[[:space:]]*/, "");
      gsub(/["'"'"' ]/, "");
      print
      exit
    }
  ' "$1"
}

# Negative control, same reason as the one above: a checker that answers with
# some key for an env no manifest wires would make every assertion below vacuous.
cat > "${work}/sign-wired.yaml" <<'EOF'
          env:
            - name: COGNEVA_NOTIFICATION_DINGTALK_SECRET
              valueFrom:
                secretKeyRef:
                  name: cogneva-secrets-oob
                  key: probe-sign-key
                  optional: true
EOF
[ "$(key_for_env "${work}/sign-wired.yaml" COGNEVA_NOTIFICATION_DINGTALK_SECRET)" = "probe-sign-key" ] \
  || fail "自检失败：命中样本没读出键名"
[ "$(object_for_env "${work}/sign-wired.yaml" COGNEVA_NOTIFICATION_DINGTALK_SECRET)" = "${OBJ_OOB}" ] \
  || fail "自检失败：命中样本没读出对象名"
if [ -n "$(key_for_env "${work}/sign-wired.yaml" COGNEVA_NOTIFICATION_FEISHU_SECRET)" ]; then
  fail "自检失败：没有这条 env 的文件被判为有接线"
fi

mapfile -t sign_envs < <(
  sed -n 's/.*const [A-Z_]*SECRET_ENV: &str = "\([^"]*\)".*/\1/p' "${platform_sign}"
)
[ "${#sign_envs[@]}" -gt 0 ] \
  || fail "没能从 ${platform_sign} 提取出签名密钥的投递键名，断言会变成空转"

shopt -s nullglob
rendered_gateways=("${repo}"/deploy/rendered/*/41-deployment-cogneva-security-gateway.yaml)
[ "${#rendered_gateways[@]}" -gt 0 ] || fail "找不到任何预渲染的安全网关清单"

for env in "${sign_envs[@]}"; do
  for manifest in \
    "${repo}/deploy/helm/cogneva/templates/security-gateway.yaml" \
    "${repo}/deploy/k3s/gateway-deployment.yaml" \
    "${rendered_gateways[@]}"
  do
    key="$(key_for_env "${manifest}" "${env}")"
    [ -n "${key}" ] \
      || fail "${manifest} 没有为 ${env} 接一个 secretKeyRef（签名面拿不到密钥）"
    object="$(object_for_env "${manifest}" "${env}")"
    [ "${object}" = "${OBJ_OOB}" ] \
      || fail "${manifest} 的 ${env} 从 ${object:-空} 读密钥，应读 ${OBJ_OOB}（内部份是整对象 get，等于铺给每个业务 Pod）"

    grep -qE "^ensure_blank_oob[[:space:]]+${key}$" "${repo}/deploy/scripts/init-secrets.sh" \
      || fail "init-secrets.sh 没有为 ${key} 在 ${OBJ_OOB} 建空占位，全新安装时运维无处可写"
    grep -qE "^[[:space:]]*${key}:" "${repo}/deploy/helm/cogneva/templates/secret-oob.yaml" \
      || fail "chart 的 ${OBJ_OOB} 模板没有声明 ${key}"
    printf '%s\n' "${out_of_band[@]}" | grep -qx "${key}" \
      || fail "${key} 不在带外凭证清单里，升级保留规则没覆盖它"
  done
done

# --- 2c) the borrow face reaches every workload that can sign --------------
# The signing secret lives in the gateway, so what a business workload needs is
# the address of the borrowing face. Both workloads run the same binary and the
# same plugin set, so a workload missing this address does not fail to build or
# fail to start -- it sends its notifications unsigned, and the platform answers
# "signature check failed", which reads like the platform's fault. The address
# is a literal in each manifest (the same shape as the GitHub/Gitee bases), and
# the constant is what it has to agree with.
sign_base="$(sed -n 's/.*const SIGN_BASE_ENV: &str = "\([^"]*\)".*/\1/p' "${platform_sign}")"
[ -n "${sign_base}" ] || fail "没能从 ${platform_sign} 提取出签名借用面的 env 名"
grep -qF "COGNEVA_GITHUB_API_BASE" "${repo}/deploy/helm/cogneva/templates/gateway.yaml" \
  || fail "自检失败：借用面清单样本读不到已知的借用地址"

rendered_apps=("${repo}"/deploy/rendered/*/41-deployment-cogneva.yaml)
rendered_evolution=("${repo}"/deploy/rendered/*/41-deployment-cogneva-evolution.yaml)
[ "${#rendered_apps[@]}" -gt 0 ] && [ "${#rendered_evolution[@]}" -gt 0 ] \
  || fail "找不到预渲染的业务工作负载清单，这一节会退化成只查 chart/k3s"

app_workloads=(
  "${repo}/deploy/helm/cogneva/templates/gateway.yaml"
  "${repo}/deploy/helm/cogneva/templates/evolution.yaml"
  "${repo}/deploy/k3s/deployment.yaml"
  "${repo}/deploy/k3s/evolution-deployment.yaml"
  "${rendered_apps[@]}"
  "${rendered_evolution[@]}"
)
for manifest in "${app_workloads[@]}"; do
  grep -qF "${sign_base}" "${manifest}" \
    || fail "${manifest} 没有 ${sign_base}：这个进程发出的通知会退回未签名"
done

# --- 3) out-of-band credentials survive a chart upgrade -------------------
chart_secret="${repo}/deploy/helm/cogneva/templates/secret-oob.yaml"
for k in "${out_of_band[@]}"; do
  grep -qF "index \$oob.data \"${k}\"" "${chart_secret}" \
    || fail "chart 升级会把 ${k} 覆盖成空值：${OBJ_OOB} 模板没有保留既有值"
done

# Every key this gate is about must be in that list, or the list drifted away
# from the keys that actually exist.
for key in "${oauth_keys[@]}"; do
  printf '%s\n' "${out_of_band[@]}" | grep -qx "${key}" \
    || fail "${key} 不在带外凭证清单里，升级保留规则没覆盖它"
done

# --- 4) no out-of-band key stayed behind in the business object -----------
# The two objects are not interchangeable: the business one is read whole-object
# by every workload that holds it, so a left-behind copy hands the credential to
# all of them. It would also be a second truth — the gateway reads the key from
# the out-of-band object, so both copies could be live at once and disagree.
chart_internal="${repo}/deploy/helm/cogneva/templates/secret.yaml"
for k in "${out_of_band[@]}"; do
  if grep -qE "^[[:space:]]*${k}:" "${chart_internal}"; then
    fail "${OBJ_INTERNAL} 模板仍在声明 ${k}：这个键只该存在于 ${OBJ_OOB}"
  fi
  if grep -qE "^ensure_blank[[:space:]]+${k}$" "${repo}/deploy/scripts/init-secrets.sh"; then
    fail "init-secrets.sh 仍在为 ${k} 写内部份占位，应改为 ensure_blank_oob"
  fi
done
# Same question for the readers. The token blocks above are already checked per
# env; this is the whole-object form: a manifest mounting the business object at
# a path the gateway would read the out-of-band keys from.
for manifest in \
  "${repo}/deploy/helm/cogneva/templates/security-gateway.yaml" \
  "${repo}/deploy/k3s/gateway-deployment.yaml"
do
  for k in "${out_of_band[@]}"; do
    if wired_keys "${manifest}" "${OBJ_INTERNAL}" | grep -qx "${k}"; then
      fail "${manifest} 从 ${OBJ_INTERNAL} 读带外键 ${k}，应读 ${OBJ_OOB}"
    fi
  done
done

# --- 5) the two key tables agree -----------------------------------------
# The same split is written down twice: the Rust constant routes writes at
# runtime, and init-secrets.sh's list is what migrates an existing install. They
# are the only two places that can move a key between the objects, and a key
# missing from the shell one leaves a copy behind in the business half — where
# every workload holding that object can read it, silently, forever.
layout="${repo}/crates/cog-gateway/src/secret_layout.rs"
[ -f "${layout}" ] || fail "找不到 ${layout}"

mapfile -t rust_oob < <(
  awk '/^pub\(crate\) const OOB_SECRET_KEYS/{in_list=1} in_list && /"/{
         while (match($0, /"[^"]+"/)) { print substr($0, RSTART+1, RLENGTH-2); $0 = substr($0, RSTART+RLENGTH) }
       }
       in_list && /^];/{exit}' "${layout}"
)
[ "${#rust_oob[@]}" -gt 0 ] || fail "没能从 ${layout} 读出带外键表，这一节会变成空转"

mapfile -t shell_oob < <(
  awk '/^OOB_KEYS=\(/{in_list=1; next} in_list && /^\)/{exit} in_list && NF{print $1}' \
    "${repo}/deploy/scripts/init-secrets.sh"
)
[ "${#shell_oob[@]}" -gt 0 ] || fail "没能从 init-secrets.sh 读出带外键表，这一节会变成空转"

for k in "${shell_oob[@]}"; do
  printf '%s\n' "${rust_oob[@]}" | grep -qx "${k}" \
    || fail "init-secrets.sh 的带外表里有 ${k}，Rust 的键表没有：写入会落在内部份，而安装期会把它当带外键搬走"
done
for k in "${rust_oob[@]}"; do
  printf '%s\n' "${shell_oob[@]}" | grep -qx "${k}" \
    || fail "Rust 的键表里有 ${k}，init-secrets.sh 的带外表没有：老安装里会留下一份副本在被业务 Pod 读得到的地方"
done

echo "PASS: ${oauth_keys[*]} 与 ${sign_envs[*]} 都有消费面（chart/k3s/预渲染），${sign_base} 到得了每个能签名的进程，带外凭证在升级时全部保留，且没有一个留在业务份里；两处键表一致（${#rust_oob[@]} 个带外键）"
