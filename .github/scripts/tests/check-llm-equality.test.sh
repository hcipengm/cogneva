#!/usr/bin/env bash
# check-llm-equality.sh 的判据：业务侧的清单/配置里不许出现上游身份面。
#
# 这条门最容易坏的两种坏法，夹具要各钉一边：
#   一是**过度**——把协议方言词（`openai`/`anthropic`）当厂商名拦下。业务侧必须声明
#      自己说哪种方言，把它拦了这条门第一天就被删掉，而它该守的东西一件没守。
#      所以夹具里必须有一份**带全这些合法形状**的基线，且它必须绿：
#      网关的 `/v1` URL、`_API_KEY` 之外的方言配置、散文里的英文常用词 `together`。
#   二是**不足**——四条判据里有一条瞎了。所以四条各配一个只撞它一条的夹具，红。
#
# 光有「红的真红、绿的真绿」还不够：那也有可能是一条判据在替另一条背锅。所以每个
# 夹具都只撞一条，然后再**逐条把那条判据拆掉**（改一个 token），看红的是不是就只有
# 它对应的那一个、别的仍红——这一步才证明四条判据是四条，不是一条加三个摆设。
#
# 只扫产物（`deploy/rendered`、`deploy/k3s`、`deploy/helm/cogneva`、
# `cogneva.example.json`），夹具就照这四个路径搭。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${here}/../check-llm-equality.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "gate not found: ${gate}"

# 夹具是 throwaway 仓，但会继承调用者的 `GIT_*`（钩子会设 `GIT_INDEX_FILE`），
# 清掉，别把夹具写进调用者的索引。
unset GIT_INDEX_FILE GIT_DIR GIT_WORK_TREE GIT_OBJECT_DIRECTORY \
      GIT_ALTERNATE_OBJECT_DIRECTORIES

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture="${work}/fixture"
git -C "${work}" init -q fixture
mkdir -p "${fixture}/deploy/rendered" "${fixture}/deploy/k3s" \
         "${fixture}/deploy/helm/cogneva"

# 基线：合法形状全上，必须绿。这一段是这条门**今天不会误报**的证据。
cat >"${fixture}/deploy/rendered/llm.yaml" <<'YAML'
# the shared pool is where together-ness comes from, not a vendor
llm:
  endpoint: http://cogneva-security-gateway:8081/v1
  api_style: anthropic
  params:
    COGNEVA_ANTHROPIC_DEFAULT_MAX_TOKENS: "4096"
YAML
git -C "${fixture}" add deploy/rendered/llm.yaml

# 跑门：<期望退出码> <期望输出里必须出现的串> [args...]
run_gate() {
  local want="$1" needle="$2" out rc=0
  shift 2
  out="$(bash "$@" 2>&1)" || rc=$?
  if [ "${rc}" -ne "${want}" ]; then
    printf '%s\n' "${out}" >&2
    fail "exit ${rc}, wanted ${want}"
  fi
  if [ -n "${needle}" ]; then
    case "${out}" in
      *"${needle}"*) ;;
      *) printf '%s\n' "${out}" >&2; fail "输出里没有「${needle}」" ;;
    esac
  fi
}

# 加一个只撞某一条判据的文件，跑门，然后撤掉。name 用来对上后面那个文件名。
# 夹具仓还没有 HEAD，普通的 `git rm` 会因为「索引里有暂存改动」拒绝，
# 所以两件事分开做：从索引摘掉 + 删文件。
drop() {
  git -C "${fixture}" rm -qf --cached -- "$1"
  rm -f "${fixture}/$1"
}

case_red() {  # <文件相对路径> <内容> <判据名>
  local rel="$1" body="$2" needle="$3"
  printf '%s\n' "${body}" >"${fixture}/${rel}"
  git -C "${fixture}" add "${rel}"
  run_gate 1 "${needle}" "${gate}" "${fixture}"
  drop "${rel}"
}

# --- 基线绿 ---
run_gate 0 '没有厂商身份面' "${gate}" "${fixture}"

# --- 四条判据各配一个只撞它一条的夹具 ---
# 1：凭证 env 名。判据是**结构**（`<X>_API_KEY`），所以夹具刻意用一个不在任何厂商
#    名单里的前缀 —— 它绿了才说明这条判据不是靠名单在办事。
case_red deploy/rendered/key.yaml \
  '          - name: SOMEVENDOR_API_KEY' '厂商凭证 env'

# 2：非网关的 /v1 端点。用一个**不在厂商域名闭集**的域名，免得撞上第 3 条。
case_red deploy/rendered/endpoint.yaml \
  '  base_url: https://api.example-vendor.test/v1' '非网关的 /v1 端点'

# 3：厂商域名。这里刻意不带 `/v1`，也不含任何厂商身份词，只撞这一条。
case_red deploy/rendered/docs.yaml \
  '  docs: https://generativelanguage.googleapis.com/docs' '厂商域名'

# 4：厂商身份词。一个裸型号名，不含域名、不含 `_API_KEY`。
case_red deploy/k3s/model.yaml '  model: qwen-max' '厂商身份词'

# --- 逐条拆掉，看翻绿的是不是只有它对应的那一个 ---
# 拆法都是改一个 token，不动结构，所以拆完的门仍然是一部门。
ablate() { sed "$1" "${gate}" >"$2"; }

# 1：把结构判据改成永不匹配 ⇒ 凭证夹具翻绿，其余仍红。
ablate 's/\[A-Z\]\[A-Z0-9_\]\*_API_KEY/zzz_neutralized_zzz/' "${work}/abl1.sh"
printf '%s\n' '          - name: SOMEVENDOR_API_KEY' >"${fixture}/deploy/rendered/key.yaml"
git -C "${fixture}" add deploy/rendered/key.yaml
printf '%s\n' '  model: qwen-max' >"${fixture}/deploy/k3s/model.yaml"
git -C "${fixture}" add deploy/k3s/model.yaml
run_gate 1 '厂商身份词' "${work}/abl1.sh" "${fixture}"   # 别的判据还在
out="$(bash "${work}/abl1.sh" "${fixture}" 2>&1 || true)"
case "${out}" in
  *'厂商凭证 env'*) fail "拆掉结构判据后凭证判据仍有命中，说明它没被真正拆掉" ;;
esac
drop deploy/rendered/key.yaml
drop deploy/k3s/model.yaml

# 2：把允许的宿主改成永不匹配 ⇒ **基线那份合法的网关 URL 变红**。
#    这一条证明了白名单不是装饰：它是今天这份配置绿的原因。
ablate "s/^ALLOWED_ENDPOINT_HOST=.*/ALLOWED_ENDPOINT_HOST='zzz-never-matches-zzz'/" \
  "${work}/abl2.sh"
run_gate 1 '非网关的 /v1 端点' "${work}/abl2.sh" "${fixture}"

# 3：把域名闭集清空 ⇒ 域名夹具翻绿（它没有厂商身份词可撞）。
ablate "s/^VENDOR_DOMAINS=.*/VENDOR_DOMAINS='zzz_never_matches_zzz'/" "${work}/abl3.sh"
printf '%s\n' '  docs: https://generativelanguage.googleapis.com/docs' \
  >"${fixture}/deploy/rendered/docs.yaml"
git -C "${fixture}" add deploy/rendered/docs.yaml
run_gate 0 '没有厂商身份面' "${work}/abl3.sh" "${fixture}"
drop deploy/rendered/docs.yaml

# 4：把身份词闭集清空 ⇒ 型号夹具翻绿（它没有域名可撞）。
ablate "s/^VENDOR_TOKENS=.*/VENDOR_TOKENS='zzz_never_matches_zzz'/" "${work}/abl4.sh"
printf '%s\n' '  model: qwen-max' >"${fixture}/deploy/k3s/model.yaml"
git -C "${fixture}" add deploy/k3s/model.yaml
run_gate 0 '没有厂商身份面' "${work}/abl4.sh" "${fixture}"
drop deploy/k3s/model.yaml

# --- 真树 —— 这棵仓库今天必须绿 ---
# 夹具全绿而真树红，说明这条门在测自己；两边都绿，才说明它今天在守东西。
run_gate 0 '没有厂商身份面' "${gate}" "${repo}"

echo "check-llm-equality.test: all checks passed"
