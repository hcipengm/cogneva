#!/usr/bin/env bash
# project-resource-governance.py 的分档：`--check` 要比过至少一份投影产物才算判过。
#
# 这条门原来只有两种结局：漂移（退 1）和 OK。`--check` 单独跑时要比对的表是空的，
# 循环一次也不进，drift 自然是空的，于是它印的正是「两份投影都与渲染集一致」——
# 零份文件与全部一致在输出上同形。第三种结局（退 3，没判成）就是这次补的那一格。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${here}/../project-resource-governance.py"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "gate not found: ${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

rendered="${repo}/deploy/rendered/k3s-single"
values="${repo}/deploy/helm/cogneva/values.yaml"
profile="${repo}/deploy/helm/cogneva/profiles/k3s-single.yaml"
for f in "${rendered}" "${values}" "${profile}"; do
    [ -e "${f}" ] || fail "缺输入：${f}"
done

run() { # <期望退出码> <输出里必须出现的串> <额外参数...>
    local want="$1" needle="$2" out rc=0
    shift 2
    out="$(python3 "${gate}" --rendered "${rendered}" --values "${values}" \
        --profile "${profile}" "$@" 2>&1)" || rc=$?
    if [ "${rc}" -ne "${want}" ]; then
        printf '%s\n' "${out}" >&2
        fail "exit ${rc}，期望 ${want}（参数：$*）"
    fi
    if [ -n "${needle}" ]; then
        case "${out}" in
            *"${needle}"*) ;;
            *) printf '%s\n' "${out}" >&2; fail "输出里没有「${needle}」" ;;
        esac
    fi
}

# --- 零份比对：什么都不判的时候不许印「一致」。
run 3 '没有可比对的投影产物' --check

# --- 一份比对：写出来再核对，必须绿，且绿的那句说清比过了。
run 0 '' --write-values "${work}/values.yaml" --quiet
run 0 '额度与渲染集一致' --write-values "${work}/values.yaml" --check

# --- 漂移仍然要红：产物被人手改过。
printf '# hand edit\n' >>"${work}/values.yaml"
run 1 '与投影不一致' --write-values "${work}/values.yaml" --check

# --- 读数面：写出来的文件必须带着额度，而不是零额度的一份。
case "$(cat "${work}/values.yaml")" in
    *limitsCpu*) ;;
    *) fail "投影产物里没有 limitsCpu —— 空投影会写出零额度" ;;
esac

echo "project-resource-governance.test: all checks passed"
