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

# --- 余量必须把稳态占比压在上游告警阈值之下。
# 这个额度的稳态 used 就是 PVC 声明和本身（准入按声明记，声明多少就占多少），所以
# used/hard 只有余量一个变量。kube-prometheus-stack 自带的 KubeQuotaAlmostFull 在
# 过 0.9 时报警：余量 1.1 落在 0.909，那盏灯从装机那一刻起就是红的（2026-10-30 现网
# 实测 requests.storage 391/430＝90.9%）。判据只有一条——额度对声明和的比值必须
# 严格大于 1/0.9；两个数都从这一段自己印的读数里取，不在这里重算渲染集。
projection="$(python3 "${gate}" --rendered "${rendered}" --values "${values}" \
    --profile "${profile}" 2>&1)"
hard="$(sed -n 's/.*storage \([0-9]\+\)Gi（声明和 .*/\1/p' <<<"${projection}")"
declared="$(sed -n 's/.*声明和 \([0-9.]\+\)GiB.*/\1/p' <<<"${projection}")"
if [ -z "${hard}" ] || [ -z "${declared}" ]; then
    printf '%s\n' "${projection}" >&2
    fail "读数里没有「storage X Gi（声明和 Y GiB）」——这个余量判不了"
fi
if ! awk -v h="${hard}" -v d="${declared}" \
    'BEGIN { exit !(h > d && h / d > 1 / 0.9) }'; then
    fail "存储余量不足：${hard}Gi 对声明和 ${declared}GiB，稳态占比高于 0.9"
fi

# --- 读数面：写出来的文件必须带着额度，而不是零额度的一份。
case "$(cat "${work}/values.yaml")" in
    *limitsCpu*) ;;
    *) fail "投影产物里没有 limitsCpu —— 空投影会写出零额度" ;;
esac

echo "project-resource-governance.test: all checks passed"
