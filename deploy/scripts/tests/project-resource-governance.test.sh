#!/usr/bin/env bash
# project-resource-governance.py 的分档：`--check` 要比过至少一份投影产物才算判过，
# 以及拓扑（节点数）从哪儿来——live cluster / 生成文件首部 / 显式参数，缺了就退 2，
# 不猜一个 1。
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
# CI 没有集群：`--check` 按产物首部记下的拓扑重算。这份文件就是那份产物。
recorded="${repo}/deploy/helm/cogneva/values.resource-governance.k3s-single.yaml"
for f in "${rendered}" "${values}" "${profile}" "${recorded}"; do
    [ -e "${f}" ] || fail "缺输入：${f}"
done

run() { # <期望退出码> <输出里必须出现的串> <额外参数...>
    local want="$1" needle="$2" out rc=0
    shift 2
    out="$(python3 "${gate}" --rendered "${rendered}" --values "${values}" \
        --profile "${profile}" --nodes-from "${recorded}" "$@" 2>&1)" || rc=$?
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
    --profile "${profile}" --nodes-from "${recorded}" 2>&1)"
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

# --- 拓扑：节点数从 live cluster 读、记进产物首部，--check 再按它重算。
# 三条来源按 --nodes > --nodes-from > COGNEVA_NODES > live 的顺序解析；哪一条都没有
# 时退 2，不猜一个 1——猜 1 与真只有 1 个节点在产物上完全同形。
cat >"${work}/kubectl-three" <<'SH'
#!/usr/bin/env bash
# 假 kubectl：只回答 `get nodes -o json` 的那一问。
printf '{"items":[{},{},{}]}\n'
SH
chmod +x "${work}/kubectl-three"

# live cluster 说 3 个节点 → 产物首部记 3，并且这一跑的数值与显式 --nodes 3 逐字节相同
# （说明它真的把 3 用进了算式，而不是只记在首部）。
COGNEVA_KUBECTL="${work}/kubectl-three" python3 "${gate}" \
    --rendered "${rendered}" --values "${values}" --profile "${profile}" \
    --write-values "${work}/live-nodes.yaml" --quiet
grep -qx '# topology-nodes: 3' "${work}/live-nodes.yaml" \
    || fail "投影产物没有记下 live cluster 的拓扑（首部没有 '# topology-nodes: 3'）"
python3 "${gate}" --rendered "${rendered}" --values "${values}" --profile "${profile}" \
    --nodes 3 --write-values "${work}/explicit-3.yaml" --quiet
cmp -s "${work}/live-nodes.yaml" "${work}/explicit-3.yaml" \
    || fail "从 live cluster 读到的 3 与显式 --nodes 3 投影出的不是同一份：节点数没有进算式"

# 拓扑变了，额度必须跟着变（DaemonSet 逐节点铺开）。
python3 "${gate}" --rendered "${rendered}" --values "${values}" --profile "${profile}" \
    --nodes 1 --write-values "${work}/explicit-1.yaml" --quiet
if cmp -s "${work}/explicit-1.yaml" "${work}/explicit-3.yaml"; then
    fail "3 节点的投影与 1 节点的逐字节相同：节点数没有进额度（DaemonSet 的副本数）"
fi

# --check 按产物首部重算：拿 3 节点那份核对自己必须绿；拿它按 1 节点重算必须红。
python3 "${gate}" --rendered "${rendered}" --values "${values}" --profile "${profile}" \
    --nodes-from "${work}/explicit-3.yaml" --write-values "${work}/explicit-3.yaml" --check --quiet \
    || fail "--check 没有按生成文件首部的拓扑重算（3 节点核对 3 节点失败）"
if python3 "${gate}" --rendered "${rendered}" --values "${values}" --profile "${profile}" \
    --nodes 1 --write-values "${work}/explicit-3.yaml" --check --quiet 2>/dev/null; then
    fail "按 1 节点重算与 3 节点的产物一致：核对没有读拓扑"
fi

# 首部没有记录的文件不能拿来核对：那是在按一个不知道的拓扑重算。
printf 'resourceGovernance:\n  quota:\n    limitsCpu: "1"\n' >"${work}/no-topology.yaml"
out=$(COGNEVA_KUBECTL=/bin/false python3 "${gate}" \
    --rendered "${rendered}" --values "${values}" --profile "${profile}" \
    --nodes-from "${work}/no-topology.yaml" 2>&1) && rc=0 || rc=$?
[ "${rc}" -eq 2 ] || fail "首部没有拓扑时期望退 2，实得 ${rc}（${out}）"
case "${out}" in
    *topology-nodes*) ;;
    *) fail "首部没有拓扑时没说缺的是什么：${out}" ;;
esac

# 一条来源都没有（无集群、无 --nodes、无 --nodes-from、无 COGNEVA_NODES）→ 退 2，不猜 1。
out=$(COGNEVA_KUBECTL=/bin/false python3 "${gate}" \
    --rendered "${rendered}" --values "${values}" --profile "${profile}" 2>&1) && rc=0 || rc=$?
[ "${rc}" -eq 2 ] || fail "拓扑一问三不知时期望退 2，实得 ${rc}（${out}）"
case "${out}" in
    *'不猜一个 1'*) ;;
    *) fail "拓扑问不到时没有说清「不猜 1」：${out}" ;;
esac

echo "project-resource-governance.test: all checks passed"
