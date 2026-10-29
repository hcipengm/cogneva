#!/usr/bin/env bash
# render-chart.sh 的定向测试：它是「交付时会是哪一份清单」这个问题的唯一构造点，
# 所以它一要渲染得出来，二是别人不许再自己拼一遍入参。
#
# 为什么要有这份测试：2026-10-29 实测——额度的投影文件成为 chart 的必填输入
# （模板里是 `required`）之后，宿主机上量 helm 归属与升级镜像的两格读数立刻
# `verdict=unreadable reason=render-failed`。那两格各有离线测试，但测试注入的是
# 固定清单，所以没有一条门会红；`check-deploy-parity.sh` 也自己拼了一遍，只是
# 恰好带上了那个文件才没跟着瞎。一篇门「自己拼入参」的代价不是一次报错，
# 而是一格读数从此长期读成「判不了」——那和「已落」在面板上长得不一样，却同样
# 不会有人来修。第二组用例守的正是这件事：树里不许有第二个 `helm template`。
#
# 用例：
#   1. 每个 profile 都渲染得出来，且带额度的 profile 渲染里 ResourceQuota 有数；
#   2. raw 一遍（投影的输入）里没有 ResourceQuota/LimitRange，但工作负载一个不少；
#   3. helm 交付形态多出两个 Secret，默认形态一个都没有（profile 里 create=false）；
#   4. 透传参数到得了 helm：`--show-only` 只出一份 gateway，`--` 本身不被当参数；
#   5. 不存在的 profile -> 退 2，stderr 点名缺的那个路径；
#   6. 树里没有第二个 `helm template` 构造点（render-deploy.sh 与 parity 也一样）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
render="${repo}/deploy/scripts/render-chart.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

profiles=()
for f in "${repo}"/deploy/helm/cogneva/profiles/*.yaml; do
  [ -e "$f" ] || continue
  profiles+=("$(basename "$f" .yaml)")
done
[ "${#profiles[@]}" -gt 0 ] || fail "一个 profile 都没有：这条用例没有题目（空跑不算过）"

# 1. 每个 profile 都能渲染；带治理的那几个里 ResourceQuota 必须带着数出来。
#    `required` 缺输入时 helm 退非 0，这里就会当场红——2026-10-29 的病正是它。
for p in "${profiles[@]}"; do
  out="${work}/${p}.yaml"
  bash "$render" "$p" > "$out" || fail "profile ${p} 渲染失败（chart 多了必填输入而这里没跟上？）"
  kinds=$(grep -c '^kind:' "$out" || true)
  [ "${kinds:-0}" -gt 0 ] || fail "profile ${p} 渲染出 0 个对象"
  if grep -q '^kind: ResourceQuota' "$out"; then
    grep -qE '^  hard:' "$out" || fail "profile ${p} 的 ResourceQuota 没有 hard 段（额度是空的）"
  fi
done

# 2. raw 是投影的输入：治理整段关掉，但工作负载声明一分不少。
COGNEVA_RENDER_RAW=1 bash "$render" k3s-single > "${work}/raw.yaml"
if grep -qE '^kind: (ResourceQuota|LimitRange)' "${work}/raw.yaml"; then
  fail "raw 一遍里仍有 ResourceQuota/LimitRange：投影读的是「还没算额度」的那一遍，关掉才是它的输入"
fi
raw_workloads=$(grep -cE '^kind: (Deployment|StatefulSet|DaemonSet|Job|CronJob)' "${work}/raw.yaml" || true)
gov_workloads=$(grep -cE '^kind: (Deployment|StatefulSet|DaemonSet|Job|CronJob)' "${work}/k3s-single.yaml" || true)
[ "${raw_workloads:-0}" -gt 0 ] || fail "raw 一遍里一个工作负载都没有"
[ "${raw_workloads}" = "${gov_workloads}" ] \
  || fail "raw 与 governed 两遍的工作负载数不同（${raw_workloads} vs ${gov_workloads}）：投影会按少了一份清单去算"

# 3. 两种交付形态的 Secret 差：helm 那一遍由 chart 的 lookup+randAlphaNum 生成。
grep -q '^kind: Secret' "${work}/k3s-single.yaml" \
  && fail "默认形态渲染出了 Secret：profile 里 create=false，内部密钥由安装时生成"
COGNEVA_RENDER_SECRETS_CREATE=1 bash "$render" k3s-single > "${work}/helm-form.yaml"
secrets=$(grep -c '^kind: Secret' "${work}/helm-form.yaml" || true)
[ "${secrets:-0}" -eq 2 ] || fail "helm 交付形态的 Secret 数 = ${secrets:-0}，应为 2（两件都在，缺标名单才不会漏）"

# 4. 透传：`--show-only` 到得了 helm，`--` 本身不被当成位置参数。
bash "$render" k3s-single -- --show-only templates/gateway.yaml > "${work}/gateway.yaml" \
  || fail "透传参数没到 helm（`--` 被当成位置参数了？）"
gw_kinds=$(grep -c '^kind:' "${work}/gateway.yaml" || true)
[ "${gw_kinds:-0}" -eq 3 ] || fail "--show-only templates/gateway.yaml 出了 ${gw_kinds:-0} 个对象，应为 3（PVC/Service/Deployment）"
grep -q '^kind: Deployment' "${work}/gateway.yaml" || fail "gateway 渲染里没有 Deployment"

# 5. 不存在的 profile：退 2 且点名缺的那个路径，不静默渲染一份空清单。
rc=0
bash "$render" no-such-profile > "${work}/bad.out" 2> "${work}/bad.err" || rc=$?
[ "$rc" -eq 2 ] || fail "不存在的 profile 退出码 = ${rc}，应为 2"
grep -q 'no-such-profile.yaml' "${work}/bad.err" || fail "stderr 没有点名缺的那个 profile 路径"
[ -s "${work}/bad.out" ] && fail "不存在的 profile 往 stdout 印了内容（读者会当成一份清单）"

# 6. 唯一构造点：树里除了这个脚本自己，不许有谁再拼 `helm template` 的入参。
#    宿主机上的那几格读数在另一个仓，它们走的是同一个脚本；这里守的是本仓这一侧。
offenders=$(grep -rln --include='*.sh' --include='*.py' -E 'helm[[:space:]]+template' \
  "${repo}/deploy" "${repo}/.github" 2>/dev/null \
  | grep -v "deploy/scripts/render-chart.sh" \
  | grep -v "/tests/" || true)
[ -z "$offenders" ] || fail "这些文件自己拼了 helm template 的入参（应改走 render-chart.sh）：${offenders}"

echo "render-chart: ${#profiles[@]} 个 profile、两种交付形态、一个构造点 — 全通过"
