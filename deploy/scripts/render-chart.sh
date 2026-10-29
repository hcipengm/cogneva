#!/usr/bin/env bash
# 把 chart 渲染成「交付时会交付的那一份」清单，打到 stdout。
#
# 为什么入参要有一个唯一的构造点：入参集合本身就是判据的一部分。chart 每多一个
# 必填的 values 文件，只有一处构造点时所有读者才会同时看见；各自拼过一遍的读者
# 会静默渲染失败，而失败还可能被读成「没有对象」。
# 2026-10-29 的现场：额度改成从投影文件来（`values.resource-governance.<profile>.yaml`，
# 模板里是 `required`）之后，宿主机上量 helm 归属与升级镜像的两格读数立刻
# `verdict=unreadable reason=render-failed`——它们自己拼的入参里没有那一个 `-f`。
# 两格各有离线测试，但测试注入的是固定清单，所以没有一条门会红。同一天
# `check-deploy-parity.sh` 也自己拼了一遍，只是它恰好带上了那个文件，才没跟着瞎。
#
# 用法：
#   bash deploy/scripts/render-chart.sh <profile>            # 与 apply 交付同一套入参
#   bash deploy/scripts/render-chart.sh <profile> -- <helm 参数…>   # 原样透传给 helm
#
# 两个 env 开关（比位置参数更不容易和透传参数打架）：
#   COGNEVA_RENDER_RAW=1             先渲染一遍好让额度投影有输入：治理整段关掉
#                                    （额度与 LimitRange 上限还没算出来，模板里的
#                                    required 会直接报错；投影要读的工作负载声明一分不少）
#   COGNEVA_RENDER_SECRETS_CREATE=1  helm 交付形态：两个 Secret 由 chart 的
#                                    lookup+randAlphaNum 生成，必须出现在清单里，
#                                    否则缺标名单会漏掉它们
#
# 退出码：0 渲染成功；2 用法错或 profile 不存在；helm 自己的退出码原样透出。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CHART_DIR="${COGNEVA_CHART:-$REPO_ROOT/deploy/helm/cogneva}"
PROFILES_DIR="$CHART_DIR/profiles"
HELM="${COGNEVA_HELM:-helm}"
NS="${COGNEVA_NS:-cogneva}"

profile="${1:-}"
[ -n "$profile" ] || {
    echo "usage: render-chart.sh <profile> [-- <helm args…>]" >&2
    echo "       env: COGNEVA_RENDER_RAW=1  COGNEVA_RENDER_SECRETS_CREATE=1" >&2
    exit 2
}
shift
# `--` 只是分隔用的，别把它当位置参数交给 helm（helm 的 `template` 只收 NAME CHART，
# 多一个 `--` 就报 "expected at most two arguments"）。
[ "${1:-}" = "--" ] && shift

values="$PROFILES_DIR/$profile.yaml"
if [ ! -f "$values" ]; then
    known="$(ls "$PROFILES_DIR" 2>/dev/null | sed -n 's/\.yaml$//p' | tr '\n' ' ')"
    echo "render-chart: 没有这个 profile：$values（现有：${known:-无}）" >&2
    exit 2
fi

overrides=()
if [ "${COGNEVA_RENDER_RAW:-0}" = 1 ]; then
    overrides+=(--set resourceGovernance.enabled=false)
else
    generated="$CHART_DIR/values.resource-governance.$profile.yaml"
    [ -f "$generated" ] && overrides+=(-f "$generated")
fi
if [ "${COGNEVA_RENDER_SECRETS_CREATE:-0}" = 1 ]; then
    overrides+=(--set secrets.create=true)
fi

exec "$HELM" template cogneva "$CHART_DIR" \
    -f "$values" \
    ${overrides[@]+"${overrides[@]}"} \
    --namespace "$NS" \
    "$@"
