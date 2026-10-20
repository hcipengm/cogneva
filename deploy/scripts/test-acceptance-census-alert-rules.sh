#!/usr/bin/env bash
# 验收普查那三条告警的规则单测跑者：让 Prometheus 自己的规则引擎跑一遍夹具，断言告警
# 真的从「空」走到「firing」。
#
# 为什么不是「在表达式里找 `stale` 这个词」那种检查：`for: 15y`、`[0m]`、把 `>` 写成
# `>= 0` 的规则在文本检查下全都合法，而它们永远不会亮。两条结构门（subjects / join-shape）
# 判的也是形状，不是「会不会响」——这里补的正是这一层。
#
# promtool 从哪来，按顺序试：`$PROMTOOL` → `PATH` 里的 `promtool` → 从 apt 取一份包解到
# 临时目录（**不安装、不动系统**）。三样都拿不到就退 3：看不了不是一致。
#
# 清单是 PrometheusRule 这个 CRD，promtool 只认裸的 `groups:`——所以这里从清单本身抽出
# `spec.groups` 写成临时规则文件再指给测试用。规则只有一份，测试读的就是发出去的那份。
#
# 用法：bash deploy/scripts/test-acceptance-census-alert-rules.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# `RULES_MANIFEST` 只给夹具自己用：它把清单的副本改坏（界写成 `>= 0`、`for: 15y`、
# 删掉一条规则）来证明这份单测**会红**。生产路径永远是下面这个文件。
MANIFEST="${RULES_MANIFEST:-${ROOT}/deploy/k3s/observability/manifests/17-prometheusrule-acceptance-census.yaml}"
CASE="${ROOT}/deploy/k3s/observability/tests/17-prometheusrule-acceptance-census.test.yaml"

[ -f "${MANIFEST}" ] || { echo "找不到清单：${MANIFEST}" >&2; exit 3; }
[ -f "${CASE}" ] || { echo "找不到夹具：${CASE}" >&2; exit 3; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

promtool="${PROMTOOL:-}"
if [ -z "${promtool}" ]; then
    if command -v promtool >/dev/null 2>&1; then
        promtool="$(command -v promtool)"
    else
        # 解包而不安装：CI 与开发机的系统状态都不该被一次测试改掉。
        if command -v apt-get >/dev/null 2>&1 && command -v dpkg-deb >/dev/null 2>&1; then
            if (cd "${work}" && apt-get download prometheus >/dev/null 2>&1); then
                deb="$(ls "${work}"/prometheus_*.deb 2>/dev/null | head -1)"
                if [ -n "${deb}" ] && dpkg-deb -x "${deb}" "${work}/deb" 2>/dev/null; then
                    promtool="${work}/deb/usr/bin/promtool"
                fi
            fi
        fi
    fi
fi
[ -n "${promtool}" ] && [ -x "${promtool}" ] || {
    echo "拿不到 promtool（\$PROMTOOL / PATH / apt 都没有）——看不了，不是一致" >&2
    exit 3
}

# 抽 spec.groups：从 `  groups:` 那一行起，整体去掉两格缩进。
awk 'f { sub(/^  /, ""); print } /^  groups:[[:space:]]*$/ { f = 1; print "groups:" }' \
    "${MANIFEST}" >"${work}/rules-dedented.yaml"
# 去掉 annotations：promtool 会**逐字**比对它，而这份夹具要断言的是「响不响」，
# 不是文案。把它留在里面，改一次措辞就得同步改夹具，而那条红说明不了任何事——
# 更糟的是有人会顺手把夹具里的文字复制回清单，从此两边都不知道哪份是真的。
awk '
    /^        annotations:[[:space:]]*$/ { skip = 1; next }
    skip && /^          / { next }
    { skip = 0; print }
' "${work}/rules-dedented.yaml" >"${work}/rules.yaml"
[ -s "${work}/rules.yaml" ] || { echo "清单里抽不出 spec.groups：${MANIFEST}" >&2; exit 3; }

# 测试文件里的 rule_files 指回清单（给人读的），这里改指抽出来的裸规则文件。
sed "s|^  - .*17-prometheusrule-acceptance-census\.yaml$|  - ${work}/rules.yaml|" \
    "${CASE}" >"${work}/case.yaml"
grep -q "^  - ${work}/rules.yaml$" "${work}/case.yaml" || {
    echo "夹具的 rule_files 行没改写成功——路径写法变了，改这条 sed" >&2
    exit 3
}

"${promtool}" test rules "${work}/case.yaml"
