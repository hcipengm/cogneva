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
# 另一类清单没有 CRD：进程内 watcher 那一库规则是部署配置里的
# `observability.infra_watch.rules`（`deploy/helm/cogneva/files/cogneva.json`），
# 由应用自己拿去查 Prometheus，从不渲染成 PrometheusRule。它们的表达式与 CRD 那批同形，
# 所以这里也认 `.json`：按夹具头部的 `# rule: <名字>` 从那一库里**按名字取**规则，
# 规则正文仍然来自发货的那一份，不是夹具里抄的。只取点名的几条，是因为 promtool 会
# 把组里每条规则在每个求值点都算一遍，269 条乘上一个六天的夹具就是把一条门跑成一次压测。
#
# 用法：
#   bash deploy/scripts/test-acceptance-census-alert-rules.sh                  # 验收普查那三条
#   bash deploy/scripts/test-acceptance-census-alert-rules.sh <清单> <夹具>    # 任一份清单
#   RULES_MANIFEST=…/cogneva.json RULES_CASE=…/<夹具> bash <本脚本>            # 进程内那一库
#
# 第二、三种用法不是顺手加的：一个规则族「会不会响」只能这样判，而它不该只对一份清单
# 有效——每复制一份跑者，取 promtool 的那段（`$PROMTOOL` → PATH → apt 解包）就会多一份
# 会各自漂的副本。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# `RULES_MANIFEST` / `RULES_CASE` 只给夹具自己用：它把清单的副本改坏（界写成 `>= 0`、
# `for: 15y`、删掉一条规则）来证明这份单测**会红**。生产路径永远是默认那两份文件。
MANIFEST="${1:-${RULES_MANIFEST:-${ROOT}/deploy/k3s/observability/manifests/17-prometheusrule-acceptance-census.yaml}}"
CASE="${2:-${RULES_CASE:-${ROOT}/deploy/k3s/observability/tests/17-prometheusrule-acceptance-census.test.yaml}}"

[ -f "${MANIFEST}" ] || { echo "找不到清单：${MANIFEST}" >&2; exit 3; }
[ -f "${CASE}" ] || { echo "找不到夹具：${CASE}" >&2; exit 3; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 解出来的 promtool 缓存在用户缓存目录：夹具那一支会把这个脚本连跑四五次（真文件一次、
# 每处改坏各一次），不缓存就是四五次 23MB 的下载，而这条门禁本来就慢。
cache="${XDG_CACHE_HOME:-$HOME/.cache}/cogneva-promtool"
promtool="${PROMTOOL:-}"
if [ -z "${promtool}" ]; then
    if command -v promtool >/dev/null 2>&1; then
        promtool="$(command -v promtool)"
    elif [ -x "${cache}/usr/bin/promtool" ]; then
        promtool="${cache}/usr/bin/promtool"
    else
        # 解包而不安装：CI 与开发机的系统状态都不该被一次测试改掉。
        if command -v apt-get >/dev/null 2>&1 && command -v dpkg-deb >/dev/null 2>&1; then
            if (cd "${work}" && apt-get download prometheus >/dev/null 2>&1); then
                deb="$(ls "${work}"/prometheus_*.deb 2>/dev/null | head -1)"
                if [ -n "${deb}" ]; then
                    # 直接解到缓存目录：`dpkg-deb -x` 就是 tar 解开，已存在的目录会合并。
                    if mkdir -p "${cache}" 2>/dev/null && dpkg-deb -x "${deb}" "${cache}" 2>/dev/null &&
                        [ -x "${cache}/usr/bin/promtool" ]; then
                        promtool="${cache}/usr/bin/promtool"
                    elif dpkg-deb -x "${deb}" "${work}/deb" 2>/dev/null; then
                        # 缓存写不进去（只读家目录之类）：退回临时目录，慢但这台机器还能跑。
                        promtool="${work}/deb/usr/bin/promtool"
                    fi
                fi
            fi
        fi
    fi
fi
[ -n "${promtool}" ] && [ -x "${promtool}" ] || {
    echo "拿不到 promtool（\$PROMTOOL / PATH / apt 都没有）——看不了，不是一致" >&2
    exit 3
}

# 抽规则正文：CRD 清单抽 `spec.groups`，部署配置抽被点名的几条。两条路都写成裸 `groups:`
# 文件，后面一视同仁。
case "${MANIFEST}" in
    *.json)
        python3 - "${MANIFEST}" "${CASE}" >"${work}/rules.yaml" <<'PY'
import json
import re
import sys

config_path, case_path = sys.argv[1], sys.argv[2]
config = json.load(open(config_path, encoding="utf-8"))
library = config["observability"]["infra_watch"]["rules"]
wanted = re.findall(r"^#\s*rule:\s*(\S+)\s*$", open(case_path, encoding="utf-8").read(), re.M)
if not wanted:
    sys.exit("夹具里没有 `# rule: <名字>` 行——不点名就等于没有受检对象")
by_name = {rule["name"]: rule for rule in library}
missing = [name for name in wanted if name not in by_name]
if missing:
    sys.exit(f"部署配置里没有这些规则：{', '.join(missing)}")
# 阈值必须折进表达式里。这一库的规则不是「表达式非空就响」：进程拿到每个序列的**值**再
# 按 `condition` 比一次（`AlertCondition::evaluate`），而 Prometheus 的告警规则只要表达式
# 返回元素就点火。两种语义在一处恰好同形——表达式自己带比较、condition 又是 `> 0`——其余
# 全部不同：`node_disk_usage_high` 的表达式返回百分比、阈值 75，不折进去的话夹具在 80 上亮、
# 在 7500 上也亮，改坏阈值这条门一格都抓不住。所以这里把条件写成 PromQL 的比较，让
# promtool 的「响不响」就是进程的「响不响」。
OPERATORS = {
    "greater_than": ("{p} > {th}"),
    "less_than": ("{p} < {th}"),
    "greater_than_or_equal": ("{p} >= {th}"),
    "less_than_or_equal": ("{p} <= {th}"),
    # 进程那两种是 EPSILON 比较，不是精确相等：`abs(v - th) < f64::EPSILON`。
    "equal": ("abs({p} - {th}) < 2.220446049250313e-16"),
    "not_equal": ("abs({p} - {th}) >= 2.220446049250313e-16"),
}


def fires_when(rule):
    condition = rule.get("condition") or {}
    if len(condition) != 1:
        sys.exit(f"{rule['name']}：condition 恰好要有一个算子，实得 {condition}")
    (key, threshold), = condition.items()
    shape = OPERATORS.get(key)
    if shape is None:
        sys.exit(f"{rule['name']}：不认识的 condition 算子 {key!r}")
    return shape.format(p=f"({rule['promql']})", th=repr(float(threshold)))


rules = []
for name in wanted:
    rule = by_name[name]
    # annotations 逐字比对，会把「改一次措辞」变成一次红；夹具判的是响不响。
    entry = {
        "alert": name,
        "expr": fires_when(rule),
        "labels": {"severity": rule.get("severity", "warning")},
    }
    if "for_seconds" in rule:
        entry["for"] = f"{rule['for_seconds']}s"
    rules.append(entry)
print(json.dumps({"groups": [{"name": "cogneva-json", "rules": rules}]}, indent=1))
PY
        ;;
    *)
        # 从 `  groups:` 那一行起，整体去掉两格缩进。
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
        ;;
esac
[ -s "${work}/rules.yaml" ] || { echo "清单里抽不出规则：${MANIFEST}" >&2; exit 3; }

# 测试文件里的 rule_files 指回清单（给人读的），这里改指抽出来的裸规则文件。
# 按**形状**改（`rule_files:` 下面那一格，行首正好两格缩进），不按某一份清单的文件名：
# 写死文件名时，夹具把清单复制到另一个名字下做改坏实验，这一行就静默改不动，promtool 读到
# CRD 文件直接判未知字段——那是一条与规则本身无关的红（实测：三格改坏实验全报 rc=3）。
sed -E "s#^  - .*\.(yaml|json)\$#  - ${work}/rules.yaml#" \
    "${CASE}" >"${work}/case.yaml"
grep -q "^  - ${work}/rules.yaml$" "${work}/case.yaml" || {
    echo "夹具的 rule_files 行没改写成功——路径写法变了，改这条 sed" >&2
    exit 3
}

"${promtool}" test rules "${work}/case.yaml"
