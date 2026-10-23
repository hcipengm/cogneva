#!/usr/bin/env bash
# principle: feedback_the_missing_hop_of_ownership_belongs_on_the_rule_not_in_a_join_table
# 告警的「归谁」那一跳：一条会被投递出去（带 `cogneva_io_managed`）的规则，必须同时申报
# 它判的是哪个面（`cogneva_io_face`）。
#
# 为什么这一格必须存在：告警链有五环——读数、产出者、消费者、判据、**归谁**。前四环各有
# 机器可读的答案，第五环没有。2026-10-23 实测的那张表说明缺的是什么：正 firing 十一行，
# `acknowledged_at` 全空，最老一行开了 5550 分钟，而 `last_seen_at` 都在半分钟内——
# 写者一直在看，是人没看。缺的不是一条更响的提醒，是「这条响了该谁动」在系统里根本
# 没有答案。人来读的时候只能靠记性，而读者恰恰是缺的那一环。
#
# 为什么是标签而不是一张连接表：缺的那一跳是「规则 → 面」。它不是会变的配置（换个人负责
# 这个面，规则判的仍然是这个面），而是规则自己的性质，和它的表达式同级。写成规则上的标签，
# 它随告警行整份流进 `alerts.labels`，于是「这一行归谁」在行上就答得出来。反过来做成一张
# `rule→face` 的 TSV，加规则的人永远不记得去补那一行，缺的形态是「新规则永远没有面」，
# 而且在表上看不出来。
#
# 为什么不在这里判「面的值对不对」：面名的正本在面主表里，而面主表不在这个仓。这条门只判
# **申报了没有**；申报了一个不存在的面名，是读取方那一步的事——它 join 面主表时找不到面主，
# 那一行照样是「没人管」。两处各判一件事，判据才不会互相抄成两个口径。
#
# 射程两套，因为告警有两个写者，两边都会往同一张 `alerts` 表里写行：
#
#   1. `deploy/k3s/observability/manifests/` 里的 PrometheusRule 清单（Prometheus 求值、
#      镜像进表）。这里判两向：带 `cogneva_io_managed` 的缺面即红；面标签挂在不投递的
#      规则上也红。
#   2. chart 里那套由进程自己求值的规则（`deploy/helm/cogneva/files/cogneva.json` 的
#      `observability.infra_watch.rules`）。它们的配置结构里有独立的 `face` 一格，进程把它
#      写进存库行的 `cogneva_io_face` 标签，读法两边一致。这里判一向：每条规则都要有面。
#      **这一套才是现网最响的那一半**——2026-10-23 实测正 firing 的十一行里八行出自它，
#      而当时这一格还不存在，于是它们全是无主的，门却绿着。
#
# 只读 chart 那一份就够：`deploy/k3s/cogneva-json-configmap.yaml` 与 `deploy/rendered/`
# 是它的载物，由 check-deploy-parity.sh 逐字段比对，在这里再抄一遍判据只会多一份会各自漂的
# 口径。
#
# 用法：bash deploy/scripts/check-alert-rule-faces.sh [manifest 目录] [chart 配置路径]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MANIFESTS="${1:-${ROOT}/deploy/k3s/observability/manifests}"
CHART_CONFIG="${2:-${ROOT}/deploy/helm/cogneva/files/cogneva.json}"
[ -d "${MANIFESTS}" ] || { echo "找不到规则目录：${MANIFESTS}" >&2; exit 2; }
[ -f "${CHART_CONFIG}" ] || { echo "找不到 chart 配置：${CHART_CONFIG}" >&2; exit 2; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "${MANIFESTS}" "${CHART_CONFIG}" <<'PYEOF'
import json
import os
import sys

manifests = sys.argv[1]
chart_config = sys.argv[2]
managed_label = "cogneva_io_managed"
face_label = "cogneva_io_face"


def indent_of(line):
    return len(line) - len(line.lstrip(" "))


def parse_labels(lines, start):
    """Read the `key: "value"` rows under a `labels:` opened at `lines[start]`."""
    base = indent_of(lines[start])
    out = {}
    i = start + 1
    while i < len(lines):
        line = lines[i]
        if not line.strip() or line.lstrip().startswith("#"):
            i += 1
            continue
        if indent_of(line) <= base:
            break
        key, _, value = line.strip().partition(":")
        out[key.strip()] = value.strip().strip('"').strip("'")
        i += 1
    return out


def alerts_in(path):
    """Every `- alert:` in every PrometheusRule document of one file.

    Read line-wise rather than through a YAML library: the shape these manifests
    are held to is exactly the shape this reads, and a gate that needs a package
    the CI image may not have is a gate that quietly stops running.
    """
    text = open(path, encoding="utf-8").read()
    docs = text.split("\n---\n")
    found = []
    for doc in docs:
        if "kind: PrometheusRule" not in doc:
            continue
        lines = doc.split("\n")
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped.startswith("- alert:"):
                continue
            name = stripped[len("- alert:"):].strip()
            labels = {}
            for j in range(i + 1, len(lines)):
                deeper = lines[j]
                if not deeper.strip() or deeper.lstrip().startswith("#"):
                    continue
                if indent_of(deeper) <= indent_of(line) and deeper.strip().startswith("- "):
                    break
                if deeper.strip() == "labels:":
                    labels = parse_labels(lines, j)
                    break
                if indent_of(deeper) <= indent_of(line):
                    break
            found.append((name, labels))
    return found


complaints = []
total = 0
managed = 0
tagged = 0
seen_files = 0

for name in sorted(os.listdir(manifests)):
    if not name.endswith((".yaml", ".yml")):
        continue
    path = os.path.join(manifests, name)
    if not os.path.isfile(path):
        continue
    seen_files += 1
    for alert, labels in alerts_in(path):
        total += 1
        is_managed = labels.get(managed_label) == "true"
        face = labels.get(face_label, "").strip()
        if is_managed:
            managed += 1
            if not face:
                complaints.append(
                    f"{name}: {alert} 会被投递进告警账（{managed_label}=\"true\"），"
                    f"却没有申报 {face_label}——这一行响了之后没有任何面的读法答得出该谁动"
                )
            else:
                tagged += 1
        elif face:
            complaints.append(
                f"{name}: {alert} 申报了 {face_label}=\"{face}\"，但它不带 "
                f"{managed_label}=\"true\"，投递不到告警账——面标签因此没有任何读者"
            )

if total == 0:
    print(f"没在 {manifests} 里读到任何告警规则，所以这条门什么都没看", file=sys.stderr)
    sys.exit(2)

# 第二套：chart 里那批由进程自己求值的规则。读 JSON 而不是扫文本，因为这一格本来就是
# 配置里的一个字段——写成文本再扫，等于把「结构里有没有」换成一门自己发明的语法。
try:
    with open(chart_config, encoding="utf-8") as f:
        chart_rules = json.load(f)["observability"]["infra_watch"]["rules"]
except (OSError, ValueError, KeyError) as e:
    print(f"读不出 {chart_config} 的 observability.infra_watch.rules：{e}", file=sys.stderr)
    sys.exit(2)

if not chart_rules:
    print(f"{chart_config} 里一条进程内规则都没有，这一半没被看过", file=sys.stderr)
    sys.exit(2)

chart_owned = 0
for rule in chart_rules:
    if not isinstance(rule, dict) or "name" not in rule:
        complaints.append(f"{os.path.basename(chart_config)}: 有一条规则连 name 都没有，读不出它归谁")
        continue
    face = str(rule.get("face") or "").strip()
    if not face:
        complaints.append(
            f"{os.path.basename(chart_config)}: {rule['name']} 由进程自己求值并写进告警账，"
            f"却没有 face——这一行响了之后没有任何面的读法答得出该谁动"
        )
    else:
        chart_owned += 1

if complaints:
    for c in complaints:
        print("RED " + c)
    print(
        f"\n{len(complaints)} 条问题：清单里托管规则 {tagged}/{managed} 条申报了面"
        f"（共 {total} 条告警，{seen_files} 份清单）；chart 里进程内规则 "
        f"{chart_owned}/{len(chart_rules)} 条申报了面"
    )
    sys.exit(1)

print(
    f"ok：清单 {managed} 条托管告警规则、chart {chart_owned} 条进程内规则都申报了面"
    f"（共 {total} 条清单告警，{seen_files} 份清单）"
)
PYEOF
