#!/usr/bin/env bash
# principle: feedback_a_new_rule_lands_in_three_places_and_the_render_freshness_gate_names_the_one_you_forgot
# render-deploy.sh --check 的定向测试。
#
# 为什么要有这份测试：这条命令是**判定管线里的准入条件**——触及 deploy/** 的变更要
# 落在树上之前，部署声明（self_evolution.deploy_admission）会跑它，退出码非零才拒收。
# 准入条件最坏的形状是「看着在判、实际永远绿」：脚本还在、输出还像样、退出码恒 0，
# 于是每次拓扑变更都被放行，却没有任何东西被检查过。所以这条门禁必须自己证明它会红：
# 在一个**人为造的漂移**上退出非零，并且报出的正是那一处漂移。
#
# 每个用例都在真树的一份副本上跑（脚本按自身位置算 REPO_ROOT，副本自成一体）：
#   1. 原样副本 -> 绿，并报出 RENDER FRESHNESS OK（空跑也是绿的，所以要看到读数）；
#   2. 渲染产物改一个字段 -> 红，diff 里正是那个字段；
#   3. chart 侧改一个值、产物没重渲染 -> 红（反向漂移同样要被看见）。
#
# 用法：bash deploy/scripts/tests/render-freshness.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate_rel="deploy/scripts/render-deploy.sh"
fail() { echo "FAIL: $*" >&2; exit 1; }

for bin in helm python3; do
  command -v "${bin}" >/dev/null || { echo "缺少依赖：${bin}" >&2; exit 2; }
done

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 基线是**真树的 deploy/**：门禁只读 chart、profiles 与 rendered 三处，复制别的目录
# 只会让「只改了一处」这件事变得难以相信。
case_dir() { # case_dir <名字> -> 打印一份可以随便改的树根
  local d="${work}/$1"
  mkdir -p "${d}"
  cp -a "${repo}/deploy" "${d}/deploy"
  printf '%s' "${d}"
}

run() { # run <树根> -> 打印合并输出，回传退出码
  local out
  if out="$(bash "$1/${gate_rel}" --check 2>&1)"; then
    printf '%s\n' "${out}"; return 0
  else
    printf '%s\n' "${out}"; return 1
  fi
}

# 在一份文档里按字面量改一处，锚点必须正好出现一次——锚点搬家后替换会变成空操作，
# 而空操作的用例是绿的（变异要先断言它生效了）。
edit_once() { # edit_once <文件> <旧串> <新串>
  python3 - "$@" <<'PY'
import sys
path, old, new = sys.argv[1:4]
text = open(path, encoding="utf-8").read()
n = text.count(old)
assert n == 1, f"{path}: 锚点 {old!r} 出现 {n} 次，要正好一次"
open(path, "w", encoding="utf-8").write(text.replace(old, new, 1))
PY
}

# 用例 1：真树副本必须绿，且给出读数。
d="$(case_dir copy)"
if out="$(run "${d}")"; then
  grep -q 'RENDER FRESHNESS OK' <<<"${out}" \
    || fail "绿的副本没有给出 RENDER FRESHNESS OK 读数：${out}"
else
  fail "真树副本本身就是漂移的（基线先红了，后面的用例都不成立）：${out}"
fi

# 用例 2：产物被改一个字段 -> 红，且 diff 里正是那个字段。
d="$(case_dir rendered-drift)"
edit_once "${d}/deploy/rendered/k3s-single/10-configmap-cogneva-json.yaml" \
  '"test_timeout_secs": 3600' '"test_timeout_secs": 3601'
if out="$(run "${d}")"; then
  fail "渲染产物被改了一个字段，--check 仍然绿（准入条件永远不会红）：${out}"
fi
grep -q 'test_timeout_secs' <<<"${out}" \
  || fail "红了，但没报出被改的那一处：${out}"

# 用例 3：chart 侧改了值、产物没重渲染 -> 红（这条命令存在的理由）。
# 改的是 profile 而不是 values.yaml：同一项在两边都写着，profile 覆盖 values，
# 只动 values 那一份渲染结果不变——那是一条**空变异**，用例会假绿。
d="$(case_dir chart-drift)"
edit_once "${d}/deploy/helm/cogneva/profiles/k3s-single.yaml" \
  'hostPath: /usr/local/bin/helm' 'hostPath: /usr/local/bin/helm-probe'
if out="$(run "${d}")"; then
  fail "chart 改了值而产物没重渲染，--check 仍然绿：${out}"
fi
grep -q 'helm-probe' <<<"${out}" \
  || fail "红了，但没报出 chart 侧的那处改动：${out}"

# 用例 4：模板里写重了一个键 -> 渲染直接拒绝写产物。
#
# 为什么这一条在「新鲜度」的测试里：重复键的产物**能渲染出来也能提交上去**，
# 它不违反新鲜度——chart 和 rendered 完全一致。它违反的是「产物能被解析」，
# 而这两件事共用同一个写出点。真实故障：一份 NetworkPolicy 的 metadata.labels
# 先由 chart 助手写了 app.kubernetes.io/name，模板里又手写了一遍，helm template
# 成功、文件写进了树，等到 API server 与部署器的清单读取器解析时才被整份拒绝，
# 装机和 4 条部署器测试同时红。所以写出点自己先把每份文档解析一遍。
d="$(case_dir duplicate-key)"
edit_once "${d}/deploy/helm/cogneva/templates/network-policy.yaml" \
  '  name: crawl4ai-ingress
  namespace: {{ include "cogneva.namespace" . }}
  labels:
    {{- include "cogneva.labels" . | nindent 4 }}
    app.kubernetes.io/component: fetch
' \
  '  name: crawl4ai-ingress
  namespace: {{ include "cogneva.namespace" . }}
  labels:
    {{- include "cogneva.labels" . | nindent 4 }}
    app.kubernetes.io/name: crawl4ai
    app.kubernetes.io/component: fetch
'
if out="$(bash "${d}/${gate_rel}" 2>&1)"; then
  fail "模板里写重了键，渲染仍然写出了产物（解析不了的清单会一直被提交）：${out}"
fi
grep -q "duplicate key 'app.kubernetes.io/name'" <<<"${out}" \
  || fail "红了，但没报出重复的那个键：${out}"

echo "OK: render freshness 门禁在两种漂移上都红，在干净树上绿；模板写重键时渲染拒绝写出"
