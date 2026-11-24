#!/usr/bin/env bash
# principle: feedback_a_new_rule_lands_in_three_places_and_the_render_freshness_gate_names_the_one_you_forgot
# render-deploy.sh --out 的定向测试。
#
# 这条命令是渲染产物的**唯一**入口：产物不进仓，读者（引导器构建、契约测试、部署
# 门、集群内落地端、回滚）在读的那一刻调它现场生成。所以它必须证明三件事：
#   1. 它真的写出了一整棵（每个 profile 都有清单），而且**一个字节都不改树**——
#      仓库里的渲染链是只读的，否则读者会互相踩，或者把「读过一次」变成一次提交；
#   2. 同样输入渲两遍字节相同——读者拿到的产物不随「谁先跑到」变；
#   3. 解析不了的产物直接拒绝写出（重复键、超 15 字符的端口名）。这两条在
#      `deploy/rendered` 还进仓时由新鲜度门顺带守着，现在写出点是唯一的守卫。
#
# 旧的 `--check`（重渲染后与已提交产物 diff）随产物一起退场：没有已提交的产物，
# 就没有「新鲜」这件事可言。这一个用例断言它真的不在了——留一个只判半边的开关，
# 下一个人会以为产物还在被比对。
#
# 每个用例都在真树 deploy/ 的一份副本上跑（脚本按自身位置算 REPO_ROOT）。
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
python3 -c 'import yaml' 2>/dev/null || {
  echo "缺少依赖：python3-yaml" >&2
  exit 2
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 基线是**真树的 deploy/**：门只读 chart、profiles 与 scripts 三处。
case_dir() { # case_dir <名字> -> 打印一份可以随便改的树根
  local d="${work}/$1"
  mkdir -p "${d}"
  cp -a "${repo}/deploy" "${d}/deploy"
  printf '%s' "${d}"
}

tree_digest() { # tree_digest <树根> -> deploy/ 下每个文件的路径与内容摘要
  ( cd "$1" && find deploy -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}' )
}

render_out() { # render_out <树根> <输出目录> -> 合并输出，回传退出码
  local out
  if out="$(bash "$1/${gate_rel}" --out "$2" 2>&1)"; then
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

# 用例 1：真树副本渲出一整棵，且树一个字节都没变。
d="$(case_dir copy)"
before="$(tree_digest "${d}")"
if ! out="$(render_out "${d}" "${work}/out-1")"; then
  fail "真树副本渲染失败（基线先红了，后面的用例都不成立）：${out}"
fi
after="$(tree_digest "${d}")"
[ "${before}" = "${after}" ] \
  || fail "--out 改了树：渲染输入被写回仓，读者之间会互相踩（差在 deploy/ 下，逐文件比对）"
profiles="$(find "${d}/deploy/helm/cogneva/profiles" -maxdepth 1 -name '*.yaml' -type f | wc -l)"
[ "${profiles}" -gt 0 ] || fail "chart 里一个 profile 都没有，这个用例没有题目"
for p in "${d}/deploy/helm/cogneva/profiles"/*.yaml; do
  name="$(basename "$p" .yaml)"
  count="$(find "${work}/out-1/${name}" -maxdepth 1 -name '*.yaml' -type f 2>/dev/null | wc -l || true)"
  [ "${count}" -gt 0 ] || fail "${name} 渲染后一个清单都没有（空目录读作「本来就没有东西」）"
done

# 用例 2：同样输入渲两遍，字节相同。
if ! render_out "${d}" "${work}/out-2" >/dev/null; then
  fail "第二次渲染失败：${d}"
fi
diff -r "${work}/out-1" "${work}/out-2" >/dev/null \
  || fail "两次渲染的字节不同：读者拿到的产物随「谁先跑到」变"

# 用例 3：`--check` 不在了（产物不提交，没有可比的另一边）。
if out="$(bash "${d}/${gate_rel}" --check 2>&1)"; then
  fail "--check 还在且绿着：新鲜度门已经随产物退场，它不该继续存在：${out}"
fi
grep -q 'usage' <<<"${out}" || fail "拒绝 --check 时没说它是用法错：${out}"

# 用例 4：模板里写重了一个键 -> 渲染直接拒绝写产物。
#
# 重复键的产物**能渲染出来也能提交上去**；它违反的是「产物能被解析」。真实故障：
# 一份 NetworkPolicy 的 metadata.labels 先由 chart 助手写了 app.kubernetes.io/name，
# 模板里又手写了一遍，helm template 成功、文件写进了树，等到 API server 与部署器的
# 清单读取器解析时才被整份拒绝，装机和 4 条部署器测试同时红。写出点自己先把每份
# 文档解析一遍——现在这是唯一的守卫。
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
if out="$(render_out "${d}" "${work}/out-dup" 2>&1)"; then
  fail "模板里写重了键，渲染仍然写出了产物（解析不了的清单会一直被提交）：${out}"
fi
grep -q "duplicate key 'app.kubernetes.io/name'" <<<"${out}" \
  || fail "红了，但没报出重复的那个键：${out}"

# 用例 5：容器端口名超过 15 字符 -> 渲染直接拒绝写产物。
#
# `postgres-metrics` 有 16 个字符，yaml 层面完全合法，CI 也全绿，直到部署器把这批
# 清单 apply 上去时整份 StatefulSet 被拒——落地通道为此停了 12 小时。
d="$(case_dir long-port-name)"
edit_once "${d}/deploy/helm/cogneva/templates/postgres.yaml" \
  '            - name: pg-metrics
' \
  '            - name: postgres-metrics
'
if out="$(render_out "${d}" "${work}/out-port" 2>&1)"; then
  fail "端口名超过 15 字符，渲染仍然写出了产物（apply 会整份拒掉）：${out}"
fi
grep -q "超过 15 的上限" <<<"${out}" \
  || fail "红了，但没报出超长的是哪个端口：${out}"

echo "OK: --out 写出每个 profile 的整棵、不改树、两次渲染字节相同；--check 已退场；模板写重键与超长端口名时渲染拒绝写出"
