#!/usr/bin/env bash
# principle: feedback_a_pinned_reading_that_grows_with_the_tree_must_be_derived_from_it
# check-credential-holder-surface.sh 的定向测试：它判的是"谁碰得到那两个 Secret
# 对象里的哪个键"，而这个面**出错时渲染期没有症状**——多一个持有者清单照旧合法、
# parity 照旧绿、Pod 照常起来，只是那个 Pod 从此握着平台 token。
#
# 为什么要有这份测试：门禁两侧都是反查出来的（env 键级、卷 items 键级或整卷、
# Role 动词且 patch 算写），反查写漏一环就退化成"什么都查不出来还报绿"；写者键表
# 是从 chart 模板解析的，解析在 helm 指令处截断过一次（`{{- if }}` 夹在键之间），
# 截断的后果正是"后面那些键被当成写者没声明"。两侧都值得钉住。
#
# 每个用例都在**真树的一份副本**上只改一处，然后断言它红的正是那一处。
#
# 用例：
#   1. 真树 -> 绿，并报出登记条数与写者声明键数（不是空跑）；
#   2. 新出现一个持有者（主应用多挂一个带外键，三形态都改）-> 红，点名那个持有者与键；
#   2b. 只在一种形态里多挂 -> 红，报出形态之间的持有面差异；
#   3. 持有者消失了而登记还在 -> 红，点名那条陈旧登记；
#   4. 登记表里写了个写者没声明的键、树上也没人读 -> 红（键名漂移）；
#   5. 写者声明的键一条登记都没有 -> 红，点名那个键；
#   6. 读一个没人建的键却不是 optional -> 红，点名工作负载与键；
#   7. 授了 Secret 的 Role 没有 RoleBinding -> 红（谁也用不到的授权）；
#   8. Role 的动词里 patch 被去掉 -> 红，报出权限面变了；
#   9. api 路径的代码指针指向不存在的文件 -> 红（指针防锈）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-credential-holder-surface.sh"
registry="deploy/credential-holders.tsv"
PROFILES=(k3s-single k3s-multi k8s-standard)
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

mirror() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy/helm/cogneva" "${work}/root/crates/cogneva/debt"
  cp -r "${repo}/deploy/rendered" "${work}/root/deploy/rendered"
  cp -r "${repo}/deploy/helm/cogneva/templates" "${work}/root/deploy/helm/cogneva/templates"
  cp "${repo}/${registry}" "${work}/root/${registry}"
  # api 路径那条路的 evidence 是代码指针，门禁只核文件在不在树上；副本里得把这些
  # 文件也摆出来，否则用例 1 会因为"副本里没有源码"而红。
  while read -r ev; do
    [ -n "${ev}" ] || continue
    mkdir -p "${work}/root/$(dirname "${ev}")"
    cp "${repo}/${ev}" "${work}/root/${ev}"
  done < <(awk -F'\t' '$3=="api" && $6!="" {print $6}' "${repo}/${registry}")
  # The role rows name their migration window's end in prose (`closes-when=`), and
  # the gate holds those pointers to files that exist just like the api rows'
  # evidence column — so they have to be staged here too, or case 1 reads the
  # copy's lack of source as a rusted pointer.
  while read -r ev; do
    [ -n "${ev}" ] || continue
    mkdir -p "${work}/root/$(dirname "${ev}")"
    cp "${repo}/${ev}" "${work}/root/${ev}"
  done < <(grep -o 'closes-when=[^ :]*' "${repo}/${registry}" | cut -d= -f2 | sort -u)
}
mirror

run_gate() { bash "${gate}" "${work}/root" 2>&1; }

APP_DEPLOY="deploy/rendered/k3s-single/41-deployment-cogneva.yaml"
LLM_ADMIN_BINDING="deploy/rendered/k3s-single/17-rolebinding-cogneva-llm-admin.yaml"

# 1) 真树绿，且读数是真读数
out="$(run_gate)" || fail "真树应当绿，实际红：${out}"
grep -q "CREDENTIAL HOLDER SURFACE OK" <<<"${out}" || fail "绿时没有报出判词：${out}"
# 登记条数不钉死在这个文件里，改成与登记表自己的数据行数比：这是两侧的一致性
# 断言（门报的条数必须就是表里的行数），钉一个数只会随每次加键过期一次——加
# `llm-pool-min-healthy` 那次就是这么红的，红得对，但红在了一个不该红的字面量上。
ledger_rows="$(grep -v '^#' "${repo}/${registry}" | grep -c .)"
grep -qE "${ledger_rows} 条登记" <<<"${out}" || fail "绿时没报出登记条数（表里 ${ledger_rows} 行）：${out}"
# 写者声明键数只能钉：它来自 helm 渲染后的模板解析，这个测试复刻不出来（复刻
# 一份解析就是第二处正本）。它在这里的作用是挡"解析被截断"——截断时报一个小得
# 多的数。加了键就改这一处。
grep -qE "声明键 28 个" <<<"${out}" || fail "绿时没报出写者声明键数（解析可能被截断）：${out}"

# 2) 新出现一个持有者：主应用多挂一个带外键（三种形态都改，才落到登记表那一侧）
mirror
for prof in "${PROFILES[@]}"; do
  python3 - "${work}/root/deploy/rendered/${prof}/41-deployment-cogneva.yaml" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
for i, line in enumerate(lines):
    if line.strip() == "- name: COGNEVA_GITHUB_API_BASE":
        lines[i:i] = [
            '            - name: COGNEVA_LEAKED_TOKEN',
            '              valueFrom:',
            '                secretKeyRef:',
            '                  name: cogneva-secrets-oob',
            '                  key: github-token',
        ]
        break
else:
    raise SystemExit("副本里没有可插入的锚点，用例失效")
open(p, "w").write("\n".join(lines))
PY
done
out="$(run_gate)" && fail "多出一个持有者时应当红，实际绿：${out}"
grep -q "渲染产物里 Deployment/cogneva 持有 cogneva-secrets-oob/github-token" <<<"${out}" \
  || fail "没点名新的持有者与键：${out}"

# 2b) 只在一种形态里多挂：另一形态的清单里永远看不见，所以要按形态比
mirror
python3 - "${work}/root/deploy/rendered/k3s-single/41-deployment-cogneva.yaml" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
for i, line in enumerate(lines):
    if line.strip() == "- name: COGNEVA_GITHUB_API_BASE":
        lines[i:i] = [
            '            - name: COGNEVA_LEAKED_TOKEN',
            '              valueFrom:',
            '                secretKeyRef:',
            '                  name: cogneva-secrets-oob',
            '                  key: github-token',
        ]
        break
else:
    raise SystemExit("副本里没有可插入的锚点，用例失效")
open(p, "w").write("\n".join(lines))
PY
out="$(run_gate)" && fail "只在一种形态里多挂时应当红，实际绿：${out}"
grep -q "\[k3s-single\] 比 \[k3s-multi\] 多一个持有者：Deployment/cogneva 持有" <<<"${out}" \
  || fail "没报出形态之间的持有面差异：${out}"

# 3) 持有者消失而登记还在
mirror
for prof in "${PROFILES[@]}"; do
  python3 - "${work}/root/deploy/rendered/${prof}/41-deployment-meilisearch.yaml" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
out, skip = [], False
for line in lines:
    if line.strip() == "- name: MEILI_MASTER_KEY":
        skip = True
        continue
    if skip:
        if line.strip().startswith("- name:"):
            skip = False
        else:
            continue
    out.append(line)
open(p, "w").write("\n".join(out))
PY
done
out="$(run_gate)" && fail "持有者消失时应当红，实际绿：${out}"
grep -q "Deployment/meilisearch 持有 cogneva-secrets/meili-master-key（env）在渲染产物里已不存在" \
  <<<"${out}" || fail "没点名那条陈旧的登记：${out}"

# 4) 登记表里的键写者没声明、树上也没人读
mirror
printf 'cogneva-secrets-oob\tgithub-tokenn\tenv\tDeployment/cogneva-security-gateway\tread\n' \
  >>"${work}/root/${registry}"
out="$(run_gate)" && fail "登记一个假键时应当红，实际绿：${out}"
grep -q "cogneva-secrets-oob/github-tokenn 不在写者声明的键表里" <<<"${out}" \
  || fail "没点名那个漂移的键：${out}"

# 5) 写者声明的键一条登记都没有
mirror
python3 - "${work}/root/${registry}" <<'PY'
import sys
p = sys.argv[1]
kept = [l for l in open(p) if "clickhouse-password" not in l]
open(p, "w").writelines(kept)
PY
out="$(run_gate)" && fail "声明键漏登记时应当红，实际绿：${out}"
grep -q "cogneva-secrets-oob/clickhouse-password 是写者声明的键" <<<"${out}" \
  || fail "没点名漏登记的键：${out}"

# 6) 读一个没人建的键却不是 optional
mirror
python3 - "${work}/root/${APP_DEPLOY}" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
for i, line in enumerate(lines):
    if line.strip() == "key: smtp-password" and lines[i + 1].strip() == "optional: true":
        del lines[i + 1]
        break
else:
    raise SystemExit("副本里 smtp-password 不是 optional 形态，用例失效")
open(p, "w").write("\n".join(lines))
PY
out="$(run_gate)" && fail "非 optional 读未声明键时应当红，实际绿：${out}"
grep -q "Deployment/cogneva/.*按名读 cogneva-secrets/smtp-password" <<<"${out}" \
  || fail "没点名工作负载与键：${out}"

# 7) 授了 Secret 的 Role 没有 RoleBinding
mirror
rm -f "${work}/root/${LLM_ADMIN_BINDING}"
out="$(run_gate)" && fail "授权没有持有者时应当红，实际绿：${out}"
grep -q "Role/cogneva-llm-admin 授了 Secret 的 read，却没有 RoleBinding" <<<"${out}" \
  || fail "没报出谁也用不到的授权：${out}"

# 8) Role 的动词里 patch 被去掉（三种形态都改，落到登记表那一侧）
mirror
for prof in "${PROFILES[@]}"; do
  python3 - "${work}/root/deploy/rendered/${prof}/16-role-cogneva-security-gateway.yaml" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = '''    resourceNames: ["cogneva-secrets", "cogneva-secrets-oob"]
    verbs: ["get", "patch"]'''
assert old in s, "副本里的 secrets 规则不是预期形态，用例失效"
open(p, "w").write(s.replace(old, '''    resourceNames: ["cogneva-secrets", "cogneva-secrets-oob"]
    verbs: ["get"]'''))
PY
done
out="$(run_gate)" && fail "权限面变窄时应当红，实际绿：${out}"
grep -q "登记表说 ServiceAccount/cogneva-security-gateway 对 cogneva-secrets/\* 是 read,write" \
  <<<"${out}" || fail "没报出权限面变了：${out}"

# 9) api 路径的代码指针锈了
mirror
python3 - "${work}/root/${registry}" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read().replace(
    "crates/cog-gateway/src/git_identity.rs",
    "crates/cog-gateway/src/renamed_away.rs")
open(p, "w").write(s)
PY
out="$(run_gate)" && fail "代码指针指向不存在的文件时应当红，实际绿：${out}"
grep -q "的 evidence 'crates/cog-gateway/src/renamed_away.rs' 在树上不存在" <<<"${out}" \
  || fail "没报出锈掉的指针：${out}"

echo "OK: credential-holder-surface 门禁定向测试通过（1 绿 + 9 红，各点名一处）"
