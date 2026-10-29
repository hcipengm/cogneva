#!/usr/bin/env bash
# principle: feedback_a_pinned_reading_that_grows_with_the_tree_must_be_derived_from_it
# check-credential-holder-surface.sh 的定向测试：它判的是"谁碰得到那两个 Secret
# 对象里的哪个键"，而这个面**出错时渲染期没有症状**——多一个持有者清单照旧合法、
# parity 照旧绿、Pod 照常起来，只是那个 Pod 从此握着平台 token。
#
# 为什么要有这份测试：门禁两侧口径不同——「谁在读」是从渲染产物反查的（env 键级、
# 卷 items 键级或整卷、Role 动词且 patch 算写），「谁该读」是键自己头上那行
# `# readers:` 声明的。反查写漏一环就退化成"什么都查不出来还报绿"；声明的解析在
# helm 指令处截断过一次（`{{- if }}` 夹在键之间），截断的后果正是"后面那些键被
# 当成没写读者"。两侧都值得钉住。
#
# 每个用例都在**真树的一份副本**上只改一处，然后断言它红的正是那一处。
#
# 用例：
#   1. 真树 -> 绿，并报出持有者数与写者声明键数（不是空跑）；
#   2. 新出现一个持有者（主应用多挂一个带外键，三形态都改）-> 红，点名那个持有者与键；
#   2b. 只在一种形态里多挂 -> 红，报出形态之间的持有面差异；
#   3. 持有者消失了而声明还在 -> 红，点名那条陈旧的声明；
#   4. 声明里写了个树上不存在的读者 -> 红（声明漂移）；
#   5. 写者声明的键头上没有 `# readers:` 行 -> 红，点名那个键；
#   6. 读一个没人建的键却不是 optional -> 红，点名工作负载与键；
#   7. 授了 Secret 的 Role 没有 RoleBinding -> 红（谁也用不到的授权）；
#   8. 整对象权限绑到了网关以外的身份 -> 红（代持带外凭证只该有一份）；
#   9. api 路径的代码指针指向不存在的文件 -> 红（指针防锈）；
#   10. api 路径的文件还在、但已不再出现那个键名 -> 红（指针锈成空指针）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-credential-holder-surface.sh"
secret_tpl="deploy/helm/cogneva/templates/secret-oob.yaml"
# 形态的集合从 `deploy/rendered/` 下真实存在的目录读，不在这里抄一份：抄的那份
# 在新增 profile 后只会覆盖老的三个，用例 2 想在所有形态里挂同一个持有者，于是
# 挂漏的那一种会和其余形态显出差异，用例假红——而红在报「形态之间持有面不一致」，
# 真因是这份名单。下面的 mirror 复制的正是这棵树。
PROFILES=()
for profile_dir in "${repo}"/deploy/rendered/*/; do
    [ -d "${profile_dir}" ] || continue
    PROFILES+=("$(basename "${profile_dir}")")
done
[ "${#PROFILES[@]}" -gt 0 ] || {
    echo "FAIL: deploy/rendered 下一个形态目录都没有——判不了该改哪几种" >&2
    exit 2
}
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

mirror() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy/helm/cogneva"
  cp -r "${repo}/deploy/rendered" "${work}/root/deploy/rendered"
  cp -r "${repo}/deploy/helm/cogneva/templates" "${work}/root/deploy/helm/cogneva/templates"
  # api 路径那条路只有代码指针，门禁核两件事：文件在不在树上、文件里还出不出现那个
  # 键名。副本里得把这些文件也摆出来，否则用例 1 会因为"副本里没有源码"而红。
  while read -r ev; do
    [ -n "${ev}" ] || continue
    mkdir -p "${work}/root/$(dirname "${ev}")"
    cp "${repo}/${ev}" "${work}/root/${ev}"
  done < <(grep -ho '(api: [^)]*)' "${repo}/deploy/helm/cogneva/templates/"*.yaml \
             | sed 's/^(api: *//; s/)$//' | sort -u)
}
mirror

run_gate() { bash "${gate}" "${work}/root" 2>&1; }
edit() { python3 - "$@" <<'PY'
import sys
p, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(p, encoding="utf-8").read()
if old not in s:
    raise SystemExit(f"副本里没有可替换的锚点（用例失效）：{old!r}")
open(p, "w", encoding="utf-8").write(s.replace(old, new, 1))
PY
}

APP_DEPLOY="deploy/rendered/k3s-single/41-deployment-cogneva.yaml"
# 用例 7 要删的那条 RoleBinding。绑的是**唯一一个今天还有 Secret 权限的角色**：
# llm-admin 那条曾经是它，但读到主应用的那三个读点搬进网关后，它不再授 Secret，
# 再拿它做用例等于删一条本来就不带权限的绑定——门没东西可报，用例就从红变绿。
# 用例本身仍然成立，变的是它必须绑在一个真授了 Secret 的角色上。
SECRETS_ROLE_BINDING="deploy/rendered/k3s-single/17-rolebinding-cogneva-security-gateway.yaml"

# 1) 真树绿，且读数是真读数
out="$(run_gate)" || fail "真树应当绿，实际红：${out}"
grep -q "CREDENTIAL HOLDER SURFACE OK" <<<"${out}" || fail "绿时没有报出判词：${out}"
# 写者声明键数只能钉：它来自 helm 渲染后的模板解析，这个测试复刻不出来（复刻
# 一份解析就是第二处正本）。它在这里的作用是挡"解析被截断"——截断时报一个小得
# 多的数。加了键就改这一处。
grep -qE "声明键 29 个" <<<"${out}" || fail "绿时没报出写者声明键数（解析可能被截断）：${out}"
# 持有者数同样只钉这一个：它随工作负载增减而变，钉死会在加一个 Deployment 时假红；
# 但一个都不报说明反查退化成空集，那正是"全绿"最危险的模样。
grep -qE "个持有者与键头上的" <<<"${out}" || fail "绿时没报出持有者数：${out}"

# 2) 新出现一个持有者：主应用多挂一个带外键（三种形态都改，才落到声明那一侧）
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

# 3) 持有者消失而声明还在
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
grep -q "声明 Deployment/meilisearch 读 cogneva-secrets/meili-master-key，渲染产物里找不到这个读者" \
  <<<"${out}" || fail "没点名那条陈旧的声明：${out}"

# 4) 声明里多了一个树上不存在的读者
mirror
edit "${work}/root/deploy/helm/cogneva/templates/secret.yaml" \
  "# readers: Deployment/cogneva — 主应用会话签名，只给主应用" \
  "# readers: Deployment/cogneva, Deployment/ghost — 主应用会话签名，只给主应用"
out="$(run_gate)" && fail "声明一个不存在的读者时应当红，实际绿：${out}"
grep -q "声明 Deployment/ghost 读 cogneva-secrets/jwt-secret" <<<"${out}" \
  || fail "没点名那个漂移的读者：${out}"

# 5) 写者声明的键头上没有 readers 行
mirror
edit "${work}/root/${secret_tpl}" \
  "  # readers: Deployment/cogneva-security-gateway — 只此一处读；放带外份是为了不让持内部份的每个业务 Pod 都能整体 get
" ""
out="$(run_gate)" && fail "键头上没有读者声明时应当红，实际绿：${out}"
grep -q "clickhouse-password 上方没有 \`# readers:\` 行" <<<"${out}" \
  || fail "没点名漏声明的键：${out}"

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
rm -f "${work}/root/${SECRETS_ROLE_BINDING}"
out="$(run_gate)" && fail "授权没有持有者时应当红，实际绿：${out}"
grep -q "Role/cogneva-security-gateway 授了 Secret 的 read,write，却没有 RoleBinding" <<<"${out}" \
  || fail "没报出谁也用不到的授权：${out}"

# 8) 整对象权限绑到了网关以外的身份：等于把「代持带外凭证」复制一份
mirror
# 三种形态都改：只改一种会先红在「形态之间持有面不一致」上，那是另一条判据。
for prof in "${PROFILES[@]}"; do
  edit "${work}/root/deploy/rendered/${prof}/17-rolebinding-cogneva-security-gateway.yaml" \
    "subjects:
  - kind: ServiceAccount
    name: cogneva-security-gateway" \
    "subjects:
  - kind: ServiceAccount
    name: cogneva-evolution"
done
out="$(run_gate)" && fail "整对象权限给了别的身份时应当红，实际绿：${out}"
grep -q "ServiceAccount/cogneva-evolution 对 cogneva-secrets-oob 有整对象权限" <<<"${out}" \
  || fail "没报出越界的整对象授权：${out}"

# 9) api 路径的代码指针锈了（文件不在）
mirror
edit "${work}/root/${secret_tpl}" \
  "(api: crates/cog-gateway/src/git_identity.rs)" \
  "(api: crates/cog-gateway/src/renamed_away.rs)"
out="$(run_gate)" && fail "代码指针指向不存在的文件时应当红，实际绿：${out}"
grep -q "的 API 读点 'crates/cog-gateway/src/renamed_away.rs' 在树上不存在" <<<"${out}" \
  || fail "没报出锈掉的指针：${out}"

# 10) 指针的文件还在，但那个文件已经不读这个键了
mirror
edit "${work}/root/${secret_tpl}" \
  "(api: crates/cog-gateway/src/git_identity.rs)" \
  "(api: crates/cog-gateway/src/contribution_admin.rs)"
out="$(run_gate)" && fail "指针指向的文件不再提这个键时应当红，实际绿：${out}"
grep -q "的 API 读点 'crates/cog-gateway/src/contribution_admin.rs' 里没有出现这个键名" <<<"${out}" \
  || fail "没报出锈成空指针的读点：${out}"

echo "OK: credential-holder-surface 门禁定向测试通过（1 绿 + 10 红，各点名一处）"
