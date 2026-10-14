#!/usr/bin/env bash
# check-git-identity-wiring.sh 的定向测试：它判的两件事都是**声明**——谁是自举循环
# 的宿主（能不能读写那两个 Secret 对象）、哪一侧拿着基线仓库名。声明错了在渲染期
# 没有症状：清单合法、parity 也绿、Pod 照常起来，只是自动登记那一支永远不触发
# （状态停在 pending 等人）或两边各登记一遍。
#
# 为什么要有这份测试：门禁的判据面是**反查**出来的（从 Role 的 verbs 反推谁写得动
# Secret，再拿 SA 去认工作负载），反查写漏一环就退化成"什么都查不出来还报绿"。
# 刚才那次扩展新加的三条判据（归属声明取值合法 / 恰好一处置真 / 置真者真写得动）
# 尤其如此：它们全落在清单里的一行 env 上，写错就静默读成 false。
#
# 每个用例都在**真树的一份副本**上只改一处，然后断言它红的正是那一处。
#
# 用例：
#   1. 真树 -> 绿，并报出基线仓库取值与两侧宿主（不是空跑：宿主非空）；
#   2. Secret 写权的宿主没带基线仓库 env -> 红，点名那个工作负载；
#   3. 两侧取值不同 -> 红（同一份清单只能是同一个仓库）；
#   4. 归属声明多写了一处（主应用也置真）-> 红，报出置真的个数；
#   5. 归属声明的取值不是 Rust 侧认的真值（写 yes）-> 红，点名字面量；
#   6. 归属声明落在没有 Secret 写权的 SA 上 -> 红（声明说真、授权没跟上）；
#   7. 归属声明一处都没有 -> 红（没有宿主＝循环无处落脚）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-git-identity-wiring.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 基线是**真树**的渲染产物：门禁只读 deploy/rendered，复制别的目录只会让
# 「只改了一处」这件事变得难以相信。
mkdir -p "${work}/root/deploy"
cp -r "${repo}/deploy/rendered" "${work}/root/deploy/rendered"

run_gate() { bash "${gate}" "${work}/root" 2>&1; }

reset_tree() {
  rm -rf "${work}/root/deploy/rendered"
  cp -r "${repo}/deploy/rendered" "${work}/root/deploy/rendered"
}

APP_DEPLOY="deploy/rendered/k3s-single/41-deployment-cogneva.yaml"
GW_DEPLOY="deploy/rendered/k3s-single/41-deployment-cogneva-security-gateway.yaml"
GW_ROLE="deploy/rendered/k3s-single/16-role-cogneva-security-gateway.yaml"
MULTI_APP="deploy/rendered/k3s-multi/41-deployment-cogneva.yaml"

# 1) 真树绿，且报出的读数非空
out="$(run_gate)" || fail "真树应当绿，实际红：${out}"
grep -q "GIT IDENTITY WIRING OK" <<<"${out}" || fail "绿时没有报出判词：${out}"
grep -q "Deployment/cogneva," <<<"${out}" || fail "绿时没报出自举循环的宿主（主应用侧）：${out}"
grep -q "Deployment/cogneva-security-gateway" <<<"${out}" \
  || fail "绿时没报出基线的另一侧（安全网关）：${out}"
value="$(sed -n "s/.*基线仓库 '\([^']*\)'.*/\1/p" <<<"${out}")"
[ -n "${value}" ] || fail "绿时没报出基线仓库取值：${out}"

# 2) 宿主丢了基线仓库 env
python3 - "${work}/root/${MULTI_APP}" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
out, skip = [], False
for line in lines:
    if line.strip() == "- name: COGNEVA_GATEWAY_GIT_IDENTITY_REPO":
        skip = True
        continue
    if skip and line.strip().startswith("value:"):
        skip = False
        continue
    out.append(line)
open(p, "w").write("\n".join(out))
PY
out="$(run_gate)" && fail "宿主没带基线仓库时应当红，实际绿：${out}"
grep -q "Deployment/cogneva" <<<"${out}" || fail "没点名缺 env 的工作负载：${out}"
grep -q "COGNEVA_GATEWAY_GIT_IDENTITY_REPO" <<<"${out}" || fail "没点名缺的是哪个 env：${out}"

# 3) 两侧取值不同
reset_tree
python3 - "${work}/root/${GW_DEPLOY}" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read().replace('value: "hcipengm/cogneva"', 'value: "someone-else/other"')
open(p, "w").write(s)
PY
out="$(run_gate)" && fail "两侧取值不同时应当红，实际绿：${out}"
grep -q "出现了多个取值" <<<"${out}" || fail "没报出取值冲突：${out}"

# 4) 归属声明多写了一处
reset_tree
python3 - "${work}/root/${APP_DEPLOY}" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
for i, line in enumerate(lines):
    if line.strip() == 'value: "hcipengm/cogneva"':
        lines[i + 1:i + 1] = ['            - name: COGNEVA_CREDENTIAL_LOOP_OWNER',
                              '              value: "true"']
        break
else:
    raise SystemExit("副本里没有基线仓库 env，用例失效")
open(p, "w").write("\n".join(lines))
PY
out="$(run_gate)" && fail "两个属主时应当红，实际绿：${out}"
grep -q "有 2 个" <<<"${out}" || fail "没报出属主个数：${out}"

# 5) 取值不是 Rust 侧认的真值
reset_tree
python3 - "${work}/root/${GW_DEPLOY}" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read().replace(
    'COGNEVA_CREDENTIAL_LOOP_OWNER\n              value: "true"',
    'COGNEVA_CREDENTIAL_LOOP_OWNER\n              value: "yes"')
assert 'value: "yes"' in s
open(p, "w").write(s)
PY
out="$(run_gate)" && fail "归属声明写成非真值时应当红，实际绿：${out}"
grep -q "'yes'" <<<"${out}" || fail "没点名那个字面量：${out}"

# 6) 声明为属主的 SA 其实写不动 Secret
reset_tree
python3 - "${work}/root/${GW_ROLE}" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
out, drop = [], 0
for line in lines:
    if '- apiGroups: [""]' in line:
        drop = 4  # 这一条规则的四行
    if drop:
        drop -= 1
        continue
    out.append(line)
open(p, "w").write("\n".join(out))
PY
out="$(run_gate)" && fail "属主没有 Secret 写权时应当红，实际绿：${out}"
grep -q "不在持有 Secret 写权的 SA 集合" <<<"${out}" || fail "没报出声明与授权对不上：${out}"

# 7) 一个属主都没有
reset_tree
python3 - "${work}/root/${GW_DEPLOY}" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().split("\n")
out, skip = [], False
for line in lines:
    if line.strip() == "- name: COGNEVA_CREDENTIAL_LOOP_OWNER":
        skip = True
        continue
    if skip and line.strip().startswith("value:"):
        skip = False
        continue
    out.append(line)
open(p, "w").write("\n".join(out))
PY
out="$(run_gate)" && fail "没有属主时应当红，实际绿：${out}"
grep -q "有 0 个" <<<"${out}" || fail "没报出属主个数为零：${out}"

echo "OK: git-identity-wiring 门禁定向测试通过（1 绿 + 6 红，各点名一处）"
