#!/usr/bin/env bash
# principle: feedback_a_gate_that_only_reads_one_side_cannot_see_a_stale_row
# check-host-credential-surface.sh 的定向测试。它判的是「宿主上的 systemd 单元
# 碰得到哪个凭证」，而这个面出错时**运行时没有症状**：单元照常起、定时器照常跑，
# 只是它从此握着一把没人登记过的钥匙，或者那把钥匙其实写在了一个 0644 的单元
# 文件里——任何一次 `cat` 都能读到。
#
# 每个用例都在真树的一份副本上只改一处，然后断言它红的正是那一处。
#
# 用例：
#   1. 真树 -> 绿，且报出登记条数与单元数（不是空跑）；
#   2. 单元里多一条没登记的 LoadCredential -> 红，点名单元与名字；
#   3. 登记表里有一行单元里已经没有了 -> 红，点名那条陈旧登记；
#   4. 单元里用 Environment= 写 RESTIC_PASSWORD 字面量 -> 红；
#   5. 渲染物里出现 RESTIC_PASSWORD -> 红（业务 Pod 零凭证的边界）；
#   6. 安装脚本加密的名字与单元要的名字对不上 -> 红；
#   7. ExecStart 指向的脚本不在树里 -> 红（指针锈了）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-host-credential-surface.sh"
registry="deploy/systemd/credential-holders.tsv"
unit="deploy/systemd/cogneva-backup-offsite.service"
installer="deploy/scripts/install-backup-offsite.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

mirror() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy/scripts"
  cp -r "${repo}/deploy/systemd" "${work}/root/deploy/systemd"
  cp "${repo}/${installer}" "${work}/root/${installer}"
  cp "${repo}/deploy/scripts/backup-offsite-restic.sh" "${work}/root/deploy/scripts/"
  # 门禁要扫渲染物/chart/k3s 里有没有 RESTIC_PASSWORD。副本里摆一棵小树就够——
  # 这些检查只关心"有没有出现"，不解析内容。
  mkdir -p "${work}/root/deploy/rendered" "${work}/root/deploy/helm" "${work}/root/deploy/k3s"
  cp -r "${repo}/deploy/rendered/." "${work}/root/deploy/rendered/" 2>/dev/null || true
}
mirror

run_gate() { bash "${gate}" "${work}/root" 2>&1; }

# 1) 真树绿，报数是真读数
out="$(run_gate)" || fail "真树应当绿，实际红：${out}"
grep -q "HOST CREDENTIAL SURFACE OK" <<<"${out}" || fail "绿时没有报出判词：${out}"
rows="$(grep -v '^#' "${repo}/${registry}" | grep -v '^unit' | grep -c .)"
grep -qE "${rows} 条登记" <<<"${out}" || fail "绿时没报出登记条数（表里 ${rows} 行）：${out}"

# 2) 单元多一条没登记的凭证
mirror
printf 'LoadCredential=llm-key:/etc/cogneva/credentials/llm-key\n' >> "${work}/root/${unit}"
out="$(run_gate)" && fail "多出一条没登记的 LoadCredential 应当红，实际绿：${out}"
grep -q "llm-key" <<<"${out}" || fail "红时没有点名那个凭证：${out}"

# 3) 登记还在、单元里已经没有了
mirror
sed -i '/LoadCredentialEncrypted=aws-secret-access-key/d' "${work}/root/${unit}"
out="$(run_gate)" && fail "登记陈旧应当红，实际绿：${out}"
grep -q "aws-secret-access-key" <<<"${out}" || fail "红时没有点名那条陈旧登记：${out}"

# 4) 单元里写字面量密钥
mirror
printf 'Environment=RESTIC_PASSWORD=hunter2\n' >> "${work}/root/${unit}"
out="$(run_gate)" && fail "单元里写字面量密钥应当红，实际绿：${out}"
grep -q "字面量密钥" <<<"${out}" || fail "红时没有说清是字面量密钥：${out}"

# 5) 渲染物里出现 RESTIC_PASSWORD
mirror
echo '        env: [{name: RESTIC_PASSWORD, value: x}]' > "${work}/root/deploy/rendered/99-leak.yaml"
out="$(run_gate)" && fail "渲染物里出现 RESTIC_PASSWORD 应当红，实际绿：${out}"
grep -q "99-leak.yaml" <<<"${out}" || fail "红时没有点名那个渲染物：${out}"

# 6) 加密名字与会话名字对不上
mirror
sed -i 's/^for name in restic-password aws-access-key-id aws-secret-access-key; do/for name in restic-password aws-access-key-id aws-secret-access-key old-name; do/' \
    "${work}/root/${installer}"
out="$(run_gate)" && fail "安装脚本加密了单元不要的名字应当红，实际绿：${out}"
grep -q "old-name" <<<"${out}" || fail "红时没有点名那个名字：${out}"

# 7) ExecStart 指向不存在的脚本
mirror
sed -i 's#^ExecStart=/usr/local/lib/cogneva/backup-offsite-restic.sh#ExecStart=/usr/local/lib/cogneva/backup-offsite-missing.sh#' \
    "${work}/root/${unit}"
out="$(run_gate)" && fail "ExecStart 指针锈了应当红，实际绿：${out}"
grep -q "backup-offsite-missing.sh" <<<"${out}" || fail "红时没有点名那个指针：${out}"

echo "host-credential-surface: 7 个用例全绿"
