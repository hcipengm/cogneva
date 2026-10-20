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
#   7. ExecStart 指向的脚本不在树里 -> 红（指针锈了）；
#   8a. --live 且单元没装、通道也从没配过 -> 不适用，绿（不判成违约）；
#   8b. --live 且单元没装、但通道配过 -> 红并点名，不许崩出 traceback（登记表加列时真的崩过）；
#   9. --live 且凭证源文件不在 -> 红并点名那个路径；
#   10. --live 的绿路：单元与登记表一致、权限一致 -> 绿并报出 --live。
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
run_gate_live() { bash "${gate}" "${work}/root" --live 2>&1; }

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

# 8a) --live：单元没装、通道也从没配过（标记文件不在）⇒ 不适用，绿
#     这一格挡住的是把「没开异地副本」读成凭证面违约：装了之后恒红，红久了没人看。
mirror
sed -i "s#/etc/cogneva/backup-offsite.env#${work}/never/backup-offsite.env#g" \
    "${work}/root/${registry}" "${work}/root/${unit}"
mkdir -p "${work}/bin-none"
# `--version` 要答得出来：那一步证明 systemctl 这一侧问得出来（8c 的对照）。
printf '#!/usr/bin/env bash\ncase "$1" in --version) echo "systemd 255"; exit 0;; esac\nexit 1\n' \
    > "${work}/bin-none/systemctl"
chmod +x "${work}/bin-none/systemctl"
out="$(PATH="${work}/bin-none:${PATH}" run_gate_live)" || fail "--live 在通道没配过时应当绿（不适用不是失败）：${out}"
grep -q "Traceback" <<<"${out}" && fail "--live 崩出 traceback 而不是判词：${out}"
grep -q "不适用" <<<"${out}" || fail "--live 没有把未配置的通道读成不适用：${out}"

# 8b) --live：单元没装、但通道配过（标记文件在）⇒ 红并点名，而不是一段 traceback
#     这一格是回归：登记表从四列长到七列时，--live 按位置解包会抛 ValueError，
#     门退出 1 但一个字也没判——看起来像红，实则崩了。判据是「有没有点名」。
mirror
mkdir -p "${work}/creds"
: > "${work}/creds/backup-offsite.env"
sed -i "s#/etc/cogneva/backup-offsite.env#${work}/creds/backup-offsite.env#g" \
    "${work}/root/${registry}" "${work}/root/${unit}"
mkdir -p "${work}/bin-none"
printf '#!/usr/bin/env bash\ncase "$1" in --version) echo "systemd 255"; exit 0;; esac\nexit 1\n' \
    > "${work}/bin-none/systemctl"
chmod +x "${work}/bin-none/systemctl"
out="$(PATH="${work}/bin-none:${PATH}" run_gate_live)" && fail "--live 在通道配过而单元没了时应当红：${out}"
grep -q "Traceback" <<<"${out}" && fail "--live 崩出 traceback 而不是判词：${out}"
grep -q "通道配过、单元却没了" <<<"${out}" || fail "--live 红时没有点名配过却缺装的单元：${out}"
grep -q "timer 没有装" <<<"${out}" || fail "--live 没有判定时器缺失：${out}"

# 9) --live：单元装了、但凭证源文件不在 ⇒ 红并点名那个路径（不崩）
mirror
mkdir -p "${work}/bin-part"
cat > "${work}/bin-part/systemctl" <<SHIM
#!/usr/bin/env bash
case "\$1" in
  --version) echo "systemd 255" ;;
  cat) cat "${work}/root/deploy/systemd/\$2" ;;
  list-unit-files) echo "cogneva-backup-offsite.timer enabled enabled" ;;
  *) exit 1 ;;
esac
SHIM
chmod +x "${work}/bin-part/systemctl"
out="$(PATH="${work}/bin-part:${PATH}" run_gate_live)" && fail "--live 在源文件缺失时应当红：${out}"
grep -q "Traceback" <<<"${out}" && fail "--live 崩出 traceback 而不是判词：${out}"
grep -q "不存在" <<<"${out}" || fail "--live 没有点名缺失的凭证源：${out}"

# 10) --live 的绿路：单元内容与登记表一致、源文件权限一致 ⇒ 绿并报 --live
#     没有这一格，「--live 永远红」与「--live 判对了」同形。
mirror
mkdir -p "${work}/creds" "${work}/bin-ok"
for name in restic-password.cred aws-access-key-id.cred aws-secret-access-key.cred; do
    : > "${work}/creds/${name}"; chmod 0400 "${work}/creds/${name}"
done
: > "${work}/creds/backup-offsite.env"; chmod 0644 "${work}/creds/backup-offsite.env"
sed -i "s#/etc/cogneva/credentials#${work}/creds#g; s#/etc/cogneva/backup-offsite.env#${work}/creds/backup-offsite.env#g" \
    "${work}/root/${registry}" "${work}/root/${unit}"
cat > "${work}/bin-ok/systemctl" <<SHIM
#!/usr/bin/env bash
case "\$1" in
  --version) echo "systemd 255" ;;
  cat) cat "${work}/root/deploy/systemd/\$2" ;;
  list-unit-files) echo "cogneva-backup-offsite.timer enabled enabled" ;;
  *) exit 1 ;;
esac
SHIM
chmod +x "${work}/bin-ok/systemctl"
out="$(PATH="${work}/bin-ok:${PATH}" run_gate_live)" || fail "--live 绿路应当通过，实际红：${out}"
grep -q -- "--live 已核" <<<"${out}" || fail "--live 绿时没有报出已核：${out}"

# 11) --live 的「读不到」：systemctl 自己跑不起来 ⇒ 3，且**不说**「单元没装」
#     这一格挡的是把「不知道」印成「没有」：两者的修法相反（修 systemd／装单元），
#     而在此以前它们逐字同形（都是 `systemctl cat` 非零）。3 与 1 也必须分开：
#     1 是「去改」，3 是「这一跑什么都没判」。
mirror
mkdir -p "${work}/bin-broken"
printf '#!/usr/bin/env bash\nexit 1\n' > "${work}/bin-broken/systemctl"
chmod +x "${work}/bin-broken/systemctl"
rc=0
out="$(PATH="${work}/bin-broken:${PATH}" run_gate_live)" || rc=$?
[ "$rc" = 3 ] || fail "--live 在 systemctl 跑不起来时应当退 3（这一跑什么都没判），实得 ${rc}：${out}"
grep -q "读不到" <<<"${out}" || fail "--live 没有把读不到说成因：${out}"
grep -q "没有装在宿主上" <<<"${out}" && fail "--live 把「读不到」印成了「单元没装」：${out}"
grep -q "Traceback" <<<"${out}" && fail "--live 崩出 traceback 而不是判词：${out}"
true

echo "host-credential-surface: 12 个用例全绿"
