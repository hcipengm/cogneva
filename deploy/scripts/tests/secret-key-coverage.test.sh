#!/usr/bin/env bash
# principle: feedback_a_gate_must_name_the_defect_it_stops
# principle: feedback_a_gate_whose_members_are_named_by_hand_must_scan_for_unlisted_members
# `check-secret-key-coverage.sh` 的八个格子：树里两面、现网一面，加一个反例。
#
# 反例是 2026-10-30 的现场：清单读 `app-password`，`init-secrets.sh` 不声明它，现网
# Secret 里也没有——三者本来是全绿的，因为没有任何东西把这三处放在一起比。这条测试
# 把那一天的形状固定下来：那个形状必须红，而且要红在原话上。
#
# 每个格子都自建一棵假树（`mktemp -d`），真仓库只在第一个格子里被读一次。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-secret-key-coverage.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "读数脚本不在树里：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 1. 真树：静态腿必须绿。它绿说明清单与生成侧当下的声明是对得上的。
out="$(bash "${gate}" 2>&1)" || fail "真树的静态腿是红的：${out}"
grep -q '^SECRET-KEYS: 绿' <<<"${out}" || fail "真树没报绿：${out}"

# 假树：一份 init-secrets.sh 加一份渲染产物。
tree() { # <目录名> <渲染产物正文>
    local d="${work}/$1"
    mkdir -p "${d}/deploy/scripts" "${d}/deploy/rendered/k3s-single"
    cp "${repo}/deploy/scripts/init-secrets.sh" "${d}/deploy/scripts/init-secrets.sh"
    printf '%s\n' "$2" >"${d}/deploy/rendered/k3s-single/41-deployment-x.yaml"
    printf '%s' "${d}"
}

env_with() { # <键> [optional]
    cat <<EOF
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: x
          env:
            - name: APP_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: cogneva-secrets
                  key: $1
EOF
    [ "${2:-}" = optional ] && printf '                  optional: true\n'
    return 0
}

# 2. 反例（2026-10-30 的形状）：清单要 app-password，生成侧不声明它 → 红并点名。
d="$(tree missing "$(env_with app-password)")"
grep -v 'ensure_random app-password' "${d}/deploy/scripts/init-secrets.sh" >"${d}/init.tmp"
mv "${d}/init.tmp" "${d}/deploy/scripts/init-secrets.sh"
if out="$(bash "${gate}" "${d}" 2>&1)"; then
    fail "清单要一个生成侧不声明的键，门却绿了：${out}"
fi
grep -q 'app-password' <<<"${out}" || fail "红了但没点名 app-password：${out}"
grep -q 'init-secrets.sh 不声明它' <<<"${out}" || fail "红的原因不对：${out}"

# 3. 同一个键标成 optional：缺了是功能降级，不是这件事 → 绿。
d="$(tree optional "$(env_with app-password optional)")"
grep -v 'ensure_random app-password' "${d}/deploy/scripts/init-secrets.sh" >"${d}/init.tmp"
mv "${d}/init.tmp" "${d}/deploy/scripts/init-secrets.sh"
out="$(bash "${gate}" "${d}" 2>&1)" || fail "optional 的键缺了也判红：${out}"

# 4. 单行 flow 写法：读不出 name/key → 红（认不出来就是失败，不是跳过）。
d="$(tree flow 'spec:
  template:
    spec:
      containers:
        - name: x
          env:
            - name: APP_PASSWORD
              valueFrom:
                secretKeyRef: {name: cogneva-secrets, key: app-password}')"
if out="$(bash "${gate}" "${d}" 2>&1)"; then
    fail "认不出的写法被当成没有引用：${out}"
fi
grep -q '认不出' <<<"${out}" || fail "红的原因不对：${out}"

# 5. 引用一个生成侧根本没有的 Secret → 红。
d="$(tree other-secret 'spec:
  template:
    spec:
      containers:
        - name: x
          env:
            - name: X
              valueFrom:
                secretKeyRef:
                  name: someone-elses-secret
                  key: token')"
if out="$(bash "${gate}" "${d}" 2>&1)"; then
    fail "引用了一个没人建的 Secret 却绿了：${out}"
fi
grep -q 'someone-elses-secret' <<<"${out}" || fail "红了但没点名那个 Secret：${out}"

# 6. 没有渲染产物：判不了，退出码 2（不是绿）。
d="${work}/empty"
mkdir -p "${d}/deploy/scripts"
cp "${repo}/deploy/scripts/init-secrets.sh" "${d}/deploy/scripts/init-secrets.sh"
set +e
bash "${gate}" "${d}" >/dev/null 2>&1
rc=$?
set -e
[ "${rc}" = 2 ] || fail "没有渲染产物时报了 ${rc}，应当是 2（判不了）"

# 7、8. 现网腿：打桩 kubectl，键齐全时绿；按 2026-10-30 那天现网的键集合时红。
stub="${work}/bin"
mkdir -p "${stub}" "${work}/keys"
cat >"${stub}/kubectl" <<EOF
#!/usr/bin/env bash
prev=""
name=""
for a in "\$@"; do
    [ "\${prev}" = secret ] && name="\$a"
    prev="\$a"
done
[ -f "${work}/keys/\${name}.keys" ] || exit 1
cat "${work}/keys/\${name}.keys"
EOF
chmod +x "${stub}/kubectl"

d="$(tree live "$(env_with app-password)")"
printf '%s\n' app-password pg-password >"${work}/keys/cogneva-secrets.keys"
out="$(PATH="${stub}:${PATH}" bash "${gate}" "${d}" --live 2>&1)" || fail "现网键齐全时判红：${out}"
grep -q '现网 Secret 已核对' <<<"${out}" || fail "现网腿没跑：${out}"

# 那天现网的 cogneva-secrets 里没有 app-password，而清单已经开始读它。
printf '%s\n' pg-password redis-password >"${work}/keys/cogneva-secrets.keys"
if out="$(PATH="${stub}:${PATH}" bash "${gate}" "${d}" --live 2>&1)"; then
    fail "现网缺必需键却绿了——那天的故障会照原样再发生一次：${out}"
fi
grep -q '现网 cogneva-secrets 缺少必需键 app-password' <<<"${out}" \
    || fail "红的原因不对：${out}"

echo "secret-key-coverage: 8 组用例通过（含 2026-10-30 的反例）"
