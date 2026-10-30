#!/usr/bin/env bash
# principle: feedback_a_gate_must_name_the_defect_it_stops
# 单元里的 `Environment=` 赋值：值里带空格就必须加引号。判的是这一条形状，不是某个
# 具体变量的名字——名单从树里来（静态单元 ＋ 每个安装脚本的只渲染产物）。
#
# 为什么要有这一条：systemd 按空格切 `Environment=` 的赋值，不加引号的
#   Environment=COGNEVA_KUBECTL=sudo -n k3s kubectl
# 会被拆成四个赋值，其中三个是 `Invalid environment assignment, ignoring`，第一个是
# 空的 COGNEVA_KUBECTL。单元照样启动、退出码照样是 0——错的是**读到了什么**：2026-10-31
# 那一拍报的是「现网读不到 cogneva-secrets」，而集群好好的。这条读数差了半分钟就被
# 当成集群故障看，靠的是回去翻 journal 才看见那三行 ignoring。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 带空格且没加引号的环境赋值，逐行点名。
scan() { # <文件> -> 有问题的行打到 stdout
    awk '
        /^[[:space:]]*Environment=/ {
            line = $0
            sub(/^[[:space:]]*Environment=/, "", line)
            if (line ~ /^["\047]/) next          # 已经带引号
            if (line ~ /[[:space:]]/) printf "%s:%d: %s\n", FILENAME, FNR, $0
        }
    ' "$1"
}

units=()
for f in "${repo}"/deploy/systemd/*.service; do
    [ -e "${f}" ] || continue
    units+=("${f}")
done

# 每个安装脚本的只渲染产物也是单元，一起判：那里才是新写单元的地方。
installers=0
for script in "${repo}"/deploy/scripts/install-*.sh; do
    [ -e "${script}" ] || continue
    grep -q 'COGNEVA_INSTALL_RENDER_ONLY' "${script}" || continue
    installers=$((installers + 1))
    out="${work}/$(basename "${script}" .sh)"
    XDG_CONFIG_HOME="${out}" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${script}" >/dev/null \
        || fail "$(basename "${script}") 只渲染失败"
    while IFS= read -r f; do units+=("${f}"); done \
        < <(shopt -s nullglob; printf '%s\n' "${out}"/systemd/user/*.service)
done
[ "${#units[@]}" -gt 0 ] || fail "一个单元都没收集到：枚举本身坏了，这条门会一直绿"

problems=""
for f in "${units[@]}"; do
    [ -e "${f}" ] || continue
    found="$(scan "${f}")"
    [ -z "${found}" ] || problems+="${found}"$'\n'
done
[ -z "${problems}" ] || fail "带空格却没加引号的 Environment= 赋值（systemd 会切成多个赋值并静默忽略）："$'\n'"${problems}"

# 反例：就是 2026-10-31 那一行。它必须被这条门抓到，否则上面那些行绿着也没有意义。
bad="${work}/bad.service"
cat >"${bad}" <<'EOF'
[Service]
Environment=COGNEVA_KUBECTL=sudo -n k3s kubectl
EOF
found="$(scan "${bad}")"
[ -n "${found}" ] || fail "反例没被这条门抓到：它会把「读不到集群」当成集群故障"
grep -q 'COGNEVA_KUBECTL=sudo -n k3s kubectl' <<<"${found}" || fail "反例被抓到了，但报的不是那一行：${found}"

echo "systemd-environment-quoting: ${#units[@]} 个单元通过（${installers} 个安装脚本的渲染产物在内，反例为 2026-10-31 那一行）"
