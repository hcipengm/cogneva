#!/usr/bin/env bash
# principle: feedback_a_hand_kept_build_list_ships_an_image_missing_servers
# principle: feedback_a_gate_whose_members_are_named_by_hand_must_scan_for_unlisted_members
# 「装哪几个宿主单元」这件事，成员必须从树里来——判的是枚举本身，不是那四个名字。
#
# 为什么要有这一条：装漏一个单元的症状是那一面永远没有读数，而 `systemctl --failed`
# 什么都不会说（它没被装上，没有失败可报）。所以成员由 `deploy/scripts/install-*.sh`
# 的 glob 决定，每个脚本在自己头部声明 `# host-install: user|root`；缺声明、写两行、
# 写错值都必须当场红，而不是被安静地跳过。
#
# 跑法：bash deploy/scripts/tests/install-host-units.test.sh
# CI 的 deploy/scripts/tests/*.test.sh 通配会带上它；全程只渲染，不装任何东西。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
scripts_dir="${repo}/deploy/scripts"
agg="${scripts_dir}/install-host-units.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${agg}" ] || fail "聚合安装脚本不存在：${agg}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# systemctl 打桩：这条命令在任何模式下都不该碰这台机器的 systemd。
stub="${work}/bin"
mkdir -p "${stub}"
cat >"${stub}/systemctl" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"${work}/systemctl.calls"
EOF
chmod +x "${stub}/systemctl"

members() { # 树里真实的成员：除聚合脚本自己以外的全部 install-*.sh
    local f n=0
    for f in "${scripts_dir}"/install-*.sh; do
        [ -e "${f}" ] || continue
        [ "$(basename "${f}")" = "install-host-units.sh" ] && continue
        n=$((n + 1))
    done
    printf '%s' "${n}"
}

# 1. 每个安装脚本都要有一行可读的声明。没有它，聚合命令就不知道该怎么处理这一份——
#    而「不知道怎么处理」在这条命令里的默认结局是不装。
bad=""
for f in "${scripts_dir}"/install-*.sh; do
    [ -e "${f}" ] || continue
    [ "$(basename "${f}")" = "install-host-units.sh" ] && continue
    n="$(grep -c '^# host-install:' "${f}" || true)"
    v="$(sed -n 's/^# host-install:[[:space:]]*//p' "${f}" | head -1)"
    case "${v}" in user|root) ;; *) bad+="$(basename "${f}"):值 ${v:-空}；" ;; esac
    [ "${n}" = 1 ] || bad+="$(basename "${f}"):${n} 行；"
done
[ -z "${bad}" ] || fail "声明行不合规：${bad}"

# 2. --list 报出来的成员数必须等于 glob 的成员数。不等就等于有一份手抄的名单。
out="$(PATH="${stub}:${PATH}" bash "${agg}" --list)"
listed="$(printf '%s\n' "${out}" | grep -cE '^(install|print)  ' || true)"
[ "${listed}" = "$(members)" ] \
    || fail "--list 报了 ${listed} 个，树里有 $(members) 个：枚举漏人或混进了自己"

# 3. 新加一个安装脚本，它必须自动出现在名单里——这条判的是「没有手抄名单」。
fixture="${work}/scripts"
mkdir -p "${fixture}"
cp "${scripts_dir}"/install-*.sh "${fixture}/"
cp "${scripts_dir}/install-host-units.sh" "${fixture}/install-host-units.sh"
printf '#!/usr/bin/env bash\n# host-install: user\nset -euo pipefail\ntrue\n' >"${fixture}/install-brand-new-reading.sh"
out="$(COGNEVA_INSTALL_SCRIPTS_DIR="${fixture}" bash "${agg}" --list)" \
    || fail "加了新脚本之后 --list 跑不通：${out}"
grep -q 'install-brand-new-reading.sh' <<<"${out}" \
    || fail "新安装脚本没有被枚举进来（名单是手抄的）：${out}"

# 4. 缺声明、写两行、写错值：三种都必须退出码 2 并点名那个文件。
broken() { # <断言名> <脚本正文>
    rm -rf "${fixture}"
    mkdir -p "${fixture}"
    cp "${scripts_dir}"/install-*.sh "${fixture}/"
    printf '#!/usr/bin/env bash\n%b\nset -euo pipefail\ntrue\n' "$2" >"${fixture}/install-broken.sh"
    if out="$(COGNEVA_INSTALL_SCRIPTS_DIR="${fixture}" bash "${agg}" --list 2>&1)"; then
        fail "$1 仍然绿：${out}"
    fi
    grep -q 'install-broken.sh' <<<"${out}" || fail "$1 红了但没点名那个文件：${out}"
}
broken "没有声明行" ''
broken "声明行写了两行" '# host-install: user\n# host-install: root\n'
broken "声明值不认识" '# host-install: maybe\n'

# 5. 全部只渲染一遍：聚合跑出来的单元集合必须等于逐个跑出来的集合。聚合少跑一个，
#    那一份就永远不会被装上，而它的读数也不会报「我缺了」。
one="${work}/one"
mkdir -p "${one}"
for f in "${scripts_dir}"/install-*.sh; do
    [ -e "${f}" ] || continue
    [ "$(basename "${f}")" = "install-host-units.sh" ] && continue
    name="$(basename "${f}" .sh)"
    XDG_CONFIG_HOME="${one}/${name}" COGNEVA_INSTALL_RENDER_ONLY=1 bash "${f}" >/dev/null \
        || fail "${name} 单独只渲染就失败了"
done
all="${work}/all"
PATH="${stub}:${PATH}" XDG_CONFIG_HOME="${all}" COGNEVA_INSTALL_RENDER_ONLY=1 \
    bash "${agg}" >/dev/null || fail "聚合只渲染跑不通"
one_set="$(cd "${one}" && find . -name '*.timer' -o -name '*.service' | sed 's|^\./[^/]*/||' | LC_ALL=C sort)"
all_set="$(cd "${all}" && find . -name '*.timer' -o -name '*.service' | sed 's|^\./||' | LC_ALL=C sort)"
[ -n "${one_set}" ] || fail "逐个只渲染一个单元都没写出来"
# 聚合脚本自己也是一个载体（它把自己装成 timer），所以它的产物比成员多两件。
self_set="$(printf '%s\n%s\n' 'systemd/user/cogneva-host-units.service' 'systemd/user/cogneva-host-units.timer')"
[ "${all_set}" = "$(printf '%s\n%s' "${one_set}" "${self_set}" | LC_ALL=C sort)" ] \
    || fail "聚合跑的单元集合与「逐个跑 ＋ 它自己」不一致：$(printf '\n逐个＋自己：%s\n聚合：%s' \
        "$(printf '%s\n%s' "${one_set}" "${self_set}" | LC_ALL=C sort)" "${all_set}")"

# 5b. 它装出来的 timer 必须是「过一会儿再来一遍」那一件事的载体：ExecStart 指向它自己，
#     排期存在。少任何一条，宿主与树的漂移就又要靠人回来重跑。
grep -q "^ExecStart=.*install-host-units.sh" "${all}/systemd/user/cogneva-host-units.service" \
    || fail "自装的服务没有指向本命令：$(cat "${all}/systemd/user/cogneva-host-units.service")"
grep -q '^OnCalendar=' "${all}/systemd/user/cogneva-host-units.timer" \
    || fail "自装的 timer 没有排期"

# 6. 全程没有碰过 systemd。
[ ! -s "${work}/systemctl.calls" ] \
    || fail "只渲染模式下仍然调了 systemctl：$(cat "${work}/systemctl.calls")"

echo "install-host-units: 6 组用例通过（$(members) 个安装脚本从树里枚举）"
