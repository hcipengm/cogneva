#!/usr/bin/env bash
# 可观测性栈安装脚本的确定性判据：安装期生成的凭证只允许经 --from-file 进 kubectl。
#
# 为什么需要它：`--from-literal=password=xxx` 与 `kubectl patch -p '{"data":...}'`
# 都把值放进子进程的 argv，宿主上任何用户 `ps` 就能看到——这类事故已经发生过一次
# （凭证被按长度打码的盘点打进会话）。凭"以后注意"守不住，所以用探针值断言：
# 密码在 argv 里出现即判失败。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
installer="${repo}/deploy/k3s/observability/scripts/install.sh"

[ -f "${installer}" ] || { echo "找不到安装脚本: ${installer}"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
stub="${work}/bin"
mkdir -p "${stub}" "${work}/home"
argv_log="${work}/argv.log"
: > "${argv_log}"

# 探针值：openssl 被替换成只打印它，安装脚本后续的每一步都拿它当凭证。
canary="CANARYobs-pw-2f8e1c4dba9703-LEAK"

cat > "${stub}/openssl" <<EOF
#!/usr/bin/env bash
printf '%s\n' "${canary}"
EOF

cat > "${stub}/helm" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

cat > "${stub}/kubectl" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${ARGV_LOG}"
# grafana 的 Secret 一律"不存在"，逼安装脚本走新生成分支。
# 判据要按整条命令行匹配：调用形如 `kubectl -n monitoring get secret <name>`，
# 按位置取 $1 会把 -n 当成子命令而永远判为"已存在"。
case " $* " in
  *" get secret grafana-admin-credentials "*) exit 1 ;;
esac
# 支撑 create ... --dry-run=client -o yaml | kubectl apply -f -
case " $* " in
  *" create "*) printf 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: stub\n' ;;
esac
exit 0
EOF

chmod +x "${stub}/openssl" "${stub}/helm" "${stub}/kubectl"

HOME="${work}/home" PATH="${stub}:${PATH}" ARGV_LOG="${argv_log}" BACKENDS=0 PROFILE=small \
  bash "${installer}" >"${work}/out.log" 2>&1 || {
    echo "安装脚本以非零退出，输出如下："
    cat "${work}/out.log"
    exit 1
  }

fail() { echo "FAIL: $*"; exit 1; }

# 1) 走了生成分支（否则下面的断言会因为"压根没执行"而假绿）
grep -q -- "--from-file=admin-password=" "${argv_log}" \
  || fail "没有观察到 --from-file 建 Secret 的调用，断言无效"

# 2) 探针值不得出现在任何一次 kubectl 的 argv 里
if grep -qF "${canary}" "${argv_log}"; then
  echo "泄漏的调用：$(grep -F "${canary}" "${argv_log}")"
  fail "密码出现在 kubectl 的 argv 中（宿主 ps 可见）"
fi

# 3) 也不得进日志
grep -qF "${canary}" "${work}/out.log" && fail "密码出现在安装日志中"

# 4) 但仍然要被正确落盘：600 的本地文件，内容就是它
pw_file="${work}/home/.cogneva/grafana-admin-password"
[ -f "${pw_file}" ] || fail "没有生成 ${pw_file}"
[ "$(cat "${pw_file}")" = "${canary}" ] || fail "落盘的密码与生成值不一致"
perm="$(stat -c '%a' "${pw_file}")"
[ "${perm}" = "600" ] || fail "密码文件权限是 ${perm}，应为 600"

echo "PASS: 凭证经 --from-file 进入集群，未出现在 argv 与日志中，且落盘文件为 600"
