#!/usr/bin/env bash
# 推前门不再跑 clippy：lint 交给 CI，而这份绿必须自己说清它没判 lint。
#
# 属主 2026-11-19 定：clippy 红在 CI 上按红修就行——本机跑不跑都不改变要修的东西，CI 的
# 日志里同样是 lint 名、文件与行号。本机那一格换来的只有几分钟提前量，代价是这台机器上最重
# 的一次并行编译与别的活抢内存。代价的另一面是 clippy 红会落在 main 上一次（部署器按设计
# 在红期间不滚），所以门收尾那句「未覆盖」必须点名它：**「没判」与「判过且绿」在读数面上
# 要分得开**，否则这份绿会被读成「lint 也验过了」。
#
# 判两件事，各有一条不会在空输入上通过的守卫：
#   - 门里没有一条**可执行**的 clippy 调用（整行注释里提到 clippy 不算——那张说明为什么
#     交给 CI 的注就印在那儿）；
#   - 门收尾印的那份「未覆盖」清单里点名 Clippy。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gate="$(cd "${here}/.." && pwd)/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

# 整行注释去掉，剩下的才是会执行的东西。只认行首（允许缩进）的 `#`：行内 `#` 剥不干净
# 会把 `${#array[@]}` 这类截断，那份漏判比误判更难看出。
executable_lines="$(awk '!/^[[:space:]]*#/' "${gate}" | sed '/^[[:space:]]*$/d')"
[ -n "${executable_lines}" ] || fail "门里读不出一行可执行的东西：这份判据在空输入上会通过"

# 调用长在非 echo 的可执行行上（`"${cargo_bin}" clippy …`）；判词里的 Clippy 是 echo 印的
# 那句话，是门自己要说清「这一面没验」，不算重复。所以先把 echo 行排除，再按大小写不敏感找。
if offenders="$(printf '%s\n' "${executable_lines}" | grep -vE '^[[:space:]]*echo ' | grep -inE 'clippy')"; then
    fail "门里还有可执行的 clippy 调用，本机又在重复 CI 那一格：
${offenders}"
fi

# 收尾的「未覆盖」清单：从写着「未覆盖」的那行起，取紧挨着的连续 echo 行。
uncovered="$(awk '
    /^echo ".*未覆盖/ { take = 1 }
    take && /^echo "/ { print; next }
    take { exit }
' "${gate}")"
[ -n "${uncovered}" ] || fail "门里找不到印「未覆盖」清单的那几行：这份判据在空输入上会通过"

if ! grep -q 'Clippy' <<<"${uncovered}"; then
    fail "「未覆盖」清单里没点名 Clippy，读判词的人会把这份绿读成 lint 也验过了：
${uncovered}"
fi

echo "OK：本机的推前门不跑 clippy，并在「未覆盖」清单里点名它（这一面由 CI 判）"
