#!/usr/bin/env bash
# 普查：`check-before-push.sh` 里**只有两个地方**会印 SKIP。
#
# 为什么要有这一条：判词末行现在会说「另有 M 面本机没验」，M 与名字都由 `skipped_faces`
# 数出来。把 15 处改成 `skip_step` 是一次性动作，树里没有任何东西阻止第 16 处直接写
# `echo "SKIP …"`。到那天判词照样印「M 面没验」，只是 M 少一个——**名单自己不会告诉你
# 名单不全**，读的人看不出少的是哪一格。
#
# 判据按「出现形式」判，不按裸子串：一行以 echo/printf 起头、紧跟的字符串开头就是 SKIP，
# 才算一处打印。注释里写「印一行 SKIP」不算，`skip_step "面" "…SKIP…"` 这种把话当参数
# 传进去的也不算——它们最终仍然走那两个点。
#
# 反面标本（第三条断言）是这一条自检的部分：把一份多写了一处 `echo "SKIP …"` 的副本交给
# 同一个判据，它必须数出 3 处。只测真树的话，判据写错了（比如正则漏了 printf）也一样绿，
# 而它本该红的那一类改动正是「多加一处跳过」。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

# 印 SKIP 的行号与正文。判的是**形状**：一行以 echo/printf 起头，紧跟的那个字符串
# 开头就是 SKIP。这样三条边都切得干净：
#   - 注释里写「印一行 SKIP」——不以 echo/printf 起头，不算；
#   - `skip_step "面" "理由里带 SKIP"`——把话当参数传，第一步仍是走那两个点，不算；
#   - `printf 'ok  %s\n' "SKIP 只是个参数"`——格式串不是 SKIP 开头，不算。
# 切不到的形状（`echo -e "SKIP …"` 这种在选项后面的）这里认不出，所以两个记账入口
# 用的是同一个写法，新增的跳过照着抄就不会漏。
skip_printers() {
    grep -nE '^[[:space:]]*(echo|printf)[[:space:]]+["'"'"']SKIP' "$1" || true
}

mapfile -t printers < <(skip_printers "${gate}")
[ "${#printers[@]}" -eq 2 ] ||
    fail "印 SKIP 的地方应当是 2 处（skip_step 与 skip_ci_step）；
实得 ${#printers[@]} 处，多出来的那些没有进 skipped_faces，判词里的面数会少一个而看不出来：
$(printf '  %s\n' "${printers[@]}")"

for p in "${printers[@]}"; do
    case "${p}" in
        *'%s'*) ;;
        *) fail "这一处印 SKIP 用的是写死的一整句，不是「面 + 理由」的模板：${p}" ;;
    esac
done

# 两个模板各自要在它那个函数里——函数没了、模板还在别处，等于没有记账入口。
grep -q '^skip_step() {' "${gate}" || fail "缺少 skip_step 的定义"
grep -q '^skip_ci_step() {' "${gate}" || fail "缺少 skip_ci_step 的定义"

# 反面标本：同一份字节里多加一处 `echo "SKIP …"`，同一个判据必须数出 3 处。
poison="$(mktemp)"
trap 'rm -f "${poison}"' EXIT
{ cat "${gate}"; echo 'echo "SKIP  新加的一格：没人记账"'; } >"${poison}"
mapfile -t poisoned < <(skip_printers "${poison}")
[ "${#poisoned[@]}" -eq 3 ] ||
    fail "判据漏判：多写了一处 echo SKIP 的副本只数出 ${#poisoned[@]} 处（应当是 3）。
这说明上面那条断言在真树上绿，是被判据自己漏了，不是树干净。"

# 注释与参数里的 SKIP 不算：写一份只有这两种写法的副本，它必须数出 0 处。
benign="$(mktemp)"
trap 'rm -f "${poison}" "${benign}"' EXIT
cat >"${benign}" <<'SH'
# 缺工具的格子印一行 SKIP，这一行是注释
skip_step "某一面" "理由里带 SKIP 三个字母"
printf 'ok  %s\n' "SKIP 出现在参数里"
SH
mapfile -t benign_hits < <(skip_printers "${benign}")
[ "${#benign_hits[@]}" -eq 0 ] ||
    fail "判据把注释 / 参数里的 SKIP 也数了进来（${#benign_hits[@]} 处），它会误报：
$(printf '  %s\n' "${benign_hits[@]}")"

echo "ok - 全树只有 skip_step / skip_ci_step 两个点会印 SKIP，两者都是「面 + 理由」的模板；多加一处越过它们的跳过会被这条普查拦下"
