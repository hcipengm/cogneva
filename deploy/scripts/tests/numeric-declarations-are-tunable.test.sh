#!/usr/bin/env bash
# principle: feedback_no_hardcoding_config_surface
#
# 部署脚本里**运行参数**不许写成裸字面量：`TIMEOUT=30` 只能改脚本才能改，
# 而这台机器上换一个节奏、换一个尺寸是运维动作，不是改写代码。
#
# 判据只有一条，因为它得能被机器判：顶层数值声明只有两种合法形状——
#   0 / 1        开关。它是"这次怎么跑"，不是"跑多大"，没有可调的量。
#   ${VAR:-N}    从环境读、默认值兜底。这是"配置化"的及格线。
# 其余一律红，判词点名文件与行。
#
# 为什么不在一个 tab 文件里逐个登记：这张表的内容随脚本增删而变，登记等于把
# 可算的东西抄一遍；要的只是"不许写死"这一条判据。
#
# 反空洞断言：扫描到 0 个声明判红——"一个都没扫到"与"一个都没写死"同形。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$(cd "$HERE/.." && pwd)"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 顶层（行首无缩进）的 `<大写名>=<纯数字>`。`$(( ))` 与字符串不匹配。
scan() {
    grep -HnE '^[A-Z][A-Z0-9_]*=[0-9]+$' "$@" 2>/dev/null
}

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT

# --- 判据本身：两种形状 ---
printf 'TIMEOUT=30\n' >"$T/bare.sh"
[ -n "$(scan "$T/bare.sh")" ] && ok "裸数值声明被扫到" || bad "裸数值声明被扫到"

printf 'LIVE=0\nRESUMED=1\n' >"$T/switch.sh"
out="$(scan "$T/switch.sh" | grep -vE '=[01]$' || true)"
[ -z "$out" ] && ok "0/1 开关放行" || bad "0/1 开关放行：$out"

# --- 真树：扫得出来、且全都合格 ---
files=("$SCRIPTS"/*.sh)
[ "${#files[@]}" -gt 50 ] && ok "扫到 ${#files[@]} 个部署脚本" || bad "扫描面太窄：${#files[@]} 个脚本"

bad_lines="$(scan "${files[@]}" | grep -vE '=[01]$' || true)"
if [ -z "$bad_lines" ]; then
    ok "真树里没有写死的运行参数（只剩 0/1 开关）"
else
    while IFS= read -r line; do bad "写死的运行参数：$line"; done <<<"$bad_lines"
fi

# 反空洞：真树确实扫到过声明（否则上一格是恒真）。
seen="$(scan "${files[@]}" | wc -l)"
[ "$seen" -gt 0 ] && ok "反空洞：真树扫到 $seen 条数值声明" || bad "反空洞：真树一条声明都没扫到（判据瞎了）"

# --- 判据要能红：把一个脚本拷出来写成裸的，必须点名 ---
mkdir -p "$T/tree"
printf 'INTERVAL=1800\n' >"$T/tree/hardcoded.sh"
out="$(scan "$T/tree"/*.sh | grep -vE '=[01]$' || true)"
[ -n "$out" ] && ok "反向对照：写死的 1800 点名" || bad "反向对照：写死的 1800 没被点名"

printf '\nnumeric-declarations-are-tunable.test: %s passed / %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
