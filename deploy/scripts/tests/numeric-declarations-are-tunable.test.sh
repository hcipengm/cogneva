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

# 第二个面：**命令行里的等待与超时数**。`sleep 30`、`--max-time 15`、`--timeout=600s`
# 与行首声明是同一类东西——换台慢机器就要改的那个数——但它们既不在行首、也不是
# `NAME=` 的形状，所以上面那条判据看不见它们。实测：这条腿补上之前，七个脚本里
# 藏着十七处这样的数（恢复路径等 Job 1800 秒、分发器等节点 900 秒……），全都只能
# 改脚本才改得动。
#
# 只认这几个旗标，因为它们的参数**就是**「等多久」：其余数字旗标（`--replicas`、
# `-n 3`）有的是尺寸、有的是位置，判据一宽就会把正当用法判红。
# 合格形状只有一个：数字从变量来（`--timeout="${WAIT}s"`、`sleep "$N"`）——
# 至于那个变量是 `${X:-默认}` 的声明还是命令行给的，上一条判据已经管了。
scan_inline() {
    grep -HnE '(^|[^-[:alnum:]_])(sleep|--max-time|--timeout|--timeout-seconds|--retries)[ =][0-9]+' "$@" 2>/dev/null
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

# --- 第二个面：命令行里的等待与超时 ---
# 对它的三条，与前一个面一一对应：抓得住、放得过、真树上没有。
printf 'kubectl wait --for=condition=Ready pod/x --timeout=600s\nsleep 30\n' >"$T/inline.sh"
[ "$(scan_inline "$T/inline.sh" | wc -l)" -eq 2 ] \
    && ok "命令行里的 600s 与 30 都被扫到" || bad "命令行里的等待数扫漏了：$(scan_inline "$T/inline.sh")"

printf 'kubectl wait --for=condition=Ready pod/x --timeout="${WAIT}s"\nsleep "$POLL"\n' >"$T/inline-ok.sh"
out="$(scan_inline "$T/inline-ok.sh")"
[ -z "$out" ] && ok "从变量来的等待数放行" || bad "从变量来的等待数被判红：$out"

inline_bad="$(scan_inline "${files[@]}")"
if [ -z "$inline_bad" ]; then
    ok "真树里没有写死的等待/超时数"
else
    while IFS= read -r line; do bad "写死的等待/超时数：$line"; done <<<"$inline_bad"
fi

# 反空洞。上一个面的反空洞是「真树扫到过声明」；这个面不能照抄——树干净之后裸
# 等待本来就是零条，判「扫到过」恒假。要判的是**这些旗标还在用、只是都从变量来**：
# 一个旗标都没用上时，上一条判据看的是一片空地，它绿不说明任何事。
var_form="$(grep -HEn '(^|[^-[:alnum:]_])(sleep|--max-time|--timeout|--timeout-seconds|--retries)[ =]["'"'"']?\$\{?' "${files[@]}" 2>/dev/null | wc -l)"
[ "$var_form" -gt 0 ] \
    && ok "反空洞：真树有 $var_form 处等待/超时是从变量给的（上一条判据在看真东西）" \
    || bad "反空洞：真树一处变量形式的等待/超时都没有——这条判据失去了扫描面"

printf '\nnumeric-declarations-are-tunable.test: %s passed / %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
