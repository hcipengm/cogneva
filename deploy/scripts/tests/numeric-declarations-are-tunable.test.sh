#!/usr/bin/env bash
# principle: feedback_no_hardcoding_config_surface
#
# 部署脚本里**运行参数**不许写成裸字面量：`TIMEOUT=30` 只能改脚本才能改，
# 而这台机器上换一个节奏、换一个尺寸是运维动作，不是改写代码。
#
# 判据有两个面，各一条，因为同一种数在脚本里只有这两种写法：
#
# 一、顶层数值声明（`TIMEOUT=30`）。只有两种合法形状——
#   0 / 1        开关。它是"这次怎么跑"，不是"跑多大"，没有可调的量。
#   ${VAR:-N}    从环境读、默认值兜底。这是"配置化"的及格线。
# 其余一律红，判词点名文件与行。
#
# 二、命令行里的等待/超时（`sleep 30`、`--timeout=600s`）。它们不在行首、也不是
# `NAME=` 的形状，第一条判据看不见；合法形状同样只有一个：数字从变量来。
#
# 为什么不在一个 tab 文件里逐个登记：这张表的内容随脚本增删而变，登记等于把
# 可算的东西抄一遍；要的只是"不许写死"这条判据本身。
#
# 反空洞断言：扫描面太窄、或判据瞎了（一处可扫的东西都没有），都判红——
# "一个都没扫到"与"一个都没写死"同形。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"

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
#
# 注释行不算：这棵树的注释里到处写着「以前这里是 sleep 30，抬不动」这类现场，
# 那正是判据要留的东西。把它们判红，下一步就是把记录改写成不含数字的含糊句子——
# 判据没变严，现场先没了。
scan_inline() {
    grep -HnE '(^|[^-[:alnum:]_])(sleep|--max-time|--timeout|--timeout-seconds|--retries)[ =][0-9]+' "$@" 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#'
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
# 扫的是**在宿主上由人（或 CI）跑的**脚本：`deploy/` 下的全部（测试夹具除外）、
# `.github/scripts/` 与仓库根。判据只看一棵树时，同一类数在另一棵里写死不会红，而
# 三棵树的运行者没有区别——都是在别人的机器上执行的运维动作。
#
# 排除 `deploy/helm/**/files/`：那些脚本在 Pod 里跑，改一次等待的正当路径是 chart
# 的 value 与 Pod 的 env，不是编辑脚本；把它们收进来只会要求一堆没人能设的变量。
mapfile -t files < <(find "$REPO/deploy" -name '*.sh' -not -path '*/tests/*' \
    -not -path "$REPO/deploy/helm/*/files/*" | sort)
mapfile -t gh_scripts < <(find "$REPO/.github/scripts" -maxdepth 1 -name '*.sh' | sort)
files+=("${gh_scripts[@]}")
# 仓库根的那一个（元启动）。它是操作者跑得最多的脚本，也是最容易漏在扫描面外的一个：
# 既不在 deploy/ 下，也不在 .github/scripts 下。
mapfile -t root_scripts < <(find "$REPO" -maxdepth 1 -name '*.sh' | sort)
files+=("${root_scripts[@]}")
[ "${#root_scripts[@]}" -gt 0 ] \
    && ok "仓库根的脚本在扫描面里（${root_scripts[*]##*/}）" \
    || bad "仓库根一个 .sh 都没扫到（元启动漏在面外）"
[ "${#files[@]}" -gt 50 ] \
    && ok "扫到 ${#files[@]} 个脚本（deploy 全部 + ${#gh_scripts[@]} 个 .github/scripts + ${#root_scripts[@]} 个仓库根）" \
    || bad "扫描面太窄：${#files[@]} 个脚本"
# 反空洞的另一半：另两棵树真的被扫进来了，而不是两个没展开的 glob。
[ "${#gh_scripts[@]}" -gt 0 ] \
    && ok ".github/scripts 那棵树在扫描面里（${#gh_scripts[@]} 个）" \
    || bad ".github/scripts 那棵树没被扫到（glob 没展开）"
in_pod="$(find "$REPO/deploy/helm" -path '*/files/*' -name '*.sh' | wc -l)"
[ "$in_pod" -gt 0 ] \
    && ok "Pod 内脚本确实被排除在外（${in_pod} 个：改它们走 chart value，不是改脚本）" \
    || bad "一个 Pod 内脚本都没找到——排除的路径写错了，那个面向下会漏扫"

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

printf '  # 以前这里是 sleep 30，慢机器上抬不动\nrun_thing\n' >"$T/inline-comment.sh"
out="$(scan_inline "$T/inline-comment.sh")"
[ -z "$out" ] && ok "注释里的等待数放行（现场要留得住）" || bad "注释里的等待数被判红：$out"

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
