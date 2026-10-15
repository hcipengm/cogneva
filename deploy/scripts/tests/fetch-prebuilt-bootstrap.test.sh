#!/usr/bin/env bash
# 判据测试：预编译引导器的**第一跳**，以及它取不到时那个结局叫什么。
#
# `fetch_prebuilt_bootstrap` 是元启动默认路径的第一步。它成功与否决定这次安装是
# 「下载一个 musl 静态二进制、几十秒」还是「回退源码构建：取码 + 装 Rust + cargo
# build，几十分钟」。而它的失败是**静默**的：函数返回 1，调用方接着 fetch_source，
# 机器上没有任何一处记录「第一跳其实没成，是被迫回退的」。所以这里钉两件事：
#
#   1. 受限网络（CN_MIRROR=1）下第一跳落在 Gitee、开放网络下落在 GitHub。只断言
#      「两条腿都在」会把腿序弄反漏过去，而腿序反了的形态正是「能装，但慢到被读成
#      挂死」——第一跳打的是被墙的那一侧，超时才轮到另一侧。
#   2. 第一跳取不到时，结局必须是有声音的其中之一：要么换到第二条腿仍装成，要么
#      返回非零让调用方回退源码构建，且盘上什么都没留下。不许出现「返回 0、盘上
#      没东西」——那个形状调用方读不出来，会一路走到「启动 Rust 引导器」并失败在
#      一个指向别处的地方。
#
# 全程不联网：PATH 前置假 curl / 假 uname，把「哪一跳通、哈希对不对、装没装」当作
# 注入项。跑的是 bootstrap.sh 自己的函数，不是把命令重抄一遍。
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
BOOT="$ROOT/bootstrap.sh"
ORIG_PATH="$PATH"
REAL_UNAME="$(command -v uname)"
FAKE="$(mktemp -d)"
WORK="$(mktemp -d)"
trap 'rm -rf "$FAKE" "$WORK"' EXIT

fails=0
check() { # check <描述> <期望> <实际>
    if [ "$2" = "$3" ]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1: 期望 [$2] 实际 [$3]"
        fails=$((fails + 1))
    fi
}
ok_or() { # ok_or <描述> <必须出现的串> <输出>
    case "$3" in
        *"$2"*) echo "ok   - $1" ;;
        *) echo "FAIL - $1: 输出里没有 [$2]: $3"; fails=$((fails + 1)) ;;
    esac
}
not_or() { # not_or <描述> <不许出现的串> <输出>
    case "$3" in
        *"$2"*) echo "FAIL - $1: 输出里不该有 [$2]: $3"; fails=$((fails + 1)) ;;
        *) echo "ok   - $1" ;;
    esac
}

TAG="v9.9.9-test"
TAG_BODY="{\"tag_name\": \"$TAG\"}"
BINNAME="cogneva-bootstrap-${TAG}-linux-x86_64"
GITEE_API="https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest"
GITHUB_API="https://api.github.com/repos/hcipengm/cogneva/releases/latest"
GITEE_DL="https://gitee.com/hcipengm/cogneva/releases/download"
GITHUB_DL="https://github.com/hcipengm/cogneva/releases/download"

# ---------- 假 curl ----------
# 只覆盖 bootstrap.sh 用到的三种用法：取 tag（无 -o，正文进 stdout）、探活（有 -w，
# 只回状态码）、下载（有 -o <路径>，正文落盘，<路径>.sha256 那份按腿决定哈希对错）。
cat > "$FAKE/curl" <<'EOF'
#!/bin/sh
out=""; url=""; want_code=0
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        -w) want_code=1; shift 2 ;;
        http*|https*) url="$1"; shift ;;
        *) shift ;;
    esac
done
[ -n "$url" ] || url="(no-url)"
printf '%s\n' "$url" >> "${CURL_LOG:-/dev/null}"
if [ "$want_code" = "1" ]; then echo 200; exit 0; fi
# 取 release 标签：哪一侧的 API 由 URL 定，DOWN=1 时按真 curl 的失败形态退出（22）。
case "$url" in
    *api.github.com*|*/api/v5/repos/*)
        case "$url" in
            *api.github.com*) leg=github ;;
            *) leg=gitee ;;
        esac
        up="$(printf '%s' "$leg" | tr 'a-z' 'A-Z')"
        eval "down=\${API_${up}_DOWN:-0}"
        [ "$down" = "1" ] && exit 22
        eval "body=\${API_${up}_BODY:-}"
        [ -n "$body" ] || exit 22
        printf '%s' "$body"
        exit 0
        ;;
esac
# 下载腿：来源由主机名定。
case "$url" in
    *gitee.com*) leg=gitee ;;
    *github.com*) leg=github ;;
    *) exit 22 ;;
esac
up="$(printf '%s' "$leg" | tr 'a-z' 'A-Z')"
eval "down=\${DL_${up}_DOWN:-0}"
[ "$down" = "1" ] && exit 22
case "$url" in
    *.sha256)
        name="$(basename "$url")"; name="${name%.sha256}"
        eval "bad=\${DL_${up}_BADSHA:-0}"
        if [ "$bad" = "1" ]; then
            # 长度对、内容不是那两个字节的哈希：只有哈希本身能指出这一点。
            printf '%s  %s\n' \
                '0000000000000000000000000000000000000000000000000000000000000000' \
                "$name" > "$out"
        else
            bin="$(cat "${CURL_LASTBIN:-/dev/null}" 2>/dev/null)"
            sha="$(sha256sum "$bin" 2>/dev/null | cut -d' ' -f1)"
            printf '%s  %s\n' "$sha" "$name" > "$out"
        fi
        exit 0
        ;;
esac
printf 'BINARY-FROM-%s\n' "$leg" > "$out"
printf '%s' "$out" > "${CURL_LASTBIN:-/dev/null}"
exit 0
EOF

# 假 uname：只改 -m，其余原样交给真 uname。
cat > "$FAKE/uname" <<EOF
#!/bin/sh
if [ "\${1:-}" = "-m" ]; then echo "\${FAKE_ARCH:-x86_64}"; exit 0; fi
exec "$REAL_UNAME" "\$@"
EOF
chmod +x "$FAKE/curl" "$FAKE/uname"

# ---------- 跑一次 fetch_prebuilt_bootstrap ----------
# 每次都在干净的子 shell 里重 source 一遍 bootstrap.sh，结果是文件：
#   <case>/rc   函数退出码与它算出的 binpath
#   <case>/log  假 curl 见过的每一个 URL，按次序
#   <case>/out  函数与 run_launcher 的全部输出
run_fetch() { # run_fetch <case-dir> [<bootstrap.sh>]  （注入项走环境变量）
    local case="$1" boot="${2:-$BOOT}"
    mkdir -p "$case/home"
    : > "$case/log"
    CASE="$case" PATH="$FAKE:$ORIG_PATH" \
        COGNEVA_HOME="$case/home" \
        CURL_LOG="$case/log" CURL_LASTBIN="$case/lastbin" \
        COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 \
        FAKE_ARCH="${FAKE_ARCH:-x86_64}" \
        COGNEVA_CN_MIRROR="${CN_FORCE:-1}" \
        API_GITEE_DOWN="${API_GITEE_DOWN:-0}" API_GITHUB_DOWN="${API_GITHUB_DOWN:-0}" \
        API_GITEE_BODY="$TAG_BODY" API_GITHUB_BODY="$TAG_BODY" \
        DL_GITEE_DOWN="${DL_GITEE_DOWN:-0}" DL_GITHUB_DOWN="${DL_GITHUB_DOWN:-0}" \
        DL_GITEE_BADSHA="${DL_GITEE_BADSHA:-0}" DL_GITHUB_BADSHA="${DL_GITHUB_BADSHA:-0}" \
        /bin/sh -c '
            . "$1" >/dev/null 2>&1
            # 覆盖 exec 版 launcher：把它当落点记下来，别真去 exec。
            run_launcher() { printf "LAUNCH %s CN=%s\n" "$1" "$COGNEVA_CN_MIRROR"; }
            detect_restricted_net >/dev/null 2>&1
            # bootstrap.sh 顶部是 \`set -eu\`，收起控制权后它仍在生效：非零返回一旦
            # 落在语句末尾就会当场终止整个 shell，退出码还没被读走就没了。所以按
            # main 里调用它的那个形状调——当 if 的条件——才量得到返回码。
            rc=0
            if fetch_prebuilt_bootstrap; then rc=0; else rc=$?; fi
            printf "RC=%s BINPATH=%s\n" "$rc" "${binpath:-}" > "$CASE/rc"
        ' _ "$boot" > "$case/out" 2>&1
}

first_line() { head -n1 "$1" 2>/dev/null; }
nth_line()   { sed -n "$2p" "$1" 2>/dev/null; }
launch_line() { grep -m1 '^LAUNCH' "$1" 2>/dev/null; }
first_dl()   { grep -m1 'releases/download' "$1" 2>/dev/null; }
last_dl()    { grep 'releases/download' "$1" 2>/dev/null | tail -n1; }
n_dl()       { grep -c 'releases/download' "$1" 2>/dev/null || true; }
n_api()      { grep -cE 'api\.github\.com|gitee\.com/api/v5' "$1" 2>/dev/null || true; }
rc_of()      { sed -n 's/^RC=\([0-9]*\).*/\1/p' "$1/rc" 2>/dev/null; }
bin_of()     { sed -n 's/^.*BINPATH=\(.*\)$/\1/p' "$1/rc" 2>/dev/null; }

# ---------- 1) 受限网络：第一跳 Gitee ----------
c1="$WORK/cn"
run_fetch "$c1"
check "CN：第一跳是 Gitee 的 release API" "$GITEE_API" "$(first_line "$c1/log")"
check "CN：只问了 Gitee 一次，没有顺手去问 GitHub" 1 "$(n_api "$c1/log")"
case "$(first_dl "$c1/log")" in
    "$GITEE_DL"/*) echo "ok   - CN：第一条下载腿是 Gitee" ;;
    *) echo "FAIL - CN：第一条下载腿不是 Gitee: $(first_dl "$c1/log")"; fails=$((fails + 1)) ;;
esac
check "CN：二进制与它的 sha256 各下载一次" 2 "$(n_dl "$c1/log")"
check "CN：取到并装成，退出码 0" 0 "$(rc_of "$c1")"
check "CN：装成的就是那个名字的预编译二进制" "$c1/home/bin/$BINNAME" "$(bin_of "$c1")"
check "CN：装成的文件可执行" "yes" "$([ -x "$c1/home/bin/$BINNAME" ] && echo yes || echo no)"
check "CN：走到了 launcher，且被交付的值就是那条二进制" \
    "LAUNCH $c1/home/bin/$BINNAME CN=1" "$(launch_line "$c1/out")"

# ---------- 2) 开放网络：第一跳 GitHub ----------
c2="$WORK/intl"
CN_FORCE=0 run_fetch "$c2"
check "海外：第一跳是 GitHub 的 release API" "$GITHUB_API" "$(first_line "$c2/log")"
case "$(first_dl "$c2/log")" in
    "$GITHUB_DL"/*) echo "ok   - 海外：第一条下载腿是 GitHub" ;;
    *) echo "FAIL - 海外：第一条下载腿不是 GitHub: $(first_dl "$c2/log")"; fails=$((fails + 1)) ;;
esac
check "海外：走到 launcher 时 CN 透传为 0" \
    "LAUNCH $c2/home/bin/$BINNAME CN=0" "$(launch_line "$c2/out")"

# ---------- 3) Gitee API 不通：第二跳 GitHub 取标签，下载腿序仍是 CN 序 ----------
c3="$WORK/cn-api-fallback"
API_GITEE_DOWN=1 run_fetch "$c3"
check "CN：Gitee API 不通 → 第一跳仍是 Gitee（先试首选）" "$GITEE_API" "$(first_line "$c3/log")"
check "CN：Gitee API 不通 → 第二跳是 GitHub API" "$GITHUB_API" "$(nth_line "$c3/log" 2)"
# 下载腿的次序只看 CN_MIRROR，不看是哪一侧答的 API；把它写下来，免得被顺手改成
# 「谁答的 API 就先下谁」——那会让中间那个源被换掉却没有任何东西变红。
case "$(first_dl "$c3/log")" in
    "$GITEE_DL"/*) echo "ok   - CN：标签来自 GitHub，下载第一跳仍是 Gitee" ;;
    *) echo "FAIL - CN：下载第一跳跟着答 API 的一侧跑了: $(first_dl "$c3/log")"; fails=$((fails + 1)) ;;
esac
check "CN：标签取到，仍装成" 0 "$(rc_of "$c3")"

# ---------- 4) Gitee 两跳都不通：轮到 GitHub 下载腿，仍装成 ----------
c4="$WORK/cn-dl-fallback"
API_GITEE_DOWN=1 DL_GITEE_DOWN=1 run_fetch "$c4"
check "CN：Gitee 下载腿不通 → 退出码 0（换到 GitHub 腿装成）" 0 "$(rc_of "$c4")"
case "$(last_dl "$c4/log")" in
    "$GITHUB_DL"/*) echo "ok   - CN：最后一条下载腿落到了 GitHub" ;;
    *) echo "FAIL - CN：没有轮到 GitHub 下载腿: $(last_dl "$c4/log")"; fails=$((fails + 1)) ;;
esac
check "CN：装成的是那一条二进制" "yes" "$([ -x "$c4/home/bin/$BINNAME" ] && echo yes || echo no)"

# ---------- 5) 两侧 API 都不通：有声音的失败，什么都不下载 ----------
c5="$WORK/no-tag"
API_GITEE_DOWN=1 API_GITHUB_DOWN=1 run_fetch "$c5"
check "取不到标签 → 非零退出（调用方据此回退源码构建）" 1 "$(rc_of "$c5")"
check "取不到标签 → 一条下载都不发" 0 "$(n_dl "$c5/log")"
ok_or "取不到标签 → 说清是回退源码构建" "回退源码构建" "$(cat "$c5/out")"
not_or "取不到标签 → 不许走到 launcher" "LAUNCH" "$(cat "$c5/out")"

# ---------- 6) Gitee 那份哈希对不上：拒绝它，换 GitHub 腿 ----------
c6="$WORK/gitee-badsha"
DL_GITEE_BADSHA=1 run_fetch "$c6"
check "CN：Gitee 腿哈希对不上 → 仍装成（换 GitHub 腿）" 0 "$(rc_of "$c6")"
ok_or "CN：说清了拒绝的原因" "sha256 校验失败" "$(cat "$c6/out")"
case "$(last_dl "$c6/log")" in
    "$GITHUB_DL"/*) echo "ok   - CN：哈希不符的那条腿之后真的轮到了下一条" ;;
    *) echo "FAIL - CN：哈希不符后没换腿: $(last_dl "$c6/log")"; fails=$((fails + 1)) ;;
esac

# ---------- 7) 两份哈希都对不上：非零退出，盘上不许留下东西 ----------
c7="$WORK/both-badsha"
DL_GITEE_BADSHA=1 DL_GITHUB_BADSHA=1 run_fetch "$c7"
check "两条腿的哈希都对不上 → 非零退出" 1 "$(rc_of "$c7")"
ok_or "两条腿的哈希都对不上 → 原因写成校验失败" "sha256 校验失败" "$(cat "$c7/out")"
check "两条腿的哈希都对不上 → 盘上没有那条二进制" "no" \
    "$([ -e "$c7/home/bin/$BINNAME" ] && echo yes || echo no)"
not_or "两条腿的哈希都对不上 → 不许走到 launcher" "LAUNCH" "$(cat "$c7/out")"

# ---------- 8) 已经装过：一个字节都不下载 ----------
c8="$WORK/idempotent"
mkdir -p "$c8/home/bin"
printf 'already here\n' > "$c8/home/bin/$BINNAME"
chmod 0755 "$c8/home/bin/$BINNAME"
run_fetch "$c8"
check "已就位 → 退出码 0" 0 "$(rc_of "$c8")"
check "已就位 → 不下载任何东西" 0 "$(n_dl "$c8/log")"
check "已就位 → 仍问了标签（需要它拼出那条路径）" 1 "$(n_api "$c8/log")"
check "已就位 → 走到 launcher 且是那条已有的二进制" \
    "LAUNCH $c8/home/bin/$BINNAME CN=1" "$(launch_line "$c8/out")"

# ---------- 9) 本机没这个架构的产物：不联网，当场回退 ----------
c9="$WORK/other-arch"
FAKE_ARCH=riscv64 run_fetch "$c9"
check "未知架构 → 非零退出" 1 "$(rc_of "$c9")"
check "未知架构 → 一个请求都不发" 0 "$(wc -l < "$c9/log" | tr -d ' ')"
ok_or "未知架构 → 说清是回退源码构建" "回退源码构建" "$(cat "$c9/out")"

# ---------- 对照：这两条判据拒得掉该拒的东西 ----------
# 前面每一条断言都在读真实函数跑出来的次序与退出码，但「读的是真实行为」本身不证明
# 断言能拒。下面拿两个变异体各拒一次——两处都踩在本判据要守的那条线上：腿序、以及
# 「取不到」到底叫什么。
mutate() { # mutate <from-file> <to-file> <python 改写脚本>
    python3 "$3" "$1" "$2"
}

cat > "$WORK/mut1.py" <<'PY'
import sys
old = '''    if [ "$CN_MIRROR" = "1" ]; then
        api_candidates="https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest https://api.github.com/repos/hcipengm/cogneva/releases/latest"
        dl_primary="https://gitee.com/hcipengm/cogneva/releases/download"
        dl_secondary="https://github.com/hcipengm/cogneva/releases/download"'''
new = '''    if [ "$CN_MIRROR" = "1" ]; then
        api_candidates="https://api.github.com/repos/hcipengm/cogneva/releases/latest https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest"
        dl_primary="https://github.com/hcipengm/cogneva/releases/download"
        dl_secondary="https://gitee.com/hcipengm/cogneva/releases/download"'''
s = open(sys.argv[1]).read()
if old not in s:
    sys.exit(3)
open(sys.argv[2], 'w').write(s.replace(old, new, 1))
PY

cat > "$WORK/mut2.py" <<'PY'
import sys
old = '''    if [ -z "$tag" ]; then
        echo "[bootstrap] 未能获取最新 release 标签（release 未发布或网络不可达），回退源码构建" >&2
        return 1
    fi'''
new = '''    if [ -z "$tag" ]; then
        echo "[bootstrap] 未能获取最新 release 标签（release 未发布或网络不可达），回退源码构建" >&2
        return 0
    fi'''
s = open(sys.argv[1]).read()
if old not in s:
    sys.exit(3)
open(sys.argv[2], 'w').write(s.replace(old, new, 1))
PY

c10="$WORK/mut-tie"
if mutate "$BOOT" "$WORK/boot-legorder.sh" "$WORK/mut1.py"; then
    chmod 0755 "$WORK/boot-legorder.sh"
    run_fetch "$c10" "$WORK/boot-legorder.sh"
    got="$(first_line "$c10/log")"
    check "变异体：CN 腿序颠倒 → 第一跳不再是 Gitee（判据拒得掉）" \
        "no" "$([ "$got" = "$GITEE_API" ] && echo yes || echo no)"
else
    echo "FAIL - 变异体：腿序改写没生效（bootstrap.sh 的 CN 分支原文找不到了，这条对照是空跑）"
    fails=$((fails + 1))
fi

c11="$WORK/mut-silent"
if mutate "$BOOT" "$WORK/boot-silent.sh" "$WORK/mut2.py"; then
    chmod 0755 "$WORK/boot-silent.sh"
    API_GITEE_DOWN=1 API_GITHUB_DOWN=1 run_fetch "$c11" "$WORK/boot-silent.sh"
    check "变异体：取不到标签却返回 0 → 退出码不再是 1（判据拒得掉）" \
        "no" "$([ "$(rc_of "$c11")" = "1" ] && echo yes || echo no)"
else
    echo "FAIL - 变异体：静默返回改写没生效（那段原文找不到了，这条对照是空跑）"
    fails=$((fails + 1))
fi

if [ "$fails" -ne 0 ]; then
    echo "预编译引导器第一跳判据测试失败: $fails"
    exit 1
fi
echo "PASS: CN_MIRROR=1 时第一跳落在 Gitee（海外落在 GitHub），取不到时逐腿回退并在全灭时非零退出、盘上不留东西"
