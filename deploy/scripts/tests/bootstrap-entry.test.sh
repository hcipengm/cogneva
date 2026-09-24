#!/usr/bin/env bash
# bootstrap.sh 的判据测试：受限网络分类、apt 源改写、权限结算。
# 全程不联网——PATH 前置假 curl / 假 sudo / 假 id，注入"通/不通"与"是不是 root"。
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
FAKE="$(mktemp -d)"
FAKE2="$(mktemp -d)"
trap 'rm -rf "$FAKE" "$FAKE2"' EXIT

fails=0
check() { # check <描述> <期望> <实际>
    if [ "$2" = "$3" ]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1: 期望 [$2] 实际 [$3]"
        fails=$((fails + 1))
    fi
}

# 假 curl：按 $BLOCKED（空格分隔的 URL 子串）决定回 000（不通）还是 200。
# 只回状态码，正好是 probe_reachable 用 -w '%{http_code}' 取的那一项。
cat > "$FAKE/curl" <<'EOF'
#!/bin/sh
url=""
for a in "$@"; do
    case "$a" in http*) url="$a" ;; esac
done
for h in ${BLOCKED:-}; do
    case "$url" in *"$h"*) echo 000; exit 0 ;; esac
done
echo 200
EOF
# 假 sudo：模拟已配置免密 sudo（`sudo -n true` 成功）
cat > "$FAKE/sudo" <<'EOF'
#!/bin/sh
[ "${1:-}" = "-n" ] && shift
exec "$@"
EOF
# 假 id：模拟非 root 用户
cat > "$FAKE/id" <<'EOF'
#!/bin/sh
[ "${1:-}" = "-u" ] && { echo 1000; exit 0; }
exec /usr/bin/id "$@"
EOF
chmod +x "$FAKE"/*
cp "$FAKE/id" "$FAKE2/id"
cp "$FAKE/curl" "$FAKE2/curl"

ORIG_PATH="$PATH"
export PATH="$FAKE:$PATH"

COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 . "$ROOT/bootstrap.sh"
set +eu

# ---------- 受限网络判据 ----------
export BLOCKED=""
detect_restricted_net >/dev/null 2>&1
check "全部信号可达 → 开放网络" 0 "$CN_MIRROR"

export BLOCKED="registry-1.docker.io"
# 直接调用而不是 $(...) 捕获：命令替换会开子 shell，函数里算出的 CN_MIRROR 出不来
detect_restricted_net > "$FAKE/out" 2>&1
out="$(cat "$FAKE/out")"
check "单条信号不可达 → 受限网络" 1 "$CN_MIRROR"
case "$out" in
    *registry-1.docker.io*) echo "ok   - 受限判定点名了不可达的信号" ;;
    *) echo "FAIL - 受限判定未点名不可达信号: $out"; fails=$((fails + 1)) ;;
esac

# B1 的真实事故形态：rustup 分发域可达（旧判据的唯一探针），GitHub raw 被墙。
export BLOCKED="raw.githubusercontent.com"
detect_restricted_net >/dev/null 2>&1
check "rustup 可达但 GitHub raw 被墙 → 仍判受限" 1 "$CN_MIRROR"

export BLOCKED=""
COGNEVA_CN_MIRROR=1 detect_restricted_net >/dev/null 2>&1
check "COGNEVA_CN_MIRROR=1 强制受限（跳过探测）" 1 "$CN_MIRROR"
export BLOCKED="registry-1.docker.io"
COGNEVA_CN_MIRROR=0 detect_restricted_net >/dev/null 2>&1
check "COGNEVA_CN_MIRROR=0 强制开放（不因不可达改判）" 0 "$CN_MIRROR"
unset BLOCKED COGNEVA_CN_MIRROR

# ---------- apt 源改写（纯函数） ----------
MIRROR="https://mirrors.tuna.tsinghua.edu.cn/ubuntu"
check "legacy 一行式换源" \
    "deb $MIRROR noble main" \
    "$(printf '%s\n' 'deb http://archive.ubuntu.com/ubuntu noble main' | rewrite_apt_sources "$MIRROR")"
check "deb822 URIs 换源" \
    "URIs: $MIRROR" \
    "$(printf '%s\n' 'URIs: http://archive.ubuntu.com/ubuntu' | rewrite_apt_sources "$MIRROR")"
check "security 主机也换" \
    "deb $MIRROR noble-security main" \
    "$(printf '%s\n' 'deb http://security.ubuntu.com/ubuntu noble-security main' | rewrite_apt_sources "$MIRROR")"
# 第三方 PPA / 内网源不能被顺手改掉
check "非官方源原样保留" \
    "deb https://ppa.example.com/ubuntu noble main" \
    "$(printf '%s\n' 'deb https://ppa.example.com/ubuntu noble main' | rewrite_apt_sources "$MIRROR")"
DMIRROR="https://mirrors.ustc.edu.cn/debian"
check "debian-security 走 -security 后缀" \
    "deb $DMIRROR-security bookworm-security main" \
    "$(printf '%s\n' 'deb http://security.debian.org/debian-security bookworm-security main' | rewrite_apt_sources "$DMIRROR")"

# ---------- 权限结算 ----------
ensure_privileges >/dev/null 2>&1
check "非 root + 免密 sudo → 用 sudo 提权" "sudo" "$SUDO"

# 没有 sudo 可用时必须当场退出并说清要什么权限，而不是跑到深处 EACCES
err="$(PATH="$FAKE2" COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 /bin/sh -c \
    ". '$ROOT/bootstrap.sh' >/dev/null 2>&1; ensure_privileges" 2>&1)"
rc=$?
check "非 root 且无 sudo → 非零退出" 1 "$rc"
case "$err" in
    *"需要 root"*) echo "ok   - 缺权限时报错指明需要 root" ;;
    *) echo "FAIL - 缺权限报错未指明原因: $err"; fails=$((fails + 1)) ;;
esac

export PATH="$ORIG_PATH"
if [ "$fails" -ne 0 ]; then
    echo "bootstrap 入口判据测试失败: $fails"
    exit 1
fi
echo "bootstrap 入口判据测试全部通过"
