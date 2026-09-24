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

# ---------- git 选路（实测优先 + 策略表兜底） ----------
# 假 git：ls-remote 的成败与耗时由 GIT_TEST_* 注入，clone 只记一次调用。
# 这样测的是真实的候选次序判据，而不是把脚本里的常量抄一遍。
cat > "$FAKE/git" <<'EOF'
#!/bin/sh
case "$1" in
    ls-remote)
        url=""
        for a in "$@"; do
            case "$a" in git@*|ssh://*|http*) url="$a" ;; esac
        done
        case "$url" in
            git@*|ssh://*)
                ms="${GIT_TEST_SSH_MS:-}"
                [ "${GIT_TEST_SSH_OK:-1}" = "1" ] || exit 128
                ;;
            *)
                ms="${GIT_TEST_HTTPS_MS:-}"
                [ "${GIT_TEST_HTTPS_OK:-1}" = "1" ] || exit 128
                ;;
        esac
        [ -n "$ms" ] && sleep "$ms"
        exit 0
        ;;
    clone)
        echo "clone $*" >> "${GIT_TEST_LOG:-/dev/null}"
        exit "${GIT_TEST_CLONE_RC:-0}"
        ;;
esac
exit 0
EOF
chmod +x "$FAKE/git"

KEYDIR="$(mktemp -d)"
trap 'rm -rf "$FAKE" "$FAKE2" "$KEYDIR"' EXIT
: > "$KEYDIR/id_ed25519"
SSH_URL="git@github.com:hcipengm/cogneva.git"

# 候选取序（空格分隔），断言用全序列：只断言首选会把"回落次序丢了"漏过去
cands() { git_candidates 2>/dev/null | tr '\n' ' ' | sed 's/ $//'; }

# 密钥一律显式给路径：不给就落到 ~/.cogneva/.ssh/id_ed25519，那台机器上有没有
# 部署密钥会决定判据方向，测试不能依赖它。
export COGNEVA_GIT_SSH_KEY="$KEYDIR/id_ed25519"

# 开放网络 + 有密钥：都通、HTTPS 更快 → HTTPS 优先（与加选路之前一致）
export CN_MIRROR=0 GIT_TEST_SSH_MS=0.2 GIT_TEST_HTTPS_MS=0.05
check "开放网络实测 HTTPS 更快 → HTTPS 优先" \
    "$REPO_URL $SSH_URL $GITEE_REPO_URL" "$(cands)"

# 实测压过画像：开放网络本该 HTTPS 优先，但实测 SSH 快就用 SSH
export CN_MIRROR=0 GIT_TEST_SSH_MS=0.05 GIT_TEST_HTTPS_MS=0.2
check "实测 SSH 更快 → SSH 优先（区域只是画像，不是测量）" \
    "$SSH_URL $REPO_URL $GITEE_REPO_URL" "$(cands)"

# 受限网络 + SSH 不通：实测给结论 → HTTPS 优先，SSH 留作兜底而不是被删掉
export CN_MIRROR=1 GIT_TEST_SSH_OK=0 GIT_TEST_HTTPS_MS=0.05
check "SSH 不可达 → HTTPS 优先（SSH 仍在候选里兜底）" \
    "$REPO_URL $SSH_URL $GITEE_REPO_URL" "$(cands)"

# 两条都测不到：实测没有信息量 → 退回策略表（受限网络 SSH 优先）
export CN_MIRROR=1 GIT_TEST_SSH_OK=0 GIT_TEST_HTTPS_OK=0
check "两条都没测通 → 按策略表（受限网络 SSH 优先）" \
    "$SSH_URL $REPO_URL $GITEE_REPO_URL" "$(cands)"

# 没有部署密钥：SSH 不进候选（结构上不可用，不是排后面）
export CN_MIRROR=1 COGNEVA_GIT_SSH_KEY="$KEYDIR/absent"
check "无部署密钥 → 候选里只有 HTTPS（即便 SSH 测得通）" \
    "$REPO_URL $GITEE_REPO_URL" "$(cands)"
export COGNEVA_GIT_SSH_KEY="$KEYDIR/id_ed25519"
unset GIT_TEST_SSH_MS GIT_TEST_HTTPS_MS GIT_TEST_SSH_OK GIT_TEST_HTTPS_OK CN_MIRROR

# 首选失败必须真的轮到下一个，而不是停在原地（早先的写法里第二个源永远试不到）。
# 这里跑的是真的 fetch_source，所以三件事缺一不可：
#   COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 —— 否则 source 本文件会**执行整个安装流程**
#     （测试里跑装机，且装到一半退出、后面的 fetch_source 根本不执行）；
#   COGNEVA_HOME 指向临时目录 —— 空机器模式把源码落在 $DEFAULT_HOME/src，
#     不指走就会去看真实仓库在不在，命中"源码已存在"直接返回、一个 clone 都不发；
#   cd 到临时目录 —— 克隆后执行 ./bootstrap.sh 的入口靠 `dirname $0` 认当前仓库，
#     从仓库根跑会命中"使用当前仓库"。
: > "$FAKE/clone.log"
(
    cd "$FAKE" || exit 1
    PATH="$FAKE:$ORIG_PATH" COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 COGNEVA_HOME="$FAKE/home" \
        COGNEVA_GIT_SSH_KEY="$KEYDIR/id_ed25519" \
        GIT_TEST_LOG="$FAKE/clone.log" GIT_TEST_CLONE_RC=1 \
        /bin/sh -c ". '$ROOT/bootstrap.sh' >/dev/null 2>&1; fetch_source" >/dev/null 2>&1
)
check "首选远端失败 → 逐个试完全部候选" 3 "$(wc -l < "$FAKE/clone.log" | tr -d ' ')"
rm -rf "${FAKE:?}/home" "${FAKE:?}/clone.log"

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
