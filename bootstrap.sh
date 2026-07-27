#!/usr/bin/env sh
# Cogneva 元启动入口（第一步：Shell 拉引导器）。
# 用法: curl -fsSL https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh | sh
# 职责仅三件：确保源码 → 确保 Rust 工具链 → 编译引导器并移交控制权。
set -eu

REPO_URL="https://github.com/hcipengm/cogneva.git"
GITEE_REPO_URL="https://gitee.com/hcipengm/cogneva.git"
TARBALL_URL="https://codeload.github.com/hcipengm/cogneva/tar.gz/refs/heads/main"
GITEE_TARBALL_URL="https://gitee.com/hcipengm/cogneva/repository/archive/main.tar.gz"
DEFAULT_HOME="${COGNEVA_HOME:-$HOME/.cogneva}"

# 受限网络探测：直接探 rustup 分发域（国内被墙），不通即走 TUNA 镜像。
# 可用 COGNEVA_CN_MIRROR=1/0 强制开关，跳过探测。
detect_restricted_net() {
    if [ -n "${COGNEVA_CN_MIRROR:-}" ]; then
        [ "$COGNEVA_CN_MIRROR" = "1" ] && CN_MIRROR=1 || CN_MIRROR=0
        return
    fi
    if curl --proto '=https' --tlsv1.2 -fsSL -m 5 -o /dev/null https://static.rust-lang.org/rustup/release-stable.toml 2>/dev/null; then
        CN_MIRROR=0
    else
        CN_MIRROR=1
        echo "[bootstrap] 检测到受限网络（rustup 分发域不可达），启用 TUNA 镜像..."
    fi
}

fetch_source() {
    # 已在仓库内（克隆后执行 ./bootstrap.sh）则直接使用
    if [ -f "$(dirname "$0")/crates/bootstrap/Cargo.toml" ] 2>/dev/null; then
        REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
        echo "[bootstrap] 使用当前仓库: $REPO_ROOT"
        return
    fi
    if [ -n "${COGNEVA_REPO_ROOT:-}" ] && [ -f "$COGNEVA_REPO_ROOT/crates/bootstrap/Cargo.toml" ]; then
        REPO_ROOT="$COGNEVA_REPO_ROOT"
        echo "[bootstrap] 使用 COGNEVA_REPO_ROOT: $REPO_ROOT"
        return
    fi
    # curl | sh 模式：空机器，先取源码
    REPO_ROOT="$DEFAULT_HOME/src"
    if [ -f "$REPO_ROOT/crates/bootstrap/Cargo.toml" ]; then
        echo "[bootstrap] 源码已存在: $REPO_ROOT"
        return
    fi
    echo "[bootstrap] 空机器模式，获取 Cogneva 源码 → $REPO_ROOT"
    mkdir -p "$REPO_ROOT"
    if command -v git >/dev/null 2>&1; then
        if ! git clone --depth 1 "$REPO_URL" "$REPO_ROOT"; then
            echo "[bootstrap] GitHub 克隆失败，改用 Gitee 镜像..."
            rm -rf "$REPO_ROOT"
            git clone --depth 1 "$GITEE_REPO_URL" "$REPO_ROOT"
        fi
    else
        echo "[bootstrap] 无 git，改用 tarball 下载..."
        if ! curl --proto '=https' --tlsv1.2 -fsSL -m 120 "$TARBALL_URL" | tar -xz --strip-components=1 -C "$REPO_ROOT"; then
            echo "[bootstrap] GitHub tarball 失败，改用 Gitee 归档..."
            curl --proto '=https' --tlsv1.2 -fsSL "$GITEE_TARBALL_URL" | tar -xz --strip-components=1 -C "$REPO_ROOT"
        fi
    fi
}

ensure_rust() {
    if command -v cargo >/dev/null 2>&1; then
        echo "[bootstrap] Rust 工具链已存在: $(rustc --version)"
        return
    fi
    echo "[bootstrap] 未检测到 Rust，安装 rustup..."
    if [ "$CN_MIRROR" = "1" ]; then
        export RUSTUP_DIST_SERVER="https://mirrors.tuna.tsinghua.edu.cn/rustup"
        export RUSTUP_UPDATE_ROOT="https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup"
        # TUNA 不托管 rustup-init.sh 脚本（404），直接拉 rustup-init 二进制
        arch="$(uname -m)"
        curl --proto '=https' --tlsv1.2 -fsSL \
            "$RUSTUP_UPDATE_ROOT/dist/$arch-unknown-linux-gnu/rustup-init" -o /tmp/rustup-init
        chmod +x /tmp/rustup-init
        /tmp/rustup-init -y --profile minimal
        rm -f /tmp/rustup-init
    else
        curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
    fi
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
}

ensure_cargo_mirror() {
    [ "$CN_MIRROR" = "1" ] || return
    local cfg="$HOME/.cargo/config.toml"
    mkdir -p "$HOME/.cargo"
    if [ -f "$cfg" ] && grep -q 'source.crates-io' "$cfg"; then
        echo "[bootstrap] cargo 已配置源替换，跳过镜像写入"
        return
    fi
    # crates 用 rsproxy 而非 TUNA：TUNA 稀疏索引的 dl 仍指向 static.crates.io，
    # crate 文件直连国外会超时；rsproxy 索引与文件都自托管（字节 CDN）
    cat >> "$cfg" <<'EOF'
[source.crates-io]
replace-with = "rsproxy"
[source.rsproxy]
registry = "sparse+https://rsproxy.cn/index/"

[http]
multiplexing = false

[net]
retry = 10
EOF
    echo "[bootstrap] 已写入 cargo rsproxy 镜像源: $cfg"
}

build_bootstrap() {
    echo "[bootstrap] 编译 cogneva-bootstrap（release）..."
    cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" -p cogneva-bootstrap
}

ensure_cc() {
    if command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1; then
        return
    fi
    echo "[bootstrap] 未检测到 C 工具链（Rust 链接与部分依赖需要），尝试自动安装..."
    SUDO=""
    if [ "$(id -u)" -ne 0 ]; then
        if command -v sudo >/dev/null 2>&1; then
            SUDO="sudo"
        else
            echo "[bootstrap] 需要 root 或 sudo 安装 gcc，请手动安装后重试" >&2
            exit 1
        fi
    fi
    if command -v apt-get >/dev/null 2>&1; then
        $SUDO apt-get update -qq && $SUDO apt-get install -y build-essential
    elif command -v dnf >/dev/null 2>&1; then
        $SUDO dnf install -y gcc gcc-c++ make
    elif command -v yum >/dev/null 2>&1; then
        $SUDO yum install -y gcc gcc-c++ make
    elif command -v apk >/dev/null 2>&1; then
        $SUDO apk add build-base
    else
        echo "[bootstrap] 不认识的包管理器，请手动安装 gcc 后重试" >&2
        exit 1
    fi
}

main() {
    detect_restricted_net
    fetch_source
    ensure_rust
    ensure_cc
    ensure_cargo_mirror
    build_bootstrap
    echo "[bootstrap] 启动 Rust 引导器，移交控制权..."
    export COGNEVA_REPO_ROOT="$REPO_ROOT"
    export COGNEVA_CN_MIRROR="$CN_MIRROR"
    exec "$REPO_ROOT/target/release/cogneva-bootstrap" "$@"
}

main "$@"
