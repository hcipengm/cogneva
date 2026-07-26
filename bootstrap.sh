#!/usr/bin/env sh
# Cogneva 元启动入口（第一步：Shell 拉引导器）。
# 用法: curl -fsSL https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh | sh
# 职责仅三件：确保源码 → 确保 Rust 工具链 → 编译引导器并移交控制权。
set -eu

REPO_URL="https://github.com/hcipengm/cogneva.git"
TARBALL_URL="https://codeload.github.com/hcipengm/cogneva/tar.gz/refs/heads/main"
DEFAULT_HOME="${COGNEVA_HOME:-$HOME/.cogneva}"

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
        git clone --depth 1 "$REPO_URL" "$REPO_ROOT"
    else
        echo "[bootstrap] 无 git，改用 tarball 下载..."
        curl --proto '=https' --tlsv1.2 -fsSL "$TARBALL_URL" | tar -xz --strip-components=1 -C "$REPO_ROOT"
    fi
}

ensure_rust() {
    if command -v cargo >/dev/null 2>&1; then
        echo "[bootstrap] Rust 工具链已存在: $(rustc --version)"
        return
    fi
    echo "[bootstrap] 未检测到 Rust，安装 rustup..."
    curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
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
    fetch_source
    ensure_rust
    ensure_cc
    build_bootstrap
    echo "[bootstrap] 启动 Rust 引导器，移交控制权..."
    export COGNEVA_REPO_ROOT="$REPO_ROOT"
    exec "$REPO_ROOT/target/release/cogneva-bootstrap" "$@"
}

main "$@"
