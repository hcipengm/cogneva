#!/usr/bin/env sh
# Cogneva 元启动入口（第一步：Shell 拉引导器）。
# 用法: curl -fsSL https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh | sh
# Linux 裸机直接引导；macOS 自动经 Lima 虚拟机提供 Linux 运行层后走同一流程；
# Windows 请用 bootstrap.ps1（WSL2）。
# Linux 上默认下载 release 预编译静态引导器（内嵌全部部署资产）直接运行；
# 下载/校验失败或 COGNEVA_BOOTSTRAP_FROM_SOURCE=1 时回退源码构建路径
# （取码 → 装 Rust 工具链 → 编译引导器），两条路径最终都移交 Rust 引导器。
set -eu

REPO_URL="https://github.com/hcipengm/cogneva.git"
GITEE_REPO_URL="https://gitee.com/hcipengm/cogneva.git"
TARBALL_URL="https://codeload.github.com/hcipengm/cogneva/tar.gz/refs/heads/main"
GITEE_TARBALL_URL="https://gitee.com/hcipengm/cogneva/repository/archive/main.tar.gz"
DEFAULT_HOME="${COGNEVA_HOME:-$HOME/.cogneva}"
# 与 README 完全同一条入口命令（VM/WSL 内复用），CN 模式 Gitee 优先
ENTRY_CMD_INTL='(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh || curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh) | sh'
ENTRY_CMD_CN='(curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh || curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh) | sh'

# COGNEVA_BOOTSTRAP_FAKE_OS 仅用于干跑测试（模拟 darwin 分支）
detect_os() {
    if [ -n "${COGNEVA_BOOTSTRAP_FAKE_OS:-}" ]; then
        BOOTSTRAP_OS="$COGNEVA_BOOTSTRAP_FAKE_OS"
        return
    fi
    case "$(uname -s)" in
        Linux)  BOOTSTRAP_OS="linux" ;;
        Darwin) BOOTSTRAP_OS="darwin" ;;
        *)      BOOTSTRAP_OS="other" ;;
    esac
}

# 受限网络探测：直接探 rustup 分发域（国内被墙），不通即走国内镜像。
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
        echo "[bootstrap] 检测到受限网络（rustup 分发域不可达），启用国内镜像..."
    fi
}

# 多镜像候选探测：按顺序探活（5s 超时），返回第一个可达的地址，
# 全部不可达时回退第一个候选（保持与写死单镜像相同的下限行为，
# 后续下载层的重试机制仍会兜底）。
pick_first_ok() {
    for url in "$@"; do
        if curl --proto '=https' --tlsv1.2 -fsSL -m 5 -o /dev/null "$url" 2>/dev/null; then
            echo "$url"
            return
        fi
        echo "[bootstrap] 镜像不可达，换下一个: $url" >&2
    done
    echo "$1"
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
        # rustup 候选：TUNA → USTC（两家布局不同，探活 rustup-init 二进制路径）
        arch="$(uname -m)"
        init_url=$(pick_first_ok \
            "https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup/dist/$arch-unknown-linux-gnu/rustup-init" \
            "https://mirrors.ustc.edu.cn/rust-static/rustup/dist/$arch-unknown-linux-gnu/rustup-init")
        case "$init_url" in
            *ustc*)
                export RUSTUP_DIST_SERVER="https://mirrors.ustc.edu.cn/rust-static"
                export RUSTUP_UPDATE_ROOT="https://mirrors.ustc.edu.cn/rust-static/rustup" ;;
            *)
                export RUSTUP_DIST_SERVER="https://mirrors.tuna.tsinghua.edu.cn/rustup"
                export RUSTUP_UPDATE_ROOT="https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup" ;;
        esac
        echo "[bootstrap] rustup 镜像: $RUSTUP_DIST_SERVER"
        # 镜像站都不托管 rustup-init.sh 脚本（404），直接拉 rustup-init 二进制
        curl --proto '=https' --tlsv1.2 -fsSL "$init_url" -o /tmp/rustup-init
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
    cfg="$HOME/.cargo/config.toml"
    mkdir -p "$HOME/.cargo"
    if [ -f "$cfg" ] && grep -q 'source.crates-io' "$cfg"; then
        echo "[bootstrap] cargo 已配置源替换，跳过镜像写入"
        return
    fi
    # crates 候选：rsproxy（字节 CDN）→ USTC。不能用 TUNA——其稀疏索引的 dl
    # 仍指向 static.crates.io，crate 文件直连国外会超时；这两家索引与文件都自托管
    sparse=$(pick_first_ok \
        "https://rsproxy.cn/index/config.json" \
        "https://mirrors.ustc.edu.cn/crates.io-index/config.json")
    sparse="sparse+${sparse%/config.json}/"
    cat >> "$cfg" <<EOF
[source.crates-io]
replace-with = "mirror"
[source.mirror]
registry = "$sparse"

[http]
multiplexing = false

[net]
retry = 10
EOF
    echo "[bootstrap] 已写入 cargo 镜像源 $sparse: $cfg"
}

build_bootstrap() {
    echo "[bootstrap] 编译 cogneva-bootstrap（release）..."
    cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" -p cogneva-bootstrap
}

ensure_cc() {
    # git 同样是硬依赖：ensure_git_remote 要对源码做 git clone --bare，
    # tarball 方式取得的源码没有 .git，必须由本函数保证 git 可用
    if command -v cc >/dev/null 2>&1 && command -v git >/dev/null 2>&1; then
        return
    fi
    echo "[bootstrap] 未检测到 C 工具链或 git（Rust 链接、依赖与自进化仓库需要），尝试自动安装..."
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
        $SUDO apt-get update -qq && $SUDO apt-get install -y build-essential git
    elif command -v dnf >/dev/null 2>&1; then
        $SUDO dnf install -y gcc gcc-c++ make git
    elif command -v yum >/dev/null 2>&1; then
        $SUDO yum install -y gcc gcc-c++ make git
    elif command -v apk >/dev/null 2>&1; then
        $SUDO apk add build-base git
    else
        echo "[bootstrap] 不认识的包管理器，请手动安装 gcc 后重试" >&2
        exit 1
    fi
}

# ---------- 预编译静态引导器（默认路径） ----------
# 下载 release 附件里的 musl 静态二进制（内嵌全部部署资产，无需源码/Rust），
# sha256 校验通过后直接 exec，成功不返回。任何失败返回非零，由调用方回退到
# 源码构建路径（fetch_source → cargo build），两条路径互不影响。
fetch_prebuilt_bootstrap() {
    case "$(uname -m)" in
        x86_64|aarch64) arch="$(uname -m)" ;;
        *) echo "[bootstrap] 预编译引导器无 $(uname -m) 架构产物，回退源码构建" >&2; return 1 ;;
    esac
    # 最新 release 标签与下载基址：CN 先 Gitee 后 GitHub，海外反之
    if [ "$CN_MIRROR" = "1" ]; then
        api_candidates="https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest https://api.github.com/repos/hcipengm/cogneva/releases/latest"
        dl_primary="https://gitee.com/hcipengm/cogneva/releases/download"
        dl_secondary="https://github.com/hcipengm/cogneva/releases/download"
    else
        api_candidates="https://api.github.com/repos/hcipengm/cogneva/releases/latest https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest"
        dl_primary="https://github.com/hcipengm/cogneva/releases/download"
        dl_secondary="https://gitee.com/hcipengm/cogneva/releases/download"
    fi
    tag=""
    for api in $api_candidates; do
        tag=$(curl --proto '=https' --tlsv1.2 -fsSL -m 10 "$api" 2>/dev/null \
            | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
            | head -n1)
        [ -n "$tag" ] && break
    done
    if [ -z "$tag" ]; then
        echo "[bootstrap] 未能获取最新 release 标签（release 未发布或网络不可达），回退源码构建" >&2
        return 1
    fi
    name="cogneva-bootstrap-${tag}-linux-${arch}"
    bindir="$DEFAULT_HOME/bin"
    binpath="$bindir/$name"
    mkdir -p "$bindir"
    if [ ! -x "$binpath" ]; then
        tmp="$(mktemp -d)"
        ok=0
        for base in "$dl_primary" "$dl_secondary"; do
            echo "[bootstrap] 下载预编译引导器 $name: $base/$tag/$name"
            if curl --proto '=https' --tlsv1.2 -fsSL -m 300 --retry 2 \
                    -o "$tmp/$name" "$base/$tag/$name" \
                && curl --proto '=https' --tlsv1.2 -fsSL -m 30 \
                    -o "$tmp/$name.sha256" "$base/$tag/$name.sha256"; then
                if (cd "$tmp" && sha256sum -c "$name.sha256" >/dev/null 2>&1); then
                    mv "$tmp/$name" "$binpath"
                    chmod 0755 "$binpath"
                    ok=1
                    break
                fi
                echo "[bootstrap] sha256 校验失败，换下一个来源" >&2
            else
                echo "[bootstrap] 下载失败，换下一个来源: $base/$tag/$name" >&2
            fi
        done
        rm -rf "$tmp"
        [ "$ok" = "1" ] || return 1
    fi
    echo "[bootstrap] 使用预编译静态引导器 $tag（$arch），移交控制权..."
    # 不 export COGNEVA_REPO_ROOT：二进制解包内嵌资产自取自用
    export COGNEVA_CN_MIRROR="$CN_MIRROR"
    exec "$binpath" "$@"
}

# ---------- macOS：Lima 虚拟机提供 Linux 运行层 ----------
# K3s 不能原生运行于 macOS；Lima（CNCF 项目）是最小 Linux VM 方案。
# 所有依赖都装在 VM 内，宿主只需 limactl。

ensure_lima() {
    if command -v limactl >/dev/null 2>&1; then
        echo "[bootstrap] Lima 已安装: $(limactl --version 2>/dev/null | head -1)"
        return
    fi
    if ! command -v brew >/dev/null 2>&1; then
        echo "[bootstrap] macOS 需要 Lima 虚拟机，安装 Lima 需要 Homebrew：" >&2
        echo '  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"' >&2
        echo "  （国内可用 TUNA 镜像安装 Homebrew: https://mirrors.tuna.tsinghua.edu.cn/help/homebrew/）" >&2
        exit 1
    fi
    echo "[bootstrap] 安装 Lima（brew install lima）..."
    if [ "$CN_MIRROR" = "1" ]; then
        # brew bottle 候选：TUNA → USTC → 阿里云
        bottle=$(pick_first_ok \
            "https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles/api/formula.json" \
            "https://mirrors.ustc.edu.cn/homebrew-bottles/api/formula.json" \
            "https://mirrors.aliyun.com/homebrew/homebrew-bottles/api/formula.json")
        bottle="${bottle%/api/formula.json}"
        export HOMEBREW_BOTTLE_DOMAIN="$bottle"
        export HOMEBREW_API_DOMAIN="$bottle/api"
        echo "[bootstrap] Homebrew bottle 镜像: $bottle"
    fi
    brew install lima
}

write_lima_config() {
    LIMA_CFG="$DEFAULT_HOME/lima-cogneva.yaml"
    mkdir -p "$DEFAULT_HOME"
    # 镜像文件名用 amd64/arm64，lima arch 字段用 x86_64/aarch64
    case "$(uname -m)" in
        arm64)  img_name_arch="arm64"; lima_arch="aarch64" ;;
        *)      img_name_arch="amd64"; lima_arch="x86_64" ;;
    esac
    # 资源：默认 2 核 / 4GiB，小内存 Mac 收敛
    host_cpus="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
    host_mem_bytes="$(sysctl -n hw.memsize 2>/dev/null || echo 8589934592)"
    cpus=2
    [ "$host_cpus" -lt 4 ] && cpus=1
    mem_gib=4
    [ $((host_mem_bytes / 1073741824)) -lt 8 ] && mem_gib=2
    if [ "$CN_MIRROR" = "1" ]; then
        # ubuntu cloudimg 国内候选只有 USTC 收录完整目录（TUNA/阿里无此路径）；
        # 不可达时回退官方站（直连慢但可用）。img 文件数百 MB，探测用 Range 取 1 字节
        img_name="ubuntu-24.04-server-cloudimg-$img_name_arch.img"
        img_base=""
        for base in "https://mirrors.ustc.edu.cn/ubuntu-cloud-images/releases/24.04/release" \
                    "https://cloud-images.ubuntu.com/releases/24.04/release"; do
            if curl --proto '=https' --tlsv1.2 -fsSL -m 8 -r 0-0 -o /dev/null "$base/$img_name" 2>/dev/null; then
                img_base="$base"
                break
            fi
            echo "[bootstrap] cloudimg 镜像不可达，换下一个: $base" >&2
        done
        [ -z "$img_base" ] && img_base="https://mirrors.ustc.edu.cn/ubuntu-cloud-images/releases/24.04/release"
    else
        img_base="https://cloud-images.ubuntu.com/releases/24.04/release"
    fi
    cat > "$LIMA_CFG" <<EOF
# Cogneva Linux 运行层（bootstrap.sh 生成；VM 已存在时本文件改动不生效，
# 需 limactl delete cogneva 后重跑才会按新配置重建）
images:
  - location: "$img_base/ubuntu-24.04-server-cloudimg-$img_name_arch.img"
    arch: "$lima_arch"
cpus: $cpus
memory: "${mem_gib}GiB"
disk: "60GiB"
containerd:
  system: false
  user: false
mounts:
  - location: "~"
    writable: false
portForwards:
  - guestIP: "0.0.0.0"
    guestPort: 8080
    hostIP: "127.0.0.1"
    hostPort: 8080
EOF
    echo "[bootstrap] Lima 配置: $LIMA_CFG（$cpus 核 / ${mem_gib}GiB / 60GiB 磁盘）"
}

start_lima_vm() {
    if limactl list -q 2>/dev/null | grep -qx "cogneva"; then
        if [ "$(limactl list 2>/dev/null | awk '$1=="cogneva" {print $2}')" = "Running" ]; then
            echo "[bootstrap] Lima VM 'cogneva' 已在运行，复用"
            return
        fi
        echo "[bootstrap] 启动已存在的 Lima VM 'cogneva'..."
        limactl start cogneva
        return
    fi
    echo "[bootstrap] 创建 Lima VM 'cogneva'（首次需下载 Ubuntu 镜像，约数百 MB）..."
    limactl start --name=cogneva "$LIMA_CFG"
}

macos_bootstrap() {
    echo "[bootstrap] 检测到 macOS：K3s 需 Linux 内核，将使用 Lima 虚拟机作为运行层（依赖全部装在 VM 内）..."
    detect_restricted_net
    ensure_lima
    write_lima_config
    start_lima_vm
    if [ "$CN_MIRROR" = "1" ]; then
        entry="$ENTRY_CMD_CN"
    else
        entry="$ENTRY_CMD_INTL"
    fi
    echo "[bootstrap] 在 VM 内执行与 Linux 完全相同的一键命令，COGNEVA_CN_MIRROR=$CN_MIRROR 已透传..."
    # shellcheck disable=SC2086
    limactl shell cogneva -- sh -c "COGNEVA_CN_MIRROR=$CN_MIRROR $entry"
    echo ""
    echo "[bootstrap] 完成！Cogneva 已在 VM 内运行，WebUI 经端口转发暴露到本机："
    echo "  http://localhost:8080"
    echo "常用命令: limactl shell cogneva（进 VM）| limactl stop cogneva | limactl delete cogneva（还原）"
    if command -v open >/dev/null 2>&1; then
        open http://localhost:8080 2>/dev/null || true
    fi
}

main() {
    detect_os
    case "$BOOTSTRAP_OS" in
        darwin)
            macos_bootstrap
            return
            ;;
        linux)
            ;;
        *)
            echo "[bootstrap] 未支持的操作系统: $(uname -s)" >&2
            echo "  Windows 请用管理员 PowerShell 运行:" >&2
            echo "  iwr -useb https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.ps1 | iex" >&2
            exit 1
            ;;
    esac
    detect_restricted_net
    # 默认路径：预编译静态二进制（下载 → 校验 → 运行，无需源码与 Rust）；
    # 失败自动回退源码构建路径（取码 → 装 Rust → cargo build）。
    # COGNEVA_BOOTSTRAP_FROM_SOURCE=1 强制源码构建（离线介质 / 本地改动调试）。
    if [ -z "${COGNEVA_BOOTSTRAP_FROM_SOURCE:-}" ] && fetch_prebuilt_bootstrap "$@"; then
        exit 0
    fi
    echo "[bootstrap] 预编译引导器不可用，回退源码构建路径..."
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
