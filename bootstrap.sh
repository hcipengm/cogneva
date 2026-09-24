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
# 提权前缀（空 = 已是 root），由 ensure_privileges 结算；CN_MIRROR 同理由
# detect_restricted_net 结算，这里给初值只是为了让 `set -u` 下的引用安全。
SUDO=""
CN_MIRROR=0
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

# 单条信号探活：5s 内拿到**任何** HTTP 状态码即算可达。
# 判据用状态码而不是 curl 退出码——401/403/404 都说明"这条路通到目标了"，
# 只有连接失败/超时（000）才是不通。用 -f 会让这些状态码被当成不通，
# 于是把一台网络正常的机器误判成受限。
probe_reachable() {
    code=$(curl --proto '=https' --tlsv1.2 -sS -o /dev/null -m 5 -w '%{http_code}' "$1" 2>/dev/null || true)
    [ -n "$code" ] && [ "$code" != "000" ]
}

# 受限网络探测：多信号判据。信号表与 cog-core 的 RESTRICTED_NET_SIGNALS 一致
# （Rust 侧有测试盯着这两份清单不漂移）。
#
# 判据方向偏保守：任何一条不可达即判受限，全部可达才判开放。两个方向的代价
# 不对称——误判「开放」会让安装在墙前挂死（拉镜像超时、取码 404），误判
# 「受限」只是多走一趟镜像站，慢但能成。
# 单探一个 rustup 分发域是不够的：它可达而 GitHub 被墙的机器真实存在，
# 那种机器会判成「开放」，紧接着源码 clone 就失败。
# 可用 COGNEVA_CN_MIRROR=1/0 强制开关，跳过探测。
detect_restricted_net() {
    if [ -n "${COGNEVA_CN_MIRROR:-}" ]; then
        [ "$COGNEVA_CN_MIRROR" = "1" ] && CN_MIRROR=1 || CN_MIRROR=0
        return
    fi
    blocked=""
    for url in \
        "https://registry-1.docker.io/v2/" \
        "https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh" \
        "https://static.rust-lang.org/rustup/release-stable.toml"
    do
        probe_reachable "$url" || blocked="$blocked $url"
    done
    if [ -n "$blocked" ]; then
        CN_MIRROR=1
        echo "[bootstrap] 检测到受限网络（不可达:$blocked），启用国内镜像..."
    else
        CN_MIRROR=0
    fi
}

# 提权：元启动会把宿主机改成另一个状态（装 K3s、写 /etc/rancher、建
# /var/lib/cogneva-data），硬依赖 root。非 root 时在这里把提权方式一次定下来，
# 并在 exec 引导器时升到位——而不是把 sudo 渗透进下面每个特权步骤：漏一处就会在
# 深处的写文件上 EACCES，报错还指向不相干的那一步（实测就是这么发生的：
# 报的是「安装 K3s 失败」，真正失败的是它前面写 registries.yaml）。
ensure_privileges() {
    SUDO=""
    [ "$(id -u)" -eq 0 ] && return 0
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
        SUDO="sudo"
        echo "[bootstrap] 非 root 运行：特权步骤经 sudo 执行"
        return 0
    fi
    echo "[bootstrap] 元启动需要 root 权限（安装 K3s、写 /etc/rancher、建 /var/lib/cogneva-data）。" >&2
    echo "  管道方式: curl -fsSL <入口地址>/bootstrap.sh | sudo sh" >&2
    echo "  脚本方式: sudo -E ./bootstrap.sh" >&2
    echo "  （已配置免密 sudo 时会自动提权，此处未检测到）" >&2
    exit 1
}

# exec 引导器：非 root 时连同环境一起提权。提权边界只此一处，
# 下游（Rust 引导器）永远以 root 运行，不必再关心权限。
run_launcher() {
    if [ "$(id -u)" -eq 0 ]; then
        exec "$@"
    fi
    exec sudo -E "$@"
}

# 把官方 apt 源换成国内镜像基址（$1 已含发行版路径，如 .../ubuntu）。纯函数：
# stdin 进 stdout 出，便于单测。只认这几家官方站，其余条目（第三方 PPA、
# 内网源）原样不动。宿主清单与 Rust 侧 apt.rs 的 APT_HOST_REWRITES 同源，
# 由那个模块的测试盯着不漂移。
rewrite_apt_sources() {
    sed -E \
        -e "s#https?://archive\.ubuntu\.com/ubuntu#${1}#g" \
        -e "s#https?://security\.ubuntu\.com/ubuntu#${1}#g" \
        -e "s#https?://ports\.ubuntu\.com/ubuntu#${1}#g" \
        -e "s#https?://deb\.debian\.org/debian#${1}#g" \
        -e "s#https?://security\.debian\.org/debian-security#${1}-security#g"
}

# 探活出可用的国内 apt 镜像基址（回显；全不可达返回非零）。
# 探测目标取 dists/<codename>/Release：镜像站有没有收录这个发行版一看便知。
pick_cn_apt_mirror() {
    id=ubuntu
    codename=stable
    if [ -r /etc/os-release ]; then
        id="$(sed -n 's/^ID=//p' /etc/os-release | tr -d '"' | head -n1)"
        codename="$(sed -n 's/^VERSION_CODENAME=//p' /etc/os-release | tr -d '"' | head -n1)"
    fi
    [ -n "$id" ] || id=ubuntu
    [ -n "$codename" ] || codename=stable
    for base in \
        "https://mirrors.tuna.tsinghua.edu.cn/$id" \
        "https://mirrors.ustc.edu.cn/$id" \
        "https://mirrors.aliyun.com/$id" \
        "https://mirrors.huaweicloud.com/$id"
    do
        if probe_reachable "$base/dists/$codename/Release"; then
            echo "$base"
            return 0
        fi
    done
    return 1
}

# CN 模式下换 apt 源：不换的话 `apt-get update` 直连 archive.ubuntu.com /
# deb.debian.org，装 gcc/git 会长时间卡在这一步（国内几十 KB/s 甚至超时）。
# 原地改并留 *.cogneva-orig 备份；标记文件记录已切到哪个基址，命中即跳过（幂等）。
apply_apt_mirror() {
    base="$1"
    mark=/etc/apt/.cogneva-cn-mirror
    if $SUDO test -f "$mark" && [ "$($SUDO cat "$mark")" = "$base" ]; then
        return 0
    fi
    changed=0
    for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.list \
             /etc/apt/sources.list.d/*.sources
    do
        [ -f "$f" ] || continue
        new="$(rewrite_apt_sources "$base" < "$f")"
        if [ "$new" = "$(cat "$f")" ]; then
            continue
        fi
        if [ ! -f "$f.cogneva-orig" ]; then
            $SUDO cp -p "$f" "$f.cogneva-orig"
        fi
        printf '%s\n' "$new" | $SUDO tee "$f" >/dev/null
        changed=1
    done
    printf '%s' "$base" | $SUDO tee "$mark" >/dev/null
    if [ "$changed" = "1" ]; then
        echo "[bootstrap] apt 源已切到 $base（原件备份为 *.cogneva-orig）"
    fi
}

ensure_apt_mirror() {
    [ "$CN_MIRROR" = "1" ] || return 0
    command -v apt-get >/dev/null 2>&1 || return 0
    base="$(pick_cn_apt_mirror)" || base=""
    if [ -z "$base" ]; then
        echo "[bootstrap] 国内 apt 镜像站均不可达，沿用原有源" >&2
        return 0
    fi
    apply_apt_mirror "$base"
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

# 部署密钥路径；不存在就说明 SSH 通道结构上不可用（判据与网关/引导器同款）。
git_ssh_key() {
    key="${COGNEVA_GIT_SSH_KEY:-$DEFAULT_HOME/.ssh/id_ed25519}"
    [ -f "$key" ] && printf '%s\n' "$key"
    return 0
}

# SSH 传输命令：与网关/引导器逐字同款。BatchMode 让任何需要交互的提示直接失败，
# 而不是在无人值守的元启动里挂住等输入。
git_ssh_command() {
    printf 'ssh -i %s -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o BatchMode=yes' "$1"
}

# 当前时刻（毫秒）。优先纳秒时钟；不支持 %N 的 date 退回秒级。
now_ms() {
    ns=$(date +%s%N 2>/dev/null || true)
    case "$ns" in
        *[!0-9]*|"") echo "$(date +%s)000" ;;
        *) echo $((ns / 1000000)) ;;
    esac
}

# 给一条本该快速给结论的命令加硬上限。丢包型防火墙（不是回 RST）会让 TCP 连接
# 挂满内核重传（分钟级），而没有上限的探测会把"选路"这一步变成新的卡点——
# 这里只是挑个先后，测不出来就该立刻退回策略表。系统无 timeout 时原样执行。
within_timeout() {
    secs="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$secs" "$@"
    else
        "$@"
    fi
}

# 一次真实握手的耗时（毫秒）；不可达则无输出。
# 用 ls-remote 而不是 curl：判的必须是**这条通道**通不通、多快。
git_handshake_ms() {
    url="$1"
    key="$2"
    start=$(now_ms)
    if [ -n "$key" ]; then
        GIT_SSH_COMMAND="$(git_ssh_command "$key")" GIT_TERMINAL_PROMPT=0 \
            within_timeout 20 git ls-remote --exit-code "$url" HEAD >/dev/null 2>&1 || return 0
    else
        GIT_TERMINAL_PROMPT=0 within_timeout 20 git ls-remote --exit-code "$url" HEAD >/dev/null 2>&1 || return 0
    fi
    echo $(( $(now_ms) - start ))
}

# git 候选远端，按序尝试。次序 = **实测优先 + 策略表兜底**，与网关/Rust 侧同一张
# 选路表（判据本体在 cog-core 的选路契约里，这里只做同一件事）：
#   受限网络 → SSH 优先、失败回落 HTTPS；开放网络 → HTTPS 优先、失败回落 SSH；
#   没有部署密钥时 SSH 不进候选（不是排后面，而是结构上不可用）。
# 实测只决定"先用哪个"，测不到就退回策略表——一次握手的耗时**不预测**大流量下的
# 吞吐，所以两条通道始终都在候选里，先选的失败了就轮下一个。
# 末尾恒定补一个 Gitee：它不在传输层的候选里，而是引导链自己的区域镜像（恒定
# HTTPS），只在 GitHub 两条通道都不通时才轮到——它的 main 是对齐基线的同步点，
# 不是基线本身。
git_candidates() {
    ssh_url="git@github.com:hcipengm/cogneva.git"
    key="$(git_ssh_key)"
    ssh_ms=""
    [ -n "$key" ] && ssh_ms="$(git_handshake_ms "$ssh_url" "$key")"
    https_ms="$(git_handshake_ms "$REPO_URL" "")"

    if [ -n "$ssh_ms" ] && [ -n "$https_ms" ]; then
        if [ "$ssh_ms" -le "$https_ms" ]; then first=ssh; else first=https; fi
        why="实测 SSH ${ssh_ms}ms / HTTPS ${https_ms}ms"
    elif [ -n "$ssh_ms" ]; then
        first=ssh
        why="实测仅 SSH 可达（${ssh_ms}ms）"
    elif [ -n "$https_ms" ]; then
        first=https
        why="实测仅 HTTPS 可达（${https_ms}ms）"
    else
        # 两条都没测到：实测给不出结论，按部署期画像（策略表）
        if [ "$CN_MIRROR" = "1" ]; then first=ssh; else first=https; fi
        why="两条通道均未测通，按策略表（CN_MIRROR=$CN_MIRROR）"
    fi
    # 没有部署密钥时 SSH 即便被排在前面也走不通（不是"慢"，是结构上不可用）
    if [ -z "$key" ] && [ "$first" = "ssh" ]; then
        first=https
    fi
    echo "[bootstrap] git 选路: $why" >&2

    if [ "$first" = "ssh" ]; then
        printf '%s\n' "$ssh_url" "$REPO_URL" "$GITEE_REPO_URL"
    elif [ -n "$key" ]; then
        printf '%s\n' "$REPO_URL" "$ssh_url" "$GITEE_REPO_URL"
    else
        printf '%s\n' "$REPO_URL" "$GITEE_REPO_URL"
    fi
}

# 按远端形态选传输：SSH 远端带部署密钥，HTTPS 远端不带凭证（公开仓库）。
clone_repo() {
    url="$1"
    case "$url" in
        git@*|ssh://*)
            key="$(git_ssh_key)"
            [ -n "$key" ] || return 1
            GIT_SSH_COMMAND="$(git_ssh_command "$key")" GIT_TERMINAL_PROMPT=0 \
                git clone --depth 1 "$url" "$REPO_ROOT"
            ;;
        *)
            GIT_TERMINAL_PROMPT=0 git clone --depth 1 "$url" "$REPO_ROOT"
            ;;
    esac
}

# 源码归档（恒定 HTTPS：归档不是 git 对象，SSH 结构上够不着）。
# GitHub 与 Gitee 的先后按部署期画像排——国内直连 codeload 常年不稳，先试近的。
fetch_source_tarball() {
    if [ "$CN_MIRROR" = "1" ]; then
        tb_first="$GITEE_TARBALL_URL"
        tb_second="$TARBALL_URL"
    else
        tb_first="$TARBALL_URL"
        tb_second="$GITEE_TARBALL_URL"
    fi
    if ! curl --proto '=https' --tlsv1.2 -fsSL -m 120 "$tb_first" | tar -xz --strip-components=1 -C "$REPO_ROOT"; then
        echo "[bootstrap] 归档下载失败，改用另一个镜像源..."
        rm -rf "$REPO_ROOT"
        mkdir -p "$REPO_ROOT"
        curl --proto '=https' --tlsv1.2 -fsSL -m 120 "$tb_second" | tar -xz --strip-components=1 -C "$REPO_ROOT"
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
    # 候选次序由 git_candidates 给出（实测优先 + 策略表兜底）；一个个试过去，
    # 谁先成功算谁的。clone 失败会留半份目录，试下一个前必须清干净——半份克隆
    # 上接着 clone 会以"目录非空"再失败一次，看起来像是"所有源都不通"。
    if command -v git >/dev/null 2>&1; then
        for url in $(git_candidates); do
            echo "[bootstrap] 尝试 git 远端: $url"
            if clone_repo "$url"; then
                return 0
            fi
            echo "[bootstrap] 该远端失败，试下一个..."
            rm -rf "$REPO_ROOT"
            mkdir -p "$REPO_ROOT"
        done
        echo "[bootstrap] 所有 git 远端均失败，改用归档下载..."
    else
        echo "[bootstrap] 无 git，改用归档下载..."
    fi
    fetch_source_tarball
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
    if command -v apt-get >/dev/null 2>&1; then
        ensure_apt_mirror
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
    run_launcher "$binpath"
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

usage() {
    cat <<'EOF'
Cogneva 元启动入口（Shell 层：取引导器并移交控制权）

用法:
    curl -fsSL <地址>/bootstrap.sh | sh

本脚本不接受参数，安装参数一律经环境变量传入；不认识的参数一律拒绝执行。
（管道方式下要传参得写成 `sh -s -- <参数>`，参数才会到本脚本。）

    COGNEVA_CN_MIRROR=1              强制国内镜像路径（0 强制海外），缺省自动探测
    COGNEVA_HOME=<目录>              安装目录，缺省 ~/.cogneva
    COGNEVA_BOOTSTRAP_FROM_SOURCE=1  强制源码构建引导器（离线介质 / 本地改动调试）
    COGNEVA_BOOTSTRAP_NONINTERACTIVE=1   全程不提问（无人值守）
    COGNEVA_REPO_ROOT=<目录>         引导器使用的部署资产目录

引导器（cogneva-bootstrap）自身的选项见 `cogneva-bootstrap --help`。
EOF
}

# 参数在入口处就结算：本脚本会把宿主机改成另一个状态（装集群、写
# /var/lib/cogneva-data、装宿主工具），静默吞掉一个参数等于让调用方以为它
# 生效了——安装照样成功，错的是配置而不是结果，事后无从发现。所以不认识的
# 参数只能拒绝，而不是丢给下一环去丢。
settle_args() {
    for arg in "$@"; do
        case "$arg" in
            -h|--help)
                usage
                exit 0
                ;;
            -V|--version)
                echo "[bootstrap] 本入口脚本无版本号：引导器版本在安装时按最新 release 选定。" >&2
                echo "  安装后可运行 cogneva-bootstrap --version 查看实际版本。" >&2
                exit 2
                ;;
            *)
                echo "[bootstrap] 无法识别的参数: $arg" >&2
                usage >&2
                exit 2
                ;;
        esac
    done
}

main() {
    settle_args "$@"
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
    # 权限在此结算：后面每一步都要写系统目录，越早失败越省事，报错也才指得准。
    ensure_privileges
    # 默认路径：预编译静态二进制（下载 → 校验 → 运行，无需源码与 Rust）；
    # 失败自动回退源码构建路径（取码 → 装 Rust → cargo build）。
    # COGNEVA_BOOTSTRAP_FROM_SOURCE=1 强制源码构建（离线介质 / 本地改动调试）。
    # 参数已由 settle_args 结算，两条路径都不再透传任何参数。
    if [ -z "${COGNEVA_BOOTSTRAP_FROM_SOURCE:-}" ] && fetch_prebuilt_bootstrap; then
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
    run_launcher "$REPO_ROOT/target/release/cogneva-bootstrap"
}

# 测试钩子：置位时只加载函数定义、不执行安装（deploy/scripts/tests/ 下的
# 判据测试靠它 source 本文件）。
if [ -z "${COGNEVA_BOOTSTRAP_SOURCE_ONLY:-}" ]; then
    main "$@"
fi
