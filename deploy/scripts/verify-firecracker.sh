#!/usr/bin/env bash
# Firecracker 真机端到端一键验证（审计 L2 缺口收口）。
#
# 在带 /dev/kvm 的 Linux 主机上验证 microvm 沙盒的完整「冷启动 → 挂 PV →
# 执行 → 阅后即焚」循环。脚本负责全部镜像准备（幂等，已存在即跳过）：
#   1. 前置检查：/dev/kvm、root 权限（PV 回环挂载核验需要）、e2fsprogs
#   2. firecracker 二进制（GitHub release，已存在跳过）
#   3. guest 内核 vmlinux（firecracker-ci 预构建）
#   4. rootfs.ext4（alpine minirootfs + /evolution/init：挂 vdb 写标记后关机）
#   5. evolution-pv.ext4（空 ext4 卷，Retain 语义核验对象）
#   6. cargo example 跑真机循环，逐项 PASS/FAIL
#   7. 回环挂载 PV 核验 VERIFY_MARKER（证明进化产物真落到了持久卷）
#
# 用法：
#   deploy/scripts/verify-firecracker.sh
#   COGNEVA_MICROVM_DIR=/data/microvm deploy/scripts/verify-firecracker.sh
#
# 国内网络： alpine 走 TUNA 镜像（COGNEVA_CN_MIRROR=1 默认自动探测）；
# firecracker 与内核在 GitHub/S3，不可达时按报错提示手动下载放到
# $WORKDIR/bin/firecracker 与 $WORKDIR/vmlinux 后重跑（幂等续跑）。
set -euo pipefail

WORKDIR="${COGNEVA_MICROVM_DIR:-/opt/cogneva/microvm}"
FC_VERSION="v1.11.0"
ALPINE_VERSION="3.20.3"
ALPINE_BRANCH="v3.20"
PV_SIZE_MB=64
ROOTFS_SIZE_MB=128
ARCH="$(uname -m)"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

step() { echo ""; echo "==> $*"; }
fail() { echo "[verify] 失败: $*" >&2; exit 1; }

# ---------- 0. 前置检查 ----------
step "前置检查"
[ -e /dev/kvm ] || fail "/dev/kvm 不存在：KVM 不可用。裸机需在 BIOS 开虚拟化；虚拟机需开嵌套虚拟化（VMware: vhv.enable；KVM: nested=1；云主机选支持嵌套的实例规格）"
[ -w /dev/kvm ] || fail "/dev/kvm 无写权限：把用户加入 kvm 组或用 root 运行"
[ "$(id -u)" -eq 0 ] || fail "需要 root（PV 镜像回环挂载核验）；sudo 重跑本脚本"
command -v mkfs.ext4 >/dev/null || fail "缺 e2fsprogs：apt-get install -y e2fsprogs（PV/rootfs 镜像制作需要）"
command -v cargo >/dev/null || fail "缺 cargo：先跑 bootstrap.sh 或用 rustup 安装"
echo "  /dev/kvm 可用，root 权限具备，工具链齐备（arch=$ARCH）"

# CN 探测（只影响 alpine 镜像站选择）
CN_MIRROR="${COGNEVA_CN_MIRROR:-0}"
if [ -z "${COGNEVA_CN_MIRROR:-}" ]; then
    if ! curl --proto '=https' --tlsv1.2 -fsSL -m 5 -o /dev/null https://dl-cdn.alpinelinux.org/ 2>/dev/null; then
        CN_MIRROR=1
    fi
fi
[ "$CN_MIRROR" = "1" ] && echo "  受限网络：alpine 走 TUNA 镜像"

mkdir -p "$WORKDIR/bin"

# ---------- 1. firecracker 二进制 ----------
FC_BIN="$WORKDIR/bin/firecracker"
if [ ! -x "$FC_BIN" ]; then
    step "下载 firecracker $FC_VERSION ($ARCH)"
    fc_url="https://github.com/firecracker-microvm/firecracker/releases/download/$FC_VERSION/firecracker-$FC_VERSION-$ARCH.tgz"
    curl --proto '=https' --tlsv1.2 -fsSL -m 300 "$fc_url" -o /tmp/firecracker.tgz \
        || fail "firecracker 下载失败（$fc_url）。手动下载解压后把 firecracker 放到 $FC_BIN 再重跑"
    tar -xzf /tmp/firecracker.tgz -C /tmp
    cp "/tmp/release-$FC_VERSION-$ARCH/firecracker-$FC_VERSION-$ARCH" "$FC_BIN"
    chmod +x "$FC_BIN"
    rm -rf /tmp/firecracker.tgz "/tmp/release-$FC_VERSION-$ARCH"
fi
echo "  firecracker: $("$FC_BIN" --version | head -1)"

# ---------- 2. guest 内核 ----------
KERNEL="$WORKDIR/vmlinux"
if [ ! -f "$KERNEL" ]; then
    step "下载 firecracker-ci 预构建内核 ($ARCH)"
    case "$ARCH" in
        x86_64)  kernel_url="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.11/x86_64/vmlinux-5.10.223" ;;
        aarch64) kernel_url="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.11/aarch64/vmlinux-5.10.223" ;;
        *)       fail "不支持的架构 $ARCH：请手动准备 vmlinux 放到 $KERNEL" ;;
    esac
    curl --proto '=https' --tlsv1.2 -fsSL -m 600 "$kernel_url" -o "$KERNEL" \
        || fail "内核下载失败（$kernel_url）。手动下载放到 $KERNEL 再重跑"
fi
echo "  kernel: $KERNEL ($(du -h "$KERNEL" | cut -f1))"

# ---------- 3. rootfs（alpine + 验证 init） ----------
ROOTFS="$WORKDIR/rootfs.ext4"
if [ ! -f "$ROOTFS" ]; then
    step "构建 rootfs.ext4（alpine minirootfs + /evolution/init）"
    if [ "$CN_MIRROR" = "1" ]; then
        alpine_base="https://mirrors.tuna.tsinghua.edu.cn/alpine/$ALPINE_BRANCH/releases/$ARCH"
    else
        alpine_base="https://dl-cdn.alpinelinux.org/alpine/$ALPINE_BRANCH/releases/$ARCH"
    fi
    curl --proto '=https' --tlsv1.2 -fsSL -m 300 \
        "$alpine_base/alpine-minirootfs-$ALPINE_VERSION-$ARCH.tar.gz" -o /tmp/alpine.tgz \
        || fail "alpine minirootfs 下载失败（$alpine_base）"
    staging="$(mktemp -d)"
    tar -xzf /tmp/alpine.tgz -C "$staging"
    rm -f /tmp/alpine.tgz
    # 验证 init：挂 PV（/dev/vdb）写标记 → 卸载 → 关机（firecracker 随 guest
    # poweroff 退出，run_evolution 判定 completed=true）
    mkdir -p "$staging/evolution" "$staging/mnt"
    cat > "$staging/evolution/init" <<'INIT'
#!/bin/sh
mount -t proc proc /proc
mount -t ext4 /dev/vdb /mnt || { echo "PV mount failed" > /dev/console; poweroff -f; }
echo "cogneva-microvm-verified $(date -u +%Y-%m-%dT%H:%M:%SZ)" > /mnt/VERIFY_MARKER
sync
umount /mnt
echo "evolution init done" > /dev/console
poweroff -f
INIT
    chmod +x "$staging/evolution/init"
    dd if=/dev/zero of="$ROOTFS" bs=1M count="$ROOTFS_SIZE_MB" status=none
    mkfs.ext4 -q -d "$staging" "$ROOTFS"
    rm -rf "$staging"
fi
echo "  rootfs: $ROOTFS ($(du -h "$ROOTFS" | cut -f1))"

# ---------- 4. PV 镜像 ----------
PV="$WORKDIR/evolution-pv.ext4"
if [ ! -f "$PV" ]; then
    step "创建空 PV 镜像（${PV_SIZE_MB}MiB ext4，Retain 核验对象）"
    dd if=/dev/zero of="$PV" bs=1M count="$PV_SIZE_MB" status=none
    mkfs.ext4 -q "$PV"
fi
echo "  pv: $PV"

# ---------- 5. 真机循环 ----------
step "运行真机循环（preflight → 冷启动 → 执行 → 阅后即焚）"
cargo run -q --manifest-path "$REPO_ROOT/Cargo.toml" -p cogneva --example firecracker_verify -- \
    --firecracker-bin "$FC_BIN" \
    --kernel "$KERNEL" \
    --rootfs "$ROOTFS" \
    --pv "$PV" \
    --instance-root "$WORKDIR/instances" \
    --timeout 300

# ---------- 6. PV 落盘核验 ----------
step "核验 PV 标记（进化产物真落到持久卷）"
mnt="$(mktemp -d)"
trap 'umount "$mnt" 2>/dev/null || true; rmdir "$mnt" 2>/dev/null || true' EXIT
mount -o loop,ro "$PV" "$mnt"
if [ -f "$mnt/VERIFY_MARKER" ]; then
    echo "  VERIFY_MARKER: $(cat "$mnt/VERIFY_MARKER")"
else
    fail "PV 中无 VERIFY_MARKER：guest init 未写或写到了别处"
fi
umount "$mnt"; rmdir "$mnt"; trap - EXIT

# ---------- 7. 报告 ----------
report="$WORKDIR/verify-report-$(date -u +%Y%m%d-%H%M%S).txt"
{
    echo "Firecracker 真机验证报告"
    echo "时间: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "主机: $(uname -a)"
    echo "firecracker: $("$FC_BIN" --version | head -1)"
    echo "PV 标记: 存在"
    echo "结论: PASS"
} > "$report"
echo ""
echo "[verify] 全部通过。报告: $report"
echo "[verify] 生产启用：cogneva.json 置 self_evolution.microvm.enabled=true，"
echo "          kernel/rootfs/pv 路径指向 $WORKDIR 下对应文件。"
