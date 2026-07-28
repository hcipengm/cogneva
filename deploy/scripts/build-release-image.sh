#!/usr/bin/env bash
# 构建 Cogneva 预构建运行时镜像 release 产物。
# 产物：dist/cogneva-image-v{VERSION}-linux-{ARCH}.tar.gz + .sha256
# bootstrap 引导器在空白机上优先下载该产物导入 K3s containerd（数分钟），
# 下载不可用时才回退源码构建（1-3 小时）。
#
# 用法：
#   deploy/scripts/build-release-image.sh          # 官方源构建
#   CN_MIRROR=1 deploy/scripts/build-release-image.sh   # 受限网络（TUNA/daocloud 镜像）
#   JOBS=2 deploy/scripts/build-release-image.sh        # 低内存机器限 cargo 并行度防 OOM
#
# 发布（产物必须与真实成功构建一一对应，sha256 文件随 tar.gz 一起上传）：
#   gh release create v{VERSION} dist/cogneva-image-*.tar.gz* --repo hcipengm/cogneva
#   Gitee：在仓库 Releases 页面创建 v{VERSION} 标签并上传同名两个文件
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VERSION="$(grep -m1 '^version' "$REPO_ROOT/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')"
ARCH="$(uname -m)"
IMAGE="localhost/cogneva:local"
NAME="cogneva-image-v${VERSION}-linux-${ARCH}.tar.gz"
DIST_DIR="$REPO_ROOT/dist"

command -v buildah >/dev/null || { echo "缺少 buildah"; exit 1; }

BUILD_ARGS=(build -t "$IMAGE" -t "localhost/cogneva:v${VERSION}" -f "$REPO_ROOT/Dockerfile")
BUILD_ARGS+=(--build-arg "CARGO_BUILD_JOBS=${JOBS:-default}")
if [ "${CN_MIRROR:-0}" = "1" ]; then
    # TUNA 不镜像按版本 channel，CN 模式工具链只能用 stable；
    # crates 走 rsproxy 而非 TUNA——TUNA 稀疏索引的 dl 仍指向 static.crates.io，
    # crate 文件直连国外会超时，rsproxy 索引与文件都自托管
    BUILD_ARGS+=(
        --build-arg RUST_TOOLCHAIN=stable
        --build-arg RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup
        --build-arg RUSTUP_UPDATE_ROOT=https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup
        --build-arg RUSTUP_INIT_URL=https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup-init.sh
        --build-arg "CARGO_REGISTRY_SPARSE=${CRATES_SPARSE:-https://rsproxy.cn/index/}"
        --build-arg APT_MIRROR_HOST=mirrors.tuna.tsinghua.edu.cn
    )
fi
BUILD_ARGS+=("$REPO_ROOT")

echo "==> 构建 $IMAGE（v$VERSION，首次全量构建需 1-3 小时）"
buildah "${BUILD_ARGS[@]}"

mkdir -p "$DIST_DIR"
WORKDIR="$(mktemp -d /var/tmp/cogneva-release-XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "==> 导出镜像（docker-archive）"
# 注：buildah push docker-archive 带引用时只写单标签，故归档内仅 :local
#（消费方——应用清单与分发器——也只引用 :local）；版本溯源靠文件名 + sha256，
# 本地 buildah 存储里另有 :v$VERSION 标签可查
buildah push "$IMAGE" "docker-archive:$WORKDIR/image.tar:$IMAGE"

echo "==> 压缩并计算 sha256"
gzip -9 -c "$WORKDIR/image.tar" > "$DIST_DIR/$NAME"
(cd "$DIST_DIR" && sha256sum "$NAME" > "$NAME.sha256")

# Gitee 附件单文件限 100MB，同步产出 95MB 分卷（part-aa/ab/...）；
# bootstrap 整包下载失败时自动回退逐卷下载拼接，sha256 对拼接结果强校验
echo "==> 生成 Gitee 分卷（95MB/卷）"
rm -f "$DIST_DIR/$NAME".part-*
split -b 95m "$DIST_DIR/$NAME" "$DIST_DIR/$NAME.part-"

echo "==> 完成："
echo "    $DIST_DIR/$NAME"
echo "    $DIST_DIR/$NAME.sha256"
echo "    $DIST_DIR/$NAME.part-*（Gitee 用）"
echo
echo "上传：gh release create v$VERSION '$DIST_DIR/$NAME' '$DIST_DIR/$NAME.sha256' --repo hcipengm/cogneva"
echo "      Gitee 上传 .sha256 + 全部 .part-* 分卷（标签 v$VERSION）"
