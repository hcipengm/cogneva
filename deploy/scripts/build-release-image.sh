#!/usr/bin/env bash
# 构建 Cogneva 预构建运行时镜像 release 产物。
# 产物：dist/cogneva-image-v{VERSION}-linux-{ARCH}.tar.gz + .sha256
# bootstrap 引导器在空白机上优先下载该产物导入 K3s containerd（数分钟），
# 下载不可用时才回退源码构建（1-3 小时）。
#
# 版本单一同源：VERSION 取 Cargo.toml workspace.package.version；镜像 tag、
# helm appVersion、清单 version label、release 标签全部派生自它，构建前
# 强制一致性门禁，任何一处漂移直接失败。
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
VERSIONED_IMAGE="localhost/cogneva:${VERSION}"
NAME="cogneva-image-v${VERSION}-linux-${ARCH}.tar.gz"
DIST_DIR="$REPO_ROOT/dist"

command -v buildah >/dev/null || { echo "缺少 buildah"; exit 1; }

cd "$REPO_ROOT"

# release 构建必须在提交后的干净树上：镜像内嵌的 git revision 要能对应到
# 一个真实 commit，否则线上版本追溯断链。
if [ -n "$(git status --porcelain)" ]; then
    echo "工作树不干净，release 构建必须在提交后的干净树上：" >&2
    git status --short >&2
    exit 1
fi
GIT_REVISION="$(git rev-parse --short HEAD)"

echo "==> 版本一致性门禁（VERSION=$VERSION, rev=$GIT_REVISION）"
fail() { echo "版本漂移: $1" >&2; exit 1; }
grep -m1 '^appVersion:' deploy/helm/cogneva/Chart.yaml | grep -q "\"$VERSION\"" \
    || fail "deploy/helm/cogneva/Chart.yaml appVersion != $VERSION"
grep -m1 '^version:' deploy/helm/cogneva/Chart.yaml | grep -q "$VERSION" \
    || fail "deploy/helm/cogneva/Chart.yaml version != $VERSION"
python3 - "$VERSION" <<'EOF' || exit 1
import re, sys
want = sys.argv[1]
text = open("deploy/helm/cogneva/values.yaml").read()
m = re.search(r"^image:\s*\n(?:\s+[^\n]*\n)*?\s+tag:\s*\"([^\"]+)\"", text, re.M)
if not m or m.group(1) != want:
    sys.exit(f"values.yaml image.tag != {want} (got {m.group(1) if m else 'missing'})")
for f in ("deployment", "gateway-deployment", "sandbox-executor-deployment", "evolution-deployment"):
    t = open(f"deploy/k3s/{f}.yaml").read()
    if f'app.kubernetes.io/version: "{want}"' not in t:
        sys.exit(f"deploy/k3s/{f}.yaml 缺 version label {want}")
cfg = open("deploy/k3s/configmap.yaml").read()
if f'COGNEVA_APP_VERSION: "{want}"' not in cfg:
    sys.exit(f"deploy/k3s/configmap.yaml COGNEVA_APP_VERSION != {want}")
print(f"    chart appVersion/version、values tag、k3s 清单 label 均为 {want}")
EOF

# 预渲染产物（元启动 apply 主路径，覆盖 k3s 单/多节点与 k8s 标准三 profile）
# 必须与 chart 同源新鲜，否则发出去的 release 带的是旧拓扑/旧版本
echo "==> 预渲染产物新鲜度门禁"
bash deploy/scripts/render-deploy.sh --check

BUILD_ARGS=(build -t "$IMAGE" -t "$VERSIONED_IMAGE" -f "$REPO_ROOT/Dockerfile")
BUILD_ARGS+=(--build-arg "VERSION=${VERSION}" --build-arg "GIT_REVISION=${GIT_REVISION}")
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
        --build-arg NPM_REGISTRY=https://registry.npmmirror.com
    )
fi
BUILD_ARGS+=("$REPO_ROOT")

echo "==> 构建 $IMAGE + $VERSIONED_IMAGE（v$VERSION，首次全量构建需 1-3 小时）"
buildah "${BUILD_ARGS[@]}"

mkdir -p "$DIST_DIR"
WORKDIR="$(mktemp -d /var/tmp/cogneva-release-XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "==> 导出镜像（docker-archive）"
# 归档内只带 :local 单标签（buildah docker-archive 不支持一次写多个引用）：
# 消费面——预渲染清单与 profile values——都 pin :local；导入后由 bootstrap
# （单节点快路径）或镜像分发器 DaemonSet（多节点）retag 出不可变版本 tag
# localhost/cogneva:$VERSION 供追溯与按版本引用。版本溯源同时有镜像内
# OCI LABEL（version/revision）与二进制 --version 兜底。
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
