#!/usr/bin/env bash
# cogneva K3s 快速换版：增量构建 → buildah 叠层 → :local 同步重打 → 导入 k3s → 三部署滚动
#
# 用法：
#   deploy/k3s/swap-image.sh <新版本号> [--prev <基镜像tag>] [--web] [--no-deploy]
#
# 关键不变式：deploy yaml 的 image 字段 pin 的是 localhost/cogneva:local，
# 换版必须同步把 :local 重打到新版本（宿主 buildah + 集群 containerd 两侧），
# 否则任何人 kubectl apply 一下 yaml 就会把线上镜像静默打回旧版（2026-08-06 透传 404 事故根因）。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS=cogneva
IMAGE=localhost/cogneva
NEW_TAG=""
PREV_TAG=""
BUILD_WEB=0
DO_DEPLOY=1

while [ $# -gt 0 ]; do
  case "$1" in
    --prev) PREV_TAG="$2"; shift 2 ;;
    --web) BUILD_WEB=1; shift ;;
    --no-deploy) DO_DEPLOY=0; shift ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) NEW_TAG="$1"; shift ;;
  esac
done

if [ -z "$NEW_TAG" ]; then
  echo "用法: $0 <新版本号> [--prev <基镜像tag>] [--web] [--no-deploy]" >&2
  exit 1
fi

# 基镜像默认取线上 cogneva 部署当前版本（保证叠在最新一层上）
if [ -z "$PREV_TAG" ]; then
  PREV_TAG="$(kubectl get deploy cogneva -n "$NS" -o jsonpath='{.spec.template.spec.containers[?(@.name=="cogneva")].image}' | sed "s|^${IMAGE}:||")"
  PREV_TAG="${PREV_TAG:-local}"
fi
echo "==> 基镜像 ${IMAGE}:${PREV_TAG} → 新版本 ${IMAGE}:${NEW_TAG}"

cd "$REPO_ROOT"

if [ "$BUILD_WEB" = 1 ]; then
  echo "==> 构建 web 前端"
  (cd web && npm run build)
fi

echo "==> 增量构建 release 二进制（CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}）"
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" cargo build --release --bin cogneva
strip target/release/cogneva

echo "==> buildah 叠层 ${PREV_TAG} → ${NEW_TAG}"
CTR="$(buildah from "${IMAGE}:${PREV_TAG}")"
trap 'buildah rm "$CTR" >/dev/null 2>&1 || true' EXIT
buildah copy "$CTR" target/release/cogneva /opt/cogneva/cogneva
buildah copy "$CTR" crates/cog-storage/migrations /opt/cogneva/crates/cog-storage/migrations
if [ -f web/dist/index.html ]; then
  # web/dist 是 git 忽略的构建产物——拷的是磁盘现状，要新鲜前端先加 --web
  buildah copy "$CTR" web/dist /opt/cogneva/web
fi
buildah commit "$CTR" "${IMAGE}:${NEW_TAG}"
buildah rm "$CTR" >/dev/null
trap - EXIT

echo "==> 同步重打 :local（宿主侧）"
buildah tag "${IMAGE}:${NEW_TAG}" "${IMAGE}:local"

TAR="/tmp/cogneva-${NEW_TAG}.tar"
echo "==> 导出并导入 k3s（tar 已存在会报 modifying existing images，先删）"
rm -f "$TAR"
buildah push "${IMAGE}:${NEW_TAG}" "docker-archive:${TAR}:${IMAGE}:${NEW_TAG}"
k3s ctr -n k8s.io images import "$TAR"
rm -f "$TAR"
# 集群内 :local 也要指向新版本（evolution initContainer 与 yaml pin 都用 :local）
k3s ctr -n k8s.io images tag --force "${IMAGE}:${NEW_TAG}" "${IMAGE}:local"

if [ "$DO_DEPLOY" = 1 ]; then
  echo "==> 三部署滚动到 ${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva "cogneva=${IMAGE}:${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva-evolution "cogneva=${IMAGE}:${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva-security-gateway "security-gateway=${IMAGE}:${NEW_TAG}"
  kubectl rollout status -n "$NS" deployment/cogneva --timeout=180s
  kubectl rollout status -n "$NS" deployment/cogneva-security-gateway --timeout=180s
  kubectl rollout status -n "$NS" deployment/cogneva-evolution --timeout=300s
fi

echo "==> 完成：${IMAGE}:${NEW_TAG}（:local 已同步）"
