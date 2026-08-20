#!/usr/bin/env bash
# cogneva K3s 快速换版：增量构建 → buildah 叠层 → :local 同步重打 → 导入 k3s → 四部署滚动
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
# kubectl 自愈层：GitOps 拉取端（主 Pod 内）apply L0 配置/金丝雀编排需要。
# 叠层流不经过全量构建，基镜像可能还没有 kubectl。首选宿主 k3s 二进制
# （多调用，拷为 kubectl 即用，版本与集群一致）；宿主没有就跳过——
# deployment.yaml 的 hostPath 文件挂载会在运行时兜底提供。
if ! buildah run "$CTR" -- test -x /usr/local/bin/kubectl 2>/dev/null; then
  if [ -x /usr/local/bin/k3s ]; then
    echo "==> 拷入宿主 k3s 作为镜像内 kubectl（多调用二进制）"
    buildah copy "$CTR" /usr/local/bin/k3s /usr/local/bin/kubectl
  else
    echo "==> 宿主无 k3s，kubectl 由部署清单 hostPath 挂载兜底，跳过镜像层"
  fi
fi
# python3 是工具执行面（sandbox-executor / run_command 写脚本再跑）的硬依赖，
# 老基镜像没有。叠层流补装；全量构建由 Containerfile.local 运行阶段保证。
# CN 模式下 ubuntu 24.04 的 deb822 源换成 TUNA，避免 archive.ubuntu.com 超时。
if ! buildah run "$CTR" -- python3 --version >/dev/null 2>&1; then
  echo "==> 基镜像缺 python3，叠层补装"
  buildah run -e "COGNEVA_CN_MIRROR=${COGNEVA_CN_MIRROR:-0}" "$CTR" --user root -- sh -c '
    if [ "${COGNEVA_CN_MIRROR:-0}" = "1" ]; then
      sed -i -e "s|//archive.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             -e "s|//security.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
    fi
    apt-get update && apt-get install -y --no-install-recommends python3 \
      && rm -rf /var/lib/apt/lists/*'
fi
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
  # 结构性变更随版本滚动（幂等 apply）：GitOps 拉取端 RBAC、进化配置、
  # 主部署（GitOps env/git-remote 挂载/prompts 挂载等）。apply 用的是
  # 仓库 yaml 的 :local pin，此时 :local 已重打到新版本，不会打回旧版。
  echo "==> 应用 GitOps RBAC / 进化 configmap / 主部署结构"
  kubectl apply -f deploy/k3s/gitops-puller-rbac.yaml
  kubectl apply -f deploy/k3s/evolution-configmap.yaml
  kubectl apply -f deploy/k3s/deployment.yaml
  # 主配置（sandbox_executor_url 等 system 段）随版本幂等 apply；
  # 下方 set image 触发的滚动会让新 Pod 启动时读到新值。
  kubectl apply -f deploy/k3s/cogneva-json-configmap.yaml
  # 沙箱执行器（第 5 Pod）：deployment+service 幂等 apply，主应用经
  # system.sandbox_executor_url 路由 run_command/read_file/write_file 到此。
  kubectl apply -f deploy/k3s/sandbox-executor-deployment.yaml

  # prompts configmap 随换版重建：挂载的旧 prompts 会遮蔽新镜像的更新，
  # 每次换版从仓库 prompts/ 全量刷新（与 GitOps 拉取端 L0/L1 重建同源）
  echo "==> 重建 cogneva-prompts configmap"
  kubectl create configmap cogneva-prompts -n "$NS" \
    --from-file=prompts/ --dry-run=client -o yaml | kubectl apply -f -

  echo "==> 四部署滚动到 ${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva "cogneva=${IMAGE}:${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva-evolution "cogneva=${IMAGE}:${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva-security-gateway "security-gateway=${IMAGE}:${NEW_TAG}"
  kubectl set image -n "$NS" deployment/cogneva-sandbox-executor "sandbox-executor=${IMAGE}:${NEW_TAG}"
  kubectl rollout status -n "$NS" deployment/cogneva --timeout=180s
  kubectl rollout status -n "$NS" deployment/cogneva-security-gateway --timeout=180s
  kubectl rollout status -n "$NS" deployment/cogneva-evolution --timeout=300s
  kubectl rollout status -n "$NS" deployment/cogneva-sandbox-executor --timeout=180s
fi

echo "==> 完成：${IMAGE}:${NEW_TAG}（:local 已同步）"
