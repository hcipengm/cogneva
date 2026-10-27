#!/usr/bin/env bash
# cogneva K3s 快速换版：增量构建 → buildah 叠层 → 版本 tag + :local 双标签 →
# 导入 k3s → 换镜像面滚动 → 滚动后校验实际运行镜像。
#
# 用法：
#   deploy/k3s/swap-image.sh [版本号] [--prev <基镜像tag>] [--web] [--no-deploy]
#   [--check-only] [--list-workloads]
#   版本号缺省取 Cargo.toml workspace version（镜像版本与代码版本单一同源）。
#   --check-only：只跑启动前校验（registry 的 :local vs 线上运行镜像），
#   不构建、不 apply——校验本身要能在不动线上时被单独读一次。
#   --list-workloads：只印出这一趟会滚哪些工作负载（kind/名字/容器），不构建、
#   不碰集群。这一份是从交付树读出来的，所以它能被单独读一次、也能被单独测。
#
# 版本与标签不变式：
#   - deploy yaml pin 的是集群内 registry 浮动签 localhost:30500/cogneva:local；
#     每次换版必须在 apply/滚动之前把新镜像（不可变版本 tag + :local）推进
#     registry，否则任何人 kubectl apply 一下 yaml（或 GitOps 拉取端 apply）
#     就会让线上拉回 registry 里 :local 指向的旧镜像（2026-08-06 透传 404
#     事故根因；ctr tag 出的节点本地引用名 kubelet 不一定采信，故权威只放
#     registry）。播种失败直接中止——此刻线上未动，安全失败。
#   - 叠层基镜像永远取"线上 Ready Pod 实际运行镜像"对应的不可变版本 tag，
#     绝不基于 :local 叠层——:local 一旦与运行版本脱节，叠层会把错误镜像
#     当基底自我放大（2026-09-04 :local 指向一个多月前老镜像的事故根因）。
#     脚本启动即校验 registry 里的 :local 与运行镜像一致，脱节则拒绝执行并给
#     止血命令。判据必须读 registry 那一份：跑本镜像的工作负载 pin 的是
#     localhost:30500/cogneva:local 且 imagePullPolicy: Always，kubelet 每次
#     启动都从 registry 拉，节点本地 :local 只作暖缓存/离线回退，不在运行路径上
#     ——按它判会把运行面已经对齐的换版拒掉，而印出的止血命令修的是那个不参与
#     运行的面。故节点 :local 不一致只告警。
#   - 镜像带 OCI LABEL（version/revision），二进制内嵌 git sha（--version），
#     线上版本可直接追溯到 commit，不靠标签记忆。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS=cogneva
IMAGE=localhost/cogneva
NEW_TAG=""
PREV_TAG=""
BUILD_WEB=0
DO_DEPLOY=1
CHECK_ONLY=0
LIST_WORKLOADS=0

while [ $# -gt 0 ]; do
  case "$1" in
    --prev) PREV_TAG="$2"; shift 2 ;;
    --web) BUILD_WEB=1; shift ;;
    --no-deploy) DO_DEPLOY=0; shift ;;
    --check-only) CHECK_ONLY=1; DO_DEPLOY=0; shift ;;
    --list-workloads) LIST_WORKLOADS=1; shift ;;
    # 头部注释区块整体就是帮助文本：按内容取，不按行号取
    -h|--help) awk 'NR>1 && /^set -euo/{exit} NR>1' "$0"; exit 0 ;;
    *) NEW_TAG="$1"; shift ;;
  esac
done

cd "$REPO_ROOT"

# 换镜像面：哪些工作负载跑着本项目的二进制，由交付树回答（见
# deploy/scripts/platform-image-workloads.sh），不在这里列名字。镜像被不止四个
# Deployment 用着：宿主账本 / 进程普查 / 声明仓是 DaemonSet，注册仓的卷走查是
# 边车，redis 的 AOF 修复是 initContainer。写死一份四个名字的名单，它们不在
# 这一趟滚动里，而脚本末尾的「均运行新镜像」照样成立——它们的「已更新」与
# 「根本没查」在读数上分不开，于是旧二进制可以一直跑下去。
#
# 取的是**常驻容器**：initContainer 只在新 Pod 起来时跑一次，它跟着所属工作负载
# 一起换，单独 set image 反而会为了一个不常驻的进程重启整个 StatefulSet（redis
# 就是这样）。CronJob 也不进来：它每次起 Pod 都按浮动签 :local + Always 拉，
# 下一次运行自然就是新镜像，没有要滚的东西。
#
# 镜像仓库名用不上 IMAGE（那是节点本地 tag localhost/cogneva）：清单 pin 的是
# 集群内 registry 引用 localhost:30500/cogneva，两者都认。清单里一个都认不出来时
# 读出的集合是空的，于是这一趟判不了——而不是悄悄换掉另一批工作负载。
platform_image_workloads() {
  bash "$REPO_ROOT/deploy/scripts/platform-image-workloads.sh"
}

if [ "$LIST_WORKLOADS" = 1 ]; then
  platform_image_workloads || exit 2
  exit 0
fi

# One source for the version: Cargo.toml's workspace.package.version, read only
# through declared-version.sh
WORKSPACE_VERSION="$(bash "$REPO_ROOT/deploy/scripts/declared-version.sh")"
[ -n "$NEW_TAG" ] || NEW_TAG="$WORKSPACE_VERSION"
echo "==> 新版本 ${IMAGE}:${NEW_TAG}（workspace version ${WORKSPACE_VERSION}）"

# git revision 注入二进制（build.rs 也会自查 git，显式传双保险）
GIT_REVISION="$(git rev-parse --short HEAD)$(git status --porcelain | grep -q . && echo -dirty || true)"
# The derived label: declared version plus commits since the nearest release plus
# the rev. Injected alongside the rev; without it a new image can only report the
# bare declared version and cannot tell "the 0.5.8 release" from "the 93rd commit
# after 0.5.8".
VERSION_ID="$(bash "$REPO_ROOT/deploy/scripts/version-id.sh")"

# 输出线上 Ready 主 Pod 的 <name> <imageID>（完整 repo@sha256:manifest 引用），没有则空
running_pod_info() {
  kubectl -n "$NS" get pods -l app.kubernetes.io/name=cogneva -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for p in d.get("items",[]):
    for cs in p.get("status",{}).get("containerStatuses",[]):
        if cs.get("name")=="cogneva" and cs.get("ready"):
            print(p["metadata"]["name"], cs.get("imageID",""))
            sys.exit(0)
'
}

# 线上 Ready Pod 实际运行镜像对应的不可变版本 tag（非 local）。
# imageID 是 repo@sha256:manifest 引用：crictl 不认裸 digest，完整引用才能查。
resolve_running_tag() {
  local info pod image_id inspect tag
  info="$(running_pod_info)"
  [ -n "$info" ] || {
    echo "找不到 Ready 的 cogneva Pod（集群未部署？用 --no-deploy 并显式 --prev）" >&2
    return 1; }
  pod="${info%% *}"; image_id="${info##* }"
  inspect="$(k3s crictl inspecti "$image_id" 2>/dev/null)" || {
    echo "集群 containerd 查不到 Pod $pod 的运行镜像 ${image_id}" >&2; return 1; }
  tag="$(printf '%s' "$inspect" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for t in d.get("status",{}).get("repoTags") or []:
    p=("localhost/cogneva:", "localhost:30500/cogneva:")
    for pre in p:
        if t.startswith(pre):
            v=t[len(pre):]
            if v != "local":
                print(v); sys.exit(0)
')"
  [ -n "$tag" ] || {
    echo "Pod $pod 运行镜像只有 :local 标签、无不可变版本 tag；请显式 --prev" >&2
    return 1; }
  printf '%s' "$tag"
}

# Pod imageID 是 registry 拉取侧的 manifest digest（repo@sha256:...），而
# containerd CRI 的 image id 是 config digest——两种 digest 直接比永远不相等。
# 统一折算到 config digest 再比：manifest 经 registry API 解出 config。
resolve_config_digest() {
  local iid="$1"
  case "$iid" in
    *@sha256:*)
      local host name manifest
      host="${iid%%/*}"
      name="${iid#*/}"; name="${name%@sha256:*}"
      manifest="${iid##*@}"
      curl -sf -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
        "http://${host}/v2/${name}/manifests/${manifest}" | python3 -c '
import json,sys
print(json.load(sys.stdin)["config"]["digest"].rsplit(":",1)[-1])
'
      ;;
    sha256:*) printf '%s' "${iid#sha256:}" ;;
    *) printf '%s' "$iid" ;;
  esac
}

# registry 里某个 tag 的 config digest（与 resolve_config_digest / 滚动后校验同源折算）
registry_config_digest() {
  local tag="$1"
  curl -sf -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://localhost:30500/v2/cogneva/manifests/${tag}" | python3 -c '
import json,sys
print(json.load(sys.stdin)["config"]["digest"].rsplit(":",1)[-1])
'
}

# 启动前校验：registry 里的浮动 :local 与线上运行镜像一致；脱节直接拒绝
# （apply/滚动会把线上打回旧版）。
#
# 判据面是 registry 那一份，不是节点本地 tag：跑本镜像的工作负载 pin localhost:30500/cogneva:local
# 且 imagePullPolicy: Always，kubelet 每次启动都从 registry 拉。节点本地 :local 只作
# 暖缓存/离线回退，与运行镜像不一致不影响运行时；拿它当判据的方向是误报脱节——
# 运行面已经对齐也照样拒绝换版，而印出的止血命令修的是那个不参与运行的面。
verify_the_pin_matches_running() {
  local running_ref running_id registry_id node_id running_tag
  running_ref="$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=cogneva -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for p in d.get("items",[]):
    for cs in p.get("status",{}).get("containerStatuses",[]):
        if cs.get("name")=="cogneva" and cs.get("ready"):
            print(cs.get("imageID",""))
            sys.exit(0)
')"
  [ -n "$running_ref" ] || { echo "线上没有 Ready 的 cogneva Pod" >&2; return 1; }
  running_id="$(resolve_config_digest "$running_ref")"
  [ -n "$running_id" ] || { echo "运行镜像 $running_ref 折算 config digest 失败" >&2; return 1; }
  registry_id="$(registry_config_digest local || true)"
  [ -n "$registry_id" ] || {
    echo "registry 里读不到 localhost:30500/cogneva:local（registry 没起或没播种？）" >&2
    return 1; }
  if [ "$running_id" != "$registry_id" ]; then
    # 运行版本 tag：优先用调用方给的 --prev（同一件事的显式说法），否则现场解析
    running_tag="${PREV_TAG:-$(resolve_running_tag 2>/dev/null || true)}"
    echo ":local 与线上运行镜像脱节！" >&2
    echo "  运行中: ${running_id:0:12}（版本 tag: ${running_tag:-未知}）" >&2
    echo "  registry :local: ${registry_id:0:12}" >&2
    echo "跑本镜像的工作负载 pin 的是 localhost:30500/cogneva:local 且 imagePullPolicy: Always——" >&2
    echo "此刻任何 apply/rollout 都会把线上打回旧版。" >&2
    echo "止血后重跑本脚本：" >&2
    echo "  buildah push --tls-verify=false ${IMAGE}:${running_tag:-<运行版本tag>} localhost:30500/cogneva:local" >&2
    return 1
  fi
  echo "==> registry :local 与运行镜像一致（${registry_id:0:12}）"

  # 节点本地 tag 只是暖缓存/离线回退：不同步是换版末尾 import + tag 自己会修的事。
  # 它一旦被当成判据，就会拒绝一个运行面已经对齐的换版，故这里只告警不阻断。
  node_id="$(k3s crictl inspecti "${IMAGE}:local" 2>/dev/null | python3 -c '
import json,sys
try:
    print(json.load(sys.stdin)["status"]["id"].rsplit(":",1)[-1])
except Exception:
    pass
' || true)"
  if [ -n "$node_id" ] && [ "$node_id" != "$running_id" ]; then
    echo "==> 注意：节点本地 ${IMAGE}:local（${node_id:0:12}）与运行镜像不同；它只作暖缓存/离线回退，本趟末尾的 import + tag 会同步" >&2
  fi
}

if [ "$CHECK_ONLY" = 1 ]; then
  verify_the_pin_matches_running
  echo "==> 启动前校验通过（未构建、未 apply）"
  exit 0
fi

if [ "$DO_DEPLOY" = 1 ]; then
  [ -n "$PREV_TAG" ] || PREV_TAG="$(resolve_running_tag)"
  verify_the_pin_matches_running
else
  [ -n "$PREV_TAG" ] || { echo "--no-deploy 模式必须显式 --prev <基镜像tag>" >&2; exit 1; }
fi

# 叠层在宿主 buildah 存储发生，基镜像版本 tag 必须在宿主侧存在
buildah images -q "${IMAGE}:${PREV_TAG}" | grep -q . || {
  echo "宿主 buildah 存储缺基镜像 ${IMAGE}:${PREV_TAG}。" >&2
  echo "可从集群导回: k3s ctr -n k8s.io images export /tmp/prev.tar ${IMAGE}:${PREV_TAG} \\" >&2
  echo "             && buildah pull docker-archive:/tmp/prev.tar" >&2
  exit 1; }
echo "==> 基镜像 ${IMAGE}:${PREV_TAG} → 新版本 ${IMAGE}:${NEW_TAG}（rev ${GIT_REVISION}）"

if [ "$BUILD_WEB" = 1 ]; then
  echo "==> 构建 web 前端"
  (cd web && npm run build)
fi

# 与 chart 的 mainlineDeployer.cargoBuildJobs 同一个数、同一条推导：宿主 24 核，
# 这份构建独占一台宿主时没有别的界可撞，2 只是旧机上留下的值（实测它让一次构建
# 跑 33 分钟，而构建期间上游就落四笔）。
echo "==> 增量构建 release 二进制（CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-6}）"
COGNEVA_GIT_REVISION="$GIT_REVISION" COGNEVA_VERSION_ID="$VERSION_ID" \
  CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-6}" \
  cargo build --release --bin cogneva
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
# 老基镜像没有。叠层流补装；全量构建由 Dockerfile runtime stage 保证。
# CN 模式下 ubuntu 24.04 的 deb822 源换成 TUNA，避免 archive.ubuntu.com 超时。
if ! buildah run "$CTR" -- python3 --version >/dev/null 2>&1; then
  echo "==> 基镜像缺 python3，叠层补装"
  buildah run --user root -e "COGNEVA_CN_MIRROR=${COGNEVA_CN_MIRROR:-0}" "$CTR" -- sh -c '
    if [ "${COGNEVA_CN_MIRROR:-0}" = "1" ]; then
      sed -i -e "s|//archive.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             -e "s|//security.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
    fi
    apt-get update && apt-get install -y --no-install-recommends python3 \
      && rm -rf /var/lib/apt/lists/*'
fi
# buildah 是 GitOps publisher 打金丝雀 overlay 镜像的硬依赖（仅特权进化
# Pod 用；主应用 Pod 非特权装了也用不了）。老基镜像（Dockerfile runtime
# 补装前的全量构建）没有，叠层流补装；全量构建由 Dockerfile 保证。
# buildah 在 ubuntu 24.04 的 universe 源，官方基础镜像可能只开 main，
# 先显式补齐 deb822 Components；CN 模式同步换 TUNA。
if ! buildah run "$CTR" -- buildah --version >/dev/null 2>&1; then
  echo "==> 基镜像缺 buildah，叠层补装（启用 universe 源）"
  buildah run --user root -e "COGNEVA_CN_MIRROR=${COGNEVA_CN_MIRROR:-0}" "$CTR" -- sh -c '
    if [ "${COGNEVA_CN_MIRROR:-0}" = "1" ]; then
      sed -i -e "s|//archive.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             -e "s|//security.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
    fi
    sed -i "s/^Components: main.*/Components: main restricted universe multiverse/" \
      /etc/apt/sources.list.d/*.sources 2>/dev/null || true
    apt-get update && apt-get install -y --no-install-recommends buildah \
      && rm -rf /var/lib/apt/lists/*'
fi
# 原生构建依赖：沙盒内自进化/主线跟踪的 cargo build 要编译完整依赖树——
# openssl-sys 的 build script 靠 pkg-config 找 libssl-dev 头文件与链接符号，
# etcd-client 的 build.rs 用 tonic-build 编 .proto，容器内必须有 protoc；
# buildah run 在叠层内执行 --version 校验需要 OCI 运行时 crun（build/copy
# /push 不需要，所以缺它到主线部署才暴露 "exec: no command"）。
# 只装 libssl3 运行库时整个构建在 openssl-sys 处必挂。老基镜像（Dockerfile
# runtime stage 补装前）缺这些，叠层流补装；全量构建由 Dockerfile 保证。
# protobuf-compiler/crun 在 universe 源，显式补齐 Components。
if ! buildah run "$CTR" -- sh -c 'command -v pkg-config >/dev/null && pkg-config --exists openssl && command -v protoc >/dev/null && command -v crun >/dev/null' 2>/dev/null; then
  echo "==> 基镜像缺原生构建依赖（pkg-config/libssl-dev/protobuf-compiler/crun），叠层补装"
  buildah run --user root -e "COGNEVA_CN_MIRROR=${COGNEVA_CN_MIRROR:-0}" "$CTR" -- sh -c '
    if [ "${COGNEVA_CN_MIRROR:-0}" = "1" ]; then
      sed -i -e "s|//archive.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             -e "s|//security.ubuntu.com|//mirrors.tuna.tsinghua.edu.cn|" \
             /etc/apt/sources.list /etc/apt/sources.list.d/*.sources 2>/dev/null || true
    fi
    sed -i "s/^Components: main.*/Components: main restricted universe multiverse/" \
      /etc/apt/sources.list.d/*.sources 2>/dev/null || true
    apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libssl-dev protobuf-compiler crun \
      && rm -rf /var/lib/apt/lists/*'
fi
# cargo sparse 镜像配置：沙盒内自进化/主线跟踪构建直接跑 cargo，
# 缺 config 会直连 crates.io，受限网络下稀疏索引拉取慢到构建超时。
# 叠层自愈（全量构建由 Dockerfile runtime stage 保证）。rsproxy 索引与
# crate 文件都自托管（TUNA 的 crate 文件回源 static.crates.io，国内必挂）。
if [ "${COGNEVA_CN_MIRROR:-0}" = "1" ] && ! buildah run "$CTR" -- test -s /usr/local/cargo/config.toml 2>/dev/null; then
  echo "==> 写入 cargo sparse 镜像配置（rsproxy）"
  buildah run --user root "$CTR" -- sh -c '
    mkdir -p /usr/local/cargo &&
    printf "[source.crates-io]\nreplace-with = \"mirror\"\n[source.mirror]\nregistry = \"sparse+https://rsproxy.cn/index/\"\n\n[http]\nmultiplexing = false\n\n[net]\nretry = 10\n" > /usr/local/cargo/config.toml'
fi
if [ -f web/dist/index.html ]; then
  # web/dist 是 git 忽略的构建产物——拷的是磁盘现状，要新鲜前端先加 --web
  buildah copy "$CTR" web/dist /opt/cogneva/web
fi
# 换版即验证：新二进制必须能自报版本与 revision（追溯性的运行时证据）
buildah run "$CTR" -- /opt/cogneva/cogneva --version
# OCI label 经 config 写进容器配置再 commit（buildah commit 无 --label flag）。
buildah config \
  --label "org.opencontainers.image.version=${NEW_TAG}" \
  --label "org.opencontainers.image.revision=${GIT_REVISION}" \
  "$CTR"
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
# 节点本地 :local 同步到新版本（暖缓存/离线回退；运行时权威是集群内 registry）
k3s ctr -n k8s.io images tag --force "${IMAGE}:${NEW_TAG}" "${IMAGE}:local"

if [ "$DO_DEPLOY" = 1 ]; then
  # Registry 播种必须先于 apply：静态清单 pin 的是 localhost:30500/cogneva:local，
  # 不先前移浮动签，apply 会让线上拉回 registry 里的旧镜像。版本 tag 一并推送
  # （按版本引用/追溯）。任一失败立即中止——此时尚未 apply/滚动，线上不受影响。
  echo "==> 播种集群内 registry（${NEW_TAG} + :local）"
  kubectl -n "$NS" wait --for=condition=Available deployment/cogneva-registry --timeout=180s
  buildah push --tls-verify=false "${IMAGE}:${NEW_TAG}" "localhost:30500/cogneva:${NEW_TAG}"
  buildah push --tls-verify=false "${IMAGE}:local" "localhost:30500/cogneva:local"
  echo "    registry 已更新（localhost:30500）"

  # 结构性变更随版本滚动（幂等 apply）：GitOps 拉取端 RBAC、进化配置、
  # 主部署（GitOps env/git-remote 挂载/prompts 挂载等）。apply 用的是
  # 仓库 yaml 的 registry :local pin，该浮动签已在上方前移到新版本，
  # 不会打回旧版。
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
  kubectl apply -f deploy/k3s/sandbox-executor-service.yaml
  # 集群内镜像仓库（金丝雀 overlay 秒级分发通道），随换版幂等落地存量集群
  kubectl apply -f deploy/k3s/cluster-registry.yaml

  # prompts configmap 随换版重建：挂载的旧 prompts 会遮蔽新镜像的更新，
  # 每次换版从仓库 prompts/ 全量刷新（与 GitOps 拉取端 L0/L1 重建同源）
  echo "==> 重建 cogneva-prompts configmap"
  kubectl create configmap cogneva-prompts -n "$NS" \
    --from-file=prompts/ --dry-run=client -o yaml | kubectl apply -f -

  # 工作负载按拓扑约定 pin 集群内 registry 引用（localhost:30500），且
  # imagePullPolicy: Always——必须用 registry 引用滚动，写节点本地
  # localhost/cogneva:<tag> 会让 kubelet 去 https://localhost 拉取直接失败。
  workloads="$(platform_image_workloads)" || {
    echo "换镜像面的工作负载读不出来（理由见上），拒绝在判不了该滚谁时动集群" >&2
    exit 1
  }
  echo "==> 换镜像面滚动到 localhost:30500/cogneva:${NEW_TAG}（$(printf '%s\n' "$workloads" | wc -l) 个容器）"
  while IFS=$'\t' read -r workload_kind workload_name container_name; do
    [ -n "$workload_kind" ] || continue
    kubectl set image -n "$NS" \
      "$(printf '%s' "$workload_kind" | tr 'A-Z' 'a-z')/${workload_name}" \
      "${container_name}=localhost:30500/cogneva:${NEW_TAG}"
  done <<<"$workloads"
  # 一个工作负载只等一次：同一份清单里两个容器指向同一对象时，第二条 rollout
  # status 等的是同一个滚动，读起来却像是两个各自独立的证据。
  while IFS=$'\t' read -r workload_kind workload_name; do
    [ -n "$workload_kind" ] || continue
    kubectl rollout status -n "$NS" \
      "$(printf '%s' "$workload_kind" | tr 'A-Z' 'a-z')/${workload_name}" --timeout=300s
  done < <(printf '%s\n' "$workloads" | cut -f1,2 | sort -u)

  echo "==> 校验新 Pod 实际运行镜像"
  # want 侧从 registry 取 NEW_TAG 的 manifest 折算 config digest；Pod imageID
  # 是 manifest digest，两侧统一经 resolve_config_digest 折算再比（同
  # verify_local_matches_running 的 digest 种类对齐）。
  want_id="$(curl -sf \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://localhost:30500/v2/cogneva/manifests/${NEW_TAG}" | python3 -c '
import json,sys
print(json.load(sys.stdin)["config"]["digest"].rsplit(":",1)[-1])
')"
  [ -n "$want_id" ] || { echo "registry 查询 localhost:30500/cogneva:${NEW_TAG} manifest 失败" >&2; exit 1; }
  # 核对的是命名空间里**每一个**跑着本镜像的 Ready 常驻容器，不是四个
  # component 标签：DaemonSet 与边车跑的是同一个二进制，按标签点名的四个看不见
  # 它们，于是「四部署均运行」与「其余那些没查」在读数上分不开。同版本号重跑时
  # tag 字符串没变，set image 不触发滚动，所以这里读的是容器实际在跑的 digest。
  #
  # 排掉两种 Pod：正在删除的（滚动收尾阶段旧 Pod 仍 Ready，读它会得到一份
  # 已经过期的旧镜像读数），以及 Job 起的（CronJob 每次起 Pod 按浮动签拉，
  # 它跑旧镜像不代表工作负载没换，把它算进来会触发一次没有理由的全量重启）。
  # stdout 仍只留结果（空 = 全部已更新），逐行读数走 stderr。
  running_image_digests() {
    kubectl -n "$NS" get pods -o json 2>/dev/null | python3 -c '
import json,sys
REPOS=("localhost:30500/cogneva","localhost/cogneva")
def repo_of(image):
    image=image.split("@")[0]
    last=image.rsplit("/",1)[-1]
    return image.rsplit(":",1)[0] if ":" in last else image
def job_owned(pod):
    return any(ref.get("kind")=="Job" for ref in pod["metadata"].get("ownerReferences") or [])
d=json.load(sys.stdin)
for p in d.get("items",[]):
    if p["metadata"].get("deletionTimestamp") or job_owned(p):
        continue
    for cs in p.get("status",{}).get("containerStatuses",[]) or []:
        if cs.get("ready") and repo_of(cs.get("image","")) in REPOS:
            print("%s\t%s\t%s" % (p["metadata"]["name"], cs.get("name"), cs.get("imageID","")))
'
  }
  stale_containers() {
    local pod container iid rid stale=""
    while IFS=$'\t' read -r pod container iid; do
      [ -n "$pod" ] || continue
      if [ -z "$iid" ]; then
        rid="MISSING"
      else
        rid="$(resolve_config_digest "$iid")"
        [ -n "$rid" ] || rid="UNRESOLVED"
      fi
      printf '    %-30s %-18s %s\n' "$pod" "$container" "${rid:0:12}" >&2
      [ "$rid" = "$want_id" ] || stale="${stale:+$stale }${pod}/${container}"
    done < <(running_image_digests)
    printf '%s' "$stale"
  }
  stale="$(stale_containers)"
  if [ -n "$stale" ]; then
    # 同版本号重跑（tag 字符串没变）时 set image 不触发滚动，Pod 仍跑旧镜像
    # ID；强制重建让 :<tag> 解析到新 ID。重建面与滚动面是同一份从树里读出
    # 来的集合——写死四个名字时，DaemonSet 上那份旧镜像永远不会被这条兜底救到。
    echo "==> 以下容器仍跑旧镜像（同版本重跑未触发滚动），强制重启：${stale}"
    while IFS=$'\t' read -r workload_kind workload_name; do
      [ -n "$workload_kind" ] || continue
      kubectl -n "$NS" rollout restart \
        "$(printf '%s' "$workload_kind" | tr 'A-Z' 'a-z')/${workload_name}"
    done < <(printf '%s\n' "$workloads" | cut -f1,2 | sort -u)
    while IFS=$'\t' read -r workload_kind workload_name; do
      [ -n "$workload_kind" ] || continue
      kubectl rollout status -n "$NS" \
        "$(printf '%s' "$workload_kind" | tr 'A-Z' 'a-z')/${workload_name}" --timeout=300s
    done < <(printf '%s\n' "$workloads" | cut -f1,2 | sort -u)
    stale="$(stale_containers)"
  fi
  if [ -n "$stale" ]; then
    echo "滚动后仍有容器镜像不符（期望 ${want_id:0:12}）：${stale}" >&2
    exit 1
  fi
  echo "==> 换镜像面的容器均运行 localhost:30500/cogneva:${NEW_TAG}（${want_id:0:12}，rev ${GIT_REVISION}）"
fi

echo "==> 完成：${IMAGE}:${NEW_TAG}（节点与 registry 的 :local 均已同步，rev ${GIT_REVISION}）"
