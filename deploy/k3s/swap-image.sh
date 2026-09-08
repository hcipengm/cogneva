#!/usr/bin/env bash
# cogneva K3s 快速换版：增量构建 → buildah 叠层 → 版本 tag + :local 双标签 →
# 导入 k3s → 四部署滚动 → 滚动后校验实际运行镜像。
#
# 用法：
#   deploy/k3s/swap-image.sh [版本号] [--prev <基镜像tag>] [--web] [--no-deploy]
#   版本号缺省取 Cargo.toml workspace version（镜像版本与代码版本单一同源）。
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
#     脚本启动即校验节点 :local 与运行镜像一致，脱节则拒绝执行并给止血命令。
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

while [ $# -gt 0 ]; do
  case "$1" in
    --prev) PREV_TAG="$2"; shift 2 ;;
    --web) BUILD_WEB=1; shift ;;
    --no-deploy) DO_DEPLOY=0; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) NEW_TAG="$1"; shift ;;
  esac
done

cd "$REPO_ROOT"

# 版本号单一源：Cargo.toml workspace.package.version
WORKSPACE_VERSION="$(grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')"
[ -n "$NEW_TAG" ] || NEW_TAG="$WORKSPACE_VERSION"
echo "==> 新版本 ${IMAGE}:${NEW_TAG}（workspace version ${WORKSPACE_VERSION}）"

# git revision 注入二进制（build.rs 也会自查 git，显式传双保险）
GIT_REVISION="$(git rev-parse --short HEAD)$(git status --porcelain | grep -q . && echo -dirty || true)"

# 输出线上 Ready 主 Pod 的 <name> <imageID>，没有则空
running_pod_info() {
  kubectl -n "$NS" get pods -l app.kubernetes.io/name=cogneva -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for p in d.get("items",[]):
    for cs in p.get("status",{}).get("containerStatuses",[]):
        if cs.get("name")=="cogneva" and cs.get("ready"):
            print(p["metadata"]["name"], cs.get("imageID","").rsplit(":",1)[-1])
            sys.exit(0)
'
}

# 线上 Ready Pod 实际运行镜像对应的不可变版本 tag（非 local）
resolve_running_tag() {
  local info pod image_id inspect tag
  info="$(running_pod_info)"
  [ -n "$info" ] || {
    echo "找不到 Ready 的 cogneva Pod（集群未部署？用 --no-deploy 并显式 --prev）" >&2
    return 1; }
  pod="${info%% *}"; image_id="${info##* }"
  inspect="$(k3s crictl inspecti "$image_id" 2>/dev/null)" || {
    echo "集群 containerd 查不到 Pod $pod 的运行镜像 ${image_id:0:12}" >&2; return 1; }
  tag="$(printf '%s' "$inspect" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for t in d.get("status",{}).get("repoTags") or []:
    p="localhost/cogneva:"
    if t.startswith(p):
        v=t[len(p):]
        if v != "local":
            print(v); break
')"
  [ -n "$tag" ] || {
    echo "Pod $pod 运行镜像只有 :local 标签、无不可变版本 tag；请显式 --prev" >&2
    return 1; }
  printf '%s' "$tag"
}

# 校验浮动 :local 与线上运行镜像一致；脱节直接拒绝（叠层会把错误自我放大）
verify_local_matches_running() {
  local running_id local_id
  running_id="$(running_pod_info | awk '{print $2}')"
  local_id="$(k3s crictl inspecti "${IMAGE}:local" 2>/dev/null | python3 -c '
import json,sys
try:
    print(json.load(sys.stdin)["status"]["id"].rsplit(":",1)[-1])
except Exception:
    pass
' || true)"
  [ -n "$running_id" ] || { echo "线上没有 Ready 的 cogneva Pod" >&2; return 1; }
  if [ -z "$local_id" ]; then
    echo "集群 containerd 没有 ${IMAGE}:local 标签（首次部署？）" >&2
    return 1
  fi
  if [ "$running_id" != "$local_id" ]; then
    echo ":local 与线上运行镜像脱节！" >&2
    echo "  运行中: ${running_id:0:12}（版本 tag: ${PREV_TAG:-未知}）" >&2
    echo "  :local: ${local_id:0:12}" >&2
    echo "此刻叠层会把旧镜像当基底；任何 apply/rollout 也会把线上打回旧版。" >&2
    echo "止血后重跑本脚本：" >&2
    echo "  k3s ctr -n k8s.io images tag --force ${IMAGE}:<运行版本tag> ${IMAGE}:local" >&2
    echo "  k3s ctr -n k8s.io images push --plain-http localhost:30500/cogneva:local ${IMAGE}:local" >&2
    return 1
  fi
  echo "==> :local 与运行镜像一致（${local_id:0:12}）"
}

if [ "$DO_DEPLOY" = 1 ]; then
  [ -n "$PREV_TAG" ] || PREV_TAG="$(resolve_running_tag)"
  verify_local_matches_running
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

echo "==> 增量构建 release 二进制（CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}）"
COGNEVA_GIT_REVISION="$GIT_REVISION" CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" \
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
  # 集群内镜像仓库（金丝雀 overlay 秒级分发通道），随换版幂等落地存量集群
  kubectl apply -f deploy/k3s/cluster-registry.yaml

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

  echo "==> 校验新 Pod 实际运行镜像"
  want_id="$(k3s crictl inspecti "${IMAGE}:${NEW_TAG}" 2>/dev/null | python3 -c '
import json,sys
print(json.load(sys.stdin)["status"]["id"].rsplit(":",1)[-1])')"
  # 四部署逐一核对 Ready Pod 的镜像 id。同版本号重跑时 tag 字符串没变，
  # set image 不触发滚动；只核对单个 Pod 会漏掉其余部署——主应用还可能因
  # 前面 kubectl apply 结构变更偶然重建而"假通过"，evolution 等却仍跑旧
  # 镜像。四个 component 标签唯一，必须全查。stdout 输出仍停留在旧镜像的
  # component 列表（空 = 全部已更新），诊断信息走 stderr。
  stale_deploys() {
    kubectl -n "$NS" get pods -o json 2>/dev/null | WANT="$want_id" python3 -c '
import json, os, sys
want = os.environ["WANT"]
comps = ["gateway", "security-gateway", "evolution", "sandbox-executor"]
data = json.load(sys.stdin)
seen = {}
for p in data.get("items", []):
    comp = p.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
    if comp not in comps:
        continue
    for cs in p.get("status", {}).get("containerStatuses", []):
        if cs.get("ready"):
            seen.setdefault(comp, cs.get("imageID", "").rsplit(":", 1)[-1])
stale = [c for c in comps if seen.get(c) != want]
for c in comps:
    rid = seen.get(c, "MISSING")[:12]
    print(f"    {c:18s} {rid}", file=sys.stderr)
if stale:
    print(" ".join(stale))
'
  }
  stale="$(stale_deploys)"
  if [ -n "$stale" ]; then
    # 同版本号重跑（tag 字符串没变）时 set image 不触发滚动，Pod 仍跑旧镜像
    # ID；强制重建让 :<tag> 解析到新 ID。
    echo "==> 以下部署仍跑旧镜像（同版本重跑未触发滚动），强制重启：${stale}"
    kubectl -n "$NS" rollout restart deployment/cogneva deployment/cogneva-evolution \
      deployment/cogneva-security-gateway deployment/cogneva-sandbox-executor
    kubectl rollout status -n "$NS" deployment/cogneva --timeout=180s
    kubectl rollout status -n "$NS" deployment/cogneva-security-gateway --timeout=180s
    kubectl rollout status -n "$NS" deployment/cogneva-evolution --timeout=300s
    kubectl rollout status -n "$NS" deployment/cogneva-sandbox-executor --timeout=180s
    stale="$(stale_deploys)"
  fi
  if [ -n "$stale" ]; then
    echo "滚动后仍有部署镜像不符（期望 ${want_id:0:12}）：${stale}" >&2
    exit 1
  fi
  echo "==> 四部署均运行 ${IMAGE}:${NEW_TAG}（${want_id:0:12}，rev ${GIT_REVISION}）"
fi

echo "==> 完成：${IMAGE}:${NEW_TAG}（节点与 registry 的 :local 均已同步，rev ${GIT_REVISION}）"
