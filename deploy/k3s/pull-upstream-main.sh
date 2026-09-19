#!/usr/bin/env bash
# 公版 GitHub main → 宿主工作仓库 → K3s git-remote bare 仓库的源头拉取。
#
# 【已被取代】集群内的主线跟踪部署器现在自己从各平台拉 main（配置
# self_evolution.mainline_deployer.upstreams，两端镜像都跟，经安全网关的
# git 透传面取，凭证只存在于网关）。这条链不再需要宿主机参与，正常部署
# 不要启用下面的 timer：两个写者同时推 bare 的 main 会互相打脸。
# 本脚本保留作无网/救援时的手动通道（宿主机是唯一能直连 GitHub SSH 的地方）。
#
# 历史：2026-09-08 的"20 个修复在 main 躺一天、集群仍跑旧镜像"事故，根因
# 就是当时 bare 只由 sync-git-remote.sh 从宿主工作树推送，而宿主工作树
# 不会自己从 GitHub 拉取——公版 main 前进后没有任何环节把它带进集群。
# 当时的补法是在宿主补一个定时器；现在这条源头搬进了集群内。
#
# 手动用法（仅在集群内跟踪不可用时）：
#   /root/omc_workspace/cogneva/deploy/k3s/pull-upstream-main.sh
# 配套 timer 已于 2026-09-19 停用（systemctl disable --now
# cogneva-upstream-pull.timer）；本脚本只是手动通道，别再把 timer 装回去。
#
# 安全约束：只快进，永不强推/reset；GitHub 走 SSH deploy key（本机 HTTPS
# 被墙、SSH 通）；任何失败只记日志不致命。
set -euo pipefail

SRC_REPO="${1:-/root/omc_workspace/cogneva}"
BARE="${2:-/var/lib/cogneva-data/git-remote}"
DEPLOY_KEY="${COGNEVA_DEPLOY_KEY:-/root/omc_workspace/backups/keys/cogneva_deploy_key}"
UPSTREAM_URL="${COGNEVA_UPSTREAM_URL:-git@github.com:hcipengm/cogneva.git}"
SYNC_SCRIPT="$(dirname "$0")/sync-git-remote.sh"

# flock 防重入：上一轮 fetch/推送还没跑完时跳过。
LOCK="/tmp/cogneva-upstream-pull.lock"
exec 9>"$LOCK"
if ! flock -xn 9; then
  echo "another upstream pull in progress; skip" >&2
  exit 0
fi

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

[ -d "$SRC_REPO/.git" ] || { log "源仓库不存在: $SRC_REPO" >&2; exit 1; }
[ -f "$BARE/HEAD" ] || { log "bare 仓库不存在: $BARE（bootstrap 未 seed？）" >&2; exit 1; }
[ -f "$DEPLOY_KEY" ] || { log "deploy key 不存在: $DEPLOY_KEY" >&2; exit 1; }

# GIT_CONFIG_GLOBAL=/dev/null：绕开宿主全局 git 配置里的 HTTPS 重写/署名，
# 仓库级配置（user/remote）不受影响；显式指定 deploy key，不吃 ssh-agent。
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_TERMINAL_PROMPT=0
export GIT_SSH_COMMAND="ssh -i $DEPLOY_KEY -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

log "fetching upstream main from $UPSTREAM_URL"
if ! git -C "$SRC_REPO" fetch "$UPSTREAM_URL" main; then
  log "fetch 失败（网络/密钥？）；下轮重试" >&2
  exit 1
fi

UPSTREAM="$(git -C "$SRC_REPO" rev-parse FETCH_HEAD)"
BARE_MAIN="$(git --git-dir="$BARE" rev-parse main 2>/dev/null || true)"

if [ "$UPSTREAM" = "$BARE_MAIN" ]; then
  log "bare main 已是上游 ${UPSTREAM:0:12}，无事可做"
  exit 0
fi

# 工作树干净且停在 main 分支：快进合并本地 main，再串调既有同步脚本
# （它自带祖先校验，推本地 main → bare）。
DIRTY="$(git -C "$SRC_REPO" status --porcelain | head -1 || true)"
CUR_BRANCH="$(git -C "$SRC_REPO" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
if [ -z "$DIRTY" ] && [ "$CUR_BRANCH" = "main" ]; then
  log "工作树干净，ff-only 合并本地 main → ${UPSTREAM:0:12}"
  git -C "$SRC_REPO" merge --ff-only "$UPSTREAM"
  if [ -x "$SYNC_SCRIPT" ]; then
    "$SYNC_SCRIPT" "$SRC_REPO" "$BARE" || log "sync-git-remote 失败（不致命）" >&2
  fi
  log "done（经本地 main）"
  exit 0
fi

# 工作树忙：不动用户现场，直接把上游提交快进推给 bare。普通 push 对已
# 存在引用天然拒绝非快进，分叉时祖先校验先拦下并留人工处置。
log "工作树忙（dirty='${DIRTY:-none}' branch='${CUR_BRANCH:-detached}'），直推上游到 bare"
if [ -n "$BARE_MAIN" ] && ! git -C "$SRC_REPO" merge-base --is-ancestor "$BARE_MAIN" "$UPSTREAM"; then
  log "bare main 与上游分叉（bare=$BARE_MAIN upstream=$UPSTREAM），拒绝推送，人工处置" >&2
  exit 1
fi
git -C "$SRC_REPO" push "$BARE" "$UPSTREAM:main"
log "done（直推 bare）: ${BARE_MAIN:0:12} -> ${UPSTREAM:0:12}"
