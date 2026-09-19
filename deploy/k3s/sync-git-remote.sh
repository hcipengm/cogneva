#!/usr/bin/env bash
# 宿主主线 → K3s git-remote bare 仓库同步。
#
# 【已被取代，除救援外不要启用】集群内的主线跟踪部署器现在自己从各平台取
# 上游 main 并 CAS 推进 bare（配置 self_evolution.mainline_deployer.upstreams）。
# 本脚本连同 /etc/crontab 里那行 `*/5` 以及 pull-upstream-main.sh 的 timer
# 都属旧的宿主写者链：两个写者同时推 bare 的 main 会互相打脸，正常部署
# 应全部停用。本脚本保留作无网/救援时的手动通道（宿主能直连 GitHub）。
#
# 停用宿主写者：
#   systemctl disable --now cogneva-upstream-pull.timer
#   sed -i '/sync-git-remote.sh/d' /etc/crontab
#
# 安全约束：只快进（--no-force）；分叉即报错留人工处置，绝不强推覆盖。
set -euo pipefail

SRC_REPO="${1:-/root/omc_workspace/cogneva}"
BARE="${2:-/var/lib/cogneva-data/git-remote}"

[ -d "$SRC_REPO/.git" ] || { echo "源仓库不存在: $SRC_REPO" >&2; exit 1; }
[ -f "$BARE/HEAD" ] || { echo "bare 仓库不存在: $BARE（bootstrap 未 seed？）" >&2; exit 1; }

LOCAL_MAIN="$(git -C "$SRC_REPO" rev-parse main)"
REMOTE_MAIN="$(git --git-dir="$BARE" rev-parse main 2>/dev/null || true)"

if [ "$LOCAL_MAIN" = "$REMOTE_MAIN" ]; then
  exit 0
fi

# 快进校验：bare main 必须是本地 main 的祖先，否则分叉报错。
if [ -n "$REMOTE_MAIN" ] && ! git -C "$SRC_REPO" merge-base --is-ancestor "$REMOTE_MAIN" "$LOCAL_MAIN"; then
  echo "bare main 与宿主 main 分叉（bare=$REMOTE_MAIN local=$LOCAL_MAIN），拒绝推送，人工处置" >&2
  exit 1
fi

git -C "$SRC_REPO" push "$BARE" main:main
echo "synced: $REMOTE_MAIN -> $LOCAL_MAIN"
