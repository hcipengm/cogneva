#!/usr/bin/env bash
# 宿主主线 → K3s git-remote bare 仓库同步。
#
# 为什么需要：GitOps 晋级通道以 /var/lib/cogneva-data/git-remote 为中央仓库
# （沙盒推送端写 evolution-release，各集群拉取端 poll）。沙盒源码树基于
# bare 的 main——bare 陈旧会让拉取端应用晋级产物时连带回退无关文件
# （2026-08-06 设计评审结论）。本脚本把宿主开发仓库 main 快进推送到 bare，
# 配合 cron 每 5 分钟跑一次，保持通道新鲜：
#   */5 * * * * root /root/omc_workspace/cogneva/deploy/k3s/sync-git-remote.sh
#
# 安全约束：只快进（--no-force）；bare 的 main 只由本脚本写（沙盒推送端
# 只写 evolution-release 分支），出现分叉即报错留人工处置，绝不强推覆盖。
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
