#!/usr/bin/env bash
# 装宿主侧的异地副本：单元、定时器、凭证、目录。跑一次就够，之后由定时器自己跑。
#
# 用法：
#   sudo install-backup-offsite.sh <仓库地址> <明文凭证目录> [本机要推的目录]
#
# 明文凭证目录里按名字放三个文件：restic-password（必须）、aws-access-key-id 与
# aws-secret-access-key（用 S3 兼容后端时必须有）。**它们只在加密的这一刻存在**：
# 每个文件加密进 /etc/cogneva/credentials/ 之后立刻 shred 掉原文件，装完再确认
# 盘上找不到明文。仓库地址不是秘密，写进 0644 的环境文件。
set -euo pipefail

REPO_URL="${1:-}"
PLAINTEXT_DIR="${2:-}"
SRC_DIR="${3:-/var/lib/cogneva-offsite}"
if [ -z "$REPO_URL" ] || [ -z "$PLAINTEXT_DIR" ]; then
    echo "用法: $0 <仓库地址，如 s3:https://bucket.example/cogneva> <明文凭证目录> [要推的本机目录]" >&2
    exit 2
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
UNIT_SRC="$REPO_ROOT/deploy/systemd"
UNIT_DST=/etc/systemd/system
CRED_DST=/etc/cogneva/credentials
ENV_FILE=/etc/cogneva/backup-offsite.env
LIB_DST=/usr/local/lib/cogneva/backup-offsite-restic.sh

command -v systemd-creds >/dev/null || { echo "缺少 systemd-creds（systemd 249+）" >&2; exit 2; }
command -v restic >/dev/null || { echo "缺少 restic：apt install restic / dnf install restic" >&2; exit 2; }
[ -r "$PLAINTEXT_DIR/restic-password" ] || { echo "$PLAINTEXT_DIR/restic-password 不存在" >&2; exit 2; }

install -d -m 0755 "$CRED_DST" /etc/cogneva /usr/local/lib/cogneva "$SRC_DIR"
install -m 0755 "$HERE/backup-offsite-restic.sh" "$LIB_DST"

# 加密凭证：--name 必须与单元里 LoadCredentialEncrypted=<name>:<path> 冒号前的名字一致，
# 否则 systemd 解不出来，单元起不来（这条一致性由 check-host-credential-surface.sh
# 两侧互判）。
for name in restic-password aws-access-key-id aws-secret-access-key; do
    src="$PLAINTEXT_DIR/$name"
    [ -r "$src" ] || { echo "跳过 $name（没有）"; continue; }
    systemd-creds encrypt --name="$name" "$src" "$CRED_DST/$name.cred"
    chmod 0400 "$CRED_DST/$name.cred"
    shred -u "$src" 2>/dev/null || rm -f "$src"
    echo "已加密 $name"
done

# 明文既不能留在原目录，也不能留在 /etc 下任何地方。装完查一遍：这条腿的全部意义
# 是「宿主盘上没有仓库口令」，装完留一份就等于没装。
for f in "$PLAINTEXT_DIR/restic-password" "$PLAINTEXT_DIR/aws-access-key-id" "$PLAINTEXT_DIR/aws-secret-access-key"; do
    if [ -e "$f" ]; then echo "明文还在：$f —— 加密失败，停在这里" >&2; exit 1; fi
done

umask 022
cat > "$ENV_FILE" <<EOF
# 由 install-backup-offsite.sh 写。没有秘密：仓库地址与保留策略。
RESTIC_REPOSITORY=$REPO_URL
COGNEVA_BACKUP_OFFSITE_DIR=$SRC_DIR
EOF
chmod 0644 "$ENV_FILE"

install -m 0644 "$UNIT_SRC/cogneva-backup-offsite.service" "$UNIT_DST/"
install -m 0644 "$UNIT_SRC/cogneva-backup-offsite.timer" "$UNIT_DST/"
systemctl daemon-reload
systemctl enable --now cogneva-backup-offsite.timer

echo "装好了：$(systemctl list-timers cogneva-backup-offsite.timer --no-pager | sed -n '2p')"
echo "手动跑一次看输出：systemctl start cogneva-backup-offsite.service && journalctl -u cogneva-backup-offsite -n 50"
