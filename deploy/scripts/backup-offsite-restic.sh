#!/usr/bin/env bash
# 把本机的备份包推到异地 restic 仓库。由 cogneva-backup-offsite.timer 每天调一次。
#
# 这一步补的是「同主机另一条路径」补不了的那一格：备份卷和同机副本都在同一台机器
# 上，卷删了、盘坏了、机器没了，两份一起没。restic 仓库在另一台机器上，且客户端
# 加密——桶里存的是密文，桶的密钥只有写权限，读不回来。
#
# 三条输入，全部由 systemd 送进来，脚本自己不读宿主的任何配置文件：
#   RESTIC_REPOSITORY          仓库地址（EnvironmentFile，非密钥）
#   $CREDENTIALS_DIRECTORY/*   仓库口令与桶密钥（LoadCredentialEncrypted，tmpfs）
#   COGNEVA_BACKUP_OFFSITE_DIR 要推的本机目录
#
# 口令只经环境变量交给 restic，不落到命令行（`ps` 看得见命令行）、不 echo。
set -euo pipefail

SRC="${COGNEVA_BACKUP_OFFSITE_DIR:-/var/lib/cogneva-offsite}"
CREDENTIALS="${CREDENTIALS_DIRECTORY:-}"
CACHE_DIR="${CACHE_DIRECTORY:-/var/cache/cogneva-backup-offsite}"
KEEP_DAILY="${COGNEVA_OFFSITE_KEEP_DAILY:-7}"
KEEP_WEEKLY="${COGNEVA_OFFSITE_KEEP_WEEKLY:-4}"
KEEP_MONTHLY="${COGNEVA_OFFSITE_KEEP_MONTHLY:-12}"

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }
die() { echo "ERROR: $*" >&2; exit 78; }   # 78 = EX_CONFIG：不是这一跑的问题，是配置没配好

command -v restic >/dev/null || die "缺少 restic，无法推异地副本"
[ -n "${RESTIC_REPOSITORY:-}" ] || die "RESTIC_REPOSITORY 为空（/etc/cogneva/backup-offsite.env）"
[ -n "$CREDENTIALS" ] || die "CREDENTIALS_DIRECTORY 未设置：这个单元必须由 systemd 启动，密钥才在 tmpfs 里"
[ -r "$CREDENTIALS/restic-password" ] || die "$CREDENTIALS/restic-password 读不到"

# restic 只认 RESTIC_PASSWORD 或 RESTIC_PASSWORD_FILE。前者经环境变量传（子进程继承，
# `ps` 看不到值）；后者要求盘上有一个明文文件，而这条腿的前提就是盘上没有。
RESTIC_PASSWORD="$(cat "$CREDENTIALS/restic-password")"
export RESTIC_PASSWORD
if [ -r "$CREDENTIALS/aws-access-key-id" ]; then
    AWS_ACCESS_KEY_ID="$(cat "$CREDENTIALS/aws-access-key-id")"
    export AWS_ACCESS_KEY_ID
fi
if [ -r "$CREDENTIALS/aws-secret-access-key" ]; then
    AWS_SECRET_ACCESS_KEY="$(cat "$CREDENTIALS/aws-secret-access-key")"
    export AWS_SECRET_ACCESS_KEY
fi

[ -d "$SRC" ] || die "要推的目录不存在：$SRC（备份 Job 的 offsiteHostPath 指向它）"
mkdir -p "$CACHE_DIR"
# restic 的索引缓存落在 XDG_CACHE_HOME/restic。指到单元给的那个目录里，是因为
# ProtectSystem=strict 下别处都不可写。
export XDG_CACHE_HOME="$CACHE_DIR"

# 目录里一个包都没有时推一次空目录，会在仓库里留下一个与「备份没在跑」无法区分的
# 快照。这里直接跳过并说清：这条腿的读数由备份那一侧负责，不在这里发明。
packages="$(find "$SRC" -maxdepth 1 -name 'cogneva-backup-*.tar.zst' -type f | wc -l)"
if [ "$packages" -eq 0 ]; then
    say "跳过：$SRC 里还没有备份包"
    exit 0
fi

# 仓库第一次用时建它。判据是 restic 的退出码 10（仓库不存在），不是「目录是不是空」——
# 远端桶里有没有 config 文件只有 restic 自己知道。
set +e
restic --no-lock snapshots >/dev/null 2>&1
rc=$?
set -e
if [ "$rc" -eq 10 ]; then
    say "仓库不存在，初始化"
    restic init
elif [ "$rc" -ne 0 ]; then
    die "仓库读不通（restic 退出码 $rc）：$RESTIC_REPOSITORY"
fi

say "推 $packages 个包：$SRC → $RESTIC_REPOSITORY"
restic backup --tag cogneva-backup --hostname "$(hostname)" "$SRC"

# 保留策略与本地目录那一条对齐：仓库是第二份，不是「所有历史都留着」的账本。
# --prune 放在同一手里，否则 forget 只删索引、数据块永远留在桶里。
restic forget --tag cogneva-backup \
    --keep-daily "$KEEP_DAILY" --keep-weekly "$KEEP_WEEKLY" --keep-monthly "$KEEP_MONTHLY" \
    --prune

# 只核元数据（不读全部数据块）：每晚读一遍全量数据要按小时算，而「桶里少了一个块」
# 这条故障在这一步之前不会有人发现——核到能核的那一半，比不核多一层。
restic check

say "完成：仓库里最新一个快照"
restic snapshots --latest 1
