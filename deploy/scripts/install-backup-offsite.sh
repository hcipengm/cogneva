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
#
# 只渲染：COGNEVA_INSTALL_RENDER_ONLY=1 跳过凭证、restic、systemd-creds 与 /etc，
# 只把两份单元写进 $XDG_CONFIG_HOME/systemd/user。测试要判「算出来的时刻对不对」，
# 而它不该在这台机器上装一个推数据的定时器。
# 装这一份要 root，还要两个只有属主能给的东西（异地仓库地址、明文凭证目录），所以
# install-host-units.sh 不代跑它，只把带占位符的调用印出来。这一行同样由它读。
# host-install: root
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
UNIT_SRC="$REPO_ROOT/deploy/systemd"
UNIT_DST=/etc/systemd/system
CRED_DST=/etc/cogneva/credentials
ENV_FILE=/etc/cogneva/backup-offsite.env
LIB_DST=/usr/local/lib/cogneva/backup-offsite-restic.sh
VALUES="$REPO_ROOT/deploy/helm/cogneva/values.yaml"

die() { printf 'install-backup-offsite: %s\n' "$*" >&2; exit 1; }

# 推的时点不写死。备份作业的排期、它的时区、以及它的两个界都在 values.yaml 里，
# 这里读出来算：备份**最晚可能跑完**的时刻 = 排期 + startingDeadlineSeconds（控制器
# 允许迟到起跑的上限）+ runDeadlineSeconds（Job 到点被杀）。早于这一刻推，推到的还是
# 上一份包，而推成功是绿的——2026-10-19 写下的 02:17 就是这样：它照着 03:17 排的，
# 可那个 03:17 属于 Etc/UTC（= 本地 11:17），于是每天的推都早于当天那份包九个多小时。
value_of() { # <values.yaml 里的叶子名> -> 去引号、去行尾注释的值
    awk -v key="$1" '
        $0 ~ "^[[:space:]]*" key ":" {
            sub("^[[:space:]]*" key ":[[:space:]]*", "")
            sub(/[[:space:]]+#.*$/, "")
            gsub(/"/, "")
            sub(/[[:space:]]+$/, "")
            print; exit
        }' "$VALUES"
}

[ -f "$VALUES" ] || die "读不到 ${VALUES}：推的时点是从它算的，缺了就不猜"
schedule="$(value_of schedule)"
tz="$(value_of timeZone)"
run_deadline="$(value_of runDeadlineSeconds)"
start_deadline="$(value_of startingDeadlineSeconds)"
for pair in "schedule=$schedule" "timeZone=$tz" \
            "runDeadlineSeconds=$run_deadline" "startingDeadlineSeconds=$start_deadline"; do
    [ -n "${pair#*=}" ] || die "values.yaml 里没有 ${pair%%=*}：推的时点算不出来"
done
case "$tz" in
*/*|UTC) ;;
*) die "timeZone 不像 IANA 名字：${tz}" ;;
esac
case "$run_deadline$start_deadline" in
*[!0-9]*) die "界必须是非负整数秒：run=${run_deadline} start=${start_deadline}" ;;
esac

# 只认 `M H * * *`（每天一拍）。别的形状（每周、每 N 小时）换算成 OnCalendar 是另一件事，
# 猜错了就是又一次「推的时候包还没出来」，所以直接拒绝。
read -r s_min s_hour s_dom s_mon s_dow <<<"$schedule" || true
case "${s_dom}${s_mon}${s_dow}" in
"***") ;;
*) die "备份排期只支持每天一拍（M H * * *），现在是：${schedule}" ;;
esac
case "${s_min}${s_hour}" in
''|*[!0-9]*) die "排期的时与分必须是数字：${schedule}" ;;
esac

start_epoch="$(TZ="$tz" date -d "today ${s_hour}:${s_min}" +%s)" \
    || die "时区 ${tz} 解释不了"
push_hm="$(date -d "@$((start_epoch + run_deadline + start_deadline))" +%H:%M)"
on_calendar="*-*-* ${push_hm}:00"

render_units() { # <落点目录>
    local dir="$1"
    install -d -m 0755 "$dir"
    install -m 0644 "$UNIT_SRC/cogneva-backup-offsite.service" "$dir/"
    sed "s|@ON_CALENDAR@|${on_calendar}|" "$UNIT_SRC/cogneva-backup-offsite.timer" \
        >"$dir/cogneva-backup-offsite.timer"
    chmod 0644 "$dir/cogneva-backup-offsite.timer"
    # 占位符还在＝这份 timer 没有排期，而 systemd 对没有 OnCalendar 的 timer 是装载
    # 成功、永不触发：装完看着是绿的，什么都没有推。
    grep -q '@ON_CALENDAR@' "$dir/cogneva-backup-offsite.timer" \
        && die "模板里的 @ON_CALENDAR@ 没被替换：${dir}/cogneva-backup-offsite.timer"
    return 0
}

if [ "${COGNEVA_INSTALL_RENDER_ONLY:-0}" = 1 ]; then
    render_units "${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
    printf 'install-backup-offsite: 只渲染（COGNEVA_INSTALL_RENDER_ONLY=1）：%s\n' \
        "${XDG_CONFIG_HOME:-${HOME:?}/.config}/systemd/user"
    exit 0
fi

REPO_URL="${1:-}"
PLAINTEXT_DIR="${2:-}"
SRC_DIR="${3:-/var/lib/cogneva-offsite}"
if [ -z "$REPO_URL" ] || [ -z "$PLAINTEXT_DIR" ]; then
    echo "用法: $0 <仓库地址，如 s3:https://bucket.example/cogneva> <明文凭证目录> [要推的本机目录]" >&2
    exit 2
fi

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

render_units "$UNIT_DST"
systemctl daemon-reload
systemctl enable --now cogneva-backup-offsite.timer

echo "装好了：$(systemctl list-timers cogneva-backup-offsite.timer --no-pager | sed -n '2p')"
echo "手动跑一次看输出：systemctl start cogneva-backup-offsite.service && journalctl -u cogneva-backup-offsite -n 50"
