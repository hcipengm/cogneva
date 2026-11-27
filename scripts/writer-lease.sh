#!/usr/bin/env bash
# 写者租约：一段时间里，**谁**可以往变更面写。
#
# 为什么需要它：带外（宿主上的 claude code 会话）与带内（cogneva 自己的智能体）
# 都能改同一棵树。池恢复时带内接手写 diff，带外那些进程必须停手——但「通知了」
# 不是判据，「写的时候被挡下」才是。所以租约不是一条消息，是写在**写入口**上的一份状态：
#   有人持有租约 ⇒ 不是持有者的写入被拒，拒词里带着持有者与到期时间。
#
# 状态住在宿主（默认 $CLAUDE_HOME/cogneva-writer-lease.json），不进仓库：它是机器状态，
# 不是知识。并发用 flock + 原子 rename；没有 flock 时退化为「读-改-写」，仍靠 rename 保原子。
#
# 用法：
#   writer-lease.sh show
#   writer-lease.sh claim <owner> <reason> [--ttl 秒]   # 空闲或已过期才拿得到
#   writer-lease.sh renew <owner> [--ttl 秒]
#   writer-lease.sh release <owner>
#   writer-lease.sh check <owner>        # 0=本人持有 1=别人持有 2=空闲 3=读不回来
#
# 读法：`show` 的 owner / expires_at 是唯一正本；`check` 的出口码就是门用的判据。
set -uo pipefail

FILE="${WRITER_LEASE_FILE:-${CLAUDE_HOME:-$HOME/.claude}/cogneva-writer-lease.json}"
LOCK="$FILE.lock"
cmd="${1:-show}"; shift || true

# keep: 租约的默认有效期：删掉后一把没人续的租约永不过期，后来者永远拿不到这条写权；由租约协议（ttl 必须有限）兜住
owner=""; reason=""; ttl=3600
case "$cmd" in
  claim)
    owner="${1:-}"; reason="${2:-}"; shift 2 2>/dev/null || true ;;
  renew|release|check)
    owner="${1:-}"; shift 2>/dev/null || true ;;
esac
while (($#)); do
  case "$1" in
    --ttl) ttl="${2:?--ttl 需要秒数}"; shift 2 ;;
    *) shift ;;
  esac
done

now=$(date +%s)
expires=$((now + ttl))

# 读当前租约：没有文件 / 空文件 / 坏 JSON 都算「读不回来」而不是「空闲」。
read_lease() {
  [ -f "$FILE" ] || { echo "none"; return 0; }
  python3 -c '
import json,sys
try:
    d=json.load(open(sys.argv[1]))
except Exception:
    sys.exit(3)
if not isinstance(d,dict) or not d.get("owner"):
    print("none"); sys.exit(0)
print("%s\t%s\t%s\t%s\t%s" % (d.get("owner",""), d.get("since",""),
      d.get("expires_at",0), d.get("epoch",0), d.get("reason","")))
' "$FILE" 2>/dev/null
  return $?
}

write_lease() { # $1 owner $2 since $3 expires $4 epoch $5 reason
  python3 -c '
import json,os,sys
d={"owner":sys.argv[1],"since":sys.argv[2],"expires_at":int(sys.argv[3]),
   "epoch":int(sys.argv[4]),"reason":sys.argv[5]}
tmp=sys.argv[6]+".tmp.%d" % os.getpid()
with open(tmp,"w") as f: json.dump(d,f,ensure_ascii=False,indent=1)
os.replace(tmp,sys.argv[6])
' "$1" "$2" "$3" "$4" "$5" "$FILE"
}

lock() {
  mkdir -p "$(dirname "$FILE")" 2>/dev/null || true
  exec 9>"$LOCK"
  command -v flock >/dev/null 2>&1 && flock 9
  return 0
}

case "$cmd" in
  show)
    cur="$(read_lease)"; rc=$?
    if [ "$rc" -eq 3 ]; then echo "读不回来：$FILE 不是合法 JSON" >&2; exit 3; fi
    if [ "$cur" = none ]; then echo "空闲（无租约）"; exit 0; fi
    IFS=$'\t' read -r o since exp epoch why <<<"$cur"
    state=有效; [ "$exp" -le "$now" ] && state=已过期
    printf '持有者 %s（%s，epoch %s）\n到期 %s（%s）\n理由 %s\n' \
      "$o" "$since" "$epoch" "$(date -d "@$exp" '+%F %T')" "$state" "$why"
    ;;

  claim)
    [ -n "$owner" ] && [ -n "$reason" ] || { echo "用法：writer-lease.sh claim <owner> <reason> [--ttl 秒]" >&2; exit 2; }
    lock
    cur="$(read_lease)"; rc=$?
    [ "$rc" -eq 3 ] && { echo "读不回来：$FILE" >&2; exit 3; }
    if [ "$cur" != none ]; then
      IFS=$'\t' read -r o since exp epoch why <<<"$cur"
      if [ "$exp" -gt "$now" ] && [ "$o" != "$owner" ]; then
        printf '拒绝：租约在 %s 手上，到期 %s（%s）\n' "$o" "$(date -d "@$exp" '+%F %T')" "$why" >&2
        exit 1
      fi
      epoch=$((epoch + 1))
      since="$(date '+%F %T')"
    else
      epoch=1; since="$(date '+%F %T')"
    fi
    write_lease "$owner" "$since" "$expires" "$epoch" "$reason" || { echo "写不进 $FILE" >&2; exit 3; }
    printf 'ok：%s 持有租约（epoch %s，到期 %s）\n' "$owner" "$epoch" "$(date -d "@$expires" '+%F %T')"
    ;;

  renew)
    [ -n "$owner" ] || { echo "用法：writer-lease.sh renew <owner> [--ttl 秒]" >&2; exit 2; }
    lock
    cur="$(read_lease)"; rc=$?
    [ "$rc" -eq 3 ] && { echo "读不回来：$FILE" >&2; exit 3; }
    [ "$cur" = none ] && { echo "没有租约可续" >&2; exit 2; }
    IFS=$'\t' read -r o since exp epoch why <<<"$cur"
    [ "$o" = "$owner" ] || { echo "拒绝：租约不在 $owner 手上（$o）" >&2; exit 1; }
    write_lease "$o" "$since" "$expires" "$epoch" "$why" || exit 3
    printf 'ok：%s 续到 %s\n' "$owner" "$(date -d "@$expires" '+%F %T')"
    ;;

  release)
    [ -n "$owner" ] || { echo "用法：writer-lease.sh release <owner>" >&2; exit 2; }
    lock
    cur="$(read_lease)"; rc=$?
    if [ "$cur" = none ]; then echo "本来就没有租约"; exit 0; fi
    IFS=$'\t' read -r o since exp epoch why <<<"$cur"
    [ "$o" = "$owner" ] || { echo "拒绝：租约不在 $owner 手上（$o）" >&2; exit 1; }
    rm -f "$FILE"
    echo "ok：$owner 已释放租约"
    ;;

  check)
    [ -n "$owner" ] || { echo "用法：writer-lease.sh check <owner>" >&2; exit 2; }
    cur="$(read_lease)"; rc=$?
    [ "$rc" -eq 3 ] && { echo "读不回来：$FILE" >&2; exit 3; }
    [ "$cur" = none ] && exit 2
    IFS=$'\t' read -r o since exp epoch why <<<"$cur"
    [ "$exp" -le "$now" ] && exit 2
    [ "$o" = "$owner" ] && exit 0
    exit 1
    ;;

  *)
    echo "用法：writer-lease.sh show|claim|renew|release|check …" >&2; exit 2 ;;
esac
