#!/usr/bin/env bash
# `measure-postgres-exporter-surface.sh` 的定向测试：量完写回那一段会不会写错。
#
# 为什么要有这一层：这个脚本以前是一段贴在名单文件头里的配方，人照着抄。抄错一格不会
# 红——名单还是合法的 YAML，`reading_series_have_a_producer` 那条门照样拿它去担保读数
# 的序列有生产者。把它变成脚本，错的地方就从「人抄错」挪到了「脚本写错」，所以脚本
# 自己得有一条会红的路。
#
# 这里不连集群（CI 没有）：`--metrics-file` / `--help-file` 喂两份抓好的输出，判的是
# 测量**之后**那一半——取名字、按声明折叠、保留表头、判新旧。每一格对着一种「写回会
# 怎么错」：
#   1. 一致 ⇒ 退出 0，文件一个字节都不动
#   2. 名单里有、镜像不发 ⇒ 退出 1，点名「过期」——这是唯一会让门担保假序列的方向
#   3. 镜像发了、名单没有 ⇒ 退出 1，点名「新增」
#   4. 写回 ⇒ 表头逐字节保留，三节重写，幂等
#   5. 只有声明过的前缀才折叠：没声明的大族逐条列，声明了的折成一行
#   6. 没有顶格 image: 行 ⇒ 退出 2（分不出表头，写下去就把人写的那段吃掉）
#   7. 抓回来的 /metrics 里一条 pg_ 序列都没有 ⇒ 退出 3，不是「一致」
#   8. --help 里一条 collector 旗标都没有 ⇒ 退出 3
#
# 跑法：bash deploy/scripts/tests/measure-postgres-exporter-surface.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
me="$(cd "${here}/.." && pwd)/measure-postgres-exporter-surface.sh"
[ -f "$me" ] || { echo "找不到被测脚本：$me" >&2; exit 1; }

pass=0
fail=0
ok()  { pass=$((pass + 1)); printf 'ok    %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL  %s\n' "$1" >&2; [ -n "${2:-}" ] && printf '      期望 %s\n' "$2" >&2; [ -n "${3:-}" ] && printf '      实得 %s\n' "$3" >&2; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

IMAGE="example/exporter:v1"

# ---- 夹具 ----
# 表头故意带一段会被写回逻辑「顺手格式化掉」的东西（空行、缩进、行尾空格），
# 写回后必须一字不差。
mklist() { # <文件> <序列行…>：<序列行> 是完整的 `  - name` 行
    local dst="$1"
    shift
    {
        printf '# 表头第一行：这段是人写的，写回不许动  \n'
        printf '#\n'
        printf '#   image 换过就重量一次——这是量出来的那一行下面的分界。\n'
        printf 'image: %s\n' "$IMAGE"
        printf '\ncollector_flags:\n'
        printf '  - --collector.locks\n'
        printf '  - --collector.wal\n'
        printf '\nseries:\n'
        printf '%s\n' "$@"
    } >"$dst"
}

mkmetrics() { # <文件> <序列名…>
    local dst="$1"
    shift
    : >"$dst"
    local name
    for name in "$@"; do
        printf '%s 1\n' "$name" >>"$dst"
    done
    # 抓取输出里本来就有一堆非 pg_ 的行，判据不能把它们收进来。
    printf '# HELP something_total ...\n' >>"$dst"
    printf 'process_cpu_seconds_total 12.5\n' >>"$dst"
}

mkhelp() {
    cat >"$1" <<'EOF'
usage: postgres_exporter [<flags>]

Flags:
  -h, --[no-]help                Show context-sensitive help.
      --[no-]collector.locks     Enable the locks collector (default: enabled).
      --[no-]collector.wal       Enable the wal collector (default: enabled).
      --[no-]version             Show application version.
EOF
}

# ---- 1. 一致 ⇒ 退出 0，文件不动 ----
list="$work/same.yaml"
mklist "$list" '  - pg_locks_count' '  - pg_settings_*' '  - pg_up'
mkmetrics "$work/same.metrics" pg_locks_count pg_settings_max_connections pg_settings_shared_buffers pg_up
mkhelp "$work/same.help"
before="$(sha256sum "$list" | cut -d' ' -f1)"

bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/same.metrics" --help-file "$work/same.help" --check \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 0 ] && ok "一致的名单：退出 0" || bad "一致的名单应退出 0，实得 $rc：$(cat "$work/out")"
[ "$before" = "$(sha256sum "$list" | cut -d' ' -f1)" ] && ok "一致的名单：--check 一个字节都没写" \
    || bad "--check 改了文件"

# ---- 2. 名单里有、镜像不发 ⇒ 退出 1，点名过期 ----
list="$work/stale.yaml"
mklist "$list" '  - pg_locks_count' '  - pg_up'
mkmetrics "$work/stale.metrics" pg_locks_count
bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/stale.metrics" --help-file "$work/same.help" --check \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 1 ] && ok "名单里的序列镜像不发：退出 1" || bad "应退出 1，实得 $rc：$(cat "$work/out")"
grep -q '过期：pg_up' "$work/out" && ok "名单里的序列镜像不发：点名 pg_up 过期" \
    || bad "没点名过期的 pg_up：$(cat "$work/out")"
grep -q 'pg_up' "$list" && ok "名单里的序列镜像不发：--check 没把它删掉" \
    || bad "--check 写了文件"

# ---- 3. 镜像发了、名单没有 ⇒ 退出 1，点名新增 ----
list="$work/fresh.yaml"
mklist "$list" '  - pg_locks_count' '  - pg_up'
mkmetrics "$work/fresh.metrics" pg_locks_count pg_up pg_wal_size_bytes
bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/fresh.metrics" --help-file "$work/same.help" --check \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 1 ] && ok "镜像发了名单没有：退出 1" || bad "应退出 1，实得 $rc：$(cat "$work/out")"
grep -q '新增：pg_wal_size_bytes' "$work/out" && ok "镜像发了名单没有：点名 pg_wal_size_bytes 新增" \
    || bad "没点名新增的一行：$(cat "$work/out")"

# ---- 4. 写回：表头逐字节保留，三节重排，幂等 ----
list="$work/write.yaml"
mklist "$list" '  - pg_locks_count' '  - pg_up'
mkmetrics "$work/write.metrics" pg_locks_count pg_up pg_wal_size_bytes
cat >"$work/write.help" <<'EOF'
Flags:
      --[no-]collector.wal       Enable the wal collector (default: enabled).
      --[no-]collector.locks     Enable the locks collector (default: enabled).
EOF
bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/write.metrics" --help-file "$work/write.help" \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 0 ] && ok "写回：退出 0" || bad "写回应退出 0，实得 $rc：$(cat "$work/out")"

# 表头 = 顶格 image: 之前的一切，逐字节比。
awk '/^image:/{exit} {print}' "$list" >"$work/head.after"
printf '# 表头第一行：这段是人写的，写回不许动  \n#\n#   image 换过就重量一次——这是量出来的那一行下面的分界。\n' >"$work/head.want"
cmp -s "$work/head.want" "$work/head.after" && ok "写回：表头逐字节保留（含行尾空格）" \
    || bad "表头被改写了" "$(cat "$work/head.want")" "$(cat "$work/head.after")"

grep -qx '  - --collector.locks' "$list" && grep -qx '  - --collector.wal' "$list" \
    && ok "写回：旗标来自 --help 输出" || bad "旗标没写对：$(sed -n '/collector_flags:/,/^$/p' "$list")"
grep -qx '  - pg_wal_size_bytes' "$list" && ok "写回：新序列进了名单" || bad "新序列没进名单"

# 顺序由脚本定（C 排序），第二次写必须一个字都不变。
before="$(sha256sum "$list" | cut -d' ' -f1)"
bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/write.metrics" --help-file "$work/write.help" \
    >"$work/out" 2>&1
[ "$before" = "$(sha256sum "$list" | cut -d' ' -f1)" ] && ok "写回：第二次写一个字都不变（幂等）" \
    || bad "第二次写回改了文件，比对：" "" "$(cat "$work/out")"

# ---- 5. 只有声明过的前缀才折叠 ----
# 两个族各 40 条：声明了 pg_settings_*，没声明 pg_stat_user_tables_*。
{
    printf 'image: %s\n\ncollector_flags:\n  - --collector.locks\n\nseries:\n  - pg_settings_*\n' "$IMAGE"
} >"$work/fold.yaml"
: >"$work/fold.metrics"
for i in $(seq 1 40); do printf 'pg_settings_setting_%02d 1\n' "$i"; done >"$work/fold.settings"
for i in $(seq 1 40); do printf 'pg_stat_user_tables_col_%02d 1\n' "$i"; done >"$work/fold.tables"
cat "$work/fold.tables" "$work/fold.settings" >"$work/fold.metrics"
printf 'Flags:\n      --[no-]collector.locks     x.\n' >"$work/fold.help"
bash "$me" --file "$work/fold.yaml" --image "$IMAGE" --metrics-file "$work/fold.metrics" --help-file "$work/fold.help" \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 0 ] && ok "折叠：退出 0" || bad "折叠：应退出 0，实得 $rc：$(cat "$work/out")"

awk '/^series:/{f=1;next} /^[a-z_]+:/{f=0} f && /^  - .*/{n++} END{print n+0}' "$work/fold.yaml" >"$work/n"
[ "$(cat "$work/n")" = 41 ] && ok "折叠：声明过的族折成一行，没声明的 40 条逐条列" \
    || bad "折叠后的行数应是 41（40 + pg_settings_*）" 41 "$(cat "$work/n")"
grep -qx '  - pg_settings_\*' "$work/fold.yaml" && ok "折叠：留下的是声明的前缀 + *" \
    || bad "没留下 pg_settings_*：$(sed -n '/^series:/,$p' "$work/fold.yaml" | head -3)"
grep -qx '  - pg_stat_user_tables_col_01' "$work/fold.yaml" && ok "折叠：没声明的族逐条在" \
    || bad "没声明的族被折了（阈值不该替人决定）"

# ---- 6. 没有顶格 image: 行 ⇒ 退出 2，不写 ----
printf 'collector_flags:\n  - --collector.locks\n' >"$work/nohead.yaml"
before="$(sha256sum "$work/nohead.yaml" | cut -d' ' -f1)"
bash "$me" --file "$work/nohead.yaml" --image "$IMAGE" --metrics-file "$work/same.metrics" --help-file "$work/same.help" \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 2 ] && ok "没有 image: 行：退出 2" || bad "应退出 2，实得 $rc：$(cat "$work/out")"
[ "$before" = "$(sha256sum "$work/nohead.yaml" | cut -d' ' -f1)" ] && ok "没有 image: 行：没写文件" \
    || bad "退出 2 却写了文件"

# ---- 7. 一条 pg_ 序列都没有 ⇒ 退出 3 ----
list="$work/empty.yaml"
mklist "$list" '  - pg_locks_count'
mkmetrics "$work/empty.metrics"
bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/empty.metrics" --help-file "$work/same.help" \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 3 ] && ok "抓取里一条 pg_ 序列都没有：退出 3" || bad "应退出 3，实得 $rc：$(cat "$work/out")"

# ---- 8. 一条 collector 旗标都没有 ⇒ 退出 3 ----
list="$work/noflag.yaml"
mklist "$list" '  - pg_locks_count'
printf 'usage: postgres_exporter [<flags>]\n' >"$work/noflag.help"
bash "$me" --file "$list" --image "$IMAGE" --metrics-file "$work/same.metrics" --help-file "$work/noflag.help" \
    >"$work/out" 2>&1
rc=$?
[ "$rc" = 3 ] && ok "--help 里没有 collector 旗标：退出 3" || bad "应退出 3，实得 $rc：$(cat "$work/out")"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
