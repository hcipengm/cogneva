#!/usr/bin/env bash
#
# 把还没应用的迁移，对着**真库的形状**重放一遍。
#
# 为什么需要这一步：迁移链自己的判据只能看见「链里写了什么」。空库重放看得见
# 「链和 store 的建表语句合起来能不能从零建出来」，但它看不见真库上**此刻**有哪些
# 依赖着被改列的活对象——表达式索引、生成列、视图、约束，全是 store 首启时建的，
# 迁移链里没有它们。
#
# 实测于 2026-11-16：`112` 把 `audit_logs.details` 转成 text，而真库上有一条
# `(details->>'project_id')` 的表达式索引。空库重放绿（那个索引不存在），真库上
# 这句话以 `operator does not exist: text ->> text` 失败——一次会在部署那一刻发生的
# 失败，被这里在推之前拦下。
#
# 做法：把真库的 schema 导进一个临时库，按 `schema_migrations` 的最大版本号挑出
# 待应用的迁移，带着 `ON_ERROR_STOP` 跑一遍，然后删掉临时库。只读真库（`-s` 导出
# 不带行），不改它。
#
# carrier: 人 —— 推之前手动跑，不进整门。它要集群可达（`sudo -n kubectl` 默认，
#   可用 `COGNEVA_KUBECTL` 覆盖），而 `check-before-push.sh` 在没有集群的机器上也要
#   能跑：把这一步挂进去，那台机器上的整门会卡在等集群而不是判字节。
#   `check-gate-carriers.py` 认这一行。
set -euo pipefail

namespace="${COGNEVA_NAMESPACE:-cogneva}"
pod="${COGNEVA_PG_POD:-postgres-0}"
pg_user="${COGNEVA_PG_USER:-cog_user}"
pg_db="${COGNEVA_PG_DB:-cogneva}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 第一个参数是仓库根（这一族门的约定）；不给时才按脚本自己的位置推。
root="${1:-$(cd "${here}/../.." && pwd)}"
migrations="${root}/crates/cog-storage/migrations/postgres"

# 读不到迁移目录 ⇒ 退 3（没判）。这条门是 `carrier: 人`、要集群可达；在一棵没有
# 迁移的树上它没有可判的东西，退 0 会把「没读到」印成「全部合规」。
if ! compgen -G "${migrations}/*.up.sql" >/dev/null 2>&1; then
    echo "migrations-against-live-schema: 迁移目录里没有 *.up.sql（${migrations}），没判。" >&2
    exit 3
fi

# `COGNEVA_KUBECTL` 是这棵树里给集群客户端的唯一入口（见 `token-face-ingest.py`）。
read -r -a kubectl <<<"${COGNEVA_KUBECTL:-sudo -n kubectl}"

run_sql() { # run_sql <库> <sql>
    "${kubectl[@]}" -n "${namespace}" exec -i "${pod}" -- \
        psql -U "${pg_user}" -d "$1" -v ON_ERROR_STOP=1 -tA -c "$2"
}

# 记录过的版本是**一张名单**，不是一个最大值：手工应用的迁移可以是跳着的
# （2026-11-27 现网就是 131/139/140/141 已在、120–138 未在）。按最大值挑待应用
# 的那一版会把中间没应用的整段看漏，而漏掉的正是下一次部署要跑的那些。
recorded="$(run_sql "${pg_db}" "select version from schema_migrations order by version")"
if [ -z "${recorded}" ]; then
    echo "RED  读不到 ${pg_db}.schema_migrations 的已应用版本：这份检查没有起点" >&2
    exit 1
fi
applied="$(printf '%s\n' "${recorded}" | wc -l | tr -d ' ')"

scratch="mig_rehearsal_$$"
pending="$(mktemp "${TMPDIR:-/tmp}/cogneva-pending-migrations.XXXXXX")"
dump="$(mktemp "${TMPDIR:-/tmp}/cogneva-live-schema.XXXXXX")"
cleanup() {
    run_sql postgres "drop database if exists ${scratch}" >/dev/null 2>&1 || true
    rm -f "${pending}" "${dump}"
}
trap cleanup EXIT

# 待应用的那几份，按数字顺序拼起来。文件名是 `NNN_名字.up.sql`，版本号取前缀。
if ! ls "${migrations}"/*.up.sql >/dev/null 2>&1; then
    echo "RED  在 ${migrations} 一份迁移也没读到：读空与「没有待应用的迁移」在判词上同形，" >&2
    echo "     而读空通常只意味着这份检查走错了路径" >&2
    exit 1
fi
count=0
for path in $(ls "${migrations}"/*.up.sql | sort -V); do
    version="$(basename "${path}" | sed 's/^\([0-9]*\)_.*/\1/')"
    # 文件名有前导零（`010_…`），表里存的是整数（`10`）：不归一化就会把每一条老
    # 迁移都看成「没记录过」，于是回放从第一份开始重跑，红在早已存在的对象上。
    version="$(printf '%s' "${version}" | sed 's/^0*//')"
    [ -n "${version}" ] || version=0
    if printf '%s\n' "${recorded}" | grep -qx "${version}"; then
        continue
    fi
    cat "${path}" >>"${pending}"
    echo "" >>"${pending}"
    count=$((count + 1))
done
if [ "${count}" -eq 0 ]; then
    echo "OK：真库记录过的 ${applied} 个版本覆盖了树里的全部迁移，没有待应用的"
    exit 0
fi

# 只导形状，不导行；`--no-owner`/`--no-privileges` 让它在临时库里以同一个角色装得下。
"${kubectl[@]}" -n "${namespace}" exec "${pod}" -- \
    pg_dump -U "${pg_user}" -s --no-owner --no-privileges "${pg_db}" >"${dump}"

# 临时库里 `public` 已经存在，`CREATE SCHEMA public` 会报错；`\restrict` 是 16.15 的
# 导出器加的会话锁，本地重放不需要。
sed -e 's/^CREATE SCHEMA public;$/-- (public exists)/' \
    -e '/^\\restrict /d' -e '/^\\unrestrict /d' -i "${dump}"

# 库里有 `uuid-ossp` 的函数被列默认值用到，而 `-s` 的导出里没有 `CREATE EXTENSION`。
printf 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";\n' | cat - "${dump}" >"${dump}.tmp"
mv "${dump}.tmp" "${dump}"

"${kubectl[@]}" -n "${namespace}" cp "${dump}" "${pod}:/tmp/live-schema.sql" >/dev/null
"${kubectl[@]}" -n "${namespace}" cp "${pending}" "${pod}:/tmp/pending-migrations.sql" >/dev/null
run_sql postgres "drop database if exists ${scratch}" >/dev/null
run_sql postgres "create database ${scratch}" >/dev/null
"${kubectl[@]}" -n "${namespace}" exec "${pod}" -- \
    psql -U "${pg_user}" -d "${scratch}" -v ON_ERROR_STOP=1 -q -f /tmp/live-schema.sql >/dev/null

set +e
out="$("${kubectl[@]}" -n "${namespace}" exec "${pod}" -- \
    psql -U "${pg_user}" -d "${scratch}" -v ON_ERROR_STOP=1 -q \
    -f /tmp/pending-migrations.sql 2>&1)"
status=$?
set -e
"${kubectl[@]}" -n "${namespace}" exec "${pod}" -- \
    rm -f /tmp/live-schema.sql /tmp/pending-migrations.sql >/dev/null 2>&1 || true

if [ "${status}" -ne 0 ]; then
    echo "RED  ${count} 份未记录的迁移在真库的形状上跑不完：" >&2
    printf '%s\n' "${out}" | tail -n 20 >&2
    echo "     空库重放看不出这一类：依赖被改列的表达式索引 / 生成列 / 视图都是" >&2
    echo "     store 首启时建的，迁移链里没有它们。" >&2
    exit 1
fi

echo "OK：${count} 份未记录的迁移在真库的形状上跑得完（已记录 ${applied} 个版本）"
