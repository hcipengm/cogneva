#!/usr/bin/env bash
# 事件流的读写口：把一段 SQL 交给集群里的 Postgres，行按制表符拼接、没有表头。
#
# `scripts/hooks/round.py` 与 `deploy/scripts/batch-gate.py` 都经环境变量
# `COGNEVA_EVENT_PSQL` 找这条命令，而它必须满足两个形状：**接收一段 SQL 作为第
# 一个参数**、**把结果行原样印到 stdout**。这个脚本就是那个形状，宿主上的用法：
#
#     export COGNEVA_EVENT_PSQL=$PWD/scripts/host/event-psql.sh
#
# 它不判断 SQL 写了什么，只负责送达：判词在调用方（那一轮开没开、台账钉在哪个 rev）。
# 数据库连不上时把 psql 的 stderr 原样透出并退同样的码——「读不到」不能印成「没有」。
set -uo pipefail

sql="${1-}"
if [ -z "$sql" ]; then
    echo "用法：event-psql.sh \"<SQL>\"" >&2
    exit 2
fi

ns="${COGNEVA_NS:-cogneva}"
pod="${COGNEVA_PG_POD:-postgres-0}"
kubectl_bin="${COGNEVA_KUBECTL:-kubectl}"
role="${COGNEVA_PG_ROLE:-cog_user}"
db="${COGNEVA_PG_DB:-cogneva}"

kube() {
    if sudo -n true 2>/dev/null; then
        sudo -n "$kubectl_bin" "$@"
    else
        "$kubectl_bin" "$@"
    fi
}

err="$(mktemp)"
trap 'rm -f "$err"' EXIT

# 密码不从宿主传：Pod 里已经有 `POSTGRES_PASSWORD`，在那一侧展开。
# 制表符既是列分隔符，也是 round 载荷里字段的分隔符，两处一致才不会错位。
kube -n "$ns" exec "$pod" -- sh -c \
    'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$1" -d "$2" -tA -F "$(printf "\t")" -c "$3"' \
    _ "$role" "$db" "$sql" 2>"$err"
rc=$?
if [ "$rc" -ne 0 ]; then
    cat "$err" >&2
    exit "$rc"
fi
