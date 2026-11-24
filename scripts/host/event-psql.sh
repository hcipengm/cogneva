#!/usr/bin/env bash
# 事件流的读写口：把一段 SQL 交给集群里的 Postgres，行按制表符拼接、没有表头。
#
# 轮书那几条路由产品二进制直接连库，这条命令留给人在宿主上查一次读数；仍在的
# `scripts/hooks/round.py` 也经它读事件流那本书。它必须满足两个形状：**接收一段 SQL 作为第一个参数**、
# **把结果行原样印到 stdout**。用法：
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
