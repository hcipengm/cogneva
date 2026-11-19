#!/usr/bin/env bash
# principle: feedback_a_predicate_must_match_the_form_not_a_bare_substring
# 角色引导脚本的**顺序**：先把 cog_app 做实，再去等迁移建出来的表。
#
# 为什么要有这一条：2026-11-19 在一台新装机上实测，这个 Job 与业务 Pod 互相等——
# Job 等 `public.backup_drills`（迁移建出来的表），业务 Pod 要拿 cog_app 连库才能跑
# 迁移，而 cog_app 由这个 Job 在「等完表之后」才建。业务 Pod 反复 CrashLoopBackOff
# （`password authentication failed for user "cog_app" / Role "cog_app" does not exist`），
# Job 停在 active 1/1，几分钟里两边都没有任何进展。修法是把这个顺序摆正，本门判的
# 就是「角色与扩展先落到库上，等表的循环排在它们后面」。
#
# 两腿：
#   静态腿——按行号读脚本：建角色、设口令、装扩展都排在第一个 `to_regclass` 轮询之前，
#     `GRANT INSERT ON public.backup_drills` 排在它之后。故意把顺序写反的一段文件必须
#     在这条腿上红，否则这条判据没有区分力。
#   实跑腿（不需要数据库）——把 pg_isready / psql 换成记录调用与 stdin 的桩，跑真的
#     脚本，桩永远回答「表还不存在」，脚本会停在等待里；此时看调用记录：建角色那段
#     heredoc 必须已经发出去了，而 backup_drills 的 GRANT 一句都不许出现。这样判的是
#     脚本实际做了什么，不是文件里字出现的先后。
#
# 跑法：bash deploy/scripts/tests/db-role-bootstrap-ordering.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
sql_file="${repo}/deploy/helm/cogneva/files/db-role-bootstrap.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${sql_file}" ] || fail "引导脚本不在树里：${sql_file}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ---- 静态腿 -------------------------------------------------------------

line_of_first() { # <文件> <固定字符串> -> 第一个命中的行号；没命中印 0
    awk -v needle="$2" '
        index($0, needle) && !found { print NR; found = 1; exit }
        END { if (!found) print 0 }
    ' "$1"
}

check_order() { # <文件> -> 0 绿；1 红（原因一行一条印到 stdout）
    local f="$1" wait_line role_line pw_line ext_line grant_line bad=0
    wait_line="$(line_of_first "${f}" "to_regclass('public.backup_drills')")"
    role_line="$(line_of_first "${f}" 'CREATE ROLE cog_app')"
    pw_line="$(line_of_first "${f}" "PASSWORD :'app_password'")"
    ext_line="$(line_of_first "${f}" 'CREATE EXTENSION IF NOT EXISTS')"
    grant_line="$(line_of_first "${f}" 'GRANT INSERT ON public.backup_drills')"

    [ "${wait_line}" != 0 ] || {
        echo "没找到等 backup_drills 的轮询：这段的形状变了，先把本门的读法对准它"
        return 1
    }
    [ "${role_line}" != 0 ] || {
        echo "没找到创建 cog_app 的那一句"
        bad=1
    }
    [ "${role_line}" = 0 ] || [ "${role_line}" -lt "${wait_line}" ] || {
        echo "创建 cog_app 排在等 backup_drills 之后：新装机上没人能跑迁移，两边互等到超时"
        bad=1
    }
    [ "${pw_line}" = 0 ] || [ "${pw_line}" -lt "${wait_line}" ] || {
        echo "给 cog_app 设口令排在等 backup_drills 之后：角色存在也连不上，迁移照样跑不动"
        bad=1
    }
    [ "${ext_line}" = 0 ] || [ "${ext_line}" -lt "${wait_line}" ] || {
        echo "装扩展排在等 backup_drills 之后：迁移第一条就在装扩展，等到表出现是等一个死结"
        bad=1
    }
    [ "${grant_line}" = 0 ] || [ "${grant_line}" -gt "${wait_line}" ] || {
        echo "backup_drills 的 GRANT 排在等它出现之前：新装机上这一句会授在一张还不存在的表上"
        bad=1
    }
    return "${bad}"
}

check_order "${sql_file}" || fail "引导脚本的顺序不合规（原因见上）"

# 反例：事故那天的顺序（先等表、再建角色）。它必须红。
old_form="${work}/old-order.sh"
cat >"${old_form}" <<'OLD'
#!/bin/sh
set -eu
attempt=0
until [ "$(psql -tAqc "select to_regclass('public.backup_drills') is not null")" = t ]; do
  attempt=$((attempt + 1)); sleep 2
done
psql -v ON_ERROR_STOP=1 -v app_password=x -f /dev/stdin <<'SQL'
DO $do$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cog_app') THEN
    CREATE ROLE cog_app LOGIN;
  END IF;
END $do$;
ALTER ROLE cog_app WITH LOGIN PASSWORD :'app_password';
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
GRANT INSERT ON public.backup_drills TO cog_backup;
SQL
OLD
if check_order "${old_form}" >/dev/null; then
    fail "反例（先等表、再建角色）在这条腿上跑得过：没有区分力"
fi

# ---- 实跑腿：桩掉 psql，看脚本实际发出了什么 -----------------------------

stub="${work}/bin"
mkdir -p "${stub}"
cat >"${stub}/pg_isready" <<'STUB'
#!/bin/sh
exit 0
STUB
cat >"${stub}/psql" <<'STUB'
#!/bin/sh
# 记录参数与 heredoc 走的 stdin；轮询表时一律回答「还不存在」，其余调用当作成功。
log="${COGNEVA_PSQL_LOG:?}"
{
    printf 'CALL %s\n' "$*"
    # 只有 heredoc 那几次带 /dev/stdin；带 -c 的调用没有 stdin，读它会挂住。
    input=""
    case " $* " in
    *"/dev/stdin"*) input="$(cat)" ;;
    esac
    if [ -n "${input}" ]; then
        printf 'STDIN\n%s\nENDSTDIN\n' "${input}"
    fi
} >>"${log}"
case " $* " in
*to_regclass*) echo f ;;
*) echo t ;;
esac
STUB
chmod +x "${stub}/pg_isready" "${stub}/psql"

log="${work}/psql.log"
: >"${log}"
COGNEVA_PSQL_LOG="${log}" PATH="${stub}:${PATH}" \
    APP_PASSWORD=probe-password BACKUP_PASSWORD=probe-backup-password \
    TEST_PASSWORD=probe-test-password \
    PGHOST=stub PGPORT=5432 PGDATABASE=stub PGUSER=stub \
    bash "${sql_file}" >/dev/null 2>&1 </dev/null &
pid=$!
for _ in $(seq 1 100); do
    grep -q 'to_regclass' "${log}" 2>/dev/null && break
    sleep 0.2
done
kill "${pid}" 2>/dev/null || true
wait "${pid}" 2>/dev/null || true

grep -q 'to_regclass' "${log}" || fail "脚本没有走到等 backup_drills 那一步：实跑腿没有分母"

wait_at="$(line_of_first "${log}" 'to_regclass')"
role_at="$(line_of_first "${log}" 'CREATE ROLE cog_app')"
[ "${role_at}" != 0 ] || fail "脚本在等表之前没有建 cog_app：业务 Pod 连不上库，迁移永远不会跑"
[ "${role_at}" -lt "${wait_at}" ] || fail "建 cog_app 的语句排在等表之后：新装机会互等到超时"
grant_at="$(line_of_first "${log}" 'GRANT INSERT ON public.backup_drills')"
[ "${grant_at}" = 0 ] || [ "${grant_at}" -gt "${wait_at}" ] \
    || fail "backup_drills 的 GRANT 在表出现之前就发出去了：授的是一张不存在的表"

echo "db-role-bootstrap-ordering: 静态腿（含反例）+ 实跑腿通过（角色与扩展先落库，等表在后）"
