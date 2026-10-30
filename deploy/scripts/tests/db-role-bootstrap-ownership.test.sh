#!/usr/bin/env bash
# principle: feedback_a_gate_whose_members_are_named_by_hand_must_scan_for_unlisted_members
# principle: feedback_a_predicate_must_match_the_form_not_a_bare_substring
# 角色引导脚本的对象属主循环：每一种取出来的 relkind 都要有它自己的 ALTER 关键字，
# 附属于某张表的序列必须排除在外。判的是「种类与动作一一对上」，不是「这行字还在」。
#
# 为什么要有这一条：这段 SQL 在 2026-10-30 第一次在真实库上跑，第一个自增序列就把
# 整个循环打断了——`ALTER TABLE <seq> OWNER TO` 被 Postgres 拒绝（序列不能当表改），
# 而已挪走的那部分对象留在 cog_app 名下。Job 只在日志里留一行 ERROR，部署器把它读成
# 「这一版有问题」并回滚，于是整条主线停在旧镜像上。这段 SQL 在树里躺了十几天，期间
# 没有任何东西碰过它。
#
# 两腿：
#   静态腿（任何机器都跑）——从 SQL 里读出 relkind 清单，要求 CASE 分派覆盖清单里的
#     每一种；要求附属序列的排除条件同时覆盖 'a'（serial）与 'i'（identity 列），
#     因为两种都会让 Postgres 拒绝单独换属主。修复前的写法在这里当反例，它必须红。
#   实跑腿（有 COGNEVA_TEST_DATABASE_URL 且指向 localhost 时）——建一个探针库，造出
#     serial / identity / 独立序列 / 视图 / 物化视图 / 分区，跑真的这段脚本，再判
#     public 下没有对象还挂在别人名下；随后拿旧写法当反例，它必须失败。
#
# 跑法：bash deploy/scripts/tests/db-role-bootstrap-ownership.test.sh
# 探针库只建在 localhost 上：这一步会建库、建角色、改属主，指到别处就是拿别人的
# 生产库当夹具。库名固定带 cogneva_bootstrap_probe 前缀，误指也不会删掉同名生产库。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
sql_file="${repo}/deploy/helm/cogneva/files/db-role-bootstrap.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${sql_file}" ] || fail "引导脚本不在树里：${sql_file}"

work="$(mktemp -d)"
probe_db=""
probe_cleanup() {
    [ -n "${probe_db}" ] || return 0
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d postgres -tAqc \
        "DROP DATABASE IF EXISTS ${probe_db}" >/dev/null 2>&1 || true
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d postgres -tAqc \
        "DROP ROLE IF EXISTS cog_app" >/dev/null 2>&1 || true
}
trap 'probe_cleanup; rm -rf "${work}"' EXIT

# ---- 静态腿 -------------------------------------------------------------

# 每一种取出来的对象，换属主的关键字都不一样。写成一张表，缺一条就红。
dispatch() { # <relkind> -> 期望的关键字
    case "$1" in
    r|p) printf 'TABLE' ;;
    v) printf 'VIEW' ;;
    m) printf 'MATERIALIZED VIEW' ;;
    S) printf 'SEQUENCE' ;;
    *) printf '' ;;
    esac
}

static_leg() { # <文件> -> 0 绿；1 红，原因一行一条印到 stdout
    local f="$1" kinds kind_list unknown="" k want bad=0
    kinds="$(grep -o "relkind IN ([^)]*)" "${f}" | head -1)"
    if [ -z "${kinds}" ]; then
        echo "没找到 relkind 清单：这段 SQL 的形状变了，先把本门的读法对准它"
        return 1
    fi
    kind_list="$(printf '%s' "${kinds}" | tr -d "()' " | sed 's/.*IN//')"
    for k in $(printf '%s' "${kind_list}" | tr ',' ' '); do
        want="$(dispatch "${k}")"
        if [ -z "${want}" ]; then
            unknown+="${k} "
            continue
        fi
        if [ "${want}" = TABLE ]; then
            grep -q "ELSE 'TABLE'" "${f}" || {
                echo "relkind ${k} 没有分派到 TABLE"
                bad=1
            }
        else
            grep -q "WHEN '${k}' THEN '${want}'" "${f}" || {
                echo "relkind ${k} 没有分派到 ALTER ${want}：循环会拿别的关键字改它"
                bad=1
            }
        fi
    done
    [ -z "${unknown}" ] || {
        echo "清单里有本门不认识的 relkind：${unknown}（先确认它该用哪个 ALTER 关键字）"
        bad=1
    }
    grep -q "format('ALTER TABLE %s OWNER TO" "${f}" && {
        echo "又出现了整段 ALTER TABLE：序列/视图不接受这种写法"
        bad=1
    }
    # 附属于表的序列：serial 造出来的是 'a'，identity 列造出来的是 'i'，两种都会被
    # Postgres 以「linked to table」拒绝。只排除其中一个，另一个就照样把循环打断。
    grep -q "deptype IN ('a', 'i')" "${f}" || {
        echo "附属序列的排除条件没有同时盖住 'a'(serial) 与 'i'(identity)：缺一个就会在真库上断"
        bad=1
    }
    return "${bad}"
}

static_leg "${sql_file}" || fail "引导脚本的对象属主循环不合规（原因见上）"

# 反例：修复前的写法本身就是一段可读的 SQL，它必须在这条静态腿上红。它若绿，上面
# 那些判据就没有区分力——上一版树里就是它，而它一路绿灯走到了真库上。
old_form="${work}/old-form.sql"
cat >"${old_form}" <<'OLD'
    FOR obj IN
      SELECT c.oid::regclass AS name FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public'
        AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
        AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
    LOOP
      EXECUTE format('ALTER TABLE %s OWNER TO cog_app', obj.name);
    END LOOP;
OLD
static_leg "${old_form}" >/dev/null && fail "修复前的写法在这条静态腿上跑得过：没有区分力"

# ---- 实跑腿 -------------------------------------------------------------

url="${COGNEVA_TEST_DATABASE_URL:-}"
if [ -z "${url}" ]; then
    echo "db-role-bootstrap-ownership: 静态腿通过（含反例）；本机没有 COGNEVA_TEST_DATABASE_URL，实跑腿未执行"
    exit 0
fi
case "${url}" in
postgres://*@localhost[:/]* | postgres://*@127.0.0.1[:/]*) ;;
*) fail "实跑腿只对 localhost 跑（它会建库、建角色、改属主）：${url}" ;;
esac
command -v psql >/dev/null || fail "实跑腿要 psql（postgresql-client）"

rest="${url#postgres://}"
creds="${rest%%@*}"
hostpart="${rest#*@}"
PGUSER="${creds%%:*}"
PGPASSWORD="${creds#*:}"
PGHOST="${hostpart%%[:/]*}"
tail_part="${hostpart#*:}"
PGPORT="${tail_part%%/*}"
export PGPASSWORD

as_super() { # <sql> 连到实例默认库，用来建库建角色
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d postgres -tAqc "$1"
}
in_db() { # <sql> 连到探针库
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${probe_db}" -tAqc "$1"
}

probe_db="cogneva_bootstrap_probe"
as_super "DROP DATABASE IF EXISTS ${probe_db}" >/dev/null
as_super "CREATE DATABASE ${probe_db}" >/dev/null
as_super "DROP ROLE IF EXISTS cog_app" >/dev/null 2>&1 || true
as_super "DROP ROLE IF EXISTS cog_user" >/dev/null 2>&1 || true
# cog_user 在真部署里是库的属主角色；这里给它超级用户，好让它造出各种对象。
# 密码必须给：CI 的 Postgres 对 TCP 连接走 scram，没有密码的角色连不上——第一版
# 探针角色只写了 LOGIN SUPERUSER，引导脚本还没碰到对象就先在连接上被拒
# （2026-10-31 CI：password authentication failed for user "cog_user"），而这一行
# 红的是探针自己的形状，不是它要判的那段 SQL。密码用本文件自己的字面量，不从
# URL 里拆出来再拼进 SQL：URL 里那一串要经过两层引号，值带一个 `'` 就能把它拼坏。
probe_pw='cogneva-probe-cog-user'
as_super "CREATE ROLE cog_user LOGIN SUPERUSER PASSWORD '${probe_pw}'" >/dev/null

# 夹具：把这段 SQL 要处理的每一种对象都造出来，且都归 cog_user。
#
# 造两个用途的东西要分开跑两遍（引导脚本那一次、反例那一次），所以是一段函数：
# 反例必须从**还没被动过的**夹具起跑，见下面那段的理由。
make_fixture() {
    in_db "CREATE TABLE t_serial (id serial PRIMARY KEY, v text);
           CREATE TABLE t_identity (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text);
           CREATE SEQUENCE t_standalone;
           CREATE TABLE t_plain (id int);
           CREATE VIEW v_plain AS SELECT 1 AS x;
           CREATE MATERIALIZED VIEW mv_plain AS SELECT 1 AS x;
           CREATE TABLE t_part (id int, k int) PARTITION BY RANGE (k);
           CREATE TABLE t_part_1 PARTITION OF t_part FOR VALUES FROM (0) TO (10);
           ALTER TABLE t_serial OWNER TO cog_user;
           ALTER TABLE t_identity OWNER TO cog_user;
           ALTER SEQUENCE t_standalone OWNER TO cog_user;
           ALTER TABLE t_plain OWNER TO cog_user;
           ALTER VIEW v_plain OWNER TO cog_user;
           ALTER MATERIALIZED VIEW mv_plain OWNER TO cog_user;
           ALTER TABLE t_part OWNER TO cog_user;
           ALTER TABLE t_part_1 OWNER TO cog_user;" >/dev/null \
        || fail "夹具造不出来"
}
make_fixture

PGHOST="${PGHOST}" PGPORT="${PGPORT}" PGUSER=cog_user PGPASSWORD="${probe_pw}" \
    PGDATABASE="${probe_db}" APP_PASSWORD=probe-password \
    bash "${sql_file}" >/dev/null 2>"${work}/bootstrap.err" \
    || fail "引导脚本在真库上失败：$(tail -3 "${work}/bootstrap.err")"

left="$(in_db "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
                 where n.nspname = 'public'
                   and c.relkind in ('r', 'p', 'v', 'm', 'S')
                   and c.relowner <> (select oid from pg_roles where rolname = 'cog_app')")"
[ "${left}" = 0 ] || fail "还有 ${left} 个对象没挪到 cog_app 名下"

# 反例：旧写法（对每种对象都发 ALTER TABLE）必须在同一份夹具上失败。它若跑得过，
# 上面那条判据就没有区分力——这条门判的是「种类要对上」，不是「脚本跑完了」。
#
# 夹具必须重建一遍再跑：上面那次引导已经把 public 下每个对象都挪到 cog_app 了，
# 旧写法在那个库里一个对象也选不到，于是「什么都没做」退出 0——2026-10-31 实测，
# 这条反例正是这样在本机上绿的，而它想证明的恰恰相反。空转的绿比没有这条腿更糟：
# 它替一段被真库拒掉的写法背书。
as_super "DROP DATABASE IF EXISTS ${probe_db}" >/dev/null
as_super "CREATE DATABASE ${probe_db}" >/dev/null
make_fixture
old_err="${work}/old-form.err"
if PGPASSWORD="${probe_pw}" psql -h "${PGHOST}" -p "${PGPORT}" -U cog_user \
    -d "${probe_db}" -v ON_ERROR_STOP=1 >/dev/null 2>"${old_err}" <<'SQL'
DO $do$
DECLARE obj record;
BEGIN
  FOR obj IN
    SELECT c.oid::regclass AS name FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = 'cog_user')
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
      AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
    ORDER BY c.oid
  LOOP
    EXECUTE format('ALTER TABLE %s OWNER TO cog_app', obj.name);
  END LOOP;
END
$do$;
SQL
then
    fail "旧写法在同一份夹具上跑得过：这条门没有区分力"
fi
# 非零退出还不够：连不上库、密码不对也一样非零，把它读成「旧写法被拒」就是拿一个
# 与判据无关的红替这条门背书。原因必须是 Postgres 拒了这个 ALTER 本身。
#
# 判据是「附属序列不能单独换属主」（实测原文：cannot change owner of sequence
# "t_serial_id_seq" / DETAIL: Sequence ... is linked to table ...），而不是「非表对象
# 不接受 ALTER TABLE」：PG 16 上视图、物化视图、独立序列都照收 ALTER TABLE，只有
# 连着表的那条序列会被拒。旧写法会不会撞上它取决于循环顺序——序列先于它的表被取到
# 才会撞——所以上面的 `ORDER BY c.oid` 把顺序钉在造表时序列先建的那个顺序上
# （serial/identity 都是先建序列），事故那天遇到的也是这一条。不钉，这条反例就是一
# 次掷硬币。
grep -q 'cannot change owner of sequence' "${old_err}" \
    || fail "旧写法失败了，但不是因为附属序列被拒：$(tail -2 "${old_err}")"

echo "db-role-bootstrap-ownership: 静态腿 + 实跑腿通过（serial/identity/独立序列/视图/物化视图/分区都挪到了 cog_app）"
