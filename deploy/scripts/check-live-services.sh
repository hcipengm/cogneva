#!/usr/bin/env bash
# 需要真服务的检查：只有这一份清单。
#
# 这 64 条原先在工作流里是 64 个独立的 step。它们是「上游判过、本地一条都没问过」
# 的那一类：变更管线从工作流里读门禁时只认「启动器 + 脚本 + 纯旗标」的命令，而这些
# step 的命令里带着包名与测试名，于是被记成「工作流给了它参数，这棵树给不出来」跳
# 过。代价是这一类红的第一次开口永远在上游 CI——那时已经落地，账记在下一版头上。
#
# 现在它们是这份清单、这一个脚本、这一条命令：CI 起一次性服务容器之后调它，沙箱在
# 变更验证里也调它（名字以 `check-` 开头，按全树的约定被自动认成一条门禁），两边跑
# 的是同一批命令。清单只有这一份，所以两边不会各自漂。
#
# 调用方给两件事：`COGNEVA_TEST_DATABASE_URL` 与 `COGNEVA_TEST_REDIS_URL`。清单里
# 用到了哪一个、缺了哪一个，就 exit 2——这是全树的约定，意思是「我跑不了」，不是
# 跳过，也不是当通过。一条只跑了一半的清单比一条都没跑更接近「绿」，而那正是这段
# 要消灭的读数。
#
# 每条仍然经 `run-a-ci-step.sh` 跑：红的时候把「哪条测试红了」写成注解，形状与主套件
# 那一格相同。所有条目都会跑完，任何一条红整段就红，最后点名红的是哪几条。
#
# 它们要的是一个专供测试的、而且**每一轮开始时是空的**库：CI 的库随 runner 消失，
# 沙箱的库由部署指向一个测试专用库，开跑之前这一份脚本自己把整条库重建一次（见
# 下面那段「每轮一个干净的库」）。指错了就是拿业务数据当 playground，所以这条 URL
# 只许指向名字带 test 的库。
#
# 用法：
#   bash deploy/scripts/check-live-services.sh          # 跑全部
#   bash deploy/scripts/check-live-services.sh --list   # 只列出清单
set -uo pipefail

# 自己的目录，不是调用者的 cwd：这条脚本既被 CI 从仓库根调、也被沙箱从一条工作树
# 里调，而两者都不该决定 `run-a-ci-step.sh` 在哪里。
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "${1:-}" = "--list" ]; then
    awk '/^live_check /{print $2 "\t" $0}' "$0" | cut -f2 | sed -E "s/^live_check [a-z]+ '//; s/' .*//"
    exit 0
fi

passed=0
failed=0
failed_names=()

live_check() {
    local service="$1"
    shift
    local name="$1"
    shift
    printf '\n== %s\n' "${name}"
    if bash "${HERE}/run-a-ci-step.sh" "$@"; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
        failed_names+=("${name}")
    fi
}

# 这份清单要哪几个服务，从清单自己身上读：手抄一份「需要 db 和 redis」的常量，
# 就会在加了一条只用 redis 的检查之后仍然对 db 提要求，或者反过来，在缺服务时
# 静默地少跑一条。
services="$(awk '/^live_check /{print $2}' "$0" | sort -u)"
missing=()
needs_db=false
while IFS= read -r service; do
    case "${service}" in
        db)
            needs_db=true
            [ -n "${COGNEVA_TEST_DATABASE_URL:-}" ] || missing+=("COGNEVA_TEST_DATABASE_URL")
            ;;
        redis) [ -n "${COGNEVA_TEST_REDIS_URL:-}" ] || missing+=("COGNEVA_TEST_REDIS_URL") ;;
    esac
done <<< "${services}"

# 一个测试二进制里同时跑几条，由这里定，不由宿主的核数定。
#
# 这些用例每条都连库，而 sqlx 的连接池默认开到十条，跑得越并行问服务器要的连接就
# 越多。CI 的一次性容器只服务这一跑，要多少给多少；沙箱与本机不一样——这棵树的用例
# 连的那台服务器同时还在服务平台本身。2026-11-06 实测：`change_ledger_store` 那 30
# 条并行起来撞上 `remaining connection slots are reserved for roles with the SUPERUSER
# attribute`，那一刻平台上 idle 着 53 条连接，留给非超权角色的余量只剩四十来条。
# 同一个红在读的人眼里是「这棵树坏了」，而上游永远复现不出来——正是这份清单要消灭
# 的那种差。
#
# 四条并行：30 条用例那一份文件峰值也就几条连接，跑完仍然只要几十秒；调大是在向一台
# 同时在干活的服务器要它没有的东西。
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-4}"

if [ "${#missing[@]}" -gt 0 ]; then
    printf 'check-live-services: 这些检查需要的服务没有配置：%s\n' "$(IFS=、; echo "${missing[*]}")" >&2
    printf 'check-live-services: %d 条检查一条都没跑\n' "$(grep -c '^live_check ' "$0")" >&2
    exit 2
fi

# 指向的必须是测试库，不是业务库。清单里有几条一上来就 `DELETE FROM cog_dag_tasks`，
# 指错了就是把线上数据当 playground——而这一步做错的时候，什么都不会红：那些用例
# 清完表照样通过。所以判据放在最前面：库名里要有 `test`，没有就拒绝跑。
#
# 这条判据在 CI 与沙箱两侧指的是同一件事，因为两边的库同名（`cogneva_test`）：
# CI 的 postgres 服务容器按 `POSTGRES_DB` 建它，沙箱的由 `db-role-bootstrap` 建。
# 退 2 而不是退 1：这是调用方配错了，不是这棵树判出了什么。
#
# 2026-11-06 实测它拦不住的那一版：判据写成了对多行 `services` 做 `case` 匹配
# （`*" db "*`），而多行之间是换行不是空格，于是整段判据一次都没生效，清单直接
# 对着业务库跑了起来。当时挡住的是 `cog-test-support` 自己的那道守卫——每条用例
# 都 panic 在 `points at the platform's own database`，一个行都没写进去。也就是说：
# 这一层与那一层不是重复，是**谁先开口**的区别；而这条判据写错时，它自己不报错。
if [ "${needs_db}" = true ]; then
    db_name="${COGNEVA_TEST_DATABASE_URL##*/}"
    db_name="${db_name%%\?*}"
    case "${db_name}" in
        *test*) ;;
        *)
            printf 'check-live-services: COGNEVA_TEST_DATABASE_URL 指向的库叫 `%s`，不像是测试库。\n' "${db_name}" >&2
            printf 'check-live-services: 这些用例会清空整张任务表，所以只许指向名字带 test 的库；%d 条检查一条都没跑\n' "$(grep -c '^live_check ' "$0")" >&2
            exit 2
            ;;
    esac
fi

# 同一个库上两条并发跑会互相清表：清单里几条一上来就 `DELETE FROM` 整张任务表，而
# 另一条正在数它的行。红会落在「数出来的数不对」上，看起来像代码坏了，而代码没坏。
# CI 每一格有自己的 postgres（一次性容器），撞不上；沙箱会撞——同一个 Pod 里两条
# 变更同时在验证时共用这一个测试库。所以按库名拿一把锁：另一跑在跑就等，不跳过。
# 跳过在这里等于「没被问过」，而这条脚本存在的理由就是不再有「没被问过」。
#
# 等是有界的。门禁那一段的墙钟界是几条门共用的一个数：在这里无限等，等于让这一条
# 把后面每一条都饿成「没跑完」。等不到就 exit 2，如实说「这条我跑不了」——那是调用
# 方调度的问题（另一跑卡住了），不是这棵树判出了什么。
lock_key="$(printf '%s' "${COGNEVA_TEST_DATABASE_URL:-}" | cksum | cut -d' ' -f1)"
lock_dir="${TMPDIR:-/tmp}"
lock_wait_secs=900
if [ -d "${lock_dir}" ] && [ -w "${lock_dir}" ] && command -v flock >/dev/null 2>&1; then
    exec 9>"${lock_dir}/cogneva-live-checks.${lock_key}.lock"
    if ! flock -n 9; then
        printf 'check-live-services: 同一个库上另有一跑在跑，最多等 %d 秒…\n' "${lock_wait_secs}" >&2
        if ! flock -w "${lock_wait_secs}" 9; then
            printf 'check-live-services: 等了 %d 秒还没拿到测试库的锁，这一跑不算数（exit 2）\n' "${lock_wait_secs}" >&2
            exit 2
        fi
    fi
fi

# 每轮一个干净的库：这一跑自己建一条，跑完删掉。
#
# 「干净」不交给用例自己保证。多数用例确实自己建表自己删，但有几条跑在 public 上
# （迁移建出来的那些表），它们留下的行与对象决定下一轮读到什么；而一条漏清表的用例
# 只在下一轮变红，红在一条看起来与它无关的检查上。这里的标准与 CI 的一次性容器是
# 同一句话：这一轮开始时，库里什么都没有。
#
# 2026-11-06 实测的代价：前一轮以 cog_app 身份在 public 上跑完迁移，这一轮把库属主
# 换成 cog_test 之后，`partition_maintainer` 连 `schema_migrations` 都读不到——红的是
# 上一轮留下的对象，不是这一轮的代码，而它长得与「代码坏了」一模一样。
#
# 建新库而不是把调用方给的库清空：那条库可能在别处还有人跑（沙箱里的进化 Pod 与本机
# 指的是同一台服务器上的同一条库），清空它等于从另一跑脚下抽地板，而上面那把 flock
# 只管同一台机器。名字里带这一跑的 pid 与起点时刻，两跑就不可能落到同一条库上；
# 跑完就删，删不掉（被 kill）的由下一次开跑顺手扫掉——只扫两小时以前的，正在跑的
# 那条不会被扫到。
#
# 库名从调用方的 URL 派生，所以「名字里要有 test」那条判据对这一跑仍然成立。建库要连
# 到另一条库去，所以把 URL 的库名换成 `postgres`——initdb 一定建出来的那一条。
if [ "${needs_db}" = true ]; then
    if ! command -v psql >/dev/null 2>&1; then
        printf 'check-live-services: 没有 psql，建不出这一跑自己的测试库（exit 2）\n' >&2
        exit 2
    fi
    admin_url="$(printf '%s' "${COGNEVA_TEST_DATABASE_URL}" \
        | sed -E 's#^(postgres(ql)?://[^/]+)/[^?]*#\1/postgres#')"
    started_at="$(date +%s)"
    run_db="${db_name}_run_${started_at}_$$"

    # 上一跑被 kill 时留下的库。只收两小时以前的：正在跑的那条比这年轻，而且
    # 不带 FORCE，所以它即使被扫到（时钟歪了）也只是删不掉，不是被清空。
    psql "${admin_url}" -qtA -c \
        "select datname from pg_database where datname like '${db_name}_run_%'" 2>/dev/null \
        | while IFS= read -r stale; do
            stale_at="$(printf '%s' "${stale}" | sed -E 's/.*_run_([0-9]+)_[0-9]+$/\1/')"
            case "${stale_at}" in ''|*[!0-9]*) continue ;; esac
            [ $((started_at - stale_at)) -gt 7200 ] || continue
            psql "${admin_url}" -qtA \
                -c "DROP DATABASE IF EXISTS \"${stale}\"" >/dev/null 2>&1 \
                && printf 'check-live-services: 收掉上一跑留下的测试库 %s\n' "${stale}" >&2
        done

    if ! psql "${admin_url}" -v ON_ERROR_STOP=1 -qtA \
        -c "CREATE DATABASE \"${run_db}\"" >/dev/null; then
        printf 'check-live-services: 建不出这一跑的测试库 %s（exit 2）\n' "${run_db}" >&2
        exit 2
    fi
    trap 'psql "${admin_url}" -qtA \
        -c "DROP DATABASE IF EXISTS \"${run_db}\" WITH (FORCE)" >/dev/null 2>&1 || true' EXIT

    COGNEVA_TEST_DATABASE_URL="$(printf '%s' "${COGNEVA_TEST_DATABASE_URL}" \
        | sed -E "s#^(postgres(ql)?://[^/]+)/[^?]*#\1/${run_db}#")"
    export COGNEVA_TEST_DATABASE_URL
    printf 'check-live-services: 这一跑用测试库 %s（跑完就删）\n' "${run_db}" >&2
fi

# The keypair the contribution admin hands out is written by our own
# encoder, so every test we can write about it agrees with itself; the
# question that decides whether it works is whether the tool that will
# actually use the key reads the PEM. That binary ships on the runner, so
# this ran nowhere only because it was ignored and never named -- the same
# account the live-DB steps below settle. It is fast, and it panics
# rather than passes when the binary is missing, so it cannot go quiet.
live_check none 'The generated SSH keypair is readable by ssh-keygen' cargo test -p cog-gateway --lib -- --ignored
# The capped agent log is the first state this backend can get wrong only
# after the cap fires: a trim moves every list position, so a reader that
# indexes by position skips as many records as were dropped and a next
# sequence taken from the list length hands out numbers already used. Both
# are invisible until a log crosses the cap, which is to say in
# production, and the service above makes the server cost nothing here.
# Every live step below carries `if: ${{ !cancelled() }}`, for one
# reason: a step with no condition runs only when all of the ones above
# it succeeded, so any unrelated red earlier in this job skips the whole
# chain, and a skipped assertion reads on the job summary exactly like a
# passing one. That is how the change-ledger census test went red on the
# first run it ever had, after being green in name for as long as the
# reading it judges has existed. The price is that a build which does not
# compile makes every live step below fail too: a page of the same error,
# rather than a silence that looks like agreement.
live_check redis 'Live-Redis capped agent log' cargo test -p cog-storage --test wal_redis_trim -- --ignored
# The two retirement passes are judged on the one thing a fixture cannot
# hold: they work by asking the server what a key is now, and then acting
# on the answer. An inbox's emptiness is a stream length the pass never
# wrote; a log's owner is a registration that may have been renewed after
# the scan and before the delete, and a pass that deletes on the stale
# answer takes the tail of an agent that is still reading. Both were
# expensive in production -- 12,571 inboxes of which 12,568 were empty,
# and a log store that was almost all logs of agents that had retired,
# both read off the deployment the pass was written against -- which is
# why they exist at all,
# and why the thing they must never get wrong is checked against a real
# server rather than a stand-in.
live_check redis 'Live-Redis inbox retirement' cargo test -p cog-storage --test inbox_retirement -- --ignored
live_check redis 'Live-Redis agent log retirement' cargo test -p cog-storage --test wal_retirement -- --ignored
# The registry answers two questions through one key: who holds a name,
# and whether the holder is still beating. The second used to ride on the
# expiry alone, so a record whose writer died between `SET` and `EXPIRE`
# read as live forever; a live cluster held twenty of those, three days
# cold, offered as agents that could be sent work. Expiry and its absence
# are the server's own facts -- a fixture store has no clock to lose one
# against -- so the record that lost its expiry is planted here, on the
# server, and the registry is asked what it makes of it.
live_check redis 'Live-Redis agent registry liveness' cargo test -p cog-storage --test agent_registry_liveness -- --ignored
# 工号发在 Lua 脚本里，而内存实现根本没有脚本：两套实现各写一遍时，最贵的错法
# 是「测试里工号能回收、线上不能」——两边都绿，线上在换人。这条用例在真 Redis 上
# 注册、重复注册、退役，逐条问号是不是同一个。
live_check redis 'Live-Redis employee roster' cargo test -p cog-storage --test employee_roster -- --ignored
live_check db 'Live-DB partition tests' cargo test -p cog-storage --test partition_maintainer -- --ignored
# 完成 / 失败 / 取消一条任务前那条把整个 workspace 锁住的语句，2026-11-05 一天
# 在生产上死锁了 13 次：等待链两头都是同一条 `SELECT ... WHERE workspace_id = $1
# FOR UPDATE`，而它没规定取行的顺序，两个进程可以各握着对方要的那一行等下去。
# 被杀掉的那个事务是一次已经算完、结果没落库的完成，下一位持锁者只看得到租约
# 过期，于是把整条任务重新派发、重新花一遍钱。这条用例同一个 workspace 上并发
# 完成 8 条任务、并让两个会话故意走不同计划去抢第一行；去掉全序它们就红。
live_check db 'Live-DB workspace lock order' cargo test -p cog-storage --test workspace_lock_takes_rows_in_one_order -- --ignored
# The venue board is handed a subset of the task table, and both halves of
# that rule are the server's: the trim that decides which rows carry a
# label, and the terminal rows it has to keep -- `completed` and `failed`
# rows feed the board's latest-verdict and written-back columns, so a
# filter that reused the dispatch exclusion would empty those two columns
# while still returning rows, and an empty column reads like a venue where
# nothing has finished. A DB-free test can see neither half.
live_check db 'Live-DB venue rows' cargo test -p cog-storage --test venue_tasks_are_the_only_rows_read -- --ignored
live_check db 'Live-DB venue index' cargo test -p cog-storage --test venue_query_is_an_index_scan -- --ignored
# A row names its workspace twice -- a column and a field in the payload -- and
# rows written before the field was filled carry only the column. The gateway's
# visibility check reads the field, so on 2026-11-06 a workspace of 9,920 rows
# was reported to an authenticated reader as 42 tasks while the same query
# without a declared workspace returned 9,920; both numbers called themselves
# the total. Half of the fix is a read that fills the field from the column,
# the other half a write that fills it, and the second half cannot be checked
# through the backend's own reader once the first half exists -- so this case
# reads the stored JSON directly. Both halves are the server's; a fixture
# without a database can see neither.
live_check db 'Live-DB task rows know their workspace' cargo test -p cog-storage --test a_task_read_back_knows_its_workspace -- --ignored
# 取消之后交回来的失败不能改写那一行。判据在 `dag_fail_task` 里，而它读的是**库里
# 那一行的当前状态**，不是调用方手里的快照——没有真库这一格判不了。这一条来自
# 2026-11-06 的现场：13 轮 `ci-red-*-fix` 全部被部署器主动取消，又被执行侧交回的
# 「没人答」改写成环境故障，修复回路于是自己指着假证据干活。
live_check db 'Live-DB a cancelled row stays cancelled' cargo test -p cog-storage --test a_cancelled_task_is_not_judged_again -- --ignored
# The claim is a count of commits per burst, and a stand-in has no commit
# to count: the reading is whatever the server's own log says. An ignored
# test whose file says how to run it still ran nowhere until a step named
# it, which is what this step is.
live_check db 'Live-DB metric writes batch' cargo test -p cog-storage --test metric_writes_batch_into_few_commits -- --ignored
# The metering readings are a column the ledger gained later, so the three
# shapes that matter are the ones a server leaves behind and no fixture can
# hold: a row that spoke, a row whose upstream said nothing, and a row
# written before the column existed (`ALTER ... ADD COLUMN` leaves NULL,
# which is "no reading" rather than "a reading of nothing"). The panel
# divides by the count whose outcome is `read`, so a store that wrote the
# column but did not bring it back turns that share into a number about a
# column nobody can see.
live_check db 'Live-DB metering readings' cargo test -p cog-observability --test usage_metering -- --ignored
# The two daily spend reads feed projections that overwrite a quota cell,
# and the whole point of a projection is that the cell equals the ledger.
# Both answers are decided in SQL: a person's spend comes from two pieces
# of evidence (the row's owner, or the submitter of the task it names) and
# the task read has to be day-scoped rather than lifetime-scoped. Neither
# can be restated without a server, and a wrong window or a missing half
# of the evidence makes a ceiling that never moves on the path that spends
# the most -- in production, where no fixture can see it.
live_check db 'Live-DB person and task spend' cargo test -p cog-observability --test person_and_task_spend -- --ignored
# The quota path asks the task graph who submitted a task once per request,
# and that read is served by an index whose first column has to be the one
# the lookup filters on. Neither half can be answered without a server: an
# index's leading column is what the planner reads out of `pg_index`, and
# whether the read uses it shows up only in a plan taken with sequential
# scans disabled. A name-only check stays green after the column order is
# swapped, which is the failure this is here to see.
live_check db 'Live-DB task attribution index' cargo test -p cog-storage --test task_attribution_index -- --ignored
# One commit per event is invisible in every reading that runs without a
# server: the statements' own durations are the same under both spellings,
# and only the server's commit counter shows that the event rate has become
# the commit rate -- which is the cost, because every commit waits on the
# WAL. A fixture that writes through the same pool cannot tell the two
# apart; `pg_stat_database` can.
live_check db 'Live-DB observability event write batching' cargo test -p cog-storage --test observability_events_batch_writes -- --ignored
# A stream delta is the one event class produced per token, so it is both
# the most numerous thing in the event log and the only one whose reader
# is a live subscriber rather than a query. Keeping it out of the table
# therefore has two halves, and dropping the write without keeping the
# broadcast would pass a test that only looks at the table.
live_check db 'Live-DB stream deltas stay out of the event log' cargo test -p cog-storage --test stream_deltas_reach_live_readers_but_not_the_event_log -- --ignored
# A stalled writer used to turn every surplus event into its own task
# waiting for a connection -- the repair cost more than the fault, and on
# 2026-11-04 it was the whole pool. Only the commit counter separates the
# two spellings, because both eventually write the rows.
live_check db 'Live-DB full event queue does not fan out' cargo test -p cog-storage --test a_full_write_queue_does_not_give_each_event_its_own_connection -- --ignored
# A schema statement's wait for its locks is a fact about the server's
# lock queue across two sessions, so nothing that runs without a server
# can see any of it: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` takes
# ACCESS EXCLUSIVE before it checks whether the column is already there,
# and once it is waiting, every later read of that table queues behind
# it. That is the shape a rolling restart walks into -- the incoming
# replica's schema sweep freezing the outgoing replica that is still
# serving -- and it is only visible as one session blocking another.
live_check db 'Live-DB schema lock bounds' cargo test -p cog-storage --test schema_ddl_lock -- --ignored --test-threads=1
# Splitting a store's schema into several lists buys the readers the
# difference between "closed while the shape changes" and "closed while
# the rows are walked", but it turns one transaction into three, so the
# order the lists land in is now something a start depends on. Neither
# wrong order is visible on the fresh database every other test builds:
# `SET NOT NULL` on a column the repair has not filled fails only on a
# table that predates the column, and a repair that runs before the
# `ADD COLUMN` it needs fails for the same reason. Both failures land at
# startup, on an old shape -- which is to say in production.
live_check db 'Live-DB memory schema lists' cargo test -p cog-memory --test schema_lists_land_in_order_on_a_legacy_table -- --ignored --test-threads=1
# The decision counters are a sum the database keeps, and the one read a
# DB-free test cannot take is two writers counting into the same cell at
# once: a store that reads, adds and writes back loses an increment, while
# a test that writes one row and reads it back passes against exactly that
# store. The table also refuses successes above attempts, and that
# invariant lives on the server -- the file that this table replaced could
# not have it at all.
live_check db 'Live-DB decision counters' cargo test -p cog-storage --test decision_stats_store -- --ignored
# The change ledger's reason column is written through one statement and
# read through a fixed column list, and both are the kind of coupling a
# DB-free test cannot see: a column added to the write and not the read
# fails only against a server, and "a reason outside the closed set is
# refused" is a fact about the column's own constraint. Without this the
# round trip that the reason column exists to make possible is never run.
live_check db 'Live-DB change-ledger store' cargo test -p cog-storage --test change_ledger_store -- --ignored
# The task graph is a ledger, and its history is append-only because a trigger
# refuses the rewrite. That is a fact about the server -- no fixture can hold a
# trigger. The same case reads the other direction too: an attempt closes in
# place and a removal still deletes, because a guard that refused those would
# block every completion the ledger makes.
live_check db 'Live-DB task ledger is append-only' cargo test -p cog-storage --test task_ledger_is_append_only -- --ignored
# The projection is derived from the ledger. This case deletes it, checks that
# the state reads still answer, and rebuilds: the one thing a rebuild cannot
# restore is the lease, and "presence is not history" is only observable on a
# server.
live_check db 'Live-DB task projection rebuilds from the ledger' cargo test -p cog-storage --test task_projection_rebuilds_from_the_ledger -- --ignored
# The command a person runs at three in the morning, end to end: the binary,
# its flags, its connection string, and the row count it prints. A library-level
# test would leave the wiring between those parts unread.
live_check db 'Live-DB task projection rebuild command' cargo test -p cog-storage --test the_rebuild_command_restores_the_projection -- --ignored
# The backup account's write path is statements whose whole content is what
# the server does with them: whether the conflict clause wrote a row decides
# if a repeated opening moved a start instant already recorded, whether the
# closing's guard matched is what makes a terminal row unrewritable, and
# whether the pass over the overdue runs concluded one row or two is decided
# by the deadline each of those rows carries rather than by anything the
# caller passes in. All of those are facts about `rows_affected` and about a
# `WHERE` the server evaluated, so a DB-free test cannot see any of them --
# the closed set on the outcome, likewise, is the column's constraint and
# not a check in the caller.
live_check db 'Live-DB backup-run store' cargo test -p cog-storage --test backup_run_store -- --ignored
# The transcript's three claims are the database's: that a turn is written
# as a pair in one statement so no reader sees a question whose answer is
# still in flight, that the bound trims per session rather than across the
# table, and that a new handle reads the same conversation -- the last one
# being the whole point, since what this replaced lived in one process.
live_check db 'Live-DB chat turn store' cargo test -p cog-storage --test chat_turn_store -- --ignored
# The agent conversation's claims are the database's rather than the
# caller's: that one exchange lands in both histories, which is decided by
# how the read side joins the two unit columns; that the retention bound
# trims per receiving peer so a chatty peer cannot push another
# conversation out of reach, which is decided by the window function the
# trim statement uses; and that an answer arriving after its row was
# written settles that row once -- which is the update's guard, not
# anything the caller can check. An in-memory store answers the first by
# construction and has no bound at all, so none are visible without a
# server.
live_check db 'Live-DB agent message store' cargo test -p cog-storage --test agent_message_store -- --ignored
# The catalog's one invariant is a unique constraint and its optional
# columns are nullable on purpose: that a version holds one content, that
# an absent row count stays absent instead of becoming zero, and that the
# ordering a restore depends on is the database's. All three are decided by
# the server, so no in-memory fake can see them.
live_check db 'Live-DB dataset catalog' cargo test -p cog-storage --test dataset_catalog -- --ignored
# The principle ledger's intake is keyed on `(slug, source)`, so every
# later reading of a principle lands on the row already queued for it, and
# whether that row is rewritten is decided by the conflict clause alone --
# which branch of an `ON CONFLICT` runs is a fact about the server, and so
# is whether `rows_affected` counted a row. A clause that leaves the
# existing row alone drops corrected readings while reporting the pass as
# done, and no reading of the account can show it: the queue is the only
# path to the account, so a reading it refused is one the account never
# gets. Without this step that shape passes everything.
live_check db 'Live-DB principle-ledger intake' cargo test -p cog-storage --test principle_ledger_intake -- --ignored
# The set a measuring pass looks at is a bounded query, and what the bound
# cuts is decided by an ordering the server evaluates. A row whose verdict
# is "a person holds this" writes no measurement, so it keeps a null
# `measured_at` and returns to the head of every pass; once more of them
# exist than the bound takes, the pass judges those same rows forever and
# reports a full set rather than a stopped one. No DB-free test can see
# which rows a `NULLS FIRST` ordering hands back.
live_check db 'Live-DB principle-ledger outstanding set' cargo test -p cog-storage --test principle_ledger_outstanding -- --ignored
# The census counts the corpus, and the account also holds rows that are
# not corpus: a gate records its own refusals under its own name, and
# those rows carry no state by construction. Which rows a grouped count
# covered cannot be seen from the values -- a count is one number per
# group either way -- so the predicate that separates the two has to be
# run against a server.
live_check db 'Live-DB principle-ledger census' cargo test -p cog-storage --test principle_ledger_census -- --ignored
# A refusal now carries the shape that caused it, and the shapes are rows
# in a table rather than a number: two shapes refused in the same second
# have to be two rows, the matched text has to survive verbatim, and a
# row written before the column existed has to stay uncounted instead of
# joining a shape that never produced it. All three are properties of what
# the insert and the grouped read do on a server -- the interesting ones
# are the same-instant collision and the null, neither of which a
# database-free double reproduces.
live_check db 'Live-DB principle refusal shapes' cargo test -p cog-storage --test principle_refusal_shapes -- --ignored
# Which principle governs a path is a question only the store can answer:
# the answer is a row whose `where_red` names the path, so it is the
# match the predicate evaluates and the order the rows come back in --
# both decided on the server, and the order is what makes two rounds of
# the same task see one arrangement rather than two. A database-free
# double would restate the caller's own assumption about both.
live_check db 'Live-DB principle gating paths' cargo test -p cog-storage --test principle_gating_paths -- --ignored
# The sample-log sweep is expressed almost entirely as one DELETE, so its
# correctness cannot be checked without a server; the DB-free tests beside
# it only cover the sweep period. Leaving these out of the gate is how a
# sweep that kept every row sharing the newest instant passed both the
# local gate and CI.
live_check db 'Live-DB metrics sample capacity sweep' cargo test -p cog-storage --test metrics_sample_cap -- --ignored
# Same shape one table over: the event log's sweep is one DELETE whose
# correctness is a claim about which rows a WHERE and an ORDER BY keep.
# The floor differs from the sample log's -- nothing inside the read
# window goes, whatever the capacity says -- and that difference only
# exists in the statement the server executes.
live_check db 'Live-DB observability event capacity sweep' cargo test -p cog-storage --test observability_event_cap -- --ignored
# The checkpoint sweep has two floors rather than one: the read window and
# every board pointer. The pointer is an anti-join, so "a row some board
# names is never deleted" is a fact about the server and nothing else --
# the DB-free test beside it reads the statement's text, which cannot tell
# an anti-join that excludes from one that returns the same rows.
live_check db 'Live-DB checkpoint capacity sweep' cargo test -p cog-storage --test checkpoint_cap -- --ignored
# Retention is one DELETE, and the exception inside it is what keeps the
# table honest: read rows expire, unread rows do not, however old they are.
# Dropping that predicate leaves a statement that still deletes, still
# reports a count and still shrinks a table, so nothing about the pass
# looks different until an unread notification is removed before its
# reader sees it. Only the server can say which rows a WHERE kept.
live_check db 'Live-DB notification retention' cargo test -p cog-storage --test notification_retention -- --ignored
# The sweep that closes the alert rows a dead process left firing is two
# predicates in one statement, and only the server can say which rows a
# WHERE kept: the liveness clock (a row nothing has refreshed) and the
# rule name (whose rows these are). Drop the clock and a live alert is
# closed the moment the bridge starts; drop the rule and every other
# producer's stale row goes with it, with nobody left to re-raise it.
# Both regressions still close *some* rows and still report a count.
live_check db 'Live-DB alert row sweep' cargo test -p cog-observability --test alert_row_sweep -- --ignored
# The prompt-and-response archive is retired by age, and the three claims
# this pass makes are the server's rather than the caller's: which rows a
# `ts < cutoff` keeps, that a second pass over the same cutoff finds
# nothing (so two replicas sweeping together is not a correctness
# question), and that an empty archive answers zero instead of an error --
# the last being what keeps the age rule quiet on a clean archive instead
# of making it disappear. The callback count is what says progress is
# reported per batch rather than once per pass, which is the difference
# between a first run against a grown backlog and a loop that looks
# stopped for as long as that run lasts.
live_check db 'Live-DB LLM exchange retention' cargo test -p cog-observability --test exchange_retention -- --ignored
# The per-task event archive retires whole streams, and every claim that
# makes it safe is the server's: that a quiet stream goes in full and that
# a stream whose newest event is inside the retention keeps even the
# events that are older than it (retirement is not a prefix delete, which
# is what both readers would misread as the start of a run), that a
# checkpoint pins its stream however old it is, that a second pass finds
# nothing, that an empty archive answers zero, and that the candidate
# query is answered from the timestamp index rather than by scanning the
# archive every two minutes. The callback count says progress is reported
# per batch, which is the difference between a first run over a grown
# backlog and a loop that reads as stopped for as long as that run lasts.
live_check db 'Live-DB task event retention' cargo test -p cog-storage --test task_event_retention -- --ignored
# The per-call token ledger is the one archive here whose rows are a
# derivation's input: the hourly fold keeps the sums, so a row the fold
# never reached is the only copy of that spend. The sweep therefore
# deletes on a pair of conditions -- past the retention *and* inside what
# the fold covers -- and only the server can say which rows a WHERE kept.
# Drop the second half and the statement still deletes, still reports a
# count and still shrinks the table; what disappears is the record of an
# hour the aggregate now reports as quiet forever, with nothing left to
# recompute it from. The floor also has to follow the fold down on its own,
# because the backfill walks the fold's oldest window backwards toward the
# ledger's oldest row: a sweep that read the floor once would stop retiring
# anything the moment the fold started catching up.
live_check db 'Live-DB LLM usage retention' cargo test -p cog-observability --test usage_retention -- --ignored
# Which replica may act is decided by one statement, and the acceptance
# for it is a question about two processes: two asks landing at once leave
# exactly one holder, and a holder that stops renewing loses the role to
# whoever asks next. The statement's text is guarded beside its producer
# without a server; that guard cannot tell a statement that yields from one
# that takes, so this is the half that holds.
live_check db 'Live-DB owner-lease arbitration' cargo test -p cog-storage --test owner_lease -- --ignored
# Which transitions the owner guard lets through is decided in the
# statement that runs the write, and the answer is not visible from the
# call sites: a check that reads status alone lets a unit move work it
# does not own, and the write succeeds either way. One thread, because
# each test builds the schema on the shared database first: run together
# they create the same types at once and the catalog refuses the second.
live_check db 'Live-DB task ownership guard' cargo test -p cog-storage --test task_ownership_store -- --ignored --test-threads=1
# A face is a task row, and every rule the governance layer trusts is a
# claim about which rows a query selects -- which rows are faces, which
# are open work for one, and which a handover moves. A read that matches
# the wrong type returns an empty registry, which reads as "no faces"
# rather than as a broken query, and a handover that moved a running task
# would leave its lease and its owner describing two units. Those shapes
# are invisible without a server, so the statements are run against one.
# One thread, because each test builds the schema on the shared database
# first: run together they create the same types at once and the catalog
# refuses the second.
live_check db 'Live-DB governance store reads' cargo test -p cog-storage --test governance_store -- --ignored --test-threads=1
# What a single pruning statement costs, and which rows it takes. The
# rank question is a plan question: ranking every row of the log first
# answers it for the whole table, which is a cost no reading of the
# statement's text shows.
live_check db 'Live-DB metrics surplus-delete statement' cargo test -p cog-storage --test metrics_surplus_delete -- --ignored
# The gauge read and the retirement pass were written as live tests and
# then never named by any gate, which is the same as not having them: a
# scrape pays for the newest-sample read continuously, and the retirement
# has to reach every table a retired name is stored in. Both run here so
# the claim is checked rather than asserted.
live_check db 'Live-DB metrics latest-sample read' cargo test -p cog-storage --test metrics_latest_reading -- --ignored
# A `_total` series is read by differencing two scrapes, so the stored
# total has to be accumulated -- and a boot-time seed of a closed set
# writes zeros over totals other processes have counted up, which only
# works while the write adds. Whether Postgres's upsert adds is a question
# only a server answers, and an assigning one would look healthy from
# every call site.
live_check db 'Live-DB counter totals accumulate' cargo test -p cog-storage --test metrics_counter_accumulation -- --ignored
# One task id is not one run: the pipeline hands the same id to its
# planner, generator and evaluator, and each reports only its own usage.
# The per-task row is read as the task's cost, so a later run has to add
# rather than replace it — and which one the conflict clause does is a
# fact about the server. Replacing keeps every row plausible and drops
# every role but the last, which no DB-free test can see.
live_check db 'Live-DB task census accumulates runs' cargo test -p cog-storage --test observability_task_census -- --ignored
# A task's log is written append-only and read in tasks, so the writer
# trims the task it just wrote to rather than the table. Two writes and a
# count cannot see either mistake: a trim that took the newest line keeps
# every count plausible, and a table-wide cap lets one busy task empty a
# quiet one's log without changing any total.
live_check db 'Live-DB task log stays inside its bound' cargo test -p cog-storage --test observability_task_log_bound -- --ignored
# A task's event offsets are the backend's to assign, and Postgres derives
# them from a count two writers can read before either has inserted --
# while a reclaimed lease presumes the previous holder stopped rather than
# stopping it. Whether the table refuses a position it already gave out,
# and whether a page follows the position or the task's whole history, are
# both questions only a server answers.
live_check db 'Live-DB event offset sequence' cargo test -p cog-storage --test events_offset_sequence -- --ignored
# The same append read back through `get_events` must mean the same event
# whichever backend is wired, and the one production runs stored the event
# itself in the payload column rather than the payload. Whether the read
# undoes exactly that much is a question only a server answers: an
# in-memory double answers it the way its own implementation behaves, and
# that is the implementation that was right.
live_check db 'Live-DB event payload round trip' cargo test -p cog-storage --test events_payload_round_trip -- --ignored
# Whether an event published through the trait object reaches the table
# is a question the concrete type cannot answer: production holds the
# gateway as `dyn ObservabilityGateway`, and the fault this pins is that
# the trait arm sent without writing while the table stayed empty and
# read as "no events happened". The write is spawned, so the wait for the
# row and the schema it lands in are both the server's to settle.
live_check db 'Live-DB observability event persistence' cargo test -p cog-storage --test observability_event_persistence -- --ignored
live_check db 'Live-DB retires a metrics name' cargo test -p cog-storage --test metrics_retirement -- --ignored
# One test in this binary restores a package written by an older image and
# kept on the host's volume; its input is not something a runner can build
# for itself, and the test refuses rather than passes when the input is
# absent. Naming it here is therefore a red that says nothing about the
# code. It is skipped by its exact name -- spelled with `=` so that the
# ignored-corpus contract still reads every word after ` -- ` as a flag --
# and runs on a host that was given an extracted package.
live_check db 'Live-DB backup/restore round trip' cargo test -p cogneva --test backup_pg -- --ignored --skip=pg_restore_loads_a_package_written_before_the_schema_moved_on
# The platform-identity lookup fails at parse time against the real
# schema (both tables own an `id`), so it cannot be caught without a
# server. Leaving it out of the gate is how a query that is only reached
# on a first login stays broken while every DB-free test passes.
live_check db 'Live-DB platform identity lookup' cargo test -p cog-storage --test platform_identity -- --ignored
# The notification store's insert is a statement only the server parses,
# and its owner is a column the contract carries as a string. Without a
# server a row that can never be written, a page boundary that repeats
# rows, or a read that answers for every user at once all leave every
# DB-free test green.
live_check db 'Live-DB notification store' cargo test -p cog-storage --test notification_store -- --ignored
# The token ledger's cache column is added by an ALTER that has to land on
# a table already holding rows, and the INSERT that fills it is a
# statement only the server parses. Neither face is visible without a
# server: without this step a column that never gets written, or a
# migration that rewrites the rows it lands beside, leaves every DB-free
# test green.
live_check db 'Live-DB LLM usage ledger' cargo test -p cog-observability --test usage_store_ledger -- --ignored
# The money read is a statement, and the projection above it is tested
# against a stand-in: a `GROUP BY` that forgets `price_book` returns a
# plausible total no price list produced, and a `SUM` wrapped in
# `COALESCE` reports an unpriced range as free. Both read correctly on a
# page and as a type. Only a server parses the statement.
live_check db 'Live-DB LLM usage spend by price book' cargo test -p cog-observability --test usage_store_spend -- --ignored
# The window fold's three claims are all decisions the server makes: that
# a window's totals are the ledger's sum over it, that rerunning a window
# lands the same rows with the same values (idempotence rests on the
# primary key), and that a known combination the window did not use still
# lands a zero row. A fold that rewrote a window's keys or dropped the
# idle rows would leave every call site and every DB-free test unchanged.
# The read's own claim is decided by the server too: the range reported
# back has to be the range of windows that were summed, because the fold
# lands closed hours and the trailing hour a caller asks for first is the
# one window it cannot have reached. A read that answered with the window
# it was asked for makes an unfolded hour and an idle one the same empty
# list, and only a server can say which windows exist to be summed.
live_check db 'Live-DB LLM usage window rollup' cargo test -p cog-observability --test usage_rollup -- --ignored
# The saved calls a call site decided not to make and the verdict a
# window's silence gets are two claims about rows one writer leaves and
# another reads: a skip is read back under the stage it named with the
# reason kept apart from the count, and the fold turns usage rows and
# silence into `necessary` / `documented_skip` / `unexpected_absent`. Both
# hinge on the server -- a `GROUP BY` that folds two stages into one, a
# conflict clause whose key is not the key the reader reads with, and
# whether a re-run lands the same rows are all decided there, and each of
# them passes every DB-free test beside it. The three verdicts are checked
# in one run because the difference between them is the claim: a face that
# can report only one of them answers the same word every time and passes
# any single assertion.
live_check db 'Live-DB LLM skip and spend verdict' cargo test -p cog-observability --test skip_and_verdict_round_trip -- --ignored
# The alert DDL is one multi-statement string sent through the simple
# query protocol, so the server parses all of it and a malformed statement
# anywhere inside stops every statement before it from landing. That is
# how the notification mark was absent on a deployment whose binary wrote
# it: the code compiled, the DB-free tests passed, and the only witness
# was a warning at startup. The queue queries are server decisions too --
# which rows a firing row without the mark returns, and whether a new
# firing episode clears the mark so the next episode is announced.
live_check db 'Live-DB alert schema and notification mark' cargo test -p cog-observability --test alert_schema -- --ignored
# Every store in the tree creates its schema through the shared bounded
# runner, and what the bound buys is an end rather than speed: a sweep
# whose locks are held gives them back and names the table, so the startup
# path reports a store that failed to initialise instead of a pod that
# never becomes ready. Only two sessions can witness that -- without a
# server the test does not fail, it never finishes -- and the failure
# message is the whole deliverable, since the caller's alternative is a
# silence. The test lives beside the runner, in the crate that owns it:
# the stores in other crates know the contract and not the wait.
live_check db 'Live-DB schema lock bounds (schema_lock_bound)' cargo test -p cog-storage --test schema_lock_bound -- --ignored --test-threads=1
# The alert mirror decides what to close by subtracting the keys it just
# read from the rows the store says it owns. Both ends are server
# answers: ownership is a JSONB label lookup, and "still firing" is a
# state column. A lookup that matched nothing would make the subtraction
# empty and the mirror would report a clean reconciliation every tick
# while every row it opened stayed firing -- the shape measured on
# 2026-10-23, six rows frozen at one second. The other half of the
# reading is that rows of other producers and resolved rows are *not*
# returned, because returning them would have the mirror announce
# recoveries that never happened.
# Two alert bridges -- the application pod and the evolution pod -- write
# one alerts table with no lease between them, and the row keeps only the
# last writer's verdict. These tests are about the data that makes a
# disagreement readable at all: the row names the pod that wrote it, and
# every writer's own verdict survives per (key, writer), tagged with the
# deployment that reached it. The DDL half matters for the same reason the
# notification mark's does -- it is one multi-statement string, so a
# statement broken in the middle leaves the table without a column the
# writes name, silently, on a tree where every DB-free test still passes.
live_check db 'Live-DB alert writer identity and verdicts' cargo test -p cog-observability --test alert_writer_divergence -- --ignored
live_check db 'Live-DB alert mirror ownership' cargo test -p cog-observability --test alert_mirror_source -- --ignored
# Adoption and retirement ask the store "which of my rules still have a
# firing row". A read that returns the newest N rows answers that
# question only for rules whose rows happen to be young: a busy rule
# pushes an older one past the cutoff, and the caller reads the empty
# answer as "it recovered" -- adopting nothing after a restart, or
# settling a row that never closed. The test seeds a thousand newer rows
# so a bounded read loses the three rows under test; the rule list
# replaces the row count.
#
# The step names the whole lib rather than filtering to the one test by
# name: a name filter that matches nothing exits 0, so the step would go
# green after a rename having run nothing, and the ignored-test corpus
# contract refuses to read a filter it cannot resolve to a test it knows.
#
# The second test under this step asks the same server a different
# question: whether a close is decided by the local miss count alone or by
# the row's shared sighting clock. Both need a database, so one step runs
# both.
live_check db 'Live-DB watcher adoption, retirement and close' cargo test -p cog-observability --lib -- --ignored
# A taste submission is evidence and a claimable row at once, and both
# faces are decided by one statement each: whether a second disposition
# rewrites the first fate, and whether a re-sent id replaces the words
# already stored. Both are questions only a server answers -- an
# implementation that overwrote either would leave every call site and
# every DB-free test unchanged, and would only show up as a submission
# whose recorded fate or wording disagrees with the work filed for it.
live_check db 'Live-DB taste intent claims' cargo test -p cog-storage --test taste_intent_store -- --ignored
# A finished task's work duration is taken from two stamps inside the
# row's JSON, and whether a row whose work time was never measured counts
# as a sample is the server's grouping -- neither is visible without one.
# Both tests empty the task and metrics tables, so the binary runs one
# test at a time, and it sits here because the census step below empties
# the task table last.
live_check db 'Live-DB task work samples' cargo test -p cog-storage --test task_kind_samples -- --ignored --test-threads=1
# The status census is one GROUP BY plus a branch that decides what a
# status string this build cannot name does to the total; neither is
# visible without a server. It runs last because it empties the task
# table on entry and exit.
live_check db 'Live-DB task status census' cargo test -p cog-storage --test task_status_counts -- --ignored
# Whether a ledger row that names something counts as attributed is a
# `NOT EXISTS` against the task graph, so only a server can answer it. It
# writes its rows into 2001 and deletes them, so it neither reads nor
# disturbs what the steps above leave behind.
live_check db 'Live-DB unattributed spend' cargo test -p cog-observability --test usage_unattributed -- --ignored

# 用 redis 的那几条再跑一遍，第二次必须给出同一个答案。
#
# 这一遍不是重复，是这两台服务器不是同一件事：CI 每条检查都碰上一台刚起的空
# 服务器，沙箱碰的是上一轮留下的那台。于是一条「自己写、不自己清」的用例在 CI
# 永远绿，在沙箱上第二轮才红——2026-11-06 实测：`wal_retirement` 往固定的
# `wal:probe_wal_retirement:*` 里追加而不先清，第一次种 3 条、第二次读到 6 条，
# 断言的 `left: Some(6) right: Some(3)` 说得像代码坏了，其实坏的是「这一轮看见
# 的世界取决于上一轮跑到哪」。它已经改成先清再种；这一段判的是这一族。
#
# 只对 redis 做：库里那几十条每条都要建 schema，再跑一遍是分钟级的钱；这四条
# 加起来几秒，而它们要的服务器与库里那些是同一种「会留着上一轮」的服务器。
while IFS= read -r line; do
    renamed="$(printf '%s\n' "${line}" | sed -E "s/^live_check redis '/live_check redis 'second run: /")"
    eval "${renamed}"
done < <(grep '^live_check redis ' "$0")

printf '\ncheck-live-services: %d 条通过' "${passed}"
if [ "${failed}" -gt 0 ]; then
    printf '，%d 条红：\n' "${failed}"
    printf '  - %s\n' "${failed_names[@]}"
    exit 1
fi
printf '，0 条红\n'
