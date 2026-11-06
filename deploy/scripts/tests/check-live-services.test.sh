#!/usr/bin/env bash
# 需要真服务的检查只有一份清单，而这份清单是两处调用者共享的：上游 CI 的 `live`
# 那一格，以及树自己的变更验证（名字以 `check-` 开头，所以被当成一条门禁问）。
#
# 这条测试判的是「共享」这件事本身，不是那 64 条检查各自对不对——后者要真服务，
# 而这里要能在没有服务的机器上跑。判四件事：
#
#   1. `--list` 与清单是同一份：条数一致，没有两条同名。红的时候要靠名字点名，
#      两个同名条目会让摘要里那一行指不到是谁。
#   2. 缺服务时 exit 2，而且点名缺的是哪一个。全树的约定是「2 = 我跑不了」；把
#      缺服务记成通过，就是这条脚本存在要消灭的那种读数。
#   3. 清单要哪个服务是它自己算出来的，不是手抄的常量：只缺 redis 时只报 redis。
#      抄一份常量，加了一条只用 redis 的检查之后仍然对 db 提要求，或者在缺服务时
#      静默少跑一条。
#   4. 工作流里不许再出现第二条 `--ignored` 命令，而且必须调这条脚本。两份清单会
#      漂，漂了之后本地绿、上游红的差就回来了——那正是这次改动的起因。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${HERE}/../check-live-services.sh"
ROOT="$(cd "${HERE}/../../.." && pwd)"
WORKFLOW="${ROOT}/.github/workflows/ci.yml"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# 1. `--list` 与清单是同一份。
listing="$(bash "${SCRIPT}" --list)"
listed="$(printf '%s\n' "${listing}" | grep -c . || true)"
grep -c '^live_check ' "${SCRIPT}" >"${WORK}/declared" 2>/dev/null || true
declared="$(cat "${WORK}/declared")"
if [ "${declared}" -lt 30 ]; then
  fail "the list carries ${declared} checks; this test found 64 the day the list was written, and a list that shrank silently is the shape it exists to catch"
fi
if [ "${listed}" -ne "${declared}" ]; then
  fail "--list printed ${listed} names but the list declares ${declared} checks"
fi
duplicates="$(printf '%s\n' "${listing}" | sort | uniq -d)"
if [ -n "${duplicates}" ]; then
  fail "two checks share a name, so a red summary cannot say which one broke: ${duplicates}"
fi

# 2. 缺服务：exit 2，且点名缺的那两个。
env -u COGNEVA_TEST_DATABASE_URL -u COGNEVA_TEST_REDIS_URL bash "${SCRIPT}" \
  >"${WORK}/out" 2>"${WORK}/err"
status=$?
if [ "${status}" -ne 2 ]; then
  fail "with no service configured the script exited ${status}, not 2 (\"I cannot run this\"); a run that reports anything else here is a run that was read as a verdict"
fi
grep -q 'COGNEVA_TEST_DATABASE_URL' "${WORK}/err" ||
  fail "the refusal did not name COGNEVA_TEST_DATABASE_URL: $(cat "${WORK}/err")"
grep -q 'COGNEVA_TEST_REDIS_URL' "${WORK}/err" ||
  fail "the refusal did not name COGNEVA_TEST_REDIS_URL: $(cat "${WORK}/err")"
if [ -s "${WORK}/out" ]; then
  fail "the script printed check output before it decided it could not run: $(head -3 "${WORK}/out")"
fi

# 3. 只缺 redis 时只报 redis：要哪个服务由清单自己算。
env -u COGNEVA_TEST_REDIS_URL COGNEVA_TEST_DATABASE_URL=postgres://unused \
  bash "${SCRIPT}" >/dev/null 2>"${WORK}/err2"
status=$?
if [ "${status}" -ne 2 ]; then
  fail "with only the database configured the script exited ${status}, not 2"
fi
grep -q 'COGNEVA_TEST_REDIS_URL' "${WORK}/err2" ||
  fail "the refusal did not name the variable that is actually missing: $(cat "${WORK}/err2")"
if grep -q 'COGNEVA_TEST_DATABASE_URL' "${WORK}/err2"; then
  fail "the refusal named the database variable even though it is set: $(cat "${WORK}/err2")"
fi

# 4. 指向业务库时一条都不跑。清单里有几条一上来就清空整张任务表，而这类配错不会
#    自己红：那些用例清完表照样通过。
COGNEVA_TEST_DATABASE_URL=postgres://cog_app:x@db.invalid:5432/cogneva \
  COGNEVA_TEST_REDIS_URL=redis://redis.invalid:6379 bash "${SCRIPT}" \
  >"${WORK}/out3" 2>"${WORK}/err3"
status=$?
if [ "${status}" -ne 2 ]; then
  fail "a database URL naming the platform's own database exited ${status}, not 2 -- this is the one misconfiguration that does not announce itself"
fi
if [ -s "${WORK}/out3" ]; then
  fail "checks started against a non-test database: $(head -3 "${WORK}/out3")"
fi
grep -q 'cogneva' "${WORK}/err3" ||
  fail "the refusal did not name the database it was given: $(cat "${WORK}/err3")"

# 5. 这一跑自己建一条库，建不出来就一条都不跑。
#
#    这条判的不是「连得上有多少条红」——那要真服务。判的是两件不需要服务也能看的：
#    脚本问服务器的是建一条**新**库（名字里带这一跑的 pid 与起点时刻），而不是清空
#    调用方点名的那个；以及建不出来时 exit 2，一条检查都不起。后者尤其重要：库没
#    建出来而清单照跑，每条都会红在「连不上」，读起来像这棵树坏了。
mkdir -p "${WORK}/fakebin"
cat > "${WORK}/fakebin/psql" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >> "${PSQL_LOG}"
exit 1
EOF
chmod +x "${WORK}/fakebin/psql"
PSQL_LOG="${WORK}/psql.log" PATH="${WORK}/fakebin:${PATH}" \
  COGNEVA_TEST_DATABASE_URL=postgres://u:p@db.invalid:5432/cogneva_test \
  COGNEVA_TEST_REDIS_URL=redis://redis.invalid:6379 \
  bash "${SCRIPT}" >"${WORK}/out5" 2>"${WORK}/err5"
status=$?
if [ "${status}" -ne 2 ]; then
  fail "with no database to be built the script exited ${status}, not 2"
fi
if [ -s "${WORK}/out5" ]; then
  fail "checks ran even though the run's own database was never created: $(head -3 "${WORK}/out5")"
fi
grep -q 'CREATE DATABASE "cogneva_test_run_[0-9]*_[0-9]*"' "${WORK}/psql.log" ||
  fail "the script did not create a database of its own: $(cat "${WORK}/psql.log" 2>/dev/null)"
if grep -q 'DROP DATABASE "cogneva_test"' "${WORK}/psql.log"; then
  fail "the script dropped the database the caller named instead of building its own"
fi
grep -q '/postgres' "${WORK}/psql.log" ||
  fail "the create was not issued against the \`postgres\` database, which is the one initdb always makes: $(cat "${WORK}/psql.log" 2>/dev/null)"

# 6. 工作流里只剩这一条路。
if grep -q -- '--ignored' "${WORKFLOW}"; then
  fail "${WORKFLOW} still carries an \`--ignored\` command. The list is the only copy; a second copy is the one that drifts, and that drift is what makes a red show up on the upstream run and nowhere before it"
fi
if ! grep -q 'bash deploy/scripts/check-live-services.sh' "${WORKFLOW}"; then
  fail "${WORKFLOW} does not run the list, so nothing upstream checks these 64 commands at all"
fi
# 带参数就会在本地被记成「这棵树给不出来」而跳过——那正是这 64 条原先的样子。
if grep -q 'bash deploy/scripts/check-live-services.sh ' "${WORKFLOW}"; then
  fail "${WORKFLOW} passes the list an argument, which the local verification reads as a command it cannot reproduce and skips"
fi

if [ "${failures}" -ne 0 ]; then
  echo "${failures} check(s) failed" >&2
  exit 1
fi
echo "live-check list: one copy, ${declared} checks, refuses to run without the services it needs"
