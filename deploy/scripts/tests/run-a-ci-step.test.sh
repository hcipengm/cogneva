#!/usr/bin/env bash
# `run-a-ci-step.sh` 自己能不能红：它判的是「红的步骤说得出是哪条测试」。
#
# 为什么要有这一层：这一步的全部价值在于注解里那几行字，而一个只会原样跑命令、
# 失败就退出的包装与没有包装在读数上完全一样——CI 面板上仍然只有 `exit code 101`。
# 所以这里逐格钉住：绿的时候不许发注解（发了就是把噪音当信号）、红的时候必须发、
# 发出来的那条必须点得出失败的测试名、认不出标记时也不许交白卷、退出码必须原样透传
# （吞掉它等于把红报成绿，那是这套东西唯一不能错的地方）。
#
# 跑法：bash deploy/scripts/tests/run-a-ci-step.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/run-a-ci-step.sh"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

[ -f "$runner" ] || { printf 'FAIL- 没有 %s\n' "$runner"; exit 1; }

# 1) 绿：原样退 0，且一个 `::error::` 都不许发。
bash "$runner" bash -c 'echo "test result: ok. 3 passed"' >"${work}/out" 2>&1
rc=$?
[ "$rc" -eq 0 ] && ok "绿：退 0" || bad "绿：应退 0" "0" "$rc"
grep -q '::error::' "${work}/out" && bad "绿：不该发注解" "无 ::error::" "有" || ok "绿：不发注解"

# 2) 红：退出码原样透传（3 与 101 各一格，别让 101 掩盖别的码）。
bash "$runner" bash -c 'exit 101' >"${work}/out" 2>&1
rc=$?
[ "$rc" -eq 101 ] && ok "红：退出码 101 透传" || bad "红：应退 101" "101" "$rc"
bash "$runner" bash -c 'exit 3' >"${work}/out" 2>&1
rc=$?
[ "$rc" -eq 3 ] && ok "红：退出码 3 透传" || bad "红：应退 3" "3" "$rc"

# 3) 红且认得出标记：注解里必须点得出失败的测试名，并给出条数。
cat >"${work}/fake-cargo.sh" <<'SH'
#!/usr/bin/env bash
echo "running 4 tests"
echo "test a_passing_case ... ok"
echo "test the_one_that_broke ... FAILED"
echo "failures:"
echo "    the_one_that_broke"
echo "test result: FAILED. 3 passed; 1 failed; 0 ignored"
exit 101
SH
bash "$runner" bash "${work}/fake-cargo.sh" >"${work}/out" 2>&1
rc=$?
[ "$rc" -eq 101 ] && ok "红：退出码仍透传" || bad "红：应退 101" "101" "$rc"
grep -q '::error::' "${work}/out" && ok "红：发了注解" || bad "红：应发注解" "有 ::error::" "无"
grep -q 'the_one_that_broke' "${work}/out" && ok "红：注解点得出失败的测试名" \
    || bad "红：注解应含 the_one_that_broke" "含" "$(head -c 200 "${work}/out")"
grep -q '1 failing test(s)' "${work}/out" && ok "红：注解带条数" \
    || bad "红：注解应带条数" "1 failing test(s)" "$(head -c 200 "${work}/out")"

# 4) 红但一个标记都没有：不许交白卷——退出码与尾巴都得在注解里。
bash "$runner" bash -c 'echo "container shutdown noise"; exit 101' >"${work}/out" 2>&1
rc=$?
[ "$rc" -eq 101 ] && ok "无标记：退出码仍透传" || bad "无标记：应退 101" "101" "$rc"
grep -q '::error::' "${work}/out" && ok "无标记：仍然发注解" || bad "无标记：仍应发注解" "有" "无"
grep -q 'container shutdown noise' "${work}/out" && ok "无标记：尾巴带进注解" \
    || bad "无标记：注解应含尾巴" "含 container shutdown noise" "$(head -c 200 "${work}/out")"

# 5) runner 的收尾行不算「在哪里失败」：只有它的时候必须落到尾巴那一格，而不是
#    把它当成失败的证据报上去。
bash "$runner" bash -c 'echo "##[error]Process completed with exit code 101."; exit 101' >"${work}/out" 2>&1
grep -q 'no marker recognised' "${work}/out" && ok "runner 收尾行不算标记" \
    || bad "runner 收尾行不该被当标记" "走尾巴那一格" "$(head -c 200 "${work}/out")"

# 6) 不给命令：退 2，且说得出是自己没拿到命令。
bash "$runner" >"${work}/out" 2>&1
rc=$?
[ "$rc" -eq 2 ] && ok "没有命令：退 2" || bad "没有命令：应退 2" "2" "$rc"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
