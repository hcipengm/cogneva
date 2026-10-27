#!/usr/bin/env bash
# principle: feedback_a_guard_that_nothing_asserts_can_be_deleted_in_silence
# check-gate-carriers.py 的定向测试。
#
# 这条门禁判的是「谁跑这个门禁」。它之所以值得一条门禁，是因为失败的形状不是报错：
# 一个 check-* 脚本没有任何调用点，它就不会执行——而 CI 上「从不执行」与「一直绿」
# 完全一样。所以每一格都造一棵能判的树，然后只改一处：要么给一个门禁去掉载体（该
# 红），要么把「提及」摆成一个看起来像载体的形状（该红——提及不是跑），要么给一个
# 载体（该绿）。断言都落在**点名了哪一个**上，不只看退出码。
#   1. 真树 → 0，并且报出的门禁条数大于 1（不是空跑全绿）；
#   2. 最小树里四个载体各一条 → 0（ci / runtime / script / test 各认一次）；
#   3. 加一个没人跑的 check-* → 红，点名它；
#   4. 给它加头部的 `# carrier: …` → 绿；
#   5. 同一行写在代码之后 → 仍红（声明只认头部注释块）；
#   6. `# carrier:` 后面是空的 → 仍红（空声明不是声明）；
#   7. 只在 CI 的**注释**里出现名字 → 仍红（注释不是调用）；
#   8. CI 点名一个树里没有的 check-* → 红，判词是「陈旧的登记」；
#   9. 目录里一个 check-* 都没有 → 2（判不了），不是 0；
#  10. deploy/scripts 不在 → 2。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-gate-carriers.py"
[ -f "$gate" ] || { echo "FAIL: 没有 $gate" >&2; exit 1; }

pass=0; fail=0
ck() { # 名称 期望退出码 输出 实得退出码
  if [ "$4" -eq "$2" ]; then pass=$((pass + 1)); echo "ok    - $1"; else
    fail=$((fail + 1)); echo "FAIL  - $1（期望 rc=$2，实得 $4）"; echo "$3" | sed 's/^/        /'; fi
}
ck_has() {
  if printf '%s' "$3" | grep -qF -- "$2"; then pass=$((pass + 1)); echo "ok    - $1"; else
    fail=$((fail + 1)); echo "FAIL  - $1（输出里没有「$2」）"; echo "$3" | sed 's/^/        /'; fi
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
run() { python3 -B "$gate" "$1" 2>&1 || true; }
rc() { python3 -B "$gate" "$1" >/dev/null 2>&1; echo $?; }

# 最小树：一个门禁一个载体，四种载体各走一次。
seed() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy/scripts/tests" "${work}/root/.github/workflows"
  cat >"${work}/root/deploy/scripts/check-in-ci.sh" <<'EOF'
#!/usr/bin/env bash
EOF
  cat >"${work}/root/deploy/scripts/check-on-timer.sh" <<'EOF'
#!/usr/bin/env bash
EOF
  cat >"${work}/root/deploy/scripts/check-via-script.sh" <<'EOF'
#!/usr/bin/env bash
EOF
  cat >"${work}/root/deploy/scripts/check-via-test.sh" <<'EOF'
#!/usr/bin/env bash
EOF
  cat >"${work}/root/deploy/scripts/check-declared.sh" <<'EOF'
#!/usr/bin/env bash
# carrier: 人 —— 只在排查事故时手动跑
EOF
  cat >"${work}/root/deploy/scripts/install-timer.sh" <<'EOF'
#!/usr/bin/env bash
bash "$repo/deploy/scripts/check-on-timer.sh"
EOF
  cat >"${work}/root/deploy/scripts/runner.sh" <<'EOF'
#!/usr/bin/env bash
bash deploy/scripts/check-via-script.sh
EOF
  cat >"${work}/root/deploy/scripts/tests/via.test.sh" <<'EOF'
#!/usr/bin/env bash
gate="deploy/scripts/check-via-test.sh"
EOF
  cat >"${work}/root/.github/workflows/ci.yml" <<'EOF'
jobs:
  x:
    steps:
      - run: bash deploy/scripts/check-in-ci.sh
EOF
}

# 1. 真树
out="$(run "$repo")"
ck "1. 真树：绿" 0 "$out" "$(rc "$repo")"
count="$(printf '%s' "$out" | grep -c '^ok    - check-' || true)"
if [ "${count}" -gt 1 ]; then pass=$((pass + 1)); echo "ok    - 1. 真树判到 ${count} 个门禁（不是空跑）"
else fail=$((fail + 1)); echo "FAIL  - 1. 真树只判到 ${count} 个门禁"; fi

# 2. 最小树：四种载体 + 一条就地声明
seed
out="$(run "${work}/root")"
ck "2. 四种载体与一条声明：绿" 0 "$out" "$(rc "${work}/root")"
for label in ci runtime script test '就地声明'; do
  ck_has "2. 认出载体「${label}」" "${label}" "$out"
done

# 3. 一个没有人跑的门禁
seed
cat >"${work}/root/deploy/scripts/check-nobody-runs.sh" <<'EOF'
#!/usr/bin/env bash
# 谁跑我？
EOF
out="$(run "${work}/root")"
ck "3. 没人跑：红" 1 "$out" "$(rc "${work}/root")"
ck_has "3. 点名它" "check-nobody-runs.sh" "$out"

# 4. 就地声明之后变绿
cat >"${work}/root/deploy/scripts/check-nobody-runs.sh" <<'EOF'
#!/usr/bin/env bash
# carrier: 人 —— 换镜像前手敲一次
EOF
out="$(run "${work}/root")"
ck "4. 头上写了 carrier：绿" 0 "$out" "$(rc "${work}/root")"

# 5. 声明写在代码之后 → 不算头部
cat >"${work}/root/deploy/scripts/check-nobody-runs.sh" <<'EOF'
#!/usr/bin/env bash
echo hi
# carrier: 人 —— 这句写在正文里，不是头
EOF
out="$(run "${work}/root")"
ck "5. 正文里的 carrier：红" 1 "$out" "$(rc "${work}/root")"
ck_has "5. 仍然点名它" "check-nobody-runs.sh" "$out"

# 6. 空声明
cat >"${work}/root/deploy/scripts/check-nobody-runs.sh" <<'EOF'
#!/usr/bin/env bash
# carrier:
EOF
out="$(run "${work}/root")"
ck "6. 空的 carrier：红" 1 "$out" "$(rc "${work}/root")"

# 7. CI 只在注释里提名字 → 不是调用
seed
cat >"${work}/root/.github/workflows/ci.yml" <<'EOF'
jobs:
  x:
    steps:
      - run: bash deploy/scripts/check-in-ci.sh
      # 以后也许该跑 deploy/scripts/check-via-script.sh
EOF
rm -f "${work}/root/deploy/scripts/runner.sh"
out="$(run "${work}/root")"
ck "7. 注释里的名字不算载体：红" 1 "$out" "$(rc "${work}/root")"
ck_has "7. 点名 check-via-script" "check-via-script.sh" "$out"

# 8. 反向：CI 点名了树里没有的门禁
seed
printf '      - run: bash deploy/scripts/check-ghost.sh\n' >>"${work}/root/.github/workflows/ci.yml"
out="$(run "${work}/root")"
ck "8. 登记了不存在的门禁：红" 1 "$out" "$(rc "${work}/root")"
ck_has "8. 判词是陈旧的登记" "陈旧的登记" "$out"
ck_has "8. 点名那个名字" "check-ghost.sh" "$out"

# 9. 一个 check-* 都没有 → 判不了
rm -rf "${work}/empty"
mkdir -p "${work}/empty/deploy/scripts"
out="$(run "${work}/empty")"
ck "9. 空集合：2" 2 "$out" "$(rc "${work}/empty")"
ck_has "9. 说清是判不了" "判不了" "$out"

# 10. deploy/scripts 不在 → 判不了
rm -rf "${work}/bare"
mkdir -p "${work}/bare"
out="$(run "${work}/bare")"
ck "10. 目录不在：2" 2 "$out" "$(rc "${work}/bare")"
ck_has "10. 说清是判不了" "判不了" "$out"

printf '\n%s\n' "通过 ${pass}，失败 ${fail}"
[ "${fail}" -eq 0 ]
