#!/usr/bin/env bash
# principle: feedback_a_sample_i_write_myself_only_tests_the_gate_against_itself
# 红证跑法：给它一笔提交，它答出「父树上红在哪一条判据上」。
#
# 判据四条：
#   1. 跑的是**父树**：临时树里的源文件是这笔提交之前的那一版，不是它改过的那一版；
#   2. 放进父树的只有**新判据**（测试文件），源文件的改动不搬——搬了测的就不是父树；
#   3. 三种结局分得开：父树上红（0）、父树上绿（没有牙齿，1）、这一笔没有判据（2）；
#   4. 产品树不动：跑前跑后 `git status` 一样。
#
# 跑测试的那条命令由 `COGNEVA_RED_PROOF_TEST_CMD` 换成夹具里的替身，所以这里判的是
# 红证的接线（跑在哪棵树、判哪几条、怎么读三种结局），不是 cargo 本身。
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
script="${repo}/deploy/scripts/red-proof.sh"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
fixture="${work}/repo"
mkdir -p "${fixture}"
git -C "${fixture}" init -q
# `red-proof.sh` 在自己的仓里找对象库，所以夹具里也放一份它自己。
mkdir -p "${fixture}/deploy/scripts"
cp "${script}" "${fixture}/deploy/scripts/red-proof.sh"
chmod +x "${fixture}/deploy/scripts/red-proof.sh"

git_c() { git -C "${fixture}" -c user.email=gate@example.invalid -c user.name=gate "$@"; }

mkdir -p "${fixture}/crates/demo/src" "${fixture}/crates/demo/tests"
printf 'pub fn answer() -> u32 { 1 }\n' >"${fixture}/crates/demo/src/lib.rs"
printf 'docs\n' >"${fixture}/README.md"
git_c add -A
git_c commit -q -m 'the parent: answer() is 1'
parent="$(git_c rev-parse HEAD)"

# 这一笔：改源文件（1 → 2），并新增一条只有在新代码上才过的判据。
printf 'pub fn answer() -> u32 { 2 }\n' >"${fixture}/crates/demo/src/lib.rs"
cat >"${fixture}/crates/demo/tests/teeth.rs" <<'EOF'
#[test]
fn answer_is_two() {
    assert_eq!(demo::answer(), 2);
}
EOF
git_c add -A
git_c commit -q -m 'this batch: answer() is 2, and the test that says so'
subject="$(git_c rev-parse HEAD)"
# 一笔只改文档的提交：它没有判据可跑。
printf 'more docs\n' >>"${fixture}/README.md"
git_c add -A
git_c commit -q -m 'docs only'
docs="$(git_c rev-parse HEAD)"

# 测试命令的替身：把「在哪棵树、参数是什么」记下来，结局由 RED_PROOF_WANT 给。
cat >"${work}/fake-test" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\n' "${PWD}" "$*" >>"${RED_PROOF_LOG:?}"
case "${RED_PROOF_WANT:-red}" in
red) printf 'test result: FAILED. 0 passed; 1 failed\n' >&2; exit 101 ;;
green) printf 'test result: ok. 1 passed\n'; exit 0 ;;
esac
EOF
chmod +x "${work}/fake-test"

run_proof() { # <commit> [额外 env…]
    local c="$1"
    shift
    env "${@}" COGNEVA_RED_PROOF_TEST_CMD="${work}/fake-test" RED_PROOF_LOG="${work}/log" \
        bash "${fixture}/deploy/scripts/red-proof.sh" "${c}" >"${work}/out" 2>&1
}

before="$(git_c status --porcelain)"

# --- 1) 红：跑在父树上，只带新判据 ---
: >"${work}/log"
rc=0
env COGNEVA_RED_PROOF_TEST_CMD="${work}/fake-test" RED_PROOF_LOG="${work}/log" RED_PROOF_WANT=red \
    bash "${fixture}/deploy/scripts/red-proof.sh" --keep "${subject}" >"${work}/out" 2>&1 || rc=$?
[ "${rc}" = 0 ] || fail "父树上红了，红证却说没成立（rc=${rc}）：$(cat "${work}/out")"
grep -q '红证成立' "${work}/out" || fail "没说红证成立：$(cat "${work}/out")"
ran_dir="$(cut -f1 "${work}/log" | head -1)"
[ -n "${ran_dir}" ] || fail "替身一次都没跑到：$(cat "${work}/out")"
[ "${ran_dir}" != "${fixture}" ] || fail "红证在产品树里跑测试；它该在临时树里跑"
grep -qx 'pub fn answer() -> u32 { 1 }' "${ran_dir}/crates/demo/src/lib.rs" \
    || fail "跑的不是父树：源文件已经是这笔提交之后的那一版（$(cat "${ran_dir}/crates/demo/src/lib.rs")）"
[ -f "${ran_dir}/crates/demo/tests/teeth.rs" ] \
    || fail "父树里没有这一笔的新判据；那跑的是别人的测试，红证不成立"
[ "$(cat "${work}/log" | wc -l)" = 1 ] || fail "一个判据被跑成了多趟：$(cat "${work}/log")"
grep -q -- '--test teeth' "${work}/log" || fail "集成测试没有按 --test <name> 跑：$(cat "${work}/log")"

# --- 4) 产品树不动 ---
after="$(git_c status --porcelain)"
[ "${before}" = "${after}" ] || fail "红证在树上留了东西：$(diff <(echo "${before}") <(echo "${after}") || true)"
[ "$(git_c rev-parse HEAD)" = "${docs}" ] || fail "红证动了 HEAD"
# 这一例用了 --keep，所以那棵临时树还在；读它是这一例的断言要用的，读完整棵拆掉。
git -C "${fixture}" worktree remove --force "${ran_dir}" 2>/dev/null || rm -rf "${ran_dir}"

# --- 2) 绿：判据对这笔改动没有反应 ---
: >"${work}/log"
rc=0
run_proof "${subject}" RED_PROOF_WANT=green || rc=$?
[ "${rc}" = 1 ] || fail "父树上绿了，红证却没说「没有牙齿」（rc=${rc}）：$(cat "${work}/out")"
grep -q '没有牙齿' "${work}/out" || fail "没说判据没有牙齿：$(cat "${work}/out")"

# --- 3) 没有判据：不是绿，也不是红 ---
: >"${work}/log"
rc=0
run_proof "${docs}" RED_PROOF_WANT=red || rc=$?
[ "${rc}" = 2 ] || fail "没有判据的一笔被答成了 ${rc}，不是「没有判据」：$(cat "${work}/out")"
[ -s "${work}/log" ] && fail "没有判据还跑了测试：$(cat "${work}/log")"

# --- 5) --plan 只印要跑什么 ---
: >"${work}/log"
env COGNEVA_RED_PROOF_TEST_CMD="${work}/fake-test" RED_PROOF_LOG="${work}/log" \
    bash "${fixture}/deploy/scripts/red-proof.sh" --plan "${subject}" >"${work}/plan" 2>&1 \
    || fail "--plan 退了非零：$(cat "${work}/plan")"
grep -q 'cargo test -p demo --test teeth' "${work}/plan" \
    || fail "--plan 没印出要跑的那一条：$(cat "${work}/plan")"
[ -s "${work}/log" ] && fail "--plan 跑了测试：$(cat "${work}/log")"

# --- 6) 根提交：没有父树可问 ---
root_rc=0
run_proof "${parent}" RED_PROOF_WANT=red || root_rc=$?
[ "${root_rc}" = 2 ] || fail "父树这一笔自己才是根提交…（rc=${root_rc}）：$(cat "${work}/out")"

echo "PASS: 红证（父树上跑、只带新判据、红/绿/无判据三档分开、产品树不动、--plan 只印）"
