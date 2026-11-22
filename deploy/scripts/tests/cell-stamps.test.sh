#!/usr/bin/env bash
# 按格记账：一格判过、它读的东西没变，下一次就不必再判一遍。
#
# 为什么要这一格：门的戳是整份的。整门实测 8107 秒，其中脚本测试段约 2500 秒，而改一个
# 测试文件会让全部格子一起作废重跑——代价不是「判得仔细」，是每改一行付一遍两小时。
#
# 这一格判四件事，缺一条这套账就是假的：
#
#   1. 第一次跑：一格都没有账，每一格都真跑（没有账 ≠ 沿用，两者在读数上必须分得开）。
#   2. 一棵没动过的树再跑：每一格都沿用上次的判词，零格重判。
#   3. 动一个**被某格读过**的文件：读了它的那格重判，没读的照旧沿用。从子目录里按
#      相对路径读到的那一份，改它同样要重判（轨迹里只有一个名字，基准拼错就等于没判）。
#   4. 动判据自己的输入（`ci.yml`）：全部作废——判据变了，上一次的结论就不作数。
#   5. 量不出作用域（strace 没起来）：不记账，并且当场说一句。空作用域若当成键记下来，
#      这一格此后永不重判，而读数上与「判过、是绿的」同形。
#   6. 红的判词从**不沿用**：红的那一次常常停在第一条断言上，作用域因此比这一格真读的
#      少；修掉第一处之后，第二处要读的那份字节不在作用域里，键不变——沿用下去就是一句
#      永远修不掉的红，而它看起来像「同一处还没修好」。这里用最锋利的那一形来判：这一格
#      红在一份仓外的文件上，那份字节从来不在键里，修好它键一动不动。
#
# 反证：把 `cell_run` 换回 `run_step`（或让 `cell_key` 恒等于一个常数），第 2 条就红在
# 「没动过的树上有 N 格重判」上；只让键恒等于常数、不做作用域，第 3 条会红在「读了它的
# 那格没有重判」上。
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
# shellcheck source=tests/fixtures/stage-gate.sh
. "${here}/fixtures/stage-gate.sh"
gate="${repo}/deploy/scripts/check-before-push.sh"
fail() { echo "FAIL: $*"; exit 1; }
note() { echo "OK: $*"; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

if ! command -v strace >/dev/null 2>&1; then
    echo "SKIP: 本机没有 strace，量不出作用域；这一跑按格记账一格都不记（每格照旧重判），这里没有可判的东西"
    exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

export COGNEVA_BUILD_GATE_LOCK_DIR="${work}/no-gate"
export COGNEVA_BUILD_LOCK_FILE="${work}/build.lock"

tree="${work}/tree"
mkdir -p "${tree}/deploy/scripts/tests" "${tree}/.github/workflows" "${tree}/.github/scripts/tests" \
    "${tree}/fixtures" "${tree}/crates/fake/src" "${tree}/bin"
stage_gate "${repo}" "${tree}"

cat >"${tree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: a block the local runner cannot replay
        run: |
          echo one
YAML

# 两格，各自只读一个文件。作用域是量出来的，所以这里不给任何声明——门自己在 strace 下
# 跑一次、记下它开了哪些路径。
cat >"${tree}/deploy/scripts/tests/reads-x.test.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
cat fixtures/input-x.txt >/dev/null
SH
cat >"${tree}/deploy/scripts/tests/reads-y.test.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
cat fixtures/input-y.txt >/dev/null
SH
# 第四格**只在被追踪时失败**：它读自己的 TracerPid，被 strace 跟着时那一格不是 0。
# 追踪只用来量作用域，判词必须来自不带追踪的那一次——否则这一格会红在一件与它判的字节
# 无关的事上，而现实里真有一格是这样红的（2026-11-12：`bootstrap-entry.test.sh` 里
# 计时的判据在追踪下反向，四格同时红）。
cat >"${tree}/deploy/scripts/tests/hates-tracer.test.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[ "$(sed -n 's/^TracerPid:[[:space:]]*//p' /proc/self/status)" = 0 ]
SH
cat >"${tree}/.github/scripts/tests/noop.test.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exit 0
SH
# 第三格从**子目录里面**按相对路径读：`cd fixtures && cat input-rel.txt`。`openat` 那一行
# 里只有 `input-rel.txt` 与它自己的 dirfd，照着仓根拼出来的是 `${tree}/input-rel.txt`——
# 一个不存在的路径，或者更糟：仓根下同名的另一份文件。两种都让这一格永不重判，而「永不
# 重判」与「一直绿」在读数上同形。所以它单独成一格，判据是「改了那个相对路径，这一格重判」。
cat >"${tree}/deploy/scripts/tests/reads-rel.test.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
cd fixtures
cat input-rel.txt >/dev/null
SH
# 第六格：判词取决于一个**仓外**的文件。作用域只记仓里的路径，所以修好它时这一格的键
# 一动不动——沿用红的形状因此是「这一格读的东西没变，上次是红的」：一句永远不会自己消失
# 的红，而它看起来像「同一处还没修好」。现实里更常见的是红的那次停在第一条断言上、作用域
# 因此比这一格真判的短，同一个病。
outside="${work}/outside-flag"
cat >"${tree}/deploy/scripts/tests/red-until-fixed.test.sh" <<SH
#!/usr/bin/env bash
set -euo pipefail
[ "\$(cat ${outside})" = fixed ] || exit 1
SH
printf 'broken\n' >"${outside}"
printf 'x1\n' >"${tree}/fixtures/input-x.txt"
printf 'y1\n' >"${tree}/fixtures/input-y.txt"
printf 'r1\n' >"${tree}/fixtures/input-rel.txt"
printf 'nobody reads me\n' >"${tree}/fixtures/unrelated.txt"

printf '[package]\nname = "fake"\nversion = "0.0.0"\n' >"${tree}/crates/fake/Cargo.toml"
printf 'pub fn a() {}\n' >"${tree}/crates/fake/src/lib.rs"
cat >"${tree}/bin/cargo" <<'SH'
#!/usr/bin/env bash
exit 0
SH
chmod +x "${tree}/bin/cargo"

(
    cd "${tree}" || exit 1
    git init -q .
    git -c user.email=t@t.invalid -c user.name=t add -A
    git -c user.email=t@t.invalid -c user.name=t commit -qm base
    git update-ref refs/remotes/origin/main HEAD
    printf 'pub fn b() {}\n' >>crates/fake/src/lib.rs
    git -c user.email=t@t.invalid -c user.name=t add -A
    git -c user.email=t@t.invalid -c user.name=t commit -qm change
) >/dev/null 2>&1 || fail "造不出夹具（init / origin/main / 一次改动）"

run_gate() { # [标签]
    local out="${work}/out-${1:-run}.log"
    ( cd "${tree}" && HOME="${work}/nohome" PATH="${tree}/bin:${PATH}" \
        bash deploy/scripts/check-before-push.sh ) >"${out}" 2>&1 || true
    printf '%s\n' "${out}"
}

# 断言：一格（按名字）这一次是「沿用」还是「真跑」。门的每一格印一行 `== <说明>`，
# 沿用时同一行末尾带「（沿用 …」。
cell_state() { # <日志> <格名>
    local lines
    lines="$(grep -F '== ' "$1" | grep -F "$2" || true)"
    if [ -z "${lines}" ]; then
        echo absent
    elif printf '%s\n' "${lines}" | grep -qF '（沿用'; then
        echo hit
    else
        echo ran
    fi
}

# 判词看成本表那一行 `<秒>  ok|RED  <格名>`，不看「== 」那几句——那几句只说这一次是
# 真跑还是沿用。
cell_verdict() { # <日志> <格名>
    awk -v n="$2" '$2=="ok"||$2=="RED"||$2=="UNJUDGED" { if ($3==n || $3 ~ ("/" n "$")) v=$2 } END { print v }' "$1"
}

# ---- 1. 第一次跑：没有账，每一格都真跑 ----
out1="$(run_gate first)"
for cell in reads-x.test.sh reads-y.test.sh reads-rel.test.sh noop.test.sh hates-tracer.test.sh; do
    [ "$(cell_state "${out1}" "${cell}")" = ran ] ||
        fail "第一次跑时 ${cell} 不在「真跑」状态（没有账应当真跑，不是沿用）：$(grep -F "== ${cell}" "${out1}" || true)"
done
note "第一次跑：没有账的格子一律真跑"

# 追踪只用来量作用域：只在追踪下失败的格子，判词必须是**不带追踪**那一次的绿。
[ "$(cell_verdict "${out1}" hates-tracer.test.sh)" = ok ] ||
    fail "只在被追踪时失败的那一格被判成了 $(cell_verdict "${out1}" hates-tracer.test.sh)：追踪下的判词被当成判词了"
note "追踪下不是绿的格子，判词取自不带追踪的那一次"

# ---- 2. 树没动：每一格都沿用 ----
out2="$(run_gate unchanged)"
for cell in reads-x.test.sh reads-y.test.sh reads-rel.test.sh noop.test.sh hates-tracer.test.sh; do
    [ "$(cell_state "${out2}" "${cell}")" = hit ] ||
        fail "树没动过，${cell} 却没有沿用上次的判词：$(grep -F "== ${cell}" "${out2}" || true)"
done
note "树没动过：每一格都沿用上次的判词"

# ---- 2b. 红的判词不沿用：把它红的那份字节修好，它必须重判并转绿 ----
# 这一格红在一份仓外的文件上，那份字节不在键里：修好它时这一格的键一动不动。
[ "$(cell_state "${out2}" red-until-fixed.test.sh)" = ran ] ||
    fail "红的格子第二次跑仍然沿用了旧判词：$(grep -F '== red-until-fixed.test.sh' "${out2}" || true)"
printf 'fixed\n' >"${outside}"
out2b="$(run_gate red-refixed)"
[ "$(cell_state "${out2b}" red-until-fixed.test.sh)" = ran ] ||
    fail "修好它红的那份字节之后那一格没有重判（沿用了旧红）：$(grep -F '== red-until-fixed.test.sh' "${out2b}" || true)"
[ "$(cell_verdict "${out2b}" red-until-fixed.test.sh)" = ok ] ||
    fail "那份字节已经修好，这一格却还是 $(cell_verdict "${out2b}" red-until-fixed.test.sh)"
note "红的判词不沿用：修好它红的那份字节就转绿"

# ---- 3. 动一个被某格读过的文件：只有那格重判 ----
printf 'x2\n' >"${tree}/fixtures/input-x.txt"
out3="$(run_gate changed-x)"
[ "$(cell_state "${out3}" reads-x.test.sh)" = ran ] ||
    fail "读了 fixtures/input-x.txt 的那一格没有重判：$(grep -F '== reads-x.test.sh' "${out3}" || true)"
[ "$(cell_state "${out3}" reads-y.test.sh)" = hit ] ||
    fail "没读 input-x.txt 的那一格跟着重判了（作用域比真实读取宽）：$(grep -F '== reads-y.test.sh' "${out3}" || true)"
note "动了一个被读过的文件：读了它的格重判，没读的格沿用"

# ---- 3c. 相对路径也要落在它真实打开的那一份上 ----
# 这一条单独判：轨迹里只有 `input-rel.txt` 这个名字，拼错基准时它指向仓根下一份不存在
# （或同名的另一份）文件，那一格从此永不重判，而读数上它是绿的。
printf 'r2\n' >"${tree}/fixtures/input-rel.txt"
out3c="$(run_gate changed-rel)"
[ "$(cell_state "${out3c}" reads-rel.test.sh)" = ran ] ||
    fail "从子目录里按相对路径读的文件变了，那一格没有重判：$(grep -F '== reads-rel.test.sh' "${out3c}" || true)"
[ "$(cell_state "${out3c}" reads-y.test.sh)" = hit ] ||
    fail "相对路径那一格重判时，无关的 reads-y 也跟着重判了：$(grep -F '== reads-y.test.sh' "${out3c}" || true)"
note "子目录里的相对路径读取也进了作用域：改它，那一格重判"

# ---- 3b. 动一个没人读的文件：零格重判 ----
printf 'still nobody reads me\n' >"${tree}/fixtures/unrelated.txt"
out4="$(run_gate unrelated)"
for cell in reads-x.test.sh reads-y.test.sh; do
    [ "$(cell_state "${out4}" "${cell}")" = hit ] ||
        fail "没人读的文件变了，${cell} 却重判了（作用域比真实读取宽）：$(grep -F "== ${cell}" "${out4}" || true)"
done
note "动一个没有任何格读过的文件：零格重判"

# ---- 4. 动判据自己的输入：全部作废 ----
printf '# ci 改了\n' >>"${tree}/.github/workflows/ci.yml"
out5="$(run_gate ci-changed)"
for cell in reads-x.test.sh reads-y.test.sh; do
    [ "$(cell_state "${out5}" "${cell}")" = ran ] ||
        fail "ci.yml 变了，${cell} 仍然沿用旧判词（判据变了，上一次的结论就不作数）：$(grep -F "== ${cell}" "${out5}" || true)"
done
note "ci.yml 变了：全部重判"

# ---- 5. 量不出作用域：不记账，并且当场说一句 ----
# 空作用域也是一个**合法的键**——它说的是「树怎么变，这一格都不必重判」。而 strace 起不来
# 时量到的正是空。两者在账上必须分得开：记下来就等于替一个可能根本没跑起来的格子背书，
# 而它此后永不重判，读数上与「判过、是绿的」同形。
unit="${work}/unit"
mkdir -p "${unit}"
# shellcheck source=lib/cell-stamp.sh
. "${repo}/deploy/scripts/lib/cell-stamp.sh"
# 这一句只被 source 进来的 `cell_stamp_root` 读，shellcheck 看不见；导出去，让「账本在
# 哪里」是经环境交给它的，而不是在这个文件里凭空出现一个没人读的变量。
export COGNEVA_CELL_STAMP_DIR="${unit}"
out_unit="$(cell_run "一格什么都不读" true 2>&1)" || true
case "${out_unit}" in
    *"量不出作用域"*) ;;
    *) fail "量不出作用域时没有说出来，退回重判与判过同形 —— ${out_unit}" ;;
esac
[ -z "$(find "${unit}" -name verdict -print -quit)" ] ||
    fail "量不出作用域的一格仍然记了账：$(find "${unit}" -name verdict -print -quit)"
note "量不出作用域的一格不记账，并且说得出这一跑为什么没记"

# ---- 6. 格的键只对「装置」失效，不对别人那一格的措辞失效 ----
# 原先 `gate_inputs` 哈希的是整份门脚本：改一个只影响日志的槽标签，137 格的键同时变，
# 一次 105 格只沿用 2 格。格子自己的命令进了 `cell_id`，所以真正该全体作废的是装置——
# 函数体与顶层赋值。这一节把这两半分开验：
#   改别格的一行说明 -> 装置哈希不变；
#   改一个函数体     -> 装置哈希变。
machinery_hash() { # <脚本内容>
    printf '%s' "$1" | awk '
        /^[A-Za-z_][A-Za-z0-9_]*\(\) *\{/ { in_fn = 1 }
        in_fn { print; if ($0 ~ /^\}/) { in_fn = 0; print "" }; next }
        /^[A-Za-z_][A-Za-z0-9_]*=/ { print; next }
        /^set / { print; next }
    ' | sha256sum
}

base_script='set -uo pipefail
helper() {
    echo one
}
run_step "格子甲" true
run_step "格子乙" true
'
relabelled='set -uo pipefail
helper() {
    echo one
}
run_step "格子甲（改了措辞）" true
run_step "格子乙" true
'
helper_changed='set -uo pipefail
helper() {
    echo two
}
run_step "格子甲" true
run_step "格子乙" true
'

[ "$(machinery_hash "${base_script}")" = "$(machinery_hash "${relabelled}")" ] ||
    fail "只改了某一格的措辞，装置哈希也变了：整门 137 格会一起作废"
[ "$(machinery_hash "${base_script}")" != "$(machinery_hash "${helper_changed}")" ] ||
    fail "改了装置（函数体），装置哈希没变：所有格会沿用一份已经不对的判词"
note "格戳只对装置失效：改措辞不动哈希，改函数体才动"

# ---- 7. 轨迹有上限：一个格子的轨迹不会把盘写满，而且停下之后仍要判一次 ----
# 2026-11-19 实测：`host-target-gc.test.sh` 这一格判的是宿主机（回收器按 /proc 枚举
# 进程），而 `-y` 每解一个 fd 也读 /proc —— 轨迹一边写一边放大；本机实测 2026-11-19
# 本机五分钟写了 256 MiB，同一台机器盘上用量从 20 GiB 涨到 32 GiB。量作用域的仪器
# 把「判」变成了「填盘」。
# 这一节判三件事：超过上限要停下并说出来；停下之后按不带追踪重跑出判词；半份轨迹不记账
# （半份轨迹解出的作用域会让这一格永不重判，而永不重判与一直绿同形）。
unit_cap="${work}/unit-cap"
mkdir -p "${unit_cap}"
export COGNEVA_CELL_STAMP_DIR="${unit_cap}"
export COGNEVA_CELL_TRACE_MAX_BYTES=65536
status_cap=0
out_cap="$(cell_run "一格把轨迹写爆" bash -c 'for _ in $(seq 1 800); do cat /proc/self/stat >/dev/null; done' 2>&1)" || status_cap=$?
case "${out_cap}" in
    *"轨迹超过"*) ;;
    *) fail "轨迹超过上限时没有停下来说一句 —— ${out_cap}" ;;
esac
[ "${status_cap}" -eq 0 ] ||
    fail "停掉追踪之后没有按不带追踪重跑出判词：退出码 ${status_cap} —— ${out_cap}"
[ -z "$(find "${unit_cap}" -name verdict -print -quit)" ] ||
    fail "半份轨迹的那一格记了账：$(find "${unit_cap}" -name verdict -print -quit)"
[ ! -e "${TMPDIR:-/tmp}/cogneva-cell.$$.trace" ] ||
    fail "轨迹文件留在了盘上：${TMPDIR:-/tmp}/cogneva-cell.$$.trace"
note "轨迹超过上限时停下追踪、按不带追踪判一次，且不记半份作用域的账"
unset COGNEVA_CELL_TRACE_MAX_BYTES

echo "cell-stamps.test: all checks passed"

