#!/usr/bin/env bash
# Deterministic gate for the host build-cache reclaimer.
#
# The reclaimer deletes files, so every judgement it makes has to be provable
# here, on fixtures, before it ever runs on a real work root. What is asserted:
# it refuses to guess a work root, it does nothing below the trigger, it
# escalates tier by tier and stops at the floor, it never touches a directory
# that merely is named `target`, and it leaves a tree alone while a process is
# building into it — by name and by an open file descriptor, which are two
# different legs of that judgement.
#
# The floor is what makes the tier order worth anything: reaching it is the
# reason the full-rebuild tier is not run. A real filesystem cannot be told to
# sit at 80% and then at 40% between two tiers, so the reading itself is the
# injected part, and the rest of the run is real.
#
# The fail-closed leg (a process of ours whose files cannot be read) has no
# fixture: a process of ours is readable by us by construction, so it can only
# arise from hidepid or a race. It is exercised by hand instead, and asserted
# here only to the extent that the run reports the count.
#
# The suite runs a copy of the script from a throwaway repo rather than the
# checkout in place, and that is a consequence of one of the judgements under
# test: the deletion path refuses to run anything but the committed version of
# the file, so a checkout with an edit in progress -- which is exactly the state
# a suite is run in -- would refuse every run. The copy is asserted byte-equal to
# the checkout's file, so what is exercised is still the code under test; what
# moves is only the version identity, which the fixture has to control to be able
# to test the refusal at all.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gc="${repo}/deploy/scripts/host-target-gc.sh"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
fake_pids=()
cleanup() {
  for pid in "${fake_pids[@]+"${fake_pids[@]}"}"; do kill "${pid}" 2>/dev/null || true; done
  rm -rf "${work}"
}
trap cleanup EXIT

fixture_repo="${work}/repo"
mkdir -p "${fixture_repo}/scripts"
cp "${gc}" "${fixture_repo}/scripts/host-target-gc.sh"
cmp -s "${gc}" "${fixture_repo}/scripts/host-target-gc.sh" \
  || fail "副本与检出里的脚本不一致；那测的就不是被测的代码"
git -C "${fixture_repo}" init -q
git -C "${fixture_repo}" -c user.email=gate@example.invalid -c user.name=gate \
  add scripts/host-target-gc.sh
git -C "${fixture_repo}" -c user.email=gate@example.invalid -c user.name=gate \
  commit -qm 'the reclaimer under test'
gc_run="${fixture_repo}/scripts/host-target-gc.sh"

readonly signature='Signature: 8a477f597d28d172789f06886806bc55'

# --- fixtures ----------------------------------------------------------------
# A cache tree carrying a stale artifact, a fresh one, an incremental cache, a
# coverage profile and a profile directory.
new_tree() { # name, tag signature (empty: no tag at all)
  local root="$work/host/$1"
  mkdir -p "${root}/target/debug/deps" "${root}/target/debug/incremental/x" \
    "${root}/target/llvm-cov-target/debug/deps"
  printf 'same bytes\n' >"${root}/target/debug/deps/libstale.rlib"
  touch -d '10 days ago' "${root}/target/debug/deps/libstale.rlib"
  printf 'same bytes\n' >"${root}/target/debug/deps/libfresh.rlib"
  printf 'same bytes\n' >"${root}/target/debug/incremental/x/dep-graph.bin"
  printf 'same bytes\n' >"${root}/target/llvm-cov-target/debug/deps/instrumented"
  if [ -n "$2" ]; then printf '%s\n' "$2" >"${root}/target/CACHEDIR.TAG"; fi
  printf '%s\n' "${root}/target"
}

tagged="$(new_tree tagged "${signature}")"
untagged="$(new_tree untagged 'Signature: 00000000000000000000000000000000')"
byname="$(new_tree byname "${signature}")"
byopen="$(new_tree byopen "${signature}")"

run_gc() { # env assignments as arguments
  env "${@}" COGNEVA_HOST_WORK_ROOT="$work/host" \
    COGNEVA_TARGET_GC_STATE="$work/state.json" \
    bash "${gc_run}" 2>&1
}

# --- 1) it refuses to guess --------------------------------------------------
if env -u COGNEVA_HOST_WORK_ROOT bash "${gc_run}" >"${work}/out" 2>&1; then
  fail "工作根没设时仍然跑完了；它会去猜一个目录，而猜错的那次是删文件"
fi
grep -q 'COGNEVA_HOST_WORK_ROOT is unset' "${work}/out" \
  || fail "拒绝的理由没说清是缺工作根：$(cat "${work}/out")"

if run_gc COGNEVA_TARGET_GC_TRIGGER_PCT=50 COGNEVA_TARGET_GC_FLOOR_PCT=70 >"${work}/out"; then
  fail "地板高于触发线时仍然跑完了；迟滞带被反过来，每次都会删到地板以下"
fi
grep -q 'must sit above the floor' "${work}/out" \
  || fail "没有拒绝地板高于触发线：$(cat "${work}/out")"

# --- 2) below the trigger nothing happens ------------------------------------
out="$(run_gc COGNEVA_TARGET_GC_TRIGGER_PCT=100)"
grep -q 'outcome=below_trigger' <<<"${out}" || fail "触发线以下没动作时没有报 below_trigger：${out}"
[ -f "${tagged}/debug/deps/libstale.rlib" ] \
  || fail "低于触发线时仍删了文件；判定门没关住"

# --- 3) the first tier releases what is stale and stops at the floor ---------
# The reading starts above the trigger and comes back below the floor as soon as
# the first stale artifact is gone, which is the whole point of the tiers: the
# run must stop, not carry on to the tier that costs a full rebuild of the tree.
# Which tree that first artifact belonged to is not part of the contract: the
# enumeration order decides it. The reclaimer sorts its candidates precisely so
# that order is a property of the decision rather than of the filesystem, and
# the assertions below are written to survive either order anyway -- an earlier
# version asserted that every tree gets its turn before the floor is reached,
# which is the opposite of the property under test and passed or failed by
# readdir order alone (it passed here and failed in CI).
# The release that ends the pass is the one that leaves the count at one, and
# what that rules out is the run sweeping every tree regardless of the floor.
reader="${work}/reader"
cat >"${reader}" <<EOF
#!/usr/bin/env bash
# Above the trigger while both trees still carry their stale artifact, below the
# floor as soon as one of them has lost it.
n=0
for tree in "${tagged}" "${byname}"; do
  if [ -e "\${tree}/debug/deps/libstale.rlib" ]; then n=\$((n + 1)); fi
done
if [ "\${n}" -ge 2 ]; then echo '800000 80'; else echo '400000 40'; fi
EOF
chmod +x "${reader}"
out="$(run_gc COGNEVA_TARGET_GC_DISK_READER="${reader}" \
  COGNEVA_TARGET_GC_TRIGGER_PCT=70 COGNEVA_TARGET_GC_FLOOR_PCT=50)"
grep -q 'outcome=released' <<<"${out}" || fail "够到地板后没有报 released：${out}"
grep -q 'after=40%' <<<"${out}" || fail "释放后的读数是注入的 40%，没有如实报出来：${out}"
grep -q 'not from the filesystem' <<<"${out}" \
  || fail "换过读数来源没有说明；用替代读数做的判定会和真读数长得一样：${out}"
remaining=0
for tree in "${tagged}" "${byname}"; do
  if [ -f "${tree}/debug/deps/libstale.rlib" ]; then remaining=$((remaining + 1)); fi
done
[ "${remaining}" -eq 1 ] \
  || fail "两棵树里剩 ${remaining} 份过期产物，应为 1：够到地板后还在往下删，或者第一档一份都没删掉"
for tree in "${tagged}" "${byname}"; do
  [ -f "${tree}/debug/deps/libfresh.rlib" ] \
    || fail "第一档删掉了刚写过的产物（${tree}）；这会把下一次构建变成全量重编"
  [ -d "${tree}/debug/incremental" ] \
    || fail "够到地板后还是动了第二档（${tree}）；分级释放的意义就在于停手"
  [ -d "${tree}/llvm-cov-target" ] || fail "够到地板后还是动了第三档（${tree}）"
done

# --- 4) a directory named target without cargo's tag is not a candidate ------
[ -d "${untagged}/debug/deps" ] \
  || fail "没有 cargo 缓存标签的目录被当成候选删掉了；名字不是证据"

# --- 5) escalating: every tier, and the tree keeps its tag -------------------
# The reading here is the real one, and a real filesystem does not fall to 0%
# because a fixture was deleted: the run has to walk every tier, then say the
# floor was out of reach instead of pretending it got there.
out="$(run_gc COGNEVA_TARGET_GC_TRIGGER_PCT=1 COGNEVA_TARGET_GC_FLOOR_PCT=0)"
grep -q 'outcome=floor_unreachable' <<<"${out}" \
  || fail "全档跑完没够到地板时没有报 floor_unreachable：${out}"
grep -q 'cache is not what is filling' <<<"${out}" \
  || fail "够不到地板时没有把「缓存不是原因」说出来：${out}"
[ -e "${tagged}/debug/incremental" ] && fail "第二档没有删掉增量缓存"
[ -e "${tagged}/llvm-cov-target" ] && fail "第三档没有删掉覆盖率目录"
[ -e "${tagged}/debug" ] && fail "第四档没有删掉 profile 目录"
[ -f "${tagged}/CACHEDIR.TAG" ] \
  || fail "连缓存标签都删了；下一次运行会看不到这棵树"
[ -d "${untagged}/debug/deps" ] || fail "无标签的那棵树在全档运行里被动了"

# --- 6) a tree being built into is left alone --------------------------------
# The trees the previous run released are refilled: this section deletes on its
# own terms, and an empty tree could not tell a held tree from a released one.
byname="$(new_tree byname "${signature}")"
byopen="$(new_tree byopen "${signature}")"
# Leg one: a process that builds, recognised by its name, sitting in its own
# worktree — the tree it writes follows from its working directory.
cp "$(command -v sleep)" "${work}/cargo"
( cd "${byname}" && exec "${work}/cargo" 60 ) &
fake_pids+=("$!")
# Leg two: a process that builds with its working directory somewhere else,
# writing into this tree — visible only through an open file descriptor. An fd
# is inherited across exec, so the process really holds this file open.
( cd "${work}" && exec 8<"${byopen}/debug/deps/libstale.rlib" && exec "${work}/cargo" 60 ) &
fake_pids+=("$!")
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [ "$(pgrep -u "$(id -u)" -x cargo | wc -l)" -ge 2 ] && break
  sleep 0.5
done
[ "$(pgrep -u "$(id -u)" -x cargo | wc -l)" -ge 2 ] \
  || fail "自检失败：造出来的写者进程没起来，下面几条断言会永远通过"

# The floor is out of reach here, so every tier runs; being in use has to hold
# across all of them, not just the cheap one.
out="$(run_gc COGNEVA_TARGET_GC_TRIGGER_PCT=1 COGNEVA_TARGET_GC_FLOOR_PCT=0)"
grep -q "leaving ${byname} alone: pid" <<<"${out}" \
  || fail "按进程名判的写者没有挡住清扫：${out}"
[ -f "${byname}/debug/deps/libstale.rlib" ] \
  || fail "有构建正在写入的树被删了；按进程名的判据 fail-open"
[ -d "${byname}/debug/incremental" ] || fail "被占用的树在第二档还是被动了"
[ -d "${byopen}/debug" ] || fail "被占用的树在第四档还是被动了"
grep -q "leaving ${byopen} alone: pid" <<<"${out}" \
  || fail "只看 cwd 的判据漏掉了从别处写进来的构建（${byopen}）：${out}"
[ -f "${byopen}/debug/deps/libstale.rlib" ] \
  || fail "持有该树内文件描述符的构建被无视了；fd 那条腿 fail-open"
grep -q 'unreadable=0' <<<"${out}" || fail "无法归属的进程没有被单列成读数：${out}"

# --- 7) no cache at all is a reading, not a failure --------------------------
empty="$work/empty"
mkdir -p "${empty}"
cat >"${work}/reader-full" <<'EOF'
#!/usr/bin/env bash
echo '800000 80'
EOF
chmod +x "${work}/reader-full"
out="$(env COGNEVA_HOST_WORK_ROOT="${empty}" COGNEVA_TARGET_GC_STATE="$work/state-empty.json" \
  COGNEVA_TARGET_GC_DISK_READER="${work}/reader-full" \
  COGNEVA_TARGET_GC_TRIGGER_PCT=70 COGNEVA_TARGET_GC_FLOOR_PCT=50 bash "${gc_run}" 2>&1)"
grep -q 'outcome=floor_unreachable' <<<"${out}" \
  || fail "没有候选时没有报 floor_unreachable：${out}"
grep -q 'cache is not what is filling' <<<"${out}" \
  || fail "够不到地板时没有把「缓存不是原因」说出来：${out}"

# --- 8) a dry run is a plan, at any water level ------------------------------
# The plan is what an operator asks for while deciding, and the asking does not
# wait for the day the disk is above the trigger. So a dry run answers below the
# trigger too, walks every tier (nothing it does can move the reading, so the
# floor never ends the pass), and reports `planned` -- not `released`, which
# would be a statement about a run that did not happen.
dry_tree="$(new_tree dryrun "${signature}")"
cat >"${work}/reader-below" <<'EOF'
#!/usr/bin/env bash
echo '400000 40'
EOF
chmod +x "${work}/reader-below"
out="$(env COGNEVA_HOST_WORK_ROOT="$work/host" COGNEVA_TARGET_GC_STATE="$work/state-dry.json" \
  COGNEVA_TARGET_GC_DISK_READER="${work}/reader-below" COGNEVA_TARGET_GC_DRY_RUN=1 \
  COGNEVA_TARGET_GC_TRIGGER_PCT=70 COGNEVA_TARGET_GC_FLOOR_PCT=50 bash "${gc_run}" 2>&1)"
grep -q 'outcome=planned' <<<"${out}" || fail "dry run 没有报 planned：${out}"
grep -q 'below trigger' <<<"${out}" \
  || fail "dry run 在触发线以下没有说明它给的仍然是计划：${out}"
grep -q 'stale file(s) under' <<<"${out}" \
  || fail "dry run 没有报第一档会释放多少：${out}"
grep -q 'full rebuild of this tree' <<<"${out}" \
  || fail "dry run 没有走到最深的档；计划不等于只走第一步：${out}"
[ -f "${dry_tree}/debug/deps/libstale.rlib" ] || fail "dry run 删了过期产物"
[ -d "${dry_tree}/debug/incremental" ] || fail "dry run 删了增量缓存"
[ -d "${dry_tree}/llvm-cov-target" ] || fail "dry run 删了覆盖率目录"

# --- 9) the run leaves its own reading ---------------------------------------
state="$(cat "$work/state.json")"
for key in outcome used_pct_before used_pct_after released_bytes gap_pct rev rev_state script; do
  grep -q "\"${key}\":" <<<"${state}" || fail "状态文件里没有 ${key}：${state}"
done

# --- 10) the carrier: a timer, wired from computed paths ---------------------
# The reclaimer is only worth anything if something runs it. The unit is
# generated rather than kept as a file, because a unit file with a path in it is
# a path someone has to keep true by hand; systemctl is stubbed here so that
# this asserts the wiring without enabling anything on the machine running the
# test.
installer="${repo}/deploy/scripts/install-host-target-gc.sh"
stub="${work}/bin"
mkdir -p "${stub}"
cat >"${stub}/systemctl" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"${work}/systemctl.calls"
case "\$*" in
*is-active*) echo active ;;
esac
EOF
chmod +x "${stub}/systemctl"

install() { # work root (empty: let it derive one)
  PATH="${stub}:${PATH}" XDG_CONFIG_HOME="${work}/config" \
    bash "${installer}" ${1:+"$1"} 2>&1
}
out="$(install "${work}/host")" || fail "安装脚本没跑通：${out}"
unit="${work}/config/systemd/user/cogneva-host-target-gc"
[ -f "${unit}.service" ] || fail "没有生成 ${unit}.service"
[ -f "${unit}.timer" ] || fail "没有生成 ${unit}.timer"
grep -q -F "ExecStart=${repo}/deploy/scripts/host-target-gc.sh" "${unit}.service" \
  || fail "服务没有指向本检出里的回收脚本：$(cat "${unit}.service")"
grep -q -F "Environment=COGNEVA_HOST_WORK_ROOT=${work}/host" "${unit}.service" \
  || fail "工作根没有写进服务环境：$(cat "${unit}.service")"
grep -q 'OnCalendar=' "${unit}.timer" || fail "定时器没有排期"
grep -q 'Persistent=true' "${unit}.timer" || fail "停机错过的运行不会被补上"
calls="$(cat "${work}/systemctl.calls")"
grep -q 'daemon-reload' <<<"${calls}" || fail "改了 unit 没有 daemon-reload：${calls}"
grep -q 'enable --now cogneva-host-target-gc.timer' <<<"${calls}" \
  || fail "定时器没有被启用：${calls}"
grep -q 'list-timers' <<<"${calls}" \
  || fail "启用之后没有回读排期；「启用」与「下一次会跑」不是同一句话：${calls}"

# The work root is the checkout's parent when the caller does not name one, and
# it is never guessed from a literal.
out="$(install "")" || fail "不给工作根时安装脚本没跑通：${out}"
grep -q -F "Environment=COGNEVA_HOST_WORK_ROOT=$(dirname "${repo}")" "${unit}.service" \
  || fail "不给工作根时没有落回检出的上一级：$(cat "${unit}.service")"

for bad in "relative/root" "${work}/not-there"; do
  if out="$(install "${bad}")"; then
    fail "工作根是 ${bad} 时仍然装上了；它会去删一个没验证过的目录：${out}"
  fi
done

# --- 11) only the committed version of this file may delete -------------------
# The unit runs the file straight out of the checkout, so an edit in progress is
# the code that runs -- on the day the disk is full enough to delete. Two cases
# are asserted with the same tree: committed, the run releases; one line appended
# to the copy, the run releases nothing and says why. The third case is a copy
# with no repo around it: it cannot say which version it is, and "cannot tell" is
# not "trusted".
gate_tree="$(new_tree gate "${signature}")"
gate_reader="${work}/gate-reader"
cat >"${gate_reader}" <<EOF
#!/usr/bin/env bash
# Above the trigger while the stale artifact is there, below the floor once a
# run has removed it -- so "the reading moved" is this fixture's evidence that
# something was actually released.
if [ -e "${gate_tree}/debug/deps/libstale.rlib" ]; then echo '900000 90'; else echo '400000 40'; fi
EOF
chmod +x "${gate_reader}"

out="$(run_gc COGNEVA_TARGET_GC_DISK_READER="${gate_reader}" \
  COGNEVA_TARGET_GC_TRIGGER_PCT=70 COGNEVA_TARGET_GC_FLOOR_PCT=50)"
grep -q 'outcome=released' <<<"${out}" || fail "提交过的版本没有动手：${out}"
grep -q 'rev_state=committed' <<<"${out}" || fail "读数里没有说这是提交过的版本：${out}"
grep -q 'rev=[0-9a-f]\{12\}' <<<"${out}" || fail "读数里没有版本号：${out}"
grep -q 'script=[0-9a-f]\{12\}' <<<"${out}" || fail "读数里没有脚本自身的哈希：${out}"
[ -f "${gate_tree}/debug/deps/libstale.rlib" ] \
  && fail "报 released 但过期产物还在；读数与动作不一致"

gate_tree="$(new_tree gate "${signature}")"
printf '\n# an edit nobody committed\n' >>"${gc_run}"
out="$(run_gc COGNEVA_TARGET_GC_DISK_READER="${gate_reader}" \
  COGNEVA_TARGET_GC_TRIGGER_PCT=70 COGNEVA_TARGET_GC_FLOOR_PCT=50)"
grep -q 'outcome=script_unverified' <<<"${out}" \
  || fail "改过还没提交的脚本仍然动手了：${out}"
grep -q 'rev_state=modified' <<<"${out}" || fail "没有说明它为什么不动手：${out}"
[ -f "${gate_tree}/debug/deps/libstale.rlib" ] \
  || fail "改过的脚本删了文件；删除路径必须只跑提交过的版本"
[ -f "${gate_tree}/debug/incremental/x/dep-graph.bin" ] \
  || fail "改过的脚本动了第二档"
# A dry run releases nothing, so it still answers with the plan -- and the plan
# carries the version it would run under.
out="$(run_gc COGNEVA_TARGET_GC_DRY_RUN=1 COGNEVA_TARGET_GC_DISK_READER="${gate_reader}" \
  COGNEVA_TARGET_GC_TRIGGER_PCT=70 COGNEVA_TARGET_GC_FLOOR_PCT=50)"
grep -q 'outcome=planned' <<<"${out}" || fail "dry run 被身份门挡掉了：${out}"
grep -q 'rev_state=modified' <<<"${out}" || fail "dry run 的计划没有带上版本身份：${out}"

plain="${work}/plain"
mkdir -p "${plain}"
cp "${gc}" "${plain}/host-target-gc.sh"
out="$(env COGNEVA_HOST_WORK_ROOT="$work/host" COGNEVA_TARGET_GC_STATE="$work/state.json" \
  COGNEVA_TARGET_GC_DISK_READER="${gate_reader}" COGNEVA_TARGET_GC_TRIGGER_PCT=70 \
  COGNEVA_TARGET_GC_FLOOR_PCT=50 bash "${plain}/host-target-gc.sh" 2>&1)"
grep -q 'outcome=script_unverified' <<<"${out}" \
  || fail "读不出身份的脚本仍然动手了；读不到不等于可信：${out}"
grep -q 'rev_state=norepo' <<<"${out}" || fail "没有说出读不出身份的原因：${out}"
[ -f "${gate_tree}/debug/deps/libstale.rlib" ] \
  || fail "读不出身份的脚本删了文件"

echo "PASS: 工作根不猜、低于触发线不动手、四档按序升级并在地板停手、名字不够格不当候选、构建中的树按进程名与 fd 两条腿都挡住、无候选时报「缓存不是原因」、dry run 在任何水位都给计划且不删、每次运行都留下自己的读数、只有提交过的版本才动手（改过的与读不出身份的都不删）、载体是算出来的定时器且启用后回读排期"
