#!/usr/bin/env bash
# read-resource-account.sh 的离线测试：查询结果换成文件，不碰集群。
#
# 判的是判词那一层，不是时间戳排版：
#   1. 给定文件读不了 ⇒ 退出码 3，且不冒充「空账」。
#   2. 2026-10-19 的现网形状（两个 kind 各只有一堵平台自造的墙）⇒ readable，
#      coverage 两行都 external=0——**这是读数不是判据**，脚本不许为它报红或报绿。
#   3. 出现一堵外面的墙（`mainline advance:quota`）⇒ 单独点名，external=1。
#   4. 有未折叠行 ⇒ 总量、pending、最老时刻与年龄都给。
#   5. 查询回来了但没有 TOT 节（输出认不得）⇒ 退出码 3。
#   6. 卷容量环写进来的 subject 归到自造那一栏，不被读成「外面又多了一堵墙」。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="${here}/../read-resource-account.sh"
[ -f "$script" ] || { echo "FAIL: 读数脚本不在：$script" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_has() {
  if printf '%s\n' "$3" | grep -qF -- "$2"; then ok "$1"; else
    printf 'FAIL- %s\n        期望含：%s\n' "$1" "$2" >&2
    printf '%s\n' "$3" | sed 's/^/        实得: /' | head -25 >&2
    fail=$((fail + 1))
  fi
}
ck_absent() {
  if printf '%s\n' "$3" | grep -qF -- "$2"; then
    printf 'FAIL- %s\n        不该出现：%s\n' "$1" "$2" >&2; fail=$((fail + 1))
  else ok "$1"; fi
}
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else
  printf 'FAIL- %s（期望退出码 %s，实得 %s）\n' "$1" "$2" "$3" >&2; fail=$((fail + 1)); fi; }

work="$(mktemp -d)"; trap 'rm -rf "${work}"' EXIT
run() { COGNEVA_RESOURCE_EVENTS_FILE="$1" bash "$script" 2>&1; }

# 1. 文件读不了。
out="$(run "${work}/does-not-exist")"; rc=$?
ck_rc "文件读不了退出码 3" 3 "$rc"
ck_has "判词是读不到" "resource_account_verdict=unreadable" "$out"

# 2. 现网形状：两个 kind，各一堵自造的墙。
printf '1TOT\t53\t0\t\t\t\n2KIND\tadmission_refusal\trefused\t28\t2026-10-18 03:28:46.819633+00\t2026-10-19 00:38:30.565484+00\n2KIND\tbound_change\tapplied\t25\t2026-10-18 02:09:44.088667+00\t2026-10-18 23:34:25.605546+00\n3SUBJ\tadmission_refusal\tmainline advance:busy\t28\t2026-10-18 03:28:46.819633+00\t2026-10-19 00:38:30.565484+00\n3SUBJ\tbound_change\tbuild_gate.bound\t25\t2026-10-18 02:09:44.088667+00\t2026-10-18 23:34:25.605546+00\n' >"${work}/live.tsv"
out="$(run "${work}/live.tsv")"; rc=$?
ck_rc "现网形状可读，退出码 0" 0 "$rc"
ck_has "总量" "resource_events_total=53" "$out"
ck_has "折叠进度" "resource_events_pending=0" "$out"
ck_has "没有未折叠行" "resource_events_oldest_unfolded=<none>" "$out"
ck_has "拒绝类的覆盖" "coverage kind=admission_refusal subjects=1 self_raised=1 external=0" "$out"
ck_has "界变动类的覆盖" "coverage kind=bound_change subjects=1 self_raised=1 external=0" "$out"
ck_has "subject 逐条列出" 'subject kind=admission_refusal subject="mainline advance:busy" rows=28' "$out"
ck_has "判词可读" "resource_account_verdict=readable" "$out"
ck_absent "external=0 不冒充外部墙" "external kind=" "$out"

# 3. 外面的墙出现：单独点名，并从自造里分出去。
printf '1TOT\t2\t0\t\t\t\n2KIND\tadmission_refusal\trefused\t2\tx\ty\n3SUBJ\tadmission_refusal\tmainline advance:busy\t1\tx\ty\n3SUBJ\tadmission_refusal\tmainline advance:quota\t1\tx\ty\n' >"${work}/external.tsv"
out="$(run "${work}/external.tsv")"; rc=$?
ck_rc "外部墙出现时仍可读" 0 "$rc"
ck_has "点名外部那堵墙" 'external kind=admission_refusal subject="mainline advance:quota"' "$out"
ck_has "分档正确" "coverage kind=admission_refusal subjects=2 self_raised=1 external=1" "$out"
ck_absent "自造的墙不进 external" 'external kind=admission_refusal subject="mainline advance:busy"' "$out"

# 4. 有未折叠行：给最老时刻，并给年龄（秒）。
old="$(date -u -d '-120 seconds' '+%Y-%m-%d %H:%M:%S+00')"
printf '1TOT\t3\t1\t%s\t\t\n2KIND\tbound_change\tapplied\t3\t%s\t%s\n' "$old" "$old" "$old" >"${work}/pending.tsv"
out="$(run "${work}/pending.tsv")"; rc=$?
ck_rc "有未折叠行仍可读" 0 "$rc"
ck_has "最老未折叠时刻" "resource_events_oldest_unfolded=$old" "$out"
if printf '%s\n' "$out" | grep -qE '^resource_events_oldest_unfolded_age_seconds=[0-9]+$'; then
  ok "最老未折叠年龄是秒数"
else
  bad "最老未折叠年龄是秒数"; printf '%s\n' "$out" | sed 's/^/        实得: /' >&2
fi

# 5. 输出认不得（没有 TOT 节）：读不到，不是空账。
printf '2KIND\tbound_change\tapplied\t1\tx\ty\n' >"${work}/garbage.tsv"
out="$(run "${work}/garbage.tsv")"; rc=$?
ck_rc "认不得的输出退出码 3" 3 "$rc"
ck_has "认不得时说认不得" "resource_account_verdict=unreadable" "$out"

# 6. 卷容量环写的两族 subject：一堵自造墙（clamp）与一次界变动（grew）。
printf '1TOT\t2\t0\t\t\t\n2KIND\tadmission_refusal\trefused\t1\tx\ty\n2KIND\tbound_change\tapplied\t1\tx\ty\n3SUBJ\tadmission_refusal\tvolume_capacity.cogneva-evolution-source-pvc@admission_ceiling\t1\tx\ty\n3SUBJ\tbound_change\tvolume_capacity.cogneva-evolution-source-pvc\t1\tx\ty\n' >"${work}/volume.tsv"
out="$(run "${work}/volume.tsv")"; rc=$?
ck_rc "卷容量环的账可读" 0 "$rc"
ck_has "卷容量拒绝归自造" "coverage kind=admission_refusal subjects=1 self_raised=1 external=0" "$out"
ck_has "卷容量调界归自造" "coverage kind=bound_change subjects=1 self_raised=1 external=0" "$out"
ck_absent "卷容量不算外部的墙" "external kind=" "$out"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
