#!/usr/bin/env bash
# principle: feedback_a_key_value_reading_parsed_to_end_of_line_swallows_the_fields_that_follow
# 空白机恢复的回执：可重入、按包认身份、逐段对账。
#
# 这里用假的 kubectl 与假的恢复实现，是因为被测的是「回执怎么判」，不是集群
# 怎么动。三段行为各自对应一个真会出现的现场：
#   1. 跑完一遍，重跑不重做（属主中途断线、重连后从下一段继续）；
#   2. 换一个包再跑，回执属于另一个包——不能接着上一个包的进度；
#   3. 恢复 Job 报的产物层条数与包里声明不等，回收执这一步必须红。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPT="$ROOT/deploy/scripts/restore-blank-machine.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

mkdir -p "$TMP/bin"
cat >"$TMP/bin/kubectl" <<'SH'
#!/usr/bin/env bash
# 只认脚本用到的那几种调用；别的一律失败，免得「假集群」比真集群还宽容。
args="$*"
case "$args" in
    "-n cogneva get deployment cogneva") exit 0 ;;
    "-n cogneva get deploy -o name") echo "deployment.apps/cogneva"; exit 0 ;;
    "-n cogneva rollout status "*) exit 0 ;;
    "-n cogneva logs job/"*) cat "$FAKE_JOB_LOG"; exit 0 ;;
    *) echo "fake kubectl: 不认识的调用: $args" >&2; exit 1 ;;
esac
SH
chmod +x "$TMP/bin/kubectl"

cat >"$TMP/restore.sh" <<'SH'
#!/usr/bin/env bash
# 假恢复：只证明调用方把包与 Job 名交了过来。
printf '%s\n' "$COGNEVA_RESTORE_JOB_NAME $1" >>"$FAKE_RESTORE_CALLS"
SH
chmod +x "$TMP/restore.sh"

# 一个最小的包：清单声明一个数据根、一个库、一份决策统计 => 产物层 2 项。
make_pkg() {  # make_pkg <名字> <artifact 行数>
    local dir="$TMP/${1}/cogneva-backup-20261019T000000Z"
    mkdir -p "$dir"
    local stores="" stats="null" i=0
    while [ "$i" -lt "$2" ]; do
        stores="$stores{\"name\":\"s$i\",\"versions\":1},"
        i=$((i + 1))
    done
    [ "$3" = "1" ] && stats='{"groups":1,"trials":2,"bytes":40}'
    printf '{"version":"0.1.0","revision":"r","instance_fingerprint":"fp","namespace":"cogneva","created_at":"2026-10-19T00:00:00Z","artifacts":[{"root":"app","data_dir":"/var/lib/cogneva-data","policies_present":true,"policy_stores":[%s],"decision_stats":%s}]}' \
        "${stores%,}" "$stats" >"$dir/manifest.json"
    tar --zstd -cf "$TMP/${1}.tar.zst" -C "$TMP/$1" "cogneva-backup-20261019T000000Z"
}

export PATH="$TMP/bin:$PATH"
export COGNEVA_RESTORE_PACKAGE_CMD="$TMP/restore.sh"
export COGNEVA_RESTORE_RECEIPT="$TMP/receipt"
export FAKE_RESTORE_CALLS="$TMP/calls"
export FAKE_JOB_LOG="$TMP/job.log"
: >"$FAKE_RESTORE_CALLS"

make_pkg good 1 1 # 1 个库 + 1 份统计 = 2 项
printf '%s\n' "artifact restored" "artifact restored" >"$FAKE_JOB_LOG"

# 1. 跑一遍：四段都过，回执里有包哈希与 Job 名。
bash "$SCRIPT" "$TMP/good.tar.zst" >/dev/null || fail "第一遍没跑通"
[ -s "$TMP/receipt/verify.txt" ] || fail "没有写回执"
grep -q "artifacts_restored=2" "$TMP/receipt/verify.txt" || fail "回执里的读数不对"
[ "$(wc -l <"$FAKE_RESTORE_CALLS")" = "1" ] || fail "恢复被调用的次数不对"
# 属主输入项：每一段都带这个数（事后能看出这条腿本来要人给东西），
# 用掉包字节的两段要点名是它；引擎与对账两段不依赖属主到场。
for f in package engine restore verify; do
    grep -qx "restore_needs_owner_input=1" "$TMP/receipt/$f.txt" \
        || fail "$f 回执没记「这一条腿要属主输入」"
done
grep -qx "owner_input=package_bytes" "$TMP/receipt/package.txt" \
    || fail "package 回执没记它用掉的是包的字节"
grep -qx "owner_input=package_bytes" "$TMP/receipt/restore.txt" \
    || fail "restore 回执没记它用掉的是包的字节"
grep -qx "owner_input=" "$TMP/receipt/verify.txt" \
    || fail "verify 回执声明了不存在的属主输入"

# 2. 重跑：不重做恢复，只把已记录的段读回来。
bash "$SCRIPT" "$TMP/good.tar.zst" >/dev/null || fail "第二遍没跑通"
[ "$(wc -l <"$FAKE_RESTORE_CALLS")" = "1" ] || fail "重跑又恢复了一次"

# 3. 换一个包：回执属于另一个包，必须停。
# 两份内容必须真的不同：同样的字节会被同一个 sha256 认成同一个包，那条路不该
# 判红（回执本来就允许重跑同一份）。
make_pkg other 2 1
if bash "$SCRIPT" "$TMP/other.tar.zst" >/dev/null 2>&1; then
    fail "换了包还接着上一次的进度跑"
fi

# 4. 新回执 + 声明的条数与 Job 报的对不上：对账那一段必须红。
export COGNEVA_RESTORE_RECEIPT="$TMP/receipt2"
printf '%s\n' "artifact restored" >"$FAKE_JOB_LOG" # 只报 1 项，包里声明 2 项
if bash "$SCRIPT" "$TMP/good.tar.zst" >/dev/null 2>&1; then
    fail "产物层条数对不上却判成恢复完成"
fi

# 5. 口令在环境里：空白机恢复要交互输入，不从宿主常驻环境里捡。这一条与
#    「包里解不开」不是一回事——它拦的是「钥匙已经常驻在这台机器上」。
export COGNEVA_RESTORE_RECEIPT="$TMP/receipt3"
if RESTIC_PASSWORD=ambient bash "$SCRIPT" "$TMP/good.tar.zst" >/dev/null 2>&1; then
    fail "宿主环境里的口令被当成了合法输入"
fi
[ ! -e "$TMP/receipt3/package.txt" ] || fail "被拒之后还是写了回执"

echo "blank-machine receipt OK"
