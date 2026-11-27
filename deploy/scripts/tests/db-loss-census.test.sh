#!/usr/bin/env bash
# `scripts/db-loss-census.py` 的离线标本：库里的有损面（外键引用动作、分区摘除）与树那本
# 同一条判据——默认无损，有损必须写出「不丢会撑不住什么」。
#
# 钉四种形状：没有判词的外键／分区被点名；判成 loss 但理由没说性能资源的被点名；判词比
# 人口大也红；`keep` 与带理由的 `loss` 通过。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
census="$root/scripts/db-loss-census.py"
[ -f "$census" ] || { echo "FAIL: 普查器不在：$census" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_has() { if grep -qF -- "$2" < <(printf '%s\n' "$3"); then ok "$1"; else
  printf 'FAIL- %s\n        期望含：%s\n' "$1" "$2" >&2
  printf '%s\n' "$3" | sed 's/^/        实得: /' | head -20 >&2; fail=$((fail + 1)); fi; }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else
  printf 'FAIL- %s（期望退出码 %s，实得 %s）\n' "$1" "$2" "$3" >&2; fail=$((fail + 1)); fi; }

fx="$tmp/tree"
mkdir -p "$fx/scripts" "$fx/crates/x/src"
cp "$census" "$fx/scripts/db-loss-census.py"

cat >"$tmp/fks.tsv" <<'TSV'
orders.items_order_id_fkey|c|a
audit_logs.audit_logs_user_id_fkey|n|a
TSV
cat >"$tmp/parts.tsv" <<'TSV'
events
TSV

cat >"$fx/crates/x/src/lib.rs" <<'RS'
// db-loss: orders.items_order_id_fkey — loss — 子行是父行的从属记录，父行被删即整件事被擦除；不丢会让容量撑不住
// db-loss: audit_logs.audit_logs_user_id_fkey — keep — 引用动作已改为 RESTRICT，父行删不掉
// db-loss: events — loss — 老分区落在保留期之外，不丢会让磁盘撑不住
RS

run() { python3 -I "$fx/scripts/db-loss-census.py" --check \
    --fks-file "$tmp/fks.tsv" --partitions-file "$tmp/parts.tsv" 2>&1; }

out=$(run); rc=$?
ck_rc "带理由的有损与外键 keep ⇒ 0" 0 "$rc"
ck_has "把人口与处置印出来" "undisposed=0" "$out"

# 判成 loss 却没说什么会撑不住。
cat >"$fx/crates/x/src/lib.rs" <<'RS'
// db-loss: orders.items_order_id_fkey — loss — 先这样
// db-loss: audit_logs.audit_logs_user_id_fkey — keep — 引用动作已是 RESTRICT
// db-loss: events — loss — 老分区在保留期外，不丢会让磁盘撑不住
RS
out=$(run); rc=$?
ck_rc "loss 的理由没说撑不住什么 ⇒ 1" 1 "$rc"
ck_has "点名那条外键" "orders.items_order_id_fkey" "$out"

# 没人写判词：默认无损，有损就是未处置。
cat >"$fx/crates/x/src/lib.rs" <<'RS'
// db-loss: orders.items_order_id_fkey — keep — 引用动作已改为 RESTRICT
RS
out=$(run); rc=$?
ck_rc "没有判词的外键／分区 ⇒ 1" 1 "$rc"
ck_has "点名外键" "audit_logs.audit_logs_user_id_fkey" "$out"
ck_has "点名分区表" "events" "$out"

# 判词比人口大。
cat >"$fx/crates/x/src/lib.rs" <<'RS'
// db-loss: orders.items_order_id_fkey — keep — 引用动作已改为 RESTRICT
// db-loss: audit_logs.audit_logs_user_id_fkey — keep — 引用动作已改为 RESTRICT
// db-loss: events — keep — 没有维护器摘分区
// db-loss: gone.table_id_fkey — keep — 已经不在库里
RS
out=$(run); rc=$?
ck_rc "判词比人口大 ⇒ 1" 1 "$rc"
ck_has "点名那条判词" "gone.table_id_fkey" "$out"

# 没有理由的判词当场拒绝。
cat >"$fx/crates/x/src/lib.rs" <<'RS'
// db-loss: orders.items_order_id_fkey — loss —
RS
out=$(run); rc=$?
ck_rc "判词没有理由 ⇒ 非零" 1 "$rc"
ck_has "说清为什么不收" "没有理由" "$out"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
