#!/usr/bin/env bash
# `scripts/db-bounds-census.py` 的离线标本：库里的界，处置写在**它自己的声明处**——
# 设置写在 deploy/ 的声明里，列写在建表/读写那一行的 `// db-column:` 判词里。没有中央账。
#
# 钉四种形状：改过的设置必须有 deploy 声明且值一致；服务端缺省不点名；界形状的列没有
# 判词要点名、有判词要通过；判词比人口大也要红。另钉一条：`derived` 不点名来源当场拒绝。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
census="$root/scripts/db-bounds-census.py"
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
mkdir -p "$fx/scripts" "$fx/deploy" "$fx/crates/x/src"
cp "$census" "$fx/scripts/db-bounds-census.py"

run() { python3 -I "$fx/scripts/db-bounds-census.py" --check \
    --settings-file "$tmp/settings.tsv" --columns-file "$tmp/columns.tsv" 2>&1; }

# 人口：改过的设置（有声明）、改过的设置（没声明）、服务端缺省。
cat >"$tmp/settings.tsv" <<'TSV'
foo_limit|42|10|-|command line
foo_timeout|30|10|s|command line
bar_timeout|5|5|s|default
TSV
cat >"$tmp/columns.tsv" <<'TSV'
files.file_size
task_defined.declared_duration_seconds
TSV
cat >"$fx/deploy/postgres.yaml" <<'YAML'
args:
  - foo_limit=42
YAML

# 列的判词写在代码里。
cat >"$fx/crates/x/src/lib.rs" <<'RS'
// db-column: task_defined.declared_duration_seconds — not-a-bound — 任务自己声明的时长
RS

out=$(run); rc=$?
ck_rc "改过的设置没有 deploy 声明 ⇒ 1" 1 "$rc"
ck_has "点名那条设置" "foo_timeout" "$out"
if printf '%s' "$out" | grep -q "库设置 foo_limit"; then
    bad "有声明的设置被当成未登记"
else
    ok "有声明且值一致的设置通过"
fi
if printf '%s' "$out" | grep -q "库设置 bar_timeout"; then
    bad "服务端缺省被当成未登记"
else
    ok "服务端缺省不点名"
fi

# 值分家：deploy 写 42，库里是 43。
cat >"$tmp/settings.tsv" <<'TSV'
foo_limit|43|10|-|command line
TSV
out=$(run); rc=$?
ck_rc "库值与部署声明不一致 ⇒ 1" 1 "$rc"
ck_has "说清分家" "与 deploy/ 的声明 42 不一致" "$out"
cat >"$tmp/settings.tsv" <<'TSV'
foo_limit|42|10|-|command line
TSV

# 界形状的列没有判词要点名；判词在库里找不到也要红。
cat >"$tmp/columns.tsv" <<'TSV'
files.file_size
task_defined.declared_duration_seconds
principle_producer_pushes.period_seconds
TSV
out=$(run); rc=$?
ck_rc "界形状的列没有判词 ⇒ 1" 1 "$rc"
ck_has "点名那一列" "principle_producer_pushes.period_seconds" "$out"
cat >>"$fx/crates/x/src/lib.rs" <<'RS'
// db-column: principle_producer_pushes.period_seconds — not-a-bound — 生产者自报的周期
RS
out=$(run); rc=$?
ck_rc "每列都有判词 ⇒ 0" 0 "$rc"
ck_has "把人口与处置印出来" "undisposed=0" "$out"

cat >>"$fx/crates/x/src/lib.rs" <<'RS'
// db-column: gone.thing_seconds — not-a-bound — 已经不在库里
RS
out=$(run); rc=$?
ck_rc "判词比人口大 ⇒ 1" 1 "$rc"
ck_has "点名那条判词" "gone.thing_seconds" "$out"

# derived 不点名来源：当场拒绝，不是记账。
cat >"$tmp/columns.tsv" <<'TSV'
files.file_size
TSV
cat >>"$fx/crates/x/src/lib.rs" <<'RS'
// db-column: files.file_ttl — derived — -
RS
cat >"$tmp/columns.tsv" <<'TSV'
files.file_size
files.file_ttl
TSV
out=$(run); rc=$?
ck_rc "derived 判词不点名来源 ⇒ 非零" 1 "$rc"
ck_has "说清为什么不收" "标 derived 却没点名来源" "$out"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
