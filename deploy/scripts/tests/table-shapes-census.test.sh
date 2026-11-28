#!/usr/bin/env bash
# `scripts/table-shapes-census.py` 的离线标本：表的三种形状只留两种。
#
# 判据：一份「权威文档」只有在 why 里点名**流里没有的那份输入**（凭据、外部身份、
# 并发原语当前持有者）时成立；只说「读者问的是现在」不算——`task_current` 就是当前值，
# 它仍是投影。新增的文档写不出这份输入就红（存量清单在台账里，不在这里拦）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
census="$root/scripts/table-shapes-census.py"
[ -f "$census" ] || { echo "FAIL: 普查器不在：$census" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else
  printf 'FAIL- %s（期望退出码 %s，实得 %s）\n' "$1" "$2" "$3" >&2; fail=$((fail + 1)); fi; }
ck_has() { if grep -qF -- "$2" < <(printf '%s\n' "$3"); then ok "$1"; else
  printf 'FAIL- %s\n        期望含：%s\n' "$1" "$2" >&2
  printf '%s\n' "$3" | sed 's/^/        实得: /' | head -20 >&2; fail=$((fail + 1)); fi; }

fixture() {
    cat <<'RS'
pub const MUTABLE_TABLES: &[DeclaredTable] = &[
    DeclaredTable {
        table: "users",
        shape: TableShape::Document {
            why: "一个人是权威文档：改邮箱不是新事件，是同一个人的新描述，重算不出来。",
        },
    },
    DeclaredTable {
        table: "progress_bounds",
        shape: TableShape::Document {
            why: "一条进度轴的当前边界：行是那个边界的当前值，历史每一跳对它没有意义。",
        },
    },
];
RS
}

fixture >"$tmp/shapes.rs"
out=$(python3 -I "$census" --text "$tmp/shapes.rs" 2>&1)
ck_has "外部输入被认出来" "external	users" "$out"
ck_has "说「当前值」的进转换清单" "projection-candidate	progress_bounds" "$out"
ck_has "两类分开计数" "external_input=1	projection_candidates=1" "$out"

# 新增的文档写不出外部输入 ⇒ 红。
repo="$tmp/repo"
mkdir -p "$repo/scripts" "$repo/crates/cog-storage/src"
cp "$census" "$repo/scripts/table-shapes-census.py"
cp "$tmp/shapes.rs" "$repo/crates/cog-storage/src/table_shapes.rs"
git -C "$repo" init -q .
git -C "$repo" add -- scripts crates
git -C "$repo" -c user.name=t -c user.email=t@t commit -qm base

cat >"$tmp/entry.rs" <<'RS'
    DeclaredTable {
        table: "cog_context_boards",
        shape: TableShape::Document {
            why: "一块上下文板是这一轮装配出来的当前内容：再装配一次就覆盖掉同一块板。",
        },
    },
RS
python3 -I - "$repo/crates/cog-storage/src/table_shapes.rs" "$tmp/entry.rs" <<'PY2'
import sys
p, entry_path = sys.argv[1], sys.argv[2]
s = open(p).read()
entry = open(entry_path).read()
s = s.replace('\n];', '\n' + entry + '];', 1)
open(p, 'w').write(s)
PY2
git -C "$repo" add -- crates/cog-storage/src/table_shapes.rs
out=$(cd "$repo" && python3 -I scripts/table-shapes-census.py --check-staged 2>&1); rc=$?
ck_rc "新增的文档写不出外部输入 ⇒ 1" 1 "$rc"
ck_has "点名那张表" "cog_context_boards" "$out"

# 写得出的通过。
cat >>"$repo/crates/cog-storage/src/table_shapes.rs" <<'RS'
// 一份凭据散列：流里没有这份输入，也不该有。
RS
sed -i 's/一块上下文板是这一轮装配出来的当前内容：再装配一次就覆盖掉同一块板。/凭据散列是外部输入：流里没有这份输入，行本身就是真值。/' \
    "$repo/crates/cog-storage/src/table_shapes.rs"
git -C "$repo" add -- crates/cog-storage/src/table_shapes.rs
out=$(cd "$repo" && python3 -I scripts/table-shapes-census.py --check-staged 2>&1); rc=$?
ck_rc "写得出外部输入 ⇒ 0" 0 "$rc"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
