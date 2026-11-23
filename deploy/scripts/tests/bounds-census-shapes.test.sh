#!/usr/bin/env bash
# `scripts/bounds-census.py` 认形状的本事：每一面各带一组标本——该认出的与不该认出的。
#
# 为什么要有这一条：普查器读到 0 时，「这棵树真没有」与「这一段没认出来」在输出上
# 同形（2026-11-23 实测：`walls` 与 `index-rows` 两面都读 0，没人分得开是哪一种）。
# 标本是唯一能把这两种「空」分开的东西：每一面都有至少一件它**必须**认出来的语料，
# 与至少一件它**不许**认成界的语料。
#
# 判的是函数本身，不是命令行输出：命令行只印计数，计数对不上一片语料时读不出红在哪。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
census="$root/scripts/bounds-census.py"
[ -f "$census" ] || { echo "FAIL: 普查器不在：$census" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/crates/a/src" "$tree/deploy/helm/cogneva/files" "$tree/scripts"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

cat >"$tree/crates/a/src/x.rs" <<'RS'
// cap: bound — 一份标记面的标本：它自己说了这个界从哪来
pub const BATCH_SIZE: usize = 16;

/// 一个 serde 默认值：它不是一个人拍的界，而是「没写时怎么办」的那条约定。
fn default_timeout_secs() -> u64 {
    30
}

pub fn purge() {
    sqlx::query("DELETE FROM task_rows WHERE id = $1");
    // DELETE FROM commented_out WHERE id = 1;
    let head: String = text.chars().take(32).collect();
}
RS

cat >"$tree/deploy/helm/cogneva/files/cogneva.json" <<'JSON'
{
  "app": {
    "limits": {
      "max_rows": 100,
      "ratio": 0.5,
      "enabled": true,
      "name": "not-a-number"
    },
    "list": [1, 2]
  }
}
JSON

# 后缀不在扫描面里的毁数据语句：它不该被数进来。
echo "DELETE FROM invented WHERE x = 1;" >"$tree/scripts/notes.txt"

rows() { # $1 = 面的函数名（不带 `surface_` 前缀）
  python3 -I - "$census" "$tree" "$1" <<'PY'
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location("bounds_census", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
face = getattr(module, "surface_" + sys.argv[3])
for row in face(pathlib.Path(sys.argv[2])):
    print(row["kind"], row["name"][:80])
PY
}

# 面一 markers：标了来源的界要认出来。
out=$(rows markers); rc=$?
printf '%s' "$out" | grep -q "BATCH_SIZE" \
    && ok "标记面：cap 标记认出来了" || bad "cap 标记没认出来（rc=$rc）：$out"

# 面二 consts：形如界的常量要认出来。
out=$(rows consts); rc=$?
printf '%s' "$out" | grep -q "BATCH_SIZE" \
    && ok "常量面：常量认出来了" || bad "常量没认出来（rc=$rc）：$out"

# 面三 config：数值叶子逐个认出来，布尔与字符串不许被当成数。
out=$(rows config); rc=$?
printf '%s' "$out" | grep -q "app.limits.max_rows" \
    && ok "配置面：整数叶子认出来了" || bad "整数叶子没认出来（rc=$rc）：$out"
printf '%s' "$out" | grep -q "app.limits.ratio" \
    && ok "配置面：浮点叶子认出来了" || bad "浮点叶子没认出来（rc=$rc）：$out"
printf '%s' "$out" | grep -q "app.list.0" \
    && ok "配置面：数组里的数也认" || bad "数组里的数没认出来（rc=$rc）：$out"
printf '%s' "$out" | grep -q "app.limits.enabled" \
    && bad "布尔被当成了界：$out" || ok "配置面：布尔不是数"
printf '%s' "$out" | grep -q "app.limits.name" \
    && bad "字符串被当成了界：$out" || ok "配置面：字符串不是数"
printf '%s' "$out" | grep -q "serde-default default_timeout_secs" \
    && ok "配置面：serde 默认函数认出来了" || bad "serde 默认函数没认出来（rc=$rc）：$out"

# 面四 destroys：真正的毁数据语句认出来；注释掉的、后缀不认识的不算。
out=$(rows destroys); rc=$?
printf '%s' "$out" | grep -q "DELETE FROM task_rows" \
    && ok "毁数据面：DELETE FROM 认出来了" || bad "DELETE FROM 没认出来（rc=$rc）：$out"
printf '%s' "$out" | grep -q "commented_out" \
    && bad "毁数据面把注释里那一句也数进来了：$out" || ok "毁数据面：注释不算"
printf '%s' "$out" | grep -q "\.chars()\.take(" \
    && ok "毁数据面：截断式 take 认出来了" || bad "截断式 take 没认出来（rc=$rc）：$out"
printf '%s' "$out" | grep -q "invented" \
    && bad "毁数据面扫了后缀之外的文件：$out" || ok "毁数据面：后缀之外的文件不扫"

# 这四面在这份标本上都必须**非空**：空产出与「这一面没跑」在命令行上是同一个读数。
for face in markers consts config destroys; do
  count=$(rows "$face" | wc -l)
  [ "$count" -gt 0 ] \
      && ok "面 $face 在这份标本上非空（$count）" \
      || bad "面 $face 在这份标本上读到 0——「没有」与「没认出来」分不开"
done

printf 'bounds-census-shapes.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
