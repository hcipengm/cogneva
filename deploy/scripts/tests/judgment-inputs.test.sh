#!/usr/bin/env bash
# principle: feedback_a_projection_needs_a_named_projector_or_it_is_just_a_copy
#
# 判的是 `deploy/scripts/check-judgment-inputs.py` 本尊：一张表被判词脚本读、却没有生产
# 者，就是「改一格改判词」的路。夹具造小树，真门跑真树（本仓）。
#
# 每一格对着「门会怎么错」，不是「表长什么样」：
#   - 被读、没身份 ⇒ 红；读者注解说自己只验它 ⇒ 绿，摘要记成 subject
#   - 注解写在代码里，正文却不再碰那份文件 ⇒ 红（注解不替代码作证）
#   - 写者注解的正文不点那份表 ⇒ 红；两个写者匹配同一份表 ⇒ 红（一个生产者）
#   - 注解写不成／类写错／reads 写在非判词脚本里 ⇒ 红
#   - 通配只在一段里匹配：`data/*.yaml` 不把 `data/sub/x.yaml` 一起放行
#   - `**`、绝对路径、没有静态前缀的模式 ⇒ 红（写出来像是能跨目录的，宁可拒掉）
#   - yaml／json 按路径认（只提到文件名不算被读）；tsv 按文件名认
#   - 文件头不是身份：写着 `debt:`／`carrier:` 的手写表照样判红（没有欠账、没有到期、
#     没有棘轮这种中间态——中间态就是允许继续手写）
#   - 写者优先：机器写的表被门读来验仍是 generated，文件头只是一行注释
#   - 没人读的表不判；没数据文件／没判词脚本／根不在 ⇒ 退出码 3，不是绿
#   - 提交车道给索引时，判的是**索引里的字节**：工作树上删掉的注解不算数
#   - 真树是绿的：那张手写表（tunables.yaml）已经删掉，判词搬到了每个数值叶子头上；
#     再长出一张没有身份的手写表，这一格立刻变红

set -uo pipefail
# 索引那一格会在夹具里建仓并 `git add`：外层若导出了索引指针，那几笔写进的是别人正在
# 提交的那一份。其余格不建仓，但同一个变量也会让门改走索引模式，所以一并摘掉。
# 摘之前留一份：提交车道跑这一族时给的正是这一笔的索引，真树那一格要用它——不用它，
# 判的是共享工作树，别处还没落的表会替这一笔判红或判绿。
IDX_IN="${GIT_INDEX_FILE:-}"
unset GIT_INDEX_FILE COGNEVA_COMMIT_INDEX GIT_DIR GIT_WORK_TREE
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
GATE="$ROOT/deploy/scripts/check-judgment-inputs.py"

pass=0
fail=0
ok()  { printf 'ok    %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL  %s\n' "$1" >&2; fail=$((fail + 1)); }

[ -f "$GATE" ] || { bad "门不在：$GATE"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

run() { # $1=根 → stdout+stderr 合流，rc 由调用方从 $? 取
    python3 -I "$GATE" --root "$1" 2>&1
}

probe() { # $1=根，其余=判词脚本的正文行（注解也在这里给，行首在夹具文件里）
    local t="$1"; shift
    mkdir -p "$t/scripts"
    { printf '#!/usr/bin/env bash\n'
      for line in "$@"; do printf '%s\n' "$line"; done
    } > "$t/scripts/probe.test.sh"
}

writer() { # $1=树里的路径，其余=正文行
    local f="$1"; shift
    mkdir -p "$(dirname "$f")"
    { printf '#!/usr/bin/env bash\n'
      for line in "$@"; do printf '%s\n' "$line"; done
    } > "$f"
}

# 1. 被读、没有任何身份 ⇒ 红并点名
t="$work/1"; probe "$t" 'X="$ROOT/data/x.tsv"'; mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '没有身份：data/x.tsv' < <(printf '%s' "$out"); then
    ok "被判词脚本读、没有身份的表判红"
else
    bad "没有身份的表应红（rc=$rc）：$out"
fi

# 2. 读者注解：只读来验它 ⇒ 绿，摘要记一层 subject
t="$work/2"; probe "$t" '# reads: data/x.tsv subject' 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'subject 1' < <(printf '%s' "$out"); then
    ok "读者注解让一张只被验的表判绿"
else
    bad "读者注解应判绿（rc=$rc）：$out"
fi

# 3. 注解写了、正文却不碰那份文件 ⇒ 红（注解不替代码作证）
t="$work/3"; probe "$t" '# writes: data/x.tsv generated' 'nothing here'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '注解正文没有点到 data/x.tsv' < <(printf '%s' "$out"); then
    ok "注解所在的代码正文没有点到那份表时判红"
else
    bad "注解不替代码作证（rc=$rc）：$out"
fi

# 4. 写者注解 ⇒ 绿，摘要记成 generated
t="$work/4"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: data/x.tsv generated' 'out="$ROOT/data/x.tsv"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'generated 1' < <(printf '%s' "$out"); then
    ok "写者注解让机器写的表判绿"
else
    bad "写者注解应判绿（rc=$rc）：$out"
fi

# 5. 两个写者匹配同一份表 ⇒ 红（一个生产者，不是一个先到先得）
t="$work/5"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/a.sh" '# writes: data/x.tsv generated' 'out="$ROOT/data/x.tsv"'
writer "$t/gen/b.sh" '# writes: data/*.tsv generated' 'out="$ROOT/data/"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '两个写者匹配同一份表' < <(printf '%s' "$out"); then
    ok "两个写者匹配同一份表时判红"
else
    bad "重复的写者应红（rc=$rc）：$out"
fi

# 6. 注解只有两个字段（缺类） ⇒ 红
t="$work/6"; probe "$t" '# writes: data/x.tsv' 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '注解写不成' < <(printf '%s' "$out"); then
    ok "写不成形状的注解判红，不是被当成没有"
else
    bad "残缺的注解应红（rc=$rc）：$out"
fi

# 7. 类写错（写者说 subject） ⇒ 红
t="$work/7"; probe "$t" '# writes: data/x.tsv subject' 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '注解的类不对' < <(printf '%s' "$out"); then
    ok "writes 写成 subject 时判红"
else
    bad "写错的类应红（rc=$rc）：$out"
fi

# 8. reads 写在不是判词脚本的文件里 ⇒ 红（取判据的读者不能自己认领）
t="$work/8"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/read.sh" '# reads: data/x.tsv subject' 'X="$ROOT/data/x.tsv"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'reads 注解只写在判词脚本里' < <(printf '%s' "$out"); then
    ok "非判词脚本里的 reads 注解判红"
else
    bad "非判词面的 reads 应红（rc=$rc）：$out"
fi

# 9. 通配的 `*` 不跨目录：`data/*.yaml` 不把 `data/sub/x.yaml` 一起放行
t="$work/9"; probe "$t" 'X="$ROOT/data/sub/x.yaml"'
mkdir -p "$t/data/sub"; : > "$t/data/sub/x.yaml"
writer "$t/gen/make.sh" '# writes: data/*.yaml generated' 'out="$ROOT/data/"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'data/sub/x.yaml' < <(printf '%s' "$out"); then
    ok "通配的 * 不跨目录，子目录里的表仍要求身份"
else
    bad "通配不应跨目录匹配（rc=$rc）：$out"
fi

# 10. `**` 不支持 ⇒ 红（写出来像是跨目录、实际不跨，宁可拒掉）
t="$work/10"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: data/** generated' 'out="$ROOT/data/"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '通配写成了 **' < <(printf '%s' "$out"); then
    ok "通配写成 ** 时判红"
else
    bad "** 应被拒（rc=$rc）：$out"
fi

# 11. 绝对路径 ⇒ 红
t="$work/11"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: /data/x.tsv generated' 'out="/data/x.tsv"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '绝对路径' < <(printf '%s' "$out"); then
    ok "绝对路径的模式判红"
else
    bad "绝对路径应被拒（rc=$rc）：$out"
fi

# 12. 通配没有静态前缀（`*.yaml`）⇒ 红（否则整棵树是一个身份）
t="$work/12"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: *.tsv generated' 'out="$ROOT/data/x.tsv"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '静态前缀' < <(printf '%s' "$out"); then
    ok "没有静态前缀的通配判红"
else
    bad "无静态前缀应被拒（rc=$rc）：$out"
fi

# 13. yaml 按路径认：判词脚本只提到文件名，读的不是这张
t="$work/13"; probe "$t" 'X="$ROOT/other/x.yaml"'
mkdir -p "$t/data"; : > "$t/data/x.yaml"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && ! grep -qF 'data/x.yaml' < <(printf '%s' "$out"); then
    ok "yaml 按路径认，文件名相同不构成被读"
else
    bad "只被提到名字的 yaml 不应被判（rc=$rc）：$out"
fi

# 14. 同一个目录、按路径被读 ⇒ 红（后缀不是豁免）
t="$work/14"; probe "$t" 'X="$ROOT/data/x.yaml"'
mkdir -p "$t/data"; : > "$t/data/x.yaml"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'data/x.yaml' < <(printf '%s' "$out"); then
    ok "被判词脚本按路径读的 yaml 没有身份时判红"
else
    bad "没有身份的 yaml 应红（rc=$rc）：$out"
fi

# 14b. json 与 yaml 同一条读法：交付配置与清单按相对路径认。被读、没身份 ⇒ 红；
#      读者认领「只验它」⇒ 绿。
t="$work/14b"; probe "$t" 'X="$ROOT/chart/files/app.json"'
mkdir -p "$t/chart/files"; printf '{}\n' > "$t/chart/files/app.json"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '没有身份：chart/files/app.json' < <(printf '%s' "$out"); then
    ok "被判词读的 json 没有身份时判红"
else
    bad "没有身份的 json 应红（rc=$rc）：$out"
fi
t="$work/14c"; probe "$t" '# reads: chart/files/app.json subject' 'X="$ROOT/chart/files/app.json"'
mkdir -p "$t/chart/files"; printf '{}\n' > "$t/chart/files/app.json"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'subject 1' < <(printf '%s' "$out"); then
    ok "读来验它的 json 认领 subject 后判绿"
else
    bad "认领 subject 的 json 应绿（rc=$rc）：$out"
fi

# 15. tsv 按文件名认：路径藏得更深也照样是同一张表
t="$work/15"; probe "$t" 'X="$ROOT/data/deep/x.tsv"'
mkdir -p "$t/data/deep"; : > "$t/data/deep/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'data/deep/x.tsv' < <(printf '%s' "$out"); then
    ok "tsv 按文件名认，深路径也判得出来"
else
    bad "按名字被读的 tsv 应红（rc=$rc）：$out"
fi

# 16. 手写的表没有身份：文件头写一行 `debt:` 也不构成身份 ⇒ 红，并点名那张表
t="$work/16"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"
printf '# debt: 迁移要有平台库; expires: 2099-01-01\n' > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '没有身份：data/x.tsv' < <(printf '%s' "$out"); then
    ok "欠账头不是身份：写着 debt 头的表照样判红"
else
    bad "写着欠账头的表应判红（rc=$rc）：$out"
fi

# 17. 载体头同样不是身份：点名了消费者的手写表也不该被判词读 ⇒ 红
t="$work/17"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"
printf '# carrier: thing; consumed-by: governance import-declarations --kind thing; dies: when thing.tsv lands\n' > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '没有身份：data/x.tsv' < <(printf '%s' "$out"); then
    ok "载体头不是身份：写着 carrier 头的表照样判红"
else
    bad "写着载体头的表应判红（rc=$rc）：$out"
fi

# 18. 文件头是普通注释：写者注解在时照样按 generated 记（头不制造第二个身份）
t="$work/18"; probe "$t" 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"
printf '# debt: 迁移要有平台库; expires: 2099-01-01\n' > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: data/x.tsv generated' 'out="$ROOT/data/x.tsv"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'generated 1' < <(printf '%s' "$out"); then
    ok "文件头不制造身份，写者注解照旧把它记成 generated"
else
    bad "有写者的表应判绿（rc=$rc）：$out"
fi

# 19. 没人读的表不判；带着欠账头的没人读表也不再印出来（它只是一份文档）
t="$work/19"; probe "$t" '# reads: data/y.tsv subject' 'X="$ROOT/data/y.tsv"'
mkdir -p "$t/data"; : > "$t/data/y.tsv"
printf '# debt: 迁移要有平台库; expires: 2099-01-01\n' > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && ! grep -q 'debt' < <(printf '%s' "$out"); then
    ok "没人读的表不判，欠账头也不再被印出来"
else
    bad "没人读的表应只数不判（rc=$rc）：$out"
fi

# 20. 注解匹配不到任何被读的表 ⇒ 印 stale，不红
t="$work/20"; probe "$t" '# reads: data/x.tsv subject' 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: data/gone/*.tsv generated' 'out="$ROOT/data/gone/"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'stale writes data/gone/*.tsv' < <(printf '%s' "$out"); then
    ok "匹配不到被读表的注解印 stale 而不红"
else
    bad "stale 注解应只印不红（rc=$rc）：$out"
fi

# 21. 写者优先：同一份表既有写者又有读者 ⇒ 仍是 generated，不是两个身份
t="$work/21"; probe "$t" '# reads: data/x.tsv subject' 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
writer "$t/gen/make.sh" '# writes: data/x.tsv generated' 'out="$ROOT/data/x.tsv"'
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'generated 1' < <(printf '%s' "$out"); then
    ok "机器写的表被门读来验时仍记成 generated"
else
    bad "写者应优先于读者（rc=$rc）：$out"
fi

# 22. 没有数据文件 ⇒ 3（量不出来不是绿）
t="$work/22"; mkdir -p "$t/scripts"; : > "$t/scripts/probe.test.sh"
out="$(run "$t")"; rc=$?
if [ "$rc" = 3 ] && grep -qF '一张数据文件都没扫到' < <(printf '%s' "$out"); then
    ok "没有数据文件时退出码 3"
else
    bad "没有数据文件应退 3（rc=$rc）：$out"
fi

# 23. 有数据文件、没有判词脚本 ⇒ 3
t="$work/23"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 3 ] && grep -qF '一份判词脚本都没找到' < <(printf '%s' "$out"); then
    ok "没有判词脚本时退出码 3"
else
    bad "没有判词脚本应退 3（rc=$rc）：$out"
fi

# 24. 根不在 ⇒ 3
out="$(python3 -I "$GATE" --root "$work/nope" 2>&1)"; rc=$?
if [ "$rc" = 3 ] && grep -qF '根不在' < <(printf '%s' "$out"); then
    ok "根不在时退出码 3"
else
    bad "根不在应退 3（rc=$rc）：$out"
fi

# 25. 提交车道给的是索引：判索引里的字节，不是工作树上此刻的字节
t="$work/25idx"; probe "$t" '# reads: data/x.tsv subject' 'X="$ROOT/data/x.tsv"'
mkdir -p "$t/data"; : > "$t/data/x.tsv"
if git -C "$t" init -q 2>/dev/null && git -C "$t" add -A 2>/dev/null && [ -f "$t/.git/index" ]; then
    # 工作树上把注解删掉、再加一张没暂存的表：索引里没有这两件事。
    probe "$t" 'X="$ROOT/data/x.tsv"'
    : > "$t/data/unstaged.tsv"
    out="$(GIT_INDEX_FILE="$t/.git/index" python3 -I "$GATE" --root "$t" 2>&1)"; rc=$?
    if [ "$rc" = 0 ] && ! grep -qF 'unstaged.tsv' < <(printf '%s' "$out"); then
        ok "给了索引时判索引里的字节，不判工作树上未暂存的编辑"
    else
        bad "索引模式应只判这一笔（rc=$rc）：$out"
    fi
else
    printf 'check 夹具建仓失败——索引模式这一臂没判成，不是绿\n'
fi

# 26. 真树（本仓）必须是绿的：手写登记表已经删掉，判词搬到了 `values.yaml` 每个数值
#     叶子头上，没有第二份要人同步的名单。这一格钉的是「没有红」——真树里再长出一张
#     没有身份的手写表，它立刻变红（判据：同一命令 rc 0）。
if [ -n "$IDX_IN" ]; then
    out="$(GIT_INDEX_FILE="$IDX_IN" python3 -I "$GATE" --root "$ROOT" 2>&1)"; rc=$?
else
    out="$(run "$ROOT")"; rc=$?
fi
# 真树的判词：**绿**。
#
# 这一格原先钉的是「真树只红在 `tunables.yaml` 这一张手写表上」——那是当时那笔债的
# 快照。债已经还了：那本登记簿只被 `tunable_registry_contract_test` 读来验，所以它在
# 自己的文件头声明了 `reads: … subject`，身份是「读者」。判据在这一格上从此钉的不再是
# 「红在哪」，而是「没有红」：真树里再长出来一张没有身份的手写表，这一格会立刻变红。
if [ "$rc" = 0 ]; then
    ok "真树上每张被读的表都有身份（手写的登记簿按读者登记）"
else
    bad "真树应该是绿的（rc=$rc）：$out"
fi
printf 'check-judgment-inputs.test: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
