#!/usr/bin/env bash
# principle: feedback_a_projection_needs_a_named_projector_or_it_is_just_a_copy
#
# 判的是 `deploy/scripts/check-judgment-inputs.sh` 本尊：一张表被判词脚本读、却没有生产
# 者，就是「改一格改判词」的路。夹具造小树，真门跑真树（本仓）。
#
# 每一格对着「门会怎么错」，不是「表长什么样」：
#   - 被读、没登记 ⇒ 红；登记成 debt（带理由与到期）⇒ 绿，且把这一行印出来
#   - 欠账没有到期日 / 到期过了 ⇒ 红（豁免与欠账的区别就在这一列）
#   - 登记成 generated，生产者不在 / 正文没提到这张表 ⇒ 红（链断在哪儿就红在哪儿）
#   - 登记成闭集外的类 ⇒ 红
#   - 表放在散文目录里被读 ⇒ 照样红（不按目录豁免；记录只在没人读它的时候才是记录）
#   - yaml 按路径认：只被提到文件名、路径没被读 ⇒ 不判；路径被读 ⇒ 红／登记
#   - 通配行只在一段里匹配：`data/*.yaml` 不把 `data/sub/x.yaml` 一起放行
#   - subject 点名的判词脚本必须在判词面里（不是随便一份提到它的文件）
#   - 一份表被两行匹配 ⇒ 红（一个身份，不是一个先到先得）
#   - 登记表自己也在数据面里：判词面的脚本按名字读到它时，它也得给自己登记——没登记 ⇒ 红，
#     记成欠账 ⇒ 绿并每次印出来（门不给自己的账开后门）
#   - 欠账不许通配 ⇒ 红（理由与到期要对得上那一份文件，不是一个目录）
#   - 终态的落点是图投影（`judgment-input.tsv`，头两行生成戳）：读它、判绿、摘要说是投影；
#     投影没有生成戳 ⇒ 红；投影与手写表并排 ⇒ 红（一份分类只能有一个答案），不是「量不出来」
#   - 没人读的表不判；没有数据文件 / 没有判词脚本 / 根不在 ⇒ 退出码 3，不是绿
#   - 真树上这门是绿的，而那些欠账每次都印出来——绿不是把欠账藏起来

set -uo pipefail
# 索引模式那一格会在夹具里建一个小仓并 `git add`：外层若导出了索引指针，那几笔写进的是
# 别人正在提交的那一份。其余格不建仓，但同一个变量也会让门改走索引模式，所以一并摘掉。
# 摘之前留一份：提交车道跑这一族时给的正是这一笔的索引，真树那一格要用它——不用它，
# 判的是共享工作树，别处还没落的表会替这一笔判红或判绿。
IDX_IN="${GIT_INDEX_FILE:-}"
unset GIT_INDEX_FILE COGNEVA_COMMIT_INDEX GIT_DIR GIT_WORK_TREE
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
GATE="$ROOT/deploy/scripts/check-judgment-inputs.sh"

pass=0
fail=0
ok()  { printf 'ok    %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL  %s\n' "$1" >&2; fail=$((fail + 1)); }

[ -f "$GATE" ] || { bad "门不在：$GATE"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# 一棵最小树：一个判词脚本按名字找表，一份（空的）登记表。表放哪儿由各格自己决定；
# 登记表先放空的——「登记表是空的」判红，「登记表根本没有」判 3，两件事不能同形。
tree() { # $1=目录；表路径相对根由第二个参数给
    mkdir -p "$1/scripts/gates" "$1/scripts/data"
    printf '#!/usr/bin/env bash\nX="$ROOT/%s"\n' "${2:-data/x.tsv}" > "$1/scripts/probe.test.sh"
    : > "$1/scripts/data/judgment-inputs.pin"
}

run() { # $1=根 → stdout+stderr 合流，rc 由调用方从 $? 取
    bash "$GATE" --root "$1" 2>&1
}

pin_row() { # $1=根 $2=行（含 TAB）→ 追加进默认登记表
    printf '%s\n' "$2" >> "$1/scripts/data/judgment-inputs.pin"
}

# 1. 被读、没登记 ⇒ 红，并点名
t="$work/1"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'data/x.tsv' < <(printf '%s' "$out"); then
    ok "被判词脚本读、没有登记的表判红"
else
    bad "未登记的表应红并点名（rc=$rc）：$out"
fi

# 2. debt：带理由与到期 ⇒ 绿，且每次运行都印出来
t="$work/2"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\tdebt\t迁移要有平台库\t2099-01-01')"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'debt  data/x.tsv' < <(printf '%s' "$out"); then
    ok "欠账行带理由与到期时判绿，并把欠账印出来"
else
    bad "未过期的欠账应绿且被印出（rc=$rc）：$out"
fi

# 3. debt 到期 ⇒ 红（到期那一列就是豁免与欠账的区别）
t="$work/3"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\tdebt\t早该修了\t2020-01-01')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "欠账到期后判红" || bad "过期的欠账应红（rc=$rc）：$out"

# 4. debt 没写到期日 ⇒ 红（没有到期日就不是欠账，是豁免）
t="$work/4"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\tdebt\t没有到期日')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "欠账没写到期日判红" || bad "缺到期日的欠账应红（rc=$rc）：$out"

# 5. generated：生产者提到这张表 ⇒ 绿
t="$work/5"; tree "$t"; mkdir -p "$t/data"
printf 'f = open("x.tsv", "w")\n' > "$t/scripts/produce.py"
: > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\tgenerated\tscripts/produce.py')"
out="$(run "$t")"; rc=$?
[ "$rc" = 0 ] && ok "生产者存在且正文提到表名时判绿" || bad "生产者链完整应绿（rc=$rc）：$out"

# 6. generated：生产者不提这张表 ⇒ 红（文件还在、名字走了，表读起来仍然像有生产者）
t="$work/6"; tree "$t"; mkdir -p "$t/data"
printf 'f = open("something-else.tsv", "w")\n' > "$t/scripts/produce.py"
: > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\tgenerated\tscripts/produce.py')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "生产者正文没提到这张表时判红" || bad "生产者不提到表名应红（rc=$rc）：$out"

# 7. generated：生产者不在 ⇒ 红
t="$work/7"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\tgenerated\tscripts/gone.py')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "登记的生产者不在时判红" || bad "生产者缺席应红（rc=$rc）：$out"

# 8. 闭集外的类 ⇒ 红
t="$work/8"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/x.tsv\thandwritten\t随便写的')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "闭集外的类判红" || bad "未知的类应红（rc=$rc）：$out"

# 9. 散文目录里被读的表 ⇒ 照样红（记录只在没人读它时才是记录）
t="$work/9"; mkdir -p "$t/scripts/gates" "$t/scripts/data" "$t/internal-docs"; : > "$t/scripts/data/judgment-inputs.pin"
printf '#!/usr/bin/env bash\nX="$ROOT/internal-docs/z.tsv"\n' > "$t/scripts/probe.test.sh"
: > "$t/internal-docs/z.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'internal-docs/z.tsv' < <(printf '%s' "$out"); then
    ok "散文目录里被读的表不因目录获得豁免"
else
    bad "散文目录里的判据表应红（rc=$rc）：$out"
fi

# 10. 没人读的表不判（它是记录，不是判据）
t="$work/10"; tree "$t" data/used.tsv; mkdir -p "$t/data" "$t/internal-docs"
: > "$t/data/used.tsv"; : > "$t/internal-docs/frozen.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && ! grep -qF 'internal-docs/frozen.tsv' < <(printf '%s' "$out"); then
    ok "没人读的表不进判词（红只来自被读的那张）"
else
    bad "没人读的表不应被判（rc=$rc）：$out"
fi

# 11. 根不在 ⇒ 3，不是 0 也不是 1
out="$(run "$work/does-not-exist")"; rc=$?
[ "$rc" = 3 ] && ok "根不在时退出码 3" || bad "根不在应退 3（rc=$rc）：$out"

# 12. 有判词脚本、没有数据文件 ⇒ 3（量法失效不是绿）。登记表自己也是数据文件，所以
#     拿它放在根外、用 --pin 指进来——根底下真的一张都没有，才是这一臂要问的事。
t="$work/12"; mkdir -p "$t/scripts/gates"
printf '#!/usr/bin/env bash\n' > "$t/scripts/probe.test.sh"
: > "$work/12.pin"
out="$(bash "$GATE" --root "$t" --pin "$work/12.pin" 2>&1)"; rc=$?
[ "$rc" = 3 ] && ok "没有数据文件时退出码 3" || bad "没有数据文件应退 3（rc=$rc）：$out"

# 13. 有数据文件、没有判词脚本 ⇒ 3
t="$work/13"; mkdir -p "$t/data" "$t/scripts/data"; : > "$t/data/x.tsv"; : > "$t/scripts/data/judgment-inputs.pin"
out="$(run "$t")"; rc=$?
[ "$rc" = 3 ] && ok "没有判词脚本时退出码 3" || bad "没有判词脚本应退 3（rc=$rc）：$out"

# 14. 真树是绿的，且欠账每次都印出来（绿不是把欠账藏起来）。提交车道给了索引就判
#     索引里那一笔，不给才扫盘。登记表不给路径，让门自己按布局找——CI 就是这么跑的。
if [ -n "$IDX_IN" ]; then
    out="$(GIT_INDEX_FILE="$IDX_IN" bash "$GATE" --root "$ROOT" 2>&1)"; rc=$?
else
    out="$(run "$ROOT")"; rc=$?
fi
if [ "$rc" = 0 ] && grep -qF 'debt  deploy/helm/cogneva/tunables.yaml' < <(printf '%s' "$out") \
   && grep -qF 'debt  deploy/scripts/judgment-inputs.pin' < <(printf '%s' "$out"); then
    ok "本仓的登记表让这门判绿，欠账行（含登记表自己那行）被印出"
else
    bad "本仓应为绿并印出欠账（rc=$rc）：$out"
fi

# 15. 登记表按布局发现：只有 `deploy/scripts/judgment-inputs.pin` 的树也判得出来
#     （写死一种布局，另一种布局的树上就只剩「没登记」与「没判成」两种同行的红）。
t="$work/15"; mkdir -p "$t/scripts/gates" "$t/data" "$t/deploy/scripts"
printf '#!/usr/bin/env bash\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe.test.sh"
: > "$t/data/x.tsv"
printf 'data/x.tsv\tdebt\t迁移动了平台\t2099-01-01\n' > "$t/deploy/scripts/judgment-inputs.pin"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'debt  data/x.tsv' < <(printf '%s' "$out"); then
    ok "只有 deploy/scripts/ 那份登记表的树也判成，欠账被印出"
else
    bad "第二种布局的登记表应被发现（rc=$rc）：$out"
fi

# 16. 提交车道给的是索引：判这一笔的字节，不判工作树上隔壁还没落的表
t="$work/16idx"; mkdir -p "$t/scripts/gates" "$t/data" "$t/scripts/data"
printf '#!/usr/bin/env bash\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe.test.sh"
: > "$t/data/x.tsv"; : > "$t/scripts/data/judgment-inputs.pin"
if git -C "$t" init -q 2>/dev/null && git -C "$t" add -A 2>/dev/null && [ -f "$t/.git/index" ]; then
    : > "$t/data/unstaged.tsv"   # 盘上有、索引里没有
    out="$(GIT_INDEX_FILE="$t/.git/index" bash "$GATE" --root "$t" 2>&1)"; rc=$?
    if [ "$rc" = 1 ] && grep -qF 'data/x.tsv' < <(printf '%s' "$out") \
       && ! grep -qF 'unstaged.tsv' < <(printf '%s' "$out"); then
        ok "给了索引时只判索引里的表，不判工作树上未暂存的表"
    else
        bad "索引模式应只判这一笔（rc=$rc）：$out"
    fi
else
    printf 'check 夹具建仓失败——索引模式这一臂没判成，不是绿\n'
fi

# 17. yaml 被按路径读、没登记 ⇒ 红（后缀不是豁免）
t="$work/17"; tree "$t" data/x.yaml; mkdir -p "$t/data"; : > "$t/data/x.yaml"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'data/x.yaml' < <(printf '%s' "$out"); then
    ok "被判词脚本按路径读的 yaml 没登记时判红"
else
    bad "未登记的 yaml 应红（rc=$rc）：$out"
fi

# 18. yaml 只被提到文件名、路径没被读 ⇒ 不判（同名 yaml 太多，按名字放行会连带别人）
#     判词脚本提到的是另一个目录下的同名文件，而那份不在树里——读的不是这张。
t="$work/18"; mkdir -p "$t/scripts/gates" "$t/data" "$t/scripts/data"
printf '#!/usr/bin/env bash\nX="$ROOT/other/x.yaml"\n' > "$t/scripts/probe.test.sh"
: > "$t/data/x.yaml"; : > "$t/scripts/data/judgment-inputs.pin"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && ! grep -qF 'data/x.yaml' < <(printf '%s' "$out"); then
    ok "yaml 按路径认，文件名相同不构成被读"
else
    bad "只被提到名字的 yaml 不应被判（rc=$rc）：$out"
fi

# 19. 通配行：一份生产者写的目录里，每张表都归它 ⇒ 绿
t="$work/19"; tree "$t" data/x.yaml; mkdir -p "$t/data" "$t/gen"
printf '#!/usr/bin/env bash\nX="$ROOT/data/x.yaml"\n' > "$t/scripts/probe.test.sh"
printf 'writes data/x.yaml\n' > "$t/gen/make.sh"
: > "$t/data/x.yaml"
pin_row "$t" "$(printf 'data/*.yaml\tgenerated\tgen/make.sh')"
out="$(run "$t")"; rc=$?
[ "$rc" = 0 ] && ok "通配行匹配时判绿" || bad "通配行应绿（rc=$rc）：$out"

# 20. 通配的 `*` 不跨目录：`data/*.yaml` 不把 `data/sub/x.yaml` 一起放行
t="$work/20"; tree "$t" data/sub/x.yaml; mkdir -p "$t/data/sub" "$t/gen"
printf 'writes data/ and sub/\n' > "$t/gen/make.sh"
: > "$t/data/sub/x.yaml"
pin_row "$t" "$(printf 'data/*.yaml\tgenerated\tgen/make.sh')"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF 'data/sub/x.yaml' < <(printf '%s' "$out"); then
    ok "通配的 * 不跨目录，子目录里的表仍要求登记"
else
    bad "通配不应跨目录匹配（rc=$rc）：$out"
fi

# 21. subject 点名的必须是一份能变红的判词脚本，不是随便一份提到它的文件
t="$work/21"; tree "$t" data/x.yaml; mkdir -p "$t/data" "$t/scripts"
printf 'mentions data/x.yaml\n' > "$t/scripts/notes.md"
: > "$t/data/x.yaml"
pin_row "$t" "$(printf 'data/x.yaml\tsubject\tscripts/notes.md')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "subject 的判词脚本不在判词面里时判红" || bad "非判词面的 subject 应红（rc=$rc）：$out"

# 22. 一份表被两行匹配 ⇒ 红（一个身份，不是先到先得）
t="$work/22"; tree "$t" data/x.yaml; mkdir -p "$t/data" "$t/gen"
printf 'writes data/x.yaml\n' > "$t/gen/make.sh"
: > "$t/data/x.yaml"
pin_row "$t" "$(printf 'data/x.yaml\tgenerated\tgen/make.sh')"
pin_row "$t" "$(printf 'data/*.yaml\tgenerated\tgen/make.sh')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "两行登记匹配同一份表时判红" || bad "重复登记应红（rc=$rc）：$out"

# 23. `**` 不支持 ⇒ 红（写出来像是跨目录、实际不跨，宁可拒掉）
t="$work/23"; tree "$t" data/x.yaml; mkdir -p "$t/data"
: > "$t/data/x.yaml"
pin_row "$t" "$(printf 'data/**\tgenerated\tgen/make.sh')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "通配写成 ** 时判红" || bad "** 应被拒（rc=$rc）：$out"

# 24. 通配行的生产者正文没提到那个目录 ⇒ 红（链断在目录这一级也算断）
t="$work/24"; tree "$t" data/x.yaml; mkdir -p "$t/data" "$t/gen"
printf 'writes somewhere else\n' > "$t/gen/make.sh"
: > "$t/data/x.yaml"
pin_row "$t" "$(printf 'data/*.yaml\tgenerated\tgen/make.sh')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "通配行的生产者不提那个目录时判红" || bad "目录链断应红（rc=$rc）：$out"

# 25. 通配没有静态前缀（`*.yaml`）⇒ 红（否则整棵树是一个身份）
t="$work/25"; tree "$t" data/x.yaml; mkdir -p "$t/data"
: > "$t/data/x.yaml"
pin_row "$t" "$(printf '*.yaml\tgenerated\tgen/make.sh')"
out="$(run "$t")"; rc=$?
[ "$rc" = 1 ] && ok "通配没有静态前缀时判红" || bad "无静态前缀的通配应红（rc=$rc）：$out"

# 26. 登记表根本没有 ⇒ 3（「登记表是空的」判红，「没有登记表」是量不出来）。两处布局
#     都找过才这么说：少一处候选，换布局的树就会被判成「没登记」而不是「没判成」。
t="$work/26"; tree "$t"
rm -f "$t/scripts/data/judgment-inputs.pin"
out="$(run "$t")"; rc=$?
if [ "$rc" = 3 ] && grep -qF '找不到登记表' < <(printf '%s' "$out"); then
    ok "两处布局都没有登记表时退出码 3"
else
    bad "没有登记表应退 3（rc=$rc）：$out"
fi

# 27. 登记表被判词面的脚本按名字读到、却没给自己登记 ⇒ 红。它是一张手写的、决定别的
#     文件判词的表，与这门要治的病同形；豁免它，门就是给自己留了后门。
t="$work/27"; tree "$t"; mkdir -p "$t/data"; : > "$t/data/x.tsv"
printf '#!/usr/bin/env bash\nPIN="judgment-inputs.pin"\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe2.test.sh"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '没有登记：scripts/data/judgment-inputs.pin' < <(printf '%s' "$out"); then
    ok "登记表自己被读却没登记时判红"
else
    bad "登记表自己漏登记应红（rc=$rc）：$out"
fi

# 28. 同上，但登记表给自己写了一行欠账 ⇒ 绿，并把那行印出来（终态是图里的声明＋投影，
#     导入之前它是一笔看得出、到得了期的欠账，不是豁免）
t="$work/28"; tree "$t" data/x.tsv; mkdir -p "$t/data"; : > "$t/data/x.tsv"
printf '#!/usr/bin/env bash\nPIN="judgment-inputs.pin"\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe2.test.sh"
pin_row "$t" "$(printf 'data/x.tsv\tdebt\t迁移要有平台库\t2099-01-01')"
pin_row "$t" "$(printf 'scripts/data/judgment-inputs.pin\tdebt\t分类是手写的决定\t2099-01-01')"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'debt  scripts/data/judgment-inputs.pin' < <(printf '%s' "$out"); then
    ok "登记表给自己记欠账时判绿，且那行被印出"
else
    bad "登记表自记欠账应绿并印出（rc=$rc）：$out"
fi

# 29. debt 行用通配 ⇒ 红（一行欠账盖住一个目录，理由和到期就对不上任何一份文件了）
t="$work/29"; tree "$t" data/x.tsv; mkdir -p "$t/data"; : > "$t/data/x.tsv"
pin_row "$t" "$(printf 'data/*.tsv\tdebt\t整目录一起欠\t2099-01-01')"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '欠账行不能通配' < <(printf '%s' "$out"); then
    ok "欠账行带通配时判红"
else
    bad "欠账通配应红（rc=$rc）：$out"
fi

# 30. 终态：登记表是图写出的投影（`<layout>/judgment-input.tsv`，头两行生成戳）。行
#     的规则不变，摘要要说明它读的是投影而不是手写表。
t="$work/30"; mkdir -p "$t/scripts/gates" "$t/data" "$t/deploy/scripts"
printf '#!/usr/bin/env bash\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe.test.sh"
: > "$t/data/x.tsv"
printf '# generated_at: 2026-11-15T00:00:00+00:00\n# source_revision: 7\ndata/x.tsv\tdebt\t迁移要有平台库\t2099-01-01\n' > "$t/deploy/scripts/judgment-input.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 0 ] && grep -qF 'debt  data/x.tsv' < <(printf '%s' "$out") \
   && grep -qF '登记表：图投影 deploy/scripts/judgment-input.tsv' < <(printf '%s' "$out"); then
    ok "图投影当登记表时判绿，摘要说明来源是投影"
else
    bad "投影布局应绿并说明来源（rc=$rc）：$out"
fi

# 31. 投影没有生成戳 ⇒ 红（分不出它是哪一版图写的，就分不出它记得的是不是昨天的分类）
t="$work/31"; mkdir -p "$t/scripts/gates" "$t/data" "$t/deploy/scripts"
printf '#!/usr/bin/env bash\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe.test.sh"
: > "$t/data/x.tsv"
printf 'data/x.tsv\tdebt\t迁移要有平台库\t2099-01-01\n' > "$t/deploy/scripts/judgment-input.tsv"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '投影没有生成戳' < <(printf '%s' "$out"); then
    ok "没有生成戳的投影判红"
else
    bad "无戳投影应红（rc=$rc）：$out"
fi

# 32. 投影与手写表并排 ⇒ 红（一份分类两个答案，读哪份都是猜），且不是「量不出来」的 3
t="$work/32"; mkdir -p "$t/scripts/gates" "$t/data" "$t/deploy/scripts" "$t/scripts/data"
printf '#!/usr/bin/env bash\nX="$ROOT/data/x.tsv"\n' > "$t/scripts/probe.test.sh"
: > "$t/data/x.tsv"
printf '# generated_at: 2026-11-15T00:00:00+00:00\n# source_revision: 7\ndata/x.tsv\tdebt\t迁移要有平台库\t2099-01-01\n' > "$t/deploy/scripts/judgment-input.tsv"
: > "$t/scripts/data/judgment-inputs.pin"
out="$(run "$t")"; rc=$?
if [ "$rc" = 1 ] && grep -qF '登记表有两份' < <(printf '%s' "$out"); then
    ok "投影与手写表并排时判红"
else
    bad "两份登记表应红（rc=$rc）：$out"
fi

echo "check-judgment-inputs.test: $pass passed, $fail failed"
[ "$fail" = 0 ]
