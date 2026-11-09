#!/usr/bin/env bash
# `--status` 判的是「这一笔提交验过没有」，不是「现在替我验一遍」，两者必须分得开。
#
# 为什么要有这一层：钩子最后一次问这条门的时候，问的是「验过没有」（`--status`）。这一问
# 要是落空，它就会把整条判定（整棵 `cargo test` 加全部脚本测试，分钟级到小时级）在**每一次
# 推送**里跑一遍，而且是在推的人干等的这段时间里跑；远端在这段时间里往前走，一被拒这一段
# 全白等。所以这里把三种结局钉住：
#
#   1. 没验过的提交 ⇒ 退出 1，并且话里点明要怎么验；
#   2. 验过的那一笔 ⇒ 退出 0；
#   3. 又提交了一笔 ⇒ 回到退出 1（旧账不能替新提交说话）。
#
# 还有一条反向的，钉的是共享工作树上的那个形状：别人的在飞改动不随这一推走，**不该**让
# 一笔已经验过的提交失效——否则三四个会话共写一棵树时，戳永远命不中自己，等于没记。
#
# 夹具在一个临时树里跑这份门的副本（它按自己的位置算仓库根），所以不用动真仓库��也不跑
# 那整条分钟级的判定：记账的键由 `--key` 印出来，这里自己落一笔，判的是读账那一侧。
set -euo pipefail

# 这份夹具在临时树里跑 `git init`。被钩子拉起来时，git 把 GIT_DIR / GIT_WORK_TREE /
# GIT_INDEX_FILE 指向**调用方那棵树**，夹具里的 init 就会落到别人的仓库上。文件级清掉；
# 第 10 例要的是「带着这些变量被拉起来」那个形状，它在命令行上自己按需再给一次。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-before-push.sh"
# shellcheck source=tests/fixtures/stage-gate.sh
. "${here}/fixtures/stage-gate.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

tree="${work}/tree"
mkdir -p "${tree}/deploy/scripts/tests" "${tree}/.github/workflows"
stage_gate "${repo}" "${tree}"
cd "${tree}"

git init -q -b main .
git config user.email gate@example.invalid
git config user.name gate
printf 'one\n' > tracked.txt
git add tracked.txt
git commit -q -m "one"

run() { bash deploy/scripts/check-before-push.sh "$@" >"${work}/out" 2>&1; }

# 1) 没验过：退出 1，且话里给出验证命令（只印「没验过」而不说怎么办，等于把人挡在门外）。
if run --status; then
    fail "没验过的提交报了绿；实得：$(cat "${work}/out")"
fi
grep -q 'check-before-push.sh`' "${work}/out" ||
    fail "退出 1 的话里没有给出验证命令；实得：$(cat "${work}/out")"

# 2) 记账那一笔：绿的一跑按提交落账。这里用 --key 拿同一个键自己落，判的是读账。
key="$(bash deploy/scripts/check-before-push.sh --key)"
[ -n "${key}" ] || fail "--key 什么都没印"
mkdir -p .git/check-before-push
printf 'fixture\n' > ".git/check-before-push/${key}"
run --status || fail "记过账的提交仍报没验过；实得：$(cat "${work}/out")"

# 3) 别人在这棵树上留下的未提交改动（改了跟踪文件、多了未跟踪文件）：不随这一推走，
#    已经验过的这一笔仍然是验过的。
printf 'two\n' >> tracked.txt
printf 'new\n' > untracked.txt
run --status || fail "别人的未提交改动让已验过的提交失效了；实得：$(cat "${work}/out")"

# 4) 又提交了一笔：键跟着 HEAD 走，旧账不再覆盖这一份字节。
git add -A
git commit -q -m "two"
if run --status; then
    fail "新提交仍报验过（旧账替新提交说了话）"
fi

# 5) 提交之前那一问（`--status-content`）：键是**工作树的字节**，脏树上也记账——判的就是
#    这份脏，提交之后字节不变，所以两头的指纹一样。上面第 2 格的推送键在这里命不中。
printf 'three\n' > third.txt
git add third.txt
ckey="$(bash deploy/scripts/check-before-push.sh --key-content)" || fail "--key-content 退非零"
[ -n "${ckey}" ] || fail "--key-content 什么都没印"
printf 'fixture\n' > ".git/check-before-push/content.${ckey}"
run --status-content || fail "判过的这份字节在提交前仍报没验过；实得：$(cat "${work}/out")"

# 6) 提交不改变这棵树的内容：提交之后的同一份字节仍然命中（这是提交前那一问存在的理由）。
git commit -q -m "three"
run --status-content || fail "提交之后同一份字节的账失效了；实得：$(cat "${work}/out")"

# 7) 再改一个字节：内容键跟着变，旧账不再覆盖这份新字节。
printf 'four\n' >> tracked.txt
if run --status-content; then
    fail "改过之后仍报验过（旧账替新字节说了话）"
fi
printf 'one\ntwo\n' > tracked.txt

# 8) 判据本身变了（门自己的字节进指纹）：上一份结论不再作数。
printf '\n# judged differently\n' >> deploy/scripts/check-before-push.sh
if run --status-content; then
    fail "门自己的字节改了，旧账仍报验过"
fi

# 9) 还没有 HEAD 的树：读不出指纹 ⇒ 退 2。问不了不是红，退 1 会把它说成没验过。
bare="${work}/bare"
mkdir -p "${bare}/deploy/scripts"
stage_gate "${repo}" "${bare}"
( cd "${bare}" && git init -q -b main . )
rc=0
( cd "${bare}" && bash deploy/scripts/check-before-push.sh --status-content ) >"${work}/out" 2>&1 || rc=$?
[ "${rc}" -eq 2 ] || fail "还没有 HEAD 的树应退 2（问不了），实得 ${rc}：$(cat "${work}/out")"

# 10) 从钩子里被拉起来时，git 会把 `GIT_DIR`/`GIT_WORK_TREE` 指向**调用方那棵树**：
#     指纹必须仍描述这道门所在的这棵树。`GIT_WORK_TREE` 在手上时 `add -A` 收的是另一棵树
#     的文件——指纹会指着一棵与这次提交无关的树，而两边的判词看上去完全一样。
caller="${work}/caller"
git init -q -b main "${caller}"
git -C "${caller}" config user.email gate@example.invalid
git -C "${caller}" config user.name gate
printf 'caller\n' >"${caller}/only-in-caller.txt"
git -C "${caller}" add only-in-caller.txt
git -C "${caller}" commit -q -m caller
here_key="$(bash deploy/scripts/check-before-push.sh --key-content)"
there_key="$(GIT_DIR="${caller}/.git" GIT_WORK_TREE="${caller}" \
    bash deploy/scripts/check-before-push.sh --key-content)"
[ "${here_key}" = "${there_key}" ] ||
    fail "继承来的 git 定位变量换了指纹（这棵树 ${here_key:0:12}，调用方那棵树 ${there_key:0:12}）"

# 11~14) 第三把键：这批**改动**本身，而不是它踩在哪棵树上。
#
# 为什么要有它：2026-11-08 实测，整门跑两小时，而远端每 66～123 分钟往前走一笔。要推就得
# 先变基，一变基这棵树里就多了别人的文件，按树记的两把键一起作废——于是每个会话都在重复
# 这两小时，不是红，是永远差一步。这几格钉住的是 patch-id 的两面：干净的变基之后同一批
# 改动仍是验过的，而解冲突改了解法之后它就不再是同一批。
ptree="${work}/ptree"
mkdir -p "${ptree}/deploy/scripts"
stage_gate "${repo}" "${ptree}"
(
    cd "${ptree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'a\nb\nc\n' >f.txt
    git add f.txt
    git commit -q -m base
) || fail "夹具仓建不起来"
base1="$(git -C "${ptree}" rev-parse HEAD)"
printf 'a\nB\nc\n' >"${ptree}/f.txt"
printf 'new\n' >"${ptree}/g.txt"

# 11) 判的那一头读工作树、推的那一头读 HEAD：同一批字节，两份必须是同一个键。这就是
#     「判完 → 提交 → 变基 → 推」整条链的接口，对不上则整条链断在这里。
wtkey="$( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --key-patch-tree "${base1}" )" ||
    fail "--key-patch-tree 退非零"
[ -n "${wtkey}" ] || fail "--key-patch-tree 什么都没印"
(
    cd "${ptree}" || exit 1
    git add -A
    git commit -q -m delta
) || fail "夹具的这批改动提交不掉"
pkey="$( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --key-patch "${base1}" )" ||
    fail "--key-patch 退非零"
[ "${wtkey}" = "${pkey}" ] ||
    fail "同一批字节，工作树那份与提交后那份算出了两个键（${wtkey:0:12} ≠ ${pkey:0:12}）"
pkey2="$( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --key-patch "${base1}" )"
[ "${pkey}" = "${pkey2}" ] || fail "同一个基底两次问出不同的改动键（${pkey:0:12} ≠ ${pkey2:0:12}）"
mkdir -p "${ptree}/.git/check-before-push"
printf 'fixture\n' >"${ptree}/.git/check-before-push/patch.${wtkey}"
( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --status-patch "${base1}" ) >"${work}/out" 2>&1 ||
    fail "判过、提交过的这批改动仍报没验过；实得：$(cat "${work}/out")"

# 12) 干净的变基（远端往前走了一笔与这批改动无关的提交）⇒ 还是同一批改动，账照样命中。
(
    cd "${ptree}" || exit 1
    git checkout -q -b upstream "${base1}"
    printf 'unrelated\n' >u.txt
    git add u.txt
    git commit -q -m upstream
) || fail "夹具的远端那一笔建不起来"
base2="$(git -C "${ptree}" rev-parse upstream)"
( cd "${ptree}" && git checkout -q main && git rebase -q upstream ) || fail "夹具的干净变基没走完"
( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --status-patch "${base2}" ) >"${work}/out" 2>&1 ||
    fail "干净的变基之后这批改动的账失效了；实得：$(cat "${work}/out")"
# 放行的那一句要自己说清验的是什么：改动本身，不是它与这次推的基底的合并结果。干净的
# 变基与「套得上但语义变了」的变基在 patch-id 上同形，本地分不出，只有 CI 判得出——一句
# 笼统的「已验证」会让读的人把这两件事当成一件。
grep -q '不是它与基底' "${work}/out" ||
    fail "放行的那一句没说清「验的是改动本身、不是合并结果」；实得：$(cat "${work}/out")"

# 13) 解冲突改了解法 ⇒ 这批改动不再是判过的那一批，旧账不许替它说话。
(
    cd "${ptree}" || exit 1
    git checkout -q -b conflict "${base1}"
    printf 'a\nX\nc\n' >f.txt
    git add f.txt
    git commit -q -m conflicting
) || fail "夹具的冲突那一笔建不起来"
base3="$(git -C "${ptree}" rev-parse conflict)"
( cd "${ptree}" && git checkout -q main && GIT_EDITOR=true git rebase conflict >/dev/null 2>&1 ) &&
    fail "夹具没造出冲突（变基自己走完了）"
printf 'a\nBX\nc\n' >"${ptree}/f.txt"
( cd "${ptree}" && git add f.txt && GIT_EDITOR=true git rebase --continue >/dev/null 2>&1 ) ||
    fail "夹具的冲突变基没走完"
if ( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --status-patch "${base3}" ) >"${work}/out" 2>&1; then
    fail "解冲突改了解法之后，旧账仍把这批改动说成验过"
fi

# 14) 没有改动可判（基底就是这棵树）：问不了 ⇒ 退 2。退 1 会把它说成「没验过」。
rc=0
( cd "${ptree}" && bash deploy/scripts/check-before-push.sh --status-patch "$(git -C "${ptree}" rev-parse HEAD)" ) \
    >"${work}/out" 2>&1 || rc=$?
[ "${rc}" -eq 2 ] || fail "没有改动时 --status-patch 应退 2（问不了），实得 ${rc}：$(cat "${work}/out")"

# 15) 工具来源只有一个：键不该取决于调用方的 PATH 里有没有 cargo。钩子里 PATH 里没有它，
#     跑绿的那一跑靠 `~/.cargo/bin` 兜底找到了它；工具解析若发生在子命令分派**之后**，
#     分派时问出的键里写的是 `cargo=absent`，跑绿时写的是版本号——同一份树的两个键，
#     刚验过的字节在提交前永远报没验过，整条记账的链断在接缝上。
#     这里用一份自己的 HOME 造出「只有兜底那条路找得到」的形状，两条发现路径必须同键。
fakehome="${work}/fakehome"
mkdir -p "${fakehome}/.cargo/bin"
cat >"${fakehome}/.cargo/bin/cargo" <<'EOF'
#!/usr/bin/env bash
echo 'cargo 9.9.9 (fixture)'
EOF
chmod +x "${fakehome}/.cargo/bin/cargo"
IFS=':' read -r -a path_dirs <<<"${PATH:-}"
stripped=()
for d in "${path_dirs[@]}"; do
    [ -n "$d" ] || continue
    # 只摘掉「只装着 cargo」的那些目录；系统目录不摘，门自己要的 git/sha256sum 在那儿。
    if [ -x "$d/cargo" ] && [ "$d" != /usr/bin ] && [ "$d" != /bin ]; then continue; fi
    stripped+=("$d")
done
stripped_path="$(IFS=:; printf '%s' "${stripped[*]}")"
bare_key="$(HOME="${fakehome}" PATH="${stripped_path}" \
    bash deploy/scripts/check-before-push.sh --key-content)" ||
    fail "--key-content 在 PATH 里没有 cargo 时退非零"
found_key="$(HOME="${fakehome}" PATH="${fakehome}/.cargo/bin:${stripped_path}" \
    bash deploy/scripts/check-before-push.sh --key-content)" ||
    fail "--key-content 在 PATH 里带着 cargo 时退非零"
[ -n "${bare_key}" ] || fail "PATH 里没有 cargo 时 --key-content 什么都没印"
[ "${bare_key}" = "${found_key}" ] ||
    fail "同一个工具、两条发现路径算出了两个键（兜底 ${bare_key:0:12}，PATH ${found_key:0:12}）：工具解析必须在子命令分派之前"

# 16) 跑的途中这棵树变过 ⇒ 这一跑的判词不属于现在这份字节，两把键都不记。
#
# 整门一两个小时，期间树上多出来的任何一格都没被判过。以前指纹只在收尾时取，于是跑完
# 之后才出现的字节拿到了「验过」——2026-11-09 实测就是这一格：17:24 起跑的那一跑，在
# 文件改完之后的 18:xx 收尾，内容键记的是新字节。这一格让一步在跑的过程中改一个跟踪
# 文件，然后问：这一跑的账记在谁身上。
mtree="${work}/mtree"
mkdir -p "${mtree}/deploy/scripts/tests" "${mtree}/.github/scripts/tests" \
    "${mtree}/.github/workflows" "${mtree}/bin"
stage_gate "${repo}" "${mtree}"
# 这一跑必须**绿**才走得到记账那一段，所以树上要有它认得的那几件东西：两个非空的脚本
# 测试目录（空目录会让 shellcheck 读 stdin 退 0，门把那一格判红）、一个说得出 `--version`
# 的 cargo（fmt / clippy 那一面故意说不认识，格式与 lint 的门就不进场）。变的那个文件由
# CI 里的一步来改：门只回放 `run: bash …` 这一种写法，所以改树的是一个真脚本，而不是
# 一段多行块——多行块会被跳过，那一跑就红了。
printf '#!/usr/bin/env bash\nexit 0\n' >"${mtree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${mtree}/.github/scripts/tests/ok.test.sh"
# `bootstrap.ps1` 也是「它认得的那几件东西」之一，而且是最阴的一件：门那一步只在
# 装了 pwsh 的机器上跑，没装就印 SKIP。夹具里缺这个文件时，本机（没有 pwsh）照样
# 绿，CI（runner 自带 pwsh）上这一步去 `Get-Content bootstrap.ps1` 拿不到文件、
# 退非零 ⇒ 整门红 ⇒ 这一格报「变树那一跑自己红了」。于是同一条测试的绿红取决于
# 跑它的机器装了哪些可选工具——2026-11-10 实测，Entry Scripts Lint 就红在这一行，
# 而本机四轮都是绿的。真仓库里这个文件永远在，夹具要连它一起造齐。
printf 'Write-Host "fixture"\n' >"${mtree}/bootstrap.ps1"
cat >"${mtree}/bin/cargo" <<'SH'
#!/usr/bin/env bash
case "$1" in
  --version|-V) echo "cargo 9.9.9 (fixture)"; exit 0 ;;
  fmt|clippy) exit 1 ;;
  *) echo "test result: ok. 1 passed; 0 failed; 0 ignored"; exit 0 ;;
esac
SH
chmod +x "${mtree}/bin/cargo"
printf '#!/usr/bin/env bash\necho mid-run >> tracked.txt\n' >"${mtree}/touch-tree.sh"
cat >"${mtree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: change the tree while the gate is judging it
        run: bash touch-tree.sh
YAML
(
    cd "${mtree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    git add tracked.txt touch-tree.sh .github/workflows/ci.yml bootstrap.ps1 \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh
    git commit -q -m base -- tracked.txt touch-tree.sh .github/workflows/ci.yml bootstrap.ps1 \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh
) || fail "跑的途中变树那一格的夹具建不起来"
# 槽目录指到夹具里：这一跑不是这台机器上的构建方，不能站在宿主的构建后面排队。
mt_env=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/mtree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/mtree.lock"
    PATH="${mtree}/bin:${PATH}")
mt_before="$( cd "${mtree}" && "${mt_env[@]}" bash deploy/scripts/check-before-push.sh --key-content )" ||
    fail "--key-content 在变树那一格的夹具里退非零"
( cd "${mtree}" && "${mt_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/out" 2>&1 ||
    fail "变树那一跑自己红了，这一格判不了记账：$(cat "${work}/out")"
mt_after="$( cd "${mtree}" && "${mt_env[@]}" bash deploy/scripts/check-before-push.sh --key-content )"
[ "${mt_before}" != "${mt_after}" ] ||
    fail "夹具没造出「跑的过程中树变了」：开跑与收尾是同一个指纹"
grep -q '不属于现在这份字节' "${work}/out" ||
    fail "跑的途中变过，记账时没说这一跑的判词不属于现在这份字节；实得：$(cat "${work}/out")"
# 17) 这一跑里有面没验（夹具没有 origin/main，推的范围判不出来），判词必须把它自己验了
#     多少说清。上一版只说「上列的判据全绿」：缺工具的格子印一行 SKIP，然后在判词上与
#     验过同形。同一份字节在一台装了 pwsh 的机器上多验一面，两边的判词一模一样——2026-11-10
#     的 Entry Scripts Lint 就是这么红了一整天而本地四轮全绿。
grep -q '本机没验' "${work}/out" ||
    fail "这一跑有面没验，判词没说；实得：$(grep '^PRE-PUSH' "${work}/out")"
grep -q '^PRE-PUSH: 上列的判据全绿。' "${work}/out" &&
    fail "这一跑有面没验，判词却说「上列的判据全绿」：没验与验过在判词上同形，读的人分不出来"
grep -q '^PRE-PUSH: 判过的 .* 步全绿；另有 .* 面本机没验，不算验过：' "${work}/out" ||
    fail "判词没有印出「判了几步、另有几面没验、是哪几面」；实得：$(grep '^PRE-PUSH' "${work}/out")"
# 没验的那几面要点名，光一个数说不出来是哪些——读的人还得回去翻日志。
grep -q 'crate 骨架' "${work}/out" ||
    fail "没验的面没有点名（夹具里 crate 骨架 没验）；实得：$(grep '^PRE-PUSH' "${work}/out")"
# 18) 同一份夹具换到「装了 pwsh 的机器」上再跑一次：那一面必须真的被判，而不是印 SKIP
#     了事。没有这一格，夹具里的 bootstrap.ps1 哪天写错了，本机照样绿，只有 CI 会红——
#     2026-11-10 的 Entry Scripts Lint 红了一整天，本机四轮全绿，就是缺这一格。
#     标本见 fixtures/pwsh：它只回答「那个文件在不在」，不解析 PowerShell。
# PATH 要在 `mt_env` 那一份**之上**加：`mt_env` 里那个 `PATH=${PATH}` 是赋值时就展开的，
# 再在后面另起一个 `PATH=` 会把它整个换掉，夹具里的假 cargo 随之消失——那一跑的
# fmt / 名字契约 / 容量登记表 会拿真的 cargo 去判，红成与这一格无关的样子。
( cd "${mtree}" && "${mt_env[@]}" PATH="${repo}/deploy/scripts/tests/fixtures:${mtree}/bin:${PATH}" \
    bash deploy/scripts/check-before-push.sh ) >"${work}/out2" 2>&1 ||
    fail "「装了 pwsh」的那一跑红了：$(cat "${work}/out2")"
grep -qE '^[[:space:]]*[0-9]+[[:space:]]+ok[[:space:]]+bootstrap\.ps1 语法$' "${work}/out2" ||
    fail "装了 pwsh 时 bootstrap.ps1 语法这一面没被判；实得：$(grep 'bootstrap' "${work}/out2")"
grep -q '^SKIP  bootstrap\.ps1' "${work}/out2" &&
    fail "装了 pwsh 却仍把这一面印成 SKIP：那一格一次都没被判过"
if ( cd "${mtree}" && "${mt_env[@]}" bash deploy/scripts/check-before-push.sh --status-content ) >"${work}/out" 2>&1; then
    fail "跑的途中变过的树仍被记成验过：那一笔一秒都没被判过；实得：$(cat "${work}/out")"
fi

# 19) 盘上有还没提交的 `crates/*.rs` ⇒ Test 这一格不许跳过。
#
# Test 与 Clippy 的题目原先只从 `origin/main...HEAD` 推：那是一段**已提交**的范围。可收尾
# 记的内容键是**盘上**这份字节——于是带着未提交 Rust 改动的树跑完整门，两格一句「没动
# crates/ 下的 .rs」就跳过了，而这份字节照样拿到「验过」。2026-11-09 实测：15 个未提交的
# `crates/*.rs`，0 个在任何一笔提交里。
ftree="${work}/ftree"
mkdir -p "${ftree}/deploy/scripts/tests" "${ftree}/.github/scripts/tests" \
    "${ftree}/.github/workflows" "${ftree}/bin" "${ftree}/crates/foo/src"
stage_gate "${repo}" "${ftree}"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ftree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ftree}/.github/scripts/tests/ok.test.sh"
cat >"${ftree}/bin/cargo" <<SH
#!/usr/bin/env bash
case "\$1" in
  --version|-V) echo "cargo 9.9.9 (fixture)"; exit 0 ;;
  fmt) exit 1 ;;
  clippy) exit 1 ;;
  *)
    printf '%s\n' "\$*" >>"${work}/ftree-cargo-calls"
    echo "test result: ok. 1 passed; 0 failed; 0 ignored"; exit 0 ;;
esac
SH
chmod +x "${ftree}/bin/cargo"
# pwsh 那一格只在装了 pwsh 的机器上跑（没装就 SKIP），所以夹具缺这个文件时本机照绿、
# CI 上一跑就红。真仓库里它永远在，夹具得连它一起造齐。
printf 'Write-Host "fixture"\n' >"${ftree}/bootstrap.ps1"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ftree}/nothing.sh"
printf '[workspace]\nmembers = ["crates/foo"]\n' >"${ftree}/Cargo.toml"
printf '[package]\nname = "foo"\nversion = "0.0.0"\nedition = "2021"\n' >"${ftree}/crates/foo/Cargo.toml"
printf 'pub fn one() -> u8 { 1 }\n' >"${ftree}/crates/foo/src/lib.rs"
cat >"${ftree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: nothing
        run: bash nothing.sh
YAML
(
    cd "${ftree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    git add tracked.txt bootstrap.ps1 nothing.sh Cargo.toml crates/foo/Cargo.toml crates/foo/src/lib.rs \
        .github/workflows/ci.yml deploy/scripts/tests/ok.test.sh \
        .github/scripts/tests/ok.test.sh deploy/scripts/check-before-push.sh \
        deploy/scripts/lib/build-slot.sh
    git commit -q -m base
    # 门只跟 `origin/main...HEAD` 比，所以要有一条 origin/main；没有它这一格走的是「推的
    # 范围判不出来」那条 SKIP，而不是这一例要判的那条。
    git update-ref refs/remotes/origin/main HEAD
) || fail "未提交改动那一格的夹具建不起来"
# 提交之后才改：这一改不进任何一笔提交，正是这一例要的形状。
printf 'pub fn two() -> u8 { 2 }\n' >"${ftree}/crates/foo/src/lib.rs"
ft_env=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/ftree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/ftree.lock"
    PATH="${ftree}/bin:${PATH}")
( cd "${ftree}" && "${ft_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/out" 2>&1 ||
    fail "未提交改动那一跑自己红了：$(cat "${work}/out")"
grep -q '没动 crates/ 下的 .rs' "${work}/out" &&
    fail "盘上有未提交的 crates/foo/src/lib.rs，Test 那一格却说「没动 crates/ 下的 .rs」跳过了：这份字节没有被它判过；实得：$(cat "${work}/out")"
grep -q -- '-p foo' "${work}/ftree-cargo-calls" 2>/dev/null ||
    fail "Test 那一格没有把 foo 交给 cargo（未提交的改动没进题目）；实得：$(cat "${work}/ftree-cargo-calls" 2>/dev/null)"

# 20) 树脏时 Clippy 就地判，不判 HEAD 展开出来的临时树。
#
# 临时树里放不下脏树的改动，而收尾盖的内容键是盘上这份字节。脏树上去判那份拷贝，绿的是
# 一份没人要推的字节；把它记成「这份字节验过」，等于用一个别的树的结论替这棵树背书。
ctree="${work}/ctree"
mkdir -p "${ctree}/deploy/scripts/tests" "${ctree}/.github/scripts/tests" \
    "${ctree}/.github/workflows" "${ctree}/bin" "${ctree}/crates/foo/src"
stage_gate "${repo}" "${ctree}"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ctree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ctree}/.github/scripts/tests/ok.test.sh"
cat >"${ctree}/bin/cargo" <<SH
#!/usr/bin/env bash
case "\$1" in
  --version|-V) echo "cargo 9.9.9 (fixture)"; exit 0 ;;
  fmt) exit 1 ;;
  clippy)
    case "\$2" in
      --version) echo "clippy 9.9.9 (fixture)"; exit 0 ;;
      *) printf '%s\n' "\$(pwd)" >>"${work}/ctree-clippy-cwd"; exit 0 ;;
    esac ;;
  *)
    printf '%s\n' "\$*" >>"${work}/ctree-cargo-calls"
    echo "test result: ok. 1 passed; 0 failed; 0 ignored"; exit 0 ;;
esac
SH
chmod +x "${ctree}/bin/cargo"
# pwsh 那一格只在装了 pwsh 的机器上跑（没装就 SKIP），所以夹具缺这个文件时本机照绿、
# CI 上一跑就红。真仓库里它永远在，夹具得连它一起造齐。
printf 'Write-Host "fixture"\n' >"${ctree}/bootstrap.ps1"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ctree}/nothing.sh"
printf '[workspace]\nmembers = ["crates/foo"]\n' >"${ctree}/Cargo.toml"
printf '[package]\nname = "foo"\nversion = "0.0.0"\nedition = "2021"\n' >"${ctree}/crates/foo/Cargo.toml"
printf 'pub fn one() -> u8 { 1 }\n' >"${ctree}/crates/foo/src/lib.rs"
cat >"${ctree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: nothing
        run: bash nothing.sh
YAML
(
    cd "${ctree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    git add tracked.txt bootstrap.ps1 nothing.sh Cargo.toml crates/foo/Cargo.toml crates/foo/src/lib.rs \
        .github/workflows/ci.yml deploy/scripts/tests/ok.test.sh \
        .github/scripts/tests/ok.test.sh deploy/scripts/check-before-push.sh \
        deploy/scripts/lib/build-slot.sh
    git commit -q -m base
    git update-ref refs/remotes/origin/main HEAD
) || fail "Clippy 脏树那一格的夹具建不起来"
ct_env=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/ctree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/ctree.lock"
    PATH="${ctree}/bin:${PATH}")
printf 'pub fn two() -> u8 { 2 }\n' >"${ctree}/crates/foo/src/lib.rs"
rm -f "${work}/ctree-clippy-cwd"
( cd "${ctree}" && "${ct_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/out" 2>&1 ||
    fail "Clippy 脏树那一跑自己红了：$(cat "${work}/out")"
dirty_cwd="$(head -n 1 "${work}/ctree-clippy-cwd" 2>/dev/null)"
[ -n "${dirty_cwd}" ] || fail "Clippy 在脏树上没跑：$(cat "${work}/out")"
case "${dirty_cwd}" in
    "${ctree}") : ;;
    *) fail "脏树上的 Clippy 判的不是工作树（cwd=${dirty_cwd}），而收尾记的是工作树的键" ;;
esac
# 干净树上反过来：判的是 HEAD 展开出来的那份拷贝（原行为，不许被这一改顺手丢掉）。
rm -f "${work}/ctree-clippy-cwd"
( cd "${ctree}" && git add crates/foo/src/lib.rs && git commit -q -m "two" ) || fail "干净树造不出来"
( cd "${ctree}" && "${ct_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/out" 2>&1 ||
    fail "Clippy 干净树那一跑自己红了：$(cat "${work}/out")"
clean_cwd="$(head -n 1 "${work}/ctree-clippy-cwd" 2>/dev/null)"
[ -n "${clean_cwd}" ] || fail "Clippy 在干净树上没跑：$(cat "${work}/out")"
case "${clean_cwd}" in
    *cogneva-push-clippy*) : ;;
    *) fail "干净树上的 Clippy 没有判 HEAD 展开出来的那份拷贝（cwd=${clean_cwd}）" ;;
esac

# 21) 容量登记表那格单文件 rustc 直编也要走构建槽。
#
# 这一格绕开槽的那段时间里，它与带内构建、别的会话的构建一起抢同一台机器的 CPU 与 IO，
# 而界是同一个数：不在槽里，读数就归不到因，界也就管不住它。
rtree="${work}/rtree"
mkdir -p "${rtree}/deploy/scripts/tests" "${rtree}/.github/scripts/tests" \
    "${rtree}/.github/workflows" "${rtree}/bin" "${rtree}/crates/cogneva/tests"
stage_gate "${repo}" "${rtree}"
printf '#!/usr/bin/env bash\nexit 0\n' >"${rtree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${rtree}/.github/scripts/tests/ok.test.sh"
printf 'fn main() {}\n' >"${rtree}/crates/cogneva/tests/rust_capacity_constants_registry_test.rs"
cat >"${rtree}/bin/rustc" <<SH
#!/usr/bin/env bash
# 跑起来的那一刻把槽文件抄一份：这一例判的就是「它当时在槽里」。
head -n 1 "\${COGNEVA_BUILD_GATE_LOCK_DIR:-/nonexistent}/slot-0.lock" >>"${work}/rtree-slot-seen" 2>/dev/null
out=""
while [ \$# -gt 0 ]; do
    case "\$1" in -o) out="\$2"; shift 2 ;; *) shift ;; esac
done
[ -n "\${out}" ] || exit 1
printf '#!/usr/bin/env bash\nexit 0\n' >"\${out}"
chmod +x "\${out}"
SH
chmod +x "${rtree}/bin/rustc"
# 这一例问的是槽，不是 cargo：没有假 cargo 时真 cargo 会去找 `Cargo.toml`、找不到就红，
# 那一步红会把这一例的结论盖掉。fmt／clippy 的版本探测回非零让那两格照常 SKIP。
cat >"${rtree}/bin/cargo" <<'SH'
#!/usr/bin/env bash
case "$1" in
  --version|-V) echo "cargo 9.9.9 (fixture)"; exit 0 ;;
  fmt) exit 1 ;;
  clippy) exit 1 ;;
  *) echo "test result: ok. 1 passed; 0 failed; 0 ignored"; exit 0 ;;
esac
SH
chmod +x "${rtree}/bin/cargo"
# pwsh 那一格只在装了 pwsh 的机器上跑（没装就 SKIP），所以夹具缺这个文件时本机照绿、
# CI 上一跑就红。真仓库里它永远在，夹具得连它一起造齐。
printf 'Write-Host "fixture"\n' >"${rtree}/bootstrap.ps1"
printf '#!/usr/bin/env bash\nexit 0\n' >"${rtree}/nothing.sh"
cat >"${rtree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: nothing
        run: bash nothing.sh
YAML
(
    cd "${rtree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    git add tracked.txt bootstrap.ps1 nothing.sh .github/workflows/ci.yml \
        crates/cogneva/tests/rust_capacity_constants_registry_test.rs \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh
    git commit -q -m base
    git update-ref refs/remotes/origin/main HEAD
) || fail "rustc 进槽那一格的夹具建不起来"
mkdir -p "${work}/rtree-gate"
: >"${work}/rtree-gate/slot-0.lock"
: >"${work}/rtree-gate/builds.claim"
printf '1\n' >"${work}/rtree-gate/buildgate.bound"
rm -f "${work}/rtree-slot-seen"
ct_env2=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/rtree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/rtree.lock"
    PATH="${rtree}/bin:${PATH}")
( cd "${rtree}" && "${ct_env2[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/out" 2>&1 ||
    fail "rustc 进槽那一跑自己红了：$(cat "${work}/out")"
grep -q '容量登记表' "${work}/rtree-slot-seen" 2>/dev/null ||
    fail "单文件 rustc 直编没有在槽里跑（槽里那行没有它）：$(cat "${work}/rtree-slot-seen" 2>/dev/null)"

# 22) 收尾算不出内容键 ≠ 这棵树在跑的过程中变过。
#
# 两者都让这一跑不能记账，但叫人去做的事相反：前者是「算不出」，后者是「有人动了树」。
# 开跑指纹取到、收尾取不到，原先落进「变过」那条消息里，读的人会去找一个不存在的改动者。
gtree="${work}/gtree"
mkdir -p "${gtree}/deploy/scripts/tests" "${gtree}/.github/scripts/tests" \
    "${gtree}/.github/workflows" "${gtree}/bin"
stage_gate "${repo}" "${gtree}"
printf '#!/usr/bin/env bash\nexit 0\n' >"${gtree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${gtree}/.github/scripts/tests/ok.test.sh"
cat >"${gtree}/bin/cargo" <<'SH'
#!/usr/bin/env bash
case "$1" in
  --version|-V) echo "cargo 9.9.9 (fixture)"; exit 0 ;;
  fmt) exit 1 ;;
  clippy) exit 1 ;;
  *) echo "test result: ok. 1 passed; 0 failed; 0 ignored"; exit 0 ;;
esac
SH
chmod +x "${gtree}/bin/cargo"
# 收尾那一次 `write-tree` 失败，开跑那一次正常。用计数包一层 git，而不是把 `.git` 弄坏：
# 弄坏会让门里排在收尾之前的每一步都读不到仓库、一起红，于是门在记账之前就退出，这一例
# 问的那句话根本没被执行到。计数只让第二次 `write-tree` 失败——那正是收尾算内容键的第
# 一步，其余调用照常转发。
real_git="$(command -v git)"
cat >"${gtree}/bin/git" <<SH
#!/usr/bin/env bash
if [ "\$1" = write-tree ]; then
    n="\$(cat "${work}/gtree-write-tree" 2>/dev/null || echo 0)"
    n=\$((n + 1))
    echo "\$n" >"${work}/gtree-write-tree"
    [ "\$n" -eq 1 ] || { echo "fixture: refusing the second write-tree" >&2; exit 1; }
fi
exec "${real_git}" "\$@"
SH
chmod +x "${gtree}/bin/git"
cat >"${gtree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: nothing
        run: bash nothing.sh
YAML
(
    cd "${gtree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    printf '#!/usr/bin/env bash\nexit 0\n' >nothing.sh
    # pwsh 那一格只在装了 pwsh 的机器上跑（没装就 SKIP），所以夹具缺这个文件时本机照绿、
    # CI 上一跑就红。真仓库里它永远在，夹具得连它一起造齐。
    printf 'Write-Host "fixture"\n' >bootstrap.ps1
    git add tracked.txt bootstrap.ps1 nothing.sh .github/workflows/ci.yml \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh
    git commit -q -m base
    git update-ref refs/remotes/origin/main HEAD
) || fail "算不出键那一格的夹具建不起来"
gt_env=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/gtree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/gtree.lock"
    PATH="${gtree}/bin:${PATH}")
rm -f "${work}/gtree-write-tree"
( cd "${gtree}" && "${gt_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/out" 2>&1 || true
grep -q '收尾算不出内容键' "${work}/out" ||
    fail "收尾算不出内容键时没有说清楚（读的人会当成树变过）：$(cat "${work}/out")"
grep -q '这棵树在跑的过程中变过' "${work}/out" &&
    fail "收尾算不出内容键被写成了「这棵树在跑的过程中变过」：叫人去找一个不存在的改动者；实得：$(cat "${work}/out")"
# 这一支只丢内容键：改动键判的是「这一跑判过的是不是这批改动」，收尾那一问读不到与
# 这件事无关。说成「两把都不记」会让读的人把一份已经判过的改动重跑一遍两小时。
grep -q '两把键都不记' "${work}/out" &&
    fail "收尾算不出内容键那一支说「两把键都不记」，但改动键照常落地：那句话会把一份已经判过的改动说成没判过；实得：$(cat "${work}/out")"

# 19) 远端在这一跑的期间往前走了，而这棵树一个字节都没动。改动键的基底跟着变了，
#     于是开跑那把与收尾那把对不上——上一版把这当成「这批改动在跑的过程中变过」，
#     改动键直接作废：判过的明明是同一份字节，只因为远端追上了一笔就要重判两小时。
#     2026-11-10 实测：两边同时在门下跑，谁先推谁就把对方这一跑白烧掉。
#     这一格要的是：收尾按**新基底**重算改动键并记下来，而且这一笔在提交之后、以及
#     变基到新基底之后都仍然命中（推前钩子问的正是那个基底）。
#     边界：共同祖先只会因为「远端涨到了这个 HEAD 的后代」而变深。旁支型前进（远端落了
#     与 HEAD 无关的一笔）不会让共同祖先动，改动键本来就不变，也就轮不到这里。
btree="${work}/btree"
mkdir -p "${btree}/deploy/scripts/tests" "${btree}/.github/workflows" "${btree}/.github/scripts/tests" "${btree}/bin"
stage_gate "${repo}" "${btree}"
printf '#!/usr/bin/env bash\nexit 0\n' >"${btree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${btree}/.github/scripts/tests/ok.test.sh"
printf 'Write-Host "fixture"\n' >"${btree}/bootstrap.ps1"
cat >"${btree}/bin/cargo" <<'SH'
#!/usr/bin/env bash
case "$1" in
  --version|-V) echo "cargo 9.9.9 (fixture)"; exit 0 ;;
  fmt|clippy) exit 1 ;;
  *) echo "test result: ok. 1 passed; 0 failed; 0 ignored"; exit 0 ;;
esac
SH
chmod +x "${btree}/bin/cargo"
# 这一步只动 `.git` 里的那个引用：造一个 C1 的后代提交（内容多一个 peer.txt），
# 把 refs/remotes/origin/main 指过去。工作树与真索引都不碰——碰了就成了第 16 格
# 那种「树变过」，两件事就分不开了。
cat >"${btree}/advance-origin.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
idx="$(mktemp)"
rm -f "${idx}"
GIT_INDEX_FILE="${idx}" git read-tree HEAD
printf 'peer\n' >peer.txt
GIT_INDEX_FILE="${idx}" git add peer.txt
tree="$(GIT_INDEX_FILE="${idx}" git write-tree)"
rm -f peer.txt "${idx}"
commit="$(printf 'peer advance\n' | git commit-tree "${tree}" -p HEAD)"
git update-ref refs/remotes/origin/main "${commit}"
SH
cat >"${btree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: a peer pushes main while the gate is judging this tree
        run: bash advance-origin.sh
YAML
(
    cd "${btree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    git add tracked.txt advance-origin.sh .github/workflows/ci.yml bootstrap.ps1 \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh \
        deploy/scripts/*.py
    git commit -q -m base
    # 起跑时远端在 C0、HEAD 在 C1：改动键里含 C1 这一笔。
    git update-ref refs/remotes/origin/main HEAD
    printf 'two\n' >second.txt
    git add second.txt
    git commit -q -m second
    # 未提交的那一份：这一跑判的是它，提交之后它就该命中推前钩子问的那个键。
    printf 'dirty\n' >>tracked.txt
) || fail "远端前进那一格的夹具建不起来"
bt_env=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/btree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/btree.lock"
    PATH="${btree}/bin:${PATH}")
( cd "${btree}" && "${bt_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/bout" 2>&1 ||
    fail "远端前进那一跑自己红了，这一格判不了记账：$(cat "${work}/bout")"
grep -q '这批改动在跑的过程中变过' "${work}/bout" &&
    fail "远端往前走被当成「这批改动变过」：判过的字节一个位没动，改动键不该作废；实得：$(grep '记账' "${work}/bout")"
grep -q '远端在这跑期间往前走了' "${work}/bout" ||
    fail "远端在跑期间往前走了，记账时没说；实得：$(grep '记账' "${work}/bout")"
# 提交的是**这一跑判过的那份字节**，所以用 `add -A`：夹具里未跟踪的东西（假 cargo 之类）
# 也在判过的那份里，只提交 touched 的那个文件会让两边对不上，而那种对不上是夹具的错，
# 不是门的错。真实流程里也一样——你判完把自己那份提交掉。
git -C "${btree}" add -A
git -C "${btree}" commit -q -m 'the change the gate judged'
( cd "${btree}" && "${bt_env[@]}" bash deploy/scripts/check-before-push.sh --status-patch ) >"${work}/bout3" 2>&1 ||
    fail "提交之后改动键不再命中：这一笔本可以不再重判，现在要重跑；$(cat "${work}/bout3")"
# 真正的结局在这里：推之前必须先把远端那笔合进来（快进不了就被拒），而合进来之后
# 从新基底到 HEAD 的 diff 正是这批改动本身——钩子问的是这一把。只有开跑那把会命中；
# 照新基底重算的那把（diff 里反着算进 peer.txt）在这里必然对不上。
git -C "${btree}" rebase -q refs/remotes/origin/main >"${work}/brebase" 2>&1 ||
    fail "夹具的变基做不成，这一格判不了：$(cat "${work}/brebase")"
( cd "${btree}" && "${bt_env[@]}" bash deploy/scripts/check-before-push.sh --status-patch ) >"${work}/bout4" 2>&1 ||
    fail "变基到远端新基底之后改动键不再命中：这才是这一格要防的白跑；$(cat "${work}/bout4")"

# 20) 第三种形状：远端在这跑期间**含了这一支自己**（远端涨到了 HEAD），工作树干净。
#     这时从新基底（= HEAD）到工作树是一份空 diff，patch-id 本来就是空的。上一版在这一
#     形状下先印「改动键按新基底重记（）」——括号里什么都没有——再印一句「没有共同祖先，
#     或临时索引写不了」。读的人会照着后一句去查共同祖先、去查临时索引，而两样都不缺；
#     真正的事实是「没有要补的那一段」。这一格钉三件事：两种话分得开、不印空的键、
#     这一跑照常退出 0（判词照旧有效，只是没有改动键）。
ctree="${work}/ctree"
mkdir -p "${ctree}/deploy/scripts/tests" "${ctree}/.github/workflows" "${ctree}/.github/scripts/tests" "${ctree}/bin"
stage_gate "${repo}" "${ctree}"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ctree}/deploy/scripts/tests/ok.test.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ctree}/.github/scripts/tests/ok.test.sh"
printf 'Write-Host "fixture"\n' >"${ctree}/bootstrap.ps1"
cp "${btree}/bin/cargo" "${ctree}/bin/cargo"
chmod +x "${ctree}/bin/cargo"
# 远端在这一跑期间涨到 HEAD 自己：别人把这一支推上去了。只动 `.git` 里的引用，
# 工作树与真索引一个字节都不碰。
cat >"${ctree}/catch-up.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
git update-ref refs/remotes/origin/main HEAD
SH
cat >"${ctree}/.github/workflows/ci.yml" <<'YAML'
name: ci
jobs:
  repo-conventions:
    steps:
      - name: a peer pushes this very branch while the gate is judging it
        run: bash catch-up.sh
YAML
(
    cd "${ctree}" || exit 1
    git init -q -b main .
    git config user.email gate@example.invalid
    git config user.name gate
    printf 'one\n' >tracked.txt
    git add tracked.txt catch-up.sh .github/workflows/ci.yml bootstrap.ps1 \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh \
        deploy/scripts/*.py bin/cargo
    git commit -q -m base
    # 起跑时远端在 C0、HEAD 在 C1：开跑那把改动键非空，收尾没东西可算。
    git update-ref refs/remotes/origin/main HEAD
    printf 'two\n' >second.txt
    git add second.txt
    git commit -q -m second
)
ct_env=(env COGNEVA_BUILD_GATE_LOCK_DIR="${work}/ctree-gate" COGNEVA_BUILD_LOCK_FILE="${work}/ctree.lock"
    PATH="${ctree}/bin:${PATH}")
( cd "${ctree}" && "${ct_env[@]}" bash deploy/scripts/check-before-push.sh ) >"${work}/cout" 2>&1 ||
    fail "远端已经含了这一支的那一跑自己红了：没有改动可判不等于判过的那份字节变红了；$(cat "${work}/cout")"
grep -q '改动键按新基底重记' "${work}/cout" &&
    fail "远端已经含了这一支，仍按新基底重记改动键（那意味着印了个空的键）；实得：$(grep '记账' "${work}/cout")"
grep -q '算不出改动键' "${work}/cout" &&
    fail "空 diff 被说成「算不出改动键」：共同祖先与临时索引都在，缺的是可判的改动；实得：$(grep '记账' "${work}/cout")"
grep -q '远端在这跑期间已经含了这一支' "${work}/cout" ||
    fail "收尾没说「远端已经含了这一支」；实得：$(grep '记账' "${work}/cout")"
grep -q '没有改动键可记' "${work}/cout" ||
    fail "收尾没把「没有要推的那一段」说成一句自己的话；实得：$(grep '记账' "${work}/cout")"

echo "ok - before-push 的戳：没验过 / 验过 / 新提交 三种结局分得开，未提交改动不进键；提交前那一问按工作树字节记，改字节或改判据都失效；改动键在干净的变基后仍命中、解冲突后失效，远端在跑期间追上了这一支时按新基底重记而不是作废、并且变基之后照样命中；远端已经含了这一支时说的是「没有改动可判」而不是「算不出」；键不取决于调用方的 PATH 里有没有 cargo"
