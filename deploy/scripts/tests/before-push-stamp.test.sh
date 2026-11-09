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
    git add tracked.txt touch-tree.sh .github/workflows/ci.yml \
        deploy/scripts/tests/ok.test.sh .github/scripts/tests/ok.test.sh \
        deploy/scripts/check-before-push.sh deploy/scripts/lib/build-slot.sh
    git commit -q -m base -- tracked.txt touch-tree.sh .github/workflows/ci.yml \
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
if ( cd "${mtree}" && "${mt_env[@]}" bash deploy/scripts/check-before-push.sh --status-content ) >"${work}/out" 2>&1; then
    fail "跑的途中变过的树仍被记成验过：那一笔一秒都没被判过；实得：$(cat "${work}/out")"
fi

echo "ok - before-push 的戳：没验过 / 验过 / 新提交 三种结局分得开，未提交改动不进键；提交前那一问按工作树字节记，改字节或改判据都失效；改动键在干净的变基后仍命中、解冲突后失效；键不取决于调用方的 PATH 里有没有 cargo"
