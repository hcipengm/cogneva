#!/usr/bin/env bash
# principle: feedback_a_template_is_not_a_dead_path
# check-readme-paths.sh 的判据：文档点名的路径必须在这棵树里。
#
# 这条规则的存在理由决定了夹具的形状——它要防的是**静默**的死指针（路径搬了家，
# 正文没跟，读的人照着开文件才发现），所以夹具里必须同时有「真路径」和「死指针」，
# 只有前者绿、后者红，才说明它读的是存在性而不是别的什么。
#
# 三组夹具，每组钉一条口径（口径的来历见脚本头部注释）：
#   - 真路径：带 `crates/` 前缀的、不带前缀的（crate 名直呼）、根目录的文件 —— 都要绿；
#   - 死指针：今天不存在的 .rs 路径（就是真实发生过的那个）—— 必须红，且判词指到行；
#   - 不是路径的东西：运行期目录（无扩展名）、散装斜杠、绝对路径、URL —— 都要绿，
#     否则这条门会因为文档里一句与路径无关的话而红，红了就没人信它。
#
# 最后一条断言是**在真文件上跑**：这份仓库根的两份 README 必须绿。夹具全绿而真 README
# 红，说明这条门只是在测自己；夹具全绿而真 README 也绿，才说明它今天在守东西。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-readme-paths.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# expects <name> <exit-code> <body>
expect() {
  local name="$1" want="$2" body="$3"
  local path="${work}/${name}.md"
  printf '%s\n' "${body}" >"${path}"
  local got=0
  bash "${checker}" "${path}" >"${work}/${name}.out" 2>&1 || got=$?
  [ "${got}" -eq "${want}" ] || { cat "${work}/${name}.out" >&2; fail "${name}: exit ${got}, wanted ${want}"; }
}

# 带前缀的真路径：就是本轮修掉的那一条改对之后的样子。
expect real_path 0 '`MessageBackend` Trait 定义（`crates/cog-core/src/contract/stream.rs`）：
- publish / subscribe'

# 不带前缀的真路径：README 里 crate 名是直呼的（`cogneva/src/bootstrap.rs`），
# 按 `crates/` 兜底解析，不能因为少了前缀就判红。
expect bare_crate 0 '**Dependency topological sorting**: `cogneva/src/bootstrap.rs` resolves the graph'

# 根目录的文件：Markdown 之间的相对链接。
expect root_file 0 '[简体中文](README.zh-CN.md)'

# 死指针：本轮真实发生过的那一条。改回旧写法必须红。
expect dead_pointer 1 '`MessageBackend` Trait definition (`cog-core/src/stream/message_bus.rs`):'

# 运行期目录：程序运行时才创建，不在仓库里。没有扩展名 ⇒ 不被当路径 ⇒ 绿。
expect runtime_dir 0 '4. 通过质量门后写入 `evolution-changes/`'

# 散装斜杠：不是反引号括起来的，不解析。
expect prose_slash 0 'The pipeline reads and writes files, and the ratio is read / write bound.'

# 模板占位：描述产物命名的一句，花括号说明「这里是个变量」。与 `<`、`*` 同族，
# 都不是在说某一份文件今天在树上——判红的修法会变成「把文档改糊涂」。
expect template_placeholder 0 '产物存放在 `{name}/{version:06}.json` 加一个 `ACTIVE` 指针'

# 绝对路径与 URL：容器里的路径与外部文档，不该按仓库根解析。
expect absolute_and_url 0 '装到 `/opt/cogneva`，文档见 `https://example.com/a/b.md`。'

# 判词要指到行：一行里两个 token、第二个是死指针，报的行号得是那一行。
printf '%s\n' 'first `crates/cog-core/src/contract/stream.rs`' \
              'second `crates/cog-core/src/no/such/file.rs`' >"${work}/lineno.md"
got=0
out="$(bash "${checker}" "${work}/lineno.md" 2>&1)" || got=$?
[ "${got}" -eq 1 ] || fail "lineno: exit ${got}, wanted 1"
case "${out}" in
  *lineno.md:2:*) ;;
  *) fail "lineno: 判词没有指到第 2 行 —— ${out}" ;;
esac

# 读不出来不等于「里面没有路径」：这是「一个存在的普通文件，但 grep 读不动」的形状，
# 用 /proc/self/mem 是因为它对 root 也一样读不动（chmod 000 对 root 无效，夹具会在
# 某些宿主上假绿）。旧脚本把它当零命中，印绿。
if [ -e /proc/self/mem ]; then
  got=0
  out="$(bash "${checker}" /proc/self/mem 2>&1)" || got=$?
  [ "${got}" -eq 2 ] || fail "unreadable: exit ${got}, wanted 2 —— ${out}"
  case "${out}" in
    *读不出*) ;;
    *) fail "unreadable: 判词没说清是读不出来 —— ${out}" ;;
  esac
fi

# 门自己挑人口时（不给参数 ⇒ 两份 README），一处路径都认不出来 ⇒ 3：零处与
# 「每一处都还在」在输出上同形。用一个自己的小树测，否则要动真 README。
tree="${work}/tree"
mkdir -p "${tree}/deploy/scripts"
cp "${checker}" "${tree}/deploy/scripts/check-readme-paths.sh"
printf '# 项目简介\n\n一段话，一个反引号也没有。\n' >"${tree}/README.md"
printf '# 项目简介\n\n一段话，一个反引号也没有。\n' >"${tree}/README.zh-CN.md"
got=0
out="$(cd "${tree}" && bash "${tree}/deploy/scripts/check-readme-paths.sh" 2>&1)" || got=$?
[ "${got}" -eq 3 ] || fail "collapsed: exit ${got}, wanted 3 —— ${out}"
case "${out}" in
  *"下限"*) ;;
  *) fail "collapsed: 判词没说是人口塌了 —— ${out}" ;;
esac

# 调用方点名一份没有路径的文档 ⇒ 仍绿：那份文档里没有路径就是它今天的实情，
# 门不该为了自己的下限去逼文档写路径。
printf '# 简介\n\n没有反引号。\n' >"${work}/plain.md"
got=0
out="$(bash "${checker}" "${work}/plain.md" 2>&1)" || got=$?
[ "${got}" -eq 0 ] || fail "plain_doc: exit ${got}, wanted 0 —— ${out}"

# 真文件：仓库根的两份 README 今天必须绿——给文件与不给文件两条路都走一遍，
# 后者走的就是带下限的那条。
for f in "${repo}/README.md" "${repo}/README.zh-CN.md"; do
  got=0
  out="$(bash "${checker}" "${f}" 2>&1)" || got=$?
  [ "${got}" -eq 0 ] || fail "$(basename "${f}") 里有死指针：${out}"
done
got=0
out="$(bash "${checker}" 2>&1)" || got=$?
[ "${got}" -eq 0 ] || fail "no-arg run on the real tree: exit ${got} —— ${out}"

echo "readme-paths.test: all checks passed"
