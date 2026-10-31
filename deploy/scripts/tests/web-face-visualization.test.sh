#!/usr/bin/env bash
# check-web-face-visualization.sh 的判据：第 3 条（画出来的那几笔有没有可读名、
# 每一格的原值还读不读得到）必须**认机制**，不能认属性名。
#
# 这一条怎么坏是静默的：把原生 `title=` 换成页内 `Note`（文字可选、可复制、走
# `aria-describedby`），读者拿到的路更多了，而门按字面数 `title=` 的数量，于是红了
# ——红在一个**更好的**写法上。这类假红比漏判更贵：它挡住的是整条主干。
#
# 所以夹具直接用真树里的 `ui/plots.tsx`（不是手抄一份骨架）：判据要跟的是这份产品
# 文件当前的写法，抄一份骨架就等于门在测自己。逐段剪断、每剪一刀必须红：
#   - 一个画法一条原值路都没有 → 红（逐画法判，不是全文件数总数）
#   - 一个画法丢了 aria-label  → 红（可读名与可读值是两个面）
#   - 一个面既没画法也没写「画面：」→ 红
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-face-visualization.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/web"
  mkdir -p "${work}/web/src/ui" "${work}/web/src/views"
  cp "${repo}/web/src/ui/plots.tsx" "${work}/web/src/ui/plots.tsx"
  cat >"${work}/web/src/views/One.tsx" <<'EOF'
import { BarList } from '@/ui/plots';

export function One() {
  return <BarList rows={[]} label="某一份读数" />;
}
EOF
}

confirm() { # file, literal
  grep -qF "$2" "$1" || fail "改动没落到 $(basename "$1")：找不到「$2」"
}

expect_red() { # name, substring
  local out rc
  rc=0
  out="$(bash "${checker}" "${work}" 2>&1)" || rc=$?
  [ "${rc}" -ne 0 ] || fail "「$1」应该红，实际绿了（这条判据抓不住它）"
  case "${out}" in
    *"$2"*) ;;
    *) fail "「$1」红了但判词不对：期望含「$2」，实际是「${out}」" ;;
  esac
  echo "ok(red): $1"
}

# 把一个画法的函数体里所有 `Note` 换成 `span`：原值那几条路全没了，而这一幅还在画。
strip_paths() { # file, function name
  python3 - "$1" "$2" <<'PY'
import re
import sys

path, name = sys.argv[1], sys.argv[2]
src = open(path, encoding="utf-8").read()
body = re.search(r"export function %s\(.*?\n\}\n" % name, src, re.S)
if not body:
    sys.exit("fixture: 读不出 %s 的函数体" % name)
cut = re.sub(r"<Note\b", "<span", body.group(0))
path_out = src.replace(body.group(0), cut)
if path_out == src:
    sys.exit("fixture: %s 里没有可剪的 <Note" % name)
open(path, "w", encoding="utf-8").write(path_out)
PY
}

# 1. 一个画法一条原值路都没有：条形还画得出来，读者却只剩相对长短。
fixture
strip_paths "${work}/web/src/ui/plots.tsx" BarList
confirm "${work}/web/src/ui/plots.tsx" "<span"
expect_red "画法没有原值路" "没有任何一条给原值的路"

# 2. 可读名丢了：这一幅对读屏软件不存在，和「画得好不好」是两件事。
fixture
python3 - "${work}/web/src/ui/plots.tsx" <<'PY'
import re
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
body = re.search(r"export function Strip\(.*?\n\}\n", src, re.S)
if not body:
    sys.exit("fixture: 读不出 Strip 的函数体")
cut = re.sub(r"\s+aria-label=\{[^}]*\}", "", body.group(0))
if cut == body.group(0):
    sys.exit("fixture: Strip 里没有可剪的 aria-label")
open(path, "w", encoding="utf-8").write(src.replace(body.group(0), cut))
PY
if grep -qF 'aria-label' <(python3 - "${work}/web/src/ui/plots.tsx" <<'PY'
import re
import sys
src = open(sys.argv[1], encoding="utf-8").read()
print(re.search(r"export function Strip\(.*?\n\}\n", src, re.S).group(0))
PY
); then
  fail "改动没落到 plots.tsx：Strip 的 aria-label 还在"
fi
expect_red "画法没有可读名" "没有 \`aria-label\`"

# 3. 一个面既没有画法，也没有在文件头写明「画面：」：一整屏表格，读者要在脑子里
#    把一列数字排成条形。
fixture
cat >"${work}/web/src/views/Two.tsx" <<'EOF'
export function Two() {
  return <table><tbody><tr><td>0</td></tr></tbody></table>;
}
EOF
confirm "${work}/web/src/views/Two.tsx" "<table>"
expect_red "面没有画法" "一个形状都画不出来"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-face-visualization.test: all checks passed"
