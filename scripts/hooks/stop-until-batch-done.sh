#!/usr/bin/env bash
# 这一批没改完，就不许收工。
#
# 光有「编译前问批次」还不够：那一条只挡住「没改完就编译」，挡不住「改一半就停」。
# 2026-11-19 的现场是后者——批次没清空，我却一轮一轮停下来汇报。停手不是错误，
# 但在还有待办、而且刚刚还在改的时候停手，就是把这一批又切成了小段。
#
# 判据，缺一条就放行：
#   0. 没有开着的一轮，或这一轮已终态（`cogneva ledger stop-gate` 退 0）。这一问是
#      向产品要的，不按 `status` 的裸往返码判：那边「没有轮」与「有缺口」都是非零，
#      按码分支就把关完的一轮也拦下（2026-11-23 实测，关完轮照拦）；
#   1. 最近 15 分钟内这棵树被改过（说明在干活，不是等外部事件）；
#   2. 这一遍编辑的触点还没都动到（`cogneva ledger editing` 退 1）；
#   3. 没有放行标记文件 `.git/cogneva-stop-allowed`（等 CI / 等部署 / 等属主时用）。
#
# 退出码 2 ＝ 不许停，并把缺口打给模型看；0 ＝ 可以停。
# 它不判「改得对不对」，只判「这一批还没完就别把活放下」。
set -uo pipefail

root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$root" || exit 0

marker="$(git rev-parse --git-common-dir 2>/dev/null || echo .git)/cogneva-stop-allowed"
[ -f "$marker" ] && exit 0

# 刚改过才算在干活：15 分钟内没有编辑，说明在等外部事件，放行。
recent=$(find crates deploy scripts .github -type f -mmin -15 2>/dev/null | head -1)
[ -z "$recent" ] && exit 0

# 判词在产品里（`cogneva ledger`）：带外不再有第二份实现。没有二进制就是判不了——
# 判不了不当作可以停，判词要说清是读不出，不是「没关」。
bin="${COGNEVA_BIN:-$root/target/debug/cogneva}"
if [ ! -x "$bin" ]; then
  printf '停手门：没有平台二进制（%s），这一问没判，不当作可以停。\n' "$bin" >&2
  exit 2
fi

# 三态，按这一问自己的约定分：0 可以停、2 有缺口（把缺口打出来）、3 判不了。
out="$("$bin" ledger stop-gate 2>&1)"; rc=$?
case "$rc" in
0) ;;
2)
  printf '这一轮还没关，别停：\n%s\n' "$out" >&2
  exit 2
  ;;
*)
  printf '停手门读不出这一轮（往返码 %s），不当作可以停：\n%s\n' "$rc" "$out" >&2
  exit 2
  ;;
esac

# 同一份触点在提交那一刀也判（`pre-commit.d/55-...`），两处问同一个问题。
out="$("$bin" ledger editing 2>&1)"
rc=$?
[ "$rc" -eq 0 ] && exit 0
[ "$rc" -eq 2 ] && exit 0

printf '这一批还没改完，别停：\n%s\n\n' "$out" >&2
cat >&2 <<'EOF'
要继续改缺口；确实在等外部事件（CI、部署、属主输入）就先落一个放行标记，再停：
    touch "$(git rev-parse --git-common-dir)/cogneva-stop-allowed"
事件过去之后把它删掉，下一次收工又会按批次判。
EOF
exit 2
