#!/usr/bin/env bash
# 这一批没改完，就不许收工。
#
# 光有「编译前问批次」还不够：那一条只挡住「没改完就编译」，挡不住「改一半就停」。
# 2026-11-19 的现场是后者——批次没清空，我却一轮一轮停下来汇报。停手不是错误，
# 但在还有待办、而且刚刚还在改的时候停手，就是把这一批又切成了小段。
#
# 判据三条，缺一不可（否则放行）：
#   1. 本轮批次还有缺口（`family.py --all-todos` 退出码非 0）；
#   2. 最近 15 分钟内这棵树被改过（说明在干活，不是等外部事件）；
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

round="$root/scripts/hooks/round.py"
if [ -f "$round" ] && ! python3 "$round" status >/dev/null 2>&1; then
  printf '这一轮还没关，别停：\n' >&2
  python3 "$round" status >&2 || true
  exit 2
fi

family="$root/scripts/hooks/family.py"
[ -f "$family" ] || exit 0

out="$(python3 "$family" --root "$root" --all-todos 2>&1)"
rc=$?
[ "$rc" -eq 0 ] && exit 0

printf '这一批还没改完，别停：\n%s\n\n' "$out" >&2
cat >&2 <<'EOF'
要继续改缺口；确实在等外部事件（CI、部署、属主输入）就先落一个放行标记，再停：
    touch "$(git rev-parse --git-common-dir)/cogneva-stop-allowed"
事件过去之后把它删掉，下一次收工又会按批次判。
EOF
exit 2
