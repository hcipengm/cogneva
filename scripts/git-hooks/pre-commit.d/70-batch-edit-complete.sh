#!/usr/bin/env bash
# 编辑没走完就不许提交（属主 2026-11-21 定的流程：不能边改边提交）。
#
# 判据是 `scripts/hooks/edit-reading.py --stage edit`：这一轮那一批，每条都点得出
# 一条测试、那条测试在本批 diff 里、函数带测试属性。为真才许提交。
#
# 读不到（没有开着一轮、台账读不到、git 判不了）**不当绿**：退 1 并说清是哪一种。
# 要绕过只有 `git commit --no-verify`，而那一笔不会被当成走过这一关。
set -uo pipefail
root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
reader="${root:+$root/scripts/hooks/edit-reading.py}"
[ -n "$root" ] && [ -f "$reader" ] || exit 0

# 只动这道门自己的提交放行：否则第一笔「把这道门装上去」永远提交不了，而一门
# 修不了自己的门等于没有。放行范围就是这一个目录，别的一律照判。
staged="$(git diff --cached --name-only 2>/dev/null)"
if [ -n "$staged" ] && ! printf '%s\n' "$staged" | grep -qv '^scripts/git-hooks/'; then
  echo "pre-commit: 这一笔只动门自己，跳过批次判据。" >&2
  exit 0
fi

out="$(python3 "$reader" --root "$root" --stage edit 2>&1)"
rc=$?
case "$rc" in
  0) exit 0 ;;
  1)
    printf '%s\n' "$out" >&2
    cat >&2 <<'EOF'

pre-commit: 这一批还没编辑完——上面每一条 `代码=false` 都还没做到「点得出一条
本批写下的测试」。流程是整批编辑完再提交、再编译一次，不是边改边提交。

要绕过：`git commit --no-verify`（那一笔不算走过这一关）。
EOF
    exit 1
    ;;
  *)
    printf '%s\n' "$out" >&2
    echo "pre-commit: 编辑完成度判不了（退 $rc）——不当绿。" >&2
    exit 1
    ;;
esac
