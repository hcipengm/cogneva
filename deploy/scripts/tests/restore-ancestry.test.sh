#!/usr/bin/env bash
# 代次判据：源码比包新就装，反向不装。
#
# 属主要的是「空白机上仓库源码版本 ≥ 包版本就能恢复」，不是「两边必须相等」。这一格钉的
# 就是这个不等号的方向：向前放行、向后拒绝、分叉拒绝、宿主没有那段历史时拒绝、相等不判。
#
# 判据从脚本里按标记抽出来跑，不在这里抄一份实现——抄的那份会在脚本改过之后继续绿。
# 两个产物分开断言：交给 Job 的 env 走 stdout（有/无本身就是判词），人读的原因走 stderr。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
script="${repo}/deploy/scripts/restore-from-package.sh"
pass=0; fail=0
ok()  { printf 'ok    %s\n' "$1"; pass=$((pass+1)); }
bad() { printf 'FAIL  %s\n' "$1"; fail=$((fail+1)); }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

sed -n '/^# ancestry: start$/,/^# ancestry: end$/p' "${script}" >"${work}/ancestry.sh"
if [ -s "${work}/ancestry.sh" ]; then ok "脚本里有 ancestry 块（标记在）"
else bad "脚本里没有 ancestry 块：标记被删了，这一族判据会静默变成空跑"; exit 1; fi

# 一个小仓库：a 是祖先，b 是 a 的后代，x 与 b 从 a 分叉。
r="${work}/r"
mkdir -p "$r"
git -C "$r" init -q
git -C "$r" config user.email t@example.invalid
git -C "$r" config user.name t
a="$(git -C "$r" commit -q --allow-empty -m A && git -C "$r" rev-parse HEAD)"
b="$(git -C "$r" commit -q --allow-empty -m B && git -C "$r" rev-parse HEAD)"
git -C "$r" checkout -q -b side "$a"
x="$(git -C "$r" commit -q --allow-empty -m X && git -C "$r" rev-parse HEAD)"
git -C "$r" checkout -q - 2>/dev/null || git -C "$r" checkout -q main 2>/dev/null || true

run() { # run <PKG_REV> <IMAGE_REV>：跑一次抽出来的函数，全局 rc/out/err 带回结果
    out="$(ROOT="$r" PKG_REV="$1" IMAGE_REV="$2" bash -c 'source "$1"; ancestry_proof_env' _ "${work}/ancestry.sh" 2>"${work}/err")"
    rc=$?
    err="$(cat "${work}/err")"
}

# 1. 向前：包是旧版、源码是它的后代 —— 这就是「≥ 就能装」的那条路。
run "$a" "$b"
[ "$rc" = 0 ] && ok "向前恢复：rc=0" || bad "向前恢复被判失败（rc=$rc）：源码比包新本应放行"
case "$out" in *COGNEVA_RESTORE_ANCESTRY_PACKAGE_REV*) ok "向前恢复：打印了包 rev 的 env" ;;
    *) bad "向前恢复没打印包 rev 的 env（产物为空＝Job 拿不到证明）" ;; esac
case "$out" in *"\"$a\""*) ok "向前恢复：env 里的包 rev 是包自己那个（$a）" ;;
    *) bad "向前恢复的包 rev 不对：$out" ;; esac
case "$out" in *"\"$b\""*) ok "向前恢复：env 里的源 rev 是恢复镜像那个（$b）" ;;
    *) bad "向前恢复的源 rev 不对：$out" ;; esac
case "$err" in *按前进方向安装*) ok "向前恢复：说清了为什么放行" ;;
    *) bad "向前恢复没说明放行理由：$err" ;; esac

# 2. 向后：包比源码新 —— 装回去会缺掉包里那些代次的列/行。
run "$b" "$a"
[ "$rc" = 1 ] && ok "向后恢复：rc=1（拒绝）" || bad "向后恢复被放行了（rc=$rc）：源码比包旧不该装"
[ -z "$out" ] && ok "向后恢复：没有打印任何 env" || bad "向后恢复仍打印了 env：$out"
case "$err" in *COGNEVA_RESTORE_ACCEPT_REV_MISMATCH*) ok "向后恢复：点名了显式接受的开关" ;;
    *) bad "向后恢复没给退路：$err" ;; esac

# 3. 分叉：两支互不包含，git 答不了「谁在谁里面」，与向后同样拒绝。
run "$x" "$b"
[ "$rc" = 1 ] && ok "分叉恢复：rc=1（拒绝）" || bad "分叉恢复被放行了（rc=$rc）"
[ -z "$out" ] && ok "分叉恢复：没有打印任何 env" || bad "分叉恢复仍打印了 env：$out"

# 4. 宿主上没有镜像那一版（浅克隆、换过远端）——证明不出来就拒绝，不能当成「相等」。
run "$a" "0000000000000000000000000000000000000000"
[ "$rc" = 1 ] && ok "源 rev 不在宿主历史里：rc=1（拒绝）" || bad "源 rev 不在历史里却放行了（rc=$rc）"
[ -z "$out" ] && ok "源 rev 不在历史里：没有打印任何 env" || bad "源 rev 不在历史里仍打印了 env：$out"

# 5. 同一版：既不判代次、也不打印、也不说话——同代恢复是常态，不该被拒也不该被记成跨代。
run "$b" "$b"
[ "$rc" = 0 ] && ok "同代恢复：rc=0" || bad "同代恢复被拒了（rc=$rc）"
[ -z "$out" ] && ok "同代恢复：没有打印 env" || bad "同代恢复打印了 env：$out"
[ -z "$err" ] && ok "同代恢复：没有说多余的话" || bad "同代恢复说了话：$err"

# 6. 前后差很多代也照放：判据是「包含」，不是「相邻」。
d="$(
    git -C "$r" checkout -q main 2>/dev/null || git -C "$r" checkout -q master 2>/dev/null || true
    git -C "$r" commit -q --allow-empty -m C
    git -C "$r" commit -q --allow-empty -m D
    git -C "$r" rev-parse HEAD
)"
run "$a" "$d"
[ "$rc" = 0 ] && ok "差多代的前向恢复：rc=0" || bad "差多代的前向恢复被拒了（rc=$rc）"

# 调用点必须吞掉失败：这一段在 set -e 下运行，返回 1 会让整个恢复脚本在「拒绝」那句之后
# 直接退出，而拒绝的处置是「继续 apply、由 Job 侧再判」，不是「脚本死在这里」。
if grep -q 'ANCESTRY_ENV="$(ancestry_proof_env)" || true' "${script}"; then
    ok "调用点吞掉了拒绝：拒绝不中断脚本（Job 侧才判）"
else
    bad "调用点没吞掉 ancestry_proof_env 的失败：set -e 下拒绝会提前退出脚本"
fi

printf '\nrestore-ancestry.test: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
