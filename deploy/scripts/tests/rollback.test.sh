#!/usr/bin/env bash
# rollback.sh 的定向测试：这条路径是「改坏了怎么退回去」唯一的入口，而它的每一格
# 都只在真出事的那一晚被走到。所以每一格单独造一份仓，断言**它拒绝那一句 / 它执行那一条**，
# 不只断言退出码——退出码 2 在「没有 --to」与「不是祖先」之间是同一个数。
#
# 被测的两件事分开量：
#   1. **默认不动集群**：同一份输入，不带 --apply 时必须一条 kubectl 都没执行，
#      带 --apply 时执行的必须正好是那条命令。只测「有输出」会把「已经 apply 了」
#      读成「计划好了」。
#   2. **不是回滚的四种形状都拒**：往前/旁支、不存在的版本、那一版没有清单目录、
#      清单目录是空的。放行前两种会让这条路径变成绕过评审的部署入口；放行后两种
#      会 apply 一个空目录并退出 0，读起来像回滚成功。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
tool="${repo}/deploy/scripts/rollback.sh"

pass=0
fail=0
ok()  { printf 'ok    %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL  %s\n' "$1" >&2; fail=$((fail + 1)); }

[ -f "$tool" ] || { bad "回滚入口不在树上：$tool"; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# kubectl 桩：把每次调用的参数记下来，不碰真集群。退出码由 KC_RC 给。
mkdir -p "$WORK/bin"
cat > "$WORK/bin/kubectl" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$KC_LOG"
# apply 与 rollout 分开给退出码：一条 `KC_RC=1` 会让第一句就失败，那一趟证明的
# 是「apply 失败被看见」，而不是「rollout 不绿被看见」——两句话的修法不同。
case "$1" in
    apply)   exit "${KC_APPLY_RC:-0}" ;;
    -n)      exit "${KC_ROLLOUT_RC:-0}" ;;
    *)       exit 0 ;;
esac
STUB
chmod +x "$WORK/bin/kubectl"

# 夹具仓：两个提交，旧的带清单、新的带清单；第三个提交里把清单目录删掉。
FIX="$WORK/repo"
mkdir -p "$FIX/deploy/rendered/k3s-single"
git -C "$FIX" init -q
git -C "$FIX" config user.email t@example.com
git -C "$FIX" config user.name t
printf 'kind: ConfigMap\nmetadata:\n  name: old\n' > "$FIX/deploy/rendered/k3s-single/10-configmap.yaml"
git -C "$FIX" add -A && git -C "$FIX" commit -qm one
old_rev="$(git -C "$FIX" rev-parse HEAD)"
printf 'kind: ConfigMap\nmetadata:\n  name: new\n' > "$FIX/deploy/rendered/k3s-single/10-configmap.yaml"
git -C "$FIX" add -A && git -C "$FIX" commit -qm two

# 旁支：从 old 拉出来的一条线，不是 HEAD 的祖先。
git -C "$FIX" branch side "$old_rev"
git -C "$FIX" checkout -q side
printf 'kind: ConfigMap\nmetadata:\n  name: side\n' > "$FIX/deploy/rendered/k3s-single/10-configmap.yaml"
git -C "$FIX" add -A && git -C "$FIX" commit -qm side
side_rev="$(git -C "$FIX" rev-parse HEAD)"
git -C "$FIX" checkout -q master 2>/dev/null || git -C "$FIX" checkout -q main

# 没有清单目录的一版。
git -C "$FIX" rm -rq deploy/rendered
git -C "$FIX" commit -qm "drop rendered"
empty_rev="$(git -C "$FIX" rev-parse HEAD)"

run() { # rc 与 stdout 都交给调用方
    KC_LOG="$WORK/calls" PATH="$WORK/bin:$PATH" \
        bash "$tool" "$@" 2>&1
}

# 1. 计划：默认不碰集群，印出目标与那条命令。
: > "$WORK/calls"
out="$(run --repo "$FIX" --to "$old_rev")"
rc=$?
if [ "$rc" -eq 0 ] && [ ! -s "$WORK/calls" ] \
   && printf '%s' "$out" | grep -q "to=${old_rev:0:12}" \
   && printf '%s' "$out" | grep -q 'rollback_mode=plan'; then
    ok "默认只出计划：一条 kubectl 都没执行，印出目标与 plan"
else
    bad "默认那一趟不对（rc=$rc，kubectl 调用=$(wc -l <"$WORK/calls")）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 2. --apply：执行的正好是 apply -f <那一版>/。
: > "$WORK/calls"
out="$(run --repo "$FIX" --to "$old_rev" --apply)"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(wc -l <"$WORK/calls")" -eq 1 ] \
   && grep -q '^apply -f .*/deploy/rendered/k3s-single/$' "$WORK/calls"; then
    ok "--apply 执行 apply -f <那一版>/，且只执行一条"
else
    bad "--apply 那一趟不对（rc=$rc）：$(cat "$WORK/calls" 2>/dev/null | tr '\n' ' ')"
fi

# 3. --apply --wait：apply 之后对三个 deployment 等 rollout。
: > "$WORK/calls"
out="$(run --repo "$FIX" --to "$old_rev" --apply --wait)"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(grep -c '^apply ' "$WORK/calls")" -eq 1 ] \
   && [ "$(grep -c 'rollout status deployment/' "$WORK/calls")" -eq 3 ]; then
    ok "--apply --wait 在 apply 之后逐个等 rollout（3 个 deployment）"
else
    bad "--wait 那一趟不对（rc=$rc）：$(cat "$WORK/calls" 2>/dev/null | tr '\n' ' ')"
fi

# 4. rollout 起不来 ⇒ 非零，并点名是 rollout 那一阶段。
: > "$WORK/calls"
out="$(KC_ROLLOUT_RC=1 run --repo "$FIX" --to "$old_rev" --apply --wait)"
rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q 'stage=rollout'; then
    ok "rollout 不绿时非零退出，并点名 stage=rollout"
else
    bad "rollout 失败那一趟没有判红（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 5. 往前/旁支：不是 HEAD 的祖先 ⇒ 拒绝，一条 kubectl 都不执行。
: > "$WORK/calls"
out="$(run --repo "$FIX" --to "$side_rev" --apply)"
rc=$?
if [ "$rc" -eq 2 ] && [ ! -s "$WORK/calls" ] && printf '%s' "$out" | grep -q '不是 HEAD 的祖先'; then
    ok "旁支那一版被拒（rc=2），集群没被碰"
else
    bad "旁支那一版没被拒（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 6. 不存在的版本 ⇒ 拒绝，并说「不认识」而不是「没有清单」。
out="$(run --repo "$FIX" --to deadbeefdeadbeef)"
rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '不认识这一版'; then
    ok "不存在的版本被拒，判词是「不认识这一版」"
else
    bad "不存在的版本没被拒（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 7. 那一版没有清单目录 ⇒ 拒绝。
out="$(run --repo "$FIX" --to "$empty_rev" --apply)"
rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '没有 deploy/rendered/k3s-single'; then
    ok "没有清单目录的那一版被拒"
else
    bad "没有清单目录的那一版没被拒（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 8. 目录在但是空的（一份 yaml 都没有）⇒ 拒绝：apply 空目录会退 0。
git -C "$FIX" checkout -q -b emptydir "$old_rev"
git -C "$FIX" rm -q deploy/rendered/k3s-single/10-configmap.yaml
mkdir -p "$FIX/deploy/rendered/k3s-single"
printf 'not yaml\n' > "$FIX/deploy/rendered/k3s-single/README.txt"
git -C "$FIX" add -A && git -C "$FIX" commit -qm "empty rendered"
empty_yaml_rev="$(git -C "$FIX" rev-parse HEAD)"
: > "$WORK/calls"
out="$(run --repo "$FIX" --to "$empty_yaml_rev" --apply)"
rc=$?
if [ "$rc" -eq 2 ] && [ ! -s "$WORK/calls" ] && printf '%s' "$out" | grep -q '一份 yaml 都没有'; then
    ok "清单目录里没有 yaml 的一版被拒，集群没被碰"
else
    bad "空清单目录没被拒（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi
git -C "$FIX" checkout -q master 2>/dev/null || git -C "$FIX" checkout -q main

# 9. 不认识的 profile ⇒ 拒绝（闭集之外不猜）。
out="$(run --repo "$FIX" --to "$old_rev" --profile k8s-weird)"
rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '不认识的 profile'; then
    ok "profile 闭集之外被拒"
else
    bad "不认识的 profile 没被拒（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 10. 不传 --to ⇒ 用法错（不是在当前版本上瞎滚）。
out="$(run --repo "$FIX")"
rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '缺 --to'; then
    ok "没有 --to 时是用法错，不是「回滚到当前」"
else
    bad "没有 --to 那一趟不对（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

printf '\nrollback.test: %s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
