#!/usr/bin/env bash
# 判据：换版脚本对 `deploy/k3s/sandbox-executor-statefulset.yaml` 的那一步，必须先问
# 「这个 StatefulSet 在不在」——在就只 patch Pod 模板，不在才整份 apply。
#
# 为什么这一条要单独判：`spec.volumeClaimTemplates` 在 StatefulSet 上是**不可变**字段。
# chart 装出来的那份卷模板带安装来源标签（instance/managed-by），静态清单没有；整份
# apply 想改这个字段就会撞上
#   The StatefulSet "cogneva-sandbox-executor" is invalid:
#   spec.volumeClaimTemplates: Invalid value: …: field is immutable
# ——client-side 与 server-side 同一条（实测 2026-11-25，两种模式各 dry-run 一次）。
# 换版脚本是 `set -euo pipefail`，这一步失败整次换版停在这里，而它排在主部署
# 之后、工作负载滚动之前：停下来就是「主线在跑旧版、执行器与主部署停在半路」。
#
#   1. 真树里的换版脚本 ⇒ 绿（有存在性判断、有 patch 分支）。
#   2. 夹具里同样的 apply 不判存在性 ⇒ 红，并点名行号。
#   3. 夹具里判了存在性、但两支都 apply（没有 patch 分支）⇒ 红。
#   4. 夹具里整步消失 ⇒ 红（删掉这一步不能读成「判过了」）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
target="deploy/k3s/sandbox-executor-statefulset.yaml"

# 判一份脚本：打印违规行；退出码 0 = 绿。
judge() {
  local file="$1" line_idx=0 get_at=0 patch_at=0 seen=0 bad=0
  while IFS= read -r line; do
    line_idx=$((line_idx + 1))
    case "$line" in
      *get*statefulset*cogneva-sandbox-executor*) [ "$get_at" -eq 0 ] && get_at=$line_idx ;;
    esac
    case "$line" in
      *patch*statefulset*cogneva-sandbox-executor*) patch_at=$line_idx ;;
    esac
    case "$line" in
      *apply*"$target"*)
        seen=1
        # 这一行必须排在存在性判断之后，且判断与它之间已经出现过 patch 分支。
        if [ "$get_at" -eq 0 ] || [ "$patch_at" -eq 0 ] || [ "$patch_at" -gt "$line_idx" ]; then
          printf 'EXECUTOR-STS-REAPPLIED %s:%d %s\n' "$file" "$line_idx" "$line" >&2
          bad=$((bad + 1))
        fi
        ;;
    esac
  done < "$file"
  if [ "$seen" -eq 0 ]; then
    printf 'EXECUTOR-STS-NOT-STEPPED %s\n' "$file" >&2
    return 1
  fi
  if [ "$get_at" -eq 0 ]; then
    printf 'EXECUTOR-STS-NO-EXISTENCE-CHECK %s\n' "$file" >&2
    bad=$((bad + 1))
  fi
  if [ "$patch_at" -eq 0 ]; then
    printf 'EXECUTOR-STS-NO-PATCH-BRANCH %s\n' "$file" >&2
    bad=$((bad + 1))
  fi
  [ "$bad" -eq 0 ]
}

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 1. 真树
if out=$(judge "$root/deploy/k3s/swap-image.sh" 2>&1); then
  ok "真树：执行器 StatefulSet 先判存在性、存在时只 patch 模板"
else
  bad "真树：换版脚本仍会整份 apply 这份 StatefulSet"
  printf '%s\n' "$out" | sed 's/^/        /' >&2
fi

tmp="$(mktemp -d)" || exit 1
trap 'rm -rf "$tmp"' EXIT

# 2. 反面夹具：整份 apply，没有存在性判断
cat > "$tmp/unconditional.sh" <<SH
kubectl apply -f $target
SH
if judge "$tmp/unconditional.sh" >/dev/null 2>&1; then
  bad "夹具：不判存在性的 apply 被判成绿"
else
  ok "夹具：不判存在性的 apply 被判成红"
fi

# 3. 夹具：判了存在性，但两支都 apply（没有 patch 分支）
cat > "$tmp/both-apply.sh" <<SH
if kubectl -n cogneva get statefulset cogneva-sandbox-executor >/dev/null 2>&1; then
  kubectl apply -f $target
else
  kubectl apply -f $target
fi
SH
if judge "$tmp/both-apply.sh" >/dev/null 2>&1; then
  bad "夹具：两支都 apply 仍被判成绿"
else
  ok "夹具：两支都 apply 被判成红"
fi

# 4. 夹具：整步消失 ⇒ 必须红
cat > "$tmp/no-step.sh" <<'SH'
kubectl apply -f deploy/k3s/sandbox-executor-service.yaml
SH
if judge "$tmp/no-step.sh" >/dev/null 2>&1; then
  bad "夹具：整步不见了仍被判成绿"
else
  ok "夹具：整步不见了被判成红"
fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
