#!/usr/bin/env bash
# `check-manifest-duplicate-keys.py` 的离线测试：夹具是一棵小树，不是本仓。
#
# 判的是**「同一个键在同一份清单里出现了两次」**——两笔改动各自都对，合起来是一份
# server-side apply 会拒收的清单。2026-11-18 现场：`COGNEVA_MEMORY_API_BASE` 在网关的
# env 里出现两次，整轮滚动失败并回滚到旧镜像。
#
#   1. 干净清单 ⇒ 绿。
#   2. 同一个 env 列表里两次同名 ⇒ 红，并点名键与两行行号。
#   3. 不同层里的同名不算重复（containers 里的 env 与容器自己的 name）。
#   4. `volumeMounts` 里同名 ⇒ 红（这一族不止 env 一种列表）。
#   5. 目录不存在 ⇒ 绿（没有清单不是坏清单）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
gate="$root/deploy/scripts/check-manifest-duplicate-keys.py"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)" || exit 1
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else
  printf 'FAIL- %s（期望退出码 %s，实得 %s）\n' "$1" "$2" "$3" >&2; fail=$((fail + 1)); fi; }
ck_has() { if printf '%s\n' "$3" | grep -qF -- "$2"; then ok "$1"; else
  printf 'FAIL- %s\n        期望含：%s\n' "$1" "$2" >&2
  printf '%s\n' "$3" | sed 's/^/        实得: /' | head -20 >&2; fail=$((fail + 1)); fi; }

# 门按仓根找自己，所以夹具必须放在一棵形如 <root>/deploy/scripts 的树里。
tree="$tmp/tree"
mkdir -p "$tree/deploy/scripts" "$tree/deploy/k3s"
cp "$gate" "$tree/deploy/scripts/check-manifest-duplicate-keys.py"
run() { COGNEVA_ROOT="$tree" python3 "$tree/deploy/scripts/check-manifest-duplicate-keys.py" 2>&1; }

clean="$tree/deploy/k3s/clean.yaml"
cat > "$clean" <<'YAML'
spec:
  template:
    spec:
      containers:
        - name: gateway
          env:
            - name: A
              value: "1"
            - name: B
              value: "2"
          volumeMounts:
            - name: x
              mountPath: /a
            - name: y
              mountPath: /b
YAML
out=$(run); rc=$?
ck_rc "干净清单：绿" 0 "$rc"
ck_has "干净清单：报的是零重复" "没有重复键" "$out"

dupe="$tree/deploy/k3s/dupe.yaml"
cat > "$dupe" <<'YAML'
spec:
  template:
    spec:
      containers:
        - name: gateway
          env:
            - name: COGNEVA_MEMORY_API_BASE
              value: "http://cogneva:8085"
            - name: COGNEVA_GITHUB_TOKEN
              valueFrom:
                secretKeyRef:
                  name: cogneva-secrets-oob
                  key: github-token
            - name: COGNEVA_MEMORY_API_BASE
              value: "http://cogneva:8085"
YAML
out=$(run); rc=$?
ck_rc "同一个 env 列表里两次同名：红" 1 "$rc"
ck_has "点名重复的键" "COGNEVA_MEMORY_API_BASE" "$out"
ck_has "说的是 apply 会拒收" "duplicate entries" "$out"

mounts="$tree/deploy/k3s/mounts.yaml"
cat > "$mounts" <<'YAML'
spec:
  template:
    spec:
      containers:
        - name: c
          volumeMounts:
            - name: shared
              mountPath: /a
            - name: shared
              mountPath: /b
YAML
rm -f "$clean" "$dupe"
out=$(run); rc=$?
ck_rc "volumeMounts 里同名：红" 1 "$rc"
ck_has "点名重复的挂载" "shared" "$out"
rm -f "$mounts"
out=$(run); rc=$?
ck_rc "没有清单：绿" 0 "$rc"

printf '\nmanifest-duplicate-keys.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
