#!/usr/bin/env bash
# 换版脚本「这一趟会滚谁」的判据来自交付树，不是脚本里的一份名单。
#
# 为什么要有这一格：镜像不只有四个 Deployment 在用——宿主账本 / 进程普查 /
# 声明仓是 DaemonSet，注册仓的卷走查是边车。名单里没有它们时，换版不会滚它们，
# 而脚本最后那句「均运行新镜像」照样成立：它们的「已更新」与「根本没查」在读数
# 上分不开，旧二进制于是一直跑下去。所以这里判的不是「名单对不对」，是「名单还
# 在不在」——脚本一旦退回写死几个名字，--list-workloads 就读不出清单里新增的
# 工作负载，这一格先红。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
tool="${repo}/deploy/k3s/swap-image.sh"

pass=0
fail=0
ok()  { printf 'ok    %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL  %s\n' "$1" >&2; fail=$((fail + 1)); }

[ -f "$tool" ] || { bad "换版脚本不在树上：$tool"; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

run() { bash "$1" --list-workloads 2>&1; }

# 1. 真树：DaemonSet 与边车都在集合里，第三方镜像的工作负载不在。
out="$(run "$tool")"
rc=$?
if [ "$rc" -eq 0 ] \
   && printf '%s\n' "$out" | grep -q '^DaemonSet	cogneva-claim-store	claim-store$' \
   && printf '%s\n' "$out" | grep -q '^Deployment	cogneva	cogneva$' \
   && printf '%s\n' "$out" | grep -q 'volume-walker'; then
    ok "真树里读出了 DaemonSet 与边车，不只是几个 Deployment"
else
    bad "--list-workloads 没读全（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi
if printf '%s\n' "$out" | grep -q 'postgres'; then
    bad "第三方镜像的工作负载也进了滚动集合：postgres"
else
    ok "只收跑着本项目镜像的容器，postgres 这类第三方镜像不进集合"
fi
# redis 只有 initContainer 用本镜像：它跟着所属 StatefulSet 一起换，单独 set image
# 会为一个不常驻的进程重启数据存储。
if printf '%s\n' "$out" | grep -q 'aof-repair'; then
    bad "initContainer 进了常驻滚动集合（aof-repair）：会为一次性的修复进程重启 redis"
else
    ok "initContainer 不进常驻集合（redis 的 aof-repair 不在里面）"
fi

# 2. 夹具树：清单里新加一个 DaemonSet，它必须自己出现在集合里。
FIX="$WORK/fixture"
mkdir -p "$FIX/deploy/k3s"
cp "$tool" "$FIX/deploy/k3s/swap-image.sh"
cat > "$FIX/deploy/k3s/10-extra.yaml" <<'EOF'
kind: DaemonSet
metadata:
  name: cogneva-branch-watcher
spec:
  template:
    spec:
      containers:
        - name: branch-watcher
          image: localhost:30500/cogneva:local
EOF
# 同一个文件里放三种不该进来的：第三方镜像、CronJob（每次起 Pod 自己拉新签）、
# 只出现在 initContainers 里的容器。
cat > "$FIX/deploy/k3s/20-not-rolled.yaml" <<'EOF'
kind: Deployment
metadata:
  name: postgres
spec:
  template:
    spec:
      containers:
        - name: postgres
          image: postgres:16
---
kind: CronJob
metadata:
  name: cogneva-backup
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: localhost:30500/cogneva:local
---
kind: StatefulSet
metadata:
  name: redis
spec:
  template:
    spec:
      initContainers:
        - name: aof-repair
          image: localhost:30500/cogneva:local
      containers:
        - name: redis
          image: redis:7
EOF

out="$(run "$FIX/deploy/k3s/swap-image.sh")"
rc=$?
if [ "$rc" -eq 0 ] \
   && printf '%s\n' "$out" | grep -q '^DaemonSet	cogneva-branch-watcher	branch-watcher$' \
   && [ "$(printf '%s\n' "$out" | wc -l)" -eq 1 ]; then
    ok "清单里新增的工作负载自己进了集合，另外三种形状都没进来"
else
    bad "夹具树那一趟不对（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

# 3. 一棵读不出任何工作负载的树：必须是「判不了」而不是「没有要滚的」。
mkdir -p "$WORK/empty/deploy/k3s"
cp "$tool" "$WORK/empty/deploy/k3s/swap-image.sh"
printf 'kind: ConfigMap\nmetadata:\n  name: nothing\n' \
    > "$WORK/empty/deploy/k3s/10-configmap.yaml"
out="$(run "$WORK/empty/deploy/k3s/swap-image.sh")"
rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q '判不了该滚谁'; then
    ok "空树是「判不了该滚谁」，不是一次静默的空滚动"
else
    bad "空树没有判不了（rc=$rc）：$(printf '%s' "$out" | tr '\n' ' ')"
fi

printf '\nswap-image-workloads.test: %s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
