#!/usr/bin/env bash
# 空白机无损重装：公开仓库 + 一个备份包 + 属主手上的字节。
#
# 这条腿走的是「宿主上有包」：包由属主带来（或从宿主上的异地副本取回），
# 不向网关要任何一次性地址、不用任何长期只读凭证。理由不是偏好——空白机上还
# 没有网关，等它签发的地址就是死循环；而为了省一次人工在宿主上常驻长期凭证，
# 等于把整库的读取权留给任何拿到宿主的人。
#
# 步骤的顺序与依赖声明在 `deploy/restore/blank-machine.steps.tsv`，
# `check-restore-order.sh` 判它闭合。脚本与那张表双向对齐：每个 `run_step`
# 都要在表里有行。
#
# 可重入：每一步做完往回执目录写一条记录，重跑从下一段继续。回执里的包哈希与
# 这次给的包不同就停——「这份回执属于另一个包」不是可以接着跑的状态。
#
# 今天包里是明文 tar.zst。加密副本（restic）那条腿落地后，解包口令在这里
# 交互输入、只留在进程环境里，不进回执、不落盘。
#
# 用法：deploy/scripts/restore-blank-machine.sh <cogneva-backup-*.tar.zst> [命名空间]
# 回执目录：$COGNEVA_RESTORE_RECEIPT（默认 ./restore-receipt）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PKG="${1:-}"
NS="${2:-cogneva}"
RECEIPT="${COGNEVA_RESTORE_RECEIPT:-$PWD/restore-receipt}"
# 恢复那一段的执行者。默认是同一棵树里的恢复脚本；换一份实现（例如这张空白机
# 取自另一份检出的镜像）时可以指过去，回执里记的 Job 名仍然由这里给。
RESTORE_CMD="${COGNEVA_RESTORE_PACKAGE_CMD:-$ROOT/deploy/scripts/restore-from-package.sh}"

if [ -z "$PKG" ] || [ ! -f "$PKG" ]; then
    echo "用法: $0 <cogneva-backup-*.tar.zst> [命名空间，默认 cogneva]" >&2
    exit 2
fi
command -v kubectl >/dev/null || { echo "缺少 kubectl" >&2; exit 2; }
command -v tar >/dev/null || { echo "缺少 tar" >&2; exit 2; }
mkdir -p "$RECEIPT"

PKG_SHA="$(sha256sum "$PKG" | awk '{print $1}')"

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

read_field() {  # read_field <file> <key>
    # 文件不在是正常状态（这一遍还没走到那一步），不是错误；`grep` 找不到键
    # 也一样。回执是**可缺**的，缺它只说明「还没做」。
    [ -f "$1" ] || return 0
    grep -m1 "^$2=" "$1" | cut -d= -f2- || true
}

# 回执是给人读的 key=value，不是程序之间传的格式：出问题时属主会直接打开它。
record() {  # record <step> <reading>
    {
        echo "status=done"
        echo "at=$(now)"
        echo "package_sha256=$PKG_SHA"
        echo "reading=$2"
    } >"$RECEIPT/$1.txt"
}

step_done() { [ "$(read_field "$RECEIPT/$1.txt" status)" = "done" ]; }

# ── 每一步 ────────────────────────────────────────────────────────────────
# run_step <id> 的 id 与步骤表逐字相同，门的双向对齐读的就是这一行。

step_package() {
    local recorded
    recorded="$(read_field "$RECEIPT/package.txt" package_sha256)"
    if [ -n "$recorded" ] && [ "$recorded" != "$PKG_SHA" ]; then
        echo "回执属于另一个包（记录 $recorded，这次 $PKG_SHA）；换一个回执目录再跑" >&2
        exit 1
    fi
    if step_done package && [ "$recorded" = "$PKG_SHA" ]; then
        echo "    已记录：$PKG ($PKG_SHA)"
        return 0
    fi
    # 旧格式的包没有 artifacts 段，那是「当年没声明」，不是错；这里只读、不判。
    local expected
    expected="$(pkg_artifacts_expected)"
    record package "$PKG sha256=$PKG_SHA artifacts_expected=$expected"
    echo "    包 $PKG sha256=$PKG_SHA 产物层声明 $expected 项"
}

pkg_artifacts_expected() {
    local entry
    entry="$(tar --zstd -tf "$PKG" | grep -m1 '/manifest.json$' || true)"
    if [ -z "$entry" ]; then
        echo 0
        return 0
    fi
    tar --zstd -xOf "$PKG" "$entry" | python3 -c '
import json, sys
doc = json.load(sys.stdin)
n = 0
for root in doc.get("artifacts") or []:
    if root.get("policies_present"):
        n += len(root.get("policy_stores") or [])
    if root.get("decision_stats"):
        n += 1
print(n)
'
}

step_engine() {
    # 判据是现网的部署在不在，不是回执说过没有：集群被人拆掉后重跑，回执照样
    # 写着「装过」，而部署已经不在了。
    if kubectl -n "$NS" get deployment cogneva >/dev/null 2>&1; then
        echo "    引擎已就位（deployment/cogneva 存在），不重装"
        record engine "deployment/cogneva present"
        return 0
    fi
    local cmd="${COGNEVA_BOOTSTRAP_CMD:-$ROOT/bootstrap.sh}"
    [ -x "$cmd" ] || [ -f "$cmd" ] || {
        echo "找不到元启动入口 $cmd（可用 COGNEVA_BOOTSTRAP_CMD 指定）" >&2
        exit 1
    }
    echo "    跑元启动：$cmd"
    ( cd "$ROOT" && sh "$cmd" )
    kubectl -n "$NS" rollout status deployment/cogneva --timeout=600s
    record engine "bootstrap via $cmd"
}

step_restore() {
    if step_done restore && [ "$(read_field "$RECEIPT/restore.txt" package_sha256)" = "$PKG_SHA" ]; then
        echo "    已记录：$(read_field "$RECEIPT/restore.txt" reading)"
        return 0
    fi
    local job
    job="cogneva-restore-blank-$(date +%s)"
    COGNEVA_RESTORE_JOB_NAME="$job" bash "$RESTORE_CMD" "$PKG" "$NS"
    record restore "job=$job"
    echo "    恢复 Job 完成：$job"
}

step_verify() {
    if step_done verify; then
        echo "    已记录：$(read_field "$RECEIPT/verify.txt" reading)"
        return 0
    fi
    local expected job log restored
    expected="$(read_field "$RECEIPT/package.txt" reading | sed -n 's/.*artifacts_expected=//p')"
    job="$(read_field "$RECEIPT/restore.txt" reading | sed -n 's/^job=//p')"
    if [ -z "$job" ]; then
        echo "回执里没有恢复 Job 的名字，无法对账" >&2
        exit 1
    fi
    log="$(kubectl -n "$NS" logs "job/$job" 2>&1 || true)"
    restored="$(printf '%s\n' "$log" | grep -c 'artifact restored' || true)"
    if [ "$restored" != "$expected" ]; then
        echo "产物层读数对不上：包里声明 $expected 项，恢复 Job 报 $restored 项" >&2
        printf '%s\n' "$log" | grep -i 'artifact' >&2 || true
        exit 1
    fi
    while IFS= read -r d; do
        [ -n "$d" ] || continue
        kubectl -n "$NS" rollout status "deployment/$d" --timeout=300s >/dev/null
    done < <(kubectl -n "$NS" get deploy -o name | sed 's#deployment.apps/##')
    record verify "artifacts_restored=$restored artifacts_expected=$expected job=$job deployments=ready"
    echo "    产物层 $restored/$expected 项，部署全部就绪"
}

run_step() {
    local id="$1"
    echo "==> $id"
    "step_$id"
}

run_step package
run_step engine
run_step restore
run_step verify

echo "==> 恢复完成；回执：$RECEIPT"
