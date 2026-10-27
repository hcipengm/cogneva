#!/usr/bin/env bash
# Put a cluster back on the manifests of an earlier revision (apply delivery).
#
# Why this exists: the zero-helm delivery path (bootstrap's own cluster, a bare
# machine, an air-gapped host) has an install command and an upgrade command —
# `kubectl apply -f deploy/rendered/<profile>/` — and nothing that goes the
# other way. A bad revision therefore has no stated undo: the operator either
# remembers the earlier commit by hand or rebuilds. Neither is a path anyone
# else can follow at 3am.
#
# The unit of a rollback here is the rendered directory at a revision, because
# that is exactly what the upgrade command consumed. Re-applying it returns the
# cluster to the topology that revision carried; it does not touch data
# (PVCs, database contents), and it does not revert the repository.
#
# Default is a plan: it prints the command it would run and exits 0 without
# touching the cluster. `--apply` runs it. A rollback is a write against a
# running system, so the read-only outcome has to be the one you get by
# accident.
#
# Refusals (exit 2) — each is a case where the honest answer is "this is not a
# rollback", not "apply anyway":
#   - no --to, or a revision that is not in this repository;
#   - a revision that is not an ancestor of HEAD: going *forward* or sideways
#     is a change to the platform, and changes go through review, not through
#     this script;
#   - a revision with no deploy/rendered/<profile>/ — there is nothing there to
#     put back, and applying an empty directory would report success; this is
#     also how an unknown profile is refused, since the profiles that exist are
#     the directories under deploy/rendered/ in that revision, not a list kept
#     here;
#   - a profile name outside [a-z0-9-] — it is pasted into a path;
#   - --wait against a tree that declares no Deployment: there is nothing to
#     wait for, and reporting ready would be a claim about nothing. The set of
#     Deployments comes from that revision's own manifests, not from a list
#     kept here — a renamed or added workload has to be waited for, and a list
#     would either wait on a name that no longer exists or skip the new one.
#
# Usage:
#   bash deploy/scripts/rollback.sh --to <rev> [--profile k3s-single] [--apply] [--wait]
#   bash deploy/scripts/rollback.sh --to <rev> --repo /path/to/checkout
#
# Environment:
#   COGNEVA_KUBECTL  the kubectl to use (default: kubectl)
#   COGNEVA_ROLLBACK_WAIT_SECS  timeout for each rollout status (default: 300)
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="${COGNEVA_REPO:-$(cd "$script_dir/../.." && pwd)}"
kubectl="${COGNEVA_KUBECTL:-kubectl}"
profile="k3s-single"
rev=""
apply=0
wait=0
namespace="cogneva"
wait_secs="${COGNEVA_ROLLBACK_WAIT_SECS:-300}"

usage() {
    printf 'usage: bash %s --to <rev> [--profile <名字>] [--apply] [--wait] [--repo <path>] [--namespace <ns>]\n' "${BASH_SOURCE[0]}" >&2
    printf '       <名字> 是那一版 deploy/rendered/ 下的一个目录（默认 k3s-single）\n' >&2
}

while [ $# -gt 0 ]; do
    case "$1" in
        --to)      rev="${2:-}"; shift 2 ;;
        --profile) profile="${2:-}"; shift 2 ;;
        --repo)    repo="${2:-}"; shift 2 ;;
        --namespace) namespace="${2:-}"; shift 2 ;;
        --apply)   apply=1; shift ;;
        --wait)    wait=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done

# profile 的闭集不在这里列：闭集是目标那一版的 `deploy/rendered/` 下真实存在的目录，
# 下面那条 ls-tree 判的就是它。写死几个名字，新增的 profile 回滚时会被拒绝，理由还
# 说成「不认识」——而那一版的树明明在。这里只判名字的形状，因为它要拼进路径。
case "$profile" in
    ""|*[!a-z0-9-]*|-*)
        printf 'rollback: profile 名字只认 [a-z0-9-] 且不以 - 开头（它要拼进 deploy/rendered/<名字>/）：%s\n' "$profile" >&2
        exit 2 ;;
esac

if [ -z "$rev" ]; then
    printf 'rollback: 缺 --to <rev>。不知道回到哪一版就不叫回滚。\n' >&2
    usage
    exit 2
fi

if ! git -C "$repo" rev-parse --git-dir >/dev/null 2>&1; then
    printf 'rollback: %s 不是 git 仓库，读不出任何一版的清单\n' "$repo" >&2
    exit 2
fi

# 名字先解析成提交，再问祖先关系：`--to v0.5.8` 与 `--to <sha>` 是同一条路。
if ! full_rev="$(git -C "$repo" rev-parse --verify --quiet "${rev}^{commit}" 2>/dev/null)"; then
    printf 'rollback: 不认识这一版「%s」——本仓库里没有这个提交\n' "$rev" >&2
    exit 2
fi

# 只在「往回」时叫回滚。往前或旁支都是对平台的变更，该走评审而不是这条脚本；
# 放行它们会让这条路径变成绕过评审的部署入口。
if ! git -C "$repo" merge-base --is-ancestor "$full_rev" HEAD 2>/dev/null; then
    printf 'rollback: %s 不是 HEAD 的祖先——那是往前或旁支，走正常变更，不是回滚\n' "${full_rev:0:12}" >&2
    exit 2
fi

rendered_rel="deploy/rendered/$profile"
if ! git -C "$repo" ls-tree -d --name-only "$full_rev" -- "$rendered_rel" 2>/dev/null | grep -qx "$rendered_rel"; then
    printf 'rollback: %s 里没有 %s/——那一版没有可回滚的清单\n' "${full_rev:0:12}" "$rendered_rel" >&2
    exit 2
fi

# 空目录不是一版清单：apply 一个空目录会退出 0，读起来像回滚成功。
file_count="$(git -C "$repo" ls-tree -r --name-only "$full_rev" -- "$rendered_rel" | grep -c '\.yaml$' || true)"
if [ "${file_count:-0}" -eq 0 ]; then
    printf 'rollback: %s 的 %s/ 里一份 yaml 都没有——不把空目录当回滚\n' "${full_rev:0:12}" "$rendered_rel" >&2
    exit 2
fi

current_rev="$(git -C "$repo" rev-parse --short=12 HEAD 2>/dev/null || printf 'unknown')"
printf 'rollback_plan repo=%s\n' "$repo"
printf 'rollback_plan from=%s\n' "$current_rev"
printf 'rollback_plan to=%s\n' "${full_rev:0:12}"
printf 'rollback_plan profile=%s files=%s\n' "$profile" "$file_count"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
if ! git -C "$repo" archive "$full_rev" "$rendered_rel" | tar -x -C "$work"; then
    printf 'rollback: 取不出 %s 的清单（git archive 失败）\n' "${full_rev:0:12}" >&2
    exit 2
fi
manifest_dir="$work/$rendered_rel"

# 要等哪些 Deployment 从**这一版清单**里读，不在这里列名字。写死的那份名单在工作
# 负载改名或多出第四个时，`--wait` 会去等一个不存在的东西（rollout status 对它报错，
# 于是这一趟报回滚失败，而集群其实已经退回去了），或者漏掉新增的那一个，在它还没
# 起来时就报 ready。判在 apply 之前：判不了的那一版不该先动集群再说不算数。
deployments=()
if [ "$wait" -eq 1 ]; then
    while read -r name; do
        [ -n "$name" ] && deployments+=("$name")
    done < <(awk '
        /^kind: Deployment$/ { want = 1; next }
        want && /^  name: / { print $2; want = 0 }
    ' "$manifest_dir"/*.yaml 2>/dev/null)
    if [ "${#deployments[@]}" -eq 0 ]; then
        printf 'rollback: %s 的 %s/ 里一个 Deployment 都没有——等不了 rollout，这不等于就绪\n' \
            "${full_rev:0:12}" "$rendered_rel" >&2
        exit 2
    fi
fi

printf 'rollback_cmd %s apply -f %s/\n' "$kubectl" "$rendered_rel"

if [ "$apply" -ne 1 ]; then
    printf 'rollback_mode=plan（未改动集群；加 --apply 执行）\n'
    exit 0
fi

if ! $kubectl apply -f "$manifest_dir/"; then
    printf 'rollback_failed stage=apply to=%s\n' "${full_rev:0:12}" >&2
    exit 1
fi

if [ "$wait" -eq 1 ]; then
    failed=""
    for d in "${deployments[@]}"; do
        if ! $kubectl -n "$namespace" rollout status "deployment/$d" --timeout="${wait_secs}s" >/dev/null 2>&1; then
            failed="$failed $d"
        fi
    done
    if [ -n "$failed" ]; then
        printf 'rollback_failed stage=rollout to=%s deployments=%s\n' "${full_rev:0:12}" "${failed# }" >&2
        exit 1
    fi
    printf 'rollback_rollout=ready deployments=%s\n' "${#deployments[@]}"
fi

printf 'rollback_applied to=%s profile=%s\n' "${full_rev:0:12}" "$profile"
