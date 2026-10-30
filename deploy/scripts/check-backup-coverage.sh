#!/usr/bin/env bash
# 备份覆盖门：每个在工作负载里挂到应用数据目录的 PVC，都必须同时出现在备份
# CronJob 的挂载面里。只比「有没有这个 claim」，不比工作负载名——同一块卷可以
# 被多个进程挂，读者与写者都算；漏一个，换机恢复时那个数据根整个不在包里。
#
# 为什么按挂载点反查 claim，而不是维护一份 dataRoots 白名单：白名单要和渲染物
# 各写一遍，漏写的那一项恰好是门看不见的那一项。渲染物是 bootstrap 真正 apply
# 的东西，读它才是读安装面。
#
# 「每个数据根的一级子目录都被分类过」那一条**不在这里**：分类的判词长在备份逻辑
# 自己旁边（`crates/cogneva/src/backup.rs` 的 `DATA_ROOT_SUBDIRS`），普查面必须跟着
# 代码走，判据在 `crates/cogneva/tests/data_root_subdirs.rs`。这个脚本只管卷。
#
# 用法：
#   bash deploy/scripts/check-backup-coverage.sh            # 人读，差异即失败
#   bash deploy/scripts/check-backup-coverage.sh --json     # 机器读
#   bash deploy/scripts/check-backup-coverage.sh --profile k3s-single
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# 第一个不带 `-` 的参数是仓库根：测试在真树的一份副本上跑这个门，改的是副本。
ARGS=()
for arg in "$@"; do
    case "$arg" in
        -*) ARGS+=("$arg") ;;
        *) ROOT="$arg" ;;
    esac
done
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - ${ARGS[@]+"${ARGS[@]}"} <<'PYEOF'
import argparse
import glob
import json
import os
import re
import sys

try:
    import yaml
except ImportError:
    print("缺少依赖：python3-yaml（CI 用 apt-get install python3-yaml）", file=sys.stderr)
    sys.exit(2)

DEFAULT_DATA_DIR = "/var/lib/cogneva-data"


def data_dir() -> str:
    """应用的 data_dir 以仓库里的配置为源；读不到才用默认值。"""
    path = "deploy/helm/cogneva/files/cogneva.json"
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        value = doc.get("app", {}).get("data_dir")
        if isinstance(value, str) and value:
            return value.rstrip("/")
    except (OSError, json.JSONDecodeError):
        pass
    return DEFAULT_DATA_DIR


def pod_spec(doc):
    kind = doc.get("kind")
    if kind == "CronJob":
        return doc.get("spec", {}).get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec", {})
    if kind == "Job":
        return doc.get("spec", {}).get("template", {}).get("spec", {})
    if kind == "Pod":
        return doc.get("spec", {})
    return doc.get("spec", {}).get("template", {}).get("spec", {})


def claims_in(spec):
    """Pod 模板里出现的全部 PVC claim，不管挂在哪个路径。

    备份 Job 读一个数据根时不要求路径与工作负载相同：进化卷在进化 Pod 里挂在
    `/var/lib/cogneva-data`，在备份 Pod 里必须换个路径才能与主卷共存。判「这个根
    进没进备份面」问的是 claim 在不在 Pod 的卷声明里，不是路径字面相等。
    """
    out = []
    for volume in spec.get("volumes", []) or []:
        claim = (volume.get("persistentVolumeClaim") or {}).get("claimName")
        if claim:
            out.append(claim)
    return out


def mounts_at(spec, mount_path):
    """把 Pod 模板里挂到 mount_path 的 claim 点出来，容器与 init 容器都算。"""
    volumes = {}
    for volume in spec.get("volumes", []) or []:
        claim = (volume.get("persistentVolumeClaim") or {}).get("claimName")
        if claim:
            volumes[volume.get("name")] = claim
    out = []
    for key in ("initContainers", "containers"):
        for container in spec.get(key, []) or []:
            for mount in container.get("volumeMounts", []) or []:
                path = (mount.get("mountPath") or "").rstrip("/")
                if path != mount_path:
                    continue
                claim = volumes.get(mount.get("name"))
                if not claim:
                    continue
                out.append(
                    {
                        "container": container.get("name"),
                        "claim": claim,
                        "read_only": bool(mount.get("readOnly", False)),
                    }
                )
    return out


def load_profile(profile_dir, mount_path):
    workloads = []
    for path in sorted(glob.glob(os.path.join(profile_dir, "*.yaml"))):
        with open(path, encoding="utf-8") as f:
            for doc in yaml.safe_load_all(f):
                if not doc or not doc.get("kind"):
                    continue
                name = (doc.get("metadata") or {}).get("name", "")
                spec = pod_spec(doc)
                mounts = mounts_at(spec, mount_path)
                if mounts:
                    workloads.append(
                        {
                            "kind": doc["kind"],
                            "name": name,
                            "mounts": mounts,
                            "claims": claims_in(spec),
                        }
                    )
    return workloads


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="机器可读输出")
    parser.add_argument("--profile", help="只查一个 profile")
    args = parser.parse_args()

    mount_path = data_dir()
    base = "deploy/rendered"
    if not os.path.isdir(base):
        print(f"找不到 {base}/：先跑 deploy/scripts/render-deploy.sh", file=sys.stderr)
        return 2

    profiles = sorted(
        p for p in glob.glob(os.path.join(base, "*")) if os.path.isdir(p)
    )
    if args.profile:
        profiles = [p for p in profiles if os.path.basename(p) == args.profile]
    if not profiles:
        print("没有可查的 profile", file=sys.stderr)
        return 2

    result = {}
    failed = False
    for profile_dir in profiles:
        profile = os.path.basename(profile_dir)
        workloads = load_profile(profile_dir, mount_path)
        backup = [w for w in workloads if w["name"] == "cogneva-backup"]
        # 数据根只从**别的**工作负载里取。备份 CronJob 自己也是 workload，把它算进
        # 数据根会让两侧恒等：`extra`（备份挂了没人用的卷）永远空，而 `data_roots`
        # 永远非空——「一个工作负载都没挂数据目录」也就永远读不成红。备份门拿被备份
        # 方当自己的判据，是自己跟自己比。
        data_roots = sorted(
            {m["claim"] for w in workloads if w["name"] != "cogneva-backup" for m in w["mounts"]}
        )
        # 覆盖按 claim 判，不按挂载路径判：同一个 claim 在备份 Pod 里可以换个
        # 路径（进化卷与主卷同路径会撞）。路径相同只在「备份 Pod 把数据目录
        # 挂到了别的 claim」时才有意义，那一项仍由 extra 抓。
        backup_claims = sorted({c for w in backup for c in w["claims"]})
        backup_roots = sorted({m["claim"] for w in backup for m in w["mounts"]})
        missing = sorted(set(data_roots) - set(backup_claims))
        extra = sorted(set(backup_roots) - set(data_roots))
        # 备份 CronJob 自己没挂到任何数据根，等于这一份 profile 完全没有覆盖：
        # data_roots 与 backup_claims 都空时也不能读成绿。
        no_backup_mount = not backup_claims
        if missing or extra or no_backup_mount or not data_roots:
            failed = True
        result[profile] = {
            "data_mount": mount_path,
            "data_roots": data_roots,
            "backup_roots": backup_roots,
            "backup_claims": backup_claims,
            "missing": missing,
            "extra": extra,
            "no_backup_mount": no_backup_mount,
            "workloads": workloads,
        }

    if args.json:
        print(json.dumps({"profiles": result}, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        for profile, row in result.items():
            print(f"[{profile}] data_dir={row['data_mount']}")
            print(f"  数据根: {', '.join(row['data_roots']) or '(空)'}")
            print(f"  备份挂载: {', '.join(row['backup_claims']) or '(空)'}")
            if row["no_backup_mount"]:
                print("  红: 备份 CronJob 没有挂到数据目录")
            for claim in row["missing"]:
                print(f"  红: {claim} 挂在工作负载上，但不在备份 Job 的挂载面里")
            for claim in row["extra"]:
                print(f"  红: {claim} 只在备份 Job 里挂，没有工作负载使用它")
            if not row["data_roots"]:
                print("  红: 没有任何工作负载挂到数据目录（空不是绿）")
            if not (row["missing"] or row["extra"] or row["no_backup_mount"] or not row["data_roots"]):
                print("  绿")
    return 1 if failed else 0


sys.exit(main())
PYEOF
