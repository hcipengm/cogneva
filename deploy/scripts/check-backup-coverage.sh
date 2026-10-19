#!/usr/bin/env bash
# 备份覆盖门：每个在工作负载里挂到应用数据目录的 PVC，都必须同时出现在备份
# CronJob 的挂载面里。只比「有没有这个 claim」，不比工作负载名——同一块卷可以
# 被多个进程挂，读者与写者都算；漏一个，换机恢复时那个数据根整个不在包里。
#
# 为什么按挂载点反查 claim，而不是维护一份 dataRoots 白名单：白名单要和渲染物
# 各写一遍，漏写的那一项恰好是门看不见的那一项。渲染物是 bootstrap 真正 apply
# 的东西，读它才是读安装面。
#
# 它还判第二件事：每个数据根的一级子目录都被分类过（authoritative / discardable）。
# 覆盖门只问「这个 claim 进没进备份面」，而 claim 里少一棵子树它看不见——
# `warehouse/` 就是这样在没有任何清单提过它的情况下出现的。
#
# 用法：
#   bash deploy/scripts/check-backup-coverage.sh            # 人读，差异即失败
#   bash deploy/scripts/check-backup-coverage.sh --json     # 机器读（顶层有 .unclassified）
#   bash deploy/scripts/check-backup-coverage.sh --profile k3s-single
#   bash deploy/scripts/check-backup-coverage.sh --live     # 把本机数据根的实盘子目录也算进普查
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


SUBDIR_LEDGER = "deploy/data-root-subdirs.tsv"
CLASSES = ("authoritative", "discardable")


def subdir_ledger():
    """数据根一级子目录的分类表。格式问题本身就是红的：读不出来的一行等于没分类。"""
    rows, problems = {}, []
    if not os.path.exists(SUBDIR_LEDGER):
        return rows, [f"{SUBDIR_LEDGER} 不在树里：子目录分类无从判"]
    with open(SUBDIR_LEDGER, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            body = line.rstrip("\n")
            if not body or body.startswith("#") or body.startswith("subdir\t"):
                continue
            cols = body.split("\t")
            if len(cols) != 6:
                problems.append(f"{SUBDIR_LEDGER}:{lineno} 是 {len(cols)} 列，不是 6 列")
                continue
            name, roots, cls, what, why, evidence = cols
            if cls not in CLASSES:
                problems.append(f"{SUBDIR_LEDGER}:{lineno} 的 class={cls!r} 不在 {CLASSES}")
            if not why.strip():
                problems.append(f"{SUBDIR_LEDGER}:{lineno}（{name}）没有 why：只写名字的条目等于把问题留给下一个人")
            if not os.path.exists(evidence):
                problems.append(f"{SUBDIR_LEDGER}:{lineno}（{name}）的 evidence 指向 {evidence}，树上没有")
            rows[name] = {"roots": roots.split(","), "class": cls, "what": what,
                          "why": why, "evidence": evidence}
    return rows, problems


def paths_under(text, mount):
    """文本里所有 `<数据根>/<一级段>` 的一级段（配置值、渲染清单的 hostPath 都走这条）。"""
    pattern = re.compile(r"""[\s:"'=]""" + re.escape(mount) + r"/([A-Za-z0-9_-]+)")
    return set(pattern.findall(text))


def rust_segments(mount):
    """代码里由 data_dir / COGNEVA_DATA_DIR 拼出来的一级路径段。

    两种写法都要认：`.join("x")` 与 `format!("{}/x", …data_dir…)`。接收者不一定是
    字面上的 `data_dir`（`state.data_dir`、`cfg.data_dir`、`data_dir()` 都算），所以
    判据是「这一行提到 data_dir」而不是「接收者名字相等」。`git-workdir` 那处把
    `COGNEVA_DATA_DIR` 读进局部变量、下一行才 join，因此看两行的窗口。
    """
    # 接收者必须本身就是数据根：`staging.join("objects")` 也在提 data_dir 的行上
    # （那条语句的另一个参数是 data_dir），但它拼的是包里的目录，不是数据根下的。
    join_pat = re.compile(r'(?:[\w.()]*[Dd]ata_dir[\w.()]*)\.join\("([A-Za-z0-9_-]+)"\)')
    # 例外：先把 `COGNEVA_DATA_DIR` 读进局部变量、下一行才 join 的写法。上一行提到
    # DATA_DIR 时，这一行的 join 就是在它下面拼路径——这一行本身看不出接收者是谁。
    local_join = re.compile(r'\.join\("([A-Za-z0-9_-]+)"\)')
    found = {}
    for path in glob.glob("crates/**/*.rs", recursive=True):
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        # 测试块里的 data_dir 是临时目录：`..data`、`jwt-secret` 这类名字会把普查面
        # 变成一幅假的现场。
        cut = text.find("#[cfg(test)]")
        if cut >= 0:
            text = text[:cut]
        # 往回看四行而不是一行：`std::env::var("COGNEVA_DATA_DIR")` 后面常跟
        # `.ok().filter(…).unwrap_or_else(…)` 的链，join 落在第三、四行上。
        recent = [""] * 4
        for line in text.splitlines():
            mentions = ("data_dir" in line or "DATA_DIR" in line)
            window = mentions or any("data_dir" in l or "DATA_DIR" in l for l in recent)
            if window:
                for name in join_pat.findall(line):
                    found.setdefault(name, path)
                for name in re.findall(r'"\{[^"]*\}/([A-Za-z0-9_-]+)', line):
                    found.setdefault(name, path)
            if any("DATA_DIR" in l for l in recent):
                for name in local_join.findall(line):
                    found.setdefault(name, path)
            recent = recent[1:] + [line]
    return found


def census_subdirs(mount, live):
    """普查面：代码、配置、渲染物、本机实盘，四条来源的并集。

    并集而不是任何一条单独成面：`git-remote` 只在渲染清单里出现，`warehouse` 只在
    代码里出现，而「代码里新加的子目录」和「部署里新挂的子目录」是两种各自独立
    会漏的分类对象。
    """
    sources = {}
    for name, path in rust_segments(mount).items():
        sources.setdefault(name, path)
    config = "deploy/helm/cogneva/files/cogneva.json"
    if os.path.exists(config):
        with open(config, encoding="utf-8") as fh:
            for name in paths_under(fh.read(), mount):
                sources.setdefault(name, config)
    for path in glob.glob("deploy/rendered/*/*.yaml"):
        with open(path, encoding="utf-8", errors="replace") as fh:
            for name in paths_under(fh.read(), mount):
                sources.setdefault(name, path)
    if live and os.path.isdir(mount):
        for name in sorted(os.listdir(mount)):
            if os.path.isdir(os.path.join(mount, name)):
                sources.setdefault(name, f"live:{mount}")
    return sources


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="机器可读输出")
    parser.add_argument("--profile", help="只查一个 profile")
    parser.add_argument("--live", action="store_true",
                        help="把本机数据根上真实存在的一级子目录也算进普查面")
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
        data_roots = sorted({m["claim"] for w in workloads for m in w["mounts"]})
        backup = [w for w in workloads if w["name"] == "cogneva-backup"]
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

    ledger, ledger_problems = subdir_ledger()
    census = census_subdirs(mount_path, args.live)
    unclassified = sorted(set(census) - set(ledger))
    stale = sorted(set(ledger) - set(census))
    if ledger_problems or unclassified:
        failed = True
    out = {
        "profiles": result,
        "data_root_subdirs": {
            "census": census,
            "classified": len(ledger),
            "unclassified": unclassified,
            "stale": stale,
            "problems": ledger_problems,
        },
        # 规格里那条判据读的是顶层这个键：分类面不通过时恢复端没有一条能判红的
        # 读数，它和「每个 claim 都进了备份面」是同一件事的两半。
        "unclassified": unclassified,
    }

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"数据根子目录：普查 {len(census)} 个，已分类 {len(ledger)} 条")
        for name in unclassified:
            print(f"  红: {name}/ 出现在 {census[name]}，但不在 {SUBDIR_LEDGER} 里（未分类）")
        for problem in ledger_problems:
            print(f"  红: {problem}")
        if not unclassified and not ledger_problems:
            print("  绿")
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
