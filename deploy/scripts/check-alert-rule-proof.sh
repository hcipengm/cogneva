#!/usr/bin/env bash
# 托管告警规则（`deploy/k3s/observability/manifests/*prometheusrule*.yaml`）里，每一条
# 都得有一份 promtool 证明：一格它**会响**，一格它**不响**。
#
# 为什么要有这一格：`test-acceptance-census-alert-rules.sh` 能跑任何一组规则，结构门
# （subjects / join-shape / faces / derived-bounds）判的是形状，没有一处问「这条新加的
# 规则真的被跑过吗」。2026-10-28 两条规则（`CognevaBuildOutsideTheGate`、
# `CognevaBuildSliceUnbounded`）就这么落进来了——表达式对不对没人验证，而所有门全绿。
# 真正发现它们的是值守那棵树里的一条 must-green：它比对「清单里每条规则」与「夹具里
# 点到的名字」，宽限 24 小时后判红——于是第二天 19:13，那棵树的**每一笔提交**都开始
# 被两条与提交者无关的规则挡住，而挡人的那个人当时在改的是别的面。
#
# 24 小时的延迟不是宽限，是把红的时刻挪到别人头上。这条门把同一个判据放在**同仓、
# 同一笔**：新加一条规则而没带两格证明，这一笔就红，红在自己身上。
#
# 判据四条：
#   1. 清单里每条 `alert:` 都要在夹具里被点到名（`alertname:`）；
#   2. 被点到名还不算证明：至少要有一格断言它**响**（`exp_alerts` 非空）。只有静格的
#      夹具证明的是「这份数据上它没响」，恒假规则同样满足——而恒假正是这一族最常见的
#      坏法；
#   3. 也要有一格断言它**不响**（`exp_alerts: []`），否则「永远在响」与「写对了」在
#      夹具上同形；
#   4. 夹具点到的名字必须还在清单里。改名、拼错、删规则时那一格会变成永远不执行的死
#      指针，promtool 那边只会报「取不到规则」。
#
# 射程只看 CRD 清单。进程内那一库（`cogneva.json` 的 `observability.infra_watch.rules`）
# 有它自己的门 `check-alert-rule-firing-coverage.sh`，判据同形、人口不同，两边不重叠。
#
# 基线：这一族里有 56 条规则的证明**不在本仓**——它们住在值守用的另一棵树
# （`scripts/host/cogneva-host-readings-red-proof.promtool.yaml`），那边的门读本仓的
# 清单去跑。这正是这条门存在的理由：证明与本仓的规则分居两处，加规则的人看不见那一头，
# 那一头要等宽限期过了才红，红的时刻落在别人头上。基线把「还没搬进来」记成一个数，
# **只许变小**：给一条规则在本仓写了夹具，就得把它从基线划掉，否则基线是永久免检。
# 搬完那天基线应当是空的（判据：`--stock` 里不再有 unproven-in-repo 的行）。
#
# 用法：check-alert-rule-proof.sh [--stock] [--update-baseline [--allow-grow]]
#   --stock            印出每一条规则的证明情况（含基线内的），退 0——它是读数不是判据
#   --update-baseline  按当前树重写基线；基线只能变小，除非 --allow-grow
# 退出码：0 绿 / 1 红 / 2 用法不对 / 3 读不到（清单目录或夹具目录不存在、一份清单都没有
#         ——看不了不是一致，空目录上「全覆盖」是假的）
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../.." && pwd)

manifest_dir="${COGNEVA_ALERT_MANIFEST_DIR:-${root}/deploy/k3s/observability/manifests}"
fixtures_dir="${COGNEVA_ALERT_FIXTURES:-${root}/deploy/k3s/observability/tests}"
baseline_file="${COGNEVA_ALERT_PROOF_BASELINE:-${here}/alert-rule-proof.baseline}"

stock=0
update=0
grow=0
for arg in "$@"; do
    case "$arg" in
        --stock) stock=1 ;;
        --update-baseline) update=1 ;;
        --allow-grow) grow=1 ;;
        *) echo "usage: $0 [--stock] [--update-baseline [--allow-grow]]" >&2; exit 2 ;;
    esac
done

[ -d "$manifest_dir" ] || { echo "找不到告警清单目录：$manifest_dir" >&2; exit 3; }
[ -d "$fixtures_dir" ] || { echo "找不到夹具目录：$fixtures_dir" >&2; exit 3; }

# 人口与证据一次算完，交给 python 判：集合运算是它的活，bash 的排序规则依赖 LC_ALL。
python3 -I - "$manifest_dir" "$fixtures_dir" "$stock" "$update" "$grow" "$baseline_file" <<'PY'
import glob
import os
import sys

import yaml

manifest_dir, fixtures_dir = sys.argv[1:3]
stock, update, grow, baseline_path = (
    sys.argv[3] == "1", sys.argv[4] == "1", sys.argv[5] == "1", sys.argv[6])

manifests = sorted(glob.glob(os.path.join(manifest_dir, "*prometheusrule*.yaml")))
if not manifests:
    print(f"清单目录里一份 *prometheusrule*.yaml 都没读到：{manifest_dir}。"
          "零份清单与「每条规则都被证明了」在判词上同形，所以这里退 3。", file=sys.stderr)
    sys.exit(3)

population = {}
for path in manifests:
    try:
        docs = list(yaml.safe_load_all(open(path, encoding="utf-8")))
    except Exception as e:  # noqa: BLE001 - 原话就是判词
        print(f"读不了清单 {path}：{e}", file=sys.stderr)
        sys.exit(3)
    for doc in docs:
        if not doc:
            continue
        for group in (doc.get("spec") or {}).get("groups") or []:
            for rule in group.get("rules", []):
                name = rule.get("alert")
                if name:
                    population.setdefault(name, []).append(os.path.basename(path))

if not population:
    print(f"清单里一条带 alert 名的规则都没有：{manifest_dir}。人口为空时"
          "「全覆盖」是假的，所以这里退 3。", file=sys.stderr)
    sys.exit(3)

fixtures = sorted(glob.glob(os.path.join(fixtures_dir, "*prometheusrule*.test.yaml")))
if not fixtures:
    print(f"夹具目录里一份 *prometheusrule*.test.yaml 都没读到：{fixtures_dir}。"
          "零份证据与「整族都还没被证明」在判词上同形，所以这里退 3。", file=sys.stderr)
    sys.exit(3)

fired, quiet, named = {}, {}, {}
for path in fixtures:
    base = os.path.basename(path)
    try:
        case = yaml.safe_load(open(path, encoding="utf-8")) or {}
    except Exception as e:  # noqa: BLE001
        print(f"读不了夹具 {path}：{e}", file=sys.stderr)
        sys.exit(3)
    for one in case.get("tests", []):
        for t in one.get("alert_rule_test", []) or []:
            name = t.get("alertname")
            if not name:
                continue
            named.setdefault(name, set()).add(base)
            (fired if t.get("exp_alerts") else quiet).setdefault(name, set()).add(base)

pop = set(population)
# 一条规则的证明分成三块，缺哪块说哪块：名字都没点到、只有静格、只有响格。
missing = pop - set(named)
no_fire = pop - set(fired)
no_quiet = pop - set(quiet)
stray = set(named) - pop
# 「没证明」是这三块的并集；基线记的是这一族，不是其中一块——基线按名字存，一条规则
# 不会因为「缺的是响格还是静格」而占两行。
unproven = missing | no_fire | no_quiet

baseline = set()
if os.path.exists(baseline_path):
    for line in open(baseline_path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#"):
            baseline.add(line)

if update:
    # 基线只能变小：变大意味着这一笔新加了没人跑过的规则，那是要被判红的债，不是要记下来的。
    grew = unproven - baseline
    retired = baseline - unproven
    if grew and not grow:
        print("FAIL --update-baseline 会让基线变大，拒绝：", file=sys.stderr)
        for name in sorted(grew):
            print(f"       {name}", file=sys.stderr)
        print("      新加的规则要带两格证明。确实需要重新划分射程时用 --allow-grow 并说明为什么。",
              file=sys.stderr)
        sys.exit(1)
    with open(baseline_path, "w", encoding="utf-8") as out:
        out.write("# 托管告警规则（deploy/k3s/observability/manifests/*prometheusrule*.yaml）里\n")
        out.write("# 还没有**本仓**证明的规则名。一行一条。划掉一条 = 它在本仓有了一格会响、一格不响；\n")
        out.write("# 新债不许记进来（--update-baseline 会先拒绝）。旧的证明先在值守那棵树里，搬过来就划掉。\n")
        for name in sorted(unproven):
            out.write(name + "\n")
    print(f"基线已重写：{baseline_path}（{len(unproven)} 条未证明，划掉 {len(retired)} 条）")
    sys.exit(0)

new_debt = unproven - baseline
retired = baseline - unproven

if stock:
    for name in sorted(pop):
        if name not in unproven:
            state = "proven"
        elif name in baseline:
            state = "unproven-in-repo"
        else:
            state = "NEW-DEBT"
        blocks = []
        if name in missing:
            blocks.append("not-named")
        if name in no_fire:
            blocks.append("no-firing-cell")
        if name in no_quiet:
            blocks.append("no-quiet-cell")
        who = ",".join(sorted(named.get(name, ())))
        print(f"{state}\t{name}\t{'+'.join(blocks) or '-'}\t{who}")
    # `--stock` 是读数不是判据：印完就退 0，不落到下面那几条判词上。
    sys.exit(0)

ok = True
if new_debt:
    ok = False
    print("FAIL 这些规则没有本仓证明，也不在基线里（新加的规则要自带一格会响、一格不响）：",
          file=sys.stderr)
    for name in sorted(new_debt):
        blocks = []
        if name in missing:
            blocks.append("没有夹具点到名")
        if name in no_fire:
            blocks.append("没有一格断言它会响")
        if name in no_quiet:
            blocks.append("没有一格断言它不响")
        print(f"       {name}（{','.join(population[name])}；{'，'.join(blocks)}）", file=sys.stderr)
    print("      写哪一份：deploy/k3s/observability/tests/<清单号>-prometheusrule-<主题>.test.yaml，"
          "并在 deploy/scripts/tests/ 里配一个把它改坏就必须红的跑者测试。", file=sys.stderr)
if retired:
    ok = False
    print("FAIL 基线里的这些规则已经有本仓证明了，要从基线划掉（留着就是永久免检）：",
          file=sys.stderr)
    for name in sorted(retired):
        print(f"       {name}", file=sys.stderr)
    print("      划法：bash deploy/scripts/check-alert-rule-proof.sh --update-baseline",
          file=sys.stderr)
if stray:
    ok = False
    print("FAIL 这些夹具点到的名字在清单里不存在（改名／拼错／规则已删，那一格是死指针）：",
          file=sys.stderr)
    for name in sorted(stray):
        print(f"       {name}（{','.join(sorted(named[name]))}）", file=sys.stderr)

if ok:
    print(f"ALERT RULE PROOF OK：{len(pop)} 条托管告警规则，{len(fixtures)} 份夹具；"
          f"本仓已证明 {len(pop) - len(unproven)} 条，基线内未证明 {len(unproven)} 条，新增零条。")
    sys.exit(0)
sys.exit(1)
PY
