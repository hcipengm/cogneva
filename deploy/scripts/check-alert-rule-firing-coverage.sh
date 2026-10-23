#!/usr/bin/env bash
# 进程内那一库（部署配置的 `observability.infra_watch.rules`）里，**哪几条真的被证明过会响**。
#
# 为什么要有这一格：`test-acceptance-census-alert-rules.sh` 现在能读 `.json`，通路有了——
# 通路不等于覆盖。一库两百多条规则里有一条被夹具点名，剩下那些与「没人跑过」在读数上完全
# 一样，而所有结构门对它们一视同仁地绿。这条门把「谁被证明过」变成一个能读的数，并让
# **新增**规则必须自带一格证明：不带，这一笔就红。
#
# 判据三条：
#   1. 没被证明的规则必须在基线里。基线外的＝这一笔新加了一条没人跑过的规则。
#   2. 基线里的规则必须**仍然**没被证明。已经有人给它写了夹具的，要从基线里划掉——
#      留着不划，基线就成了「这些永远免检」的许可证。
#   3. 夹具点名的规则必须在这一库里。名字漂了（改名、拼错、删了规则）时，那一格夹具
#      会变成永远不执行的死指针，而 promtool 那边只会报「取不到规则」——这里先拦住。
#
# 基线按**名字**存，不按文件：改名就是删一条加一条，两条都会红，这是对的。
#
# 用法：check-alert-rule-firing-coverage.sh [--stock] [--update-baseline [--allow-grow]]
#   --stock            印出全部覆盖情况（含基线内的），退 0
#   --update-baseline  按当前树重写基线；基线只能变小，除非 --allow-grow
#   --allow-grow       允许 --update-baseline 把基线变大（重新划分射程时才用，并且要说清为什么）
#
# 退出码：0 绿 / 1 红 / 2 用法不对 / 3 读不到（配置或夹具不在——看不了不是一致）。
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../.." && pwd)

# 覆盖口只给夹具用：它把配置复制到临时文件里加一条规则，看这一笔会不会红。
config="${COGNEVA_ALERT_CONFIG:-${root}/deploy/helm/cogneva/files/cogneva.json}"
fixtures_dir="${COGNEVA_ALERT_FIXTURES:-${root}/deploy/k3s/observability/tests}"
baseline_file="${COGNEVA_ALERT_COVERAGE_BASELINE:-${here}/alert-rule-firing-coverage.baseline}"

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

[ -f "$config" ] || { echo "找不到部署配置：$config" >&2; exit 3; }
[ -d "$fixtures_dir" ] || { echo "找不到夹具目录：$fixtures_dir" >&2; exit 3; }

# 人口与证据一次算完，交给 python 判：bash 里做集合运算是错的（排序规则依赖 LC_ALL，
# 而这里比的是标识符）。
python3 -I - "$config" "$fixtures_dir" "$baseline_file" "$stock" "$update" "$grow" <<'PY'
import glob
import json
import os
import re
import sys

config_path, fixtures_dir, baseline_path = sys.argv[1:4]
stock, update, grow = (a == "1" for a in sys.argv[4:7])

try:
    config = json.load(open(config_path, encoding="utf-8"))
except Exception as e:  # noqa: BLE001 - the reason is the message
    sys.exit(f"部署配置读不了：{config_path}: {e}")
library = ((config.get("observability") or {}).get("infra_watch") or {}).get("rules")
if not isinstance(library, list) or not library:
    sys.exit(f"部署配置里没有 observability.infra_watch.rules：{config_path}")
population = [r.get("name") for r in library if isinstance(r, dict) and r.get("name")]
if not population:
    sys.exit("规则库里一条带名字的规则都没有——人口为空时「全覆盖」是假的")

# 证据：夹具头部 `# rule: <名字>` 行。与跑者取规则的判据是同一条正则，两处漂了就会
# 出现「门说覆盖了、跑者却取不到」或反过来。
named = re.compile(r"^#\s*rule:\s*(\S+)\s*$", re.M)
proofs = {}
for path in sorted(glob.glob(os.path.join(fixtures_dir, "*.test.yaml"))):
    for name in named.findall(open(path, encoding="utf-8").read()):
        proofs.setdefault(name, []).append(os.path.basename(path))

pop = set(population)
covered = pop & set(proofs)
uncovered = pop - set(proofs)
stray = set(proofs) - pop

baseline = set()
if os.path.exists(baseline_path):
    for line in open(baseline_path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#"):
            baseline.add(line)

# 基线里那些「已经不算欠账」的条目：已经被证明，或规则已经不在库里。
retired = baseline - uncovered
# 基线外的新欠账：这一笔新加的、没人跑过的规则。
new_debt = uncovered - baseline

if stock:
    for name in sorted(pop):
        who = ",".join(proofs.get(name, []))
        print(f"{'proven' if name in covered else 'unproven'}\t{name}\t{who}")
    print()
    for name in sorted(stray):
        print(f"stray\t{name}\t{','.join(proofs[name])}")

if update:
    # 基线还不存在＝这门自己刚立起来，第一次写不算「长债」；存在了再变大就必须显式放行。
    # 删掉基线再重建也能绕过这一步，但那要两笔可见的提交，且 baseline 文件本身在 diff 里。
    if not grow and os.path.exists(baseline_path) and len(uncovered) > len(baseline):
        print(
            f"拒绝写基线：现在有 {len(uncovered)} 条没被证明，基线是 {len(baseline)} 条。"
            " 新债要补夹具，不是记进基线（真的要重新划分射程时加 --allow-grow 并在提交信息里说清）。",
            file=sys.stderr,
        )
        sys.exit(1)
    with open(baseline_path, "w", encoding="utf-8") as f:
        f.write(
            "# 进程内那一库（observability.infra_watch.rules）里还没有「会响」证明的规则名。\n"
            "# 一行一条。划掉一条 = 给它写了夹具；新债不许记进来（--update-baseline 会先拒绝）。\n"
        )
        for name in sorted(uncovered):
            f.write(name + "\n")
    print(f"BASELINE WRITTEN：{baseline_path}（未证明 {len(uncovered)} / 全库 {len(pop)}）")
    sys.exit(0)

fail = 0

if new_debt:
    fail = 1
    print(
        f"FAIL：{len(new_debt)} 条规则没有「会响」的证明，也不在基线里"
        "（新加的规则要自带夹具，或在提交信息里说明为什么这一条不适用）:",
        file=sys.stderr,
    )
    for name in sorted(new_debt):
        print(f"  {name}", file=sys.stderr)

if retired:
    fail = 1
    print(
        f"FAIL：基线里 {len(retired)} 条已经不是欠账了，把它们划掉"
        "（已证明，或规则已不在库里——留着就是一张永久免检的许可证）:",
        file=sys.stderr,
    )
    for name in sorted(retired):
        why = "已证明" if name in covered else "规则不在库里"
        print(f"  {name}（{why}）", file=sys.stderr)

if stray:
    fail = 1
    print(
        f"FAIL：{len(stray)} 条夹具点名的规则不在这一库里（改名或删了规则，夹具成了死指针）:",
        file=sys.stderr,
    )
    for name in sorted(stray):
        print(f"  {name}（{','.join(proofs[name])}）", file=sys.stderr)

if fail:
    sys.exit(1)

print(
    f"ALERT FIRING COVERAGE OK：全库 {len(pop)} 条，已证明 {len(covered)} 条，"
    f"基线内未证明 {len(uncovered)} 条（新增零条）。"
)
PY
