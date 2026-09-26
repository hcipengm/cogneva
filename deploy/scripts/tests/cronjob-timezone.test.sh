#!/usr/bin/env bash
# 部署面里每个 CronJob 都必须显式钉住 `spec.timeZone`。
#
# 为什么需要这条判据：`spec.timeZone` 留空时，触发时刻由 kube-controller-manager
# 所在节点的 local 解释。于是"每日 03:17 备份"这句话会随宿主时区**静默漂移**——
# 2026-09-26 本机 local 由 Etc/UTC 改成 Asia/Shanghai，`17 3 * * *` 实际就从
# UTC 03:17 变成 UTC 19:17；而按 schedule 字面值做的任何核对都看不出这条漂移，
# 因为清单里那行字一个字符都没变。这又是个平时没人盯的每日作业，漂了不会有人报。
#
# 判据面覆盖**所有**载体（三份渲染产物 + `deploy/k3s/` 静态清单），不针对某一个
# CronJob：病因是"任何一处把触发时刻交给节点 local 解释"，只盯备份那一条会让
# 下一个新增的 CronJob 静默漏过。
#
# 判据读的是**渲染产物**而不是 chart 模板/values：产物是真正 apply 下去的东西，
# 从 values 读到非空、而模板没把它渲染出去，是这条判据要挡的另一种坏法。
#
# 两个控制项证明判据真的跑到了被检对象上（而不是"扫了个空"就当通过）：
#   1) 扫到的 CronJob 数必须 > 0；
#   2) 一个已知缺 timeZone 的样本必须被拒绝，一个带 timeZone 的样本必须被放行。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/../../.." && pwd)"

checker() {
  python3 - "$@" <<'PY'
import re
import sys

CRONJOB = re.compile(r'(?m)^kind:\s*CronJob\s*$')
TOP_SPEC = re.compile(r'^spec:\s*$')
# 只有正好缩进两格才是 CronJob 自己的 spec.timeZone：jobTemplate 里更深的那层
# 同名键（容器时区之类）不算数。
TIMEZONE = re.compile(r'(?m)^\s{2}timeZone:\s*["\']?[^"\'\s]')


def documents(text):
    return re.split(r'(?m)^---\s*$', text)


def spec_block(doc):
    """CronJob 文档的 spec 块正文；不是 CronJob 就返回 None。"""
    if not CRONJOB.search(doc):
        return None
    lines = doc.splitlines()
    for i, line in enumerate(lines):
        if TOP_SPEC.match(line):
            block = []
            for rest in lines[i + 1:]:
                # 顶格（非缩进、非注释）即 spec 块结束。
                if rest and not rest.startswith((' ', '\t', '#')):
                    break
                block.append(rest)
            return '\n'.join(block)
    return ''


bad = []
found = 0
for path in sys.argv[1:]:
    with open(path, encoding='utf-8') as fh:
        for doc in documents(fh.read()):
            block = spec_block(doc)
            if block is None:
                continue
            found += 1
            if not TIMEZONE.search(block):
                name = re.search(r'(?m)^\s{2}name:\s*(\S+)', doc)
                bad.append(f'{path}: {name.group(1) if name else "?"}')

for entry in bad:
    print(f'CronJob 没有显式 timeZone（触发时刻会跟随节点 local）: {entry}')
print(f'检查了 {found} 个 CronJob，{len(bad)} 个缺 timeZone')
sys.exit(1 if bad else 0)
PY
}

fail=0

# 控制项：判据必须拒绝缺 timeZone 的样本，且必须放行带 timeZone 的样本。
# 少了这两个，下面那句"通过"可能只是判据从来不会说"不"。
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cat >"$tmp/missing.yaml" <<'YAML'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sample-without-timezone
spec:
  schedule: "17 3 * * *"
  concurrencyPolicy: Forbid
YAML
cat >"$tmp/present.yaml" <<'YAML'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sample-with-timezone
spec:
  schedule: "17 3 * * *"
  timeZone: "Etc/UTC"
  concurrencyPolicy: Forbid
YAML
if checker "$tmp/missing.yaml" >/dev/null 2>&1; then
  echo "反向实验失败：缺 timeZone 的样本被判据放行了，这条判据不会拒绝任何东西"
  fail=1
fi
if ! checker "$tmp/present.yaml" >/dev/null 2>&1; then
  echo "反向实验失败：带 timeZone 的样本被判据拒绝了，判据认的不是这个字段"
  fail=1
fi

# 被检对象：部署面里所有可能被 apply 的清单。
mapfile -t targets < <(
  find "${repo_root}/deploy/rendered" "${repo_root}/deploy/k3s" -name '*.yaml' -type f | sort
)
if [ "${#targets[@]}" -eq 0 ]; then
  echo "没有找到任何部署清单，判据面是空的"
  exit 1
fi

# 先把读数取下来再看，不要 `checker | grep` 直接接：grep 命中就退出会让 python
# 那头吃 SIGPIPE，而 `set -o pipefail` 把那个非零算成整条管道的结论——判据明明
# 命中了，管道却报失败。
scan_out="$(checker "${targets[@]}" || true)"
printf '%s\n' "$scan_out"
if printf '%s\n' "$scan_out" | grep -qE '检查了 [1-9][0-9]* 个 CronJob'; then
  :
else
  echo "部署面里一个 CronJob 都没扫到，这条判据这次并没有在工作"
  fail=1
fi
if printf '%s\n' "$scan_out" | grep -q '没有显式 timeZone'; then
  fail=1
fi

[ "$fail" -eq 0 ] || exit 1
echo "CRONJOB TIMEZONE OK：部署面所有 CronJob 都显式钉住了触发时区"
