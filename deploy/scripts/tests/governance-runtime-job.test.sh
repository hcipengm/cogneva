#!/usr/bin/env bash
# Deterministic gate for the headroom reading in check-governance-consistency.sh.
#
# The reading that went wrong: the quota headroom was computed over the rendered
# workload set alone, and the one pod the deployer creates at *runtime* -- the
# rollout judgement Job -- is not in any rendered set. It is declared, though: its
# resource face lives in the evolution ConfigMap, which the same render carries.
# Measured live on 2026-09-29: the note printed "headroom 600m" while the namespace
# was at 17/17 during a rollout (steady pods 15000m + judgement Job 2000m), and the
# admission failure landed as `FailedCreate ... exceeded quota: cogneva-quota` which
# the rollout reports as a *timeout*, not as a resource verdict. Two carriers held
# the same figure and disagreed: the ConfigMap said 2000m, the quota comment's
# calibration (and therefore the note) still counted 500m.
#
# The reading is now part of the verdict: the judgement Job runs on every
# rollout, so it is not a "lower bound that might be exceeded on more nodes" but a
# pod that is always there, and the gate goes red once the rendered set plus the
# Job passes the ceiling rather than printing a note and passing. What this test
# pins is that the number is *read from the declaration* (it follows a mutation),
# that crossing the ceiling is red, and that a disabled deployer removes the line
# instead of printing a stale one.
#
# Directions:
#   - the real rendered profiles print the line, and the runtime-inclusive figure
#     equals the rendered-set sum plus the Job's declared limit;
#   - raising the declared limit within the headroom moves the printed number by
#     exactly that much;
#   - raising it past the headroom turns the gate red, naming the quota;
#   - a manifest-declared Job is counted in the lower bound (the note has always
#     claimed "concurrent Jobs count as 1" while the enumeration listed four kinds
#     without Job -- a claim wider than the code);
#   - a disabled deployer removes the line instead of printing a stale one.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-governance-consistency.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "missing dependency: python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Reads the two cpu numbers the gate prints for the limits face: the rendered-set
# lower bound and the runtime-inclusive one. The `requests` face prints a line of
# the same shape, so the together-line is taken from the block that follows the
# `limits.CPU` line and is required to name the *limit* key -- matching the first
# "把它一起算进来" in the file would silently read the request face instead.
read_cpu() {
  python3 - "$1" <<'PYEOF'
import re, sys

lines = open(sys.argv[1]).read().splitlines()
alone = together = None
for i, line in enumerate(lines):
    m = re.search(r"limits\.CPU 单副本下限和 (\d+)m", line)
    if not m:
        continue
    alone = m.group(1)
    for follow in lines[i + 1:]:
        if "└" not in follow:
            break
        if "ROLLOUT_JOB_CPU_LIMIT" not in follow:
            break
        t = re.search(r"把它一起算进来 (\d+)m vs", follow)
        if t:
            together = t.group(1)
        break
    break
if alone is None:
    sys.exit("the gate printed no limits.CPU lower-bound reading")
print(alone, together or "-")
PYEOF
}

# The Job's declared cpu limit, read from the same ConfigMap the deployer reads.
declared_cpu_m() {
  python3 - "$1" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    for doc in yaml.safe_load_all(open(path)):
        data = (doc or {}).get("data") or {}
        if str(data.get("COGNEVA_MAINLINE_DEPLOYER_ENABLED", "")).lower() != "true":
            continue
        raw = data.get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT")
        if raw is None:
            continue
        text = str(raw).strip()
        if text.endswith("m"):
            print(int(float(text[:-1])))
        else:
            print(int(float(text) * 1000))
        raise SystemExit
sys.exit("no enabled deployer ConfigMap declaring a rollout job cpu limit")
PYEOF
}

for profile in k3s-single k3s-multi; do
  dir="${repo}/deploy/rendered/${profile}"
  bash "${gate}" "${dir}" > "${work}/${profile}.out" 2>&1 \
    || fail "the gate is red on ${profile}: $(tail -3 "${work}/${profile}.out")"

  job_m="$(declared_cpu_m "${dir}")"
  read -r alone together < <(read_cpu "${work}/${profile}.out")
  [ "${together}" != "-" ] \
    || fail "${profile}: the gate printed no runtime-inclusive headroom line"
  [ $(( together - alone )) -eq "${job_m}" ] \
    || fail "${profile}: runtime-inclusive ${together}m minus rendered-set ${alone}m is not the declared ${job_m}m"

  # Mutation control: a limit that cannot coincide with anything else in the tree.
  # If the number were a constant, or read from the wrong carrier, this would not
  # move. It stays within the headroom (3 cores on top of the rendered set is
  # still under the ceiling) so the gate stays green and the numbers are printed.
  mutated="${work}/mutated-${profile}"
  cp -r "${dir}" "${mutated}"
  python3 - "${mutated}" <<'PYEOF'
import glob, os, sys, yaml

target = os.path.join(sys.argv[1])
for path in sorted(glob.glob(os.path.join(target, "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        data = d.get("data") or {}
        if "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT" in data:
            data["COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT"] = "3"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  bash "${gate}" "${mutated}" > "${work}/mutated-${profile}.out" 2>&1 \
    || fail "the gate is red on the mutated ${profile}"
  read -r _ mutated_together < <(read_cpu "${work}/mutated-${profile}.out")
  [ $(( mutated_together - alone )) -eq 3000 ] \
    || fail "${profile}: raising the declared limit to 3 cores moved the reading to ${mutated_together}m from ${alone}m, not by 3000m"

  # Verdict control: once the rendered set plus the judgement Job passes the
  # ceiling the gate must go red -- the Job is inside the verdict now, not a note.
  # 9 cores pushes the runtime-inclusive figure well past the 23000m ceiling.
  over="${work}/over-${profile}"
  cp -r "${dir}" "${over}"
  python3 - "${over}" <<'PYEOF'
import glob, os, sys, yaml

target = os.path.join(sys.argv[1])
for path in sorted(glob.glob(os.path.join(target, "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        data = d.get("data") or {}
        if "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT" in data:
            data["COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT"] = "9"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  if bash "${gate}" "${over}" > "${work}/over-${profile}.out" 2>&1; then
    fail "${profile}: the gate stayed green while rendered set + judgement Job passed the ceiling"
  fi
  grep -q "已超过配额" "${work}/over-${profile}.out" \
    || fail "${profile}: the over-ceiling gate did not name the quota"

  # Reverse control: with the deployer disabled the Job never exists, so the line
  # must disappear rather than keep printing a number nothing declares.
  off="${work}/off-${profile}"
  cp -r "${dir}" "${off}"
  python3 - "${off}" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        data = d.get("data") or {}
        if "COGNEVA_MAINLINE_DEPLOYER_ENABLED" in data:
            data["COGNEVA_MAINLINE_DEPLOYER_ENABLED"] = "false"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  bash "${gate}" "${off}" > "${work}/off-${profile}.out" 2>&1 \
    || fail "the gate is red with the deployer disabled on ${profile}"
  read -r _ off_together < <(read_cpu "${work}/off-${profile}.out")
  [ "${off_together}" = "-" ] \
    || fail "${profile}: the runtime-inclusive line is still printed with the deployer disabled"
  echo "OK: ${profile}: rendered-set ${alone}m + judgement Job ${job_m}m = ${together}m, and the reading follows the declaration"
done

# A manifest-declared Job has to move the lower bound: the note has always claimed
# concurrent Jobs count as 1, and until now the enumeration could not see any Job.
synth="${work}/synth"
mkdir -p "${synth}"
job_limit="700m"
cat > "${synth}/10-quota.yaml" <<YAML
apiVersion: v1
kind: ResourceQuota
metadata:
  name: cogneva-quota
  # A zero budget is still a declaration: an absent annotation is an error, not 0.
  annotations:
    cogneva.io/runtime-pods-limits-cpu: "0"
    cogneva.io/runtime-pods-limits-memory: "0"
spec:
  hard:
    limits.cpu: "8"
YAML
cat > "${synth}/11-limits.yaml" <<'YAML'
apiVersion: v1
kind: LimitRange
metadata:
  name: cogneva-limits
spec:
  limits:
    - type: Container
      default:
        cpu: 500m
      defaultRequest:
        cpu: 50m
      max:
        cpu: "4"
      min:
        cpu: 10m
YAML
cat > "${synth}/40-job.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: a-rolling-job
spec:
  template:
    spec:
      containers:
        - name: c
          image: example.invalid/x:1
          resources:
            limits:
              cpu: ${job_limit}
            requests:
              cpu: 100m
YAML

echo "--- with the Job ---"
bash "${gate}" "${synth}" > "${work}/synth-with.out" 2>&1 || fail "the gate is red on the synthetic tree with a Job"
read -r synth_alone _ < <(read_cpu "${work}/synth-with.out")
[ "${synth_alone}" = "700" ] \
  || fail "a manifest-declared Job with 700m did not enter the limits.cpu lower bound (read ${synth_alone}m)"

echo "--- without the Job ---"
rm "${synth}/40-job.yaml"
bash "${gate}" "${synth}" > "${work}/synth-without.out" 2>&1 || fail "the gate is red on the synthetic tree without a Job"
read -r synth_alone _ < <(read_cpu "${work}/synth-without.out")
[ "${synth_alone}" = "0" ] \
  || fail "the lower bound did not fall back to 0m without the Job (read ${synth_alone}m)"

# The other runtime-born term: pods the operating face creates on demand (the eval
# table, the drill Job). Nothing in the render set declares their size, so the quota
# carries it as an annotation and the gate must fold it into the same inequality.
# Deleting the annotation must be red -- an absent budget read as 0 is exactly the
# reading that said "headroom 1.8Gi" while admission was already refusing the Job.
# Reads the runtime-inclusive figure from the budget line (cpu face).
read_budget_cpu() {
  python3 - "$1" <<'PYEOF'
import re, sys

lines = open(sys.argv[1]).read().splitlines()
for line in lines:
    if "runtime-pods-limits-cpu" not in line or "再计入" not in line:
        continue
    m = re.search(r"= (\d+)m vs 配额", line)
    if m:
        print(m.group(1))
        raise SystemExit
sys.exit("the gate printed no runtime-pod budget line for cpu")
PYEOF
}

budget_cpu_m() {
  python3 - "$1" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    for doc in yaml.safe_load_all(open(path)):
        if (doc or {}).get("kind") != "ResourceQuota":
            continue
        raw = ((doc.get("metadata") or {}).get("annotations") or {}).get(
            "cogneva.io/runtime-pods-limits-cpu")
        if raw is None:
            raise SystemExit("the rendered quota carries no cpu budget annotation")
        text = str(raw).strip()
        print(int(float(text[:-1])) if text.endswith("m") else int(float(text) * 1000))
        raise SystemExit
sys.exit("no ResourceQuota in the rendered set")
PYEOF
}

for profile in k3s-single k3s-multi; do
  dir="${repo}/deploy/rendered/${profile}"
  bash "${gate}" "${dir}" > "${work}/budget-${profile}.out" 2>&1 \
    || fail "the gate is red on ${profile}: $(tail -3 "${work}/budget-${profile}.out")"

  job_m="$(declared_cpu_m "${dir}")"
  bud_m="$(budget_cpu_m "${dir}")"
  read -r alone _ < <(read_cpu "${work}/budget-${profile}.out")
  covered="$(read_budget_cpu "${work}/budget-${profile}.out")"
  [ $(( covered - alone )) -eq $(( job_m + bud_m )) ] \
    || fail "${profile}: budget-inclusive ${covered}m minus rendered-set ${alone}m is not judgement Job ${job_m}m + budget ${bud_m}m"

  # Mutation control: the printed figure follows the annotation, so it cannot be a
  # constant computed from something else.
  mutated="${work}/budget-mut-${profile}"
  cp -r "${dir}" "${mutated}"
  python3 - "${mutated}" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        ann = ((d.get("metadata") or {}).get("annotations") or {})
        if "cogneva.io/runtime-pods-limits-cpu" in ann:
            ann["cogneva.io/runtime-pods-limits-cpu"] = "1.5"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  bash "${gate}" "${mutated}" > "${work}/budget-mut-${profile}.out" 2>&1 \
    || fail "the gate is red on the mutated budget in ${profile}"
  read -r mut_alone _ < <(read_cpu "${work}/budget-mut-${profile}.out")
  mut_covered="$(read_budget_cpu "${work}/budget-mut-${profile}.out")"
  [ $(( mut_covered - mut_alone )) -eq $(( job_m + 1500 )) ] \
    || fail "${profile}: setting the budget to 1.5 cores moved the reading to ${mut_covered}m from ${mut_alone}m, not to Job ${job_m}m + 1500m"

  # Absence control: no annotation means no declaration, and that is red. Reading it
  # as 0 would restore the pre-incident illusion of headroom.
  gone="${work}/budget-gone-${profile}"
  cp -r "${dir}" "${gone}"
  python3 - "${gone}" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        ann = ((d.get("metadata") or {}).get("annotations") or {})
        if "cogneva.io/runtime-pods-limits-memory" in ann:
            del ann["cogneva.io/runtime-pods-limits-memory"]
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  if bash "${gate}" "${gone}" > "${work}/budget-gone-${profile}.out" 2>&1; then
    fail "${profile}: the gate stayed green with the runtime-pod budget annotation deleted"
  fi
  grep -q "cogneva.io/runtime-pods-limits-memory" "${work}/budget-gone-${profile}.out" \
    || fail "${profile}: the missing-budget verdict did not name the annotation"

  # Verdict control: a budget that no longer fits must turn the gate red, even when
  # the rendered set plus the judgement Job alone still fit.
  over="${work}/budget-over-${profile}"
  cp -r "${dir}" "${over}"
  python3 - "${over}" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        ann = ((d.get("metadata") or {}).get("annotations") or {})
        if "cogneva.io/runtime-pods-limits-cpu" in ann:
            ann["cogneva.io/runtime-pods-limits-cpu"] = "9"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  if bash "${gate}" "${over}" > "${work}/budget-over-${profile}.out" 2>&1; then
    fail "${profile}: the gate stayed green while rendered set + Job + budget passed the ceiling"
  fi
  grep -q "运行期自建 Pod 预算" "${work}/budget-over-${profile}.out" \
    || fail "${profile}: the over-budget verdict did not name the runtime-pod budget"
  echo "OK: ${profile}: budget ${bud_m}m follows the annotation, absence is red"
done

echo "OK: governance runtime-job headroom gate"
