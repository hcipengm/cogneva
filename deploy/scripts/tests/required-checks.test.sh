#!/usr/bin/env bash
# `main`'s required-check list names checks by string, and the checks that exist
# are whatever the workflows publish. Nothing compared the two until 2026-10-28,
# when three of the nine required contexts named a job that had been renamed:
# `Bootstrap Check (ubuntu-latest)` against a job called `Bootstrap
# Cross-Platform Check`. A context that is never reported cannot be satisfied,
# so the requirement blocked nobody -- it only made every push echo "9 of 9
# required status checks are expected" and then land on an admin bypass. Three
# commits with red `Test` and `Code Coverage` sat on main for an hour.
#
# The gate derives the producer side from the workflow files, so each case here
# has to say what shape of workflow it is testing against. One case per leg:
#   1. a plain job name and a matrix job's appended platform both resolve;
#   2. a job with no `name:` resolves as its job id, which is what GitHub
#      publishes -- otherwise a correct declaration reads as unproduced;
#   3. a renamed job is refused and the message names the closest producer,
#      because the repair is "spell it like the job", not "find a missing job";
#   4. a matrix value outside the matrix is refused: the base name resolves and
#      the suffix does not, so a check keyed on the suffix alone would pass;
#   5. an empty declaration is refused -- satisfied by every commit, and it
#      reads the same as a repository that requires nothing;
#   6. a matrix this reader cannot expand is "cannot decide", not "produced",
#      so an unreadable `include:` cannot turn into a green;
#   7. the real tree passes, which is the only case that guards the declaration
#      this repository actually ships.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-required-checks.py"
[ -f "${gate}" ] || { echo "missing ${gate}" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# A scratch root carrying the gate at the depth it computes its own root from,
# plus the workflow shape the case under test needs.
scratch() {
    local name="$1" workflow="$2" declaration="$3"
    local dir="${work}/${name}"
    mkdir -p "${dir}/deploy/scripts" "${dir}/.github/workflows"
    cp "${gate}" "${dir}/deploy/scripts/check-required-checks.py"
    printf '%s\n' "${workflow}" > "${dir}/.github/workflows/ci.yml"
    printf '%s\n' "${declaration}" > "${dir}/.github/required-checks.txt"
    printf '%s' "${dir}"
}

run_gate() { python3 "${1}/deploy/scripts/check-required-checks.py"; }

plain_and_matrix='jobs:
  check:
    name: Check
    runs-on: ubuntu-latest
  bootstrap-cross-platform:
    name: Bootstrap Cross-Platform Check
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest]
    runs-on: ${{ matrix.os }}'

# 1) a plain job name and a matrix job's appended platform
d="$(scratch plain "${plain_and_matrix}" 'Check
Bootstrap Cross-Platform Check (ubuntu-latest)')"
out="$(run_gate "${d}" 2>&1)" || fail "名字都对得上时应当绿，实际红：${out}"
printf '%s' "${out}" | grep -q 'declared=2 produced=3' \
    || fail "绿的读数要带出两侧的条数（2 条声明 / 3 个产出物），实际：${out}"

# 2) no `name:` on the job: GitHub publishes the job id
d="$(scratch jobid 'jobs:
  entry-scripts:
    runs-on: ubuntu-latest' 'entry-scripts')"
run_gate "${d}" >/dev/null || fail "没有 name 的 job 按 job id 发布，声明 job id 应当绿"

# 3) the renamed job: refused, and the closest producer is named
d="$(scratch renamed "${plain_and_matrix}" 'Bootstrap Check (ubuntu-latest)')"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "声明了一个没有人发的名字时应当红（这正是 2026-10-28 的现场）：${out}"
fi
printf '%s' "${out}" | grep -q 'line 1' || fail "红的时候要点名是第几行：${out}"
printf '%s' "${out}" | grep -q 'Bootstrap Cross-Platform Check (ubuntu-latest)' \
    || fail "红的时候要给出最接近的产出物名字，否则修法只能靠猜：${out}"

# 4) the base resolves but the platform does not
d="$(scratch badplatform "${plain_and_matrix}" 'Bootstrap Cross-Platform Check (freebsd-latest)')"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "矩阵里没有的平台后缀应当红：${out}"
fi

# 5) an empty declaration
d="$(scratch empty "${plain_and_matrix}" '# 只有注释')"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "空声明应当红——它被每一笔提交满足，且与「这个仓库不要求检查」同形：${out}"
fi

# 6) a matrix this reader cannot expand is not a yes
d="$(scratch include 'jobs:
  wide:
    name: Wide
    strategy:
      matrix:
        include:
          - os: ubuntu-latest' 'Wide (ubuntu-latest)')"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "读不出来的矩阵应当判「无法判定」而不是「有产出物」：${out}"
fi
printf '%s' "${out}" | grep -qi 'cannot expand' \
    || fail "无法判定要说清是读不出来，不是没人发：${out}"

# 7) the real tree
out="$(cd "${repo}" && python3 deploy/scripts/check-required-checks.py 2>&1)" \
    || fail "本仓的必需检查声明与产出物对不上：${out}"
printf '%s' "${out}" | grep -q 'every required check has a producer' \
    || fail "真树上的绿要说明它判了什么：${out}"

echo "required-checks：七支各判一次，真树绿"
