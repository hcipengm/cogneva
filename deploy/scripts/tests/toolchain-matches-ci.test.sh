#!/usr/bin/env bash
# The compiler-drift gate answers two questions that only look like one.
#
# The structural half — the channel in rust-toolchain.toml, and no version
# pinned in any workflow — is a fact about this tree, and it is the same fact
# wherever it is read. The other half asks whether the machine running the gate
# is behind the released stable, which is a fact about **that machine**: true of
# an author's laptop that skipped an upgrade, and equally true of a CI runner
# image that ships whatever stable it was built with and upgrades on somebody
# else's schedule.
#
# Judging the second half on CI is not a stricter gate, it is a gate whose
# subject is the runner image: the tree under judgement is the same, the answer
# flips with a pull of the base image, and the repair it names — run
# `rustup update` — is not something anyone can do to a runner. Observed on
# 2026-10-27: that step failed alone, on a revision whose whole workspace was
# compiling, and the deployer held every rollout behind it.
#
# What this test pins, one case per leg:
#   1. the clean tree passes;
#   2. a pinned channel in the toml is refused;
#   3. a pinned version in a workflow is refused;
#   4. a machine whose stable is behind is refused — but the same machine under
#      GITHUB_ACTIONS is not. Case 4 is the one that goes red if the CI half is
#      ever folded back in, and it needs no network and no real rustup.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-toolchain-matches-ci.sh"
[ -f "${gate}" ] || { echo "missing ${gate}" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# A scratch root that carries the gate at the depth it computes its own root
# from, plus the two files the structural half reads.
scratch() {
    local name="$1"
    local dir="${work}/${name}"
    mkdir -p "${dir}/deploy/scripts" "${dir}/.github/workflows"
    cp "${gate}" "${dir}/deploy/scripts/check-toolchain-matches-ci.sh"
    printf 'channel = "stable"\n' > "${dir}/rust-toolchain.toml"
    printf 'jobs:\n  x:\n    steps:\n      - uses: dtolnay/rust-toolchain@stable\n' \
        > "${dir}/.github/workflows/ci.yml"
    printf '%s' "${dir}"
}

# No rustup on PATH unless a case puts one there: the structural cases must not
# depend on what this machine happens to have installed.
#
# HOME moves too, and it has to: after PATH the gate looks in `~/.cargo/bin`,
# so a runner that answers with its own preinstalled stable is reached through
# HOME even when PATH was emptied. Measured on 2026-10-27: case 1 passed on
# every author machine and failed on the CI runner, which is the same defect
# the gate itself has — the question "is the author's compiler current" being
# answered by whichever machine runs the gate. The fixture owns HOME so the
# answer can only come from what the case put there.
run_gate() {
    local dir="$1"
    shift
    env -u GITHUB_ACTIONS HOME="${dir}/home" PATH="${PATH_OVERRIDE:-/usr/bin:/bin}" "$@" \
        bash "${dir}/deploy/scripts/check-toolchain-matches-ci.sh"
}

# 1) the clean tree
d="$(scratch clean)"
run_gate "${d}" >/dev/null || fail "干净树上这条门应当绿"

# 2) a pinned channel: the tree stops tracking the channel both sides read
d="$(scratch pinned-channel)"
printf 'channel = "1.99.0"\n' > "${d}/rust-toolchain.toml"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "toml 写死通道时这条门应当红，实际绿：${out}"
fi
printf '%s' "${out}" | grep -q 'rust-toolchain.toml' \
    || fail "红的时候要说清是 toml 这一处：${out}"

# 3) a pinned version in a workflow: it bypasses the toml entirely
d="$(scratch pinned-workflow)"
printf 'jobs:\n  x:\n    steps:\n      - uses: dtolnay/rust-toolchain@1.99.0\n' \
    > "${d}/.github/workflows/ci.yml"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "工作流写死版本号时这条门应当红，实际绿：${out}"
fi
printf '%s' "${out}" | grep -q 'dtolnay/rust-toolchain@1.99.0' \
    || fail "红的时候要点名那一处，实际：${out}"

# 4a) a machine whose stable is behind: still the case this gate exists for
fake="${work}/fakebin"
mkdir -p "${fake}"
cat > "${fake}/rustup" <<'FAKE'
#!/usr/bin/env bash
echo "stable-x86_64-unknown-linux-gnu - update available: 1.0.0 (aaa) -> 2.0.0 (bbb)"
exit 100
FAKE
chmod +x "${fake}/rustup"
d="$(scratch behind)"
if out="$(PATH_OVERRIDE="${fake}:/usr/bin:/bin" run_gate "${d}" 2>&1)"; then
    fail "本机 stable 落后时这条门应当红（否则它唯一该红的时候静默放行）：${out}"
fi
printf '%s' "${out}" | grep -q 'update available' \
    || fail "红的时候要带出 rustup 说的那句话：${out}"

# 4b) the same machine on CI: the runner image is not the author
d="$(scratch behind-on-ci)"
if ! out="$(PATH_OVERRIDE="${fake}:/usr/bin:/bin" run_gate "${d}" GITHUB_ACTIONS=true 2>&1)"; then
    fail "CI 上不判 runner 镜像的版本（与这笔改动无关，也没人能在 runner 上修）：${out}"
fi
printf '%s' "${out}" | grep -q 'SKIP' \
    || fail "跳过要留下那一行，不能静默绿：${out}"

# 5) rustup is not on PATH but lives where the gate looks second: `~/.cargo/bin`,
#    which is how this build host installs it. That copy is the author's, so it
#    must be read — a case that emptied PATH and stopped there would let the gate
#    answer "current" for a machine that never asked.
d="$(scratch behind-in-home)"
mkdir -p "${d}/home/.cargo/bin"
cp "${fake}/rustup" "${d}/home/.cargo/bin/rustup"
if out="$(run_gate "${d}" 2>&1)"; then
    fail "本机 ~/.cargo/bin 里的 stable 落后时这条门应当红：${out}"
fi
printf '%s' "${out}" | grep -q 'update available' \
    || fail "红的时候要带出 rustup 说的那句话：${out}"

echo "toolchain-matches-ci：五支各判一次，干净树绿"
