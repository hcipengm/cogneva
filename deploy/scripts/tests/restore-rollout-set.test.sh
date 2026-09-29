#!/usr/bin/env bash
# Deterministic gate for "the restore restarts every deployment that has to replay
# in-memory state after the persistent layer is put back".
#
# The failure this pins is silent and only shows up at restore time, which is the
# worst moment to find it. A transcribed list of deployment names keeps working
# when a deployment is added: the restore reports success, and the new deployment
# goes on serving the state it held before the restore -- a mismatch between two
# layers that nothing in the cluster reports. The judgement has to be structural,
# so the set is read from the cluster and keyed on "runs an image this project
# publishes", which is what `$IMAGE` (already read from the live deployment) names.
#
# The criterion has three properties worth pinning, all of which a name list would
# fail:
#   - a deployment is matched by image repository, not by tag, because the restore
#     runs against deployments carrying whatever rev the cluster is on;
#   - every container is looked at, not just the first, because a sidecar can be
#     the one carrying our image;
#   - a deployment running someone else's image is not restarted, because the
#     persistence behind it is not the one this path restored.
#
# The block is extracted from the script between its markers rather than copied
# here: a copy would be a second reader of the criterion and would go on passing
# after the script changed.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
script="${repo}/deploy/scripts/restore-from-package.sh"
fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/bin"

# Only the one call the derivation makes is answered, and it is answered by shape:
# the fixture is one line per deployment with every container image on it, so a
# query that asks for the first container only would get an answer the fixture
# does not describe. Anything else is an error rather than an empty list.
cat >"${work}/bin/kubectl" <<'STUB'
#!/usr/bin/env bash
case "$*" in
    *"get deploy"*"containers[*]"*) cat "${FIXTURE}" ;;
    *"get deploy"*) echo "stub kubectl: the rollout set has to read every container: $*" >&2; exit 1 ;;
    *) echo "stub kubectl: unexpected call: $*" >&2; exit 1 ;;
esac
STUB
chmod +x "${work}/bin/kubectl"
export PATH="${work}/bin:${PATH}"

sed -n '/^# rollout-set: start$/,/^# rollout-set: end$/p' "${script}" >"${work}/derive.sh"
[ -s "${work}/derive.sh" ] || fail "no rollout-set block found in ${script}"

# The four deployments the cluster runs today, a sidecar deployment whose second
# container carries the image, a tag that is not `main-<rev>`, and two deployments
# on other images. The fixture is JSONPath output: one line per deployment, name,
# tab, then the container images separated by spaces.
cat >"${work}/fixture.live" <<'FIXTURE'
cogneva	localhost:30500/cogneva:main-c8e245c9978e
cogneva-evolution	localhost:30500/cogneva:main-c8e245c9978e
cogneva-sandbox-executor	localhost:30500/cogneva:main-c8e245c9978e
cogneva-security-gateway	localhost:30500/cogneva:main-c8e245c9978e
cogneva-registry	registry:2 localhost:30500/cogneva:local
qdrant	qdrant/qdrant:v1.13.4
meilisearch	getmeili/meilisearch:v1.10.3
FIXTURE

run_derivation() {
    FIXTURE="$1" NS=cogneva IMAGE="localhost:30500/cogneva:main-c8e245c9978e" \
        bash -c "source '${work}/derive.sh'; printf '%s\n' \"\${APP_DEPLOYMENTS[@]}\"" |
        sort
}

got="$(run_derivation "${work}/fixture.live")"
want="$(printf '%s\n' cogneva cogneva-evolution cogneva-registry cogneva-sandbox-executor cogneva-security-gateway | sort)"
[ "${got}" = "${want}" ] || fail "restarted set is wrong:
got:
${got}
want:
${want}"

# A mirror-prefixed registry of the same product is a different distribution: the
# live cluster names one, and a deployment outside it is not this restore's.
cat >"${work}/fixture.foreign" <<'FIXTURE'
cogneva	localhost:30500/cogneva:main-c8e245c9978e
mirrored	registry.example.cn/cogneva/cogneva:main-c8e245c9978e
FIXTURE
got="$(run_derivation "${work}/fixture.foreign")"
[ "${got}" = "cogneva" ] || fail "a deployment from a different registry was restarted: ${got}"

# Nothing matching has to be a failure, not an empty loop: a restore that restarts
# nothing looks exactly like a restore that had nothing to restart.
cat >"${work}/fixture.none" <<'FIXTURE'
qdrant	qdrant/qdrant:v1.13.4
FIXTURE
if run_derivation "${work}/fixture.none" >/dev/null 2>&1; then
    fail "an empty set is reported as success"
fi

echo "restore rollout set OK"
