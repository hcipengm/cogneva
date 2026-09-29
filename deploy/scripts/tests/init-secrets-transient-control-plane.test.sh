#!/usr/bin/env bash
# Install-time key init against a control plane that is not settled yet.
#
# Background: on a blank machine the bootstrap imports a ~550MB image into
# containerd and then runs init-secrets.sh while the machine is still writing
# those pages back. The fifth key's `patch` came back
#
#   Error from server (Timeout): request did not complete within requested
#   timeout - context deadline exceeded
#
# and `set -e` ended the script with rc=1 -- the whole install was reported as
# failed, while the same machine finished converging minutes later (12/12 pods
# Running, /health/ready 200). "Not finished yet" was read as "failed".
#
# The judgement under test is the split between two failure classes:
#   * timeout / cannot-connect  -> the control plane is slow: retry (bounded);
#   * a real rejection (invalid, forbidden) -> retry changes nothing: the very
#     first answer is the answer, and the error is not stretched out.
# And a third one, on the read side: "could not read" must not be read as "not
# there", because ensure_random would then mint a new value for a key that a
# running workload may be using -- and nothing about that write reports itself.
#
# No cluster is touched: a stub `kubectl` goes on the front of PATH, its state
# lives in a temporary directory, and it can be told to fail a chosen call a
# chosen number of times.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPT="${ROOT}/deploy/scripts/init-secrets.sh"
FAKE="$(mktemp -d)"
trap 'rm -rf "${FAKE}"' EXIT

fails=0
check() { # check <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1: expected [$2] got [$3]"
        fails=$((fails + 1))
    fi
}
check_contains() { # check_contains <description> <whole output> <substring>
    case "$2" in
        *"$3"*) echo "ok   - $1" ;;
        *) echo "FAIL - $1: [$3] not in the output"$'\n'"--- actual output ---"$'\n'"$2"; fails=$((fails + 1)) ;;
    esac
}
check_not_contains() { # check_not_contains <description> <whole output> <substring>
    case "$2" in
        *"$3"*) echo "FAIL - $1: [$3] appeared in the output"$'\n'"--- actual output ---"$'\n'"$2"; fails=$((fails + 1)) ;;
        *) echo "ok   - $1" ;;
    esac
}

# Stub kubectl: the verbs init-secrets.sh uses, plus failure injection.
#   FAKE_FAIL_ON     substring of the command line that should fail
#   FAKE_FAIL_TIMES  how many of those calls fail before one goes through
#   FAKE_FAIL_MSG    what the failing call writes to stderr
# Every call is appended to ${state}/calls, so the test can count attempts.
cat > "${FAKE}/kubectl" <<'EOF'
#!/usr/bin/env bash
set -u
state="${FAKE_STATE:?}"
mkdir -p "${state}/data"

args=("$@")
rest=(); for ((i = 0; i < ${#args[@]}; i++)); do
    if [ "${args[i]}" = "-n" ]; then i=$((i + 1)); continue; fi
    rest+=("${args[i]}")
done
# verb first, so a count can say "how many patches for this key" rather than
# "how many calls that mention this key" (the read mentions it too).
printf '%s|%s\n' "${rest[0]:-}" "$*" >> "${state}/calls"

if [ -n "${FAKE_FAIL_ON:-}" ]; then
    case "$*" in
    *"${FAKE_FAIL_ON}"*)
        n=0
        [ -f "${state}/failcount" ] && n="$(cat "${state}/failcount")"
        if [ "${n}" -lt "${FAKE_FAIL_TIMES:-0}" ]; then
            echo "$((n + 1))" > "${state}/failcount"
            printf '%s\n' "${FAKE_FAIL_MSG:-boom}" >&2
            exit 1
        fi
        ;;
    esac
fi

case "${rest[0]:-}" in
create)
    case "${rest[1]:-}" in
    namespace) exit 0 ;;
    secret) touch "${state}/secret"; exit 0 ;;
    esac
    exit 0
    ;;
apply)
    cat >/dev/null; exit 0
    ;;
get)
    [ "${rest[1]:-}" = "secret" ] || exit 0
    [ -f "${state}/secret" ] || exit 1
    path=""
    for ((i = 0; i < ${#rest[@]}; i++)); do
        [ "${rest[i]}" = "-o" ] && path="${rest[i + 1]}"
    done
    [ -n "${path}" ] || exit 0
    key="${path##*.data.}"; key="${key%\}}"
    [ -f "${state}/data/${key}" ] && cat "${state}/data/${key}"
    exit 0
    ;;
patch)
    [ "${rest[1]:-}" = "secret" ] || exit 0
    patch=""
    for ((i = 0; i < ${#rest[@]}; i++)); do
        case "${rest[i]}" in
        -p=*) patch="${rest[i]#-p=}" ;;
        -p) patch="${rest[i + 1]}" ;;
        esac
    done
    python3 - "${state}" "${patch}" <<'PY'
import json, os, sys
state, patch = sys.argv[1], sys.argv[2]
try:
    data = json.loads(patch)["data"]
except Exception:
    sys.exit(0)
for key, value in data.items():
    if isinstance(value, str):
        open(os.path.join(state, "data", key), "w").write(value)
PY
    exit 0
    ;;
esac
exit 0
EOF
chmod +x "${FAKE}/kubectl"

# One install: <state dir> <host copy dir> <fail-on> <fail-times> <fail-msg>.
# Sleep is turned off: the retry budget is what is under test, not the clock.
run_install() {
    local state="$1" hostdir="$2" failon="${3:-}" times="${4:-0}" msg="${5:-}"
    env -i PATH="${FAKE}:${PATH}" HOME="${FAKE}/home" \
        FAKE_STATE="${state}" \
        FAKE_FAIL_ON="${failon}" \
        FAKE_FAIL_TIMES="${times}" \
        FAKE_FAIL_MSG="${msg}" \
        COGNEVA_HOST_STATE_DIR="${hostdir}" \
        COGNEVA_NS="cogneva" \
        COGNEVA_KUBECTL_RETRY_SLEEP=0 \
        bash "${SCRIPT}" 2>&1
}

calls_matching() { # calls_matching <state> <verb> <substring>
    [ -f "$1/calls" ] || { echo 0; return; }
    grep -F -- "$3" "$1/calls" 2>/dev/null \
        | grep -c -F -- "$2|" \
        || true
}

mkdir -p "${FAKE}/home"

TIMEOUT_MSG='Error from server (Timeout): Timeout: request did not complete within requested timeout - context deadline exceeded'
INVALID_MSG='Error from server (Invalid): Secret "cogneva-secrets" is invalid'

# ---------- 1) the incident: the fifth key's patch times out, twice ----------
# This is the call that ended the install. With the retry it has to go through
# and the key has to end up in the Secret.
s1="${FAKE}/s1"
set +e
out="$(run_install "${s1}" "${FAKE}/h1" '{"data":{"evolution-jwt-secret"' 2 "${TIMEOUT_MSG}")"; rc=$?
set -e
check "a timed-out patch no longer ends the install" "0" "${rc}"
check "the timed-out key is written in the end" "yes" \
    "$([ -s "${s1}/data/evolution-jwt-secret" ] && echo yes || echo no)"
check_contains "the retry says the control plane is the reason" "${out}" "控制面还没稳"
check "the patch was really attempted three times" "3" \
    "$(calls_matching "${s1}" patch 'evolution-jwt-secret')"
check_contains "the keys after it are still written" "${out}" "meili-master-key: 已生成随机强密钥"

# ---------- 2) a real rejection is not retried ----------
# "Invalid" is an answer, not a delay. Retrying it would only make the install
# take longer to say the same thing.
s2="${FAKE}/s2"
set +e
out="$(run_install "${s2}" "${FAKE}/h2" '{"data":{"evolution-jwt-secret"' 99 "${INVALID_MSG}")"; rc=$?
set -e
[ "${rc}" -ne 0 ] || { echo "FAIL - an invalid patch still ended with rc=0"; fails=$((fails + 1)); }
check "the rejected patch was attempted exactly once" "1" \
    "$(calls_matching "${s2}" patch 'evolution-jwt-secret')"
check_not_contains "no retry is announced for a rejection" "${out}" "控制面还没稳"
check "nothing was written for the rejected key" "no" \
    "$([ -s "${s2}/data/evolution-jwt-secret" ] && echo yes || echo no)"

# ---------- 3) "could not read" is not "not there" ----------
# A key that is already in the Secret, read while the control plane is slow:
# the read has to be retried, and the existing value has to survive. Reading a
# failure as "absent" would mint a new password for a key a running workload
# may be using, and nothing about that write says so.
s3="${FAKE}/s3"
mkdir -p "${s3}/data"
touch "${s3}/secret"
existing="$(printf '%s' 'keep-this-password' | base64 | tr -d '\n')"
printf '%s' "${existing}" > "${s3}/data/pg-password"
set +e
out="$(run_install "${s3}" "${FAKE}/h3" 'jsonpath={.data.pg-password}' 2 "${TIMEOUT_MSG}")"; rc=$?
set -e
check "a slow read does not end the install either" "0" "${rc}"
check "the existing value is not rotated" "${existing}" "$(cat "${s3}/data/pg-password")"
check_contains "the read was retried, not given up on" "${out}" "控制面还没稳"
check_contains "the key is reported as kept" "${out}" "pg-password: 已存在，保留不动"
check_not_contains "no new value is announced for it" "${out}" "pg-password: 已生成随机强密钥"

# ---------- 4) control for 3: a read that really fails stops the install ----------
# Without this, "the value survived" above could just mean the script never
# writes anything when a read fails for any reason -- including the one where
# proceeding would rotate a live password.
s4="${FAKE}/s4"
mkdir -p "${s4}/data"
touch "${s4}/secret"
printf '%s' "${existing}" > "${s4}/data/pg-password"
set +e
out="$(run_install "${s4}" "${FAKE}/h4" 'jsonpath={.data.pg-password}' 99 'Error from server (Forbidden): secrets "cogneva-secrets" is forbidden')"; rc=$?
set -e
[ "${rc}" -ne 0 ] || { echo "FAIL - an unreadable Secret still ended with rc=0"; fails=$((fails + 1)); }
check_contains "the refusal names the read as the reason" "${out}" "读不到"
check_contains "the refusal says why absent is not the reading" "${out}" "不等于"
check "the unreadable key was not overwritten" "${existing}" "$(cat "${s4}/data/pg-password")"
check "a forbidden read is not retried" "1" \
    "$(calls_matching "${s4}" get 'jsonpath={.data.pg-password}')"

# ---------- 5) a failing step still ends the install ----------
# The retry wrappers must not turn "the step failed" into "the step passed":
# reading the dry run into a variable is what keeps its exit code observable
# (inside `$( )` it would be printf's). If that were lost, this run would go on
# and end with rc=0 while never having made the namespace.
s5="${FAKE}/s5"
set +e
out="$(run_install "${s5}" "${FAKE}/h5" 'create namespace cogneva' 99 'Error from server (Forbidden): namespaces "cogneva" is forbidden')"; rc=$?
set -e
[ "${rc}" -ne 0 ] || { echo "FAIL - a failed namespace step still ended with rc=0"; fails=$((fails + 1)); }
check "nothing was written after the failed step" "no" \
    "$([ -s "${s5}/data/pg-password" ] && echo yes || echo no)"
check_contains "the failure that stopped it is on the terminal" "${out}" "is forbidden"

if [ "${fails}" -ne 0 ]; then
    echo "FAILED: ${fails} assertion(s)"
    exit 1
fi
echo "PASS: a slow control plane is waited out, a real refusal is not, and an unreadable key is never replaced"
