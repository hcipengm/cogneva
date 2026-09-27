#!/usr/bin/env bash
# The host copy of the instance identity: the judgement is "is this still the
# same instance after a reinstall".
#
# Background: the reinstall at 2026-09-23T14:19:19Z rebuilt the namespace and
# `cogneva-secrets` together, `instance-fingerprint` was not carried over, and
# `Vera#9ff96101` became `Luna#1822ab0e`. The fingerprint is the input to the
# name and the name is the input to the git author, so "the Secret was not
# carried over" looks like "the author of every self-authored change is someone
# new" in the repository -- and whoever traces those changes by author finds
# none of them.
#
# This gate does not check that the Secret has a value (plainly visible). It
# checks that the same identity can be installed again after the namespace has
# been wiped. Two cases must stay apart:
#   1. nothing on the cluster, host copy present -> the copy is what comes back;
#   2. nothing on the cluster, host copy damaged -> refuse, rather than mint a
#      new one (which is the silent rename).
# No real cluster is touched: a bookkeeping stub `kubectl` goes on the front of
# PATH and its state lives in a temporary directory.

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

# Stub kubectl: only what init-secrets.sh uses. The Secret's state is a
# directory with one file per key, holding base64 -- the same shape as a real
# Secret's data, so the `base64 -d` steps in the script take the same path they
# would take against a cluster.
cat > "${FAKE}/kubectl" <<'EOF'
#!/usr/bin/env bash
set -u
state="${FAKE_STATE:?}"
mkdir -p "${state}/data"

args=("$@")
ns=""; for ((i = 0; i < ${#args[@]}; i++)); do
    [ "${args[i]}" = "-n" ] && ns="${args[i + 1]}"
done
# the positional arguments with `-n <ns>` taken out
rest=(); for ((i = 0; i < ${#args[@]}; i++)); do
    if [ "${args[i]}" = "-n" ]; then i=$((i + 1)); continue; fi
    rest+=("${args[i]}")
done

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
    # The script builds `-p={...}` (the JSON follows the equals sign in the same
    # argument), so both shapes have to be picked up.
    patch=""
    for ((i = 0; i < ${#rest[@]}; i++)); do
        case "${rest[i]}" in
        -p=*) patch="${rest[i]#-p=}" ;;
        -p) patch="${rest[i + 1]}" ;;
        esac
    done
    # Only the "key":"b64" pairs are needed; the JSON the script builds has a
    # fixed shape.
    python3 - "${state}" "${patch}" <<'PY'
import json, os, sys
state, patch = sys.argv[1], sys.argv[2]
try:
    data = json.loads(patch)["data"]
except Exception:
    sys.exit(0)
for key, value in data.items():
    if isinstance(value, str) and value:
        open(os.path.join(state, "data", key), "w").write(value)
PY
    exit 0
    ;;
esac
exit 0
EOF
chmod +x "${FAKE}/kubectl"

# One install: <state dir> <host copy dir>. Output is returned verbatim so the
# assertions can read it.
run_install() {
    local state="$1" hostdir="$2"
    env -i PATH="${FAKE}:${PATH}" HOME="${FAKE}/home" \
        FAKE_STATE="${state}" \
        COGNEVA_HOST_STATE_DIR="${hostdir}" \
        COGNEVA_NS="cogneva" \
        bash "${SCRIPT}" 2>&1
}

fingerprint_in_secret() {
    [ -f "$1/data/instance-fingerprint" ] || return 1
    base64 -d < "$1/data/instance-fingerprint" 2>/dev/null
}

mkdir -p "${FAKE}/home"
GOOD="$(printf 'ab%.0s' $(seq 1 32))"   # 64 hex digits

# ---------- 1) first install: mint a new identity and leave a host copy ----------
s1="${FAKE}/s1"; h1="${FAKE}/h1"
out="$(run_install "${s1}" "${h1}")"
check_contains "a first install mints a new fingerprint" "${out}" "生成了新实例指纹"
check_contains "a first install says this is not a reinstall" "${out}" "第一次安装"
minted="$(fingerprint_in_secret "${s1}")"
check "the minted fingerprint is 64 hex digits" "yes" \
    "$(printf '%s' "${minted}" | grep -qE '^[0-9a-f]{64}$' && echo yes || echo no)"
check "the host copy is the same one as in the Secret" "${minted}" "$(tr -d '[:space:]' < "${h1}/instance-fingerprint")"
check "the host copy is 0600" "600" "$(stat -c '%a' "${h1}/instance-fingerprint")"

# ---------- 2) reinstall: namespace and Secret wiped, host copy survives ----------
# This is the shape of the 2026-09-23 event; the judgement is that what comes
# back has to be the original.
s2="${FAKE}/s2"; h2="${FAKE}/h2"
mkdir -p "${h2}"
printf '%s\n' "${GOOD}" > "${h2}/instance-fingerprint"
out="$(run_install "${s2}" "${h2}")"
check_contains "a reinstall restores from the host copy" "${out}" "已从宿主保留副本恢复"
check "what comes back is the host copy, not a freshly minted one" "${GOOD}" "$(fingerprint_in_secret "${s2}")"
check_not_contains "a reinstall mints no new identity" "${out}" "生成了新实例指纹"

# ---------- 3) damaged host copy: refuse, do not mint one silently ----------
s3="${FAKE}/s3"; h3="${FAKE}/h3"
mkdir -p "${h3}"
printf '%s\n' "这不是一个指纹" > "${h3}/instance-fingerprint"
set +e
out="$(run_install "${s3}" "${h3}")"; rc=$?
set -e
[ "${rc}" -ne 0 ] || { echo "FAIL - the host copy was unusable and the run still succeeded"; fails=$((fails + 1)); }
check_contains "the refusal names the shape as the reason" "${out}" "不是一个指纹"
check_contains "the refusal says how to confirm a new identity" "${out}" "清空它并重跑"
check "the refusal wrote no fingerprint into the Secret" "no" \
    "$([ -f "${s3}/data/instance-fingerprint" ] && echo yes || echo no)"

# ---------- 4) Secret still there, host copy gone: keep it and back the copy up ----------
# An instance installed before this change has this shape; without the backfill
# its next reinstall renames it just the same.
s4="${FAKE}/s4"; h4="${FAKE}/h4"
mkdir -p "${s4}/data"
printf '%s' "$(printf '%s' "${GOOD}" | base64 | tr -d '\n')" > "${s4}/data/instance-fingerprint"
touch "${s4}/secret"
out="$(run_install "${s4}" "${h4}")"
check_contains "an existing Secret is kept as it is" "${out}" "已存在，保留不动"
check "the backfilled host copy matches the identity in use" "${GOOD}" "$(tr -d '[:space:]' < "${h4}/instance-fingerprint" 2>/dev/null)"

# ---------- 5) self-check: the judgement can catch what it is meant to catch ----------
# Replay the restore step with no host copy: it has to fall through to minting.
# Otherwise "no new identity was reported" above could just mean the script
# never reports one, rather than that it restored.
s5="${FAKE}/s5"
out="$(run_install "${s5}" "${FAKE}/h5-does-not-exist")"
check_contains "control: with no copy it mints" "${out}" "生成了新实例指纹"

if [ "${fails}" -ne 0 ]; then
    echo "FAILED: ${fails} assertion(s)"
    exit 1
fi
echo "PASS: the identity survives a reinstall through the host copy, and a damaged copy is refused rather than silently renamed"
