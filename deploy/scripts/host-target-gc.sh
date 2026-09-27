#!/usr/bin/env bash
# Release the host's cargo build caches when the disk is above a trigger, down to
# a floor, cheapest tier first.
#
# Why this exists: cargo never removes the artifacts an earlier revision
# invalidated. An old rlib or test binary stays on disk forever, so a shared
# build cache only grows -- and the tempting remedy (wait until the disk is
# full, then delete the whole target) throws away the dependency artifacts a
# full rebuild needs, which is an order of magnitude more than the invalidated
# ones. Releasing in tiers keeps the expensive part and drops the cheap part.
#
# Tiers, cheapest first; every one of them stops as soon as the floor is met:
#   stale       artifacts older than the window -- nothing that old can still be
#               current unless the source has not moved, and nothing ran
#   incremental cargo's incremental caches: pure acceleration, cargo rebuilds
#               them, the dependency rlibs next to them stay
#   coverage    the llvm-cov profile directory: only the coverage gate pays
#   profile     the profile directory itself: this is the full-rebuild tier and
#               is why it is last
#
# What it refuses to do, each refusal a reading rather than a guess:
#   - deletes only directories carrying cargo's own cache tag, so a directory
#     that merely is named `target` is never a candidate;
#   - deletes only inside the configured work root, and only on the filesystem
#     the trigger was read from (freeing another filesystem would not move the
#     reading that fired, and the run would escalate for nothing);
#   - deletes nothing at all while a build may be writing into a tree. A process
#     that builds is recognised by its name and by its owner (the work root is
#     ours; a process of another uid cannot be writing it), and the tree it
#     writes is resolved from CARGO_TARGET_DIR, --target-dir or <cwd>/target;
#     any process of ours with a working directory or an open file descriptor
#     inside a tree counts as well. A process of ours whose files cannot be read
#     is not evidence of absence: it fails the whole run closed.
set -euo pipefail

readonly CARGO_CACHE_TAG_SIGNATURE='Signature: 8a477f597d28d172789f06886806bc55'
readonly TIERS='stale incremental coverage profile'

work_root="${COGNEVA_HOST_WORK_ROOT:-}"
trigger_pct="${COGNEVA_TARGET_GC_TRIGGER_PCT:-70}"
floor_pct="${COGNEVA_TARGET_GC_FLOOR_PCT:-50}"
stale_days="${COGNEVA_TARGET_GC_STALE_DAYS:-3}"
dry_run="${COGNEVA_TARGET_GC_DRY_RUN:-0}"
writer_processes="${COGNEVA_TARGET_GC_WRITER_PROCESSES:-cargo rustc rustdoc cc gcc g++ clang clang++ ld lld mold as make ninja cmake}"
# The reading the whole decision hangs on, and therefore the one thing a test
# has to be able to hand over: a command that prints "<used_kb> <percent>" for
# the path it is given. Left unset it is df. When it is set the run says so,
# because a run that decided on a substitute reading must not look like a run
# that decided on the disk.
disk_reader="${COGNEVA_TARGET_GC_DISK_READER:-}"
state_file="${COGNEVA_TARGET_GC_STATE:-${XDG_STATE_HOME:-${HOME:?}/.local/state}/cogneva/target-gc.json}"

log() { printf 'target-gc: %s\n' "$*"; }
die() { printf 'target-gc: %s\n' "$*" >&2; exit 1; }

# --- configuration -----------------------------------------------------------

[ -n "${work_root}" ] || die "COGNEVA_HOST_WORK_ROOT is unset: this script never guesses where the worktrees are"
[ -d "${work_root}" ] || die "COGNEVA_HOST_WORK_ROOT=${work_root} is not a directory"
# Resolve once so that paths read out of /proc compare against it textually.
work_root="$(cd "${work_root}" && pwd -P)"

case "${trigger_pct}${floor_pct}" in
*[!0-9]*) die "the trigger and the floor are percentages, got trigger=${trigger_pct} floor=${floor_pct}" ;;
esac
[ "${trigger_pct}" -gt "${floor_pct}" ] ||
    die "the trigger (${trigger_pct}%) must sit above the floor (${floor_pct}%): that gap is the hysteresis"
case "${stale_days}" in
*[!0-9]*) die "COGNEVA_TARGET_GC_STALE_DAYS is a whole number of days, got ${stale_days}" ;;
esac

# --- readings ----------------------------------------------------------------

# Sets disk_used_kb and disk_pct for the filesystem holding a path.
read_disk() {
    local reading
    if [ -n "${disk_reader}" ]; then
        reading="$("${disk_reader}" "$1")" || die "the configured disk reader reported no reading for $1"
    else
        reading="$(df -Pk -- "$1" | awk 'NR==2 { print $3, $5 }')"
    fi
    disk_used_kb="${reading%% *}"
    disk_pct="${reading##* }"
    disk_pct="${disk_pct%\%}"
    case "${disk_used_kb}" in '' | *[!0-9]*) die "the disk reading of $1 is not \"<used_kb> <percent>\": ${reading}" ;; esac
    case "${disk_pct}" in '' | *[!0-9]*) die "the disk reading of $1 is not \"<used_kb> <percent>\": ${reading}" ;; esac
}

human_kb() {
    awk -v k="$1" 'BEGIN {
        split("KiB MiB GiB TiB PiB", unit, " ")
        i = 1
        while (k >= 1024 && i < 5) { k /= 1024; i++ }
        printf "%.1f%s", k, unit[i]
    }'
}

# Which filesystem a path is on, as the mount table spells it. A device number
# would be the natural identity, but the number a process is handed for the same
# filesystem is not the same number in every process -- a path compared against
# a path read earlier can differ on a device that never changed. The source and
# the mount point are text, and text compares.
fs_identity() {
    df -P -- "$1" 2>/dev/null | awk 'NR == 2 { print $1 "|" $6 }'
}

# The paths a live process of ours sits in or holds open: the working directory
# and every open file descriptor, resolved. `(deleted)` marks a file that is
# already unlinked; the path before it is still the tree it belonged to.
process_paths() {
    find /proc/[0-9]* -user "$(id -u)" -maxdepth 2 \
        \( -path '*/cwd' -o -path '*/fd/*' \) -type l \
        -printf '%p\t%l\n' 2>/dev/null || true
}

# The directory a building process writes into: --target-dir, or
# CARGO_TARGET_DIR, or <cwd>/target. A relative one resolves against its cwd.
build_dir_of() {
    local pid="$1" cwd="$2" dir=""
    local argv=()
    mapfile -d '' -t argv <"/proc/${pid}/cmdline" 2>/dev/null || true
    set -- "${argv[@]+"${argv[@]}"}"
    while [ "$#" -gt 0 ]; do
        case "$1" in
        --target-dir=*) dir="${1#--target-dir=}" ;;
        --target-dir)
            shift
            [ "$#" -gt 0 ] && dir="$1"
            ;;
        esac
        shift
    done
    if [ -z "${dir}" ]; then
        dir="$(tr '\0' '\n' <"/proc/${pid}/environ" 2>/dev/null | sed -n 's/^CARGO_TARGET_DIR=//p' | head -1 || true)"
    fi
    [ -n "${dir}" ] || dir="target"
    case "${dir}" in
    /*) ;;
    *) dir="${cwd}/${dir}" ;;
    esac
    realpath -m -- "${dir}" 2>/dev/null || printf '%s\n' "${dir}"
}

# Every reason to leave a tree alone, as `path<TAB>pid<TAB>reason`. A process of
# ours whose own files cannot be read goes to the second file: that is a failure
# to attribute, which is not the same reading as "nobody is writing there".
collect_process_views() {
    local busy="$1" unattributable="$2"
    local pid cwd dir name
    local -a writers=()
    read -r -a writers <<<"${writer_processes}"
    : >"${busy}"
    : >"${unattributable}"

    for name in "${writers[@]}"; do
        while IFS= read -r pid; do
            [ -n "${pid}" ] || continue
            # A process that exited between the listing and this read is not a
            # failure to attribute.
            [ -d "/proc/${pid}" ] || continue
            if ! cwd="$(readlink "/proc/${pid}/cwd" 2>/dev/null)"; then
                printf '%s\t%s\tcwd of %s is unreadable\n' "${pid}" "${name}" "${name}" >>"${unattributable}"
                continue
            fi
            dir="$(build_dir_of "${pid}" "${cwd}")"
            printf '%s\t%s\tbuilds (%s)\n' "${dir}" "${pid}" "${name}" >>"${busy}"
        done < <(pgrep -u "$(id -u)" -x "${name}" 2>/dev/null || true)
    done

    process_paths |
        awk -F'\t' -v root="${work_root}/" -v bare="${work_root}" '
            {
                path = $2
                sub(/ \(deleted\)$/, "", path)
                if (path == bare || index(path, root) == 1) {
                    split($1, part, "/")
                    printf "%s\t%s\tholds it open\n", path, part[3]
                }
            }' >>"${busy}"
}

# A reason if anything is using the tree or writing into it, empty otherwise.
busy_reason() {
    awk -F'\t' -v t="$1" '
        # Inside the tree always counts. Containing the tree only counts for a
        # process that builds: it was told to write into that directory, whereas
        # any process sitting in a worktree -- a shell, an editor -- holds the
        # tree only as a parent and must not block its cache forever.
        $1 == t || index($1, t "/") == 1 || ($3 ~ /^builds/ && index(t, $1 "/") == 1) {
            printf "%s %s", $2, $3
            exit
        }
    ' "$2"
}

# --- candidates --------------------------------------------------------------

# A cache is a directory cargo tagged as one: the name is not the evidence.
is_cargo_cache() {
    local tag="$1/CACHEDIR.TAG"
    [ -f "${tag}" ] || return 1
    local signature
    IFS= read -r signature <"${tag}" || true
    [ "${signature}" = "${CARGO_CACHE_TAG_SIGNATURE}" ]
}

find_candidates() {
    # Depth 5 reaches a worktree nested inside a repository (its own worktrees).
    # Sorted, because the order decides which trees are reached before the floor
    # ends the pass, and readdir order is a property of the filesystem rather
    # than of any decision: two machines in the same state would release
    # different files. The locale is pinned with it so the order is the bytes.
    find "${work_root}" -maxdepth 5 -type d -name target -prune -print 2>/dev/null |
        LC_ALL=C sort |
        while IFS= read -r dir; do
            is_cargo_cache "${dir}" && printf '%s\n' "${dir}"
        done
}

# --- release -----------------------------------------------------------------

tier_stale() {
    local minutes=$((stale_days * 24 * 60))
    if [ "${dry_run}" = 1 ]; then
        find "$1" -type f -mmin "+${minutes}" -printf '%s\t%p\n' 2>/dev/null || true
        return 0
    fi
    find "$1" -type f -mmin "+${minutes}" -delete 2>/dev/null || true
    find "$1" -depth -type d -empty -delete 2>/dev/null || true
}

tier_dirs_named() { # target, directory name
    local dir
    while IFS= read -r dir; do
        [ -n "${dir}" ] || continue
        if [ "${dry_run}" = 1 ]; then
            log "dry run would release ${dir}"
        else
            log "releasing $1: ${dir}"
            rm -rf -- "${dir}"
        fi
    done < <(find "$1" -maxdepth 3 -type d -name "$2" -prune -print 2>/dev/null || true)
}

# A profile directory is the one holding deps/: that is the structure cargo
# makes, and it holds for a nested target directory (a triple, llvm-cov) too.
tier_profile() {
    local profile
    while IFS= read -r profile; do
        [ -n "${profile}" ] || continue
        if [ "${dry_run}" = 1 ]; then
            log "dry run would release ${profile} (full rebuild of this tree)"
        else
            log "releasing $1: ${profile} (full rebuild of this tree)"
            rm -rf -- "${profile}"
        fi
    done < <(find "$1" -maxdepth 3 -type d -name deps -prune -printf '%h\n' 2>/dev/null || true)
}

release_tier() { # tier, target
    case "$1" in
    stale) tier_stale "$2" ;;
    incremental) tier_dirs_named "$2" incremental ;;
    coverage) tier_dirs_named "$2" llvm-cov-target ;;
    profile) tier_profile "$2" ;;
    *) die "unknown tier $1" ;;
    esac
}

write_state() { # outcome, released_kb, gap_pct, targets, skipped, unreadable
    mkdir -p -- "$(dirname -- "${state_file}")"
    local tmp
    tmp="$(mktemp "${state_file}.XXXXXX")"
    cat >"${tmp}" <<EOF
{"last_run":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","outcome":"$1","trigger_pct":${trigger_pct},"floor_pct":${floor_pct},"used_pct_before":${before_pct},"used_pct_after":${disk_pct},"released_bytes":$(( $2 * 1024 )),"gap_pct":$3,"targets":$4,"skipped":$5,"unreadable":$6,"dry_run":${dry_run}}
EOF
    mv -f -- "${tmp}" "${state_file}"
}

# One reading per run, whatever it decided: the three quantities an operator
# needs are how much was released, where the filesystem ended up, and how far
# from the floor it stopped.
report() { # outcome, released_kb, covered, skipped, unreadable
    local gap=0
    [ "${disk_pct}" -gt "${floor_pct}" ] && gap=$((disk_pct - floor_pct))
    printf 'target-gc: outcome=%s trigger=%s%% floor=%s%% before=%s%% after=%s%% released=%s gap=%s%% caches=%s skipped=%s unreadable=%s\n' \
        "$1" "${trigger_pct}" "${floor_pct}" "${before_pct}" "${disk_pct}" \
        "$(human_kb "$2")" "${gap}" "$3" "$4" "$5"
    write_state "$1" "$2" "${gap}" "$3" "$4" "$5"
}

# --- run ---------------------------------------------------------------------

mkdir -p -- "$(dirname -- "${state_file}")"
exec 9>"${state_file}.lock"
flock -n 9 || die "another run holds the lock"

read_disk "${work_root}"
[ -z "${disk_reader}" ] || log "warning: the disk reading comes from ${disk_reader}, not from the filesystem"
before_pct="${disk_pct}"
before_used_kb="${disk_used_kb}"
readonly before_pct before_used_kb
work_fs="$(fs_identity "${work_root}")"
[ -n "${work_fs}" ] || die "cannot read which filesystem ${work_root} is on"

if [ "${disk_pct}" -lt "${trigger_pct}" ]; then
    log "below trigger: ${disk_pct}% used, trigger ${trigger_pct}%"
    report below_trigger 0 0 0 0
    exit 0
fi

mapfile -t candidates < <(find_candidates)
skipped=0
covered=0
for target in "${candidates[@]}"; do
    if [ "$(fs_identity "${target}")" != "${work_fs}" ]; then
        log "skipping ${target}: another filesystem (the trigger was read from ${work_root})"
        skipped=$((skipped + 1))
        continue
    fi
    covered=$((covered + 1))
done

if [ "${covered}" -eq 0 ]; then
    log "nothing to release: ${disk_pct}% used, trigger ${trigger_pct}%, no cargo cache under ${work_root} -- the cache is not what is filling this filesystem"
    report floor_unreachable 0 "${#candidates[@]}" "${skipped}" 0
    exit 0
fi

if [ "${dry_run}" = 1 ]; then
    log "dry run: ${covered} cache(s) under ${work_root}, ${disk_pct}% used, trigger ${trigger_pct}%, floor ${floor_pct}% -- nothing is released, so the plan runs to the deepest tier a real run would reach"
    for target in "${candidates[@]}"; do
        log "candidate ${target}: $(human_kb "$(du -sk -- "${target}" 2>/dev/null | cut -f1)") on disk"
    done
fi

released_dir="$(mktemp -d)"
trap 'rm -rf -- "${released_dir}"' EXIT
outcome="floor_unreachable"
for tier in ${TIERS}; do
    collect_process_views "${released_dir}/busy" "${released_dir}/unattributable"
    unreadable="$(wc -l <"${released_dir}/unattributable")"
    if [ "${unreadable}" -gt 0 ]; then
        log "a process of ours is unreadable, so no tree can be cleared:"
        sed 's/^/target-gc:   /' "${released_dir}/unattributable"
        outcome="probe_failed"
        break
    fi

    progressed=0
    for target in "${candidates[@]}"; do
        [ "${disk_pct}" -gt "${floor_pct}" ] || break
        if [ "$(fs_identity "${target}")" != "${work_fs}" ]; then
            continue
        fi
        reason="$(busy_reason "${target}" "${released_dir}/busy")"
        if [ -n "${reason}" ]; then
            log "leaving ${target} alone: pid ${reason}"
            continue
        fi
        progressed=1
        any_progress=1
        release_tier "${tier}" "${target}"
        read_disk "${work_root}"
    done

    if [ "${disk_pct}" -le "${floor_pct}" ]; then
        outcome="released"
        break
    fi
    if [ "${progressed}" = 0 ]; then
        log "tier ${tier}: every candidate is in use, nothing released in this tier"
    fi
done

released_kb=$((before_used_kb - disk_used_kb))
[ "${released_kb}" -gt 0 ] || released_kb=0

if [ "${outcome}" != probe_failed ]; then
    if [ "${disk_pct}" -le "${floor_pct}" ]; then
        outcome="released"
    elif [ "${any_progress:-0}" = 1 ]; then
        outcome="floor_unreachable"
    fi
fi

gap=0
[ "${disk_pct}" -gt "${floor_pct}" ] && gap=$((disk_pct - floor_pct))

printf 'target-gc: outcome=%s trigger=%s%% floor=%s%% before=%s%% after=%s%% released=%s gap=%s%% caches=%s skipped=%s unreadable=%s\n' \
    "${outcome}" "${trigger_pct}" "${floor_pct}" "${before_pct}" "${disk_pct}" \
    "$(human_kb "${released_kb}")" "${gap}" "${covered}" "${skipped}" "${unreadable:-0}"
if [ "${outcome}" = floor_unreachable ]; then
    log "still above the trigger after every tier: the build cache is not what is filling this filesystem (gap ${gap} points)"
fi
write_state "${outcome}" "${released_kb}" "${gap}" "${covered}" "${skipped}" "${unreadable:-0}"
