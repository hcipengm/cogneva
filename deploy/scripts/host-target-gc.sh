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
# COGNEVA_TARGET_GC_DRY_RUN=1 prints the plan and releases nothing. It answers
# the same way at any water level -- a plan is a question about what a run would
# do, not about whether today is the day -- and it reports `planned` rather than
# `released`, because a reading that cannot tell those apart is worse than none.
#
# What it refuses to do, each refusal a reading rather than a guess:
#   - deletes only directories carrying cargo's own cache tag, so a directory
#     that merely is named `target` is never a candidate -- and never deletes the
#     tag itself, which is the tree's identity rather than cache data. Cargo
#     writes that file once, when it makes the tree, and never writes it again:
#     a run that removed it would leave a cache no later run can judge and no
#     rebuild repairs. A tree that looks like a cache and has no tag is named in
#     the reading, never guessed at -- the tag is the evidence, and a blind spot
#     is a thing the reader has to be able to see;
#   - deletes only on the filesystem the trigger was read from, and only inside
#     a directory carrying that tag: freeing another filesystem would not move
#     the reading that fired, and the tag is what makes a directory cargo's
#     rather than someone's. The configured root names the filesystem to judge,
#     not the set of caches to judge -- a cache it does not happen to sit above
#     is still a cache on that disk. A run may be narrowed to one subtree, and a
#     narrowed run says so: otherwise "no cache here" and "a cache here that
#     this run was not allowed to reach" read the same;
#   - deletes nothing at all while a build may be writing into a tree. A process
#     that builds is recognised by its name and by its owner (the judged caches
#     are ours; a process of another uid cannot be writing them), and the tree
#     it writes is resolved from CARGO_TARGET_DIR, --target-dir or
#     <cwd>/target; any process of ours with a working directory or an open file
#     descriptor inside a tree counts as well. A process of ours whose files
#     cannot be read is not evidence of absence: it fails the whole run closed.
#   - deletes nothing unless the file about to run is exactly the committed
#     version of it. The unit points straight at the checkout, which is what
#     keeps the script from ever going stale -- and the same property means an
#     edit in progress is the code that runs, on the day the disk is full enough
#     to delete. So an identity that cannot be established is a reason not to
#     act: the disk waits for a commit, which is minutes, while a deletion
#     cannot be undone at all. Every run reports the version it ran either way,
#     including the ones that release nothing, because a reading that cannot be
#     tied to a version cannot be reviewed afterwards.
set -euo pipefail

readonly CARGO_CACHE_TAG_SIGNATURE='Signature: 8a477f597d28d172789f06886806bc55'
readonly TIERS='stale incremental coverage profile'

work_root="${COGNEVA_HOST_WORK_ROOT:-}"
delete_under="${COGNEVA_TARGET_GC_DELETE_UNDER:-}"
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
# This file is the code that deletes, so it is also the thing whose identity the
# readings carry. Not configured: a path that is passed in could disagree with
# the path that was executed, which is the one fact this has to be right about.
script_path="${BASH_SOURCE[0]}"

log() { printf 'target-gc: %s\n' "$*"; }
die() { printf 'target-gc: %s\n' "$*" >&2; exit 1; }

# The version of the file being executed, as two words: the commit it came from
# (`none` when that cannot be read) and whether the file is exactly that commit's
# version of it (`committed`, or the reason it cannot be said). Run against a
# clean checkout the answer is `<sha> committed`; against an edit in progress it
# is `<sha> modified`. Both are readings; only the first is allowed to delete.
script_identity() {
    local here dir top head rel blob
    if ! here="$(realpath -- "${script_path}" 2>/dev/null)"; then
        printf 'none unreadable\n'
        return 0
    fi
    dir="$(dirname -- "${here}")"
    if ! command -v git >/dev/null 2>&1; then
        printf 'none nogit\n'
        return 0
    fi
    if ! top="$(git -C "${dir}" rev-parse --show-toplevel 2>/dev/null)"; then
        printf 'none norepo\n'
        return 0
    fi
    if ! head="$(git -C "${top}" rev-parse HEAD 2>/dev/null)"; then
        printf 'none nohead\n'
        return 0
    fi
    head="${head:0:12}"
    rel="${here#"${top}"/}"
    if ! blob="$(git -C "${top}" rev-parse "HEAD:${rel}" 2>/dev/null)"; then
        printf '%s notracked\n' "${head}"
        return 0
    fi
    if [ "$(git -C "${top}" hash-object -- "${here}" 2>/dev/null)" = "${blob}" ]; then
        printf '%s committed\n' "${head}"
    else
        printf '%s modified\n' "${head}"
    fi
}

# --- configuration -----------------------------------------------------------

[ -n "${work_root}" ] || die "COGNEVA_HOST_WORK_ROOT is unset: this script never guesses which filesystem to judge"
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

# The mount point out of a `source|mountpoint` identity.
fs_mount_point() {
    printf '%s\n' "${1#*|}"
}

# How many path components a path has below one of its ancestors. Used to keep
# the scan's reach equal to what it was when it started somewhere else.
depth_below() { # ancestor, path
    local rel="${2#"${1}"}"
    rel="${rel#/}"
    [ -n "${rel}" ] || {
        printf '0\n'
        return 0
    }
    printf '%s\n' "$(($(printf '%s' "${rel}" | tr -cd '/' | wc -c) + 1))"
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
#
# The held-open leg is not filtered to any set of directories before it is
# written: the caches are enumerated from the judged filesystem, so which trees
# matter is not known until then, and the membership test is the one
# `busy_reason` performs -- per tree, at the moment it decides. A filter here
# would be a second copy of that test, and a second copy is one that can
# disagree with the first.
collect_process_views() {
    local busy="$1" unattributable="$2"
    local pid cwd dir name state
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
                # A process that ended while this pass was reading it is not a
                # failure to attribute. The kernel tears down the address space
                # before the pid is reaped, so `/proc/<pid>/cwd` stops being
                # readable a moment before `/proc/<pid>` disappears -- and a
                # build is processes ending constantly, so an abort here would
                # fire on ordinary churn, on exactly the runs where the disk is
                # full enough to act. The state line is what tells the two
                # apart: `Z` is a process that has ended and is not yet reaped,
                # and a stat line that is gone too is one already reaped. A
                # process that is still there and still unreadable stays a
                # failure, because that is a tree this run cannot attribute.
                state="$(sed -n 's/^[^)]*) \(.\).*/\1/p' "/proc/${pid}/stat" 2>/dev/null || true)"
                if [ -z "${state}" ] || [ "${state}" = Z ]; then
                    continue
                fi
                printf '%s\t%s\tcwd of %s is unreadable\n' "${pid}" "${name}" "${name}" >>"${unattributable}"
                continue
            fi
            dir="$(build_dir_of "${pid}" "${cwd}")"
            printf '%s\t%s\tbuilds (%s)\n' "${dir}" "${pid}" "${name}" >>"${busy}"
        done < <(pgrep -u "$(id -u)" -x "${name}" 2>/dev/null || true)
    done

    process_paths |
        awk -F'\t' '
            {
                path = $2
                sub(/ \(deleted\)$/, "", path)
                split($1, part, "/")
                printf "%s\t%s\tholds it open\n", path, part[3]
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

# A tree cargo made and did not label. Its own layout file is what says so; the
# tag is missing. This is a reading and never a candidate: accepting another file
# in place of the tag would be judging by evidence this script picked rather than
# by cargo's own statement, and the deletion path is not the place to widen that.
# What it is for is naming the trees no run can reach -- a labelled tree that
# lost its tag stays invisible to every later run, so the only way it can be
# acted on is if a reading says it is there.
looks_like_cargo_cache() {
    [ -f "$1/.rustc_info.json" ]
}

find_candidates() { # file to list the cache-like trees that carry no tag
    # Every cargo cache on the filesystem the trigger was read from, not only
    # the ones under one configured directory. A second checkout is a checkout:
    # one living under $HOME rather than beside this one grew to 146 GiB without
    # a single reading naming it, because the enumeration started at a directory
    # someone had named rather than at the file system the water level was read
    # from. Which caches exist is a fact about the disk; which ones someone
    # remembered to configure is not.
    #
    # `${scan_depth}` is the reach this enumeration always had -- a worktree
    # nested inside a repository -- measured from where it now starts, so
    # widening the start did not narrow the reach. `-xdev` keeps the scan on the
    # judged filesystem: a cache on another one cannot move the reading that
    # fired, and freeing it would escalate for nothing.
    #
    # Sorted, because the order decides which trees are reached before the floor
    # ends the pass, and readdir order is a property of the filesystem rather
    # than of any decision: two machines in the same state would release
    # different files. The locale is pinned with it so the order is the bytes.
    find "${scan_root}" -xdev -maxdepth "${scan_depth}" -type d -name target -prune -print 2>/dev/null |
        LC_ALL=C sort |
        while IFS= read -r dir; do
            if [ -n "${bound}" ]; then
                case "${dir}" in
                "${bound}" | "${bound}"/*) ;;
                *) continue ;;
                esac
            fi
            if is_cargo_cache "${dir}"; then
                printf '%s\n' "${dir}"
            elif looks_like_cargo_cache "${dir}"; then
                # Off the candidate list and onto a side channel: the list is
                # this function's stdout, and a path that is not a candidate
                # must never be read as one.
                printf '%s\t%s\n' "${dir}" \
                    "$(human_kb "$(du -sk -- "${dir}" 2>/dev/null | cut -f1)")" >>"$1"
            fi
        done
}

# --- release -----------------------------------------------------------------

# What a stale pass finds, in one place because the plan and the run have to
# agree on it: a second hand-written copy of the predicate is how the reading a
# plan prints stops being the reading a run produces.
#
# The cache tag is excluded, and that is the whole reason this exists as a
# function rather than as the one-line find it used to be. The tag is written
# once, when cargo makes the tree, so it is the oldest file in every tree and
# the first thing an age filter matches -- deleting it takes the tree off the
# candidate list for good, since cargo does not rewrite a missing tag and no
# rebuild restores it. The cheapest tier would otherwise be the one that blinds
# the mechanism: measured on a fixture, the tag was gone after the first run and
# the tree was invisible to every run after it.
stale_files() { # target, find action...
    local target="$1"
    shift
    local minutes=$((stale_days * 24 * 60))
    find "${target}" -type f ! -name CACHEDIR.TAG -mmin "+${minutes}" "$@"
}

tier_stale() {
    if [ "${dry_run}" = 1 ]; then
        local count size
        read -r count size < <(stale_files "$1" -printf '%s\n' 2>/dev/null |
            awk '{ n++; s += $1 } END { print n + 0, s + 0 }')
        log "dry run would release ${count} stale file(s) under $1 ($(human_kb "${size}"))"
        return 0
    fi
    stale_files "$1" -delete 2>/dev/null || true
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

write_state() { # outcome, released_kb, gap_pct, targets, skipped, unreadable, unlabelled
    mkdir -p -- "$(dirname -- "${state_file}")"
    local tmp
    tmp="$(mktemp "${state_file}.XXXXXX")"
    cat >"${tmp}" <<EOF
{"last_run":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","outcome":"$1","trigger_pct":${trigger_pct},"floor_pct":${floor_pct},"used_pct_before":${before_pct},"used_pct_after":${disk_pct},"released_bytes":$(( $2 * 1024 )),"gap_pct":$3,"targets":$4,"skipped":$5,"unreadable":$6,"unlabelled":$7,"dry_run":${dry_run},"rev":"${script_rev}","rev_state":"${script_state}","script":"${script_sha}"}
EOF
    mv -f -- "${tmp}" "${state_file}"
}

# One reading per run, whatever it decided: the quantities an operator needs are
# how much was released, where the filesystem ended up, how far from the floor it
# stopped, and which version of this file produced the reading -- the last one is
# what makes any of the others reviewable later.
report() { # outcome, released_kb, covered, skipped, unreadable, unlabelled
    local gap=0
    [ "${disk_pct}" -gt "${floor_pct}" ] && gap=$((disk_pct - floor_pct))
    printf 'target-gc: outcome=%s trigger=%s%% floor=%s%% before=%s%% after=%s%% released=%s gap=%s%% caches=%s skipped=%s unlabelled=%s unreadable=%s rev=%s rev_state=%s script=%s\n' \
        "$1" "${trigger_pct}" "${floor_pct}" "${before_pct}" "${disk_pct}" \
        "$(human_kb "$2")" "${gap}" "$3" "$4" "$6" "$5" \
        "${script_rev}" "${script_state}" "${script_sha}"
    write_state "$1" "$2" "${gap}" "$3" "$4" "$5" "$6"
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
# The configured root still says which filesystem to judge -- the water level is
# read from it -- but it no longer says where to look for caches. The scan
# starts at that filesystem's mount point and reaches as deep as it did when it
# started at the root, so no cache on the judged disk is out of reach.
scan_root="$(fs_mount_point "${work_fs}")"
[ -n "${scan_root}" ] || die "cannot read the mount point of ${work_root}"
scan_depth="$((5 + $(depth_below "${scan_root}" "${work_root}")))"
readonly scan_root scan_depth

# What this run may release. Unset -- the shape a production unit runs in -- it
# is every cargo cache on the judged filesystem, because which caches exist is a
# fact about the disk and a list of directories only ever covers the ones
# somebody remembered to name. Set, it narrows the run to one subtree, and a
# narrowed run says so out loud: a bound that applied silently would make "there
# is no cache here" and "there is one, and this run was not allowed to reach it"
# the same reading.
bound=""
if [ -n "${delete_under}" ]; then
    [ -d "${delete_under}" ] ||
        die "COGNEVA_TARGET_GC_DELETE_UNDER=${delete_under} is not a directory"
    bound="$(cd "${delete_under}" && pwd -P)"
    [ "$(fs_identity "${bound}")" = "${work_fs}" ] ||
        die "COGNEVA_TARGET_GC_DELETE_UNDER=${bound} is not on the filesystem holding ${work_root}"
fi
readonly bound
[ -z "${bound}" ] || log "warning: this run releases only under ${bound}; a cargo cache anywhere else on the judged filesystem is left alone, and an empty result below is a statement about ${bound} rather than about the disk"
if [ -n "${bound}" ]; then
    searched="${bound}"
else
    searched="the filesystem holding ${work_root}"
fi
readonly searched

identity="$(script_identity)"
script_rev="${identity%% *}"
script_state="${identity##* }"
script_sha="$(sha256sum -- "${script_path}" 2>/dev/null | cut -c1-12)"
[ -n "${script_sha}" ] || script_sha="unreadable"
readonly script_rev script_state script_sha

# Nothing is deleted unless the file that would delete it is the committed one.
# The check sits before the candidates are enumerated: a tree edit that broke the
# enumeration itself would otherwise be reported as "the cache is not what is
# filling this filesystem", which reads like a finding about the disk and is
# really a finding about the code. A dry run passes -- it releases nothing, and
# the reading it prints carries the same rev_state for whoever reads the plan.
if [ "${script_state}" != committed ] && [ "${dry_run}" != 1 ]; then
    log "the script about to delete is not the version in the commit (rev=${script_rev} rev_state=${script_state}); releasing nothing in this run"
    report script_unverified 0 0 0 0 0
    exit 0
fi

# The unlabelled trees are listed by the enumeration, so the scratch has to exist
# before it runs -- and the process views are written here later.
released_dir="$(mktemp -d)"
trap 'rm -rf -- "${released_dir}"' EXIT

# The candidates are enumerated before the trigger is consulted, so `caches=` is a
# reading on every run instead of a zero written by the runs that never looked.
# "This mechanism can see no cache" and "this mechanism did not look" are two
# different findings about the disk, and the below-trigger runs are most of the
# runs there are: a count that only exists on the days the disk is full cannot
# show a tree that stopped being visible, which is how a cache drifts out of
# reach without anyone noticing.
mapfile -t candidates < <(find_candidates "${released_dir}/unlabelled")
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

unlabelled=0
if [ -s "${released_dir}/unlabelled" ]; then
    unlabelled="$(wc -l <"${released_dir}/unlabelled")"
    log "${unlabelled} tree(s) under ${searched} look like cargo caches and carry no CACHEDIR.TAG, so no run can judge them:"
    # The listing is capped and the cap is named, following the busy listing: a
    # count that quietly stops is a wrong count, but an unbounded list is not a
    # reading either.
    head -n 5 "${released_dir}/unlabelled" |
        while IFS=$'\t' read -r path size; do
            log "  ${path}: ${size} on disk, cargo's .rustc_info.json present, no cache tag"
        done
    [ "${unlabelled}" -le 5 ] || log "  (+$((unlabelled - 5)) more)"
fi

if [ "${disk_pct}" -lt "${trigger_pct}" ]; then
    # A dry run is a question about what the plan would be, not about whether
    # today is the day: it answers the same way at 33% as at 80%. A real run
    # below the trigger releases nothing, so what it has to say is what it can
    # see -- the inventory above is the reading, and it is the same one the
    # triggered runs report.
    if [ "${dry_run}" != 1 ]; then
        log "below trigger: ${disk_pct}% used, trigger ${trigger_pct}%"
        report below_trigger 0 "${covered}" "${skipped}" 0 "${unlabelled}"
        exit 0
    fi
    log "below trigger: ${disk_pct}% used, trigger ${trigger_pct}% -- this is a dry run, so the plan below is what a run would do on the day the trigger is reached"
fi

if [ "${covered}" -eq 0 ]; then
    log "nothing to release: ${disk_pct}% used, trigger ${trigger_pct}%, no cargo cache under ${searched} -- the cache is not what is filling this filesystem$(
        [ "${unlabelled}" -eq 0 ] || printf ', and %s tree(s) that look like caches cannot be judged at all' "${unlabelled}"
    )"
    report floor_unreachable 0 "${#candidates[@]}" "${skipped}" 0 "${unlabelled}"
    exit 0
fi

if [ "${dry_run}" = 1 ]; then
    log "dry run: ${covered} cache(s) under ${searched}, ${disk_pct}% used, trigger ${trigger_pct}%, floor ${floor_pct}% -- nothing is released, so the plan runs to the deepest tier a real run would reach"
    for target in "${candidates[@]}"; do
        log "candidate ${target}: $(human_kb "$(du -sk -- "${target}" 2>/dev/null | cut -f1)") on disk"
    done
fi

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
        # A dry run releases nothing, so the reading never moves and the floor
        # can never be reached: it walks every tier and every candidate, which
        # is what makes it a plan rather than a first step.
        if [ "${dry_run}" != 1 ]; then
            [ "${disk_pct}" -gt "${floor_pct}" ] || break
        fi
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

    if [ "${dry_run}" = 1 ]; then
        continue
    fi
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
if [ "${dry_run}" = 1 ]; then
    # A dry run releases nothing, so the difference between the two disk readings
    # is the rest of the machine writing, not this run. Reporting it as released
    # bytes would put a number next to a plan that is somebody else's traffic.
    released_kb=0
fi

if [ "${outcome}" != probe_failed ]; then
    if [ "${dry_run}" = 1 ]; then
        # Nothing was released, so "released" and "floor_unreachable" would both
        # be statements about a run that did not happen: the plan is the reading.
        outcome="planned"
    elif [ "${disk_pct}" -le "${floor_pct}" ]; then
        outcome="released"
    elif [ "${any_progress:-0}" = 1 ]; then
        outcome="floor_unreachable"
    fi
fi

gap=0
[ "${disk_pct}" -gt "${floor_pct}" ] && gap=$((disk_pct - floor_pct))

if [ "${outcome}" = floor_unreachable ]; then
    log "still above the trigger after every tier: the build cache is not what is filling this filesystem (gap ${gap} points)$(
        [ "${unlabelled}" -eq 0 ] || printf ', and %s cache-like tree(s) no run can judge were never in reach' "${unlabelled}"
    )"
fi
if [ "${outcome}" = planned ]; then
    log "dry run: nothing was released, so this plan is what a run would do today, not what happened"
fi
# One site prints the reading and writes the state: two of them would be two
# chances for a field to exist in one and not the other.
report "${outcome}" "${released_kb}" "${covered}" "${skipped}" "${unreadable:-0}" "${unlabelled}"
